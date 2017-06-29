
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log;
extern crate rand;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::iter;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

use fuzzy_log::async::fuzzy_log::log_handle::LogHandle;
use fuzzy_log::packets::order;

use rand::distributions::{Normal, Sample};

use structopt::StructOpt;


#[derive(StructOpt, Debug)]
#[structopt(name = "micropayments_bench", about = "micropayments example benchmark.")]
struct Args {
    #[structopt(help = "head of the FuzzyLog server chain to run against.")]
    head_server: SocketAddr,

    #[structopt(help = "tail of the FuzzyLog server chain to run against.")]
    tail_server: Option<SocketAddr>,

    #[structopt(short="t", long="time_per_round", help = "number of milliseconds per round.", default_value = "2000")]
    ms_per_round: usize,

    #[structopt(short="r", long="rounds", help = "number of rounds.", default_value = "5")]
    num_rounds: usize,

    #[structopt(short="i", long="inc_window", help = "Number of outstanding increments.", default_value = "100")]
    inc_window: usize,

    #[structopt(short="d", long="dec_window", help = "Number of outstanding decrements.", default_value = "2")]
    dec_window: usize,

    #[structopt(short = "b", long = "baseline", help = "Run non-fuzzy version (used for comparison).")]
    baseline: bool,

    #[structopt(short = "s", long = "single_threaded", help = "Handle both increments and decrements in the same thread.")]
    single_thread: bool,

    #[structopt(short = "l", long = "split", help = "split threads in a basic into increment and decerement threads.")]
    split: bool,
}

//remove range normal around 10_000
//insert 5-15, avg 10
// avg 1000 inserts per removal

fn main() {
    let start_time = Instant::now();
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();
    println!("#{:?}", args);
    let num_incs = AtomicUsize::new(0);
    let num_decs = AtomicUsize::new(0);
    let done = AtomicBool::new(false);

    let (avg_incs, avg_decs) =
        crossbeam::scope(|scope| {
            let mut joins = Vec::with_capacity(2);
            match (args.single_thread, args.baseline) {
                (true, true) => {
                    let log = scope.spawn(|| baseline(&num_incs, &num_decs, &done, &args));
                    joins.push(log)
                },
                (true, false) => {
                    let log = scope.spawn(|| fuzzy(&num_incs, &num_decs, &done, &args));
                    joins.push(log)
                },
                (false, true) => {
                    if args.split {
                        let inc = scope.spawn(|| baseline_inc(&num_incs, &done, &args));
                        joins.push(inc);
                        let dec = scope.spawn(|| baseline_dec(&num_decs, &done, &args));
                        joins.push(dec)
                    } else {
                        let t0 = scope.spawn(|| baseline(&num_incs, &num_decs, &done, &args));
                        joins.push(t0);
                        let t1 = scope.spawn(|| baseline(&num_incs, &num_decs, &done, &args));
                        joins.push(t1)
                    }

                },
                (false, false) => {
                    let inc = scope.spawn(|| fuzzy_inc(&num_incs, &done, &args));
                    joins.push(inc);
                    let dec = scope.spawn(|| fuzzy_dec(&num_decs, &done, &args));
                    joins.push(dec)
                },
            }
            let avgs = scope.spawn(|| collector(&num_incs, &num_decs, &done, &args));
            for join in joins {
                join.join()
            }
            avgs.join()
        });
    println!("#inc Hz {:?}", avg_incs);
    println!("#dec Hz {:?}", avg_decs);
    let is: usize = avg_incs.iter().sum();
    println!("inc {:?}, {:?}, {:?}",
        avg_incs.iter().min(), is / avg_incs.len(), avg_incs.iter().max()
    );
    let ds: usize = avg_decs.iter().sum();
    println!("dec {:?}, {:?}, {:?}",
        avg_decs.iter().min(), ds / avg_decs.len(), avg_decs.iter().max()
    );
    println!("#elapsed time {:?}", start_time.elapsed());

    // let num_started = AtomiUsize::new(0);
    // let (avg_incs, avg_decs) = if args.baseline {
    //     crossbeam::scope(|scope| {
    //         let avg_incs = scope.spawn(|| baseline_writer(&num_started));
    //         let avg_decs = scope.spawn(|| baseline_reader(&num_started));
    //         (avg_incs, avg_decs)
    //     })
    // } else {
    //     crossbeam::scope(|scope| {
    //         let avg_incs = scope.spawn(|| fuzzy_writer(&num_started));
    //         let avg_decs = scope.spawn(|| fuzzy_reader(&num_started));
    //         (avg_incs, avg_decs)
    //     })
    // };
    // println!("read Hz  {:?}", avg_incs);
    // println!("write Hz {:?}", avg_decs);
}

fn baseline(num_incs: &AtomicUsize, num_decs: &AtomicUsize, done: &AtomicBool, args: &Args) {
    let chain = order::from(1);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::new_tcp_log_with_replication(
            iter::once((args.head_server, tail_server)), iter::once(chain)
        )
    } else {
        LogHandle::new_tcp_log(
            iter::once(args.head_server), iter::once(order::from(chain))
        )
    };

    let mut outstanding_incs = VecDeque::with_capacity(
        args.inc_window / if args.single_thread { 1 } else { 2 }
    );
    let mut outstanding_decs = VecDeque::with_capacity(args.dec_window
        / if args.single_thread { 1 } else { 2 });

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10.0, 2.0);
    let mut inc_sample = || dist.sample(&mut rng).abs();
    for _ in 0..outstanding_incs.capacity() {
        let id = log.async_append(chain, &inc_sample(), &[]);
        outstanding_incs.push_back(id);
    }

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10_000.0, 200.0);
    let mut dec_sample = || -dist.sample(&mut rng).abs();

    let mut balance = 0.0;

    log.snapshot(chain);
    while !done.load(Ordering::Relaxed) {
        let _ = log.flush_completed_appends();
        while let Ok((&v, _, id)) = log.try_get_next2() {
            if outstanding_incs.front() == Some(id) {
                outstanding_incs.pop_front();
                num_incs.fetch_add(1, Ordering::Relaxed);
            } else if outstanding_decs.front() == Some(id) {
                outstanding_decs.pop_front();
                num_decs.fetch_add(1, Ordering::Relaxed);
            } else {
                //
            }
            if v.is_sign_positive() {
                balance += v
            } else if balance >= v {
                balance += v
            } else {
                //TODO num_failures.fetch_add(1, Ordering::Relaxed);
            }
        }

        let mut new_snap = false;
        if outstanding_incs.len() < outstanding_incs.capacity() {
            let num_new_ops = outstanding_incs.capacity() - outstanding_incs.len();
            for _ in 0..num_new_ops {
                let id = log.async_append(chain, &inc_sample(), &[]);
                outstanding_incs.push_back(id);
            }
            new_snap = true;
        }

        if outstanding_decs.len() < outstanding_decs.capacity() {
            let num_new_ops = outstanding_decs.capacity() - outstanding_decs.len();
            for _ in 0..num_new_ops {
                let id = log.async_append(chain, &dec_sample(), &[]);
                outstanding_decs.push_back(id);
            }
            new_snap = true;
        }

        if new_snap {
            log.snapshot(chain)
        }
    }
}

fn baseline_inc(num_incs: &AtomicUsize, done: &AtomicBool, args: &Args) {
    let chain = order::from(1);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::new_tcp_log_with_replication(
            iter::once((args.head_server, tail_server)), iter::once(chain)
        )
    } else {
        LogHandle::new_tcp_log(
            iter::once(args.head_server), iter::once(order::from(chain))
        )
    };

    let mut outstanding_incs = VecDeque::with_capacity(args.inc_window);

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10.0, 2.0);
    let mut inc_sample = || dist.sample(&mut rng).abs();
    for _ in 0..outstanding_incs.capacity() {
        let id = log.async_append(chain, &inc_sample(), &[]);
        outstanding_incs.push_back(id);
    }

    let mut balance = 0.0;

    log.snapshot(chain);
    while !done.load(Ordering::Relaxed) {
        let _ = log.flush_completed_appends();
        while let Ok((&v, _, id)) = log.try_get_next2() {
            if outstanding_incs.front() == Some(id) {
                outstanding_incs.pop_front();
                num_incs.fetch_add(1, Ordering::Relaxed);
            }

            if v.is_sign_positive() {
                balance += v
            } else if balance >= v {
                balance += v
            } else {
                //TODO num_failures.fetch_add(1, Ordering::Relaxed);
            }
        }

        if outstanding_incs.len() < outstanding_incs.capacity() {
            let num_new_ops = outstanding_incs.capacity() - outstanding_incs.len();
            for _ in 0..num_new_ops {
                let id = log.async_append(chain, &inc_sample(), &[]);
                outstanding_incs.push_back(id);
            }
            log.snapshot(chain)
        }
    }
}

fn baseline_dec(num_decs: &AtomicUsize, done: &AtomicBool, args: &Args) {
    let chain = order::from(1);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::new_tcp_log_with_replication(
            iter::once((args.head_server, tail_server)), iter::once(chain)
        )
    } else {
        LogHandle::new_tcp_log(
            iter::once(args.head_server), iter::once(order::from(chain))
        )
    };

    let mut outstanding_decs = VecDeque::with_capacity(args.dec_window);

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10_000.0, 200.0);
    let mut dec_sample = || -dist.sample(&mut rng).abs();

    let mut balance = 0.0;

    log.snapshot(chain);
    while !done.load(Ordering::Relaxed) {
        let _ = log.flush_completed_appends();
        while let Ok((&v, _, id)) = log.try_get_next2() {
            let v: f64 = v;
            if outstanding_decs.front() == Some(id) {
                outstanding_decs.pop_front();
                num_decs.fetch_add(1, Ordering::Relaxed);
            }

            if v.is_sign_positive() {
                balance += v
            } else if balance >= v {
                balance += v
            } else {
                //TODO num_failures.fetch_add(1, Ordering::Relaxed);
            }
        }

        if outstanding_decs.len() < outstanding_decs.capacity() {
            let num_new_ops = outstanding_decs.capacity() - outstanding_decs.len();
            for _ in 0..num_new_ops {
                let id = log.async_append(chain, &dec_sample(), &[]);
                outstanding_decs.push_back(id);

            }
            log.snapshot(chain)
        }
    }
}

fn fuzzy(num_incs: &AtomicUsize, num_decs: &AtomicUsize, done: &AtomicBool, args: &Args) {
    let inc_chain = order::from(1);
    let dec_chain = order::from(2);

    let mut inc_log = if let Some(tail_server) = args.tail_server {
        LogHandle::new_tcp_log_with_replication(
            iter::once((args.head_server, tail_server)), iter::once(inc_chain)
        )
    } else {
        LogHandle::new_tcp_log(
            iter::once(args.head_server), iter::once(order::from(inc_chain))
        )
    };

    let mut dec_log = if let Some(tail_server) = args.tail_server {
        LogHandle::new_tcp_log_with_replication(
            iter::once((args.head_server, tail_server)), iter::once(dec_chain)
        )
    } else {
        LogHandle::new_tcp_log(
            iter::once(args.head_server), iter::once(order::from(dec_chain))
        )
    };

    let mut outstanding_incs = VecDeque::with_capacity(args.inc_window);
    let mut outstanding_digests = VecDeque::with_capacity(args.inc_window);
    let mut outstanding_decs = VecDeque::with_capacity(args.dec_window);

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10.0, 2.0);
    let mut inc_sample = || dist.sample(&mut rng).abs();
    for _ in 0..outstanding_incs.capacity() {
        let val = inc_sample();
        let id = inc_log.async_append(inc_chain, &val, &[]);
        // let id = log.async_dependent_multiappend(
        //     &[inc_chain], &[dec_chain], &inc_sample(), &[]
        // );
        outstanding_incs.push_back((val, id));
    }

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10_000.0, 200.0);
    let mut dec_sample = || -dist.sample(&mut rng).abs();

    let mut inc_balance = 0.0;
    let mut inc_added = 0.0;

    let mut dec_balance = 0.0;
    inc_log.snapshot(inc_chain);
    dec_log.snapshot(dec_chain);
    while !done.load(Ordering::Relaxed) {
        while let Ok((id, locs)) = inc_log.try_wait_for_any_append() {
            //TODO
            //if loc != expected {
                // read others change and update out state
            //}
            if Some(id) == outstanding_incs.front().map(|i| i.1) {
                let (v, _) = outstanding_incs.pop_front().unwrap();
                inc_balance += v;
                inc_added += v;
                if inc_added < 5_000.0 {
                    num_incs.fetch_add(1, Ordering::Relaxed);
                } else {
                    let id = inc_log.async_append(dec_chain, &inc_added, &[locs[0]]);
                    outstanding_digests.push_back(id);
                    inc_added = 0.0;
                }
            } else if Some(&id) == outstanding_digests.front() {
                outstanding_digests.pop_front();
                num_incs.fetch_add(1, Ordering::Relaxed);
            } else {

            }
        }
        let _ = dec_log.flush_completed_appends();

        while let Ok((&v, l, id)) = dec_log.try_get_next2() {
            assert_eq!(l.len(), 1);
            let v: f64 = v;
            if outstanding_decs.front() == Some(id) {
                outstanding_decs.pop_front();
                num_decs.fetch_add(1, Ordering::Relaxed);
            }

            if v.is_sign_positive() {
                dec_balance += v
            } else if dec_balance >= v {
                dec_balance += v
            } else {
                //TODO num_failures.fetch_add(1, Ordering::Relaxed);
            }
        }

        if outstanding_incs.len() < outstanding_incs.capacity() {
            let num_new_ops = outstanding_incs.capacity() - outstanding_incs.len();
            for _ in 0..num_new_ops {
                let v = inc_sample();
                let id = inc_log.async_append(inc_chain, &v, &[]);
                // let id = log.async_dependent_multiappend(
                //     &[inc_chain], &[dec_chain], &inc_sample(), &[]
                // );
                outstanding_incs.push_back((v, id));
            }
            inc_log.snapshot(inc_chain)
        }

        if outstanding_decs.len() < outstanding_decs.capacity() {
            let num_new_ops = outstanding_decs.capacity() - outstanding_decs.len();
            for _ in 0..num_new_ops {
                let id = dec_log.async_append(dec_chain, &dec_sample(), &[]);
                // let id = log.async_dependent_multiappend(
                //     &[dec_chain], &[inc_chain], &dec_sample(), &[]
                // );
                outstanding_decs.push_back(id);

            }
            dec_log.snapshot(dec_chain)
        }
    }
}

fn fuzzy_inc(num_incs: &AtomicUsize, done: &AtomicBool, args: &Args) {
    let inc_chain = order::from(1);
    let dec_chain = order::from(2);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::new_tcp_log_with_replication(
            iter::once((args.head_server, tail_server)), iter::once(inc_chain)
        )
    } else {
        LogHandle::new_tcp_log(
            iter::once(args.head_server), iter::once(order::from(inc_chain))
        )
    };

    let mut outstanding_incs = VecDeque::with_capacity(args.inc_window);
    let mut outstanding_digests = VecDeque::with_capacity(args.inc_window);

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10.0, 2.0);
    let mut inc_sample = || dist.sample(&mut rng).abs();
    for _ in 0..outstanding_incs.capacity() {
        let val = inc_sample();
        let id = log.async_append(inc_chain, &val, &[]);
        // let id = log.async_dependent_multiappend(
        //     &[inc_chain], &[dec_chain], &inc_sample(), &[]
        // );
        outstanding_incs.push_back((val, id));
    }

    let mut balance = 0.0;
    let mut added = 0.0;

    log.snapshot(inc_chain);
    while !done.load(Ordering::Relaxed) {
        while let Ok((id, locs)) = log.try_wait_for_any_append() {
            //TODO
            //if loc != expected {
                // read others change and update out state
            //}
            if Some(id) == outstanding_incs.front().map(|i| i.1) {
                let (v, _) = outstanding_incs.pop_front().unwrap();
                balance += v;
                added += v;
                if added < 5_000.0 {
                    num_incs.fetch_add(1, Ordering::Relaxed);
                } else {
                    let id = log.async_append(dec_chain, &added, &[locs[0]]);
                    outstanding_digests.push_back(id);
                    added = 0.0;
                }
            } else if Some(&id) == outstanding_digests.front() {
                outstanding_digests.pop_front();
                num_incs.fetch_add(1, Ordering::Relaxed);
            } else {

            }
        }

        // while let Ok((v, loc, id)) = log.try_get_next2()
        //     .map(|(&v, l, &i)| { assert_eq!(l.len(), 1); (v, l[0], i) }) {
        //     let v: f64 = v;
        //     let my_inc = outstanding_incs.front() == Some(&id);
        //     if my_inc { outstanding_incs.pop_front(); }

        //     assert!(v.is_sign_positive());
        //     balance += v;
        //     added += v;
        //     if added >= 5_000.0 {
        //         if my_inc {
        //             let id = log.async_append(dec_chain, &added, &[loc]);
        //             outstanding_digests.push_back(id);
        //         }
        //         added = 0.0;
        //     } else if my_inc {
        //         num_incs.fetch_add(1, Ordering::Relaxed);
        //     }
        // }

        if outstanding_incs.len() < outstanding_incs.capacity() {
            let num_new_ops = outstanding_incs.capacity() - outstanding_incs.len();
            for _ in 0..num_new_ops {
                let v = inc_sample();
                let id = log.async_append(inc_chain, &v, &[]);
                // let id = log.async_dependent_multiappend(
                //     &[inc_chain], &[dec_chain], &inc_sample(), &[]
                // );
                outstanding_incs.push_back((v, id));
            }
            log.snapshot(inc_chain)
        }
    }
}
fn fuzzy_dec(num_decs: &AtomicUsize, done: &AtomicBool, args: &Args) {
    let inc_chain = order::from(1);
    let dec_chain = order::from(2);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::new_tcp_log_with_replication(
            iter::once((args.head_server, tail_server)), iter::once(dec_chain)
        )
    } else {
        LogHandle::new_tcp_log(
            iter::once(args.head_server), iter::once(order::from(dec_chain))
        )
    };

    let mut outstanding_decs = VecDeque::with_capacity(args.dec_window);

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10_000.0, 200.0);
    let mut dec_sample = || -dist.sample(&mut rng).abs();

    let mut balance = 0.0;

    log.snapshot(dec_chain);
    while !done.load(Ordering::Relaxed) {
        let _ = log.flush_completed_appends();
        while let Ok((&v, l, id)) = log.try_get_next2() {
            assert_eq!(l.len(), 1);
            let v: f64 = v;
            if outstanding_decs.front() == Some(id) {
                outstanding_decs.pop_front();
                num_decs.fetch_add(1, Ordering::Relaxed);
            }

            if v.is_sign_positive() {
                balance += v
            } else if balance >= v {
                balance += v
            } else {
                //TODO num_failures.fetch_add(1, Ordering::Relaxed);
            }
        }

        if outstanding_decs.len() < outstanding_decs.capacity() {
            let num_new_ops = outstanding_decs.capacity() - outstanding_decs.len();
            for _ in 0..num_new_ops {
                let id = log.async_append(dec_chain, &dec_sample(), &[]);
                // let id = log.async_dependent_multiappend(
                //     &[dec_chain], &[inc_chain], &dec_sample(), &[]
                // );
                outstanding_decs.push_back(id);

            }
            log.snapshot(dec_chain)
        }
    }
}

fn collector(num_incs: &AtomicUsize, num_decs: &AtomicUsize, done: &AtomicBool, args: &Args)
-> (Vec<usize>, Vec<usize>, ) {
    let (mut avg_incs, mut avg_decs) =
        (Vec::with_capacity(args.num_rounds), Vec::with_capacity(args.num_rounds));
    let (mut last_incs, mut last_decs) = (0, 0);
    for _ in 0..args.num_rounds {
        sleep(Duration::from_millis(args.ms_per_round as u64));
        let incs = num_incs.load(Ordering::Relaxed);
        let decs = num_decs.load(Ordering::Relaxed);
        let new_incs = incs - last_incs;
        let new_decs = decs - last_decs;
        last_incs = incs;
        last_decs = decs;
        avg_incs.push(new_incs);
        avg_decs.push(new_decs);
    }
    done.store(true, Ordering::Relaxed);
    (avg_incs, avg_decs)
}

/*fn baseline_writer(num_started: &AtomicUsize) {
    let Args{ ref servers, ref write_window, ref ms_per_round, ref num_rounds, .. } = args;
}

fn baseline_reader(num_started: &AtomicUsize) {
    let Args{ ref servers, ref write_window, ref ms_per_round, ref num_rounds, .. } = args;
}

fn fuzzy_writer(num_started: &AtomicUsize) {
    let Args{ ref servers, ref write_window, ref ms_per_round, ref num_rounds, .. } = args;
    unimplemented!()
}

fn fuzzy_reader(num_started: &AtomicUsize) {
    let Args{ ref servers, ref write_window, ref ms_per_round, ref num_rounds, .. } = args;
    unimplemented!()
}*/
