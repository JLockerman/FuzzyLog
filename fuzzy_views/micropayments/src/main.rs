
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

use fuzzy_log::async::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log::packets::{order, OrderIndex};

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

    #[structopt(short="w", long="workers", help = "incrementing threads.", default_value = "2")]
    workers: usize,

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
    let avg_inc_latency = (0..args.workers).map(|_| AtomicUsize::new(0)).collect();
    let avg_dec_latency = AtomicUsize::new(0);
    let done = AtomicBool::new(false);

    if args.workers > 0 {
        assert!(!args.single_thread,
            "Cannot change the number of workers in single threaded mode"
        );
    }

    let num_decs = &num_decs;
    let num_incs = &num_incs;
    let avg_inc_latency = &avg_inc_latency;
    let avg_dec_latency = &avg_dec_latency;
    let done = &done;
    let args = &args;

    let (avg_incs, avg_decs, inc_latencies, dec_latencies) =
        crossbeam::scope(|scope| {
            let mut joins = Vec::with_capacity(args.workers + 1);
            match (args.single_thread, args.baseline) {
                (true, true) => {
                    let log = scope.spawn(|| baseline(
                            num_incs,
                            num_decs,
                            avg_inc_latency,
                            avg_dec_latency,
                            done,
                            args,
                            0,
                    ));
                    joins.push(log)
                },
                (true, false) => {
                    let log = scope.spawn(move || fuzzy(
                        num_incs, num_decs, avg_inc_latency, avg_dec_latency, done, args
                    ));
                    joins.push(log)
                },
                (false, true) => {
                    if args.split {
                        for i in 0..args.workers {
                            let inc = scope.spawn(move || baseline_inc(
                                num_incs, avg_inc_latency, done, args, i
                            ));
                            joins.push(inc);
                        }
                        let dec = scope.spawn(move || baseline_dec(
                            num_decs, avg_dec_latency, done, args
                        ));
                        joins.push(dec)
                    } else {
                        for i in 0..args.workers {
                            let t = scope.spawn(move || baseline(
                                num_incs,
                                num_decs,
                                avg_inc_latency,
                                avg_dec_latency,
                                done,
                                args,
                                i,
                            ));
                            joins.push(t);
                        }
                        let t = scope.spawn(move || baseline(
                            num_incs,
                            num_decs,
                            avg_inc_latency,
                            avg_dec_latency,
                            done,
                            args,
                            0
                        ));
                        joins.push(t);
                    }

                },
                (false, false) => {
                    for i in 0..args.workers {
                        let inc = scope.spawn(move || fuzzy_inc(
                            num_incs, avg_inc_latency, done, args, i
                        ));
                        joins.push(inc);
                    }
                    let dec = scope.spawn(|| fuzzy_dec(
                        num_decs, avg_dec_latency, done, args
                    ));
                    joins.push(dec)
                },
            }
            let avgs = scope.spawn(|| collector(
                num_incs, num_decs, avg_inc_latency, avg_dec_latency, done, args
            ));
            for join in joins {
                join.join()
            }
            avgs.join()
        });
    println!("#inc tr {:?}", avg_incs);
    println!("#dec tr {:?}", avg_decs);
    println!("#inc la {:?}", inc_latencies);
    println!("#dec la {:?}", dec_latencies);
    let secs_per_round = args.ms_per_round / 1000;
    let is: usize = avg_incs.iter().sum();
    let is = is / avg_incs.len();
    let ils= inc_latencies.last().cloned().unwrap();
    println!("inc {:?}, {:?}, {:?} | {:?}",
        avg_incs.iter().min(), is / secs_per_round, avg_incs.iter().max(), ils
    );
    let ds: usize = avg_decs.iter().sum();
    let ds = ds / avg_decs.len();
    let dls = dec_latencies.last().cloned().unwrap();
    println!("dec {:?}, {:?}, {:?} | {:?}",
        avg_decs.iter().min(), ds / secs_per_round, avg_decs.iter().max(), dls
    );
    println!("#elapsed time {:?}, {:?} incs, {:?} decs",
        start_time.elapsed(), num_incs, num_decs);

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

#[derive(Debug, Clone)]
struct MovingAvg {
    avg: usize,
    n: usize,
}

impl MovingAvg {
    fn new() -> Self {
        MovingAvg{avg: 0, n: 0}
    }

    fn add_point(&mut self, p: usize) {
        self.avg = (p + self.n * self.avg) / (self.n + 1);
        self.n += 1;
    }

    fn val(&self) -> usize {
        self.avg
    }
}

fn baseline(
    num_incs: &AtomicUsize,
    num_decs: &AtomicUsize,
    inc_latency: &Vec<AtomicUsize>,
    dec_latency: &AtomicUsize,
    done: &AtomicBool,
    args: &Args,
    worker_num: usize,
) {
    let inc_latency = &inc_latency[worker_num];

    let chain = order::from(1);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(iter::once(order::from(chain)))
            .reads_my_writes()
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(iter::once(order::from(chain)))
            .reads_my_writes()
            .build()
    };

    let mut outstanding_incs = VecDeque::with_capacity(args.inc_window);
    let mut outstanding_decs = VecDeque::with_capacity(args.dec_window
        / if args.single_thread { 1 } else { args.workers }
    );

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10.0, 2.0);
    let mut inc_sample = || dist.sample(&mut rng).abs();
    for _ in 0..outstanding_incs.capacity() {
        let id = log.async_append(chain, &(inc_sample(), Instant::now()), &[]);
        outstanding_incs.push_back(id);
    }

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10_000.0, 200.0);
    let mut dec_sample = || -dist.sample(&mut rng).abs();

    let mut balance = 0.0;
    let mut avg_inc_latency = MovingAvg::new();
    let mut avg_dec_latency = MovingAvg::new();

    while !done.load(Ordering::Relaxed) {
        let mut max_index = None;
        while let Ok((_, locs)) = log.try_wait_for_any_append() {
            if max_index.map(|OrderIndex(_, i)| i < locs[0].1).unwrap_or(true) {
                max_index = Some(locs[0]);
            }
        }
        if let Some(loc) = max_index {
            log.read_until(loc)
        }
        'recv: loop {
            match log.try_get_next2() {
                Ok((&(v, t), _, id)) => {
                    if outstanding_incs.front() == Some(id) {
                        outstanding_incs.pop_front();
                        num_incs.fetch_add(1, Ordering::Relaxed);
                        avg_inc_latency.add_point(t.elapsed().subsec_nanos() as usize);
                        inc_latency.store(avg_inc_latency.val(), Ordering::Relaxed);
                    } else if outstanding_decs.front() == Some(id) {
                        outstanding_decs.pop_front();
                        num_decs.fetch_add(1, Ordering::Relaxed);
                        avg_dec_latency.add_point(t.elapsed().subsec_nanos() as usize);
                        dec_latency.store(avg_dec_latency.val(), Ordering::Relaxed);
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
                },
                Err(..) => break 'recv,
            }
        }

        if outstanding_incs.len() < outstanding_incs.capacity() {
            let num_new_ops = outstanding_incs.capacity() - outstanding_incs.len();
            for _ in 0..num_new_ops {
                let id = log.async_append(chain, &(inc_sample(), Instant::now()), &[]);
                outstanding_incs.push_back(id);
            }
        }

        if outstanding_decs.len() < outstanding_decs.capacity() {
            let num_new_ops = outstanding_decs.capacity() - outstanding_decs.len();
            for _ in 0..num_new_ops {
                let id = log.async_append(chain, &(dec_sample(), Instant::now()), &[]);
                outstanding_decs.push_back(id);
            }
        }
    }
}

fn baseline_inc(
    num_incs: &AtomicUsize, inc_latency: &Vec<AtomicUsize>, done: &AtomicBool, args: &Args,
    worker_num: usize,
) {
    if args.inc_window == 0 {
        return
    }

    let inc_latency = &inc_latency[worker_num];

    let chain = order::from(1);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(iter::once(order::from(chain)))
            .reads_my_writes()
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(iter::once(order::from(chain)))
            .reads_my_writes()
            .build()
    };

    let mut outstanding_incs = VecDeque::with_capacity(args.inc_window);

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10.0, 2.0);
    let mut inc_sample = || dist.sample(&mut rng).abs();
    for _ in 0..outstanding_incs.capacity() {
        let id = log.async_append(chain, &(inc_sample(), Instant::now()), &[]);
        outstanding_incs.push_back(id);
    }

    let mut balance = 0.0;
    let mut avg_inc_latency = MovingAvg::new();

    while !done.load(Ordering::Relaxed) {
        let mut max_index = None;
        while let Ok((_, locs)) = log.try_wait_for_any_append() {
            if max_index.map(|OrderIndex(_, i)| i < locs[0].1).unwrap_or(true) {
                max_index = Some(locs[0]);
            }
        }
        if let Some(loc) = max_index {
            log.read_until(loc)
        }
        'recv: loop {
            match log.try_get_next2() {
                Ok((&(v, t), _, id)) => {
                    if outstanding_incs.front() == Some(id) {
                        outstanding_incs.pop_front();
                        num_incs.fetch_add(1, Ordering::Relaxed);
                        avg_inc_latency.add_point(t.elapsed().subsec_nanos() as usize);
                        inc_latency.store(avg_inc_latency.val(), Ordering::Relaxed);
                    }

                    if v.is_sign_positive() {
                        balance += v
                    } else if balance >= v {
                        balance += v
                    } else {
                        //TODO num_failures.fetch_add(1, Ordering::Relaxed);
                    }
                },
                Err(..) => break 'recv,
            }
        }

        if outstanding_incs.len() < outstanding_incs.capacity() {
            let num_new_ops = outstanding_incs.capacity() - outstanding_incs.len();
            for _ in 0..num_new_ops {
                let id = log.async_append(chain, &(inc_sample(), Instant::now()), &[]);
                outstanding_incs.push_back(id);
            }
        }
    }
}

fn baseline_dec(
    num_decs: &AtomicUsize, dec_latency: &AtomicUsize, done: &AtomicBool, args: &Args
) {
    let chain = order::from(1);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(iter::once(order::from(chain)))
            .reads_my_writes()
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(iter::once(order::from(chain)))
            .reads_my_writes()
            .build()
    };

    let mut outstanding_decs = VecDeque::with_capacity(args.dec_window);

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10_000.0, 200.0);
    let mut dec_sample = || -dist.sample(&mut rng).abs();

    let mut balance = 0.0;
    let mut avg_dec_latency = MovingAvg::new();

    while !done.load(Ordering::Relaxed) {
        // let _ = log.flush_completed_appends();
        let mut max_index = None;
        while let Ok((_, locs)) = log.try_wait_for_any_append() {
            if max_index.map(|OrderIndex(_, i)| i < locs[0].1).unwrap_or(true) {
                max_index = Some(locs[0]);
            }
        }
        if let Some(loc) = max_index {
            log.read_until(loc)
        }
        'recv: loop {
            match log.try_get_next2() {
                Ok((&(v, t), _, id)) => {
                    let v: f64 = v;
                    let t: Instant = t;
                    if outstanding_decs.front() == Some(id) {
                        outstanding_decs.pop_front();
                        num_decs.fetch_add(1, Ordering::Relaxed);
                        let t = t.elapsed().subsec_nanos();
                        avg_dec_latency.add_point(t as usize);
                        dec_latency.store(avg_dec_latency.val(), Ordering::Relaxed);
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
                },
                Err(..) => break 'recv,
            }
        }

        if outstanding_decs.len() < outstanding_decs.capacity() {
            let num_new_ops = outstanding_decs.capacity() - outstanding_decs.len();
            for _ in 0..num_new_ops {
                let v = (dec_sample(), Instant::now());
                let id = log.async_append(chain, &v, &[]);
                outstanding_decs.push_back(id);
            }
        }
    }
}

fn fuzzy(
    num_incs: &AtomicUsize,
    num_decs: &AtomicUsize,
    inc_latency: &Vec<AtomicUsize>,
    dec_latency: &AtomicUsize,
    done: &AtomicBool,
    args: &Args
) {
    let inc_latency = &inc_latency[0];

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
        let val = (inc_sample(), Instant::now());
        let id = inc_log.async_append(inc_chain, &val, &[]);
        // let id = log.async_dependent_multiappend(
        //     &[inc_chain], &[dec_chain], &inc_sample(), &[]
        // );
        outstanding_incs.push_back((val, id));
    }

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10_000.0, 200.0);
    let mut dec_sample = || -dist.sample(&mut rng).abs();

    let mut _inc_balance = 0.0;
    let mut inc_added = 0.0;

    let mut dec_balance = 0.0;

    let mut avg_inc_latency = MovingAvg::new();
    let mut avg_dec_latency = MovingAvg::new();

    while !done.load(Ordering::Relaxed) {
        while let Ok((id, locs)) = inc_log.try_wait_for_any_append() {
            //TODO
            //if loc != expected {
                // read others change and update out state
            //}
            if Some(id) == outstanding_incs.front().map(|i| i.1) {
                let ((v, t), _) = outstanding_incs.pop_front().unwrap();
                _inc_balance += v;
                inc_added += v;
                if inc_added < 5_000.0 {
                    num_incs.fetch_add(1, Ordering::Relaxed);
                    avg_inc_latency.add_point(t.elapsed().subsec_nanos() as usize);
                    inc_latency.store(avg_inc_latency.val(), Ordering::Relaxed);
                } else {
                    let id = inc_log.async_append(dec_chain, &(inc_added, t), &[locs[0]]);
                    outstanding_digests.push_back((id, t));
                    inc_added = 0.0;
                }
            } else if Some(&id) == outstanding_digests.front().map(|a| &a.0) {
                let (_, t) = outstanding_digests.pop_front().unwrap();
                num_incs.fetch_add(1, Ordering::Relaxed);
                avg_inc_latency.add_point(t.elapsed().subsec_nanos() as usize);
                inc_latency.store(avg_inc_latency.val(), Ordering::Relaxed);
            } else {

            }
        }
        let _ = dec_log.flush_completed_appends();

        let mut needs_snap = false;
        'recv: loop {
            match dec_log.try_get_next2() {
                Ok((&(v, t), l, id)) => {
                    assert_eq!(l.len(), 1);
                    let v: f64 = v;
                    let t: Instant = t;
                    if outstanding_decs.front() == Some(id) {
                        outstanding_decs.pop_front();
                        num_decs.fetch_add(1, Ordering::Relaxed);
                        avg_dec_latency.add_point(t.elapsed().subsec_nanos() as usize);
                        dec_latency.store(avg_dec_latency.val(), Ordering::Relaxed);
                    }

                    if v.is_sign_positive() {
                        dec_balance += v
                    } else if dec_balance >= v {
                        dec_balance += v
                    } else {
                        //TODO num_failures.fetch_add(1, Ordering::Relaxed);
                    }
                },
                Err(r) => {
                    if r == GetRes::Done && outstanding_decs.len() > 0 {
                        needs_snap = true;
                    }
                    break 'recv
                }
            }
        }

        if outstanding_incs.len() < outstanding_incs.capacity() {
            let num_new_ops = outstanding_incs.capacity() - outstanding_incs.len();
            for _ in 0..num_new_ops {
                let v = (inc_sample(), Instant::now());
                let id = inc_log.async_append(inc_chain, &v, &[]);
                // let id = log.async_dependent_multiappend(
                //     &[inc_chain], &[dec_chain], &inc_sample(), &[]
                // );
                outstanding_incs.push_back((v, id));
            }
            //inc_log.snapshot(inc_chain)
        }

        if outstanding_decs.len() < outstanding_decs.capacity() {
            let num_new_ops = outstanding_decs.capacity() - outstanding_decs.len();
            for _ in 0..num_new_ops {
                let v = (dec_sample(), Instant::now());
                let id = dec_log.async_append(dec_chain, &v, &[]);
                // let id = log.async_dependent_multiappend(
                //     &[dec_chain], &[inc_chain], &dec_sample(), &[]
                // );
                outstanding_decs.push_back(id);

            }
            needs_snap = true;
        }

        if needs_snap {
            dec_log.snapshot(dec_chain)
        }
    }
}

fn fuzzy_inc(
    num_incs: &AtomicUsize, inc_latency: &Vec<AtomicUsize>, done: &AtomicBool, args: &Args, worker_num: usize,
) {
    if args.inc_window == 0 {
        return
    }

    let inc_latency = &inc_latency[worker_num];

    let dec_chain = order::from(2);
    let inc_chain = order::from((3 + worker_num) as u32);
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
    // let mut outstanding_digests: VecDeque<((f64, Instant), _)> =
        // VecDeque::with_capacity(args.inc_window);
    let mut outstanding_digests = VecDeque::with_capacity(args.inc_window);

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10.0, 2.0);
    let mut inc_sample = || dist.sample(&mut rng).abs();
    for _ in 0..outstanding_incs.capacity() {
        let val = (inc_sample(), Instant::now());
        let id = log.async_append(inc_chain, &val, &[]);
        // let id = log.async_dependent_multiappend(
        //     &[inc_chain], &[dec_chain], &val, &[]
        // );
        outstanding_incs.push_back((val, id));
    }

    let mut _balance = 0.0;
    let mut added = 0.0;

    let mut avg_inc_latency = MovingAvg::new();

    while !done.load(Ordering::Relaxed) {
        while let Ok((id, locs)) = log.try_wait_for_any_append() {
            //TODO
            //if loc != expected {
                // read others change and update out state
            //}
            if Some(id) == outstanding_incs.front().map(|i| i.1) {
                let ((v, t), _) = outstanding_incs.pop_front().unwrap();
                _balance += v;
                added += v;
                if added < 5_000.0 {
                    num_incs.fetch_add(1, Ordering::Relaxed);
                    avg_inc_latency.add_point(t.elapsed().subsec_nanos() as usize);
                    inc_latency.store(avg_inc_latency.val(), Ordering::Relaxed);
                } else {
                    let id = log.async_append(dec_chain, &(added, t), &[locs[0]]);
                    // let id = log.async_dependent_multiappend(
                    //     &[dec_chain], &[inc_chain], &(added, t), &[locs[0]]);
                    outstanding_digests.push_back((id, t));
                    added = 0.0;
                }
            } else if Some(&id) == outstanding_digests.front().map(|a| &a.0) {
                let (_, t) = outstanding_digests.pop_front().unwrap();
                num_incs.fetch_add(1, Ordering::Relaxed);
                avg_inc_latency.add_point(t.elapsed().subsec_nanos() as usize);
                inc_latency.store(avg_inc_latency.val(), Ordering::Relaxed);
            } else {

            }
        }

        // while let Ok((id, locs)) = log.try_wait_for_any_append() {
        //     //TODO
        //     //if loc != expected {
        //         // read others change and update out state
        //     //}
        //     if Some(&id) == outstanding_digests.front().map(|a| &a.1) {
        //         let ((_, t), _) = outstanding_digests.pop_front().unwrap();
        //         num_incs.fetch_add(1, Ordering::Relaxed);
        //         avg_inc_latency.add_point(t.elapsed().subsec_nanos() as usize);
        //         inc_latency.store(avg_inc_latency.val(), Ordering::Relaxed);
        //     } else {

        //     }
        // }

        // let mut needs_snap = false;
        // 'recv: loop {
        //     match log.try_get_next2()
        //         .map(|(&v, l, &i)| { assert_eq!(l.len(), 1); (v, l[0], i) }) {
        //         Ok(((v, t), loc, id)) => {
        //             let v: f64 = v;
        //             let my_inc = outstanding_incs.front().map(|a| &a.1) == Some(&id);
        //             if my_inc { outstanding_incs.pop_front(); }

        //             balance += v;
        //             added += v;
        //             if added >= 5_000.0 {
        //                 if my_inc {
        //                     let id = log.async_append(dec_chain, &(added, t), &[loc]);
        //                     outstanding_digests.push_back(((v, t), id));
        //                 }
        //                 added = 0.0;
        //             } else if my_inc {
        //                 num_incs.fetch_add(1, Ordering::Relaxed);
        //                 avg_inc_latency.add_point(t.elapsed().subsec_nanos() as usize);
        //                 inc_latency.store(avg_inc_latency.val(), Ordering::Relaxed);
        //             }
        //         },
        //         Err(r) => {
        //             if r == GetRes::Done && outstanding_incs.len() > 0 {
        //                 needs_snap = true;
        //             }
        //             break 'recv
        //         }
        //     }
        // }

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
                let v = (inc_sample(), Instant::now());
                let id = log.async_append(inc_chain, &v, &[]);
                // let id = log.async_dependent_multiappend(
                //     &[inc_chain], &[dec_chain], &v, &[]
                // );
                outstanding_incs.push_back((v, id));
            }
        }
    }
}
fn fuzzy_dec(
    num_decs: &AtomicUsize, dec_latency: &AtomicUsize, done: &AtomicBool, args: &Args
) {
    println!("fuzzy dec");
    let dec_chain = order::from(2);
    let inc_chains: Vec<order> = (3..(3+args.workers as u32)).map(Into::into).collect();

    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(iter::once(order::from(dec_chain)))
            .reads_my_writes()
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(iter::once(order::from(dec_chain)))
            .reads_my_writes()
            .build()
    };

    let mut outstanding_decs = VecDeque::with_capacity(args.dec_window);

    let mut rng = rand::thread_rng();
    let mut dist = Normal::new(10_000.0, 200.0);
    let mut dec_sample = || -dist.sample(&mut rng).abs();

    let mut balance = 0.0;
    let mut avg_dec_latency = MovingAvg::new();

    while !done.load(Ordering::Relaxed) {
        let mut max_index = None;
        while let Ok((_, locs)) = log.try_wait_for_any_append() {
            if max_index.map(|OrderIndex(_, i)| i < locs[0].1).unwrap_or(true) {
                assert_eq!(locs[0].0, dec_chain);
                max_index = Some(locs[0]);
            }
        }
        if let Some(loc) = max_index {
            log.read_until(loc)
        }
        'recv: loop {
            match log.try_get_next2() {
                Ok((&(v, t), l, id)) => {
                    //assert_eq!(l.len(), 2 + args.workers);
                    assert_eq!(l[0].0, dec_chain);
                    let v: f64 = v;
                    let t: Instant = t;
                    if outstanding_decs.front() == Some(id) {
                        outstanding_decs.pop_front();
                        num_decs.fetch_add(1, Ordering::Relaxed);
                        avg_dec_latency.add_point(t.elapsed().subsec_nanos() as usize);
                        dec_latency.store(avg_dec_latency.val(), Ordering::Relaxed);
                    }

                    if v.is_sign_positive() {
                        balance += v
                    } else if balance >= v {
                        balance += v
                    } else {
                        //TODO num_failures.fetch_add(1, Ordering::Relaxed);
                    }
                },
                Err(..) => break 'recv,
            }
        }

        // while let Ok((&(v, t), l, id)) = log.try_get_next2() {
        //     assert_eq!(l.len(), 1);
        //     let v: f64 = v;
        //     let t: Instant = t;
        //     if outstanding_decs.front() == Some(id) {
        //         outstanding_decs.pop_front();
        //         num_decs.fetch_add(1, Ordering::Relaxed);
        //         avg_dec_latency.add_point(t.elapsed().subsec_nanos() as usize);
        //         dec_latency.store(avg_dec_latency.val(), Ordering::Relaxed);
        //     }

        //     if v.is_sign_positive() {
        //         balance += v
        //     } else if balance >= v {
        //         balance += v
        //     } else {
        //         //TODO num_failures.fetch_add(1, Ordering::Relaxed);
        //     }
        // }

        if outstanding_decs.len() < outstanding_decs.capacity() {
            let num_new_ops = outstanding_decs.capacity() - outstanding_decs.len();
            for _ in 0..num_new_ops {
                let v = (dec_sample(), Instant::now());
                //let id = log.async_append(dec_chain, &v, &[]);
                let id = log.async_dependent_multiappend(
                    &[dec_chain], &inc_chains, &v, &[]
                );
                outstanding_decs.push_back(id);

            }
        }
    }
}

fn collector(
    num_incs: &AtomicUsize,
    num_decs: &AtomicUsize,
    inc_latency: &Vec<AtomicUsize>,
    dec_latency: &AtomicUsize,
    done: &AtomicBool,
    args: &Args
) -> (Vec<usize>, Vec<usize>, Vec<usize>, Vec<usize>) {
    let (mut avg_incs, mut avg_decs) =
        (Vec::with_capacity(args.num_rounds), Vec::with_capacity(args.num_rounds));
    let (mut inc_latencies, mut dec_latencies) =
        (Vec::with_capacity(args.num_rounds), Vec::with_capacity(args.num_rounds));
    let (mut last_incs, mut last_decs) = (0, 0);
    for _ in 0..args.num_rounds {
        sleep(Duration::from_millis(args.ms_per_round as u64));
        let incs = num_incs.load(Ordering::Relaxed);
        let decs = num_decs.load(Ordering::Relaxed);
        let inc_lat = inc_latency.iter()
            .fold(0, |lat, l| lat + l.load(Ordering::Relaxed)) / inc_latency.len();
        let dec_lat = dec_latency.load(Ordering::Relaxed);
        let new_incs = incs - last_incs;
        let new_decs = decs - last_decs;
        last_incs = incs;
        last_decs = decs;
        avg_incs.push(new_incs);
        avg_decs.push(new_decs);
        inc_latencies.push(inc_lat);
        dec_latencies.push(dec_lat);
    }
    done.store(true, Ordering::Relaxed);
    (avg_incs, avg_decs, inc_latencies, dec_latencies)
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
