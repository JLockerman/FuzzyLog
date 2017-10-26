
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log;
extern crate rand;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering, ATOMIC_BOOL_INIT};
use std::iter;
use std::net::SocketAddr;
use std::thread::{sleep, yield_now};
use std::time::{Duration, Instant};

use fuzzy_log::async::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log::packets::order;

use structopt::StructOpt;

use rand::Rng;

#[derive(StructOpt, Debug)]
#[structopt(name = "pre_v_post", about = "pre vs post-hoc filtering benchmark.")]
struct Args {
    #[structopt(help = "head of the FuzzyLog server chain to run against.")]
    head_server: SocketAddr,

    #[structopt(help = "tail of the FuzzyLog server chain to run against.")]
    tail_server: Option<SocketAddr>,

    #[structopt(short="c", long="client_num", help = "Client number.")]
    client_num: usize,

    #[structopt(short="t", long="time_per_round", help = "number of milliseconds per round.", default_value = "10000")]
    ms_per_round: usize,

    #[structopt(short="r", long="rounds", help = "number of rounds.", default_value = "5")]
    num_rounds: usize,

    #[structopt(short="i", long="inc_window", help = "Number of outstanding mutations.", default_value = "500")]
    inc_window: usize,

    #[structopt(short="w", long="writers", help = "mutating threads.", default_value = "1")]
    writers: usize,

    #[structopt(short="d", long="work_duration", help = "Nanoseconds the client should spin per event.", default_value = "1.0")]
    duration: usize,
}

const NUM_LEVELS: usize = 3;
const WAIT_CHAIN: u32 = NUM_LEVELS as u32 + 10;

static STARTED: AtomicBool = ATOMIC_BOOL_INIT;

fn main() {
    let start_time = Instant::now();
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();
    println!("#{:?}", args);
    assert!(args.client_num <= NUM_LEVELS);

    let done = AtomicBool::new(false);
    let mutations = (0..args.writers).map(|_| AtomicUsize::new(0)).collect();
    let avg_latencies = (0..NUM_LEVELS).map(|_| AtomicUsize::new(0)).collect();

    let (_avg_inc_throughput, get_latencies) =
        crossbeam::scope(|scope| {
            let args = &args;
            let done = &done;
            let mutations = &mutations;
            let avg_latencies = &avg_latencies;

            let mut joins = Vec::with_capacity(args.writers + NUM_LEVELS);
            if args.client_num == NUM_LEVELS {
                for i in 0..args.writers {
                    let j = scope.spawn(move || mutate(mutations, done, args, i));
                    joins.push(j);
                }
            } else {
                let j = scope.spawn(move || get_latency(avg_latencies, done, args, args.client_num));
                joins.push(j);
            }
            let record = scope.spawn(move || collector(mutations, avg_latencies, done, args));
            // for join in joins {
            //     join.join();
            // }
            record.join()
        });
    println!("#elapsed {:?}", start_time.elapsed());
    println!("µs per get V error base, filtered ");
    for latency in get_latencies {
        print!("{}, ", latency);
    }
    println!("");
}

/////////////////////////

#[inline(never)]
fn collector(
    mutations: &Vec<AtomicUsize>, avg_latencies: &Vec<AtomicUsize>, done: &AtomicBool,
    args: &Args,
) -> (Vec<usize>, Vec<usize>) {
    while STARTED.load(Ordering::Relaxed) { yield_now() }
    let mut incs = Vec::with_capacity(args.num_rounds);
    let mut latencies: Vec<_> =
        (0..NUM_LEVELS).map(|_| Vec::with_capacity(args.num_rounds)).collect();
    for _ in 0..args.num_rounds {
        sleep(Duration::from_millis(args.ms_per_round as u64));
        let total_incs = mutations.iter().fold(0, |inc, i| inc + i.load(Ordering::Relaxed));
        incs.push(total_incs);
        for j in 0..NUM_LEVELS {
            latencies[j].push(avg_latencies[j].load(Ordering::Relaxed));
        }
    }
    sleep(Duration::from_secs(1));
    done.store(true, Ordering::Relaxed);
    println!("#mutations {:?}", incs);
    println!("#latencies  {:?}", latencies);
    let avg_incs =
        incs.windows(2)
        .map(|w|((w[1] - w[0]) / (args.ms_per_round / 1000)) * args.writers )
        .collect();
    println!("#avg muts   {:?}", avg_incs);
    let latencies = latencies.into_iter().map(|l| l.last().unwrap() / 1000).collect();
    (avg_incs, latencies)
}

/////////////////////////

#[inline(never)]
fn mutate(
    mutations: &Vec<AtomicUsize>, done: &AtomicBool, args: &Args, worker_num: usize,
) -> u64 {
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(iter::once(order::from(WAIT_CHAIN)))
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(iter::once(order::from(WAIT_CHAIN)))
            .build()
    };

    wait_for_clients_start(&mut log, args);

    let chains: Vec<_> = (1..(NUM_LEVELS+1)).map(|l| order::from(l as u32)).collect();
    let mut num_mutations = 0;
    let mut total_mutations = 0;
    let mut inserts = 0;

    let mut gen = rand::thread_rng();

    while !done.load(Ordering::Relaxed) {
        while let Ok(..) = log.try_wait_for_any_append() {
            num_mutations -= 1;
            total_mutations += 1;
        }
        mutations[worker_num].store(total_mutations, Ordering::Relaxed);

        for (_, all_use) in (num_mutations..args.inc_window).zip(gen.gen_iter()) {
            if all_use {
                inserts += 1;
                log.async_no_remote_multiappend(&chains[..], &(), &[]);
            } else {
                log.async_append(chains[0], &(), &[]);
            }
            num_mutations += 1;
        }
    }
    println!("inserts/total {}/{}: {}%", inserts, total_mutations,
        100.0 * ((inserts as f64) / (total_mutations as f64)));
    total_mutations as u64
}

/////////////////////////

#[inline(never)]
fn get_latency(
    avg_latencies: &Vec<AtomicUsize>, done: &AtomicBool, args: &Args, level: usize,
) -> u64 {
    let (chain, post_filter) = if level == NUM_LEVELS - 1 {
            (0, true)
        } else {
            (level, false)
        };
    let chain = order::from((chain + 1) as u32);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(iter::once(chain).chain(iter::once(order::from(WAIT_CHAIN))))
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(iter::once(chain).chain(iter::once(order::from(WAIT_CHAIN))))
            .build()
    };

    wait_for_clients_start(&mut log, args);

    let mut avg_latency = MovingAvg::new();
    let mut completed_ops = 0;
    let mut handled_events = 0;
    let work_duration = Duration::new(0, args.duration as u32);
    let mut iters_working = 0u64;

    assert!(level < NUM_LEVELS);

    let mut started: bool = false;
    while !done.load(Ordering::Relaxed) {
        let start_time = Instant::now();
        log.snapshot(chain);
        'recv: loop {
            let next = log.get_next();
            if !started && next.is_ok(){
                started = true;
                STARTED.store(true, Ordering::Relaxed);
            };
            match next {
                Ok((&(), locs)) if post_filter => if locs.len() == 1 {
                    let work_time = Instant::now();
                    while work_time.elapsed() < work_duration {
                        iters_working = iters_working.wrapping_add(1);
                    }
                    handled_events += 1;
                },
                Ok(..) => {
                    let work_time = Instant::now();
                    while work_time.elapsed() < work_duration {
                        iters_working = iters_working.wrapping_add(1);
                    }
                    handled_events += 1;
                },
                Err(GetRes::Done) => break 'recv,
                Err(r) => panic!("{:?}", r),
            }
        }
        let latency = start_time.elapsed().subsec_nanos();
        if started {
            completed_ops += 1;
            avg_latency.add_point(latency as usize);
            avg_latencies[level].store(avg_latency.val(), Ordering::Relaxed);
        }
    }
    println!("{:?}: {:?}, {:?}, {:?}", chain, post_filter, completed_ops, handled_events);
    iters_working
}

/////////////////////////

fn wait_for_clients_start<V: Default>(log: &mut LogHandle<V>, args: &Args) {
    log.append(WAIT_CHAIN.into(), &Default::default(), &[]);
    let mut clients_started = 0;
    if args.client_num < NUM_LEVELS {
        return
    }
    while clients_started < NUM_LEVELS + args.writers {
        log.snapshot(WAIT_CHAIN.into());
        while let Ok(..) = log.get_next() {
            clients_started += 1
        }
    }
    STARTED.store(true, Ordering::Relaxed);
}


/////////////////////////

#[derive(Debug, Clone)]
struct MovingAvg {
    avg: usize,
    n: usize,
}

impl MovingAvg {
    pub fn new() -> Self {
        MovingAvg{avg: 0, n: 0}
    }

    pub fn add_point(&mut self, p: usize) {
        self.avg = (p + self.n * self.avg) / (self.n + 1);
        self.n += 1;
    }

    pub fn val(&self) -> usize {
        self.avg
    }
}
