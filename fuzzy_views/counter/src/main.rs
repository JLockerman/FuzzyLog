
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log;
extern crate rand;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::iter;
use std::net::SocketAddr;
use std::thread::sleep;
use std::time::{Duration, Instant};

use fuzzy_log::async::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log::packets::order;

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

    #[structopt(short="w", long="writers", help = "incrementing threads.", default_value = "1")]
    writers: usize,
}

const NUM_LEVELS: usize = 6;
const LEVEL: [u64; NUM_LEVELS] = [1, 5, 10, 100, 1_000, 10_000];

fn main() {
    let start_time = Instant::now();
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();
    println!("#{:?}", args);

    let done = AtomicBool::new(false);
    let increments = (0..args.writers).map(|_| AtomicUsize::new(0)).collect();
    let avg_latencies = (0..NUM_LEVELS).map(|_| AtomicUsize::new(0)).collect();

    let (_avg_inc_throughput, get_latencies) =
        crossbeam::scope(|scope| {
            let args = &args;
            let done = &done;
            let increments = &increments;
            let avg_latencies = &avg_latencies;

            let mut joins = Vec::with_capacity(args.writers + NUM_LEVELS);
            for i in 0..args.writers {
                let j = scope.spawn(move || increment(increments, done, args, i));
                joins.push(j);
            }
            for i in 0..NUM_LEVELS {
                let j = scope.spawn(move || get_latency(avg_latencies, done, args, i));
                joins.push(j);
            }
            let record = scope.spawn(move || collector(increments, avg_latencies, done, args));
            for join in joins {
                join.join();
            }
            record.join()
        });
    println!("#elapsed {:?}", start_time.elapsed());
    print!("µs per get V error, [n; n], ");
    for i in 1..LEVEL.len() {
        print!("[n; n + {}), ", LEVEL[i] * args.writers as u64);
    }
    println!("");
    print!(", ");
    for latency in get_latencies {
        print!("{}, ", latency);
    }
    println!("");
}

/////////////////////////

#[inline(never)]
fn collector(
    increments: &Vec<AtomicUsize>, avg_latencies: &Vec<AtomicUsize>, done: &AtomicBool,
    args: &Args,
) -> (Vec<usize>, Vec<usize>) {
    let mut incs = Vec::with_capacity(args.num_rounds);
    let mut latencies: Vec<_> =
        (0..NUM_LEVELS).map(|_| Vec::with_capacity(args.num_rounds)).collect();
    for _ in 0..args.num_rounds {
        sleep(Duration::from_millis(args.ms_per_round as u64));
        let total_incs = increments.iter().fold(0, |inc, i| inc + i.load(Ordering::Relaxed));
        incs.push(total_incs);
        for j in 0..NUM_LEVELS {
            latencies[j].push(avg_latencies[j].load(Ordering::Relaxed));
        }
    }
    done.store(true, Ordering::Relaxed);
    println!("#increments {:?}", incs);
    println!("#latencies  {:?}", latencies);
    let avg_incs =
        incs.windows(2)
        .map(|w|((w[1] - w[0]) / (args.ms_per_round / 1000)) * args.writers )
        .collect();
    println!("#avg incs   {:?}", avg_incs);
    let latencies = latencies.into_iter().map(|l| l.last().unwrap() / 1000).collect();
    (avg_incs, latencies)
}

/////////////////////////

#[inline(never)]
fn increment(
    increments: &Vec<AtomicUsize>, done: &AtomicBool, args: &Args, worker_num: usize,
) -> u64 {
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(iter::empty::<order>())
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(iter::empty::<order>())
            .build()
    };

    let chains: Vec<_> = (1..(NUM_LEVELS+1)).map(|l| order::from(l as u32)).collect();
    let mut accumulated = 0;
    let mut num_increments = 0;
    let mut total_increments = 0;

    while !done.load(Ordering::Relaxed) {
        while let Ok(..) = log.try_wait_for_any_append() {
            num_increments -= 1;
            total_increments += 1;
        }
        increments[worker_num].store(total_increments, Ordering::Relaxed);

        for _ in num_increments..args.inc_window {
            accumulated += 1; //TODO switch to N
            if accumulated % 10 != 0 {
                log.async_append(chains[0], &1u64, &[]);
            } else if accumulated % 10_000 == 0 {
                log.async_no_remote_multiappend(&chains[..5], &1u64, &[]);
            } else if accumulated % 1_000 == 0 {
                log.async_no_remote_multiappend(&chains[..4], &1u64, &[]);
            } else if accumulated % 100 == 0 {
                log.async_no_remote_multiappend(&chains[..3], &1u64, &[]);
            } else if accumulated % 10 == 0 {
                log.async_no_remote_multiappend(&chains[..2], &1u64, &[]);
            }
            num_increments += 1;
        }
    }
    num_increments as u64
}

/////////////////////////

#[inline(never)]
fn get_latency(
    avg_latencies: &Vec<AtomicUsize>, done: &AtomicBool, args: &Args, level: usize,
) -> u64 {
    let chain = order::from((level + 1) as u32);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(iter::once(chain))
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(iter::once(chain))
            .build()
    };

    let mut avg_latency = MovingAvg::new();
    let mut val = 0u64;

    assert!(level < NUM_LEVELS);

    while !done.load(Ordering::Relaxed) {
        let start_time = Instant::now();
        log.snapshot(chain);
        'recv: loop {
            match log.get_next() {
                Ok((&i, _)) => val +=
                    if level == 0 { i }
                    else if i > LEVEL[level] { i % LEVEL[level] }
                    else { LEVEL[level] },
                //TODO +=
                Err(GetRes::Done) => break 'recv,
                Err(r) => panic!("{:?}", r),
            }
        }
        let latency = start_time.elapsed().subsec_nanos();
        avg_latency.add_point(latency as usize);
        avg_latencies[level].store(avg_latency.val(), Ordering::Relaxed);
    }
    val
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
