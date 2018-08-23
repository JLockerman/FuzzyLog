
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log_client;
extern crate rand;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;

use std::iter;
use std::net::SocketAddr;
use std::thread::{sleep, yield_now};
use std::time::{Duration, Instant};

use fuzzy_log_client::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log_client::packets::order;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "micropayments_bench", about = "micropayments example benchmark.")]
struct Args {
    #[structopt(help = "head of the FuzzyLog server chain to run against.")]
    head_server: SocketAddr,

    #[structopt(help = "tail of the FuzzyLog server chain to run against.")]
    tail_server: Option<SocketAddr>,

    #[structopt(short="t", long="time_per_round", help = "number of milliseconds per round.", default_value = "1000")]
    ms_per_round: usize,

    #[structopt(short="r", long="rounds", help = "number of rounds.", default_value = "5")]
    num_rounds: usize,

    #[structopt(short="i", long="inc_window", help = "Number of outstanding increments.", default_value = "100")]
    inc_window: usize,

    #[structopt(short="c", long="clients", help = "total clients.")]
    clients: usize,

    #[structopt(short="l", long="level", help = "which level to run.")]
    level: usize,

    #[structopt(short="e", long="levels", help = "levels.")]
    levels: Vec<usize>,
}

fn main() {
    let _ = env_logger::init();
    let mut args @ Args{..} = StructOpt::from_args();
    args.levels.sort();
    println!("#{:?}", args);

    let state = State::default();

    crossbeam::scope(|scope| {
        let args = &args;
        let state = &state;

        let r = scope.spawn(move || {
            while state.setup.load(Relaxed) { yield_now() }
            sleep(Duration::from_millis(args.ms_per_round as u64));
            state.started.store(true, Relaxed);
            for _ in 0..args.num_rounds {
                sleep(Duration::from_millis(args.ms_per_round as u64));
            }
            state.done.store(true, Relaxed);
            sleep(Duration::from_millis(args.ms_per_round as u64));
            state.drained.store(true, Relaxed);
        });

        if args.level < args.levels.len() {
            let avg_latency = get_latency(state, args, args.level);
            println!(
                "> [n; n + {}): {}",
                if args.level == 0 { 1 }
                else {
                    args.levels[args.level] as u64
                    * (args.clients - args.levels.len()) as u64
                },
                avg_latency
            );
        } else {
            let total_incs = increment(state, args, args.level);
            let hz = total_incs as f64 / args.num_rounds as f64;
            println!("> {:.0} Hz", hz);
        }
        r.join();
    })
}

#[derive(Default)]
struct State {
    setup: AtomicBool,
    started: AtomicBool,
    done: AtomicBool,
    drained: AtomicBool,
}

/////////////////////////

#[inline(never)]
fn increment(
    state: &State,
    args: &Args,
    _worker_num: usize,
) -> u64 {
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(&[WAIT_CHAIN.into()])
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(&[WAIT_CHAIN.into()])
            .build()
    };

    wait_for_clients_start(&mut log, args.clients);

    let chains: Vec<_> = (1..(args.levels.len()+1))
        .map(|l| order::from(l as u32))
        .collect();

    let accumulated = &mut (*args.levels.last().unwrap() as u64 - 1);
    let num_increments = &mut 0;
    let total_increments = &mut 0u64;

    {
        let mut send = |real| {
            while let Ok(..) = log.try_wait_for_any_append() {
                *num_increments -= 1;
                if real {
                    *total_increments += 1;
                }
            }

            for _ in *num_increments..args.inc_window {
                *accumulated += 1; //TODO switch to N
                if *accumulated % args.levels[1] as u64 != 0 {
                    log.async_append(chains[0], &1u64, &[]);
                } else {
                    for (i, &l) in args.levels.iter().rev().enumerate() {
                        if *accumulated % l as u64 == 0 {
                            let n = args.levels.len() - i;
                            log.async_no_remote_multiappend(&chains[..n], &1u64, &[]);
                            break
                        }
                    }
                }
                *num_increments += 1;
            }
        };

        send(false);
        state.setup.store(true, Relaxed);

        while !state.started.load(Relaxed) {
            send(false);
        }

        while !state.done.load(Relaxed) {
            send(true);
        }

        while !state.drained.load(Relaxed) {
            send(false);
        }

        sleep(Duration::from_millis(args.ms_per_round as u64));
    }

    *total_increments
}

/////////////////////////

#[inline(never)]
fn get_latency(
    state: &State,
    args: &Args,
    level: usize,
) -> u64 {
    let chain = order::from((level + 1) as u32);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(&[chain, WAIT_CHAIN.into()])
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(&[chain, WAIT_CHAIN.into()])
            .build()
    };

    wait_for_clients_start(&mut log, args.clients);

    let mut avg_latency = MovingAvg::new();

    let mut val = 0u64;
    {
        let threshold = args.levels[level]  as u64;
        let val = &mut val;
        let mut recv = || {
            log.snapshot(chain);
            let mut update_val = |i| *val +=
                if level == 0 { i }
                else if i > threshold { i / threshold }
                else { threshold };
            match log.get_next() {
                Ok((&i, _)) => update_val(i),
                Err(GetRes::Done) => return false,
                Err(r) => panic!("{:?}", r),
            }
            'recv: loop {
                match log.get_next() {
                    Ok((&i, _)) => update_val(i),
                    Err(GetRes::Done) => return true,
                    Err(r) => panic!("{:?}", r),
                }
            }
        };

        while !recv() {}
        state.setup.store(true, Relaxed);

        while !state.started.load(Relaxed) {
            recv();
        }

        while !state.done.load(Relaxed) {
            let start_time = Instant::now();
            recv();
            let latency = start_time.elapsed().subsec_nanos();
            avg_latency.add_point(latency as u64);
        }

        while !state.drained.load(Relaxed) {
            recv();
        }
    }

    if val == 0 { panic!("zero val for {}", level); }
    println!("val = {}", val);
    avg_latency.val()
}

/////////////////////////

const WAIT_CHAIN: u32 = 10_000;

fn wait_for_clients_start<V: Default>(
    log: &mut LogHandle<V>, num_clients: usize
) {
    log.append(WAIT_CHAIN.into(), &Default::default(), &[]);
    let mut clients_started = 0;
    while clients_started < num_clients {
        log.snapshot(WAIT_CHAIN.into());
        while let Ok(..) = log.get_next() { clients_started += 1 }
    }
}

/////////////////////////

#[derive(Debug, Clone)]
struct MovingAvg {
    avg: u64,
    n: u64,
}

impl MovingAvg {
    pub fn new() -> Self {
        MovingAvg{avg: 0, n: 0}
    }

    pub fn add_point(&mut self, p: u64) {
        self.avg = (p + self.n * self.avg) / (self.n + 1);
        self.n += 1;
    }

    pub fn val(&self) -> u64 {
        self.avg
    }
}
