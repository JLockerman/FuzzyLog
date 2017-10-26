
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
use std::time::Duration;

use fuzzy_log_client::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log_client::packets::{order, entry, OrderIndex};

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

    #[structopt(short="i", long="inc_window", help = "Number of outstanding increments.", default_value = "500")]
    inc_window: usize,

    #[structopt(short="c", long="clients", help = "total clients.")]
    clients: usize,

    #[structopt(short="l", long="level", help = "which level to run.")]
    level: usize,
}

fn main() {
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();
    println!("#{:?}", args);

    let state = State::default();

    crossbeam::scope(|scope| {
        let args = &args;
        let state = &state;

        let r = scope.spawn(move || {
            while !state.setup.load(Relaxed) { yield_now() }
            sleep(Duration::from_millis(args.ms_per_round as u64));
            state.done.store(true, Relaxed);
        });

        if args.level == 0 {
            record(state, args);
        } else {
            let total_incs = increment(state, args, args.level);
            let hz = total_incs;
            println!("> {} Hz", hz);
        }
        r.join();
    })
}

#[derive(Default)]
struct State {
    setup: AtomicBool,
    done: AtomicBool,
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

    let accumulated = &mut 0i64;
    let num_increments = &mut 0;
    let total_increments = &mut 0u64;
    let counts = &mut 0u64;
    let mut multi_nums = Vec::with_capacity(100_000);

    {
        let mut send = |real, i| {
            while let Ok(..) = log.try_wait_for_any_append() {
                *num_increments -= 1;
                if real {
                    *total_increments += 1;
                }
            }

            for _ in *num_increments..args.inc_window {
                *accumulated += i; //TODO switch to N
                if *accumulated % 10 as i64 != 0 {
                    log.async_append(1.into(), &i, &[]);
                } else {
                    multi_nums.push(*counts);
                    log.async_no_remote_multiappend(&[1.into(), 2.into()], &i, &[]);
                }
                *num_increments += 1;
                *counts += 1;
            }
        };

        send(false, 10);
        state.setup.store(true, Relaxed);

        while !state.done.load(Relaxed) {
            send(true, 1);
        }

        sleep(Duration::from_millis(args.ms_per_round as u64));
    }

    println!("> multis @ {:?}", multi_nums);

    *total_increments
}

/////////////////////////

#[inline(never)]
fn record(
    state: &State,
    args: &Args,
) {
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::<i64>::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(&[2.into(), WAIT_CHAIN.into()])
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(&[2.into(), WAIT_CHAIN.into()])
            .build()
    };

    wait_for_clients_start(&mut log, args.clients);

    let mut val = 0i64;
    {
        let threshold = 10;
        let val = &mut val;
        let mut recv = || {
            log.strong_snapshot(&[1.into(), 2.into()]);
            let mut update_val = |_| *val += threshold;
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

        while !recv() { }
        state.setup.store(true, Relaxed);
        println!("start actual");

        while !state.done.load(Relaxed) {
            recv();
        }
    }

    if val == 0 { panic!("zero val for"); }
    println!("done setup, val = {}", val);

    /////////////////////////
    let (full, fuzzy) = log.full_and_fuzzy_snapshots();
    {

        fn record_vals(mut log: LogHandle<i64>, chain: order, snaps: Vec<entry>, threshold: i64) -> Vec<i64> {
            let mut counts = Vec::with_capacity(snaps.len());
            // log.rewind(OrderIndex(chain, 1.into()));
            let mut count = 0;
            for s in snaps {
                log.read_until(OrderIndex(chain, s));
                'recv2: loop {
                    match log.get_next() {
                        Ok((&i, _)) => count += if threshold == 1 { i } else { threshold },
                        Err(GetRes::Done) => break 'recv2,
                        Err(r) => panic!("{:?}", r),
                    }
                }
                counts.push(count);
            }
            return counts
        };

        let full_log = if let Some(tail_server) = args.tail_server {
            LogHandle::<i64>::replicated_with_servers(
                    iter::once((args.head_server, tail_server)))
                .chains(&[1.into()])
                .build()
            } else {
                LogHandle::unreplicated_with_servers(iter::once(args.head_server))
                    .chains(&[1.into()])
                    .build()
            };

        let fuzz_log = if let Some(tail_server) = args.tail_server {
            LogHandle::<i64>::replicated_with_servers(
                    iter::once((args.head_server, tail_server)))
                .chains(&[2.into()])
                .build()
            } else {
                LogHandle::unreplicated_with_servers(iter::once(args.head_server))
                    .chains(&[2.into()])
                    .build()
            };

        print!("> snaps [ ");
        for (&u, &z) in full.iter().zip(fuzzy.iter()) {
            print!("{:?}, ", (u32::from(u), u32::from(z)));
        }
        println!("]");

        let full = record_vals(full_log, 1.into(), full, 1);
        let fuzzy = record_vals(fuzz_log, 2.into(), fuzzy, 10);

        println!("> full {:?}\n", full);
        println!("> fuzzy {:?}\n", fuzzy);

        let deltas: Vec<_> = full.into_iter()
            .zip(fuzzy.into_iter())
            .map(|(u, z)| u - z)
            .collect();

        println!("> delta {:?}\n", deltas);
    }
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

