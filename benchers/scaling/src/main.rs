
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log_client;
extern crate fuzzy_log_util;
extern crate rand;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::thread::{sleep, yield_now};
use std::time::{Duration};

use fuzzy_log_client::fuzzy_log::log_handle::LogHandle;
use fuzzy_log_client::packets::{order, entry};

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "scaling", about = "")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: ServerAddrs,

    #[structopt(help = "FuzzyLog server to use as a sequencer.")]
    sequencer: SocketAddr,

    #[structopt(help = "client num.")]
    client_num: usize,

    #[structopt(short="r", long="rounds", help = "number of rounds.", default_value = "10")]
    num_rounds: usize,

    #[structopt(short="t", long="time_per_round", help = "number of milliseconds per round.", default_value = "1000")]
    ms_per_round: usize,

    #[structopt(short="w", long="write_window", help = "Number of outstanding mutations.", default_value = "500")]
    write_window: usize,

    #[structopt(short="a", long="total_handles", help = "Number of handles that need to have started before we collect data.")]
    total_handles: usize,
}

#[derive(Debug)]
struct ServerAddrs(Vec<(SocketAddr, SocketAddr)>);

impl FromStr for ServerAddrs {
    type Err = std::string::ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        println!("{}", s);
        Ok(ServerAddrs(
            s.split('^').map(|t|{
                let mut addrs = t.split('#').map(|s| {
                    match SocketAddr::from_str(s) {
                        Ok(addr) => addr,
                        Err(e) => panic!("head parse err {} @ {}", e, s),
                    }
                });
                let head = addrs.next().expect("no head");
                let tail = addrs.next().expect("no tail");
                (head, tail)
            }).collect()
        ))
    }
}

fn main() {
    use std::iter;
    // let start_time = Instant::now();
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();
    // println!("#{:?}", args);
    //println!("#{:?} servers", args.servers.0.len());


    let mut data_handle = LogHandle::replicated_with_servers(&args.servers.0)
        .chains((1u32..22).chain(12_000..12_010).chain(iter::once(10_000)).map(order::from))
        .build();

    let reg_writes = AtomicUsize::new(0);
    let started = AtomicBool::new(false);
    let done = AtomicBool::new(false);
    let regular_throughput =
        crossbeam::scope(|scope| {
            let args = &args;
            let done = &done;
            let started = &started;

            let reg_writes = &reg_writes;

            let record = scope.spawn(move || collector(reg_writes, started, done, args));

            wait_for_clients_start(&mut data_handle, 10_000.into(), started, args);

            regular_append(
                &mut data_handle, &[(args.client_num as u32 + 1).into()], reg_writes, done, args
            );
            record.join()
        });


    let mut sequencer_handle = LogHandle::unreplicated_with_servers(&[args.sequencer])
        .chains(&[order::from(1)])
        .build();

    let corfu_writes = AtomicUsize::new(0);
    let started = AtomicBool::new(false);
    let done = AtomicBool::new(false);
    let corfu_throughput = crossbeam::scope(|scope| {
            let data_chains: Vec<_> = (0..args.servers.0.len())
                .map(|i| i as u32 + 12_000).map(order::from).collect();

            let args = &args;
            let done = &done;
            let started = &started;

            let corfu_writes = &corfu_writes;

            let record = scope.spawn(move || collector(corfu_writes, started, done, args));

            wait_for_clients_start(&mut data_handle, 10_000.into(), started, args);

            corfu_append(
                &mut sequencer_handle,
                &mut data_handle,
                &data_chains[..],
                corfu_writes,
                done,
                args
            );
            record.join()
        });

    //println!("#elapsed {:?}", start_time.elapsed());
    println!("reg appends/s @ {} = {}", args.servers.0.len(), regular_throughput);
    println!("cor appends/s @ {} = {}", args.servers.0.len(), corfu_throughput);
}

/////////////////////////

#[inline(never)]
fn collector(
    writes: &AtomicUsize, started: &AtomicBool, done: &AtomicBool, args: &Args,
) -> usize {
    while !started.load(Ordering::Relaxed) { yield_now() }
    let mut throughput = Vec::with_capacity(args.num_rounds);
    for _ in 0..args.num_rounds {
        sleep(Duration::from_millis(args.ms_per_round as u64));
        let write = writes.swap(0, Ordering::Relaxed);
        throughput.push(write);
    }
    sleep(Duration::from_millis(args.ms_per_round as u64));
    done.store(true, Ordering::Relaxed);
    sleep(Duration::from_millis(2 * args.ms_per_round as u64));
    done.store(true, Ordering::Relaxed);
    let total_writes = throughput.iter().sum::<usize>();
    let avg_writes = total_writes / args.num_rounds;
    avg_writes
}

/////////////////////////
/////////////////////////


#[inline(never)]
fn corfu_append(
    sequencer_handle: &mut LogHandle<()>,
    data_handle: &mut LogHandle<[u8]>,
    storage_chains: &[order],
    writes: &AtomicUsize,
    done: &AtomicBool,
    args: &Args,
) {
    let sequencer = order::from(1);

    let bytes = vec![0xf; 40];

    let mut outstanding_r1 = 0;
    let mut outstanding_r2 = 0;

    while !done.load(Ordering::Relaxed) {
        let outstanding_writes = outstanding_r1 + outstanding_r2;
        for _ in outstanding_writes..(args.write_window*2) {
            sequencer_handle.async_append(sequencer, &(), &[]);
            outstanding_r1 += 1;
        }

        while let Ok((_, locs)) = sequencer_handle.try_wait_for_any_append() {
            outstanding_r1 -= 1;
            let index: entry = locs[0].1;
            let storage_chain = u32::from(index) as usize % storage_chains.len();
            let _ = data_handle.async_append(storage_chains[storage_chain], &bytes[..], &[]);
            outstanding_r2 += 1;
        }
        while let Ok((..)) = data_handle.try_wait_for_any_append() {
            outstanding_r2 -= 1;
            writes.fetch_add(1, Ordering::Relaxed);
        }
    }

    data_handle.wait_for_all_appends().unwrap();
    sequencer_handle.wait_for_all_appends().unwrap();
}

/////////////////////////


#[inline(never)]
fn regular_append(
    data_handle: &mut LogHandle<[u8]>,
    chains: &[order],
    writes: &AtomicUsize,
    done: &AtomicBool,
    args: &Args,
) {
    let bytes = vec![0xf; 40];

    let mut outstanding = 0;

    while !done.load(Ordering::Relaxed) {
        for _ in outstanding..args.write_window {;
            if chains.len() == 1 {
                data_handle.async_append(chains[0], &bytes[..], &[]);
            } else {
                data_handle.async_multiappend(chains, &bytes[..], &[]);
            }

            outstanding += 1;
        }

        while let Ok((..)) = data_handle.try_wait_for_any_append() {
            outstanding -= 1;
            writes.fetch_add(1, Ordering::Relaxed);
        }
    }

    data_handle.wait_for_all_appends().unwrap();
}

/////////////////////////

fn wait_for_clients_start(
    log: &mut LogHandle<[u8]>, wait_chain: order, start: &AtomicBool, args: &Args
) {
    log.append(wait_chain, &[], &[]);
    let mut clients_started = 0;
    while clients_started < args.total_handles {
        log.snapshot(wait_chain);
        while let Ok(..) = log.get_next() {
            clients_started += 1
        }
    }
    start.store(true, Ordering::Relaxed);
}
