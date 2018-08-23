//XXX remember, the servers must be in a group
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log_client;
extern crate fuzzy_log_util;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::{Duration, Instant};

use fuzzy_log_client::fuzzy_log::log_handle::LogHandle;
use fuzzy_log_client::packets::{entry, order};

use fuzzy_log_util::hash::UuidHashMap;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "append_lat", about = "latency vs num events benchmark.")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: ServerAddrs,

    #[structopt(help = "FuzzyLog server to use as a sequencer.")]
    sequencer: SocketAddr,

    #[structopt(short="m", long="ms", help = "ms per round.", default_value = "2000")]
    round_ms: usize,
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
    let start_time = Instant::now();
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();
    println!("#{:?}", args);
    let window_sizes = [10, 20, 40, 80, 160, 320, 640, 1280];
    let mut avg_corfu_latencies = Vec::with_capacity(window_sizes.len());
    let mut avg_corfu_throughput = Vec::with_capacity(window_sizes.len());

    let mut avg_corfu_multi_latencies = Vec::with_capacity(window_sizes.len());
    let mut avg_corfu_multi_throughput = Vec::with_capacity(window_sizes.len());

    let mut avg_regular_latencies = Vec::with_capacity(window_sizes.len());
    let mut avg_regular_throughput = Vec::with_capacity(window_sizes.len());

    let mut avg_regular_dist_latencies = Vec::with_capacity(window_sizes.len());
    let mut avg_regular_dist_throughput = Vec::with_capacity(window_sizes.len());

    let mut avg_regular_local_latencies = Vec::with_capacity(window_sizes.len());
    let mut avg_regular_local_throughput = Vec::with_capacity(window_sizes.len());

    let mut sequencer_handle = LogHandle::unreplicated_with_servers(&[args.sequencer])
        .chains(&[order::from(1), 2.into(), 3.into()])
        .build();
    let mut data_handle = LogHandle::replicated_with_servers(&args.servers.0)
        .chains(&[order::from(2), order::from(3)])
        .build();

    let storage_chains = [order::from(2), order::from(3)];

    for &window_size in window_sizes.into_iter() {
        {
            let (l, t) = corfu_append(
                &mut sequencer_handle, &mut data_handle, &storage_chains, &args, window_size
            );
            println!("cc{:?}events: {:>5}ns\t {:?}Hz", window_size, l, t);
            avg_corfu_latencies.push(l);
            avg_corfu_throughput.push(t);
        }


        {
            let (l, t) = regular_append(&mut data_handle, &args, window_size);
            println!("rr{:?}events: {:>5}ns\t {:?}Hz", window_size, l, t);
            avg_regular_latencies.push(l);
            avg_regular_throughput.push(t);
        }

        {
            let (l, t) = corfu_multi(
                &mut sequencer_handle, &mut data_handle, &storage_chains, &args, window_size
            );
            println!("cm{:?}events: {:>5}ns\t {:?}Hz", window_size, l, t);
            avg_corfu_multi_latencies.push(l);
            avg_corfu_multi_throughput.push(t);
        }

        {
            let (l, t) = regular_distributed_multi(&mut data_handle, &args, window_size);
            println!("rd{:?}events: {:>5}ns\t {:?}Hz", window_size, l, t);
            avg_regular_dist_latencies.push(l);
            avg_regular_dist_throughput.push(t);
        }

        {
            let (l, t) = regular_local_multi(&mut data_handle, &args, window_size);
            println!("rl{:?}events: {:>5}ns\t {:?}Hz", window_size, l, t);
            avg_regular_local_latencies.push(l);
            avg_regular_local_throughput.push(t);
        }
    }

    println!("#elapsed {:?}", start_time.elapsed());
    println!("windows = {:?}", window_sizes);
    println!("avg_corfu_latencies  = {:?}", avg_corfu_latencies);
    println!("avg_corfu_throughput = {:?}", avg_corfu_throughput);

    println!("avg_corfu_multi_latencies  = {:?}", avg_corfu_multi_latencies);
    println!("avg_corfu_multi_throughput = {:?}", avg_corfu_multi_throughput);

    println!("avg_regular_latencies  = {:?}", avg_regular_latencies);
    println!("avg_regular_throughput = {:?}", avg_regular_throughput);

    println!("avg_regular_dist_latencies  = {:?}", avg_regular_dist_latencies);
    println!("avg_regular_dist_throughput = {:?}", avg_regular_dist_throughput);

    println!("avg_regular_local_latencies  = {:?}", avg_regular_local_latencies);
    println!("avg_regular_local_throughput = {:?}", avg_regular_local_throughput);
}

/////////////////////////

type Latency = u64;
type Throughput = u64;

#[inline(never)]
fn corfu_append(
    sequencer_handle: &mut LogHandle<()>,
    data_handle: &mut LogHandle<[u8]>,
    storage_chains: &[order],
    args: &Args,
    window_size: usize
) -> (Latency, Throughput) {
    let sequencer = order::from(1);

    let bytes = vec![0xf; 40];

    let mut outstanding_r1 = UuidHashMap::default();
    let mut outstanding_r2 = UuidHashMap::default();
    outstanding_r1.reserve(window_size);
    outstanding_r2.reserve(window_size);

    let mut finished_writes = 0;
    let mut avg_latency = MovingAvg::new();

    let write_start = Instant::now();
    while write_start.elapsed() < Duration::from_millis(args.round_ms as u64) {
        let outstanding_writes = outstanding_r1.len() + outstanding_r2.len();
        for _ in outstanding_writes..window_size {
            let start_time = Instant::now();
            let id = sequencer_handle.async_append(sequencer, &(), &[]);
            outstanding_r1.insert(id, start_time);
        }

        while let Ok((id, locs)) = sequencer_handle.try_wait_for_any_append() {
            let start_time = outstanding_r1.remove(&id).unwrap();
            let index: entry = locs[0].1;
            let storage_chain = u32::from(index) as usize % storage_chains.len();
            let id = data_handle.async_append(storage_chains[storage_chain], &bytes[..], &[]);
            outstanding_r2.insert(id, start_time);
        }
        while let Ok((id, ..)) = data_handle.try_wait_for_any_append() {
            let start_time = outstanding_r2.remove(&id).unwrap();
            let latency = start_time.elapsed();
            avg_latency.add_point(latency.subsec_nanos() as u64);
            assert_eq!(latency.as_secs(), 0);
            finished_writes += 1;
        }
    }

    data_handle.wait_for_all_appends().unwrap();
    sequencer_handle.wait_for_all_appends().unwrap();

    let num_secs = args.round_ms / 1000;
    (avg_latency.val(), finished_writes / num_secs as u64)
}


#[inline(never)]
fn corfu_multi(
    sequencer_handle: &mut LogHandle<()>,
    data_handle: &mut LogHandle<[u8]>,
    storage_chains: &[order],
    args: &Args,
    window_size: usize
) -> (Latency, Throughput) {
    let sequencer = &[order::from(2), order::from(3)];

    let bytes = vec![0xf; 40];

    let mut outstanding_r1 = UuidHashMap::default();
    let mut outstanding_r2 = UuidHashMap::default();
    outstanding_r1.reserve(window_size);
    outstanding_r2.reserve(window_size);

    let mut finished_writes = 0;
    let mut avg_latency = MovingAvg::new();

    let write_start = Instant::now();
    while write_start.elapsed() < Duration::from_millis(args.round_ms as u64) {
        let outstanding_writes = outstanding_r1.len() + outstanding_r2.len();
        for _ in outstanding_writes..window_size {
            let start_time = Instant::now();
            let id = sequencer_handle.async_multiappend(sequencer, &(), &[]);
            outstanding_r1.insert(id, start_time);
        }

        while let Ok((id, locs)) = sequencer_handle.try_wait_for_any_append() {
            let start_time = outstanding_r1.remove(&id).unwrap();
            let index: entry = locs[0].1;
            let storage_chain = u32::from(index) as usize % storage_chains.len();
            let id = data_handle.async_append(storage_chains[storage_chain], &bytes[..], &[]);
            outstanding_r2.insert(id, start_time);
        }
        while let Ok((id, ..)) = data_handle.try_wait_for_any_append() {
            let start_time = outstanding_r2.remove(&id).unwrap();
            let latency = start_time.elapsed();
            avg_latency.add_point(latency.subsec_nanos() as u64);
            assert_eq!(latency.as_secs(), 0);
            finished_writes += 1;
        }
    }

    data_handle.wait_for_all_appends().unwrap();
    sequencer_handle.wait_for_all_appends().unwrap();

    let num_secs = args.round_ms / 1000;
    (avg_latency.val(), finished_writes / num_secs as u64)
}


#[inline(never)]
fn regular_append(
    data_handle: &mut LogHandle<[u8]>,
    args: &Args,
    window_size: usize
) -> (Latency, Throughput) {
    let chain = order::from(100);

    let bytes = vec![0xf; 40];

    let mut outstanding = UuidHashMap::default();
    outstanding.reserve(window_size);

    let mut finished_writes = 0;
    let mut avg_latency = MovingAvg::new();

    let write_start = Instant::now();
    while write_start.elapsed() < Duration::from_millis(args.round_ms as u64) {
        let outstanding_writes = outstanding.len();
        for _ in outstanding_writes..window_size {
            let start_time = Instant::now();
            let id = data_handle.async_append(chain, &bytes[..], &[]);
            outstanding.insert(id, start_time);
        }

        while let Ok((id, ..)) = data_handle.try_wait_for_any_append() {
            if let Some(start_time) = outstanding.remove(&id) {
                let latency = start_time.elapsed();
                avg_latency.add_point(latency.subsec_nanos() as u64);
                assert_eq!(latency.as_secs(), 0);
                finished_writes += 1;
            }
        }
    }

    data_handle.wait_for_all_appends().unwrap();

    let num_secs = args.round_ms / 1000;
    (avg_latency.val(), finished_writes / num_secs as u64)
}

#[inline(never)]
fn regular_distributed_multi(
    data_handle: &mut LogHandle<[u8]>,
    args: &Args,
    window_size: usize
) -> (Latency, Throughput) {
    let chains = &[order::from(101), order::from(102)];

    let bytes = vec![0xf; 40];

    let mut outstanding = UuidHashMap::default();
    outstanding.reserve(window_size);

    let mut finished_writes = 0;
    let mut avg_latency = MovingAvg::new();

    let write_start = Instant::now();
    while write_start.elapsed() < Duration::from_millis(args.round_ms as u64) {
        let outstanding_writes = outstanding.len();
        for _ in outstanding_writes..window_size {
            let start_time = Instant::now();
            let id = data_handle.async_multiappend(chains, &bytes[..], &[]);
            outstanding.insert(id, start_time);
        }

        while let Ok((id, ..)) = data_handle.try_wait_for_any_append() {
            if let Some(start_time) = outstanding.remove(&id) {
                let latency = start_time.elapsed();
                avg_latency.add_point(latency.subsec_nanos() as u64);
                assert_eq!(latency.as_secs(), 0);
                finished_writes += 1;
            }
        }
    }

    data_handle.wait_for_all_appends().unwrap();

    let num_secs = args.round_ms / 1000;
    (avg_latency.val(), finished_writes / num_secs as u64)
}

#[inline(never)]
fn regular_local_multi(
    data_handle: &mut LogHandle<[u8]>,
    args: &Args,
    window_size: usize
) -> (Latency, Throughput) {
    let chains = &[order::from(103), order::from(105)];

    let bytes = vec![0xf; 40];

    let mut outstanding = UuidHashMap::default();
    outstanding.reserve(window_size);

    let mut finished_writes = 0;
    let mut avg_latency = MovingAvg::new();

    let write_start = Instant::now();
    while write_start.elapsed() < Duration::from_millis(args.round_ms as u64) {
        let outstanding_writes = outstanding.len();
        for _ in outstanding_writes..window_size {
            let start_time = Instant::now();
            let id = data_handle.async_multiappend(chains, &bytes[..], &[]);
            outstanding.insert(id, start_time);
        }

        while let Ok((id, ..)) = data_handle.try_wait_for_any_append() {
            if let Some(start_time) = outstanding.remove(&id) {
                let latency = start_time.elapsed();
                avg_latency.add_point(latency.subsec_nanos() as u64);
                assert_eq!(latency.as_secs(), 0);
                finished_writes += 1;
            }
        }
    }

    data_handle.wait_for_all_appends().unwrap();

    let num_secs = args.round_ms / 1000;
    (avg_latency.val(), finished_writes / num_secs as u64)
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
