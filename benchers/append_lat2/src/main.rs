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

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "append_lat", about = "latency vs num events benchmark.")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: ServerAddrs,
}

#[derive(Debug, Clone)]
pub struct ServerAddrs(Vec<(SocketAddr, SocketAddr)>);

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
                let tail = if let Some(addr) = addrs.next() {
                    addr
                } else {
                    head
                };
                assert!(addrs.next().is_none());
                (head, tail)
            }).collect()
        ))
    }
}

impl From<Vec<(SocketAddr, SocketAddr)>> for ServerAddrs {
    fn from(v: Vec<(SocketAddr, SocketAddr)>) -> Self {
        ServerAddrs(v)
    }
}

fn main() {
    // let _ = env_logger::init();
    let start = Instant::now();
    let args @ Args{..} = StructOpt::from_args();



    let replicated = args.servers.0[0].0 != args.servers.0[0].1;
    let mut handle = if replicated {
            LogHandle::<[u8]>::replicated_with_servers(&args.servers.0)
        } else {
            LogHandle::<[u8]>::unreplicated_with_servers(args.servers.0.iter().map(|&(a, _)| a))
        }.chains(&[1.into()])
            .build();

    let num_samples = 110_000;
    let mut latencies = Vec::with_capacity(num_samples);

    for _ in 0..num_samples {
        let start = Instant::now();
        let _ = handle.append(1.into(), &[1, 2, 3, 4, 5, 6, 7, 8], &[]);
        let elapsed = start.elapsed();
        latencies.push(elapsed);
    }

    let mut histogram = vec![0; 200];

    for latency in latencies.into_iter().skip(10_000) {
        histogram[round_to_10(subsec_micros(latency)) / 10] += 1;
    }

    let total_samples: usize = histogram.iter().sum();

    let percentages: Vec<_> = histogram.into_iter()
        .map(|h| (100 * h) / num_samples) // h / num_samples = x / 100, x = 100 * h / num_samples
        .collect();

    for i in 0..percentages.len() {
        println!("> {:<4} {:>3}", i * 10, percentages[i]);
    }

    let total_percent: usize = percentages.iter().sum();
    println!("{} samples", total_samples);
    println!("{}% visible", total_percent);
    println!("Finished in {}s", start.elapsed().as_secs());
}

fn round_to_10(time: usize) -> usize {
    if time % 10 == 0 {
        time
    } else if time % 10 >= 5 {
        (10 - time % 10) + time
    } else {
        time - time % 10
    }
}

fn subsec_micros(time: Duration) -> usize {
    (time.subsec_nanos() / 1_000) as usize
}

