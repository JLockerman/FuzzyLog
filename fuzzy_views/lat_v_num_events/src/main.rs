
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::iter;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use fuzzy_log::async::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log::packets::{order, OrderIndex};

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "lat_v_event", about = "latency vs num events benchmark.")]
struct Args {
    #[structopt(help = "head of the FuzzyLog server chain to run against.")]
    head_server: SocketAddr,

    #[structopt(help = "tail of the FuzzyLog server chain to run against.")]
    tail_server: Option<SocketAddr>,

    #[structopt(short="r", long="rounds", help = "number of rounds.", default_value = "10000")]
    num_rounds: usize,
}

fn main() {
    let start_time = Instant::now();
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();
    println!("#{:?}", args);
    let mut avg_latencies = Vec::new();
    let num_events = [1, 10, 20, 40, 80, 160, 320, 640, 1280];

    for &events in num_events.into_iter() {
        let (l, w) = get_latency(&mut avg_latencies, &args, events);
        println!("{:?}events: {:>5}ns\t {:.1}",
            events, l, w);
    }

    println!("#elapsed {:?}", start_time.elapsed());
    println!("events = {:?}", num_events);
    println!("{:?}", avg_latencies);
}

/////////////////////////

#[inline(never)]
fn get_latency(
    avg_latencies: &mut Vec<u32>, args: &Args, events: usize) -> (u32, u64) {
    let chain = order::from(events as u32);
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

    let bytes = vec![0xf; 40];

    let write_start = Instant::now();
    for _ in 0..events {
        log.async_append(chain, &bytes[..], &[]);
    }
    log.wait_for_all_appends().unwrap();
    let write_time = write_start.elapsed();
    println!("writes took {}.{:010}s", write_time.as_secs(), write_time.subsec_nanos());

    let mut latencies = Vec::with_capacity(args.num_rounds);

    let mut work = 0u64;
    let lat;

    {
        let mut record_latency = |chain| {
            for _ in 0..args.num_rounds {
                let mut num_events = 0;
                let start_time = Instant::now();
                log.snapshot(chain);
                'recv1: loop {
                    let next = log.get_next();
                    match next {
                        Ok((b, _)) =>{
                            work = work.wrapping_add(b.iter().fold(0,
                                |a, &b| a.wrapping_add(b as u64)));
                            num_events += 1;
                        },
                        Err(GetRes::Done) => break 'recv1,
                        Err(r) => panic!("{:?}", r),
                    }
                }
                let lat = start_time.elapsed();
                assert_eq!(num_events, events);
                latencies.push(lat);
                log.rewind(OrderIndex(chain.into(), 1.into()))
            }

            let sum: Duration = latencies.drain(..).sum();
            let avg: Duration = sum / (args.num_rounds as u32);
            assert_eq!(avg.as_secs(), 0);
            avg.subsec_nanos()
        };

        lat = record_latency(chain);
    }

    avg_latencies.push(lat);

    (lat, work)
}
