
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log;
extern crate rand;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::iter;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use fuzzy_log::async::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log::packets::{order, OrderIndex};

use structopt::StructOpt;

use rand::Rng;

#[derive(StructOpt, Debug)]
#[structopt(name = "pre_v_post", about = "pre vs post-hoc filtering benchmark.")]
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
    //let byte_sizes = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000];
    let byte_sizes = [10_000, 100_000, 1_000_000];

    for &bytes in byte_sizes.into_iter() {
        let ((b, f, p), w) = get_latency(&mut avg_latencies, &args, bytes);
        println!("{:?}b: [{:>5}ns, {:>5}ns, {:>5}ns]\t {:.1}",
            bytes, b, f, p, w);
    }



    println!("#elapsed {:?}", start_time.elapsed());
    println!("bytes = {:?}", byte_sizes);
    let (base, others): (Vec<_>, Vec<_>) = avg_latencies
        .into_iter().map(|(b, f, p)| (b, (f, p))).unzip();
    let (pre, post): (Vec<_>, Vec<_>) = others.into_iter().unzip();
    println!("base = {:?}", base);
    println!("pre = {:?}", pre);
    println!("post = {:?}", post);
}

/////////////////////////

#[inline(never)]
fn get_latency(
    avg_latencies: &mut Vec<(u32, u32, u32)>, args: &Args, level: u32,
) -> ((u32, u32, u32), u64) {
    let chain = order::from(level);
    let filtered = order::from(level + 1);
    let mut log = if let Some(tail_server) = args.tail_server {
        LogHandle::replicated_with_servers(
                iter::once((args.head_server, tail_server)))
            .chains(iter::once(chain).chain(iter::once(filtered)))
            .build()
    } else {
        LogHandle::unreplicated_with_servers(iter::once(args.head_server))
            .chains(iter::once(chain).chain(iter::once(filtered)))
            .build()
    };

    let bytes = vec![0xf; level as usize];

    let write_start = Instant::now();
    for (_, solo) in (0..100).zip(rand::thread_rng().gen_iter()) {
        if solo {
            log.async_append(chain, &bytes[..], &[]);
        } else {
            log.async_no_remote_multiappend(&[chain, filtered], &bytes[..], &[]);
        }
    }
    log.wait_for_all_appends().unwrap();
    let write_time = write_start.elapsed();
    println!("writes took {}.{:010}s", write_time.as_secs(), write_time.subsec_nanos());

    let (base, pre, post): (u32, u32, u32);

    let mut latencies = Vec::with_capacity(args.num_rounds);

    let mut work = 0u64;

    {
        let mut record_latency = |chain, post_filter| {
            let mut expected_num_events = None;
            for _ in 0..args.num_rounds {
                let mut num_events = 0;
                let start_time = Instant::now();
                log.snapshot(chain);
                'recv1: loop {
                    let next = log.get_next();
                    match next {
                        Ok((b, locs)) if post_filter => if locs.len() != 1 {
                            work = work.wrapping_add(b.iter().fold(0,
                                |a, &b| a.wrapping_add(b as u64)));
                            num_events += 1;
                        },
                        Ok((b, _)) =>{
                            work = work.wrapping_add(b.iter().fold(0,
                                |a, &b| a.wrapping_add(b as u64)));
                            num_events += 1;
                        },
                        Err(GetRes::Done) => break 'recv1,
                        Err(r) => panic!("{:?}", r),
                    }
                }
                expected_num_events.map(|e| assert_eq!(num_events, e));
                expected_num_events = Some(num_events);
                let lat = start_time.elapsed();
                latencies.push(lat);
                log.rewind(OrderIndex(chain.into(), 1.into()))
            }

            let sum: Duration = latencies.drain(..).sum();
            let avg: Duration = sum / (args.num_rounds as u32);
            assert_eq!(avg.as_secs(), 0);
            avg.subsec_nanos()
        };

        base = record_latency(chain, false);
        pre = record_latency(filtered, false);
        post = record_latency(chain, true);
    }

    avg_latencies.push((base, pre, post));

    ((base, pre, post), work)
}
