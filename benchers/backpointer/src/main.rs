
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log_client;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::{Duration, Instant};

use fuzzy_log_client::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log_client::packets::{entry, order, OrderIndex};

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "backpointer", about = "backpointer vs forward benchmark.")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: ServerAddrs,

    #[structopt(short="r", long="rounds", help = "number of rounds.", default_value = "10000")]
    num_rounds: usize,
}

#[derive(Debug)]
struct ServerAddrs(Vec<SocketAddr>);

impl FromStr for ServerAddrs {
    type Err = std::string::ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        println!("{}", s);
        Ok(ServerAddrs(
            s.split('^').map(|t|{
                match SocketAddr::from_str(t) {
                    Ok(addr) => addr,
                    Err(e) => panic!("head parse err {} @ {}", e, s),
                }
            }).collect()
        ))
    }
}

fn main() {
    let start_time = Instant::now();
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();
    println!("#{:?}", args);
    let num_appends = [1, 4, 10, 20, 40, 80, 160]; //, 320, 640, 1280];
    let mut tango_latencies = [
        (1, 0, Vec::with_capacity(num_appends.len())),
        (4, 0, Vec::with_capacity(num_appends.len())),
        (10, 0, Vec::with_capacity(num_appends.len())),
    ];
    let mut reg_written = 0;
    let mut regular_latencies = Vec::with_capacity(num_appends.len());

    let mut handle = LogHandle::unreplicated_with_servers(&args.servers.0)
        .chains(
            [order::from(1)].iter().cloned().chain(
                [1, 4, 10].iter().flat_map(|&i| (0..1380).map(move |j| order::from(i * 1000 + j)))
            )
        )
        .build();

    for &num_appends in num_appends.into_iter() {
        for &mut (num_backpointers, ref mut written, ref mut latencies) in &mut tango_latencies {
            let l = tango_read(
                &mut handle, num_backpointers, &args, num_appends, written, latencies
            );
            println!("t{:?}: {:?}  {:>5}ns", num_backpointers, num_appends, l);
        }

        let l = regular_read(
            &mut handle, &args, num_appends, &mut reg_written, &mut regular_latencies
        );
        println!("r: {:?} {:>5}ns", num_appends, l);
    }

    println!("#elapsed {:?}", start_time.elapsed());
    println!("num_appends = {:?}", num_appends);
    for &(num_backpointers, _, ref latencies) in &tango_latencies {
        println!("t{:?} = {:?}", num_backpointers, latencies);
    }
    println!("avg_regular_latencies = {:?}", regular_latencies);
}

/////////////////////////

static BYTES: [u8; 40] = [0xf; 40];

#[inline(never)]
fn tango_read(
    handle: &mut LogHandle<[u8]>,
    num_backpointers: u32,
    args: &Args,
    num_appends: u32,
    written: &mut u32,
    avg_latencies: &mut Vec<u32>,
) -> u32 {
    let num_appends = num_appends+1;
    let start_chain = order::from(num_backpointers * 1000);
    let mut current_chain = start_chain + *written;
    let needed_backpointers = ::std::cmp::min(num_backpointers, *written);
    let mut back_pointers: Vec<_> = (0..needed_backpointers).map(|i|
            OrderIndex(order::from(current_chain - (i + 1)), entry::from(1))
        )
        .collect();
    //println!("w {:?} n {:?}", written, num_appends);
    for _ in *written..num_appends {
        //println!("b{:?}: {:?}", num_backpointers, back_pointers);
        handle.async_append(current_chain, &BYTES[..], &back_pointers);
        back_pointers.push(OrderIndex(current_chain, entry::from(1)));
        if back_pointers.len() > num_backpointers as usize { back_pointers.remove(0); }

        current_chain = current_chain + 1;
        *written += 1;
    }
    handle.wait_for_all_appends().unwrap();
    let snap_chain = current_chain - 1;

    let mut latencies = Vec::with_capacity(args.num_rounds);
    for _ in 0..args.num_rounds {
        let mut num_events = 0;
        let start_time = Instant::now();
        handle.snapshot(snap_chain);
        'recv1: loop {
            let next = handle.get_next();
            match next {
                Ok(..) => num_events += 1,
                Err(GetRes::Done) => break 'recv1,
                Err(r) => panic!("{:?}", r),
            }
        }
        assert_eq!(num_events, num_appends);
        let lat = start_time.elapsed();
        latencies.push(lat);
        for i in 0..num_appends {
            handle.rewind(OrderIndex(start_chain + i, 1.into()))
        }
    }

    let sum: Duration = latencies.drain(..).sum();
    let avg: Duration = sum / (args.num_rounds as u32);
    assert_eq!(avg.as_secs(), 0);
    let lat = avg.subsec_nanos();
    avg_latencies.push(lat);
    lat
}

#[inline(never)]
fn regular_read(
    handle: &mut LogHandle<[u8]>,
    args: &Args,
    num_appends: u32,
    written: &mut u32,
    avg_latencies: &mut Vec<u32>,
) -> u32 {
    let chain = order::from(1);
    for _ in *written..num_appends {
        handle.async_append(chain, &BYTES[..], &[]);
        *written += 1;
    }
    handle.wait_for_all_appends().unwrap();

    let mut latencies = Vec::with_capacity(args.num_rounds);
    for _ in 0..args.num_rounds {
        let mut num_events = 0;
        let start_time = Instant::now();
        handle.snapshot(chain);
        'recv1: loop {
            let next = handle.get_next();
            match next {
                Ok(..) => num_events += 1,
                Err(GetRes::Done) => break 'recv1,
                Err(r) => panic!("{:?}", r),
            }
        }
        assert_eq!(num_events, num_appends);
        let lat = start_time.elapsed();
        latencies.push(lat);
        handle.rewind(OrderIndex(chain, 1.into()))
    }

    let sum: Duration = latencies.drain(..).sum();
    let avg: Duration = sum / (args.num_rounds as u32);
    assert_eq!(avg.as_secs(), 0);
    let lat = avg.subsec_nanos();
    avg_latencies.push(lat);
    lat
}
