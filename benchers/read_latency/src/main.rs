
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
use fuzzy_log_client::packets::order;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "", about = "")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: SocketAddr,

    #[structopt(short="r", long="rounds", help = "number of rounds.", default_value = "100")]
    num_rounds: usize,

    experiment: Experiment,

    num_chains: u32,
    nodes_per_chain: u32,
}


#[derive(StructOpt, Debug, Copy, Clone)]
enum Experiment {
    NoLink,
    ZigZag,
}

impl FromStr for Experiment {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "no_link" => Ok(Experiment::NoLink),
            "zigzag" => Ok(Experiment::ZigZag),
            s => Err(format!("invalid experiment {}, expected one of [NoLink, ZigZag]", s)),
        }
    }
}

fn main() {
    let start_time = Instant::now();
    let args @ Args{..} = StructOpt::from_args();

    let mut handle = LogHandle::unreplicated_with_servers(&[args.servers])
        .chains(args.chain_range().map(order::from))
        .build();

    let (pr_avg, pn_avg, _latencies) = regular_read(&mut handle, &args);
    println!("> r {:?} {}.{}: {:?} ns", args.experiment, args.num_chains, args.nodes_per_chain, pr_avg);
    println!("> n {:?} {}.{}: {:?} ns", args.experiment, args.num_chains, args.nodes_per_chain, pn_avg);
    // println!("{:?} {}.{}: {:?}", args.experiment, args.num_chains, args.nodes_per_chain, latencies);
    println!("experiment time {:?}", start_time.elapsed());
}

impl Args {
    fn chain_range(&self) -> ::std::ops::Range<u32> {
        self.first_chain()..(self.last_chain()+1)
    }

    fn first_chain(&self) -> u32 {
        1
    }

    fn last_chain(&self) -> u32 {
        self.num_chains
    }
}

/////////////////////////

static BYTES: [u8; 40] = [0xf; 40];

// #[inline(never)]
// fn tango_read(
//     handle: &mut LogHandle<[u8]>,
//     num_backpointers: u32,
//     args: &Args,
//     num_appends: u32,
//     written: &mut u32,
//     avg_latencies: &mut Vec<u32>,
// ) -> u32 {
//     let num_appends = num_appends+1;
//     let start_chain = order::from(num_backpointers * 1000);
//     let mut current_chain = start_chain + *written;
//     let needed_backpointers = ::std::cmp::min(num_backpointers, *written);
//     let mut back_pointers: Vec<_> = (0..needed_backpointers).map(|i|
//             OrderIndex(order::from(current_chain - (i + 1)), entry::from(1))
//         )
//         .collect();
//     //println!("w {:?} n {:?}", written, num_appends);
//     for _ in *written..num_appends {
//         //println!("b{:?}: {:?}", num_backpointers, back_pointers);
//         handle.async_append(current_chain, &BYTES[..], &back_pointers);
//         back_pointers.push(OrderIndex(current_chain, entry::from(1)));
//         if back_pointers.len() > num_backpointers as usize { back_pointers.remove(0); }

//         current_chain = current_chain + 1;
//         *written += 1;
//     }
//     handle.wait_for_all_appends().unwrap();
//     let snap_chain = current_chain - 1;

//     let mut latencies = Vec::with_capacity(args.num_rounds);
//     for _ in 0..args.num_rounds {
//         let mut num_events = 0;
//         let start_time = Instant::now();
//         handle.snapshot(snap_chain);
//         'recv1: loop {
//             let next = handle.get_next();
//             match next {
//                 Ok(..) => num_events += 1,
//                 Err(GetRes::Done) => break 'recv1,
//                 Err(r) => panic!("{:?}", r),
//             }
//         }
//         assert_eq!(num_events, num_appends);
//         let lat = start_time.elapsed();
//         latencies.push(lat);
//         for i in 0..num_appends {
//             handle.rewind(OrderIndex(start_chain + i, 1.into()))
//         }
//     }

//     let sum: Duration = latencies.drain(..).sum();
//     let avg: Duration = sum / (args.num_rounds as u32);
//     assert_eq!(avg.as_secs(), 0);
//     let lat = avg.subsec_nanos();
//     avg_latencies.push(lat);
//     lat
// }

#[inline(never)]
fn regular_read(
    handle: &mut LogHandle<[u8]>,
    args: &Args,
) -> (u32, u32, Vec<Duration>) {

    for chain in args.chain_range() {
        for i in 0..args.nodes_per_chain {
            append(handle, args, chain, i);
        }
    }
    handle.wait_for_all_appends().unwrap();

    let mut latencies = Vec::with_capacity(args.num_rounds);
    let num_appends = args.num_chains * args.nodes_per_chain;
    for _ in 0..args.num_rounds {
        let mut num_events = 0;
        let mut chains = args.chain_range().cycle();
        let start_time = Instant::now();
        handle.take_snapshot();
        'recv1: loop {
            let next = handle.get_next();
            match next {
                Ok((_, locs)) => {
                    if let Experiment::ZigZag = args.experiment {
                        // assert_eq!(locs[0].0, order::from(chains.next().unwrap()));
                    }
                    num_events += 1
                },
                Err(GetRes::Done) => break 'recv1,
                Err(r) => panic!("{:?}", r),
            }
        }
        assert_eq!(num_events, num_appends);
        let lat = start_time.elapsed();
        latencies.push(lat);
        for chain in args.chain_range() {
            handle.rewind((chain, 1).into())
        }

    }

    let sum: Duration = latencies.iter().sum();
    let per_round_avg: Duration = sum / (args.num_rounds as u32);
    let per_node_avg: Duration = (sum / num_appends) / (args.num_rounds as u32);
    assert_eq!(per_round_avg.as_secs(), 0);
    let per_round_avg = per_round_avg.subsec_nanos();
    let per_node_avg  = per_node_avg.subsec_nanos();
    (per_round_avg, per_node_avg, latencies)
}

fn append(handle: &mut LogHandle<[u8]>, args: &Args, chain: u32, i: u32) {
    match args.experiment {
        Experiment::NoLink => {
            handle.async_append(chain.into(), &BYTES[..], &[]);
        }
        Experiment::ZigZag => {
            let index = i + 1;
            if chain == args.first_chain() {
                if args.num_chains > 1 && index > 1 {
                    handle.async_append(
                        chain.into(), &BYTES[..], &[(args.last_chain(), index-1).into()]
                    );
                } else {
                    handle.async_append(chain.into(), &BYTES[..], &[]);
                }
            } else {
                assert!(chain > 1);
                handle.async_append(
                    chain.into(), &BYTES[..], &[(chain-1, index).into()]
                );
            }
        },
    }
}
