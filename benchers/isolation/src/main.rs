
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log_client;
extern crate rand;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering, ATOMIC_BOOL_INIT, ATOMIC_USIZE_INIT};
use std::thread::{sleep, yield_now};
use std::time::{Duration, Instant};

use fuzzy_log_client::fuzzy_log::log_handle::LogHandle;
// use fuzzy_log_client::packets::order;

use structopt::StructOpt;

use rand::Rng;

#[derive(StructOpt, Debug)]
#[structopt(name = "pre_v_post", about = "pre vs post-hoc filtering benchmark.")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: ServerAddrs,

    //#[structopt(short="c", long="client", help = "client num.")]
    //client_num: usize,

    #[structopt(short="h", long="handles", help = "Num log handles a client should run.", default_value = "2")]
    handles: usize,

    #[structopt(short="r", long="rounds", help = "number of rounds.", default_value = "5")]
    num_rounds: usize,

    #[structopt(short="t", long="time_per_round", help = "number of milliseconds per round.", default_value = "2000")]
    ms_per_round: usize,

    #[structopt(short="w", long="write_window", help = "Number of outstanding mutations.", default_value = "1000")]
    write_window: usize,

    #[structopt(short="p", long="percent_multiappends", help = "percent multiappends.")]
    percent_multiappends: usize,

    //#[structopt(short="a", long="total_handles", help = "Number of handles that need to have started before we collect data.")]
    //total_handles: usize,
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

static STARTED: AtomicBool = ATOMIC_BOOL_INIT;
// const WAIT_CHAIN: u32 = 1010;

fn main() {
    let start_time = Instant::now();
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();
    println!("#{:?}", args);
    println!("#{:?} servers", args.servers.0.len());

    let done = AtomicBool::new(false);
    let writes = (0..args.handles).map(|_| AtomicUsize::new(0)).collect();

    let avg_throughput =
        crossbeam::scope(|scope| {
            let args = &args;
            let done = &done;
            let writes = &writes;

            let joins: Vec<_> = (0..args.handles).map(|i| {
                scope.spawn(move || write(writes, done, args, i))
            }).collect();
            let record = scope.spawn(move || collector(writes, done, args));
            let total_writes: Vec<_> = joins.into_iter().map(|j| j.join()).collect();
            println!("#total_writes = {:?}", total_writes);
            record.join()
        });



    println!("#elapsed {:?}", start_time.elapsed());
    println!("appends/s @ {}% = {:?}", args.percent_multiappends, avg_throughput);
}

/////////////////////////

#[inline(never)]
fn collector(
    writes: &Vec<AtomicUsize>, done: &AtomicBool, args: &Args,
) -> Vec<usize> {
    while !STARTED.load(Ordering::Relaxed) { yield_now() }
    let mut throughput: Vec<_> =
        (0..args.handles).map(|_| Vec::with_capacity(args.num_rounds)).collect();
    for _ in 0..args.num_rounds {
        sleep(Duration::from_millis(args.ms_per_round as u64));
        for (i, num_writes) in writes.iter().enumerate() {
            let write = num_writes.swap(0, Ordering::Relaxed);
            throughput[i].push(write);
        }
    }
    sleep(Duration::from_millis(args.ms_per_round as u64));
    println!("#throughput {:?}", throughput);
    println!("#writes {:?}", writes);
    done.store(true, Ordering::Relaxed);
    throughput.iter().map(|writes_per_round| {
            let writes_per_handle = writes_per_round.iter().sum::<usize>();
            (writes_per_handle / args.num_rounds)/(args.ms_per_round/1000)
        })
        .collect()
}

/////////////////////////
/////////////////////////

#[inline(never)]
fn write(
    mutations: &Vec<AtomicUsize>, done: &AtomicBool, args: &Args, worker_num: usize,
) -> u64 {
    let do_multis = worker_num % 2 != 0;
    println!("#client {:?} {}", worker_num, if do_multis { "multi" } else { "single" } );
    let colors = [2.into(), 3.into(), 4.into()];
    let mut log = LogHandle::replicated_with_servers(&args.servers.0)
        .chains(&colors)
        .build();

    wait_for_clients_start(&mut log, args);

    let mut used_window = 0;
    let mut total_writes = 0u64;

    let mut gen = rand::thread_rng();
    while !done.load(Ordering::Relaxed) {
        let mut num_appends = 0;
        while let Ok((_, locs)) = log.try_wait_for_any_append() {
            if do_multis && locs.len() > 1 {
                used_window -= 1;
            } else {
                used_window -= 2;
            }
            num_appends += 1;
        }
        mutations[worker_num].fetch_add(num_appends, Ordering::Relaxed);

        while used_window < args.write_window {
            // x / 100 = 1 / n
            // n x / 100 = 1
            // n = 100 / x
            if do_multis && gen.gen_weighted_bool(100/args.percent_multiappends as u32) {
                log.async_multiappend(&colors[1..], &total_writes, &[]);
                used_window += 1;
            } else {
                let color = if do_multis { colors[1] } else { colors[0] };
                log.async_append(color, &total_writes, &[]);
                used_window += 2;
            }
            total_writes += 1;
        }
    }
    sleep(Duration::from_millis(args.ms_per_round as u64));
    total_writes
}

/////////////////////////

fn wait_for_clients_start<V: Default>(_log: &mut LogHandle<V>, args: &Args) {
    static NUM_STARTED: AtomicUsize = ATOMIC_USIZE_INIT;
    NUM_STARTED.fetch_add(1, Ordering::Relaxed);
    while NUM_STARTED.load(Ordering::Relaxed) < args.handles {}
    // log.append(WAIT_CHAIN.into(), &Default::default(), &[]);
    // let mut clients_started = 0;
    // while clients_started < args.total_handles {
    //     log.snapshot(WAIT_CHAIN.into());
    //     while let Ok(..) = log.get_next() {
    //         clients_started += 1
    //     }
    // }
    STARTED.store(true, Ordering::Relaxed);
}
