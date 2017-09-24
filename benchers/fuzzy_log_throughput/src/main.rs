
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log_client;
extern crate rand;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::iter;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering, ATOMIC_BOOL_INIT};
use std::thread::{sleep, yield_now};
use std::time::{Duration, Instant};

use fuzzy_log_client::fuzzy_log::log_handle::LogHandle;
use fuzzy_log_client::packets::order;

use structopt::StructOpt;

use rand::Rng;

#[derive(StructOpt, Debug)]
#[structopt(name = "pre_v_post", about = "pre vs post-hoc filtering benchmark.")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: ServerAddrs,

    #[structopt(short="c", long="client", help = "client num.")]
    client_num: usize,

    #[structopt(short="h", long="handles", help = "Num log handles a client should run.", default_value = "1")]
    handles: usize,

    #[structopt(short="r", long="rounds", help = "number of rounds.", default_value = "5")]
    num_rounds: usize,

    #[structopt(short="t", long="time_per_round", help = "number of milliseconds per round.", default_value = "2000")]
    ms_per_round: usize,

    #[structopt(short="w", long="write_window", help = "Number of outstanding mutations.", default_value = "500")]
    write_window: usize,

    #[structopt(short="o", long="colors_per_append", help = "Number of colors in each append.")]
    colors_per_append: usize,

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

static STARTED: AtomicBool = ATOMIC_BOOL_INIT;
const WAIT_CHAIN: u32 = 1010;

fn main() {
    let start_time = Instant::now();
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();
    //println!("#{:?}", args);
    //println!("#{:?} servers", args.servers.0.len());

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
            for join in joins {
                join.join();
            }
            record.join()
        });



    //println!("#elapsed {:?}", start_time.elapsed());
    println!("appends/s @ {} = {}", args.colors_per_append, avg_throughput);
}

/////////////////////////

#[inline(never)]
fn collector(
    writes: &Vec<AtomicUsize>, done: &AtomicBool, args: &Args,
) -> usize {
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
    done.store(true, Ordering::Relaxed);
    //println!("#mutations {:?}", throughput);
    // (sum_incs_per_round / num_rounds) = (incs/round);
    // (incs/round) / (ms/round) = incs/ms
    // incs/ms * 1000 = incs/s
    // incs/s = 1000 * incs/ms = 1000 * (incs/round) / (ms/round) =
    // 1000 * (sum_incs_per_round/num_rounds) / (ms/round)
    let avg_writes_per_handle_per_round: Vec<_> = throughput.iter().map(|writes_per_round| {
        let writes_per_handle = writes_per_round.iter().sum::<usize>();
        writes_per_handle / args.num_rounds
    }).collect();
    //println!("#avg_writes_per_handle_per_round {:?}", avg_writes_per_handle_per_round);
    let avg_writes_per_round = avg_writes_per_handle_per_round.iter().sum::<usize>();
    let awphpr = avg_writes_per_round / args.handles;
    //println!("#avg_writes_per_handle_per_round {:?}", awphpr);
    //println!("#avg_writes_per_round {:?}", avg_writes_per_round);
    let avg_throughput = avg_writes_per_round/(args.ms_per_round/1000);
    avg_throughput
}

/////////////////////////
/////////////////////////

#[inline(never)]
fn write(
    mutations: &Vec<AtomicUsize>, done: &AtomicBool, args: &Args, worker_num: usize,
) -> u64 {
    //let num_chains = ::std::cmp::max(args.servers.0.len(), args.total_handles);
    let mut num_chains = args.servers.0.len();
    while num_chains % args.colors_per_append != 0 { num_chains += 1 }
    let global_client_number = worker_num + args.client_num * args.handles;
    // println!("#gcn {:?}", global_client_number);
    let chains: Vec<_> = (0..num_chains)
        .map(|c| order::from((c+1) as u32)).collect();
    let handels_per_server = args.total_handles / args.servers.0.len();
    println!("#hps {:?}", handels_per_server);
    let home_chain = (global_client_number / handels_per_server) + 1;
    // the server doesn't like an id of all 0's, it thinks it's UUid::nil()
    let id = (global_client_number + 1) * 5;
    println!("id: {:?}, home: {:?}", id, home_chain);
    let mut log = LogHandle::replicated_with_servers(&args.servers.0)
        // .id([global_client_number as u8; 16])
        // .id([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, id as u8])
        .chains(chains.iter().cloned().chain(iter::once(order::from(WAIT_CHAIN))))
        .build();
    // println!("#home_chain {:?}", home_chain);
    // chains.swap(0, home_chain);
    /*let full_shards = num_chains / args.colors_per_append;
    println!("#full {:?}", full_shards);
    let partial_shards = 1;
    let partial_shard_len = num_chains % args.colors_per_append;
    println!("#part {:?}", partial_shards);
    let mut shard_num = global_client_number / (full_shards + partial_shards);
    println!("#shard {:?}", shard_num);
    if shard_num >= (full_shards + partial_shards) {
        shard_num = shard_num % (full_shards + partial_shards);
    }
    let (home_chain, end_chain) = if shard_num < full_shards {
        (shard_num * args.colors_per_append, shard_num * args.colors_per_append + args.colors_per_append)
    } else {
        (full_shards * args.colors_per_append, full_shards * args.colors_per_append + partial_shard_len)
    };
    let is_multi = end_chain - home_chain > 1;*/

    //println!("#chains [{:?}..{:?}], end_chain", home_chain, end_chain);

    let mut my_chains = None;
    for chain in chains.chunks(args.colors_per_append) {
        if chain.contains(&order::from(home_chain as u32)) {
            my_chains = Some(chain);
            break
        }
    }
    let chains = my_chains.unwrap();
    // println!("#my_chains {:?}", my_chains);
    // let my_chains = my_chains.unwrap_or_else(||
    //     panic!("could not find {:?} in {:?}", home_chain, chains));
    //println!("#chains {:?} @ {:?}", my_chains, home_chain);
    /*let chains: Vec<_> = chains.chunks(args.colors_per_append).collect();*/
    //println!("{:?}", chains);

    wait_for_clients_start(&mut log, args);

    let mut outstanding_writes = 0;
    let mut total_writes = 0u64;

    /*let mut gen = rand::thread_rng();*/
    // gen.shuffle(&mut chains[..]);
    while !done.load(Ordering::Relaxed) {
        let mut num_appends = 0;
        while let Ok(..) = log.try_wait_for_any_append() {
            outstanding_writes -= 1;
            num_appends += 1;
        }
        mutations[worker_num].fetch_add(num_appends, Ordering::Relaxed);

        for _ in outstanding_writes..args.write_window {
            /*if args.colors_per_append == 1 /*|| gen.gen()*/ {
                //gen.shuffle(&mut chains[..]);
                log.async_append(my_chains[0], &total_writes, &[]);
            } else if my_chains.len() == 1 {
                log.async_append(my_chains[0], &total_writes, &[]);
            } else {
                // if total_writes % 500 == 0 {
                //     gen.shuffle(&mut chains[..]);
                // }
                // gen.shuffle(&mut chains[..]);
                // if chains.len() > 2 {
                //     let len = chains.len();
                //     chains.swap(1, gen.gen_range(1, len))
                // }
                log.async_multiappend(
                    my_chains,
                    &total_writes,
                    &[]);
            }*/
            // let selection = gen.choose(&chains[..]).unwrap();
            let selection = chains;
            if selection.len() > 1 {
               log.async_multiappend(selection, &total_writes, &[]);
            } else {
                log.async_append(selection[0], &total_writes, &[]);
            }
            outstanding_writes += 1;
            total_writes += 1;
        }
    }
    sleep(Duration::from_millis(args.ms_per_round as u64));
    total_writes
}

/////////////////////////

fn wait_for_clients_start<V: Default>(log: &mut LogHandle<V>, args: &Args) {
    log.append(WAIT_CHAIN.into(), &Default::default(), &[]);
    let mut clients_started = 0;
    while clients_started < args.total_handles {
        log.snapshot(WAIT_CHAIN.into());
        while let Ok(..) = log.get_next() {
            clients_started += 1
        }
    }
    STARTED.store(true, Ordering::Relaxed);
}
