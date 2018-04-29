
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
use std::sync::mpsc;
use std::thread::{spawn, sleep, yield_now};
use std::time::{Instant, Duration};

use fuzzy_log_client::fuzzy_log::log_handle::{Uuid, append_message};
use fuzzy_log_client::packets::{order, OrderIndex};
use fuzzy_log_client::store::{AsyncTcpStore, AsyncStoreClient, mio};

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

    #[structopt(short="w", long="write_window", help = "Number of outstanding mutations.", default_value = "500")]
    write_window: usize,

    #[structopt(short="h", long="handles_per_client", help = "Number of handles per client instance.")]
    handles_per_client: usize,

    #[structopt(short="c", long="corfu", help = "use corfu style appends instead of fuzzylog.")]
    corfu: bool,
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

fn main() {
    let start_time = Instant::now();
    let mut args @ Args{..} = StructOpt::from_args();
    args.client_num = args.client_num * args.handles_per_client + 1;
    // args.client_num += 1;
    println!("{:?}", args);

    let client_num = args.client_num;
    let handles_per_client = args.handles_per_client;
    let corfu = args.corfu;

    // let balance_num = if corfu {
    //         [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15][args.client_num]
    //     } else {
    //         [1, 61, 2, 62, 3, 63, 4, 64, 5, 65, 6, 66, 7, 67, 8, 68, 9, 69, 10, 70, 11, 71, 12, 72, 13, 73, 14, 74, 15, 75][args.client_num]
    //     };

    //TODO
    let balance_num = |balance_num| if corfu || balance_num % 2 != 0 {
        balance_num
    } else {
        (balance_num - 1 + 60)
    };

    let data_handles: Vec<_> = (0..handles_per_client).map(|i| {
        let (ack, acks) = mpsc::channel();
        let balance_num = balance_num(client_num + i) as _;
        let ack2 = if corfu { Some(ack.clone()) } else { None };
        let handle = build_store(args.servers.0.clone(), balance_num, ack.into());
        (handle, ack2, acks)
    }).collect();



    let (kind, throughput) = if corfu {
        let handles = data_handles.into_iter().enumerate().map(|(i, (data_handle, ack, acks))| {
            let balance_num = balance_num(client_num + i) as _;
            let sequencer_handle = build_store(
                vec![(args.sequencer, args.sequencer)], balance_num, ack.unwrap().into()
            );
            (sequencer_handle, data_handle, acks)
        }).collect();

        ("c", measure_corfu(handles, args))
    } else {
        let handles = data_handles.into_iter().map(|(h, _, a)| (h, a)).collect();
        ("r", measure_fuzzy_log(handles, args))
    };

    println!("> {} {}.{} = {} Hz", kind, client_num, handles_per_client, throughput);
    println!("experiment {:?}s", start_time.elapsed().as_secs());
}

fn measure_corfu(
    handles: Vec<(ToStore, ToStore, RecvAck)>, args: Args
) -> usize {
    let corfu_writes = AtomicUsize::new(0);
    let done = AtomicBool::new(false);
    let data_chains: Vec<_> = (0..args.servers.0.len())
            .map(|i| i as u32 + 12_000).map(order::from).collect();
    crossbeam::scope(|scope| {
        let data_chains = &*data_chains;

        let args = &args;
        let done = &done;

        let corfu_writes = &corfu_writes;

        let record = scope.spawn(move || collector(corfu_writes, done, args));

        let handles: Vec<_> = handles.into_iter().map(move |(sequencer_handle, data_handle, acks)|
            scope.spawn(move ||
                corfu_append(
                    sequencer_handle,
                    data_handle,
                    acks,
                    &data_chains[..],
                    corfu_writes,
                    done,
                    args
                )
            )
        ).collect();
        for handle in handles {
            handle.join()
        }

        record.join()
    })
}

fn measure_fuzzy_log(data_handles: Vec<(ToStore, RecvAck)>, args: Args) -> usize {
    let reg_writes = AtomicUsize::new(0);
    let done = AtomicBool::new(false);
    crossbeam::scope(|scope| {
        let args = &args;
        let done = &done;

        let reg_writes = &reg_writes;

        let record = scope.spawn(move || collector(reg_writes, done, args));

        let handles: Vec<_> = data_handles.into_iter().enumerate().map(move |(i, (data_handle, acks))|
            scope.spawn(move ||
                regular_append(
                    data_handle, acks, &[((args.client_num + i) as u32).into()], reg_writes, done, args
                )
            )
        ).collect();
        for handle in handles {
            handle.join()
        }

        record.join()
    })
}

/////////////////////////

#[inline(never)]
fn collector(
    writes: &AtomicUsize, done: &AtomicBool, args: &Args,
) -> usize {
    let mut throughput = Vec::with_capacity(args.num_rounds);
    sleep(Duration::from_secs(1));
    writes.swap(0, Ordering::Relaxed);

    for _ in 0..args.num_rounds {
        sleep(Duration::from_secs(1));
        let write = writes.swap(0, Ordering::Relaxed);
        throughput.push(write);
    }

    sleep(Duration::from_secs(1));
    done.store(true, Ordering::Relaxed);

    let total_writes = throughput.iter().sum::<usize>();
    let avg_writes = total_writes / args.num_rounds;
    avg_writes
}

/////////////////////////
/////////////////////////


#[inline(never)]
fn corfu_append(
    sequencer_handle: ToStore,
    data_handle: ToStore,
    acks: RecvAck,
    storage_chains: &[order],
    writes: &AtomicUsize,
    done: &AtomicBool,
    args: &Args,
) {
    let sequencer = order::from(1);

    let bytes = [0xf; 20];

    let mut outstanding = 0;

    while !done.load(Ordering::Relaxed) {

        for _ in outstanding..args.write_window {
            #[allow(deprecated)]
            let _ = sequencer_handle.send(append_message(sequencer, &(), &[]));

            outstanding += 1;
        }

        for OrderIndex(chain, index) in acks.try_iter() {
            if chain == sequencer {
                let storage_chain = u32::from(index) as usize % storage_chains.len();
                let chain = storage_chains[storage_chain];
                #[allow(deprecated)]
                let _ = data_handle.send(append_message(chain, &bytes[..], &[]));
            } else {
                outstanding -= 1;
                writes.fetch_add(1, Ordering::Relaxed);
            }

        }

        if outstanding == args.write_window {
            yield_now()
        }
    }

    if outstanding > 0 {
        for _ in acks.iter() {
            outstanding -= 1;
            if outstanding == 0 {
                break
            }
        }
    }
}

/////////////////////////


#[inline(never)]
fn regular_append(
    data_handle: ToStore,
    acks: RecvAck,
    chains: &[order],
    writes: &AtomicUsize,
    done: &AtomicBool,
    args: &Args,
) {
    let bytes = [0xf; 20];

    let mut outstanding = 0;

    while !done.load(Ordering::Relaxed) {
        for _ in outstanding..args.write_window {
            #[allow(deprecated)]
            let _ = data_handle.send(append_message(chains[0], &bytes[..], &[]));

            outstanding += 1;
        }

        for _ in acks.try_iter() {
            outstanding -= 1;
            writes.fetch_add(1, Ordering::Relaxed);
        }

        if outstanding == args.write_window {
            yield_now()
        }
    }

    if outstanding > 0 {
        for _ in acks.iter() {
            outstanding -= 1;
            if outstanding == 0 {
                break
            }
        }
    }
}

/////////////////////////

type ToStore = ::fuzzy_log_client::store::ToSelf;
type RecvAck = mpsc::Receiver<OrderIndex>;

fn build_store(servers: Vec<(SocketAddr, SocketAddr)>, balance_num: u64, ack_on: Acker)
-> ToStore {
    let (s, r) = mpsc::channel();
    spawn(move || {
        let replicated = servers[0].0 != servers[0].1;
        let id = balance_num.into();
        let mut event_loop = mio::Poll::new().unwrap();
        let (store, to_store) = if replicated {
            AsyncTcpStore::replicated_new_tcp(id, servers, ack_on, &mut event_loop).unwrap()
        } else {
            AsyncTcpStore::new_tcp(id, servers.iter().map(|&(a, _)| a), ack_on, &mut event_loop).unwrap()
        };
        s.send(to_store).unwrap();
        store.run(event_loop)
    });
    r.recv().unwrap()
}

struct Acker {
    ack: mpsc::Sender<OrderIndex>,
}

impl From<mpsc::Sender<OrderIndex>> for Acker {
    fn from(ack: mpsc::Sender<OrderIndex>) -> Self {
        Self {
            ack
        }
    }
}

impl AsyncStoreClient for Acker {
    fn on_finished_read(
        &mut self,
        _read_loc: OrderIndex,
        _read_packet: Vec<u8>
    ) -> Result<(), ()> {
        // self.ack.send(()).map_err(|_| {})
        Ok(())
    }

    fn on_finished_write(
        &mut self,
        _write_id: Uuid,
        write_locs: Vec<OrderIndex>
    ) -> Result<(), ()> {
        self.ack.send(write_locs[0]).map_err(|_| {})
    }

    fn on_io_error(&mut self, _err: ::std::io::Error, _server: usize) -> Result<(), ()> {
        Err(())
    }
}
