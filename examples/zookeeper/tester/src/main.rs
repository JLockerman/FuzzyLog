extern crate zookeeper;

extern crate rand;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, AtomicBool, ATOMIC_USIZE_INIT, ATOMIC_BOOL_INIT};
use std::sync::atomic::Ordering::Relaxed;
use std::thread;
use std::time::Duration;

use rand::{XorShiftRng as Rand, SeedableRng, Rng};

use structopt::StructOpt;

use zookeeper::{Client, CreateMode, LogHandle};

fn main() {
    let args @ Args{..} = StructOpt::from_args();
    let client_num = args.client_num as u32;
    let color = (client_num + 1).into();
    let servers = &*args.servers.0;
    let (mut reader, writer) = if servers[0].0 != servers[0].1 {
            LogHandle::<[u8]>::replicated_with_servers(&servers[..])
        } else {
            LogHandle::<[u8]>::unreplicated_with_servers(servers.iter().map(|&(a, _)| a))
        }
        .chains(&[color, 10_001.into()])
        .reads_my_writes()
        .build_handles();


    {
        let num_sync_clients = args.sync_clients;
        ::std::thread::sleep(::std::time::Duration::from_millis(10));
        writer.async_append(10_001.into(), &[], &[]);
        let mut clients_started = 0;
        while clients_started < num_sync_clients {
            reader.snapshot(10_001.into());
            while let Ok(..) = reader.get_next() { clients_started += 1 }
        }
    }

    let num_clients = args.sync_clients;

    let my_dir = client_num;
    let my_root = format!("/foo{}", my_dir).into();
    let roots = (0..num_clients).map(|i| (format!("/foo{}",i), (i as u32 + 1).into()))
        .collect();

    let mut client = Client::new(reader, writer, color, my_root, roots);


    static RECEIVED: AtomicUsize = ATOMIC_USIZE_INIT;
    static SENT: AtomicUsize = ATOMIC_USIZE_INIT;
    static DONE: AtomicBool = ATOMIC_BOOL_INIT;
    static STOPPED: AtomicBool = ATOMIC_BOOL_INIT;

    DONE.store(false, Relaxed);
    STOPPED.store(false, Relaxed);

    thread::spawn(move ||{
        let mut rand = Rand::from_seed([111, 234, client_num, 1010]);
        let mut file_num = 0;
        let mut rename_num = 0;
        let mut window = 0;
        while !DONE.load(Relaxed) {
            while window - RECEIVED.load(Relaxed) > 10000 { thread::yield_now() }
            if rand.gen_weighted_bool(100) && rename_num < RECEIVED.load(Relaxed) {
                let rename_dir = rand.gen_range(0, num_clients);
                client.rename(
                    format!("/foo{}/{}", my_dir, rename_num),
                    format!("/foo{}/r{}_{}", rename_dir, client_num, rename_num),
                    Box::new(|_, _| { RECEIVED.fetch_add(1, Relaxed); }),
                );
                rename_num += 1;
                window += 1;
            } else {
                client.create(
                    format!("/foo{}/{}", my_dir, file_num),
                    (&b"AAA"[..]).into(),
                    CreateMode::persistent(),
                    Box::new(|_, _, _| { RECEIVED.fetch_add(1, Relaxed); }),
                );
                file_num += 1;
                window += 1;
            }
            SENT.fetch_add(1, Relaxed);
        }
        STOPPED.store(true, Relaxed);
    });

    let num_rounds = 10;
    let mut total_completed = 0;
    let mut total_sent = 0;
    let mut complete = vec![0; num_rounds];
    let mut sent = vec![0; num_rounds];
    let mut outstanding = vec![0; num_rounds];
    for i in 0..num_rounds {
        thread::sleep(Duration::from_millis(3000));
        let done = RECEIVED.load(Relaxed);
        complete[i] = done - total_completed;
        let send = SENT.load(Relaxed);
        sent[i] = send - total_sent;

        total_completed = done;
        total_sent = send;
        outstanding[i] = total_sent - total_completed;
    }
    thread::sleep(Duration::from_millis(3000));
    DONE.store(true, Relaxed);
    thread::sleep(Duration::from_millis(3000));

    let avg: usize = complete.iter().map(|&c| c / 3).sum();
    let avg = avg / complete.len();

    println!("> {}: {:6} Hz\n", client_num, avg);
    println!(
        "{client_num}: {:?}\n{client_num}: {:?}\n{client_num} {:?}",
        complete,
        sent,
        outstanding,
        client_num=client_num,
    );
    while STOPPED.load(Relaxed) { thread::yield_now() }
    thread::sleep(Duration::from_millis(30));
}

#[derive(StructOpt, Debug)]
#[structopt(name = "zk_test", about = "")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: ServerAddrs,

    client_num: usize,

    sync_clients: usize,
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
