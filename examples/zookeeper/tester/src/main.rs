extern crate crossbeam;
extern crate zipf;
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

use zipf::ZipfDistribution;

use zookeeper::*;

fn main() {
    let args @ Args{..} = StructOpt::from_args();
    println!("{:?}", args);
    let client_num = args.client_num as u32;
    ::zookeeper::message::set_client_id(client_num + 1);
    let color = (client_num + 1).into();
    let servers = &*args.servers.0;
    let (mut reader, writer) = if servers[0].0 != servers[0].1 {
            LogHandle::<[u8]>::replicated_with_servers(&servers[..])
        } else {
            LogHandle::<[u8]>::unreplicated_with_servers(servers.iter().map(|&(a, _)| a))
        }
        .chains(&[color, 10_001.into()])
        .client_num((client_num as u64 + 1) * 1)
        .reads_my_writes()
        .build_handles();

        let my_dir = client_num;

    let num_prealloc = 0;
    // let num_prealloc = 2_000_000;
    // let mut serialize_cache = vec![];
    // for i in 0..num_prealloc {
    //     let msg = Mutation::Create {
    //         id: Id::new(),
    //         create_mode: CreateMode::persistent(),
    //         path: format!("/foo{}/{}", my_dir, i).into(),
    //         data: vec![],
    //     };
    //     serialize_into(&mut serialize_cache, &msg, Infinite).expect("cannot serialize");
    //     writer.async_append(color, &*serialize_cache, &[]);
    //     serialize_cache.clear();
    // }

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

    let my_root = format!("/foo{}/", my_dir).into();
    let roots = (0..num_clients).map(|i| (format!("foo{}",i).into(), (i as u32 + 1).into()))
        .collect();

    let mut client = Client::new(reader, writer, color, my_root, roots);

    static RECEIVED: AtomicUsize = ATOMIC_USIZE_INIT;
    static COMPLETE_WINDOW: AtomicUsize = ATOMIC_USIZE_INIT;
    static CREATED: AtomicUsize = ATOMIC_USIZE_INIT;
    static SENT: AtomicUsize = ATOMIC_USIZE_INIT;
    static DONE: AtomicBool = ATOMIC_BOOL_INIT;
    static STOPPED: AtomicBool = ATOMIC_BOOL_INIT;

    CREATED.store(num_prealloc, Relaxed);

    while CREATED.load(Relaxed) < num_prealloc {
        thread::yield_now()
    }

    DONE.store(false, Relaxed);
    STOPPED.store(false, Relaxed);

    let window_size = args.window;

    thread::spawn(move || {
        let mut rand = Rand::from_seed([111, 234, client_num, 1010]);
        let mut file_num = 0;
        let mut rename_num = 0;
        let mut window = 0;
        // let rename_dir = my_dir;
        // let rename_dir = if client_num % 2 == 0 {
        //     client_num + 1
        // } else {
        //     client_num - 1
        // };
        let mut dist = ZipfDistribution::new(Rand::from_seed([client_num, 234, client_num, 1010]), num_clients, 1.01).unwrap();
        'work: while !DONE.load(Relaxed) {
            // let mut print_since_last_stall = false;
            'wait: while window - COMPLETE_WINDOW.load(Relaxed) > window_size {
                thread::yield_now();
                if DONE.load(Relaxed) { break 'work }
                // for _ in 0..10_000 {
                //     thread::yield_now();
                //     if DONE.load(Relaxed) { break 'work }
                //     if window - COMPLETE_WINDOW.load(Relaxed) <= 100 { break 'wait }
                // }
                // if !print_since_last_stall {
                //     println!("{:?} stall at f {:?}, r {:?}, w {:?}",
                //         my_dir, file_num, rename_num, window);
                //     print_since_last_stall = true
                // }

            }
            if /*true*/ rand.gen_weighted_bool(1000) && rename_num < CREATED.load(Relaxed) {
                let rename_dir = rand.gen_range(0, num_clients);
                // let rename_dir = (dist.next_u32() + client_num + 1) as usize % num_clients;
                // if rename_dir == my_dir as usize {
                //     rename_dir = (my_dir as usize + 1) % num_clients
                // }
                client.rename(
                    format!("/foo{}/{}", my_dir, rename_num).into(),
                    format!("/foo{}/r{}_{}", rename_dir, client_num, rename_num).into(),
                    Box::new(|res| {
                        match res {
                            Ok(_) => (),
                            Err(e) => panic!("Rename error!? {:?}", e),

                        }
                        RECEIVED.fetch_add(1, Relaxed);
                        COMPLETE_WINDOW.fetch_add(10, Relaxed);
                    }),
                );
                rename_num += 1;
                window += 10;
            } else {
                client.create(
                    format!("/foo{}/{}", my_dir, file_num).into(),
                    (&b"AAA"[..]).into(),
                    CreateMode::persistent(),
                    Box::new(|res| {
                        match res {
                            Ok(_) => {
                                CREATED.fetch_add(1, Relaxed);
                            },
                            Err(e) => panic!("Create error!? {:?}", e),
                        };
                        RECEIVED.fetch_add(1, Relaxed);
                        COMPLETE_WINDOW.fetch_add(1, Relaxed);
                    }),
                );
                file_num += 1;
                window += 1;
            }
            SENT.fetch_add(1, Relaxed);
        }
        STOPPED.store(true, Relaxed);
        return
    });

    let num_rounds = 10;
    let mut total_completed = 0;
    let mut total_sent = 0;
    let mut total_created = 0;
    let mut complete = vec![0; num_rounds];
    let mut sent = vec![0; num_rounds];
    let mut outstanding = vec![0; num_rounds];
    let mut created = vec![0; num_rounds];
    for i in 0..num_rounds {
        thread::sleep(Duration::from_secs(3));
        let done = RECEIVED.load(Relaxed);
        complete[i] = done - total_completed;
        let send = SENT.load(Relaxed);
        sent[i] = send - total_sent;
        let create = CREATED.load(Relaxed);
        created[i] = create - total_created;

        total_completed = done;
        total_sent = send;
        total_created = create;

        outstanding[i] = total_sent - total_completed;
    }
    thread::sleep(Duration::from_secs(3));
    DONE.store(true, Relaxed);
    thread::sleep(Duration::from_secs(3));

    let avg: usize = complete.iter().map(|&c| c / 3).sum();
    let avg = avg / (complete.len() - 1);

    println!("> {}: {:6} Hz\n", client_num, avg);
    println!(
        "{client_num}: complete {:?}\n\
        {client_num}: sent {:?}\n\
        {client_num}: outstanding {:?}\n\
        {client_num}: created {:?}\n",
        complete,
        sent,
        outstanding,
        created,
        client_num=client_num,
    );
    while !STOPPED.load(Relaxed) { thread::yield_now() }
    thread::sleep(Duration::from_millis(30));
}

// fn run(client_num: u32, num_clients: u32, servers: ServerAddrs) {

// }

#[derive(StructOpt, Debug)]
#[structopt(name = "zk_test", about = "")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: ServerAddrs,

    client_num: usize,

    sync_clients: usize,

    window: usize
}

#[derive(Debug, Clone)]
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
