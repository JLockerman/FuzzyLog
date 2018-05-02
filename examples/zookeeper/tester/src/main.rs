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

// use zipf::ZipfDistribution;

use zookeeper::*;

fn main() {
    let args @ Args{..} = StructOpt::from_args();
    println!("{:?}", args);
    let client_num = args.client_num as u32;
    ::zookeeper::message::set_client_id(client_num + 1);
    let color = if args.one_chain {
        (client_num % 2 + 1).into()
    } else {
        (client_num + 1).into()
    };
    // let color = (client_num + 1).into();
    let servers = &*args.servers.0;
    let replicated = servers[0].0 != servers[0].1;
    let balance_num = if servers.len() > 1 {
        let numbers = [1, 8, 2, 9, 3, 10, 4, 11, 5, 12, 6, 13, 7, 14];
        println!("client_num {:?}, balance_num {:?}, color {:?}", client_num, numbers[client_num as usize], color);
        numbers[client_num as usize]
    } else {
        (client_num as u64 + 1) * 1
    };
    let (mut reader, writer) = if replicated {
            // LogHandle::<[u8]>::replicated_with_servers(&servers[..])
            // if servers.len() > 1 {
            //     if u32::from(color) % 2 == 0 {
            //         LogHandle::<[u8]>::replicated_with_servers(&servers[0..1])
            //     } else {
            //         LogHandle::<[u8]>::replicated_with_servers(&servers[1..2])
            //     }
            // } else {
            //     LogHandle::<[u8]>::replicated_with_servers(&servers[..])
            // }
            LogHandle::<[u8]>::replicated_with_servers(&servers[..])

        } else {
            println!("unreplicated!!");
            LogHandle::<[u8]>::unreplicated_with_servers(servers.iter().map(|&(a, _)| a))
        }
        .chains(&[color, 10_001.into()])
        .client_num(balance_num)
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
    let roots = (0..num_clients).map(|i| (format!("foo{}",i).into(), (i as u32 % 2 + 1).into()))
        .collect();
    // let roots = (0..num_clients).map(|i| (format!("foo{}",i).into(), (i as u32 + 1).into()))
    //     .collect();

    let mut client = Client::new(reader, writer, color, my_root, roots);

    static RECEIVED: AtomicUsize = ATOMIC_USIZE_INIT;
    static COMPLETE_WINDOW: AtomicUsize = ATOMIC_USIZE_INIT;
    static CREATED: AtomicUsize = ATOMIC_USIZE_INIT;
    static SENT: AtomicUsize = ATOMIC_USIZE_INIT;
    static DONE: AtomicBool = ATOMIC_BOOL_INIT;
    static STOPPED: AtomicBool = ATOMIC_BOOL_INIT;

    static SEEDS: &[[u32; 4]] = &[
        [3454879434, 2514150530, 2307584831, 3259277550],
        [2675717231, 299990257, 193027058, 1682055575],
        [788589415, 2003917122, 2795537290, 4104195951],
        [596554834, 2952690321, 2189744100, 3027160689],
        [1334634195, 1026708322, 3011024850, 2756417011],
        [18275768, 2614899434, 2088480912, 853841533],
        [3358821467, 1283043092, 1353023546, 832266079],
        [1309154875, 3426142528, 313832552, 1805116763],
        [2851648276, 4056285604, 2631203974, 1623572954],
        [1476563191, 4003711912, 73809950, 2924249062],
        [3235329122, 3299649619, 607456431, 3418298942],
        [1020519273, 2806165509, 557625698, 2476164065],
        [117057784, 617664764, 2594871634, 4199852127],
        [527079516, 3447830561, 3249831678, 3677883057],
        [1455014137, 41605930, 4204995745, 3904995018],
        [3178902777, 366272502, 3040677457, 998192506],
        [57103185, 1151241394, 3820680740, 3235644221],
        [4251192596, 1647543665, 2564031356, 815853107],
        [3098893464, 2575669856, 1344303158, 3402402061],
        [293661294, 2712744277, 1327105791, 2664210530],
    ];

    CREATED.store(num_prealloc, Relaxed);

    while CREATED.load(Relaxed) < num_prealloc {
        thread::yield_now()
    }

    DONE.store(false, Relaxed);
    STOPPED.store(false, Relaxed);

    let window_size = args.window;

    let one_in = args.one_in.unwrap_or(0);

    let do_transactions = args.one_in.is_some();

    thread::spawn(move || {
        let mut rand = Rand::from_seed([111, 234, client_num, 1010]);
        // let mut rand = Rand::from_seed(SEEDS[client_num as usize]);
        let mut file_num = 0;
        let mut rename_num = 0;
        let mut window = 0;
        // let rename_dir = my_dir;
        // let rename_dir = if client_num % 2 == 0 {
        //     client_num + 1
        // } else {
        //     client_num - 1
        // };
        // let mut dist = ZipfDistribution::new(Rand::from_seed([client_num, 234, client_num, 1010]), num_clients, 1.01).unwrap();
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
            if /*true*/ /*client_num % 2 == 0 &&*/ do_transactions &&
                rand.gen_weighted_bool(one_in as _) && rename_num < CREATED.load(Relaxed) {
                // let rename_dir = client_num + 1;
                let mut rename_dir = rand.gen_range(0, num_clients);
                // let rename_dir = (dist.next_u32() + client_num + 1) as usize % num_clients;
                //FIXME
                while rename_dir == my_dir as usize {
                    rename_dir = rand.gen_range(0, num_clients)//(my_dir as usize + 1) % num_clients
                }
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

    // complete.remove(0);
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

    window: usize,

    #[structopt(short = "t", long = "transactions")]
    one_in: Option<usize>,

    #[structopt(short = "o", long = "one_chain")]
    one_chain: bool,
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
