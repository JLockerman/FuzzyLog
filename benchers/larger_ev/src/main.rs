extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log_client;
extern crate fuzzy_log_server;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::{Duration, Instant};

use fuzzy_log_client::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log_client::packets::{entry, order, OrderIndex};

use fuzzy_log_server::tcp::run_server;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "backpointer", about = "backpointer vs forward benchmark.")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: SocketAddr,

    #[structopt(short="r", long="rounds", help = "number of rounds.", default_value = "10000")]
    num_rounds: usize,
}

fn main() {
    const NUM_EVENTS: usize = 100_000;
    // const NUM_EVENTS: usize = 10;
    let start_time = Instant::now();
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();

    let addr = args.servers;
    ::std::thread::spawn(move || {
        let s = ::std::sync::atomic::AtomicUsize::new(0);
        run_server(addr, 0, 1, None, None, 1, &s)
    });

    ::std::thread::yield_now();

    let handle = LogHandle::unreplicated_with_servers(&[args.servers])
        .chains(&[order::from(1), order::from(2)])
        .reads_my_writes()
        .build();

    let (mut reader, mut writer) = handle.split();
    let test_start = Instant::now();
    crossbeam::scope(|scope| {
        let sender = scope.spawn(|| {
            let data: Vec<u8> = (0..461u32).map(|i| i as u8).collect();
            for _ in 0..NUM_EVENTS {
                writer.async_append(order::from(1), &data[..], &[]);
                let _ = writer.flush_completed_appends();
            }
            let _ = writer.wait_for_all_appends();
        });


        let mut gotten = 0;
        while gotten < NUM_EVENTS {
            // reader.snapshot(order::from(1));
            reader.take_snapshot();
            while let Ok(..) = reader.get_next() {
                gotten += 1;
            }
        }
        sender.join();

    });
    let elapsed = test_start.elapsed();
    println!("got {} events in {:?}, {}Hz",
        NUM_EVENTS,
        elapsed,
        1_000_000_000.0 * NUM_EVENTS as f64 / elapsed.subsec_nanos() as f64
    );
    println!("TEST Time {:?}", start_time.elapsed());
    ::std::thread::yield_now();
}
