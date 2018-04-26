
extern crate mio;
extern crate reactor;
extern crate rand;
extern crate structopt;
pub extern crate serde;
extern crate zk_view as msg;
pub extern crate zookeeper;

#[macro_use] extern crate structopt_derive;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::thread;
use std::time::Duration;

use structopt::StructOpt;

pub use msg::ServerAddrs;
pub use msg::bincode;

use view::View;

pub mod view;
pub mod read_gen;

#[derive(StructOpt, Debug)]
#[structopt(name = "zk_test", about = "")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: ServerAddrs,
    my_port: u16,
    client_num: u32,
    num_clients: u32,
    window: usize,
    views: ServerAddrs,
    num_threads: usize,
    #[structopt(short="o", long="one_chain")]
    one_chain: bool,
    #[structopt(short="t", long="transactions")]
    transactions_one_in: Option<u32>,
}

fn main() {
    let args: Args = StructOpt::from_args();

    zookeeper::message::set_client_id(args.client_num + 1);
    if args.client_num < args.num_clients {
        run_view(args)
    } else {
        run_reads(args)
    }
}

fn run_view(args: Args) {
    let completed_writes = Arc::new(AtomicUsize::new(0));
    let cw = completed_writes.clone();
    let client_num = args.client_num;
    thread::spawn(|| {
        let client_num = args.client_num;
        let color = if args.one_chain {
            (client_num % 2 + 1).into()
        } else {
            (client_num + 1).into()
        };

        let balance_num = if args.one_chain {
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
            } else {
                [1, 8, 2, 9, 3, 10, 4, 11, 5, 12, 6, 13, 7, 14]
            }[client_num as usize];

        let my_dir = client_num;
        let my_root = format!("/foo{}/", my_dir);

        let roots = (0..args.num_clients).map(|i| (format!("foo{}",i).into(), (i as u32 % 2 + 1).into()))
            .collect();
        let view = View::new(
            args.client_num as usize,
            args.num_clients as usize,
            args.transactions_one_in,
            args.window,
            args.my_port,
            args.servers,
            color,
            balance_num,
            my_root,
            roots,
            cw,
        ).expect("cannot start view");
        view.run();
    });

    thread::sleep(Duration::from_secs(2));

    completed_writes.swap(0, Relaxed);
    let mut completed = [0; 10];
    for i in 0..10 {
        thread::sleep(Duration::from_secs(1));
        completed[i] = completed_writes.swap(0, Relaxed);
    }
    thread::sleep(Duration::from_secs(2));

    let total: usize = completed.iter().sum();
    let avg = total / completed.len();

    println!("> w {}: {:6} Hz\n", client_num, avg);
    println!("w {}: complete {:?}", client_num, completed);
}

fn run_reads(args: Args) {
    let num_threads = args.num_threads;

    let view_addr = args.views.0[(args.client_num % args.num_clients) as usize].0;
    let window = args.window / 2;

    let mut ops_completed = vec![[0; 10]; num_threads];
    let completed_reads: Vec<_> = (0..num_threads)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();

    for i in 0..args.num_threads {
        let read = completed_reads[i].clone();
        thread::spawn(move || read_gen::work(view_addr, window, read));
    }

    thread::sleep(Duration::from_secs(1));

    for i in 0..10 {
        thread::sleep(Duration::from_secs(1));
        for j in 0..num_threads {
            ops_completed[j][i] = completed_reads[j].swap(0, Relaxed);
        }
    }

    thread::sleep(Duration::from_secs(1));
    for j in 0..num_threads {
        println!("r {:2>}.{:}: {:?}", args.client_num, j, ops_completed[j]);
        let total_ops: usize = ops_completed[j].iter().sum();
        let ops_s = total_ops / ops_completed[0].len();
        println!("> r {:2>}.{:}: {:?}  Hz", args.client_num, j, ops_s);
    }
}
