
#[macro_use]
extern crate log;

extern crate env_logger;
extern crate num_cpus;
extern crate fuzzy_log;
extern crate mio;

use std::env;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use std::sync::atomic::AtomicUsize;

use fuzzy_log::servers2;

pub fn main() {
    let _ = env_logger::init();
    let Args {port_number, group, num_worker_threads, upstream, downstream}
        = parse_args();
    let ip_addr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
    let addr = SocketAddr::new(ip_addr, port_number);
    let (server_num, group_size) = match group {
        Group::Singleton | Group::LockServer => (0, 1),
        Group::InGroup(server_num, group_size) => (server_num, group_size),
    };
    let acceptor = mio::tcp::TcpListener::bind(&addr);
    let a = AtomicUsize::new(0);
    let replicated = upstream.is_some() || downstream.is_some();
    let print_start = |addr| match group {
        Group::Singleton =>
            println!("Starting singleton server at {} with {} worker threads",
                addr, num_worker_threads),
        Group::LockServer =>
            println!("Starting lock server at {} with {} worker threads",
                addr, num_worker_threads),
        Group::InGroup(..) =>
            println!("Starting server {} out of {} at {} with {} worker threads",
                server_num, group_size, addr, num_worker_threads),
    };
    match acceptor {
        Ok(accept) => {
            let addr = accept.local_addr().unwrap();
            print_start(addr);
            if replicated {
                println!("upstream {:?}, downstream {:?}", upstream, downstream);
                servers2::tcp::run_with_replication(accept, server_num, group_size,
                    upstream, downstream, num_worker_threads, &a)
            }
            else {
                servers2::tcp::run(accept, server_num, group_size, num_worker_threads, &a)
            }
        }
        Err(e) => {
            error!("Could not start server due to {}.", e);
            std::process::exit(1)
        }
    }
    /*match (acceptor, group) {
        (Ok(accept), Group::Singleton) => {
            let addr = accept.local_addr().unwrap();
            println!("Starting singleton server at {} with {} worker threads",
                addr, num_worker_threads);
            servers2::tcp::run(accept, 0, 1, num_worker_threads, &a)
        }
        (Ok(accept), Group::LockServer) => {
            let addr = accept.local_addr().unwrap();
            println!("Starting lock server at {} with {} worker threads",
                addr, num_worker_threads);
            servers2::tcp::run(accept, 0, 1, num_worker_threads, &a)
        }
        (Ok(accept), Group::InGroup(server_num, group_size)) => {
            let addr = accept.local_addr().unwrap();
            println!("Starting server {} out of {} at {} with {} worker threads",
                server_num, group_size, addr, num_worker_threads);
            servers2::tcp::run(accept, server_num, group_size, num_worker_threads, &a)
        }
        (Err(e), _) => {
            error!("Could not start server due to {}.", e);
            std::process::exit(1)
        }
    }*/
}

const USAGE: &'static str =
"Usage:
\ttcp_server <port number> [-w | --workers <num worker threads>]
\ttcp_server (-ls | --lock-server) [-w | --workers <num worker threads>]
\ttcp_server (-ig | --in-group <server num>:<num servers in group>) [--workers <num worker threads>]

can also be run with 'cargo run --release -- <args>...'";

struct Args {
    port_number: u16,
    group: Group,
    num_worker_threads: usize,
    upstream: Option<SocketAddr>,
    downstream: Option<IpAddr>,
}

#[derive(PartialEq, Eq)]
enum Group {
    LockServer,
    Singleton,
    InGroup(u32, u32),
}

enum Flag {
    None,
    Workers,
    InGroup,
    Upstream,
    Downstream,
}

fn parse_args() -> Args {
    let env_args = env::args();
    if env_args.len() < 2 {
        println!("{}", USAGE);
        std::process::exit(1)
    }
    let mut args = Args {
        port_number: 0,
        group: Group::Singleton,
        num_worker_threads: num_cpus::get() - 2,
        upstream: None,
        downstream: None,
    };
    let mut last_flag = Flag::None;
    for arg in env_args.skip(1) {
        match last_flag {
            Flag::None => {
                match &*arg {
                    "-w" | "--workers" => last_flag = Flag::Workers,
                    "-ig" | "--in-group" => {
                        if args.group != Group::Singleton {
                            error!("A server cannot both be in a group and a lock server.");
                            std::process::exit(1)
                        }
                        last_flag = Flag::InGroup
                    }
                    "-ls" | "--lock-server" => {
                        if args.group != Group::Singleton {
                            error!("A server cannot both be in a group and a lock server.");
                            std::process::exit(1)
                        }
                        args.group = Group::LockServer
                    }
                    "-up" | "--upstream" => {
                        last_flag = Flag::Upstream
                    }
                    "-dwn" | "--downstream" => {
                        last_flag = Flag::Downstream
                    }
                    port => {
                        match port.parse() {
                            Ok(port) => args.port_number = port,
                            Err(e) => {
                                error!("Invalid flag: {}.", port);
                                debug!("caused by {}", e);
                                std::process::exit(1)
                            }
                        }
                    }
                }
            }
            Flag::Workers => {
                match arg.parse() {
                    Ok(num_workers) => {
                        if num_workers == 0 {
                            println!("WARNING: Number of worker threads must be non-zero, will default to 1");
                            args.num_worker_threads = 1
                        }
                        else {
                            args.num_worker_threads = num_workers
                        }
                        last_flag = Flag::None
                    }
                    Err(e) => {
                        error!("Invalid <num worker threads> at '--workers': {}.", e);
                        std::process::exit(1)
                    }
                }
            }
            Flag::Upstream => {
                match arg.parse() {
                    Ok(addr) => {
                        args.upstream = Some(addr)
                    }
                    Err(e) => {
                        error!("Invalid <upstream addr> at '--upstream': {}.", e);
                    }
                }
                last_flag = Flag::None;
            }
            Flag::Downstream => {
                match arg.parse() {
                    Ok(addr) => {
                        args.downstream = Some(addr)
                    }
                    Err(e) => {
                        error!("Invalid <downstream addr> at '--downstream': {}.", e);
                    }
                }
                last_flag = Flag::None;
            }
            Flag::InGroup => {
                let split: Vec<_> = arg.split(':').collect();
                if split.len() != 2 {
                    error!("Invalid '--in-group {}': must be in the form of '--in-group <server num>:<num servers in group>'.", arg);
                    std::process::exit(1)
                }
                match (split[0].parse(), split[1].parse()) {
                    (Ok(server_num), Ok(group_size)) => {
                        if group_size <= server_num {
                            error!("<server num>: {} must be less than <num servers in group>: {} in '--in-group'",
                            split[0], split[1]);
                            std::process::exit(1)
                        }
                        last_flag = Flag::None;
                        args.group = Group::InGroup(server_num, group_size)
                    }
                    (Err(e1), Err(e2)) => {
                        error!("Invalid <server num>: {} '{}' at '--in-group'", e1, split[0]);
                        error!("Invalid <num servers in group>: {} '{}' at '--in-group'",
                            e2, split[1]);
                        std::process::exit(1)
                    }
                    (Err(e1), _) => {
                        error!("Invalid <server num>: {} '{}' at '--in-group'", e1, split[0]);
                        std::process::exit(1)
                    }
                    (_, Err(e2)) => {
                        error!("Invalid <num servers in group>: {} '{}' at '--in-group'",
                            e2, split[1]);
                        std::process::exit(1)
                    }
                }
            }
        }
    }
    match last_flag {
        Flag::None => args,
        Flag::InGroup => {
            error!("Missing <server num>:<num servers in group> for '--in-group'");
            std::process::exit(1)
        },
        Flag::Workers => {
            error!("Missing <num worker threads> for '--workers'");
            std::process::exit(1)
        }
        Flag::Downstream => {
            error!("Missing <downstream addr> for '--downstream'");
            std::process::exit(1)
        }
        Flag::Upstream => {
            error!("Missing <upstream addr> for '--upstream'");
            std::process::exit(1)
        }
    }

}
