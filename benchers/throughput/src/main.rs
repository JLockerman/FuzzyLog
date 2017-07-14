
#[macro_use] extern crate clap;
#[macro_use] extern crate log;

extern crate fuzzy_log;
extern crate env_logger;
extern crate mio;
extern crate num_cpus;
extern crate rand;

use std::net::{IpAddr, SocketAddr};

mod servers;
mod workloads;

macro_rules! invalid_value {
    ($val:expr, $arg_name:expr) => (
        clap::Error::with_description(
            &*format!("The argument '{:?}' isn't a valid value for {}", $val, $arg_name),
            clap::ErrorKind::InvalidValue,
        )
    );
}

macro_rules! value_or {
    ($m:ident, $v:expr, $t:ty, $e:expr) => (
        if let Some(v) = $m.value_of($v) {
            match v.parse::<$t>() {
                Ok(val) => Ok(val),
                Err(_)  => Err(invalid_value!(v, $v)),
            }
        } else {
            Ok($e)
        }
    );

    (let $v:ident; $m:ident, $t:ty, $e:expr) => (
        let $v =
            match $m.value_of(stringify!($v)) {
                Some(v) => match v.parse::<$t>() {
                    Ok(val) => val,
                    Err(_)  => invalid_value!(v, stringify!($v)).exit(),
                },
                None => $e,
            };
    );
}

macro_rules! value_if {
    (let $v:ident; $m:ident, $t:ty) => (
        let $v =
            match $m.value_of(stringify!($v)) {
                Some(v) => match v.parse::<$t>() {
                    Ok(val) => Some(val),
                    Err(_)  => invalid_value!(v, stringify!($v)).exit(),
                },
                None => None,
            };
    );
}

fn main() {
    let _ = env_logger::init();

    let app = clap_app!(myapp =>
        (@subcommand uwr =>
            (about: "unreplicated read/write throughput workload.")
            (@arg servers: -s ... +required +takes_value "Server addrs.")
            (@arg clients: -n --num_clients +takes_value "Number of clients to run.")
            (@arg total_clients: -t --total_clients +takes_value requires[clients]
               "Number of clients involved in the experiment.")
            (@arg jobsize: -j --jobsize  +takes_value "Number of bytes per entry.")
            (@arg num_writes: -w --num_writes +takes_value "Number writes tp perform.")
            (@arg random_colors: -r --random_colors
               "If set clients will randomly spray their writes across total-clients colors, otherwise each client will write to its own color.")
            (@arg random_seed: -e --radom_seed +takes_value requires[random_colors]
                "Sets the random seed random color selection uses.")
            (@arg write_window: -i --write_window +takes_value
                "Window size for writes.")
        )

        (@subcommand rwr =>
            (about: "replicated read/write throughput workload.")
            (@arg servers: -s ... +required +takes_value "<chain head addr>-<chain-tailaddr> tuples.")
            (@arg clients: -n --num_clients +takes_value "Number of clients to run.")
            (@arg total_clients: -t --total_clients +takes_value requires[clients]
               "Number of clients involved in the experiment.")
            (@arg jobsize: -j --jobsize  +takes_value "Number of bytes per entry.")
            (@arg num_writes: -w --num_writes +takes_value "Number writes to perform.")
            (@arg random_colors: -r --random_colors
               "If set clients will randomly spray their writes across total-clients colors, otherwise each client will write to its own color.")
            (@arg random_seed: -e --radom_seed +takes_value requires[random_colors]
                "Sets the random seed random color selection uses.")
            (@arg write_window: -i --write_window +takes_value
                "Window size for writes.")
        )

        (@subcommand dc =>
            (about: "dependent multiappend cost evaluation.")
            (@arg server: -s +required +takes_value "Server addr.")
            (@arg jobsize: -j --jobsize  +takes_value "Number of bytes per entry.")
            (@arg num_writes: -w --num_writes +takes_value "Number writes tp perform.")
            (@arg non_dep_portion: -d +takes_value "1/d writes will be non-dependent, default 2")
            (@arg random_seed: -e --radom_seed +takes_value "Sets the random seed.")
        )

        (@subcommand server =>
            (about: "run a fuzzy log server.")
            (@arg port: +required +takes_value "Port the server should listen on.")
            (@arg lock_server: -l --lock_server conflicts_with[group]
                "Run the server as a lock server.")
            (@arg group: -g --in_group +takes_value
                "<server num>:<num servers in group> Run this server as part of a group of servers.")
            (@arg upstream: -u --upstream +takes_value
                "Address and port of the server that comes before this one in it's replication chain.")
            (@arg downstream: -d --downstream +takes_value
                "Address and port of the server that comes after this one in it's replication chain.")
            (@arg workers: -w --workers +takes_value
                "Number of worker threads this server should use (default is <number of cores> - 2).")
        )
    );


    let mut help = Vec::new();
    let _ = app.write_help(&mut help);
    let args = app.get_matches();

    let (subcommand, args) = match args.subcommand() {
        (_, None) => {
            println!("{}", unsafe { String::from_utf8_unchecked(help) });
            clap::Error::with_description(
                "Must input subcommand.",
                clap::ErrorKind::MissingRequiredArgument
            ).exit()
        },
        (subcommand, Some(args)) => (subcommand, args),
    };

    match subcommand {
        "uwr" => {
            let server_addrs = values_t!(args, "servers", SocketAddr)
                .unwrap_or_else(|e| e.exit());
            let clients_to_run = value_or!(args, "clients", usize, 1)
                .unwrap_or_else(|e| e.exit());
            let total_clients = value_or!(args, "total_clients", usize, clients_to_run)
                .unwrap_or_else(|e| e.exit());
            let jobsize = value_or!(args, "jobsize", usize, 1)
                .unwrap_or_else(|e| e.exit());
            let num_writes = value_or!(args, "num_writes", u32, 100000)
                .unwrap_or_else(|e| e.exit());
            let random_colors = args.is_present("random_colors");
            let random_seed = if random_colors {
                let mut seed = [0x193a6754, 0xa8a7d469, 0x97830e05, 0x113ba7bb];
                if let Some(v) = args.values_of("random_seed") {
                    seed.iter_mut().zip(v).fold((), |_, (s, v)|
                        *s = v.parse().unwrap_or_else(|_|
                            invalid_value!(v, "random_seed").exit()))
                };
                Some(seed)
            } else { None };
            value_or!(let write_window; args, u32, num_writes);
            drop(help);
            workloads::run_unreplicated_write_read(
                server_addrs.into_boxed_slice(),
                clients_to_run,
                total_clients,
                jobsize,
                num_writes,
                write_window,
                random_seed,
            )
        },

        ///////////////////////////////////////

        "rwr" => {
            let server_addrs = args.values_of("servers").map(|v| v.map(|addr_pair| {
                let mut addr_pair = addr_pair.split('-');
                let chain_head = addr_pair.next();
                let chain_head = chain_head.and_then(|a| a.parse().ok())
                    .unwrap_or_else(|| invalid_value!(chain_head, "servers").exit());
                let chain_tail = addr_pair.next();
                let chain_tail = chain_tail.and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| invalid_value!(chain_tail, "servers").exit());
                (chain_head, chain_tail)
            }).collect()).unwrap();
            println!("servers: {:?}", server_addrs);
            let clients_to_run = value_or!(args, "clients", usize, 1)
                .unwrap_or_else(|e| e.exit());
            let total_clients = value_or!(args, "total_clients", usize, clients_to_run)
                .unwrap_or_else(|e| e.exit());
            let jobsize = value_or!(args, "jobsize", usize, 1)
                .unwrap_or_else(|e| e.exit());
            let num_writes = value_or!(args, "num_writes", u32, 100000)
                .unwrap_or_else(|e| e.exit());
            let random_colors = args.is_present("random_colors");
            let random_seed = if random_colors {
                let mut seed = [0x193a6754, 0xa8a7d469, 0x97830e05, 0x113ba7bb];
                if let Some(v) = args.values_of("random_seed") {
                    seed.iter_mut().zip(v).fold((), |_, (s, v)|
                        *s = v.parse().unwrap_or_else(|_|
                            invalid_value!(v, "random_seed").exit()))
                };
                Some(seed)
            } else { None };
            value_or!(let write_window; args, u32, num_writes);
            drop(help);
            workloads::run_replicated_write_read(
                None,
                server_addrs,
                clients_to_run,
                total_clients,
                jobsize,
                num_writes,
                write_window,
                random_seed,
            )
        },

        ///////////////////////////////////////

        "dc" => {
            value_if!(let server; args, SocketAddr);
            let server = server.unwrap_or_else(|| invalid_value!("", "server").exit());
            value_or!(let jobsize; args, usize, 1);
            value_or!(let num_writes; args, u32, 100_000);
            value_or!(let non_dep_portion; args, u32, 2);
            let mut random_seed = None;
            if args.is_present("random_seed") {
                let mut seed = [0x193a6754, 0xa8a7d469, 0x97830e05, 0x113ba7bb];
                if let Some(v) = args.values_of("random_seed") {
                    seed.iter_mut().zip(v).fold((), |_, (s, v)|
                        *s = v.parse().unwrap_or_else(|_|
                            invalid_value!(v, "random_seed").exit()))
                };
                random_seed = Some(seed)
            }
            drop(help);
            workloads::dependent_cost(
                server,
                jobsize,
                num_writes,
                non_dep_portion,
                random_seed,
            )
        }

        ///////////////////////////////////////

        "server" => {
            let port = value_t!(args, "port", u16).unwrap_or_else(|e| e.exit());
            //FIXME max 1
            value_or!(let workers; args, usize, num_cpus::get() - 2);
            value_if!(let upstream; args, SocketAddr);
            value_if!(let downstream; args, IpAddr);
            let group = args.value_of("group").map(|a| {
                let mut a = a.split(':');
                let next = a.next();
                let server_num = next.and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| invalid_value!(next, "group").exit());
                let next = a.next();
                let group_size = next.and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| invalid_value!(next, "group").exit());
                (server_num, group_size)
            });

            servers::run(port, workers, upstream, downstream, group)
        }

        ///////////////////////////////////////

        cmd => {
            //technically unreachable
            println!("{}", unsafe { String::from_utf8_unchecked(help) });
            clap::Error::with_description(
                &*format!("Unknown subcommand: {}.", cmd),
                clap::ErrorKind::UnknownArgument
            ).exit()
        }
    }
}


