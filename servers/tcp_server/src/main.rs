
#[macro_use]
extern crate log;

extern crate env_logger;
extern crate fuzzy_log;
extern crate mio;

use std::env;

use fuzzy_log::servers::tcp::Server;

use mio::prelude::*;

pub fn main() {
    let _ = env_logger::init();
    if env::args().len() < 2 {
        println!("usage\n\tdelos_tcp_server <port> [<server num> <total servers>]");
        return
    }
    let mut args = env::args().skip(1);
    let port_num = args.next().unwrap();
    let addr = ("0.0.0.0:".to_owned() + &port_num).parse()
        .expect("Invalid port number.");
    let (server_num, total_servers) = if let Some(n) = args.next() {
        let n = n.parse().expect("Invalid server number.");
        let t = args.next()
            .expect("If a server number is given the total number of servers is also required.")
            .parse().expect("Invalid total servers.");
        (n, t)
    } else {
        (0, 1)
    };
    let mut event_loop = EventLoop::new().unwrap();
    let mut server = Server::new(&addr, server_num, total_servers, &mut event_loop).unwrap();
    trace!("starting server");
    let _ = event_loop.run(&mut server);
}
