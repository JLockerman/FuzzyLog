
#[macro_use]
extern crate log;

extern crate env_logger;
extern crate fuzzy_log;
extern crate mio;

use std::env;

use fuzzy_log::servers::tcp::Server;

use mio::deprecated::EventLoop;

const ADDR_STR: &'static str = "0.0.0.0:13265";

pub fn main() {
    let _ = env_logger::init();
    let addr = match env::args().skip(1).next() {
        Some(ref s) => ("0.0.0.0:".to_owned() + &*s).parse().expect("invalid inet address"),
        _ => ADDR_STR.parse().expect("invalid default inet address"),
    };
    let mut event_loop = EventLoop::new().unwrap();
    let mut server = Server::new(&addr, 0, 1, &mut event_loop).unwrap();
    trace!("starting server");
    let _ = event_loop.run(&mut server);
}
