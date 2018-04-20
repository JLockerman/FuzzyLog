pub extern crate bincode;
// extern crate mio;
// extern crate reactor;
pub extern crate serde;
pub extern crate zookeeper;

#[macro_use] extern crate serde_derive;

use std::net::SocketAddr;
use std::str::FromStr;

mod msg;

pub use msg::*;

#[derive(Debug, Clone)]
pub struct ServerAddrs(pub Vec<(SocketAddr, SocketAddr)>);

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

impl From<Vec<(SocketAddr, SocketAddr)>> for ServerAddrs {
    fn from(v: Vec<(SocketAddr, SocketAddr)>) -> Self {
        ServerAddrs(v)
    }
}
