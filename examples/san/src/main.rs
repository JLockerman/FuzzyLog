#![feature(alloc_system)]

extern crate alloc_system;

extern crate fuzzy_log;

extern crate env_logger;

#[macro_use] extern crate log;

extern crate mio;

use fuzzy_log::packets::*;
use fuzzy_log::async::fuzzy_log::log_handle::LogHandle;

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
use std::{thread, iter};
use std::time::Instant;
use std::sync::mpsc;
use std::mem;
use std::net::{SocketAddr, Ipv4Addr, IpAddr};

use fuzzy_log::async::store::AsyncTcpStore;

fn main() {
    use std::cell::UnsafeCell;
    //test_multi1()
    let cell1 = UnsafeCell::new(NoisyDrop(1));
    let cell2 = UnsafeCell::new(NoisyDrop(387));
    let cell2 = UnsafeCell::new(NoisyDrop(2));
}

struct NoisyDrop(u64);

impl Drop for NoisyDrop {
    fn drop(&mut self) {
        println!("dropping {:?}:{:?}", self as *mut _, self.0);
    }
}

pub fn test_multi1() {
    let _ = env_logger::init();
    trace!("TEST multi");

    let columns = vec![23.into(), 24.into(), 25.into()];
    let mut lh = new_thread_log::<u64>(columns.clone());
    let _ = lh.multiappend(&columns, &0xfeed, &[]);
    let _ = lh.multiappend(&columns, &0xbad , &[]);
    let _ = lh.multiappend(&columns, &0xcad , &[]);
    let _ = lh.multiappend(&columns, &13    , &[]);
    lh.snapshot(24.into());
    assert_eq!(lh.get_next(), Some((&0xfeed, &[OrderIndex(23.into(), 1.into()),
        OrderIndex(24.into(), 1.into()), OrderIndex(25.into(), 1.into())][..])));
    assert_eq!(lh.get_next(), Some((&0xbad , &[OrderIndex(23.into(), 2.into()),
        OrderIndex(24.into(), 2.into()), OrderIndex(25.into(), 2.into())][..])));
    assert_eq!(lh.get_next(), Some((&0xcad , &[OrderIndex(23.into(), 3.into()),
        OrderIndex(24.into(), 3.into()), OrderIndex(25.into(), 3.into())][..])));
    assert_eq!(lh.get_next(), Some((&13    , &[OrderIndex(23.into(), 4.into()),
        OrderIndex(24.into(), 4.into()), OrderIndex(25.into(), 4.into())][..])));
    assert_eq!(lh.get_next(), None);
}

pub fn run_many() {
    let start = Instant::now();
    let _ = env_logger::init();
    //trace!("TEST multi and single");

    let lhs: Vec<_> = (1..11u32).map(|_|
        LogHandle::new_tcp_log(
            [&"172.31.3.64:13289", &"172.31.4.131:13289"].into_iter().map(|s| s.parse().unwrap()),
            (1..11).map(|o| order::from(o)))
    ).collect();

    let h: Vec<_> = lhs.into_iter().enumerate().map(|(i, mut lh)| {
        let i = i as u32;
        thread::spawn(move ||
            for _ in 0..50_000 {
                (1..11u32).filter(|&j| j != i).fold((), |_, j| {
                    lh.multiappend(&[i.into(), j.into()], &60u64, &[]);
                });
            }
        )
    }).collect();

    h.into_iter().fold((), |_, h| { let _ = h.join(); });
    println!("finished in {:?}s", start.elapsed().as_secs());
}

const SERVER1_ADDRS: &'static [&'static str] = &["0.0.0.0:13590", "0.0.0.0:13591"];
const SERVER2_ADDRS: &'static [&'static str] = &["0.0.0.0:13690", "0.0.0.0:13691"];

fn new_thread_log<V>(interesting_chains: Vec<order>) -> LogHandle<V> {
    start_tcp_servers();

    let addrs = iter::once((SERVER1_ADDRS[0], SERVER1_ADDRS[1]))
        .chain(iter::once((SERVER2_ADDRS[0], SERVER2_ADDRS[1])))
        .map(|(s1, s2)| (s1.parse().unwrap(), s2.parse().unwrap()));
    LogHandle::new_tcp_log_with_replication(
        addrs,
        interesting_chains.into_iter(),
    )
}


fn start_tcp_servers()
{
    static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;
    static SERVER_STARTING: AtomicUsize = ATOMIC_USIZE_INIT;

    if SERVER_STARTING.swap(2, Ordering::SeqCst) == 0 {

        let local_host = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        for (i, &addr_str) in SERVER1_ADDRS.into_iter().enumerate() {
            let prev_server: Option<SocketAddr> =
                if i > 0 { Some(SERVER1_ADDRS[i-1]) } else { None }
                .map(|s| s.parse().unwrap());
            let prev_server = prev_server.map(|mut s| {s.set_ip(local_host); s});

            let next_server: Option<SocketAddr> = SERVER1_ADDRS.get(i+1)
                .map(|s| s.parse().unwrap());
            let next_server = next_server.map(|mut s| {s.set_ip(local_host); s});
            let next_server = next_server.map(|s| s.ip());

            let addr = addr_str.parse().expect("invalid inet address");
            let acceptor = mio::tcp::TcpListener::bind(&addr);
            let acceptor = acceptor.unwrap();
            thread::spawn(move || {
                trace!("starting replica server {:?}", (0, i));
                fuzzy_log::servers2::tcp::run_with_replication(
                    acceptor, 0, 2,
                    prev_server,
                    next_server,
                    1, //TODO switch to 2
                    &SERVERS_READY
                )
            });
        }

        for (i, &addr_str) in SERVER2_ADDRS.into_iter().enumerate() {

            let prev_server: Option<SocketAddr> =
                if i > 0 { Some(SERVER2_ADDRS[i-1]) } else { None }
                .map(|s| s.parse().unwrap());
            let prev_server = prev_server.map(|mut s| {s.set_ip(local_host); s});

            let next_server: Option<SocketAddr> = SERVER2_ADDRS.get(i+1)
                .map(|s| s.parse().unwrap());
            let next_server = next_server.map(|mut s| {s.set_ip(local_host); s});
            let next_server = next_server.map(|s| s.ip());

            let addr = addr_str.parse().expect("invalid inet address");
            let acceptor = mio::tcp::TcpListener::bind(&addr);
            let acceptor = acceptor.unwrap();
            thread::spawn(move || {
                trace!("starting replica server {:?}", (1, i));
                fuzzy_log::servers2::tcp::run_with_replication(
                    acceptor, 1, 2,
                    prev_server,
                    next_server,
                    1, //TODO switch to 2
                    &SERVERS_READY
                )
            });
        }
    }

    while SERVERS_READY.load(Ordering::Acquire) < SERVER1_ADDRS.len() + SERVER2_ADDRS.len() {}
}
