
use std::thread;

use packets::*;
use async::fuzzy_log::*;

use fuzzy_log_client::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log_client::replicator::Replicator;

const ADDR_STR1: &'static str = "127.0.0.1:13990";
const ADDR_STR2: &'static str = "127.0.0.1:13991";

#[test]
fn basic() {
    //chains 1
    let chains: Vec<_> = (0..1).map(|i| order::from(i + 1)).collect();
    let mut remote = remote_log_handle(chains.clone());
    let mut local = local_log_handle(chains);
    remote.append(1.into(), &[1, 1, 1], &[]);
    remote.append(1.into(), &[1, 1, 2], &[]);
    remote.append(1.into(), &[1, 1, 3], &[]);
    remote.append(1.into(), &[1, 1, 4], &[]);

    let mut last = 0;
    while last < 4 {
        local.snapshot(1.into());
        loop {
            let next = local.get_next();
            match next {
                Ok((val, locs)) => {
                    assert_eq!(locs.len(), 1);
                    assert_eq!(locs[0], (1, (last + 1)).into());
                    assert_eq!(val, &[1, 1, (last + 1) as u8][..]);
                    last += 1;
                }
                Err(GetRes::Done) => break,
                e @ Err(..) => panic!("{:?}", e),
            }
        }
    }
}

#[test]
fn deps() {
    //chains 2, 3
    let chains: Vec<_> = (2..4).map(|i| order::from(i)).collect();
    let mut remote = remote_log_handle(chains.clone());
    let mut local = local_log_handle(chains);
    remote.append(2.into(), &[1, 2, 1], &[]);
    remote.append(2.into(), &[1, 2, 2], &[]);
    remote.append(2.into(), &[1, 2, 3], &[]);
    remote.append(2.into(), &[1, 2, 4], &[]);
    remote.append(3.into(), &[1, 3, 0], &[(2, 4).into()]);

    let mut last = 0;
    while last < 4 {
        local.snapshot(3.into());
        loop {
            let next = local.get_next();
            match next {
                Ok((val, locs)) => {
                    assert_eq!(locs.len(), 1);
                    assert_eq!(locs[0], (2, (last + 1)).into());
                    assert_eq!(val, &[1, 2, (last + 1) as u8][..]);
                    last += 1;
                    if last >= 4 { break }
                }
                Err(GetRes::Done) => break,
                e @ Err(..) => panic!("{:?}", e),
            }
        }
    }
    let next = local.get_next();
    match next {
        Ok((val, locs)) => {
            assert_eq!(locs.len(), 1);
            assert_eq!(locs[0], (3, 1).into());
            assert_eq!(val, &[1, 3, 0][..]);
        }
        Err(GetRes::Done) => unreachable!(),
        e @ Err(..) => panic!("{:?}", e),
    }
}

#[test]
fn long_deps() {
    //chains 4, 5
    let chains: Vec<_> = (4..6).map(|i| order::from(i)).collect();
    let mut remote = remote_log_handle(chains.clone());
    let mut local = local_log_handle(chains);
    for i in 0..100 {
        if i == 0 {
            remote.append(4.into(), &[i], &[]);
        } else {
            remote.append(4.into(), &[i], &[OrderIndex(5.into(), (i as u32).into())]);
        }
    }

    for i in 0..100 {
        if i == 0 {
            remote.append(5.into(), &[i], &[]);
        } else {
            remote.append(5.into(), &[i], &[OrderIndex(4.into(), (i as u32 + 1).into())]);
        }
    }

    thread::sleep_ms(1000);

    local.snapshot(5.into());
    loop {
        let next = local.get_next();
        match next {
            Ok((val, locs)) => {
                // assert_eq!(locs.len(), 1);
                // assert_eq!(locs[0], (2, (last + 1)).into());
                // assert_eq!(val, &[1, 2, (last + 1) as u8][..]);
                // last += 1;
                // if last >= 4 { break }
            }
            Err(GetRes::Done) => break,
            e @ Err(..) => panic!("{:?}", e),
        }
    }
    // let mut last = 0;
    // while last < 4 {
    //     local.snapshot(3.into());
    //     loop {
    //         let next = local.get_next();
    //         match next {
    //             Ok((val, locs)) => {
    //                 // assert_eq!(locs.len(), 1);
    //                 // assert_eq!(locs[0], (2, (last + 1)).into());
    //                 // assert_eq!(val, &[1, 2, (last + 1) as u8][..]);
    //                 // last += 1;
    //                 // if last >= 4 { break }
    //             }
    //             Err(GetRes::Done) => break,
    //             e @ Err(..) => panic!("{:?}", e),
    //         }
    //     }
    // }
}

fn local_log_handle(chains: Vec<order>) -> LogHandle<[u8]> {
    start_tcp_servers();
    LogHandle::unreplicated_with_servers(Some(&ADDR_STR1.parse().unwrap()))
        .chains(chains)
        .build()
}

fn remote_log_handle(chains: Vec<order>) -> LogHandle<[u8]> {
    start_tcp_servers();
    LogHandle::unreplicated_with_servers(Some(&ADDR_STR2.parse().unwrap()))
        .chains(chains)
        .build()
}

fn start_tcp_servers() {
    use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
    use std::thread;

    use mio;

    static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

    {
        let addr = ADDR_STR1.parse().expect("invalid inet address");
        let acceptor = mio::tcp::TcpListener::bind(&addr);
        if let Ok(acceptor) = acceptor {
            thread::spawn(move || {
                trace!("starting server");
                ::servers2::tcp::run(
                    acceptor, 0, 1, 1, &SERVERS_READY,
                )
            });
        }
        else {
            trace!("server already started");
        }
    }

    {
        let addr = ADDR_STR2.parse().expect("invalid inet address");
        let acceptor = mio::tcp::TcpListener::bind(&addr);
        if let Ok(acceptor) = acceptor {
            thread::spawn(move || {
                trace!("starting server");
                ::servers2::tcp::run(
                    acceptor, 0, 1, 1, &SERVERS_READY,
                )
            });
        }
        else {
            trace!("server already started");
        }
    }

    while SERVERS_READY.load(Ordering::Acquire) < 2 {}

    start_replicator();
}

fn start_replicator() {
    use std::sync::{Once, ONCE_INIT};

    static START_REPLICATOR: Once = ONCE_INIT;

    START_REPLICATOR.call_once(|| {
        thread::spawn(||
            Replicator::with_unreplicated_servers(
                Some(ADDR_STR1.parse().unwrap()),
                Some(ADDR_STR2.parse().unwrap()),
            ).chains((1..101).map(|i| i.into())).build().run()
        );
    });
}
