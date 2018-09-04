// Test code for the map example.

extern crate fuzzy_log_server;

use std::{
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT},
    thread,
};

use ::*;

use tests::fuzzy_log_server::tcp::run_server;

#[test]
fn one_column_unshared_map() {
    static STARTED: AtomicUsize = ATOMIC_USIZE_INIT;
    thread::spawn(|| {
        print!("starting server...");
        run_server(
            "127.0.0.1:8223".parse().unwrap(), 0, 1, None, None, 1, &STARTED)
    });

    while STARTED.load(Ordering::Relaxed) == 0 {
        thread::yield_now()
    }

    println!(" done.");

    println!("Running a single column Map");
    let addr: SocketAddr = "127.0.0.1:8223".parse().unwrap();
    let handle = LogHandle::unreplicated_with_servers(Some(addr))
        .my_colors_chains(Some(1.into()))
        .reads_my_writes()
        .build();
    let mut map: Map<_, _> = (handle, order::from(1)).into();

    println!("puting {:?} => {:?}", 333, 124);
    map.put(&333, &124);

    println!("puting {:?} => {:?}", 3, 42);
    map.put(&3, &42);

    println!("puting {:?} => {:?}", 651, 0);
    map.put(&651, &0);

    println!("puting {:?} => {:?}", 1, 2);
    map.put(&1, &2);

    println!("cached value for {:3?} is {:?}", 333, map.get_cached(&333));
    assert_eq!(map.get_cached(&333), None);
    println!("cached value for {:3?} is {:?}", 3, map.get_cached(&3));
    assert_eq!(map.get_cached(&3), None);
    println!("cached value for {:3?} is {:?}", 651, map.get_cached(&651));
    assert_eq!(map.get_cached(&651), None);
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1));
    assert_eq!(map.get_cached(&1), None);
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1000));
    assert_eq!(map.get_cached(&1000), None);

    println!("reading {:?}", 651);
    assert_eq!(map.get(&651), Some(&mut 0));

    println!("cached value for {:3?} is {:?}", 333, map.get_cached(&333));
    assert_eq!(map.get_cached(&333), Some(&mut 124));
    println!("cached value for {:3?} is {:?}", 3, map.get_cached(&3));
    assert_eq!(map.get_cached(&3), Some(&mut 42));
    println!("cached value for {:3?} is {:?}", 651, map.get_cached(&651));
    assert_eq!(map.get_cached(&651), Some(&mut 0));
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1));
    assert_eq!(map.get_cached(&1), Some(&mut 2));
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1000));
    assert_eq!(map.get_cached(&1000), None);
}
