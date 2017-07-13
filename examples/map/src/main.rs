//! #Map Example
//!
//! This file contains a simple implementation of a Map Datastructure based on a
//! the fuzzy log.
//!
//!

extern crate fuzzy_log;
use map::Map;

use std::thread;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};

fn main() {
    one_column_unshared_map();
    println!("\n\n                          ///////////////////////////");
    println!("                          ///////////////////////////");
    println!("                          ///////////////////////////\n\n");
    four_column_unshared_map();
}

///////////////////

mod map {
    use std::collections::HashMap;
    use std::fmt::{Debug, Formatter, Result as FmtResult};
    use std::hash::Hash;
    use std::iter;
    use std::net::SocketAddr;
    use std::ops::Range;

    use fuzzy_log::{order, LogHandle};

    #[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
    pub struct MapEntry<K, V>(pub K, pub V);

    pub struct Map<K, V, F>
    where K: Hash + Eq + Copy, V: Copy,
          F: Fn(&K) -> order {
        log: LogHandle<MapEntry<K, V>>,
        local_view: HashMap<K, V>,
        key_to_column: F,
    }

    impl<K: 'static, V: 'static, F>
    Map<K, V, F>
    where K: Hash + Eq + Copy,
          V: Copy,
          F: Fn(&K) -> order, {

        pub fn new(addr: SocketAddr, interesting_columns: Range<u32>, key_to_column: F) -> Self {
            Map {
                log: LogHandle::new_tcp_log(
                    iter::once(addr), interesting_columns.map(order::from),
                ),
                local_view: Default::default(),
                key_to_column: key_to_column
            }
        }

    }

    impl<K: 'static, V: 'static, F> Map<K, V, F>
    where K: Hash + Eq + Copy,
          V: Copy,
          F: Fn(&K) -> order, {

        pub fn put(&mut self, key: K, val: V) {
            self.log.append((self.key_to_column)(&key), &MapEntry(key, val), &[]);
        }

        pub fn get(&mut self, key: K) -> Option<V> {
            self.log.snapshot((self.key_to_column)(&key));
            while let Ok((&MapEntry(k, v), _)) = self.log.get_next() {
                self.local_view.insert(k, v);
            }
            self.local_view.get(&key).cloned()
        }

        pub fn get_cached(&mut self, key: &K) -> Option<V> {
            self.local_view.get(key).cloned()
        }
    }

    impl<K, V, F> Debug for Map<K, V, F>
    where K: Hash + Eq + Copy + Debug, V: Copy + Debug,
          F: Fn(&K) -> order, {

        fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
            formatter.debug_struct("Map").field("local_view", &self.local_view).finish()
        }
    }
}


///////////////////


fn one_column_unshared_map() {
    static STARTED: AtomicUsize = ATOMIC_USIZE_INIT;
    thread::spawn(|| {
        print!("starting server...");
        fuzzy_log::run_server(
            "127.0.0.1:8223".parse().unwrap(), 0, 1, None, None, 1, &STARTED)
    });

    while STARTED.load(Ordering::Relaxed) == 0 {
        thread::yield_now()
    }

    println!(" done.");

    println!("Running a single column Map");
    let mut map = Map::new("127.0.0.1:8223".parse().unwrap(), 1..2, |_: &u32| 1.into());

    println!("puting {:?} => {:?}", 333, 124);
    map.put(333, 124);

    println!("puting {:?} => {:?}", 3, 42);
    map.put(3, 42);

    println!("puting {:?} => {:?}", 651, 0);
    map.put(651, 0);

    println!("puting {:?} => {:?}", 1, 2);
    map.put(1, 2);

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
    assert_eq!(map.get(651), Some(0));

    println!("cached value for {:3?} is {:?}", 333, map.get_cached(&333));
    assert_eq!(map.get_cached(&333), Some(124));
    println!("cached value for {:3?} is {:?}", 3, map.get_cached(&3));
    assert_eq!(map.get_cached(&3), Some(42));
    println!("cached value for {:3?} is {:?}", 651, map.get_cached(&651));
    assert_eq!(map.get_cached(&651), Some(0));
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1));
    assert_eq!(map.get_cached(&1), Some(2));
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1000));
    assert_eq!(map.get_cached(&1000), None);
}


///////////////////


fn four_column_unshared_map() {
    static STARTED: AtomicUsize = ATOMIC_USIZE_INIT;
    thread::spawn(|| {
        print!("starting server...");
        fuzzy_log::run_server(
            "127.0.0.1:8224".parse().unwrap(), 0, 1, None, None, 1, &STARTED)
    });

    while STARTED.load(Ordering::Relaxed) == 0 {
        thread::yield_now()
    }

    println!(" done.");

    println!("Running a four column Map");
    let mut map = Map::new(
        "127.0.0.1:8224".parse().unwrap(), 1..6, |i: &u32| ((i % 4) + 1).into()
    );

    println!("puting {:?} => {:?}", 333, 124);
    map.put(333, 124);

    println!("puting {:?} => {:?}", 3, 42);
    map.put(3, 42);

    println!("puting {:?} => {:?}", 651, 0);
    map.put(651, 0);

    println!("puting {:?} => {:?}", 1, 2);
    map.put(1, 2);

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
    assert_eq!(map.get(651), Some(0));

    println!("cached value for {:3?} is {:?}", 333, map.get_cached(&333));
    assert_eq!(map.get_cached(&333), None);
    println!("cached value for {:3?} is {:?}", 3, map.get_cached(&3));
    assert_eq!(map.get_cached(&3), Some(42));
    println!("cached value for {:3?} is {:?}", 651, map.get_cached(&651));
    assert_eq!(map.get_cached(&651), Some(0));
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1));
    assert_eq!(map.get_cached(&1), None);
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1000));
    assert_eq!(map.get_cached(&1000), None);

    println!("reading {:?}", 333);
    assert_eq!(map.get(333), Some(124));

    println!("cached value for {:3?} is {:?}", 333, map.get_cached(&333));
    assert_eq!(map.get_cached(&333), Some(124));
    println!("cached value for {:3?} is {:?}", 3, map.get_cached(&3));
    assert_eq!(map.get_cached(&3), Some(42));
    println!("cached value for {:3?} is {:?}", 651, map.get_cached(&651));
    assert_eq!(map.get_cached(&651), Some(0));
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1));
    assert_eq!(map.get_cached(&1), Some(2));
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1000));
    assert_eq!(map.get_cached(&1000), None);
}


///////////////////


#[test]
fn test_one_column_unshared_map() {
    one_column_unshared_map()
}

#[test]
fn test_four_column_unshared_map() {
    four_column_unshared_map()
}
