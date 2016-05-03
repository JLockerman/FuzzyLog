#![feature(const_fn)]
//! #Map Example
//!
//! This file contains a simple implementation of a Map Datastructure based on a
//! shared fuzzy log.
//!
//!

extern crate fuzzy_log;
extern crate time;
extern crate rand;
extern crate env_logger;
extern crate thread_scoped;

use map::{Map, MapEntry};

use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
//use std::thread;

use rand::Rng;

const SERVER_ADDR_STR: &'static str = "10.21.7.4:13265";

fn main() {
    let start: AtomicBool = AtomicBool::new(false);
    let args_len = env::args().len();
    let addr = match (args_len, env::args().next()) {
        (i, Some(ref s)) if i > 1 => ("0.0.0.0".to_owned() + &*s).parse().expect("invalid inet address"),
        _ => SERVER_ADDR_STR.parse().expect("invalid inet address"),
    };
    let _ = env_logger::init();
    println!("threads, iters/s");
    unsafe {
        for j in 1..50 {
            start.store(false, Ordering::Relaxed);
            let mut handles = Vec::new();
            for _ in 0..j {
                let handle = thread_scoped::scoped(|| {
                    let mut iters = 0u64;
                    let mut map = Map::non_shared(addr, vec![(23 + j).into()], 23 + j);
                    while !start.load(Ordering::Relaxed) {}
                    let start_time = time::precise_time_ns();
                    while time::precise_time_ns() - start_time < 1000000000 {
                        map.put(&[MapEntry(7i32, rand::thread_rng().gen::<i32>()),
                            MapEntry(11, rand::thread_rng().gen()), MapEntry(13, rand::thread_rng().gen())]);
                        iters += 1;
                    }
                    iters
                });
                handles.push(handle);
            }
            assert_eq!(handles.len(), j as usize);
            start.store(true, Ordering::Relaxed);
            let mut total_iters = 0;
            for handle in handles {
                let i = handle.join();
                total_iters += i;
                //println!("thread {} iters/s {}", threads, i);
                //threads += 1;
            }
            println!("{}, {}", j, total_iters);
        }
    }
}

pub mod map {
    use std::rc::Rc;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::hash::Hash;
    use std::net::SocketAddr;

    use fuzzy_log::prelude::*;
    use fuzzy_log::local_store::LocalHorizon;
    use fuzzy_log::tcp_store::TcpStore;

    #[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
    pub struct MapEntry<K, V>(pub K, pub V);

    pub struct Map<K, V, S, H>
    where K: Hash + Eq + Copy, V: Copy,
          S: Store<[MapEntry<K, V>]>,
          H: Horizon, {
        pub log: FuzzyLog<[MapEntry<K, V>], S, H>,
        local_view: Rc<RefCell<HashMap<K, V>>>,
        column: order,
    }

    impl<K: 'static, V: 'static>
    Map<K, V, TcpStore<[MapEntry<K, V>]>, LocalHorizon>
    where K: Hash + Eq + Copy + Debug,
          V: Copy + Debug, {

        pub fn non_shared(addr: SocketAddr, interesting_columns: Vec<order>, column: u32)
            -> Self {
            Self::new(TcpStore::new(addr),
                HashMap::new(), interesting_columns, column)
        }

    }

    impl<K: 'static, V: 'static, S, H> Map<K, V, S, H>
    where K: Hash + Eq + Copy,
          V: Copy,
          S: Store<[MapEntry<K, V>]>,
          H: Horizon, {

        pub fn new(store: S, horizon: H, interesting_columns: Vec<order>, column: u32)
            -> Map<K, V, S, H> {
            let local_view = Rc::new(RefCell::new(HashMap::new()));
            let re = local_view.clone();
            Map {
                log: FuzzyLog::new(store, horizon,
                    interesting_columns.into_iter().map(|c| {
                        let re = re.clone();
                        let upcall:  Box<Fn(&_) -> _> = Box::new(move |entries|
                            {
                                for &MapEntry(k, v) in entries {
                                    re.borrow_mut().insert(k, v);
                                }
                                true
                            }
                        );
                        (c, upcall)
                    }).collect(),
                ),
                local_view: local_view,
                column: column.into()
            }
        }

        pub fn put(&mut self, entries: &[MapEntry<K, V>]) {
            self.log.append(self.column, entries, &[]);
        }

        pub fn get(&mut self, key: K) -> Option<V> {
            self.log.play_foward(self.column);
            self.local_view.borrow().get(&key).cloned()
        }

        pub fn get_cached(&mut self, key: &K) -> Option<V> {
            self.local_view.borrow().get(key).cloned()
        }
    }

    //impl<K, V, S, H, F> Debug for Map<K, V, S, H, F>
    //where K: Hash + Eq + Copy + Debug, V: Copy + Debug,
    //      S: Store<[MapEntry<K, V>]>,
    //      H: Horizon,
    //      F: Fn(&K) -> order, {

    //    fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
    //        formatter.debug_struct("Map").field("local_view", &self.local_view).finish()
    //    }
    //}
}
