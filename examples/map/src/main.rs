//! #Map Example
//!
//! This file contains a simple implementation of a Map Datastructure based on a
//! shared fuzzy log.
//!
//!

extern crate fuzzy_log;

use map::{Map, MapEntry};
use std::fmt::Debug;

use fuzzy_log::local_store::LocalStore;

fn main() {
    one_column_unshared_map();
    println!("\n\n                          ///////////////////////////");
    println!("                          ///////////////////////////");
    println!("                          ///////////////////////////\n\n");
    four_column_unshared_map();
}

fn one_column_unshared_map() {
    println!("Running a single column Map");
    let mut map = Map::non_shared(vec![1.into()], |_: &u32| 1.into());
    print_local_store(&map.log.store);

    println!("puting {:?} => {:?}", 333, 124);
    map.put(333, 124);
    print_local_store(&map.log.store);

    println!("puting {:?} => {:?}", 3, 42);
    map.put(3, 42);
    print_local_store(&map.log.store);

    println!("puting {:?} => {:?}", 651, 0);
    map.put(651, 0);
    print_local_store(&map.log.store);

    println!("puting {:?} => {:?}", 1, 2);
    map.put(1, 2);
    print_local_store(&map.log.store);

    println!("cached value for {:3?} is {:?}", 333, map.get_cached(&333));
    assert_eq!(map.get_cached(&333), None);
    println!("cached value for {:3?} is {:?}", 3, map.get_cached(&3));
    assert_eq!(map.get_cached(&3), None);
    println!("cached value for {:3?} is {:?}", 651, map.get_cached(&651));
    assert_eq!(map.get_cached(&651), None);
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1));
    assert_eq!(map.get_cached(&1), None);

    println!("reading {:?}", 651);
    assert_eq!(map.get(651), Some(0));
    print_local_store(&map.log.store);

    println!("cached value for {:3?} is {:?}", 333, map.get_cached(&333));
    assert_eq!(map.get_cached(&333), Some(124));
    println!("cached value for {:3?} is {:?}", 3, map.get_cached(&3));
    assert_eq!(map.get_cached(&3), Some(42));
    println!("cached value for {:3?} is {:?}", 651, map.get_cached(&651));
    assert_eq!(map.get_cached(&651), Some(0));
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1));
    assert_eq!(map.get_cached(&1), Some(2));
}

fn four_column_unshared_map() {
    println!("Running a four column Map");
    let interesting_columns = (0..5).map(|i| i.into()).collect();
    let mut map = Map::non_shared(interesting_columns, |i: &u32| (i % 4).into());
    print_local_store(&map.log.store);

    println!("puting {:?} => {:?}", 333, 124);
    map.put(333, 124);
    print_local_store(&map.log.store);

    println!("puting {:?} => {:?}", 3, 42);
    map.put(3, 42);
    print_local_store(&map.log.store);

    println!("puting {:?} => {:?}", 651, 0);
    map.put(651, 0);
    print_local_store(&map.log.store);

    println!("puting {:?} => {:?}", 1, 2);
    map.put(1, 2);
    print_local_store(&map.log.store);

    println!("cached value for {:3?} is {:?}", 333, map.get_cached(&333));
    assert_eq!(map.get_cached(&333), None);
    println!("cached value for {:3?} is {:?}", 3, map.get_cached(&3));
    assert_eq!(map.get_cached(&3), None);
    println!("cached value for {:3?} is {:?}", 651, map.get_cached(&651));
    assert_eq!(map.get_cached(&651), None);
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1));
    assert_eq!(map.get_cached(&1), None);

    println!("reading {:?}", 651);
    assert_eq!(map.get(651), Some(0));
    print_local_store(&map.log.store);

    println!("cached value for {:3?} is {:?}", 333, map.get_cached(&333));
    assert_eq!(map.get_cached(&333), None);
    println!("cached value for {:3?} is {:?}", 3, map.get_cached(&3));
    assert_eq!(map.get_cached(&3), Some(42));
    println!("cached value for {:3?} is {:?}", 651, map.get_cached(&651));
    assert_eq!(map.get_cached(&651), Some(0));
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1));
    assert_eq!(map.get_cached(&1), None);

    println!("reading {:?}", 333);
    assert_eq!(map.get(333), Some(124));
    print_local_store(&map.log.store);

    println!("cached value for {:3?} is {:?}", 333, map.get_cached(&333));
    assert_eq!(map.get_cached(&333), Some(124));
    println!("cached value for {:3?} is {:?}", 3, map.get_cached(&3));
    assert_eq!(map.get_cached(&3), Some(42));
    println!("cached value for {:3?} is {:?}", 651, map.get_cached(&651));
    assert_eq!(map.get_cached(&651), Some(0));
    println!("cached value for {:3?} is {:?}", 1, map.get_cached(&1));
    assert_eq!(map.get_cached(&1), Some(2));
}


fn print_local_store<K: Debug, V: Debug>(store: &LocalStore<MapEntry<K, V>>) {
    let mut store_entries: Vec<_> = store.iter().collect();
    store_entries.sort_by(|&l, &r,| l.0.cmp(r.0));
    let mut lst_col = 0.into();
    print!("\nStore is {{");
    for &(&(col, row), ent) in &store_entries {
        if col != lst_col {
            print!("\n  {:?}: ", col);
            println!("{:?} => {:?}", row, ent);
            lst_col = col;
        }
        else {
            println!("            {:?} => {:?} ", row, ent);
        }
    }
    println!("}}\n");
}

#[test]
fn test_one_column_unshared_map() {
    one_column_unshared_map()
}

#[test]
fn test_four_column_unshared_map() {
    four_column_unshared_map()
}

/// Implementation Details TODO
mod map {
    use std::rc::Rc;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fmt::{Debug, Formatter, Result as FmtResult};
    use std::hash::Hash;

    use fuzzy_log::prelude::*;
    use fuzzy_log::local_store::{LocalHorizon, LocalStore};

    #[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
    pub struct MapEntry<K, V>(pub K, pub V);

    pub struct Map<K, V, S, H, F>
    where K: Hash + Eq + Copy, V: Copy,
          S: Store<MapEntry<K, V>>,
          H: Horizon,
          F: Fn(&K) -> order {
        pub log: FuzzyLog<MapEntry<K, V>, S, H>,
        local_view: Rc<RefCell<HashMap<K, V>>>,
        key_to_column: F,
    }

    impl<K: 'static, V: 'static, F>
    Map<K, V, LocalStore<MapEntry<K, V>>, LocalHorizon, F>
    where K: Hash + Eq + Copy,
          V: Copy,
          F: Fn(&K) -> order, {

        pub fn non_shared(interesting_columns: Vec<order>, key_to_column: F)
            -> Self {
            Self::new(HashMap::new(), HashMap::new(), interesting_columns, key_to_column)
        }

    }

    impl<K: 'static, V: 'static, S, H, F> Map<K, V, S, H, F>
    where K: Hash + Eq + Copy,
          V: Copy,
          S: Store<MapEntry<K, V>>,
          H: Horizon,
          F: Fn(&K) -> order, {

        pub fn new(store: S, horizon: H, interesting_columns: Vec<order>, key_to_column: F)
            -> Map<K, V, S, H, F> {
            let local_view = Rc::new(RefCell::new(HashMap::new()));
            let re = local_view.clone();
            Map {
                log: FuzzyLog::new(store, horizon,
                    interesting_columns.into_iter().map(|c| {
                        let re = re.clone();
                        let upcall = Box::new(move |MapEntry(k, v)| {
                            re.borrow_mut().insert(k, v);
                            true
                        }) as Box<Fn(_) -> _>;
                        (c, upcall)
                    }).collect(),
                ),
                local_view: local_view,
                key_to_column: key_to_column
            }
        }

        pub fn put(&mut self, key: K, val: V) {
            self.log.append((self.key_to_column)(&key), MapEntry(key, val), vec![]);
        }

        pub fn get(&mut self, key: K) -> Option<V> {
            self.log.play_foward((self.key_to_column)(&key));
            self.local_view.borrow().get(&key).cloned()
        }

        pub fn get_cached(&mut self, key: &K) -> Option<V> {
            self.local_view.borrow().get(key).cloned()
        }
    }

    impl<K, V, S, H, F> Debug for Map<K, V, S, H, F>
    where K: Hash + Eq + Copy + Debug, V: Copy + Debug,
          S: Store<MapEntry<K, V>>,
          H: Horizon,
          F: Fn(&K) -> order, {

        fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
            formatter.debug_struct("Map").field("local_view", &self.local_view).finish()
        }
    }
}
