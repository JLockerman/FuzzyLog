
macro_rules! general_tests {
    ($new_store:path) => (general_tests!($new_store,););
    //($new_store:path, $delete: path) => (general_tests!($new_store;$delete,););
    //($new_store:path; $($import:path),*) => {
    //    general_tests!($new_store, $crate::general_tests::noop; $($import,)*);
    //};
    ($new_store:path, $($import:path),*) => {

        #[cfg(test)]
        mod general_tests {
            extern crate env_logger;

            use prelude::*;
            use local_store::{Map, MapEntry};
            $(use $import;)*

            use std::cell::RefCell;
            use std::collections::HashMap;
            use std::rc::Rc;
            use std::sync::{Arc, Mutex};
            use std::thread;

            #[test]
            fn test_get_none() {
                let _ = env_logger::init();
                let mut store = $new_store(Vec::new());
                let r = <Store<MapEntry<i32, i32>>>::get(&mut store, (0.into(), 0.into()));
                assert_eq!(r, Err(GetErr::NoValue))
            }

            #[test]
            fn test_threaded() {
                let _ = env_logger::init();
                let store = $new_store(
                    (0..22).map(|i| (0.into(), i.into()))
                        .chain((0..4).map(|i| (1.into(), i.into())))
                        .collect()
                );
                let s = store.clone();
                let s1 = store.clone();
                let horizon = Arc::new(Mutex::new(HashMap::new()));
                let h = horizon.clone();
                let h1 = horizon.clone();
                let map0 = Rc::new(RefCell::new(HashMap::new()));
                let map1 = Rc::new(RefCell::new(HashMap::new()));
                let re0 = map0.clone();
                let re1 = map1.clone();
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
                    re0.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(1.into(), Box::new(move |MapEntry(k, v)| {
                    re1.borrow_mut().insert(k, v);
                    true
                }));
                let join = thread::spawn(move || {
                    let mut log = FuzzyLog::new(s, h, HashMap::new());
                    let mut last_index = (0.into(), 0.into());
                    for i in 0..10 {
                        last_index = log.append(0.into(), MapEntry(i * 2, i * 2), vec![]);
                    }
                    log.append(1.into(), MapEntry(5, 17), vec![last_index]);
                });
                let join1 = thread::spawn(|| {
                    let mut log = FuzzyLog::new(s1, h1, HashMap::new());
                    let mut last_index = (0.into(), 0.into());
                    for i in 0..10 {
                        last_index = log.append(0.into(), MapEntry(i * 2 + 1, i * 2 + 1), vec![]);
                    }
                    log.append(1.into(), MapEntry(9, 20), vec![last_index]);
                });
                join1.join().unwrap();
                join.join().unwrap();
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.play_foward(1.into());

                let cannonical_map0 = {
                    let mut map = HashMap::new();
                    for i in 0..20 {
                        map.insert(i, i);
                    }
                    map
                };
                let cannonical_map1 = {
                    let mut map = HashMap::new();
                    map.insert(5, 17);
                    map.insert(9, 20);
                    map
                };
                assert_eq!(*map1.borrow(), cannonical_map1);
                assert_eq!(*map0.borrow(), cannonical_map0);
            }

            #[test]
            fn test_1_column() {
                let _ = env_logger::init();
                let store = $new_store(
                    vec![(2.into(), 1.into()), (2.into(), 2.into()),
                    (2.into(), 3.into())]
                );
                let horizon = HashMap::new();
                let mut map = Map::new(store, horizon, 2.into());
                map.put(0, 1);
                map.put(1, 17);
                map.put(32, 5);
                assert_eq!(map.get(1), Some(17));
                assert_eq!(*map.local_view.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
                assert_eq!(map.get(0), Some(1));
                assert_eq!(map.get(32), Some(5));
            }

            #[test]
            fn test_1_column_ni() {
                let _ = env_logger::init();
                let store = $new_store(
                    vec![(0.into(), 1.into()), (0.into(), 2.into()),
                        (0.into(), 3.into()), (1.into(), 1.into())]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
                    re.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(1.into(), Box::new(|_| false));

                let mut log = FuzzyLog::new(store, horizon, upcalls);
                let e1 = log.append(0.into(), MapEntry(0, 1), vec![]);
                assert_eq!(e1, (0.into(), 1.into()));
                let e2 = log.append(0.into(), MapEntry(1, 17), vec![]);
                assert_eq!(e2, (0.into(), 2.into()));
                let last_index = log.append(0.into(), MapEntry(32, 5), vec![]);
                assert_eq!(last_index, (0.into(), 3.into()));
                let en = log.append(1.into(), MapEntry(0, 0), vec![last_index]);
                assert_eq!(en, (1.into(), 1.into()));
                log.play_foward(0.into());
                assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
            }

            #[test]
            fn test_deps() {
                let _ = env_logger::init();
                let store = $new_store(
                    vec![(0.into(), 1.into()), (0.into(), 2.into()),
                        (0.into(), 3.into()), (1.into(), 1.into())]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
                    re.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(1.into(), Box::new(|_| false));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                let e1 = log.append(0.into(), MapEntry(0, 1), vec![]);
                assert_eq!(e1, (0.into(), 1.into()));
                let e2 = log.append(0.into(), MapEntry(1, 17), vec![]);
                assert_eq!(e2, (0.into(), 2.into()));
                let last_index = log.append(0.into(), MapEntry(32, 5), vec![]);
                assert_eq!(last_index, (0.into(), 3.into()));
                let en = log.append(1.into(), MapEntry(0, 0), vec![last_index]);
                assert_eq!(en, (1.into(), 1.into()));
                log.play_foward(1.into());
                assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
            }

            #[test]
            fn test_transaction_1() {
                let _ = env_logger::init();
                let store = $new_store(

                    vec![(0.into(), 1.into()), (0.into(), 2.into()),
                        (1.into(), 1.into()),
                        (3.into(), 1.into())]
                );
                let horizon = HashMap::new();
                let map0 = Rc::new(RefCell::new(HashMap::new()));
                let map1 = Rc::new(RefCell::new(HashMap::new()));
                let map01 = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re0 = map0.clone();
                let re1 = map1.clone();
                let re01 = map01.clone();
                upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
                    re0.borrow_mut().insert(k, v);
                    re01.borrow_mut().insert(k, v);
                    true
                }));
                let re01 = map01.clone();
                upcalls.insert(1.into(), Box::new(move |MapEntry(k, v)| {
                    re1.borrow_mut().insert(k, v);
                    re01.borrow_mut().insert(k, v);
                    true
                }));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                let mutations = vec![(0.into(), MapEntry(0, 1)),
                    (1.into(), MapEntry(4.into(), 17)),
                    (3.into(), MapEntry(22, 9))];
                {
                    let transaction = log.start_transaction(mutations, Vec::new());
                    transaction.commit();
                }

                log.play_foward(0.into());
                assert_eq!(*map0.borrow(), collect![0 => 1]);
                assert_eq!(*map1.borrow(), collect![4 => 17]);
                assert_eq!(*map01.borrow(), collect![0 => 1, 4 => 17]);
                log.play_foward(1.into());
                assert_eq!(*map0.borrow(), collect![0 => 1]);
                assert_eq!(*map1.borrow(), collect![4 => 17]);
                assert_eq!(*map01.borrow(), collect![0 => 1, 4 => 17]);
            }

            #[test]
            fn test_threaded_transaction() {
                let _ = env_logger::init();
                let store = $new_store(
                    (0..40).map(|i| (0.into(), i.into()))
                        .chain((0..40).map(|i| (1.into(), i.into())))
                        .collect()
                );
                let s = store.clone();
                let s1 = store.clone();
                let horizon = Arc::new(Mutex::new(HashMap::new()));
                let h = horizon.clone();
                let h1 = horizon.clone();
                let map0 = Rc::new(RefCell::new(HashMap::new()));
                let map1 = Rc::new(RefCell::new(HashMap::new()));
                let re0 = map0.clone();
                let re1 = map1.clone();
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
                    re0.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(1.into(), Box::new(move |MapEntry(k, v)| {
                    re1.borrow_mut().insert(k, v);
                    true
                }));
                let join = thread::spawn(move || {
                    let mut log = FuzzyLog::new(s, h, HashMap::new());
                    for i in 0..10 {
                        let change = vec![(0.into(), MapEntry(i * 2, i * 2)), (1.into(), MapEntry(i * 2, i * 2))];
                        let trans = log.start_transaction(change, vec![]);
                        trans.commit();
                    }
                });
                let join1 = thread::spawn(|| {
                    let mut log = FuzzyLog::new(s1, h1, HashMap::new());
                    for i in 0..10 {
                        let change = vec![(0.into(), MapEntry(i * 2 + 1, i * 2 + 1)), (1.into(), MapEntry(i * 2 + 1, i * 2 + 1))];
                        let trans = log.start_transaction(change, vec![]);
                        trans.commit();
                    }
                });
                join1.join().unwrap();
                join.join().unwrap();
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.play_foward(1.into());

                let cannonical_map = {
                    let mut map = HashMap::new();
                    for i in 0..20 {
                        map.insert(i, i);
                    }
                    map
                };
                //println!("{:#?}", *log.store.lock().unwrap());
                assert_eq!(*map1.borrow(), cannonical_map);
                assert_eq!(*map0.borrow(), cannonical_map);
            }

            #[test]
            fn test_abort_transaction() {
                let _ = env_logger::init();
                let store = $new_store(
                    vec![(0.into(), 0.into()), (0.into(), 1.into()),
                    (0.into(), 2.into()), (0.into(), 3.into()),
                    (0.into(), 4.into()), (0.into(), 5.into()),]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
                    re.borrow_mut().insert(k, v);
                    true
                }));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.append(0.into(), MapEntry(0, 1), vec![]);
                let mutation = vec![(0.into(), MapEntry(13, 13))];
                log.append(0.into(), MapEntry(1, 17), vec![]);
                drop(log.start_transaction(mutation, vec![]));
                log.play_foward(0.into());
                assert_eq!(*map.borrow(), collect![0 => 1, 1 => 17]);
            }

            #[test]
            fn test_transaction_timeout() {
                use std::mem::forget;
                let _ = env_logger::init();
                let store = $new_store(
                    vec![(0.into(), 0.into()), (0.into(), 1.into()),
                    (0.into(), 2.into()), (0.into(), 3.into()),
                    (0.into(), 4.into()), (0.into(), 5.into()),]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(0.into(), Box::new(move |MapEntry(k, v)| {
                    trace!("MapEntry({:?}, {:?})", k, v);
                    re.borrow_mut().insert(k, v);
                    true
                }));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.append(0.into(), MapEntry(0, 1), vec![]);
                log.append(0.into(), MapEntry(1, 17), vec![]);

                let mutation = vec![(0.into(), MapEntry(13, 13))];
                forget(log.start_transaction(mutation, vec![]));

                log.play_foward(0.into());
                assert_eq!(*map.borrow(), collect![0 => 1, 1 => 17]);
            }
        }
    };
}
