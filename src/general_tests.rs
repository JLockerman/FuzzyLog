
macro_rules! general_tests {
    ($new_store:path) => (general_tests!($new_store,););
    //($new_store:path, $delete: path) => (general_tests!($new_store;$delete,););
    //($new_store:path; $($import:path),*) => {
    //    general_tests!($new_store, $crate::general_tests::noop; $($import,)*);
    //};
    ($new_store:path, $($import:path),*) => {

        #[cfg(test)]
        mod general_tests {
            //last column used is 18

            extern crate env_logger;

            use prelude::*;
            use local_store::MapEntry;
            use local_store::test::Map;
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
                let r = <Store<MapEntry<i32, i32>>>::get(&mut store, (14.into(), 0.into()));
                assert_eq!(r, Err(GetErr::NoValue))
            }

            #[test]
            fn test_threaded() {
                let _ = env_logger::init();
                let store = $new_store(
                    (0..22).map(|i| (1.into(), i.into()))
                        .chain((0..4).map(|i| (2.into(), i.into())))
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
                upcalls.insert(1.into(), Box::new(move |MapEntry(k, v)| {
                    re0.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(2.into(), Box::new(move |MapEntry(k, v)| {
                    re1.borrow_mut().insert(k, v);
                    true
                }));
                let join = thread::spawn(move || {
                    let mut log = FuzzyLog::new(s, h, HashMap::new());
                    let mut last_index = (1.into(), 0.into());
                    for i in 0..10 {
                        last_index = log.append(1.into(), MapEntry(i * 2, i * 2), vec![]);
                    }
                    log.append(2.into(), MapEntry(5, 17), vec![last_index]);
                });
                let join1 = thread::spawn(|| {
                    let mut log = FuzzyLog::new(s1, h1, HashMap::new());
                    let mut last_index = (1.into(), 0.into());
                    for i in 0..10 {
                        last_index = log.append(1.into(), MapEntry(i * 2 + 1, i * 2 + 1), vec![]);
                    }
                    log.append(2.into(), MapEntry(9, 20), vec![last_index]);
                });
                join1.join().unwrap();
                join.join().unwrap();
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.play_foward(2.into());

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
                    vec![(3.into(), 1.into()), (3.into(), 2.into()),
                    (3.into(), 3.into())]
                );
                let horizon = HashMap::new();
                let mut map = Map::new(store, horizon, 3.into());
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
                    vec![(4.into(), 1.into()), (4.into(), 2.into()),
                        (4.into(), 3.into()), (5.into(), 1.into())]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(4.into(), Box::new(move |MapEntry(k, v)| {
                    re.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(5.into(), Box::new(|_| false));

                let mut log = FuzzyLog::new(store, horizon, upcalls);
                let e1 = log.append(4.into(), MapEntry(0, 1), vec![]);
                assert_eq!(e1, (4.into(), 1.into()));
                let e2 = log.append(4.into(), MapEntry(1, 17), vec![]);
                assert_eq!(e2, (4.into(), 2.into()));
                let last_index = log.append(4.into(), MapEntry(32, 5), vec![]);
                assert_eq!(last_index, (4.into(), 3.into()));
                let en = log.append(5.into(), MapEntry(0, 0), vec![last_index]);
                assert_eq!(en, (5.into(), 1.into()));
                log.play_foward(4.into());
                assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
            }

            #[test]
            fn test_deps() {
                let _ = env_logger::init();
                let store = $new_store(
                    vec![(6.into(), 1.into()), (6.into(), 2.into()),
                        (6.into(), 3.into()), (7.into(), 1.into())]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(6.into(), Box::new(move |MapEntry(k, v)| {
                    re.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(7.into(), Box::new(|_| false));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                let e1 = log.append(6.into(), MapEntry(0, 1), vec![]);
                assert_eq!(e1, (6.into(), 1.into()));
                let e2 = log.append(6.into(), MapEntry(1, 17), vec![]);
                assert_eq!(e2, (6.into(), 2.into()));
                let last_index = log.append(6.into(), MapEntry(32, 5), vec![]);
                assert_eq!(last_index, (6.into(), 3.into()));
                let en = log.append(7.into(), MapEntry(0, 0), vec![last_index]);
                assert_eq!(en, (7.into(), 1.into()));
                log.play_foward(7.into());
                assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
            }

            #[cfg(FALSE)]
            #[test]
            fn test_transaction_1() {
                let _ = env_logger::init();
                let store = $new_store(

                    vec![(8.into(), 1.into()), (8.into(), 2.into()),
                        (9.into(), 1.into()),
                        (114.into(), 1.into())]
                );
                let horizon = HashMap::new();
                let map0 = Rc::new(RefCell::new(HashMap::new()));
                let map1 = Rc::new(RefCell::new(HashMap::new()));
                let map01 = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re0 = map0.clone();
                let re1 = map1.clone();
                let re01 = map01.clone();
                upcalls.insert(8.into(), Box::new(move |MapEntry(k, v)| {
                    re0.borrow_mut().insert(k, v);
                    re01.borrow_mut().insert(k, v);
                    true
                }));
                let re01 = map01.clone();
                upcalls.insert(9.into(), Box::new(move |MapEntry(k, v)| {
                    re1.borrow_mut().insert(k, v);
                    re01.borrow_mut().insert(k, v);
                    true
                }));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                let mutations = vec![(8.into(), MapEntry(0, 1)),
                    (9.into(), MapEntry(4.into(), 17)),
                    (114.into(), MapEntry(22, 9))];
                {
                    let transaction = log.start_transaction(mutations, Vec::new());
                    transaction.commit();
                }

                log.play_foward(8.into());
                assert_eq!(*map0.borrow(), collect![0 => 1]);
                assert_eq!(*map1.borrow(), collect![4 => 17]);
                assert_eq!(*map01.borrow(), collect![0 => 1, 4 => 17]);
                log.play_foward(9.into());
                assert_eq!(*map0.borrow(), collect![0 => 1]);
                assert_eq!(*map1.borrow(), collect![4 => 17]);
                assert_eq!(*map01.borrow(), collect![0 => 1, 4 => 17]);
            }

            #[cfg(FALSE)]
            #[test]
            fn test_threaded_transaction() {
                let _ = env_logger::init();
                let store = $new_store(
                    (0..40).map(|i| (11.into(), i.into()))
                        .chain((0..40).map(|i| (12.into(), i.into())))
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
                upcalls.insert(11.into(), Box::new(move |MapEntry(k, v)| {
                    re0.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(12.into(), Box::new(move |MapEntry(k, v)| {
                    re1.borrow_mut().insert(k, v);
                    true
                }));
                let join = thread::spawn(move || {
                    let mut log = FuzzyLog::new(s, h, HashMap::new());
                    for i in 0..10 {
                        let change = vec![(11.into(), MapEntry(i * 2, i * 2)), (12.into(), MapEntry(i * 2, i * 2))];
                        let trans = log.start_transaction(change, vec![]);
                        trans.commit();
                    }
                });
                let join1 = thread::spawn(|| {
                    let mut log = FuzzyLog::new(s1, h1, HashMap::new());
                    for i in 0..10 {
                        let change = vec![(11.into(), MapEntry(i * 2 + 1, i * 2 + 1)), (12.into(), MapEntry(i * 2 + 1, i * 2 + 1))];
                        let trans = log.start_transaction(change, vec![]);
                        trans.commit();
                    }
                });
                join1.join().unwrap();
                join.join().unwrap();
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.play_foward(12.into());

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

            #[cfg(FALSE)]
            #[test]
            fn test_abort_transaction() {
                let _ = env_logger::init();
                let store = $new_store(
                    vec![(13.into(), 0.into()), (13.into(), 1.into()),
                    (13.into(), 2.into()), (13.into(), 3.into()),
                    (13.into(), 4.into()), (13.into(), 5.into()),]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(13.into(), Box::new(move |MapEntry(k, v)| {
                    re.borrow_mut().insert(k, v);
                    true
                }));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.append(13.into(), MapEntry(0, 1), vec![]);
                let mutation = vec![(13.into(), MapEntry(13, 13))];
                log.append(13.into(), MapEntry(1, 17), vec![]);
                drop(log.start_transaction(mutation, vec![]));
                log.play_foward(13.into());
                assert_eq!(*map.borrow(), collect![0 => 1, 1 => 17]);
            }

            #[cfg(FALSE)]
            #[test]
            fn test_transaction_timeout() {
                use std::mem::forget;
                let _ = env_logger::init();
                let store = $new_store(
                    vec![(14.into(), 0.into()), (14.into(), 1.into()),
                    (14.into(), 2.into()), (14.into(), 3.into()),
                    (14.into(), 4.into()), (14.into(), 5.into()),]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(14.into(), Box::new(move |MapEntry(k, v)| {
                    trace!("MapEntry({:?}, {:?})", k, v);
                    re.borrow_mut().insert(k, v);
                    true
                }));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.append(14.into(), MapEntry(0, 1), vec![]);
                log.append(14.into(), MapEntry(1, 17), vec![]);

                let mutation = vec![(14.into(), MapEntry(13, 13))];
                forget(log.start_transaction(mutation, vec![]));

                log.play_foward(14.into());
                assert_eq!(*map.borrow(), collect![0 => 1, 1 => 17]);
            }

            #[cfg(FALSE)]
            #[test]
            fn test_multiput() {
                let _ = env_logger::init();
                let store = $new_store(
                    vec![(15.into(), 0.into()), (15.into(), 1.into()),
                    (15.into(), 2.into()), (15.into(), 3.into()),
                    (16.into(), 0.into()), (16.into(), 1.into()),
                    (16.into(), 2.into()), (16.into(), 3.into()),]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re1 = map.clone();
                let re2 = map.clone();
                upcalls.insert(15.into(), Box::new(move |MapEntry(k, v)| {
                    trace!("MapEntry({:?}, {:?})", k, v);
                    re1.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(16.into(), Box::new(move |MapEntry(k, v)| {
                    trace!("MapEntry({:?}, {:?})", k, v);
                    re2.borrow_mut().insert(k, v);
                    true
                }));

                let mut log = FuzzyLog::new(store, horizon, upcalls);
                //try_multiput(&mut self, offset: u32, mut columns: Vec<(order, V)>, deps: Vec<OrderIndex>)
                let columns = vec![(15.into(), MapEntry(13, 5)),
                    (16.into(), MapEntry(92, 7))];
                let v = log.try_multiput(0u32.into(), columns, vec![]);
                assert_eq!(v, Some(vec![(15.into(), 1.into()),
                    (16.into(), 1.into())]));

                let columns = vec![(15.into(), MapEntry(3, 8)),
                    (16.into(), MapEntry(2, 54))];
                let v = log.try_multiput(0u32.into(), columns, vec![]);
                assert_eq!(v, Some(vec![(15.into(), 2.into()),
                    (16.into(), 2.into())]));

                log.play_until((15.into(), 1.into()));

                assert_eq!(*map.borrow(), collect![13 => 5, 92 => 7]);
                log.play_until((15.into(), 2.into()));
                assert_eq!(*map.borrow(), collect![13 => 5, 92 => 7, 3 => 8, 2 => 54]);
            }

            #[cfg(FALSE)]
            #[test]
            fn test_threaded_multiput() {
                let _ = env_logger::init();
                let store = $new_store(
                    (0..20).map(|i| (17.into(), i.into()))
                        .chain((0..20).map(|i| (18.into(), i.into())))
                        .collect()
                );
                let s = store.clone();
                let s1 = store.clone();
                let horizon = Arc::new(Mutex::new(HashMap::new()));
                let h = horizon.clone();
                let h1 = horizon.clone();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<Fn(_) -> bool>> = HashMap::new();
                let re1 = map.clone();
                let re2 = map.clone();
                upcalls.insert(17.into(), Box::new(move |MapEntry(k, v)| {
                    trace!("MapEntry({:?}, {:?})", k, v);
                    re1.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(18.into(), Box::new(move |MapEntry(k, v)| {
                    trace!("MapEntry({:?}, {:?})", k, v);
                    re2.borrow_mut().insert(k, v);
                    true
                }));

                let mut log = FuzzyLog::new(store, horizon, upcalls);
                //try_multiput(&mut self, offset: u32, mut columns: Vec<(order, V)>, deps: Vec<OrderIndex>)

                let join = thread::spawn(move || {
                    let mut log = FuzzyLog::new(s, h, HashMap::new());
                    for i in 0..10 {
                        let change = vec![17.into(), 18.into()];
                        let data = MapEntry(i * 2, i * 2);
                        log.multiappend(change.clone(), data, vec![])
                    }
                });
                let join1 = thread::spawn(|| {
                    let mut log = FuzzyLog::new(s1, h1, HashMap::new());
                    for i in 0..10 {
                        let change = vec![17.into(), 18.into()];
                        let data = MapEntry(i * 2 + 1, i * 2 + 1);
                        log.multiappend(change, data, vec![])
                    }
                });
                join1.join().unwrap();
                join.join().unwrap();

                log.play_foward(17.into());

                let cannonical_map = {
                    let mut map = HashMap::new();
                    for i in 0..20 {
                        map.insert(i, i);
                    }
                    map
                };
                assert_eq!(*map.borrow(), cannonical_map);
            }

        }//End mod test

    };
}
