
macro_rules! general_tests {
    ($new_store:path) => (general_tests!($new_store,););
    //($new_store:path, $delete: path) => (general_tests!($new_store;$delete,););
    //($new_store:path; $($import:path),*) => {
    //    general_tests!($new_store, $crate::general_tests::noop; $($import,)*);
    //};
    ($new_store:path, $($import:path),*) => {

        #[cfg(test)]
        mod general_tests {
            //last column used is 28

            extern crate env_logger;

            use prelude::*;
            use local_store::MapEntry;
            use local_store::test::Map;
            $(use $import;)*

            use std::cell::RefCell;
            use std::collections::{HashMap, BTreeMap};
            use std::rc::Rc;
            use std::sync::{Arc, Mutex};
            use std::thread;

            #[test]
            fn test_get_none() {
                let _ = env_logger::init();
                let mut store = $new_store(Vec::new());
                let r = <Store<MapEntry<i32, i32>>>::get(&mut store,
                    OrderIndex(14.into(), 0.into()));
                assert_eq!(r, Err(GetErr::NoValue(0.into())))
            }

            #[test]
            fn test_get_none2() {
                let _ = env_logger::init();
                let mut store = $new_store(vec![OrderIndex(19.into(), 1.into()), OrderIndex(19.into(), 2.into()), OrderIndex(19.into(), 3.into()), OrderIndex(19.into(), 4.into()), OrderIndex(19.into(), 5.into())]);
                for i in 0..5 {
                    let r = store.insert(OrderIndex(19.into(), 0.into()), EntryContents::Data(&63, &[]));
                    assert_eq!(r, Ok(OrderIndex(19.into(), (i + 1).into())))
                }
                let r = store.get(OrderIndex(19.into(), ::std::u32::MAX.into()));
                assert_eq!(r, Err(GetErr::NoValue(5.into())))
            }

            #[test]
            fn test_threaded_appends() {
                let _ = env_logger::init();
                let store = $new_store(
                    (0..22).map(|i| OrderIndex(1.into(), i.into()))
                        .chain((0..4).map(|i| OrderIndex(2.into(), i.into())))
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
                let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = HashMap::new();
                upcalls.insert(1.into(), Box::new(move |_, _, &MapEntry(k, v)| {
                    re0.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(2.into(), Box::new(move |_, _, &MapEntry(k, v)| {
                    re1.borrow_mut().insert(k, v);
                    true
                }));
                let join = thread::spawn(move || {
                    trace!("T1 start");
                    let mut log = FuzzyLog::new(s, h, HashMap::new());
                    let mut last_index = OrderIndex(1.into(), 0.into());
                    for i in 0..10 {
                        last_index = log.append(1.into(), &MapEntry(i * 2, i * 2), &*vec![]);
                        trace!("T1 inserted {:?} at {:?}", (i * 2, i * 2), last_index);
                    }
                    let index2 = log.append(2.into(), &MapEntry(5, 17), &*vec![last_index]);
                    trace!("T1 inserted {:?} at {:?}", last_index, index2);
                });
                let join1 = thread::spawn(|| {
                    trace!("T2 start");
                    let mut log = FuzzyLog::new(s1, h1, HashMap::new());
                    let mut last_index = OrderIndex(1.into(), 0.into());
                    for i in 0..10 {
                        last_index = log.append(1.into(), &MapEntry(i * 2 + 1, i * 2 + 1), &*vec![]);
                        trace!("T2 inserted {:?} at {:?}", (i * 2 + 1, i * 2 + 1), last_index);
                    }
                    let index2 = log.append(2.into(), &MapEntry(9, 20), &*vec![last_index]);
                    trace!("T2 inserted {:?} at {:?}", last_index, index2);
                });
                trace!("started threads");
                join1.join().unwrap();
                join.join().unwrap();
                trace!("finished inserts");
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.play_foward(2.into());
                trace!("finished reads");
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
                    vec![OrderIndex(3.into(), 1.into()), OrderIndex(3.into(), 2.into()),
                    OrderIndex(3.into(), 3.into())]
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
                    vec![OrderIndex(4.into(), 1.into()), OrderIndex(4.into(), 2.into()),
                        OrderIndex(4.into(), 3.into()), OrderIndex(5.into(), 1.into())]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(4.into(), Box::new(move |_, _, &MapEntry(k, v)| {
                    re.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(5.into(), Box::new(|_, _, _| false));

                let mut log = FuzzyLog::new(store, horizon, upcalls);
                let e1 = log.append(4.into(), &MapEntry(0, 1), &*vec![]);
                assert_eq!(e1, OrderIndex(4.into(), 1.into()));
                let e2 = log.append(4.into(), &MapEntry(1, 17), &*vec![]);
                assert_eq!(e2, OrderIndex(4.into(), 2.into()));
                let last_index = log.append(4.into(), &MapEntry(32, 5), &*vec![]);
                assert_eq!(last_index, OrderIndex(4.into(), 3.into()));
                let en = log.append(5.into(), &MapEntry(0, 0), &*vec![last_index]);
                assert_eq!(en, OrderIndex(5.into(), 1.into()));
                log.play_foward(4.into());
                assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
            }

            #[test]
            fn test_deps() {
                let _ = env_logger::init();
                let store = $new_store(
                    vec![OrderIndex(6.into(), 1.into()), OrderIndex(6.into(), 2.into()),
                        OrderIndex(6.into(), 3.into()), OrderIndex(7.into(), 1.into())]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(6.into(), Box::new(move |_, _, &MapEntry(k, v)| {
                    re.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(7.into(), Box::new(|_, _, _| false));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                let e1 = log.append(6.into(), &MapEntry(0, 1), &*vec![]);
                assert_eq!(e1, OrderIndex(6.into(), 1.into()));
                let e2 = log.append(6.into(), &MapEntry(1, 17), &*vec![]);
                assert_eq!(e2, OrderIndex(6.into(), 2.into()));
                let last_index = log.append(6.into(), &MapEntry(32, 5), &*vec![]);
                assert_eq!(last_index, OrderIndex(6.into(), 3.into()));
                let en = log.append(7.into(), &MapEntry(0, 0), &*vec![last_index]);
                assert_eq!(en, OrderIndex(7.into(), 1.into()));
                log.play_foward(7.into());
                assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
            }

            #[cfg(FALSE)]
            #[test]
            fn test_transaction_1() {
                let _ = env_logger::init();
                let store = $new_store(

                    vec![OrderIndex(8.into(), 1.into()), OrderIndex(8.into(), 2.into()),
                        OrderIndex(9.into(), 1.into()),
                        OrderIndex(114.into(), 1.into())]
                );
                let horizon = HashMap::new();
                let map0 = Rc::new(RefCell::new(HashMap::new()));
                let map1 = Rc::new(RefCell::new(HashMap::new()));
                let map01 = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = HashMap::new();
                let re0 = map0.clone();
                let re1 = map1.clone();
                let re01 = map01.clone();
                upcalls.insert(8.into(), Box::new(move |_, _, &MapEntry(k, v)| {
                    re0.borrow_mut().insert(k, v);
                    re01.borrow_mut().insert(k, v);
                    true
                }));
                let re01 = map01.clone();
                upcalls.insert(9.into(), Box::new(move |_, _, &MapEntry(k, v)| {
                    re1.borrow_mut().insert(k, v);
                    re01.borrow_mut().insert(k, v);
                    true
                }));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                let mutations = vec![(8.into(), &MapEntry(0, 1)),
                    (9.into(), &MapEntry(4.into(), 17)),
                    (114.into(), &MapEntry(22, 9))];
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
                    (0..40).map(|i| OrderIndex(11.into(), i.into()))
                        .chain((0..40).map(|i| OrderIndex(12.into(), i.into())))
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
                let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = HashMap::new();
                upcalls.insert(11.into(), Box::new(move |_, _, _, &MapEntry(k, v)| {
                    re0.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(12.into(), Box::new(move |_, _, _, &MapEntry(k, v)| {
                    re1.borrow_mut().insert(k, v);
                    true
                }));
                let join = thread::spawn(move || {
                    let mut log = FuzzyLog::new(s, h, HashMap::new());
                    for i in 0..10 {
                        let change = vec![(11.into(), &MapEntry(i * 2, i * 2)), (12.into(), &MapEntry(i * 2, i * 2))];
                        let trans = log.start_transaction(change, vec![]);
                        trans.commit();
                    }
                });
                let join1 = thread::spawn(|| {
                    let mut log = FuzzyLog::new(s1, h1, HashMap::new());
                    for i in 0..10 {
                        let change = vec![(11.into(), &MapEntry(i * 2 + 1, i * 2 + 1)), (12.into(), &MapEntry(i * 2 + 1, i * 2 + 1))];
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
                    vec![OrderIndex(13.into(), 0.into()), OrderIndex(13.into(), 1.into()),
                    OrderIndex(13.into(), 2.into()), OrderIndex(13.into(), 3.into()),
                    OrderIndex(13.into(), 4.into()), OrderIndex(13.into(), 5.into()),]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<for<'o, 'r> Fn(&'o OrderIndex, &'r _) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(13.into(), Box::new(move |_, &MapEntry(k, v)| {
                    re.borrow_mut().insert(k, v);
                    true
                }));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.append(13.into(), &MapEntry(0, 1), vec![]);
                let mutation = vec![(13.into(), &MapEntry(13, 13))];
                log.append(13.into(), &MapEntry(1, 17), vec![]);
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
                    vec![OrderIndex(14.into(), 0.into()), OrderIndex(14.into(), 1.into()),
                    OrderIndex(14.into(), 2.into()), OrderIndex(14.into(), 3.into()),
                    OrderIndex(14.into(), 4.into()), OrderIndex(14.into(), 5.into()),]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<for<'o, 'r> Fn(&'o OrderIndex, &'r _) -> bool>> = HashMap::new();
                let re = map.clone();
                upcalls.insert(14.into(), Box::new(move |_, &MapEntry(k, v)| {
                    trace!("MapEntry({:?}, {:?})", k, v);
                    re.borrow_mut().insert(k, v);
                    true
                }));
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.append(14.into(), &MapEntry(0, 1), vec![]);
                log.append(14.into(), &MapEntry(1, 17), vec![]);

                let mutation = vec![(14.into(), &MapEntry(13, 13))];
                forget(log.start_transaction(mutation, vec![]));

                log.play_foward(14.into());
                assert_eq!(*map.borrow(), collect![0 => 1, 1 => 17]);
            }

            #[cfg(FALSE)]
            #[test]
            fn test_multiput() {
                let _ = env_logger::init();
                let store = $new_store(
                    vec![OrderIndex(15.into(), 0.into()), OrderIndex(15.into(), 1.into()),
                    OrderIndex(15.into(), 2.into()), OrderIndex(15.into(), 3.into()),
                    OrderIndex(16.into(), 0.into()), OrderIndex(16.into(), 1.into()),
                    OrderIndex(16.into(), 2.into()), OrderIndex(16.into(), 3.into()),]
                );
                let horizon = HashMap::new();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<for<'o, 'r> Fn(&'o OrderIndex, &'r _) -> bool>> = HashMap::new();
                let re1 = map.clone();
                let re2 = map.clone();
                upcalls.insert(15.into(), Box::new(move |_, &MapEntry(k, v)| {
                    trace!("MapEntry({:?}, {:?})", k, v);
                    re1.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(16.into(), Box::new(move |_, &MapEntry(k, v)| {
                    trace!("MapEntry({:?}, {:?})", k, v);
                    re2.borrow_mut().insert(k, v);
                    true
                }));

                let mut log = FuzzyLog::new(store, horizon, upcalls);
                //try_multiput(&mut self, offset: u32, mut columns: Vec<(order, V)>, deps: Vec<OrderIndex>)
                let columns = &*vec![(15.into(), &MapEntry(13, 5)),
                    (16.into(), &MapEntry(92, 7))];
                let v = log.try_multiput(0u32.into(), columns, &*vec![]);
                assert_eq!(v, Some(&*vec![OrderIndex(15.into(), 1.into()),
                    OrderIndex(16.into(), 1.into())]));

                let columns = &*vec![(15.into(), &MapEntry(3, 8)),
                    (16.into(), &MapEntry(2, 54))];
                let v = log.try_multiput(0u32.into(), columns, &*vec![]);
                assert_eq!(v, Some(&*vec![OrderIndex(15.into(), 2.into()),
                    OrderIndex(16.into(), 2.into())]));

                log.play_until(OrderIndex(15.into(), 1.into()));

                assert_eq!(*map.borrow(), collect![13 => 5, 92 => 7]);
                log.play_until(OrderIndex(15.into(), 2.into()));
                assert_eq!(*map.borrow(), collect![13 => 5, 92 => 7, 3 => 8, 2 => 54]);
            }

            #[test]
            fn test_threaded_multiput() {
                let _ = env_logger::init();
                let store = $new_store(
                    (0..20).map(|i| OrderIndex(17.into(), i.into()))
                        .chain((0..20).map(|i| OrderIndex(18.into(), i.into())))
                        .collect()
                );
                let s = store.clone();
                let s1 = store.clone();
                let horizon = Arc::new(Mutex::new(HashMap::new()));
                let h = horizon.clone();
                let h1 = horizon.clone();
                let map = Rc::new(RefCell::new(HashMap::new()));
                let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = HashMap::new();
                let re1 = map.clone();
                let re2 = map.clone();
                upcalls.insert(17.into(), Box::new(move |_, _, &MapEntry(k, v)| {
                    trace!("MapEntry({:?}, {:?})", k, v);
                    re1.borrow_mut().insert(k, v);
                    true
                }));
                upcalls.insert(18.into(), Box::new(move |_, _, &MapEntry(k, v)| {
                    trace!("MapEntry({:?}, {:?})", k, v);
                    re2.borrow_mut().insert(k, v);
                    true
                }));

                let mut log = FuzzyLog::new(store, horizon, upcalls);
                //try_multiput(&mut self, offset: u32, mut columns: Vec<(order, V)>, deps: Vec<OrderIndex>)

                let join = thread::spawn(move || {
                    let mut log = FuzzyLog::new(s, h, HashMap::new());
                    for i in 0..10 {
                        let change = &*vec![17.into(), 18.into()];
                        let data = &MapEntry(i * 2, i * 2);
                        log.multiappend(change.clone(), data, &*vec![])
                    }
                });
                let join1 = thread::spawn(|| {
                    let mut log = FuzzyLog::new(s1, h1, HashMap::new());
                    for i in 0..10 {
                        let change = &*vec![17.into(), 18.into()];
                        let data = &MapEntry(i * 2 + 1, i * 2 + 1);
                        log.multiappend(change, data, &*vec![])
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

            #[test]
            fn test_order() {
                let _ = env_logger::init();
                let store = $new_store(
                    (0..5).map(|i| OrderIndex(20.into(), i.into()))
                        .chain((0..21).map(|i| OrderIndex(21.into(), i.into())))
                        .chain((0..22).map(|i| OrderIndex(22.into(), i.into())))
                        .collect());
                let horizon = HashMap::new();
                let list: Rc<RefCell<Vec<i32>>> = Default::default();
                let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = Default::default();
                for i in 20..23 {
                    let l = list.clone();
                    upcalls.insert(i.into(), Box::new(move |_,_,&v| { l.borrow_mut().push(v);
                        true
                    }));
                }
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                log.append(22.into(), &4, &[]);
                log.append(20.into(), &2, &[]);
                log.append(21.into(), &3, &[]);
                log.multiappend(&[20.into(),21.into(),22.into()], &-1, &[]);
                log.play_foward(20.into());
                assert_eq!(&**list.borrow(), &[2,3,4,-1,-1,-1][..]);
            }

            #[test]
            fn test_dependent_singlethreaded() {
                let _ = env_logger::init();
                let store = $new_store(
                    (0..10).map(|i| OrderIndex(23.into(), i.into()))
                        .chain((0..24).map(|i| OrderIndex(21.into(), i.into())))
                        .chain((0..25).map(|i| OrderIndex(22.into(), i.into())))
                        .collect());
                let horizon = HashMap::new();
                let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = Default::default();
                let list: Rc<RefCell<Vec<(u32, u32)>>> = Rc::new(RefCell::new(Vec::with_capacity(29)));
                for i in 23..26 {
                    let l = list.clone();
                    upcalls.insert(i.into(), Box::new(move |_,_,&v| { l.borrow_mut().push(v);
                        true
                    }));
                }
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                for i in 1..10 {
                    for j in 23..25 {
                        trace!("append {:?}", (j, i));
                        log.append(j.into(), &(j, i), &[]);
                    }
                    trace!("dappend {:?}", (25, i));
                    log.dependent_multiappend(&[25.into()], &[23.into(), 24.into()], &(25, i), &[]);
                    trace!("finished {:?}", i);
                }
                log.play_until(OrderIndex(25.into(), 1.into()));
                assert_eq!(list.borrow_mut().last(), Some(&(25, 1)));
                assert!(list.borrow_mut().contains(&(23, 1)));
                assert!(list.borrow_mut().contains(&(24, 1)));
                log.play_until(OrderIndex(25.into(), 2.into()));
                assert_eq!(list.borrow_mut().last(), Some(&(25, 2)));
                assert!(list.borrow_mut().contains(&(23, 2)));
                assert!(list.borrow_mut().contains(&(24, 2)));
                log.play_until(OrderIndex(25.into(), 9.into()));
                let mut last_multi_loc = None;
                for i in 1..9 {
                    let multi_loc = list.borrow().iter().position(|l| l == &(25, i));
                    assert!(multi_loc.is_some(), "multi not found @ iter {:?}", i);
                    let multi_loc = multi_loc.unwrap();
                    assert!(last_multi_loc.map(|lml| lml < multi_loc).unwrap_or(true),
                        "multi loc to early @ iter {:?}", i);
                    last_multi_loc = Some(multi_loc);
                    for j in 23..25 {
                        let loc = list.borrow().iter().position(|l| l == &(j, i));
                        assert!(loc.is_some(), "iter {:?} not found", (i, j));
                        let loc = loc.unwrap();
                        assert!(loc < multi_loc, "{:?} to early", (i, j));
                    }
                }
            }

            #[test]
            fn test_dependent_multithreaded() { //TODO multithreaded version
                let _ = env_logger::init();
                let store = $new_store(
                    (0..60).map(|i| OrderIndex(26.into(), i.into()))
                        .chain((0..27).map(|i| OrderIndex(21.into(), i.into())))
                        .chain((0..28).map(|i| OrderIndex(22.into(), i.into())))
                        .collect());
                let s1 = store.clone();
                let s2 = store.clone();
                const ROUNDS: u32 = 61;
                let h1 = thread::spawn(move || {
                    let mut log = FuzzyLog::new(s1, HashMap::new(), HashMap::new());
                    for i in 1..ROUNDS {
                        for j in 26..28 {
                            trace!("append {:?}", (j, i));
                            log.append(j.into(), &(j, i), &[]);
                        }
                    }
                });
                let h2 = thread::spawn(move || {
                    let mut log = FuzzyLog::new(s2, HashMap::new(), HashMap::new());
                    for i in 1..ROUNDS {
                        trace!("dappend {:?}", (28, i));
                        log.dependent_multiappend(&[28.into()], &[26.into(), 27.into()], &(28, i), &[]);
                        trace!("finished {:?}", i);
                    }
                });
                h2.join().unwrap();
                h1.join().unwrap();
                let horizon = HashMap::new();
                let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = Default::default();
                let list: Rc<RefCell<Vec<(u32, u32)>>> = Rc::new(RefCell::new(Vec::with_capacity(29)));
                for i in 26..29 {
                    let l = list.clone();
                    upcalls.insert(i.into(), Box::new(move |_,_,&v| { l.borrow_mut().push(v);
                        true
                    }));
                }
                let mut log = FuzzyLog::new(store, horizon, upcalls);
                let le = log.snapshot(28.into()).unwrap();
                log.play_until(OrderIndex(28.into(), le));
                let inverse_index: BTreeMap<_, _>  =
                    list.borrow().iter().enumerate().map(|(i, &oe)| (oe, i)).collect();
                let mut last_multi_loc = None;
                trace!("ii {:?}", inverse_index);
                for i in 1..ROUNDS {
                    let multi_loc = inverse_index.get(&(28, i));
                    assert!(multi_loc.is_some(), "multi not found @ iter {:?}", i);
                    let multi_loc = multi_loc.cloned().unwrap();
                    assert!(last_multi_loc.map(|lml| lml < multi_loc).unwrap_or(true),
                        "multi loc to early @ iter {:?}", i);
                    let last_loc = last_multi_loc.unwrap_or(0);
                    for &(k, v) in &list.borrow()[last_loc..multi_loc] {
                        if k == 27 {
                            let i26 = inverse_index.get(&(26, v)).cloned().unwrap();
                            assert!(i26 < multi_loc,
                                "(26, {:?}) @ {:?} < {:?}", v, i26, multi_loc);
                        }
                    }
                    last_multi_loc = Some(multi_loc);
                }
            }

            //TODO add test which ensures that multiappeds are linearizeable with respect to single appends

        }//End mod test

    };
}
