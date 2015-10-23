
#[macro_use] extern crate bitflags;
#[macro_use] extern crate custom_derive;
#[macro_use] extern crate grabbag_macros;
#[macro_use] extern crate log;
#[macro_use] extern crate newtype_derive;

extern crate hyper;
extern crate rustc_serialize;
extern crate uuid;

use std::collections::HashMap;
use std::collections::hash_map;
use std::convert::Into;
use std::time::Duration;
use uuid::Uuid;

extern crate rusoto;

pub mod local_horizons;
pub mod dynamo_store;

pub trait Store<V> {
    fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult;
    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>>;
}

pub type OrderIndex = (order, entry);

pub type InsertResult = Result<(), InsertErr>;
pub type GetResult<T> = Result<T, GetErr>;

#[derive(Debug, Hash, PartialEq, Eq, Clone, RustcDecodable, RustcEncodable)]
pub enum Entry<V> {
    Data(V, Vec<OrderIndex>),
    TransactionCommit{uuid: Uuid, start_entries: Vec<OrderIndex>}, //TODO do commits need dependencies?
    TransactionStart(V, order, Uuid, Vec<OrderIndex>), //TODO do Starts need dependencies?
    TransactionAbort(Uuid), //TODO do Starts need dependencies?
}

impl<V> Entry<V> {
    pub fn dependencies(&self) -> &[OrderIndex] {
        use Entry::*;
        match self {
            &Data(_, ref deps) => &deps,
            &TransactionCommit{..} => &[],
            &TransactionAbort(..) => &[],
            &TransactionStart(_, _, _, ref deps) => &deps,
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum InsertErr {
    AlreadyWritten
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum GetErr {
    NoValue
}

custom_derive! {
    #[derive(Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Clone, Copy, RustcDecodable, RustcEncodable, NewtypeFrom, NewtypeAdd(u32), NewtypeSub(u32), NewtypeMul(u32), NewtypeRem(u32))]
    #[allow(non_camel_case_types)]
    pub struct order(u32);
}

custom_derive! {
    #[derive(Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Clone, Copy, RustcDecodable, RustcEncodable, NewtypeFrom, NewtypeAdd(u32), NewtypeSub(u32), NewtypeMul(u32), NewtypeRem(u32))]
    #[allow(non_camel_case_types)]
    pub struct entry(u32);
}

pub fn order_index_to_u64(o: OrderIndex) -> u64 {
    let hig: u32 = o.0.into();
    let low: u32 = o.1.into();
    let hig = (hig as u64) << 32;
    let low = low as u64;
    hig | low
}

pub fn u64_to_order_index(u: u64) -> OrderIndex {
    let ord = (u & 0xFFFFFFFF00000000) >> 32;
    let ent = u & 0x00000000FFFFFFFF;
    let ord: u32 = ord as u32;
    let ent: u32 = ent as u32;
    (ord.into(), ent.into())
}

pub trait Horizon {
    fn get_horizon(&mut self, order) -> entry;
    fn update_horizon(&mut self, order, entry) -> entry;
}

pub type LogResult = Result<(), ()>;
pub type ApplyResult = Result<(), ()>;

impl Horizon for HashMap<order, entry> {
    fn get_horizon(&mut self, ord: order) -> entry {
        self.get(&ord).cloned().unwrap_or(0.into())
    }

    fn update_horizon(&mut self, ord: order,  new_entry: entry) -> entry {
        match self.entry(ord) {
            hash_map::Entry::Occupied(mut o) => {
                let old_val = o.get().clone();
                if old_val < new_entry {
                    o.insert(new_entry);
                    new_entry
                }
                else {
                    old_val
                }
            }
            hash_map::Entry::Vacant(v) => {
                v.insert(new_entry).clone()
            }
        }
    }
}

pub struct FuzzyLog<V, S, H>
where V: Copy, S: Store<V>, H: Horizon {
    pub store: S,
    pub horizon: H,
    local_horizon: HashMap<order, entry>,
    upcalls: HashMap<order, Box<Fn(V) -> bool>>,
}

//TODO should impl some trait FuzzyLog instead of providing methods directly to allow for better sharing?
//TODO allow dynamic register of new upcalls?
impl<V, S, H> FuzzyLog<V, S, H>
where V: Copy, S: Store<V>, H: Horizon{
    pub fn new(store: S, horizon: H, upcalls: HashMap<order, Box<Fn(V) -> bool>>) -> Self {
        FuzzyLog {
            store: store,
            horizon: horizon,
            local_horizon: HashMap::new(),
            upcalls: upcalls,
        }
    }

    pub fn append(&mut self, column: order, data: V, deps: Vec<OrderIndex>) -> OrderIndex {
        self.append_entry(column, Entry::Data(data, deps))
    }

    pub fn try_append(&mut self, column: order, data: V, deps: Vec<OrderIndex>) -> Option<OrderIndex> {
        let next_entry = self.horizon.get_horizon(column);
        let insert_loc = (column, next_entry);
        self.store.insert(insert_loc, Entry::Data(data, deps)).ok().map(|_| {
            self.horizon.update_horizon(column, next_entry);
            insert_loc
        })
    }

    fn append_entry(&mut self, column: order, ent: Entry<V>) -> OrderIndex {
        let mut inserted = false;
        let mut insert_loc = (column, 0.into());
        let mut next_entry = self.horizon.get_horizon(column);
        while !inserted {
            next_entry = next_entry + 1; //TODO jump ahead
            insert_loc = (column, next_entry);
            inserted = self.store.insert(insert_loc, ent.clone()).is_ok();
        }
        self.horizon.update_horizon(column, next_entry);
        insert_loc
    }

    pub fn get_next_unseen(&mut self, column: order) -> Option<OrderIndex> {
        let index = self.local_horizon.get(&column).cloned().unwrap_or(0.into()) + 1;
        trace!("next unseen: {:?}", (column, index));
        let ent = self.store.get((column, index)).clone();
        let ent = match ent { Err(GetErr::NoValue) => return None, Ok(e) => e };
        self.play_deps(ent.dependencies());
        match ent {
            Entry::TransactionStart(data, commit_column, uuid, _) => {
                self.play_transaction((column, index), commit_column, uuid, data);
            }
            Entry::TransactionCommit{..} => {} //TODO skip?
            Entry::TransactionAbort(..) => {} //TODO skip?

            Entry::Data(data, _) => {
                self.upcalls.get(&column).map(|f| f(data));
            }
        }
        self.local_horizon.insert(column, index);
        Some((column, index))
    }

    fn play_transaction(&mut self, (start_column, _): OrderIndex, commit_column: order, start_uuid: Uuid, data: V) {
        let mut next_entry = self.local_horizon.get(&commit_column).cloned()
            .unwrap_or(0.into()) + 1;

        let transaction_start_entries;
        let mut timed_out = false;
        'find_commit: loop {
            trace!("transaction reading: {:?}", (commit_column, next_entry));
            let next = self.store.get((commit_column, next_entry)).clone();
            match next {
                Err(GetErr::NoValue) if timed_out => {
                    let inserted = self.store.insert((commit_column, next_entry),
                        Entry::TransactionAbort(start_uuid));
                    if let Ok(..) = inserted {
                        return
                    }
                }
                Err(GetErr::NoValue) => {
                    std::thread::sleep(Duration::from_millis(100));
                    timed_out = true;
                    continue 'find_commit
                }
                Ok(Entry::TransactionCommit{uuid, start_entries}) =>
                    if uuid == start_uuid {
                        transaction_start_entries = start_entries;
                        break 'find_commit
                    },
                Ok(Entry::TransactionAbort(uuid)) =>
                    if uuid == start_uuid {
                        return //local_horizon is updated in get_next_unseen
                    },
                Ok(..) => {}
            }
            next_entry = next_entry + 1;
            timed_out = false;
            continue 'find_commit
        }

        self.upcalls.get(&start_column).map(|f| f(data));

        for (column, index) in transaction_start_entries {
            if column != start_column {
                self.play_until((column, index - 1)); //TODO underflow
                let start_entry = self.store.get((column, index)).clone().expect("invalid commit entry");
                if let Entry::TransactionStart(data, commit_col, uuid, deps) = start_entry {
                    assert_eq!(commit_column, commit_col);
                    assert_eq!(start_uuid, uuid);
                    self.play_deps(&deps);
                    self.upcalls.get(&column).map(|f| f(data));
                    self.local_horizon.insert(column, index);
                }
                else {
                    panic!("invalid start entry {:?} or commit entry", (column, index))
                }
            }
        }
    }

    fn play_deps(&mut self, deps: &[OrderIndex]) {
        for &dep in deps {
            self.play_until(dep)
        }
    }

    pub fn play_until(&mut self, dep: OrderIndex) {
        //TODO end if run out?
        while self.local_horizon.get(&dep.0).cloned().unwrap_or(0.into()) < dep.1 {
            self.get_next_unseen(dep.0);
        }
    }

    pub fn play_foward(&mut self, column: order) -> Option<OrderIndex> {
        let index = self.horizon.get_horizon(column);
        if index == 0.into() { return None }//TODO
        self.play_until((column, index));
        Some((column, index))
    }

    pub fn start_transaction(&mut self, mut columns: Vec<(order, V)>, deps: Vec<OrderIndex>) -> Transaction<V, S, H> {
        columns.sort_by(|a, b| a.0.cmp(&b.0));
        //TODO assert columns.dedup()
        let min = columns[0].0;
        let mut start_entries = Vec::new();
        let transaction_id = Uuid::new_v4();
        for &(column, val)  in &columns {
            let loc = self.append_entry(column,
                Entry::TransactionStart(val, min, transaction_id, deps.clone()));
            start_entries.push(loc)
        }
        Transaction {
            log: self,
            start_entries: Some(start_entries),
            uuid: transaction_id,
        }
    }
}

#[must_use]
pub struct Transaction<'t, V, S, H>
where V: 't + Copy, S: 't + Store<V>, H: 't + Horizon {
    log: &'t mut FuzzyLog<V, S, H>,
    start_entries: Option<Vec<OrderIndex>>,
    uuid: Uuid,
}

impl<'t, V, S, H> Transaction<'t, V, S, H>
where V: 't + Copy, S: 't + Store<V>, H: 't + Horizon {
    pub fn commit(mut self) -> (OrderIndex, Vec<OrderIndex>) {
        let start_entries = self.start_entries.take().expect("Double committed transaction");
        (self.log.append_entry(start_entries[0].0,
            Entry::TransactionCommit {
                uuid: self.uuid,
                start_entries: start_entries.clone()
            }),
        start_entries)
    }

    //TODO pub fn add(&mut self, column, val)
    //TODO pub fn abort(self)
}

impl<'t, V, S, H> Drop for Transaction<'t, V, S, H>
where V: 't + Copy, S: 't + Store<V>, H: 't + Horizon {
    fn drop(&mut self) {
        let start_entries = self.start_entries.take();
        if let Some(entries) = start_entries {
            self.log.append_entry(entries[0].0,
                Entry::TransactionAbort(self.uuid));
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::mem;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use std::fmt::{Debug, Formatter, Result as FmtResult};


    #[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, RustcDecodable, RustcEncodable)]
    pub struct MapEntry<K, V>(pub K, pub V);

    pub struct Map<K, V, S, H>
    where K: Hash + Eq + Copy, V: Copy,
          S: Store<MapEntry<K, V>>,
          H: Horizon, {
        pub log: FuzzyLog<MapEntry<K, V>, S, H>,
        pub local_view: Rc<RefCell<HashMap<K, V>>>,
        pub order: order,
    }

    impl<K: 'static, V: 'static, S, H> Map<K, V, S, H>
    where K: Hash + Eq + Copy, V: Copy,
          S: Store<MapEntry<K, V>>,
          H: Horizon, {

        pub fn new(store: S, horizon: H, ord: order) -> Map<K, V, S, H> {
            let local_view = Rc::new(RefCell::new(HashMap::new()));
            let re = local_view.clone();
            Map {
                log: FuzzyLog::new(store, horizon, collect!(
                    ord =>
                    Box::new(
                        move |MapEntry(k, v)| {
                            re.borrow_mut().insert(k, v);
                            true
                        }) as Box<Fn(_) -> _>
                )),
                order: ord,
                local_view: local_view,
            }
        }

        pub fn put(&mut self, key: K, val: V) {
            self.log.append(self.order, MapEntry(key, val), vec![]);
            //TODO deps
        }

        pub fn get(&mut self, key: K) -> Option<V> {
            self.log.play_foward(self.order);
            self.local_view.borrow().get(&key).cloned()
        }
    }

    impl<K, V, S, H> Debug for Map<K, V, S, H>
    where K: Hash + Eq + Copy + Debug, V: Copy + Debug,
          S: Store<MapEntry<K, V>>,
          H: Horizon, {

        fn fmt(&self, formatter: &mut Formatter) -> FmtResult {
            formatter.debug_struct("Map").field("local_view", &self.local_view).finish()
        }
    }

    #[test]
    fn test_threaded() {
        let store = Arc::new(Mutex::new(HashMap::new()));
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
        let store = HashMap::new();
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
        let store = HashMap::new();
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
        log.append(0.into(), MapEntry(0, 1), vec![]);
        log.append(0.into(), MapEntry(1, 17), vec![]);
        let last_index = log.append(0.into(), MapEntry(32, 5), vec![]);
        log.append(1.into(), MapEntry(0, 0), vec![last_index]);
        log.play_foward(0.into());
        assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
        let cannonical = collect! {
            (0.into(), 1.into()) => Entry::Data(MapEntry(0, 1), vec![]),
            (0.into(), 2.into()) => Entry::Data(MapEntry(1, 17), vec![]),
            (0.into(), 3.into()) => Entry::Data(MapEntry(32, 5), vec![]),
            (1.into(), 1.into()) => Entry::Data(MapEntry(0, 0), vec![last_index])
        };
        assert_eq!(log.store, cannonical);
    }

    #[test]
    fn test_deps() {
        let store = HashMap::new();
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
        log.append(0.into(), MapEntry(0, 1), vec![]);
        log.append(0.into(), MapEntry(1, 17), vec![]);
        let last_index = log.append(0.into(), MapEntry(32, 5), vec![]);
        log.append(1.into(), MapEntry(0, 0), vec![last_index]);
        log.play_foward(1.into());
        assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
        let cannonical = collect! {
            (0.into(), 1.into()) => Entry::Data(MapEntry(0, 1), vec![]),
            (0.into(), 2.into()) => Entry::Data(MapEntry(1, 17), vec![]),
            (0.into(), 3.into()) => Entry::Data(MapEntry(32, 5), vec![]),
            (1.into(), 1.into()) => Entry::Data(MapEntry(0, 0), vec![last_index])
         };
        assert_eq!(log.store, cannonical);
    }

    #[test]
    fn test_transaction_1() {
        let store = HashMap::new();
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
        let mutations = vec![(0.into(), MapEntry(0, 1)), (1.into(), MapEntry(4.into(), 17)), (3.into(), MapEntry(22, 9))];
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
        let store = Arc::new(Mutex::new(HashMap::new()));
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
        let store = Arc::new(Mutex::new(HashMap::new()));
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
        let store = Arc::new(Mutex::new(HashMap::new()));
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

        forget(log.start_transaction(mutation, vec![]));

        log.play_foward(0.into());
        assert_eq!(*map.borrow(), collect![0 => 1, 1 => 17]);
    }

    #[test]
    fn test_sizes() {
        assert_eq!(mem::size_of::<order>(), 4);
        assert_eq!(mem::size_of::<entry>(), 4);
        assert_eq!(mem::size_of::<(order, entry)>(), 8);
        assert_eq!(mem::size_of::<u16>(), 2);
    }
}
