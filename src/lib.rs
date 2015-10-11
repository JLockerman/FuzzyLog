#[macro_use] extern crate bitflags;
#[macro_use] extern crate custom_derive;
#[macro_use] extern crate newtype_derive;

use std::collections::HashMap;
use std::collections::hash_map;
use std::marker::PhantomData;

pub trait Store<V> {
    fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult;
    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>>;
}

pub type OrderIndex = (order, entry);

pub type InsertResult = Result<(), InsertErr>;
pub type GetResult<T> = Result<T, GetErr>;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum Entry<V> {
    Data(V, Vec<OrderIndex>),
    TransactionCommit(V, Vec<OrderIndex>),
    TransactionStart(order, Vec<OrderIndex>),
}

impl<V> Entry<V> {
    pub fn dependencies(&mut self) -> &[OrderIndex] {
        use Entry::*;
        match self {
            &mut Data(_, ref deps) => &deps,
            &mut TransactionCommit(_, ref deps) => &deps,
            &mut TransactionStart(_, ref deps) => &deps,
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
    #[derive(Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Clone, Copy, NewtypeFrom, NewtypeAdd(u32), NewtypeMul(u32), NewtypeRem(u32))]
    #[allow(non_camel_case_types)]
    pub struct order(u32);
}

custom_derive! {
    #[derive(Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Clone, Copy, NewtypeFrom, NewtypeAdd(u32), NewtypeMul(u32), NewtypeRem(u32))]
    #[allow(non_camel_case_types)]
    pub struct entry(u32);
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
    _pd: PhantomData<*mut V>,
}

impl<V, S, H> FuzzyLog<V, S, H>
where V: Copy, S: Store<V>, H: Horizon{
    pub fn new(store: S, horizon: H, upcalls: HashMap<order, Box<Fn(V) -> bool>>) -> Self {
        FuzzyLog {
            store: store,
            horizon: horizon,
            local_horizon: HashMap::new(),
            upcalls: upcalls,
            _pd: PhantomData,
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
        let ent = self.store.get((column, index)).clone();
        let ent = match ent { Err(..) => return None, Ok(e) => e };
        match ent {
            Entry::TransactionStart(..) => panic!(), //TODO
            Entry::TransactionCommit(..) => panic!(), //TODO

            Entry::Data(data, deps) => {
                for dep in deps {
                    self.play_until(dep)
                }
                self.upcalls.get(&column).map(|f| f(data));
                self.local_horizon.insert(column, index);
                Some((column, index))
            }
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

    pub fn start_transaction(&mut self, mut columns: Vec<order>, deps: Vec<OrderIndex>) -> Transaction<V, S, H> {
        columns.sort();
        let min = columns[0];
        let mut start_entries = Vec::new();
        for &column in &columns {
            let loc = self.append_entry(column, Entry::TransactionStart(min, deps.clone()));
            start_entries.push(loc);
        }
        Transaction {
            log: self,
            start_entries: start_entries,
        }
    }
}

pub struct Transaction<'t, V, S, H>
where V: 't + Copy, S: 't + Store<V>, H: 't + Horizon {
    log: &'t mut FuzzyLog<V, S, H>,
    start_entries: Vec<OrderIndex>,
}

impl<'t, V, S, H> Transaction<'t, V, S, H>
where V: 't + Copy, S: 't + Store<V>, H: 't + Horizon {
    pub fn commit(self, val: V, deps: Vec<OrderIndex>) -> OrderIndex {
        self.log.append_entry(self.start_entries[0].0,
            Entry::TransactionCommit(val, deps))
    }
}

#[cfg(test)]
mod test {

    use super::*;

    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::mem;
    use std::rc::Rc;

    impl<V: Copy> Store<V> for HashMap<OrderIndex, Entry<V>> {
        fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult {
            use std::collections::hash_map::Entry::*;
            match self.entry(key) {
                Occupied(..) => Err(InsertErr::AlreadyWritten),
                Vacant(v) => {
                    v.insert(val);
                    Ok(())
                }
            }
        }

        fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
            HashMap::get(self, &key).cloned().ok_or(GetErr::NoValue)
        }
    }
/*
    #[derive(Debug)]
    struct Map<K, V, S, H>
    where K: Hash + Eq + Copy, V: Copy,
          S: Store<Entry<MapEntry<K, V>>>,
          H: Horizon {
        map: HashMap<K, V>,
        store: S,
        horizon: H,
        our_order: order,
        observed_horizon: HashMap<(order, entry), u64>,
    }

    impl<K, V, S, H> Map<K, V, S, H>
    where K: Hash + Eq + Copy, V: Copy,
          S: Store<LocalEntry<MapEntry<K, V>>> + Borrow<S>,
          H: Horizon, {

        fn put(&mut self, key: K, val: V) -> Option<V> {
            //TODO handle failures
            let next_entry = self.horizon.get_horizon(self.our_order);
            //TODO self.apply_updates(..)
            //     self.observed_horizon.update(..)
            let inserted = self.store.insert((self.our_order, next_entry),
                LocalEntry{data: EntryKind::Data(MapEntry(key, val))});
            inserted.unwrap(); //TODO
            let last_read_entry = self.horizon.update_horizon(self.our_order, next_entry);
            //TODO last_read_entry?
            self.map.insert(key, val)
        }

        fn get(&mut self, key: K) -> Option<V> {
            panic!()
        }
    }*/

    #[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
    struct MapEntry<K, V>(K, V);

    #[test]
    fn test() {
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
        let cannonical =
            [((0.into(), 1.into()), Entry::Data(MapEntry(0, 1), vec![])),
             ((0.into(), 2.into()), Entry::Data(MapEntry(1, 17), vec![])),
             ((0.into(), 3.into()), Entry::Data(MapEntry(32, 5), vec![])),
             ((1.into(), 1.into()), Entry::Data(MapEntry(0, 0), vec![last_index]))]
                .into_iter().cloned().collect();
        assert_eq!(log.store, cannonical);
    }

    #[test]
    fn test_sizes() {
        assert_eq!(mem::size_of::<order>(), 4);
        assert_eq!(mem::size_of::<entry>(), 4);
        assert_eq!(mem::size_of::<(order, entry)>(), 8);
        assert_eq!(mem::size_of::<u16>(), 2);
    }
}
