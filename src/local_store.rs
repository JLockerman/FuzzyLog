
use prelude::*;

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::hash::Hash;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

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

impl<V: Copy, S> Store<V> for Mutex<S>
where S: Store<V> {
    fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult {
        self.lock().unwrap().insert(key, val)
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        self.lock().unwrap().get(key)
    }
}

impl<V: Copy, S> Store<V> for RefCell<S>
where S: Store<V> {
    fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult {
        self.borrow_mut().insert(key, val)
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        self.borrow_mut().get(key)
    }
}

impl<V: Copy, S> Store<V> for Arc<Mutex<S>>
where S: Store<V> {
    fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult {
        self.lock().unwrap().insert(key, val)
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        self.lock().unwrap().get(key)
    }
}

impl<H> Horizon for Arc<Mutex<H>>
where H: Horizon {
    fn get_horizon(&mut self, ord: order) -> entry {
        self.lock().unwrap().get_horizon(ord)
    }

    fn update_horizon(&mut self, ord: order, index: entry) -> entry {
        self.lock().unwrap().update_horizon(ord, index)
    }
}


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

#[cfg(test)]
mod test {

    use prelude::*;
    use local_store::{MapEntry};

    use std::cmp::Eq;
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::mem;
    use std::sync::{Arc, Mutex};

    fn new_store<K, V>(_: Vec<OrderIndex>) -> Arc<Mutex<HashMap<K, V>>>
    where K: Eq + Hash {
        Arc::new(Mutex::new(HashMap::new()))
    }

    //TODO cannonical store tests

    #[test]
    fn test_1_column_ni_cannonical() {
        let store = HashMap::new();
        let horizon = HashMap::new();
        let mut log = FuzzyLog::new(store, horizon, HashMap::new());
        log.append(0.into(), MapEntry(0, 1), vec![]);
        log.append(0.into(), MapEntry(1, 17), vec![]);
        let last_index = log.append(0.into(), MapEntry(32, 5), vec![]);
        log.append(1.into(), MapEntry(0, 0), vec![last_index]);
        let cannonical = collect! {
            (0.into(), 1.into()) => Entry::Data(MapEntry(0, 1), vec![]),
            (0.into(), 2.into()) => Entry::Data(MapEntry(1, 17), vec![]),
            (0.into(), 3.into()) => Entry::Data(MapEntry(32, 5), vec![]),
            (1.into(), 1.into()) => Entry::Data(MapEntry(0, 0), vec![last_index])
        };
        assert_eq!(log.store, cannonical);
    }

    #[test]
    fn test_deps_cannonical() {
        let store = HashMap::new();
        let horizon = HashMap::new();
        let mut log = FuzzyLog::new(store, horizon, HashMap::new());
        log.append(0.into(), MapEntry(0, 1), vec![]);
        log.append(0.into(), MapEntry(1, 17), vec![]);
        let last_index = log.append(0.into(), MapEntry(32, 5), vec![]);
        log.append(1.into(), MapEntry(0, 0), vec![last_index]);
        let cannonical = collect! {
            (0.into(), 1.into()) => Entry::Data(MapEntry(0, 1), vec![]),
            (0.into(), 2.into()) => Entry::Data(MapEntry(1, 17), vec![]),
            (0.into(), 3.into()) => Entry::Data(MapEntry(32, 5), vec![]),
            (1.into(), 1.into()) => Entry::Data(MapEntry(0, 0), vec![last_index])
         };
        assert_eq!(log.store, cannonical);
    }

    #[test]
    fn test_sizes() {
        assert_eq!(mem::size_of::<order>(), 4);
        assert_eq!(mem::size_of::<entry>(), 4);
        assert_eq!(mem::size_of::<(order, entry)>(), 8);
        assert_eq!(mem::size_of::<u16>(), 2);
    }

    general_tests!(super::new_store);
}
