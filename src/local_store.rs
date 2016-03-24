
use prelude::*;

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

pub type LocalHorizon = HashMap<order, entry>;

impl Horizon for LocalHorizon {
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

pub struct LocalStore<V: Clone> {
     data: HashMap<OrderIndex, Entry<V>>,
     horizon: HashMap<order, entry>,
}

impl<V: Clone> LocalStore<V> {
    pub fn new() -> Self {
        LocalStore {
            data: HashMap::new(),
            horizon: HashMap::new(),
        }
    }
}

impl<V: ::std::fmt::Debug + Clone> Store<V> for LocalStore<V> {
    fn insert(&mut self, key: OrderIndex, val: EntryContents<V>) -> InsertResult {
        use std::collections::hash_map::Entry::*;
        match self.data.entry(key) {
            Occupied(..) => Err(InsertErr::AlreadyWritten),
            Vacant(v) => {
                let new_val = val.clone_entry();
                trace!("new val {:?}", new_val);
                v.insert(new_val);
                self.horizon.insert(key.0, key.1 + 1);
                Ok(key)
            }
        }
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        HashMap::get(&self.data, &key).cloned().ok_or(GetErr::NoValue)
    }

    fn multi_append(&mut self, chains: &[OrderIndex], data: &V, deps: &[OrderIndex]) -> InsertResult {
        let entr = EntryContents::Multiput{data: data, uuid: &Uuid::new_v4(),
            columns: chains, deps: deps};
        for &(chain, _) in chains {
            let horizon = {
                let horizon_loc = self.horizon.entry(chain).or_insert(1.into());
                let horizon = *horizon_loc;
                *horizon_loc = horizon + 1;
                horizon
            };
            self.insert((chain, horizon), entr.clone());
        }
        Ok((0.into(), 0.into()))
    }
}

impl<V: Copy, S> Store<V> for Mutex<S>
where S: Store<V> {
    fn insert(&mut self, key: OrderIndex, val: EntryContents<V>) -> InsertResult {
        self.lock().unwrap().insert(key, val)
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        self.lock().unwrap().get(key)
    }

    fn multi_append(&mut self, chains: &[OrderIndex], data: &V, deps: &[OrderIndex]) -> InsertResult {
        self.lock().unwrap().multi_append(chains, data, deps)
    }
}

impl<V: Copy, S> Store<V> for RefCell<S>
where S: Store<V> {
    fn insert(&mut self, key: OrderIndex, val: EntryContents<V>) -> InsertResult {
        self.borrow_mut().insert(key, val)
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        self.borrow_mut().get(key)
    }

    fn multi_append(&mut self, chains: &[OrderIndex], data: &V, deps: &[OrderIndex]) -> InsertResult {
        self.borrow_mut().multi_append(chains, data, deps)
    }
}

impl<V: Copy, S> Store<V> for Arc<Mutex<S>>
where S: Store<V> {
    fn insert(&mut self, key: OrderIndex, val: EntryContents<V>) -> InsertResult {
        self.lock().expect("cannot acquire lock").insert(key, val)
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        self.lock().expect("cannot acquire lock").get(key)
    }

    fn multi_append(&mut self, chains: &[OrderIndex], data: &V, deps: &[OrderIndex]) -> InsertResult {
        self.lock().expect("cannot acquire lock").multi_append(chains, data, deps)
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

#[cfg(test)]
pub mod test {

    use prelude::*;
    use local_store::{LocalStore, MapEntry};

	use std::cell::RefCell;
    use std::cmp::Eq;
    use std::collections::HashMap;
    use std::fmt::{Debug, Formatter, Result as FmtResult};
    use std::hash::Hash;
    use std::mem;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    fn new_store<V>(_: Vec<OrderIndex>) -> Arc<Mutex<LocalStore<V>>>
    where V: Clone {
        Arc::new(Mutex::new(LocalStore::new()))
    }

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
	                ord => {
	                    let b: Box<Fn(&MapEntry<K, V>) -> bool> = Box::new(
	                        move |&MapEntry(k, v)| {
	                            re.borrow_mut().insert(k, v);
	                            true
	                        }
	                    );
	                    b
	                } 
	            )),
	            order: ord,
	            local_view: local_view,
	        }
	    }

	    pub fn put(&mut self, key: K, val: V) {
	        self.log.append(self.order, &MapEntry(key, val), vec![]);
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

    //TODO cannonical store tests

    /* TODO
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
    }*/

    #[test]
    fn test_sizes() {
        assert_eq!(mem::size_of::<order>(), 4);
        assert_eq!(mem::size_of::<entry>(), 4);
        assert_eq!(mem::size_of::<(order, entry)>(), 8);
        assert_eq!(mem::size_of::<u16>(), 2);
    }

    general_tests!(super::new_store);
}
