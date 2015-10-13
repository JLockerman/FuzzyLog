
use super::*;

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
