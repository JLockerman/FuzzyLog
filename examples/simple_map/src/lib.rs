
extern crate bincode;
extern crate fuzzy_log_client;

pub extern crate serde;

#[macro_use] extern crate serde_derive;

use bincode::{
    deserialize,
    serialize,
};

use std::{
    collections::HashMap,
    hash::Hash,
};

use serde::{
    Deserialize,
    Serialize,
};

pub use fuzzy_log_client::{
    LogHandle, order,
};

#[cfg(test)]
mod tests;

pub struct Map<K, V> {

    // The Map contains a handle for the fuzzy log.
    log: LogHandle<[u8]>,

    // a local view to allow the the map materializing the logs' state
    local_view: HashMap<K, V>,

    // which partition we write to
    append_to: order,
}

#[derive(Debug, Serialize, Deserialize)]
enum Op<K, V> {
    Put(K, V),
    Rem(K),
    PutOnNotFound(K, V),
}

impl<K, V> Map<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Hash + Eq,
    V: Serialize + for<'de> Deserialize<'de>, {

    pub fn put(&mut self, key: &K, val: &V) {
        // To insert an entry into the map we simply append the mutation to the log
        // and wait for the append to be completed.
        let _ = self.log.simple_append(
            &Op::Put(key, val).serialize(),
            &mut [self.append_to]
        );
        // Since this API doesn't return a value, we don't need to play the log.
    }

    pub fn get(&mut self, key: &K) -> Option<&mut V> {
        // To read a value from the map, we first synchronize out local view
        // with the log
        self.sync();
        // after which we return the value form our local view
        self.get_cached(key)
    }

    pub fn get_cached(&mut self, key: &K) -> Option<&mut V> {
        // We can also return query results based on our cached copy of the
        // datatstructure.
        // This is much faster, but not linearizable.
        self.local_view.get_mut(key)
    }

    pub fn put_on_not_found(&mut self, key: &K, val: &V) {
        let _ = self.log.simple_append(
            &Op::PutOnNotFound(key, val).serialize(),
            &mut [self.append_to]
        );
    }

    pub fn remove(&mut self, key: &K) {
        let _ = self.log.simple_append(
            &Op::Rem::<_, V>(key).serialize(),
            &mut [self.append_to]
        );
    }

    pub fn contains_key(&mut self, key: &K) -> bool {
        self.sync();
        self.get(key).is_some()
    }

    fn sync(&mut self) {
        let local_view = &mut self.local_view;
        let _ = self.log.sync_events(|e| update_local(local_view, e.data));
    }

    // In a more realistic setting we would most likely wish to perform
    // multiple queries at a time using the LogHandle's async_* APIs.
}

fn update_local<K, V>(local_view: &mut HashMap<K, V>, data: &[u8])
where
    K: for<'de> Deserialize<'de> + Hash + Eq,
    V: for<'de> Deserialize<'de>, {
    match deserialize(data).unwrap() {
        Op::Put(key, val) => { local_view.insert(key, val); },
        Op::Rem(key) => { local_view.remove(&key); },
        Op::PutOnNotFound(key, val) => {
            local_view.entry(key).or_insert(val);
        },
    }
}

impl<K, V> Map<K, V>
where K: Hash + Eq {
    pub fn from_log_handle(log: LogHandle<[u8]>, append_to: order) -> Self {
        (log, append_to).into()
    }
}

impl<K, V> From<(LogHandle<[u8]>, order)> for Map<K, V>
where K: Hash + Eq {
    fn from((log, append_to): (LogHandle<[u8]>, order)) -> Self {
        Self {
            log,
            local_view: HashMap::default(),
            append_to,
        }
    }
}


impl<K, V> Op<K, V> {
    fn serialize(&self) -> Vec<u8>
    where
        K: Serialize,
        V: Serialize, {
        serialize(self, bincode::Infinite).unwrap()
    }
}
