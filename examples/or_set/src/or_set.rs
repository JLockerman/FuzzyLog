
use std::collections::{HashMap, HashSet};

pub use fuzzy_log_client::fuzzy_log::log_handle::{
    entry,
    order,
    AtomicWriteHandle,
    GetRes,
    LogHandle,
    OrderIndex,
    ReadHandle,
    TryWaitRes,
    Uuid
};

pub use bincode::{deserialize, serialize_into, Infinite};

pub use serde::{Deserialize, Serialize, Serializer, Deserializer, de::{self, Visitor}};

pub struct OrSet {
    state: OrState,

    my_chain: order,
    handle: LogHandle<[u8]>,

    serialize_cache: Vec<u8>,
}

#[derive(Default)]
struct OrState { state: HashMap<u64, HashSet<GUID>> }

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Op {
    Add(u64),
    Remove(u64, HashSet<GUID>),
    Transaction(Vec<Op>),
}

impl OrSet {

    pub fn new(handle: LogHandle<[u8]>, my_chain: order) -> Self {
        Self {
            state: Default::default(),
            my_chain,
            handle,
            serialize_cache: Vec::default(),
        }
    }

    pub fn async_add(&mut self, val: u64) -> Uuid {
        let op = Op::Add(val);
        self.send(&op)
    }

    pub fn async_remove(&mut self, val: u64) -> Uuid {
        let to_remove = self.state.get(&val)
            .cloned()
            .unwrap_or_else(Default::default);
        self.async_remove_ids(val, to_remove)
    }

    pub fn try_wait_for_op_finished(&mut self) -> Option<Uuid> {
        self.handle.try_wait_for_any_append().ok().map(|(id, _)| id)
    }

    pub fn async_remove_ids(&mut self, val: u64, ids: HashSet<GUID>) -> Uuid {
        let op = Op::Remove(val, ids);
        self.send(&op)
    }

    pub fn async_transaction(&mut self, transaction: Transaction) -> Uuid {
        let ops = transaction.ops;
        let op = Op::Transaction(ops);
        self.send(&op)
    }

    fn send(&mut self, op: &Op) -> Uuid {
        self.serialize_cache.clear();
        serialize_into(&mut self.serialize_cache, op, Infinite).unwrap();

        self.handle.simpler_causal_append(&*self.serialize_cache, &mut [self.my_chain])
    }

    pub fn update(&mut self) {
        let set = &mut self.state;
        self.handle.sync_chain(self.my_chain, |op, _, id| set.update(op, *id)).unwrap();
    }

    // pub fn update_until(&mut self, id: &Uuid) {
    //     let mut found = false;
    //     while !found {
    //         self.handle.snapshot(self.my_chain);
    //         self.play_log(|found_id,_| { found |= id == found_id });
    //     }
    // }

    pub fn update_all(&mut self) {
        let set = &mut self.state;
        self.handle.sync(|op, _, id| set.update(op, *id)).unwrap();
    }

    pub fn local_contains(&self, val: &u64) -> bool {
        self.state.contains_key(&val)
    }

    pub fn local_ids_for(&self, val: &u64) -> Option<impl Iterator<Item=&GUID>> {
        self.state.get(val).map(|ids| ids.iter())
    }

    pub fn get_remote(&mut self) -> usize {
        let mut gotten = 0;
        let set = &mut self.state;
        let _ = self.handle.sync(|op, _, id| {
            set.update(op, *id);
            gotten += 1;
        });
        gotten
    }
}

impl OrState {

    pub fn update(&mut self, bytes: &[u8], id: Uuid) {
        let op: Op = deserialize(bytes).expect("bad msg");
        self.handle_op(op, GUID(id));
    }

    fn handle_op(&mut self, op: Op, id: GUID) {
        match op {
            Op::Add(val) => self.local_add(val, id),
            Op::Remove(val, ids) => self.local_remove(val, ids.iter()),
            Op::Transaction(ops) => {
                for op in ops {
                    self.handle_op(op, id)
                }
            }
        }
    }

    fn local_add(&mut self, val: u64, id: GUID) {
        self.state.entry(val).or_insert_with(HashSet::new).insert(id);
    }

    fn local_remove<'a, Ids>(&mut self, val: u64, ids: Ids)
    where Ids: IntoIterator<Item=&'a GUID>, {
        use std::collections::hash_map::Entry;
        if let Entry::Occupied(mut entry) = self.state.entry(val) {
            for id in ids {
                entry.get_mut().remove(id);
            }
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }
}

impl ::std::ops::Deref for OrState {
    type Target = HashMap<u64, HashSet<GUID>>;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

#[derive(Debug, Default)]
pub struct Transaction {
    ops: Vec<Op>,
}

impl Transaction {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(mut self, val: u64) -> Self {
        self.ops.push(Op::Add(val));
        self
    }

    pub fn remove(mut self, val: u64, ids: HashSet<GUID>) -> Self {
        self.ops.push(Op::Remove(val, ids));
        self
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Copy, Clone)]
pub struct GUID(Uuid);

impl Serialize for GUID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_bytes(self.0.as_bytes())
    }
}

impl<'de> Deserialize<'de> for GUID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        use ::std::fmt;
        struct GUIDVisitor;

        impl<'d> Visitor<'d> for GUIDVisitor {
            type Value = GUID;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a UUID")
            }

            fn visit_bytes<E>(self, value: &[u8]) -> Result<GUID, E>
            where
                E: de::Error,
            {
                if let Ok(id) = Uuid::from_bytes(value) {
                    Ok(GUID(id))
                } else {
                    Err(E::custom(format!("invalid UUID")))
                }
            }

        }

        deserializer.deserialize_bytes(GUIDVisitor)
    }
}
