#![allow(dead_code)]
#![allow(unused_variables)]

// Try multiple partitions per client
// add a optional return snapshot, which returns what the snapshotted OrderIndex is
// use to know when we can return observations and resnapshot for a partition

pub extern crate bincode;
extern crate fuzzy_log_client;
pub extern crate serde;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate env_logger;

#[cfg(test)]
extern crate fuzzy_log_server;

#[macro_use]
extern crate matches;

use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::cell::RefCell;

pub use fuzzy_log_client::fuzzy_log::log_handle::{entry, order, AtomicWriteHandle, GetRes,
                                                  LogHandle, OrderIndex, ReadHandle, TryWaitRes,
                                                  Uuid};

pub use bincode::{deserialize, serialize_into, Infinite};

pub use serde::{Deserialize, Serialize};

mod tester;

struct Args {}

fn main() {
    let args: Args = unimplemented!();
    if unimplemented!() {
        do_put_experiment(args)
    } else {
        do_get_experiment(args)
    }
}

fn do_put_experiment(args: &Args) {
    run_putter()
}

fn run_putter(args: &Args) {
    let set = OrSet::new(
        (args.server_id + 1).into(),
        remote_chains.iter().cloned(),
        builder,
    );
}

pub type GUID = (u32, u32);

#[derive(Debug, Serialize, Deserialize)]
enum Op {
    Add(u64),
    Remove(u64, HashSet<GUID>),
    Transaction(Vec<Op>),
}

struct OrSet {
    state: HashMap<u64, HashSet<GUID>>,
    my_chain: order,
    remote_chains: HashSet<order>,
    writer: AtomicWriteHandle<[u8]>,
    reader: ReadHandle<[u8]>,
    serialize_cache: Vec<u8>,
    deps: HashMap<order, entry>,
    deps_cache: Vec<OrderIndex>,
    // transactions: HashMap<u64, Rc<RefCell<TransactionState>>>,
}

// #[derive(Debug)]
// struct TransactionState {
//     adds: HashSet<u64>,
//     removes: HashSet<u64>,
//     pending: VecDeque<PendingOp>,
// }

#[derive(Debug)]
enum PendingOp {
    Op(Op),
    Contains,
    IdsFor,
}

impl OrSet {

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

    pub fn async_add(&mut self, val: u64) -> Uuid {
        let op = Op::Add(val);
        self.send(&op)
    }

    pub fn async_remove(&mut self, val: u64, ids: HashSet<GUID>) -> Uuid {
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
        self.deps_cache.clear();

        serialize_into(&mut self.serialize_cache, op, Infinite).unwrap();
        self.deps_cache.extend(self.deps.drain().map(|oi| OrderIndex::from(oi)));
        self.writer.async_append(self.my_chain, &*self.serialize_cache, &*self.deps_cache)
    }

    pub fn update(&mut self) {
        self.reader.snapshot(self.my_chain);
        self.play_log(|_,_| {});
    }

    pub fn update_until(&mut self, id: &Uuid) {
        let mut found = false;
        while !found {
            self.reader.snapshot(self.my_chain);
            self.play_log(|found_id,_| { found |= id == found_id });
        }
    }

    pub fn update_all(&mut self) {
        self.reader.snapshot(0.into());
        self.play_log(|_,_| {});
    }

    fn play_log<F: FnMut(&Uuid, &[OrderIndex])>(&mut self, mut f: F) {
        'play: loop {
            let (op, id) = match self.reader.get_next2() {
                Err(GetRes::Done) => break 'play,
                Err(e) => panic!(e),
                Ok((bytes, locs, id)) => {
                    f(id, locs);
                    let op: Op = deserialize(bytes).expect("bad msg");
                    for &OrderIndex(o, i) in locs {
                        if o == self.my_chain { continue; }
                        if o == 0.into() { continue; }

                        let max = self.deps.entry(o).or_insert(i);
                        *max = ::std::cmp::max(*max, i);
                    }
                    let OrderIndex(o, i) = locs[0];
                    (op, (o.into(), i.into()))
                }
            };
            self.handle_op(op, id);
        }
    }

    pub fn local_contains(&self, val: &u64) -> bool {
        self.state.contains_key(&val)
    }

    pub fn local_ids_for(&self, val: &u64) -> Option<::std::collections::hash_set::Iter<GUID>> {
        self.state.get(val).map(|ids| ids.iter())
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
