
use std::collections::HashMap;
use std::convert::Into;
use std::time::Duration;
use std::thread;
use uuid::Uuid;

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
        match self {
            &Entry::Data(_, ref deps) => &deps,
            &Entry::TransactionCommit{..} => &[],
            &Entry::TransactionAbort(..) => &[],
            &Entry::TransactionStart(_, _, _, ref deps) => &deps,
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
                    thread::sleep(Duration::from_millis(100));
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
