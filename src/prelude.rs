
use std::collections::HashMap;
use std::convert::Into;

pub use packets::*;

use self::EntryContents::*;

//TODO FIX
pub trait Store<V: ?Sized> {
    fn insert(&mut self, key: OrderIndex, val: EntryContents<V>) -> InsertResult; //TODO nocopy
    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>>;

    fn multi_append(&mut self, chains: &[OrderIndex], data: &V, deps: &[OrderIndex]) -> InsertResult; //TODO -> MultiAppendResult

    //A dependent multiappend happens-after on the current horizon of some chains
    fn dependent_multi_append(&mut self, chains: &[order], depends_on: &[order], data: &V, deps: &[OrderIndex]) -> InsertResult;

    //fn snapshot(&mut self, chains: &mut [OrderIndex]);
    //fn get_horizon(&mut self, chains: &mut [OrderIndex]);
}

pub type InsertResult = Result<OrderIndex, InsertErr>;
pub type GetResult<T> = Result<T, GetErr>;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum InsertErr {
    AlreadyWritten
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum GetErr {
    NoValue(entry)
}

pub trait Horizon {
    fn get_horizon(&mut self, order) -> entry;
    fn update_horizon(&mut self, order, entry) -> entry;
}

pub type LogResult = Result<(), ()>;
pub type ApplyResult = Result<(), ()>;

pub struct FuzzyLog<V: ?Sized, S, H>
where V: Storeable, S: Store<V>, H: Horizon {
    pub store: S,
    //The horizon contains the nodes which must be read until
    pub horizon: H,
    //The local_horizon contains the nodes which have already been read
    local_horizon: HashMap<order, entry>,
    upcalls: HashMap<order, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r V) -> bool>>,
}

//TODO should impl some trait FuzzyLog instead of providing methods directly to allow for better sharing?
//TODO allow dynamic register of new upcalls?
impl<V: ?Sized, S, H> FuzzyLog<V, S, H>
where V: Storeable, S: Store<V>, H: Horizon{
    pub fn new(store: S, horizon: H,
        upcalls: HashMap<order, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r V) -> bool>>) -> Self {
        FuzzyLog {
            store: store,
            horizon: horizon,
            local_horizon: HashMap::new(),
            upcalls: upcalls,
        }
    }

    pub fn append(&mut self, column: order, data: &V, deps: &[OrderIndex]) -> OrderIndex {
        self.append_entry(column, Data(data, &*deps))
    }

    pub fn try_append(&mut self, column: order, data: &V, deps: &[OrderIndex]) -> Option<OrderIndex> {
        let next_entry = self.horizon.get_horizon(column);
        let insert_loc = (column, next_entry);
        trace!("append num deps: {:?}", deps.len());
        self.store.insert(insert_loc, Data(data, &*deps)).ok().map(|loc| {
            self.horizon.update_horizon(column, loc.1);
            loc
        })
    }

    fn append_entry(&mut self, column: order, ent: EntryContents<V>) -> OrderIndex {
        let mut inserted = false;
        let mut insert_loc = (column, 0.into());
        let mut next_entry = self.horizon.get_horizon(column);
        while !inserted {
            next_entry = next_entry + 1; //TODO jump ahead
            insert_loc = (column, next_entry);
            inserted = match self.store.insert(insert_loc, ent.clone()) {
                Err(..) => false,
                Ok(loc) => {
                    insert_loc = loc;
                    true
                }
            }
        }
        self.horizon.update_horizon(column, insert_loc.1);
        insert_loc
    }

    pub fn multiappend(&mut self, columns: &[order], data: &V, deps: &[OrderIndex]) {
        let columns: Vec<OrderIndex> = columns.into_iter().map(|i| (*i, 0.into())).collect();
        self.store.multi_append(&columns[..], data, &deps[..]); //TODO error handling
        for &(column, _) in &*columns {
            let next_entry = self.horizon.get_horizon(column) + 1; //TODO
            self.horizon.update_horizon(column, next_entry); //TODO
        }
    }

    pub fn dependent_multiappend(&mut self, columns: &[order],
        depends_on: &[order], data: &V, deps: &[OrderIndex]) {
        trace!("dappend: {:?}, {:?}, {:?}", columns, depends_on, deps);
        self.store.dependent_multi_append(columns, depends_on, data, deps);
        //TODO
        for &column in &*columns {
            let next_entry = self.horizon.get_horizon(column) + 1; //TODO
            self.horizon.update_horizon(column, next_entry); //TODO
        }
    }

    pub fn multiappend2(&mut self, columns: &mut [OrderIndex], data: &V, deps: &[OrderIndex]) {
        self.store.multi_append(&columns[..], data, &deps[..]); //TODO error handling
        for &(column, _) in &*columns {
            let next_entry = self.horizon.get_horizon(column) + 1; //TODO
            self.horizon.update_horizon(column, next_entry); //TODO
        }
    }

    pub fn get_next_unseen(&mut self, column: order) -> Option<OrderIndex> {
        let index = self.local_horizon.get(&column).cloned().unwrap_or(0.into()) + 1;
        trace!("next unseen: {:?}", (column, index));
        let ent = self.store.get((column, index)).clone();
        let ent = match ent { Err(GetErr::NoValue(..)) => return None, Ok(e) => e };
        self.play_deps(ent.dependencies());
        match ent.contents() {
            Multiput{data, uuid, columns, deps} => {
                //TODO
                trace!("Multiput {:?}", deps);
                self.read_multiput(data, uuid, columns);
            }
            Data(data, deps) => {
                trace!("Data {:?}", deps);
                self.upcalls.get(&column).map(|f| f(&Uuid::nil(), &(column, index), data.clone())); //TODO clone
            }
            Sentinel(..) => {}
        }
        self.local_horizon.insert(column, index);
        Some((column, index))
    }

    fn read_multiput(&mut self, data: &V, put_id: &Uuid, columns: &[OrderIndex]) {

        for &(column, index) in columns { //TODO only relevent cols
            if (column, index) != (0.into(), 0.into()) {
                trace!("play multiput for col {:?}", column);
                self.play_until_multiput(column, put_id);
            }
        }

        //TODO multiple multiput returns here
        //XXX TODO note multiserver validation happens at the store layer?
        //TODO don't return for sentinels
        'ret: for &(column, index) in columns {
            if (column, index) == (0.into(), 0.into()) { break 'ret }
            self.upcalls.get(&column).map(|f| f(put_id, &(column, index), data));
        }
    }

    fn play_until_multiput(&mut self, column: order, put_id: &Uuid) {
        //TODO instead, just mark all interesting columns not in the
        //     transaction as stale, and only read the interesting
        //     columns of the transaction
        'search: loop {
            let index = self.local_horizon.get(&column).cloned().unwrap_or(0.into()) + 1;
            trace!("seatching for multiput {:?}\n\tat: {:?}", put_id, (column, index));
            let ent = self.store.get((column, index)).clone();
            let ent = match ent {
                Err(GetErr::NoValue(..)) => panic!("invalid multiput."),
                Ok(e) => e
            };
            self.play_deps(ent.dependencies());
            match ent.contents() {
                Multiput{uuid, ..} if uuid == put_id => {
                    trace!("found multiput {:?} for {:?} at: {:?}", put_id, column, index);
                    self.local_horizon.insert(column, index);
                    break 'search
                }
                Sentinel(uuid) if uuid == put_id => {
                    trace!("found sentinel {:?} for {:?} at: {:?}", put_id, column, index);
                    self.local_horizon.insert(column, index);
                    break 'search
                }
                Sentinel(..) => continue 'search,
                Multiput{data, uuid, columns, ..} => {
                    //TODO
                    trace!("Multiput");
                    self.read_multiput(data, uuid, columns);
                    self.local_horizon.insert(column, index);
                }
                Data(data, _) => {
                    trace!("Data");
                    self.upcalls.get(&column).map(|f| f(&Uuid::nil(), &(column, index), data)); //TODO clone
                    self.local_horizon.insert(column, index);
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
        trace!("play_foward");
        //let index = self.horizon.get_horizon(column);
        //trace!("play until {:?}", index);
        //if index == 0.into() { return None }//TODO
        //self.play_until((column, index));
        //Some((column, index))
        let mut res = None;
        while let Some(index) = self.get_next_unseen(column) {
            res = Some(index);
        }
        res
    }

    pub fn local_horizon(&self) -> &HashMap<order, entry> {
        &self.local_horizon
    }

    pub fn snapshot(&mut self, chain: order) -> Option<entry> {
        //TODO clean this a bit
        //TODO buffer the value if it actually exists?
        let end = ::std::u32::MAX.into();
        let err = self.store.get((chain, end)).err().unwrap_or(GetErr::NoValue(end));
        let GetErr::NoValue(last_valid_entry) = err;
        //Zero is not a valid entry so if the store returns that the chain is empty
        if self.local_horizon.get(&chain)
            .map(|&e| e < last_valid_entry && last_valid_entry != 0.into())
            .unwrap_or(last_valid_entry != 0.into()) {
            Some(last_valid_entry)
        }
        else {
            None
        }
    }
}
