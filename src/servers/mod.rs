use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::rc::Rc;

use prelude::*;

pub mod tcp;
pub mod udp;

mod trie;

struct ServerLog {
    log: HashMap<OrderIndex, Rc<Entry<()>>>,
    horizon: HashMap<order, entry>,
    last_lock: u64,
    last_unlock: u64,
    total_servers: u32,
    this_server_num: u32,
    _seen_ids: HashSet<Uuid>,
}

impl ServerLog {
    fn new(this_server_num: u32, total_servers: u32) -> Self {
        //TODO
        //assert!(this_server_num > 0);
        assert!(this_server_num <= total_servers,
            "this_server_num <= total_servers, {:?} <= {:?}",
            this_server_num, total_servers);
        ServerLog {
            log: HashMap::new(),
            horizon: HashMap::new(),
            last_lock: 0,
            last_unlock: 0,
            _seen_ids: HashSet::new(),
            this_server_num: this_server_num,
            total_servers: total_servers,
        }
    }

    fn handle_op(&mut self, val: &mut Entry<()>) -> bool {
        let kind = val.kind;
        match kind.layout() {
            EntryLayout::Multiput | EntryLayout::Sentinel => {
                trace!("SERVER {:?} multiput", self.this_server_num);
                //TODO handle sentinels
                //TODO check TakeLock flag?
                if self.try_lock(val.lock_num()) {
                    assert!(self._seen_ids.insert(val.id));
                    assert!(self.last_lock == self.last_unlock + 1 || self.last_lock == 0,
                        "SERVER {:?} l {:?} == ul {:?} + 1",
                        self.this_server_num, self.last_lock, self.last_unlock);
                    assert!(self.last_lock == val.lock_num(),
                        "SERVER {:?} l {:?} == vl {:?}",
                        self.this_server_num, self.last_lock, val.lock_num());
                    let mut sentinel_start_index = None;
                    {
                        val.kind.insert(EntryKind::ReadSuccess);
                        let locs = val.locs_mut();
                        //TODO select only relevent chains
                        //trace!("SERVER {:?} Horizon A {:?}", self.horizon);
                        'update_append_horizon: for i in 0..locs.len() {
                            if locs[i] == (0.into(), 0.into()) {
                                sentinel_start_index = Some(i + 1);
                                break 'update_append_horizon
                            }
                            let chain = locs[i].0;
                            if self.stores_chain(chain) {
                                let hor: entry = self.increment_horizon(chain);
                                locs[i].1 = hor;
                            }
                        }
                        if let Some(ssi) = sentinel_start_index {
                            for i in ssi..locs.len() {
                                assert!(locs[i] != (0.into(), 0.into()));
                                if self.stores_chain(locs[i].0) {
                                    let hor: entry = self.increment_horizon(locs[i].0);
                                    locs[i].1 = hor;
                                }
                            }
                        }
                        //trace!("SERVER {:?} Horizon B {:?}", self.horizon);
                        trace!("SERVER {:?} locs {:?}", self.this_server_num, locs);
                        trace!("SERVER {:?} ssi {:?}", self.this_server_num, sentinel_start_index);
                    }
                    trace!("SERVER {:?} appending at {:?}", self.this_server_num, val.locs());
                    let contents = Rc::new(val.clone());
                    'emplace: for &loc in val.locs() {
                        if loc == (0.into(), 0.into()) {
                            break 'emplace
                        }
                        if self.stores_chain(loc.0) {
                            self.log.insert(loc, contents.clone());
                        }
                        // trace!("SERVER {:?} appended at {:?}", loc);
                    }
                    if let Some(ssi) = sentinel_start_index {
                        val.kind.remove(EntryKind::Multiput);
                        val.kind.insert(EntryKind::Sentinel);
                        let contents = Rc::new(val.clone());
                        trace!("SERVER {:?} sentinal locs {:?}", self.this_server_num, &val.locs()[ssi..]);
                        for &loc in &val.locs()[ssi..] {
                            if self.stores_chain(loc.0) {
                                self.log.insert(loc, contents.clone());
                            }
                            // trace!("SERVER {:?} appended at {:?}", loc);
                        }
                        val.kind.remove(EntryKind::Sentinel);
                        val.kind.insert(EntryKind::Multiput);
                    }
                } else {
                    trace!("SERVER {:?} wrong lock {} @ ({},{})", self.this_server_num,
                        val.lock_num(), self.last_lock, self.last_unlock);
                }
            }
            EntryLayout::Read => {
                trace!("SERVER {:?} Read", self.this_server_num);
                let loc = unsafe { val.as_data_entry_mut().flex.loc };
                match self.log.entry(loc) {
                    Vacant(..) => {
                        trace!("SERVER {:?} Read Vacant entry {:?}", self.this_server_num, loc);
                        if val.kind == EntryKind::Read {
                            //TODO validate lock
                            //     this will come after per-chain locks
                            let l = loc.0;
                            let last_entry = self.horizon.get(&l).cloned().unwrap_or(0.into());
                            assert!(last_entry == 0.into() || last_entry < loc.1,
                                "{:?} >= {:?}", last_entry, loc);
                            let (old_id, old_loc) = unsafe {
                                (val.id, val.as_data_entry().flex.loc)
                            };
                            *val = EntryContents::Data(&(), &[(l, last_entry)]).clone_entry();
                            val.id = old_id;
                            val.kind = EntryKind::NoValue;
                            unsafe {
                                val.as_data_entry_mut().flex.loc = old_loc;
                            }
                            trace!("SERVER {:?} empty read {:?}", self.this_server_num, loc)
                        }
                        else {
                            trace!("SERVER {:?} nop {:?}", self.this_server_num, val.kind)
                        }
                    }
                    Occupied(mut e) => {
                        trace!("SERVER {:?} Read Occupied entry {:?} {:?}", self.this_server_num, loc, e.get().id);
                        let packet = e.get_mut();
                        *val = (**packet).clone();
                        // trace!("SERVER {:?} returning {:?}", packet);
                    }
                }
            }

            EntryLayout::Data => {
                trace!("SERVER {:?} Append", self.this_server_num);
                //TODO locks
                if !self.is_unlocked() {
                    trace!("SERVER {:?} {:?} > {:?}", self.this_server_num, self.last_lock, self.last_unlock);
                    trace!("SERVER {:?} append during lock", self.this_server_num);
                    return true
                }
                let loc = {
                    let l = unsafe { &mut val.as_data_entry_mut().flex.loc };
                    debug_assert!(self.stores_chain(l.0),
                        "tried to store {:?} at server {:?} of {:?}",
                        l, self.this_server_num, self.total_servers);
                    let hor = self.increment_horizon(l.0);
                    l.1 = hor;
                    *l
                };

                match self.log.entry(loc) {
                    Vacant(e) => {
                        trace!("SERVER {:?} Writing vacant entry {:?}", self.this_server_num, loc);
                        assert_eq!(unsafe { val.as_data_entry().flex.loc }, loc);
                        //TODO validate lock
                        val.kind.insert(EntryKind::ReadSuccess);
                        let packet = Rc::new(val.clone());
                        e.insert(packet);
                    }
                    _ => {
                        unreachable!("SERVER Occupied entry {:?}", loc)
                        //*val = (**packet).clone();
                    }
                }
            }

            EntryLayout::Lock => {
                trace!("SERVER {:?} Lock", self.this_server_num);
                let lock_num = unsafe { val.as_lock_entry().lock };
                if kind.is_taking_lock() {
                    trace!("SERVER {:?} TakeLock {:?}", self.this_server_num, self.last_lock);
                    let acquired_loc = self.try_lock(lock_num);
                    if acquired_loc {
                        val.kind.insert(EntryKind::ReadSuccess);
                    }
                    else {
                        unsafe { val.as_lock_entry_mut().lock = self.last_lock };
                    }
                    return true
                }
                else {
                    trace!("SERVER {:?} UnLock {:?}", self.this_server_num, self.last_lock);
                    if lock_num == self.last_lock {
                        trace!("SERVER {:?} Success", self.this_server_num);
                        val.kind.insert(EntryKind::ReadSuccess);
                        self.last_unlock = self.last_lock;
                    }
                    return false
                }
            }
        }
        true
    }

    fn stores_chain(&self, chain: order) -> bool {
        chain % self.total_servers == self.this_server_num.into()
    }

    fn increment_horizon(&mut self, chain: order) -> entry {
        let h = self.horizon.entry(chain).or_insert(0.into());
        *h = *h + 1;
        *h
    }

    fn try_lock(&mut self, lock_num: u64) -> bool {
        trace!("SERVER {:?} is unlocked {:?}", self.this_server_num, self.is_unlocked());
        if self.is_unlocked() {
            if lock_num == self.last_lock + 1 {
                trace!("SERVER {:?} Lock {:?}", self.this_server_num, lock_num);
                self.last_lock = lock_num;
                true
            }
            else if lock_num == 0 {
                trace!("SERVER {:?} NoLock", self.this_server_num);
                true
            }
            else {
                trace!("SERVER {:?} wrong lock", self.this_server_num);
                false
            }
        }
        else {
            trace!("SERVER {:?} already locked", self.this_server_num);
            false
        }
    }

    fn is_unlocked(&self) -> bool {
        self.last_lock == self.last_unlock
    }

    fn server_num(&self) -> u32 {
        self.this_server_num
    }
}
