use std::collections::HashSet;

use prelude::*;

use servers::trie::Trie;

use hash::HashMap;

pub mod tcp;
pub mod udp;

mod trie;

struct ServerLog {
    //log: HashMap<OrderIndex, Rc<Entry<()>>>,
    log: HashMap<order, Trie<Entry<()>>>,
    //TODO per chain locks...
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
            log: Default::default(),
            last_lock: 0,
            last_unlock: 0,
            _seen_ids: HashSet::new(),
            this_server_num: this_server_num,
            total_servers: total_servers,
        }
    }

    //FIXME pass buffer so it can be resized as needed
    fn handle_op(&mut self, val: &mut Entry<()>) -> bool {
        let kind = val.kind;
        match kind.layout() {
            EntryLayout::Multiput | EntryLayout::Sentinel => {
                trace!("SERVER {:?} multiput", self.this_server_num);
                //TODO handle sentinels
                //TODO check TakeLock flag?
                if !self.try_lock(val.lock_num()) {
                    trace!("SERVER {:?} wrong lock {} @ ({},{})", self.this_server_num,
                        val.lock_num(), self.last_lock, self.last_unlock);
                        return true
                }

                debug_assert!(self._seen_ids.insert(val.id));
                if kind.contains(EntryKind::TakeLock) {
                    assert!(self.last_lock == self.last_unlock + 1,
                        "SERVER {:?} lock: {:?} == unlock {:?} + 1",
                        self.this_server_num, self.last_lock, self.last_unlock);
                    assert!(self.last_lock == val.lock_num(),
                        "SERVER {:?} lock {:?} == valock {:?}",
                        self.this_server_num, self.last_lock, val.lock_num());

                } else {
                    assert!(val.lock_num() == 0 && self.last_lock == self.last_unlock);
                }

                let mut sentinel_start_index = None;
                {
                    val.kind.insert(EntryKind::ReadSuccess);
                    let locs = val.locs_mut();
                    //TODO select only relevent chains
                    //trace!("SERVER {:?} Horizon A {:?}", self.horizon);
                    //FIXME handle duplicates
                    'update_append_horizon: for i in 0..locs.len() {
                        if locs[i] == OrderIndex(0.into(), 0.into()) {
                            sentinel_start_index = Some(i + 1);
                            break 'update_append_horizon
                        }
                        let chain = locs[i].0;
                        if self.stores_chain(chain) {
                            //FIXME handle duplicates
                            let next_entry = self.ensure_chain(chain).len();
                            assert!(next_entry > 0);
                            let next_entry = entry::from(next_entry);
                            locs[i].1 = next_entry;
                        }
                    }
                    if let Some(ssi) = sentinel_start_index {
                        for i in ssi..locs.len() {
                            assert!(locs[i] != OrderIndex(0.into(), 0.into()));
                            let chain = locs[i].0;
                            if self.stores_chain(chain) {
                                //FIXME handle duplicates
                                let next_entry = self.ensure_chain(chain).len();
                                assert!(next_entry > 0);
                                let next_entry = entry::from(next_entry);
                                locs[i].1 = next_entry;
                            }
                        }
                    }
                    //trace!("SERVER {:?} Horizon B {:?}", self.horizon);
                    trace!("SERVER {:?} locs {:?}", self.this_server_num, locs);
                    trace!("SERVER {:?} ssi {:?}", self.this_server_num, sentinel_start_index);
                }
                trace!("SERVER {:?} appending at {:?}", self.this_server_num, val.locs());
                'emplace: for &OrderIndex(chain, index) in val.locs() {
                    if (chain, index) == (0.into(), 0.into()) {
                        break 'emplace
                    }
                    if self.stores_chain(chain) {
                        assert!(index != entry::from(0));
                        self.ensure_chain(chain).append(&val);
                    }
                    // trace!("SERVER {:?} appended at {:?}", loc);
                }
                if let Some(ssi) = sentinel_start_index {
                    val.kind.remove(EntryKind::Multiput);
                    val.kind.insert(EntryKind::Sentinel);
                    trace!("SERVER {:?} sentinal locs {:?}", self.this_server_num, &val.locs()[ssi..]);
                    for &OrderIndex(chain, index) in &val.locs()[ssi..] {
                        if self.stores_chain(chain) {
                            assert!(index != entry::from(0));
                            self.ensure_chain(chain).append(&val);
                        }
                        // trace!("SERVER {:?} appended at {:?}", loc);
                    }
                    val.kind.remove(EntryKind::Sentinel);
                    val.kind.insert(EntryKind::Multiput);
                }
            }

            EntryLayout::Read => {
                trace!("SERVER {:?} Read", self.this_server_num);
                let OrderIndex(chain, index) = val.locs()[0];
                //TODO validate lock
                //     this will come after per-chain locks
                match self.log.get(&chain) {
                    None => {
                        trace!("SERVER {:?} Read Vacant chain {:?}",
                            self.this_server_num, chain);
                        match val.kind {
                            EntryKind::Read => empty_read(val, 0),
                            _ => trace!("SERVER {:?} nop {:?}", self.this_server_num, val.kind)
                        }
                    }
                    Some(log) => match log.get(index.into()) {
                        None => empty_read(val, log.len() - 1),
                        Some(packet) => {
                            trace!("SERVER {:?} Read Occupied entry {:?} {:?}",
                                self.this_server_num, (chain, index), packet.id);
                            //FIXME
                            *val = (*packet).clone();
                        }
                    },
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

                let chain = val.locs()[0].0;
                debug_assert!(self.stores_chain(chain),
                    "tried to store {:?} at server {:?} of {:?}",
                    chain, self.this_server_num, self.total_servers);

                let this_server_num = self.this_server_num;
                let log = self.ensure_chain(chain);
                {
                    let index = log.len();
                    val.kind.insert(EntryKind::ReadSuccess);
                    let l = unsafe { &mut val.as_data_entry_mut().flex.loc };
                    l.1 = entry::from(index);
                    trace!("SERVER {:?} Writing entry {:?}",
                        this_server_num, (chain, index));
                };
                log.append(val);
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

    fn ensure_chain(&mut self, chain: order) -> &mut Trie<Entry<()>> {
        self.log.entry(chain).or_insert_with(|| {
            let mut t = Trie::new();
            t.append(&EntryContents::Data(&(), &[]).clone_entry());
            t
        })
    }

    fn is_unlocked(&self) -> bool {
        self.last_lock == self.last_unlock
    }

    fn server_num(&self) -> u32 {
        self.this_server_num
    }
}

fn empty_read(packet: &mut Entry<()>, last_loc: u32) {
    let (old_id, old_loc) = (packet.id, packet.locs()[0]);
    let chain: order = old_loc.0;
    assert!(last_loc == 0 || entry::from(last_loc) < old_loc.1,
        "{:?} >= {:?}", last_loc, old_loc);
    *packet = EntryContents::Data(&(),
        &[OrderIndex(chain, entry::from(last_loc))]).clone_entry();
    packet.id = old_id;
    packet.kind = EntryKind::NoValue;
    unsafe {
        packet.as_data_entry_mut().flex.loc = old_loc;
    }
    trace!("SERVER : empty read {:?}", old_loc);
}
