//use std::collections::HashSet;
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::{mem, ptr, slice};
//use std::rc::Rc;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

use prelude::*;
use buffer::Buffer;

use servers2::skeens::{SkeensState, SkeensAppendRes, SkeensSetMaxRes, GotMax};
use servers2::trie::{AppendSlot, Trie};

use hash::HashMap;

use self::ToWorker::*;

pub mod tcp;
pub mod udp;

pub mod spmc;
pub mod spsc;

mod skeens;
//TODO remove `pub`, it only exists for testing purposes
pub mod trie;
pub mod byte_trie;

#[cfg(test)]
mod tests;

struct ServerLog<T: Send + Sync + Copy, ToWorkers>
where ToWorkers: DistributeToWorkers<T> {
    log: HashMap<order, Chain<T>>,
    //TODO per chain locks...
    total_servers: u32,
    this_server_num: u32,
    //_seen_ids: HashSet<Uuid>,
    to_workers: ToWorkers, //spmc::Sender<ToWorker<T>>,
    _pd: PhantomData<T>,

    print_data: LogData,
}

counters! {
    struct LogData {
        msgs_recvd: u64,
        msgs_sent: u64,
    }
}

struct Chain<T: Copy> {
    trie: Trie<Entry<()>>,
    skeens: SkeensState<T>,
}

pub trait DistributeToWorkers<T: Send + Sync> {
    fn send_to_worker(&mut self, msg: ToWorker<T>);
}

impl<T: Send + Sync> DistributeToWorkers<T> for spmc::Sender<ToWorker<T>> {
    fn send_to_worker(&mut self, msg: ToWorker<T>) {
        self.send(msg)
    }
}

impl DistributeToWorkers<()> for VecDeque<ToWorker<()>> {
    fn send_to_worker(&mut self, msg: ToWorker<()>) {
        self.push_front(msg);
    }
}

//TODO
pub type BufferSlice = Buffer;

pub enum ToWorker<T: Send + Sync> {
    Write(BufferSlice, AppendSlot<Entry<()>>, T),

    //TODO shrink?
    MultiReplica {
        buffer: BufferSlice,
        multi_storage: *mut u8,
        senti_storage: *mut u8,
        t: T,
        num_places: usize,
    },

    Skeens1 {
        buffer: BufferSlice,
        storage: SkeensMultiStorage,
        t: T,
    },

    SkeensFinished {
        loc: OrderIndex,
        trie_slot: *mut *const u8,
        storage: SkeensMultiStorage,
        t: T,
    },

    //This is not racey b/c both this and DelayedSingle single get sent to the same thread
    SingleSkeens {
        buffer: BufferSlice,
        storage: *mut u8,
        storage_loc: u64,
        t: T,
    },

    DelayedSingle {
        index: u64,
        trie_slot: *mut *const u8,
        storage: *const u8,
        t: T,
    },

    //FIXME we don't have a good way to make pointers send...
    Read(&'static Entry<()>, BufferSlice, T),
    EmptyRead(entry, BufferSlice, T),
    Reply(BufferSlice, T),
    ReturnBuffer(BufferSlice, T)
}

#[derive(Clone, Debug)]
pub struct SkeensMultiStorage(
    Arc<UnsafeCell<(&'static mut [u64], &'static mut [u8], &'static mut [u8])>>
);

unsafe impl Send for SkeensMultiStorage {}

impl SkeensMultiStorage {
    fn new(num_locs: usize, entry_size: usize, sentinel_size: Option<usize>) -> Self {
        unsafe {
            let mut locs = Vec::with_capacity(num_locs);
            let mut data = Vec::with_capacity(entry_size);
            locs.set_len(num_locs);
            data.set_len(entry_size);
            let locs = &mut *Box::into_raw(locs.into_boxed_slice());
            let data = &mut *Box::into_raw(data.into_boxed_slice());
            let senti = sentinel_size.map(|s| {
                let mut senti = Vec::with_capacity(s);
                senti.set_len(s);
                &mut *Box::into_raw(senti.into_boxed_slice())
            }).unwrap_or(&mut []);
            SkeensMultiStorage(Arc::new(UnsafeCell::new((locs, data, senti))))
        }
    }
}

impl ::std::ops::Deref for SkeensMultiStorage {
    type Target = UnsafeCell<(&'static mut [u64], &'static mut [u8], &'static mut [u8])>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

#[derive(Debug)]
pub enum WhichMulti {
    Data,
    Senti,
    Both,
}

//enum PendingAppend {
//    Single(*const u8, T),
//    Multi(*const (Box<[u8]>, Box<[u8]>), t),
//}

impl<T> ToWorker<T>
where T: Send + Sync {

    #[allow(dead_code)]
    fn edit_associated_data<F>(&mut self, f: F)
    where F: FnOnce(&mut T) {
        match self {
            &mut Write(_, _, ref mut t) | &mut Read(_, _, ref mut t)
            | &mut EmptyRead(_, _, ref mut t) | &mut Reply(_, ref mut t)
            | &mut MultiReplica{ref mut t, ..}
            | &mut Skeens1{ref mut t, ..} | &mut SkeensFinished{ref mut t, ..}
            | &mut SingleSkeens {ref mut t, ..} | &mut DelayedSingle {ref mut t, .. }
            => f(t),
            &mut ReturnBuffer(_, ref mut t) => f(t),
        }
    }
}

impl<T> ToWorker<T>
where T: Copy + Send + Sync {
    fn get_associated_data(&self) -> T {
        match self {
            &Write(_, _, t) | &Read(_, _, t) | &EmptyRead(_, _, t) | &Reply(_, t) => t,
            &MultiReplica{t, ..} => t,
            &Skeens1{t, ..} | &SkeensFinished{t, ..} => t,
            &SingleSkeens {t, ..} | &DelayedSingle {t, .. } => t,
            &ReturnBuffer(_, t) => t,
        }
    }
}

unsafe impl<T> Send for ToWorker<T> where T: Send + Sync {}

pub enum ToReplicate {
    Data(BufferSlice, u64),
    //TODO probably needs a custom Rc for garbage collection
    //     ideally one with the layout { count, entry }
    //     since the entry's size is stored internally
    Multi(BufferSlice, Box<[u8]>, Box<[u8]>),
    UnLock(BufferSlice),
}

#[derive(Debug, PartialEq, Eq)]
enum Troption<L, R> {
    None,
    Left(L),
    Right(R),
}

impl<L, R> Troption<L, R> {
    fn unwrap_left(self) -> L {
        match self {
            Troption::Left(l) => l,
            _ => unreachable!(),
        }
    }

    fn unwrap_right(self) -> R {
        match self {
            Troption::Right(r) => r,
            _ => unreachable!(),
        }
    }
}

impl<T: Send + Sync + Copy, ToWorkers> ServerLog<T, ToWorkers>
where ToWorkers: DistributeToWorkers<T> {

    fn new(this_server_num: u32, total_servers: u32, to_workers: ToWorkers)
     -> Self {
        //TODO
        //assert!(this_server_num > 0);
        assert!(this_server_num <= total_servers,
            "this_server_num <= total_servers, {:?} <= {:?}",
            this_server_num, total_servers);
        ServerLog {
            log: Default::default(),
            //_seen_ids: HashSet::new(),
            this_server_num: this_server_num,
            total_servers: total_servers,
            to_workers: to_workers,
            _pd: PhantomData,
            print_data: Default::default(),
        }
    }

    #[cfg(feature = "print_stats")]
    fn print_stats(&self) {
        println!("{:?}, {:?}", self.print_data, self.this_server_num);
    }

    //FIXME pass buffer-slice so we can read batches
    //FIXME we don't want the log thread free'ing, return storage on lock failure?
    //TODO replace T with U and T: ReturnAs<U> so we don't have to send as much data
    fn handle_op(
        &mut self,
        mut buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(Box<[u8]>, Box<[u8]>)>>,
        t: T
    ) {
        self.print_data.msgs_recvd(1);
        let kind = buffer.entry().kind;
        match kind.layout() {
            EntryLayout::Multiput | EntryLayout::Sentinel =>
                self.handle_multiappend(kind, buffer, storage, t),

            /////////////////////////////////////////////////

            EntryLayout::Read => {
                trace!("SERVER {:?} Read", self.this_server_num);
                let OrderIndex(chain, index) = buffer.entry().locs()[0];
                //TODO validate lock
                //     this will come after per-chain locks
                match self.log.get(&chain) {
                    None => {
                        trace!("SERVER {:?} Read Vacant chain {:?}",
                            self.this_server_num, chain);
                        match buffer.entry().kind {
                            EntryKind::Read => {
                                self.print_data.msgs_sent(1);
                                self.to_workers.send_to_worker(
                                    EmptyRead(entry::from(0), buffer, t))
                            },
                            _ => trace!("SERVER {:?} nop {:?}",
                                self.this_server_num, buffer.entry().kind)
                        }
                    }
                    Some(lg) => match lg.trie.get(u32::from(index) as u64) {
                        None => {
                            self.print_data.msgs_sent(1);
                            self.to_workers.send_to_worker(
                            //FIXME needs 64 entries
                                EmptyRead(entry::from((lg.trie.len() - 1) as u32), buffer, t))
                        },
                        Some(packet) => {
                            trace!("SERVER {:?} Read Occupied entry {:?} {:?}",
                                self.this_server_num, (chain, index), packet.id);
                            //FIXME
                            let packet = unsafe { extend_lifetime(packet) };
                            self.print_data.msgs_sent(1);
                            self.to_workers.send_to_worker(Read(packet, buffer, t))
                        }
                    },
                }
            }

            /////////////////////////////////////////////////

            EntryLayout::Data => {
                trace!("SERVER {:?} Single Append", self.this_server_num);

                let chain = buffer.entry().locs()[0].0;
                debug_assert!(self.stores_chain(chain),
                    "tried to store {:?} at server {:?} of {:?}",
                    chain, self.this_server_num, self.total_servers);

                let this_server_num = self.this_server_num;
                let delay = {
                    let log = self.ensure_chain(chain);
                    if log.needs_skeens_single() {
                        let s = log.handle_skeens_single(&mut buffer, t);
                        Some(s)
                    } else {
                        None
                    }
                };
                if let Some((slot, storage_loc)) = delay {
                    self.print_data.msgs_sent(1);
                    let msg = SingleSkeens {
                        buffer: buffer,
                        storage: slot,
                        storage_loc: storage_loc,
                        t: t,
                    };
                    self.to_workers.send_to_worker(msg);
                    return
                }

                let slot = {
                    let log = self.ensure_trie(chain);
                    if log.is_locked() {
                        trace!("SERVER append during lock {:?} @ {:?}",
                            log.lock_pair(), chain,
                        );
                        None
                    }
                    else {
                        //FIXME this shuld be done in the worker thread?
                        let index = log.len();
                        let val = buffer.entry_mut();
                        val.kind.insert(EntryKind::ReadSuccess);
                        let size = val.entry_size();
                        let l = unsafe { &mut val.as_data_entry_mut().flex.loc };
                        //FIXME 64b entries
                        //FIXME this should be done on the worker?
                        l.1 = entry::from(index as u32);
                        trace!("SERVER {:?} Writing entry {:?}",
                            this_server_num, (chain, index));
                        Some(unsafe { log.partial_append(size) })
                    }
                };
                match slot {
                    Some(slot) => {
                        self.print_data.msgs_sent(1);
                        self.to_workers.send_to_worker(Write(buffer, slot, t))
                    },
                    None => {
                        self.print_data.msgs_sent(1);
                        self.to_workers.send_to_worker(Reply(buffer, t))
                    },
                }

            }

            /////////////////////////////////////////////////

            EntryLayout::Lock => {
                trace!("SERVER {:?} Lock", self.this_server_num);
                //FIXME this needs to be perchain now
                unimplemented!()
            }
        }
    }

    /////////////////////////////////////////////////

    fn handle_multiappend(
        &mut self,
        kind: EntryKind::Kind,
        mut buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(Box<[u8]>, Box<[u8]>)>>,
        t: T
    ) {
        if kind.contains(EntryKind::NewMultiPut) {
            self.handle_new_multiappend(kind, buffer, storage, t)
        }
        else {
            self.handle_old_multiappend(kind, buffer, storage, t)
        }
    }

    //////////////////////

    fn handle_new_multiappend(
        &mut self,
        kind: EntryKind::Kind,
        mut buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(Box<[u8]>, Box<[u8]>)>>,
        t: T
    ) {
        trace!("SERVER {:?} new-style multiput {:?}", self.this_server_num, kind);
        assert!(kind.contains(EntryKind::TakeLock));
        if kind.contains(EntryKind::Unlock) {
            self.new_multiappend_round2(kind, &mut buffer, t);
            self.print_data.msgs_sent(1);
            self.to_workers.send_to_worker(ReturnBuffer(buffer, t))
        } else {
            let storage = storage.unwrap_left();
            self.new_multiappend_round1(kind, &mut buffer, &storage, t);
            self.print_data.msgs_sent(1);
            self.to_workers.send_to_worker(
                Skeens1 { buffer: buffer, storage: storage, t: t }
            );
        }
    }

    fn new_multiappend_round1(
        &mut self,
        kind: EntryKind::Kind,
        buffer: &mut BufferSlice,
        storage: &SkeensMultiStorage,
        t: T
    ) {
        trace!("SERVER {:?} new-style multiput Round 1 {:?}", self.this_server_num, kind);
        let val = buffer.entry();
        let locs = val.locs();
        let timestamps = &mut unsafe { &mut (&mut *storage.get()).0 }[..locs.len()];
        let id = val.id;
        for i in 0..locs.len() {
            if locs[i] == OrderIndex(0.into(), 0.into()) {
                continue
            }

            let chain = locs[i].0;
            if self.stores_chain(chain) {
                let chain = self.ensure_chain(chain);
                //FIXME handle repeat skeens-1
                let local_timestamp = chain.timestamp_for_multi(id, storage.clone(), t).unwrap();
                timestamps[i] = local_timestamp;
            } else {
                timestamps[i] = 0;
            }
        }
        trace!("SERVER {:?} new-style multiput Round 1 timestamps {:?}",
            self.this_server_num, timestamps);
    }

    fn new_multiappend_round2(
        &mut self,
        kind: EntryKind::Kind,
        buffer: &mut BufferSlice,
        t: T
    ) {
        // In round two we flush some the queues... an possibly a partial entry...
        let val = buffer.entry_mut();
        let id = val.id;
        let max_timestamp = val.lock_num();
        let locs = val.locs_mut();
        trace!("SERVER {:?} new-style multiput Round 2 {:?} mts {:?}",
            self.this_server_num, kind, max_timestamp
        );
        let mut is_sentinel = false;
        for i in 0..locs.len() {
            if locs[i] == OrderIndex(0.into(), 0.into()) {
                is_sentinel = true;
                continue
            }

            let chain_num = locs[i].0;
            if self.stores_chain(chain_num) {
                //let chain = self.ensure_trie(chain);
                let chain = self.log.get_mut(&chain_num)
                    .expect("cannot have skeens-2 as the first op on a chain");
                    /*.or_insert_with(|| {
                    let mut t = Trie::new();
                    t.append(&EntryContents::Data(&(), &[]).clone_entry());
                    Chain{ trie: t, skeens: SkeensState::new() }
                });*/
                let to_workers = &mut self.to_workers;
                let print_data = &mut self.print_data;
                chain.finish_multi(id, max_timestamp,
                    |finished| match finished {
                        FinishSkeens::Multi(index, trie_slot, storage, t) => {
                            trace!("server finish sk multi");
                            print_data.msgs_sent(1);
                            to_workers.send_to_worker(
                                SkeensFinished {
                                    loc: OrderIndex(chain_num, (index as u32).into()),
                                    trie_slot: trie_slot,
                                    storage: storage,
                                    t: t
                                }
                            )
                        },
                        FinishSkeens::Single(index, trie_slot, storage, t) => {
                            trace!("server finish sk single");
                            print_data.msgs_sent(1);
                            to_workers.send_to_worker(
                                DelayedSingle {
                                    index: index,
                                    trie_slot: trie_slot,
                                    storage: storage,
                                    t: t
                                }
                            )
                        },
                    }
                );
            } else {
                locs[i].1 = entry::from(0);
            }
        }
    }

    //////////////////////

    fn handle_old_multiappend(
        &mut self,
        kind: EntryKind::Kind,
        mut buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(Box<[u8]>, Box<[u8]>)>>,
        t: T
    ) {
        trace!("SERVER {:?} old-style multiput {:?}", self.this_server_num, kind);
        if kind.contains(EntryKind::Unlock) {
            {
                let mut all_unlocked = true;
                let val = buffer.entry_mut();
                {
                    let locs = val.locs_mut();
                    trace!("SERVER {} try unlock {:?}", self.this_server_num, locs);
                    for i in 0..locs.len() {
                        if locs[i] == OrderIndex(0.into(), 0.into()) {
                            continue
                        }
                        let chain = locs[i].0;
                        if self.stores_chain(chain) {
                            let chain = self.ensure_trie(chain);
                            all_unlocked &= chain.unlock(u32::from(locs[i].1)
                                as u64);
                        }
                    }
                }
                if all_unlocked {
                    trace!("SERVER {} unlocked {:?}", self.this_server_num, val.locs());
                    val.kind.insert(EntryKind::ReadSuccess)
                }
                else {
                    trace!("SERVER {} failed unlock {:?}", self.this_server_num, val.locs());
                }
            };

            self.print_data.msgs_sent(1);
            self.to_workers.send_to_worker(Reply(buffer, t));
            return
        }

        trace!("SERVER {:?} multiput {:?}", self.this_server_num, kind);
        /*if kind.contains(EntryKind::TakeLock) {
            debug_assert!(self.last_lock == buffer.entry().lock_num(),
                "SERVER {:?} lock {:?} == valock {:?}",
                self.this_server_num, self.last_lock, buffer.entry().lock_num());
            debug_assert!(self.last_lock == self.last_unlock + 1,
                "SERVER {:?} lock: {:?} == unlock {:?} + 1",
                self.this_server_num, self.last_lock, self.last_unlock);

        } else {
            debug_assert!(buffer.entry().lock_num() == 0
                && self.last_lock == self.last_unlock,
                "unlocked mutli failed; lock_num: {}, last_lock: {}, last_unlock: {} @ {:?}",
                buffer.entry().lock_num(), self.last_lock, self.last_unlock,
                buffer.entry().locs(),
            );
        }*/

        let mut sentinel_start_index = None;
        let mut num_places = 0;
        let lock_failed = {
            let val = buffer.entry_mut();
            let locs = val.locs_mut();
            //FIXME handle duplicates
            let mut lock_failed = false;
            'update_append_horizon: for i in 0..locs.len() {
                if locs[i] == OrderIndex(0.into(), 0.into()) {
                    sentinel_start_index = Some(i + 1);
                    break 'update_append_horizon
                }
                let chain = locs[i].0;
                if self.stores_chain(chain) {
                    //FIXME handle duplicates
                    let next_entry = {
                        let chain = self.ensure_trie(chain);
                        if kind.contains(EntryKind::TakeLock) {
                            if chain.cannot_lock(u32::from(locs[i].1) as u64) {
                                //the lock that failed is always the first MAX
                                trace!("SERVER wrong lock {} @ {:?}",
                                    u32::from(locs[i].1),
                                    chain.lock_pair()
                                );
                                locs[i].1 = entry::from(::std::u64::MAX as u32);
                                lock_failed = true;
                                break 'update_append_horizon
                            }
                        }
                        chain.len()
                    };
                    assert!(next_entry > 0);
                    //FIXME 64b entries
                    let next_entry = entry::from(next_entry as u32);
                    locs[i].1 = next_entry;
                    num_places += 1
                } else {
                    locs[i].1 = entry::from(0)
                }
            }
            debug_assert!(!(sentinel_start_index.is_none() && kind.contains(EntryKind::Sentinel)),
                "BAD SENTINEL @ {:?}",
                locs,
            );
            if let (Some(ssi), false) = (sentinel_start_index, lock_failed) {
                'senti_horizon: for i in ssi..locs.len() {
                    assert!(locs[i] != OrderIndex(0.into(), 0.into()));
                    let chain = locs[i].0;
                    if self.stores_chain(chain) {
                        //FIXME handle duplicates
                        let next_entry = {
                            let chain = self.ensure_trie(chain);
                            if kind.contains(EntryKind::TakeLock) {
                                if chain.cannot_lock(u32::from(locs[i].1) as u64) {
                                    trace!("SERVER wrong lock {} @ {:?}",
                                        u32::from(locs[i].1),
                                        chain.lock_pair()
                                    );
                                    locs[i].1 = entry::from(::std::u64::MAX as u32);
                                    lock_failed = true;
                                    break 'senti_horizon
                                }
                            }
                            chain.len()
                        };
                        assert!(next_entry > 0);
                        //FIXME 64b entries
                        let next_entry = entry::from(next_entry as u32);
                        locs[i].1 = next_entry;
                        num_places += 1
                    } else {
                        locs[i].1 = entry::from(0)
                    }
                }
            }
            trace!("SERVER {:?} locs {:?}", self.this_server_num, locs);
            trace!("SERVER {:?} ssi {:?}", self.this_server_num, sentinel_start_index);
            lock_failed
        };
        if lock_failed {
            self.print_data.msgs_sent(1);
            self.to_workers.send_to_worker(Reply(buffer, t));
            return
        }
        //debug_assert!(self._seen_ids.insert(buffer.entry().id));

        trace!("SERVER {:?} appending at {:?}",
            self.this_server_num, buffer.entry().locs());

        let (multi_storage, senti_storage) = *storage.unwrap_right();
        let multi_storage = unsafe {
            debug_assert_eq!(multi_storage.len(), buffer.entry_size());
            (*Box::into_raw(multi_storage)).as_mut_ptr()
        };
        trace!("multi_storage @ {:?}", multi_storage);
        let senti_storage = unsafe {
            debug_assert_eq!(senti_storage.len(), buffer.entry().sentinel_entry_size());
            (*Box::into_raw(senti_storage)).as_mut_ptr()
        };
        //LOGIC sentinal storage must contain at least 64b
        //      (128b with 64b entry address space)
        //      thus has at least enough storage for 1 ptr per entry
        let mut next_ptr_storage = senti_storage as *mut *mut *const u8;
        {
            let val = buffer.entry_mut();
            val.kind.insert(EntryKind::ReadSuccess);
            'emplace: for &OrderIndex(chain, index) in val.locs() {
                if (chain, index) == (0.into(), 0.into()) {
                    break 'emplace
                }
                if self.stores_chain(chain) {
                    assert!(index != entry::from(0));
                    unsafe {
                        let ptr = {
                            let chain = self.ensure_trie(chain);
                            if kind.contains(EntryKind::TakeLock) {
                                chain.increment_lock();
                            }
                            chain.prep_append(ptr::null()).1
                        };
                        *next_ptr_storage = ptr;
                        next_ptr_storage = next_ptr_storage.offset(1);
                    };
                }
            }
            if let Some(ssi) = sentinel_start_index {
                trace!("SERVER {:?} sentinal locs {:?}", self.this_server_num, &val.locs()[ssi..]);
                for &OrderIndex(chain, index) in &val.locs()[ssi..] {
                    if self.stores_chain(chain) {
                        assert!(index != entry::from(0));
                        unsafe {
                            let ptr = {
                                let chain = self.ensure_trie(chain);
                                if kind.contains(EntryKind::TakeLock) {
                                    chain.increment_lock()
                                }
                                chain.prep_append(ptr::null()).1
                            };
                            *next_ptr_storage = ptr;
                            next_ptr_storage = next_ptr_storage.offset(1);
                        };
                    }
                }
            }
        }

        self.print_data.msgs_sent(1);
        self.to_workers.send_to_worker(
            MultiReplica {
                buffer: buffer,
                multi_storage: multi_storage,
                senti_storage: senti_storage,
                t: t,
                num_places: num_places,
            }
        );
    }

    /////////////////////////////////////////////////

    fn stores_chain(&self, chain: order) -> bool {
        chain % self.total_servers == self.this_server_num.into()
    }

    fn ensure_chain(&mut self, chain: order) -> &mut Chain<T> {
        self.log.entry(chain).or_insert_with(|| {
            let mut t = Trie::new();
            t.append(&EntryContents::Data(&(), &[]).clone_entry());
            Chain{ trie: t, skeens: SkeensState::new() }
        })
    }

    fn ensure_trie(&mut self, chain: order) -> &mut Trie<Entry<()>> {
        &mut self.ensure_chain(chain).trie
    }

    #[allow(dead_code)]
    fn server_num(&self) -> u32 {
        self.this_server_num
    }

    fn handle_replication(
        &mut self,
        to_replicate: ToReplicate,
        t: T
    ) {
        use std::ptr;
        match to_replicate {
            ToReplicate::Data(buffer, storage_loc) => {
                trace!("SERVER {:?} Append", self.this_server_num);
                //FIXME locks

                let loc = buffer.entry().locs()[0];
                debug_assert!(self.stores_chain(loc.0),
                    "tried to rep {:?} at server {:?} of {:?}",
                    loc, self.this_server_num, self.total_servers);

                let this_server_num = self.this_server_num;
                let slot = {
                    let log = self.ensure_trie(loc.0);
                    let size = buffer.entry_size();
                    trace!("SERVER {:?} replicating entry {:?}",
                        this_server_num, loc);
                    unsafe {
                        log.partial_append_at(u32::from(loc.1) as u64,
                            storage_loc, size)
                    }
                };

                self.print_data.msgs_sent(1);
                self.to_workers.send_to_worker(Write(buffer, slot, t))
            },

            ToReplicate::Multi(buffer, multi_storage, senti_storage) => {
                trace!("SERVER {:?} replicate multiput", self.this_server_num);

                //debug_assert!(self._seen_ids.insert(buffer.entry().id));
                //FIXME this needs to be aware of locks...
                //      but I think only unlocks, the primary
                //      will ensure that locks happen in order...

                let mut sentinel_start_index = None;
                let mut num_places = 0;
                {
                    let val = buffer.entry();
                    let locs = val.locs();
                    //FIXME handle duplicates
                    'update_append_horizon: for i in 0..locs.len() {
                        if locs[i] == OrderIndex(0.into(), 0.into()) {
                            sentinel_start_index = Some(i + 1);
                            break 'update_append_horizon
                        }
                        let chain = locs[i].0;
                        if self.stores_chain(chain) { num_places += 1 }
                    }
                    if let Some(ssi) = sentinel_start_index {
                        for i in ssi..locs.len() {
                            assert!(locs[i] != OrderIndex(0.into(), 0.into()));
                            let chain = locs[i].0;
                            if self.stores_chain(chain) { num_places += 1 }
                        }
                    }
                    trace!("SERVER {:?} rep locs {:?}", self.this_server_num, locs);
                    trace!("SERVER {:?} rep ssi {:?}",
                        self.this_server_num, sentinel_start_index);
                }

                trace!("SERVER {:?} rep at {:?}",
                    self.this_server_num, buffer.entry().locs());

                let multi_storage = unsafe {
                    (*Box::into_raw(multi_storage)).as_mut_ptr()
                };
                let senti_storage = unsafe {
                    (*Box::into_raw(senti_storage)).as_mut_ptr()
                };
                //LOGIC sentinal storage must contain at least 64b
                //      (128b with 64b entry address space)
                //      thus has at least enough storage for 1 ptr per entry
                let mut next_ptr_storage = senti_storage as *mut *mut *const u8;
                'emplace: for &OrderIndex(chain, index) in buffer.entry().locs() {
                    if (chain, index) == (0.into(), 0.into()) {
                        //*next_ptr_storage = ptr::null_mut();
                        //next_ptr_storage = next_ptr_storage.offset(1);
                        break 'emplace
                    }
                    if self.stores_chain(chain) {
                        assert!(index != entry::from(0));
                        unsafe {
                            let ptr = self.ensure_trie(chain)
                                .prep_append_at(
                                    u32::from(index) as u64,
                                    ptr::null(),
                                );
                            *next_ptr_storage = ptr;
                            next_ptr_storage = next_ptr_storage.offset(1);
                        };
                    }
                }
                if let Some(ssi) = sentinel_start_index {
                    trace!("SERVER {:?} sentinal locs {:?}", self.this_server_num, &buffer.entry().locs()[ssi..]);
                    for &OrderIndex(chain, index) in &buffer.entry().locs()[ssi..] {
                        if self.stores_chain(chain) {
                            assert!(index != entry::from(0));
                            unsafe {
                                let ptr = self.ensure_trie(chain)
                                    .prep_append_at(
                                        u32::from(index) as u64,
                                        ptr::null(),
                                    );
                                *next_ptr_storage = ptr;
                                next_ptr_storage = next_ptr_storage.offset(1);
                            };
                        }
                    }
                }

                self.print_data.msgs_sent(1);
                self.to_workers.send_to_worker(
                    MultiReplica {
                        buffer: buffer,
                        multi_storage: multi_storage,
                        senti_storage: senti_storage,
                        t: t,
                        num_places: num_places,
                    }
                );
            },

            ToReplicate::UnLock(_buffer) => {
                //FIXME

                //let lock_num = unsafe { buffer.entry().as_lock_entry().lock };
                //trace!("SERVER {:?} repli UnLock {:?}", self.this_server_num, self.last_lock);
                //if lock_num == self.last_lock {
                //    trace!("SERVER {:?} Success", self.this_server_num);
                //    self.last_unlock = self.last_lock;
                //}
                //FIXME this needs to be perchain now
                unimplemented!()
            }
        }
    }
}

enum FinishSkeens<T> {
    Single(u64, *mut *const u8, *const u8, T),
    Multi(u64, *mut *const u8, SkeensMultiStorage, T),
}

impl<T: Copy> Chain<T> {
    fn needs_skeens_single(&mut self) -> bool {
        !self.skeens.is_empty()
    }

    fn handle_skeens_single(&mut self, buffer: &mut BufferSlice, t: T) -> (*mut u8, u64) {
        let val = buffer.entry_mut();
        val.kind.insert(EntryKind::ReadSuccess);
        let size = val.entry_size();
        let id = val.id;
        let l = unsafe { &mut val.as_data_entry_mut().flex.loc };
        let (slot, storage_loc) = unsafe { self.trie.reserve_space(size) };
        self.skeens.add_single_append(id, slot as *const _, t);
        (slot, storage_loc)
    }

    fn timestamp_for_multi(
        &mut self, id: Uuid, storage: SkeensMultiStorage, t: T
    ) -> Result<u64, u64> {
        match self.skeens.add_multi_append(id, storage, t) {
            SkeensAppendRes::OldPhase1(ts) | SkeensAppendRes::NewAppend(ts)
                => Ok(ts),
            SkeensAppendRes::Phase2(ts) => Err(ts),
        }
    }

    fn finish_multi<F>(
        &mut self, id: Uuid, max_timestamp: u64, mut on_finish: F)
    where F: FnMut(FinishSkeens<T>) { //Ret val?
        use self::FinishSkeens::*;
        let r = self.skeens.set_max_timestamp(id, max_timestamp);
        match r {
            SkeensSetMaxRes::Ok => trace!("multi with ts {:?} must wait", max_timestamp),
            SkeensSetMaxRes::Duplicate(u64) => unimplemented!(),
            SkeensSetMaxRes::NotWaiting => unimplemented!(),
            SkeensSetMaxRes::NeedsFlush => {
                trace!("multi flush due to {:?}", max_timestamp);
                let trie = &mut self.trie;
                self.skeens.flush_got_max_timestamp(|g| {
                    match g {
                        GotMax::Single{..} => unimplemented!(),
                        GotMax::SimpleSingle{storage, t, timestamp, ..} => unsafe {
                            trace!("flush single {:?}", timestamp);
                            let (loc, ptr) = trie.prep_append(ptr::null());
                            //println!("s id {:?} ts {:?}", id, timestamp);
                            on_finish(Single(loc, ptr, storage, t));
                            None
                        },
                        GotMax::Multi{storage, t, id, timestamp, ..} => unsafe {
                            trace!("flush multi {:?}", timestamp);
                            //println!("m id {:?} ts {:?}", id, timestamp);
                            let (loc, ptr) = trie.prep_append(ptr::null());
                            on_finish(Multi(loc, ptr, storage, t));
                            Some(id)
                        },
                    }
                })
            }
        }
    }
}

fn handle_to_worker<T: Send + Sync>(msg: ToWorker<T>, worker_num: usize)
-> (Option<Buffer>, &'static [u8], T, u64, bool) {
    match msg {
        Reply(buffer, t) => {
            trace!("WORKER {} finish reply", worker_num);
            (Some(buffer), &[], t, 0, false)
        },

        Write(buffer, slot, t) => unsafe {
            trace!("WORKER {} finish write", worker_num);
            let loc = slot.loc();
            let ret = extend_lifetime(slot.finish_append(buffer.entry()).bytes());
            (Some(buffer), ret, t, loc, false)
        },

        SingleSkeens { mut buffer, storage, storage_loc, t, } => unsafe {
            {
                let e = buffer.entry_mut();
                e.kind.insert(EntryKind::ReadSuccess);
                {
                    let b = e.bytes();
                    ptr::copy_nonoverlapping(b.as_ptr(), storage, b.len());
                }
                e.kind.insert(EntryKind::Skeens1Queued);
            }
            (Some(buffer), &[], t, storage_loc, true)
        },

        // Safety, since both the original append and the delayed portion
        // get finished by the same worker this does not race
        // the storage_loc is sent with the first round
        DelayedSingle { index, trie_slot, storage, t, } => unsafe {
            trace!("WORKER {} finish delayed single", worker_num);
            let len = {
                let storage = storage as *mut u8;
                let e = Entry::<()>::wrap_mut(&mut *storage);
                e.locs_mut()[0].1 = entry::from(index as u32);
                e.entry_size()
            };
            let trie_entry: *mut AtomicPtr<u8> = trie_slot as *mut _;
            (*trie_entry).store(storage as *mut u8, Ordering::Release);
            let ret = slice::from_raw_parts(storage, len);
            (None, ret, t, 0, false)
        },

        Read(read, mut buffer, t) => {
            trace!("WORKER {} finish read", worker_num);
            let bytes = read.bytes();
            //FIXME needless copy
            buffer.ensure_capacity(bytes.len());
            buffer[..bytes.len()].copy_from_slice(bytes);
            (Some(buffer), bytes, t, 0, false)
        },

        EmptyRead(last_valid_loc, mut buffer, t) => {
            let (old_id, old_loc) = {
                let e = buffer.entry();
                (e.id, e.locs()[0])
            };
            {
                let chain: order = old_loc.0;
                //TODO so this assersion is not valid do to parrallel placement
                //debug_assert!(last_valid_loc == 0.into()
                //    || last_valid_loc < old_loc.1,
                //    "{:?} >= {:?}", last_valid_loc, old_loc);
                let e = buffer.fill_from_entry_contents(
                    EntryContents::Data(&(), &[OrderIndex(chain, last_valid_loc)]));
                e.id = old_id;
                e.kind = EntryKind::NoValue;
                //FIXME where do I sent loc to old loc?
            }
            buffer.ensure_len();
            trace!("WORKER {} finish empty read {:?}", worker_num, old_loc);
            (Some(buffer), &[], t, 0, false)
        },

        MultiReplica {
            buffer, multi_storage, senti_storage, t, num_places,
        } => unsafe {
            let (remaining_senti_places, len) = {
                let e = buffer.entry();
                let b = e.bytes();
                let len = b.len();
                trace!("place multi_storage @ {:?}, len {}", multi_storage, b.len());
                ptr::copy_nonoverlapping(b.as_ptr(), multi_storage, b.len());
                let places: &[*mut *const u8] = slice::from_raw_parts(
                   senti_storage as *const _, num_places
                );
                trace!("multi places {:?}, locs {:?}", places, e.locs());
                debug_assert!(places.len() <= e.locs().len());
                //alt let mut sentinel_start = places.len();
                let mut sentinel_start = None;
                for i in 0..num_places {

                    if e.locs()[i] == OrderIndex(0.into(), 0.into()) {
                        //alt sentinel_start = i;
                        sentinel_start = Some(i);
                        break
                    }

                    let trie_entry: *mut AtomicPtr<u8> = mem::transmute(places[i]);
                    //TODO mem barrier ordering
                    (*trie_entry).store(multi_storage, Ordering::Release);
                }
                let remaining_places = if let Some(i) = sentinel_start {
                    &places[i..]
                }
                else {
                    &[]
                };
                // we finished with the first portion,
                // if there is a second, we'll need auxiliary memory
                (remaining_places.to_vec(), len)
            };
            let ret = slice::from_raw_parts(multi_storage, len);
            //TODO is re right for sentinel only writes?
            if remaining_senti_places.len() == 0 {
                return (Some(buffer), ret, t, 0, false)
            }
            else {
                let e = buffer.entry();
                let len = e.sentinel_entry_size();
                trace!("place senti_storage @ {:?}, len {}", senti_storage, len);
                let b = e.bytes();
                ptr::copy_nonoverlapping(b.as_ptr(), senti_storage, len);
                {
                    let e = Entry::<()>::wrap_mut(&mut *senti_storage);
                    e.kind.remove(EntryKind::Multiput);
                    e.kind.insert(EntryKind::Sentinel);
                }
                for place in remaining_senti_places {
                    let _: *mut *const u8 = place;
                    let trie_entry: *mut AtomicPtr<u8> = mem::transmute(place);
                    //TODO mem barrier ordering
                    (*trie_entry).store(senti_storage, Ordering::Release);
                }

            }
            //TODO is re right for sentinel only writes?
            (Some(buffer), ret, t, 0, false)
        },

        Skeens1{mut buffer, storage, t} => unsafe {
            {
                let e = buffer.entry_mut();
                e.kind.insert(EntryKind::ReadSuccess);
                let &mut (ref mut ts, ref mut st0, ref mut st1) = &mut (*storage.get());
                trace!("WORKER {} finish skeens1 {:?}", worker_num, ts);
                let num_ts = ts.len();
                st0.copy_from_slice(e.bytes());
                e.kind.insert(EntryKind::Sentinel);
                if st1.len() > 0 {
                    st1.copy_from_slice(e.bytes());
                }
                {
                    let locs = &mut e.locs_mut()[..num_ts];
                    for i in 0..num_ts {
                        locs[i].1 = entry::from(ts[i] as u32)
                    }
                }
                e.kind.insert(EntryKind::Skeens1Queued);
            }
            (Some(buffer), &[], t, 0, false)
        },
        SkeensFinished{loc, trie_slot, storage, t,} => unsafe {
            trace!("WORKER {} finish skeens2 @ {:?}", worker_num, loc);
            let chain = loc.0;
            let &mut (ref mut ts, ref mut st0, ref mut st1) = &mut (*storage.get());
            let is_sentinel = {
                let st0 = Entry::<()>::wrap_bytes_mut(st0);
                let st0_l = st0.locs_mut();
                let i = st0_l.iter().position(|oi| oi.0 == chain).unwrap();
                //FIXME atomic?
                st0_l[i].1 = loc.1;
                if st1.len() > 0 {
                    Entry::<()>::wrap_bytes_mut(st1).locs_mut()[i].1 = loc.1;
                    let s_i = st0_l.iter()
                        .position(|oi| oi == &OrderIndex(0.into(), 0.into()));
                    i > s_i.unwrap()
                }
                else {
                    false
                }
            };
            let trie_entry: *mut AtomicPtr<u8> = trie_slot as *mut _;
            {
                let to_store: *mut u8 = if is_sentinel {
                    st1.as_mut_ptr()
                } else {
                    st0.as_mut_ptr()
                };
                (*trie_entry).store(to_store, Ordering::Release);
            }
            (None, if st1.len() > 0 { &**st1 } else { &**st0 }, t, 0, false)
        },
        ReturnBuffer(buffer, t) => {
            trace!("WORKER {} return buffer", worker_num);
            (Some(buffer), &[], t, 0, true)
        },
    }
}

unsafe fn extend_lifetime<'a, 'b, V: ?Sized>(v: &'a V) -> &'b V {
    ::std::mem::transmute(v)
}
