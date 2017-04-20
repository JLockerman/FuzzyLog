//use std::collections::HashSet;
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::{mem, ptr, slice};
//use std::rc::Rc;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

// use prelude::*;
use buffer::Buffer;

use servers2::skeens::{
    SkeensState,
    SkeensAppendRes,
    SkeensSetMaxRes,
    GotMax,
    ReplicatedSkeens,
    Time,
    QueueIndex,
};
use servers2::trie::{AppendSlot, ByteLoc, Trie};

use hash::HashMap;

use self::ToWorker::*;

use packets::*;

pub mod tcp;
// pub mod udp;

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
    trie: Trie,
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
    Write(BufferSlice, AppendSlot<Entry<'static>>, T),

    //TODO shrink?
    MultiReplica {
        buffer: BufferSlice,
        multi_storage: *mut u8,
        senti_storage: *mut u8,
        t: T,
        num_places: usize,
    },

    MultiFastPath(BufferSlice, SkeensMultiStorage, T),

    Skeens1 {
        buffer: BufferSlice,
        storage: SkeensMultiStorage,
        t: T,
    },

    SkeensFinished {
        loc: OrderIndex,
        trie_slot: *mut *const u8,
        storage: SkeensMultiStorage,
        timestamp: u64,
        t: T,
    },

    SingleServerSkeens1(SkeensMultiStorage, T),

    //FIXME this is getting too big...
    //This is not racey b/c both this and DelayedSingle single get sent to the same thread
    SingleSkeens {
        buffer: BufferSlice,
        storage: *mut u8,
        storage_loc: u64,
        time: Time,
        queue_num: QueueIndex,
        t: T,
    },

    DelayedSingle {
        index: u64,
        trie_slot: *mut *const u8,
        storage: *const u8,
        t: T,
    },

    Skeens1Replica {
        buffer: BufferSlice,
        storage: SkeensMultiStorage,
        t: T,
    },

    Skeens2MultiReplica {
        loc: OrderIndex,
        trie_slot: *mut *const u8,
        timestamp: u64,
        storage: SkeensMultiStorage,
        t: T,
    },

    Skeens1SingleReplica {
        buffer: BufferSlice,
        storage: *mut u8,
        storage_loc: u64,
        t: T,
    },

    Skeens2SingleReplica {
        index: u64,
        trie_slot: *mut *const u8,
        storage: *const u8,
        t: T,
    },

    //FIXME we don't have a good way to make pointers send...
    Read(Entry<'static>, BufferSlice, T),
    EmptyRead(entry, BufferSlice, T),
    Reply(BufferSlice, T),
    ReturnBuffer(BufferSlice, T),

    GotRecovery(BufferSlice, T),
    DidntGetRecovery(BufferSlice, Uuid, T),

    ContinueRecovery(Buffer, T),
    EndRecovery(Buffer, T),
}

#[derive(Clone, Debug)]
pub struct SkeensMultiStorage(
    Arc<UnsafeCell<
        (Box<[Time]>, Box<[QueueIndex]>, &'static mut [u8], &'static mut [u8], Uuid)
    >>
);

unsafe impl Send for SkeensMultiStorage {}

impl SkeensMultiStorage {
    fn new(
        num_locs: usize,
        entry_size: usize,
        sentinel_size: Option<usize>,
        writer_id: Uuid
    ) -> Self {
            let timestamps = vec![0; num_locs];
            let queue_indicies = vec![0; num_locs];
            let mut data = Vec::with_capacity(entry_size);
            unsafe { data.set_len(entry_size) };
            let timestamps = timestamps.into_boxed_slice();
            let queue_indicies = queue_indicies.into_boxed_slice();
            let data = unsafe { &mut *Box::into_raw(data.into_boxed_slice()) };
            let senti = sentinel_size.map(|s| {
                let mut senti = Vec::with_capacity(s);
                unsafe { senti.set_len(s) };
                unsafe { &mut *Box::into_raw(senti.into_boxed_slice()) }
            }).unwrap_or(&mut []);
            debug_assert_eq!(timestamps.len(), num_locs);
            debug_assert_eq!(timestamps.len(), queue_indicies.len());
            SkeensMultiStorage(Arc::new(UnsafeCell::new(
                (timestamps, queue_indicies, data, senti, writer_id)
            )))
    }

    fn try_unwrap(s: Self) -> Result<UnsafeCell<(Box<[Time]>, Box<[QueueIndex]>, &'static mut [u8], &'static mut [u8], Uuid)>, Self> {
        Arc::try_unwrap(s.0).map_err(SkeensMultiStorage)
    }

    unsafe fn get(&self) -> (&Box<[Time]>, &Box<[QueueIndex]>, &'static [u8], &'static [u8]) {
        let &(ref ts, ref indicies, ref st0, ref st1, ref _id) = &*self.0.get();
        (ts, indicies, *st0, *st1)
    }

    unsafe fn get_id(&self) -> &Uuid {
        let &(ref _ts, ref _indicies, ref _st0, ref _st1, ref id) = &*self.0.get();
        id
    }

    unsafe fn get_mut(&self)
    -> (&mut Box<[Time]>, &mut Box<[QueueIndex]>, &mut [u8], &mut [u8]) {
        let &mut (ref mut ts, ref mut indicies, ref mut st0, ref mut st1, ref _id)
            = &mut *self.0.get();
        (ts, indicies, &mut **st0, &mut **st1)
    }

    fn ptr(&self)
    -> *const UnsafeCell<
        (Box<[Time]>, Box<[QueueIndex]>, &'static mut [u8], &'static mut [u8], Uuid)>
    {
        &*(self.0)
    }

    fn fill_from(&mut self, buffer: &mut Buffer) {
        let (_ts, _indicies, st0, st1) = unsafe { self.get_mut() };
        let len = {
            let mut e = buffer.contents_mut();
            e.flag_mut().insert(EntryFlag::ReadSuccess);
            e.as_ref().len()
        };
        //let num_ts = ts.len();
        st0.copy_from_slice(&buffer[..len]);
        //TODO just copy from sentinel Ref
        let was_multi = buffer.to_sentinel();
        if st1.len() > 0 {
            let len = buffer.contents().len();
            st1.copy_from_slice(&buffer[..len]);
        }
        buffer.from_sentinel(was_multi);
    }
}

/*impl ::std::ops::Deref for SkeensMultiStorage {
    type Target = UnsafeCell<(Box<[Time]>, Box<[QueueIndex]>, &'static mut [u8], &'static mut [u8])>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}*/

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
            | &mut Skeens1Replica {ref mut t, ..}
            | &mut Skeens2MultiReplica {ref mut t,..}
            | &mut Skeens2SingleReplica {ref mut t,..}
            | &mut Skeens1SingleReplica {ref mut t,..}=> f(t),

            &mut GotRecovery(_, ref mut t)
            | &mut DidntGetRecovery(_, _, ref mut t)
            | &mut ContinueRecovery(_, ref mut t)
            | &mut EndRecovery(_, ref mut t) => f(t),

            &mut SingleServerSkeens1(_, ref mut t) => f(t),

            &mut ReturnBuffer(_, ref mut t) | &mut MultiFastPath(_, _, ref mut t) => f(t),
        }
    }
}

impl<T> ToWorker<T>
where T: Copy + Send + Sync {

    #[inline(always)]
    fn get_associated_data(&self) -> T {
        match self {
            &Write(_, _, t) | &Read(_, _, t) | &EmptyRead(_, _, t) | &Reply(_, t) => t,
            &MultiFastPath(_, _, t) => t,
            &MultiReplica{t, ..} => t,
            &Skeens1{t, ..} | &SkeensFinished{t, ..} => t,
            &SingleSkeens {t, ..} | &DelayedSingle {t, .. } => t,
            &Skeens1SingleReplica {t, ..} => t,
            &ReturnBuffer(_, t) => t,
            &SingleServerSkeens1(_, t) => t,

            &Skeens1Replica {t, ..}
            | &Skeens2MultiReplica {t,..}
            | &Skeens2SingleReplica {t,..} => t,

            &GotRecovery(_, t)
            | &DidntGetRecovery(_, _, t)
            | &ContinueRecovery(_, t)
            | &EndRecovery(_, t) => t,
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
    Skeens1(BufferSlice, SkeensMultiStorage),
    SingleSkeens1(BufferSlice, StorageLoc),

    Skeens2(BufferSlice),

    UnLock(BufferSlice),
}

pub enum Recovery {
    TasRecoverer(BufferSlice, Box<(Uuid, Box<[OrderIndex]>)>),
    CheckSkeens1(BufferSlice),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Troption<L, R> {
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
        let (kind, flag) = {
            let c = buffer.contents();
            (c.kind(), *c.flag())
        };
        match kind.layout() {
            EntryLayout::Multiput | EntryLayout::Sentinel => {
                self.handle_multiappend(flag, buffer, storage, t)
            },

            /////////////////////////////////////////////////

            EntryLayout::Read => {
                trace!("SERVER {:?} Read", self.this_server_num);
                let OrderIndex(chain, index) = buffer.contents().locs()[0];
                //TODO validate lock
                //     this will come after per-chain locks
                match self.log.get(&chain) {
                    None => {
                        trace!("SERVER {:?} Read Vacant chain {:?}",
                            self.this_server_num, chain);
                        self.print_data.msgs_sent(1);
                        self.to_workers.send_to_worker(
                            EmptyRead(entry::from(0), buffer, t))
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
                                self.this_server_num, (chain, index), packet.contents().id());
                            //FIXME
                            let packet = unsafe { packet.extend_lifetime() };
                            self.print_data.msgs_sent(1);
                            self.to_workers.send_to_worker(Read(packet, buffer, t))
                        }
                    },
                }
            }

            /////////////////////////////////////////////////

            EntryLayout::Data => {
                trace!("SERVER {:?} Single Append", self.this_server_num);

                let chain = buffer.contents().locs()[0].0;
                debug_assert!(self.stores_chain(chain),
                    "tried to store {:?} at server {:?} of {:?}",
                    chain, self.this_server_num, self.total_servers);

                let this_server_num = self.this_server_num;

                let send = {
                    let log = self.ensure_chain(chain);
                    if log.needs_skeens_single() {
                        let s = log.handle_skeens_single(&mut buffer, t);
                        Troption::Right(s)
                    } else if log.trie.is_locked() {
                        trace!("SERVER append during lock {:?} @ {:?}",
                            log.trie.lock_pair(), chain,
                        );
                        Troption::None
                    } else {
                        //FIXME this shuld be done in the worker thread?
                        let index = log.trie.len();
                        let size = {
                            let mut val = buffer.contents_mut();
                            val.flag_mut().insert(EntryFlag::ReadSuccess);
                            //FIXME 64b entries
                            //FIXME this should be done on the worker?
                            val.locs_mut()[0].1 = entry::from(index as u32);
                            val.as_ref().len()
                        };
                        trace!("SERVER {:?} Writing entry {:?}",
                            this_server_num, (chain, index));
                        let slot = unsafe { log.trie.partial_append(size).extend_lifetime() };
                        Troption::Left(slot)
                    }
                };
                match send {
                    Troption::None => {
                        self.print_data.msgs_sent(1);
                        self.to_workers.send_to_worker(Reply(buffer, t))
                    },
                    Troption::Left(slot) => {
                        self.print_data.msgs_sent(1);
                        self.to_workers.send_to_worker(Write(buffer, slot, t))
                    }
                    Troption::Right((slot, storage_loc, time, queue_num)) => {
                        self.print_data.msgs_sent(1);
                        let msg = SingleSkeens {
                            buffer: buffer,
                            storage: slot,
                            storage_loc: storage_loc,
                            time: time,
                            queue_num: queue_num,
                            t: t,
                        };
                        self.to_workers.send_to_worker(msg);
                    }
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
        kind: EntryFlag::Flag,

        buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(Box<[u8]>, Box<[u8]>)>>,
        t: T
    ) {
        //FXIME fastpath for single server appends
        if !kind.contains(EntryFlag::TakeLock) {
            self.handle_single_server_append(kind, buffer, storage, t)
        } else if kind.contains(EntryFlag::NewMultiPut) {
            self.handle_new_multiappend(kind, buffer, storage, t)
        }
        else {
            self.handle_old_multiappend(kind, buffer, storage, t)
        }
    }

    //////////////////////

    fn handle_single_server_append(
        &mut self,
        kind: EntryFlag::Flag,
        buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(Box<[u8]>, Box<[u8]>)>>,
        t: T
    ) {
        let storage = storage.unwrap_left();

        let (mut needs_lock, mut needs_skeens) = (false, false);
        {
            for &OrderIndex(c, _) in buffer.contents().locs() {
                if c == order::from(0) || !self.stores_chain(c) {
                    continue
                }

                let chain =  self.ensure_chain(c);
                needs_skeens |= chain.needs_skeens_single();
                needs_lock |= chain.trie.is_locked();
            }
        }
        debug_assert!(!(needs_lock && needs_skeens));
        if !needs_lock && !needs_skeens {
            self.single_server_single_append_fast_path(kind, buffer, storage, t)
        } else if needs_skeens {
            self.single_server_local_skeens(kind, buffer, storage, t)
        } else if needs_lock {
            self.multi_append_lock_failed(kind, buffer, storage, t)
        }
    }

    fn single_server_single_append_fast_path(
        &mut self,
        _kind: EntryFlag::Flag,
        mut buffer: BufferSlice,
        storage: SkeensMultiStorage,
        t: T
    ) {
        unsafe {
            let (pointers, _indicies, _st0, _st1) = storage.get_mut();
            let mut contents = buffer.contents_mut();
            let locs = contents.locs_mut();
            let pointers = &mut pointers[..locs.len()];
            for (j, &mut OrderIndex(ref o, ref mut i)) in locs.into_iter().enumerate() {
                if *o == order::from(0) || !self.stores_chain(*o) {
                    *i = entry::from(0);
                    pointers[j] = 0;
                    continue
                }

                let (index, ptr) =
                    self.ensure_chain(*o).trie.prep_append(ptr::null());
                *i = (index as u32).into();
                pointers[j] = ptr as *mut *const u8 as usize as u64;
            }
        }

        self.print_data.msgs_sent(1);
        self.to_workers.send_to_worker(MultiFastPath(buffer, storage, t))
    }

    fn single_server_local_skeens(
        &mut self,
        kind: EntryFlag::Flag,
        mut buffer: BufferSlice,
        storage: SkeensMultiStorage,
        t: T
    ) {
        self.new_multiappend_round1(kind, &mut buffer, &storage, t);

        let max_timestamp = unsafe { storage.get().0.iter().cloned().max() };

        self.print_data.msgs_sent(1);
        self.to_workers.send_to_worker(SingleServerSkeens1(storage, t));

        *buffer.contents_mut().lock_mut() = max_timestamp.unwrap();
        self.new_multiappend_round2(kind, &mut buffer);

        self.print_data.msgs_sent(1);
        self.to_workers.send_to_worker(ReturnBuffer(buffer, t));
    }

    fn multi_append_lock_failed(
        &mut self,
        _kind: EntryFlag::Flag,
        mut buffer: BufferSlice,
        storage: SkeensMultiStorage,
        t: T
    ) {
        {
            for &mut OrderIndex(ref o, ref mut i) in buffer.contents_mut().locs_mut() {
                if *o == order::from(0) || !self.stores_chain(*o) {
                    continue
                }

                if self.ensure_chain(*o).trie.is_locked() {
                    *i = entry::from(::std::u64::MAX as u32)
                }
            }
        }

        mem::drop(storage);

        self.print_data.msgs_sent(1);
        self.to_workers.send_to_worker(Reply(buffer, t));
    }

    //////////////////////

    fn handle_new_multiappend(
        &mut self,
        kind: EntryFlag::Flag,
        mut buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(Box<[u8]>, Box<[u8]>)>>,
        t: T
    ) {
        trace!("SERVER {:?} new-style multiput {:?}", self.this_server_num, kind);
        assert!(kind.contains(EntryFlag::TakeLock));
        if kind.contains(EntryFlag::Unlock) {
            self.new_multiappend_round2(kind, &mut buffer);
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
        kind: EntryFlag::Flag,
        buffer: &mut BufferSlice,
        storage: &SkeensMultiStorage,
        t: T
    ) {
        trace!("SERVER {:?} new-style multiput Round 1 {:?}", self.this_server_num, kind);
        let val = buffer.contents();
        let locs = val.locs();
        let timestamps = &mut unsafe { storage.get_mut().0 }[..locs.len()];
        let queue_indicies = &mut unsafe { storage.get_mut().1 }[..locs.len()];
        let id = val.id().clone();
        for i in 0..locs.len() {
            let chain = locs[i].0;
            if chain == order::from(0) || !self.stores_chain(chain) {
                timestamps[i] = 0;
                continue
            }

            let chain = self.ensure_chain(chain);
            //FIXME handle repeat skeens-1
            let (local_timestamp, num) =
                chain.timestamp_for_multi(id, storage.clone(), t).unwrap();
            timestamps[i] = local_timestamp;
            queue_indicies[i] = num;
        }
        trace!("SERVER {:?} new-style multiput Round 1 timestamps {:?}",
            self.this_server_num, timestamps);
    }

    fn new_multiappend_round2(
        &mut self,
        kind: EntryFlag::Flag,
        buffer: &mut BufferSlice,
    ) {
        // In round two we flush some the queues... an possibly a partial entry...
        let mut val = buffer.contents_mut();
        let id = val.as_ref().id().clone();
        let max_timestamp = val.as_ref().lock_num();
        let locs = val.locs_mut();
        trace!("SERVER {:?} new-style multiput Round 2 {:?} mts {:?}",
            self.this_server_num, kind, max_timestamp
        );
        for i in 0..locs.len() {
            let chain_num = locs[i].0;
            if chain_num == order::from(0) || !self.stores_chain(chain_num) {
                locs[i].1 = entry::from(0);
                continue
            }

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
                    FinishSkeens::Multi(index, trie_slot, storage, timestamp, t) => {
                        trace!("server finish sk multi");
                        print_data.msgs_sent(1);
                        to_workers.send_to_worker(
                            SkeensFinished {
                                loc: OrderIndex(chain_num, (index as u32).into()),
                                trie_slot: trie_slot,
                                storage: storage,
                                timestamp: timestamp,
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
        }
    }

    //////////////////////

    fn handle_old_multiappend(
        &mut self,
        kind: EntryFlag::Flag,
        mut buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(Box<[u8]>, Box<[u8]>)>>,
        t: T
    ) {
        trace!("SERVER {:?} old-style multiput {:?}", self.this_server_num, kind);
        if kind.contains(EntryFlag::Unlock) {
            {
                let mut all_unlocked = true;
                let mut val = buffer.contents_mut();
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
                    trace!("SERVER {} unlocked {:?}",
                        self.this_server_num, val.as_ref().locs());
                    val.flag_mut().insert(EntryFlag::ReadSuccess)
                }
                else {
                    trace!("SERVER {} failed unlock {:?}",
                        self.this_server_num, val.as_ref().locs());
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
            let mut val = buffer.contents_mut();
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
                        if kind.contains(EntryFlag::TakeLock) {
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
            // debug_assert!(!(sentinel_start_index.is_none() && kind.contains(EntryKind::Sentinel)),
            //     "BAD SENTINEL @ {:?}",
            //     locs,
            // );
            if let (Some(ssi), false) = (sentinel_start_index, lock_failed) {
                'senti_horizon: for i in ssi..locs.len() {
                    assert!(locs[i] != OrderIndex(0.into(), 0.into()));
                    let chain = locs[i].0;
                    if self.stores_chain(chain) {
                        //FIXME handle duplicates
                        let next_entry = {
                            let chain = self.ensure_trie(chain);
                            if kind.contains(EntryFlag::TakeLock) {
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
            self.this_server_num, buffer.contents().locs());

        let (multi_storage, senti_storage) = *storage.unwrap_right();
        let multi_storage = unsafe {
            debug_assert_eq!(multi_storage.len(), buffer.entry_size());
            (*Box::into_raw(multi_storage)).as_mut_ptr()
        };
        trace!("multi_storage @ {:?}", multi_storage);
        let senti_storage = unsafe {
            debug_assert_eq!(senti_storage.len(), buffer.contents().sentinel_entry_size());
            (*Box::into_raw(senti_storage)).as_mut_ptr()
        };
        //LOGIC sentinal storage must contain at least 64b
        //      (128b with 64b entry address space)
        //      thus has at least enough storage for 1 ptr per entry
        let mut next_ptr_storage = senti_storage as *mut *mut *const u8;
        {
            let mut val = buffer.contents_mut();
            val.flag_mut().insert(EntryFlag::ReadSuccess);
            'emplace: for &OrderIndex(chain, index) in val.as_ref().locs() {
                if (chain, index) == (0.into(), 0.into()) {
                    break 'emplace
                }
                if self.stores_chain(chain) {
                    assert!(index != entry::from(0));
                    unsafe {
                        let ptr = {
                            let chain = self.ensure_trie(chain);
                            if kind.contains(EntryFlag::TakeLock) {
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
                trace!("SERVER {:?} sentinal locs {:?}",
                    self.this_server_num, &val.as_ref().locs()[ssi..]);
                for &OrderIndex(chain, index) in &val.as_ref().locs()[ssi..] {
                    if self.stores_chain(chain) {
                        assert!(index != entry::from(0));
                        unsafe {
                            let ptr = {
                                let chain = self.ensure_trie(chain);
                                if kind.contains(EntryFlag::TakeLock) {
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
        self.log.entry(chain).or_insert_with(|| unsafe {
            let mut t = Trie::new();
            let _ = t.partial_append(1);
            Chain{ trie: t, skeens: SkeensState::new() }
        })
    }

    fn ensure_trie(&mut self, chain: order) -> &mut Trie {
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

                let loc = buffer.contents().locs()[0];
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
                            storage_loc, size).extend_lifetime()
                    }
                };

                self.print_data.msgs_sent(1);
                self.to_workers.send_to_worker(Write(buffer, slot, t))
            },

            ToReplicate::SingleSkeens1(buffer, storage_loc) => unsafe {
                trace!("SERVER {:?} replicate skeens single", self.this_server_num);
                let (id, (OrderIndex(c, ts), node_num), size) = {
                    let e = buffer.contents();
                    (*e.id(), e.locs_and_node_nums().map(|(&o, &n)| (o, n)).next().unwrap(),
                        e.non_replicated_len())
                };
                let storage;
                {
                    let c = self.ensure_chain(c);
                    storage = c.trie.reserve_space_at(storage_loc, size);
                    c.skeens.replicate_single_append_round1(
                        u32::from(ts) as u64, node_num, id, storage, t
                    );
                }
                self.to_workers.send_to_worker(
                    Skeens1SingleReplica {
                        buffer: buffer,
                        storage: storage,
                        storage_loc: storage_loc,
                        t: t,
                    }
                );
            },

            ToReplicate::Skeens1(buffer, storage) => {
                trace!("SERVER {:?} replicate skeens multiput {:?}",
                    self.this_server_num, buffer.contents());
                let id = *buffer.contents().id();
                'sk_rep: for (&OrderIndex(o, i), &node_num) in buffer.contents().locs_and_node_nums() {
                    if o == order::from(0) || !self.stores_chain(o) { continue 'sk_rep }
                    let c = self.ensure_chain(o);
                    c.skeens.replicate_multi_append_round1(
                        u32::from(i) as u64, node_num, id, storage.clone(), t
                    );
                }
                self.print_data.msgs_sent(1);
                self.to_workers.send_to_worker(
                    Skeens1Replica {
                        buffer: buffer,
                        storage: storage,
                        t: t,
                    }
                );
            }

            ToReplicate::Skeens2(buffer) => {
                use self::ReplicatedSkeens::*;
                trace!("SERVER {:?} replicate skeens2 max", self.this_server_num);
                let id = *buffer.contents().id();
                let max_timestamp = buffer.contents().lock_num();
                'sk2_rep: for &OrderIndex(o, i) in buffer.contents().locs() {
                    if o == order::from(0) || !self.stores_chain(o) { continue 'sk2_rep }
                    //let c = self.ensure_chain(chain);
                    let c = self.log.entry(o).or_insert_with(|| unsafe {
                        let mut t = Trie::new();
                        let _ = t.partial_append(1);
                        Chain{ trie: t, skeens: SkeensState::new() }
                    });
                    let to_workers = &mut self.to_workers;
                    let print_data = &mut self.print_data;
                    let index = u32::from(i) as u64;
                    let trie = &mut c.trie;
                    c.skeens.replicate_round2(&id, max_timestamp, index, |rep| match rep {
                        Multi{index, storage, max_timestamp, t} => {
                            trace!("SERVER finish sk multi rep ({:?}, {:?})", o, index);
                            let slot = unsafe { trie.prep_append_at(index, ptr::null()) };
                            print_data.msgs_sent(1);
                            to_workers.send_to_worker(
                                Skeens2MultiReplica {
                                    loc: OrderIndex(o, (index as u32).into()),
                                    trie_slot: slot,
                                    storage: storage,
                                    timestamp: max_timestamp,
                                    t: t
                                }
                            )
                        },
                        Single{index, storage, t} => unsafe {
                            //trace!("SERVER finish sk single rep");
                            //let size = buffer.entry_size();
                            trace!("SERVER replicating single sk2 ({:?}, {:?})", o, index);
                            let slot = trie.prep_append_at(index, ptr::null());
                            print_data.msgs_sent(1);
                            to_workers.send_to_worker(
                                Skeens2SingleReplica {
                                    index: index,
                                    trie_slot: slot,
                                    storage: storage,
                                    t: t
                                }
                            )
                        }
                    });
                }
                trace!("SRVER {:?} skeens2 over", self.this_server_num);
                self.to_workers.send_to_worker(ReturnBuffer(buffer, t))
            }

            ToReplicate::Multi(buffer, multi_storage, senti_storage) => {
                trace!("SERVER {:?} replicate multiput", self.this_server_num);

                //debug_assert!(self._seen_ids.insert(buffer.entry().id));
                //FIXME this needs to be aware of locks...
                //      but I think only unlocks, the primary
                //      will ensure that locks happen in order...

                let mut sentinel_start_index = None;
                let mut num_places = 0;
                {
                    let val = buffer.contents();
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
                    self.this_server_num, buffer.contents().locs());

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
                'emplace: for &OrderIndex(chain, index) in buffer.contents().locs() {
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
                    trace!("SERVER {:?} sentinal locs {:?}",
                        self.this_server_num, &buffer.contents().locs()[ssi..]);
                    for &OrderIndex(chain, index) in &buffer.contents().locs()[ssi..] {
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

    pub fn handle_recovery(&mut self, recovery: Recovery, t: T) {
        match recovery {
            Recovery::TasRecoverer(buffer, recoverer) => {
                let (&write_id, &old_recoverer) = buffer.contents().write_id_and_old_recoverer();
                let old_recoverer = if old_recoverer == Uuid::nil() {
                    None
                } else {
                    Some(old_recoverer)
                };
                let chain = recoverer.1[0].0;
                let res = self.ensure_chain(chain).skeens
                    .tas_recoverer(write_id, recoverer, old_recoverer);
                self.print_data.msgs_sent(1);
                self.to_workers.send_to_worker(match res {
                    Ok(()) => ToWorker::GotRecovery(buffer, t),
                    Err(id) => ToWorker::DidntGetRecovery(buffer, id.unwrap_or_else(Uuid::nil), t),
                })
            },

            Recovery::CheckSkeens1(buffer) => {
                let (id, OrderIndex(chain, time)) = {
                    let c = buffer.contents();
                    (*c.id(), c.locs()[0])
                };
                let time = u32::from(time) as u64;
                let still_there = self.log.get(&chain)
                    .map(|c| c.skeens.check_skeens1(id, time))
                    .unwrap_or(false);
                self.to_workers.send_to_worker(if still_there {
                    ToWorker::ContinueRecovery(buffer, t)
                } else {
                    ToWorker::EndRecovery(buffer, t)
                });
            },
        }
    }
}

enum FinishSkeens<T> {
    Single(u64, *mut *const u8, *const u8, T),
    Multi(u64, *mut *const u8, SkeensMultiStorage, u64, T),
}

impl<T: Copy> Chain<T> {
    fn needs_skeens_single(&mut self) -> bool {
        !self.skeens.is_empty()
    }

    fn handle_skeens_single(&mut self, buffer: &mut BufferSlice, t: T)
    -> (*mut u8, ByteLoc, Time, QueueIndex) {
        let val = buffer.contents();
        let size = val.len();
        let id = val.id().clone();
        let (slot, storage_loc) = unsafe { self.trie.reserve_space(size) };
        let ts_and_queue_index = self.skeens.add_single_append(id, slot as *const _, t);
        match ts_and_queue_index {
            SkeensAppendRes::NewAppend(ts, queue_num) => {
                trace!("singleton skeens @ {:?}", (ts, queue_num));
                (slot, storage_loc, ts, queue_num)
            },
            _ => unimplemented!(),
        }

    }

    fn timestamp_for_multi(
        &mut self, id: Uuid, storage: SkeensMultiStorage, t: T
    ) -> Result<(Time, QueueIndex), Time> {
        match self.skeens.add_multi_append(id, storage, t) {
            SkeensAppendRes::NewAppend(ts, num) => Ok((ts, num)),
            //TODO
            SkeensAppendRes::OldPhase1(ts) => Err(ts),
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
            SkeensSetMaxRes::Duplicate(_ts) => unimplemented!(),
            SkeensSetMaxRes::NotWaiting => unimplemented!(),
            SkeensSetMaxRes::NeedsFlush => {
                trace!("multi flush due to {:?}", max_timestamp);
                let trie = &mut self.trie;
                self.skeens.flush_got_max_timestamp(|g| {
                    match g {
                        GotMax::SimpleSingle{storage, t, timestamp, ..}
                        | GotMax::Single{storage, t, timestamp, ..} => unsafe {
                            trace!("flush single {:?}", timestamp);
                            let (loc, ptr) = trie.prep_append(ptr::null());
                            //println!("s id {:?} ts {:?}", id, timestamp);
                            on_finish(Single(loc, ptr, storage, t));
                        },
                        GotMax::Multi{storage, t, id, timestamp, ..} => unsafe {
                            trace!("flush multi {:?}: {:?}", id, timestamp);
                            //println!("m id {:?} ts {:?}", id, timestamp);
                            let (loc, ptr) = trie.prep_append(ptr::null());
                            on_finish(Multi(loc, ptr, storage, timestamp, t));
                        },
                    }
                })
            }
        }
    }
}

pub type StorageLoc = u64;
//pub type SkeensTimestamp = u64;
pub type SkeensReplicationOrder = u64;


pub enum ServerResponse<T: Send + Sync>{
    None(BufferSlice, T),
    Echo(BufferSlice, T),
    Read(BufferSlice, T, &'static [u8]),
    EmptyRead(BufferSlice, T),
    FinishedAppend(BufferSlice, T, &'static [u8], StorageLoc),
    FinishedSingletonSkeens1(BufferSlice, T, StorageLoc), //TODO SkeensReplicationOrder),
    FinishedSingletonSkeens2(&'static [u8], T),
    FinishedSkeens1(BufferSlice, T), //TODO &[SkeensReplicationOrder]
    FinishedSkeens2(&'static [u8], T),

    FinishOldMultiappend(BufferSlice, T, &'static [u8]),
}

pub enum ToSend<'a> {
    Nothing,
    Contents(EntryContents<'a>),
    OldContents(EntryContents<'a>, StorageLoc),
    Slice(&'a [u8]),
    StaticSlice(&'static [u8]),
    Read(&'static [u8]),
    OldReplication(&'static [u8], StorageLoc),
}

fn handle_to_worker2<T: Send + Sync, U, SendFn>(
    msg: ToWorker<T>, worker_num: usize, continue_replication: bool, send: SendFn
) -> (Option<BufferSlice>, U)
where SendFn: for<'a> FnOnce(ToSend<'a>, T) -> U {
    match msg {

        ReturnBuffer(buffer, t) => {
            trace!("WORKER {} return buffer", worker_num);
            let u = send(ToSend::Nothing, t);
            (Some(buffer), u)
            //ServerResponse::None(buffer, t)
            //(Some(buffer), &[], t, 0, true)
        },

        Reply(buffer, t) => {
            trace!("WORKER {} finish reply", worker_num);
            let u = send(ToSend::Slice(buffer.entry_slice()), t);
            (Some(buffer), u)
            //ServerResponse::Echo(buffer, t)
            //(Some(buffer), &[], t, 0, false)
        },

        Read(read, buffer, t) => {
            trace!("WORKER {} finish read", worker_num);
            //let bytes = read.bytes();
            //FIXME needless copy
            //buffer.ensure_capacity(bytes.len());
            //buffer[..bytes.len()].copy_from_slice(bytes);
            let u = send(ToSend::Read(read.bytes()), t);
            (Some(buffer), u)
            //ServerResponse::Read(buffer, t, bytes)
            //(Some(buffer), bytes, t, 0, false)
        },

        EmptyRead(last_valid_loc, buffer, t) => {
            let (old_id, old_loc) = {
                let e = buffer.contents();
                (e.id().clone(), e.locs()[0])
            };
            let chain: order = old_loc.0;
            trace!("WORKER {} finish empty read {:?}", worker_num, old_loc);
            let u = send(ToSend::Contents(EntryContents::Read{
                        id: &old_id,
                        flags: &EntryFlag::Nothing,
                        loc: &old_loc,
                        data_bytes: &0,
                        dependency_bytes: &0,
                        horizon: &OrderIndex(chain, last_valid_loc),
                    }), t);
            (Some(buffer), u)
            //ServerResponse::EmptyRead(buffer, t)
            //(Some(buffer), &[], t, 0, false)
        },

        Write(buffer, slot, t) => unsafe {
            trace!("WORKER {} finish write", worker_num);
            let loc = slot.loc();
            let ret = extend_lifetime(slot.finish_append(buffer.entry()).bytes());
            let u = if continue_replication {
                send(ToSend::OldReplication(ret, loc), t)
            } else {
                send(ToSend::StaticSlice(ret), t)
            };
            (Some(buffer), u)
            //ServerResponse::FinishedAppend(buffer, t, ret, loc)
            //(Some(buffer), ret, t, loc, false)
        },

        SingleSkeens { mut buffer, storage, storage_loc, time, queue_num, t, } => unsafe {
            {
                let len = {
                    let mut e = buffer.contents_mut();
                    e.flag_mut().insert(EntryFlag::ReadSuccess);
                    e.as_ref().len()
                };
                ptr::copy_nonoverlapping(buffer[..len].as_ptr(), storage, len);
                buffer.contents_mut().flag_mut().insert(EntryFlag::Skeens1Queued);
            }
            let u = if continue_replication {
                let to_send = buffer.contents().single_skeens_to_replication(&time, &queue_num);
                send(ToSend::OldContents(to_send, storage_loc), t)
            } else {
                send(ToSend::Nothing, t)
            };
            (Some(buffer), u)
            //ServerResponse::FinishedSingletonSkeens1(buffer, t, storage_loc)
            //(Some(buffer), &[], t, storage_loc, true)
        },

        Skeens1SingleReplica { buffer, storage, storage_loc, t, } => unsafe {
            trace!("WORKER {} finish skeens single replication", worker_num);
            let len = buffer.contents().non_replicated_len();
            ptr::copy_nonoverlapping(buffer[..len].as_ptr(), storage, len);
            let mut e = MutEntry::wrap_slice(slice::from_raw_parts_mut(storage, len));
            e.to_non_replicated();
            e.contents().flag_mut().remove(EntryFlag::Skeens1Queued);
            e.contents().flag_mut().insert(EntryFlag::ReadSuccess);
            let u = if continue_replication {
                trace!("WORKER {} skeens no-ack", worker_num);
                //send(ToSend::OldReplication(buffer.entry_slice(), storage_loc), t)
                send(ToSend::OldContents(buffer.contents(), storage_loc), t)
            } else {
                send(ToSend::Nothing, t)
            };
            (Some(buffer), u)
        },

        // Safety, since both the original append and the delayed portion
        // get finished by the same worker this does not race
        // the storage_loc is sent with the first round
        DelayedSingle { index, trie_slot, storage, t, }
        | Skeens2SingleReplica { index, trie_slot, storage, t, } => unsafe {
            //trace!("WORKER {} finish delayed single @ {:?}", worker_num);
            let color;
            let len = {
                let storage = storage as *mut u8;
                let mut e = MutEntry::wrap_bytes(&mut *storage).into_contents();
                {
                    let locs = e.locs_mut();
                    color = locs[0].0;
                    locs[0].1 = entry::from(index as u32);
                }
                debug_assert!(e.flag_mut().contains(EntryFlag::ReadSuccess));
                e.as_ref().len()
            };
            trace!("WORKER {} finish delayed single @ {:?}",
                worker_num, OrderIndex(color, entry::from(index as u32)));
            let trie_entry: *mut AtomicPtr<u8> = trie_slot as *mut _;
            (*trie_entry).store(storage as *mut u8, Ordering::Release);
            let u = if continue_replication {
                let e = Entry::wrap_bytes(&*storage).contents();
                send(ToSend::Contents(EntryContents::Skeens2ToReplica{
                    id: e.id(),
                    lock: &0,//TODO
                    loc: &OrderIndex(color, entry::from(index as u32)),
                }), t)
            } else {
                let ret = slice::from_raw_parts(storage, len);
                send(ToSend::StaticSlice(ret), t)
            };
            (None, u)
            //ServerResponse::FinishedSingletonSkeens2(ret, t)
            //(None, ret, t, 0, false)
        },

        MultiReplica {
            buffer, multi_storage, senti_storage, t, num_places,
        } => unsafe {
            let (remaining_senti_places, len) = {
                let e = buffer.contents();
                let len = e.len();
                let b = &buffer[..len];
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
                let u = if continue_replication {
                    send(ToSend::OldReplication(ret, 0), t)
                } else {
                    send(ToSend::StaticSlice(ret), t)
                };
                return (Some(buffer), u)
                //return ServerResponse::FinishOldMultiappend(buffer, t, ret) //(Some(buffer), ret, t, 0, false)
            }
            else {
                let e = buffer.contents();
                let len = e.sentinel_entry_size();
                trace!("place senti_storage @ {:?}, len {}", senti_storage, len);
                let b = &buffer[..];
                ptr::copy_nonoverlapping(b.as_ptr(), senti_storage, len);
                {
                    let senti_storage = slice::from_raw_parts_mut(senti_storage, len);
                    slice_to_sentinel(&mut *senti_storage);
                }
                for place in remaining_senti_places {
                    let _: *mut *const u8 = place;
                    let trie_entry: *mut AtomicPtr<u8> = mem::transmute(place);
                    //TODO mem barrier ordering
                    (*trie_entry).store(senti_storage, Ordering::Release);
                }
            }
            //TODO is re right for sentinel only writes?
            let u = if continue_replication {
                send(ToSend::OldReplication(ret, 0), t)
            } else {
                send(ToSend::StaticSlice(ret), t)
            };
            (Some(buffer), u)
            //ServerResponse::FinishOldMultiappend(buffer, t, ret)
            //(Some(buffer), ret, t, 0, false)
        },

        MultiFastPath(mut buffer, storage, t) => unsafe {
            trace!("WORKER {} finish fastpath", worker_num);
            {
                let (ptrs, _indicies, st0, st1) = storage.get_mut();
                let len = {
                    let mut e = buffer.contents_mut();
                    e.flag_mut().insert(EntryFlag::ReadSuccess);
                    e.as_ref().len()
                };
                st0.copy_from_slice(&buffer[..len]);
                let was_multi = buffer.to_sentinel();
                if st1.len() > 0 {
                    let len = buffer.contents().len();
                    st1.copy_from_slice(&buffer[..len]);
                }
                {
                    buffer.from_sentinel(was_multi);
                    let mut is_sentinel = false;
                    let locs = buffer.contents().locs();
                    for (&oi, &trie_slot) in locs.iter().zip(ptrs.iter()) {
                        if oi == OrderIndex(0.into(), 0.into()) {
                            is_sentinel = true;
                            continue
                        }
                        if trie_slot == 0 { continue }

                        let trie_slot: u64 = trie_slot;
                        let trie_entry: *mut AtomicPtr<u8> = trie_slot as usize as *mut _;
                        let to_store: *mut u8 = if is_sentinel {
                            st1.as_mut_ptr()
                        } else {
                            st0.as_mut_ptr()
                        };
                        (*trie_entry).store(to_store, Ordering::Release);
                    }
                }
            }
            let (_ptrs, _indicies, st0, _st1) = storage.get();
            let u = if continue_replication {
                send(ToSend::OldReplication(st0, 0), t)
            } else {
                send(ToSend::StaticSlice(st0), t)
            };
            (Some(buffer), u)
        },

        SingleServerSkeens1(storage, t) => unsafe {
            let (ts, indicies, st0, _st1) = storage.get_mut();
            trace!("WORKER {} finish s skeens1 {:?}", worker_num, ts);
            let mut c = bytes_as_entry_mut(st0);
            c.flag_mut().insert(EntryFlag::ReadSuccess);
            let u;
            if continue_replication {
                {
                    c.flag_mut().insert(EntryFlag::Skeens1Queued);
                    let locs = c.locs_mut();
                    for i in 0..locs.len() {
                        locs[i].1 = entry::from(ts[i] as u32)
                    }
                }
                u = send(ToSend::Contents(c.as_ref().multi_skeens_to_replication(indicies)), t);
                {
                    c.flag_mut().remove(EntryFlag::Skeens1Queued);
                    c.locs_mut().iter_mut().fold((),
                        |(), &mut OrderIndex(_, ref mut i)| *i = entry::from(0)
                    );
                }
            } else {
                u = send(ToSend::Nothing, t)
            };
            (None, u)
        },

        Skeens1{mut buffer, storage, t} => unsafe {
            let (ts, indicies, st0, st1) = storage.get_mut();
            trace!("WORKER {} finish skeens1 {:?}", worker_num, ts);
            let len = {
                let mut e = buffer.contents_mut();
                e.flag_mut().insert(EntryFlag::ReadSuccess);
                e.as_ref().len()
            };
            //let num_ts = ts.len();
            st0.copy_from_slice(&buffer[..len]);
            //TODO just copy from sentinel Ref
            let was_multi = buffer.to_sentinel();
            if st1.len() > 0 {
                let len = buffer.contents().len();
                st1.copy_from_slice(&buffer[..len]);
            }
            {
                let mut c = buffer.contents_mut();
                c.flag_mut().insert(EntryFlag::Skeens1Queued);
                let locs = c.locs_mut();
                for i in 0..locs.len() {
                    locs[i].1 = entry::from(ts[i] as u32)
                }
            }
            let u = if continue_replication {
                buffer.from_sentinel(was_multi);
                trace!("WORKER {} skeens1 to rep @ {:?}", worker_num, ts);
                send(ToSend::Contents(
                    buffer.contents().multi_skeens_to_replication(indicies))
                , t)
            } else {
                trace!("WORKER {} skeens1 to client @ {:?}", worker_num, ts);
                send(ToSend::Slice(buffer.entry_slice()), t)
            };
            (Some(buffer), u)
            //ServerResponse::FinishedSkeens1(buffer, t)
            //(Some(buffer), &[], t, 0, false)
        },

        SkeensFinished{loc, trie_slot, storage, timestamp, t,}
        | Skeens2MultiReplica{loc, trie_slot, storage, timestamp, t} => unsafe {
            trace!("WORKER {} finish skeens2 @ {:?}", worker_num, loc);
            let chain = loc.0;
            let id;
            {
                let (_ts, _indicies, st0, st1) = storage.get_mut();
                let is_sentinel = {
                    let mut st0 = bytes_as_entry_mut(st0);
                    id = *st0.as_ref().id();
                    let st0_l = st0.locs_mut();
                    let i = st0_l.iter().position(|oi| oi.0 == chain).unwrap();
                    //FIXME atomic?
                    st0_l[i].1 = loc.1;
                    if st1.len() > 0 {
                        bytes_as_entry_mut(st1).locs_mut()[i].1 = loc.1;
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
            }

            let to_send = if continue_replication {
                trace!("WORKER {} continue skeens2 replication @ {:?}", worker_num, loc);
                ToSend::Contents(EntryContents::Skeens2ToReplica{
                    id: &id,
                    lock: &timestamp,
                    loc: &loc,
                })
            } else {
                match SkeensMultiStorage::try_unwrap(storage) {
                    Ok(storage) => {
                        trace!("WORKER {} skeens2 to client @ {:?}", worker_num, loc);
                        let &(_, _, ref st0, ref st1, _) = &*storage.get();
                        ToSend::StaticSlice(if st1.len() > 0 { *st1 } else { *st0 })
                    }
                    Err(..) => {
                        trace!("WORKER {} incomplete skeens2 @ {:?}", worker_num, loc);
                        ToSend::Nothing
                    }
                }
            };

            let u = send(to_send, t);
            (None, u)

            //ServerResponse::FinishedSkeens2(if st1.len() > 0 { &**st1 } else { &**st0 }, t)
            //(None, if st1.len() > 0 { &**st1 } else { &**st0 }, t, 0, false)
        },

        Skeens1Replica{mut buffer, storage, t} => unsafe {
            let (ts, indicies, st0, st1) = storage.get_mut();
            let len = buffer.contents().non_replicated_len();
            st0.copy_from_slice(&buffer[..len]);
            let is_multi_server = {
                MutEntry::wrap_slice(st0).to_non_replicated();
                let mut e = bytes_as_entry_mut(st0);
                e.locs_mut().iter_mut().enumerate().fold((),
                    |(), (j, &mut OrderIndex(_, ref mut i))| {
                        ts[j] = u32::from(*i) as u64;
                        *i = entry::from(0);
                });
                e.flag_mut().remove(EntryFlag::Skeens1Queued);
                e.flag_mut().contains(EntryFlag::TakeLock)
            };
            indicies.iter_mut().zip(buffer.contents().queue_nums().iter()).fold((),
                    |(), (q, num)| *q = *num);
            trace!("WORKER {} finish skeens1 rep {:?}", worker_num, ts);
            //TODO just copy from sentinel Ref
            let was_multi = buffer.to_sentinel();
            if st1.len() > 0 {
                trace!("WORKER {} finish skeens1 rep sentinel", worker_num);
                let len = buffer.contents().len();
                st1.copy_from_slice(&buffer[..len]);
                slice_to_sentinel(st1);
                let mut e = bytes_as_entry_mut(st0);
                e.locs_mut().iter_mut().fold((),
                    |(), &mut OrderIndex(_, ref mut i)| *i = entry::from(0));
                e.flag_mut().remove(EntryFlag::Skeens1Queued);
            }
            let u = if continue_replication {
                buffer.skeens1_rep_from_sentinel(was_multi);
                send(ToSend::Slice(buffer.entry_slice()) , t)
            } else if is_multi_server {
                send(ToSend::Slice(buffer.entry_slice()), t)
            } else {
                send(ToSend::Nothing, t)
            };
            (Some(buffer), u)
        },

        GotRecovery(mut buffer, t) => {
            {
                let mut e = buffer.contents_mut();
                e.flag_mut().insert(EntryFlag::ReadSuccess);
            }
            let u = if continue_replication {
                unimplemented!()
            } else {
                send(ToSend::Slice(buffer.entry_slice()), t)
            };
            (Some(buffer), u)
        },

        DidntGetRecovery(mut buffer, id, t) => {
            {
                let mut e = buffer.contents_mut();
                e.recoverer_is(id);
            }
            let u = if continue_replication {
                unimplemented!()
            } else {
                send(ToSend::Slice(buffer.entry_slice()), t)
            };
            (Some(buffer), u)
        },

        ToWorker::ContinueRecovery(mut buffer, t) => {
            {
                let mut e = buffer.contents_mut();
                e.flag_mut().insert(EntryFlag::ReadSuccess);
            }
            let u = if continue_replication {
                unimplemented!()
            } else {
                send(ToSend::Slice(buffer.entry_slice()), t)
            };
            (Some(buffer), u)
        },

        ToWorker::EndRecovery(buffer, t) => {
            let u = if continue_replication {
                unimplemented!()
            } else {
                send(ToSend::Slice(buffer.entry_slice()), t)
            };
            (Some(buffer), u)
        },
    }
}

/*fn handle_to_worker_for_replication<T: Send + Sync, U, ToSend>(
    msg: ToWorker<T>, worker_num: usize, send: ToSend
) -> (Option<BufferSlice>, U)
where ToSend: for<'a> FnOnce(Troption<EntryContents<'static>, EntryContents<'a>>, T) -> U {
*/

unsafe fn extend_lifetime<'a, 'b, V: ?Sized>(v: &'a V) -> &'b V {
    ::std::mem::transmute(v)
}
