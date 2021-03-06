#![allow(deprecated)] //TODO we're using an old mio, need to move queues in tree and update

#[macro_use] extern crate log;

extern crate byteorder;
extern crate deque;
extern crate evmap;
extern crate lazycell;
extern crate mio;
extern crate uuid;
extern crate reactor;

extern crate fuzzy_log_packets;
#[macro_use] extern crate fuzzy_log_util;

use fuzzy_log_packets as packets;
use fuzzy_log_packets::{buffer, storeables};

use fuzzy_log_util::{hash, socket_addr, vec_deque_map};

//use std::collections::HashSet;
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::{mem, ptr, slice};
//use std::rc::Rc;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;

// use prelude::*;
use buffer::Buffer;

use skeens::{
    SkeensState,
    Time,
    QueueIndex,
};
use trie::{AppendSlot, Trie};

use evmap::{WriteHandle, ReadHandle};

use self::ToWorker::*;

use packets::*;

use self::trivial_eq_arc::TrivialEqArc;

pub use self::worker_thread::{handle_to_worker2, ToSend};

use self::trie::ValEdge;

use self::shared_slice::RcSlice;

pub mod tcp;
// pub mod udp;

pub mod spmc;
pub mod spsc;

mod skeens;
//TODO remove `pub`, it only exists for testing purposes
pub mod trie;
pub mod byte_trie;

pub mod trivial_eq_arc;

mod ordering_thread;
pub mod worker_thread;
pub mod shared_slice;

#[cfg(test)]
mod tests;

pub type ChainStore<T> = WriteHandle<order, TrivialEqArc<Chain<T>>>;
pub type ChainReader<T> = ReadHandle<order, TrivialEqArc<Chain<T>>>;

pub struct ServerLog<T: Send + Sync + Copy, ToWorkers>
where ToWorkers: DistributeToWorkers<T> {
    log: ChainStore<T>,
    //TODO per chain locks...
    total_servers: u32,
    this_server_num: u32,
    // seen_ids: hash::UuidHashSet,
    pub to_workers: ToWorkers, //spmc::Sender<ToWorker<T>>,
    _pd: PhantomData<T>,

    print_data: LogData,
}

pub fn new_chain_store_and_reader<T: Copy>() -> (ChainStore<T>, ChainReader<T>) {
    let (read, write) = ::evmap::new();
    (write, read)
}

counters! {
    struct LogData {
        msgs_recvd: u64,
        msgs_sent: u64,
    }
}

pub struct Chain<T: Copy> {
    trie: Trie,
    skeens: SkeensState<T>,
}

unsafe impl<T: Copy> Sync for Chain<T> {}

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
        storage: Box<(RcSlice, RcSlice)>,
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
        trie_slot: *mut ValEdge,
        storage: SkeensMultiStorage,
        timestamp: u64,
        t: T,
    },

    SnapSkeensFinished {
        loc: OrderIndex,
        storage: SkeensMultiStorage,
        timestamp: u64,
        t: T,
    },

    SnapshotSkeens1 {
        buffer: BufferSlice,
        storage: SkeensMultiStorage,
        t: T,
    },

    SnapshotSkeens1Replica {
        buffer: BufferSlice,
        storage: SkeensMultiStorage,
        t: T,
    },

    SingleServerSkeens1(SkeensMultiStorage, T),

    //FIXME this is getting too big...
    //This is not racey b/c both this and DelayedSingle single get sent to the same thread
    SingleSkeens {
        buffer: BufferSlice,
        storage: ValEdge,
        storage_loc: u64,
        time: Time,
        queue_num: QueueIndex,
        t: T,
    },

    DelayedSingle {
        index: u64,
        trie_slot: *mut ValEdge,
        storage: ValEdge,
        timestamp: u64,
        t: T,
    },

    Skeens1Replica {
        buffer: BufferSlice,
        storage: SkeensMultiStorage,
        t: T,
    },

    Skeens2MultiReplica {
        loc: OrderIndex,
        trie_slot: *mut ValEdge,
        timestamp: u64,
        storage: SkeensMultiStorage,
        t: T,
    },

    Skeens2SnapReplica {
        loc: OrderIndex,
        timestamp: u64,
        storage: SkeensMultiStorage,
        t: T,
    },

    Skeens1SingleReplica {
        buffer: BufferSlice,
        storage: ValEdge,
        storage_loc: u64,
        t: T,
    },

    Skeens2SingleReplica {
        index: u64,
        trie_slot: *mut ValEdge,
        storage: ValEdge,
        timestamp: u64,
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
    Arc<UnsafeCell<(Box<[Time]>, Box<[QueueIndex]>, RcSlice, Option<RcSlice>)>>
);

unsafe impl Send for SkeensMultiStorage {}

impl SkeensMultiStorage {
    pub fn new(num_locs: usize, entry_size: usize, sentinel_size: Option<usize>) -> Self {
            let timestamps = vec![0; num_locs];
            let queue_indicies = vec![0; num_locs];
            let data = RcSlice::with_len(entry_size);
            let timestamps = timestamps.into_boxed_slice();
            let queue_indicies = queue_indicies.into_boxed_slice();
            let senti = sentinel_size.map(|s| {
                RcSlice::with_len(s)
            });
            debug_assert_eq!(timestamps.len(), num_locs);
            debug_assert_eq!(timestamps.len(), queue_indicies.len());
            SkeensMultiStorage(Arc::new(UnsafeCell::new((timestamps, queue_indicies, data, senti))))
    }

    fn try_unwrap(s: Self)
    -> Result<UnsafeCell<(Box<[Time]>, Box<[QueueIndex]>, RcSlice, Option<RcSlice>)>, Self> {
        Arc::try_unwrap(s.0).map_err(SkeensMultiStorage)
    }

    unsafe fn get(&self) -> (&Box<[Time]>, &Box<[QueueIndex]>, &RcSlice, &Option<RcSlice>) {
        let &(ref ts, ref indicies, ref st0, ref st1) = &*self.0.get();
        (ts, indicies, st0, st1)
    }

    unsafe fn get_mut(&self)
    -> (&mut Box<[Time]>, &mut Box<[QueueIndex]>, &mut RcSlice, &mut Option<RcSlice>) {
        let &mut (ref mut ts, ref mut indicies, ref mut st0, ref mut st1) = &mut *self.0.get();
        (ts, indicies, st0, st1)
    }

    fn ptr(&self)
    -> *const UnsafeCell<(Box<[Time]>, Box<[QueueIndex]>, RcSlice, Option<RcSlice>)>
    {
        &*(self.0)
    }

    pub fn fill_from(&mut self, buffer: &mut Buffer) {
        let (_ts, _indicies, st0, st1) = unsafe { self.get_mut() };
        let len = {
            let mut e = buffer.contents_mut();
            e.flag_mut().insert(EntryFlag::ReadSuccess);
            e.as_ref().len()
        };
        //let num_ts = ts.len();
        st0.copy_from_slice(&buffer[..len]);
        //TODO just copy from sentinel Ref
        if let Some(st1) = st1.as_mut() {
            let was_multi = buffer.to_sentinel();
            let len = buffer.contents().len();
            st1.copy_from_slice(&buffer[..len]);
            buffer.from_sentinel(was_multi);
        }
    }
}

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
            | &mut Skeens1SingleReplica {ref mut t,..}
            | &mut SnapshotSkeens1{ref mut t, ..}
            | &mut SnapSkeensFinished{ref mut t, ..}
            | &mut SnapshotSkeens1Replica{ref mut t, ..}
            | &mut Skeens2SnapReplica{ref mut t, ..} => f(t),

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
    pub fn get_associated_data(&self) -> T {
        match self {
            &Write(_, _, t) | &Read(_, _, t) | &EmptyRead(_, _, t) | &Reply(_, t) => t,
            &MultiFastPath(_, _, t) => t,
            &MultiReplica{t, ..} => t,
            &Skeens1{t, ..} | &SkeensFinished{t, ..} => t,
            &SingleSkeens {t, ..} | &DelayedSingle {t, .. } => t,
            &Skeens1SingleReplica {t, ..} => t,
            &ReturnBuffer(_, t) => t,
            &SingleServerSkeens1(_, t) => t,
            &SnapshotSkeens1{t, ..} | &SnapSkeensFinished{t, ..} => t,

            &Skeens1Replica {t, ..}
            | &Skeens2MultiReplica {t,..}
            | &Skeens2SnapReplica{t, ..}
            | &Skeens2SingleReplica {t,..}
            | &SnapshotSkeens1Replica{t, ..} => t,

            &GotRecovery(_, t)
            | &DidntGetRecovery(_, _, t)
            | &ContinueRecovery(_, t)
            | &EndRecovery(_, t) => t,
        }
    }
}

unsafe impl<T> Send for ToWorker<T> where T: Send + Sync {}

pub type StorageLoc = u64;
//pub type SkeensTimestamp = u64;
pub type SkeensReplicationOrder = u64;

pub enum ToReplicate {
    Data(BufferSlice, u64),
    Multi(BufferSlice, Box<(RcSlice, RcSlice)>),
    Skeens1(BufferSlice, SkeensMultiStorage),
    SingleSkeens1(BufferSlice, StorageLoc),

    SnapshotSkeens1(BufferSlice, SkeensMultiStorage),

    Skeens2(BufferSlice),

    UnLock(BufferSlice),

    GC(BufferSlice),

    TasRecoverer(BufferSlice, Box<(Uuid, Box<[OrderIndex]>)>),
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
    pub fn unwrap_left(self) -> L {
        match self {
            Troption::Left(l) => l,
            _ => unreachable!(),
        }
    }

    pub fn unwrap_right(self) -> R {
        match self {
            Troption::Right(r) => r,
            _ => unreachable!(),
        }
    }
}

unsafe fn extend_lifetime<'a, 'b, V: ?Sized>(v: &'a V) -> &'b V {
    ::std::mem::transmute(v)
}
