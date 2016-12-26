use std::collections::HashSet;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::marker::PhantomData;
use std::rc::Rc;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

use prelude::*;
use buffer::Buffer;

use servers2::trie::{AppendSlot, Trie};

use hash::HashMap;

use self::ToWorker::*;

pub mod tcp;
//pub mod udp;

pub mod spmc;

mod trie;

struct ServerLog<T: Send + Sync, ToWorkers>
where ToWorkers: DistributeToWorkers<T> {
    log: HashMap<order, Trie<Entry<()>>>,
    //TODO per chain locks...
    last_lock: u64,
    last_unlock: u64,
    total_servers: u32,
    this_server_num: u32,
    _seen_ids: HashSet<Uuid>,
    to_workers: ToWorkers, //spmc::Sender<ToWorker<T>>,
    _pd: PhantomData<T>,
}

pub trait DistributeToWorkers<T: Send + Sync> {
    fn send_to_worker(&mut self, msg: ToWorker<T>);
}

impl<T: Send + Sync> DistributeToWorkers<T> for spmc::Sender<ToWorker<T>> {
    fn send_to_worker(&mut self, msg: ToWorker<T>) {
        self.send(msg)
    }
}

//TODO
pub type BufferSlice = Buffer;

pub enum ToWorker<T: Send + Sync> {
    Write(BufferSlice, AppendSlot<Entry<()>>, T),
    MultiWrite(Arc<BufferSlice>, AppendSlot<Entry<()>>, Arc<(AtomicIsize, T)>, ),
    SentinelWrite(Arc<BufferSlice>, AppendSlot<Entry<()>>, Arc<(AtomicIsize, T)>),
    //FIXME we don't have a good way to make pointers send...
    Read(&'static Entry<()>, BufferSlice, T),
    EmptyRead(entry, BufferSlice, T),
    Reply(BufferSlice, T),
}

impl<T> ToWorker<T>
where T: Copy + Send + Sync {
    fn get_associated_data(&self) -> T {
        match self {
            &Write(_, _, t) | &Read(_, _, t) | &EmptyRead(_, _, t) | &Reply(_, t) => t,
            &MultiWrite(_, _, ref at) | &SentinelWrite(_, _, ref at) => at.1
        }
    }
}

impl<T: Send + Sync, ToWorkers> ServerLog<T, ToWorkers>
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
            last_lock: 0,
            last_unlock: 0,
            _seen_ids: HashSet::new(),
            this_server_num: this_server_num,
            total_servers: total_servers,
            to_workers: to_workers,
            _pd: PhantomData
        }
    }


    //FIXME pass buffer-slice so we can read batches
    //TODO replace T with U and T: ReturnAs<U> so we don't have to send as much data
    fn handle_op(&mut self, mut buffer: BufferSlice, t: T) {
        let kind = buffer.entry().kind;
        match kind.layout() {
            EntryLayout::Multiput | EntryLayout::Sentinel => {
                trace!("SERVER {:?} multiput", self.this_server_num);
                //TODO handle sentinels
                //TODO check TakeLock flag?
                if !self.try_lock(buffer.entry().lock_num()) {
                    trace!("SERVER {:?} wrong lock {} @ ({},{})", self.this_server_num,
                        buffer.entry().lock_num(), self.last_lock, self.last_unlock);
                        self.to_workers.send_to_worker(Reply(buffer, t));
                        return
                }

                debug_assert!(self._seen_ids.insert(buffer.entry().id));
                if kind.contains(EntryKind::TakeLock) {
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
                }

                let mut sentinel_start_index = None;
                let mut num_places = 0;
                {
                    let val = buffer.entry_mut();
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
                            num_places += 1;
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
                                num_places += 1;
                            }
                        }
                    }
                    //trace!("SERVER {:?} Horizon B {:?}", self.horizon);
                    trace!("SERVER {:?} locs {:?}", self.this_server_num, locs);
                    trace!("SERVER {:?} ssi {:?}", self.this_server_num, sentinel_start_index);
                }

                trace!("SERVER {:?} appending at {:?}",
                    self.this_server_num, buffer.entry().locs());

                let marker = Arc::new((AtomicIsize::new(num_places), t));
                let val = Arc::new(buffer);
                'emplace: for &OrderIndex(chain, index) in val.entry().locs() {
                    if (chain, index) == (0.into(), 0.into()) {
                        break 'emplace
                    }
                    if self.stores_chain(chain) {
                        assert!(index != entry::from(0));
                        let slot = unsafe {
                            self.ensure_chain(chain).partial_append(val.entry_size())
                        };
                        self.to_workers.send_to_worker(MultiWrite(val.clone(), slot, marker.clone()));
                    }
                    // trace!("SERVER {:?} appended at {:?}", loc);
                }
                if let Some(ssi) = sentinel_start_index {
                    //val.kind.remove(EntryKind::Multiput);
                    //val.kind.insert(EntryKind::Sentinel);
                    trace!("SERVER {:?} sentinal locs {:?}", self.this_server_num, &val.entry().locs()[ssi..]);
                    for &OrderIndex(chain, index) in &val.entry().locs()[ssi..] {
                        if self.stores_chain(chain) {
                            assert!(index != entry::from(0));
                            let slot = unsafe {
                                self.ensure_chain(chain)
                                    .partial_append(
                                        val.entry().sentinel_entry_size()
                                    )
                            };
                            self.to_workers.send_to_worker(
                                SentinelWrite(val.clone(), slot, marker.clone())
                            );
                        }
                        // trace!("SERVER {:?} appended at {:?}", loc);
                    }
                    return
                    //val.kind.remove(EntryKind::Sentinel);
                    //val.kind.insert(EntryKind::Multiput);
                }
            }

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
                            EntryKind::Read => self.to_workers.send_to_worker(
                                EmptyRead(entry::from(0), buffer, t)),
                            _ => trace!("SERVER {:?} nop {:?}",
                                self.this_server_num, buffer.entry().kind)
                        }
                    }
                    Some(log) => match log.get(index.into()) {
                        None => self.to_workers.send_to_worker(
                            EmptyRead(entry::from(log.len() - 1), buffer, t)),
                        Some(packet) => {
                            trace!("SERVER {:?} Read Occupied entry {:?} {:?}",
                                self.this_server_num, (chain, index), packet.id);
                            //FIXME
                            let packet = unsafe { extend_lifetime(packet) };
                            self.to_workers.send_to_worker(Read(packet, buffer, t))
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
                    self.to_workers.send_to_worker(Reply(buffer, t));
                    return
                }

                let chain = buffer.entry().locs()[0].0;
                debug_assert!(self.stores_chain(chain),
                    "tried to store {:?} at server {:?} of {:?}",
                    chain, self.this_server_num, self.total_servers);

                let this_server_num = self.this_server_num;
                let slot = {
                    let log = self.ensure_chain(chain);
                    //TODO where should this be done, here or in the worker thread?
                    let index = log.len();
                    let val = buffer.entry_mut();
                    val.kind.insert(EntryKind::ReadSuccess);
                    let size = val.entry_size();
                    let l = unsafe { &mut val.as_data_entry_mut().flex.loc };
                    l.1 = entry::from(index);
                    trace!("SERVER {:?} Writing entry {:?}",
                        this_server_num, (chain, index));
                    unsafe { log.partial_append(size) }
                };

                self.to_workers.send_to_worker(Write(buffer, slot, t))
            }

            EntryLayout::Lock => {
                trace!("SERVER {:?} Lock", self.this_server_num);
                let lock_num = unsafe { buffer.entry().as_lock_entry().lock };
                if kind.is_taking_lock() {
                    trace!("SERVER {:?} TakeLock {:?}", self.this_server_num, self.last_lock);
                    let acquired_loc = self.try_lock(lock_num);
                    if acquired_loc {
                        buffer.entry_mut().kind.insert(EntryKind::ReadSuccess);
                    }
                    else {
                        unsafe { buffer.entry_mut().as_lock_entry_mut().lock = self.last_lock };
                    }
                    self.to_workers.send_to_worker(Reply(buffer, t))
                }
                else {
                    trace!("SERVER {:?} UnLock {:?}", self.this_server_num, self.last_lock);
                    if lock_num == self.last_lock {
                        trace!("SERVER {:?} Success", self.this_server_num);
                        buffer.entry_mut().kind.insert(EntryKind::ReadSuccess);
                        self.last_unlock = self.last_lock;
                    }
                    self.to_workers.send_to_worker(Reply(buffer, t))
                }
            }
        }
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

struct LogWorker {
    id: usize,
}

fn handle_to_worker<T: Send + Sync>(msg: ToWorker<T>, worker_num: usize)
-> Option<(Buffer, T)> {
    match msg {
        Reply(buffer, t) => {
            trace!("WORKER {} finish reply", worker_num);
            Some((buffer, t))
        },

        Write(buffer, slot, t) => unsafe {
            trace!("WORKER {} finish write", worker_num);
            slot.finish_append(buffer.entry());
            Some((buffer, t))
        },

        Read(read, mut buffer, t) => {
            trace!("WORKER {} finish read", worker_num);
            let bytes = read.bytes();
            buffer.ensure_capacity(bytes.len());
            buffer[..bytes.len()].copy_from_slice(bytes);
            Some((buffer, t))
        },

        EmptyRead(last_valid_loc, mut buffer, t) => {
            let (old_id, old_loc) = {
                let e = buffer.entry();
                (e.id, e.locs()[0])
            };
            {
                let chain: order = old_loc.0;
                debug_assert!(last_valid_loc == 0.into()
                    || last_valid_loc < old_loc.1,
                    "{:?} >= {:?}", last_valid_loc, old_loc);
                let e = buffer.fill_from_entry_contents(
                    EntryContents::Data(&(), &[OrderIndex(chain, last_valid_loc)]));
                e.id = old_id;
                e.kind = EntryKind::NoValue;
            }
            trace!("WORKER {} finish empty read {:?}", worker_num, old_loc);
            Some((buffer, t))
        },

        MultiWrite(buffer, slot, marker) => {
            trace!("WORKER {} finish multi part", worker_num);
            unsafe { slot.finish_append(buffer.entry()) };
            return_if_last(buffer, marker, worker_num)
        },

        SentinelWrite(buffer, slot, marker) => {
            trace!("WORKER finish sentinel part");
            {
                let e = buffer.entry();
                unsafe {
                    slot.finish_append_with(e, |e| {
                        e.kind.remove(EntryKind::Multiput);
                        e.kind.insert(EntryKind::Sentinel);
                    });
                }
            };
            return_if_last(buffer, marker, worker_num)
        },
    }
}

fn return_if_last<T: Send + Sync>(
    mut buffer: Arc<Buffer>,
    mut marker: Arc<(AtomicIsize, T)>,
    worker_num: usize,
)
-> Option<(Buffer, T)> {
    let old = marker.0.fetch_sub(1, Ordering::AcqRel);
    if old == 1 {
        //FIXME I don't think this is needed...
        if marker.0.compare_and_swap(0, -1, Ordering::AcqRel) == 0 {
            trace!("WORKER {} finish last part", worker_num);
            let marker_inner;
            let buffer_inner;
            loop {
                match Arc::try_unwrap(marker) {
                    Ok(i) => { marker_inner = i; break }
                    Err(a) => marker = a,
                }
            }
            loop {
                match Arc::try_unwrap(buffer) {
                    Ok(i) => { buffer_inner = i; break }
                    Err(a) => buffer = a,
                }
            }
            return Some((buffer_inner, marker_inner.1))
        }
    }
    return None
}

unsafe fn extend_lifetime<'a, 'b, V: ?Sized>(v: &'a V) -> &'b V {
    ::std::mem::transmute(v)
}
