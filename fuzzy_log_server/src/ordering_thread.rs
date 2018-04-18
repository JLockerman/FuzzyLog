use super::*;
use super::shared_slice::RcSlice;

use skeens::{
    SkeensState,
    SkeensAppendRes,
    SkeensSetMaxRes,
    GotMax,
    ReplicatedSkeens,
    Time,
    QueueIndex,
};
use trie::{ByteLoc, Trie, ValEdge};


impl<T: Copy> Chain<T> {
    fn needs_skeens_single(&mut self) -> bool {
        !self.skeens.is_empty()
    }

    fn handle_skeens_single(&mut self, buffer: &mut BufferSlice, timestamp: Time, t: T)
    -> (ValEdge, ByteLoc, Time, QueueIndex) {
        let val = buffer.contents();
        let size = val.len();
        let id = val.id().clone();
        let (slot, storage_loc) = unsafe { self.trie.reserve_space(size) };
        let ts_and_queue_index = self.skeens.add_single_append(id, timestamp, slot, t);
        match ts_and_queue_index {
            SkeensAppendRes::NewAppend(ts, queue_num) => {
                trace!("singleton skeens @ {:?}", (ts, queue_num));
                (slot, storage_loc, ts, queue_num)
            },
            _ => unimplemented!(),
        }

    }

    fn timestamp_for_multi(
        &mut self, id: Uuid, storage: SkeensMultiStorage, is_sentinel: bool, t: T
    ) -> Result<(Time, QueueIndex), Time> {
        match self.skeens.add_multi_append(id, storage, is_sentinel, t) {
            SkeensAppendRes::NewAppend(ts, num) => Ok((ts, num)),
            //TODO
            SkeensAppendRes::OldPhase1(ts) => Err(ts),
            SkeensAppendRes::Phase2(ts) => Err(ts),
        }
    }

    fn timestamp_for_snap(&mut self, id: Uuid, storage: SkeensMultiStorage, t: T)
    -> Result<(Time, QueueIndex), Time> {
        match self.skeens.add_snapshot(id, storage, t) {
            SkeensAppendRes::NewAppend(ts, num) => Ok((ts, num)),
            //TODO
            SkeensAppendRes::OldPhase1(ts) => Err(ts),
            SkeensAppendRes::Phase2(ts) => Err(ts),
        }
    }

    fn finish_multi<F>(
        &mut self, id: Uuid, max_timestamp: u64, chain: order, mut on_finish: F)
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
                            let (loc, ptr) = trie.prep_append(ValEdge::null());
                            //println!("s id {:?} ts {:?}", id, timestamp);
                            on_finish(Single(loc, ptr, storage, timestamp, t));
                        },
                        GotMax::Multi{storage, t, id, timestamp, ..} => unsafe {
                            trace!("flush multi {:?}: {:?}", id, timestamp);
                            //println!("m id {:?} ts {:?}", id, timestamp);
                            let (loc, ptr) = trie.prep_append(ValEdge::null());
                            on_finish(Multi(loc, ptr, storage, timestamp, t));
                        },
                        GotMax::Senti{storage, t, id, timestamp, ..} => {
                            trace!("flush senti {:?}: {:?}", id, timestamp);
                            let loc = horizon_or_add_blank(trie, chain);
                            on_finish(Multi(
                                loc, ptr::null_mut(), storage, timestamp, t)
                            );
                        },
                        GotMax::Snap{storage, t, id, timestamp, ..} => {
                            trace!("flush snap {:?}: {:?}", id, timestamp);
                            let loc = trie.horizon();
                            on_finish(Snap(
                                loc, storage, timestamp, t)
                            );
                        },
                    }
                })
            }
        }
    }
}

fn horizon_or_add_blank(trie: &mut Trie, chain: order) -> u64 {
    let horizon = trie.horizon();
    if horizon > 0 {
        return horizon
    }

    let blank = EntryContents::Senti {
        id: &Uuid::nil(),
        flags: &EntryFlag::ReadSuccess,
        locs: &[OrderIndex(chain, 1.into())],
        lock: &0,
        data_bytes: &0,
        deps: &[],
    };
    unsafe {
        trie.partial_append(blank.len()).finish_append_with_contents(blank);
    }
    trie.horizon()
}

//SAFETY: log is single writer and Chain refs never escape this thread
fn ensure_chain<T: Copy>(log: &mut ChainStore<T>, chain: order) -> &mut Chain<T> {
    let c = log.get_and(&chain, |chains| unsafe { &mut *UnsafeCell::get(&chains[0]) });
    if let Some(chain) = c {
        return chain
    }

    let mut t = Trie::new();
    //FIXME remove for GC
    unsafe {
        t.partial_append(1).write_byte(mem::transmute(EntryKind::Read));
    };
    let contents = TrivialEqArc::new(Chain{ trie: t, skeens: SkeensState::new()});
    log.insert(chain, contents);
    log.refresh();
    get_chain_mut(log, chain).unwrap()
}

fn get_chain_mut<T: Copy>(log: &mut ChainStore<T>, chain: order) -> Option<&mut Chain<T>> {
    log.get_and(&chain, |chains| unsafe { &mut *UnsafeCell::get(&chains[0]) })
}

fn get_chain<T: Copy>(log: &ChainStore<T>, chain: order) -> Option<&Chain<T>> {
    log.get_and(&chain, |chains| unsafe { &*UnsafeCell::get(&chains[0]) })
}

enum FinishSkeens<T> {
    Single(u64, *mut ValEdge, ValEdge, u64, T),
    Multi(u64, *mut ValEdge, SkeensMultiStorage, u64, T),
    Snap(u64, SkeensMultiStorage, u64, T),
}

impl<T: Send + Sync + Copy, ToWorkers> ServerLog<T, ToWorkers>
where ToWorkers: DistributeToWorkers<T> {

    pub fn new(
        this_server_num: u32,
        total_servers: u32,
        to_workers: ToWorkers,
        chains: ChainStore<T>,
    ) -> Self {
        //TODO
        //assert!(this_server_num > 0);
        assert!(this_server_num <= total_servers,
            "this_server_num <= total_servers, {:?} <= {:?}",
            this_server_num, total_servers);
        ServerLog {
            log: chains,
            // seen_ids: Default::default(),
            this_server_num: this_server_num,
            total_servers: total_servers,
            to_workers: to_workers,
            _pd: PhantomData,
            print_data: Default::default(),
        }
    }

    #[cfg(feature = "print_stats")]
    pub fn print_stats(&self) {
        println!("{:?}, {:?}", self.print_data, self.this_server_num);
    }

    //FIXME pass buffer-slice so we can read batches
    //FIXME we don't want the log thread free'ing, return storage on lock failure?
    //TODO replace T with U and T: ReturnAs<U> so we don't have to send as much data
    pub fn handle_op(
        &mut self,
        mut buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(RcSlice, RcSlice)>>,
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

            EntryLayout::Snapshot => {
                self.handle_snapshot(flag, buffer, storage, t)
            }

            /////////////////////////////////////////////////

            EntryLayout::Read => {
                trace!("SERVER {:?} Read", self.this_server_num);
                let OrderIndex(chain, index) = buffer.contents().locs()[0];
                //TODO validate lock
                //     this will come after per-chain locks
                match get_chain(&self.log, chain) {
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

                let (chain, min_timestamp) = {
                    let contents = buffer.contents();
                    // assert!(contents.lock_num() >= 1,  "bad time {:#?}", contents);
                    (contents.locs()[0].0, contents.lock_num())
                };
                debug_assert!(self.stores_chain(chain),
                    "tried to store {:?} at server {:?} of {:?}",
                    chain, self.this_server_num, self.total_servers);
                // assert!(self.seen_ids.insert(*buffer.contents().id()));
                let this_server_num = self.this_server_num;

                let send = {
                    let log = self.ensure_chain(chain);
                    if log.skeens.need_single_at(min_timestamp) {
                    // if log.needs_skeens_single() {//TODO we don't need skeens if min_timestamp < current_timestamp
                        let s = log.handle_skeens_single(&mut buffer, min_timestamp, t);
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
            },

            /////////////////////////////////////////////////

            EntryLayout::GC => {
                self.handle_gc(buffer, t)
            },
        }
    }

    /////////////////////////////////////////////////

    fn handle_snapshot(
        &mut self,
        kind: EntryFlag::Flag,
        buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(RcSlice, RcSlice)>>,
        t: T
    ) {
        trace!("SERVER {:?} strong snap", self.this_server_num);
        if !kind.contains(EntryFlag::TakeLock) {
            self.handle_single_server_snapshot(buffer, t)
        } else {
            assert!(kind.contains(EntryFlag::NewMultiPut));
            self.handle_skeens_snapshot(kind, buffer, storage, t)
        }
    }

    //////////////////////

    fn handle_single_server_snapshot(
        &mut self,
        mut buffer: BufferSlice,
        t: T
    ) {
        trace!("SERVER {:?} snap fastpath", self.this_server_num);
        {
            let mut contents = buffer.contents_mut();
            contents.flag_mut().insert(EntryFlag::ReadSuccess);
            let locs = contents.locs_mut();
            for &mut OrderIndex(o, ref mut i) in locs.into_iter() {
                if o == order::from(0) || !self.stores_chain(o) {
                    *i = entry::from(0);
                    continue
                }

                let horizon = get_chain(&self.log, o).map(|c| c.trie.horizon()).unwrap_or(0);
                *i = entry::from(horizon as u32);
            }
        }

        self.print_data.msgs_sent(1);
        self.to_workers.send_to_worker(Reply(buffer, t))
    }

    //////////////////////

    fn handle_skeens_snapshot(
        &mut self,
        kind: EntryFlag::Flag,
        mut buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(RcSlice, RcSlice)>>,
        t: T,
    ) {
        assert!(kind.contains(EntryFlag::TakeLock));
        trace!("SERVER {:?} new-style multisnap {:?}", self.this_server_num, kind);
        if kind.contains(EntryFlag::Unlock) {
            self.new_multiappend_round2(kind, &mut buffer);
            self.print_data.msgs_sent(1);
            self.to_workers.send_to_worker(ReturnBuffer(buffer, t))
        } else {
            let storage = storage.unwrap_left();
            self.skeens_snapshot_round1(kind, &mut buffer, &storage, t);
            self.print_data.msgs_sent(1);
            self.to_workers.send_to_worker(
                SnapshotSkeens1 { buffer: buffer, storage: storage, t: t }
            );
        }
    }

    fn skeens_snapshot_round1(
        &mut self,
        kind: EntryFlag::Flag,
        buffer: &mut BufferSlice,
        storage: &SkeensMultiStorage,
        t: T,
    ) {
        assert!(!kind.contains(EntryFlag::Unlock));
        trace!("SERVER {:?} new-style multisnap Round 1 {:?}", self.this_server_num, kind);
        let val = buffer.contents();
        let locs = val.locs();
        let timestamps = &mut unsafe { storage.get_mut().0 }[..locs.len()];
        let queue_indicies = &mut unsafe { storage.get_mut().1 }[..locs.len()];
        let id = val.id().clone();
        for i in 0..locs.len() {
            let chain = locs[i].0;
            if !self.stores_chain(chain) { continue }
            //TODO can we do without this?
            let chain = self.ensure_chain(chain);
            //FIXME handle repeat skeens-1
            let (local_timestamp, num) =
                chain.timestamp_for_snap(id, storage.clone(), t).unwrap();
            timestamps[i] = local_timestamp;
            queue_indicies[i] = num;
        }
        trace!("SERVER {:?} new-style multisnap Round 1 timestamps {:?}",
            self.this_server_num, timestamps);
    }

    /////////////////////////////////////////////////

    fn handle_multiappend(
        &mut self,
        kind: EntryFlag::Flag,

        buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(RcSlice, RcSlice)>>,
        t: T
    ) {
        if !kind.contains(EntryFlag::TakeLock) {
            self.handle_single_server_append(kind, buffer, storage, t)
        } else if kind.contains(EntryFlag::NewMultiPut) {
            self.handle_new_multiappend(kind, buffer, storage, t)
        } else {
            self.handle_old_multiappend(kind, buffer, storage, t)
        }
    }

    //////////////////////

    fn handle_single_server_append(
        &mut self,
        kind: EntryFlag::Flag,
        buffer: BufferSlice,
        storage: Troption<SkeensMultiStorage, Box<(RcSlice, RcSlice)>>,
        t: T
    ) {
        let storage = storage.unwrap_left();

        let (mut needs_lock, mut needs_skeens) = (false, false);
        {
            for &OrderIndex(c, _) in buffer.contents().locs() {
                assert!(self.stores_chain(c));
                //TODO
                // assert!(c != order::from(0), "{:?}", buffer.contents().locs());
                if c == order::from(0) {
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

    // if the multiappend only touches this server,
    // and there are not other multiappends pending
    // we can simply apply the multiappend directly
    fn single_server_single_append_fast_path(
        &mut self,
        kind: EntryFlag::Flag,
        mut buffer: BufferSlice,
        storage: SkeensMultiStorage,
        t: T
    ) {
        unsafe {
            let (_locs, _indicies, _st0, pointers) = storage.get_mut();
            let pointers = &mut **pointers.as_mut().unwrap()
                as *mut [u8]  as *mut [*mut ValEdge];
            let mut contents = buffer.contents_mut();
            // assert!(self.seen_ids.insert(*contents.as_ref().id()));
            let locs = contents.locs_mut();
            let pointers = &mut (&mut *pointers)[..locs.len()];
            let mut is_sentinel = false;
            for (j, &mut OrderIndex(ref o, ref mut i)) in locs.into_iter().enumerate() {
                if *o == order::from(0) || !self.stores_chain(*o) {
                    *i = entry::from(0);
                    pointers[j] = ptr::null_mut();
                    if *o == order::from(0) { is_sentinel = true }
                    continue
                }

                if !is_sentinel {
                    let (index, ptr) =
                        self.ensure_chain(*o).trie.prep_append(ValEdge::null());
                    *i =  entry::from(index as u32);
                    pointers[j] = ptr;
                } else {
                    let horizon = horizon_or_add_blank(&mut self.ensure_chain(*o).trie, *o);
                    *i = entry::from(horizon as u32);
                }
            }
        }

        self.print_data.msgs_sent(1);
        self.to_workers.send_to_worker(MultiFastPath(buffer, storage, t))
    }

    fn single_server_local_skeens(
        &mut self,
        mut kind: EntryFlag::Flag,
        mut buffer: BufferSlice,
        storage: SkeensMultiStorage,
        t: T
    ) {
        self.new_multiappend_round1(kind, &mut buffer, &storage, true, t);

        let max_timestamp = unsafe { storage.get().0.iter().cloned().max() };

        self.print_data.msgs_sent(1);
        self.to_workers.send_to_worker(SingleServerSkeens1(storage, t));

        {
            let mut contents = buffer.contents_mut();
            *contents.lock_mut() = max_timestamp.unwrap();
            contents.flag_mut().insert(EntryFlag::Unlock);
            kind.insert(EntryFlag::Unlock);
        };
        //FIXME this has a bug with no-remote
        //      might have a bug with replication
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
        storage: Troption<SkeensMultiStorage, Box<(RcSlice, RcSlice)>>,
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
            self.new_multiappend_round1(kind, &mut buffer, &storage, false, t);
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
        distinguish_sentinels: bool,
        t: T
    ) {
        trace!("SERVER {:?} new-style multiput Round 1 {:?}", self.this_server_num, kind);
        let val = buffer.contents();
        // assert!(self.seen_ids.insert(*val.id()));
        let locs = val.locs();
        let timestamps = &mut unsafe { storage.get_mut().0 }[..locs.len()];
        let queue_indicies = &mut unsafe { storage.get_mut().1 }[..locs.len()];
        let id = val.id().clone();
        let mut is_sentinel = false;
        for i in 0..locs.len() {
            let chain = locs[i].0;
            if chain == order::from(0) || !self.stores_chain(chain) {
                timestamps[i] = 0;
                queue_indicies[i] = 0;
                if chain == order::from(0) { is_sentinel = true }
                continue
            }

            let chain = self.ensure_chain(chain);
            //FIXME handle repeat skeens-1
            let (local_timestamp, num) =
                chain.timestamp_for_multi(
                    id,
                    storage.clone(),
                    distinguish_sentinels && is_sentinel,
                    t
                ).unwrap();
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
        assert!(kind.contains(EntryFlag::Unlock), "Bad skeens 2 {:?}", buffer.contents());
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
            let chain = get_chain_mut(&mut self.log, chain_num)
                .expect("cannot have skeens-2 as the first op on a chain");
                /*.or_insert_with(|| {
                let mut t = Trie::new();
                t.append(&EntryContents::Data(&(), &[]).clone_entry());
                Chain{ trie: t, skeens: SkeensState::new() }
            });*/
            let to_workers = &mut self.to_workers;
            let print_data = &mut self.print_data;
            chain.finish_multi(id, max_timestamp, chain_num,
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

                    FinishSkeens::Snap(index, storage, timestamp, t) => {
                        trace!("server finish sk snap");
                        print_data.msgs_sent(1);
                        to_workers.send_to_worker(
                            SnapSkeensFinished {
                                loc: OrderIndex(chain_num, (index as u32).into()),
                                storage,
                                timestamp,
                                t,
                            }
                        )
                    },

                    FinishSkeens::Single(index, trie_slot, storage, timestamp, t) => {
                        trace!("server finish sk single");
                        print_data.msgs_sent(1);
                        to_workers.send_to_worker(
                            DelayedSingle {
                                index: index,
                                trie_slot: trie_slot,
                                storage: storage,
                                timestamp,
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
        storage: Troption<SkeensMultiStorage, Box<(RcSlice, RcSlice)>>,
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
        // assert!(self.seen_ids.insert(buffer.entry().id));

        trace!("SERVER {:?} appending at {:?}",
            self.this_server_num, buffer.contents().locs());

        let mut slices = storage.unwrap_right();
        let multi_storage = {
            let mut multi_storage = &mut *slices.0;
            debug_assert_eq!(multi_storage.len(), buffer.entry_size());
            multi_storage.as_mut_ptr()
        };
        trace!("multi_storage @ {:?}", multi_storage);
        let senti_storage = {
            let mut senti_storage = &mut *slices.1;
            debug_assert_eq!(senti_storage.len(), buffer.contents().sentinel_entry_size());
            senti_storage.as_mut_ptr()
        };
        //LOGIC sentinal storage must contain at least 64b
        //      (128b with 64b entry address space)
        //      thus has at least enough storage for 1 ptr per entry
        let mut next_ptr_storage = senti_storage as *mut *mut ValEdge;
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
                            chain.prep_append(ValEdge::null()).1
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
                                chain.prep_append(ValEdge::null()).1
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
                storage: slices,
                t: t,
                num_places: num_places,
            }
        );
    }

    /////////////////////////////////////////////////

    fn stores_chain(&self, chain: order) -> bool {
        chain % self.total_servers == self.this_server_num.into()
    }

    //Safety: since this thread is the only one that mutates the map,
    //        as long as this thread holds a mutable refrence it is effectively immutable
    //FIXME: uniqueness is a lie
    fn ensure_chain(&mut self, chain: order) -> &mut Chain<T> {
        ensure_chain(&mut self.log, chain)
    }

    fn ensure_trie(&mut self, chain: order) -> &mut Trie {
        &mut self.ensure_chain(chain).trie
    }

    #[allow(dead_code)]
    fn server_num(&self) -> u32 {
        self.this_server_num
    }

    pub fn handle_replication(
        &mut self,
        to_replicate: ToReplicate,
        t: T
    ) {
        self.print_data.msgs_recvd(1);
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
                let (id, (OrderIndex(c, _), node_num), size, ts) = {
                    let e = buffer.contents();
                    (*e.id(),
                        e.locs_and_node_nums().map(|(&o, &n)| (o, n)).next().expect("must have nodes & loc_nums"),
                        e.non_replicated_len(),
                        e.lock_num())
                };
                trace!("SERVER {:?} replicate skeens single {:?} {}", self.this_server_num, c, ts);
                let storage;
                {
                    let c = self.ensure_chain(c);
                    storage = c.trie.reserve_space_at(storage_loc, size);
                    c.skeens.replicate_single_append_round1(
                        ts, node_num, id, storage, t
                    );
                }
                self.print_data.msgs_sent(1);
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
                let (id, flag) = {
                    let c = buffer.contents();
                    (*c.id(), *c.flag())
                };
                let is_single_server = !flag.contains(EntryFlag::TakeLock);
                let mut is_sentinel = false;
                'sk_rep: for (&OrderIndex(o, i), &node_num) in buffer.contents().locs_and_node_nums() {
                    if o == order::from(0) || !self.stores_chain(o) {
                        if o == order::from(0) {
                            is_sentinel = true;
                        }
                        continue 'sk_rep
                    }
                    let c = self.ensure_chain(o);
                    c.skeens.replicate_multi_append_round1(
                        u32::from(i) as u64,
                        node_num,
                        id,
                        storage.clone(),
                        is_single_server && is_sentinel,
                        t,
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

            ToReplicate::SnapshotSkeens1(buffer, storage) => {
                trace!("SERVER {:?} replicate snap skeens", self.this_server_num);
                let id = *buffer.contents().id();
                for (&OrderIndex(o, i), &node_num) in buffer.contents().locs_and_node_nums() {
                    if o == order::from(0) || !self.stores_chain(o) {
                        continue
                    }
                    let c = self.ensure_chain(o);
                    c.skeens.replicate_snapshot_round1(
                        u32::from(i) as u64,
                        node_num,
                        id,
                        storage.clone(),
                        t,
                    );
                }
                self.print_data.msgs_sent(1);
                self.to_workers.send_to_worker(
                    SnapshotSkeens1Replica {
                        buffer: buffer,
                        storage: storage,
                        t: t,
                    }
                );
            }

            ToReplicate::Skeens2(buffer) => {
                use self::ReplicatedSkeens::*;
                let id = *buffer.contents().id();
                let max_timestamp = buffer.contents().lock_num();
                trace!("SERVER {:?} replicate skeens2 max {:?}, {:?}",
                    self.this_server_num, max_timestamp, id);
                assert!(max_timestamp > 0, "SERVER {}: replicate 0 max ts {:#?}", self.this_server_num, buffer.contents());
                'sk2_rep: for &OrderIndex(o, i) in buffer.contents().locs() {
                    if o == order::from(0) || !self.stores_chain(o) { continue 'sk2_rep }
                    //let c = self.ensure_chain(chain);
                    let c = ensure_chain(&mut self.log, o);
                    let to_workers = &mut self.to_workers;
                    let print_data = &mut self.print_data;
                    let index = u32::from(i) as u64;
                    let trie = &mut c.trie;
                    c.skeens.replicate_round2(&id, max_timestamp, index, |rep| match rep {
                        Multi{index, storage, max_timestamp, t} => {
                            trace!("SERVER finish sk multi rep ({:?}, {:?}, {})", o, index, max_timestamp);
                            let slot = unsafe { trie.prep_append_at(index) };
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
                        Senti{index, storage, max_timestamp, t} => {
                            trace!("SERVER finish sk multi rep ({:?}, {:?}, {})", o, index, max_timestamp);
                            print_data.msgs_sent(1);
                            to_workers.send_to_worker(
                                Skeens2MultiReplica {
                                    loc: OrderIndex(o, (index as u32).into()),
                                    trie_slot: ptr::null_mut(),
                                    storage: storage,
                                    timestamp: max_timestamp,
                                    t: t
                                }
                            )
                        },
                        Snap{index, storage, max_timestamp, t} => {
                            trace!("SERVER finish sk multi snap ({:?}, {:?}, {})", o, index, max_timestamp);
                            print_data.msgs_sent(1);
                            to_workers.send_to_worker(
                                Skeens2SnapReplica {
                                    loc: OrderIndex(o, (index as u32).into()),
                                    storage,
                                    timestamp: max_timestamp,
                                    t,
                                }
                            )
                        },
                        Single{index, storage, max_timestamp, t} => unsafe {
                            //trace!("SERVER finish sk single rep");
                            //let size = buffer.entry_size();
                            trace!("SERVER replicating single sk2 ({:?}, {:?}, {})", o, index, max_timestamp);
                            let slot = trie.prep_append_at(index);
                            print_data.msgs_sent(1);
                            to_workers.send_to_worker(
                                Skeens2SingleReplica {
                                    index: index,
                                    trie_slot: slot,
                                    storage: storage,
                                    timestamp: max_timestamp,
                                    t: t
                                }
                            )
                        }
                    });
                }
                trace!("SRVER {:?} skeens2 over", self.this_server_num);
                self.to_workers.send_to_worker(ReturnBuffer(buffer, t))
            }

            ToReplicate::Multi(buffer, mut storage) => {
                trace!("SERVER {:?} replicate multiput", self.this_server_num);

                //debug_assert!(self._seen_ids.insert(buffer.entry().id));
                //FIXME this needs to be aware of locks...
                //      but I think only unlocks, the primary
                //      will ensure that locks happen in order...

                let mut sentinel_start_index = None;
                let mut num_places = 0;
                let is_multiserver;
                {
                    let val = buffer.contents();
                    let locs = val.locs();
                    is_multiserver = {
                        let flag = val.flag();
                        flag.contains(EntryFlag::TakeLock)
                    };
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

                //let multi_storage = storage.0.as_mut_ptr();
                let senti_storage = storage.1.as_mut_ptr();
                //LOGIC sentinal storage must contain at least 64b
                //      (128b with 64b entry address space)
                //      thus has at least enough storage for 1 ptr per entry
                let mut next_ptr_storage = senti_storage as *mut *mut ValEdge;
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
                                .prep_append_at(u32::from(index) as u64);
                            *next_ptr_storage = ptr;
                            next_ptr_storage = next_ptr_storage.offset(1);
                        };
                    }
                }
                if let (Some(ssi), true) = (sentinel_start_index, is_multiserver) {
                    trace!("SERVER {:?} sentinal locs {:?}",
                        self.this_server_num, &buffer.contents().locs()[ssi..]);
                    for &OrderIndex(chain, index) in &buffer.contents().locs()[ssi..] {
                        if self.stores_chain(chain) {
                            assert!(index != entry::from(0));
                            unsafe {
                                let ptr = self.ensure_trie(chain)
                                    .prep_append_at(u32::from(index) as u64);
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
                        storage: storage,
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

            ToReplicate::GC(buffer) => {
                self.handle_gc(buffer, t)
            },

            ToReplicate::TasRecoverer(buffer, recoverer) => {
                let (&write_id, _) = buffer.contents().write_id_and_old_recoverer();
                let index = buffer.contents().lock_num();
                let chain = recoverer.1[0].0;
                let res = self.ensure_chain(chain).skeens.replicate_recoverer(
                    write_id, recoverer, index
                );
                self.print_data.msgs_sent(1);
                self.to_workers.send_to_worker(match res {
                    Ok(_) => ToWorker::GotRecovery(buffer, t),
                    Err(id) => ToWorker::DidntGetRecovery(buffer, id.unwrap_or_else(Uuid::nil), t),
                })
            },
        }
    }

    fn handle_gc(&mut self, buffer: BufferSlice, t: T) {
        trace!("SERVER {:?} GC", self.this_server_num);
        {
            let locs = buffer.contents().locs();
            for &OrderIndex(o, i) in locs {
                let i = u32::from(i) as u64;
                get_chain_mut(&mut self.log, o).map(|c| c.trie.set_min(i));
            }
            self.log.set_meta(());
            self.log.refresh();
            for &OrderIndex(o, _) in locs {
                get_chain_mut(&mut self.log, o).map(|c| c.trie.delete_free());
            }
        }
        //TODO send down before sending to ordering thread...
        self.print_data.msgs_sent(1);
        self.to_workers.send_to_worker(Reply(buffer, t));
    }

    pub fn handle_recovery(&mut self, recovery: Recovery, t: T) {
        match recovery {
            Recovery::TasRecoverer(mut buffer, recoverer) => {
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
                    Ok(i) => {
                        *buffer.contents_mut().lock_mut() = i;
                        ToWorker::GotRecovery(buffer, t)
                    },
                    Err(id) => ToWorker::DidntGetRecovery(buffer, id.unwrap_or_else(Uuid::nil), t),
                })
            },

            Recovery::CheckSkeens1(buffer) => {
                let (id, OrderIndex(chain, time)) = {
                    let c = buffer.contents();
                    (*c.id(), c.locs()[0])
                };
                let time = u32::from(time) as u64;
                let still_there = get_chain(&self.log, chain)
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
