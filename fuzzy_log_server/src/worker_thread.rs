use super::*;

pub enum ToSend<'a> {
    Nothing,
    Contents(EntryContents<'a>),
    OldContents(EntryContents<'a>, StorageLoc),
    Slice(&'a [u8]),
    StaticSlice(&'static [u8]),
    Read(&'static [u8]),
    OldReplication(&'static [u8], StorageLoc),
}

pub fn handle_to_worker2<T: Send + Sync, U, SendFn>(
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
                        //TODO
                        min: &OrderIndex(chain, 0.into()),
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
                    // assert!(time >= 1);
                    let mut e = buffer.contents_mut();
                    *e.lock_mut() = time;
                    e.flag_mut().insert(EntryFlag::ReadSuccess);
                    e.as_ref().len()
                };
                ptr::copy_nonoverlapping(buffer[..len].as_ptr(), storage.ptr(), len);
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
            ptr::copy_nonoverlapping(buffer[..len].as_ptr(), storage.ptr(), len);
            let mut e = MutEntry::wrap_slice(slice::from_raw_parts_mut(storage.ptr(), len));
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
        DelayedSingle { index, trie_slot, storage, timestamp, t, }
        | Skeens2SingleReplica { index, trie_slot, storage, timestamp, t, } => unsafe {
            assert!(timestamp > 0, "bad single max_ts {:#?}", Entry::wrap_bytes(&mut *storage.ptr()));
            //trace!("WORKER {} finish delayed single @ {:?}", worker_num);
            let color;
            let len = {
                let mut e = MutEntry::wrap_bytes(&mut *storage.ptr()).into_contents();
                {
                    let locs = e.locs_mut();
                    color = locs[0].0;
                    locs[0].1 = entry::from(index as u32);
                }
                // assert!(timestamp >= 1);
                *e.lock_mut() = timestamp;
                debug_assert!(e.flag_mut().contains(EntryFlag::ReadSuccess));
                e.as_ref().len()
            };
            trace!("WORKER {} finish delayed single @ {:?}",
                worker_num, OrderIndex(color, entry::from(index as u32)));
            //let trie_entry: *mut AtomicPtr<u8> = trie_slot as *mut _;
            //(*trie_entry).store(storage as *mut u8, Ordering::Release);
            ValEdge::atomic_store(trie_slot, storage, Ordering::Release);
            let u = if continue_replication {
                let e = storage.as_packet().contents();
                send(ToSend::Contents(EntryContents::Skeens2ToReplica{
                    id: e.id(),
                    lock: &timestamp,
                    loc: &OrderIndex(color, entry::from(index as u32)),
                }), t)
            } else {
                let ret = slice::from_raw_parts(storage.ptr(), len);
                send(ToSend::StaticSlice(ret), t)
            };
            (None, u)
            //ServerResponse::FinishedSingletonSkeens2(ret, t)
            //(None, ret, t, 0, false)
        },

        MultiReplica {
            buffer, storage, t, num_places,
        } => unsafe {
            let storage = *storage;
            let (mut multi_storage, mut senti_storage) = storage;
            let is_multiserver;
            let (remaining_senti_places, len) = {
                let e = buffer.contents();
                is_multiserver = {
                    let flag = e.flag();
                    flag.contains(EntryFlag::TakeLock)
                    || flag.contains(EntryFlag::LockServer)
                };
                let len = e.len();
                let b = &buffer[..len];
                trace!("place multi_storage @ {:?}, len {}", multi_storage, b.len());
                ptr::copy_nonoverlapping(b.as_ptr(), multi_storage.as_mut_ptr(), b.len());
                let places: &[*mut ValEdge] = slice::from_raw_parts(
                   (&*senti_storage).as_ptr() as *const _, num_places
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
                    //let trie_entry: *mut AtomicPtr<u8> = mem::transmute(places[i]);
                    //TODO mem barrier ordering
                    //(*trie_entry).store(multi_storage, Ordering::Release);
                    let multi_edge = ValEdge::end_from_ptr(multi_storage.clone().into_ptr());
                    ValEdge::atomic_store(places[i], multi_edge, Ordering::Release);
                }
                let remaining_places =
                    if let (Some(i), true) = (sentinel_start, is_multiserver) {
                        &places[i..]
                    } else {
                        &[]
                    };
                // we finished with the first portion,
                // if there is a second, we'll need auxiliary memory
                (remaining_places.to_vec(), len)
            };
            let ret = slice::from_raw_parts(multi_storage.as_ptr(), len);
            //TODO is re right for sentinel only writes?
            if remaining_senti_places.len() == 0 {
                let u = if continue_replication {
                    send(ToSend::OldReplication(ret, ::std::u64::MAX), t)
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
                ptr::copy_nonoverlapping(b.as_ptr(), senti_storage.as_mut_ptr(), len);
                {
                    let senti_storage = slice::from_raw_parts_mut(
                        senti_storage.as_mut_ptr(), len);
                    slice_to_sentinel(&mut *senti_storage);
                }
                for place in remaining_senti_places {
                    //let trie_entry: *mut AtomicPtr<u8> = mem::transmute(place);
                    //TODO mem barrier ordering
                    //(*trie_entry).store(senti_storage, Ordering::Release);
                    let senti_edge = ValEdge::end_from_ptr(senti_storage.clone().into_ptr());
                    ValEdge::atomic_store(place, senti_edge, Ordering::Release);
                }
            }
            //TODO is re right for sentinel only writes?
            let u = if continue_replication {
                send(ToSend::OldReplication(ret, ::std::u64::MAX), t)
            } else {
                send(ToSend::StaticSlice(ret), t)
            };
            mem::drop((multi_storage, senti_storage));
            (Some(buffer), u)
            //ServerResponse::FinishOldMultiappend(buffer, t, ret)
            //(Some(buffer), ret, t, 0, false)
        },

        MultiFastPath(mut buffer, storage, t) => unsafe {
            trace!("WORKER {} finish fastpath", worker_num);
            {
                let (_ptrs, _indicies, st0, pointers) = storage.get_mut();
                let len = {
                    let mut e = buffer.contents_mut();
                    e.flag_mut().insert(EntryFlag::ReadSuccess);
                    e.as_ref().len()
                };
                st0.copy_from_slice(&buffer[..len]);
                {
                    let pointers = &mut **pointers.as_mut().unwrap()
                        as *mut [u8]  as *mut [*mut ValEdge];
                    let locs = buffer.contents().locs();
                    let pointers = &mut (&mut *pointers)[..locs.len()];
                    for (&oi, &trie_slot) in locs.iter().zip(pointers.iter()) {
                        if oi == OrderIndex(0.into(), 0.into()) {
                            break
                        }
                        if trie_slot == ptr::null_mut() { continue }

                        let trie_entry: *mut ValEdge = trie_slot;
                        // let trie_entry: *mut AtomicPtr<u8> = trie_slot as usize as *mut _;
                        // let to_store: *mut u8 = if is_sentinel {
                        //     st1.as_mut().unwrap().as_mut_ptr()
                        // } else {
                        //     st0.as_mut_ptr()
                        // };
                        // (*trie_entry).store(to_store, Ordering::Release);
                        let to_store = ValEdge::end_from_ptr(st0.clone().into_ptr());
                        ValEdge::atomic_store(trie_entry, to_store, Ordering::Release);
                    }
                }
            }
            let (_ptrs, _indicies, st0, _pointers) = storage.get();
            let u = if continue_replication {
                send(ToSend::OldReplication(&*((&**st0) as *const _), 0), t)
            } else {
                //send(ToSend::StaticSlice(st0), t)
                send(ToSend::Slice(st0), t)
            };
            (Some(buffer), u)
        },

        SingleServerSkeens1(storage, t) => unsafe {
            let (ts, indicies, st0, _pointers) = storage.get_mut();
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
                // unreachable!();
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
            if let &mut Some(ref mut st1) = st1 {
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
            if !{ let (_ts, _indicies, st0, _st1) = storage.get();
                bytes_as_entry(st0).flag().contains(EntryFlag::TakeLock) } {
                return handle_single_server_skeens_finished(
                    loc, trie_slot, storage, timestamp, t, continue_replication, send)
            }
            let chain = loc.0;
            let id;
            {
                let (_ts, _indicies, st0, st1) = storage.get_mut();
                let is_sentinel = {
                    let mut st0 = bytes_as_entry_mut(st0);
                    // assert!(timestamp >= 1);
                    *st0.lock_mut() = timestamp;
                    id = *st0.as_ref().id();
                    let st0_l = st0.locs_mut();
                    let i = st0_l.iter().position(|oi| oi.0 == chain).expect("no val");
                    //FIXME atomic?
                    st0_l[i].1 = loc.1;

                    if let &mut Some(ref mut st1) = st1 {
                        let mut st1 = bytes_as_entry_mut(st1);
                        st1.locs_mut()[i].1 = loc.1;
                        *st1.lock_mut() = timestamp;
                        let s_i = st0_l.iter()
                            .position(|oi| oi == &OrderIndex(0.into(), 0.into()));
                        i > s_i.expect("no index")
                    }
                    else {
                        false
                    }
                };
                {
                    let to_store = if is_sentinel {
                        let ptr = st1.clone().expect("no sentinel storage");
                        ValEdge::end_from_ptr(ptr.into_ptr())
                    } else {
                        ValEdge::end_from_ptr(st0.clone().into_ptr())
                    };
                    ValEdge::atomic_store(trie_slot, to_store, Ordering::Release);
                }
            }

            let u = if continue_replication {
                trace!("WORKER {} continue skeens2 replication @ {:?}", worker_num, loc);
                send(ToSend::Contents(EntryContents::Skeens2ToReplica{
                    id: &id,
                    lock: &timestamp,
                    loc: &loc,
                }), t)
            } else {
                match SkeensMultiStorage::try_unwrap(storage) {
                    Ok(storage) => {
                        trace!("WORKER {} skeens2 to client @ {:?}", worker_num, loc);
                        let &(_, _, ref st0, ref st1) = &*storage.get();
                        //ToSend::StaticSlice(if let &Some(st1) = st1 { &*st1 } else { &*st0 })
                        if st1.is_some() {
                            send(ToSend::Slice(&*st1.as_ref().unwrap()), t)
                        } else {
                            send(ToSend::Slice(&*st0), t)
                        }
                    }
                    Err(..) => {
                        trace!("WORKER {} incomplete skeens2 @ {:?}", worker_num, loc);
                        send(ToSend::Nothing, t)
                    }
                }
            };

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
            if let &mut Some(ref mut st1) = st1 {
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

        SnapshotSkeens1{mut buffer, storage, t} => unsafe {
            let (ts, indicies, st0, _) = storage.get_mut();
            trace!("WORKER {} finish snap skeens1 {:?}", worker_num, ts);
            let len = {
                let mut e = buffer.contents_mut();
                e.flag_mut().insert(EntryFlag::ReadSuccess);
                e.as_ref().len()
            };
            //let num_ts = ts.len();
            st0.copy_from_slice(&buffer[..len]);
            {
                let mut c = buffer.contents_mut();
                c.flag_mut().insert(EntryFlag::Skeens1Queued);
                let locs = c.locs_mut();
                for i in 0..locs.len() {
                    locs[i].1 = entry::from(ts[i] as u32)
                }
            }
            trace!("WORKER {} snap skeens1 @ {:?}", worker_num, ts);
            let u =  if continue_replication {
                send(ToSend::Contents(
                    buffer.contents().multi_skeens_to_replication(indicies))
                , t)
            } else {
                send(ToSend::Slice(buffer.entry_slice()), t)
            };
            (Some(buffer), u)
        },

        SnapshotSkeens1Replica{buffer, storage, t} => unsafe {
            let (mut ts, mut indicies, _, _) = storage.get_mut();
            {
                let c = buffer.contents();
                ts.iter_mut().zip(c.locs().iter()).fold((),
                    |(), (l, loc)| *l = u32::from(loc.1) as u64);
                indicies.iter_mut().zip(c.queue_nums().iter()).fold((),
                    |(), (q, num)| *q = *num);
            }
            let u = if continue_replication {
                send(ToSend::Slice(buffer.entry_slice()), t)
            } else {
                send(ToSend::Contents(buffer.contents().to_unreplica()), t)
            };
            (Some(buffer), u)
        },

        SnapSkeensFinished{loc, storage, timestamp, t, }
        | Skeens2SnapReplica{loc, storage, timestamp, t, } => unsafe {
            trace!("WORKER {} finish snap2 @ {:?}", worker_num, loc);
            let chain = loc.0;
            let id;
            {
                let (_ts, _indicies, st0, _) = storage.get_mut();
                let mut st0 = bytes_as_entry_mut(st0);
                id = *st0.as_ref().id();
                let st0_l = st0.locs_mut();
                let i = st0_l.iter().position(|oi| oi.0 == chain).expect("no val");
                st0_l[i].1 = loc.1;
            }

            let u = if continue_replication {
                trace!("WORKER {} continue snap replication: {:?} @ {:?}",
                    worker_num, timestamp, loc);
                send(ToSend::Contents(EntryContents::Skeens2ToReplica{
                    id: &id,
                    lock: &timestamp,
                    loc: &loc,
                }), t)
            } else {
                match SkeensMultiStorage::try_unwrap(storage) {
                    Ok(storage) => {
                        trace!("WORKER {} snap to client @ {:?}", worker_num, loc);
                        let &(_, _, ref st0, _) = &*storage.get();
                        send(ToSend::Slice(&*st0), t)
                    }
                    Err(..) => {
                        trace!("WORKER {} incomplete snap @ {:?}", worker_num, loc);
                        send(ToSend::Nothing, t)
                    }
                }
            };

            (None, u)
        },

        //FIXME these may not be right
        GotRecovery(mut buffer, t) => {
            {
                let mut e = buffer.contents_mut();
                e.flag_mut().insert(EntryFlag::ReadSuccess);
            }
            let u = if continue_replication {
                send(ToSend::Slice(buffer.entry_slice()), t)
            } else {
                send(ToSend::Slice(buffer.entry_slice()), t)
            };
            (Some(buffer), u)
        },

        DidntGetRecovery(mut buffer, id, t) => {
            {
                let mut e = buffer.contents_mut();
                e.flag_mut().remove(EntryFlag::ReadSuccess);
                e.recoverer_is(id);
            }
            let u = if continue_replication {
                send(ToSend::Slice(buffer.entry_slice()), t)
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
                send(ToSend::Slice(buffer.entry_slice()), t)
            } else {
                send(ToSend::Slice(buffer.entry_slice()), t)
            };
            (Some(buffer), u)
        },

        ToWorker::EndRecovery(buffer, t) => {
            let u = if continue_replication {
                send(ToSend::Slice(buffer.entry_slice()), t)
            } else {
                send(ToSend::Slice(buffer.entry_slice()), t)
            };
            (Some(buffer), u)
        },
    }
}

unsafe fn handle_single_server_skeens_finished<T, U, SendFn>(
    loc: OrderIndex,
    slot: *mut ValEdge,
    storage: SkeensMultiStorage,
    timestamp: u64,
    t: T,
    continue_replication: bool,
    send: SendFn,
) -> (Option<Buffer>, U)
where SendFn: for<'a> FnOnce(ToSend<'a>, T) -> U {
    let (id, num_locs);
    {
        let (_ts, _indicies, st0, pointers) = storage.get_mut();
        let index = {
            let chain = loc.0;
            let mut st0 = bytes_as_entry_mut(st0);
            id = *st0.as_ref().id();
            let st0_l = st0.locs_mut();
            num_locs = st0_l.len();
            let i = st0_l.iter().position(|oi| oi.0 == chain).expect("must have chain");
            st0_l[i].1 = loc.1;
            i
        };

        if slot != ptr::null_mut() {
            // Non-Sentinel
            let pointers = &mut **pointers.as_mut().expect("must have pointers")
                as *mut [u8]  as *mut [*mut ValEdge];
            let pointers = &mut (&mut *pointers)[..num_locs];
            pointers[index] = slot;
        }
    }

    let arc;
    let to_send = match SkeensMultiStorage::try_unwrap(storage) {
        Ok(storage) => {
            let &mut (_, _, ref st0, ref mut pointers) = &mut *storage.get();
            let pointers = &mut **pointers.as_mut().expect("must have pointers 2")
                as *mut [u8]  as *mut [*mut ValEdge];
            let pointers = &mut (&mut *pointers)[..num_locs];
            for &mut trie_slot in pointers {
                let to_store = ValEdge::end_from_ptr(st0.clone().into_ptr());
                ValEdge::atomic_store(trie_slot, to_store, Ordering::Release);
            }
            st0
        },
        Err(r) => {
            arc = r;
            let (_, _, st0, _) = arc.get();
            st0
        }
    };
    let u = if continue_replication {
        trace!("WORKER continue fast skeens2 replication @ {:?}", loc);
        send(ToSend::Contents(EntryContents::Skeens2ToReplica{
            id: &id,
            lock: &timestamp,
            loc: &loc,
        }), t)
    } else {
        send(ToSend::Slice(&*to_send), t)
    };
    (None, u)
}

pub fn handle_read<U, V: Send + Sync + Copy, SendFn>(
    chains: &ChainReader<V>, buffer: &BufferSlice, worker_num: usize, mut send: SendFn
) -> U
where SendFn: for<'a> FnMut(Result<&'a [u8], EntryContents<'a>>) -> U {
    let OrderIndex(chain, index) = buffer.contents().locs()[0];
    debug_assert!(index > entry::from(0)); //TODO return error on index < GC
    let res = chains.get_and(&chain, |logs| {
        let log = unsafe {&*UnsafeCell::get(&logs[0])};
        match log.trie.atomic_get(u32::from(index) as u64) {
            Some(packet) => {
                trace!("WORKER {:?} read occupied entry {:?} {:?}",
                    worker_num, (chain, index), packet.contents().id());
                Ok(send(Ok(packet.bytes())))
            }

            None => Err(log.trie.bounds()),
        }
    })
    .unwrap_or_else(|| Err(0..0));
    match res {
        Ok(u) => u,
        Err(valid_locs) => {
            // trace!("WORKER {} overread {:?}: {:?} > {:?}",
            //     worker_num, chain, index, valid_locs);
            let (old_id, old_loc) = {
                let e = buffer.contents();
                (e.id().clone(), e.locs()[0])
            };
            let chain: order = old_loc.0;
            let max = entry::from(valid_locs.end.saturating_sub(1) as u32);
            let min = entry::from(valid_locs.start as u32);
            send(Err(EntryContents::Read{
                id: &old_id,
                flags: &EntryFlag::Nothing,
                loc: &old_loc,
                data_bytes: &0,
                dependency_bytes: &0,
                horizon: &OrderIndex(chain, max),
                min: &OrderIndex(chain, min),
            }))
        }
    }
}
