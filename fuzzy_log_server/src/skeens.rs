
use std::collections::BinaryHeap;
use std::collections::hash_map::Entry::*;

use std::cmp::{Eq, Ord, Ordering, PartialOrd};
use std::cmp::Ordering::*;

use uuid::Uuid;

use hash::UuidHashMap;

use vec_deque_map::VecDequeMap;

use super::SkeensMultiStorage;

use super::trie::ValEdge;
use packets::OrderIndex;

pub struct SkeensState<T: Copy> {
    next_timestamp: u64,
    last_flush: u64,
    phase1_queue: VecDequeMap<WaitingForMax<T>>,
    got_max_timestamp: BinaryHeap<GotMax<T>>,
    append_status: UuidHashMap<AppendStatus>,
    recovering: UuidHashMap<(u64, Box<(Uuid, Box<[OrderIndex]>)>)>,
    // early_sk2: UuidHashMap<u64>,
}

impl<T: Copy> ::std::fmt::Debug for SkeensState<T> {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error> {
        fmt.debug_struct("SkeensState")
            .field("next_timestamp", &self.next_timestamp)
            .field("last_flush", &self.last_flush)
            .field("phase1_queue", &self.phase1_queue)
            .field("got_max_timestamp", &self.got_max_timestamp)
            .field("append_status", &self.append_status)
            .finish()
    }
}

pub type QueueIndex = u64;
pub type Time = u64;
pub type TrieIndex = u64;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum AppendStatus {
    Phase1(QueueIndex),
    Phase2(Time),
    Singleton(QueueIndex),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[must_use]
pub enum SkeensAppendRes {
    OldPhase1(Time),
    Phase2(Time),
    NewAppend(Time, QueueIndex),
}

impl SkeensAppendRes {
    fn assert_new(self) {
        match self {
            SkeensAppendRes::NewAppend(..) => (),
            _ => panic!("not new"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[must_use]
pub enum SkeensSetMaxRes {
    NeedsFlush,
    Ok,
    Duplicate(Time),
    NotWaiting,
}

impl<T: Copy> SkeensState<T> {

    pub fn new() -> Self {
        SkeensState {
            append_status: Default::default(),
            phase1_queue: Default::default(),
            got_max_timestamp: Default::default(),
            next_timestamp: 1,
            last_flush: 0,
            recovering: Default::default(),
            // early_sk2: Default::default(),
        }
    }

    pub fn add_single_append(
        &mut self, id: Uuid, timestamp: Time, storage: ValEdge, t: T
    ) -> SkeensAppendRes {
        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len());
            if self.phase1_queue.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        assert!(!self.phase1_queue.is_empty());
        if self.next_timestamp <= timestamp {
            self.next_timestamp = timestamp + 1;
        }

        match self.append_status.entry(id) {
            Occupied(o) => {
                match *o.into_mut() {
                    AppendStatus::Phase2(t) => SkeensAppendRes::Phase2(t),
                    AppendStatus::Phase1(i) | AppendStatus::Singleton(i) =>
                        match self.phase1_queue[i].multi_timestamp() {
                            Timestamp::Phase1(t) => SkeensAppendRes::OldPhase1(t),
                            Timestamp::Phase2(t) => SkeensAppendRes::Phase2(t),
                        },
                }
            },
            Vacant(v) => {
                let timestamp = self.next_timestamp;
                self.next_timestamp += 1;
                let node_num = self.phase1_queue.push_index();
                let n = self.phase1_queue.push_back(WaitingForMax::SimpleSingle{
                    timestamp: timestamp,
                    storage: storage,
                    node_num: node_num,
                    t: t,
                    id: id,
                });
                assert_eq!(node_num, n);
                v.insert(AppendStatus::Singleton(node_num));
                SkeensAppendRes::NewAppend(timestamp, node_num)
            },
        }
    }

    pub fn need_single_at(&mut self, timestamp: Time) -> bool {
        let is_empty = self.phase1_queue.is_empty() && self.got_max_timestamp.is_empty();
        assert!(self.last_flush < self.next_timestamp);
        if is_empty || self.last_flush >= timestamp {
            if self.next_timestamp <= timestamp {
                self.next_timestamp = timestamp + 1;
            }
            false
        } else {
            true
        }
        // if !is_empty && (self.last_flush < timestamp || self.next_timestamp <= timestamp) {
        //     return true
        // }

        // self.next_timestamp = timestamp + 1;
        // false
    }

    //AKA skeens1
    pub fn add_multi_append(
        &mut self, id: Uuid, storage: SkeensMultiStorage, sentinel: bool, t: T,
    ) -> SkeensAppendRes {
        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //     "{:?} != {:?}", self.phase2_ids, self.got_max_timestamp);
            if self.phase1_queue.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        let ret = match self.append_status.entry(id) {
            Occupied(o) => {
                match *o.into_mut() {
                    AppendStatus::Phase2(t) => SkeensAppendRes::Phase2(t),
                    AppendStatus::Phase1(i) | AppendStatus::Singleton(i) =>
                        match self.phase1_queue[i].multi_timestamp() {
                            Timestamp::Phase1(t) => SkeensAppendRes::OldPhase1(t),
                            Timestamp::Phase2(t) => SkeensAppendRes::Phase2(t),
                        },
                }
            },
            Vacant(v) => {
                let timestamp = self.next_timestamp;
                let node_num = self.phase1_queue.push_index();
                self.next_timestamp += 1;
                let n = self.phase1_queue.push_back(if sentinel {
                    WaitingForMax::Senti{
                    timestamp: timestamp,
                    node_num: node_num,
                    storage: storage,
                    t: t,
                    id: id,
                }} else {
                    WaitingForMax::Multi{
                    timestamp: timestamp,
                    node_num: node_num,
                    storage: storage,
                    t: t,
                    id: id,
                }});
                assert_eq!(node_num, n);
                v.insert(AppendStatus::Phase1(node_num));
                SkeensAppendRes::NewAppend(timestamp, node_num)
            },
        };
        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //     "{:?} != {:?}", self.phase2_ids, self.got_max_timestamp);
            if self.phase1_queue.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        ret
    }

    pub fn add_snapshot(
        &mut self, id: Uuid, storage: SkeensMultiStorage, t: T,
    ) -> SkeensAppendRes {
        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //     "{:?} != {:?}", self.phase2_ids, self.got_max_timestamp);
            if self.phase1_queue.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        let ret = match self.append_status.entry(id) {
            Occupied(o) => {
                match *o.into_mut() {
                    AppendStatus::Phase2(t) => SkeensAppendRes::Phase2(t),
                    AppendStatus::Phase1(i) | AppendStatus::Singleton(i) =>
                        match self.phase1_queue[i].multi_timestamp() {
                            Timestamp::Phase1(t) => SkeensAppendRes::OldPhase1(t),
                            Timestamp::Phase2(t) => SkeensAppendRes::Phase2(t),
                        },
                }
            },
            Vacant(v) => {
                let timestamp = self.next_timestamp;
                let node_num = self.phase1_queue.push_index();
                self.next_timestamp += 1;
                let n = self.phase1_queue.push_back(
                    WaitingForMax::Snap {
                        timestamp: timestamp,
                        storage: storage,
                        node_num,
                        t: t,
                        id: id,
                    }
                );
                assert_eq!(node_num, n);
                v.insert(AppendStatus::Phase1(node_num));
                SkeensAppendRes::NewAppend(timestamp, node_num)
            },
        };
        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //     "{:?} != {:?}", self.phase2_ids, self.got_max_timestamp);
            if self.phase1_queue.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        ret
    }

    //AKA skeens2
    pub fn set_max_timestamp(&mut self, sk2_id: Uuid, max_timestamp: u64)
    -> SkeensSetMaxRes {
        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len());
            if self.phase1_queue.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        if self.next_timestamp <= max_timestamp {
            self.next_timestamp = max_timestamp + 1
        }

        let ret = match self.append_status.entry(sk2_id) {
            //TODO we could iterate over the queue instead of keeping a set of ids
            Vacant(..) => SkeensSetMaxRes::NotWaiting,
            Occupied(o) => {
                let state = o.into_mut();
                match *state {
                    AppendStatus::Phase2(timestamp) =>
                        SkeensSetMaxRes::Duplicate(timestamp),
                    AppendStatus::Singleton(i) => {
                        let timestamp = self.phase1_queue[i].multi_timestamp();
                        match timestamp {
                            Timestamp::Phase1(t) | Timestamp::Phase2(t) =>
                                SkeensSetMaxRes::Duplicate(t),
                        }
                    },
                    AppendStatus::Phase1(i) => {
                        let set_max = self.phase1_queue[i].set_max_timestamp(max_timestamp);
                        match set_max {
                            Err(ts) => SkeensSetMaxRes::Duplicate(ts),
                            Ok(..) => {
                                *state = AppendStatus::Phase2(max_timestamp);
                                SkeensSetMaxRes::Ok
                            },
                        }
                    },
                }
            },
        };

        if let SkeensSetMaxRes::Ok = ret {
            while self.phase1_queue.front().map(|v| v.has_max()).unwrap_or(false) {
                let s = self.phase1_queue.pop_front().expect("must have front of queue");
                let s = s.into_got_max();
                self.got_max_timestamp.push(s);
            }
            //TODO flush queue?
            return if self.can_flush() {
                SkeensSetMaxRes::NeedsFlush
            } else {
                SkeensSetMaxRes::Ok
            }
        }

        debug_assert!({
            //debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //    "{:?} != {:?}", self.phase2_ids, self.got_max_timestamp);
            if self.phase1_queue.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        ret
    }

    pub fn flush_got_max_timestamp<F>(&mut self, mut f: F)
    where F: FnMut(GotMax<T>) {
        while self.can_flush() {
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //     "{:?} != {:?}",
            //     self.phase2_ids, self.got_max_timestamp);
            let g = self.got_max_timestamp.pop().expect("if can flush, must have to flush");
            let id = g.get_id();
            //FIXME this should get delayed until the multi is fully complete
            //      due to the failing path,
            //      instead we should just set state to complete on multi-server appends
            let _old = self.append_status.remove(&id);
            self.last_flush = ::std::cmp::max(g.timestamp(), self.last_flush);
            debug_assert!(_old.is_some(), "no skeen for {:?}", id);
            f(g)
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //     "{:?} != {:?} @ {:?}",
            //     self.phase2_ids, self.got_max_timestamp, id);
        }

    }


    // TODO
    // pub fn set_snapshot_max_timestamp(&mut self, sk2_id: Uuid, max_timestamp: u64)
    // -> SkeensSetMaxRes {
    //     unimplemented!()
    // }

    ////////////////////////////////////////

    /*pub fn replicate_single_append(
        &mut self, timestamp: Time, node_num: QueueIndex, id: Uuid, storage: ValEdge, t: T
    ) -> SkeensReplicaionRes {
        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len());
            if self.phase1_queue.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });
        //FIXME check both maps

        let start_index = self.phase1_queue.start_index();
        if start_index > node_num { return SkeensReplicaionRes::Old }
        if start_index == node_num {
            self.phase1_queue.increment_start();
            return SkeensReplicaionRes::NoNeedToWait
        }

        let s = WaitingForMax::Single{
            timestamp: timestamp,
            storage: storage,
            node_num: node_num,
            t: t
        };
        let old = self.phase1_queue.insert(node_num, s);
        assert!(old.is_none());
        SkeensReplicaionRes::NewAppend
    }*/

    pub fn replicate_single_append_round1(
        &mut self, timestamp: Time, node_num: QueueIndex, id: Uuid, storage: ValEdge, t: T
    ) -> bool {
        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len());
            if self.phase1_queue.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });
        //FIXME check both maps

        let start_index = self.phase1_queue.start_index();
        assert!(start_index <= node_num, "SKEENS single re-append {} <= {}", start_index, node_num);
        // if start_index > node_num { return false }

        // if let Some(early) = self.early_sk2.get(&id) {
        //             println!("SKEENS got s round 1 {} @ {}, {:#?}, {:#?}, {}, {}, {:?}",
        //                 early, self.next_timestamp, self.phase1_queue, self.got_max_timestamp, id, timestamp, node_num
        //             );
        //         }
        // if !self.early_sk2.is_empty() {
        //     println!("Waiting on early sk2, ts {} node_num {}, {:#?}", timestamp, node_num, self);
        // }

        if self.next_timestamp <= timestamp {
            self.next_timestamp = timestamp + 1
        }

        match self.append_status.entry(id) {
            Occupied(..) => false,
            Vacant(v) => {
                v.insert(AppendStatus::Singleton(node_num));
                let s = WaitingForMax::SimpleSingle{
                    timestamp: timestamp,
                    storage: storage,
                    node_num: node_num,
                    t: t,
                    id: id,
                };
                let old = self.phase1_queue.insert(node_num, s);
                assert!(old.is_none());
                true
            },
        }
    }

    pub fn replicate_multi_append_round1(
        &mut self,
        timestamp: Time,
        node_num: QueueIndex,
        id: Uuid,
        storage: SkeensMultiStorage,
        sentinel: bool,
        t: T,
    ) -> bool {
        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len());
            if self.phase1_queue.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });
        //FIXME check both maps

        let start_index = self.phase1_queue.start_index();
        assert!(start_index <= node_num, "SKEENS multi re-append {} <= {}", start_index, node_num);
        // if start_index > node_num { return false }

        // if let Some(early) = self.early_sk2.get(&id) {
        //             println!("SKEENS got m round 1 {} @ self.nts {}, {:#?}, {:#?}, {}, ts {}, node_num {:?}",
        //                 early,
        //                 self.next_timestamp,
        //                 self.phase1_queue,
        //                 self.got_max_timestamp,
        //                 id, timestamp, node_num
        //             );
        //         }
        // if !self.early_sk2.is_empty() {
        //     println!("Waiting on early sk2, ts {} node_num {}, {:#?}", timestamp, node_num, self);
        // }

        if self.next_timestamp <= timestamp {
            self.next_timestamp = timestamp + 1
        }

        match self.append_status.entry(id) {
            Occupied(..) => false,
            Vacant(v) => {
                v.insert(AppendStatus::Phase1(node_num));
                let m = if sentinel {
                    WaitingForMax::Senti{
                        timestamp: timestamp,
                        storage: storage,
                        node_num: node_num,
                        t: t,
                        id: id,
                    }
                } else {
                    WaitingForMax::Multi{
                        timestamp: timestamp,
                        storage: storage,
                        node_num: node_num,
                        t: t,
                        id: id,
                    }
                };
                let old = self.phase1_queue.insert(node_num, m);
                assert!(old.is_none(),
                    "replace skeens? {:?} {:?} @ {:?}, {}",
                    self.phase1_queue.get(node_num), old, node_num, self.phase1_queue.start_index(),
                    //unsafe {Entry::wrap_bytes(old.as_ref().unwrap().bytes())},
                );
                true
            },
        }
    }

    pub fn replicate_snapshot_round1(
        &mut self,
        timestamp: Time,
        node_num: QueueIndex,
        id: Uuid,
        storage: SkeensMultiStorage,
        t: T,
    ) -> bool {
        let start_index = self.phase1_queue.start_index();
        assert!(start_index <= node_num, "SKEENS snap re-append {} <= {}", start_index, node_num);
        // if start_index > node_num { return false }

        if self.next_timestamp <= timestamp {
            self.next_timestamp = timestamp + 1
        }

        match self.append_status.entry(id) {
            Occupied(..) => false,
            Vacant(v) => {
                v.insert(AppendStatus::Phase1(node_num));
                let m = WaitingForMax::Snap {
                    timestamp: timestamp,
                    storage: storage,
                    node_num: node_num,
                    t: t,
                    id: id,
                };
                let old = self.phase1_queue.insert(node_num, m);
                assert!(old.is_none(),
                    "replace skeens snap? {:?} {:?}",
                    self.phase1_queue.get(node_num), old
                );
                true
            },
        }
    }

    pub fn replicate_round2<F>(&mut self, id: &Uuid, max_timestamp: u64, index: TrieIndex, mut f: F)
    where F: FnMut(ReplicatedSkeens<T>) {
        let offset = match self.append_status.get(&id) {
            Some(&AppendStatus::Phase1(offset)) | Some(&AppendStatus::Singleton(offset)) => {
                // if let Some(early) = self.early_sk2.remove(id) {
                //     println!("SKEENS got round 1 {} @ {}, {:#?}, {:#?}, {}, {}, {:?}",
                //         early, self.next_timestamp, self.phase1_queue, self.got_max_timestamp, id, max_timestamp, index
                //     );
                // }
                offset
            },
            _ => {
                // TODO panic!("SKEENS early round 2 @ {:?}, {}, {}, {:?}", self, id, max_timestamp, index);

                // let early = self.early_sk2.entry(*id).or_insert(0);
                // *early += 1;
                // println!("SKEENS early round 2 {} @ self.nts {}, {:#?}, {:#?}, {}, mts {}, idx {:?}",
                //     *early, self.next_timestamp, self.phase1_queue, self.got_max_timestamp, id, max_timestamp, index
                // );
                return
            },
        };
        if self.next_timestamp <= max_timestamp {
            self.next_timestamp = max_timestamp + 1
        }
        self.phase1_queue.get_mut(offset).map(|w|
            w.replicate_max_timetamp(max_timestamp, index, id));
        if offset != self.phase1_queue.start_index() {
            // trace!("SKEENS not ready to flush replica {:?}", self);
            return
        }
        // trace!("flush {:#?}", self);
        while self.phase1_queue.front().map(|w| w.has_replication()).unwrap_or(false) {
            let replica = self.phase1_queue.pop_front().expect("flushing nothing");
            //let old_start = self.phase1_queue.start_index() - 1;
            //assert!(replica.contains_node_num(old_start));
            //TODO self.last_flush = ::std::cmp::max(replica.timestamp(), self.last_flush);
            let (id, replica) = replica.to_replica();
            //FIXME for non-single-server appends
            //      this should get delayed until the multi is fully complete
            //      due to the failing path,
            //      here we should just update status to complete
            let _old = self.append_status.remove(&id);
            debug_assert!(_old.is_some(), "no skeen for {:?}", id);
            f(replica);
        }
    }

    fn can_flush(&self) -> bool {
        self.got_max_timestamp.peek().map(|g|
            self.phase1_queue.front().map(|v|
                g.happens_before(v)
            ).unwrap_or(true) // if there are no phase1 we can definitely flush phase2
        ).unwrap_or(false)
    }

    pub fn is_empty(&self) -> bool {
        self.phase1_queue.is_empty()
        && self.got_max_timestamp.is_empty()
    }

    pub fn tas_recoverer(
        &mut self,
        write_id: Uuid,
        recoverer: Box<(Uuid, Box<[OrderIndex]>)>,
        old_recoverer: Option<Uuid>,
    ) -> Result<u64, Option<Uuid>> {
        use std::collections::hash_map;

        match (self.recovering.entry(write_id), old_recoverer) {
            (hash_map::Entry::Vacant(..), Some(..)) => Err(None),

            (hash_map::Entry::Vacant(v), None) => {
                v.insert((1, recoverer));
                Ok(1)
            },

            (hash_map::Entry::Occupied(ref mut o), Some(ref mut old)) if (o.get().1).0 == *old
            => {
                let old = o.get().0;
                o.insert((old+1, recoverer));
                Ok(old+1)
            },

            (hash_map::Entry::Occupied(o), _) => Err(Some((o.get().1).0)),
        }
    }

    pub fn replicate_recoverer(
        &mut self,
        write_id: Uuid,
        recoverer: Box<(Uuid, Box<[OrderIndex]>)>,
        recoverer_index: u64,
    ) -> Result<(), Option<Uuid>> {
        use std::collections::hash_map;

        match self.recovering.entry(write_id) {
            hash_map::Entry::Vacant(v) => {
                v.insert((recoverer_index, recoverer));
                Ok(())
            },

            hash_map::Entry::Occupied(ref mut o) if o.get().0 < recoverer_index => {
                o.insert((recoverer_index, recoverer));
                Ok(())
            },

            hash_map::Entry::Occupied(o) => Err(Some((o.get().1).0)),
        }
    }

    pub fn check_skeens1(&self, write_id: Uuid, timestamp: Time) -> bool {
        let status = self.append_status.get(&write_id);
        if let Some(status) = status {
            if let &AppendStatus::Phase1(i) = status {
                if let Timestamp::Phase1(t) = self.phase1_queue[i].multi_timestamp() {
                    return t == timestamp
                }
            }
        }
        false
    }
}

pub enum ReplicatedSkeens<T> {
    Multi{index: TrieIndex, storage: SkeensMultiStorage, max_timestamp: Time, t: T},
    Senti{index: TrieIndex, storage: SkeensMultiStorage, max_timestamp: Time, t: T},
    Snap{index: TrieIndex, storage: SkeensMultiStorage, max_timestamp: Time, t: T},
    Single{index: TrieIndex, storage: ValEdge, max_timestamp: Time, t: T},
}

#[allow(dead_code)]
enum WaitingForMax<T> {
    GotMaxMulti{timestamp: u64, node_num: u64, storage: SkeensMultiStorage, t: T, id: Uuid},
    GotMaxSenti{timestamp: u64, node_num: u64, storage: SkeensMultiStorage, t: T, id: Uuid},
    GotMaxSnap{timestamp: u64, node_num: u64, storage: SkeensMultiStorage, t: T, id: Uuid},

    SimpleSingle{timestamp: u64, node_num: u64, storage: ValEdge, t: T, id: Uuid},
    Single{timestamp: u64, node_num: u64, storage: ValEdge, t: T, id: Uuid},
    Multi{timestamp: u64, node_num: u64, storage: SkeensMultiStorage, t: T, id: Uuid},
    Senti{timestamp: u64, node_num: u64, storage: SkeensMultiStorage, t: T, id: Uuid},
    Snap{timestamp: u64, node_num: u64, storage: SkeensMultiStorage, t: T, id: Uuid},

    ReplicatedMulti{max_timestamp: u64, index: TrieIndex, storage: SkeensMultiStorage, t: T, id: Uuid},
    ReplicatedSenti{max_timestamp: u64, index: TrieIndex, storage: SkeensMultiStorage, t: T, id: Uuid},
    ReplicatedSnap{max_timestamp: u64, index: TrieIndex, storage: SkeensMultiStorage, t: T, id: Uuid},

    ReplicatedSingle{max_timestamp: u64, index: TrieIndex, storage: ValEdge, t: T, id: Uuid},
}

enum Timestamp {
    Phase1(u64),
    Phase2(u64),
}

impl<T> WaitingForMax<T> {
    #[allow(dead_code)]
    fn contains_node_num(&self, node_num: u64) -> bool {
        use self::WaitingForMax::*;
        match self {
            &Multi{ref storage, ..}
            | &Senti{ref storage, ..}
            | &Snap{ref storage, ..}
            | &GotMaxMulti{ref storage, ..}
            | &GotMaxSenti{ref storage, ..}
            | &GotMaxSnap{ref storage, ..}
            | &ReplicatedMulti{ref storage, ..}
            | &ReplicatedSenti{ref storage, ..}
            | &ReplicatedSnap{ref storage, ..} => unsafe {
                let node_nums = storage.get().1;
                assert!(
                    node_nums.contains(&node_num),
                    "missing node num {} @ {:?}",
                    node_num, node_nums,
                );
                node_nums.contains(&node_num)
            },

            &SimpleSingle{..} | &Single{..} | &ReplicatedSingle{..} => true,
        }
    }

    fn bytes(&self) -> *const u8 {
        use self::WaitingForMax::*;
        match self {
            &Multi{ref storage, ..}
            | &Senti{ref storage, ..}
            | &Snap{ref storage, ..}
            | &GotMaxMulti{ref storage, ..}
            | &GotMaxSenti{ref storage, ..}
            | &GotMaxSnap{ref storage, ..}
            | &ReplicatedMulti{ref storage, ..}
            | &ReplicatedSenti{ref storage, ..}
            | &ReplicatedSnap{ref storage, ..} => unsafe {&**storage.get().2}.as_ptr(),

            &SimpleSingle{ref storage,..} | &Single{ref storage,..} | &ReplicatedSingle{ref storage,..} => storage.ptr(),
        }
    }

    fn set_max_timestamp(&mut self, max_timestamp: u64) -> Result<(), u64>
    where T: Copy {
        use self::WaitingForMax::*;
        *self = match self {
            &mut GotMaxMulti{timestamp, ..}
            | &mut GotMaxSenti{timestamp, ..}
            | &mut GotMaxSnap{timestamp, ..}
            | &mut SimpleSingle{timestamp, ..} =>
                return Err(timestamp),

            &mut ReplicatedMulti{max_timestamp, ..}
            | &mut ReplicatedSenti{max_timestamp, ..}
            | &mut ReplicatedSnap{max_timestamp, ..}
            | &mut ReplicatedSingle{max_timestamp, ..} =>
                return Err(max_timestamp),

            &mut Single{timestamp, ..} => return Err(timestamp),

            &mut Multi{timestamp, node_num, ref storage, t, id, ..} => {
                debug_assert!(max_timestamp >= timestamp,
                    "max_timestamp >= timestamp {:?} >= {:?},",// @ {:?}, {:#?}",
                    max_timestamp, timestamp,
                );
                GotMaxMulti {
                    timestamp: max_timestamp,
                    node_num: node_num,
                    storage: storage.clone(),
                    t: t,
                    id: id,
                }
            },

            &mut Senti{timestamp, node_num, ref storage, t, id, ..} => {
                debug_assert!(max_timestamp >= timestamp,
                    "max_timestamp >= timestamp {:?} >= {:?},",
                    max_timestamp, timestamp,
                );
                GotMaxSenti {
                    timestamp: max_timestamp,
                    node_num: node_num,
                    storage: storage.clone(),
                    t: t,
                    id: id,
                }
            },

            &mut Snap{timestamp, node_num, ref storage, t, id, ..} => {
                debug_assert!(max_timestamp >= timestamp,
                    "max_timestamp >= timestamp {:?} >= {:?},",
                    max_timestamp, timestamp,
                );
                GotMaxSnap {
                    timestamp: max_timestamp,
                    node_num: node_num,
                    storage: storage.clone(),
                    t: t,
                    id: id,
                }
            },
        };
        Ok(())
    }

    fn into_got_max(self) -> GotMax<T> {
        use self::WaitingForMax::*;
        match self {
            SimpleSingle{timestamp, storage, t, node_num: _, id} =>
                GotMax::SimpleSingle{timestamp: timestamp, storage: storage, t: t, id: id},

            Single{timestamp, node_num, storage, t, id} =>
                GotMax::Single{
                    timestamp: timestamp,
                    node_num: node_num,
                    storage: storage,
                    t: t,
                    id: id,
                },

            GotMaxMulti{timestamp, storage, t, id, node_num: _} => {
                GotMax::Multi{timestamp: timestamp, storage: storage, t: t, id: id}
            },

            GotMaxSenti{timestamp, storage, t, id, node_num: _} => {
                GotMax::Senti{timestamp: timestamp, storage: storage, t: t, id: id}
            },

            GotMaxSnap{timestamp, storage, t, id, node_num: _} => {
                GotMax::Snap{timestamp: timestamp, storage: storage, t: t, id: id}
            },

            _ => unreachable!(),
        }
    }

    fn has_max(&self) -> bool {
        use self::WaitingForMax::*;
        match self {
            &GotMaxMulti{..}
            | &GotMaxSenti{..}
            | &GotMaxSnap{..}
            | &SimpleSingle{..}
            | &Single{..}
            | &ReplicatedMulti{..}
            | &ReplicatedSenti{..}
            | &ReplicatedSnap{..}
            | &ReplicatedSingle{..} => true,

            &Multi{..} | &Senti{..} | &Snap{..} => false,
        }
    }

    fn replicate_max_timetamp(&mut self, max_timestamp: Time, index: TrieIndex, rid: &Uuid)
    where T: Copy {
        use self::WaitingForMax::*;
        *self = match self {
            &mut GotMaxMulti{..}
            | &mut GotMaxSenti{..}
            | &mut GotMaxSnap{..}
            | &mut ReplicatedMulti{..}
            | &mut ReplicatedSenti{..}
            | &mut ReplicatedSnap{..}
            | &mut ReplicatedSingle{..} =>
                unreachable!(),

            &mut Single{timestamp, storage, t, id, ..}
            | &mut SimpleSingle{timestamp, storage, t, id, ..} => {
                debug_assert_eq!(&id, rid);
                assert_eq!(max_timestamp, timestamp);
                ReplicatedSingle{
                    max_timestamp: timestamp,
                    index: index,
                    storage: storage,
                    t: t,
                    id: id,
                }
            }

            &mut Multi{timestamp, ref storage, t, id, ..} => {
                assert!(max_timestamp >= timestamp,
                    "max_timestamp >= timestamp {:?} >= {:?},",// @ {:?}, {:#?}",
                    max_timestamp, timestamp, /*unsafe {&(*storage.get()).0},
                    unsafe {
                        let e = Entry::<()>::wrap_bytes((*storage.get()).1);
                        (e.id, e.locs())
                    }*/
                );
                debug_assert_eq!(&id, rid);
                ReplicatedMulti {
                    max_timestamp: max_timestamp,
                    index: index,
                    storage: storage.clone(),
                    t: t,
                    id: id,
                }
            },

            &mut Senti{timestamp, ref storage, t, id, ..} => {
                assert!(max_timestamp >= timestamp,
                    "max_timestamp >= timestamp {:?} >= {:?},",// @ {:?}, {:#?}",
                    max_timestamp, timestamp, /*unsafe {&(*storage.get()).0},
                    unsafe {
                        let e = Entry::<()>::wrap_bytes((*storage.get()).1);
                        (e.id, e.locs())
                    }*/
                );
                debug_assert_eq!(&id, rid);
                ReplicatedSenti {
                    max_timestamp: max_timestamp,
                    index: index,
                    storage: storage.clone(),
                    t: t,
                    id: id,
                }
            },

            &mut Snap{timestamp, ref storage, t, id, ..} => {
                assert!(max_timestamp >= timestamp,
                    "max_timestamp >= timestamp {:?} >= {:?},",
                    max_timestamp, timestamp,
                );
                debug_assert_eq!(&id, rid);
                ReplicatedSnap {
                    max_timestamp,
                    index,
                    storage: storage.clone(),
                    t,
                    id,
                }
            },
        };
    }

    fn has_replication(&self) -> bool {
        use self::WaitingForMax::*;
        match self {
            &ReplicatedMulti{..} | &ReplicatedSenti{..} | &ReplicatedSingle{..}
            | &ReplicatedSnap{..} => true,
            _ => false,
        }
    }

    fn to_replica(self) -> (Uuid, ReplicatedSkeens<T>) {
        use self::WaitingForMax::*;
        match self {
            ReplicatedSingle{index, storage, t, id, max_timestamp, ..} =>
                (id, ReplicatedSkeens::Single{index: index, storage: storage, max_timestamp, t: t}),

            ReplicatedMulti{index, storage, t, id, max_timestamp, ..} => {
                (id, ReplicatedSkeens::Multi{
                    index: index, storage: storage, t: t, max_timestamp: max_timestamp,
                })
            },

            ReplicatedSenti{index, storage, t, id, max_timestamp, ..} => {
                (id, ReplicatedSkeens::Senti{
                    index: index, storage: storage, t: t, max_timestamp: max_timestamp,
                })
            },

            ReplicatedSnap{index, storage, t, id, max_timestamp, ..} => {
                (id, ReplicatedSkeens::Snap{
                    index: index, storage: storage, t: t, max_timestamp: max_timestamp,
                })
            },

            _ => unreachable!(),
        }
    }

    fn multi_timestamp(&self) -> Timestamp {
        use self::WaitingForMax::*;
        match self {
            &Multi{timestamp, ..} => Timestamp::Phase1(timestamp),
            &Senti{timestamp, ..} => Timestamp::Phase1(timestamp),
            &Snap{timestamp, ..} => Timestamp::Phase1(timestamp),
            &Single{timestamp, ..}  => Timestamp::Phase2(timestamp),

            &GotMaxMulti{timestamp, ..}
            | &GotMaxSenti{timestamp, ..}
            | &GotMaxSnap{timestamp, ..}
            | &SimpleSingle{timestamp, ..} => Timestamp::Phase2(timestamp),

            &ReplicatedMulti{max_timestamp, ..}
            | &ReplicatedSenti{max_timestamp, ..}
            | &ReplicatedSnap{max_timestamp, ..}
            | &ReplicatedSingle{max_timestamp, ..} => Timestamp::Phase2(max_timestamp),
        }
    }

    fn get_id(&self) -> Uuid {
        use self::WaitingForMax::*;
        match self {
            &Multi{id, ..}
            | &Senti{id, ..}
            | &Snap{id, ..}
            | &Single{id, ..} => id,

            &GotMaxMulti{id, ..}
            | &GotMaxSenti{id, ..}
            | &GotMaxSnap{id, ..}
            | &SimpleSingle{id, ..} => id,

            &ReplicatedMulti{id, ..}
            | &ReplicatedSenti{id, ..}
            | &ReplicatedSnap{id, ..}
            | &ReplicatedSingle{id, ..} => id,
        }
    }
}

impl<T> ::std::fmt::Debug for WaitingForMax<T> {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error> {
        match self {
            &WaitingForMax::GotMaxMulti{ref timestamp, ref node_num, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::GotMaxMulti")
                    .field("id", id)
                    .field("timestamp", timestamp)
                    .field("node_num", node_num)
                    .field("storage", &unsafe{ storage.get() })
                    .finish()
            },
            &WaitingForMax::GotMaxSenti{ref timestamp, ref node_num, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::GotMaxSenti")
                    .field("id", id)
                    .field("timestamp", timestamp)
                    .field("node_num", node_num)
                    .field("storage", &unsafe{ storage.get() })
                    .finish()
            },
            &WaitingForMax::GotMaxSnap{ref timestamp, ref node_num, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("WaitingForSnap::GotMaxSenti")
                    .field("id", id)
                    .field("timestamp", timestamp)
                    .field("node_num", node_num)
                    .field("storage", &unsafe{ storage.get() })
                    .finish()
            },
            &WaitingForMax::Multi{ref timestamp, ref node_num, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::Multi")
                    .field("id", id)
                    .field("timestamp", timestamp)
                    .field("node_num", node_num)
                    .field("storage", &unsafe{ storage.get() })
                    .finish()
            },
            &WaitingForMax::Senti{ref timestamp, ref node_num, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::Senti")
                    .field("id", id)
                    .field("timestamp", timestamp)
                    .field("node_num", node_num)
                    .field("storage", &unsafe{ storage.get() })
                    .finish()
            },
            &WaitingForMax::Snap{ref timestamp, ref node_num, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::Snap")
                    .field("id", id)
                    .field("timestamp", timestamp)
                    .field("node_num", node_num)
                    .field("storage", &unsafe{ storage.get() })
                    .finish()
            },
            &WaitingForMax::SimpleSingle{ref timestamp, ref node_num, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::SimpleSingle")
                    .field("id", id)
                    .field("timestamp", timestamp)
                    .field("node_num", node_num)
                    .field("storage", storage)
                    .finish()
            },
            &WaitingForMax::Single{ref timestamp, ref node_num, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::Single")
                    .field("id", id)
                    .field("timestamp", timestamp)
                    .field("node_num", node_num)
                    .field("storage", storage)
                    .finish()
            },

            &WaitingForMax::ReplicatedMulti{
                ref max_timestamp, ref index, ref storage, ref t, ref id
            } => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::ReplicatedMulti")
                    .field("id", id)
                    .field("max_timestamp", max_timestamp)
                    .field("index", index)
                    .field("storage", &unsafe{ storage.get() })
                    .finish()
            },
            &WaitingForMax::ReplicatedSenti{
                ref max_timestamp, ref index, ref storage, ref t, ref id
            } => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::ReplicatedSenti")
                    .field("id", id)
                    .field("max_timestamp", max_timestamp)
                    .field("index", index)
                    .field("storage", &unsafe{ storage.get() })
                    .finish()
            },
            &WaitingForMax::ReplicatedSnap{
                ref max_timestamp, ref index, ref storage, ref t, ref id
            } => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::ReplicatedSnap")
                    .field("id", id)
                    .field("max_timestamp", max_timestamp)
                    .field("index", index)
                    .field("storage", &unsafe{ storage.get() })
                    .finish()
            },
            &WaitingForMax::ReplicatedSingle{
                ref max_timestamp, ref index, ref storage, ref t, ref id
            } => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::ReplicatedSingle")
                    .field("id", id)
                    .field("max_timestamp", max_timestamp)
                    .field("index", index)
                    .field("storage", storage)
                    .finish()
            },
        }
    }
}

pub enum GotMax<T> {
    Multi{timestamp: u64, storage: SkeensMultiStorage, t: T, id: Uuid},
    Senti{timestamp: u64, storage: SkeensMultiStorage, t: T, id: Uuid},
    Snap{timestamp: u64, storage: SkeensMultiStorage, t: T, id: Uuid},
    SimpleSingle{timestamp: u64, storage: ValEdge, t: T, id: Uuid},
    Single{timestamp: u64, node_num: u64, storage: ValEdge, t: T, id: Uuid},
}

impl<T> GotMax<T> {

    fn happens_before<U>(&self, other: &WaitingForMax<U>) -> bool {
        //g.timestamp() <= v.timestamp()
        // let my_ts = self.timestamp();
        // match other.multi_timestamp() {
        //     Timestamp::Phase1(other_ts) | Timestamp::Phase2(other_ts) if my_ts < other_ts => true,
        //     Timestamp::Phase1(other_ts) | Timestamp::Phase2(other_ts) if my_ts == other_ts =>
        //         self.get_id() < other.get_id(),
        //     _ => false,
        // }
        match (self, other) {
            (&GotMax::Multi{timestamp: my_ts, ..},
                &WaitingForMax::Multi{timestamp: other_ts, ..})
            | (&GotMax::Senti{timestamp: my_ts, ..},
                &WaitingForMax::Senti{timestamp: other_ts, ..})
            | (&GotMax::Snap{timestamp: my_ts, ..},
                &WaitingForMax::Snap{timestamp: other_ts, ..})

            | (&GotMax::Senti{timestamp: my_ts, ..},
                &WaitingForMax::Multi{timestamp: other_ts, ..})
            | (&GotMax::Multi{timestamp: my_ts, ..},
                &WaitingForMax::Senti{timestamp: other_ts, ..})


            | (&GotMax::Snap{timestamp: my_ts, ..},
                &WaitingForMax::Multi{timestamp: other_ts, ..})
            | (&GotMax::Multi{timestamp: my_ts, ..},
                &WaitingForMax::Snap{timestamp: other_ts, ..})

            | (&GotMax::Senti{timestamp: my_ts, ..},
                &WaitingForMax::Snap{timestamp: other_ts, ..})
            | (&GotMax::Snap{timestamp: my_ts, ..},
                &WaitingForMax::Senti{timestamp: other_ts, ..})

                if my_ts < other_ts => true,

            (&GotMax::Multi{timestamp: my_ts, id: ref my_id, ..},
                &WaitingForMax::Multi{timestamp: other_ts, id: ref other_id, ..})
            | (&GotMax::Senti{timestamp: my_ts, id: ref my_id, ..},
                &WaitingForMax::Senti{timestamp: other_ts, id: ref other_id, ..})
            | (&GotMax::Snap{timestamp: my_ts, id: ref my_id, ..},
                &WaitingForMax::Snap{timestamp: other_ts, id: ref other_id, ..})

            | (&GotMax::Senti{timestamp: my_ts, id: ref my_id, ..},
                &WaitingForMax::Multi{timestamp: other_ts, id: ref other_id, ..})
            | (&GotMax::Multi{timestamp: my_ts, id: ref my_id, ..},
                &WaitingForMax::Senti{timestamp: other_ts, id: ref other_id, ..})

            | (&GotMax::Snap{timestamp: my_ts, id: ref my_id, ..},
                &WaitingForMax::Multi{timestamp: other_ts, id: ref other_id, ..})
            | (&GotMax::Multi{timestamp: my_ts, id: ref my_id, ..},
                &WaitingForMax::Snap{timestamp: other_ts, id: ref other_id, ..})

            | (&GotMax::Senti{timestamp: my_ts, id: ref my_id, ..},
                &WaitingForMax::Snap{timestamp: other_ts, id: ref other_id, ..})
            | (&GotMax::Snap{timestamp: my_ts, id: ref my_id, ..},
                &WaitingForMax::Senti{timestamp: other_ts, id: ref other_id, ..})

                if my_ts == other_ts && my_id < other_id => true,

            (&GotMax::Multi{..}, &WaitingForMax::Multi{..})
            | (&GotMax::Senti{..}, &WaitingForMax::Senti{..})
            | (&GotMax::Snap{..}, &WaitingForMax::Snap{..})
            | (&GotMax::Senti{..}, &WaitingForMax::Multi{..})
            | (&GotMax::Multi{..}, &WaitingForMax::Senti{..})
            | (&GotMax::Snap{..}, &WaitingForMax::Multi{..})
            | (&GotMax::Multi{..}, &WaitingForMax::Snap{..})
            | (&GotMax::Senti{..}, &WaitingForMax::Snap{..})
            | (&GotMax::Snap{..}, &WaitingForMax::Senti{..}) => false,

            //FIXME
            (_, &WaitingForMax::SimpleSingle{..})
            | (_, &WaitingForMax::Single{..})
            | (&GotMax::SimpleSingle{..}, _)
            | (&GotMax::Single{..}, _) => {
                let my_ts = self.timestamp();
                match other.multi_timestamp() {
                    Timestamp::Phase1(other_ts) | Timestamp::Phase2(other_ts) if my_ts <= other_ts => true,
                    // Timestamp::Phase1(other_ts) | Timestamp::Phase2(other_ts) if my_ts == other_ts =>
                    //     self.get_id() < other.get_id(),
                    _ => false,
                }
            },

            (_, &WaitingForMax::GotMaxMulti{..})
            | (_, &WaitingForMax::GotMaxSenti{..})
            | (_, &WaitingForMax::GotMaxSnap{..}) => unreachable!(),

            (_, &WaitingForMax::ReplicatedSingle{..})
            | (_, &WaitingForMax::ReplicatedMulti{..})
            | (_, &WaitingForMax::ReplicatedSenti{..})
            | (_, &WaitingForMax::ReplicatedSnap{..}) =>
                unimplemented!()
        }
    }

    #[allow(dead_code)]
    fn timestamp(&self) -> u64 {
        use self::GotMax::*;
        match self {
            &Multi{timestamp, ..} | &Senti{timestamp, ..} | &Snap{timestamp, ..} => timestamp,
            &SimpleSingle{timestamp, ..} => timestamp,
            &Single{timestamp, ..} => timestamp,
        }
    }

    #[allow(dead_code)]
    fn is_multi(&self) -> bool {
        use self::GotMax::*;
        match self {
            &Multi{..} | &Senti{..} | &Snap{..} => true,
            &Single{..} | &SimpleSingle{..} => false,
        }
    }

    fn get_id(&self) -> Uuid {
        use self::GotMax::*;
        match self {
            &Multi{id, ..} | &Senti{id, ..} | &Snap{id, ..}
            | &Single{id, ..} | &SimpleSingle{id, ..} => id,
        }
    }
}

impl<T> ::std::fmt::Debug for GotMax<T> {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error> {
        match self {
            &GotMax::Multi{ref timestamp, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("GotMax::Multi")
                    .field("timestamp", timestamp)
                    .field("storage", &unsafe{ storage.get() })
                    .field("id", id)
                    .finish()
            },
            &GotMax::Senti{ref timestamp, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("GotMax::Senti")
                    .field("timestamp", timestamp)
                    .field("storage", &unsafe{ storage.get() })
                    .field("id", id)
                    .finish()
            },
            &GotMax::Snap{ref timestamp, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("GotMax::Snap")
                    .field("timestamp", timestamp)
                    .field("storage", &unsafe{ storage.get() })
                    .field("id", id)
                    .finish()
            },
            &GotMax::SimpleSingle{ref timestamp, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("GotMax::SimpleSingle")
                    .field("timestamp", timestamp)
                    .field("storage", storage)
                    .field("id", id)
                    .finish()
            },
            &GotMax::Single{ref timestamp, ref node_num, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("GotMax::Single")
                    .field("timestamp", timestamp)
                    .field("node_num", node_num)
                    .field("storage", storage)
                    .field("id", id)
                    .finish()
            },
        }
    }
}

///////////////////////////////////////

//We only have a max-heap so number with a _lower_ magnitude make a GotMax _greater_
// that is, Greater ord happens earlier in time
impl<T> Ord for GotMax<T> {
    //FIXME
    fn cmp(&self, other: &Self) -> Ordering {
        use self::GotMax::*;
        match (self, other) {
            (&Multi{timestamp: my_timestamp, id: ref my_id, ..},
                &Multi{timestamp: other_timestamp, id: ref other_id, ..})
            | (&Senti{timestamp: my_timestamp, id: ref my_id, ..},
                &Senti{timestamp: other_timestamp, id: ref other_id, ..})
            | (&Snap{timestamp: my_timestamp, id: ref my_id, ..},
                &Snap{timestamp: other_timestamp, id: ref other_id, ..})

            | (&Senti{timestamp: my_timestamp, id: ref my_id, ..},
                &Multi{timestamp: other_timestamp, id: ref other_id, ..})
            | (&Multi{timestamp: my_timestamp, id: ref my_id, ..},
                &Senti{timestamp: other_timestamp, id: ref other_id, ..})

            | (&Snap{timestamp: my_timestamp, id: ref my_id, ..},
                &Multi{timestamp: other_timestamp, id: ref other_id, ..})
            | (&Multi{timestamp: my_timestamp, id: ref my_id, ..},
                &Snap{timestamp: other_timestamp, id: ref other_id, ..})

            | (&Senti{timestamp: my_timestamp, id: ref my_id, ..},
                &Snap{timestamp: other_timestamp, id: ref other_id, ..})
            | (&Snap{timestamp: my_timestamp, id: ref my_id, ..},
                &Senti{timestamp: other_timestamp, id: ref other_id, ..}) =>
                match my_timestamp.cmp(&other_timestamp) {
                    Equal => my_id.cmp(other_id).reverse(),
                    o => o.reverse(),
                },

            (&Multi{timestamp: my_timestamp, ..},
                &SimpleSingle{timestamp: other_timestamp, ..})
            |(&Senti{timestamp: my_timestamp, ..},
                &SimpleSingle{timestamp: other_timestamp, ..})
            |(&Snap{timestamp: my_timestamp, ..},
                &SimpleSingle{timestamp: other_timestamp, ..}) =>
                match my_timestamp.cmp(&other_timestamp) {
                    Equal => Greater,
                    o => o.reverse(),
                },

            // (&SimpleSingle{timestamp: my_timestamp, storage: ms, ..},
            //     &SimpleSingle{timestamp: other_timestamp, storage: os, ..}) =>
            //     match my_timestamp.cmp(&other_timestamp) {
            //         Equal => panic!(
            //             "cannot have multiple singles with the same time:
            //             \nleft: SimpleSingle {{
            //                 timestamp: {:?},
            //                 storage: {:?}
            //             }}
            //             \nright: SimpleSingle {{
            //                 timestamp: {:?},
            //                 storage: {:?}
            //             }}",
            //             my_timestamp, ms, other_timestamp, os),
            //         o => o.reverse(),
            //     },
            (&SimpleSingle{timestamp: my_timestamp, storage: ms, id: ref my_id,..},
                &SimpleSingle{timestamp: other_timestamp, storage: os, id: ref other_id, ..}) =>
                match my_timestamp.cmp(&other_timestamp) {
                    Equal => {
                        let cmp = my_id.cmp(other_id).reverse();
                        if cmp == Equal {
                            panic!(
                            "cannot have multiple singles with the same time and id:
                            \nleft: SimpleSingle {{
                                timestamp: {:?},
                                storage: {:?}
                                id: {:?}
                            }}
                            \nright: SimpleSingle {{
                                timestamp: {:?},
                                storage: {:?},
                                id: {:?}
                            }}",
                            my_timestamp, ms, my_id, other_timestamp, os, other_id)
                        }
                        cmp
                    },
                    o => o.reverse(),
                },

            (&SimpleSingle{timestamp: my_timestamp, ..},
                &Multi{timestamp: other_timestamp, ..})
            | (&SimpleSingle{timestamp: my_timestamp, ..},
                &Senti{timestamp: other_timestamp, ..})
            | (&SimpleSingle{timestamp: my_timestamp, ..},
                &Snap{timestamp: other_timestamp, ..}) =>
                match my_timestamp.cmp(&other_timestamp) {
                    Equal => Less,
                    o => o.reverse(),
                },

            (&Multi{timestamp: my_ts, ..}, &Single{timestamp: other_ts, ..})
            | (&Senti{timestamp: my_ts, ..}, &Single{timestamp: other_ts, ..})
            | (&Snap{timestamp: my_ts, ..}, &Single{timestamp: other_ts, ..}) =>
                match my_ts.cmp(&other_ts) {
                    Equal => Greater,
                    o => o.reverse(),
                },

            (&Single{timestamp: my_ts, ..}, &Multi{timestamp: other_ts, ..})
            | (&Single{timestamp: my_ts, ..}, &Senti{timestamp: other_ts, ..})
            | (&Single{timestamp: my_ts, ..}, &Snap{timestamp: other_ts, ..}) =>
                match my_ts.cmp(&other_ts) {
                    Equal => Less,
                    o => o.reverse(),
                },

            (&Single{timestamp: my_timestamp, node_num: my_num, ..},
                &Single{timestamp: other_timestamp, node_num: other_num, ..}) =>
                match my_timestamp.cmp(&other_timestamp) {
                    Equal => my_num.cmp(&other_num).reverse(),
                    o => o.reverse(),
                },

            (&SimpleSingle{..}, &Single{..}) =>
                unreachable!(),

            (&Single{..}, &SimpleSingle{..}) =>
                unreachable!(),
        }
    }
}

impl<T> PartialOrd for GotMax<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for GotMax<T> {
    fn eq(&self, other: &Self) -> bool {
        use self::GotMax::*;
        match (self, other) {
            (&Multi{timestamp: my_ts, id: ref my_id, storage: ref my_s, ..},
                &Multi{timestamp: other_ts, id: ref other_id, storage: ref other_s, ..})
            => my_ts == other_ts && my_id == other_id
                && my_s.ptr() as *const _ == other_s.ptr(),

            (&Senti{timestamp: my_ts, id: ref my_id, storage: ref my_s, ..},
                &Senti{timestamp: other_ts, id: ref other_id, storage: ref other_s, ..})
            => my_ts == other_ts && my_id == other_id
                && my_s.ptr() as *const _ == other_s.ptr(),

            (&SimpleSingle{timestamp: my_ts, storage: my_s, ..},
                &SimpleSingle{timestamp: other_ts, storage: other_s, ..})
            => my_ts == other_ts && my_s == other_s,

            (&Single{timestamp: my_m_ts, node_num: my_n, storage: my_s, ..},
                &Single{timestamp: other_m_ts, node_num: other_n, storage: other_s, ..})
            => my_m_ts == other_m_ts && my_n == other_n && my_s == other_s,

            (_, _) => false,
        }
    }
}

impl<T> Eq for GotMax<T> {}

///////////////////////////////////////

#[cfg(test)]
mod test {
    use std::cmp::Ordering::*;
    use std::collections::BinaryHeap;
    use super::ValEdge;

    use uuid::Uuid;

    use super::*;
    use super::GotMax::*;

    fn null() -> ValEdge {
        ValEdge::null()
    }

    fn val(n: usize) -> ValEdge {
        unsafe {::std::mem::transmute(n)}
    }

    #[test]
    fn got_max_m_m_cmp() {
        assert_eq!(Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Equal
        );
        assert_eq!(Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&Multi{ timestamp: 2, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Greater
        );
        assert_eq!(Multi{ timestamp: 3, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&Multi{ timestamp: 2, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Less
        );
    }

    #[test]
    fn got_max_m_s2_cmp() {
        assert_eq!(Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&SimpleSingle{ timestamp: 2, storage: null(), t: (), id: Uuid::nil()}),
            Greater
        );
        assert_eq!(Multi{ timestamp: 3, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&SimpleSingle{ timestamp: 2, storage: null(), t: (), id: Uuid::nil()}),
            Less
        );
    }

    #[test]
    fn got_max_s2_m_cmp() {
        assert_eq!(SimpleSingle{ timestamp: 1, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Less
        );
        assert_eq!(SimpleSingle{ timestamp: 1, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Multi{ timestamp: 2, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Greater
        );
        assert_eq!(SimpleSingle{ timestamp: 3, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Multi{ timestamp: 2, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Less
        );
    }

    #[test]
    fn got_max_s2_s2_cmp() {
        assert_eq!(SimpleSingle{ timestamp: 1, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&SimpleSingle{ timestamp: 2, storage: null(), t: (), id: Uuid::nil()}),
            Greater
        );
        assert_eq!(SimpleSingle{ timestamp: 3, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&SimpleSingle{ timestamp: 2, storage: null(), t: (), id: Uuid::nil()}),
            Less
        );
    }

    #[test]
    #[should_panic]
    fn s2_s2_eq() {
        SimpleSingle{ timestamp: 1, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&SimpleSingle{ timestamp: 1, storage: null(), t: (), id: Uuid::nil()});
    }

    //FIXME
    #[test]
    fn m_s2_eq() {
        Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&SimpleSingle{ timestamp: 1, storage: null(), t: (), id: Uuid::nil()});
    }

    #[test]
    fn got_max_m_s_cmp() {
        assert_eq!(Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&Single{ timestamp: 1, node_num: 0, storage: null(), t: (), id: Uuid::nil()}),
            Greater
        );
        assert_eq!(Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&Single{ timestamp: 2, node_num: 200, storage: null(), t: (), id: Uuid::nil()}),
            Greater
        );
        assert_eq!(Multi{ timestamp: 3, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&Single{ timestamp: 2, node_num: 0, storage: null(), t: (), id: Uuid::nil()}),
            Less
        );
    }

    #[test]
    fn got_max_s_m_cmp() {
        assert_eq!(Single{ timestamp: 1, node_num: 0, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Less
        );
        assert_eq!(Single{ timestamp: 1, node_num: 200,storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Multi{ timestamp: 2, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Greater
        );
        assert_eq!(Single{ timestamp: 3, node_num: 0,storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Multi{ timestamp: 2, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Less
        );
    }

    #[test]
    fn got_max_s_s_cmp() {
        assert_eq!(Single{ timestamp: 1, node_num: 200, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Single{ timestamp: 1, node_num: 200, storage: null(), t: (), id: Uuid::nil()}),
            Equal
        );
        assert_eq!(Single{ timestamp: 1, node_num: 200, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Single{ timestamp: 2, node_num: 1, storage: null(), t: (), id: Uuid::nil()}),
            Greater
        );
        assert_eq!(Single{ timestamp: 3, node_num: 1, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Single{ timestamp: 2, node_num: 200, storage: null(), t: (), id: Uuid::nil()}),
            Less
        );

        assert_eq!(Single{ timestamp: 5, node_num: 200, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Single{ timestamp: 5, node_num: 200, storage: null(), t: (), id: Uuid::nil()}),
            Equal
        );
        assert_eq!(Single{ timestamp: 5, node_num: 10, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Single{ timestamp: 5, node_num: 200, storage: null(), t: (), id: Uuid::nil()}),
            Greater
        );
        assert_eq!(Single{ timestamp: 5, node_num: 200, storage: null(), t: (), id: Uuid::nil()}
            .cmp(&Single{ timestamp: 5, node_num: 5, storage: null(), t: (), id: Uuid::nil()}),
            Less
        );
    }

    #[test]
    fn happens_before_ord() {
        let g0 = GotMax::Multi{
            timestamp: 2, storage: multi_storage(), t: (), id: Uuid::new_v4()
        };
        let id = Uuid::new_v4();
        let w1 = WaitingForMax::Multi{
            timestamp: 2, storage: multi_storage(), t: (), id: id, node_num: 0,
        };
        let g1 = GotMax::Multi{
            timestamp: 2, storage: multi_storage(), t: (), id: id
        };
        assert_eq!(g0.happens_before(&w1), g0 > g1);
    }

    #[test]
    fn binary_heap() {
        let mut heap = BinaryHeap::with_capacity(4);
        heap.push(Multi{ timestamp: 2, storage: multi_storage(), t: (), id: Uuid::nil()});
        heap.push(Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()});
        heap.push(Multi{ timestamp: 67, storage: multi_storage(), t: (), id: Uuid::nil()});
        heap.push(Multi{ timestamp: 0, storage: multi_storage(), t: (), id: Uuid::nil()});
        assert_eq!(heap.pop().unwrap().timestamp(), 0);
        assert_eq!(heap.pop().unwrap().timestamp(), 1);
        assert_eq!(heap.pop().unwrap().timestamp(), 2);
        assert_eq!(heap.pop().unwrap().timestamp(), 67);
    }

    #[test]
    fn one_multi() {
        let mut skeen = SkeensState::new();
        let s = multi_storage();
        //TODO add assert for fn res
        skeen.add_multi_append(Uuid::nil(), s.clone(), false, ()).assert_new();
        assert_eq!(skeen.next_timestamp, 2);
        let _ = skeen.set_max_timestamp(Uuid::nil(), 1);
        assert_eq!(skeen.next_timestamp, 2);
        let mut v = Vec::with_capacity(1);
        skeen.flush_got_max_timestamp(|g| v.push(g));
        assert_eq!(&*v, &[Multi{timestamp: 1, id: Uuid::nil(), t: (), storage: s}]);
        assert_eq!(skeen.next_timestamp, 2);
    }

    #[test]
    fn one_multi1() {
        let mut skeen = SkeensState::new();
        //TODO add assert for fn res
        let s = multi_storage();
        skeen.add_multi_append(Uuid::nil(), s.clone(), false, ()).assert_new();
        assert_eq!(skeen.next_timestamp, 2);
        let r = skeen.set_max_timestamp(Uuid::nil(), 1223);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 1224);
        let mut v = Vec::with_capacity(1);
        skeen.flush_got_max_timestamp(|g| v.push(g));
        assert_eq!(&*v, &[Multi{timestamp: 1223, id: Uuid::nil(), t: (), storage: s}]);
        assert_eq!(skeen.next_timestamp, 1224);
    }

    #[test]
    fn multi_gap() {
        let id0 = Uuid::new_v4();
        let s0 = multi_storage();
        let id1 = Uuid::new_v4();
        let s1 = multi_storage();
        let mut skeen = SkeensState::new();
        //TODO add assert for fn res
        skeen.add_multi_append(id0, s0.clone(), false, ()).assert_new();
        assert_eq!(skeen.next_timestamp, 2);
        skeen.add_multi_append(id1, s1.clone(), false, ()).assert_new();
        assert_eq!(skeen.next_timestamp, 3);
        let r = skeen.set_max_timestamp(id1, 122);
        assert_eq!(r, SkeensSetMaxRes::Ok);
        assert_eq!(skeen.next_timestamp, 123);
        let mut v = Vec::with_capacity(2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);
        let r = skeen.set_max_timestamp(id0, 4);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 123);
        skeen.flush_got_max_timestamp(|g| {v.push(g);});
        assert_eq!(&*v,
            &[Multi{timestamp: 4, id: id0, t: (), storage: s0},
            Multi{timestamp: 122, id: id1, t: (), storage: s1}]);
    }

    #[test]
    fn multi_rev() {
        let id0 = Uuid::new_v4();
        let s0 = multi_storage();
        let id1 = Uuid::new_v4();
        let s1 = multi_storage();
        let mut skeen = SkeensState::new();
        //TODO add assert for fn res
        skeen.add_multi_append(id0, s0.clone(), false, ()).assert_new();
        assert_eq!(skeen.next_timestamp, 2);
        skeen.add_multi_append(id1, s1.clone(), false, ()).assert_new();
        assert_eq!(skeen.next_timestamp, 3);
        let r = skeen.set_max_timestamp(id0, 122);
        assert_eq!(r, SkeensSetMaxRes::Ok);
        assert_eq!(skeen.next_timestamp, 123);
        let mut v = Vec::with_capacity(2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);
        let r = skeen.set_max_timestamp(id1, 3);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 123);

        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[Multi{timestamp: 3, id: id1, t: (), storage: s1},
            Multi{timestamp: 122, id: id0, t: (), storage: s0}]);
    }

    #[test]
    fn multi_eq() {
        let id0 = Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap();
        let s0 = multi_storage();
        let id1 = Uuid::nil();
        let s1 = multi_storage();
        let mut skeen = SkeensState::new();
        //TODO add assert for fn res
        skeen.add_multi_append(id0, s0.clone(), false, ()).assert_new();
        assert_eq!(skeen.next_timestamp, 2);
        skeen.add_multi_append(id1, s1.clone(), false, ()).assert_new();
        assert_eq!(skeen.next_timestamp, 3);
        let r = skeen.set_max_timestamp(id0, 100);
        assert_eq!(r, SkeensSetMaxRes::Ok);
        assert_eq!(skeen.next_timestamp, 101);
        let mut v = Vec::with_capacity(2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);
        let r = skeen.set_max_timestamp(id1, 100);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 101);

        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[Multi{timestamp: 100, id: id1, t: (), storage: s1},
            Multi{timestamp: 100, id: id0, t: (), storage: s0}]);
    }

    #[test]
    fn multi_eq2() {
        let id0 = Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap();
        let s0 = multi_storage();
        let id1 = Uuid::nil();
        let s1 = multi_storage();
        let mut skeen = SkeensState::new();
        //TODO add assert for fn res
        skeen.add_multi_append(id1, s1.clone(), false, ()).assert_new();
        assert_eq!(skeen.next_timestamp, 2);
        skeen.add_multi_append(id0, s0.clone(), false, ()).assert_new();
        assert_eq!(skeen.next_timestamp, 3);
        let r = skeen.set_max_timestamp(id0, 100);
        assert_eq!(r, SkeensSetMaxRes::Ok);
        assert_eq!(skeen.next_timestamp, 101);
        let mut v = Vec::with_capacity(2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);
        let r = skeen.set_max_timestamp(id1, 100);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 101);

        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[Multi{timestamp: 100, id: id1, t: (), storage: s1},
            Multi{timestamp: 100, id: id0, t: (), storage: s0}]);
    }

    #[test]
    fn multi_single_single() {
        let id0 = Uuid::new_v4();
        let s0 = multi_storage();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let mut v = Vec::with_capacity(3);
        let mut skeen = SkeensState::new();

        let r = skeen.add_multi_append(id0, s0.clone(), false, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 0, val(1), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 0, val(3), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(3, 2));
        assert_eq!(skeen.next_timestamp, 4);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 1);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 4);

        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[
                Multi{timestamp: 1, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 2, t: (), storage: val(1), id: id1},
                SimpleSingle{timestamp: 3, t: (), storage: val(3), id: id2}
            ]
        );
    }

    #[test]
    fn multi_single_single_eq1() {
        let id0 = Uuid::new_v4();
        let s0 = multi_storage();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let mut v = Vec::with_capacity(3);
        let mut skeen = SkeensState::new();

        let r = skeen.add_multi_append(id0, s0.clone(), false, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 0, val(1), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 0, val(3), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(3, 2));
        assert_eq!(skeen.next_timestamp, 4);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 2);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 4);

        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[
                Multi{timestamp: 2, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 2, t: (), storage: val(1), id: id1},
                SimpleSingle{timestamp: 3, t: (), storage: val(3), id: id2}
            ]
        );
    }

    #[test]
    fn multi_single_single_skip1() {
        let id0 = Uuid::new_v4();
        let s0 = multi_storage();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let mut v = Vec::with_capacity(3);
        let mut skeen = SkeensState::new();

        let r = skeen.add_multi_append(id0, s0.clone(), false, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 0, val(1), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 0, val(3), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(3, 2));
        assert_eq!(skeen.next_timestamp, 4);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 3);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 4);

        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 2, t: (), storage: val(1), id: id1},
                Multi{timestamp: 3, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 3, t: (), storage: val(3), id: id2}
            ]
        );
    }

    #[test]
    fn multi_single_single_skip2() {
        let id0 = Uuid::new_v4();
        let s0 = multi_storage();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let mut v = Vec::with_capacity(3);
        let mut skeen = SkeensState::new();

        let r = skeen.add_multi_append(id0, s0.clone(), false, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 0, val(1), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 0, val(3), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(3, 2));
        assert_eq!(skeen.next_timestamp, 4);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 3);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 4);

        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 2, t: (), storage: val(1), id: id1},
                Multi{timestamp: 3, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 3, t: (), storage: val(3), id: id2},
            ]
        );
    }

    #[test]
    fn multi_single_single_gap() {
        let id0 = Uuid::new_v4();
        let s0 = multi_storage();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let mut v = Vec::with_capacity(3);
        let mut skeen = SkeensState::new();

        let r = skeen.add_multi_append(id0, s0.clone(), false, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 0, val(1), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 0, val(3), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(3, 2));
        assert_eq!(skeen.next_timestamp, 4);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 5);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 6);

        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 2, t: (), storage: val(1), id: id1},
                SimpleSingle{timestamp: 3, t: (), storage: val(3), id: id2},
                Multi{timestamp: 5, id: id0, t: (), storage: s0},
            ]
        );
    }

    #[test]
    fn multi_single_gap() {
        let id0 = Uuid::new_v4();
        let s0 = multi_storage();
        let id1 = Uuid::new_v4();
        let mut v = Vec::with_capacity(3);
        let mut skeen = SkeensState::new();

        let r = skeen.add_multi_append(id0, s0.clone(), false, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 0, val(1), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 4);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 5);

        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 2, t: (), storage: val(1), id: id1},
                Multi{timestamp: 4, id: id0, t: (), storage: s0},
            ]
        );
    }

    #[test]
    fn multi_single_rev() {
        let id0 = Uuid::new_v4();
        let s0 = multi_storage();
        let id1 = Uuid::new_v4();
        let mut v = Vec::with_capacity(3);
        let mut skeen = SkeensState::new();

        let r = skeen.add_multi_append(id0, s0.clone(), false, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);
        assert!(!skeen.need_single_at(0), "{:#?}", skeen);
        assert!( skeen.need_single_at(1));
        assert!( skeen.need_single_at(4));

        let r = skeen.add_single_append(id1, 4, val(1), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(5, 1));
        assert_eq!(skeen.next_timestamp, 6);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 4);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 6);

        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[
                Multi{timestamp: 4, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 5, t: (), storage: val(1), id: id1},
            ]
        );
    }


/*
    #[test]
    fn single_multi()

    #[test]
    fn single_multi_gap()

    #[test]
    fn multi_single()

    #[test]
    fn multi_single_gap()

    #[test]
    fn multi_single_rev()

    fn multi_single_skip()?

*/

    fn multi_storage() -> SkeensMultiStorage {
        SkeensMultiStorage(Default::default())
    }
}
