
use std::collections::BinaryHeap;
use std::collections::hash_map::Entry::*;

use std::cmp::{Eq, Ord, Ordering, PartialOrd};
use std::cmp::Ordering::*;

use uuid::Uuid;

use hash::HashMap;

use vec_deque_map::VecDequeMap;

use super::SkeensMultiStorage;

#[derive(Debug)]
pub struct SkeensState<T: Copy> {
    next_timestamp: u64,
    phase1_queue: VecDequeMap<WaitingForMax<T>>,
    got_max_timestamp: BinaryHeap<GotMax<T>>,
    append_status: HashMap<Uuid, AppendStatus>,
    //waiting_for_max_timestamp: LinkedHashMap<Uuid, WaitingForMax<T>>,
    //in_progress: HashMap<Uuid, AppendStatus>, //subsumes waiting_for_max_timestamp and phase2_ids
    //phase2_ids: HashMap<Uuid, u64>,
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
        }
    }

    pub fn add_single_append(
        &mut self, id: Uuid, storage: *const u8, t: T
    ) -> SkeensAppendRes {
        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len());
            if self.phase1_queue.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        assert!(!self.phase1_queue.is_empty());

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
                self.phase1_queue.push_back(WaitingForMax::SimpleSingle{
                    timestamp: timestamp,
                    storage: storage,
                    node_num: node_num,
                    t: t,
                    id: id,
                });
                v.insert(AppendStatus::Singleton(node_num));
                SkeensAppendRes::NewAppend(timestamp, node_num)
            },
        }
    }

    //AKA skeens1
    pub fn add_multi_append(
        &mut self, id: Uuid, storage: SkeensMultiStorage, t: T
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
                self.phase1_queue.push_back(
                    WaitingForMax::Multi{
                    timestamp: timestamp,
                    node_num: node_num,
                    storage: storage,
                    t: t,
                    id: id,
                });
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
                let s = self.phase1_queue.pop_front().unwrap();
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
            let g = self.got_max_timestamp.pop().unwrap();
            let id = g.get_id();
            //FIXME this should get delayed until the multi is fully complete
            //      due to the failing path,
            //      instead we should just set state to complete on multi-server appends
            let _old = self.append_status.remove(&id);
            debug_assert!(_old.is_some(), "no skeen for {:?}", id);
            f(g)
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //     "{:?} != {:?} @ {:?}",
            //     self.phase2_ids, self.got_max_timestamp, id);
        }

    }

    ////////////////////////////////////////

    /*pub fn replicate_single_append(
        &mut self, timestamp: Time, node_num: QueueIndex, id: Uuid, storage: *const u8, t: T
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
        &mut self, timestamp: Time, node_num: QueueIndex, id: Uuid, storage: *const u8, t: T
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
        if start_index > node_num { return false }

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
        &mut self, timestamp: Time, node_num: QueueIndex, id: Uuid, storage: SkeensMultiStorage, t: T
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
        if start_index > node_num { return false }

        match self.append_status.entry(id) {
            Occupied(..) => false,
            Vacant(v) => {
                v.insert(AppendStatus::Phase1(node_num));
                let m = WaitingForMax::Multi{
                    timestamp: timestamp,
                    storage: storage,
                    node_num: node_num,
                    t: t,
                    id: id,
                };
                let old = self.phase1_queue.insert(node_num, m);
                assert!(old.is_none(),
                    "replace skeens? {:?} {:?}",
                    self.phase1_queue.get(node_num), old
                );
                true
            },
        }
    }

    pub fn replicate_round2<F>(&mut self, id: &Uuid, max_timestamp: u64, index: TrieIndex, mut f: F)
    where F: FnMut(ReplicatedSkeens<T>) {
        let offset = match self.append_status.get(&id) {
            Some(&AppendStatus::Phase1(offset)) | Some(&AppendStatus::Singleton(offset)) =>
                offset,
            _ => return,
        };
        self.phase1_queue.get_mut(offset).map(|w|
            w.replicate_max_timetamp(max_timestamp, index));
        if offset != self.phase1_queue.start_index() { return }
        while self.phase1_queue.front().map(|w| w.has_replication()).unwrap_or(false) {
            let replica = self.phase1_queue.pop_front().unwrap();
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
}

pub enum ReplicatedSkeens<T> {
    Multi{index: TrieIndex, storage: SkeensMultiStorage, max_timestamp: Time, t: T},
    Single{index: TrieIndex, storage: *const u8, t: T},
}

enum WaitingForMax<T> {
    GotMaxMulti{timestamp: u64, node_num: u64, storage: SkeensMultiStorage, t: T, id: Uuid},
    SimpleSingle{timestamp: u64, node_num: u64, storage: *const u8, t: T, id: Uuid},
    Single{timestamp: u64, node_num: u64, storage: *const u8, t: T, id: Uuid},
    Multi{timestamp: u64, node_num: u64, storage: SkeensMultiStorage, t: T, id: Uuid},

    ReplicatedMulti{max_timestamp: u64, index: TrieIndex, storage: SkeensMultiStorage, t: T, id: Uuid},
    ReplicatedSingle{max_timestamp: u64, index: TrieIndex, storage: *const u8, t: T, id: Uuid},
}

enum Timestamp {
    Phase1(u64),
    Phase2(u64),
}

impl<T: Copy> WaitingForMax<T> {

    fn set_max_timestamp(&mut self, max_timestamp: u64) -> Result<(), u64> {
        use self::WaitingForMax::*;
        *self = match self {
            &mut GotMaxMulti{timestamp, ..} | &mut SimpleSingle{timestamp, ..} =>
                return Err(timestamp),

            &mut ReplicatedMulti{max_timestamp, ..}
            | &mut ReplicatedSingle{max_timestamp, ..} =>
                return Err(max_timestamp),

            &mut Single{timestamp, ..} => return Err(timestamp),

            &mut Multi{timestamp, node_num, ref storage, t, id, ..} => {
                assert!(max_timestamp >= timestamp,
                    "max_timestamp >= timestamp {:?} >= {:?},",// @ {:?}, {:#?}",
                    max_timestamp, timestamp, /*unsafe {&(*storage.get()).0},
                    unsafe {
                        let e = Entry::<()>::wrap_bytes((*storage.get()).1);
                        (e.id, e.locs())
                    }*/
                );
                GotMaxMulti {
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

            _ => unreachable!(),
        }
    }

    fn has_max(&self) -> bool {
        use self::WaitingForMax::*;
        match self {
            &GotMaxMulti{..}
            | &SimpleSingle{..}
            | &Single{..}
            | &ReplicatedMulti{..}
            | &ReplicatedSingle{..} => true,

            &Multi{..} => false,
        }
    }

    fn replicate_max_timetamp(&mut self, max_timestamp: Time, index: TrieIndex) {
        use self::WaitingForMax::*;
        *self = match self {
            &mut GotMaxMulti{..} | &mut ReplicatedMulti{..} | &mut ReplicatedSingle{..} =>
                unreachable!(),

            &mut Single{timestamp, storage, t, id, ..}
            | &mut SimpleSingle{timestamp, storage, t, id, ..} => {
                //assert_eq!(max_timestamp, timestamp);
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
                ReplicatedMulti {
                    max_timestamp: max_timestamp,
                    index: index,
                    storage: storage.clone(),
                    t: t,
                    id: id,
                }
            },
        };
    }

    fn has_replication(&self) -> bool {
        use self::WaitingForMax::*;
        match self {
            &ReplicatedMulti{..} | &ReplicatedSingle{..} => true,
            _ => false,
        }
    }

    fn to_replica(self) -> (Uuid, ReplicatedSkeens<T>) {
        use self::WaitingForMax::*;
        match self {
            ReplicatedSingle{index, storage, t, id, ..} =>
                (id, ReplicatedSkeens::Single{index: index, storage: storage, t: t}),

            ReplicatedMulti{index, storage, t, id, max_timestamp, ..} => {
                (id, ReplicatedSkeens::Multi{
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
            &Single{timestamp, ..}  => Timestamp::Phase2(timestamp),

            &GotMaxMulti{timestamp, ..}
            | &SimpleSingle{timestamp, ..} => Timestamp::Phase2(timestamp),

            &ReplicatedMulti{max_timestamp, ..}
            | &ReplicatedSingle{max_timestamp, ..} => Timestamp::Phase2(max_timestamp),
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
                    .field("storage", &(&**storage as *const _))
                    .finish()
            },
            &WaitingForMax::Multi{ref timestamp, ref node_num, ref storage, ref t, ref id} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::Multi")
                    .field("id", id)
                    .field("timestamp", timestamp)
                    .field("node_num", node_num)
                    .field("storage", &(&**storage as *const _))
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
                    .field("storage", &(&**storage as *const _))
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
    SimpleSingle{timestamp: u64, storage: *const u8, t: T, id: Uuid},
    Single{timestamp: u64, node_num: u64, storage: *const u8, t: T, id: Uuid},
}

impl<T> GotMax<T> {

    fn happens_before<U>(&self, other: &WaitingForMax<U>) -> bool {
        //g.timestamp() <= v.timestamp()
        match (self, other) {
            (&GotMax::Multi{timestamp: my_ts, ..},
                &WaitingForMax::Multi{timestamp: other_ts, ..})
            if my_ts < other_ts => true,

            (&GotMax::Multi{timestamp: my_ts, id: ref my_id, ..},
                &WaitingForMax::Multi{timestamp: other_ts, id: ref other_id, ..})
            if my_ts == other_ts && my_id < other_id => true,

            (&GotMax::Multi{..}, &WaitingForMax::Multi{..}) => false,

            (_, &WaitingForMax::SimpleSingle{..}) => true,
            (_, &WaitingForMax::Single{..}) => true,
            (&GotMax::SimpleSingle{..}, _) => true,
            (&GotMax::Single{..}, _) => true,

            (_, &WaitingForMax::GotMaxMulti{..}) => unreachable!(),

            (_, &WaitingForMax::ReplicatedSingle{..})
            | (_, &WaitingForMax::ReplicatedMulti{..}) =>
                unimplemented!()
        }
    }

    #[allow(dead_code)]
    fn timestamp(&self) -> u64 {
        use self::GotMax::*;
        match self {
            &Multi{timestamp, ..} | &SimpleSingle{timestamp, ..} => timestamp,
            &Single{timestamp, ..} => timestamp,
        }
    }

    fn is_multi(&self) -> bool {
        use self::GotMax::*;
        match self {
            &Multi{..} => true,
            &Single{..} | &SimpleSingle{..} => false,
        }
    }

    fn get_id(&self) -> Uuid {
        use self::GotMax::*;
        match self {
            &Multi{id, ..} | &Single{id, ..} | &SimpleSingle{id, ..} => id,
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
                    .field("storage", &(&**storage as *const _))
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
    fn cmp(&self, other: &Self) -> Ordering {
        use self::GotMax::*;
        match (self, other) {
            (&Multi{timestamp: my_timestamp, id: ref my_id, ..},
                &Multi{timestamp: other_timestamp, id: ref other_id, ..}) =>
                match my_timestamp.cmp(&other_timestamp) {
                    Equal => my_id.cmp(other_id),
                    o => o.reverse(),
                },

            (&Multi{timestamp: my_timestamp, ..},
                &SimpleSingle{timestamp: other_timestamp, ..}) =>
                match my_timestamp.cmp(&other_timestamp) {
                    Equal => Greater,
                    o => o.reverse(),
                },

            (&SimpleSingle{timestamp: my_timestamp, storage: ms, ..},
                &SimpleSingle{timestamp: other_timestamp, storage: os, ..}) =>
                match my_timestamp.cmp(&other_timestamp) {
                    Equal => panic!(
                        "cannot have multiple singles with the same time:
                        \nleft: SimpleSingle {{
                            timestamp: {:?},
                            storage: {:?}
                        }}
                        \nright: SimpleSingle {{
                            timestamp: {:?},
                            storage: {:?}
                        }}",
                        my_timestamp, ms, other_timestamp, os),
                    o => o.reverse(),
                },

            (&SimpleSingle{timestamp: my_timestamp, ..},
                &Multi{timestamp: other_timestamp, ..}) =>
                match my_timestamp.cmp(&other_timestamp) {
                    Equal => Less,
                    o => o.reverse(),
                },

            (&Multi{timestamp: my_ts, ..}, &Single{timestamp: other_ts, ..}) =>
                match my_ts.cmp(&other_ts) {
                    Equal => Greater,
                    o => o.reverse(),
                },

            (&Single{timestamp: my_ts, ..}, &Multi{timestamp: other_ts, ..}) =>
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
                && &**my_s as *const _ == &**other_s as *const _,

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
    use std::ptr::null;

    use uuid::Uuid;

    use super::*;
    use super::GotMax::*;

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
        skeen.add_multi_append(Uuid::nil(), s.clone(), ());
        assert_eq!(skeen.next_timestamp, 2);
        skeen.set_max_timestamp(Uuid::nil(), 1);
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
        skeen.add_multi_append(Uuid::nil(), s.clone(), ());
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
        skeen.add_multi_append(id0, s0.clone(), ());
        assert_eq!(skeen.next_timestamp, 2);
        skeen.add_multi_append(id1, s1.clone(), ());
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
        let mut i = true;
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
        skeen.add_multi_append(id0, s0.clone(), ());
        assert_eq!(skeen.next_timestamp, 2);
        skeen.add_multi_append(id1, s1.clone(), ());
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

        let mut i = true;
        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[Multi{timestamp: 3, id: id1, t: (), storage: s1},
            Multi{timestamp: 122, id: id0, t: (), storage: s0}]);
    }

    #[test]
    fn multi_single_single() {
        let id0 = Uuid::new_v4();
        let s0 = multi_storage();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let mut v = Vec::with_capacity(3);
        let mut skeen = SkeensState::new();

        let r = skeen.add_multi_append(id0, s0.clone(), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 3 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(3, 2));
        assert_eq!(skeen.next_timestamp, 4);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 1);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 4);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[
                Multi{timestamp: 1, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 2, t: (), storage: 1 as *const _, id: id1},
                SimpleSingle{timestamp: 3, t: (), storage: 3 as *const _, id: id2}
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

        let r = skeen.add_multi_append(id0, s0.clone(), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 3 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(3, 2));
        assert_eq!(skeen.next_timestamp, 4);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 2);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 4);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[
                Multi{timestamp: 2, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 2, t: (), storage: 1 as *const _, id: id1},
                SimpleSingle{timestamp: 3, t: (), storage: 3 as *const _, id: id2}
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

        let r = skeen.add_multi_append(id0, s0.clone(), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 3 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(3, 2));
        assert_eq!(skeen.next_timestamp, 4);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 3);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 4);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 2, t: (), storage: 1 as *const _, id: id1},
                Multi{timestamp: 3, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 3, t: (), storage: 3 as *const _, id: id2}
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

        let r = skeen.add_multi_append(id0, s0.clone(), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 3 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(3, 2));
        assert_eq!(skeen.next_timestamp, 4);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 3);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 4);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 2, t: (), storage: 1 as *const _, id: id1},
                Multi{timestamp: 3, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 3, t: (), storage: 3 as *const _, id: id2},
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

        let r = skeen.add_multi_append(id0, s0.clone(), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 3 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(3, 2));
        assert_eq!(skeen.next_timestamp, 4);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 5);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 6);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 2, t: (), storage: 1 as *const _, id: id1},
                SimpleSingle{timestamp: 3, t: (), storage: 3 as *const _, id: id2},
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

        let r = skeen.add_multi_append(id0, s0.clone(), ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1, 0));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2, 1));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g)});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 4);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 5);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);});

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 2, t: (), storage: 1 as *const _, id: id1},
                Multi{timestamp: 4, id: id0, t: (), storage: s0},
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
