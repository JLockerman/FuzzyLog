
use std::collections::BinaryHeap;

use std::cmp::{Eq, Ord, Ordering, PartialOrd};
use std::cmp::Ordering::*;

use uuid::Uuid;

use linked_hash_map::LinkedHashMap;

use hash::HashMap;

use super::SkeensMultiStorage;

#[derive(Debug)]
pub struct SkeensState<T: Copy> {
    waiting_for_max_timestamp: LinkedHashMap<Uuid, WaitingForMax<T>>,
    got_max_timestamp: BinaryHeap<GotMax<T>>,
    phase2_ids: HashMap<Uuid, u64>,
    next_timestamp: u64,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[must_use]
pub enum SkeensAppendRes {
    OldPhase1(u64),
    Phase2(u64),
    NewAppend(u64),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[must_use]
pub enum SkeensSetMaxRes {
    NeedsFlush,
    Ok,
    Duplicate(u64),
    NotWaiting,
}

impl<T: Copy> SkeensState<T> {

    pub fn new() -> Self {
        SkeensState {
            waiting_for_max_timestamp: Default::default(),
            got_max_timestamp: Default::default(),
            phase2_ids: Default::default(),
            next_timestamp: 1,
        }
    }

    pub fn add_single_append(
        &mut self, id: Uuid, storage: *const u8, t: T
    ) -> SkeensAppendRes {
        use linked_hash_map::Entry::*;

        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len());
            if self.waiting_for_max_timestamp.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        assert!(!self.waiting_for_max_timestamp.is_empty());

        match self.waiting_for_max_timestamp.entry(id) {
            Occupied(o) => match o.into_mut().multi_timestamp() {
                Timestamp::Phase1(t) => SkeensAppendRes::OldPhase1(t),
                Timestamp::Phase2(t) => SkeensAppendRes::Phase2(t),
            },
            Vacant(v) => {
                let timestamp = self.next_timestamp;
                self.next_timestamp += 1;
                v.insert(WaitingForMax::SimpleSingle{
                    timestamp: timestamp,
                    storage: storage,
                    t: t
                });
                SkeensAppendRes::NewAppend(timestamp)
            },
        }
    }

    //AKA skeens1
    pub fn add_multi_append(
        &mut self, id: Uuid, storage: SkeensMultiStorage, t: T
    ) -> SkeensAppendRes {
        use linked_hash_map::Entry::*;

        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //     "{:?} != {:?}", self.phase2_ids, self.got_max_timestamp);
            if self.waiting_for_max_timestamp.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        if let Some(&timestamp) = self.phase2_ids.get(&id) {
            return SkeensAppendRes::Phase2(timestamp)
        }

        let ret = match self.waiting_for_max_timestamp.entry(id) {
            Occupied(o) => match o.into_mut().multi_timestamp() {
                Timestamp::Phase1(t) => SkeensAppendRes::OldPhase1(t),
                Timestamp::Phase2(t) => SkeensAppendRes::Phase2(t),
            },
            Vacant(v) => {
                let timestamp = self.next_timestamp;
                self.next_timestamp += 1;
                v.insert(WaitingForMax::Multi{
                    timestamp: timestamp,
                    storage: storage,
                    t: t
                });
                SkeensAppendRes::NewAppend(timestamp)
            },
        };
        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //     "{:?} != {:?}", self.phase2_ids, self.got_max_timestamp);
            if self.waiting_for_max_timestamp.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        ret
    }

    //AKA skeens2
    pub fn set_max_timestamp(&mut self, sk2_id: Uuid, max_timestamp: u64)
    -> SkeensSetMaxRes {
        use linked_hash_map::Entry::*;

        debug_assert!({
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len());
            if self.waiting_for_max_timestamp.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        if self.next_timestamp <= max_timestamp {
            self.next_timestamp = max_timestamp + 1
        }

        let ret = match self.waiting_for_max_timestamp.entry(sk2_id) {
            //TODO we could iterate over the queue instead of keeping a set of ids
            Vacant(..) => {
                match self.phase2_ids.get(&sk2_id) {
                    Some(&timestamp) => SkeensSetMaxRes::Duplicate(timestamp),
                    None => SkeensSetMaxRes::NotWaiting,
                }
            },
            Occupied(o) => {
                let s = o.into_mut();
                match s.set_max_timestamp(max_timestamp) {
                    Err(ts) => SkeensSetMaxRes::Duplicate(ts),
                    Ok(..) => SkeensSetMaxRes::Ok,
                }
            },
        };

        if let SkeensSetMaxRes::Ok = ret {
            while !self.waiting_for_max_timestamp.is_empty()
                && self.waiting_for_max_timestamp.front()
                    .map(|(_, v)| v.has_max()).unwrap_or(false) {
                let (id, s) = self.waiting_for_max_timestamp.pop_front().unwrap();
                let s = s.into_got_max(id);
                let is_multi = s.is_multi();
                self.got_max_timestamp.push(s);
                if is_multi { self.phase2_ids.insert(id, max_timestamp); }
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
            if self.waiting_for_max_timestamp.is_empty() {
                debug_assert!(self.got_max_timestamp.is_empty())
            }
            true
        });

        ret
    }

    pub fn flush_got_max_timestamp<F>(&mut self, mut f: F)
    where F: FnMut(GotMax<T>) -> Option<Uuid> {
        while self.can_flush() {
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //     "{:?} != {:?}",
            //     self.phase2_ids, self.got_max_timestamp);
            let g = self.got_max_timestamp.pop().unwrap();
            let id = f(g);
            if let Some(id) = id {
                let _old = self.phase2_ids.remove(&id);
                debug_assert!(_old.is_some(), "no skeen for {:?}", id);
            }
            // debug_assert_eq!(self.phase2_ids.len(), self.got_max_timestamp.len(),
            //     "{:?} != {:?} @ {:?}",
            //     self.phase2_ids, self.got_max_timestamp, id);
        }

    }

    fn can_flush(&self) -> bool {
        self.got_max_timestamp.peek().map(|g|
            self.waiting_for_max_timestamp.front().map(|(id, v)|
                g.happens_before(id, v)
            ).unwrap_or(true)
        ).unwrap_or(false)
    }

    pub fn is_empty(&self) -> bool {
        self.waiting_for_max_timestamp.is_empty()
        && self.got_max_timestamp.is_empty()
    }
}

enum WaitingForMax<T> {
    GotMaxMulti{timestamp: u64, storage: SkeensMultiStorage, t: T},
    SimpleSingle{timestamp: u64, storage: *const u8, t: T},
    Single{multi_timestamp: u64, single_num: u64, storage: *const u8, t: T},
    Multi{timestamp: u64, storage: SkeensMultiStorage, t: T},
}

enum Timestamp {
    Phase1(u64),
    Phase2(u64),
}

impl<T: Copy> WaitingForMax<T> {

    fn set_max_timestamp(&mut self, max_timestamp: u64) -> Result<(), u64> {
        use self::WaitingForMax::*;
        use packets::Entry;
        *self = match self {
            &mut GotMaxMulti{timestamp, ..} | &mut SimpleSingle{timestamp, ..} =>
                return Err(timestamp),

            &mut Single{multi_timestamp, ..} => return Err(multi_timestamp),

            &mut Multi{timestamp, ref storage, t} => {
                assert!(max_timestamp >= timestamp,
                    "max_timestamp >= timestamp {:?} >= {:?} @ {:?}, {:#?}",
                    max_timestamp, timestamp, unsafe {&(*storage.get()).0},
                    unsafe {
                        let e = Entry::<()>::wrap_bytes((*storage.get()).1);
                        (e.id, e.locs())
                    }
                );
                GotMaxMulti{timestamp: max_timestamp, storage: storage.clone(), t: t}
            },
        };
        Ok(())
    }

    fn into_got_max(self, id: Uuid) -> GotMax<T> {
        use self::WaitingForMax::*;
        match self {
            Multi{..} => unreachable!(),

            SimpleSingle{timestamp, storage, t} =>
                GotMax::SimpleSingle{timestamp: timestamp, storage: storage, t: t},

            Single{multi_timestamp, single_num, storage, t} =>
                GotMax::Single{multi_timestamp: multi_timestamp, single_num: single_num, storage: storage, t: t},

            GotMaxMulti{timestamp, storage, t} => {
                GotMax::Multi{timestamp: timestamp, storage: storage, t: t, id: id}
            },
        }
    }

    fn has_max(&self) -> bool {
        use self::WaitingForMax::*;
        match self {
            &GotMaxMulti{..} | &SimpleSingle{..} | &Single{..} => true,
            &Multi{..} => false,
        }
    }

    fn multi_timestamp(&self) -> Timestamp {
        use self::WaitingForMax::*;
        match self {
            &Multi{timestamp, ..} => Timestamp::Phase1(timestamp),
            &Single{multi_timestamp, ..}  => Timestamp::Phase2(multi_timestamp),
            &GotMaxMulti{timestamp, ..}
            | &SimpleSingle{timestamp, ..} => Timestamp::Phase2(timestamp),
        }
    }
}

impl<T> ::std::fmt::Debug for WaitingForMax<T> {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error> {
        match self {
            &WaitingForMax::GotMaxMulti{ref timestamp, ref storage, ref t} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::GotMaxMulti")
                    .field("timestamp", timestamp)
                    .field("storage", &(&**storage as *const _))
                    .finish()
            },
            &WaitingForMax::Multi{ref timestamp, ref storage, ref t} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::Multi")
                    .field("timestamp", timestamp)
                    .field("storage", &(&**storage as *const _))
                    .finish()
            },
            &WaitingForMax::SimpleSingle{ref timestamp, ref storage, ref t} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::SimpleSingle")
                    .field("timestamp", timestamp)
                    .field("storage", storage)
                    .finish()
            },
            &WaitingForMax::Single{ref multi_timestamp, ref single_num, ref storage, ref t} => {
                let _ = t;
                fmt.debug_struct("WaitingForMax::Single")
                    .field("multi_timestamp", multi_timestamp)
                    .field("single_num", single_num)
                    .field("storage", storage)
                    .finish()
            },
        }
    }
}

pub enum GotMax<T> {
    Multi{timestamp: u64, storage: SkeensMultiStorage, t: T, id: Uuid},
    SimpleSingle{timestamp: u64, storage: *const u8, t: T},
    Single{multi_timestamp: u64, single_num: u64, storage: *const u8, t: T},
}

impl<T> GotMax<T> {

    fn happens_before<U>(&self, other_id: &Uuid, other: &WaitingForMax<U>) -> bool {
        //g.timestamp() <= v.timestamp()
        match (self, other) {
            (&GotMax::Multi{timestamp: my_ts, ..},
                &WaitingForMax::Multi{timestamp: other_ts, ..})
            if my_ts < other_ts => true,

            (&GotMax::Multi{timestamp: my_ts, id: ref my_id, ..},
                &WaitingForMax::Multi{timestamp: other_ts, ..})
            if my_ts == other_ts && my_id < other_id => true,

            (&GotMax::Multi{..}, &WaitingForMax::Multi{..}) => false,

            (_, &WaitingForMax::SimpleSingle{..}) => true,
            (_, &WaitingForMax::Single{..}) => true,
            (&GotMax::SimpleSingle{..}, _) => true,
            (&GotMax::Single{..}, _) => true,

            (_, &WaitingForMax::GotMaxMulti{..}) => unreachable!(),
        }
    }

    fn timestamp(&self) -> u64 {
        use self::GotMax::*;
        match self {
            &Multi{timestamp, ..} | &SimpleSingle{timestamp, ..} => timestamp,
            &Single{multi_timestamp, ..} => multi_timestamp,
        }
    }

    fn is_multi(&self) -> bool {
        use self::GotMax::*;
        match self {
            &Multi{..} => true,
            &Single{..} | &SimpleSingle{..} => false,
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
            &GotMax::SimpleSingle{ref timestamp, ref storage, ref t} => {
                let _ = t;
                fmt.debug_struct("GotMax::SimpleSingle")
                    .field("timestamp", timestamp)
                    .field("storage", storage)
                    .finish()
            },
            &GotMax::Single{ref multi_timestamp, ref single_num, ref storage, ref t} => {
                let _ = t;
                fmt.debug_struct("GotMax::Single")
                    .field("multi_timestamp", multi_timestamp)
                    .field("single_num", single_num)
                    .field("storage", storage)
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

            (&Multi{timestamp, ..}, &Single{multi_timestamp, ..}) =>
                match timestamp.cmp(&multi_timestamp) {
                    Equal => Greater,
                    o => o.reverse(),
                },

            (&Single{multi_timestamp, ..}, &Multi{timestamp, ..}) =>
                match multi_timestamp.cmp(&timestamp) {
                    Equal => Less,
                    o => o.reverse(),
                },

            (&Single{multi_timestamp: my_timestamp, single_num: my_num, ..},
                &Single{multi_timestamp: other_timestamp, single_num: other_num, ..}) =>
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

            (&Single{multi_timestamp: my_m_ts, single_num: my_n, storage: my_s, ..},
                &Single{multi_timestamp: other_m_ts, single_num: other_n, storage: other_s, ..})
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
            .cmp(&SimpleSingle{ timestamp: 2, storage: null(), t: ()}),
            Greater
        );
        assert_eq!(Multi{ timestamp: 3, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&SimpleSingle{ timestamp: 2, storage: null(), t: ()}),
            Less
        );
    }

    #[test]
    fn got_max_s2_m_cmp() {
        assert_eq!(SimpleSingle{ timestamp: 1, storage: null(), t: ()}
            .cmp(&Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Less
        );
        assert_eq!(SimpleSingle{ timestamp: 1, storage: null(), t: ()}
            .cmp(&Multi{ timestamp: 2, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Greater
        );
        assert_eq!(SimpleSingle{ timestamp: 3, storage: null(), t: ()}
            .cmp(&Multi{ timestamp: 2, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Less
        );
    }

    #[test]
    fn got_max_s2_s2_cmp() {
        assert_eq!(SimpleSingle{ timestamp: 1, storage: null(), t: ()}
            .cmp(&SimpleSingle{ timestamp: 2, storage: null(), t: ()}),
            Greater
        );
        assert_eq!(SimpleSingle{ timestamp: 3, storage: null(), t: ()}
            .cmp(&SimpleSingle{ timestamp: 2, storage: null(), t: ()}),
            Less
        );
    }

    #[test]
    #[should_panic]
    fn s2_s2_eq() {
        SimpleSingle{ timestamp: 1, storage: null(), t: ()}
            .cmp(&SimpleSingle{ timestamp: 1, storage: null(), t: ()});
    }

    fn m_s2_eq() {
        Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&SimpleSingle{ timestamp: 1, storage: null(), t: ()});
    }

    #[test]
    fn got_max_m_s_cmp() {
        assert_eq!(Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&Single{ multi_timestamp: 1, single_num: 0, storage: null(), t: ()}),
            Greater
        );
        assert_eq!(Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&Single{ multi_timestamp: 2, single_num: 200, storage: null(), t: ()}),
            Greater
        );
        assert_eq!(Multi{ timestamp: 3, storage: multi_storage(), t: (), id: Uuid::nil()}
            .cmp(&Single{ multi_timestamp: 2, single_num: 0, storage: null(), t: ()}),
            Less
        );
    }

    #[test]
    fn got_max_s_m_cmp() {
        assert_eq!(Single{ multi_timestamp: 1, single_num: 0, storage: null(), t: ()}
            .cmp(&Multi{ timestamp: 1, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Less
        );
        assert_eq!(Single{ multi_timestamp: 1, single_num: 200,storage: null(), t: ()}
            .cmp(&Multi{ timestamp: 2, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Greater
        );
        assert_eq!(Single{ multi_timestamp: 3, single_num: 0,storage: null(), t: ()}
            .cmp(&Multi{ timestamp: 2, storage: multi_storage(), t: (), id: Uuid::nil()}),
            Less
        );
    }

    #[test]
    fn got_max_s_s_cmp() {
        assert_eq!(Single{ multi_timestamp: 1, single_num: 200, storage: null(), t: ()}
            .cmp(&Single{ multi_timestamp: 1, single_num: 200, storage: null(), t: ()}),
            Equal
        );
        assert_eq!(Single{ multi_timestamp: 1, single_num: 200, storage: null(), t: ()}
            .cmp(&Single{ multi_timestamp: 2, single_num: 1, storage: null(), t: ()}),
            Greater
        );
        assert_eq!(Single{ multi_timestamp: 3, single_num: 1, storage: null(), t: ()}
            .cmp(&Single{ multi_timestamp: 2, single_num: 200, storage: null(), t: ()}),
            Less
        );

        assert_eq!(Single{ multi_timestamp: 5, single_num: 200, storage: null(), t: ()}
            .cmp(&Single{ multi_timestamp: 5, single_num: 200, storage: null(), t: ()}),
            Equal
        );
        assert_eq!(Single{ multi_timestamp: 5, single_num: 10, storage: null(), t: ()}
            .cmp(&Single{ multi_timestamp: 5, single_num: 200, storage: null(), t: ()}),
            Greater
        );
        assert_eq!(Single{ multi_timestamp: 5, single_num: 200, storage: null(), t: ()}
            .cmp(&Single{ multi_timestamp: 5, single_num: 5, storage: null(), t: ()}),
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
        assert_eq!(skeen.next_timestamp, 1);
        skeen.set_max_timestamp(Uuid::nil(), 0);
        assert_eq!(skeen.next_timestamp, 1);
        let mut v = Vec::with_capacity(1);
        skeen.flush_got_max_timestamp(|g| {v.push(g); Some(Uuid::nil())});
        assert_eq!(&*v, &[Multi{timestamp: 0, id: Uuid::nil(), t: (), storage: s}]);
        assert_eq!(skeen.next_timestamp, 1);
    }

    #[test]
    fn one_multi1() {
        let mut skeen = SkeensState::new();
        //TODO add assert for fn res
        let s = multi_storage();
        skeen.add_multi_append(Uuid::nil(), s.clone(), ());
        assert_eq!(skeen.next_timestamp, 1);
        let r = skeen.set_max_timestamp(Uuid::nil(), 1223);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 1224);
        let mut v = Vec::with_capacity(1);
        skeen.flush_got_max_timestamp(|g| {v.push(g); Some(Uuid::nil())});
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
        assert_eq!(skeen.next_timestamp, 1);
        skeen.add_multi_append(id1, s1.clone(), ());
        assert_eq!(skeen.next_timestamp, 2);
        let r = skeen.set_max_timestamp(id1, 122);
        assert_eq!(r, SkeensSetMaxRes::Ok);
        assert_eq!(skeen.next_timestamp, 123);
        let mut v = Vec::with_capacity(2);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);
        let r = skeen.set_max_timestamp(id0, 3);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 123);
        let mut i = true;
        skeen.flush_got_max_timestamp(|g| {v.push(g);
            if i { i = false; Some(id0) } else { Some(id1) }
        });
        assert_eq!(&*v,
            &[Multi{timestamp: 3, id: id0, t: (), storage: s0},
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
        assert_eq!(skeen.next_timestamp, 1);
        skeen.add_multi_append(id1, s1.clone(), ());
        assert_eq!(skeen.next_timestamp, 2);
        let r = skeen.set_max_timestamp(id0, 122);
        assert_eq!(r, SkeensSetMaxRes::Ok);
        assert_eq!(skeen.next_timestamp, 123);
        let mut v = Vec::with_capacity(2);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);
        let r = skeen.set_max_timestamp(id1, 3);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 123);

        let mut i = true;
        skeen.flush_got_max_timestamp(|g| {v.push(g);
            if i { i = false; Some(id1) } else { Some(id0) }
        });

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
        assert_eq!(r, SkeensAppendRes::NewAppend(0));
        assert_eq!(skeen.next_timestamp, 1);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 3 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 0);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 3);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);
            match i {
                0 => { i += 1; Some(id0)},
                _ => None,
            }
        });

        assert_eq!(&*v,
            &[
                Multi{timestamp: 0, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 1, t: (), storage: 1 as *const _},
                SimpleSingle{timestamp: 2, t: (), storage: 3 as *const _}
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
        assert_eq!(r, SkeensAppendRes::NewAppend(0));
        assert_eq!(skeen.next_timestamp, 1);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 3 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 1);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 3);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);
            match i {
                0 => { i += 1; Some(id0)},
                _ => None,
            }
        });

        assert_eq!(&*v,
            &[
                Multi{timestamp: 1, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 1, t: (), storage: 1 as *const _},
                SimpleSingle{timestamp: 2, t: (), storage: 3 as *const _}
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
        assert_eq!(r, SkeensAppendRes::NewAppend(0));
        assert_eq!(skeen.next_timestamp, 1);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 3 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 2);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 3);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);
            match i {
                0 => { i += 1; None},
                1 => { i += 1; Some(id0)},
                _ => None,
            }
        });

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 1, t: (), storage: 1 as *const _},
                Multi{timestamp: 2, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 2, t: (), storage: 3 as *const _}
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
        assert_eq!(r, SkeensAppendRes::NewAppend(0));
        assert_eq!(skeen.next_timestamp, 1);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 3 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 2);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 3);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);
            match i {
                0 => { i += 1; None},
                1 => { i += 1; Some(id0)},
                _ => None,
            }
        });

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 1, t: (), storage: 1 as *const _},
                Multi{timestamp: 2, id: id0, t: (), storage: s0},
                SimpleSingle{timestamp: 2, t: (), storage: 3 as *const _},
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
        assert_eq!(r, SkeensAppendRes::NewAppend(0));
        assert_eq!(skeen.next_timestamp, 1);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id2, 3 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(2));
        assert_eq!(skeen.next_timestamp, 3);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 3);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 4);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);
            match i {
                0 => { i += 1; None},
                1 => { i += 1; None},
                _ => Some(id0),
            }
        });

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 1, t: (), storage: 1 as *const _},
                SimpleSingle{timestamp: 2, t: (), storage: 3 as *const _},
                Multi{timestamp: 3, id: id0, t: (), storage: s0},
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
        assert_eq!(r, SkeensAppendRes::NewAppend(0));
        assert_eq!(skeen.next_timestamp, 1);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.add_single_append(id1, 1 as *const _, ());
        assert_eq!(r, SkeensAppendRes::NewAppend(1));
        assert_eq!(skeen.next_timestamp, 2);
        skeen.flush_got_max_timestamp(|g| {v.push(g); None});
        assert_eq!(&*v, &[]);

        let r = skeen.set_max_timestamp(id0, 3);
        assert_eq!(r, SkeensSetMaxRes::NeedsFlush);
        assert_eq!(skeen.next_timestamp, 4);

        let mut i = 0;
        skeen.flush_got_max_timestamp(|g| {v.push(g);
            match i {
                0 => { i += 1; None},
                _ => Some(id0),
            }
        });

        assert_eq!(&*v,
            &[

                SimpleSingle{timestamp: 1, t: (), storage: 1 as *const _},
                Multi{timestamp: 3, id: id0, t: (), storage: s0},
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
