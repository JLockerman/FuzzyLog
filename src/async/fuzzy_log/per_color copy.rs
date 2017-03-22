
use std::mem;
use std::rc::Rc;

use hash::HashMap;
use packets::{order, entry, OrderIndex, bytes_as_entry};

use uuid::Uuid;

use super::range_tree::RangeTree;

//TODO we could add messages from the client on read, and keep a counter of messages sent
//     this would allow us to ensure that every client gets an end-of-data message, as long
//     ad there're no concurrent snapshots...
pub struct PerColor {
    //TODO repr?
    //blocking: HashMap<entry, OrderIndex>,
    //read: VecDeque<ChainEntry>,
    //searching_for_multi_appends: HashMap<Uuid, OrderIndex>,
    //found_sentinels: HashSet<Uuid>,
    pub chain: order,
    last_snapshot: entry,
    last_returned_to_client: entry,
    gotten_from_server: RangeTree,
    outstanding_reads: RangeTree,
    blocked_on_new_snapshot: Option<Vec<u8>>,
    //TODO this is where is might be nice to have a more structured id format
    found_but_unused_multiappends: HashMap<Uuid, entry>,
    is_being_read: Option<ReadState>,
    pub is_interesting: bool,
}

#[derive(Debug)]
struct ReadState {
    outstanding_snapshots: u32,
    num_multiappends_searching_for: u32,
    _being_read: IsRead,
}

impl ReadState {
    fn new(being_read: &IsRead) -> Self {
        ReadState {
            //outstanding_reads: 0,
            outstanding_snapshots: 0,
            num_multiappends_searching_for: 0,
            _being_read: being_read.clone(),
        }
    }

    fn is_finished(&self) -> bool {
        //self.outstanding_reads == 0
        self.outstanding_snapshots == 0
        && self.num_multiappends_searching_for == 0
    }
}

pub type IsRead = Rc<ReadHandle>;

#[derive(Debug)]
pub struct ReadHandle;

impl PerColor {
    pub fn new(chain: order) -> Self {
        PerColor {
            chain: chain,
            last_snapshot: 0.into(),
            //outstanding_reads: 0,
            last_returned_to_client: 0.into(),
            gotten_from_server: Default::default(),
            blocked_on_new_snapshot: None,
            found_but_unused_multiappends: Default::default(),
            outstanding_reads: Default::default(),
            is_being_read: None,
            is_interesting: false,
        }
    }

    pub fn interesting(chain: order) -> Self {
        let mut s = Self::new(chain);
        s.is_interesting = true;
        s
    }

    pub fn set_finished_reading(&mut self) {
        //FIXME should be unneeded
        assert!(
            self.is_being_read.as_ref().map(|r| r.is_finished()).unwrap_or(true),
            "Unfinished @ {:?}:{:?}",
            self.chain, self.is_being_read
        );
        self.is_being_read = None
    }

    #[inline(always)]
    pub fn set_returned(&mut self, index: entry) {
        assert!(self.next_return_is(index));
        assert!(index > self.last_returned_to_client);
        assert!(index <= self.last_snapshot);
        trace!("QQQQQ returning {:?}", (self.chain, index));
        self.last_returned_to_client = index;
        if self.is_finished() {
            trace!("QQQQQ {:?} is finished", self.chain);
            self.is_being_read = None
        }
    }

    pub fn overread_at(&mut self, index: entry) {
        // The conditional is needed because sends we sent before reseting
        // last_read_sent_to_server race future calls to this function
        trace!("FUZZY overread {:?}, {:?} >= {:?} && > {:?}",
            self.chain, self.outstanding_reads, index, self.last_returned_to_client);
        let was_waiting = self.outstanding_reads.remove_point(index);
        if was_waiting {
            trace!("FUZZY resetting read loc for {:?} from {:?} to {:?}",
                self.chain, self.outstanding_reads, index - 1);
        }
    }

    pub fn increment_outstanding_snapshots(&mut self, is_being_read: &IsRead) -> u32 {
        let out = match &mut self.is_being_read {
            &mut Some(ReadState {ref mut outstanding_snapshots, ..} ) => {
                //TODO saturating arith
                *outstanding_snapshots = *outstanding_snapshots + 1;
                *outstanding_snapshots
            }
            r @ &mut None => {
                let mut read_state = ReadState::new(is_being_read);
                read_state.outstanding_snapshots += 1;
                let outstanding_snapshots = read_state.outstanding_snapshots;
                *r = Some(read_state);
                outstanding_snapshots

            }
        };
        debug_assert!(self.is_being_read.is_some());
        out
    }

    pub fn decrement_outstanding_snapshots(&mut self) -> u32 {
        let snap = self.is_being_read.as_mut().map(|r|{
            //TODO saturating arith
            r.outstanding_snapshots = r.outstanding_snapshots - 1;
            r.outstanding_snapshots
            //TODO should this set is_being_read to None when last_returned == last snap?
        }).expect("tried to decrement snapshots on a chain not being read");
        if self.is_finished() {
            self.is_being_read = None
        }
        snap
    }

    pub fn increment_multi_search(&mut self, is_being_read: &IsRead) {
        let searching = match &mut self.is_being_read {
            &mut Some(ReadState {ref mut num_multiappends_searching_for, ..} ) => {
                //TODO saturating arith
                *num_multiappends_searching_for = *num_multiappends_searching_for + 1;
                *num_multiappends_searching_for
            }
            r @ &mut None => {
                let mut read_state = ReadState::new(is_being_read);
                read_state.num_multiappends_searching_for += 1;
                let num_multiappends_searching_for = read_state.num_multiappends_searching_for;
                *r = Some(read_state);
                num_multiappends_searching_for
            }
        };
        trace!("QQQQQ {:?} + now searching for {:?} multis", self.chain, searching);
    }

    pub fn decrement_multi_search(&mut self) {
        let num_search = self.is_being_read.as_mut().map(|r| {
            debug_assert!(r.num_multiappends_searching_for > 0);
            //TODO saturating arith
            r.num_multiappends_searching_for = r.num_multiappends_searching_for - 1;
            r.num_multiappends_searching_for
            //TODO should this set is_being_read to None when last_returned == last snap?
        }).expect("tried to decrement multi_search in a chain not being read");
        trace!("QQQQQ {:?} - now searching for {:?} multis",
            self.chain, num_search);
        if self.is_finished() {
            self.is_being_read = None
        }
    }

    #[inline(always)]
    pub fn got_read(&mut self, index: entry) -> bool {
        self.outstanding_reads.remove_point(index);
        self.gotten_from_server.add_point(index);

        index > self.last_returned_to_client
    }

    pub fn has_read_state(&self) -> bool {
        self.is_being_read.is_some()
    }

    #[allow(dead_code)]
    fn can_return(&self, index: entry) -> bool {
        self.next_return_is(index) && self.is_within_snapshot(index)
    }

    pub fn has_returned(&self, index: entry) -> bool {
        trace!{"QQQQQ last return for {:?}: {:?}", self.chain, self.last_returned_to_client};
        index <= self.last_returned_to_client
    }

    pub fn next_return_is(&self, index: entry) -> bool {
        trace!("QQQQQ check {:?} next return for {:?}: {:?}",
            index, self.chain, self.last_returned_to_client + 1);
        index == self.last_returned_to_client + 1
    }

    pub fn is_within_snapshot(&self, index: entry) -> bool {
        trace!("QQQQQ {:?}: {:?} <= {:?}", self.chain, index, self.last_snapshot);
        index <= self.last_snapshot
    }

    fn is_next_to_fetch(&self, index: entry) -> bool {
        self.last_read_sent_to_server + 1 == index
    }

    pub fn is_searching_for_multi(&self) -> bool {
        self.is_being_read.as_ref().map(|br|
            br.num_multiappends_searching_for > 0).unwrap_or(false)
    }

    pub fn mark_as_already_fetched(&mut self, index: entry) {
        self.gotten_from_server.add_point(index);
    }

    pub fn increment_fetch(&mut self, is_being_read: &IsRead) -> entry {
        //TODO assert horizon?
        self.last_read_sent_to_server = self.last_read_sent_to_server + 1;
        self.increment_outstanding_reads(is_being_read);
        //FIXME assure is_being_read
        self.last_read_sent_to_server
    }

    pub fn increment_horizon(&mut self) -> Option<Vec<u8>> {
        let new_horizon = self.last_snapshot + 1;
        self.update_horizon(new_horizon)
    }

    pub fn give_new_snapshot(&mut self, new_horizon: entry) -> Option<Vec<u8>> {
        self.decrement_outstanding_snapshots();
        if self.last_snapshot >= new_horizon {
            // If we're not searching for any multiappends,
            // and we have no pending snapshots,
            // then any possible read we receive would be an overread,
            // so there is no reason to wait for them
            if self.is_being_read.as_ref().map_or(false, |r| r.num_multiappends_searching_for == 0 && r.outstanding_snapshots == 0) {
                trace!("FUZZY {:?} dropping irrelevant reads", self.chain);
                self.is_being_read = None
            }
            trace!("FUZZY stale horizon update for {:?}: {:?} <= {:?}",
                self.chain, new_horizon, self.last_snapshot);
            return None
        }

        self.update_horizon(new_horizon)
    }

    pub fn update_horizon(&mut self, new_horizon: entry) -> Option<Vec<u8>> {
        if self.last_snapshot < new_horizon {
            trace!("FUZZY update horizon {:?}", (self.chain, new_horizon));
            self.last_snapshot = new_horizon;
            if self.last_read_sent_to_server > new_horizon {
                //see also fn overread_at
                self.last_read_sent_to_server = new_horizon;
            }
            if entry_is_unblocked(&self.blocked_on_new_snapshot, self.chain, new_horizon) {
                trace!("FUZZY unblocked entry");
                return mem::replace(&mut self.blocked_on_new_snapshot, None)
            }
        }
        else {
            trace!("FUZZY needless horizon update for {:?}: {:?} <= {:?}",
                self.chain, new_horizon, self.last_snapshot);
        }

        return None;

        fn entry_is_unblocked(val: &Option<Vec<u8>>, chain: order, new_horizon: entry) -> bool {
            val.as_ref().map_or(false, |v| {
                let locs = bytes_as_entry(v).locs();
                for &OrderIndex(o, i) in locs {
                    if o == chain && i <= new_horizon {
                        return true
                    }
                }
                false
            })
        }
    }

    pub fn block_on_snapshot(&mut self, val: Vec<u8>) {
        debug_assert!(bytes_as_entry(&val).locs().into_iter()
            .find(|&&OrderIndex(o, _)| o == self.chain).unwrap().1 == self.last_snapshot + 1);
        assert!(self.blocked_on_new_snapshot.is_none());
        self.blocked_on_new_snapshot = Some(val)
    }

    pub fn num_to_fetch(&self) -> u32 {
        use std::cmp::min;
        //TODO this decision needs to be made at the store level for this to really work
        //     a better solutions is the log tells store what range of entries it wants
        //     the store trys to send a full buffer's worth of read requests,
        //     and stores the ranges of reads it has sent.
        //     upon _receipt_ of a read response the _store_ trys to alloc a buffer
        //     from the buffer cache and forwards the read to log
        //FIXME the longer the allowed pipeline depth the better throughput,
        //      but the more memory is wasted, anyway current scheme is suboptimal,
        //      see TODO above
        const MAX_PIPELINED: u32 = 20000;
        //TODO switch to saturating sub?
        assert!(self.last_returned_to_client <= self.last_snapshot,
            "FUZZY returned value early. {:?} should be less than {:?}",
            self.last_returned_to_client, self.last_snapshot);
        let outstanding_reads = self.is_being_read.as_ref()
            .map(|r| r.outstanding_reads).unwrap_or(0);
        if self.last_read_sent_to_server < self.last_snapshot
            && outstanding_reads < MAX_PIPELINED {
            let needed_reads =
                (self.last_snapshot - self.last_read_sent_to_server.into()).into();
            let to_read = min(needed_reads, MAX_PIPELINED - outstanding_reads);
            to_read
            //std::cmp::min(
            //    (self.last_snapshot - self.last_read_sent_to_server.into()).into(),
            //    MAX_PIPELINED
            //)
        } else {
            0
        }
    }

    pub fn has_more_multi_search_than_outstanding_reads(&self) -> bool {
        let outstanding_reads = self.outstanding_reads.num_points();
        self.is_being_read.as_ref().map_or(false, |r| {
            debug_assert!(!(r.num_multiappends_searching_for > outstanding_reads + 1));
            r.num_multiappends_searching_for > outstanding_reads
        })
    }

    pub fn currently_buffering(&self) -> u32 {
        //TODO switch to saturating sub?
        let currently_buffering = self.last_read_sent_to_server
            - self.last_returned_to_client.into();
        let currently_buffering: u32 = currently_buffering.into();
        currently_buffering
    }

    pub fn add_early_sentinel(&mut self, id: Uuid, index: entry) {
        assert!(index != 0.into());
        let _old = self.found_but_unused_multiappends.insert(id, index);
        //TODO I'm not sure this is correct with how we handle overreads
        //debug_assert!(_old.is_none(),
        //    "double sentinel insert {:?}",
        //    (self.chain, index)
        //);
    }

    pub fn take_early_sentinel(&mut self, id: &Uuid) -> Option<entry> {
        self.found_but_unused_multiappends.remove(id)
    }

    pub fn has_outstanding(&self) -> bool {
        //self.is_being_read.is_some()
        self.has_read_state()
    }

    pub fn has_outstanding_reads(&self) -> bool {
        self.outstanding_reads.num_ranges() > 0
    }

    pub fn has_pending_reads_reqs(&self) -> bool {
        self.outstanding_reads.first_point().map(|p| p < self.last_snapshot)
            .unwrap_or(false)
    }

    fn has_outstanding_snapshots(&self) -> bool {
        self.is_being_read.as_ref().map(|&ReadState {outstanding_snapshots, ..}|
            outstanding_snapshots > 0).unwrap_or(false)
    }

    pub fn finished_until_snapshot(&self) -> bool {
        self.last_returned_to_client == self.last_snapshot
            && !self.has_outstanding_snapshots()
    }

    pub fn is_finished(&self) -> bool {
        debug_assert!(!(self.outstanding_reads.num_ranges() == 0
            && self.last_read_sent_to_server < self.last_snapshot),
            "outstanding_reads {:?}, last_snapshot {:?}",
            self.outstanding_reads, self.last_snapshot,
        );
        self.finished_until_snapshot()
            && !(self.is_searching_for_multi() || self.has_outstanding_snapshots())
    }

    pub fn trace_unfinished(&self) {
        trace!("chain {:?} finished? {:?}, last_ret {:?}, last_snap {:?}",
            self.chain,
            self.is_being_read,
            self.last_returned_to_client,
            self.last_snapshot
        )
    }
}
