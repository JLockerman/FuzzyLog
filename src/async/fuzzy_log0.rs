
//TODO use faster HashMap, HashSet
use std::{self, mem};
use std::collections::{HashMap, HashSet, VecDeque};
use std::collections::hash_map;
use std::rc::Rc;
use std::sync::mpsc;
use std::u32;

use bit_set::BitSet;

use mio;

use packets::*;
use async::tcp::{AsyncStoreClient, AsyncTcpStore};
use self::FromStore::*;
use self::FromClient::*;

const MAX_PREFETCH: u32 = 8;

type ChainEntry = Rc<Vec<u8>>;

pub struct ThreadLog {
    to_store: mio::Sender<Vec<u8>>, //TODO send WriteState or other enum?
    from_outside: mpsc::Receiver<Message>, //TODO should this be per-chain?
    blockers: HashMap<OrderIndex, Vec<ChainEntry>>,
    per_chains: HashMap<order, PerChain>,
    //TODO replace with queue from deque to allow multiple consumers
    ready_reads: mpsc::Sender<Vec<u8>>,
    //TODO blocked_chains: BitSet ?
    finished_writes: Vec<mpsc::Sender<()>>,
    //TODO
    no_longer_blocked: Vec<OrderIndex>,
    cache: BufferCache,
}

struct PerChain {
    //TODO repr?
    //blocking: HashMap<entry, OrderIndex>,
    //read: VecDeque<ChainEntry>,
    //searching_for_multi_appends: HashMap<Uuid, OrderIndex>,
    //found_sentinels: HashSet<Uuid>,
    chain: order,
    last_snapshot: entry,
    last_read_sent_to_server: entry,
    //TODO is this necessary first_buffered: entry,
    last_returned_to_client: entry,
    blocked_on_new_snapshot: Option<Vec<u8>>,
    is_searching_for_multiappend: bool
}

pub enum Message {
    FromStore(FromStore),
    FromClient(FromClient),
}

//TODO hide in struct
pub enum FromStore {
    WriteComplete(Uuid, Vec<OrderIndex>), //TODO
    ReadComplete(Vec<u8>),
}

pub enum FromClient {
    //TODO
    SnapshotAndPrefetch(order),
}

struct BufferCache {
    //TODO vec_cache: VecDeque<Vec<u8>>,
    //     rc_cache: VecDeque<Rc<Vec<u8>>>,
    //     alloced: usize,
    //     avg_alloced: usize,
}


impl ThreadLog {

    //TODO
    pub fn new<I>(to_store: mio::Sender<Vec<u8>>,
        from_outside: mpsc::Receiver<Message>,
        ready_reads: mpsc::Sender<Vec<u8>>,
        interesting_chains: I)
    -> Self
    where I: IntoIterator<Item=order>{
        ThreadLog {
            to_store: to_store,
            from_outside: from_outside,
            blockers: Default::default(),
            ready_reads: ready_reads,
            finished_writes: Default::default(),
            per_chains: interesting_chains.into_iter().map(|c| (c, PerChain::new(c))).collect(),
            no_longer_blocked: Default::default(),
            cache: BufferCache::new(),
        }
    }

    pub fn run(mut self) -> ! {
        loop {
            let msg = self.from_outside.recv().expect("outside is gone");
            self.handle_message(msg)
        }
    }

    fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::FromClient(msg) => self.handle_from_client(msg),
            Message::FromStore(msg) => self.handle_from_store(msg),
        }
    }

    fn handle_from_client(&mut self, msg: FromClient) {
        match msg {
            SnapshotAndPrefetch(chain) => {
                self.fetch_snapshot(chain);
                self.prefetch(chain);
            }
        }
    }

    fn handle_from_store(&mut self, msg: FromStore) {
        match msg {
            WriteComplete(..) => unimplemented!(),
            ReadComplete(msg) => self.handle_completed_read(msg),
        }
    }

    fn fetch_snapshot(&mut self, chain: order) {
        let packet = self.make_read_packet(chain, u32::MAX.into());
        self.to_store.send(packet).expect("store hung up")
    }

    fn prefetch(&mut self, chain: order) {
        //TODO allow new chains?
        //TODO how much to fetch
        let num_to_fetch = {
            let pc = &self.per_chains[&chain];
            let num_to_fetch = pc.num_to_fetch();
            let num_to_fetch = std::cmp::max(num_to_fetch, MAX_PREFETCH);
            let currently_fetching = pc.currently_fetching();
            if currently_fetching < num_to_fetch { num_to_fetch - currently_fetching }
            else { 0 }
        };
        for _ in 0..num_to_fetch {
            self.fetch_next(chain)
        }
    }

    fn handle_completed_read(&mut self, msg: Vec<u8>) {
        //TODO right now this assumes order...
        let (kind, first_loc) = {
            let e = bytes_as_entry(&msg);
            (e.kind, e.locs()[0])
        };
        trace!("FUZZY handle read @ {:?}", first_loc);

        match kind.layout() {
            EntryLayout::Read => {
                trace!("FUZZY read has no data");
                debug_assert!(!kind.contains(EntryKind::ReadSuccess));
                if first_loc.1 < u32::MAX.into() {
                    trace!("FUZZY overread at {:?}", first_loc);
                    //TODO would be nice to handle ooo reads better...
                    //     we can probably do it by checking (chain, read_loc - 1)
                    //     to see if the read we're about to attempt is there, but
                    //     it might be better to switch to a buffer per-chain model
                    self.per_chains.get_mut(&first_loc.0).map(|s| {
                        s.overread_at(first_loc.1);
                    });
                }
                else {
                    let unblocked = self.per_chains.get_mut(&first_loc.0).and_then(|s| {
                        let e = bytes_as_entry(&msg);
                        assert_eq!(e.locs()[0].1, u32::MAX.into());
                        assert!(!e.kind.contains(EntryKind::ReadSuccess));
                        let new_horizon = e.dependencies()[0].1;
                        trace!("FUZZY try update horizon to {:?}", (first_loc.0, new_horizon));
                        s.update_horizon(first_loc.0, new_horizon)
                    });
                    if let Some(val) = unblocked {
                        let locs = self.return_entry(val);
                        if let Some(locs) = locs { self.stop_blocking_on_locs(locs) }
                    }
                }
                self.continue_fetch_if_needed(first_loc.0);
            }
            EntryLayout::Data => {
                trace!("FUZZY read is single");
                debug_assert!(kind.contains(EntryKind::ReadSuccess));
                //assert!(first_loc.1 >= pc.first_buffered);
                //TODO no-alloc?
                let packet = Rc::new(msg);
                //let will_block =
                self.add_blockers(first_loc, &packet);
                self.try_returning(first_loc, packet);
                self.continue_fetch_if_needed(first_loc.0);
            }
            EntryLayout::Multiput => {
                trace!("FUZZY read is multi");
                debug_assert!(kind.contains(EntryKind::ReadSuccess));
                //TODO add read_loc to that which is returned at the next layer
                //self.add_blockers(read_loc, &msg);
                //self.update_wait_for_ids(read_loc)
                //if ? then self.per_chains.get_mut(&first_loc.0)
                //    .expect("read uninteresting chain")
                //    .add_read_packet(first_loc, msg);?
                unimplemented!()
            }
            EntryLayout::Sentinel => unimplemented!(),

            EntryLayout::Lock => unreachable!(),
        }
    }

    fn add_blockers(&mut self, loc: OrderIndex, packet: &ChainEntry) -> bool {
        //FIXME dependencies currently assumes you gave it the correct type
        //      this is unnecessary and should be changed
        let deps = bytes_as_entry(packet).dependencies();
        let mut will_block = false;
        trace!("FUZZY checking {:?} for blockers in {:?}", loc, deps);
        for &(chain, index) in deps {
            let blocker_already_returned = self.per_chains.get_mut(&chain)
                .expect("read uninteresting chain")
                .has_returned(index);
            if !blocker_already_returned {
                trace!("FUZZY read @ {:?} blocked on {:?}", loc, (chain, index));
                let blocked = self.blockers.entry((chain, index)).or_insert_with(Vec::new);
                blocked.push(packet.clone());
                will_block = true;
            } else {
                trace!("FUZZY read @ {:?} need not wait for {:?}", loc, (chain, index));
            }
        }
        let is_next_in_chain = self.per_chains.get(&loc.0)
            .expect("fetching uninteresting chain")
            .next_return_is(loc.1);
        if !is_next_in_chain {
            self.enqueue_packet(loc, packet.clone());
            will_block = true;
        }
        will_block
    }

    fn fetch_blockers_if_needed(&mut self, packet: &ChainEntry) {
        //TODO num_to_fetch
        //FIXME only do if below last_snapshot
        let deps = bytes_as_entry(packet).dependencies();
        for &(chain, index) in deps {
            let unblocked;
            let num_to_fetch: u32 = {
                let pc = self.per_chains.get_mut(&chain)
                    .expect("tried reading uninteresting chain");
                unblocked = pc.update_horizon(chain, index);
                pc.num_to_fetch()
            };
            trace!("FUZZY blocker {:?} needs {:?} additional reads", chain, num_to_fetch);
            for _ in 0..num_to_fetch {
                self.fetch_next(chain)
            }
            if let Some(val) = unblocked {
                let locs = self.return_entry(val);
                if let Some(locs) = locs { self.stop_blocking_on_locs(locs) }
            }
        }
    }

    fn try_returning(&mut self, loc: OrderIndex, packet: ChainEntry) {
        match Rc::try_unwrap(packet) {
            Ok(e) => {
                trace!("FUZZY read {:?} is next", loc);
                if self.return_entry_at(loc, e) {
                    self.stop_blocking_on(loc);
                }
            }
            //TODO should this be in add_blockers?
            Err(e) => self.fetch_blockers_if_needed(&e),
        }
    }

    fn stop_blocking_on(&mut self, loc: OrderIndex) {
        trace!("FUZZY unblocking reads after {:?}", loc);
        self.try_return_blocked_by(loc);
        while let Some(loc) = self.no_longer_blocked.pop() {
            trace!("FUZZY continue unblocking reads after {:?}", loc);
            self.try_return_blocked_by(loc);
        }
    }

    fn stop_blocking_on_locs<I>(&mut self, locs: I)
    where I: IntoIterator<Item=OrderIndex> {
        for loc in locs {
            self.try_return_blocked_by(loc);
        }
        while let Some(loc) = self.no_longer_blocked.pop() {
            trace!("FUZZY continue unblocking reads after {:?}", loc);
            self.try_return_blocked_by(loc);
        }
    }

    fn try_return_blocked_by(&mut self, loc: OrderIndex) {
        //FIXME switch to using try_returning so needed fetches are done
        let blocked = self.blockers.remove(&loc);
        if let Some(blocked) = blocked {
            for blocked in blocked.into_iter() {
                match Rc::try_unwrap(blocked) {
                    Ok(val) => {
                        {
                            let locs = bytes_as_entry(&val).locs();
                            trace!("FUZZY {:?} unblocked by {:?}", locs, loc);
                            self.no_longer_blocked.extend_from_slice(locs);
                        }
                        self.return_entry(val);
                    }
                    Err(still_blocked) =>
                        trace!("FUZZY {:?} no longer by {:?} but still blocked",
                            bytes_as_entry(&still_blocked).locs(), loc),
                }
            }
        }
    }

    fn continue_fetch_if_needed(&mut self, chain: order) {
        //TODO num_to_fetch
        let num_to_fetch: u32 = self.per_chains[&chain].num_to_fetch();
        trace!("FUZZY {:?} needs {:?} additional reads", chain, num_to_fetch);
        for _ in 0..num_to_fetch {
            self.fetch_next(chain)
        }
    }

    fn enqueue_packet(&mut self, loc: OrderIndex, packet: ChainEntry) {
        assert!(loc.1 > 1.into());
        debug_assert!(self.per_chains.get(&loc.0).unwrap().last_returned_to_client < loc.1 - 1);
        let blocked_on = (loc.0, loc.1 - 1);
        trace!("FUZZY read @ {:?} blocked on prior {:?}", loc, blocked_on);
        let blocked = self.blockers.entry(blocked_on).or_insert_with(Vec::new);
        blocked.push(packet.clone());
    }

    fn return_entry_at(&mut self, loc: OrderIndex, val: Vec<u8>) -> bool {
        debug_assert!(bytes_as_entry(&val).locs()[0] == loc);
        debug_assert!(bytes_as_entry(&val).locs().len() == 1);
        trace!("FUZZY trying to return read @ {:?}", loc);
        let (o, i) = loc;
        {
            let pc = self.per_chains.get_mut(&o).expect("fetching uninteresting chain");
            if !pc.is_within_snapshot(i) {
                trace!("FUZZY blocking read @ {:?}, waiting for snapshot", loc);
                pc.block_on_snapshot(val);
                return false
            }

            trace!("QQQQQ setting returned {:?}", (o, i));
            pc.set_returned(i);
        };
        trace!("FUZZY returning read @ {:?}", loc);
        //FIXME first_buffered?
        self.ready_reads.send(val).expect("client hung up");
        true
    }

    fn return_entry(&mut self, val: Vec<u8>) -> Option<Vec<OrderIndex>> {
        let locs = {
            let mut should_block_on = None;
            {
                let locs = bytes_as_entry(&val).locs();
                trace!("FUZZY trying to return read from {:?}", locs);
                for &(o, i) in locs.into_iter() {
                    let pc = self.per_chains.get_mut(&o).expect("fetching uninteresting chain");
                    if !pc.is_within_snapshot(i) {
                        trace!("FUZZY must block read @ {:?}, waiting for snapshot", (o, i));
                        should_block_on = Some(o);
                    }
                }
            }
            if let Some(o) = should_block_on {
                let pc = self.per_chains.get_mut(&o).expect("fetching uninteresting chain");
                pc.block_on_snapshot(val);
                return None
            }
            let locs = bytes_as_entry(&val).locs();
            for &(o, i) in locs.into_iter() {
                trace!("QQQQ setting returned {:?}", (o, i));
                let pc = self.per_chains.get_mut(&o).expect("fetching uninteresting chain");
                debug_assert!(pc.is_within_snapshot(i));
                pc.set_returned(i);
            }
            //TODO no-alloc
            locs.to_vec()
        };
        trace!("FUZZY returning read @ {:?}", locs);
        //FIXME first_buffered?
        self.ready_reads.send(val).expect("client hung up");
        Some(locs)
    }

    fn fetch_next(&mut self, chain: order) {
        let next = {
            let per_chain = &mut self.per_chains.get_mut(&chain)
                .expect("fetching uninteresting chain");
            per_chain.last_read_sent_to_server = per_chain.last_read_sent_to_server + 1;
            per_chain.last_read_sent_to_server
        };
        let packet = self.make_read_packet(chain, next);
        self.to_store.send(packet).expect("store hung up")
    }

    fn make_read_packet(&mut self, chain: order, index: entry) -> Vec<u8> {
        let mut buffer = self.cache.alloc();
        {
            let e = EntryContents::Data(&(), &[]).fill_vec(&mut buffer);
            e.kind = EntryKind::Read;
            e.locs_mut()[0] = (chain, index);
        }
        buffer
    }
}

impl PerChain {
    fn new(chain: order) -> Self {
        PerChain {
            chain: chain,
            last_snapshot: 0.into(),
            last_read_sent_to_server: 0.into(),
            last_returned_to_client: 0.into(),
            blocked_on_new_snapshot: None,
            is_searching_for_multiappend: false,
        }
    }

    fn set_returned(&mut self, index: entry) {
        assert!(self.next_return_is(index));
        assert!(index > self.last_returned_to_client);
        trace!("QQQQQ returning {:?}", (self.chain, index));
        self.last_returned_to_client = index;
        //FIXME is unreachable with the blocking code
        if self.last_snapshot + 1 == index {
            trace!("QQQQQ set updating horizon to {:?}", (self.chain, index));
            self.last_snapshot = index
        }
        debug_assert!(self.last_returned_to_client == index);
    }

    fn overread_at(&mut self, index: entry) {
        // The conditional is needed because sends we sent before reseting
        // last_read_sent_to_server race future calls to this function
        if self.last_read_sent_to_server > index
            && self.last_read_sent_to_server > self.last_returned_to_client {
            trace!("FUZZY resetting read loc for {:?} from {:?} to {:?}",
                self.chain, self.last_read_sent_to_server, index);
            self.last_read_sent_to_server = index - 1
        }
    }

    fn can_return(&self, index: entry) -> bool {
        self.next_return_is(index) && self.is_within_snapshot(index)
    }

    fn has_returned(&mut self, index: entry) -> bool {
        trace!{"QQQQQ last return for {:?}: {:?}", self.chain, self.last_returned_to_client};
        index <= self.last_returned_to_client
    }

    fn next_return_is(&self, index: entry) -> bool {
        trace!("QQQQQ next return for {:?}: {:?}", self.chain, self.last_returned_to_client + 1);
        index == self.last_returned_to_client + 1
    }

    fn is_within_snapshot(&self, index: entry) -> bool {
        trace!("QQQQQ {:?} <= {:?}", index, self.last_snapshot);
        index <= self.last_snapshot
    }

    fn update_horizon(&mut self, chain: order, new_horizon: entry) -> Option<Vec<u8>> {
        if self.last_snapshot < new_horizon {
            trace!("FUZZY update horizon {:?}", (self.chain, new_horizon));
            self.last_snapshot = new_horizon;
            if entry_is_unblocked(&self.blocked_on_new_snapshot, chain, new_horizon) {
                trace!("FUZZY unblocked entry");
                return mem::replace(&mut self.blocked_on_new_snapshot, None)
            }
        }

        return None;

        fn entry_is_unblocked(val: &Option<Vec<u8>>, chain: order, new_horizon: entry) -> bool {
            val.as_ref().map_or(false, |v| {
                let locs = bytes_as_entry(v).locs();
                for &(o, i) in locs {
                    if o == chain && i <= new_horizon {
                        return true
                    }
                }
                false
            })
        }
    }

    fn block_on_snapshot(&mut self, val: Vec<u8>) {
        debug_assert!(bytes_as_entry(&val).locs().into_iter()
            .find(|&&(o, _)| o == self.chain).unwrap().1 == self.last_snapshot + 1);
        assert!(self.blocked_on_new_snapshot.is_none());
        self.blocked_on_new_snapshot = Some(val)
    }

    fn num_to_fetch(&self) -> u32 {
        //TODO switch to saturating sub?
        assert!(self.last_returned_to_client <= self.last_snapshot,
            "FUZZY returned value early. {:?} should be less than {:?}",
            self.last_returned_to_client, self.last_snapshot);
        if self.last_read_sent_to_server >= self.last_snapshot {
            //FIXME this should be based on the number of requests outstanding from the server
            //     only if the number of requests is zero do we read beyond the horizon
            if self.is_searching_for_multiappend { 1 } else { 0 }
        } else {
            let fetches_needed = self.last_snapshot - self.last_read_sent_to_server.into();
            fetches_needed.into()
        }

    }

    fn currently_fetching(&self) -> u32 {
        //TODO switch to saturating sub?
        let currently_fetching = self.last_read_sent_to_server
            - self.last_returned_to_client.into();
        let currently_fetching: u32 = currently_fetching.into();
        currently_fetching
    }
}

impl BufferCache {
    fn new() -> Self {
        BufferCache{}
    }

    fn alloc(&mut self) -> Vec<u8> {
        //TODO
        Vec::new()
    }
}

impl AsyncStoreClient for mpsc::Sender<Message> {
    fn on_finished_read(&mut self, read_packet: Vec<u8>) {
        self.send(Message::FromStore(ReadComplete(read_packet))).expect("store disabled")
    }

    //TODO what info is needed?
    fn on_finished_write(&mut self, write_id: Uuid, write_locs: Vec<OrderIndex>) {
        self.send(Message::FromStore(WriteComplete(write_id, write_locs)))
            .expect("store disabled")
    }
}

#[cfg(test)]
mod tests {
    use packets::*;
    use prelude::FuzzyLog;
    use super::*;
    use super::FromClient::*;
    use async::tcp::AsyncTcpStore;
    use async::tcp::sync_store_tests::AsyncStoreToStore;

    use std::collections::HashMap;
    use std::marker::PhantomData;
    use std::{mem, thread};
    use std::net::SocketAddr;
    use std::sync::{mpsc, Arc, Mutex};

    use mio::{self, EventLoop};

    use local_store::MapEntry;
    //TODO move to crate root under cfg...
    extern crate env_logger;


    /*TODO #[test]
    fn test_get_none() {
        let _ = env_logger::init();
        let _ = new_store(vec![]);
        let mut lh = new_thread_log::<i32>(vec![0.into()]);
        let n = lh.get_next();
        assert_eq!(n, None);
    }*/

    #[test]
    pub fn test_1_column() {
        let _ = env_logger::init();
        let store = new_store(
            vec![(3.into(), 1.into()), (3.into(), 2.into()),
            (3.into(), 3.into())]
        );
        let mut lh = new_thread_log::<i32>(vec![3.into()]);
        let mut log = FuzzyLog::new(store, HashMap::new(), Default::default());
        let _ = log.append(3.into(), &1, &[]);
        let _ = log.append(3.into(), &17, &[]);
        let _ = log.append(3.into(), &32, &[]);
        let _ = log.append(3.into(), &-1, &[]);
        lh.snapshot(3.into());
        assert_eq!(lh.get_next(), Some((&1,  &[(3.into(), 1.into())][..])));
        assert_eq!(lh.get_next(), Some((&17, &[(3.into(), 2.into())][..])));
        assert_eq!(lh.get_next(), Some((&32, &[(3.into(), 3.into())][..])));
        assert_eq!(lh.get_next(), Some((&-1, &[(3.into(), 4.into())][..])));
        //TODO assert_eq!(lh.play_foward(), None)
    }

    #[test]
    pub fn test_3_column() {
        let _ = env_logger::init();
        let store = new_store(vec![]);
        let mut lh = new_thread_log::<i32>(vec![4.into(), 5.into(), 6.into()]);
        let mut log = FuzzyLog::new(store, HashMap::new(), Default::default());
        let cols = vec![vec![12, 19, 30006, 122, 9],
            vec![45, 111111, -64, 102, -10101],
            vec![-1, -2, -9, 16, -108]];
        for (j, col) in cols.iter().enumerate() {
            for i in col.iter() {
                let _ = log.append(((j + 4) as u32).into(), i, &[]);
            }
        }
        lh.snapshot(4.into());
        lh.snapshot(6.into());
        lh.snapshot(5.into());
        let mut is = [0u32, 0, 0, 0];
        let total_len = cols.iter().fold(0, |len, col| len + col.len());
        for _ in 0..total_len {
            let next = lh.get_next();
            assert!(next.is_some());
            let (&n, ois) = next.unwrap();
            assert_eq!(ois.len(), 1);
            let (o, i) = ois[0];
            let off: u32 = (o - 4).into();
            is[off as usize] = is[off as usize] + 1;
            let i: u32 = i.into();
            assert_eq!(is[off as usize], i);
            let c = is[off as usize] - 1;
            assert_eq!(n, cols[off as usize][c as usize]);
        }
        //TODO assert_eq!(lh.play_foward(), None)
    }

    #[test]
    pub fn test_read_deps() {
        let _ = env_logger::init();
        let store = new_store(vec![]);
        let mut lh = new_thread_log::<i32>(vec![7.into(), 8.into()]);
        let mut log = FuzzyLog::new(store, HashMap::new(), Default::default());

        let _ = log.append(7.into(), &63,  &[]);
        let _ = log.append(8.into(), &-2,  &[(7.into(), 1.into())]);
        let _ = log.append(8.into(), &-56, &[]);
        let _ = log.append(7.into(), &111, &[(8.into(), 2.into())]);
        let _ = log.append(8.into(), &0,   &[(7.into(), 2.into())]);
        lh.snapshot(8.into());
        lh.snapshot(7.into());
        assert_eq!(lh.get_next(), Some((&63,  &[(7.into(), 1.into())][..])));
        assert_eq!(lh.get_next(), Some((&-2,  &[(8.into(), 1.into())][..])));
        assert_eq!(lh.get_next(), Some((&-56, &[(8.into(), 2.into())][..])));
        assert_eq!(lh.get_next(), Some((&111, &[(7.into(), 2.into())][..])));
        assert_eq!(lh.get_next(), Some((&0,   &[(8.into(), 3.into())][..])));
        //TODO assert_eq!(lh.play_foward(), None)
    }

    #[test]
    pub fn test_long() {
        let _ = env_logger::init();
        let store = new_store(vec![]);
        let mut lh = new_thread_log::<i32>(vec![9.into()]);
        let mut log = FuzzyLog::new(store, HashMap::new(), Default::default());
        for i in 0..19i32 {
            let _ = log.append(9.into(), &i, &[]);
        }
        lh.snapshot(9.into());
        for i in 0..19i32 {
            let u = i as u32;
            assert_eq!(lh.get_next(), Some((&i,  &[(9.into(), (u + 1).into())][..])));
        }
        //TODO assert_eq!(lh.play_foward(), None)
    }

    #[test]
    pub fn test_wide() {
        let _ = env_logger::init();
        let store = new_store(vec![]);
        let interesting_chains: Vec<_> = (10..21).map(|i| i.into()).collect();
        let mut lh = new_thread_log(interesting_chains.clone());
        let mut log = FuzzyLog::new(store, HashMap::new(), Default::default());
        for &i in &interesting_chains {
            if i > 10.into() {
                let _ = log.append(i.into(), &i, &[(i - 1, 1.into())]);
            }
            else {
                let _ = log.append(i.into(), &i, &[]);
            }

        }
        lh.snapshot(20.into());
        for &i in &interesting_chains {
            assert_eq!(lh.get_next(), Some((&i,  &[(i, 1.into())][..])));
        }
        //TODO assert_eq!(lh.play_foward(), None)
    }

    #[test]
    pub fn test_append_after_fetch() {
        let _ = env_logger::init();
        let store = new_store(vec![]);
        let mut lh = new_thread_log(vec![21.into()]);
        let mut log = FuzzyLog::new(store, HashMap::new(), Default::default());
        for i in 0u32..10 {
            let _ = log.append(21.into(), &i, &[]);
        }
        lh.snapshot(21.into());
        for i in 0u32..10 {
            assert_eq!(lh.get_next(), Some((&i,  &[(21.into(), (i + 1).into())][..])));
        }
        //TODO assert_eq!(lh.play_foward(), None)
        for i in 10u32..21 {
            let _ = log.append(21.into(), &i, &[]);
        }
        lh.snapshot(21.into());
        for i in 10u32..21 {
            assert_eq!(lh.get_next(), Some((&i,  &[(21.into(), (i + 1).into())][..])));
        }
        //TODO assert_eq!(lh.play_foward(), None)
    }

    #[test]
    pub fn test_append_after_fetch_short() {
        let _ = env_logger::init();
        let store = new_store(vec![]);
        let mut lh = new_thread_log(vec![22.into()]);
        let mut log = FuzzyLog::new(store, HashMap::new(), Default::default());
        for i in 0u32..2 {
            let _ = log.append(22.into(), &i, &[]);
        }
        lh.snapshot(22.into());
        for i in 0u32..2 {
            assert_eq!(lh.get_next(), Some((&i,  &[(22.into(), (i + 1).into())][..])));
        }
        //TODO assert_eq!(lh.play_foward(), None)
        for i in 2u32..4 {
            let _ = log.append(22.into(), &i, &[]);
        }
        lh.snapshot(22.into());
        for i in 2u32..4 {
            assert_eq!(lh.get_next(), Some((&i,  &[(22.into(), (i + 1).into())][..])));
        }
        //TODO assert_eq!(lh.play_foward(), None)
    }

    /*

    #[test]
    fn test_1_column_ni() {
        let _ = env_logger::init();
        let store = new_store(
            vec![(4.into(), 1.into()), (4.into(), 2.into()),
                (4.into(), 3.into()), (5.into(), 1.into())]
        );
        let horizon = HashMap::new();
        let map = Rc::new(RefCell::new(HashMap::new()));
        let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = HashMap::new();
        let re = map.clone();
        upcalls.insert(4.into(), Box::new(move |_, _, &MapEntry(k, v)| {
            re.borrow_mut().insert(k, v);
            true
        }));
        upcalls.insert(5.into(), Box::new(|_, _, _| false));

        let mut log = FuzzyLog::new(store, horizon, upcalls);
        let e1 = log.append(4.into(), &MapEntry(0, 1), &*vec![]);
        assert_eq!(e1, (4.into(), 1.into()));
        let e2 = log.append(4.into(), &MapEntry(1, 17), &*vec![]);
        assert_eq!(e2, (4.into(), 2.into()));
        let last_index = log.append(4.into(), &MapEntry(32, 5), &*vec![]);
        assert_eq!(last_index, (4.into(), 3.into()));
        let en = log.append(5.into(), &MapEntry(0, 0), &*vec![last_index]);
        assert_eq!(en, (5.into(), 1.into()));
        log.play_foward(4.into());
        assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
    }

    #[test]
    fn test_deps() {
        let _ = env_logger::init();
        let store = new_store(
            vec![(6.into(), 1.into()), (6.into(), 2.into()),
                (6.into(), 3.into()), (7.into(), 1.into())]
        );
        let horizon = HashMap::new();
        let map = Rc::new(RefCell::new(HashMap::new()));
        let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = HashMap::new();
        let re = map.clone();
        upcalls.insert(6.into(), Box::new(move |_, _, &MapEntry(k, v)| {
            re.borrow_mut().insert(k, v);
            true
        }));
        upcalls.insert(7.into(), Box::new(|_, _, _| false));
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        let e1 = log.append(6.into(), &MapEntry(0, 1), &*vec![]);
        assert_eq!(e1, (6.into(), 1.into()));
        let e2 = log.append(6.into(), &MapEntry(1, 17), &*vec![]);
        assert_eq!(e2, (6.into(), 2.into()));
        let last_index = log.append(6.into(), &MapEntry(32, 5), &*vec![]);
        assert_eq!(last_index, (6.into(), 3.into()));
        let en = log.append(7.into(), &MapEntry(0, 0), &*vec![last_index]);
        assert_eq!(en, (7.into(), 1.into()));
        log.play_foward(7.into());
        assert_eq!(*map.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
    }

    #[test]
    fn test_order() {
        let _ = env_logger::init();
        let store = new_store(
            (0..5).map(|i| (20.into(), i.into()))
                .chain((0..21).map(|i| (21.into(), i.into())))
                .chain((0..22).map(|i| (22.into(), i.into())))
                .collect());
        let horizon = HashMap::new();
        let list: Rc<RefCell<Vec<i32>>> = Default::default();
        let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = Default::default();
        for i in 20..23 {
            let l = list.clone();
            upcalls.insert(i.into(), Box::new(move |_,_,&v| { l.borrow_mut().push(v);
                true
            }));
        }
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        log.append(22.into(), &4, &[]);
        log.append(20.into(), &2, &[]);
        log.append(21.into(), &3, &[]);
        log.multiappend(&[20.into(),21.into(),22.into()], &-1, &[]);
        log.play_foward(20.into());
        assert_eq!(&**list.borrow(), &[2,3,4,-1,-1,-1][..]);
    }

    #[test]
    fn test_dorder() {
        let _ = env_logger::init();
        let store = new_store(
            (0..5).map(|i| (23.into(), i.into()))
                .chain((0..5).map(|i| (24.into(), i.into())))
                .chain((0..5).map(|i| (25.into(), i.into())))
                .collect());
        let horizon = HashMap::new();
        let list: Rc<RefCell<Vec<i32>>> = Default::default();
        let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r _) -> bool>> = Default::default();
        for i in 23..26 {
            let l = list.clone();
            upcalls.insert(i.into(), Box::new(move |_,_,&v| { l.borrow_mut().push(v);
                true
            }));
        }
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        log.append(24.into(), &4, &[]);
        log.append(23.into(), &2, &[]);
        log.append(25.into(), &3, &[]);
        log.dependent_multiappend(&[23.into()], &[24.into(),25.into()], &-1, &[]);
        log.play_foward(23.into());
        assert_eq!(&**list.borrow(), &[2,4,3,-1,][..]);
    }*/



    struct LogHandle<V> {
            _pd: PhantomData<V>,
        to_log: mpsc::Sender<Message>,
        ready_reads: mpsc::Receiver<Vec<u8>>,
        //TODO finished_writes: ..
        curr_entry: Vec<u8>,
    }

    impl<V> LogHandle<V>
    where V: Storeable {
        fn snapshot(&mut self, chain: order) {
            self.to_log.send(Message::FromClient(SnapshotAndPrefetch(chain)))
                .unwrap();
        }

        fn get_next(&mut self) -> Option<(&V, &[OrderIndex])> {
            //TODO use recv_timeout in real version
            self.curr_entry = self.ready_reads.recv().unwrap();
            if self.curr_entry.len() == 0 {
                return None
            }

            let (val, locs, _) = Entry::<V>::wrap_bytes(&self.curr_entry).val_locs_and_deps();
            Some((val, locs))
        }
    }

    #[allow(non_upper_case_globals)]
    const lock_str: &'static str = "0.0.0.0:13389";
    #[allow(non_upper_case_globals)]
    const addr_strs: &'static [&'static str] = &["0.0.0.0:13390", "0.0.0.0:13391"];

    fn new_thread_log<V>(interesting_chains: Vec<order>) -> LogHandle<V> {
        let to_store_m = Arc::new(Mutex::new(None));
        let tsm = to_store_m.clone();
        let (to_log, from_outside) = mpsc::channel();
        let client = to_log.clone();
        let (ready_reads_s, ready_reads_r) = mpsc::channel();
        thread::spawn(move || {
            let mut event_loop = EventLoop::new().unwrap();
            let to_store = event_loop.channel();
            *tsm.lock().unwrap() = Some(to_store);
            let mut store = AsyncTcpStore::new(lock_str.parse().unwrap(),
                addr_strs.into_iter().map(|s| s.parse().unwrap()),
                client, &mut event_loop).expect("");
                event_loop.run(&mut store).expect("should never return");
        });
        let to_store;
        loop {
            let ts = mem::replace(&mut *to_store_m.lock().unwrap(), None);
            if let Some(s) = ts {
                to_store = s;
                break
            }
        }
        thread::spawn(move || {
            let log = ThreadLog::new(to_store, from_outside, ready_reads_s,
                interesting_chains.into_iter());
            log.run()
        });

        LogHandle {
            to_log: to_log,
            ready_reads: ready_reads_r,
            _pd: Default::default(),
            curr_entry: Default::default()
        }
    }


    fn new_store(_: Vec<OrderIndex>) -> AsyncStoreToStore
    {
        use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
        use std::{thread, iter};

        use servers::tcp::Server;

        static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

        for (i, &addr_str) in iter::once(&lock_str).chain(addr_strs.iter()).enumerate() {
            let handle = thread::spawn(move || {

                let addr = addr_str.parse().expect("invalid inet address");
                let mut event_loop = EventLoop::new().unwrap();
                let server = if i == 0 {
                    Server::new(&addr, 0, 1, &mut event_loop)
                }
                else {
                    Server::new(&addr, i as u32 -1, addr_strs.len() as u32,
                        &mut event_loop)
                };
                if let Ok(mut server) = server {
                    SERVERS_READY.fetch_add(1, Ordering::Release);
                    trace!("starting server");
                    event_loop.run(&mut server);
                }
                trace!("server already started");
                return;
            });
            mem::forget(handle);
        }

        while SERVERS_READY.load(Ordering::Acquire) < addr_strs.len() + 1 {}

        let lock_addr = lock_str.parse::<SocketAddr>().unwrap();
        let chain_addrs = addr_strs.into_iter().map(|s| s.parse::<SocketAddr>().unwrap());
        let store = AsyncStoreToStore::new(lock_addr, chain_addrs,
            EventLoop::new().unwrap());
        store
    }
}
