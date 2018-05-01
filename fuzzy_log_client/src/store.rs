
//TODO ensure that backpressure is off
//     implement receiver
//     skeens2


use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::mem;
use std::net::SocketAddr;
use std::rc::Rc;

use packets::*;
use packets::buffer2::Buffer;

use hash::{HashMap, HashSet, UuidHashMap};
//use servers2::spsc;
use fuzzy_log_util::socket_addr::Ipv4SocketAddr;

pub use mio;
use mio::tcp::*;
use mio::Token;

use reactor::*;

pub trait AsyncStoreClient {
    //TODO nocopy?
    fn on_finished_read(&mut self, read_loc: OrderIndex, read_packet: Vec<u8>) -> Result<(), ()>;
    //TODO what info is needed?
    fn on_finished_write(&mut self, write_id: Uuid, write_locs: Vec<OrderIndex>) -> Result<(), ()>;

    fn on_io_error(&mut self, err: io::Error, server: usize) -> Result<(), ()>;

    //TODO fn should_shutdown(&mut self) -> bool { false }
}

pub type FromClient =  mio::channel::Receiver<Vec<u8>>;
pub type ToSelf =  mio::channel::Sender<Vec<u8>>;
fn channel() -> (ToSelf, FromClient) {
    mio::channel::channel()
}

pub struct AsyncTcpStore<C: AsyncStoreClient> {
    reactor: Reactor<PerStream, StoreInner<C>>,
}

struct StoreInner<C: AsyncStoreClient> {
    sent_writes: UuidHashMap<WriteState>,
    //sent_reads: HashMap<OrderIndex, Vec<u8>>,
    sent_reads: HashMap<OrderIndex, u16>,
    waiting_buffers: VecDeque<Vec<u8>>,
    max_timestamp_seen: HashMap<order, u64>,
    num_chain_servers: usize,
    //FIXME change to spsc::Receiver<Buffer?>
    from_client: FromClient,
    client: C,
    is_unreplicated: bool,
    new_multi: bool,
    reads_my_writes: bool,
    finished: bool,

    print_data: StorePrintData,

    receiver: Ipv4SocketAddr,

    pending_skeens2: VecDeque<SK2Send>,
}

counters!{
    struct StorePrintData {
        from_client: u64,
        finished_recvs: u64,
        finished_sends: u64,
        being_sent: u64,
    }
}

type PerStream = TcpHandler<PacketReader, PacketHandler>;

counters!{
    struct PerServerPrintData {
        finished_appends: u64,
        finished_read: u64,
        redo: u64,
        packets_recvd: u64,
        bytes_recvd: u64,
        bytes_sent: u64,
        packets_sending: u64,
    }
}

#[derive(Debug)]
pub enum WriteState {
    SingleServer(Vec<u8>),
    Skeens1(
        Rc<RefCell<Vec<u8>>>,
        Rc<RefCell<HashSet<usize>>>,
        Rc<RefCell<Box<[u64]>>>,
        bool,
    ),
    Skeens2(
        Rc<RefCell<Vec<u8>>>,
        Rc<RefCell<HashSet<usize>>>,
        u64,
    ),
    SnapshotSkeens1(
        Rc<RefCell<Vec<u8>>>,
        Rc<RefCell<HashSet<usize>>>,
        Rc<RefCell<Box<[u64]>>>,
    ),
    SnapshotSkeens2(
        Rc<RefCell<Vec<u8>>>,
        Rc<RefCell<HashSet<usize>>>,
        u64,
    ),
    GC(Vec<u8>),
}

struct SK2Send {
    buf: Rc<RefCell<Vec<u8>>>,
    max_ts: u64,
    servers: Vec<usize>,
    is_snapshot: bool,
}

//TODO rename to AsyncStore
impl<C> AsyncTcpStore<C>
where C: AsyncStoreClient {

    //TODO should probably move all the poll register stuff too run
    pub fn tcp<I>(
        _lock_server: SocketAddr,
        _chain_servers: I,
        _client: C,
        _event_loop: &mut mio::Poll)
    -> Result<(Self, ToSelf), io::Error>
    where I: IntoIterator<Item=SocketAddr> {
        unimplemented!()
    }

    pub fn new_tcp<I>(
        id: Ipv4SocketAddr,
        chain_servers: I,
        client: C,
    ) -> Result<(Self, ToSelf), io::Error>
    where I: IntoIterator<Item=SocketAddr> {
        let servers: Vec<_> = chain_servers.into_iter()
            .map(Self::connect)
            .collect::<Result<_, _>>()?;
        let num_chain_servers = servers.len();
        Self::build(id, servers, num_chain_servers, client, true)
    }

    pub fn replicated_new_tcp<I>(
        id: Ipv4SocketAddr,
        chain_servers: I,
        client: C,
    ) -> Result<(Self, ToSelf), io::Error>
    where I: IntoIterator<Item=(SocketAddr, SocketAddr)> {
        let (write_servers, read_servers): (Vec<_>, Vec<_>) =
            chain_servers.into_iter().inspect(|addrs| trace!("{:?}", addrs)).unzip();

        let num_chain_servers = write_servers.len();
        let servers = write_servers
            .into_iter()
            .chain(read_servers.into_iter())
            .map(Self::connect)
            .collect::<Result<_, _>>()?;
        Self::build(id, servers, num_chain_servers, client, false)
    }

    pub fn replicated_tcp<I>(
        _lock_server: Option<SocketAddr>,
        _chain_servers: I,
        _client: C,
        _event_loop: &mut mio::Poll
    ) -> Result<(Self, ToSelf), io::Error>
    where I: IntoIterator<Item=(SocketAddr, SocketAddr)>
    {
        unimplemented!()
    }

    fn build(
        id: Ipv4SocketAddr,
        mut servers: Vec<TcpStream>,
        num_chain_servers: usize,
        client: C,
        is_unreplicated: bool
    ) -> Result<(Self, ToSelf), io::Error> {
        assert!(num_chain_servers <= servers.len());
        trace!("Client {:?} servers", num_chain_servers);
        {
            for stream in servers.iter_mut().rev() {
                blocking_read(stream, &mut [0]).unwrap();
                blocking_write(stream, &[2]).unwrap();
                blocking_write(stream, id.bytes()).unwrap();
            }

            let mut ack = [0; 16];
            for stream in servers.iter_mut().rev() {
                blocking_read(stream, &mut ack[..]).unwrap();
                assert_eq!(Ipv4SocketAddr::from_bytes(ack), id);
            }
        }

        let (to_store, from_client) = channel();
        let from_client_token = Token(servers.len() + 1_000);
        let mut reactor = Reactor::with_inner(from_client_token.into(), StoreInner {
            sent_writes: Default::default(),
            sent_reads: Default::default(),
            waiting_buffers: Default::default(),
            num_chain_servers,
            client,
            from_client,
            is_unreplicated,
            new_multi: true,
            finished: false,
            reads_my_writes: false,

            max_timestamp_seen: Default::default(),
            pending_skeens2: Default::default(),
            receiver: id,

            print_data: Default::default(),
        })?;

        trace!("Client servers {:?}",
            servers.iter().map(|s| s.local_addr()).collect::<Vec<_>>()
        );

        for (token, stream) in servers.into_iter().enumerate() {
            let token = token.into();
            reactor.add_stream(token, TcpHandler::new(stream, PacketReader,
                PacketHandler { token },
            ));
        }

        Ok((AsyncTcpStore { reactor }, to_store))
    }

    fn connect(addr: SocketAddr) -> Result<TcpStream, io::Error> {
        let stream = TcpStream::connect(&addr)?;
        let _ = stream.set_keepalive_ms(Some(1000));
        let _ = stream.set_nodelay(true);
        Ok(stream)
    }

    pub fn set_reads_my_writes(&mut self, reads_my_writes: bool) {
        self.reactor.inner().reads_my_writes = reads_my_writes
    }

    pub fn run(mut self) -> ! {
        self.reactor.run().unwrap();
        panic!("should not be");
    }
}

/////////////////////////////////////////////////

impl<C> StoreInner<C>
where C: AsyncStoreClient {
    fn handle_message(
        &mut self, token: mio::Token, _io: &mut TcpWriter, mut packet: Buffer
    ) {
        self.print_data.finished_recvs(1);
        //stay_awake = true;
        let (kind, flag) = {
            let c = packet.contents();
            (c.kind(), *c.flag())
        };
        trace!("CLIENT got a {:?} from {:?}", kind, token);
        if flag.contains(EntryFlag::ReadSuccess) {
            if !flag.contains(EntryFlag::Unlock)
                || flag.contains(EntryFlag::NewMultiPut) {
                let num_chain_servers = self.num_chain_servers;
                self.handle_completion(token, num_chain_servers, &mut packet)
            }
        }
        //TODO distinguish between locks and empties
        else if kind.layout() == EntryLayout::Read
            && !flag.contains(EntryFlag::TakeLock) {
            //A read that found an usused entry still contains useful data
            self.handle_completed_read(token, &packet, false);
        }
        //TODO use option instead
        else {
            unimplemented!()
            // self.handle_redo(Token(server), kind, &packet)
        }
    }

    ////////////////////

    fn handle_completion(
        &mut self, token: Token, num_chain_servers: usize, packet: &mut Buffer
    ) {
        let write_completed = self.handle_completed_write(token, num_chain_servers, packet);
        if let Ok(my_write) = write_completed {
            self.handle_completed_read(token, &*packet, my_write);
        }
    }

    ////////////////////

    fn handle_completed_write(&mut self, token: Token, num_chain_servers: usize,
        packet: &mut Buffer) -> Result<bool, ()> {
        let (id, kind, flag) = {
            let e = packet.contents();
            (*e.id(), e.kind(), *e.flag())
        };
        let unreplicated = self.is_unreplicated;
        trace!("CLIENT handle completed write?");
        //FIXME remove extra index?
        // self.servers[token.0].print_data.finished_appends(1);
        //TODO for multistage writes check if we need to do more work...
        if let Some(v) = self.sent_writes.remove(&id) {
            trace!("CLIENT write needed completion");
            match v {
                WriteState::Skeens1(buf, remaining_servers, timestamps, is_sentinel) => {
                    if !flag.contains(EntryFlag::Skeens1Queued) {
                        error!("CLIENT bad skeens1 ack @ {:?}", token);
                        return Err(())
                    }
                    assert!(kind.contains(EntryKind::Multiput));
                    assert!(self.new_multi);
                    trace!("CLIENT finished sk1 section");
                    let ready_for_skeens2 = skeens_finished(
                        token,
                        packet,
                        &id,
                        num_chain_servers,
                        &remaining_servers,
                        &timestamps,
                        &mut self.max_timestamp_seen,
                        unreplicated
                    );
                    match ready_for_skeens2 {
                        Some(max_ts) => {
                            self.add_skeens2(buf, max_ts);
                            //TODO
                            return Err(())
                        }
                        None => {
                            self.sent_writes.insert(id,
                                WriteState::Skeens1(buf, remaining_servers, timestamps, is_sentinel));
                            return Err(())
                        }
                    }
                }

                WriteState::Skeens2(buf, remaining_servers, max_ts) => {
                    assert!(self.new_multi);
                    trace!("CLIENT finished multi sk2 section");
                    let ready_to_unlock = {
                        let mut b = buf.borrow_mut();
                        let mut finished_writes = true;
                        {
                            let mut me = bytes_as_entry_mut(&mut *b);
                            let locs = me.locs_mut();
                            let mut contents = packet.contents_mut();
                            let fill_from = contents.locs_mut();
                            //trace!("CLIENT filling {:?} from {:?}", locs, fill_from);
                            for (i, loc) in fill_from.into_iter().enumerate() {
                                if locs[i].0 != order::from(0) {
                                    if read_server_for_chain(loc.0, self.num_chain_servers, unreplicated) == token.0
                                        && loc.1 != entry::from(0) {
                                        locs[i] = *loc;
                                    } else if locs[i].1 == entry::from(0) {
                                        finished_writes = false
                                    } else {
                                        *loc = locs[i]
                                    }
                                }
                            }
                            if finished_writes {
                                let locs = &*locs;
                                let from = &*fill_from;
                                for &OrderIndex(o, i) in locs {
                                    if o != order::from(0) {
                                        assert!(
                                            i != entry::from(0),
                                            "mywrite0 {:?} {:?}", locs, from);
                                    }
                                }
                                for &OrderIndex(o, i) in from {
                                    if o != order::from(0) {
                                        assert!(
                                            i != entry::from(0),
                                            "mywrite1 {:?} {:?}", locs, from);
                                    }
                                }
                            }
                        };
                        if finished_writes {
                            //TODO assert!(Rc::get_mut(&mut buf).is_some());
                            //let locs = buf.locs().to_vec();
                            let locs = bytes_as_entry(&b).locs().to_vec();
                            for &OrderIndex(o, _) in &locs {
                                let mts = self.max_timestamp_seen.entry(o).or_insert(max_ts);
                                if max_ts >= *mts {
                                    *mts = max_ts
                                }
                            }
                            Some(locs)
                        } else { None }
                    };
                    match ready_to_unlock {
                        Some(locs) => {
                            /*{
                                Entry::<()>::wrap_bytes_mut(&mut buf.borrow_mut())
                                    .locs_mut()
                                    .copy_from_slice(&locks)
                            }*/
                            trace!("CLIENT finished sk multi at {:?}", locs);
                            //TODO
                            for &OrderIndex(o, i) in &locs {
                                if o != 0.into() {
                                    assert!(i != entry::from(0), "0 location in {:?}", locs)
                                }
                            }
                            //TODO
                            let e = self.client.on_finished_write(id, locs);
                            if e.is_err() {
                                self.finished = true
                            }
                            return Ok(true)
                        }
                        None => {
                            self.sent_writes.insert(id,
                                WriteState::Skeens2(buf, remaining_servers, max_ts));
                            return Err(())
                        }
                    }
                }

                WriteState::SnapshotSkeens1(buf, remaining_servers, timestamps) => {
                    if !flag.contains(EntryFlag::Skeens1Queued) {
                        error!("CLIENT bad skeens1 ack @ {:?}", token);
                        return Err(())
                    }
                    assert!(kind.contains(EntryKind::Snapshot));
                    assert!(self.new_multi);
                    trace!("CLIENT finished snap sk1 section");
                    let ready_for_skeens2 = skeens_finished(
                        token,
                        packet,
                        &id,
                        num_chain_servers,
                        &remaining_servers,
                        &timestamps,
                        &mut self.max_timestamp_seen,
                        unreplicated
                    );
                    match ready_for_skeens2 {
                        Some(max_ts) => {
                            self.add_snapshot_skeens2(buf, max_ts);
                            return Err(())
                        }
                        None => {
                            self.sent_writes.insert(id,
                                WriteState::SnapshotSkeens1(buf, remaining_servers, timestamps));
                            return Err(())
                        }
                    }
                }

                WriteState::SnapshotSkeens2(buf, remaining_servers, max_ts) => {
                    assert!(self.new_multi);
                    trace!("CLIENT finished snap sk2 section");
                    {
                        let mut b = buf.borrow_mut();
                        let finished_writes = {
                            let mut r = remaining_servers.borrow_mut();
                            r.remove(&token.0);
                            r.is_empty()
                        };
                        {
                            let mut me = bytes_as_entry_mut(&mut *b);
                            if finished_writes {
                                me.flag_mut().insert(EntryFlag::ReadSuccess);
                            }
                            let locs = me.locs_mut();
                            let fill_from = packet.contents().locs();
                            //trace!("CLIENT filling {:?} from {:?}", locs, fill_from);
                            for (i, &loc) in fill_from.into_iter().enumerate() {
                                if locs[i].0 != order::from(0) {
                                    if read_server_for_chain(loc.0, self.num_chain_servers, unreplicated) == token.0 {
                                        locs[i] = loc;
                                    }
                                }
                            }
                        }
                        if finished_writes {
                            trace!("CLIENT finished snap sk2");
                            for &l in bytes_as_entry(&*b).locs() {
                                let e = self.client.on_finished_read(l, Vec::clone(&*b));
                                if e.is_err() {
                                    self.finished = true;
                                    return Err(())
                                }
                            }
                            return Err(())
                        }
                    };
                    trace!("CLIENT waiting snap sk2 pieces");
                    self.sent_writes.insert(id,
                        WriteState::SnapshotSkeens2(buf, remaining_servers, max_ts));
                    return Err(())
                }

                WriteState::SingleServer(mut buf) => {
                    //assert!(token != self.lock_token());
                    trace!("CLIENT finished single server");
                    {
                        let contents = packet.contents();
                        //Multi appends go to a single server in the fastpath
                        let filled = fill_locs(&mut buf, contents, token, self.num_chain_servers, unreplicated);
                        if filled < contents.locs().len() {
                            return Err(())
                        }
                        let max_ts = contents.lock_num();
                        // assert!(max_ts >= 1);
                        for &OrderIndex(o, _) in contents.locs() {
                            let mts = self.max_timestamp_seen.entry(o).or_insert(max_ts);
                            if max_ts >= *mts {
                                *mts = max_ts
                            }
                        }
                    }
                    let locs = packet.contents().locs().to_vec();
                    let e = self.client.on_finished_write(id, locs);
                    if e.is_err() {
                        self.finished = true
                    }
                    return Ok(true)
                }
                WriteState::GC(..) => return Err(()),
            };

            fn skeens_finished(
                token: Token,
                packet: &Buffer,
                id: &Uuid,
                num_chain_servers: usize,
                remaining_servers: &Rc<RefCell<HashSet<usize>>>,
                timestamps: &Rc<RefCell<Box<[u64]>>>,
                max_timestamp_seen: &mut HashMap<order, u64>,
                unreplicated: bool,
            ) -> Option<u64> {
                let mut r = remaining_servers.borrow_mut();
                //FIXME store if from head or tail
                //FIXME don't return until gotten from tail
                if !r.remove(&token.0) {
                    // error!("CLIENT repeat sk1 section");
                    return None
                }
                let finished_writes = r.is_empty();
                mem::drop(r);

                let mut ts = timestamps.borrow_mut();
                let e = packet.contents();
                debug_assert_eq!(id, e.id());
                for (i, oi) in e.locs().iter().enumerate() {
                    if oi.0 != order::from(0)
                        && read_server_for_chain(oi.0, num_chain_servers, unreplicated) == token.0 {
                        assert!(ts[i] == 0,
                            "repeat timestamp {:?} in {:#?}", oi, e);
                        let t: entry = oi.1;
                        ts[i] = u32::from(t) as u64;
                        assert!(ts[i] > 0,
                            "bad timestamp {:?} in {:#?}", oi, e);
                    }
                }

                if finished_writes {
                    let max_ts = ts.iter().cloned().max().unwrap();
                    for &OrderIndex(o, _) in e.locs() {
                        let mts = max_timestamp_seen.entry(o).or_insert(max_ts);
                        if max_ts >= *mts {
                            *mts = max_ts
                        }
                    }
                    // for t in ts.iter() { assert!(&max_ts >= t); }
                    trace!("CLIENT finished snap skeens1 {:?}: {:?} max {:?}",
                        e.id(), ts, max_ts);
                    Some(max_ts)
                } else { None }
            }

            fn fill_locs(buf: &mut [u8], e: EntryContents,
                server: Token, num_chain_servers: usize, unreplicated: bool) -> usize {
                let mut me = bytes_as_entry_mut(buf);
                let locs = me.locs_mut();
                let mut filled = 0;
                let fill_from = e.locs();
                //trace!("CLIENT filling {:?} from {:?}", locs, fill_from);
                for (i, &loc) in fill_from.into_iter().enumerate() {
                    if locs[i].0 == order::from(0) {
                        filled += 1;
                        continue
                    }
                    if read_server_for_chain(loc.0, num_chain_servers, unreplicated) == server.0
                        && loc.1 != entry::from(0) {//should be read_server_for_chain
                        // assert!(loc.1 != 0.into(), "zero index for {:?} @ {:?} => {:?}, nc: {:?} r: {:?}", loc.0, fill_from, locs, num_chain_servers, unreplicated);
                        locs[i] = loc;
                        filled += 1;
                    } else if locs[i].1 != entry::from(0) {
                        filled += 1;
                    }
                }
                filled
            }
        }
        else if kind.layout() == EntryLayout::Data
        || kind.layout() == EntryLayout::Multiput
        || kind.layout() == EntryLayout::Sentinel
        || kind.layout() == EntryLayout::Read
        || (kind.layout() == EntryLayout::Snapshot && !flag.contains(EntryFlag::TakeLock)) {
            // panic!("{:?}", packet.contents());
            return Ok(false)
        } else {
            return Err(())
        }
    }// end handle_completed_write

    //////////

    fn handle_completed_read(&mut self, _token: Token, packet: &Buffer, my_write: bool) -> bool {
        use std::collections::hash_map::Entry::Occupied;

        // trace!("CLIENT handle completed read?");
        let (is_sentinel, is_snapshot) = {
            let layout = packet.contents().layout();
            (layout == EntryLayout::Sentinel,
                layout == EntryLayout::Snapshot)
        };
        if is_snapshot {
            for &oi in packet.contents().locs() {
                let mut v = self.waiting_buffers.pop_front()
                    .unwrap_or_else(|| Vec::with_capacity(packet.entry_size()));
                v.clear();
                v.extend_from_slice(packet.entry_slice());
                if self.client.on_finished_read(oi, v).is_err() {
                    self.finished = true
                }
            }
            return true
        }
        //FIXME remove extra index?
        // self.servers[token.0].print_data.finished_read(1);
        //FIXME remove
        let mut was_needed = false;
        let mut is_sentinel_loc = false;
        let contents = packet.contents();
        let max_ts = contents.lock_num();
        let _num_locs = contents.locs().len();
        // if +num_locs > 1 && contents.flag().contains(EntryFlag::TakeLock) {
        //     assert!(max_ts > 0, "max_ts for {:?} = {:?}", contents.locs(), max_ts);
        // }
        // println!("> {:?}: {:?}", contents.locs(), contents.lock_num());
        for &oi in contents.locs() {
            if oi == OrderIndex(0.into(), 0.into()) {
                is_sentinel_loc = true;
                continue
            }
            if my_write {
                assert!(oi.1 != entry::from(0), "mywrite {:?}", packet.contents());
            } else {

            }
            //FIXME only needed?
            if contents.flag().contains(EntryFlag::ReadSuccess) {
                let mts = self.max_timestamp_seen.entry(oi.0).or_insert(max_ts);
                if max_ts >= *mts {
                    *mts = max_ts
                }
            }

            if is_sentinel && !is_sentinel_loc  {
                // we don't want to return a sentinel for an actual multi;
                // we need the data
                //alt if is_sentinel != is_sentinel_loc
                continue
            }
            //trace!("CLIENT completed read @ {:?}", oi);
            let needed = match self.sent_reads.entry(oi) {
                Occupied(mut oe) => {
                    assert!(oi.1 != 0.into(), "needed {:?}", packet.contents());
                    let needed = if oe.get() > &0 {
                        *oe.get_mut() -= 1;
                        true
                    } else {
                        false
                    };
                    if oe.get() == &0 {
                        oe.remove();
                    }
                    needed
                }
                _ => false,
            };
            if needed || (my_write && self.reads_my_writes) {
                assert!(oi.1 != 0.into(), "needed {:?}", packet.contents());
                was_needed = true;
                trace!("CLIENT read needed completion @ {:?}", oi);
                //TODO validate correct id for failing read
                //TODO num copies?
                let mut v = self.waiting_buffers.pop_front()
                    .unwrap_or_else(|| Vec::with_capacity(packet.entry_size()));
                v.clear();
                v.extend_from_slice(packet.entry_slice());
                if self.client.on_finished_read(oi, v).is_err() {
                    self.finished = true
                }
            }
        }
        // if my_write && self.reads_my_writes && !was_needed {
        //     let oi = packet.contents().locs()[0];
        //     assert!(oi.1 != 0.into(), "mywrite {:?}", packet.contents());
        //     let mut v = self.waiting_buffers.pop_front()
        //         .unwrap_or_else(|| Vec::with_capacity(packet.entry_size()));
        //     v.clear();
        //     v.extend_from_slice(packet.entry_slice());
        //     if self.client.on_finished_read(oi, v).is_err() {
        //         self.finished = true
        //     }
        // }

        // TODO is this assert right?
        // assert!(
        //     was_needed || my_write,
        //     "!(wn {} || mw {}) {:?}\n{:#?}",
        //     was_needed, my_write,
        //     packet.contents(),
        //     self.sent_reads,
        // );
        was_needed
    }

    ////////////////////////////////////////

    fn handle_new_requests_from_client(&mut self, inner: &mut IoState<PerStream>) -> bool {
        use std::sync::mpsc::TryRecvError;
        //trace!("CLIENT got new req");
        let mut msg = match self.from_client.try_recv() {
            Ok(msg) => msg,
            Err(TryRecvError::Empty) => return false,
            //TODO Err(TryRecvError::Disconnected) => panic!("client disconnected.")
            Err(TryRecvError::Disconnected) => {
                self.finished = true;
                return false
            },
        };
        self.print_data.from_client(1);
        if msg.is_empty() {
            self.finished = true;
            return false
        }
        let new_msg_kind = bytes_as_entry(&msg).layout();
        match new_msg_kind {
            EntryLayout::Read => {
                let loc = bytes_as_entry(&msg).locs()[0];
                trace!("CLIENT will read {:?}", loc);
                // trace!("CLIENT vec len {}, entry len {}", msg.len(), bytes_as_entry(&msg).len());
                let loc = loc.0;
                let s = self.read_server_for_chain(loc);
                let sent = self.add_single_server_send(inner, s.into(), msg);
                sent.unwrap_or_else(||
                    panic!("cannot sent read to {}, {}", s, self.num_chain_servers))
            }
            EntryLayout::Data => {
                let loc;
                {

                    let mut contents = bytes_as_entry_mut(&mut msg);
                    loc = contents.as_ref().locs()[0].0;
                    if !contents.flag_mut().contains(EntryFlag::DirectWrite) {
                        let timestamp = self.max_timestamp_seen.get(&loc)
                            .cloned().unwrap_or_else(|| 0);
                        *contents.lock_mut() = timestamp;
                    }
                    // assert!(*contents.lock_mut() >= 1, "{:#?}", self.max_timestamp_seen);
                    // assert!(contents.as_ref().lock_num() >= 1);
                }
                let s = self.write_server_for_chain(loc);
                trace!("CLIENT will write {:?}, server {:?}:{:?}",
                    loc, s, self.num_chain_servers);
                //TODO if writeable write?
                self.add_single_server_send(inner, s.into(), msg)
                    .expect("cannot sent append")

            }
            EntryLayout::Multiput => {
                trace!("CLIENT will multi write");
                //FIXME set max_timestamp from local
                //TODO
                let use_fastpath = true;
                if bytes_as_entry(&msg).flag().contains(EntryFlag::DirectWrite) {
                    let servers = self.get_servers_for_multi(&msg);
                    for s in servers {
                        self.add_single_server_send(inner, s.into(), msg.clone());

                    }
                    true
                } else if use_fastpath && self.is_single_node_append(&msg) {
                    {
                        let mut contents = bytes_as_entry_mut(&mut msg);
                        *contents.lock_mut() = 1;
                    }
                    let chain = bytes_as_entry(&msg).locs()[0].0;
                    let s = self.write_server_for_chain(chain);
                    self.add_single_server_send(inner, s.into(), msg)
                        .expect("cannot send fast multi")

                } else if self.new_multi {
                    let mut msg = msg;
                    {
                        let mut e = bytes_as_entry_mut(&mut msg);
                        e.flag_mut().insert(EntryFlag::TakeLock | EntryFlag::NewMultiPut);
                        e.locs_mut().into_iter()
                            .fold((), |_, &mut OrderIndex(_,ref mut i)| *i = 0.into());
                    }
                    self.add_skeens1(inner, msg);
                    true
                } else {
                    unreachable!("old locking code path")
                }
            }

            EntryLayout::Snapshot => {
                trace!("CLIENT will multi snapshot");
                if self.is_single_node_append(&msg) {
                    let chain = bytes_as_entry(&msg).locs()[0].0;
                    let s = self.read_server_for_chain(chain);
                    self.add_single_server_send(inner, s.into(), msg)
                        .expect("cannot send snap")

                } else {
                    let mut msg = msg;
                    assert!(self.new_multi);
                    bytes_as_entry_mut(&mut msg)
                        .flag_mut().insert(EntryFlag::TakeLock | EntryFlag::NewMultiPut);
                    self.add_snapshot_skeens1(inner, msg);
                    true
                }
            }

            EntryLayout::GC => {
                //TODO be less lazy
                // self.add_gc(msg)
                unimplemented!()
            },
            r @ EntryLayout::Sentinel | r @ EntryLayout::Lock =>
                panic!("Invalid send request {:?}", r),
        }
    } // End fn handle_new_requests_from_client

    ////////////////////

    fn add_single_server_send(
        &mut self, inner: &mut IoState<PerStream>, token: mio::Token, msg: Vec<u8>,
    ) -> Option<bool> {
        inner.mutate(token, |per_server| {
            assert_eq!(bytes_as_entry(&msg).len(), msg.len());
            // per_server.add_single_server_send(msg);
            let to_send = WriteState::SingleServer(msg);
            let receiver = self.receiver.bytes();
            to_send.with_packet(|p| per_server.add_writes(&[p, receiver]));
            let sent = to_send;
            let layout = sent.layout();
            if layout == EntryLayout::Read {
                let read_loc = sent.read_loc();
                *self.sent_reads.entry(read_loc).or_insert(0) += 1;
                self.waiting_buffers.push_back(sent.take())
            }
            else if layout == EntryLayout::Snapshot {
                if sent.is_multi() {
                    let id = sent.id();
                    self.sent_writes.insert(id, sent);
                }
            }
            else if !sent.is_unlock() {
                let id = sent.id();
                self.sent_writes.insert(id, sent);
            }
            true
        })

    }

    //////////

    fn add_skeens1(&mut self, inner: &mut IoState<PerStream>, msg: Vec<u8>) {
        debug_assert_eq!(bytes_as_entry(&msg).len(), msg.len());
        let (msg, servers, remaining_servers, timestamps) = self.prep_skeens1(msg);
        trace!("CLIENT multi to {:?}", remaining_servers);
        let mut send = None;
        for s in servers {
            inner.mutate(s.into(), |ps| {
                let msg = msg.clone();
                let remaining_servers = remaining_servers.clone();
                let timestamps = timestamps.clone();
                let num_servers = self.num_chain_servers;
                let is_data;
                {
                    let mut ts = msg.borrow_mut();
                    let send_end = {
                        {
                            let mut e = bytes_as_entry_mut(&mut *ts);
                            is_data = e.as_ref().locs().into_iter()
                                .take_while(|&&oi| oi != OrderIndex(0.into(), 0.into()))
                                .any(|oi| is_write_server_for(oi.0, s.into(), num_servers));
                            debug_assert!(e.as_ref().layout() == EntryLayout::Multiput
                                || e.as_ref().layout() == EntryLayout::Sentinel);
                            {
                                let flag = e.flag_mut();
                                debug_assert!(flag.contains(EntryFlag::TakeLock));
                                flag.insert(EntryFlag::TakeLock);
                            }
                            if !is_data {
                                debug_assert!(e.as_ref().locs()
                                    .contains(&OrderIndex(0.into(), 0.into())));
                            }
                        }
                        if is_data {
                            slice_to_multi(&mut ts[..]);
                        } else {
                            slice_to_sentinel(&mut ts[..]);
                        }
                        bytes_as_entry_mut(&mut *ts).as_ref().len()
                    };
                    //Since sentinels have a different size than multis, we need to truncate
                    //for those sends
                    let receiver = self.receiver.bytes();
                    ps.add_writes(&[&ts[..send_end], receiver]);
                }
                if send.is_none() {
                    send = Some(WriteState::Skeens1(
                            msg, remaining_servers, timestamps, !is_data
                        )
                    )
                }

            });
        }
        send.map(|s| self.sent_writes.insert(s.id(), s));
    }

    //////////

    fn add_snapshot_skeens1(&mut self, inner: &mut IoState<PerStream>, msg: Vec<u8>) {
        let (msg, servers, remaining_servers, timestamps) = self.prep_skeens1(msg);
        let mut send = None;
        for s in servers {
            inner.mutate(s.into(), |ps| {
                let msg = msg.clone();
                let remaining_servers = remaining_servers.clone();
                let timestamps = timestamps.clone();
                {
                    let ts = msg.borrow();
                    let len = bytes_as_entry(&*ts).len();
                    assert_eq!(len, ts.len());
                    trace!("{:?}", bytes_as_entry(&*ts));
                    let receiver = self.receiver.bytes();
                    ps.add_writes(&[&ts[..len], receiver]);
                }
                if send.is_none() {
                    send = Some(WriteState::SnapshotSkeens1(
                        msg, remaining_servers, timestamps
                    ))
                }
            });
        }
        send.map(|sent| {
            //FIXME is this right
            let id = sent.id();
            self.sent_writes.insert(id, sent);
        });
    }

    ////////////////////

    fn add_skeens2(&mut self, buf: Rc<RefCell<Vec<u8>>>, max_ts: u64) {
        self.add_sk2(buf, max_ts, false);
    }

    fn add_snapshot_skeens2(&mut self, buf: Rc<RefCell<Vec<u8>>>, max_ts: u64) {
        self.add_sk2(buf, max_ts, true);
    }

    fn add_sk2(
        &mut self, mut buf: Rc<RefCell<Vec<u8>>>, max_ts: u64, is_snapshot: bool,
    ) {
        let servers = match Rc::get_mut(&mut buf) {
            None => unreachable!(),
            Some(mut buf) => {
                let buf = buf.get_mut();
                self.get_servers_for_multi(&buf)
            }
        };
        self.pending_skeens2.push_back(SK2Send{
            buf: buf.clone(),
            max_ts,
            servers,
            is_snapshot,
        });
    }

    //////////

    fn send_skeens2(
        &mut self,
        inner: &mut IoState<PerStream>,
        mut buf: Rc<RefCell<Vec<u8>>>,
        max_ts: u64,
        servers: Vec<usize>,
        is_snapshot: bool,
    ) {
        match Rc::get_mut(&mut buf) {
            None => unreachable!(),
            Some(buf) => {
                let buf = buf.get_mut();
                slice_to_sentinel(buf);
                let size = {
                    let mut e = bytes_as_entry_mut(buf);
                    e.flag_mut().insert(EntryFlag::Unlock | EntryFlag::NewMultiPut);
                    e.locs_mut().iter_mut()
                        .fold((), |_, &mut OrderIndex(_, ref mut i)| *i = entry::from(0));
                    *e.lock_mut() = max_ts;
                    e.as_ref().len()
                };
                unsafe { buf.set_len(size) };
            }
        };
        let mut remaining_servers: HashSet<usize> = Default::default();
        remaining_servers.reserve(servers.len());
        for &writer in servers.iter() {
            remaining_servers.insert(self.read_server_for_write_server(writer));
        }
        let remaining_servers = Rc::new(RefCell::new(remaining_servers));
        let mut send = None;
        for token in servers {
            let token = token.into();
            inner.mutate(token, |ps| {
                let msg = buf.clone();
                let remaining_servers = remaining_servers.clone();
                let max_timestamp = max_ts;
                trace!("Add Skeens2 {}", if is_snapshot { "snapshot" } else { "" });
                {
                    let ts = msg.borrow();
                    debug_assert_eq!(bytes_as_entry(&*ts).lock_num(), max_timestamp);
                    let send_end = bytes_as_entry(&*ts).len();
                    let receiver = self.receiver.bytes();
                    ps.add_writes(&[&ts[..send_end], receiver]);
                }
                if send.is_none() {
                    send = Some(if is_snapshot {
                        WriteState::SnapshotSkeens2(
                            msg, remaining_servers, max_timestamp
                        )
                    } else {
                        WriteState::Skeens2(
                            msg, remaining_servers, max_timestamp
                        )
                    })
                }
            });
        }
        send.map(|sent| {
            let id = sent.id();
            self.sent_writes.insert(id, sent);
        });
    }

    ////////////////////

    fn prep_skeens1(&mut self, msg: Vec<u8>)
    -> (Rc<RefCell<Vec<u8>>>, Vec<usize>, Rc<RefCell<HashSet<usize>>>, Rc<RefCell<Box<[u64]>>>) {
        let timestamps = Rc::new(RefCell::new(
            (0..bytes_as_entry(&msg).locs().len())
                .map(|_| 0u64).collect::<Vec<_>>().into_boxed_slice()
        ));
        let servers = self.get_servers_for_multi(&msg);
        let mut remaining_servers: HashSet<usize> = Default::default();
        remaining_servers.reserve(servers.len());
        for &writer in servers.iter() {
            remaining_servers.insert(self.read_server_for_write_server(writer));
        }
        trace!("CLIENT multi to {:?}", remaining_servers);
        let remaining_servers = Rc::new(RefCell::new(remaining_servers));
        let msg = Rc::new(RefCell::new(msg));
        (msg, servers, remaining_servers, timestamps)
    }

    ////////////////////

    fn is_single_node_append(&self, msg: &[u8]) -> bool {
        let mut single = true;
        let mut server_token = None;
        let locked_chains = bytes_as_entry(&msg).locs()
            .iter().cloned().filter(|&oi| oi != OrderIndex(0.into(), 0.into()));
        for c in locked_chains {
            if let Some(server_token) = server_token {
                single &= self.write_server_for_chain(c.0) == server_token
            }
            else {
                server_token = Some(self.write_server_for_chain(c.0))
            }
        }
        single
    }

    fn get_servers_for_multi(&self, msg: &[u8]) -> Vec<usize> {
        debug_assert!(
            bytes_as_entry(msg).layout() == EntryLayout::Multiput
            || bytes_as_entry(msg).layout() == EntryLayout::Sentinel
            || bytes_as_entry(msg).layout() == EntryLayout::GC
            || bytes_as_entry(msg).layout() == EntryLayout::Snapshot
        );
        bytes_as_entry(msg).locs()
            .iter()
            .filter(|&&oi| oi != OrderIndex(0.into(), 0.into()))
            .fold((0..self.num_chain_servers) //FIXME ?
                .map(|_| false).collect::<Vec<_>>(),
            |mut v, &OrderIndex(o, _)| {
                v[self.write_server_for_chain(o)] = true;
                v
            }).into_iter()
            .enumerate()
            .filter_map(|(i, present)|
                if present { Some(i) }
                else { None })
            .collect()
    }

    ////////////////////

    fn write_server_for_chain(&self, chain: order) -> usize {
        write_server_for_chain(chain, self.num_chain_servers)
    }

    fn read_server_for_chain(&self, chain: order) -> usize {
        let server = if self.is_unreplicated {
            write_server_for_chain(chain, self.num_chain_servers)
        }
        else {
            read_server_for_chain(chain, self.num_chain_servers, self.is_unreplicated)
        };
        server
    }

    fn read_server_for_write_server(&self, write_server: usize) -> usize {
        let server = if self.is_unreplicated {
            write_server
        } else {
            write_server + self.num_chain_servers
        };
        server
    }
}

fn write_server_for_chain(chain: order, num_servers: usize) -> usize {
    (<u32 as From<order>>::from(chain) % (num_servers as u32)) as usize
}

fn read_server_for_chain(chain: order, num_servers: usize, unreplicated: bool) -> usize {
    //(<u32 as From<order>>::from(chain) as usize % (num_servers)  + 1) * 2
    if unreplicated {
        write_server_for_chain(chain, num_servers)
    } else {
        <u32 as From<order>>::from(chain) as usize % (num_servers) + num_servers
    }
}

fn is_write_server_for(chain: order, tok: Token, num_servers: usize) -> bool {
    write_server_for_chain(chain, num_servers) == tok.0
}

/////////////////////////////////////////////////
/////////////////////////////////////////////////
/////////////////////////////////////////////////

pub struct PacketHandler{
    token: mio::Token,
}

impl<C: AsyncStoreClient> MessageHandler<StoreInner<C>, Buffer> for PacketHandler {
    fn handle_message(
        &mut self,
        io: &mut TcpWriter,
        inner: &mut StoreInner<C>,
        msg: Buffer,
    ) -> Result<(), ()> {
        inner.handle_message(self.token, io, msg);
        Ok(())
    }
}

/////////////////////////////////////////////////

pub struct PacketReader;

impl MessageReader for PacketReader {
    type Message = Buffer;
    type Error = io::ErrorKind;

    fn deserialize_message(
        &mut self,
        bytes: &[u8]
    ) -> Result<(Self::Message, usize), MessageReaderError<Self::Error>> {
        use self::MessageReaderError::*;
        use packets::Packet::WrapErr;

        let to_read = unsafe { EntryContents::try_ref(bytes).map(|(c, _)| c.len()) };
        let size = to_read.map_err(|e| match e {
            WrapErr::NotEnoughBytes(needs) =>
                NeedMoreBytes(needs),
            _ => Other(io::ErrorKind::InvalidInput),
        })?;

        if bytes.len() < size {
            return Err(NeedMoreBytes(size))?
        }

        let buffer = Buffer::wrap_vec(bytes[..size].to_vec());
        Ok((buffer, size))
    }
}

////////////////////////////////////////////////

impl<C> Wakeable for StoreInner<C>
where C: AsyncStoreClient {
    fn init(&mut self, token: mio::Token, poll: &mut mio::Poll) {
        poll.register(
            &self.from_client,
            token,
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::level(), //TODO or edge?
        ).unwrap();
    }

    fn needs_to_mark_as_staying_awake(&mut self, _: mio::Token) -> bool { false }
    fn mark_as_staying_awake(&mut self, _: mio::Token) {}
    fn is_marked_as_staying_awake(&self, _: mio::Token) -> bool { true }
}

impl<C> Handler<IoState<PerStream>> for StoreInner<C>
where C: AsyncStoreClient {
    type Error = ();

    fn on_event(&mut self, inner: &mut IoState<PerStream>, token: mio::Token, _: mio::Event)
    -> Result<(), Self::Error> {
        self.on_poll(inner, token)
    }

    fn on_poll(&mut self, inner: &mut IoState<PerStream>, _token: mio::Token)
    -> Result<(), Self::Error> {
        self.handle_new_requests_from_client(inner);
        Ok(())
    }

    fn on_error(&mut self, _: Self::Error, _: &mut mio::Poll) -> ShouldRemove {
        error!("store inner error");
        false
    }

    fn after_work(&mut self, inner: &mut IoState<PerStream>) {
        //TODO cache?
        let mut pending_sk2 = mem::replace(&mut self.pending_skeens2, VecDeque::new());

        for SK2Send { servers, buf, max_ts, is_snapshot } in pending_sk2.drain(..) {
            self.send_skeens2(inner, buf, max_ts, servers, is_snapshot);

        }
        self.pending_skeens2 = pending_sk2;
    }
}

/////////////////////////////////////////////////
/////////////////////////////////////////////////
/////////////////////////////////////////////////

impl WriteState {
    fn with_packet<F, R>(&self, f: F) -> R
    where F: for<'a> FnOnce(&'a [u8]) -> R {
        use self::WriteState::*;
        match self {
            &SingleServer(ref buf) | &GC(ref buf) => f(&**buf),

            &Skeens1(ref buf, _, _, is_sentinel) => {
                let mut b = buf.borrow_mut();
                if is_sentinel { slice_to_sentinel(&mut b[..]); }
                else { slice_to_multi(&mut b[..]) }
                f(&*b)
            },

            &Skeens2(ref buf, ..)
            | &SnapshotSkeens1(ref buf, _, _) | &SnapshotSkeens2(ref buf, _, _) => {
                let b = buf.borrow();
                f(&*b)
            },
        }
    }

    fn take(self) -> Vec<u8> {
        use self::WriteState::*;
        match self {
            SingleServer(buf) | GC(buf) => buf,

            Skeens1(buf, ..) | Skeens2(buf, ..)
            | SnapshotSkeens1(buf, _, _) | SnapshotSkeens2(buf, _, _) =>
                Rc::try_unwrap(buf).expect("taking from non unique WriteState")
                    .into_inner()
        }
    }

    fn is_unlock(&self) -> bool {
        match self {
            _ => false,
        }
    }

    fn is_multi(&self) -> bool {
        match self {
            &WriteState::Skeens1(..) | &WriteState::Skeens2(..) => true,
            &WriteState::SnapshotSkeens1(..) | &WriteState::SnapshotSkeens2(..) => true,
            _ => false,
        }
    }

    fn id(&self) -> Uuid {
        let mut id = unsafe { mem::uninitialized() };
        self.with_packet(|p| {
            id = bytes_as_entry(p).id().clone()
        });
        id
    }

    fn layout(&self) -> EntryLayout {
        let mut layout = unsafe { mem::uninitialized() };
        self.with_packet(|p| {
            layout = bytes_as_entry(p).layout();
        });
        layout
    }

    fn read_loc(&self) -> OrderIndex {
        let mut loc = unsafe { mem::uninitialized() };
        self.with_packet(|p| {
            loc = bytes_as_entry(p).locs()[0]
        });
        loc
    }
}

/////////////////////////////////////////////////
/////////////////////////////////////////////////
/////////////////////////////////////////////////

fn blocking_write<W: Write>(w: &mut W, mut buffer: &[u8]) -> io::Result<()> {
    use std::thread;
    //like Write::write_all but doesn't die on WouldBlock
    'recv: while !buffer.is_empty() {
        match w.write(buffer) {
            Ok(i) => { let tmp = buffer; buffer = &tmp[i..]; }
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock
                | io::ErrorKind::Interrupted
                | io::ErrorKind::NotConnected => {
                    thread::yield_now();
                    continue 'recv
                },
                _ => { return Err(e) }
            }
        }
    }
    if !buffer.is_empty() {
        return Err(io::Error::new(io::ErrorKind::WriteZero,
            "failed to fill whole buffer"))
    }
    else {
        return Ok(())
    }
}

fn blocking_read<R: Read>(r: &mut R, mut buffer: &mut [u8]) -> io::Result<()> {
    use std::thread;
    //like Read::read_exact but doesn't die on WouldBlock
    'recv: while !buffer.is_empty() {
        match r.read(buffer) {
            Ok(i) => { let tmp = buffer; buffer = &mut tmp[i..]; }
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted | io::ErrorKind::NotConnected => {
                    thread::yield_now();
                    continue 'recv
                },
                _ => { return Err(e) }
            }
        }
    }
    if !buffer.is_empty() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof,
            "failed to fill whole buffer"))
    }
    else {
        return Ok(())
    }
}
