use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::{ops, mem};
use std::net::SocketAddr;
use std::rc::Rc;

use packets::*;

use bit_set::BitSet;

use mio;
use mio::prelude::*;
use mio::tcp::*;
use mio::Token;

pub trait AsyncStoreClient {
    //TODO nocopy?
    fn on_finished_read(&mut self, read_packet: Vec<u8>);
    //TODO what info is needed?
    fn on_finished_write(&mut self, write_id: Uuid, write_locs: Vec<OrderIndex>);
}

//TODO use FNV hash
pub struct AsyncTcpStore<C: AsyncStoreClient> {
    sent_writes: HashMap<Uuid, WriteState>,
    sent_reads: HashMap<OrderIndex, Vec<u8>>,
    servers: Vec<PerServer>,
    registered_for_write: BitSet,
    needs_to_write: BitSet,
    num_chain_servers: usize,
    client: C,
}

struct PerServer {
    awaiting_send: VecDeque<WriteState>,
    read_buffer: Buffer,
    stream: TcpStream,
    bytes_handled: usize,
}

#[derive(Debug)]
enum WriteState {
    SingleServer(Vec<u8>),
    ToLockServer(Vec<u8>, Vec<u8>),
    MultiServer(Rc<RefCell<Vec<u8>>>, Rc<HashMap<usize, u64>>),
    UnlockServer(Rc<RefCell<Vec<u8>>>, u64),
}

struct Buffer {
    inner: Vec<u8>,
}

impl<C> AsyncTcpStore<C>
where C: AsyncStoreClient {
    pub fn new<I>(lock_server: SocketAddr, chain_servers: I, client: C,
        event_loop: &mut EventLoop<Self>)
    -> Result<Self, io::Error>
    where I: IntoIterator<Item=SocketAddr> {
        //TODO assert no duplicates
        let mut servers = try!(chain_servers
            .into_iter()
            .map(PerServer::new)
            .collect::<Result<Vec<_>, _>>());
        let num_chain_servers = servers.len();
        let lock_server = try!(PerServer::new(lock_server));
        //TODO if let Some(lock_server) = lock_server...
        servers.push(lock_server);
        for (i, server) in servers.iter().enumerate() {
            event_loop.register(&server.stream, Token(i),
                mio::EventSet::readable() | mio::EventSet::error(),
                mio::PollOpt::edge() | mio::PollOpt::oneshot())
                .expect("could not reregister client socket")
        }
        let num_servers = servers.len();
        Ok(AsyncTcpStore {
            sent_writes: HashMap::new(),
            sent_reads: HashMap::new(),
            servers: servers,
            registered_for_write: BitSet::with_capacity(num_servers),
            needs_to_write: BitSet::with_capacity(num_servers),
            num_chain_servers: num_chain_servers,
            client: client
        })
    }
}

impl PerServer {
    fn new(addr: SocketAddr) -> Result<Self, io::Error> {
        Ok(PerServer {
            awaiting_send: VecDeque::new(),
            read_buffer: Buffer::new(), //TODO cap
            stream: try!(TcpStream::connect(&addr)),
            bytes_handled: 0,
        })
    }
}

impl<C> mio::Handler for AsyncTcpStore<C>
where C: AsyncStoreClient {
    type Timeout = ();
    type Message = Vec<u8>;

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Self::Message) {
        let new_msg_kind = Entry::<()>::wrap_bytes(&msg).kind.layout();
        match new_msg_kind {
            EntryLayout::Read | EntryLayout::Data => {
                let loc = Entry::<()>::wrap_bytes(&msg).locs()[0].0;
                let s = self.server_for_chain(loc);
                //TODO if writeable write?
                self.add_single_server_send(s, msg);
            }
            EntryLayout::Multiput => {
                if self.is_single_node_append(&msg) {
                    let chain = Entry::<()>::wrap_bytes(&msg).locs()[0].0;
                    let s = self.server_for_chain(chain);
                    self.add_single_server_send(s, msg);
                }
                else {
                    let mut msg = msg;
                    Entry::<()>::wrap_bytes_mut(&mut msg).locs_mut().into_iter()
                        .fold((), |_, &mut (_,ref mut i)| *i = 0.into());
                    self.add_get_lock_nums(msg);
                };
            }
            r @ EntryLayout::Sentinel | r @ EntryLayout::Lock =>
                panic!("Invalid send request {:?}", r),
        }
        self.regregister_writes(event_loop);
    }//End fn notify

    fn ready(&mut self, event_loop: &mut EventLoop<Self>, token: Token, events: mio::EventSet) {
        //TODO should this be in a loop?
        self.registered_for_write.remove(token.as_usize());
        if events.is_readable() {
            let finished_packet = {
                let server = self.server_for_token_mut(token);
                //TODO incremental reads
                server.read_packet()
            };
            if let Some(packet) = finished_packet {
                let kind = packet.entry().kind;
                trace!("CLIENT got a {:?} from {:?}", kind, token);
                if kind.contains(EntryKind::ReadSuccess) {
                    let num_chain_servers = self.num_chain_servers;
                    self.handle_completion(token, num_chain_servers, &packet)
                }
                //TODO distinguish between locks and empties
                else if kind.layout() == EntryLayout::Read
                    && !kind.contains(EntryKind::TakeLock) {
                    //A read that found an usused entry still contains useful data
                    self.handle_completed_read(token, &packet)
                }
                //TODO use option instead
                else {
                    self.handle_redo(token, kind, &packet)
                }
                self.server_for_token_mut(token).read_buffer = packet
            }
        }

        if self.server_for_token(token).needs_to_write() && events.is_writable() {
            let num_chain_servers = self.num_chain_servers;
            //TODO incremental writes
            let sent = self.server_for_token_mut(token).send_next_packet(token, num_chain_servers);
            if let Some(sent) = sent {
                let layout = sent.layout();
                if layout == EntryLayout::Read {
                    let read_loc = sent.read_loc();
                    self.sent_reads.insert(read_loc, sent.take());
                }
                else if !sent.is_unlock() {
                    let id = sent.id();
                    self.sent_writes.insert(id, sent);
                }
            }
            else {
                trace!("Async spurious write");
            }
        }
        let needs_to_write = self.server_for_token(token).needs_to_write();
        if needs_to_write {
            self.needs_to_write.insert(token.as_usize());
        }
        else {
            self.reregister_for_read(event_loop, token.as_usize());
        }
        self.regregister_writes(event_loop)
    }//End fn ready

}

impl<C> AsyncTcpStore<C>
where C: AsyncStoreClient {
    fn handle_completion(&mut self, token: Token, num_chain_servers: usize, packet: &Buffer) {
        let write_completed = self.handle_completed_write(token, num_chain_servers, packet);
        if write_completed {
            self.handle_completed_read(token, packet);
        }
    }

    //TODO I should probably return the buffer on read_packet, and pass it into here
    fn handle_completed_write(&mut self, token: Token, num_chain_servers: usize,
        packet: &Buffer) -> bool {
        let id = packet.entry().id;
        trace!("CLIENT handle_completed_write");
        //TODO for multistage writes check if we need to do more work...
        if let Some(v) = self.sent_writes.remove(&id) {
            match v {
                //if lock
                //    for server in v.servers()
                //         server.send_lock+data
                WriteState::ToLockServer(_, msg) => {
                    trace!("CLIENT finished lock");
                    //TODO should I bother keeping around lockbuf for resends?
                    assert_eq!(token, self.lock_token());
                    let lock_nums = packet.get_lock_nums();
                    self.add_multis(msg, lock_nums);
                    return false
                }
                WriteState::MultiServer(buf, lock_nums) => {
                    assert!(token != self.lock_token());
                    trace!("CLIENT finished multi section");
                    let ready_to_unlock = {
                        let mut b = buf.borrow_mut();
                        let finished_writes = fill_locs(&mut b, packet.entry(),
                            token, num_chain_servers);
                        if finished_writes {
                            //TODO assert!(Rc::get_mut(&mut buf).is_some());
                            //let locs = buf.locs().to_vec();
                            let locs = Entry::<()>::wrap_bytes(&b).locs().to_vec();
                            Some(locs)
                        } else { None }
                    };
                    match ready_to_unlock {
                        Some(locs) => {
                            self.add_unlocks(buf, lock_nums);
                            trace!("CLIENT finished multi at {:?}", locs);
                            //TODO
                            self.client.on_finished_write(id, locs);
                            return true
                        }
                        None => {
                            self.sent_writes.insert(id,
                                WriteState::MultiServer(buf, lock_nums));
                            return false
                        }
                    }
                }
                WriteState::SingleServer(mut buf) => {
                    assert!(token != self.lock_token());
                    trace!("CLIENT finished single server");
                    fill_locs(&mut buf, packet.entry(), token, num_chain_servers);
                    let locs = packet.entry().locs().to_vec();
                    self.client.on_finished_write(id, locs);
                    return true
                }
                WriteState::UnlockServer(..) => panic!("invalid wait state"),
            }

            fn fill_locs(buf: &mut [u8], e: &Entry<()>,
                server: Token, num_chain_servers: usize) -> bool {
                let locs = Entry::<()>::wrap_bytes_mut(buf).locs_mut();
                let mut remaining = locs.len();
                let fill_from = e.locs();
                //trace!("CLIENT filling {:?} from {:?}", locs, fill_from);
                for (i, &loc) in fill_from.into_iter().enumerate() {
                    if locs[i].0 != 0.into()
                        && server_for_chain(loc.0, num_chain_servers) == server.as_usize() {
                        assert!(loc.1 != 0.into());
                        locs[i] = loc;
                    }
                    if locs[i] == (0.into(), 0.into()) || locs[i].1 != 0.into() {
                        remaining -= 1;
                    }
                }
                remaining == 0
            }
            unreachable!()
        }
        else {
            return true
        }
    }// end handle_completed_write

    fn handle_completed_read(&mut self, token: Token, packet: &Buffer) {
        if token == self.lock_token() { return }
        for &oi in packet.entry().locs() {
            if let Some(mut v) = self.sent_reads.remove(&oi) {
                //TODO num copies?
                v.clear();
                v.extend_from_slice(&packet[..]);
                self.client.on_finished_read(v);
            }
        }
    }

    fn handle_redo(&mut self, token: Token, kind: EntryKind::Kind, packet: &Buffer) {
        let write_state = match kind.layout() {
            EntryLayout::Data | EntryLayout::Multiput | EntryLayout::Sentinel => {
                let id = packet.entry().id;
                self.sent_writes.remove(&id)
            }
            EntryLayout::Read => {
                let read_loc = packet.entry().locs()[0];
                self.sent_reads.remove(&read_loc).map(WriteState::SingleServer)
            }
            EntryLayout::Lock => {
                // The only kind of send we do with a Lock layout is unlock
                // Without timeouts, failure indicates that someone else unlocked the server
                // for us, so we have nothing to do
                trace!("CLIENT unlock failure");
                None
            }
        };
        if let Some(state) = write_state {
            let re_add = self.server_for_token_mut(token).handle_redo(state, kind);
            if let Some(state) = re_add {
                assert!(state.is_multi());
                let id = state.id();
                self.sent_writes.insert(id, state);
            }
        }
    }

    fn add_unlocks(&mut self, mut buf: Rc<RefCell<Vec<u8>>>, lock_nums: Rc<HashMap<usize, u64>>) {
        match Rc::get_mut(&mut buf) {
            None => unreachable!(),
            Some(mut buf) => {
                let buf = buf.get_mut();
                trace!("buf pre clear {:?}", buf);
                buf.clear();
                trace!("buf after clear {:?}", buf);
                let lock = Lock {
                    //TODO id?
                    id: Uuid::nil(),
                    _padding: unsafe { mem::zeroed() },
                    kind: EntryKind::Lock,
                    lock: 0,
                };
                //TODO we shouldn't just free this...
                let lock = lock.bytes();
                trace!("lock bytes {:?}", lock);
                buf.extend_from_slice(lock);
                trace!("buflock {:?}", buf);
            }
        }
        assert_eq!(Entry::<()>::wrap_bytes(&buf.borrow()).kind.layout(), EntryLayout::Lock);
        assert_eq!(Entry::<()>::wrap_bytes(&buf.borrow()).entry_size(), buf.borrow().len());
        for (&server, &lock_num) in lock_nums.iter() {
            trace!("CLIENT add unlock for {:?}", (server, lock_num));
            self.servers[server].add_unlock(buf.clone(), lock_num);
            self.needs_to_write.insert(server);
        }
    }

    fn add_multis(&mut self, msg: Vec<u8>, lock_nums: HashMap<usize, u64>) {
        assert_eq!(Entry::<()>::wrap_bytes(&msg).entry_size(), msg.len());
        let lock_nums = Rc::new(lock_nums);
        let msg = Rc::new(RefCell::new(msg));
        for (&s, _) in lock_nums.iter() {
            self.servers[s].add_multi(msg.clone(), lock_nums.clone());
            self.needs_to_write.insert(s);
        }
    }

    fn add_single_server_send(&mut self, server: usize, msg: Vec<u8>) {
        assert_eq!(Entry::<()>::wrap_bytes(&msg).entry_size(), msg.len());
        self.needs_to_write.insert(server);
        self.server_for_token_mut(Token(server)).add_single_server_send(msg);
        self.needs_to_write.insert(server);
    }

    fn add_get_lock_nums(&mut self, mut msg: Vec<u8>) {
        Entry::<()>::wrap_bytes_mut(&mut msg).kind.insert(EntryKind::TakeLock);
        let lock_chains = self.get_lock_server_chains_for(&msg);
        //TODO we shouldn't just alloc this...
        let lock_req = EntryContents::Multiput {
               data: &msg, uuid: &Uuid::new_v4(), columns: &lock_chains, deps: &[],
        }.clone_bytes();
        let lock_server = self.lock_token();
        self.server_for_token_mut(lock_server).add_get_lock_nums(lock_req, msg);
        self.needs_to_write.insert(lock_server.as_usize());
    }

    fn is_single_node_append(&self, msg: &[u8]) -> bool {
        let mut single = true;
        let mut server_token = None;
        let locked_chains = Entry::<()>::wrap_bytes(&msg).locs()
            .iter().cloned().filter(|&oi| oi != (0.into(), 0.into()));
        for c in locked_chains {
            if let Some(server_token) = server_token {
                single &= self.server_for_chain(c.0) == server_token
            }
            else {
                server_token = Some(self.server_for_chain(c.0))
            }
        }
        single
    }

    fn get_lock_server_chains_for(&self, msg: &[u8]) -> Vec<OrderIndex> {
        //assert is multi?
        Entry::<()>::wrap_bytes(msg).locs()
            .iter().cloned()
            .filter(|&oi| oi != (0.into(), 0.into()))
            .fold((0..self.servers.len())
                .map(|_| false).collect::<Vec<_>>(),
            |mut v, (o, _)| {
                v[self.server_for_chain(o)] = true;
                v
            }).into_iter()
            .enumerate()
            .filter_map(|(i, present)|
                if present { Some(((i as u32 + 1).into(), 0.into())) }
                else { None })
            .collect::<Vec<OrderIndex>>()
    }

    fn server_for_chain(&self, chain: order) -> usize {
        server_for_chain(chain, self.num_chain_servers)
    }

    fn server_for_token(&self, token: Token) -> &PerServer {
        &self.servers[token.as_usize()]
    }

    fn server_for_token_mut(&mut self, token: Token) -> &mut PerServer {
        &mut self.servers[token.as_usize()]
    }

    fn lock_token(&self) -> Token {
        Token(self.num_chain_servers)
    }

    fn regregister_writes(&mut self, event_loop: &mut EventLoop<Self>) {
        for i in 0..self.servers.len() {
            if self.needs_to_write.contains(i) && !self.registered_for_write.contains(i) {
                self.register_for_write(event_loop, i);
                self.registered_for_write.insert(i);
            }
        }
    }

    fn reregister_for_read(&self, event_loop: &mut EventLoop<Self>, server: usize) {
        let stream = &self.server_for_token(Token(server)).stream;
        event_loop.reregister(stream, Token(server),
            mio::EventSet::readable() | mio::EventSet::error(),
            mio::PollOpt::edge() | mio::PollOpt::oneshot())
            .expect("could not reregister client socket")
    }

    fn register_for_write(&self, event_loop: &mut EventLoop<Self>, server: usize) {
        let stream = &self.server_for_token(Token(server)).stream;
        event_loop.reregister(stream, Token(server),
            mio::EventSet::readable() | mio::EventSet::writable() | mio::EventSet::error(),
            mio::PollOpt::edge() | mio::PollOpt::oneshot())
            .expect("could not reregister client socket")
    }
}

impl PerServer {
    fn read_packet(&mut self) -> Option<Buffer> {
        //TODO switch to nonblocking reads
        assert_eq!(self.bytes_handled, 0);
        if self.bytes_handled < base_header_size() {
            self.read_buffer.ensure_capacity(base_header_size());
            //TODO switch to read and async reads
            //self.bytes_handled +=
            self.stream.read_exact(
                &mut self.read_buffer[self.bytes_handled..base_header_size()])
                .expect("cannot read");
            self.bytes_handled += base_header_size();
        }
        let header_size = self.read_buffer.entry().header_size();
        assert!(header_size >= base_header_size());
        if self.bytes_handled < header_size {
            self.read_buffer.ensure_capacity(header_size);
            //let read =
            self.stream.read_exact(&mut self.read_buffer[self.bytes_handled..header_size])
                .expect("cannot read");
            self.bytes_handled = header_size;
        }
        //TODO ensure cap
        let size = self.read_buffer.entry().entry_size();
        if self.bytes_handled < size {
            self.read_buffer.ensure_capacity(size);
            self.stream.read_exact(&mut self.read_buffer[self.bytes_handled..size])
                .expect("cannot read");
            self.bytes_handled = size;
        }
        //TODO multipart reads
        self.bytes_handled = 0;
        let buff = mem::replace(&mut self.read_buffer, Buffer::new());
        Some(buff)
    }

    fn handle_redo(&mut self, failed: WriteState, kind: EntryKind::Kind) -> Option<WriteState> {
        let to_ret =
            if kind.contains(EntryKind::Multiput) {
                Some(failed.clone_multi())
            }
            else {
                None
            };
        //TODO front or back?
        self.awaiting_send.push_front(failed);
        to_ret
    }

    fn send_next_packet(&mut self, token: Token, num_servers: usize) -> Option<WriteState> {
        use self::WriteState::*;
        match self.awaiting_send.pop_front() {
            None => None,
            Some(MultiServer(to_send, lock_nums)) => {
                {
                    trace!("CLIENT PerServer {:?} multi", token);
                    let mut ts = to_send.borrow_mut();
                    let kind = {
                        Entry::<()>::wrap_bytes(&*ts).kind
                    };
                    assert!(kind.layout() == EntryLayout::Multiput
                        || kind.layout() == EntryLayout::Sentinel);
                    assert!(kind.contains(EntryKind::TakeLock));
                    let lock_num = lock_nums.get(&token.as_usize())
                        .expect("sending unlock for non-locked server");
                    let send_end = unsafe {
                        let e = Entry::<()>::wrap_bytes_mut(&mut *ts);
                        {
                            let is_data = e.locs().into_iter()
                                .take_while(|&&oi| oi != (0.into(), 0.into()))
                                .any(|oi| is_server_for(oi.0, token, num_servers));
                            let kind = &mut e.kind;
                            if is_data {
                                kind.remove(EntryKind::Lock);
                                assert_eq!(kind.layout(), EntryLayout::Multiput);
                            }
                            else {
                                kind.insert(EntryKind::Lock);
                                assert_eq!(kind.layout(), EntryLayout::Sentinel);
                            }
                            kind.insert(EntryKind::TakeLock);

                        }
                        e.as_multi_entry_mut().flex.lock = *lock_num;
                        //TODO this is either vec.len, or sentinelsize
                        e.entry_size()
                    };
                    let kind = {
                        Entry::<()>::wrap_bytes(&*ts).kind
                    };
                    assert!(kind.layout() == EntryLayout::Multiput
                        || kind.layout() == EntryLayout::Sentinel);
                    //TODO nonblocking writes
                    //Since sentinels have a different size than multis, we need to truncate
                    //for those sends
                    self.stream.write_all(&ts[..send_end]).expect("cannot write");
                }
                Some(MultiServer(to_send, lock_nums))
            }
            Some(UnlockServer(to_send, lock_num)) => {
                {
                    trace!("CLIENT PerServer {:?} unlock", token);
                    let mut ts = to_send.borrow_mut();
                    let tslen = ts.len();
                    unsafe {
                        let e = Entry::<()>::wrap_bytes_mut(&mut *ts);
                        assert_eq!(e.kind.layout(), EntryLayout::Lock);
                        e.as_multi_entry_mut().flex.lock = lock_num;
                        assert_eq!(e.kind.layout(), EntryLayout::Lock);
                        assert_eq!(e.entry_size(), tslen);
                    }
                    trace!("CLIENT willsend {:?}", &*ts);
                    //TODO nonblocking writes
                    self.stream.write_all(&*ts).expect("cannot write");
                }
                Some(UnlockServer(to_send, lock_num))
            }
            Some(to_send @ ToLockServer(_, _)) | Some(to_send @ SingleServer(_)) => {
                trace!("CLIENT PerServer {:?} single", token);
                unsafe {
                    let (l, s) = to_send.with_packet(|p| {
                        let e = Entry::<()>::wrap_bytes(&*p);
                        (e.kind.layout(), e.entry_size())
                    });
                    assert!(l == EntryLayout::Data || l == EntryLayout::Multiput
                        || l == EntryLayout::Read)
                }
                //TODO multipart writes?
                to_send.with_packet(|p| self.stream.write_all(p).expect("network err") );
                Some(to_send)
            }
        }
    }

    fn add_single_server_send(&mut self, msg: Vec<u8>) {
        self.awaiting_send.push_back(WriteState::SingleServer(msg));
    }

    fn add_multi(&mut self, msg: Rc<RefCell<Vec<u8>>>, lock_nums: Rc<HashMap<usize, u64>>) {
        self.awaiting_send.push_back(WriteState::MultiServer(msg, lock_nums));
    }

    fn add_unlock(&mut self, buffer: Rc<RefCell<Vec<u8>>>, lock_num: u64) {
        //unlike other reqs here we send the unlock first to minimize the contention window
        self.awaiting_send.push_front(WriteState::UnlockServer(buffer, lock_num))
    }

    fn add_get_lock_nums(&mut self, lock_req: Vec<u8>, msg: Vec<u8>) {
        self.awaiting_send.push_back(WriteState::ToLockServer(lock_req, msg))
    }

    fn needs_to_write(&self) -> bool {
        !self.awaiting_send.is_empty()
    }
}

impl WriteState {
    fn with_packet<F, R>(&self, f: F) -> R
    where F: for<'a> FnOnce(&'a [u8]) -> R {
        use self::WriteState::*;
        match self {
            &SingleServer(ref buf) | &ToLockServer(ref buf, _) => f(&**buf),
            &MultiServer(ref buf, _) | &UnlockServer(ref buf, _) => {
                let b = buf.borrow();
                f(&*b)
            },
        }
    }

    fn take(self) -> Vec<u8> {
        use self::WriteState::*;
        match self {
            SingleServer(buf) | ToLockServer(buf, _) => buf,
            MultiServer(buf, _) | UnlockServer(buf, _) =>
                Rc::try_unwrap(buf).expect("taking from non unique WriteState").into_inner()
        }
    }

    fn is_unlock(&self) -> bool {
        match self {
            &WriteState::UnlockServer(..) => true,
            _ => false,
        }
    }

    fn is_multi(&self) -> bool {
        match self {
            &WriteState::MultiServer(..) => true,
            _ => false,
        }
    }

    fn id(&self) -> Uuid {
        let mut id = unsafe { mem::uninitialized() };
        self.with_packet(|p| {
            id = Entry::<()>::wrap_bytes(p).id
        });
        id
    }

    fn layout(&self) -> EntryLayout {
        let mut layout = unsafe { mem::uninitialized() };
        self.with_packet(|p| {
            layout = Entry::<()>::wrap_bytes(p).kind.layout();
        });
        layout
    }

    fn read_loc(&self) -> OrderIndex {
        let mut loc = unsafe { mem::uninitialized() };
        self.with_packet(|p| {
            let e = Entry::<()>::wrap_bytes(p);
            assert!(e.kind.layout() != EntryLayout::Lock);
            loc = e.locs()[0]
        });
        loc
    }
    fn clone_multi(&self) -> WriteState {
        use self::WriteState::*;
        match self {
            &MultiServer(ref b, ref l) => MultiServer(b.clone(), l.clone()),
            s => panic!("invlaid clone multi on {:?}", s)
        }

    }
}

impl Buffer {
    fn new() -> Self {
        Buffer { inner: Vec::new() }
    }

    fn ensure_capacity(&mut self, capacity: usize) {
        if self.inner.capacity() < capacity {
            let curr_cap = self.inner.capacity();
            self.inner.reserve_exact(capacity - curr_cap);
            unsafe { self.inner.set_len(capacity) }
        }
    }

    fn get_lock_nums(&self) -> HashMap<usize, u64> {
        self.entry()
            .locs()
            .into_iter()
            .cloned()
            .map(|(o, i)| {
                let (o, i): (u32, u32) = ((o - 1).into(), i.into());
                (o as usize, i as u64)
            })
            .collect()
    }

    fn entry(&self) -> &Entry<()> {
        Entry::<()>::wrap_bytes(&self[..])
    }
}

//FIXME impl AsRef for Buffer...
impl ops::Index<ops::Range<usize>> for Buffer {
    type Output = [u8];
    fn index(&self, index: ops::Range<usize>) -> &Self::Output {
        &self.inner[index]
    }
}

impl ops::IndexMut<ops::Range<usize>> for Buffer {
    fn index_mut(&mut self, index: ops::Range<usize>) -> &mut Self::Output {
        //TODO should this ensure capacity?
        &mut self.inner[index]
    }
}

impl ops::Index<ops::RangeFull> for Buffer {
    type Output = [u8];
    fn index(&self, index: ops::RangeFull) -> &Self::Output {
        &self.inner[index]
    }
}

fn server_for_chain(chain: order, num_servers: usize) -> usize {
    <u32 as From<order>>::from(chain) as usize % num_servers
}

fn is_server_for(chain: order, tok: Token, num_servers: usize) -> bool {
    server_for_chain(chain, num_servers) == tok.as_usize()
}

#[cfg(test)]
pub mod sync_store_tests {
    use super::*;
    use packets::*;
    use prelude::{Store, InsertResult, GetResult, GetErr, FuzzyLog};

    use std::cell::RefCell;
    use std::mem;
    use std::net::SocketAddr;
    use std::rc::Rc;

    use mio::prelude::EventLoop;


    extern crate env_logger;
    use local_store::MapEntry;
    use local_store::test::Map;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[derive(Clone, Default)]
    pub struct Queues {
        finished_reads: VecDeque<Vec<u8>>,
        finished_writes: VecDeque<(Uuid, Vec<OrderIndex>)>,
    }

    pub type Client = Rc<RefCell<Queues>>;

    pub struct AsyncStoreToStore {
        store: AsyncTcpStore<Client>,
        client: Client,
        to_store: ::mio::Sender<Vec<u8>>,
        event_loop: EventLoop<AsyncTcpStore<Client>>,
    }

    impl AsyncStoreClient for Client {
        fn on_finished_read(&mut self, read_packet: Vec<u8>) {
            self.borrow_mut().finished_reads.push_back(read_packet)
        }

        fn on_finished_write(&mut self, write_id: Uuid, write_locs: Vec<OrderIndex>) {
            self.borrow_mut().finished_writes.push_back((write_id, write_locs))
        }
    }

    impl AsyncStoreToStore {
        pub fn new<I>(lock_server: SocketAddr, chain_servers: I, mut event_loop: EventLoop<AsyncTcpStore<Client>>) -> Self
        where I: IntoIterator<Item=SocketAddr> {
            let client: Client = Default::default();
            let store = AsyncTcpStore::new(lock_server, chain_servers,
                client.clone(), &mut event_loop).expect("");
            AsyncStoreToStore {
                store: store,
                client: client,
                to_store: event_loop.channel(),
                event_loop: event_loop,
            }
        }
    }

    impl<V: Storeable + ?Sized> Store<V> for AsyncStoreToStore {
        fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
            let mut buffer = EntryContents::Data(&(), &[]).clone_bytes();
            let read_entry_size = mem::size_of::<Entry<(), DataFlex<()>>>();
            if buffer.capacity() < read_entry_size {
                let add_cap = read_entry_size - buffer.capacity();
                buffer.reserve_exact(add_cap)
            }
            unsafe { buffer.set_len(read_entry_size) }
            {
                let e = Entry::<()>::wrap_bytes_mut(&mut buffer);
                e.kind = EntryKind::Read;
                e.locs_mut()[0] = key;
            }
            self.to_store.send(buffer).expect("Could not send to store");
            'wait: loop {
                let read = self.client.borrow_mut().finished_reads.pop_front();
                match read {
                    None => self.event_loop.run_once(&mut self.store, None).unwrap(),
                    Some(e) => {
                        let kind = Entry::<()>::wrap_bytes(&e).kind;
                        if kind.contains(EntryKind::ReadSuccess) {
                            return Ok(Entry::wrap_bytes(&e).clone())
                        }
                        else {
                            assert_eq!(kind, EntryKind::NoValue);
                            return Err(
                                GetErr::NoValue(
                                    Entry::<()>::wrap_bytes(&e).dependencies()[0].1))
                        }
                    }
                }
            }
        }

        fn insert(&mut self, key: OrderIndex, val: EntryContents<V>) -> InsertResult {
            let mut buffer = val.clone_bytes();
            {
                let e = Entry::<()>::wrap_bytes_mut(&mut buffer);
                assert_eq!(e.kind.layout(), EntryLayout::Data);
                e.id = Uuid::new_v4();
                e.locs_mut()[0] = key;
            }
            self.to_store.send(buffer).expect("Could not send to store");
            'wait: loop {
                let read = self.client.borrow_mut().finished_writes.pop_front();
                match read {
                    None => self.event_loop.run_once(&mut self.store, None).unwrap(),
                    Some(v) => return Ok(v.1[0]),
                }
            }
        }

        fn multi_append(&mut self, chains: &[OrderIndex], data: &V, deps: &[OrderIndex])
            -> InsertResult {
            let buffer = EntryContents::Multiput {
                    data: data,
                    uuid: &Uuid::new_v4(),
                    columns: chains,
                    deps: deps,
                }.clone_bytes();
            self.to_store.send(buffer).expect("Could not send to store");
            'wait: loop {
                let read = self.client.borrow_mut().finished_writes.pop_front();
                match read {
                    None => self.event_loop.run_once(&mut self.store, None).unwrap(),
                    Some(v) => return Ok(v.1[0]),
                }
            }
        }

        fn dependent_multi_append(&mut self, chains: &[order], depends_on: &[order],
            data: &V, deps: &[OrderIndex])-> InsertResult {
            let mchains: Vec<_> = chains.into_iter()
                .map(|&c| (c, 0.into()))
                .chain(::std::iter::once((0.into(), 0.into())))
                .chain(depends_on.iter().map(|&c| (c, 0.into())))
                .collect();
            let buffer = EntryContents::Multiput {
                    data: data,
                    uuid: &Uuid::new_v4(),
                    columns: &mchains,
                    deps: deps,
                }.clone_bytes();
            self.to_store.send(buffer).expect("Could not send to store");
            'wait: loop {
                let read = self.client.borrow_mut().finished_writes.pop_front();
                match read {
                    None => self.event_loop.run_once(&mut self.store, None).unwrap(),
                    Some(v) => return Ok(v.1[0]),
                }
            }
        }
    }

    impl Clone for AsyncStoreToStore {
        fn clone(&self) -> Self {
            unimplemented!()
        }
    }

    #[allow(non_upper_case_globals)]
    fn new_store(_: Vec<OrderIndex>) -> AsyncStoreToStore
    {
        use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
        use std::{thread, iter};

        use servers::tcp::Server;

        static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

        const lock_str: &'static str = "0.0.0.0:13289";
        const addr_strs: &'static [&'static str] = &["0.0.0.0:13290", "0.0.0.0:13291", "0.0.0.0:13292"];

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

    #[test]
    fn test_get_none() {
        let _ = env_logger::init();
        let mut store = new_store(Vec::new());
        let r = <Store<MapEntry<i32, i32>>>::get(&mut store, (14.into(), 0.into()));
        assert_eq!(r, Err(GetErr::NoValue(0.into())))
    }

    #[test]
    fn test_get_none2() {
        let _ = env_logger::init();
        let mut store = new_store(vec![(19.into(), 1.into()), (19.into(), 2.into()), (19.into(), 3.into()), (19.into(), 4.into()), (19.into(), 5.into())]);
        for i in 0..5 {
            let r = store.insert((19.into(), 0.into()), EntryContents::Data(&63, &[]));
            assert_eq!(r, Ok((19.into(), (i + 1).into())))
        }
        let r: Result<Entry<u32>, _> = store.get((19.into(), ::std::u32::MAX.into()));
        assert_eq!(r, Err(GetErr::NoValue(5.into())))
    }

    #[test]
    fn test_1_column() {
        let _ = env_logger::init();
        let store = new_store(
            vec![(3.into(), 1.into()), (3.into(), 2.into()),
            (3.into(), 3.into())]
        );
        let horizon = HashMap::new();
        let mut map = Map::new(store, horizon, 3.into());
        map.put(0, 1);
        map.put(1, 17);
        map.put(32, 5);
        assert_eq!(map.get(1), Some(17));
        assert_eq!(*map.local_view.borrow(), [(0,1), (1,17), (32,5)].into_iter().cloned().collect());
        assert_eq!(map.get(0), Some(1));
        assert_eq!(map.get(32), Some(5));
    }

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
    }

} // End mod test
