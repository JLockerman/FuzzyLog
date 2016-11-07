use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::mem;
use std::net::SocketAddr;
use std::rc::Rc;

use packets::*;

use bit_set::BitSet;

use mio;
use mio::prelude::*;
use mio::tcp::*;
use mio::Token;

pub type FinishedReadChannel = Rc<RefCell<VecDeque<Vec<u8>>>>;
pub type FinishedWriteChannel = Rc<RefCell<VecDeque<(Uuid, Vec<OrderIndex>)>>>;

trait AsyncStoreClient {
    //TODO nocopy?
    fn on_finished_read(&mut self, read_packet: Vec<u8>);
    //TODO what info is needed?
    fn on_finished_write(&mut self, write_id: Uuid, write_locs: Vec<OrderIndex>);
}

//TODO use FNV hash
pub struct AsyncTcpStore {
    sent_writes: HashMap<Uuid, WriteState>,
    sent_reads: HashMap<OrderIndex, Vec<u8>>,
    servers: Vec<PerServer>,
    registered_for_write: BitSet,
    needs_to_write: BitSet,
    num_chain_servers: usize,
    finished_reads: FinishedReadChannel,
    finished_writes: FinishedWriteChannel,
}

struct PerServer {
    awaiting_send: VecDeque<WriteState>,
    buffer: Vec<u8>,
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

impl AsyncTcpStore {
    pub fn new<I>(lock_server: SocketAddr, chain_servers: I,
        finished_reads: FinishedReadChannel,
        finished_writes: FinishedWriteChannel,
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
            finished_reads: finished_reads,
            finished_writes: finished_writes
        })
    }
}

impl PerServer {
    fn new(addr: SocketAddr) -> Result<Self, io::Error> {
        Ok(PerServer {
            awaiting_send: VecDeque::new(),
            buffer: Vec::new(), //TODO cap
            stream: try!(TcpStream::connect(&addr)),
            bytes_handled: 0,
        })
    }
}

impl mio::Handler for AsyncTcpStore {
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
            let kind = {
                let server = self.server_for_token_mut(token);
                //TODO incremental reads
                server.read_packet();
                //TODO incremental reads
                server.bytes_handled = 0;
                server.entry().kind
            };
            trace!("A recv {:?} for {:?}", kind, token);
            if kind.contains(EntryKind::ReadSuccess) {
                let num_chain_servers = self.num_chain_servers;
                self.handle_completion(token, num_chain_servers)
            }
            //TODO distinguish between locks and empties
            else if kind.layout() == EntryLayout::Read
                && !kind.contains(EntryKind::TakeLock) {
                //A read that found an usused entry still contains useful data
                self.handle_completed_read(token)
            }
            else {
                self.handle_redo(token, kind)
            }
        }
        if self.server_for_token(token).needs_to_write() && events.is_writable() {
            let num_chain_servers = self.num_chain_servers;
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

impl AsyncTcpStore {
    fn handle_completion(&mut self, token: Token, num_chain_servers: usize) {
        let write_completed = self.handle_completed_write(token, num_chain_servers);
        if write_completed {
            self.handle_completed_read(token);
        }
    }

    //TODO I should probably return the buffer on read_packet, and pass it into here
    fn handle_completed_write(&mut self, token: Token, num_chain_servers: usize) -> bool {
        let id = self.server_for_token(token).entry().id;
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
                    let lock_nums = self.get_lock_nums();
                    self.add_multis(msg, lock_nums);
                    return false
                }
                WriteState::MultiServer(buf, lock_nums) => {
                    assert!(token != self.lock_token());
                    trace!("CLIENT finished multi section");
                    let ready_to_unlock = {
                        let mut b = buf.borrow_mut();
                        let finished_writes = fill_locs(&mut b,
                            self.servers[token.as_usize()].entry(),
                            token, num_chain_servers);
                        mem::drop(b);
                        if finished_writes {
                            //TODO assert!(Rc::get_mut(&mut buf).is_some());
                            //let locs = buf.locs().to_vec();
                            let locs = self.servers[token.as_usize()].entry().locs().to_vec();
                            Some(locs)
                        } else { None }
                    };
                    match ready_to_unlock {
                        Some(locs) => {
                            self.add_unlocks(buf, lock_nums);
                            trace!("CLIENT finished multi at {:?}", locs);
                            //TODO
                            self.finished_writes.borrow_mut().push_back((id, locs));
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
                    fill_locs(&mut buf, self.servers[token.as_usize()].entry(),
                        token, num_chain_servers);
                    let locs = self.servers[token.as_usize()].entry().locs().to_vec();
                    self.finished_writes.borrow_mut().push_back((id, locs));
                    return true
                }
                WriteState::UnlockServer(..) => panic!("invalid wait state"),
            }

            fn fill_locs(buf: &mut [u8], e: &Entry<()>,
                server: Token, num_chain_servers: usize) -> bool {
                let locs = Entry::<()>::wrap_bytes_mut(buf).locs_mut();
                let mut remaining = locs.len();
                for (i, &loc) in e.locs().into_iter().enumerate() {
                    if server_for_chain(loc.0, num_chain_servers) == server.as_usize() {
                        trace!("sfc {:?} =? {:?}", loc.0, server);
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

    fn handle_completed_read(&mut self, token: Token) {
        if token == self.lock_token() { return }
        for &oi in self.servers[token.as_usize()].entry().locs() {
            if let Some(mut v) = self.sent_reads.remove(&oi) {
                //TODO num copies?
                v.clear();
                v.extend_from_slice(&self.servers[token.as_usize()].buffer);
                self.finished_reads.borrow_mut().push_back(v)
            }
        }
    }

    fn handle_redo(&mut self, token: Token, kind: EntryKind::Kind) {
        let write_state = match kind.layout() {
            EntryLayout::Data | EntryLayout::Multiput | EntryLayout::Sentinel => {
                let id = self.server_for_token(token).entry().id;
                self.sent_writes.remove(&id)
            }
            EntryLayout::Read => {
                let read_loc = self.server_for_token(token).entry().locs()[0];
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
                buf.clear();
                let lock = Lock {
                    //TODO id?
                    id: Uuid::nil(),
                    _padding: unsafe { mem::uninitialized() },
                    kind: EntryKind::Lock,
                    lock: 0,
                };
                buf.extend_from_slice(lock.bytes());
            }
        }
        for (&server, &lock_num) in lock_nums.iter() {
            self.servers[server].add_unlock(buf.clone(), lock_num);
            self.needs_to_write.insert(server);
        }
    }

    fn add_multis(&mut self, msg: Vec<u8>, lock_nums: HashMap<usize, u64>) {
        let lock_nums = Rc::new(lock_nums);
        let msg = Rc::new(RefCell::new(msg));
        for (&s, _) in lock_nums.iter() {
            self.servers[s].add_multi(msg.clone(), lock_nums.clone());
            self.needs_to_write.insert(s);
        }
    }

    fn add_single_server_send(&mut self, server: usize, msg: Vec<u8>) {
        self.needs_to_write.insert(server);
        self.server_for_token_mut(Token(server)).add_single_server_send(msg);
        self.needs_to_write.insert(server);
    }

    fn add_get_lock_nums(&mut self, mut msg: Vec<u8>) {
        Entry::<()>::wrap_bytes_mut(&mut msg).kind.insert(EntryKind::TakeLock);
        let lock_chains = self.get_lock_server_chains_for(&msg);
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

    fn get_lock_nums(&self) -> HashMap<usize, u64> {
        let lock_tok = self.lock_token();
        self.server_for_token(lock_tok)
            .entry()
            .locs()
            .into_iter()
            .cloned()
            .map(|(o, i)| {
                let (o, i): (u32, u32) = ((o - 1).into(), i.into());
                (o as usize, i as u64)
            })
            .collect()
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
    fn read_packet(&mut self) -> bool {
        //TODO switch to nonblocking reads
        assert_eq!(self.bytes_handled, 0);
        if self.bytes_handled < base_header_size() {
            if self.buffer.capacity() < base_header_size() {
                let cap = self.buffer.capacity();
                self.buffer.reserve_exact(base_header_size() - cap);
            }
            unsafe { self.buffer.set_len(base_header_size()) }
            //TODO switch to read and async reads
            //self.bytes_handled +=
            self.stream.read_exact(
                &mut self.buffer[self.bytes_handled..base_header_size()])
                .expect("cannot read");
            self.bytes_handled += base_header_size();
        }
        let header_size = Entry::<()>::wrap_bytes(&self.buffer).header_size();
        assert!(header_size >= base_header_size());
        if self.bytes_handled < header_size {
            if self.buffer.capacity() < header_size {
                let cap = self.buffer.capacity();
                self.buffer.reserve_exact(header_size - cap);
            }
            unsafe { self.buffer.set_len(header_size) }
            //let read =
            self.stream.read_exact(&mut self.buffer[self.bytes_handled..header_size])
                .expect("cannot read");
            self.bytes_handled = header_size;
        }
        //TODO ensure cap
        let size = Entry::<()>::wrap_bytes(&self.buffer).entry_size();
        if self.bytes_handled < size {
            if self.buffer.capacity() < size {
                let cap = self.buffer.capacity();
                self.buffer.reserve_exact(size - cap);
            }
            unsafe { self.buffer.set_len(size) }
            self.stream.read_exact(&mut self.buffer[self.bytes_handled..size])
                .expect("cannot read");
            self.bytes_handled = size;
        }
        //TODO multipart reads
        true
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

    fn send_next_packet(&mut self, token: Token, num_servers: usize) -> Option<WriteState> {
        use self::WriteState::*;
        match self.awaiting_send.pop_front() {
            None => None,
            Some(MultiServer(to_send, lock_nums)) => {
                {
                    let mut ts = to_send.borrow_mut();
                    let kind = {
                        Entry::<()>::wrap_bytes(&*ts).kind
                    };
                    assert_eq!(kind.layout(), EntryLayout::Multiput);
                    if kind.contains(EntryKind::TakeLock) {
                        let lock_num = lock_nums.get(&token.as_usize())
                            .expect("sending unlock for non-locked server");
                        unsafe {
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
                        }
                    }
                    //TODO nonblocking writes
                    self.stream.write_all(&ts).expect("cannot write");
                }
                Some(MultiServer(to_send, lock_nums))
            }
            Some(UnlockServer(to_send, lock_num)) => {
                {
                    let mut ts = to_send.borrow_mut();
                    unsafe {
                        assert_eq!(Entry::<()>::wrap_bytes(&*ts).kind.layout(),
                            EntryLayout::Lock);
                        Entry::<()>::wrap_bytes_mut(&mut *ts).as_multi_entry_mut().flex.lock =
                            lock_num;
                    }
                    //TODO nonblocking writes
                    self.stream.write_all(&*ts).expect("cannot write");
                }
                Some(UnlockServer(to_send, lock_num))
            }
            Some(to_send @ ToLockServer(_, _)) | Some(to_send @ SingleServer(_)) => {
                //TODO multipart writes?
                to_send.with_packet(|p| self.stream.write_all(p).expect("network err") );
                Some(to_send)
            }
        }
    }

    fn entry(&self) -> &Entry<()> {
        Entry::<()>::wrap_bytes(&self.buffer)
    }

    fn needs_to_write(&self) -> bool {
        !self.awaiting_send.is_empty()
    }
}

impl WriteState {
    fn with_packet<F>(&self, f: F)
    where F: for<'a> FnOnce(&'a [u8]) {
        use self::WriteState::*;
        match self {
            &SingleServer(ref buf) | &ToLockServer(ref buf, _) => f(&**buf),
            &MultiServer(ref buf, _) | &UnlockServer(ref buf, _) => {
                let b = buf.borrow();
                f(&*b);
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

fn server_for_chain(chain: order, num_servers: usize) -> usize {
    <u32 as From<order>>::from(chain) as usize % num_servers
}

fn is_server_for(chain: order, tok: Token, num_servers: usize) -> bool {
    server_for_chain(chain, num_servers) == tok.as_usize()
}

#[cfg(test)]
mod sync_store_tests {
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
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::thread;

    struct AsyncStoreToStore {
        store: AsyncTcpStore,
        finished_reads: FinishedReadChannel,
        finished_writes: FinishedWriteChannel,
        to_store: ::mio::Sender<Vec<u8>>,
        event_loop: EventLoop<AsyncTcpStore>,
    }

    impl AsyncStoreToStore {
        fn new<I>(lock_server: SocketAddr, chain_servers: I, mut event_loop: EventLoop<AsyncTcpStore>) -> Self
        where I: IntoIterator<Item=SocketAddr> {
            let finished_reads: FinishedReadChannel = Default::default();
            let finished_writes: FinishedWriteChannel = Default::default();
            let store = AsyncTcpStore::new(lock_server, chain_servers,
                finished_reads.clone(), finished_writes.clone(),
                &mut event_loop).expect("");
            AsyncStoreToStore {
                store: store,
                finished_reads: finished_reads,
                finished_writes: finished_writes,
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
                let read = self.finished_reads.borrow_mut().pop_front();
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
                let read = self.finished_writes.borrow_mut().pop_front();
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
                let read = self.finished_writes.borrow_mut().pop_front();
                match read {
                    None => self.event_loop.run_once(&mut self.store, None).unwrap(),
                    Some(v) => return Ok(v.1[0]),
                }
            }
        }

        fn dependent_multi_append(&mut self, chains: &[order], depends_on: &[order],
            data: &V, deps: &[OrderIndex])-> InsertResult {
            unimplemented!()
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

        const server_strs: [&'static str; 4] = ["0.0.0.0:13289", "0.0.0.0:13290", "0.0.0.0:13291", "0.0.0.0:13292"];
        for &addr_str in server_strs.iter() {
            let handle = thread::spawn(move || {

                let addr = addr_str.parse().expect("invalid inet address");
                let mut event_loop = EventLoop::new().unwrap();
                let server = Server::new(&addr, 0, 1, &mut event_loop);
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

        while SERVERS_READY.load(Ordering::Acquire) < server_strs.len() {}

        const lock_str: &'static str = "0.0.0.0:13289";
        const addr_strs: &'static [&'static str] = &["0.0.0.0:13290", "0.0.0.0:13291", "0.0.0.0:13292"];

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

} // End mod test
