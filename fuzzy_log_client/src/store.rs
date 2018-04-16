use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::mem;
use std::net::SocketAddr;
use std::time::Duration;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};

use packets::*;
use packets::buffer2::Buffer;
use packets::double_buffer::DoubleBuffer;

use hash::{HashMap, HashSet, UuidHashMap};
//use servers2::spsc;
use fuzzy_log_util::socket_addr::Ipv4SocketAddr;


use mio;
use mio::tcp::*;
use mio::Token;

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

// pub type FromClient =  spsc::Receiver<Vec<u8>>;
// pub type ToSelf = spsc::Sender<Vec<u8>>;
// fn channel() -> (ToSelf, FromClient) {
    // spsc::channel()
// }

pub struct AsyncTcpStore<Socket, C: AsyncStoreClient> {
    servers: Vec<PerServer<Socket>>,
    awake_io: VecDeque<usize>,
    sent_writes: UuidHashMap<WriteState>,
    //sent_reads: HashMap<OrderIndex, Vec<u8>>,
    sent_reads: HashMap<OrderIndex, u16>,
    waiting_buffers: VecDeque<Vec<u8>>,
    max_timestamp_seen: HashMap<order, u64>,
    num_chain_servers: usize,
    lock_token: Token,
    //FIXME change to spsc::Receiver<Buffer?>
    from_client: FromClient,
    client: C,
    is_unreplicated: bool,
    new_multi: bool,
    reads_my_writes: bool,
    finished: bool,

    print_data: StorePrintData,
}

counters!{
    struct StorePrintData {
        from_client: u64,
        finished_recvs: u64,
        finished_sends: u64,
        being_sent: u64,
    }
}

pub struct PerServer<Socket> {
    being_written: DoubleBuffer,
    being_sent: VecDeque<WriteState>,
    read_buffer: Buffer,
    stream: Socket,
    bytes_read: usize,
    bytes_sent: usize,
    token: Token,
    got_new_message: bool,
    receiver: Ipv4SocketAddr,
    stay_awake: bool,

    //TODO store options instead?
    dead: bool,

    print_data: PerServerPrintData,
}

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

//FIXME ugly hack to ease impl, put in constructor
static CLIENT_ID: AtomicUsize = ATOMIC_USIZE_INIT;

fn client_id() -> Ipv4SocketAddr {
    let id = CLIENT_ID.load(Ordering::Relaxed);
    if id != 0 {
        Ipv4SocketAddr::from_u64(client_id as u64)
    } else {
        Ipv4SocketAddr::random()
    }
}

pub fn set_client_id(id: usize) {
    CLIENT_ID.store(id, Ordering::Relaxed);
}

//TODO rename to AsyncStore
impl<C> AsyncTcpStore<TcpStream, C>
where C: AsyncStoreClient {

    //TODO should probably move all the poll register stuff too run
    pub fn tcp<I>(lock_server: SocketAddr, chain_servers: I, client: C,
        event_loop: &mut mio::Poll)
    -> Result<(Self, ToSelf), io::Error>
    where I: IntoIterator<Item=SocketAddr> {
        //TODO assert no duplicates
        let id = client_id();
        trace!("Starting Store {}", id);
        let mut servers = try!(chain_servers
            .into_iter()
            .map(PerServer::tcp)
            .collect::<Result<Vec<_>, _>>());
        assert!(servers.len() > 0);
        let num_chain_servers = servers.len();
        let lock_server = try!(PerServer::tcp(lock_server));
        //TODO if let Some(lock_server) = lock_server...
        let lock_token = Token(servers.len());
        servers.push(lock_server);
        for (i, server) in servers.iter_mut().enumerate() {
            server.token = Token(i);
            server.receiver = id;
            event_loop.register(server.connection(), server.token,
                mio::Ready::readable() | mio::Ready::writable() | mio::Ready::error(),
                mio::PollOpt::edge())
                .expect("could not reregister client socket")
        }
        for server in servers.iter_mut().rev() {
            blocking_read(&mut server.stream, &mut [0]).unwrap();
            blocking_write(&mut server.stream, &[2]).unwrap();
            blocking_write(&mut server.stream, &*server.receiver.bytes()).unwrap();
        }
        let mut ack = [0; 16];
        for server in servers.iter_mut().rev() {
            blocking_read(&mut server.stream, &mut ack[..]).unwrap();
            assert_eq!(Ipv4SocketAddr::from_bytes(ack), server.receiver);
        }
        let awake_io = servers.iter().map(|s| s.token.0).collect();
        let from_client_token = Token(servers.len());
        let (to_store, from_client) = channel();
        event_loop.register(&from_client,
            from_client_token,
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::edge()
        ).expect("could not reregister client channel");
        Ok((AsyncTcpStore {
            sent_writes: Default::default(),
            sent_reads: Default::default(),
            waiting_buffers: Default::default(),
            servers: servers,
            num_chain_servers: num_chain_servers,
            lock_token: lock_token,
            client: client,
            awake_io: awake_io,
            from_client: from_client,
            is_unreplicated: true,
            new_multi: false,
            finished: false,
            reads_my_writes: false,

            max_timestamp_seen: Default::default(),

            print_data: Default::default(),
        }, to_store))
    }

    pub fn new_tcp<I>(
        id: Ipv4SocketAddr,
        chain_servers: I,
        client: C,
        event_loop: &mut mio::Poll
    ) -> Result<(Self, ToSelf), io::Error>
    where I: IntoIterator<Item=SocketAddr> {
        //TODO assert no duplicates
        trace!("Starting Store {} server addrs:", id);
        let mut servers = try!(chain_servers
            .into_iter()
            .inspect(|addr| trace!("{:?}", addr))
            .map(PerServer::tcp)
            .collect::<Result<Vec<_>, _>>());
        assert!(servers.len() > 0);
        let num_chain_servers = servers.len();
        trace!("Client {:?} servers", num_chain_servers);
        for (i, server) in servers.iter_mut().enumerate() {
            server.token = Token(i);
            server.receiver = id;
            event_loop.register(server.connection(), server.token,
                mio::Ready::readable() | mio::Ready::writable() | mio::Ready::error(),
                mio::PollOpt::edge())
                .expect("could not reregister client socket")
        }
        for server in servers.iter_mut().rev() {
            blocking_read(&mut server.stream, &mut [0]).unwrap();
            blocking_write(&mut server.stream, &[2]).unwrap();
            blocking_write(&mut server.stream, &*server.receiver.bytes()).unwrap();
        }
        let mut ack = [0; 16];
        for server in servers.iter_mut().rev() {
            blocking_read(&mut server.stream, &mut ack[..]).unwrap();
            assert_eq!(Ipv4SocketAddr::from_bytes(ack), server.receiver);
        }
        let awake_io = servers.iter().map(|s| s.token.0).collect();
        let from_client_token = Token(servers.len());
        let (to_store, from_client) = channel();
        event_loop.register(&from_client,
            from_client_token,
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::edge()
        ).expect("could not reregister client channel");
        Ok((AsyncTcpStore {
            sent_writes: Default::default(),
            sent_reads: Default::default(),
            waiting_buffers: Default::default(),
            servers: servers,
            num_chain_servers: num_chain_servers,
            lock_token: Token(::std::usize::MAX),
            client: client,
            awake_io: awake_io,
            from_client: from_client,
            is_unreplicated: true,
            new_multi: true,
            finished: false,
            reads_my_writes: false,

            max_timestamp_seen: Default::default(),

            print_data: Default::default(),
        }, to_store))
    }

    pub fn replicated_new_tcp<I>(
        id: Ipv4SocketAddr,
        chain_servers: I,
        client: C,
        event_loop: &mut mio::Poll
    ) -> Result<(Self, ToSelf), io::Error>
    where I: IntoIterator<Item=(SocketAddr, SocketAddr)> {
        //TODO assert no duplicates
        trace!("Starting Store {:?} server addrs:", id);
        let (write_servers, read_servers): (Vec<_>, Vec<_>) =
            chain_servers.into_iter().inspect(|addrs| trace!("{:?}", addrs)).unzip();

        let mut servers = try!(write_servers
            .into_iter()
            .map(PerServer::tcp)
            .collect::<Result<Vec<_>, _>>());
        let read_servers = try!(read_servers
                .into_iter()
                .map(PerServer::tcp)
                .collect::<Result<Vec<_>, _>>());
        let num_chain_servers = servers.len();
        assert_eq!(num_chain_servers, read_servers.len());
        assert!(num_chain_servers > 0);
        servers.extend(read_servers.into_iter());

        trace!("Client {:?} servers", num_chain_servers);
        for (i, server) in servers.iter_mut().enumerate() {
            server.token = Token(i);
            server.receiver = id;
            event_loop.register(server.connection(), server.token,
                mio::Ready::readable() | mio::Ready::writable() | mio::Ready::error(),
                mio::PollOpt::edge())
                .expect("could not reregister client socket")
        }
        for server in servers.iter_mut().rev() {
            blocking_read(&mut server.stream, &mut [0]).unwrap();
            blocking_write(&mut server.stream, &[2]).unwrap();
            blocking_write(&mut server.stream, &*server.receiver.bytes()).unwrap();
        }
        let mut ack = [0; 16];
        for server in servers.iter_mut().rev() {
            blocking_read(&mut server.stream, &mut ack[..]).unwrap();
            assert_eq!(Ipv4SocketAddr::from_bytes(ack), server.receiver);
        }
        trace!("Client servers {:?}",
            servers.iter().map(|s| s.connection().local_addr()).collect::<Vec<_>>()
        );
        let awake_io = servers.iter().map(|s| s.token.0).collect();
        let from_client_token = Token(servers.len());
        let (to_store, from_client) = channel();
        event_loop.register(&from_client,
            from_client_token,
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::edge()
        ).expect("could not reregister client channel");
        Ok((AsyncTcpStore {
            sent_writes: Default::default(),
            sent_reads: Default::default(),
            waiting_buffers: Default::default(),
            servers: servers,
            num_chain_servers: num_chain_servers,
            lock_token: Token(::std::usize::MAX),
            client: client,
            awake_io: awake_io,
            from_client: from_client,
            is_unreplicated: false,
            new_multi: true,
            finished: false,
            reads_my_writes: false,

            max_timestamp_seen: Default::default(),

            print_data: Default::default(),
        }, to_store))
    }

    pub fn replicated_tcp<I>(
        lock_server: Option<SocketAddr>,
        chain_servers: I,
        client: C,
        event_loop: &mut mio::Poll
    ) -> Result<(Self, ToSelf), io::Error>
    where I: IntoIterator<Item=(SocketAddr, SocketAddr)>
    {
        let id = client_id();
        //TODO assert no duplicates
        let (write_servers, read_servers): (Vec<_>, Vec<_>) =
            chain_servers.into_iter().unzip();
        let mut servers = try!(write_servers
            .into_iter()
            .map(PerServer::tcp)
            .collect::<Result<Vec<_>, _>>());
        let read_servers = try!(read_servers
                .into_iter()
                .map(PerServer::tcp)
                .collect::<Result<Vec<_>, _>>());
        let num_chain_servers = servers.len();
        assert_eq!(num_chain_servers, read_servers.len());
        assert!(num_chain_servers > 0);
        servers.extend(read_servers.into_iter());

        let lock_token = if let Some(lock_server) = lock_server {
            let lock_server = try!(PerServer::tcp(lock_server));
            servers.push(lock_server);
            Token(servers.len() - 1)
        } else { Token(servers.len() + 1) };
        //TODO if let Some(lock_server) = lock_server...
        for (i, server) in servers.iter_mut().enumerate() {
            server.token = Token(i);
            server.receiver = id;
            event_loop.register(server.connection(), server.token,
                mio::Ready::readable() | mio::Ready::writable() | mio::Ready::error(),
                mio::PollOpt::edge())
                .expect("could not reregister client socket")
        }
        for server in servers.iter_mut().rev() {
            blocking_read(&mut server.stream, &mut [0]).unwrap();
            blocking_write(&mut server.stream, &[2]).unwrap();
            blocking_write(&mut server.stream, &*server.receiver.bytes()).unwrap();
        }
        let mut ack = [0; 16];
        for server in servers.iter_mut().rev() {
            blocking_read(&mut server.stream, &mut ack[..]).unwrap();
            assert_eq!(Ipv4SocketAddr::from_bytes(ack), server.receiver);
        }
        let awake_io = servers.iter().map(|s| s.token.0).collect();
        let from_client_token = Token(servers.len());
        let (to_store, from_client) = channel();
        event_loop.register(&from_client,
            from_client_token,
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::edge()
        ).expect("could not reregister client channel");
        Ok((AsyncTcpStore {
            sent_writes: Default::default(),
            sent_reads: Default::default(),
            waiting_buffers: Default::default(),
            servers: servers,
            num_chain_servers: num_chain_servers,
            lock_token: lock_token,
            client: client,
            awake_io: awake_io,
            from_client: from_client,
            is_unreplicated: false,
            new_multi: false,
            finished: false,
            reads_my_writes: false,

            max_timestamp_seen: Default::default(),

            print_data: Default::default(),
        }, to_store))
    }

    pub fn set_reads_my_writes(&mut self, reads_my_writes: bool) {
        self.reads_my_writes = reads_my_writes
    }
}

impl PerServer<TcpStream> {
    fn tcp(addr: SocketAddr) -> Result<Self, io::Error> {
        let stream = try!(TcpStream::connect(&addr));
        let _ = stream.set_nodelay(true);
        Ok(PerServer {
            being_written: DoubleBuffer::with_first_buffer_capacity(4096),
            being_sent: Default::default(),
            read_buffer: Buffer::new(), //TODO cap
            //read_buffer: Buffer::no_drop(), //TODO cap
            stream: stream,
            bytes_read: 0,
            bytes_sent: 0,
            token: Token(::std::usize::MAX),
            got_new_message: false,
            receiver: Ipv4SocketAddr::from_uuid(&Uuid::nil()),
            stay_awake: true,
            dead: false,

            print_data: Default::default(),
        })
    }

    fn connection(&self) -> &TcpStream {
        &self.stream
    }
}

impl<S, C> AsyncTcpStore<S, C>
where PerServer<S>: Connected,
      C: AsyncStoreClient {
    pub fn run(mut self, poll: mio::Poll) {
        trace!("CLIENT start.");
        let mut events = mio::Events::with_capacity(1024);
        let mut timeout_idx = 0;
        while !self.finished || !self.sent_writes.is_empty() {
            const TIMEOUTS: [(u64, u32); 9] =
                [(0, 10_000), (0, 100_000), (0, 500_000), (0, 1_000_000),
                (0, 10_000_000), (0, 100_000_000), (1, 0), (10, 0), (10, 0)];
            //poll.poll(&mut events, None).expect("worker poll failed");
            //let _ = poll.poll(&mut events, Some(Duration::from_secs(10)));
            let timeout = TIMEOUTS[timeout_idx];
            let timeout = Duration::new(timeout.0, timeout.1);
            // let _ = poll.poll(&mut events, Some(timeout));
            let _ = poll.poll(&mut events, Some(Duration::from_secs(10)));
            if false && events.len() == 0 {
                #[cfg(feature = "print_stats")]
                {
                    if TIMEOUTS[timeout_idx].0 >= 10 {
                        self.print_stats()
                    }
                }
                for ps in self.servers.iter() {
                    if !ps.dead {
                        self.awake_io.push_back(ps.token.0)
                    }
                }
                'new_reqs: loop {
                    if !self.handle_new_requests_from_client() {
                        break 'new_reqs
                    }
                }
                if !self.awake_io.is_empty() {
                    if timeout_idx + 1 < TIMEOUTS.len() {
                        timeout_idx += 1
                    }
                } else {
                    if timeout_idx > 0 {
                       timeout_idx -= 1
                    }
                }
            } else {
                if timeout_idx > 0 {
                    timeout_idx -= 1
                }
            }

            self.handle_new_events(&poll, events.iter());


            'work: loop {
                //TODO add recv queue to this loop?
                let ops_before_poll = self.awake_io.len();
                'poll: for _ in 0..ops_before_poll {
                    let server = self.awake_io.pop_front().unwrap();
                    if self.servers[server].dead { continue 'poll }
                    let err = self.handle_server_event(server);
                    if err.is_err() {
                        let ps = &mut self.servers[server];
                        let _ = poll.deregister(ps.connection());
                        ps.close()
                    }
                }
                if self.awake_io.is_empty() {
                    break 'work
                }
                let _ = poll.poll(&mut events, Some(Duration::from_millis(0)));
                self.handle_new_events(&poll, events.iter());
            }
            #[cfg(debug_assertions)]
            for s in self.servers.iter() {
                debug_assert!(!s.stay_awake,
                    "Token {:?} sleep @ stay_awake: {}", s.token, s.stay_awake);
            }
        }
    }// end fn run

    #[cfg(feature = "print_stats")]
    fn print_stats(&self) {
        println!("store: {:#?}", self.print_data);
        for ps in self.servers.iter() {
            println!("store {:?}: {:#?}", ps.token, ps.print_data);
        }
    }

    #[allow(dead_code)]
    #[cfg(not(feature = "print_stats"))]
    #[inline(always)]
    fn print_stats(&self) {}

    fn handle_new_events(&mut self, poll: &mio::Poll, events: mio::EventsIter) {
        for event in events {
            let token = event.token();
            if token.0 >= self.servers.len() {
                'new_reqs: loop { //for _ in 0..1000 {
                    if !self.handle_new_requests_from_client() {
                        break 'new_reqs
                    }
                }
            }
            else {
                debug_assert!(token.0 < self.servers.len());
                let err = self.handle_server_event(event.token().0);
                if err.is_err() {
                    let server = event.token().0;
                    let ps = &mut self.servers[server];
                    let _ = poll.deregister(ps.connection());
                    ps.close()
                }
            }
        }
    }

    fn handle_new_requests_from_client(&mut self) -> bool {
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
                //TODO if writeable write?
                self.add_single_server_send(s, msg)
            }
            EntryLayout::Data => {
                let loc;
                {

                    let mut contents = bytes_as_entry_mut(&mut msg);
                    loc = contents.as_ref().locs()[0].0;
                    let timestamp = self.max_timestamp_seen.get(&loc).cloned().unwrap_or_else(|| 0);
                    *contents.lock_mut() = timestamp;
                    // assert!(*contents.lock_mut() >= 1, "{:#?}", self.max_timestamp_seen);
                    // assert!(contents.as_ref().lock_num() >= 1);
                }
                let s = self.write_server_for_chain(loc);
                trace!("CLIENT will write {:?}, server {:?}:{:?}",
                    loc, s, self.num_chain_servers);
                //TODO if writeable write?
                self.add_single_server_send(s, msg)
            }
            EntryLayout::Multiput => {
                trace!("CLIENT will multi write");
                //FIXME set max_timestamp from local
                //TODO
                let use_fastpath = true;
                if use_fastpath && self.is_single_node_append(&msg) {
                    {
                        let mut contents = bytes_as_entry_mut(&mut msg);
                        *contents.lock_mut() = 1;
                    }
                    let chain = bytes_as_entry(&msg).locs()[0].0;
                    let s = self.write_server_for_chain(chain);
                    self.add_single_server_send(s, msg)
                } else if self.new_multi {
                    let mut msg = msg;
                    {
                        let mut e = bytes_as_entry_mut(&mut msg);
                        e.flag_mut().insert(EntryFlag::TakeLock | EntryFlag::NewMultiPut);
                        e.locs_mut().into_iter()
                            .fold((), |_, &mut OrderIndex(_,ref mut i)| *i = 0.into());
                    }
                    self.add_skeens1(msg);
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
                    self.add_single_server_send(s, msg)
                } else {
                    let mut msg = msg;
                    assert!(self.new_multi);
                    bytes_as_entry_mut(&mut msg)
                        .flag_mut().insert(EntryFlag::TakeLock | EntryFlag::NewMultiPut);
                    self.add_snapshot_skeens1(msg);
                    true
                }
            }

            EntryLayout::GC => {
                //TODO be less lazy
                self.add_gc(msg)
            },
            r @ EntryLayout::Sentinel | r @ EntryLayout::Lock =>
                panic!("Invalid send request {:?}", r),
        }
    } // End fn handle_new_requests_from_client

    fn add_single_server_send(&mut self, server: usize, msg: Vec<u8>) -> bool {
        assert_eq!(bytes_as_entry(&msg).len(), msg.len());
        //let per_server = self.server_for_token_mut(Token(server));
        let per_server = &mut self.servers[server];
        debug_assert_eq!(per_server.token, Token(server));
        per_server.add_single_server_send(msg);
        if !per_server.got_new_message {
            per_server.got_new_message = true;
            self.awake_io.push_back(server)
        }
        for sent in per_server.being_sent.drain(..) {
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
        }
        true
    }

    fn add_gc(&mut self, msg: Vec<u8>) -> bool {
        debug_assert_eq!(bytes_as_entry(&msg).len(), msg.len());
        let mut servers = self.get_servers_for_multi(&msg);
        let last_server = servers.pop().unwrap();
        for s in servers {
            let per_server = &mut self.servers[s];
            per_server.add_gc(msg.clone());
            if !per_server.got_new_message {
                per_server.got_new_message = true;
                self.awake_io.push_back(s)
            }
        }
        let per_server = &mut self.servers[last_server];
        per_server.add_gc(msg);
        if !per_server.got_new_message {
            per_server.got_new_message = true;
            self.awake_io.push_back(last_server)
        }
        true
    }

    fn add_skeens1(&mut self, msg: Vec<u8>) {
        debug_assert_eq!(bytes_as_entry(&msg).len(), msg.len());
        let (msg, servers, remaining_servers, timestamps) = self.prep_skeens1(msg);
        trace!("CLIENT multi to {:?}", remaining_servers);
        for s in servers {
            let per_server = &mut self.servers[s];
            per_server.add_skeens1(
                msg.clone(),
                remaining_servers.clone(),
                timestamps.clone(),
                self.num_chain_servers,
            );
            if !per_server.got_new_message {
                per_server.got_new_message = true;
                self.awake_io.push_back(s)
            }
        }
    }

    fn add_skeens2(&mut self, mut buf: Rc<RefCell<Vec<u8>>>, max_ts: u64) {
        let servers = match Rc::get_mut(&mut buf) {
            None => unreachable!(),
            Some(mut buf) => {
                let buf = buf.get_mut();
                let server = self.get_servers_for_multi(&buf);
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
                server
            }
        };
        let mut remaining_servers: HashSet<usize> = Default::default();
        remaining_servers.reserve(servers.len());
        for &writer in servers.iter() {
            remaining_servers.insert(self.read_server_for_write_server(writer));
        }
        let remaining_servers = Rc::new(RefCell::new(remaining_servers));
        for s in servers {
            let per_server = &mut self.servers[s];
            per_server.add_skeens2(
                buf.clone(),
                remaining_servers.clone(),
                max_ts,
            );
            if !per_server.got_new_message {
                per_server.got_new_message = true;
                self.awake_io.push_back(s)
            }
        }
    }

    fn add_snapshot_skeens1(&mut self, msg: Vec<u8>) {
        let (msg, servers, remaining_servers, timestamps) = self.prep_skeens1(msg);
        for s in servers {
            let per_server = &mut self.servers[s];
            per_server.add_snapshot_skeens1(
                msg.clone(),
                remaining_servers.clone(),
                timestamps.clone(),
            );
            if !per_server.got_new_message {
                per_server.got_new_message = true;
                self.awake_io.push_back(s)
            }
        }
    }

    fn add_snapshot_skeens2(&mut self, mut buf: Rc<RefCell<Vec<u8>>>, max_ts: u64) {
        let servers = match Rc::get_mut(&mut buf) {
            None => unreachable!(),
            Some(mut buf) => {
                let buf = buf.get_mut();
                let server = self.get_servers_for_multi(&buf);
                let size = {
                    let mut e = bytes_as_entry_mut(buf);
                    e.flag_mut().insert(EntryFlag::Unlock | EntryFlag::NewMultiPut);
                    e.locs_mut().iter_mut()
                        .fold((), |_, &mut OrderIndex(_, ref mut i)| *i = entry::from(0));
                    *e.lock_mut() = max_ts;
                    e.as_ref().len()
                };
                unsafe { buf.set_len(size) };
                server
            }
        };
        let mut remaining_servers: HashSet<usize> = Default::default();
        remaining_servers.reserve(servers.len());
        for &writer in servers.iter() {
            remaining_servers.insert(self.read_server_for_write_server(writer));
        }
        let remaining_servers = Rc::new(RefCell::new(remaining_servers));
        for s in servers {
            let per_server = &mut self.servers[s];
            per_server.add_snapshot_skeens2(
                buf.clone(),
                remaining_servers.clone(),
                max_ts,
            );
            if !per_server.got_new_message {
                per_server.got_new_message = true;
                self.awake_io.push_back(s)
            }
        }
    }

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

    fn handle_server_event(&mut self, server: usize) -> Result<(), ()> {
        // trace!("CLIENT handle server {:?} event", server);
        self.servers[server].got_new_message = false;
        //TODO pass in whether a read or write is ready?
        self.servers[server].stay_awake = false;
        //let mut stay_awake = false;
        let token = Token(server);
        debug_assert_eq!(token, self.servers[server].token);
        //let finished_recv = self.servers[server].recv_packet().expect("cannot recv");
        let finished_recv = self.servers[server].recv_packet();
        match finished_recv {
            Err(io_err) => {
                let e = self.client.on_io_error(io_err, server);
                if e.is_err() {
                    self.finished = true;
                }
                return Err(())
            },
            Ok(Some(mut packet)) => {
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
                    self.handle_redo(Token(server), kind, &packet)
                }
                packet.finished_entry();
                self.servers.get_mut(server).unwrap().read_buffer = packet;
            }
            Ok(None) => {}
        }

        if self.servers[server].needs_to_write() {
            // trace!("CLIENT write");
            self.print_data.finished_sends(1);
            //let num_chain_servers = self.num_chain_servers;
            let finished_send = self.servers[server].send_next_burst();
            if finished_send {
                // trace!("CLIENT finished a send to {:?}", token);
                //stay_awake = true;
                //TODO used to be drain being_sent
            }
        }

        for sent in self.servers[server].being_sent.drain(..) {
            self.print_data.being_sent(1);
            let layout = sent.layout();
            if layout == EntryLayout::Read {
                let read_loc = sent.read_loc();
                *self.sent_reads.entry(read_loc).or_insert(0) += 1;
                self.waiting_buffers.push_back(sent.take())
            } else if !sent.is_unlock() {
                let id = sent.id();
                self.sent_writes.insert(id, sent);
            }
        }

        if self.servers[server].stay_awake == true {
            self.awake_io.push_back(server)
        }

        Ok(())
    } // end fn handle_server_event

    fn handle_completion(&mut self, token: Token, num_chain_servers: usize, packet: &mut Buffer) {
        //FIXME remove
        //let mut was_needed = self.sent_writes.contains_key(&packet.entry().id);
        let write_completed = self.handle_completed_write(token, num_chain_servers, packet);
        if let Ok(my_write) = write_completed {
            self.handle_completed_read(token, &*packet, my_write);
        }
        //FIXME remove
        //if !was_needed {
            //println!("Uneeded fetch @ {:?}: {:?}", token, packet.entry());
        //}
    }

    //TODO I should probably return the buffer on recv_packet, and pass it into here
    fn handle_completed_write(&mut self, token: Token, num_chain_servers: usize,
        packet: &mut Buffer) -> Result<bool, ()> {
        let (id, kind, flag) = {
            let e = packet.contents();
            (*e.id(), e.kind(), *e.flag())
        };
        let unreplicated = self.is_unreplicated;
        trace!("CLIENT handle completed write?");
        //FIXME remove extra index?
        self.servers[token.0].print_data.finished_appends(1);
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

    fn handle_completed_read(&mut self, token: Token, packet: &Buffer, my_write: bool) -> bool {
        use std::collections::hash_map::Entry::Occupied;

        // trace!("CLIENT handle completed read?");
        if !self.new_multi && token == self.lock_token() { return false }
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
        self.servers[token.0].print_data.finished_read(1);
        //FIXME remove
        let mut was_needed = false;
        let mut is_sentinel_loc = false;
        let contents = packet.contents();
        let max_ts = contents.lock_num();
        let num_locs = contents.locs().len();
        // if num_locs > 1 && contents.flag().contains(EntryFlag::TakeLock) {
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

    fn handle_redo(&mut self, token: Token, kind: EntryKind::Kind, packet: &Buffer) {
        self.servers[token.0].print_data.redo(1);
        let write_state = match kind.layout() {
            EntryLayout::Data | EntryLayout::Multiput | EntryLayout::Sentinel => {
                let id = packet.contents().id();
                self.sent_writes.remove(id)
            }
            EntryLayout::Read => {
                let read_loc = packet.contents().locs()[0];
                if self.sent_reads.contains_key(&read_loc) {
                    let mut b = self.waiting_buffers.pop_front()
                        .unwrap_or_else(|| Vec::with_capacity(50));
                    EntryContents::Read{
                        id: &Uuid::nil(),
                        flags: &EntryFlag::Nothing,
                        data_bytes: &0,
                        dependency_bytes: &0,
                        loc: &read_loc,
                        horizon: &OrderIndex(0.into(), 0.into()),
                        min: &OrderIndex(0.into(), 0.into()),
                    }.fill_vec(&mut b);
                    //FIXME better read packet handling
                    Some(WriteState::SingleServer(b))
                } else { None }
            }
            EntryLayout::Snapshot => {
                unimplemented!()
            }
            EntryLayout::Lock => {
                // The only kind of send we do with a Lock layout is unlock
                // Without timeouts, failure indicates that someone else unlocked the server
                // for us, so we have nothing to do
                trace!("CLIENT unlock failure");
                None
            }
            EntryLayout::GC => None,
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

    fn read_server_for_chain(&self, chain: order) -> usize {
        let server = if self.is_unreplicated {
            write_server_for_chain(chain, self.num_chain_servers)
        }
        else {
            read_server_for_chain(chain, self.num_chain_servers, self.is_unreplicated)
        };
        debug_assert!(server < self.servers.len());
        server
    }

    fn read_server_for_write_server(&self, write_server: usize) -> usize {
        debug_assert!(write_server < self.servers.len());
        let server = if self.is_unreplicated {
            write_server
        } else {
            write_server + self.num_chain_servers
        };
        debug_assert!(server < self.servers.len());
        server
    }

    fn write_server_for_chain(&self, chain: order) -> usize {
        write_server_for_chain(chain, self.num_chain_servers)
    }

    fn server_for_token_mut(&mut self, token: Token) -> &mut PerServer<S> {
        &mut self.servers[token.0]
    }

    fn lock_token(&self) -> Token {
        debug_assert!(!self.new_multi);
        debug_assert!(self.num_chain_servers < self.servers.len());
        //Token(self.num_chain_servers * 2)
        self.lock_token
    }

} // end impl AsyncStore

pub trait Connected {
    type Connection: mio::Evented;

    fn connection(&self) -> &Self::Connection;

    //TODO should just be fn drop
    fn close(&mut self);

    fn handle_redo(&mut self, failed: WriteState, _kind: EntryKind::Kind)
    -> Option<WriteState>;

    fn recv_packet(&mut self) -> Result<Option<Buffer>, io::Error>;
    fn send_next_burst(&mut self) -> bool;

    fn add_send(&mut self, to_send: WriteState);

    fn add_single_server_send(&mut self, msg: Vec<u8>) {
        let send = WriteState::SingleServer(msg);
        self.add_send(send)
    }

    fn add_gc(&mut self, buffer: Vec<u8>) {
        let send = WriteState::GC(buffer);
        self.add_send(send)
    }

    fn add_skeens1(
        &mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        timestamps: Rc<RefCell<Box<[u64]>>>,
        num_servers: usize,
    );

    fn add_skeens2(
        &mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        max_timestamp: u64,
    );

    fn add_snapshot_skeens1(
        &mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        timestamps: Rc<RefCell<Box<[u64]>>>,
    );

    fn add_snapshot_skeens2(
        &mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        max_timestamp: u64,
    );

    fn needs_to_write(&self) -> bool;
}

impl Connected for PerServer<TcpStream> {
    type Connection = TcpStream;
    fn connection(&self) -> &Self::Connection {
        &self.stream
    }

    fn close(&mut self) {
        use std::net::Shutdown;

        let _ = self.stream.shutdown(Shutdown::Both);
        self.being_written.clear();
        //TODO return these to the client?
        self.being_sent.clear();
        self.read_buffer.clear();
        self.bytes_read = 0;
        self.bytes_sent = 0;
        self.got_new_message = false;
        self.stay_awake = false;
        self.dead = true;
    }

    fn recv_packet(&mut self) -> Result<Option<Buffer>, io::Error> {
        use std::io::ErrorKind;
        use packets::Packet::WrapErr;
        loop {
            let to_read = self.read_buffer.finished_at(self.bytes_read);
            let size = match to_read {
                Err(WrapErr::NotEnoughBytes(needs)) => needs,
                Err(_err) => return Err(io::ErrorKind::InvalidData.into()),
                Ok(..) => {
                    debug_assert!(self.read_buffer.packet_fits());
                    debug_assert!(self.bytes_read >= self.read_buffer.entry_size());
                    // trace!("CLIENT finished recv");
                    {
                        self.print_data.packets_recvd(1);
                        self.print_data.bytes_recvd(self.bytes_read as u64);
                    }
                    let buff = mem::replace(&mut self.read_buffer, Buffer::empty());
                    self.stay_awake = true;
                    //let buff = mem::replace(&mut self.read_buffer, Buffer::new());
                    return Ok(Some(buff))
                },
            };
            let drained = self.read_buffer.ensure_capacity(size);
            // trace!("CLIENT drained {:?}", drained);
            self.bytes_read -= drained;
            let r = self.stream.read(&mut self.read_buffer[self.bytes_read..]);
            match r {
                Ok(i) if i == 0 => {
                    //self.stay_awake = true;
                    return Ok(None)
                },
                Ok(i) => {
                    self.stay_awake = true;
                    self.bytes_read += i
                },
                Err(e) => match e.kind() {
                    ErrorKind::WouldBlock | ErrorKind::NotConnected => return Ok(None),
                    _ => return Err(e),
                }
            }
        }
        /*loop {
            let to_read = self.read_buffer.finished_at(self.bytes_read);
            let size = match to_read {
                Err(WrapErr::NotEnoughBytes(needs)) => needs,
                Err(err) => panic!("{:?}", err),
                Ok(..) => {
                    debug_assert!(self.read_buffer.packet_fits());
                    debug_assert_eq!(self.bytes_read, self.read_buffer.entry_size());
                    trace!("CLIENT finished recv");
                    {
                        self.print_data.packets_recvd(1);
                        self.print_data.bytes_recvd(self.bytes_read as u64);
                    }
                    self.bytes_read = 0;
                    let buff = mem::replace(&mut self.read_buffer, Buffer::empty());
                    self.stay_awake = true;
                    //let buff = mem::replace(&mut self.read_buffer, Buffer::new());
                    return Ok(Some(buff))
                },
            };
            let r = self.stream.read(&mut self.read_buffer[self.bytes_read..size]);
            match r {
                Ok(i) if i == 0 => {
                    self.stay_awake = true;
                    return Ok(None)
                },
                Ok(i) => {
                    self.stay_awake = true;
                    self.bytes_read += i
                },
                Err(e) => match e.kind() {
                    ErrorKind::WouldBlock | ErrorKind::NotConnected => return Ok(None),
                    _ => return Err(e),
                }
            }
        }*/
    }

    fn handle_redo(&mut self, _failed: WriteState, _kind: EntryKind::Kind) -> Option<WriteState> {
        unimplemented!();
        // let to_ret = match &failed {
        //     _ => None,
        // };
        // self.add_send(failed);
        //TODO front or back?
        //self.awaiting_send.push_front(failed);
        // to_ret
    }

    fn send_next_burst(&mut self) -> bool {
        use std::io::ErrorKind;
        //FIXME add specialcase for really big send...

        if self.being_written.is_empty() {
            trace!("FFFFF empty send @ {:?}", self.token);
            return false
        }

        if !self.being_written.first_bytes().is_empty() {
            match self.stream.write(
                &self.being_written.first_bytes()[self.bytes_sent..]
            ) {
                Ok(i) => {
                    self.print_data.bytes_sent(i as u64);
                    self.stay_awake = true;
                    self.bytes_sent += i
                },
                Err(e) => match e.kind() {
                    ErrorKind::WouldBlock => {
                        // trace!("FFFFF would block @ {:?}", self.token);
                    }
                    _ => panic!("CLIENT send error {}", e),
                }
            }
            // trace!("FFFFF {:?} sent {:?}/{:?}",
                // self.token, self.bytes_sent, self.being_written.first_bytes().len()
            // );
            if self.bytes_sent < self.being_written.first_bytes().len() {
                return false
            }
            self.bytes_sent = 0;
            self.being_written.swap_if_needed();
        } else {
            self.being_written.swap_if_needed()
        }

        self.stay_awake = true;

        return true
    }

    fn add_send(&mut self, to_send: WriteState) {
        self.stay_awake = true;
        //DEBUG ME

        self.print_data.packets_sending(1);
        to_send.with_packet(|p| self.being_written.fill(&[p, self.receiver.bytes()]));
        self.being_sent.push_back(to_send);
    }

    fn add_single_server_send(&mut self, msg: Vec<u8>) {
        let send = WriteState::SingleServer(msg);
        self.add_send(send)
    }

    fn add_skeens1(
        &mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        timestamps: Rc<RefCell<Box<[u64]>>>,
        num_servers: usize,
    ) {
        self.stay_awake = true;
        // let len = msg.borrow().len();
        self.print_data.packets_sending(1);
        let is_data;
        {
            let mut ts = msg.borrow_mut();
            let send_end = {
                {
                    let mut e = bytes_as_entry_mut(&mut *ts);
                    is_data = e.as_ref().locs().into_iter()
                        .take_while(|&&oi| oi != OrderIndex(0.into(), 0.into()))
                        .any(|oi| is_write_server_for(oi.0, self.token, num_servers));
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
            let sent = self.being_written.fill(&[&ts[..send_end], self.receiver.bytes()]);
            debug_assert!(sent);
        }
        self.being_sent.push_back(WriteState::Skeens1(
            msg, remaining_servers, timestamps, !is_data
        ))
    }

    fn add_skeens2(
        &mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        max_timestamp: u64,
    ) {
        trace!("Add Skeens2");
        self.stay_awake = true;
        // let len = msg.borrow().len();
        self.print_data.packets_sending(1);
        {
            let ts = msg.borrow();
            debug_assert_eq!(bytes_as_entry(&*ts).lock_num(), max_timestamp);
            let send_end = bytes_as_entry(&*ts).len();
            let sent = self.being_written.fill(&[&ts[..send_end], self.receiver.bytes()]);
            debug_assert!(sent);
        }
        self.being_sent.push_back(WriteState::Skeens2(
            msg, remaining_servers, max_timestamp
        ))
    }

    fn add_snapshot_skeens1(
        &mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        timestamps: Rc<RefCell<Box<[u64]>>>,
    ) {
        self.stay_awake = true;
        // let len = msg.borrow().len();
        self.print_data.packets_sending(1);
        {
            let ts = msg.borrow();
            let len = bytes_as_entry(&*ts).len();
            assert_eq!(len, ts.len());
            trace!("{:?}", bytes_as_entry(&*ts));
            let sent = self.being_written.fill(&[&ts[..len], self.receiver.bytes()]);
            debug_assert!(sent);
        }
        self.being_sent.push_back(WriteState::SnapshotSkeens1(
            msg, remaining_servers, timestamps
        ))
    }

    fn add_snapshot_skeens2(
        &mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        max_timestamp: u64,
    ) {
        trace!("Add snap Skeens2");
        self.stay_awake = true;
        // let len = msg.borrow().len();
        self.print_data.packets_sending(1);
        {
            let ts = msg.borrow();
            debug_assert_eq!(bytes_as_entry(&*ts).lock_num(), max_timestamp);
            let send_end = bytes_as_entry(&*ts).len();
            let sent = self.being_written.fill(&[&ts[..send_end], self.receiver.bytes()]);
            debug_assert!(sent);
        }
        self.being_sent.push_back(WriteState::SnapshotSkeens2(
            msg, remaining_servers, max_timestamp
        ))
    }

    fn needs_to_write(&self) -> bool {
        // debug_assert!(!(self.being_written.first_bytes().is_empty()
        //     && !self.being_written.second.is_empty()));
        // debug_assert!(!(self.being_written.is_empty()
        //     && !self.awaiting_send.is_empty()));
        !self.being_written.first_bytes().is_empty()
    }
}

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
