use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::mem;
use std::net::SocketAddr;
use std::time::Duration;
use std::rc::Rc;

use packets::*;
use buffer::Buffer;

use hash::{HashMap, HashSet};
use socket_addr::Ipv4SocketAddr;

use mio;
use mio::tcp::*;
use mio::Token;
use mio::udp::UdpSocket;

pub trait AsyncStoreClient {
    //TODO nocopy?
    fn on_finished_read(&mut self, read_loc: OrderIndex, read_packet: Vec<u8>);
    //TODO what info is needed?
    fn on_finished_write(&mut self, write_id: Uuid, write_locs: Vec<OrderIndex>);

    //TODO fn should_shutdown(&mut self) -> bool { false }
}

pub struct AsyncTcpStore<Socket, C: AsyncStoreClient> {
    servers: Vec<PerServer<Socket>>,
    awake_io: VecDeque<usize>,
    sent_writes: HashMap<Uuid, WriteState>,
    //sent_reads: HashMap<OrderIndex, Vec<u8>>,
    sent_reads: HashMap<OrderIndex, u16>,
    waiting_buffers: VecDeque<Vec<u8>>,
    num_chain_servers: usize,
    lock_token: Token,
    //FIXME change to spsc::Receiver<Buffer?>
    from_client: mio::channel::Receiver<Vec<u8>>,
    client: C,
    is_unreplicated: bool,
    new_multi: bool,

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
    awaiting_send: VecDeque<WriteState>,
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
    ToLockServer(Vec<u8>),
    MultiServer(Rc<RefCell<Vec<u8>>>,
        Rc<RefCell<HashSet<usize>>>,
        Rc<Box<[OrderIndex]>>,
        bool,
    ),
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
    UnlockServer(Rc<RefCell<Vec<u8>>>),
}

struct UdpConnection {
    socket: UdpSocket,
    addr: SocketAddr,
}

//TODO rename to AsyncStore
impl<C> AsyncTcpStore<TcpStream, C>
where C: AsyncStoreClient {

    //TODO should probably move all the poll register stuff too run
    pub fn tcp<I>(lock_server: SocketAddr, chain_servers: I, client: C,
        event_loop: &mut mio::Poll)
    -> Result<(Self, mio::channel::Sender<Vec<u8>>), io::Error>
    where I: IntoIterator<Item=SocketAddr> {
        //TODO assert no duplicates
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
            event_loop.register(server.connection(), server.token,
                mio::Ready::readable() | mio::Ready::writable() | mio::Ready::error(),
                mio::PollOpt::edge())
                .expect("could not reregister client socket")
        }
        let awake_io = servers.iter().map(|s| s.token.0).collect();
        let from_client_token = Token(servers.len());
        let (to_store, from_client) = mio::channel::channel();
        event_loop.register(&from_client,
            from_client_token,
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::level()
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

            print_data: Default::default(),
        }, to_store))
    }

    pub fn new_tcp<I>(
        chain_servers: I, client: C,
        event_loop: &mut mio::Poll
    ) -> Result<(Self, mio::channel::Sender<Vec<u8>>), io::Error>
    where I: IntoIterator<Item=SocketAddr> {
        //TODO assert no duplicates
        trace!("Starting Store server addrs:");
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
            event_loop.register(server.connection(), server.token,
                mio::Ready::readable() | mio::Ready::writable() | mio::Ready::error(),
                mio::PollOpt::edge())
                .expect("could not reregister client socket")
        }
        let awake_io = servers.iter().map(|s| s.token.0).collect();
        let from_client_token = Token(servers.len());
        let (to_store, from_client) = mio::channel::channel();
        event_loop.register(&from_client,
            from_client_token,
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::level()
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

            print_data: Default::default(),
        }, to_store))
    }

    pub fn replicated_tcp<I>(
        lock_server: Option<SocketAddr>,
        chain_servers: I,
        client: C,
        event_loop: &mut mio::Poll
    ) -> Result<(Self, mio::channel::Sender<Vec<u8>>), io::Error>
    where I: IntoIterator<Item=(SocketAddr, SocketAddr)>
    {
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
            event_loop.register(server.connection(), server.token,
                mio::Ready::readable() | mio::Ready::writable() | mio::Ready::error(),
                mio::PollOpt::edge())
                .expect("could not reregister client socket")
        }
        for i in 0..num_chain_servers {
            let receiver = servers[i+num_chain_servers].receiver;
            servers[i].receiver = receiver;
        }
        let awake_io = servers.iter().map(|s| s.token.0).collect();
        let from_client_token = Token(servers.len());
        let (to_store, from_client) = mio::channel::channel();
        event_loop.register(&from_client,
            from_client_token,
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::level()
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

            print_data: Default::default(),
        }, to_store))
    }
}

impl<C> AsyncTcpStore<UdpConnection, C>
where C: AsyncStoreClient {
    pub fn udp<I>(lock_server: SocketAddr, chain_servers: I, client: C,
        event_loop: &mut mio::Poll)
    -> Result<(Self, mio::channel::Sender<Vec<u8>>), io::Error>
    where I: IntoIterator<Item=SocketAddr> {
        //TODO assert no duplicates
        let mut servers = try!(chain_servers
            .into_iter()
            //TODO socket per servers is dumb, see if there's a better way to do multiplexing
            .map(PerServer::udp)
            .collect::<Result<Vec<_>, _>>());
        let num_chain_servers = servers.len();
        let lock_server = try!(PerServer::udp(lock_server));
        //TODO if let Some(lock_server) = lock_server...
        let lock_token = Token(servers.len());
        servers.push(lock_server);
        for (i, server) in servers.iter_mut().enumerate() {
            server.token = Token(i);
            event_loop.register(server.connection(), server.token,
                mio::Ready::readable() | mio::Ready::writable() | mio::Ready::error(),
                mio::PollOpt::edge())
                .expect("could not reregister client socket")
        }
        let awake_io = servers.iter().map(|s| s.token.0).collect();
        let from_client_token = Token(servers.len());
        let (to_store, from_client) = mio::channel::channel();
        event_loop.register(&from_client,
            from_client_token,
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::level()
        ).expect("could not reregister client channel");
        Ok((AsyncTcpStore {
            sent_writes: Default::default(),
            sent_reads: Default::default(),
            waiting_buffers: Default::default(),
            awake_io: awake_io,
            servers: servers,
            num_chain_servers: num_chain_servers,
            lock_token: lock_token,
            client: client,
            from_client: from_client,
            is_unreplicated: true,
            new_multi: false,

            print_data: Default::default(),
        }, to_store))
    }
}

impl PerServer<TcpStream> {
    fn tcp(addr: SocketAddr) -> Result<Self, io::Error> {
        let stream = try!(TcpStream::connect(&addr));
        let _ = stream.set_nodelay(true);
        let local_addr = try!(stream.local_addr());
        Ok(PerServer {
            awaiting_send: VecDeque::new(),
            being_written: DoubleBuffer::with_first_buffer_capacity(1024),
            being_sent: Default::default(),
            read_buffer: Buffer::new(), //TODO cap
            //read_buffer: Buffer::no_drop(), //TODO cap
            stream: stream,
            bytes_read: 0,
            bytes_sent: 0,
            token: Token(::std::usize::MAX),
            got_new_message: false,
            receiver: Ipv4SocketAddr::from_socket_addr(local_addr),
            stay_awake: true,

            print_data: Default::default(),
        })
    }

    fn connection(&self) -> &TcpStream {
        &self.stream
    }
}

impl PerServer<UdpConnection> {
    fn udp(addr: SocketAddr) -> Result<Self, io::Error> {
        use std::os::unix::io::FromRawFd;
        use nix::sys::socket as nix;
        let fd: i32 = try!(nix::socket(
                nix::AddressFamily::Inet,
                nix::SockType::Datagram,
                nix::SOCK_NONBLOCK | nix::SOCK_CLOEXEC,
                0));
        Ok(PerServer {
            awaiting_send: VecDeque::new(),
            being_written: DoubleBuffer::new(),
            being_sent: Default::default(),
            read_buffer: Buffer::new(), //TODO cap
            stream: UdpConnection { socket: unsafe { UdpSocket::from_raw_fd(fd) }, addr: addr },
            bytes_read: 0,
            bytes_sent: 0,
            token: Token(::std::usize::MAX),
            got_new_message: false,
            receiver: Ipv4SocketAddr::nil(),
            stay_awake: true,

            print_data: Default::default(),
        })
    }

    fn connection(&self) -> &UdpSocket {
        &self.stream.socket
    }
}

impl<S, C> AsyncTcpStore<S, C>
where PerServer<S>: Connected,
      C: AsyncStoreClient {
    pub fn run(mut self, poll: mio::Poll) -> ! {
        trace!("CLIENT start.");
        let mut events = mio::Events::with_capacity(1024);
        let mut timeout_idx = 0;
        loop {
            const TIMEOUTS: [(u64, u32); 9] =
                [(0, 10_000), (0, 100_000), (0, 500_000), (0, 1_000_000),
                (0, 10_000_000), (0, 100_000_000), (1, 0), (10, 0), (10, 0)];
            //poll.poll(&mut events, None).expect("worker poll failed");
            //let _ = poll.poll(&mut events, Some(Duration::from_secs(10)));
            let timeout = TIMEOUTS[timeout_idx];
            let timeout = Duration::new(timeout.0, timeout.1);
            let _ = poll.poll(&mut events, Some(timeout));
            if events.len() == 0 {
                #[cfg(feature = "print_stats")]
                {
                    if TIMEOUTS[timeout_idx].0 >= 10 {
                        self.print_stats()
                    }
                }
                for ps in self.servers.iter() {
                    self.awake_io.push_back(ps.token.0)
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

            self.handle_new_events(events.iter());

            'work: loop {
                //TODO add recv queue to this loop?
                let ops_before_poll = self.awake_io.len();
                for _ in 0..ops_before_poll {
                    let server = self.awake_io.pop_front().unwrap();
                    self.handle_server_event(server);
                }
                if self.awake_io.is_empty() {
                    break 'work
                }
                let _ = poll.poll(&mut events, Some(Duration::new(0, 1)));
                self.handle_new_events(events.iter());
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

    fn handle_new_events(&mut self, events: mio::EventsIter) {
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
                self.handle_server_event(event.token().0)
            }
        }
    }

    fn handle_new_requests_from_client(&mut self) -> bool {
        use std::sync::mpsc::TryRecvError;
        //trace!("CLIENT got new req");
        let msg = match self.from_client.try_recv() {
            Ok(msg) => msg,
            Err(TryRecvError::Empty) => return false,
            //TODO Err(TryRecvError::Disconnected) => panic!("client disconnected.")
            Err(TryRecvError::Disconnected) => return false,
        };
        self.print_data.from_client(1);
        let new_msg_kind = Entry::<()>::wrap_bytes(&msg).kind.layout();
        match new_msg_kind {
            EntryLayout::Read => {
                let loc = Entry::<()>::wrap_bytes(&msg).locs()[0];
                trace!("CLIENT got read {:?}", loc);
                let loc = loc.0;
                let s = self.read_server_for_chain(loc);
                //TODO if writeable write?
                self.add_single_server_send(s, msg)
            }
            EntryLayout::Data => {
                let loc = Entry::<()>::wrap_bytes(&msg).locs()[0].0;
                let s = self.write_server_for_chain(loc);
                trace!("CLIENT got write {:?}, server {:?}:{:?}",
                    loc, s, self.num_chain_servers);
                //TODO if writeable write?
                self.add_single_server_send(s, msg)
            }
            EntryLayout::Multiput => {
                trace!("CLIENT got multi write");
                if self.is_single_node_append(&msg) {
                    let chain = Entry::<()>::wrap_bytes(&msg).locs()[0].0;
                    let s = self.write_server_for_chain(chain);
                    self.add_single_server_send(s, msg)
                } else if self.new_multi {
                    let mut msg = msg;
                    {
                        let e = Entry::<()>::wrap_bytes_mut(&mut msg);
                        e.kind.insert(EntryKind::TakeLock | EntryKind::NewMultiPut);
                        e.locs_mut().into_iter()
                            .fold((), |_, &mut OrderIndex(_,ref mut i)| *i = 0.into());
                    }
                    self.add_skeens1(msg);
                    true
                } else {
                    let mut msg = msg;
                    Entry::<()>::wrap_bytes_mut(&mut msg).locs_mut().into_iter()
                        .fold((), |_, &mut OrderIndex(_,ref mut i)| *i = 0.into());
                    self.add_get_lock_nums(msg)
                }
            }
            r @ EntryLayout::Sentinel | r @ EntryLayout::Lock =>
                panic!("Invalid send request {:?}", r),
        }
    } // End fn handle_new_requests_from_client

    fn add_single_server_send(&mut self, server: usize, msg: Vec<u8>) -> bool {
        assert_eq!(Entry::<()>::wrap_bytes(&msg).entry_size(), msg.len());
        //let per_server = self.server_for_token_mut(Token(server));
        let per_server = &mut self.servers[server];
        debug_assert_eq!(per_server.token, Token(server));
        let written = per_server.add_single_server_send(msg);
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
            else if !sent.is_unlock() {
                let id = sent.id();
                self.sent_writes.insert(id, sent);
            }
        }
        written
    }

    fn add_skeens1(&mut self, msg: Vec<u8>) {
        debug_assert_eq!(Entry::<()>::wrap_bytes(&msg).entry_size(), msg.len());
        let timestamps = Rc::new(RefCell::new(
            (0..Entry::<()>::wrap_bytes(&msg).locs().len())
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
                let size = {
                    let e = Entry::<()>::wrap_bytes_mut(buf);
                    e.kind.remove(EntryKind::Multiput);
                    e.kind.insert(EntryKind::Sentinel);
                    e.kind.insert(EntryKind::Unlock | EntryKind::NewMultiPut);
                    e.data_bytes = 0;
                    e.dependency_bytes = 0;
                    unsafe { e.as_multi_entry_mut().flex.lock = max_ts };
                    debug_assert_eq!(e.lock_num(), max_ts);
                    e.entry_size()
                };
                //unsafe { buf.set_len(size) };
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

    fn add_get_lock_nums(&mut self, msg: Vec<u8>) -> bool {
        let lock_server = self.lock_token();
        //let per_server = self.server_for_token_mut(lock_server);
        let per_server = &mut self.servers[lock_server.0];
        let written = per_server.add_get_lock_nums(msg);
        if !per_server.got_new_message {
            per_server.got_new_message = true;
            self.awake_io.push_back(lock_server.0)
        }
        for sent in per_server.being_sent.drain(..) {
            let layout = sent.layout();
            if layout == EntryLayout::Read {
                let read_loc = sent.read_loc();
                *self.sent_reads.entry(read_loc).or_insert(0) += 1;
                self.waiting_buffers.push_back(sent.take())
            }
            else if !sent.is_unlock() {
                let id = sent.id();
                self.sent_writes.insert(id, sent);
            }
        }
        written
    }

    fn handle_server_event(&mut self, server: usize) {
        trace!("CLIENT handle server {:?} event", server);
        self.servers[server].got_new_message = false;
        //TODO pass in whether a read or write is ready?
        self.servers[server].stay_awake = false;
        //let mut stay_awake = false;
        let token = Token(server);
        debug_assert_eq!(token, self.servers[server].token);
        let finished_recv = self.servers[server].recv_packet().expect("cannot recv");
        if let Some(packet) = finished_recv {
            self.print_data.finished_recvs(1);
            //stay_awake = true;
            let kind = packet.entry().kind;
            trace!("CLIENT got a {:?} from {:?}", kind, token);
            if kind.contains(EntryKind::ReadSuccess) {
                if !kind.contains(EntryKind::Unlock) || kind.contains(EntryKind::NewMultiPut) {
                    let num_chain_servers = self.num_chain_servers;
                    self.handle_completion(token, num_chain_servers, &packet)
                }
            }
            //TODO distinguish between locks and empties
            else if kind.layout() == EntryLayout::Read
                && !kind.contains(EntryKind::TakeLock) {
                //A read that found an usused entry still contains useful data
                self.handle_completed_read(token, &packet);
            }
            //TODO use option instead
            else {
                self.handle_redo(Token(server), kind, &packet)
            }
            self.servers.get_mut(server).unwrap().read_buffer = packet;
        }

        if self.servers[server].needs_to_write() {
            trace!("CLIENT write");
            self.print_data.finished_sends(1);
            //let num_chain_servers = self.num_chain_servers;
            let finished_send = self.servers[server].send_next_burst();
            if finished_send {
                trace!("CLIENT finished a send to {:?}", token);
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
            }
            else if !sent.is_unlock() {
                let id = sent.id();
                self.sent_writes.insert(id, sent);
            }
        }

        if self.servers[server].stay_awake == true {
            self.awake_io.push_back(server)
        }

        //if stay_awake && self.servers[server].got_new_message == false {
        //    self.awake_io.push_back(server)
        //}
    } // end fn handle_server_event

    fn handle_completion(&mut self, token: Token, num_chain_servers: usize, packet: &Buffer) {
        //FIXME remove
        //let mut was_needed = self.sent_writes.contains_key(&packet.entry().id);
        let write_completed = self.handle_completed_write(token, num_chain_servers, packet);
        if write_completed {
            self.handle_completed_read(token, packet);
        }
        //FIXME remove
        //if !was_needed {
            //println!("Uneeded fetch @ {:?}: {:?}", token, packet.entry());
        //}
    }

    //TODO I should probably return the buffer on recv_packet, and pass it into here
    fn handle_completed_write(&mut self, token: Token, num_chain_servers: usize,
        packet: &Buffer) -> bool {
        let (id, kind) = {
            let e = packet.entry();
            (e.id, e.kind)
        };
        let unreplicated = self.is_unreplicated;
        trace!("CLIENT handle completed write?");
        //FIXME remove extra index?
        self.servers[token.0].print_data.finished_appends(1);
        //TODO for multistage writes check if we need to do more work...
        if let Some(v) = self.sent_writes.remove(&id) {
            trace!("CLIENT write needed completion");
            match v {
                //if lock
                //    for server in v.servers()
                //         server.send_lock+data
                WriteState::ToLockServer(mut msg) => {
                    trace!("CLIENT finished lock");
                    assert!(!self.new_multi);
                    assert_eq!(token, self.lock_token());
                    {
                        let e = Entry::<()>::wrap_bytes_mut(&mut msg);
                        e.kind.insert(EntryKind::TakeLock);
                        let msg_locs = e.locs_mut();
                        let lock_locs = &packet.entry().locs()[..msg_locs.len()];
                        for i in 0..msg_locs.len() {
                            debug_assert_eq!(msg_locs[i].0, lock_locs[i].0);
                            msg_locs[i] = lock_locs[i];
                            debug_assert!(
                                if msg_locs[i].0 != order::from(0) {
                                    msg_locs[i].1 != entry::from(0)
                                } else {
                                    msg_locs[i].1 == entry::from(0)
                                }
                            );
                        }
                    }
                    self.add_multis(msg, num_chain_servers);
                    return false
                }
                WriteState::MultiServer(buf, remaining_servers, locks, is_sentinel) => {
                    assert!(!self.new_multi);
                    assert!(token != self.lock_token());
                    assert!(!self.new_multi);
                    trace!("CLIENT finished multi section");
                    let ready_to_unlock = {
                        let mut b = buf.borrow_mut();
                        let filled = fill_locs(&mut b, packet.entry(),
                            token, self.num_chain_servers, unreplicated);
                        let mut r = remaining_servers.borrow_mut();
                        if r.remove(&token.0) {
                            assert!(filled > 0, "no fill for {:?}", token);
                        }
                        let finished_writes = r.is_empty();
                        if finished_writes {
                            //TODO assert!(Rc::get_mut(&mut buf).is_some());
                            //let locs = buf.locs().to_vec();
                            let locs = Entry::<()>::wrap_bytes(&b).locs().to_vec();
                            Some(locs)
                        } else { None }
                    };
                    match ready_to_unlock {
                        Some(locs) => {
                            {
                                Entry::<()>::wrap_bytes_mut(&mut buf.borrow_mut())
                                    .locs_mut()
                                    .copy_from_slice(&locks)
                            }
                            self.add_unlocks(buf);
                            trace!("CLIENT finished multi at {:?}", locs);
                            //TODO
                            self.client.on_finished_write(id, locs);
                            return true
                        }
                        None => {
                            self.sent_writes.insert(id,
                                WriteState::MultiServer(buf, remaining_servers, locks, is_sentinel));
                            return false
                        }
                    }
                }

                WriteState::Skeens1(buf, remaining_servers, timestamps, is_sentinel) => {
                    if !kind.contains(EntryKind::Skeens1Queued) {
                        error!("CLIENT bad skeens1 ack @ {:?}", token);
                        return false
                    }
                    assert!(kind.contains(EntryKind::Multiput));
                    assert!(self.new_multi);
                    trace!("CLIENT finished sk1 section");
                    let ready_for_skeens2 = {
                        let mut r = remaining_servers.borrow_mut();
                        if !r.remove(&token.0) {
                            error!("CLIENT repeat sk1 section");
                            return false
                        }
                        let finished_writes = r.is_empty();
                        mem::drop(r);

                        let mut ts = timestamps.borrow_mut();
                        //alt just do ts[i] = max(oi.1, ts[i])
                        let e = packet.entry();
                        debug_assert_eq!(id, e.id);
                        for (i, oi) in e.locs().iter().enumerate() {
                            if oi.0 != order::from(0)
                                && read_server_for_chain(oi.0, self.num_chain_servers, unreplicated) == token.0 {
                                assert!(ts[i] == 0,
                                    "repeat timestamp {:?} in {:#?}", oi, e);
                                let t: entry = oi.1;
                                ts[i] = u32::from(t) as u64;
                                assert!(ts[i] > 0,
                                    "bad timestamp {:?} in {:#?}", oi, e);
                            }
                        }

                        if finished_writes {
                            //TODO assert!(Rc::get_mut(&mut buf).is_some());
                            //let locs = buf.locs().to_vec();
                            let max_ts = ts.iter().cloned().max().unwrap();
                            for t in ts.iter() { assert!(&max_ts >= t); }
                            trace!("CLIENT finished skeens1 {:?} max {:?}", ts, max_ts);
                            Some(max_ts)
                        } else { None }
                    };
                    match ready_for_skeens2 {
                        Some(max_ts) => {
                            self.add_skeens2(buf, max_ts);
                            //TODO
                            return false
                        }
                        None => {
                            self.sent_writes.insert(id,
                                WriteState::Skeens1(buf, remaining_servers, timestamps, is_sentinel));
                            return false
                        }
                    }
                }

                WriteState::Skeens2(buf, remaining_servers, max_ts) => {
                    assert!(self.new_multi);
                    trace!("CLIENT finished sk2 section");
                    let ready_to_unlock = {
                        let mut b = buf.borrow_mut();
                        let mut finished_writes = true;
                        {
                            let locs = Entry::<()>::wrap_bytes_mut(&mut *b).locs_mut();
                            let fill_from = packet.entry().locs();
                            //trace!("CLIENT filling {:?} from {:?}", locs, fill_from);
                            for (i, &loc) in fill_from.into_iter().enumerate() {
                                if locs[i].0 != order::from(0) {
                                    if read_server_for_chain(loc.0, self.num_chain_servers, unreplicated) == token.0
                                        && loc.1 != entry::from(0) {
                                        locs[i] = loc;
                                    } else if locs[i].1 == entry::from(0) {
                                        finished_writes = false
                                    }
                                }
                            }
                        };
                        if finished_writes {
                            //TODO assert!(Rc::get_mut(&mut buf).is_some());
                            //let locs = buf.locs().to_vec();
                            let locs = Entry::<()>::wrap_bytes(&b).locs().to_vec();
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
                            self.client.on_finished_write(id, locs);
                            return true
                        }
                        None => {
                            self.sent_writes.insert(id,
                                WriteState::Skeens2(buf, remaining_servers, max_ts));
                            return false
                        }
                    }
                }

                WriteState::SingleServer(mut buf) => {
                    //assert!(token != self.lock_token());
                    trace!("CLIENT finished single server");
                    fill_locs(&mut buf, packet.entry(), token, self.num_chain_servers, unreplicated);
                    let locs = packet.entry().locs().to_vec();
                    self.client.on_finished_write(id, locs);
                    return true
                }
                WriteState::UnlockServer(..) => panic!("invalid wait state"),
            };

            fn fill_locs(buf: &mut [u8], e: &Entry,
                server: Token, num_chain_servers: usize, unreplicated: bool) -> usize {
                let locs = Entry::<()>::wrap_bytes_mut(buf).locs_mut();
                let mut filled = 0;
                let fill_from = e.locs();
                //trace!("CLIENT filling {:?} from {:?}", locs, fill_from);
                for (i, &loc) in fill_from.into_iter().enumerate() {
                    if locs[i].0 != order::from(0)
                        && read_server_for_chain(loc.0, num_chain_servers, unreplicated) == server.0 {//should be read_server_for_chain
                        assert!(loc.1 != 0.into(), "zero index for {:?}", loc.0);
                        locs[i] = loc;
                        filled += 1;
                    }
                }
                filled
            }
        }
        else {
            trace!("CLIENT no write for {:?}", token);
            return true
        }
    }// end handle_completed_write

    fn handle_completed_read(&mut self, token: Token, packet: &Buffer) -> bool {
        use std::collections::hash_map::Entry::Occupied;

        trace!("CLIENT handle completed read?");
        if !self.new_multi && token == self.lock_token() { return false }
        //FIXME remove extra index?
        self.servers[token.0].print_data.finished_read(1);
        //FIXME remove
        let mut was_needed = false;
        let is_sentinel = packet.entry().kind.layout() == EntryLayout::Sentinel;
        let mut is_sentinel_loc = false;
        for &oi in packet.entry().locs() {
            if oi == OrderIndex(0.into(), 0.into()) {
                is_sentinel_loc = true;
                continue
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
            if needed {
                was_needed |= true;
                trace!("CLIENT read needed completion @ {:?}", oi);
                //TODO validate correct id for failing read
                //TODO num copies?
                let mut v = self.waiting_buffers.pop_front()
                    .unwrap_or_else(|| Vec::with_capacity(packet.entry_size()));
                v.clear();
                v.extend_from_slice(packet.entry_slice());
                self.client.on_finished_read(oi, v);
            }
        }
        was_needed
    }

    fn handle_redo(&mut self, token: Token, kind: EntryKind::Kind, packet: &Buffer) {
        self.servers[token.0].print_data.redo(1);
        let write_state = match kind.layout() {
            EntryLayout::Data | EntryLayout::Multiput | EntryLayout::Sentinel => {
                let id = packet.entry().id;
                self.sent_writes.remove(&id)
            }
            EntryLayout::Read => {
                let read_loc = packet.entry().locs()[0];
                if self.sent_reads.contains_key(&read_loc) {
                    let mut b = self.waiting_buffers.pop_front()
                        .unwrap_or_else(|| Vec::with_capacity(50));
                    {
                        let e = EntryContents::Data(&(), &[]).fill_vec(&mut b);
                        e.kind = EntryKind::Read;
                        e.locs_mut()[0] = read_loc;
                    }
                    //FIXME better read packet handling
                    Some(WriteState::SingleServer(b))
                } else { None }
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

    fn add_unlocks(&mut self, mut buf: Rc<RefCell<Vec<u8>>>) {
        let servers = match Rc::get_mut(&mut buf) {
            None => unreachable!(),
            Some(mut buf) => {
                let buf = buf.get_mut();
                let server = self.get_servers_for_multi(&buf);
                let size = {
                    let e = Entry::<()>::wrap_bytes_mut(buf);
                    e.kind.remove(EntryKind::Multiput);
                    e.kind.insert(EntryKind::Sentinel);
                    e.kind.insert(EntryKind::Unlock);
                    e.data_bytes = 0;
                    e.dependency_bytes = 0;
                    e.entry_size()
                };
                unsafe { buf.set_len(size) };
                server
            }
        };
        assert_eq!(Entry::<()>::wrap_bytes(&buf.borrow()).kind.layout(), EntryLayout::Sentinel);
        assert_eq!(Entry::<()>::wrap_bytes(&buf.borrow()).entry_size(), buf.borrow().len());
        for server in servers {
            trace!("CLIENT add unlock for {:?}", server);
            let per_server = &mut self.servers[server];
            per_server.add_unlock(buf.clone());
            if !per_server.got_new_message {
                per_server.got_new_message = true;
                self.awake_io.push_back(server)
            }
        }
    }

    fn add_multis(&mut self, msg: Vec<u8>, num_servers: usize) {
        debug_assert_eq!(Entry::<()>::wrap_bytes(&msg).entry_size(), msg.len());
        let locks = Rc::new(Entry::<()>::wrap_bytes(&msg).locs().to_vec().into_boxed_slice());
        let servers = self.get_servers_for_multi(&msg);
        let mut remaining_servers: HashSet<usize> = Default::default();
        remaining_servers.reserve(servers.len());
        for &writer in servers.iter() {
            remaining_servers.insert(self.read_server_for_write_server(writer));
        }
        trace!("CLIENT multi to {:?}", remaining_servers);
        let remaining_servers = Rc::new(RefCell::new(remaining_servers));
        let msg = Rc::new(RefCell::new(msg));
        for s in servers {
            let per_server = &mut self.servers[s];
            per_server.add_multi(
                msg.clone(),
                remaining_servers.clone(),
                locks.clone(),
                num_servers,
            );
            if !per_server.got_new_message {
                per_server.got_new_message = true;
                self.awake_io.push_back(s)
            }
        }
    }

    fn is_single_node_append(&self, msg: &[u8]) -> bool {
        let mut single = true;
        let mut server_token = None;
        let locked_chains = Entry::<()>::wrap_bytes(&msg).locs()
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
            Entry::<()>::wrap_bytes(msg).kind.layout() == EntryLayout::Multiput
            || Entry::<()>::wrap_bytes(msg).kind.layout() == EntryLayout::Sentinel
        );
        Entry::<()>::wrap_bytes(msg).locs()
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

    fn handle_redo(&mut self, failed: WriteState, _kind: EntryKind::Kind)
    -> Option<WriteState>;

    fn recv_packet(&mut self) -> Result<Option<Buffer>, io::Error>;
    fn send_next_burst(&mut self) -> bool;

    fn add_send(&mut self, to_send: WriteState) -> bool;

    fn add_single_server_send(&mut self, msg: Vec<u8>) -> bool {
        let send = WriteState::SingleServer(msg);
        self.add_send(send)
    }

    fn add_multi(&mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        locks: Rc<Box<[OrderIndex]>>,
        num_servers: usize,
    );

    fn add_unlock(&mut self, buffer: Rc<RefCell<Vec<u8>>>);

    fn add_get_lock_nums(&mut self, msg: Vec<u8>) -> bool {
        let send = WriteState::ToLockServer(msg);
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

    fn needs_to_write(&self) -> bool;
}

impl Connected for PerServer<TcpStream> {
    type Connection = TcpStream;
    fn connection(&self) -> &Self::Connection {
        &self.stream
    }

    fn recv_packet(&mut self) -> Result<Option<Buffer>, io::Error> {
        use std::io::ErrorKind;
        use packets::Packet::WrapErr;
        loop {
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
                    ErrorKind::WouldBlock => return Ok(None),
                    _ => return Err(e),
                }
            }
        }
    }

    fn handle_redo(&mut self, failed: WriteState, _kind: EntryKind::Kind) -> Option<WriteState> {
        let to_ret = match &failed {
            f @ &WriteState::MultiServer(..) => Some(f.clone_multi()),
            _ => None,
        };
        self.add_send(failed);
        //TODO front or back?
        //self.awaiting_send.push_front(failed);
        to_ret
    }

    fn send_next_burst(&mut self) -> bool {
        use std::io::ErrorKind;
        //FIXME add specialcase for really big send...

        if self.being_written.is_empty() && self.awaiting_send.is_empty() {
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
                        trace!("FFFFF would block @ {:?}", self.token);
                    }
                    _ => panic!("CLIENT send error {}", e),
                }
            }
            trace!("FFFFF {:?} sent {:?}/{:?}",
                self.token, self.bytes_sent, self.being_written.first_bytes().len()
            );
            if self.bytes_sent < self.being_written.first_bytes().len() {
                return false
            }
            self.bytes_sent = 0;
            self.being_written.swap_if_needed();
        }

        //DEBUG ME
        while !self.awaiting_send.is_empty() {
            let being_written = &mut self.being_written;
            let addr = self.receiver;
            let added = self.awaiting_send.front().unwrap()
                .with_packet(|p| being_written.try_fill(p, addr));
            if !added { break }
            self.print_data.packets_sending(1);
            let msg = self.awaiting_send.pop_front().unwrap();
            self.being_sent.push_back(msg);
        }

        self.stay_awake = true;

        return true
    }

    fn add_send(&mut self, to_send: WriteState) -> bool {
        self.stay_awake = true;
        //DEBUG ME
        if !self.awaiting_send.is_empty() {
            trace!("FFFFF add to wait");
            self.awaiting_send.push_back(to_send);
            return false
        }

        let can_write = to_send.with_packet(|p|
            self.being_written.try_fill(p, self.receiver));
        if can_write {
            self.print_data.packets_sending(1);
            trace!("FFFFF add to buffer");
            self.being_sent.push_back(to_send)
        } else {
            trace!("FFFFF buffer full");
            self.awaiting_send.push_back(to_send)
        }
        can_write
    }

    fn add_single_server_send(&mut self, msg: Vec<u8>) -> bool {
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
        let len = msg.borrow().len();
        if self.being_written.can_hold_bytes(len + mem::size_of::<Ipv4SocketAddr>()) {
            self.print_data.packets_sending(1);
            let is_data;
            {
                let mut ts = msg.borrow_mut();
                let send_end = {
                    let e = Entry::<()>::wrap_bytes_mut(&mut *ts);
                    {
                        is_data = e.locs().into_iter()
                            .take_while(|&&oi| oi != OrderIndex(0.into(), 0.into()))
                            .any(|oi| is_write_server_for(oi.0, self.token, num_servers));
                        let kind = &mut e.kind;
                        debug_assert!(kind.layout() == EntryLayout::Multiput
                            || kind.layout() == EntryLayout::Sentinel);
                        debug_assert!(kind.contains(EntryKind::TakeLock));
                        if is_data {
                            kind.remove(EntryKind::Lock);
                            debug_assert_eq!(kind.layout(), EntryLayout::Multiput);
                        }
                        else {
                            kind.insert(EntryKind::Lock);
                            debug_assert_eq!(kind.layout(), EntryLayout::Sentinel);
                        }
                        kind.insert(EntryKind::TakeLock);
                    }
                    if !is_data {
                        debug_assert!(e.locs().contains(&OrderIndex(0.into(), 0.into())));
                    }
                    e.entry_size()
                };
                //Since sentinels have a different size than multis, we need to truncate
                //for those sends
                let sent = self.being_written.try_fill(&ts[..send_end], self.receiver);
                debug_assert!(sent);
            }
            self.being_sent.push_back(WriteState::Skeens1(
                msg, remaining_servers, timestamps, !is_data
            ))
        } else {
            let is_sentinel = {
                let ts = msg.borrow();
                !Entry::<()>::wrap_bytes(&*ts)
                    .locs().into_iter()
                    .take_while(|&&oi| oi != OrderIndex(0.into(), 0.into()))
                    .any(|oi| is_write_server_for(oi.0, self.token, num_servers))
            };
            self.awaiting_send.push_back(
                WriteState::Skeens1(msg, remaining_servers, timestamps, is_sentinel)
            );
        }
    }

    fn add_skeens2(
        &mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        max_timestamp: u64,
    ) {
        trace!("Add Skeens2");
        self.stay_awake = true;
        let len = msg.borrow().len();
        if self.being_written.can_hold_bytes(len + mem::size_of::<Ipv4SocketAddr>()) {
            self.print_data.packets_sending(1);
            {
                let ts = msg.borrow();
                let send_end = {
                    let e = Entry::<()>::wrap_bytes(&*ts);
                    e.entry_size()
                };
                let sent = self.being_written.try_fill(&ts[..send_end], self.receiver);
                debug_assert!(sent);
            }
            self.being_sent.push_back(WriteState::Skeens2(
                msg, remaining_servers, max_timestamp
            ))
        } else {
            self.awaiting_send.push_front(
                WriteState::Skeens2(msg, remaining_servers, max_timestamp)
            );
        }
    }

    fn add_multi(&mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        locks: Rc<Box<[OrderIndex]>>,
        num_servers: usize,
    ) {
        self.stay_awake = true;
        let len = msg.borrow().len();
        if self.being_written.can_hold_bytes(len + mem::size_of::<Ipv4SocketAddr>()) {
            self.print_data.packets_sending(1);
            let is_data;
            {
                let mut ts = msg.borrow_mut();
                let send_end = {
                    let e = Entry::<()>::wrap_bytes_mut(&mut *ts);
                    {
                        is_data = e.locs().into_iter()
                            .take_while(|&&oi| oi != OrderIndex(0.into(), 0.into()))
                            .any(|oi| is_write_server_for(oi.0, self.token, num_servers));
                        let kind = &mut e.kind;
                        debug_assert!(kind.layout() == EntryLayout::Multiput
                            || kind.layout() == EntryLayout::Sentinel);
                        debug_assert!(kind.contains(EntryKind::TakeLock));
                        if is_data {
                            kind.remove(EntryKind::Lock);
                            debug_assert_eq!(kind.layout(), EntryLayout::Multiput);
                        }
                        else {
                            kind.insert(EntryKind::Lock);
                            debug_assert_eq!(kind.layout(), EntryLayout::Sentinel);
                        }
                        kind.insert(EntryKind::TakeLock);
                    }
                    if !is_data {
                        debug_assert!(e.locs().contains(&OrderIndex(0.into(), 0.into())));
                    }
                    e.entry_size()
                };
                //Since sentinels have a different size than multis, we need to truncate
                //for those sends
                let sent = self.being_written.try_fill(&ts[..send_end], self.receiver);
                debug_assert!(sent);
            }
            self.being_sent.push_back(WriteState::MultiServer(msg, remaining_servers, locks, !is_data))
        } else {
            let is_sentinel = {
                let ts = msg.borrow();
                !Entry::<()>::wrap_bytes(&*ts)
                    .locs().into_iter()
                    .take_while(|&&oi| oi != OrderIndex(0.into(), 0.into()))
                    .any(|oi| is_write_server_for(oi.0, self.token, num_servers))
            };
            self.awaiting_send.push_back(WriteState::MultiServer(msg, remaining_servers, locks, is_sentinel));
        }
    }

    fn add_unlock(&mut self, buffer: Rc<RefCell<Vec<u8>>>) {
        //unlike other reqs here we send the unlock first to minimize the contention window
        self.stay_awake = true;
        let can_write = {
            let b = buffer.borrow();
            self.being_written.try_fill(&b[..], self.receiver)
        };
        if can_write {
            self.print_data.packets_sending(1);
            //FIXME this is unneeded
            self.being_sent.push_back(WriteState::UnlockServer(buffer))
        } else {
            self.awaiting_send.push_front(WriteState::UnlockServer(buffer))
        }
    }

    fn add_get_lock_nums(&mut self, msg: Vec<u8>) -> bool {
        let send = WriteState::ToLockServer(msg);
        self.add_send(send)
    }

    fn needs_to_write(&self) -> bool {
        debug_assert!(!(self.being_written.first_bytes().is_empty()
            && !self.being_written.second.is_empty()));
        debug_assert!(!(self.being_written.is_empty()
            && !self.awaiting_send.is_empty()));
        !self.being_written.first_bytes().is_empty()
    }
}

impl Connected for PerServer<UdpConnection> {
    type Connection = UdpSocket;
    fn connection(&self) -> &Self::Connection {
        &self.stream.socket
    }

    fn recv_packet(&mut self) -> Result<Option<Buffer>, io::Error> {
        //TODO
        self.read_buffer.ensure_capacity(8192);
        //FIXME handle WouldBlock and number of bytes read
        let read = self.stream.socket.recv_from(&mut self.read_buffer[0..8192])
            .expect("cannot read");
        if let Some((read, _)) = read {
            if read > 0 {
                let buff = mem::replace(&mut self.read_buffer, Buffer::empty());
                self.stay_awake = true;
                return Ok(Some(buff))
            }
        }

        Ok(None)
    }

    //fn send_packet(&mut self, packet: &[u8]) -> bool {
    //    let addr = &self.stream.addr;
    //    //FIXME handle WouldBlock and number of bytes read
    //    let sent = self.stream.socket.send_to(packet, addr).expect("cannot write");
    //    if let Some(sent) = sent {
    //        return sent > 0
    //    }
    //    return false
    //}

    fn handle_redo(&mut self, failed: WriteState, _kind: EntryKind::Kind) -> Option<WriteState> {
        let to_ret = match &failed {
            f @ &WriteState::MultiServer(..) => Some(f.clone_multi()),
            _ => None,
        };
        self.stay_awake = true;
        //TODO front or back?
        self.awaiting_send.push_front(failed);
        to_ret
    }

    //FIXME add seperate write function which is split into TCP and UDP versions
    //fn send_next_burst(&mut self) -> Option<WriteState> {
    fn send_next_burst(&mut self) -> bool {
        let addr = &self.stream.addr;
        //TODO no-alloc?
        let packet = self.awaiting_send.pop_front();
        if let Some(packet) = packet {
            //FIXME handle WouldBlock and number of bytes read
            let sent = packet.with_packet(|p| self.stream.socket.send_to(p, addr)
                .expect("cannot write")
            );
            if let Some(sent) = sent {
                self.stay_awake = true;
                self.being_sent.push_back(packet);
                return sent > 0
            }
            self.awaiting_send.push_front(packet);
            return false
        }
        false
        /*
        use self::WriteState::*;

        let send_in_progress = mem::replace(&mut self.currently_sending, None);
        if let Some(currently_sending) = send_in_progress {
            let finished = currently_sending.with_packet(|p| self.send_packet(p) );
            if finished {
                return Some(currently_sending)
            }
            else {
                mem::replace(&mut self.currently_sending, Some(currently_sending));
                return None
            }
        }

        match self.awaiting_send.pop_front() {
            None => None,
            Some(MultiServer(to_send, remaining_servers, locks)) => {
                let finished = {
                    trace!("CLIENT PerServer {:?} multi", token);
                    let mut ts = to_send.borrow_mut();
                    let kind = {
                        Entry::<()>::wrap_bytes(&*ts).kind
                    };
                    assert!(kind.layout() == EntryLayout::Multiput
                        || kind.layout() == EntryLayout::Sentinel);
                    assert!(kind.contains(EntryKind::TakeLock));
                    let send_end = {
                        let e = Entry::<()>::wrap_bytes_mut(&mut *ts);
                        {
                            let is_data = e.locs().into_iter()
                                .take_while(|&&oi| oi != OrderIndex(0.into(), 0.into()))
                                .any(|oi| is_write_server_for(oi.0, token, num_servers));
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
                    //self.stream.write_all(&ts[..send_end]).expect("cannot write");
                    self.send_packet(&ts[..send_end])
                };
                if finished {
                    Some(MultiServer(to_send, remaining_servers, locks))
                } else {
                    mem::replace(
                        &mut self.currently_sending,
                        Some(MultiServer(to_send, remaining_servers, locks))
                    );
                    return None
                }
            }
            Some(UnlockServer(to_send)) => {
                let finished = {
                    trace!("CLIENT PerServer {:?} unlock", token);
                    let mut ts = to_send.borrow_mut();
                    debug_assert_eq!(Entry::<()>::wrap_bytes_mut(&mut *ts).kind.layout(), EntryLayout::Sentinel);
                    debug_assert!(Entry::<()>::wrap_bytes_mut(&mut *ts).kind.contains(EntryKind::Unlock));
                    //trace!("CLIENT willsend {:?}", &*ts);
                    self.send_packet(Entry::<()>::wrap_bytes(&ts).bytes())
                };
                if finished {
                    Some(UnlockServer(to_send))
                } else {
                    mem::replace(
                        &mut self.currently_sending,
                        Some(UnlockServer(to_send))
                    );
                    return None
                }
            }
            Some(to_send @ ToLockServer(_)) | Some(to_send @ SingleServer(_)) => {
                trace!("CLIENT PerServer {:?} single", token);
                {
                    let (l, _s) = to_send.with_packet(|p| {
                        let e = Entry::<()>::wrap_bytes(&*p);
                        (e.kind.layout(), e.entry_size())
                    });
                    assert!(l == EntryLayout::Data || l == EntryLayout::Multiput
                        || l == EntryLayout::Read)
                }
                let finished = if to_send.is_read() {
                    to_send.with_packet(|p| self.send_packet(p))
                }
                else {
                    to_send.with_packet(|p| self.send_packet(p))
                };

                trace!("CLIENT PerServer {:?} single written", token);
                if finished {
                    Some(to_send)
                } else {
                    mem::replace(&mut self.currently_sending, Some(to_send));
                    return None
                }
            }
        }*/
    }

    fn add_single_server_send(&mut self, msg: Vec<u8>) -> bool {
        self.stay_awake = true;
        self.awaiting_send.push_back(WriteState::SingleServer(msg));
        true
    }

    fn add_send(&mut self, to_send: WriteState) -> bool {
        self.stay_awake = true;
        self.awaiting_send.push_back(to_send);
        true
    }

    #[allow(unused_variables)]
    fn add_skeens1(
        &mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        timestamps: Rc<RefCell<Box<[u64]>>>,
        num_servers: usize,
    ) {
        unimplemented!()
    }

    #[allow(unused_variables)]
    fn add_skeens2(
        &mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        max_timestamp: u64,
    ) {
        unimplemented!()
    }

    #[allow(unused_variables)]
    fn add_multi(&mut self,
        msg: Rc<RefCell<Vec<u8>>>,
        remaining_servers: Rc<RefCell<HashSet<usize>>>,
        locks: Rc<Box<[OrderIndex]>>,
        num_servers: usize,
    ) {
    //fn add_multi(&mut self, msg: Rc<RefCell<Vec<u8>>>, remaining_servers: Rc<RefCell<HashSet<usize>>>, locks: Rc<Box<[OrderIndex]>>) {
        //FIXME
        unimplemented!()
        //self.awaiting_send.push_back(WriteState::MultiServer(msg, remaining_servers, locks, false));
    }

    #[allow(unused_variables)]
    fn add_unlock(&mut self, buffer: Rc<RefCell<Vec<u8>>>) {
        //unlike other reqs here we send the unlock first to minimize the contention window
        //self.awaiting_send.push_front(WriteState::UnlockServer(buffer))
        unimplemented!()
    }

    #[allow(unused_variables)]
    fn add_get_lock_nums(&mut self, msg: Vec<u8>) -> bool {
        //self.awaiting_send.push_back(WriteState::ToLockServer(msg));
        //true
        unimplemented!()
    }

    fn needs_to_write(&self) -> bool {
        !self.awaiting_send.is_empty()
    }
}

const MAX_WRITE_BUFFER_SIZE: usize = 40000;

struct DoubleBuffer {
    first: Vec<u8>,
    second: Vec<u8>,
}

impl DoubleBuffer {

    fn new() -> Self {
        DoubleBuffer {
            first: Vec::new(),
            second: Vec::new(),
        }
    }

    fn with_first_buffer_capacity(cap: usize) -> Self {
        DoubleBuffer {
            first: Vec::with_capacity(cap),
            second: Vec::new(),
        }
    }

    fn first_bytes(&self) -> &[u8] {
        &self.first[..]
    }

    fn swap_if_needed(&mut self) {
        self.first.clear();
        if self.second.len() > 0 {
            mem::swap(&mut self.first, &mut self.second)
        }
    }

    fn can_hold_bytes(&self, bytes: usize) -> bool {
        buffer_can_hold_bytes(&self.first, bytes)
        || buffer_can_hold_bytes(&self.second, bytes)
    }

    fn try_fill(&mut self, bytes: &[u8], addr: Ipv4SocketAddr) -> bool {
        if self.is_filling_first() {
            if buffer_can_hold_bytes(&self.first, bytes.len() + addr.bytes().len())
            || self.first.is_empty() {
                self.first.extend_from_slice(bytes);
                self.first.extend_from_slice(addr.bytes());
                return true
            }
        }

        if buffer_can_hold_bytes(&self.second, bytes.len() + addr.bytes().len())
        || self.second.capacity() < MAX_WRITE_BUFFER_SIZE {
            self.second.extend_from_slice(bytes);
            self.second.extend_from_slice(addr.bytes());
            return true
        }

        return false
    }

    fn is_filling_first(&self) -> bool {
        self.second.len() == 0
    }

    fn is_empty(&self) -> bool {
        self.first.is_empty() && self.second.is_empty()
    }
}

fn buffer_can_hold_bytes(buffer: &Vec<u8>, bytes: usize) -> bool {
    buffer.capacity() - buffer.len() >= bytes
}

impl WriteState {
    fn with_packet<F, R>(&self, f: F) -> R
    where F: for<'a> FnOnce(&'a [u8]) -> R {
        use self::WriteState::*;
        match self {
            &SingleServer(ref buf) | &ToLockServer(ref buf) => f(&**buf),
            &MultiServer(ref buf, _, _, is_sentinel) => {
                let mut b = buf.borrow_mut();
                {
                    let e = Entry::<()>::wrap_bytes_mut(&mut *b);
                    if is_sentinel {
                        e.kind.remove(EntryKind::Multiput);
                        e.kind.insert(EntryKind::Sentinel);
                    }
                    else {
                        e.kind.remove(EntryKind::Sentinel);
                        e.kind.insert(EntryKind::Multiput);
                    }
                }
                f(&*b)
            },

            &Skeens1(ref buf, _, _, is_sentinel) => {
                let mut b = buf.borrow_mut();
                {
                    let e = Entry::<()>::wrap_bytes_mut(&mut *b);
                    if is_sentinel {
                        e.kind.remove(EntryKind::Multiput);
                        e.kind.insert(EntryKind::Sentinel);
                    }
                    else {
                        e.kind.remove(EntryKind::Sentinel);
                        e.kind.insert(EntryKind::Multiput);
                    }
                }
                f(&*b)
            },

            &UnlockServer(ref buf) | &Skeens2(ref buf, ..) => {
                let b = buf.borrow_mut();
                f(&*b)
            },
        }
    }

    fn take(self) -> Vec<u8> {
        use self::WriteState::*;
        match self {
            SingleServer(buf) | ToLockServer(buf) => buf,
            MultiServer(buf, ..) | UnlockServer(buf)
            | Skeens1(buf, ..) | Skeens2(buf, ..) =>
                Rc::try_unwrap(buf).expect("taking from non unique WriteState")
                    .into_inner()
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
            &MultiServer(ref b, ref remaining_servers, ref locks, is_sentinel) => MultiServer(b.clone(), remaining_servers.clone(), locks.clone(), is_sentinel),
            s => panic!("invlaid clone multi on {:?}", s)
        }

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
