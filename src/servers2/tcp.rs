use std::collections::hash_map::Entry as HashEntry;
use std::collections::VecDeque;
use std::io::{self, Read, Write, ErrorKind};
use std::{mem, thread};
use std::net::IpAddr;
use std::sync::mpsc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use prelude::*;
use servers2::{self, spmc, ServerLog, ToWorker, DistributeToWorkers};
use hash::{HashMap, FxHasher};

use mio;
use mio::tcp::*;

use buffer::Buffer;

/*
  GC with parrallel readers plan:
    each worker will have a globally visible field
      currently_reading
    padded to a cache line to prevent false sharing
    the GC core (which need not be the same as the index core)
    has a globally visible field (per chain?)
      greatest_collected
    to read:
      a worker first sets its currently_reading field to the loc it wants to read,
      it then checks greatest_collected,
      if greatest_collected >= currently_reading the entry is already gc'd and the worker returns the needed respose
      otherwise it returns the found value
    to gc:
      the gcer first sets greatest_collected to the point it wants to collect,
      then waits until none of the worker are reading a value in the collection range

    this does not deadlock b/c if the worker sets its then the gcer its the worker will abort

    this should be efficient b/c in normal operation the workers are just writing to a local value

    for multi entry values store refcount before entry,
    we want bit field which tells us where seq of entries end in allocator,
    use to distinguis btwn single and multi entries...
*/

/*
  better parrallelization plan:
    each worker performs the reads for a burst of cliemt requests (say 8KB worth)
    reads are done purely at the worker,
    and the worker copies from the entry directly to the tcp stack
    for writes, the requests and index from the server,
    then emplaces it as it currently does.
    it might pay to parrellelize the emplacement,
    by having the index thread distribute the placement jobs round robin
    (or other load balancing if we figure out one that works,
     actually, dumping all of the to-copies in a work-stealing queue might actually work...)
*/

/*
  options for replication:
    a. patition to-replica by chain,
       each chain gets sent in order
         + simple on replica,
           can reuse normal appends
         + paritioning over chains improves efficency even in single server case
         - each chain has limited throughput
           (at what point does this matter?)
         - we need to store multis seperately b/c they might not be delivered in order
           (but we want to do that anyway)
         - making sure muti appends get sent in the correct order seems hard

    b. store a trie of storage,
       send (size, storage location) to replica,
       use to detrimine location
*/

pub fn run(
    acceptor: TcpListener,
    this_server_num: u32,
    total_chain_servers: u32,
    num_workers: usize,
    ready: &AtomicUsize,
) -> ! {
    use std::cmp::max;

    const ACCEPT: mio::Token = mio::Token(0);
    const FROM_WORKERS: mio::Token = mio::Token(1);
    const NEXT_SERVER: mio::Token = mio::Token(2);
    const PREV_SERVER: mio::Token = mio::Token(3);

    //let (dist_to_workers, recv_from_dist) = spmc::channel();
    //let (log_to_workers, recv_from_log) = spmc::channel();
    //TODO or sync channel?
    let (workers_to_log, recv_from_workers) = mpsc::channel();
    //let (workers_to_dist, dist_from_workers) = mio_channel::channel();
    if num_workers == 0 {
        warn!("SERVER {} started with 0 workers", this_server_num);
    }

    let is_unreplicated = if true/*TODO upstream_ip.is_none() && downstream_ip.is_none()*/ {
        warn!("SERVER {} started without replication", this_server_num);
        true
    } else {
        false
    };

    let num_workers = max(num_workers, 1);

    
    let poll = mio::Poll::new().unwrap();
    poll.register(&acceptor,
        ACCEPT,
        mio::Ready::readable(),
        mio::PollOpt::level()
    ).expect("cannot start server poll");
    let mut events = mio::Events::with_capacity(1023);

    //let next_server_ip: Option<_> = Some(panic!());
    //let prev_server_ip: Option<_> = Some(panic!());
    let next_server_ip: Option<_> = None;
    let prev_server_ip: Option<_> = None;
    let mut downstream_admin_socket = None;
    let mut upstream_admin_socket = None;
    let mut other_sockets = Vec::new();
    while next_server_ip.is_some() && downstream_admin_socket.is_none() {
        poll.poll(&mut events, None).unwrap();
        for event in events.iter() {
            match event.token() {
                ACCEPT => {
                    match acceptor.accept() {
                        Err(e) => trace!("error {}", e),
                        Ok((socket, addr)) => if Some(addr.ip()) != next_server_ip {
                            other_sockets.push((socket, addr))
                        } else {
                            let _ = socket.set_keepalive_ms(Some(1000));
                            //TODO benchmark
                            let _ = socket.set_nodelay(true);
                            downstream_admin_socket = Some(socket)
                        }
                    }
                }
                _ => unreachable!()
            }
        }
    }

    if let Some(ip) = prev_server_ip {
        upstream_admin_socket = Some(TcpStream::connect(ip).expect("cannot connect upstream"));
    }

    /*TODO
    let num_downstream = negotiate_num_downstreams(&admin_socket);
    let num_upstream = negotiate_num_downstreams(&admin_socket);
    handle connections...
    let donwstream: Vec<_> = ...
    let upstream: Vec<_> = ...
    */

    let mut downstream = vec![]; // TODO Vec::with_capacity(num_downstream);
    let mut upstream = vec![]; // TODO Vec::with_capacity(num_ipstream);

    downstream_admin_socket.take().map(|s| downstream.push(s));
    upstream_admin_socket.take().map(|s| upstream.push(s));
    let num_downstream = downstream.len();
    let num_upstream = upstream.len();

    trace!("SERVER {} {} up, {} down.", this_server_num, num_upstream, num_downstream);
    trace!("SERVER {} starting {} workers.", this_server_num, num_workers);
    let mut log_to_workers: Vec<_> = Vec::with_capacity(num_workers);
    let mut dist_to_workers: Vec<_> = Vec::with_capacity(num_workers);
    for n in 0..num_workers {
        //let from_dist = recv_from_dist.clone();
        //let to_dist   = workers_to_dist.clone();
        //let from_log  = recv_from_log.clone();
        let to_log = workers_to_log.clone();
        let (to_worker, from_log) = spmc::channel();
        let (dist_to_worker, from_dist) = spmc::channel();
        let upstream = upstream.pop();
        let downstream = downstream.pop();
        thread::spawn(move ||
            Worker::new(
                from_dist,
                from_log,
                to_log,
                upstream,
                downstream,
                num_downstream,
                is_unreplicated,
                n,
            ).run()
        );
        log_to_workers.push(to_worker);
        dist_to_workers.push(dist_to_worker);
    }

    thread::spawn(move || {
        let mut log = ServerLog::new(this_server_num, total_chain_servers, log_to_workers);
        for (buffer, st) in recv_from_workers.iter() {
            log.handle_op(buffer, st)
        }
    });

    /*FIXME poll.register(&dist_from_workers,
        FROM_WORKERS,
        mio::Ready::readable(),
        mio::PollOpt::level()
    );*/
    ready.fetch_add(1, Ordering::SeqCst);
    //let mut receivers: HashMap<_, _> = Default::default();
    //FIXME should be a single writer hashmap
    let mut worker_for_client: HashMap<_, _> = Default::default();
    let mut next_token = mio::Token(10);
    //let mut buffer_cache = VecDeque::new();
    let mut next_worker = 0usize;
    trace!("SERVER start server loop");
    loop {
        poll.poll(&mut events, None).unwrap();
        for event in events.iter() {
            match event.token() {
                ACCEPT => {
                    match acceptor.accept() {
                        Err(e) => trace!("error {}", e),
                        Ok((socket, addr)) => {
                            trace!("SERVER accepting client @ {:?}", addr);
                            let _ = socket.set_keepalive_ms(Some(1000));
                            //TODO benchmark
                            let _ = socket.set_nodelay(true);
                            //TODO oveflow
                            let tok = get_next_token(&mut next_token);
                            /*poll.register(
                                &socket,
                                tok,
                                mio::Ready::readable(),
                                mio::PollOpt::edge() | mio::PollOpt::oneshot(),
                            );
                            receivers.insert(tok, Some(socket));*/
                            let buffer = Buffer::empty();
                            let worker = if is_unreplicated {
                                let worker = next_worker;
                                next_worker = next_worker.wrapping_add(1);
                                if next_worker >= dist_to_workers.len() {
                                    next_worker = 0;
                                }
                                next_worker
                            } else {
                                worker_for_ip(addr.ip(), num_workers as u64)
                            };
                            dist_to_workers[worker].send((tok, buffer, socket));
                            worker_for_client.insert(addr, worker);
                        }
                    }
                }
                FROM_WORKERS => {
                    trace!("SERVER dist getting finished sockets");
                    /*while let Ok((buffer, socket, tok)) = dist_from_workers.try_recv() {
                        trace!("SERVER dist got {:?}", tok);
                        //buffer_cache.push_back(buffer);
                        poll.reregister(
                            &socket,
                            tok,
                            mio::Ready::readable(),
                            mio::PollOpt::edge() | mio::PollOpt::oneshot(),
                        );
                        *receivers.get_mut(&tok).unwrap() = Some(socket)
                    }*/
                },
                _recv_tok => {
                    /*let recv = receivers.get_mut(&recv_tok).unwrap();
                    let recv = mem::replace(recv, None);
                    match recv {
                        None => trace!("spurious wakeup for {:?}", recv_tok),
                        Some(socket) => {
                            trace!("SERVER need to recv from {:?}", recv_tok);
                            //TODO should be min size ?
                            let buffer =
                                buffer_cache.pop_back().unwrap_or(Buffer::empty());
                            //dist_to_workers.send((buffer, socket, recv_tok))
                            dist_to_workers[next_worker].send((buffer, socket, recv_tok));
                            next_worker = next_worker.wrapping_add(1);
                            if next_worker >= dist_to_workers.len() {
                                next_worker = 0;
                            }
                        }
                    }*/
                }
            }
        }
    }
}

fn negotiate_num_downstreams(socket: &mut Option<TcpStream>, num_workers: u16) -> u16 {
    use std::cmp::min;
    if let Some(ref mut socket) = socket.as_mut() {
        let mut num_other_threads = [0u8; 2];
        blocking_read(socket, &mut num_other_threads).expect("downstream failed");
        let num_other_threads = unsafe { mem::transmute(num_other_threads) };
        let to_write: [u8; 2] = unsafe { mem::transmute(num_workers) };
        blocking_write(socket, &to_write).expect("downstream failed");
        min(num_other_threads, num_workers)
    }
    else {
        0
    }
}

fn negotiate_num_upstreams(socket: &mut Option<TcpStream>, num_workers: u16) -> u16 {
    use std::cmp::min;
    if let Some(ref mut socket) = socket.as_mut() {
        let to_write: [u8; 2] = unsafe { mem::transmute(num_workers) };
        blocking_write(socket, &to_write).expect("upstream failed");
        let mut num_other_threads = [0u8; 2];
        blocking_read(socket, &mut num_other_threads).expect("upstream failed");
        let num_other_threads = unsafe { mem::transmute(num_other_threads) };
        min(num_other_threads, num_workers)
    }
    else {
        0
    }
}

fn blocking_read<R: Read>(r: &mut R, mut buffer: &mut [u8]) -> io::Result<()> {
    //like Read::read_exact but doesn't die on WouldBlock
    'recv: while !buffer.is_empty() {
        match r.read(buffer) {
            Ok(i) => { let tmp = buffer; buffer = &mut tmp[i..]; }
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => continue 'recv,
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

fn blocking_write<W: Write>(w: &mut W, mut buffer: &[u8]) -> io::Result<()> {
    //like Write::write_all but doesn't die on WouldBlock
    'recv: while !buffer.is_empty() {
        match w.write(buffer) {
            Ok(i) => { let tmp = buffer; buffer = &tmp[i..]; }
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => continue 'recv,
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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Io { Log, Read, Write, ReadWrite }

#[repr(u8)]
enum IoBuffer { Log, Read(Buffer), Write(Buffer), ReadWrite(Buffer) }

struct Worker {
    clients: HashMap<mio::Token, PerSocket>,
    inner: WorkerInner,
}

struct WorkerInner {
    awake_io: VecDeque<mio::Token>,
    from_dist: spmc::Receiver<(mio::Token, Buffer, TcpStream)>,
    from_log: spmc::Receiver<ToWorker<(usize, mio::Token)>>,
    to_log: mpsc::Sender<(Buffer, (usize, mio::Token))>,
    worker_num: usize,
    downstream_workers: usize,
    poll: mio::Poll,
    is_unreplicated: bool,
}

/*
struct PerSocket {
    buffer: IoBuffer,
    stream: TcpStream,
    bytes_handled: usize,
    is_from_server: bool,
}
*/

#[derive(Debug)]
enum PerSocket {
    Upstream {
        being_read: VecDeque<Buffer>,
        bytes_read: usize,
        stream: TcpStream,
    },
    Downstream {
        being_written: Vec<u8>,
        bytes_written: usize,
        stream: TcpStream,
        pending: VecDeque<Vec<u8>>,
    },
    //FIXME Client should be divided into reader and writer?
    Client {
        being_read: VecDeque<Buffer>,
        bytes_read: usize,
        stream: TcpStream,
        being_written: Vec<u8>,
        bytes_written: usize,
        pending: VecDeque<Vec<u8>>,

    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum PerSocketKind {
    Upstream,
    Downstream,
    Client,
}
/*
struct Upstream {
    being_read: Buffer,
    stream: TcpStream,
    bytes_handled: usize,
}

struct Downstream {
    being_written: Vec<u8>,
    bytes_written: usize,
    stream: TcpStream,
    pending: VecDeque<Vec<u8>>,
}

struct Client {
    being_read: Buffer,
    bytes_read: usize,
    being_written: Vec<u8>,
    bytes_written: usize,
    stream: TcpStream,
    pending: VecDeque<Vec<u8>>,
}
*/
const FROM_DIST: mio::Token = mio::Token(0);
const FROM_LOG: mio::Token = mio::Token(1);
// we don't really need to special case this;
// all writes are bascially the same,
// but it's convenient for all workers to be in the same token space
const UPSTREAM: mio::Token = mio::Token(2);
const DOWNSTREAM: mio::Token = mio::Token(3);

// It's convenient to share a single token-space among all workers so any worker
// can determine who is responsible for a client
const FIRST_CLIENT_TOKEN: mio::Token = mio::Token(10);

const NUMBER_READ_BUFFERS: usize = 5;

/*
State machine:
  1. on receive from network:
       - write:
           a. lookup next hop
                + if client, lookup client handler (hashmap ip -> (thread num, Token))
                + if server lookup server handler  (either mod num downstream, magic in above map, or self)
           b. send to log thread with next net hop set (parrallel writes on ooo?)
       - read: send to log thread with next hop = me (TODO parallel lookup)
  2. on receive from log: do work
       + if next hop has write space send to next hop
       + else enqueue, wait for current burst to be sent
       + send buffer back to original owner
*/

impl Worker {

    fn new(
        from_dist: spmc::Receiver<(mio::Token, Buffer, TcpStream)>,
        from_log: spmc::Receiver<ToWorker<(usize, mio::Token)>>,
        to_log: mpsc::Sender<(Buffer, (usize, mio::Token))>,
        upstream: Option<TcpStream>,
        downstream: Option<TcpStream>,
        downstream_workers: usize,
        is_unreplicated: bool,
        worker_num: usize
    ) -> Self {
        let poll = mio::Poll::new().unwrap();
        poll.register(
            &from_dist,
            FROM_DIST,
            mio::Ready::readable(),
            mio::PollOpt::level()
        ).expect("cannot pol from dist on worker");
        poll.register(
            &from_log,
            FROM_LOG,
            mio::Ready::readable(),
            mio::PollOpt::level()
        ).expect("cannot pol from log on worker");
        let mut clients: HashMap<_, _> = Default::default();
        if let Some(up) = upstream {
            assert!(!is_unreplicated);
            poll.register(
                &up,
                UPSTREAM,
                mio::Ready::readable(),
                mio::PollOpt::edge()
            ).expect("cannot pol from dist on worker");
            let ps = PerSocket::upstream(up);
            clients.insert(UPSTREAM, ps);
        }
        if let Some(down) = downstream {
            assert!(!is_unreplicated);
            poll.register(
                &down,
                DOWNSTREAM,
                mio::Ready::readable(),
                mio::PollOpt::edge()
            ).expect("cannot pol from dist on worker");
            let ps = PerSocket::downstream(down);
            clients.insert(DOWNSTREAM, ps);
        }
        Worker {
            clients: clients,
            inner: WorkerInner {
                awake_io: Default::default(),
                from_dist: from_dist,
                from_log: from_log,
                to_log: to_log,
                poll: poll,
                worker_num: worker_num,
                is_unreplicated: is_unreplicated,
                downstream_workers: downstream_workers,
            }
        }
    }

    fn run(mut self) {
        let mut events = mio::Events::with_capacity(1024);
        loop {
            self.inner.poll.poll(&mut events, None).expect("worker poll failed");
            self.handle_new_events(events.iter());

            'work: loop {
                let ops_before_poll = self.inner.awake_io.len();
                for _ in 0..ops_before_poll {
                    let token = self.inner.awake_io.pop_front().unwrap();
                    if let HashEntry::Occupied(o) = self.clients.entry(token) {
                        self.inner.handle_burst(token, o.into_mut());
                    }
                }
                if self.inner.awake_io.is_empty() {
                    break 'work
                }
                self.inner.poll.poll(&mut events, Some(Duration::new(0, 0)))
                    .expect("worker poll failed");
                self.handle_new_events(events.iter());
            }
        }
    }// end fn run

    fn handle_new_events(&mut self, events: mio::EventsIter) {
        'event: for event in events {
            let token = event.token();
            if token == FROM_LOG {
                if let Some(log_work) = self.inner.from_log.try_recv() {
                    let to_send = servers2::handle_to_worker(log_work, self.inner.worker_num);
                    if let Some((buffer, (_, tok))) = to_send {
                        trace!("WORKER {} recv from log.", self.inner.worker_num);
                        //TODO
                        self.clients.get_mut(&tok).unwrap().add_send_buffer(buffer);
                        //TODO replace with wake function
                        self.inner.awake_io.push_back(tok);
                    }
                }
                continue 'event
            }

            if token == FROM_DIST {
                if let Some((tok, buffer, stream)) = self.inner.from_dist.try_recv() {
                    self.inner.poll.register(
                        &stream,
                        tok,
                        mio::Ready::readable() | mio::Ready::writable() | mio::Ready::error(),
                        mio::PollOpt::edge()
                    ).unwrap();
                    //self.clients.insert(tok, PerSocket::new(buffer, stream));
                    //TODO assert unique?
                    trace!("WORKER {} recv from dist.", self.inner.worker_num);
                    let state =
                        self.clients.entry(tok).or_insert(PerSocket::client(buffer, stream));
                    self.inner.recv_packet(tok, state);
                }
                continue 'event
            }

            if let HashEntry::Occupied(o) = self.clients.entry(token) {
                //TODO check token read/write
                let state = o.into_mut();
                self.inner.handle_burst(token, state)
            }
        }
    }// end handle_new_events
}

impl WorkerInner {

    fn handle_burst(
        &mut self,
        token: mio::Token,
        socket_state: &mut PerSocket
    ) {
        use self::PerSocket::*;
        let (send, recv) = match socket_state {
            &mut Upstream {..} => (false, true),
            &mut Downstream {..} => (true, false),
            &mut Client {..} => (true, true),
        };
        if send {
            trace!("WORKER {} will try to send.", self.worker_num);
            //FIXME need to disinguish btwn to client and to upstream
            self.send_burst(token, socket_state)
        }
        if recv {
            trace!("WORKER {} will try to recv.", self.worker_num);
            //FIXME need to disinguish btwn from client and from upstream
            self.recv_packet(token, socket_state)
        }
    }

    //TODO these functions should return Result so we can remove from map
    fn send_burst(
        &mut self,
        token: mio::Token,
        socket_state: &mut PerSocket
    ) {
        match socket_state.send_burst() {
            //TODO replace with wake function
            Ok(true) => self.awake_io.push_back(token),
            Ok(false) => {},
            //FIXME remove from map, log
            Err(..) => { let _ = self.poll.deregister(socket_state.stream()); }
        }
    }

    fn recv_packet(
        &mut self,
        token: mio::Token,
        socket_state: &mut PerSocket
    ) {
        let socket_kind = socket_state.kind();
        let packet = socket_state.recv_packet();
        match packet {
            //FIXME remove from map, log
            Err(..) => { let _ = self.poll.deregister(socket_state.stream()); },
            Ok(None) => {},
            Ok(Some(packet)) => {
                trace!("WORKER {} finished recv", self.worker_num);
                //FIXME which token handles the next hop is more complicated...
                let next_hop = self.next_hop(token, socket_kind);
                //FIXME needs worker num and IP addr...
                self.send_to_log(packet, token, socket_state);
                self.awake_io.push_back(token)
            }
        }
    }

    fn next_hop(&self, token: mio::Token, socket_kind: PerSocketKind) -> mio::Token {
        //FIXME might need IP addr...
        if self.is_unreplicated {
            return token
        }
        //FIXME which token handles the next hop is more complicated...
        unimplemented!()
    }

    fn send_to_log(&mut self, buffer: Buffer, token: mio::Token, socket_state: &mut PerSocket) {
        trace!("WORKER {} send to log", self.worker_num);
        self.to_log.send((buffer, (self.worker_num, token))).unwrap();
    }
}

type ShouldContinue = bool;

const WRITE_BUFFER_SIZE: usize = 40000;

impl PerSocket {
    /*
    Upstream {
        being_read: Buffer,
        bytes_read: usize,
        stream: TcpStream,
    },
    Downstream {
        being_written: Vec<u8>,
        bytes_written: usize,
        stream: TcpStream,
        pending: VecDeque<Vec<u8>>,
    },
    Client {
        being_read: Buffer,
        bytes_read: usize,
        stream: TcpStream,
        being_written: Vec<u8>,
        bytes_written: usize,
        pending: VecDeque<Vec<u8>>,
    }
    */
    fn client(buffer: Buffer, stream: TcpStream) -> Self {
        PerSocket::Client {
            being_read: (0..NUMBER_READ_BUFFERS).map(|_| Buffer::new()).collect(),
            bytes_read: 0,
            stream: stream,
            being_written: Vec::with_capacity(WRITE_BUFFER_SIZE),
            bytes_written: 0,
            pending: Default::default(),
        }
    }

    fn upstream(stream: TcpStream) -> Self {
        PerSocket::Upstream {
            being_read: (0..NUMBER_READ_BUFFERS).map(|_| Buffer::new()).collect(),
            bytes_read: 0,
            stream: stream,
        }
    }

    fn downstream(stream: TcpStream) -> Self {
        PerSocket::Downstream {
            being_written: Vec::with_capacity(WRITE_BUFFER_SIZE),
            bytes_written: 0,
            pending: Default::default(),
            stream: stream,
        }
    }

    fn kind(&self) -> PerSocketKind {
        match self {
            &PerSocket::Upstream{..} => PerSocketKind::Upstream,
            &PerSocket::Downstream{..} => PerSocketKind::Downstream,
            &PerSocket::Client{..} => PerSocketKind::Client,
        }
    }

    //TODO recv burst
    fn recv_packet(&mut self) -> Result<Option<Buffer>, ()> {
        use self::PerSocket::*;
        trace!("SOCKET try recv");
        match self {
            &mut Upstream {ref mut being_read, ref mut bytes_read, ref stream}
            | &mut Client {ref mut being_read, ref mut bytes_read, ref stream, ..} => {
                if let Some(mut read_buffer) = being_read.pop_front() {
                    trace!("SOCKET recv actual");
                    let recv = recv_packet(&mut read_buffer, stream, *bytes_read);
                    match recv {
                        //TODO send to log
                        RecvRes::Done => Ok(Some(read_buffer)),
                        //FIXME remove from map
                        RecvRes::Error => {
                            being_read.push_front(read_buffer);
                            Err(())
                        },

                        RecvRes::NeedsMore(total_read) => {
                            *bytes_read = total_read;
                            being_read.push_front(read_buffer);
                            Ok(None)
                        },
                    }
                }
                else {
                    trace!("SOCKET recv no buffer");
                    Ok(None)
                }
            }
            _ => unreachable!()
        }
    }

    fn send_burst(&mut self) -> Result<ShouldContinue, ()> {
        use self::PerSocket::*;
        match self {
            &mut Downstream {ref mut being_written, ref mut bytes_written, ref mut stream, ref mut pending}
            | &mut Client {ref mut being_written, ref mut bytes_written, ref mut stream, ref mut pending, ..} => {
                trace!("SOCKET send actual.");
                let to_write = being_written.len();
                if to_write == 0 {
                    trace!("SOCKET empty write.");
                    return Ok(false)
                }
                match stream.write(&being_written[*bytes_written..]) {
                    Err(e) => if e.kind() == ErrorKind::WouldBlock { Ok(false) }
                        else { Err(()) },
                    Ok(i) if (*bytes_written + i) < to_write => {
                        *bytes_written = *bytes_written + i;
                        Ok(false)
                    },
                    Ok(..) => {
                        trace!("SOCKET finished sending burst.");
                        //Done with burst check if more bursts to be sent
                        being_written.clear();
                        if let Some(buffer) = pending.pop_front() {
                            let old_buffer = mem::replace(being_written, buffer);
                            //TODO
                            if old_buffer.capacity() > WRITE_BUFFER_SIZE / 4 {
                                pending.push_back(old_buffer)
                            }
                        }
                        Ok(true)
                    },
                }
            },
            _ => unreachable!()
        }
    }

    fn add_send(&mut self, to_write: &[u8]) {
        //TODO Is there some maximum size at which we shouldn't buffer?
        //TODO can we simply write directly from the trie?
        use self::PerSocket::*;
        trace!("SOCKET add send");
        match self {
            &mut Downstream {ref mut being_written, ref mut bytes_written, ref stream, ref mut pending}
            | &mut Client {ref mut being_written, ref mut bytes_written, ref stream, ref mut pending, ..} => {
                //TODO if being_written is empty try to send immdiately, place remainder
                if being_written.capacity() - being_written.len() >= to_write.len() {
                    being_written.extend_from_slice(to_write)
                } else {
                    let placed = if let Some(buffer) = pending.back_mut() {
                        if buffer.capacity() - buffer.len() >= to_write.len() {
                            buffer.extend_from_slice(to_write);
                            true
                        } else { false }
                    } else { false };
                    if !placed {
                        pending.push_back(to_write.to_vec())
                    }
                }
            },
            _ => unreachable!()
        }        
    }

    fn add_send_buffer(&mut self, to_write: Buffer) {
        //TODO Is there some maximum size at which we shouldn't buffer?
        //TODO can we simply write directly from the trie?
        use self::PerSocket::*;
        trace!("SOCKET add send_b");
        match self {
            &mut Downstream {ref mut being_written, ref mut bytes_written, ref stream, ref mut pending} => {
                //TODO if being_written is empty try to send immdiately, place remainder
                if being_written.capacity() - being_written.len() >= to_write.entry_size() {
                    being_written.extend_from_slice(to_write.entry_slice())
                } else {
                    let placed = if let Some(buffer) = pending.back_mut() {
                        if buffer.capacity() - buffer.len() >= to_write.entry_size() {
                            buffer.extend_from_slice(to_write.entry_slice());
                            true
                        } else { false }
                    } else { false };
                    if !placed {
                        pending.push_back(to_write.entry_slice().to_vec())
                    }
                }
                unimplemented!()
            }

            &mut Client {ref mut being_written, ref mut bytes_written, ref stream, ref mut pending, ref mut being_read, ..} => {
                //TODO if being_written is empty try to send immdiately, place remainder
                if being_written.capacity() - being_written.len() >= to_write.entry_size() {
                    being_written.extend_from_slice(to_write.entry_slice())
                } else {
                    let placed = if let Some(buffer) = pending.back_mut() {
                        if buffer.capacity() - buffer.len() >= to_write.entry_size() {
                            buffer.extend_from_slice(to_write.entry_slice());
                            true
                        } else { false }
                    } else { false };
                    if !placed {
                        pending.push_back(to_write.entry_slice().to_vec())
                    }
                }
                //FIXME make sure the buffer goes to the right worker
                trace!("SOCKET re-add buffer");
                being_read.push_back(to_write);
            },
            _ => unreachable!()
        }        
    }

    fn stream(&self) -> &TcpStream {
        use self::PerSocket::*;
        match self {
            &Downstream {ref stream, ..} | &Client {ref stream, ..} | &Upstream {ref stream, ..} =>
                stream,
        }
    }
}

impl IoBuffer {
    fn send_buffer(&self) -> &Buffer {
        match self {
            &IoBuffer::Write(ref buffer) => buffer,
            _ => unreachable!(),
        }
    }

    fn recv_buffer(&mut self) -> &mut Buffer {
        match self {
            &mut IoBuffer::Read(ref mut buffer) => buffer,
            _ => unreachable!(),
        }
    }
}

enum SendRes {
    Done,
    Error,
    NeedsMore(usize),
}

fn send_packet(buffer: &Buffer, mut stream: &TcpStream, sent: usize) -> SendRes {
    let bytes_to_write = buffer.entry_slice().len();
    match stream.write(&buffer.entry_slice()[sent..]) {
       Ok(i) if (sent + i) < bytes_to_write => SendRes::NeedsMore(sent + i),
       Ok(..) => SendRes::Done,
       Err(e) => if e.kind() == ErrorKind::WouldBlock { SendRes::NeedsMore(sent) }
                 else { SendRes::Error },
   }
}

enum RecvRes {
    Done,
    Error,
    NeedsMore(usize),
}

fn recv_packet(buffer: &mut Buffer, mut stream: &TcpStream, mut read: usize) -> RecvRes {
    let bhs = base_header_size();
    if read < bhs {
        let r = stream.read(&mut buffer[read..bhs])
            .or_else(|e| if e.kind() == ErrorKind::WouldBlock { Ok(read) } else { Err(e) } )
            .ok();
        match r {
            Some(i) => read += i,
            None => return RecvRes::Error,
        }
        if read < bhs {
            return RecvRes::NeedsMore(read)
        }
    }

    let header_size = buffer.entry().header_size();
    assert!(header_size >= base_header_size());
    if read < header_size {
        let r = stream.read(&mut buffer[read..header_size])
            .or_else(|e| if e.kind() == ErrorKind::WouldBlock { Ok(read) } else { Err(e) } )
            .ok();
        match r {
            Some(i) => read += i,
            None => return RecvRes::Error,
        }
        if read < header_size {
            return RecvRes::NeedsMore(read)
        }
    }

    let size = buffer.entry().entry_size();
    if read < size {
        let r = stream.read(&mut buffer[read..size])
            .or_else(|e| if e.kind() == ErrorKind::WouldBlock { Ok(read) } else { Err(e) } )
            .ok();
        match r {
            Some(i) => read += i,
            None => return RecvRes::Error,
        }
        if read < size {
            return RecvRes::NeedsMore(read);
        }
    }
    debug_assert!(buffer.packet_fits());
    // assert!(payload_size >= header_size);
    trace!("WORKER finished recv");
    RecvRes::Done
}

fn get_next_token(token: &mut mio::Token) -> mio::Token {
    let next = token.0.wrapping_add(1);
    if next == 0 { *token = mio::Token(2) }
    else { *token = mio::Token(next) };
    *token
}

impl DistributeToWorkers<(usize, mio::Token)>
for Vec<spmc::Sender<ToWorker<(usize, mio::Token)>>> {
    fn send_to_worker(&mut self, msg: ToWorker<(usize, mio::Token)>) {
        let (which_queue, token) = msg.get_associated_data();
        trace!("SERVER   sending to worker {} {:?} ", which_queue, token);
        self[which_queue].send(msg)
    }
}

fn worker_for_ip(ip: IpAddr, num_workers: u64) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher: FxHasher = Default::default();
    ip.hash(&mut hasher);
    (hasher.finish() % num_workers) as usize
}
