use std::collections::hash_map::Entry as HashEntry;
use std::collections::VecDeque;
use std::io::{self, Read, Write, ErrorKind};
use std::{mem, thread};
use std::net::{IpAddr, SocketAddr};
use std::sync::mpsc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use prelude::*;
use servers2::{self, spmc, ServerLog, ToReplicate, ToWorker, DistributeToWorkers};
use hash::{HashMap, FxHasher};
use socket_addr::Ipv4SocketAddr;

use byteorder::{ByteOrder, LittleEndian};

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

      we don't worry about gc during write (that seems like a silly scenario),
      so we can send the log's copy downstream and don't have to worry about returning buffers.
      during reads, the worker which receives the read req is the one which sends,
      so the above scheme will work

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

  Hole filling for parrallel replication:
    if read sees hole before horizon, and before lock:
    send fill_hole to head of chain
      if chain has hole => failure to replicate,
        fill hole down chain
      else write in progress,
        read again
*/

//Dist tokens
const ACCEPT: mio::Token = mio::Token(0);
const FROM_WORKERS: mio::Token = mio::Token(1);
const DIST_FROM_LOG: mio::Token = mio::Token(2);

//Worker tokens
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

type WorkerNum = usize;

enum DistToWorker {
    NewClient(mio::Token, TcpStream),
    //ToClient(ToWorker<(usize, mio::Token, Ipv4SocketAddr)>),
    ToClient(mio::Token, &'static [u8], Ipv4SocketAddr, u64),
}

//FIXME we should use something more accurate than &static [u8],
enum WorkerToDist {
    Downstream(WorkerNum, Ipv4SocketAddr, &'static [u8], u64),
    ToClient(Ipv4SocketAddr, &'static [u8]),
}

enum ToLog<T> {
    //TODO test different layouts.
    New(Buffer, Option<Box<(Box<[u8]>, Box<[u8]>)>>, T),
    Replication(ToReplicate, T)
}

pub fn run(
    acceptor: TcpListener,
    this_server_num: u32,
    total_chain_servers: u32,
    num_workers: usize,
    ready: &AtomicUsize,
) -> ! {
    run_with_replication(acceptor, this_server_num, total_chain_servers, None, None, num_workers, ready)
}

pub fn run_with_replication(
    acceptor: TcpListener,
    this_server_num: u32,
    total_chain_servers: u32,
    prev_server: Option<SocketAddr>,
    next_server: Option<IpAddr>,
    num_workers: usize,
    ready: &AtomicUsize,
) -> ! {
    use std::cmp::max;

    //let (dist_to_workers, recv_from_dist) = spmc::channel();
    //let (log_to_workers, recv_from_log) = spmc::channel();
    //TODO or sync channel?
    let (workers_to_log, recv_from_workers) = mpsc::channel();
    let (workers_to_dist, dist_from_workers) = mio::channel::channel();
    if num_workers == 0 {
        warn!("SERVER {} started with 0 workers.", this_server_num);
    }

    let is_unreplicated = prev_server.is_none() && next_server.is_none();
    if is_unreplicated {
        warn!("SERVER {} started without replication.", this_server_num);
    }
    else {
        trace!("SERVER {} prev: {:?}, next: {:?}.", this_server_num, prev_server, next_server);
    }

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
    let next_server_ip: Option<_> = next_server;
    let prev_server_ip: Option<_> = prev_server;
    let mut downstream_admin_socket = None;
    let mut upstream_admin_socket = None;
    let mut other_sockets = Vec::new();
    while next_server_ip.is_some() && downstream_admin_socket.is_none() {
        trace!("SERVER {} waiting for downstream {:?}.", this_server_num, next_server);
        poll.poll(&mut events, None).unwrap();
        for event in events.iter() {
            match event.token() {
                ACCEPT => {
                    match acceptor.accept() {
                        Err(e) => trace!("error {}", e),
                        Ok((socket, addr)) => if Some(addr.ip()) != next_server_ip {
                            trace!("SERVER got other connection {:?}", addr);
                            other_sockets.push((socket, addr))
                        } else {
                            trace!("SERVER {} connected downstream.", this_server_num);
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

    if let Some(ref ip) = prev_server_ip {
        trace!("SERVER {} waiting for upstream {:?}.", this_server_num, prev_server_ip);
        upstream_admin_socket = Some(TcpStream::connect(ip).expect("cannot connect upstream"));
        trace!("SERVER {} connected upstream.", this_server_num);
    }

    let num_downstream = negotiate_num_downstreams(&mut downstream_admin_socket, num_workers as u16);
    let num_upstream = negotiate_num_upstreams(&mut upstream_admin_socket, num_workers as u16);
    let mut downstream = Vec::with_capacity(num_downstream);
    let mut upstream = Vec::with_capacity(num_upstream);
    while downstream.len() + 1 < num_downstream {
        poll.poll(&mut events, None).unwrap();
        for event in events.iter() {
            match event.token() {
                ACCEPT => {
                    match acceptor.accept() {
                        Err(e) => trace!("error {}", e),
                        Ok((socket, addr)) => if Some(addr.ip()) == next_server_ip {
                            trace!("SERVER {} add downstream.", this_server_num);
                            let _ = socket.set_keepalive_ms(Some(1000));
                            //TODO benchmark
                            let _ = socket.set_nodelay(true);
                            downstream.push(socket)
                        } else {
                            trace!("SERVER got other connection {:?}", addr);
                            other_sockets.push((socket, addr))
                        }
                    }
                }
                _ => unreachable!()
            }
        }
    }

    if let Some(ref ip) = prev_server_ip {
        for _ in 1..num_upstream {
            upstream.push(TcpStream::connect(ip).expect("cannot connect upstream"))
        }
    }

    downstream_admin_socket.take().map(|s| downstream.push(s));
    upstream_admin_socket.take().map(|s| upstream.push(s));
    assert_eq!(downstream.len(), num_downstream);
    assert_eq!(upstream.len(), num_upstream);
    let (log_to_dist, dist_from_log) = spmc::channel();

    trace!("SERVER {} {} up, {} down.", this_server_num, num_upstream, num_downstream);
    trace!("SERVER {} starting {} workers.", this_server_num, num_workers);
    let mut log_to_workers: Vec<_> = Vec::with_capacity(num_workers);
    let mut dist_to_workers: Vec<_> = Vec::with_capacity(num_workers);
    for n in 0..num_workers {
        //let from_dist = recv_from_dist.clone();
        let to_dist   = workers_to_dist.clone();
        //let from_log  = recv_from_log.clone();
        let to_log = workers_to_log.clone();
        let (to_worker, from_log) = spmc::channel();
        let (dist_to_worker, from_dist) = spmc::channel();
        let upstream = upstream.pop();
        let downstream = downstream.pop();
        thread::spawn(move ||
            Worker::new(
                from_dist,
                to_dist,
                from_log,
                to_log,
                upstream,
                downstream,
                num_downstream,
                num_workers,
                is_unreplicated,
                n,
            ).run()
        );
        log_to_workers.push(to_worker);
        dist_to_workers.push(dist_to_worker);
    }
    assert_eq!(dist_to_workers.len(), num_workers);

    log_to_workers.push(log_to_dist);

    poll.register(
        &dist_from_log,
        DIST_FROM_LOG,
        mio::Ready::readable(),
        mio::PollOpt::level()
    ).expect("cannot pol from log on dist");

    thread::spawn(move || {
        let mut log = ServerLog::new(this_server_num, total_chain_servers, log_to_workers);
        for to_log in recv_from_workers.iter() {
            match to_log {
                ToLog::New(buffer, storage, st) => log.handle_op(buffer, storage, st),
                ToLog::Replication(tr, st) => log.handle_replication(tr, st),
            }

        }
    });

    poll.register(&dist_from_workers,
        FROM_WORKERS,
        mio::Ready::readable(),
        mio::PollOpt::level()
    ).unwrap();
    ready.fetch_add(1, Ordering::SeqCst);
    //let mut receivers: HashMap<_, _> = Default::default();
    //FIXME should be a single writer hashmap
    let mut worker_for_client: HashMap<_, _> = Default::default();
    let mut next_token = FIRST_CLIENT_TOKEN;
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
                            let worker = if is_unreplicated {
                                let worker = next_worker;
                                next_worker = next_worker.wrapping_add(1);
                                if next_worker >= dist_to_workers.len() {
                                    next_worker = 0;
                                }
                                worker
                            } else {
                                worker_for_ip(addr.ip(), num_workers as u64)
                            };
                            dist_to_workers[worker].send(DistToWorker::NewClient(tok, socket));
                            worker_for_client.insert(
                                Ipv4SocketAddr::from_socket_addr(addr), (worker, tok));
                            //FIXME tell other workers
                        }
                    }
                }
                FROM_WORKERS => {
                    trace!("SERVER dist getting finished work");
                    let packet = dist_from_workers.try_recv();
                    if let Ok(to_worker) = packet {
                        let (worker, token, buffer, addr, storage_loc) = match to_worker {
                            WorkerToDist::Downstream(worker, addr, buffer, storage_loc) => {
                                trace!("DIST {} downstream worker for {} is {}.",
                                    this_server_num, addr, worker);
                                (worker, DOWNSTREAM, buffer, addr, storage_loc)
                            },

                            WorkerToDist::ToClient(addr, buffer) => {
                                trace!("DIST {} looking for worker for {}.",
                                    this_server_num, addr);
                                let (worker, token) = worker_for_client[&addr].clone();
                                (worker, token, buffer, addr, 0)
                            }
                        };
                        dist_to_workers[worker].send(
                            DistToWorker::ToClient(token, buffer, addr, storage_loc))

                    }
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
                DIST_FROM_LOG => {
                    //unreachable!()
                    //FIXME handle completing work on original thread, only do send on DOWNSTREAM
                    /*let packet = dist_from_log.try_recv();
                    if let Some(mut to_worker) = packet {
                        let (_, _, addr) = to_worker.get_associated_data();
                        trace!("DIST {} looking for worker for {}.", this_server_num, addr);
                        let (worker, token) = worker_for_client[&addr].clone();
                        to_worker.edit_associated_data(|t| t.1 = token);
                        dist_to_workers[worker].send(DistToWorker::ToClient(to_worker))
                    }*/
                },
                _recv_tok => {
                    //unreachable!()
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

fn negotiate_num_downstreams(socket: &mut Option<TcpStream>, num_workers: u16) -> usize {
    use std::cmp::min;
    if let Some(ref mut socket) = socket.as_mut() {
        let mut num_other_threads = [0u8; 2];
        blocking_read(socket, &mut num_other_threads).expect("downstream failed");
        let num_other_threads = unsafe { mem::transmute(num_other_threads) };
        let to_write: [u8; 2] = unsafe { mem::transmute(num_workers) };
        blocking_write(socket, &to_write).expect("downstream failed");
        trace!("SERVER down workers: {}, other's workers {}.", num_workers, num_other_threads);
        min(num_other_threads, num_workers) as usize
    }
    else {
        trace!("SERVER no downstream.");
        0
    }
}

fn negotiate_num_upstreams(socket: &mut Option<TcpStream>, num_workers: u16) -> usize {
    use std::cmp::min;
    if let Some(ref mut socket) = socket.as_mut() {
        let to_write: [u8; 2] = unsafe { mem::transmute(num_workers) };
        blocking_write(socket, &to_write).expect("upstream failed");
        let mut num_other_threads = [0u8; 2];
        blocking_read(socket, &mut num_other_threads).expect("upstream failed");
        let num_other_threads = unsafe { mem::transmute(num_other_threads) };
        trace!("SERVER up workers: {}, other's workers {}.", num_workers, num_other_threads);
        min(num_other_threads, num_workers) as usize
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

struct Worker {
    clients: HashMap<mio::Token, PerSocket>,
    inner: WorkerInner,
}

struct WorkerInner {
    awake_io: VecDeque<mio::Token>,
    from_dist: spmc::Receiver<DistToWorker>,
    to_dist: mio::channel::Sender<WorkerToDist>,
    from_log: spmc::Receiver<ToWorker<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
    to_log: mpsc::Sender<ToLog<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
    ip_to_worker: HashMap<Ipv4SocketAddr, (WorkerNum, mio::Token)>,
    worker_num: WorkerNum,
    downstream_workers: WorkerNum,
    num_workers: WorkerNum,
    poll: mio::Poll,
    is_unreplicated: bool,
    has_downstream: bool,
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
        from_dist: spmc::Receiver<DistToWorker>,
        to_dist: mio::channel::Sender<WorkerToDist>,
        from_log: spmc::Receiver<ToWorker<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
        to_log: mpsc::Sender<ToLog<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
        upstream: Option<TcpStream>,
        downstream: Option<TcpStream>,
        downstream_workers: usize,
        num_workers: usize,
        is_unreplicated: bool,
        worker_num: WorkerNum,
    ) -> Self {
        assert!(downstream_workers <= num_workers);
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
        let mut has_downstream = false;
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
            has_downstream = true;
        }
        Worker {
            clients: clients,
            inner: WorkerInner {
                awake_io: Default::default(),
                from_dist: from_dist,
                to_dist: to_dist,
                from_log: from_log,
                to_log: to_log,
                ip_to_worker: Default::default(),
                poll: poll,
                worker_num: worker_num,
                is_unreplicated: is_unreplicated,
                downstream_workers: downstream_workers,
                num_workers: num_workers,
                has_downstream: has_downstream,
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
                    if let Some((buffer, bytes, (wk, token, src_addr), storage_loc)) = to_send {
                        trace!("WORKER {} recv from log for {}.",
                            self.inner.worker_num, src_addr);
                        debug_assert_eq!(wk, self.inner.worker_num);
                        let worker_tok = if src_addr != Ipv4SocketAddr::nil() {
                            self.inner.next_hop(token, src_addr)
                        } else {
                            Some((self.inner.worker_num, token))
                        };
                        let (worker, tok) = match worker_tok {
                            None => {
                                self.inner.to_dist.send(WorkerToDist::ToClient(src_addr, bytes)).ok().unwrap();
                                self.clients.get_mut(&token).unwrap().return_buffer(buffer);
                                continue 'event
                            }
                            Some((ref worker, ref tok))
                            if *worker != self.inner.worker_num && *tok != DOWNSTREAM => {
                                self.inner.to_dist.send(WorkerToDist::ToClient(src_addr, bytes)).ok().unwrap();
                                self.clients.get_mut(&token).unwrap().return_buffer(buffer);
                                continue 'event
                            }
                            Some((worker, tok)) => {
                                (worker, tok)
                            }
                        };
                        if worker != self.inner.worker_num {
                            assert_eq!(tok, DOWNSTREAM);
                            self.inner.to_dist.send(WorkerToDist::Downstream(worker, src_addr, bytes, storage_loc)).ok().unwrap();
                            self.clients.get_mut(&token).unwrap().return_buffer(buffer);
                            continue 'event
                        }
                        if tok == DOWNSTREAM {
                            {
                                let client = self.clients.get_mut(&tok).unwrap();
                                client.add_downstream_send(bytes);
                                client.add_downstream_send(src_addr.bytes());
                                let mut storage_log_bytes: [u8; 8] = [0; 8];
                                LittleEndian::write_u64(&mut storage_log_bytes, storage_loc);
                                client.add_downstream_send(&storage_log_bytes);
                            }
                            self.clients.get_mut(&token).unwrap().return_buffer(buffer);
                            self.inner.awake_io.push_back(token);
                        }
                        else {
                            //self.clients.get_mut(&tok).unwrap().add_send_buffer(buffer);
                            self.clients.get_mut(&tok).unwrap()
                                .add_downstream_send(buffer.entry_slice());
                            self.clients.get_mut(&token).unwrap().return_buffer(buffer);
                            self.inner.awake_io.push_back(token);
                        }
                        //TODO replace with wake function
                        self.inner.awake_io.push_back(tok);
                    }
                }
                continue 'event
            }

            if token == FROM_DIST {
                match self.inner.from_dist.try_recv() {
                    None => {},
                    Some(DistToWorker::NewClient(tok, stream)) => {
                        self.inner.poll.register(
                            &stream,
                            tok,
                            mio::Ready::readable() | mio::Ready::writable() | mio::Ready::error(),
                            mio::PollOpt::edge()
                        ).unwrap();
                        //self.clients.insert(tok, PerSocket::new(buffer, stream));
                        //TODO assert unique?
                        trace!("WORKER {} recv from dist.", self.inner.worker_num);
                        let client_addr = Ipv4SocketAddr::from_socket_addr(stream.peer_addr().unwrap());
                        self.inner.ip_to_worker.insert(client_addr, (self.inner.worker_num, tok));
                        let state =
                            self.clients.entry(tok).or_insert(PerSocket::client(stream));
                        self.inner.recv_packet(tok, state);
                    },
                    Some(DistToWorker::ToClient(DOWNSTREAM, buffer, src_addr, storage_loc)) => {
                        trace!("WORKER {} recv downstream from dist for {}.",
                            self.inner.worker_num, src_addr);
                        {
                            let client = self.clients.get_mut(&DOWNSTREAM).unwrap();
                            client.add_downstream_send(buffer);
                            client.add_downstream_send(src_addr.bytes());
                            let mut storage_log_bytes: [u8; 8] = [0; 8];
                            LittleEndian::write_u64(&mut storage_log_bytes, storage_loc);
                            client.add_downstream_send(&storage_log_bytes)
                        }
                        //TODO replace with wake function
                        self.inner.awake_io.push_back(DOWNSTREAM);
                    }
                    Some(DistToWorker::ToClient(tok, buffer, src_addr, storage_loc)) => {
                        debug_assert_eq!(
                            Ipv4SocketAddr::from_socket_addr(
                                    self.clients[&tok].stream().peer_addr().unwrap()
                            ),
                            src_addr
                        );
                        debug_assert_eq!(storage_loc, 0);
                        trace!("WORKER {} recv to_client from dist for {}.", self.inner.worker_num, src_addr);
                        assert!(tok >= FIRST_CLIENT_TOKEN);
                        self.clients.get_mut(&tok).unwrap().add_downstream_send(buffer);
                        //TODO replace with wake function
                        self.inner.awake_io.push_back(tok);
                    },
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
            RecvPacket::Err => { let _ = self.poll.deregister(socket_state.stream()); },
            RecvPacket::Pending => {},
            RecvPacket::FromClient(packet, src_addr) => {
                trace!("WORKER {} finished recv from client.", self.worker_num);
                let worker = self.worker_num;
                self.send_to_log(packet, worker, token, src_addr);
                self.awake_io.push_back(token)
            }
            RecvPacket::FromUpstream(packet, src_addr, storage_loc) => {
                trace!("WORKER {} finished recv from up.", self.worker_num);
                //let (worker, work_tok) = self.next_hop(token, src_addr, socket_kind);
                let worker = self.worker_num;
                self.send_replication_to_log(packet, storage_loc, worker, token, src_addr);
                self.awake_io.push_back(token)
            }
        }
    }

    fn next_hop(
        &self,
        token: mio::Token,
        src_addr: Ipv4SocketAddr,
    ) -> Option<(usize, mio::Token)> {
        //TODO specialize based on read/write sockets?
        if self.is_unreplicated {
            trace!("WORKER {} is unreplicated.", self.worker_num);
            return Some((self.worker_num, token))
        }
        else if self.downstream_workers == 0 { //if self.is_end_of_chain
            trace!("WORKER {} is end of chain.", self.worker_num);
            return match self.worker_and_token_for_addr(src_addr) {
                Some(worker_and_token) => {
                    trace!("WORKER {} send to_client to {:?}",
                        self.worker_num, worker_and_token);
                    Some(worker_and_token)
                },
                None => None,
            }
        }
        else if self.has_downstream {
            return Some((self.worker_num, DOWNSTREAM))
        }
        else {
            trace!("WORKER {} send DOWNSTREAM to {}",
                self.worker_num, self.worker_num % self.downstream_workers);
            //TODO actually balance this
            return Some((self.worker_num % self.downstream_workers, DOWNSTREAM))
        }
    }

    fn worker_and_token_for_addr(&self, addr: Ipv4SocketAddr)
    -> Option<(WorkerNum, mio::Token)> {
        self.ip_to_worker.get(&addr).cloned()
    }

    fn send_to_log(
        &mut self,
        buffer: Buffer,
        worker_num: usize,
        token: mio::Token,
        src_addr: Ipv4SocketAddr,
    ) {
        trace!("WORKER {} send to log", self.worker_num);
        let kind = buffer.entry().kind.layout();
        let storage = match kind {
            EntryLayout::Multiput | EntryLayout::Sentinel => {
                let (size, senti_size) = {
                    let e = buffer.entry();
                    (e.entry_size(), e.sentinel_entry_size())
                };
                unsafe {
                    let mut m = Vec::with_capacity(size);
                    let mut s = Vec::with_capacity(senti_size);
                    m.set_len(size);
                    s.set_len(senti_size);
                    Some(Box::new((m.into_boxed_slice(), s.into_boxed_slice())))
                }
            }
            _ => None,
        };
        let to_send = ToLog::New(buffer, storage, (worker_num, token, src_addr));
        self.to_log.send(to_send).unwrap();
    }

    fn send_replication_to_log(
        &mut self,
        buffer: Buffer,
        storage_addr: u64,
        worker_num: usize,
        token: mio::Token,
        src_addr: Ipv4SocketAddr,
    ) {
        trace!("WORKER {} send replica to log", self.worker_num);
        let kind = buffer.entry().kind.layout();
        let to_send = match kind {
            EntryLayout::Data => {
                ToReplicate::Data(buffer, storage_addr)
            },
            EntryLayout::Lock => {
                ToReplicate::UnLock(buffer)
            },
            //TODO
            EntryLayout::Multiput | EntryLayout::Sentinel => {
                let (size, senti_size) = {
                    let e = buffer.entry();
                    (e.entry_size(), e.sentinel_entry_size())
                };
                let (multi_storage, senti_storage) = unsafe {
                    let mut m = Vec::with_capacity(size);
                    let mut s = Vec::with_capacity(senti_size);
                    m.set_len(size);
                    s.set_len(senti_size);
                    (m.into_boxed_slice(), s.into_boxed_slice())
                };
                ToReplicate::Multi(buffer, multi_storage, senti_storage)
            },
            _ => unreachable!(),
        };
        let to_send = ToLog::Replication(to_send, (worker_num, token, src_addr));
        self.to_log.send(to_send).unwrap();
    }

}

type ShouldContinue = bool;

const WRITE_BUFFER_SIZE: usize = 40000;

enum RecvPacket {
    Err,
    Pending,
    FromUpstream(Buffer, Ipv4SocketAddr, u64),
    FromClient(Buffer, Ipv4SocketAddr),
}

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
    fn client(stream: TcpStream) -> Self {
        PerSocket::Client {
            being_read: (0..NUMBER_READ_BUFFERS).map(|_| Buffer::no_drop()).collect(),
            bytes_read: 0,
            stream: stream,
            being_written: Vec::with_capacity(WRITE_BUFFER_SIZE),
            bytes_written: 0,
            pending: Default::default(),
        }
    }

    fn upstream(stream: TcpStream) -> Self {
        PerSocket::Upstream {
            being_read: (0..NUMBER_READ_BUFFERS).map(|_| Buffer::no_drop()).collect(),
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
    fn recv_packet(&mut self) -> RecvPacket {
        use self::PerSocket::*;
        trace!("SOCKET try recv");
        match self {
            &mut Upstream {ref mut being_read, ref mut bytes_read, ref stream} => {
                if let Some(mut read_buffer) = being_read.pop_front() {
                    trace!("SOCKET recv actual");
                    let recv = recv_packet(&mut read_buffer, stream, *bytes_read, mem::size_of::<u64>());
                    match recv {
                        //TODO send to log
                        RecvRes::Done(src_addr) => {
                            *bytes_read = 0;
                            trace!("SOCKET recevd replication for {}.", src_addr);
                            let entry_size = read_buffer.entry_size();
                            let end = entry_size + mem::size_of::<u64>();
                            let storage_loc = LittleEndian::read_u64(&read_buffer[entry_size..end]);;
                            RecvPacket::FromUpstream(read_buffer, src_addr, storage_loc)
                        },
                        //FIXME remove from map
                        RecvRes::Error => {
                            *bytes_read = 0;
                            trace!("error; returned buffer now @ {}", being_read.len());
                            being_read.push_front(read_buffer);
                            RecvPacket::Err
                        },

                        RecvRes::NeedsMore(total_read) => {
                            *bytes_read = total_read;
                            being_read.push_front(read_buffer);
                            RecvPacket::Pending
                        },
                    }
                }
                else {
                    trace!("SOCKET Upstream recv no buffer");
                    RecvPacket::Pending
                }
            }
            &mut Client {ref mut being_read, ref mut bytes_read, ref stream, ..} => {
                if let Some(mut read_buffer) = being_read.pop_front() {
                    trace!("SOCKET recv actual");
                    let recv = recv_packet(&mut read_buffer, stream, *bytes_read, 0);
                    match recv {
                        //TODO send to log
                        RecvRes::Done(src_addr) => {
                            *bytes_read = 0;
                            trace!("SOCKET recevd for {}.", src_addr);
                            RecvPacket::FromClient(read_buffer, src_addr)
                        },
                        //FIXME remove from map
                        RecvRes::Error => {
                            *bytes_read = 0;
                            trace!("error; returned buffer now @ {}", being_read.len());
                            being_read.push_front(read_buffer);
                            RecvPacket::Err
                        },

                        RecvRes::NeedsMore(total_read) => {
                            *bytes_read = total_read;
                            being_read.push_front(read_buffer);
                            RecvPacket::Pending
                        },
                    }
                }
                else {
                    trace!("SOCKET Client recv no buffer");
                    RecvPacket::Pending
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
                        trace!("SOCKET sent {}B.", bytes_written);
                        Ok(false)
                    },
                    Ok(i) => {
                        trace!("SOCKET finished sending burst {}B.", *bytes_written + i);
                        //Done with burst check if more bursts to be sent
                        debug_assert_eq!(*bytes_written + i, being_written.len());
                        being_written.clear();
                        *bytes_written = 0;
                        if let Some(buffer) = pending.pop_front() {
                            if !buffer.is_empty() {
                                trace!("SOCKET swap buffer.");
                                let old_buffer = mem::replace(being_written, buffer);
                                if pending.is_empty() {
                                    pending.push_back(old_buffer);
                                    return Ok(true)
                                }
                                let (should_replace, add_anyway) = pending.back().map(|v| {
                                    let should_replace =
                                        v.is_empty()
                                        && v.capacity() <= old_buffer.capacity();
                                    let add_anyway =
                                        !v.is_empty() &&
                                        old_buffer.capacity() > WRITE_BUFFER_SIZE / 4;
                                    (should_replace, add_anyway)
                                }).unwrap_or((false, false));
                                if should_replace {
                                    pending.back_mut().map(|v| *v = old_buffer);
                                    return Ok(true)
                                }
                                if add_anyway {
                                    pending.push_back(old_buffer)
                                }
                                return Ok(true)
                            } else {
                                debug_assert!(pending.front().map(|v| v.is_empty()).unwrap_or(true));
                                debug_assert!(pending.back().map(|v| v.is_empty()).unwrap_or(true));
                                pending.push_front(buffer)
                            }

                        }
                        Ok(true)
                    },
                }
            },
            _ => unreachable!()
        }
    }

    fn add_downstream_send(&mut self, to_write: &[u8]) {
        //TODO Is there some maximum size at which we shouldn't buffer?
        //TODO can we simply write directly from the trie?
        use self::PerSocket::*;
        trace!("SOCKET add downstream send");
        match self {
            &mut Downstream {ref mut being_written, ref mut pending, ..}
            | &mut Client {ref mut being_written, ref mut pending, ..} => {
                trace!("SOCKET send down {}B", to_write.len());
                //FIXME send src_addr
                //TODO if being_written is empty try to send immediately, place remainder
                let no_other_inhabited_buffer =
                    pending.front().map(|v| v.is_empty()).unwrap_or(true);
                if no_other_inhabited_buffer && being_written.capacity() - being_written.len() >= to_write.len() {
                    debug_assert!(
                        pending.front().unwrap_or(&Vec::new()).is_empty(),
                        "being written {{cap: {}, len: {}}}, >= to_write {{len : {}}},\npending {:?}",
                        being_written.capacity(), being_written.len(), to_write.len(),
                        pending
                    );
                    being_written.extend_from_slice(to_write);
                    return
                }
                if let Some(buffer) = pending.back_mut() {
                    if buffer.capacity() - buffer.len() >= to_write.len()
                    || buffer.capacity() < WRITE_BUFFER_SIZE {
                        buffer.extend_from_slice(&to_write[..]);
                        return
                    }
                };
                debug_assert!(
                    pending.front().map(|v| !v.is_empty()).unwrap_or(true),
                    "pending {:?}", pending
                 );
                pending.push_back(to_write[..].to_vec())
            }
            _ => unreachable!()
        }
    }

    fn add_send_buffer(&mut self, to_write: Buffer) {
        unreachable!();
        //TODO Is there some maximum size at which we shouldn't buffer?
        //TODO can we simply write directly from the trie?
        use self::PerSocket::*;
        trace!("SOCKET add send_b");
        match self {
            &mut Client {ref mut being_written, ref mut pending, ref mut being_read, ..} => {
                //TODO if being_written is empty try to send immdiately, place remainder
                //TODO the big buffer itself may be premature, check if sending directly works...
                trace!("SOCKET send to client {}B", to_write.entry_size());
                if being_written.capacity() - being_written.len() >= to_write.entry_size() {
                    being_written.extend_from_slice(to_write.entry_slice())
                } else {
                    let placed = if let Some(buffer) = pending.back_mut() {
                        if buffer.capacity() - buffer.len() >= to_write.entry_size()
                        || buffer.capacity() < WRITE_BUFFER_SIZE {
                            buffer.extend_from_slice(to_write.entry_slice());
                            true
                        } else { false }
                    } else { false };
                    if !placed {
                        pending.push_back(to_write.entry_slice().to_vec())
                    }
                }
                //FIXME make sure the buffer goes to the right worker
                being_read.push_back(to_write);
                trace!("SOCKET re-add buffer, now @ {}", being_read.len());
                debug_assert!(being_read.len() <= NUMBER_READ_BUFFERS);
            },
            _ => unreachable!()
        }
    }

    fn return_buffer(&mut self, buffer: Buffer) {
        use self::PerSocket::*;
        match self {
            &mut Client {ref mut being_read, ..}
            | &mut Upstream {ref mut being_read, ..} => {
                being_read.push_back(buffer);
                trace!("returned buffer now @ {}", being_read.len());
                debug_assert!(being_read.len() <= NUMBER_READ_BUFFERS);
            },
            _ => unreachable!(),
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
    Done(Ipv4SocketAddr),
    Error,
    NeedsMore(usize),
}

fn recv_packet(buffer: &mut Buffer, mut stream: &TcpStream, mut read: usize, extra: usize)
 -> RecvRes {
    let bhs = base_header_size();
    if read < bhs {
        let r = stream.read(&mut buffer[read..bhs])
            .or_else(|e| if e.kind() == ErrorKind::WouldBlock { Ok(0) } else { Err(e) } )
            .ok();
        match r {
            Some(i) => read += i,
            None => return RecvRes::Error,
        }
        if read < bhs {
            return RecvRes::NeedsMore(read)
        }
    }
    trace!("WORKER recved {} bytes.", read);
    let (header_size, is_write) = {
        let e = buffer.entry();
        (e.header_size(), e.kind.layout().is_write())
    };
    assert!(header_size >= base_header_size());
    if read < header_size {
        let r = stream.read(&mut buffer[read..header_size])
            .or_else(|e| if e.kind() == ErrorKind::WouldBlock { Ok(0) } else { Err(e) } )
            .ok();
        match r {
            Some(i) => read += i,
            None => return RecvRes::Error,
        }
        if read < header_size {
            return RecvRes::NeedsMore(read)
        }
    }

    let size = buffer.entry().entry_size() + mem::size_of::<Ipv4SocketAddr>() + extra;//TODO if is_write { mem::size_of::<Ipv4SocketAddr>() } else { 0 };
    //let size = buffer.entry_size() + 6;
    debug_assert!(read <= size);
    if read < size {
        let r = stream.read(&mut buffer[read..size])
            .or_else(|e| if e.kind() == ErrorKind::WouldBlock { Ok(0) } else { Err(e) } )
            .ok();
        match r {
            Some(i) => {
                debug_assert!(i <= (size - read),
                    "read {} wanted to read {}: {} - {}",
                    i, (size - read), size, read
                );
                read += i
            },
            None => return RecvRes::Error,
        }
        if read < size {
            return RecvRes::NeedsMore(read);
        }
    }
    debug_assert!(buffer.packet_fits());
    // assert!(payload_size >= header_size);
    debug_assert_eq!(
        read, buffer.entry_size() + mem::size_of::<Ipv4SocketAddr>() + extra,//TODO if is_write { mem::size_of::<Ipv4SocketAddr>() } else { 0 },
        "entry_size {}", buffer.entry().entry_size()
    );
    let src_addr = Ipv4SocketAddr::from_slice(&buffer[read-6-extra..read-extra]);
    trace!("WORKER finished recv {} bytes for {}.", read, src_addr);
    RecvRes::Done(src_addr) //TODO ( if is_read { Some((receive_addr)) } else { None } )
}

fn get_next_token(token: &mut mio::Token) -> mio::Token {
    let next = token.0.wrapping_add(1);
    if next == 0 { *token = mio::Token(2) }
    else { *token = mio::Token(next) };
    *token
}

impl DistributeToWorkers<(usize, mio::Token, Ipv4SocketAddr)>
for Vec<spmc::Sender<ToWorker<(usize, mio::Token, Ipv4SocketAddr)>>> {
    fn send_to_worker(&mut self, msg: ToWorker<(usize, mio::Token, Ipv4SocketAddr)>) {
        let (which_queue, token, _) = msg.get_associated_data();
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

#[cfg(test)]
mod tests {
    extern crate env_logger;

    use super::*;

    use socket_addr::Ipv4SocketAddr;

    use prelude::*;
    use buffer::Buffer;

    use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
    use std::io::{Read, Write};
    use std::net::TcpStream;

    /*pub fn run(
        acceptor: TcpListener,
        this_server_num: u32,
        total_chain_servers: u32,
        num_workers: usize,
        ready: &AtomicUsize,
    ) -> ! {*/

    const basic_addr: &'static [&'static str] = &["0.0.0.0:13490"];
    const replicas_addr: &'static [&'static str] = &["0.0.0.0:13491", "0.0.0.0:13492"];
    static BASIC_SERVER_READY: AtomicUsize = ATOMIC_USIZE_INIT;
    static REPLICAS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

    #[test]
    fn test_write() {
        let _ = env_logger::init();
        trace!("TCP test write");
        start_servers(basic_addr, &BASIC_SERVER_READY);
        trace!("TCP test write start");
        let mut stream = TcpStream::connect(&"127.0.0.1:13490").unwrap();
        let mut buffer = Buffer::empty();
        buffer.fill_from_entry_contents(EntryContents::Data(&12i32, &[]));
        {
            buffer.entry_mut().locs_mut()[0] = OrderIndex(1.into(), 0.into());
        }
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        stream.read_exact(&mut buffer[..]).unwrap();
        assert!(buffer.entry().kind.contains(EntryKind::ReadSuccess));
        assert_eq!(buffer.entry().locs()[0], OrderIndex(1.into(), 1.into()));
        assert_eq!(Entry::<i32>::wrap_bytes(&buffer[..]).contents(), EntryContents::Data(&12i32, &[]));
    }

    #[test]
    fn test_write_read() {
        let _ = env_logger::init();
        trace!("TCP test write_read");
        start_servers(basic_addr, &BASIC_SERVER_READY);
        trace!("TCP test write_read start");
        let mut stream = TcpStream::connect(&"127.0.0.1:13490").unwrap();
        let mut buffer = Buffer::empty();
        buffer.fill_from_entry_contents(EntryContents::Data(&92u64, &[]));
        {
            buffer.entry_mut().locs_mut()[0] = OrderIndex(2.into(), 0.into());
        }
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        stream.read_exact(&mut buffer[..]).unwrap();
        assert!(buffer.entry().kind.contains(EntryKind::ReadSuccess));
        assert_eq!(buffer.entry().locs()[0], OrderIndex(2.into(), 1.into()));
        assert_eq!(Entry::<u64>::wrap_bytes(&buffer[..]).contents(), EntryContents::Data(&92, &[]));
        buffer.fill_from_entry_contents(EntryContents::Data(&(), &[]));
        buffer.ensure_len();
        {
            buffer.entry_mut().locs_mut()[0] = OrderIndex(2.into(), 1.into());
            buffer.entry_mut().kind = EntryKind::Read;
        }
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        stream.read_exact(&mut buffer[..]).unwrap();
        assert!(buffer.entry().kind.contains(EntryKind::ReadSuccess));
        assert_eq!(buffer.entry().locs()[0], OrderIndex(2.into(), 1.into()));
        assert_eq!(Entry::<u64>::wrap_bytes(&buffer[..]).contents(), EntryContents::Data(&92, &[]));
    }

    #[test]
    fn test_replicated_write() {
        let _ = env_logger::init();
        trace!("TCP test replicated write");
        start_servers(replicas_addr, &REPLICAS_READY);
        trace!("TCP test replicated write start");
        let mut write_stream = TcpStream::connect(&"127.0.0.1:13491").unwrap();
        let mut read_stream = TcpStream::connect(&"127.0.0.1:13492").unwrap();
        let read_addr = Ipv4SocketAddr::from_socket_addr(read_stream.local_addr().unwrap());
        let mut buffer = Buffer::empty();
        buffer.fill_from_entry_contents(EntryContents::Data(&12i32, &[]));
        {
            buffer.entry_mut().locs_mut()[0] = OrderIndex(1.into(), 0.into());
        }
        trace!("sending write");
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();
        trace!("finished sending write, waiting for ack");
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        read_stream.read_exact(&mut buffer[..]).unwrap();
        trace!("finished waiting for ack");
        assert!(buffer.entry().kind.contains(EntryKind::ReadSuccess));
        assert_eq!(buffer.entry().locs()[0], OrderIndex(1.into(), 1.into()));
        assert_eq!(Entry::<i32>::wrap_bytes(&buffer[..]).contents(), EntryContents::Data(&12i32, &[]));
    }

    #[test]
    fn test_replicated_write_read() {
        let _ = env_logger::init();
        trace!("TCP test replicated write/read");
        start_servers(replicas_addr, &REPLICAS_READY);
        trace!("TCP test replicated write/read start");
        let mut write_stream = TcpStream::connect(&"127.0.0.1:13491").unwrap();
        let mut read_stream = TcpStream::connect(&"127.0.0.1:13492").unwrap();
        let read_addr = Ipv4SocketAddr::from_socket_addr(read_stream.local_addr().unwrap());
        let mut buffer = Buffer::empty();
        buffer.fill_from_entry_contents(EntryContents::Data(&92u64, &[]));
        {
            buffer.entry_mut().locs_mut()[0] = OrderIndex(2.into(), 0.into());
        }
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        read_stream.read_exact(&mut buffer[..]).unwrap();
        assert!(buffer.entry().kind.contains(EntryKind::ReadSuccess));
        assert_eq!(buffer.entry().locs()[0], OrderIndex(2.into(), 1.into()));
        assert_eq!(Entry::<u64>::wrap_bytes(&buffer[..]).contents(), EntryContents::Data(&92, &[]));
        buffer.fill_from_entry_contents(EntryContents::Data(&(), &[]));
        buffer.ensure_len();
        {
            buffer.entry_mut().locs_mut()[0] = OrderIndex(2.into(), 1.into());
            buffer.entry_mut().kind = EntryKind::Read;
        }
        read_stream.write_all(buffer.entry_slice()).unwrap();
        read_stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        read_stream.read_exact(&mut buffer[..]).unwrap();
        assert!(buffer.entry().kind.contains(EntryKind::ReadSuccess));
        assert_eq!(buffer.entry().locs()[0], OrderIndex(2.into(), 1.into()));
        assert_eq!(Entry::<u64>::wrap_bytes(&buffer[..]).contents(), EntryContents::Data(&92, &[]));
    }

    #[test]
    fn test_empty_read() {
        let _ = env_logger::init();
        trace!("TCP test write_read");
        start_servers(basic_addr, &BASIC_SERVER_READY);
        trace!("TCP test write_read start");
        let mut stream = TcpStream::connect(&"127.0.0.1:13490").unwrap();
        let mut buffer = Buffer::empty();
        buffer.fill_from_entry_contents(EntryContents::Data(&(),
            &[OrderIndex(0.into(), 0.into())]));
        buffer.fill_from_entry_contents(EntryContents::Data(&(), &[]));
        buffer.ensure_len();
        {
            buffer.entry_mut().locs_mut()[0] = OrderIndex(0.into(), 1.into());
            buffer.entry_mut().kind = EntryKind::Read;
        }
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        stream.read_exact(&mut buffer[..]).unwrap();
        assert!(!buffer.entry().kind.contains(EntryKind::ReadSuccess));
        assert_eq!(Entry::<()>::wrap_bytes(&buffer[..]).dependencies(), &[OrderIndex(0.into(), 0.into())]);
    }

    //FIXME add empty read tests

    fn start_servers<'a, 'b>(addr_strs: &'a [&'b str], server_ready: &'static AtomicUsize) {
        use std::{thread, iter};
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        use mio;

        trace!("starting server(s) @ {:?}", addr_strs);

        //static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

        if addr_strs.len() == 1 {
            let addr = addr_strs[0].parse().expect("invalid inet address");
            let acceptor = mio::tcp::TcpListener::bind(&addr);
            if let Ok(acceptor) = acceptor {
                thread::spawn(move || {
                    trace!("starting server");
                    ::servers2::tcp::run(acceptor, 0, 1, 1, server_ready)
                });
            }
            else {
                trace!("server already started @ {}", addr_strs[0]);
            }
        }
        else {
            let local_host = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
            for i in 0..addr_strs.len() {
                let prev_server: Option<SocketAddr> =
                    if i > 0 { Some(addr_strs[i-1]) } else { None }
                    .map(|s| s.parse().unwrap());
                let prev_server = prev_server.map(|mut s| {s.set_ip(local_host); s});
                let next_server: Option<SocketAddr> = addr_strs.get(i+1)
                    .map(|s| s.parse().unwrap());
                let next_server = next_server.map(|mut s| {s.set_ip(local_host); s});
                let next_server = next_server.map(|s| s.ip());
                let addr = addr_strs[i].parse().unwrap();
                let acceptor = mio::tcp::TcpListener::bind(&addr);
                if let Ok(acceptor) = acceptor {
                    thread::spawn(move || {
                        trace!("starting replica server");
                        ::servers2::tcp::run_with_replication(acceptor, 0, 1,
                            prev_server, next_server,
                            1, server_ready)
                    });
                }
                else {
                    trace!("server already started @ {}", addr_strs[i]);
                }
            }
        }

        while server_ready.load(Ordering::Acquire) < addr_strs.len() {}
    }
}
