use std::collections::hash_map::Entry as HashEntry;
use std::io::{self, Read, Write};
use std::{mem, thread};
use std::net::{IpAddr, SocketAddr};
use std::sync::mpsc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

// use prelude::*;
use ::{spsc, ServerLog};
use hash::{HashMap, FxHasher};
use socket_addr::Ipv4SocketAddr;

use mio;
use mio::tcp::*;

use self::worker::{Worker, WorkerToDist, DistToWorker, ToLog};

use packets::EntryContents;

mod worker;
mod per_socket;

/*
  GC with parrallel readers plan:
    each worker will have a globally visible field
      epoch
    padded to a cache line to prevent false sharing
    the GC core (which need not be the same as the index core) has a globally visible epoch
    each chain has a minimum (and maximum?) valid location
    to read:
      a worker updates its epoch to the GC epoch,
      if the entry is already gc'd (< min or > max) the worker returns the needed response
      otherwise it returns the found value
    to gc:
      the gc'er increments the epoch,
      the gc'er broadcasts update-epoch to all workers
      then waits until all of the workers have seen the new epoch,

      we don't worry about gc during write (that seems like a silly scenario),
      so we can send the log's copy downstream and don't have to worry about returning buffers.
      during reads, the worker which receives the read req is the one which sends,
      so the above scheme will work

    this does not deadlock b/c gc'er must wait for at most 1 read per worker

    this should be efficient b/c in normal operation the workers are just
      reading an immutable shared value
      and writing to a local value
      (addtnl we could only have the checks immediately after update-epoch is recv'd)

    for multi entry values store refcount before entry,

    (we want bit field which tells us where seq of entries end in allocator,
    use to distinguish btwn single and multi entries?)

    store bit in flags saying start-of-run
    for now check each val to see if start-of-run if is free previous run (on other thread?),
    if multi decrement refcount
    want to use bit in pointer instead
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
  writes are a bit slower than reads, and this gets worse as entry size increases
  I suspect this due to malloc.
  We can parallelize malloc by having workers send one level's worth of alloc
  along with each write request.
  We could even use the initial recv-buffer as this alloc if we ensure that said
  buffers are always LEVEL_BYTES in size
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

/*
  Finishing up chain-replication
    1. Add unique client-ID instead of using ack socket addr
       this will make it much easier to reconfig the tail
       plus it'll prevent errors when clients exchange ack addr  on reconfig
       and it'll make switching to IPv6 easier

    2. store which appends have not finished at the tail in upstreams
       this will make updating a new middle much easier
       this would also help with the CRAQ optimization
       just storing which addrs we're waiting for an ack should suffice
       can use list, might want to switch to send-downstream based on chain,
       instead of the current send-downstream based on receiver
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

const NUMBER_READ_BUFFERS: usize = 15;

type WorkerNum = usize;

pub fn run_server(
    addr: SocketAddr,
    server_num: u32,
    group_size: u32,
    prev_server: Option<SocketAddr>,
    next_server: Option<IpAddr>,
    num_worker_threads: usize,
    started: &AtomicUsize,
) -> ! {
    let acceptor = mio::tcp::TcpListener::bind(&addr)
        .expect("Bind error");
    run_with_replication(
        acceptor, server_num, group_size, prev_server, next_server, num_worker_threads, started
    )
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
    match acceptor.accept() {
        Err(e) => trace!("error {}", e),
        Ok((socket, addr)) => if Some(addr.ip()) != next_server_ip {
            trace!("SERVER got other connection {:?}", addr);
            other_sockets.push((socket, addr))
        } else {
            trace!("SERVER {} connected downstream.", this_server_num);
            let _ = socket.set_keepalive_ms(Some(1000));
            let _ = socket.set_nodelay(true);
            downstream_admin_socket = Some(socket)
        }
    }
    while next_server_ip.is_some() && downstream_admin_socket.is_none() {
        trace!("SERVER {} waiting for downstream {:?}.", this_server_num, next_server);
        let _ = poll.poll(&mut events, None);
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
                            let _ = socket.set_nodelay(true);
                            downstream_admin_socket = Some(socket)
                        }
                    }
                }
                _ => unreachable!()
            }
        }
    }

    let is_replica = prev_server_ip.is_some();
    if let Some(ref ip) = prev_server_ip {
        trace!("SERVER {} waiting for upstream {:?}.", this_server_num, prev_server_ip);
        while upstream_admin_socket.is_none() {
            if let Ok(socket) = TcpStream::connect(ip) {
                trace!("SERVER {} connected upstream on {:?}.",
                    this_server_num, socket.local_addr().unwrap());
                let _ = socket.set_nodelay(true);
                upstream_admin_socket = Some(socket)
            } else {
                //thread::yield_now()
                thread::sleep(Duration::from_millis(1));
            }
        }
        trace!("SERVER {} connected upstream.", this_server_num);
    }

    let num_downstream = negotiate_num_downstreams(&mut downstream_admin_socket, num_workers as u16);
    let num_upstream = negotiate_num_upstreams(&mut upstream_admin_socket, num_workers as u16, prev_server_ip);
    let mut downstream = Vec::with_capacity(num_downstream);
    let mut upstream = Vec::with_capacity(num_upstream);
    while downstream.len() + 1 < num_downstream {
        let _ = poll.poll(&mut events, None);
        for event in events.iter() {
            match event.token() {
                ACCEPT => {
                    match acceptor.accept() {
                        Err(e) => trace!("error {}", e),
                        Ok((socket, addr)) => if Some(addr.ip()) == next_server_ip {
                            trace!("SERVER {} add downstream.", this_server_num);
                            let _ = socket.set_keepalive_ms(Some(1000));
                            let _ = socket.set_nodelay(true);
                            downstream.push(socket)
                        } else {
                            trace!("SERVER got other connection {:?}", addr);
                            let _ = socket.set_keepalive_ms(Some(1000));
                            let _ = socket.set_nodelay(true);
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
            let up = TcpStream::connect(ip).expect("cannot connect upstream");
            let _ = up.set_keepalive_ms(Some(1000));
            let _ = up.set_nodelay(true);
            upstream.push(up)
        }
    }

    downstream_admin_socket.take().map(|s| downstream.push(s));
    upstream_admin_socket.take().map(|s| upstream.push(s));
    assert_eq!(downstream.len(), num_downstream);
    assert_eq!(upstream.len(), num_upstream);
    //let (log_to_dist, dist_from_log) = spmc::channel();

    trace!("SERVER {} {} up, {} down.", this_server_num, num_upstream, num_downstream);
    trace!("SERVER {} starting {} workers.", this_server_num, num_workers);
    let mut log_to_workers: Vec<_> = Vec::with_capacity(num_workers);
    let mut dist_to_workers: Vec<_> = Vec::with_capacity(num_workers);
    let (log_writer, log_reader) = ::new_chain_store_and_reader();
    for n in 0..num_workers {
        //let from_dist = recv_from_dist.clone();
        let to_dist   = workers_to_dist.clone();
        //let from_log  = recv_from_log.clone();
        let to_log = workers_to_log.clone();
        let (to_worker, from_log) = spsc::channel();
        let (dist_to_worker, from_dist) = spsc::channel();
        let upstream = upstream.pop();
        let downstream = downstream.pop();
        let log_reader = log_reader.clone();
        thread::spawn(move ||
            Worker::new(
                from_dist,
                to_dist,
                from_log,
                to_log,
                log_reader,
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

    //log_to_workers.push(log_to_dist);

    // poll.register(
        // &dist_from_log,
        // DIST_FROM_LOG,
        // mio::Ready::readable(),
        // mio::PollOpt::level()
    // ).expect("cannot pol from log on dist");
    thread::spawn(move || {
        let mut log = ServerLog::new(
            this_server_num, total_chain_servers, log_to_workers, log_writer
        );
        #[cfg(not(feature = "print_stats"))]
        for to_log in recv_from_workers.iter() {
            match to_log {
                ToLog::New(buffer, storage, st) => {
                    assert!(!is_replica);
                    log.handle_op(buffer, storage, st)
                },
                ToLog::Replication(tr, st) => {
                    assert!(is_replica);
                    log.handle_replication(tr, st)
                },
                ToLog::Recovery(r, st) => log.handle_recovery(r, st),
            }
        }
        #[cfg(feature = "print_stats")]
        loop {
            use std::sync::mpsc::RecvTimeoutError;
            let msg = recv_from_workers.recv_timeout(Duration::from_secs(10));
            match msg {
                Ok(ToLog::New(buffer, storage, st)) => log.handle_op(buffer, storage, st),
                Ok(ToLog::Replication(tr, st)) => log.handle_replication(tr, st),
                ToLog::Recovery(r, st) => log.handle_recovery(r, st),
                Err(RecvTimeoutError::Timeout) => log.print_stats(),
                Err(RecvTimeoutError::Disconnected) => panic!("log disconnected"),
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
    // let mut next_worker = 0usize;

    for (mut socket, addr) in other_sockets {
        let mut id = [0u8; 16];
        blocking_read(&mut socket, &mut id).unwrap();
        let id = Ipv4SocketAddr::from_bytes(id);
        let tok = get_next_token(&mut next_token);
        /*let worker = if is_unreplicated {
            let worker = next_worker;
            next_worker = next_worker.wrapping_add(1);
            if next_worker >= dist_to_workers.len() {
                next_worker = 0;
            }
            worker
        } else {
            let worker = worker_for_ip(id, num_workers as u64);
            worker_for_client.insert(id, (worker, tok));
            worker
        };*/
        let worker = {
            let worker = worker_for_ip(id, num_workers as u64);
            worker_for_client.insert(id, (worker, tok));
            worker
        };
        trace!("SERVER new client @ {:?} => {:?}", (addr, id), (worker, tok));
        dist_to_workers[worker].send(DistToWorker::NewClient(tok, socket, id));

    }

    trace!("SERVER start server loop");
    loop {
        let _ = poll.poll(&mut events, None);
        for event in events.iter() {
            match event.token() {
                ACCEPT => {
                    match acceptor.accept() {
                        Err(e) => trace!("error {}", e),
                        Ok((mut socket, addr)) => {
                            let _ = socket.set_keepalive_ms(Some(1000));
                            let _ = socket.set_nodelay(true);
                            //TODO oveflow
                            let tok = get_next_token(&mut next_token);
                            let mut id = [0u8; 16];
                            blocking_read(&mut socket, &mut id).unwrap();
                            let id = Ipv4SocketAddr::from_bytes(id);
                            /*poll.register(
                                &socket,
                                tok,
                                mio::Ready::readable(),
                                mio::PollOpt::edge() | mio::PollOpt::oneshot(),
                            );
                            receivers.insert(tok, Some(socket));*/
                            /*let worker = if is_unreplicated {
                                let worker = next_worker;
                                next_worker = next_worker.wrapping_add(1);
                                if next_worker >= dist_to_workers.len() {
                                    next_worker = 0;
                                }
                                worker
                            } else {
                                worker_for_ip(id, num_workers as u64)
                            };*/
                            let worker = worker_for_ip(id, num_workers as u64);
                            worker_for_client.insert(id, (worker, tok));
                            trace!("SERVER accepting client @ {:?} => {:?}",
                                (addr, id), (worker, tok));
                            dist_to_workers[worker].send(DistToWorker::NewClient(tok, socket, id));
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

                            WorkerToDist::DownstreamB(worker, addr, buffer, storage_loc) => {
                                trace!("DIST {} downstream worker for {} is {}.",
                                    this_server_num, addr, worker);
                                let sent = dist_to_workers.get_mut(worker)
                                    .map(|s| {
                                        s.send(DistToWorker::ToClientB(
                                            DOWNSTREAM, buffer, addr, storage_loc));
                                        true
                                }).unwrap_or(false);
                                if !sent {
                                    panic!("No downstream for {:?} in {:?}",
                                        worker, dist_to_workers.len())
                                }
                                continue
                            },

                            WorkerToDist::ToClient(addr, buffer) => {
                                trace!("DIST {} looking for worker for {}.",
                                    this_server_num, addr);
                                //FIXME this is racey, if we don't know who gets the message it fails
                                let (worker, token) = worker_for_client[&addr].clone();
                                (worker, token, buffer, addr, 0)
                            }

                            WorkerToDist::ToClientB(addr, buffer) => {
                                trace!("DIST {} looking for worker for {}.",
                                    this_server_num, addr);
                                //FIXME this is racey, if we don't know who gets the message it fails
                                let (worker, token) = worker_for_client.get(&addr)
                                    .cloned().unwrap_or_else(||
                                        panic!(
                                            "No worker found for {:?} in {:?}",
                                            addr, worker_for_client,
                                        )
                                );

                                dist_to_workers[worker].send(
                                    DistToWorker::ToClientB(token, buffer, addr, 0)
                                );
                                continue
                            }

                            WorkerToDist::FenceClient(buffer) => {
                                let (client_to_fence, fencing_client) = match buffer.contents() {
                                    EntryContents::FenceClient{
                                        client_to_fence, fencing_client, ..
                                    } => (*client_to_fence, *fencing_client),
                                    _ => unreachable!(),
                                };
                                if let Some(&(worker, token)) = worker_for_client
                                    .get(&client_to_fence.into()) {
                                    dist_to_workers[worker].send(
                                        DistToWorker::FenceOff(token, buffer)
                                    );
                                    continue
                                };
                                worker_for_client.get(&fencing_client.into())
                                .map(|&(worker, token)|
                                    dist_to_workers[worker].send(
                                        DistToWorker::FinishedFence(token, buffer)
                                    )
                                );
                                continue
                            },

                            WorkerToDist::ClientFenced(buffer) => {
                                let fencing_client = match buffer.contents() {
                                    EntryContents::FenceClient{fencing_client, ..} =>
                                        *fencing_client,
                                    _ => unreachable!(),
                                };
                                //TODO we should probably store the worker on the outbound hop
                                //     to ensure it can get the buffer back if it's client
                                //     gets fenced in the interim

                                //TODO we should probably GC the worker_for_client entry here
                                worker_for_client.get(&fencing_client.into())
                                .map(|&(worker, token)|
                                    dist_to_workers[worker].send(
                                        DistToWorker::FinishedFence(token, buffer)
                                    )
                                );
                                continue
                            }
                        };
                        dist_to_workers[worker].send(
                            DistToWorker::ToClient(token, buffer, addr, storage_loc));
                        continue

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
        trace!("SERVER no need to negotiate downstream.");
        0
    }
}

fn negotiate_num_upstreams(
    socket: &mut Option<TcpStream>,
    num_workers: u16,
    remote_addr: Option<SocketAddr>
) -> usize {
    use std::cmp::min;
    if let Some(ref mut socket) = socket.as_mut() {
        let remote_addr = remote_addr.unwrap();
        let to_write: [u8; 2] = unsafe { mem::transmute(num_workers) };
        trace!("will req {:?}", to_write);
        let mut refusals = 0;
        'write: loop {
            let r = blocking_write(socket, &to_write);
            match r {
                Err(ref e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    if refusals >= 60000 { panic!("write fail {:?}", e) }
                    refusals += 1;
                    trace!("upstream refused reconnect attempt {}", refusals);
                    thread::sleep(Duration::from_millis(1));
                    **socket = TcpStream::connect(&remote_addr).unwrap();
                    let _ = socket.set_keepalive_ms(Some(1000));
                    let _ = socket.set_nodelay(true);
                }
                Err(ref e) if e.kind() == io::ErrorKind::NotConnected => {
                    if refusals >= 60000 { panic!("write fail {:?}", e) }
                    refusals += 1;
                    trace!("upstream connection not ready {}", refusals);
                    thread::sleep(Duration::from_millis(1));
                }
                Err(e) => panic!("write fail {:?}", e),
                Ok(..) => break 'write,
            }
        }
        trace!("req {:?}", to_write);
        let mut num_other_threads = [0u8; 2];
        blocking_read(socket, &mut num_other_threads).expect("upstream failed");
        trace!("other {:?}", to_write);
        let num_other_threads = unsafe { mem::transmute(num_other_threads) };
        trace!("SERVER up workers: {}, other's workers {}.", num_workers, num_other_threads);
        min(num_other_threads, num_workers) as usize
    }
    else {
        trace!("SERVER no need to negotiate upstream.");
        0
    }
}

fn blocking_read<R: Read>(r: &mut R, mut buffer: &mut [u8]) -> io::Result<()> {
    //like Read::read_exact but doesn't die on WouldBlock
    'recv: while !buffer.is_empty() {
        match r.read(buffer) {
            Ok(i) => { let tmp = buffer; buffer = &mut tmp[i..]; }
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => {
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

fn blocking_write<W: Write>(w: &mut W, mut buffer: &[u8]) -> io::Result<()> {
    //like Write::write_all but doesn't die on WouldBlock
    'recv: while !buffer.is_empty() {
        match w.write(buffer) {
            Ok(i) => { let tmp = buffer; buffer = &tmp[i..]; }
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => {
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

#[cfg(TODO)]
#[cfg(test)]
mod tests {
    extern crate env_logger;

    use socket_addr::Ipv4SocketAddr;

    use buffer::Buffer;

    use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
    use std::io::{Read, Write};
    use std::net::TcpStream;

    use packets::{OrderIndex, EntryFlag, EntryContents, Uuid};
    use packets::SingletonBuilder as Data;

    /*pub fn run(
        acceptor: TcpListener,
        this_server_num: u32,
        total_chain_servers: u32,
        num_workers: usize,
        ready: &AtomicUsize,
    ) -> ! {*/

    #[allow(non_upper_case_globals)]
    const basic_addr: &'static [&'static str] = &["0.0.0.0:13490"];
    #[allow(non_upper_case_globals)]
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
        let _ = stream.set_nodelay(true);
        let mut buffer = Buffer::empty();
        Data(&12i32, &[]).fill_entry(&mut buffer);
        buffer.contents_mut().locs_mut()[0] = OrderIndex(1.into(), 0.into());
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut stream);
        assert!(buffer.contents().flag().contains(EntryFlag::ReadSuccess));
        assert_eq!(buffer.contents().locs()[0], OrderIndex(1.into(), 1.into()));
        assert_eq!(buffer.contents().into_singleton_builder(), Data(&12i32, &[]));
    }

    #[test]
    fn test_write_read() {
        let _ = env_logger::init();
        trace!("TCP test write_read");
        start_servers(basic_addr, &BASIC_SERVER_READY);
        trace!("TCP test write_read start");
        let mut stream = TcpStream::connect(&"127.0.0.1:13490").unwrap();
        let _ = stream.set_nodelay(true);
        let mut buffer = Buffer::empty();
        Data(&92u64, &[]).fill_entry(&mut buffer);
        {
            buffer.contents_mut().locs_mut()[0] = OrderIndex(2.into(), 0.into());
        }
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut stream);
        assert!(buffer.contents().flag().contains(EntryFlag::ReadSuccess));
        assert_eq!(buffer.contents().locs()[0], OrderIndex(2.into(), 1.into()));
        assert_eq!(buffer.contents().into_singleton_builder(), Data(&92u64, &[]));
        buffer.fill_from_entry_contents(EntryContents::Read {
            id: &Uuid::nil(),
            flags: &EntryFlag::Nothing,
            data_bytes: &0,
            dependency_bytes: &0,
            loc: &OrderIndex(0.into(), 0.into()),
            horizon: &OrderIndex(0.into(), 0.into()),
            min: &OrderIndex(0.into(), 0.into()),
        });
        buffer.contents_mut().locs_mut()[0] = OrderIndex(2.into(), 1.into());
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut stream);
        assert!(buffer.contents().flag().contains(EntryFlag::ReadSuccess));
        assert_eq!(buffer.contents().locs()[0], OrderIndex(2.into(), 1.into()));
        assert_eq!(buffer.contents().into_singleton_builder(), Data(&92u64, &[]));
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
        Data(&12i32, &[]).fill_entry(&mut buffer);
        buffer.contents_mut().locs_mut()[0] = OrderIndex(1.into(), 0.into());
        trace!("sending write");
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();
        trace!("finished sending write, waiting for ack");
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut read_stream);
        trace!("finished waiting for ack");
        assert!(buffer.contents().flag().contains(EntryFlag::ReadSuccess));
        assert_eq!(buffer.contents().locs()[0], OrderIndex(1.into(), 1.into()));
        assert_eq!(buffer.contents().into_singleton_builder(), Data(&12i32, &[]));
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
        Data(&92u64, &[]).fill_entry(&mut buffer);;
        buffer.contents_mut().locs_mut()[0] = OrderIndex(2.into(), 0.into());
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut read_stream);
        assert!(buffer.entry().flag().contains(EntryFlag::ReadSuccess));
        assert_eq!(buffer.contents().locs()[0], OrderIndex(2.into(), 1.into()));
        assert_eq!(buffer.contents().into_singleton_builder(), Data(&92u64, &[]));
        buffer.fill_from_entry_contents(EntryContents::Read {
            id: &Uuid::nil(),
            flags: &EntryFlag::Nothing,
            data_bytes: &0,
            dependency_bytes: &0,
            loc: &OrderIndex(0.into(), 0.into()),
            horizon: &OrderIndex(0.into(), 0.into()),
            min: &OrderIndex(0.into(), 0.into()),
        });
        buffer.ensure_len();
        buffer.contents_mut().locs_mut()[0] = OrderIndex(2.into(), 1.into());
        read_stream.write_all(buffer.entry_slice()).unwrap();
        read_stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut read_stream);
        assert!(buffer.entry().flag().contains(EntryFlag::ReadSuccess));
        assert_eq!(buffer.contents().locs()[0], OrderIndex(2.into(), 1.into()));
        assert_eq!(buffer.contents().into_singleton_builder(), Data(&92u64, &[]));
    }

    #[test]
    fn test_skeens_write() {
        let _ = env_logger::init();
        trace!("TCP test write");
        start_servers(basic_addr, &BASIC_SERVER_READY);
        trace!("TCP test write start");
        let mut stream = TcpStream::connect(&"127.0.0.1:13490").unwrap();
        let _ = stream.set_nodelay(true);
        let mut buffer = Buffer::empty();
        let id = Uuid::new_v4();
        buffer.fill_from_entry_contents(EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock),
            lock: &0,
            locs: &[OrderIndex(3.into(), 0.into()), OrderIndex(4.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut stream);
        assert!(buffer.contents().flag().contains(EntryFlag::Skeens1Queued));
        assert_eq!(buffer.contents(), EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::Skeens1Queued | EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            data_bytes: &3, //TODO make 0
            lock: &0,
            locs: &[OrderIndex(3.into(), 1.into()), OrderIndex(4.into(), 1.into())],
            deps: &[],
        });
        let max_timestamp = buffer.contents().locs().iter()
        .fold(0, |max_ts, &OrderIndex(_, i)|
            ::std::cmp::max(max_ts, u32::from(i) as u64)
        );
        assert!(max_timestamp > 0);

        buffer.clear_data();

        buffer.fill_from_entry_contents(EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::Unlock),
            data_bytes: &0,
            lock: &max_timestamp,
            locs: &[OrderIndex(3.into(), 0.into()), OrderIndex(4.into(), 0.into())],
            deps: &[],
        });
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();

        /*buffer.clear_data();

        recv_packet(&mut buffer, &mut stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(3.into(), 1.into()), OrderIndex(4.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });*/

        buffer.clear_data();

        recv_packet(&mut buffer, &mut stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(3.into(), 1.into()), OrderIndex(4.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });
    }

    #[test]
    fn test_skeens_write_read() {
        let _ = env_logger::init();
        trace!("TCP test write");
        start_servers(basic_addr, &BASIC_SERVER_READY);
        trace!("TCP test write start");
        let mut stream = TcpStream::connect(&"127.0.0.1:13490").unwrap();
        let _ = stream.set_nodelay(true);
        let mut buffer = Buffer::empty();
        let id = Uuid::new_v4();
        buffer.fill_from_entry_contents(EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock),
            lock: &0,
            locs: &[OrderIndex(5.into(), 0.into()), OrderIndex(6.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut stream);
        assert!(buffer.contents().flag().contains(EntryFlag::Skeens1Queued));
        assert_eq!(buffer.contents(), EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::Skeens1Queued | EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            data_bytes: &3, //TODO make 0
            lock: &0,
            locs: &[OrderIndex(5.into(), 1.into()), OrderIndex(6.into(), 1.into())],
            deps: &[],
        });
        let max_timestamp = buffer.contents().locs().iter()
        .fold(0, |max_ts, &OrderIndex(_, i)|
            ::std::cmp::max(max_ts, u32::from(i) as u64)
        );
        assert!(max_timestamp > 0);

        buffer.clear_data();

        buffer.fill_from_entry_contents(EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::Unlock),
            data_bytes: &0,
            lock: &max_timestamp,
            locs: &[OrderIndex(5.into(), 0.into()), OrderIndex(6.into(), 0.into())],
            deps: &[],
        });
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();

        /*buffer.clear_data();

        recv_packet(&mut buffer, &mut stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(5.into(), 1.into()), OrderIndex(6.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });*/

        buffer.clear_data();

        recv_packet(&mut buffer, &mut stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(5.into(), 1.into()), OrderIndex(6.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });


        buffer.clear_data();

        buffer.fill_from_entry_contents(EntryContents::Read {
            id: &Uuid::nil(),
            flags: &EntryFlag::Nothing,
            data_bytes: &0,
            dependency_bytes: &0,
            loc: &OrderIndex(5.into(), 1.into()),
            horizon: &OrderIndex(0.into(), 0.into()),
            min: &OrderIndex(0.into(), 0.into()),
        });
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();

        buffer.clear_data();

        recv_packet(&mut buffer, &mut stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(5.into(), 1.into()), OrderIndex(6.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });

        buffer.clear_data();

        buffer.fill_from_entry_contents(EntryContents::Read {
            id: &Uuid::nil(),
            flags: &EntryFlag::Nothing,
            data_bytes: &0,
            dependency_bytes: &0,
            loc: &OrderIndex(6.into(), 1.into()),
            horizon: &OrderIndex(0.into(), 0.into()),
            min: &OrderIndex(0.into(), 0.into()),
        });
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();

        buffer.clear_data();

        recv_packet(&mut buffer, &mut stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(5.into(), 1.into()), OrderIndex(6.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });
    }

    #[test]
    fn test_replicated_skeens_write() {
        let _ = env_logger::init();
        trace!("TCP test replicated write");
        start_servers(replicas_addr, &REPLICAS_READY);
        trace!("TCP test replicated write start");
        let mut write_stream = TcpStream::connect(&"127.0.0.1:13491").unwrap();
        let mut read_stream = TcpStream::connect(&"127.0.0.1:13492").unwrap();
        let read_addr = Ipv4SocketAddr::from_socket_addr(read_stream.local_addr().unwrap());

        let mut buffer = Buffer::empty();
        let id = Uuid::new_v4();
        buffer.fill_from_entry_contents(EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock),
            lock: &0,
            locs: &[OrderIndex(3.into(), 0.into()), OrderIndex(4.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::Skeens1Queued | EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            data_bytes: &3, //TODO make 0
            lock: &0,
            locs: &[OrderIndex(3.into(), 1.into()), OrderIndex(4.into(), 1.into())],
            deps: &[],
        });
        let max_timestamp = buffer.contents().locs().iter()
        .fold(0, |max_ts, &OrderIndex(_, i)|
            ::std::cmp::max(max_ts, u32::from(i) as u64)
        );
        assert!(max_timestamp > 0);

        buffer.clear_data();

        buffer.fill_from_entry_contents(EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::Unlock),
            data_bytes: &0,
            lock: &max_timestamp,
            locs: &[OrderIndex(3.into(), 0.into()), OrderIndex(4.into(), 0.into())],
            deps: &[],
        });
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();

        /*buffer.clear_data();

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(3.into(), 1.into()), OrderIndex(4.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });*/

        buffer.clear_data();

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(3.into(), 1.into()), OrderIndex(4.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });
    }

    #[test]
    fn test_replicated_skeens_write_read() {
        let _ = env_logger::init();
        trace!("TCP test replicated write");
        start_servers(replicas_addr, &REPLICAS_READY);
        trace!("TCP test replicated write start");
        let mut write_stream = TcpStream::connect(&"127.0.0.1:13491").unwrap();
        let mut read_stream = TcpStream::connect(&"127.0.0.1:13492").unwrap();
        let read_addr = Ipv4SocketAddr::from_socket_addr(read_stream.local_addr().unwrap());

        let mut buffer = Buffer::empty();
        let id = Uuid::new_v4();
        buffer.fill_from_entry_contents(EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock),
            lock: &0,
            locs: &[OrderIndex(5.into(), 0.into()), OrderIndex(6.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::Skeens1Queued | EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            data_bytes: &3, //TODO make 0
            lock: &0,
            locs: &[OrderIndex(5.into(), 1.into()), OrderIndex(6.into(), 1.into())],
            deps: &[],
        });
        let max_timestamp = buffer.contents().locs().iter()
        .fold(0, |max_ts, &OrderIndex(_, i)|
            ::std::cmp::max(max_ts, u32::from(i) as u64)
        );
        assert!(max_timestamp > 0);

        buffer.clear_data();

        buffer.fill_from_entry_contents(EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::Unlock),
            data_bytes: &0,
            lock: &max_timestamp,
            locs: &[OrderIndex(5.into(), 0.into()), OrderIndex(6.into(), 0.into())],
            deps: &[],
        });
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();

        /*buffer.clear_data();

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(5.into(), 1.into()), OrderIndex(6.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });*/

        buffer.clear_data();

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(5.into(), 1.into()), OrderIndex(6.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });

        ///

        buffer.fill_from_entry_contents(EntryContents::Read {
            id: &Uuid::nil(),
            flags: &EntryFlag::Nothing,
            data_bytes: &0,
            dependency_bytes: &0,
            loc: &OrderIndex(5.into(), 1.into()),
            horizon: &OrderIndex(0.into(), 0.into()),
            min: &OrderIndex(0.into(), 0.into()),
        });
        read_stream.write_all(buffer.entry_slice()).unwrap();
        read_stream.write_all(&[0; 6]).unwrap();

        buffer.clear_data();

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(5.into(), 1.into()), OrderIndex(6.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });

        buffer.clear_data();

        buffer.fill_from_entry_contents(EntryContents::Read {
            id: &Uuid::nil(),
            flags: &EntryFlag::Nothing,
            data_bytes: &0,
            dependency_bytes: &0,
            loc: &OrderIndex(6.into(), 1.into()),
            horizon: &OrderIndex(0.into(), 0.into()),
            min: &OrderIndex(0.into(), 0.into()),
        });
        read_stream.write_all(buffer.entry_slice()).unwrap();
        read_stream.write_all(&[0; 6]).unwrap();

        buffer.clear_data();

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(5.into(), 1.into()), OrderIndex(6.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });
    }

    #[test]
    fn test_replicated_skeens_single_write_read() {
        let _ = env_logger::init();
        trace!("TCP test replicated s write/r");
        start_servers(replicas_addr, &REPLICAS_READY);
        trace!("TCP test replicated write start");
        let mut write_stream = TcpStream::connect(&"127.0.0.1:13491").unwrap();
        let mut read_stream = TcpStream::connect(&"127.0.0.1:13492").unwrap();
        let read_addr = Ipv4SocketAddr::from_socket_addr(read_stream.local_addr().unwrap());

        let mut buffer = Buffer::empty();
        let id = Uuid::new_v4();
        buffer.fill_from_entry_contents(EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock),
            lock: &0,
            locs: &[OrderIndex(7.into(), 0.into()), OrderIndex(8.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::Skeens1Queued | EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            data_bytes: &3, //TODO make 0
            lock: &0,
            locs: &[OrderIndex(7.into(), 1.into()), OrderIndex(8.into(), 1.into())],
            deps: &[],
        });
        let max_timestamp = buffer.contents().locs().iter()
        .fold(0, |max_ts, &OrderIndex(_, i)|
            ::std::cmp::max(max_ts, u32::from(i) as u64)
        );
        assert!(max_timestamp > 0);

        buffer.clear_data();

        trace!("test_replicated_skeens_single_write_read finished phase 1");

        let id2 = Uuid::new_v4();

        buffer.fill_from_entry_contents(EntryContents::Single {
            id: &id2,
            flags: &EntryFlag::Nothing,
            loc: &OrderIndex(7.into(), 0.into()),
            deps: &[],
            data: &[1, 1, 1, 1, 2],
        });
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();

        buffer.clear_data();

        buffer.fill_from_entry_contents(EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::Unlock),
            data_bytes: &0,
            lock: &max_timestamp,
            locs: &[OrderIndex(7.into(), 0.into()), OrderIndex(8.into(), 0.into())],
            deps: &[],
        });
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();

        buffer.clear_data();

        /*trace!("test_replicated_skeens_single_write_read finished phase 2");

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(7.into(), 1.into()), OrderIndex(8.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });

        buffer.clear_data();*/

        trace!("test_replicated_skeens_single_write_read finished phase 3");

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Single {
            id: &id2,
            flags: &EntryFlag::ReadSuccess,
            loc: &OrderIndex(7.into(), 2.into()),
            deps: &[],
            data: &[1, 1, 1, 1, 2],
        });

        buffer.clear_data();

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(7.into(), 1.into()), OrderIndex(8.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });

        trace!("test_replicated_skeens_single_write_read finished phase 4");

        ///

        buffer.fill_from_entry_contents(EntryContents::Read {
            id: &Uuid::nil(),
            flags: &EntryFlag::ReadSuccess,
            data_bytes: &0,
            dependency_bytes: &0,
            loc: &OrderIndex(7.into(), 1.into()),
            horizon: &OrderIndex(0.into(), 0.into()),
            min: &OrderIndex(0.into(), 0.into()),
        });
        read_stream.write_all(buffer.entry_slice()).unwrap();
        read_stream.write_all(&[0; 6]).unwrap();

        buffer.clear_data();

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(7.into(), 1.into()), OrderIndex(8.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });

        buffer.clear_data();

        buffer.fill_from_entry_contents(EntryContents::Read {
            id: &Uuid::nil(),
            flags: &EntryFlag::Nothing,
            data_bytes: &0,
            dependency_bytes: &0,
            loc: &OrderIndex(8.into(), 1.into()),
            horizon: &OrderIndex(0.into(), 0.into()),
            min: &OrderIndex(0.into(), 0.into()),
        });
        read_stream.write_all(buffer.entry_slice()).unwrap();
        read_stream.write_all(&[0; 6]).unwrap();

        buffer.clear_data();

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(7.into(), 1.into()), OrderIndex(8.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });
    }

    #[test]
    fn test_replicated_skeens_single_server_multi() {
        let _ = env_logger::init();
        trace!("TCP test replicated ssm");
        start_servers(replicas_addr, &REPLICAS_READY);
        trace!("TCP test replicated write ssm start");
        let mut write_stream = TcpStream::connect(&"127.0.0.1:13491").unwrap();
        let mut read_stream = TcpStream::connect(&"127.0.0.1:13492").unwrap();
        let read_addr = Ipv4SocketAddr::from_socket_addr(read_stream.local_addr().unwrap());

        let mut buffer = Buffer::empty();
        let id = Uuid::new_v4();
        let id2 = Uuid::new_v4();;
        buffer.fill_from_entry_contents(EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock),
            lock: &0,
            locs: &[OrderIndex(9.into(), 0.into()), OrderIndex(10.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();
        buffer.clear_data();
        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::Skeens1Queued | EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            data_bytes: &3, //TODO make 0
            lock: &0,
            locs: &[OrderIndex(9.into(), 1.into()), OrderIndex(10.into(), 1.into())],
            deps: &[],
        });
        let max_timestamp = buffer.contents().locs().iter()
        .fold(0, |max_ts, &OrderIndex(_, i)|
            ::std::cmp::max(max_ts, u32::from(i) as u64)
        );
        assert!(max_timestamp > 0);
        buffer.clear_data();

        buffer.fill_from_entry_contents(EntryContents::Multi{
            id: &id2,
            flags: &EntryFlag::Nothing,
            lock: &0,
            locs: &[OrderIndex(9.into(), 0.into()), OrderIndex(10.into(), 0.into())],
            deps: &[],
            data: &[123, 01, 255, 11],
        });
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();
        buffer.clear_data();


        buffer.fill_from_entry_contents(EntryContents::Senti{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::Unlock),
            data_bytes: &0,
            lock: &max_timestamp,
            locs: &[OrderIndex(9.into(), 0.into()), OrderIndex(10.into(), 0.into())],
            deps: &[],
        });
        write_stream.write_all(buffer.entry_slice()).unwrap();
        write_stream.write_all(read_addr.bytes()).unwrap();

        buffer.clear_data();

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::ReadSuccess),
            lock: &0,
            locs: &[OrderIndex(9.into(), 1.into()), OrderIndex(10.into(), 1.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        });

        buffer.clear_data();

        recv_packet(&mut buffer, &mut read_stream);
        assert_eq!(buffer.contents(), EntryContents::Multi{
            id: &id2,
            flags: &EntryFlag::ReadSuccess,
            lock: &0,
            locs: &[OrderIndex(9.into(), 2.into()), OrderIndex(10.into(), 2.into())],
            deps: &[],
            data: &[123, 01, 255, 11],
        });
    }

    #[test]
    fn test_empty_read() {
        let _ = env_logger::init();
        trace!("TCP test write_read");
        start_servers(basic_addr, &BASIC_SERVER_READY);
        trace!("TCP test write_read start");
        let mut stream = TcpStream::connect(&"127.0.0.1:13490").unwrap();
        let mut buffer = Buffer::empty();
        Data(&(), &[OrderIndex(0.into(), 0.into())]).fill_entry(&mut buffer);
        buffer.fill_from_entry_contents(EntryContents::Read {
            id: &Uuid::nil(),
            flags: &EntryFlag::Nothing,
            data_bytes: &0,
            dependency_bytes: &0,
            loc: &OrderIndex(0.into(), 0.into()),
            horizon: &OrderIndex(0.into(), 0.into()),
            min: &OrderIndex(0.into(), 0.into()),
        });
        buffer.ensure_len();
        buffer.contents_mut().locs_mut()[0] = OrderIndex(0.into(), 1.into());
        stream.write_all(buffer.entry_slice()).unwrap();
        stream.write_all(&[0; 6]).unwrap();
        buffer[..].iter_mut().fold((), |_, i| *i = 0);
        recv_packet(&mut buffer, &mut stream);
        assert!(!buffer.entry().flag().contains(EntryFlag::ReadSuccess));
        assert_eq!(buffer.contents().locs()[0], OrderIndex(0.into(), 1.into()));
        assert_eq!(buffer.contents().horizon(), OrderIndex(0.into(), 0.into()));
    }

    //FIXME add empty read tests

    fn recv_packet(buffer: &mut Buffer, mut stream: &TcpStream) {
        use packets::Packet::WrapErr;
        let mut read = 0;
        loop {
            let to_read = buffer.finished_at(read);
            let size = match to_read {
                Err(WrapErr::NotEnoughBytes(needs)) => needs,
                Err(err) => panic!("{:?}", err),
                Ok(size) if read < size => size,
                Ok(..) => return,
            };
            let r = stream.read(&mut buffer[read..size]);
            match r {
                Ok(i) => read += i,
                Err(e) => panic!("recv error {:?}", e),
            }
        }
    }

    fn start_servers<'a, 'b>(addr_strs: &'a [&'b str], server_ready: &'static AtomicUsize) {
        use std::thread;
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
                    ::tcp::run(acceptor, 0, 1, 1, server_ready)
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
                        ::tcp::run_with_replication(acceptor, 0, 1,
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

fn get_next_token(token: &mut mio::Token) -> mio::Token {
    *token = mio::Token(token.0.checked_add(1).unwrap());
    *token
}

fn worker_for_ip(ip: Ipv4SocketAddr, num_workers: u64) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher: FxHasher = Default::default();
    ip.hash(&mut hasher);
    (hasher.finish() % num_workers) as usize
}