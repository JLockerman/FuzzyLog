use std::collections::hash_map;
use std::collections::VecDeque;
use std::io::{self, Read, Write, ErrorKind};
use std::{mem, thread};
use std::sync::mpsc;
use std::sync::atomic::{AtomicUsize, Ordering};

use prelude::*;
use servers2::{self, spmc, ServerLog, ToWorker};
use hash::HashMap;

use mio;
use mio::channel as mio_channel;
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

pub fn run(
    acceptor: TcpListener,
    this_server_num: u32,
    total_chain_servers: u32,
    num_workers: usize,
    ready: &AtomicUsize,
) -> ! {
    use std::cmp::max;

    let (dist_to_workers, recv_from_dist) = spmc::channel();
    let (log_to_workers, recv_from_log) = spmc::channel();
    //TODO or sync channel?
    let (workers_to_log, recv_from_workers) = mpsc::channel();
    let (workers_to_dist, dist_from_workers) = mio_channel::channel();
    if num_workers == 0 {
        warn!("SERVER {} started with 0 workers", this_server_num);
    }
    trace!("SERVER {} starting {} workers.", this_server_num, num_workers);
    for n in 0..max(num_workers, 1) {
        let from_dist = recv_from_dist.clone();
        let to_dist   = workers_to_dist.clone();
        let from_log  = recv_from_log.clone();
        let to_log    = workers_to_log.clone();
        thread::spawn(move ||
            run_worker(
                from_dist,
                to_dist,
                from_log,
                to_log,
                n,
            )
        );
    }

    thread::spawn(move || {
        let mut log = ServerLog::new(this_server_num, total_chain_servers, log_to_workers);
        for (buffer, st) in recv_from_workers.iter() {
            log.handle_op(buffer, st)
        }
    });

    const ACCEPT: mio::Token = mio::Token(0);
    const FROM_WORKERS: mio::Token = mio::Token(1);
    let poll = mio::Poll::new().unwrap();
    poll.register(&acceptor,
        ACCEPT,
        mio::Ready::readable(),
        mio::PollOpt::level()
    );
    poll.register(&dist_from_workers,
        FROM_WORKERS,
        mio::Ready::readable(),
        mio::PollOpt::level()
    );
    ready.fetch_add(1, Ordering::SeqCst);
    let mut receivers: HashMap<_, _> = Default::default();
    let mut events = mio::Events::with_capacity(1023);
    let mut next_token = mio::Token(2);
    let mut buffer_cache = VecDeque::new();
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
                            poll.register(
                                &socket,
                                tok,
                                mio::Ready::readable(),
                                mio::PollOpt::edge() | mio::PollOpt::oneshot(),
                            );
                            receivers.insert(tok, Some(socket));
                        }
                    }
                }
                FROM_WORKERS => {
                    trace!("SERVER dist getting finished sockets");
                    while let Ok((buffer, socket, tok)) = dist_from_workers.try_recv() {
                        trace!("SERVER dist got {:?}", tok);
                        buffer_cache.push_back(buffer);
                        poll.reregister(
                            &socket,
                            tok,
                            mio::Ready::readable(),
                            mio::PollOpt::edge() | mio::PollOpt::oneshot(),
                        );
                        *receivers.get_mut(&tok).unwrap() = Some(socket)
                    }
                },
                recv_tok => {
                    let recv = receivers.get_mut(&recv_tok).unwrap();
                    let recv = mem::replace(recv, None);
                    match recv {
                        None => trace!("spurious wakeup for {:?}", recv_tok),
                        Some(socket) => {
                            trace!("SERVER need to recv from {:?}", recv_tok);
                            //TODO should be min size ?
                            let buffer =
                                buffer_cache.pop_back().unwrap_or(Buffer::empty());
                            dist_to_workers.send((buffer, socket, recv_tok))
                        }
                    }
                }
            }
        }
    }
}

#[derive(Copy, Clone, Debug)]
#[repr(u8)]
enum Io { Read, Write, ReadWrite }

#[derive(Copy, Clone, Debug)]
#[repr(u8)]
enum AfterWorker { SendToLog, SendToDist}

//TODO pipelining
fn run_worker(
    from_dist: spmc::Receiver<(Buffer, TcpStream, mio::Token)>,
    to_dist: mio_channel::Sender<(Buffer, TcpStream, mio::Token)>,
    from_log: spmc::Receiver<ToWorker<(TcpStream, mio::Token)>>,
    to_log: mpsc::Sender<(Buffer, (TcpStream, mio::Token))>,
    worker_num: usize,
) -> ! {
    const FROM_DIST: mio::Token = mio::Token(0);
    const FROM_LOG: mio::Token = mio::Token(1);
    trace!("WORKER {} start.", worker_num);
    let poll = mio::Poll::new().unwrap();
    let mut current_io: HashMap<_, _> = Default::default();
    poll.register(
        &from_dist,
        FROM_DIST,
        mio::Ready::readable(),
        mio::PollOpt::level());
    poll.register(
        &from_log,
        FROM_LOG,
        mio::Ready::readable(),
        mio::PollOpt::level());
    let mut events = mio::Events::with_capacity(127);
    loop {
        //Ideal loop
        //  1. do writes
        //  2. get from log
        //  3? do reads
        //  4. get from dist
        //  5. if no more work, poll

        poll.poll(&mut events, None).expect("worker poll failed");
        let mut handled_event = false;
        'event: for event in events.iter() {
            if let hash_map::Entry::Occupied(mut o) = current_io.entry(event.token()) {
                handled_event = true;
                let next = {
                    trace!("WORKER {} handling {:?}", worker_num, event.token());
                    let &mut (ref mut buffer, ref stream, ref mut size, _, io) = o.get_mut();
                    match io {
                        Io::Read => match recv_packet(buffer, stream, *size) {
                            RecvRes::Done => AfterWorker::SendToLog,
                            RecvRes::Error => AfterWorker::SendToDist,
                            RecvRes::NeedsMore(read) => {
                                *size = read;
                                continue 'event
                            }
                        },
                        Io::Write => match send_packet(buffer, stream, *size) {
                            None => AfterWorker::SendToDist,
                            Some(sent) => {
                                *size = sent;
                                continue 'event
                            }
                        },
                        Io::ReadWrite => unimplemented!(),
                    }
                };
                let (buffer, stream, _, tok, _) = o.remove();
                poll.deregister(&stream);
                match next {
                    AfterWorker::SendToLog => to_log.send((buffer, (stream, tok))).unwrap(),
                    AfterWorker::SendToDist => to_dist.send((buffer, stream, tok))
                        .ok().unwrap(),
                }
            }
        }

        //'send: loop {
        if !handled_event {
            match from_log.try_recv() {
                None => {}//break 'send,
                Some(tw) => {
                    servers2::handle_to_worker(tw).map(|(buffer, (stream, tok))| {
                        trace!("WORKER {} got send {:?} from log for.", worker_num, tok);
                        let continue_write = send_packet(&buffer, &stream, 0);
                        if let Some(s) = continue_write {
                            let _ = poll.register(
                                &stream,
                                tok,
                                mio::Ready::writable() | mio::Ready::error(),
                                mio::PollOpt::edge()
                            );
                            current_io.insert(tok, (buffer, stream, s, tok, Io::Write));
                        }
                        else {
                            to_dist.send((buffer, stream, tok)).ok().unwrap()
                        }
                    });
                    //break 'send
                    handled_event = true;
                },
            };
        }

        //'recv: loop {
        if !handled_event {
            match from_dist.try_recv() {
                None => {}//break 'recv,
                Some((mut buffer, stream, tok)) => {
                    trace!("WORKER {} got recv {:?} from dist.", worker_num, tok);
                    let continue_read = recv_packet(&mut buffer, &stream, 0);
                    match continue_read {
                        RecvRes::Done => to_log.send((buffer, (stream, tok))).unwrap(),
                        RecvRes::Error => to_dist.send((buffer, stream, tok)).ok().unwrap(),
                        RecvRes::NeedsMore(read) => {
                            let _ = poll.register(
                                &stream,
                                tok,
                                mio::Ready::readable() | mio::Ready::error(),
                                mio::PollOpt::edge()
                            );
                            current_io.insert(tok, (buffer, stream, read, tok, Io::Read));
                        }
                    }
                    //TODO
                    //break 'recv
                    handled_event = true
                },
            }
        }
    }
}

fn send_packet(buffer: &Buffer, mut stream: &TcpStream, sent: usize) -> Option<usize> {
    let bytes_to_write = buffer.entry_slice().len();
    match stream.write(&buffer.entry_slice()[sent..]) {
       Ok(i) if (sent + i) < bytes_to_write => Some(sent + i),
       Err(e) => if e.kind() == ErrorKind::WouldBlock { Some(sent) } else { None },
       _ => {
           trace!("WORKER finished send");
           None
       }
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
