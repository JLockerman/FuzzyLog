use std::collections::hash_map::Entry as HashEntry;
use std::collections::VecDeque;
use std::io::{self, Read, Write, ErrorKind};
use std::{mem, thread};
use std::sync::mpsc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use prelude::*;
use servers2::{self, spmc, ServerLog, ToWorker, DistributeToWorkers};
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

    //let (dist_to_workers, recv_from_dist) = spmc::channel();
    //let (log_to_workers, recv_from_log) = spmc::channel();
    //TODO or sync channel?
    let (workers_to_log, recv_from_workers) = mpsc::channel();
    //let (workers_to_dist, dist_from_workers) = mio_channel::channel();
    if num_workers == 0 {
        warn!("SERVER {} started with 0 workers", this_server_num);
    }
    trace!("SERVER {} starting {} workers.", this_server_num, num_workers);
    let mut log_to_workers: Vec<_> = Vec::with_capacity(num_workers);
    let mut dist_to_workers: Vec<_> = Vec::with_capacity(num_workers);
    for n in 0..max(num_workers, 1) {
        //let from_dist = recv_from_dist.clone();
        //let to_dist   = workers_to_dist.clone();
        //let from_log  = recv_from_log.clone();
        let to_log    = workers_to_log.clone();
        let (to_worker, from_log) = spmc::channel();
        let (dist_to_worker, from_dist) = spmc::channel();
        thread::spawn(move ||
            Worker::new(from_dist, from_log, to_log, n).run()
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

    const ACCEPT: mio::Token = mio::Token(0);
    const FROM_WORKERS: mio::Token = mio::Token(1);
    let poll = mio::Poll::new().unwrap();
    poll.register(&acceptor,
        ACCEPT,
        mio::Ready::readable(),
        mio::PollOpt::level()
    );
    /*poll.register(&dist_from_workers,
        FROM_WORKERS,
        mio::Ready::readable(),
        mio::PollOpt::level()
    );*/
    ready.fetch_add(1, Ordering::SeqCst);
    //let mut receivers: HashMap<_, _> = Default::default();
    let mut events = mio::Events::with_capacity(1023);
    let mut next_token = mio::Token(2);
    //let mut buffer_cache = VecDeque::new();
    let mut next_worker = 0;
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
                            dist_to_workers[next_worker].send((tok, buffer, socket));
                            next_worker = next_worker.wrapping_add(1);
                            if next_worker >= dist_to_workers.len() {
                                next_worker = 0;
                            }
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
                recv_tok => {
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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Io { Log, Read, Write, ReadWrite }

#[repr(u8)]
enum IoBuffer { Log, Read(Buffer), Write(Buffer), ReadWrite(Buffer) }

#[derive(Copy, Clone, Debug)]
#[repr(u8)]
enum AfterWorker { SendToLog, SendToDist}

struct Worker {
    sleeping_io: HashMap<mio::Token, PerSocket>,
    inner: WorkerInner,
}

struct WorkerInner {
    awake_io: VecDeque<mio::Token>,
    from_dist: spmc::Receiver<(mio::Token, Buffer, TcpStream)>,
    from_log: spmc::Receiver<ToWorker<(usize, mio::Token)>>,
    to_log: mpsc::Sender<(Buffer, (usize, mio::Token))>,
    worker_num: usize,
    poll: mio::Poll,
}

struct PerSocket {
    buffer: IoBuffer,
    stream: TcpStream,
    bytes_handled: usize,
}

const FROM_DIST: mio::Token = mio::Token(0);
const FROM_LOG: mio::Token = mio::Token(1);

impl Worker {

    fn new(
        from_dist: spmc::Receiver<(mio::Token, Buffer, TcpStream)>,
        from_log: spmc::Receiver<ToWorker<(usize, mio::Token)>>,
        to_log: mpsc::Sender<(Buffer, (usize, mio::Token))>,
        worker_num: usize
    ) -> Self {
        let poll = mio::Poll::new().unwrap();
        poll.register(
            &from_dist,
            FROM_DIST,
            mio::Ready::readable(),
            mio::PollOpt::level()
        );
        poll.register(
            &from_log,
            FROM_LOG,
            mio::Ready::readable(),
            mio::PollOpt::level()
        );
        Worker {
            sleeping_io: Default::default(),
            inner: WorkerInner {
                awake_io: Default::default(),
                from_dist: from_dist,
                from_log: from_log,
                to_log: to_log,
                poll: poll,
                worker_num: worker_num,
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
                for i in 0..ops_before_poll {
                    let token = self.inner.awake_io.pop_front().unwrap();
                    if let HashEntry::Occupied(o) = self.sleeping_io.entry(token) {
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
                        //TODO
                        let state = self.sleeping_io.get_mut(&tok).unwrap();
                        debug_assert_eq!(state.io(), Io::Log);
                        state.post_recv_from_log(buffer);
                        self.inner.send_packet(tok, state);
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
                    //self.sleeping_io.insert(tok, PerSocket::new(buffer, stream));
                    //TODO assert unique?
                    let state =
                        self.sleeping_io.entry(tok).or_insert(PerSocket::new(buffer, stream));
                    self.inner.recv_packet(tok, state);
                }
                continue 'event
            }

            if let HashEntry::Occupied(o) = self.sleeping_io.entry(token) {
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
        match socket_state.io() {
            Io::Read => self.recv_packet(token, socket_state),
            Io::Write => self.send_packet(token, socket_state),
            Io::Log => {/* It looks like this should be a no-op */},
            Io::ReadWrite => unimplemented!(),
        }
    }

    //TODO these functions should return Result so we can remove from map
    fn send_packet(
        &mut self,
        token: mio::Token,
        socket_state: &mut PerSocket
    ) {
        match send_packet(
            socket_state.buffer.send_buffer(),
            &socket_state.stream,
            socket_state.bytes_handled,
        ) {
            SendRes::NeedsMore(handled) => socket_state.continue_write(handled),

            //TODO or immediately recv
            SendRes::Done => {
                socket_state.finish_write();
                self.awake_io.push_back(token);
            }

            //FIXME remove from map
            SendRes::Error => { self.poll.deregister(&socket_state.stream); }
        }
    }

    fn recv_packet(
        &mut self,
        token: mio::Token,
        socket_state: &mut PerSocket
    ) {
        match recv_packet(
            socket_state.buffer.recv_buffer(),
            &socket_state.stream,
            socket_state.bytes_handled,
        ) {
            RecvRes::NeedsMore(handled) => socket_state.continue_read(handled),

            //TODO send to log
            RecvRes::Done =>  { self.send_to_log(token, socket_state); }

            //FIXME remove from map
            RecvRes::Error => { self.poll.deregister(&socket_state.stream); }
        }
    }

    fn send_to_log(&mut self, token: mio::Token, socket_state: &mut PerSocket) {
        let buffer = socket_state.pre_send_to_log();
        self.to_log.send((buffer, (self.worker_num, token)));
    }
}

impl PerSocket {
    fn new(buffer: Buffer, stream: TcpStream) -> Self {
        PerSocket {
            buffer: IoBuffer::Read(buffer),
            stream: stream,
            bytes_handled: 0,
        }
    }

    fn io(&self) -> Io {
        match self.buffer {
            IoBuffer::Log => Io::Log,
            IoBuffer::Read(..) => Io::Read,
            IoBuffer::Write(..) => Io::Write,
            IoBuffer::ReadWrite(..) => Io::ReadWrite,
        }
    }

    fn continue_write(&mut self, bytes_handled: usize) {
        self.bytes_handled = bytes_handled;
        let old = mem::replace(&mut self.buffer, IoBuffer::Log);
        match old {
            IoBuffer::Write(buffer) => self.buffer = IoBuffer::Write(buffer),
            _ => unreachable!()
        }
    }

    fn finish_write(&mut self) {
        self.bytes_handled = 0;
        let old = mem::replace(&mut self.buffer, IoBuffer::Log);
        match old {
            IoBuffer::Write(buffer) => self.buffer = IoBuffer::Read(buffer),
            _ => unreachable!()
        }
    }

    fn continue_read(&mut self, bytes_handled: usize) {
        self.bytes_handled = bytes_handled;
        let old = mem::replace(&mut self.buffer, IoBuffer::Log);
        match old {
            IoBuffer::Read(buffer) => self.buffer = IoBuffer::Read(buffer),
            _ => unreachable!()
        }
    }

    fn pre_send_to_log(&mut self) -> Buffer {
        self.bytes_handled = 0;
        let old = mem::replace(&mut self.buffer, IoBuffer::Log);
        match old {
            IoBuffer::Read(buffer) => { self.buffer = IoBuffer::Log; buffer }
            _ => unreachable!()
        }
    }

    fn post_recv_from_log(&mut self, buffer: Buffer) {
        self.bytes_handled = 0;
        let old = mem::replace(&mut self.buffer, IoBuffer::Log);
        match old {
            IoBuffer::Log => self.buffer = IoBuffer::Write(buffer),
            _ => unreachable!()
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
       Ok(i) => SendRes::Done,
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
