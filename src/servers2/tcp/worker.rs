
use std::collections::VecDeque;
use std::sync::mpsc;
use std::time::Duration;

use servers2::{self, spmc, ToReplicate, ToWorker, DistributeToWorkers};
use hash::HashMap;
use socket_addr::Ipv4SocketAddr;

use mio;
use mio::tcp::*;

use buffer::Buffer;

use byteorder::{ByteOrder, LittleEndian};

//use super::{DistToWorker, WorkerToDist, ToLog, WorkerNum};
use super::*;
use super::per_socket::{PerSocket, RecvPacket};


//FIXME we should use something more accurate than &static [u8],
pub enum WorkerToDist {
    Downstream(WorkerNum, Ipv4SocketAddr, &'static [u8], u64),
    ToClient(Ipv4SocketAddr, &'static [u8]),
}

pub enum DistToWorker {
    NewClient(mio::Token, TcpStream),
    //ToClient(ToWorker<(usize, mio::Token, Ipv4SocketAddr)>),
    ToClient(mio::Token, &'static [u8], Ipv4SocketAddr, u64),
}

pub enum ToLog<T> {
    //TODO test different layouts.
    New(Buffer, Option<Box<(Box<[u8]>, Box<[u8]>)>>, T),
    Replication(ToReplicate, T)
}

pub struct Worker {
    clients: HashMap<mio::Token, PerSocket>,
    inner: WorkerInner,
}

#[allow(dead_code)]
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

    pub fn new(
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
        let mut awake_io: VecDeque<_> = Default::default();
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
            awake_io.push_back(UPSTREAM);
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
            awake_io.push_back(DOWNSTREAM);
            clients.insert(DOWNSTREAM, ps);
            has_downstream = true;
        }
        Worker {
            clients: clients,
            inner: WorkerInner {
                awake_io: awake_io,
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

    pub fn run(mut self) -> ! {
        let mut events = mio::Events::with_capacity(1024);
        loop {
            self.inner.poll.poll(&mut events, Some(Duration::from_millis(1)))
                .expect("worker poll failed");
            let new_events = events.len();
            self.handle_new_events(events.iter());

            if new_events == 0 {
                assert!(self.inner.awake_io.is_empty());
                println!("no new events @ {:?}", self.inner.worker_num);
                for (tok, pc) in self.clients.iter_mut() {
                    self.inner.handle_burst(*tok, pc);
                }
                if !self.inner.awake_io.is_empty() {
                    println!("ERROR bad sleep @ {:?}", self.inner.worker_num);
                }
            }

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
                self.inner.poll.poll(&mut events, Some(Duration::new(0, 1)))
                    .expect("worker poll failed");
                self.handle_new_events(events.iter());
            }
            #[cfg(debug_assertions)]
            for (tok, c) in self.clients.iter() {
                debug_assert!(!(c.needs_to_stay_awake() && !c.is_backpressured()),
                    "Token {:?} sleep @ stay_awake: {}, backpressure: {}",
                    tok, c.needs_to_stay_awake(), c.is_backpressured());
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
                                self.inner.awake_io.push_back(token);
                                continue 'event
                            }
                            Some((ref worker, ref tok))
                            if *worker != self.inner.worker_num && *tok != DOWNSTREAM => {
                                self.inner.to_dist.send(WorkerToDist::ToClient(src_addr, bytes)).ok().unwrap();
                                self.clients.get_mut(&token).unwrap().return_buffer(buffer);
                                self.inner.awake_io.push_back(token);
                                self.inner.awake_io.push_back(*tok);
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
                            self.inner.awake_io.push_back(token);
                            self.inner.awake_io.push_back(tok);
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
                        let _state =
                            self.clients.entry(tok).or_insert(PerSocket::client(stream));
                        //self.inner.recv_packet(tok, _state);
                        self.inner.awake_io.push_back(tok);
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
        use super::per_socket::PerSocket::*;
        let (send, recv) = match socket_state {
            &mut Upstream {..} => (false, true),
            &mut Downstream {..} => (true, false),
            &mut Client {..} => (true, true),
        };
        socket_state.wake();
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
        if socket_state.needs_to_stay_awake() {
            self.awake_io.push_back(token)
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
            Ok(true) => {} //TODO self.awake_io.push_back(token),
            Ok(false) => {},
            //FIXME remove from map, log
            Err(e) => {
                error!("send error {:?} @ {:?}", e, token);
                let _ = self.poll.deregister(socket_state.stream());
            }
        }
    }

    fn recv_packet(
        &mut self,
        token: mio::Token,
        socket_state: &mut PerSocket
    ) {
        //TODO let socket_kind = socket_state.kind();
        let packet = socket_state.recv_packet();
        match packet {
            //FIXME remove from map, log
            RecvPacket::Err => {
                error!("recv error @ {:?}", token);
                let _ = self.poll.deregister(socket_state.stream());
            },
            RecvPacket::Pending => {},
            RecvPacket::FromClient(packet, src_addr) => {
                trace!("WORKER {} finished recv from client.", self.worker_num);
                let worker = self.worker_num;
                self.send_to_log(packet, worker, token, src_addr);
                //self.awake_io.push_back(token)
            }
            RecvPacket::FromUpstream(packet, src_addr, storage_loc) => {
                trace!("WORKER {} finished recv from up.", self.worker_num);
                //let (worker, work_tok) = self.next_hop(token, src_addr, socket_kind);
                let worker = self.worker_num;
                self.send_replication_to_log(packet, storage_loc, worker, token, src_addr);
                //self.awake_io.push_back(token)
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

impl DistributeToWorkers<(usize, mio::Token, Ipv4SocketAddr)>
for Vec<spmc::Sender<ToWorker<(usize, mio::Token, Ipv4SocketAddr)>>> {
    fn send_to_worker(&mut self, msg: ToWorker<(usize, mio::Token, Ipv4SocketAddr)>) {
        let (which_queue, token, _) = msg.get_associated_data();
        trace!("SERVER   sending to worker {} {:?} ", which_queue, token);
        self[which_queue].send(msg)
    }
}
