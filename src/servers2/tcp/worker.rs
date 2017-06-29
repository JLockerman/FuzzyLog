#![allow(non_snake_case)]

use std::collections::VecDeque;
use std::sync::mpsc;
use std::time::Duration;

use servers2::{
    self, spsc, worker_thread, ToReplicate, ToWorker,
    DistributeToWorkers, Troption, SkeensMultiStorage,
    ToSend, ChainReader,
};
use servers2::shared_slice::RcSlice;
use hash::HashMap;
use socket_addr::Ipv4SocketAddr;

use packets::{EntryLayout, OrderIndex, EntryFlag};

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
    DownstreamB(WorkerNum, Ipv4SocketAddr, Box<[u8]>, u64),
    ToClient(Ipv4SocketAddr, &'static [u8]),
    ToClientB(Ipv4SocketAddr, Box<[u8]>),
}

#[allow(dead_code)]
pub enum DistToWorker {
    NewClient(mio::Token, TcpStream),
    //ToClient(ToWorker<(usize, mio::Token, Ipv4SocketAddr)>),
    ToClient(mio::Token, &'static [u8], Ipv4SocketAddr, u64),
    ToClientB(mio::Token, Box<[u8]>, Ipv4SocketAddr, u64),
    ToClientC(mio::Token, Box<[u8]>, Ipv4SocketAddr),
    //ToReplicate(mio::Token, Box<[u8]>, Ipv4SocketAddr, u64),
    //ToClientC(mio::Token, Box<EntryContents<'static>>, Ipv4SocketAddr, u64),
}

pub enum ToLog<T> {
    //TODO test different layouts.
    New(Buffer, Troption<SkeensMultiStorage, Box<(RcSlice, RcSlice)>>, T),
    Replication(ToReplicate, T)
}

pub struct Worker {
    clients: HashMap<mio::Token, PerSocket>,
    inner: WorkerInner,
}

#[allow(dead_code)]
struct WorkerInner {
    awake_io: VecDeque<mio::Token>,
    from_dist: spsc::Receiver<DistToWorker>,
    to_dist: mio::channel::Sender<WorkerToDist>,
    from_log: spsc::Receiver<ToWorker<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
    to_log: mpsc::Sender<ToLog<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
    log_reader: ChainReader<(WorkerNum, mio::Token, Ipv4SocketAddr)>,
    ip_to_worker: HashMap<Ipv4SocketAddr, (WorkerNum, mio::Token)>,
    worker_num: WorkerNum,
    downstream_workers: WorkerNum,
    num_workers: WorkerNum,
    poll: mio::Poll,
    is_unreplicated: bool,
    has_downstream: bool,
    //waiting_for_log: usize,

    print_data: WorkerData,
}

counters! {
    struct WorkerData {
        from_dist_N: u64,
        from_dist_D: u64,
        from_dist_T: u64,
        from_log: u64,
        finished_send: u64,
        finished_recv_from_client: u64,
        finished_recv_from_up: u64,
        new_to_log: u64,
        to_log: u64,
        rep_to_log: u64,
    }
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
        from_dist: spsc::Receiver<DistToWorker>,
        to_dist: mio::channel::Sender<WorkerToDist>,
        from_log: spsc::Receiver<ToWorker<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
        to_log: mpsc::Sender<ToLog<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
        log_reader: ChainReader<(WorkerNum, mio::Token, Ipv4SocketAddr)>,
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
            mio::PollOpt::edge()
        ).expect("cannot pol from dist on worker");
        poll.register(
            &from_log,
            FROM_LOG,
            mio::Ready::readable(),
            mio::PollOpt::edge()
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
                log_reader: log_reader,
                ip_to_worker: Default::default(),
                poll: poll,
                worker_num: worker_num,
                is_unreplicated: is_unreplicated,
                downstream_workers: downstream_workers,
                num_workers: num_workers,
                has_downstream: has_downstream,
                print_data: Default::default(),
                //waiting_for_log: 0,
            }
        }
    }

    pub fn run(mut self) -> ! {
        let mut events = mio::Events::with_capacity(1024);
        let mut timeout_idx = 0;
        loop {
            //10µs, 100µs, 500µs, 1ms, 10ms, 100ms, 1s, 10s, 10s
            const TIMEOUTS: [(u64, u32); 9] =
                [(0, 10_000), (0, 100_000), (0, 500_000), (0, 1_000_000),
                (0, 10_000_000), (0, 100_000_000), (1, 0), (10, 0), (10, 0)];
            //#[cfg(feature = "print_stats")]
            //let _ = self.inner.poll.poll(&mut events, Some(Duration::from_secs(10)));
            //#[cfg(not(feature = "print_stats"))]
            //let _ = self.inner.poll.poll(&mut events, None);
            let timeout = TIMEOUTS[timeout_idx];
            let timeout = Duration::new(timeout.0, timeout.1);
            let _ = self.inner.poll.poll(&mut events, Some(timeout));
            if events.len() == 0 {
                #[cfg(feature = "print_stats")]
                {
                    if TIMEOUTS[timeout_idx].0 >= 10 {
                        println!("Worker {:?}: {:#?}",
                            self.inner.worker_num, self.inner.print_data);
                        for (&t, ps) in self.clients.iter() {
                            println!("Worker ({:?}, {:?}): {:#?}, {:#?}",
                                self.inner.worker_num, t, ps.print_data(),ps.more_data());
                            assert!(!(ps.needs_to_stay_awake() && !ps.is_backpressured()),
                                "Token {:?} sleep @ stay_awake: {}, backpressure: {}",
                                t, ps.needs_to_stay_awake(), ps.is_backpressured());
                        }
                    }
                }
                for (&t, _) in self.clients.iter() {
                    self.inner.awake_io.push_back(t)
                }
                self.handle_from_dist();
                self.handle_from_log();
                if !self.inner.awake_io.is_empty() {
                    if timeout_idx + 1 < TIMEOUTS.len() {
                        timeout_idx += 1
                    }
                } else {
                    if timeout_idx > 0 {
                       timeout_idx -= 1
                    }
                }
                //FIXME add call to handle_from_dist()
            } else {
                if timeout_idx > 0 {
                    timeout_idx -= 1
                }
            }
            //let new_events = events.len();

            self.handle_new_events(events.iter());

            // if new_events == 0 {
                // assert!(self.inner.awake_io.is_empty());
                // println!("no new events @ {:?}", self.inner.worker_num);
                // for (tok, pc) in self.clients.iter_mut() {
                    // self.inner.handle_burst(*tok, pc);
                // }
                // if !self.inner.awake_io.is_empty() {
                    // println!("ERROR bad sleep @ {:?}", self.inner.worker_num);
                // }
            // }

            /*'from_log: loop {
                self.handle_from_log();

                if self.inner.waiting_for_log == 0 {
                    break 'from_log
                }
            }*/
            'work: loop {
                let ops_before_poll = self.inner.awake_io.len();
                for _ in 0..ops_before_poll {
                    let token = self.inner.awake_io.pop_front().unwrap();
                    if let HashEntry::Occupied(mut o) = self.clients.entry(token) {
                        let e = self.inner.handle_burst(token, o.get_mut());
                        if e.is_err() {
                            o.remove();
                        }
                    }
                }

                //TODO add yield if spends too long waiting for log?
                if self.inner.awake_io.is_empty() /*&& self.inner.waiting_for_log == 0*/ {
                    break 'work
                }

                let _ = self.inner.poll.poll(&mut events, Some(Duration::from_millis(0)));
                self.handle_new_events(events.iter());
            }
            #[cfg(debug_assertions)]
            for (tok, c) in self.clients.iter() {
                debug_assert!(!(c.should_be_awake() && !c.is_backpressured()),
                    "Token {:?} sleep @ stay_awake: {}, backpressure: {}
                    {:?}",
                    tok, c.needs_to_stay_awake(), c.is_backpressured(),
                    c);
            }
        }
    }// end fn run

    fn handle_new_events(&mut self, events: mio::EventsIter) {
        'event: for event in events {
            let token = event.token();

            if token == FROM_LOG /*&& self.inner.waiting_for_log > 0*/ {
                self.handle_from_log();
                continue 'event
            }

            if token == FROM_DIST {
                self.handle_from_dist();
                continue 'event
            }

            if let HashEntry::Occupied(mut o) = self.clients.entry(token) {
                //TODO check token read/write
                //let state = o.into_mut();
                let e = self.inner.handle_burst(token, o.get_mut());
                if e.is_err() {
                    o.remove();
                }
            }
        }
    }// end handle_new_events

    fn handle_from_dist(&mut self) {
        loop {
            match self.inner.from_dist.try_recv() {
                None => return,
                Some(DistToWorker::NewClient(tok, stream)) => {
                    debug_assert!(tok.0 >= FIRST_CLIENT_TOKEN.0);
                    self.inner.print_data.from_dist_N(1);
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
                    self.inner.print_data.from_dist_D(1);
                    trace!("WORKER {} recv downstream from dist for {}.",
                        self.inner.worker_num, src_addr);
                    {
                        let client = self.clients.get_mut(&DOWNSTREAM).unwrap();
                        //client.add_downstream_send(buffer);
                        //client.add_downstream_send(src_addr.bytes());
                        let mut storage_log_bytes: [u8; 8] = [0; 8];
                        LittleEndian::write_u64(&mut storage_log_bytes, storage_loc);
                        //client.add_downstream_send(&storage_log_bytes)
                        client.add_downstream_send3(
                            buffer, &storage_log_bytes, src_addr.bytes());
                    }
                    //TODO replace with wake function
                    if self.clients[&DOWNSTREAM].needs_to_stay_awake() {
                        self.inner.awake_io.push_back(DOWNSTREAM);
                        self.clients.get_mut(&DOWNSTREAM).map(|p| p.is_staying_awake());
                    }
                }
                Some(DistToWorker::ToClient(tok, buffer, src_addr, storage_loc)) => {
                    self.inner.print_data.from_dist_T(1);
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
                    if self.clients[&tok].needs_to_stay_awake() {
                        self.inner.awake_io.push_back(tok);
                        self.clients.get_mut(&tok).map(|p| p.is_staying_awake());
                    }
                },

                Some(DistToWorker::ToClientB(DOWNSTREAM, buffer, src_addr, storage_loc)) => {
                    self.inner.print_data.from_dist_D(1);
                    trace!("WORKER {} recv downstream from dist for {}.",
                        self.inner.worker_num, src_addr);
                    {
                        let client = self.clients.get_mut(&DOWNSTREAM).unwrap();
                        //client.add_downstream_send(buffer);
                        //client.add_downstream_send(src_addr.bytes());
                        let mut storage_log_bytes: [u8; 8] = [0; 8];
                        LittleEndian::write_u64(&mut storage_log_bytes, storage_loc);
                        //client.add_downstream_send(&storage_log_bytes)
                        client.add_downstream_send3(
                            &buffer, &storage_log_bytes, src_addr.bytes());
                    }
                    //TODO replace with wake function
                    if self.clients[&DOWNSTREAM].needs_to_stay_awake() {
                        self.inner.awake_io.push_back(DOWNSTREAM);
                        self.clients.get_mut(&DOWNSTREAM).map(|p| p.is_staying_awake());
                    }
                }

                Some(DistToWorker::ToClientB(tok, buffer, src_addr, storage_loc)) => {
                    self.inner.print_data.from_dist_T(1);
                    debug_assert_eq!(
                        Ipv4SocketAddr::from_socket_addr(
                                self.clients[&tok].stream().peer_addr().unwrap()
                        ),
                        src_addr
                    );
                    debug_assert_eq!(storage_loc, 0);
                    trace!("WORKER {} recv to_client from dist for {}.", self.inner.worker_num, src_addr);
                    assert!(tok >= FIRST_CLIENT_TOKEN);
                    self.clients.get_mut(&tok).unwrap().add_downstream_send(&buffer);
                    //TODO replace with wake function
                    if self.clients[&tok].needs_to_stay_awake() {
                        self.inner.awake_io.push_back(tok);
                        self.clients.get_mut(&tok).map(|p| p.is_staying_awake());
                    }
                },

                Some(DistToWorker::ToClientC(..)) => {
                    unimplemented!()
                }
            }
        }
    }

    //#[inline(always)]
    fn handle_from_log(&mut self) {
        //if self.inner.waiting_for_log == 0 { return false }
        while let Some(log_work) = self.inner.from_log.try_recv() {
            //self.inner.waiting_for_log -= 1;
            self.inner.print_data.from_log(1);
            let (_wk, recv_token, src_addr) = log_work.get_associated_data();
            debug_assert_eq!(_wk, self.inner.worker_num);
            let next_hop = self.inner.next_hop(self.inner.worker_num, recv_token, src_addr);
            let continue_replication = next_hop
                .map(|(_, send_token)| send_token == DOWNSTREAM).unwrap_or(false);
            let (buffer, send_token) = servers2::handle_to_worker2(log_work, self.inner.worker_num, continue_replication,
                |to_send, _| {
                    match next_hop {
                        None => {
                            trace!("WORKER {:?} re dist", self.inner.worker_num);
                            self.redist_to_client(src_addr, to_send);
                            None
                        },
                        Some((worker_num, DOWNSTREAM)) => {
                            if worker_num != self.inner.worker_num {
                                self.redist_downstream(worker_num, src_addr, to_send);
                                return None
                            }

                            self.send_downsteam(recv_token, src_addr, to_send);
                            Some(DOWNSTREAM)
                        },

                        Some((worker_num, send_token)) => {
                            if worker_num != self.inner.worker_num {
                                self.redist_to_client(src_addr, to_send);
                                return None
                            }

                            self.send_to_client(send_token, src_addr, to_send);
                            Some(send_token)
                        }
                    }
                }
            );

            buffer.map(|b| self.clients.get_mut(&recv_token).map(|c| c.return_buffer(b)));
            if self.clients.get(&recv_token).map(|c| c.needs_to_stay_awake()).unwrap_or(false) {
                self.inner.awake_io.push_back(recv_token);
                self.clients.get_mut(&recv_token).map(|p| p.is_staying_awake());
            }
            if let Some((send_token, Some(true))) = send_token.map(
                |t| (t, self.clients.get(&t).map(|c| c.needs_to_stay_awake()))) {
                self.inner.awake_io.push_back(send_token);
                self.clients.get_mut(&send_token).map(|p| p.is_staying_awake());
            }
            continue
        }
    }// end handle_from_log

    fn send_downsteam(
        &mut self, _recv_token: mio::Token, src_addr: Ipv4SocketAddr, to_send: ToSend
    ) {
        match to_send {
            ToSend::Nothing => return,
            ToSend::OldReplication(to_replicate, storage_loc) => {
                let mut storage_log_bytes: [u8; 8] = [0; 8];
                LittleEndian::write_u64(&mut storage_log_bytes, storage_loc);
                self.clients.get_mut(&DOWNSTREAM).unwrap()
                    .add_downstream_send3(to_replicate, &storage_log_bytes, src_addr.bytes())
            },

            ToSend::Contents(to_send) => {
                let storage_log_bytes: [u8; 8] = [0; 8];
                self.clients.get_mut(&DOWNSTREAM).unwrap()
                    .add_downstream_contents3(to_send, &storage_log_bytes, src_addr.bytes())
            },

            ToSend::OldContents(to_send, storage_loc) => {
                let mut storage_log_bytes: [u8; 8] = [0; 8];
                LittleEndian::write_u64(&mut storage_log_bytes, storage_loc);
                self.clients.get_mut(&DOWNSTREAM).unwrap().
                    add_downstream_contents3(to_send, &storage_log_bytes, src_addr.bytes())
            }

            ToSend::Slice(to_send) => {
                let storage_loc_bytes: [u8; 8] = [0; 8];
                self.clients.get_mut(&DOWNSTREAM).unwrap()
                    .add_downstream_send3(to_send, &storage_loc_bytes, src_addr.bytes())
            }

            ToSend::StaticSlice(to_send) => {
                let storage_loc_bytes: [u8; 8] = [0; 8];
                self.clients.get_mut(&DOWNSTREAM).unwrap()
                    .add_downstream_send3(to_send, &storage_loc_bytes, src_addr.bytes())
            }

            ToSend::Read(_to_send) => unreachable!(),
        }
    }

    fn send_to_client(&mut self, send_token: mio::Token, _src_addr: Ipv4SocketAddr, to_send: ToSend) {
        match to_send {
            ToSend::Nothing => return,
            ToSend::OldReplication(..) => unreachable!(),

            ToSend::Contents(to_send) | ToSend::OldContents(to_send, _) =>
                self.clients.get_mut(&send_token).map(|c| c.add_downstream_contents(to_send)),

            ToSend::Slice(to_send) =>
                self.clients.get_mut(&send_token).map(|c| c.add_downstream_send(to_send)),

            ToSend::StaticSlice(to_send) =>
                self.clients.get_mut(&send_token).map(|c| c.add_downstream_send(to_send)),

            ToSend::Read(to_send) =>
                self.clients.get_mut(&send_token).map(|c| c.add_downstream_send(to_send)),
        };
    }

    fn redist_downstream(&mut self, worker: WorkerNum, src_addr: Ipv4SocketAddr, to_send: ToSend) {
        match to_send {
            ToSend::Nothing => return,
            ToSend::Read(..) => unreachable!(),

            ToSend::Slice(to_send) =>
                self.inner.to_dist.send(
                    WorkerToDist::DownstreamB(
                        worker, src_addr, to_send.to_vec().into_boxed_slice(), 0
                    )
                ).ok().unwrap(),

            ToSend::StaticSlice(to_send) =>
                self.inner.to_dist.send(
                    WorkerToDist::Downstream(
                        worker, src_addr, to_send, 0
                    )
                ).ok().unwrap(),

            ToSend::OldReplication(to_replicate, storage_loc) =>
                self.inner.to_dist.send(
                    WorkerToDist::Downstream(worker, src_addr, to_replicate, storage_loc)
                ).ok().unwrap(),

            ToSend::Contents(to_replicate) => {
                let mut vec = Vec::with_capacity(to_replicate.len());
                to_replicate.fill_vec(&mut vec);
                let to_replicate = vec.into_boxed_slice();
                self.inner.to_dist.send(
                    WorkerToDist::DownstreamB(worker, src_addr, to_replicate, 0)
                ).ok().unwrap()
            },

            ToSend::OldContents(to_replicate, storage_loc) => {
                let mut vec = Vec::with_capacity(to_replicate.len());
                to_replicate.fill_vec(&mut vec);
                let to_replicate = vec.into_boxed_slice();
                self.inner.to_dist.send(
                    WorkerToDist::DownstreamB(worker, src_addr, to_replicate, storage_loc)
                ).ok().unwrap()
            },
        }
    }

    fn redist_to_client(&mut self, src_addr: Ipv4SocketAddr, to_send: ToSend) {
        match to_send {
            ToSend::Nothing => return,
            ToSend::OldReplication(..) => unreachable!(),

            ToSend::Read(to_send) =>
                self.inner.to_dist.send(WorkerToDist::ToClient(src_addr, to_send))
                    .ok().unwrap(),

            ToSend::Slice(to_send) =>
                self.inner.to_dist.send(
                    WorkerToDist::ToClientB(src_addr, to_send.to_vec().into_boxed_slice())
                ).ok().unwrap(),

            ToSend::StaticSlice(to_send) =>
                self.inner.to_dist.send(
                    WorkerToDist::ToClient(src_addr, to_send)
                ).ok().unwrap(),

            ToSend::Contents(to_send) | ToSend::OldContents(to_send, _) => {
                let mut vec = Vec::with_capacity(to_send.len());
                to_send.fill_vec(&mut vec);
                let to_send = vec.into_boxed_slice();
                self.inner.to_dist.send(
                    WorkerToDist::ToClientB(src_addr, to_send)
                ).ok().unwrap()
            },
        }
    }
}

impl WorkerInner {

    fn handle_burst(
        &mut self,
        token: mio::Token,
        socket_state: &mut PerSocket
    ) -> Result<(), ()> {
        use super::per_socket::PerSocket::*;
        let (send, recv) = match socket_state {
            &mut Upstream {..} => (false, true),
            &mut Downstream {..} => (true, false),
            &mut Client {..} => (true, true),
        };
        socket_state.wake();

        if send {
            //FIXME need to disinguish btwn to client and to upstream
            self.send_burst(token, socket_state)?
        }
        if recv {
            //FIXME need to disinguish btwn from client and from upstream
            for _ in 0..5 { self.recv_packet(token, socket_state)? }
        }

        if socket_state.needs_to_stay_awake() {
            self.awake_io.push_back(token);
            socket_state.is_staying_awake();
        }
        Ok(())
    }

    //TODO these functions should return Result so we can remove from map
    fn send_burst(
        &mut self,
        _token: mio::Token,
        socket_state: &mut PerSocket
    ) -> Result<(), ()> {
        match socket_state.send_burst() {
            //TODO replace with wake function
            Ok(true) => { self.print_data.finished_send(1); } //TODO self.awake_io.push_back(token),
            Ok(false) => {},
            //FIXME remove from map, log
            Err(_e) => {
                //error!("send error {:?} @ {:?}", _e, _token);
                let _ = self.poll.deregister(socket_state.stream());
                return Err(())
            }
        }
        Ok(())
    }

    fn recv_packet(
        &mut self,
        token: mio::Token,
        socket_state: &mut PerSocket
    ) -> Result<(), ()> {
        //TODO let socket_kind = socket_state.kind();
        let packet = socket_state.recv_packet();
        match packet {
            //FIXME remove from map, log
            RecvPacket::Err => {
                // error!("recv error @ {:?}", token);
                let _ = self.poll.deregister(socket_state.stream());
                return Err(())
            },
            RecvPacket::Pending => {},
            RecvPacket::FromClient(packet, src_addr) => {
                // trace!("WORKER {} finished recv from client.", self.worker_num);
                self.print_data.finished_recv_from_client(1);
                let worker = self.worker_num;
                self.send_to_log(packet, worker, token, src_addr, socket_state);
                //self.awake_io.push_back(token)
            }
            RecvPacket::FromUpstream(packet, src_addr, storage_loc) => {
                // trace!("WORKER {} finished recv from up.", self.worker_num);
                self.print_data.finished_recv_from_up(1);
                //let (worker, work_tok) = self.next_hop(token, src_addr, socket_kind);
                let worker = self.worker_num;
                self.send_replication_to_log(packet, storage_loc, worker, token, src_addr);
                //self.awake_io.push_back(token)
            }
        }
        Ok(())
    }

    fn next_hop(
        &self,
        worker_num: WorkerNum,
        token: mio::Token,
        src_addr: Ipv4SocketAddr,
    ) -> Option<(WorkerNum, mio::Token)> {
        //TODO specialize based on read/write sockets?
        if src_addr == Ipv4SocketAddr::nil() { return Some((worker_num, token)) }

        if self.is_unreplicated {
            // trace!("WORKER {} is unreplicated.", self.worker_num);
            return Some((self.worker_num, token))
        }
        else if self.downstream_workers == 0 { //if self.is_end_of_chain
            // trace!("WORKER {} is end of chain.", self.worker_num);
            return match self.worker_and_token_for_addr(src_addr) {
                Some(worker_and_token) => {
                    // trace!("WORKER {} send to_client to {:?}",
                        // self.worker_num, worker_and_token);
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
        mut buffer: Buffer,
        worker_num: usize,
        token: mio::Token,
        src_addr: Ipv4SocketAddr,
        socket_state: &mut PerSocket,
    ) {
        // trace!("WORKER {} send to log", self.worker_num);
        let (k, f) = {
            let c = buffer.contents();
            (c.kind().clone(), c.flag().clone())
        };
        let kind = k.layout();
        let storage = match kind {
            EntryLayout::Read => {
                worker_thread::handle_read(&self.log_reader, &buffer, worker_num, |to_send| {
                    match to_send {
                        Ok(to_send) => socket_state.add_downstream_send(to_send),
                        Err(to_send) => socket_state.add_downstream_contents(to_send),
                    }
                });
                socket_state.return_buffer(buffer);
                return
            },

            EntryLayout::Multiput | EntryLayout::Sentinel => {
                let (size, senti_size, num_locs, has_senti, is_unlock) = {
                    let e = buffer.contents();
                    let locs = e.locs();
                    let num_locs = locs.len();
                    //FIXME
                    let has_senti = locs.contains(&OrderIndex(0.into(), 0.into()))
                        || !e.flag().contains(EntryFlag::TakeLock);
                    (e.len(), e.sentinel_entry_size(), num_locs, has_senti, e.flag().contains(EntryFlag::Unlock))
                };
                if is_unlock {
                    Troption::None
                } else if f.contains(EntryFlag::NewMultiPut) || !f.contains(EntryFlag::TakeLock) {
                    let senti_size = if has_senti { Some(senti_size) } else { None };
                    let mut storage = SkeensMultiStorage::new(num_locs, size, senti_size);
                    if !f.contains(EntryFlag::TakeLock) {
                        storage.fill_from(&mut buffer)
                    }
                    Troption::Left(storage)
                } else {
                    let m = RcSlice::with_len(size);
                    let s = RcSlice::with_len(senti_size);
                    Troption::Right(Box::new((m, s)))
                }
            },

            EntryLayout::GC => {
                //TODO send downstream first?
                Troption::None
            },
            _ => Troption::None,
        };
        self.print_data.new_to_log(1);
        self.print_data.to_log(1);
        //self.waiting_for_log += 1;
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
        use packets::EntryKind;
        trace!("WORKER {} send replica to log", self.worker_num);
        let kind = buffer.contents().kind();
        let to_send = match kind {
            EntryKind::Data => {
                trace!("WORKER {} replicate Data", self.worker_num);
                ToReplicate::Data(buffer, storage_addr)
            },
            EntryKind::Lock => {
                trace!("WORKER {} replicate Unlock", self.worker_num);
                ToReplicate::UnLock(buffer)
            },
            //TODO
            EntryKind::Multiput | EntryKind::Sentinel => {
                trace!("WORKER {} replicate Multi/Senti", self.worker_num);
                let (size, senti_size) = {
                    let e = buffer.contents();
                    (e.len(), e.sentinel_entry_size())
                };
                let storage = {
                    let m = RcSlice::with_len(size);
                    let s = RcSlice::with_len(senti_size);
                    Box::new((m, s))
                };
                ToReplicate::Multi(buffer, storage)
            },
            EntryKind::SingleToReplica => {
                trace!("WORKER {} replicate single skeens 1", self.worker_num);
                ToReplicate::SingleSkeens1(buffer, storage_addr)
            }
            EntryKind::MultiputToReplica => {
                trace!("WORKER {} replicate multi skeens 1", self.worker_num);
                let (size, senti_size, num_locs, has_senti) = {
                    let e = buffer.contents();
                    let locs = e.locs();
                    let num_locs = locs.len();
                    //FIXME
                    let has_senti = locs.contains(&OrderIndex(0.into(), 0.into()));
                    (e.non_replicated_len(), e.sentinel_entry_size(), num_locs, has_senti)
                };
                let senti_size = if has_senti { Some(senti_size) } else { None };
                let storage = SkeensMultiStorage::new(num_locs, size, senti_size);
                ToReplicate::Skeens1(buffer, storage)
            }
            EntryKind::SentinelToReplica => {
                trace!("WORKER {} replicate senti skeens 1", self.worker_num);
                let (size, senti_size, num_locs, has_senti) = {
                    let e = buffer.contents();
                    let locs = e.locs();
                    let num_locs = locs.len();
                    //FIXME
                    let has_senti = locs.contains(&OrderIndex(0.into(), 0.into()));
                    (e.non_replicated_len(), e.sentinel_entry_size(), num_locs, has_senti)
                };
                let senti_size = if has_senti { Some(senti_size) } else { None };
                let storage = SkeensMultiStorage::new(num_locs, size, senti_size);
                ToReplicate::Skeens1(buffer, storage)
            }
            EntryKind::Skeens2ToReplica => {
                trace!("WORKER {} replicate skeens 2", self.worker_num);
                ToReplicate::Skeens2(buffer)
            }
            EntryKind::GC => {
                //TODO send downstream first?
                ToReplicate::GC(buffer)
            },
            _ => unreachable!(),
        };
        self.print_data.rep_to_log(1);
        self.print_data.to_log(1);
        //self.waiting_for_log += 1;
        let to_send = ToLog::Replication(to_send, (worker_num, token, src_addr));
        self.to_log.send(to_send).unwrap();
    }

}

impl DistributeToWorkers<(usize, mio::Token, Ipv4SocketAddr)>
for Vec<spsc::Sender<ToWorker<(usize, mio::Token, Ipv4SocketAddr)>>> {
    #[inline(always)]
    fn send_to_worker(&mut self, msg: ToWorker<(usize, mio::Token, Ipv4SocketAddr)>) {
        let (which_queue, token, _) = msg.get_associated_data();
        // trace!("SERVER   sending to worker {} {:?} ", which_queue, token);
        self[which_queue].send(msg)
    }
}
