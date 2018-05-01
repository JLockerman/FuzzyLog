#![allow(non_snake_case)]

use std::collections::VecDeque;
use std::sync::mpsc;

use ::{
    spsc, worker_thread, ToReplicate, ToWorker,
    DistributeToWorkers, Troption, Recovery, SkeensMultiStorage,
    ToSend, ChainReader,
};
use shared_slice::RcSlice;
use hash::HashMap;
use socket_addr::Ipv4SocketAddr;

use packets::{EntryLayout, OrderIndex, EntryFlag};

use mio;
use mio::tcp::*;

use buffer::Buffer;

use byteorder::{ByteOrder, LittleEndian};

//use super::{DistToWorker, WorkerToDist, ToLog, WorkerNum};
use super::*;
use super::per_socket::{PerSocket, PerStream};

use reactor::*;


//FIXME we should use something more accurate than &static [u8],
pub enum WorkerToDist {
    #[allow(dead_code)]
    FenceClient(Buffer),
    ClientFenced(Buffer),
}

#[allow(dead_code)]
pub enum DistToWorker {
    NewClient(mio::Token, TcpStream, Option<(mio::Token, TcpStream)>, Ipv4SocketAddr),
    FenceOff(mio::Token, Buffer),
    FinishedFence(mio::Token, Buffer),
}

pub enum ToLog<T> {
    //TODO test different layouts.
    New(Buffer, Troption<SkeensMultiStorage, Box<(RcSlice, RcSlice)>>, T),
    Replication(ToReplicate, T),

    #[allow(dead_code)]
    Recovery(Recovery, T),
}

pub struct Worker {
    reactor: Reactor<PerStream, WorkerInner>
}

#[allow(dead_code)]
pub struct WorkerInner {
    from_dist: spsc::Receiver<DistToWorker>,
    to_dist: mio::channel::Sender<WorkerToDist>,
    from_log: spsc::Receiver<ToWorker<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
    to_log: mpsc::Sender<ToLog<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
    log_reader: ChainReader<(WorkerNum, mio::Token, Ipv4SocketAddr)>,
    downstream_for_addr: HashMap<Ipv4SocketAddr, mio::Token>,
    worker_num: WorkerNum,
    num_workers: WorkerNum,
    poll: mio::Poll,
    is_unreplicated: bool,
    has_upstream: bool,
    has_downstream: bool,
    //waiting_for_log: usize,

    remove_backpressure: VecDeque<mio::Token>,


    next_token: usize,

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

impl Wakeable for WorkerInner {
    fn init(&mut self, _: mio::Token, poll: &mut mio::Poll) {
        poll.register(
            &self.from_dist,
            FROM_DIST,
            mio::Ready::readable(),
            mio::PollOpt::level() //TODO or edge?
        ).expect("cannot pol from dist on worker");

        poll.register(
            &self.from_log,
            FROM_LOG,
            mio::Ready::readable(),
            mio::PollOpt::level() //TODO or edge?
        ).expect("cannot pol from log on worker");
    }

    fn needs_to_mark_as_staying_awake(&mut self, _: mio::Token) -> bool { false }
    fn mark_as_staying_awake(&mut self, _: mio::Token) {}
    fn is_marked_as_staying_awake(&self, _: mio::Token) -> bool { true }
}

impl Handler<IoState<PerStream>> for WorkerInner {
    type Error = ();

    fn on_event(&mut self, inner: &mut IoState<PerStream>, token: mio::Token, _: mio::Event)
    -> Result<(), Self::Error> {
        self.on_poll(inner, token)
    }

    fn on_poll(&mut self, inner: &mut IoState<PerStream>, token: mio::Token)
    -> Result<(), Self::Error> {
        match token {
            FROM_LOG => self.handle_from_log(inner),
            FROM_DIST => self.handle_from_dist(inner),
            _ => {},
        }
        Ok(())
    }

    fn on_error(&mut self, _: Self::Error, _: &mut mio::Poll) -> ShouldRemove {
        false
    }

    fn after_work(&mut self, inner: &mut IoState<PerStream>) {
        for token in self.remove_backpressure.drain(..) {
            inner.mutate(token, |s| s.mark_as_not_backpressured());
        }
    }
}

impl Worker {

    pub fn new(
        from_dist: spsc::Receiver<DistToWorker>,
        to_dist: mio::channel::Sender<WorkerToDist>,
        from_log: spsc::Receiver<ToWorker<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
        to_log: mpsc::Sender<ToLog<(WorkerNum, mio::Token, Ipv4SocketAddr)>>,
        log_reader: ChainReader<(WorkerNum, mio::Token, Ipv4SocketAddr)>,
        num_workers: usize,
        is_unreplicated: bool,
        has_upstream: bool,
        has_downstream: bool,
        worker_num: WorkerNum,
    ) -> Self {
        let poll = mio::Poll::new().unwrap();
        let inner = WorkerInner {
            from_dist,
            to_dist,
            from_log,
            to_log,
            log_reader,
            downstream_for_addr: HashMap::default(),
            worker_num,
            num_workers,
            poll,
            is_unreplicated,
            has_upstream,
            has_downstream,
            //waiting_for_log: usize,

            next_token: FIRST_CLIENT_TOKEN.0,

            remove_backpressure: Default::default(),

            print_data: Default::default(),
        };
        let reactor = Reactor::with_inner(0.into(), inner).unwrap();
        Self { reactor }
    }

    pub fn run(mut self) -> ! {
        self.reactor.run().unwrap();
        panic!("should not be")
    }// end fn run
}

impl WorkerInner {

    fn handle_from_log(&mut self, streams: &mut IoState<PerStream>) {
        while let Some(log_work) = self.from_log.try_recv() {
            self.print_data.from_log(1);
            let (_wk, recv_token, src_addr) = log_work.get_associated_data();
            debug_assert_eq!(_wk, self.worker_num);
            let continue_replication = self.has_downstream;
            let send_token = self.downstream_for_addr.get(&src_addr)
                .cloned()
                .unwrap_or(recv_token);
            // trace!("{} from log {} {:?} => {:?}", self.worker_num, src_addr, recv_token, send_token);
            let (_buffer, needs_backpressure) =
                ::handle_to_worker2(log_work, self.worker_num, continue_replication,
                |to_send, head_ack, _u| {
                    if head_ack {
                        unimplemented!()
                    }
                    if continue_replication {
                        // trace!("WORKER {} replicate {:?}", self.inner.worker_num, to_send);
                        self.send_downsteam(streams, send_token, src_addr, to_send)
                    } else {
                        // trace!("WORKER {} ack {:?}", self.inner.worker_num, to_send);
                        self.send_to_client(streams, send_token, src_addr, to_send)
                    }
                }
            );

            streams.wake(send_token);
            streams.wake(recv_token);
            if needs_backpressure {
                streams.mutate(recv_token, |s| s.mark_as_backpressured());
            }
            //FIXME
            // buffer.map(|b| self.clients.get_mut(&recv_token).map(|c| c.return_buffer(b)));
            continue
        }
    }

    fn send_downsteam(
        &mut self,
        streams: &mut IoState<PerStream>,
        send_token: mio::Token,
        src_addr: Ipv4SocketAddr,
        to_send: ToSend,
    ) -> bool {
        match to_send {
            ToSend::Nothing => return false,
            ToSend::OldReplication(to_replicate, storage_loc) => {
                let mut storage_log_bytes: [u8; 8] = [0; 8];
                LittleEndian::write_u64(&mut storage_log_bytes, storage_loc);
                streams.mutate(send_token, |s| {
                    s.add_writes(&[to_replicate, &storage_log_bytes, src_addr.bytes()]);
                    s.is_overflowing() && !s.is_backpressured()
                })
            },

            ToSend::Contents(to_send) => {
                let storage_log_bytes: [u8; 8] = [0; 8];
                streams.mutate(send_token, |s| {
                    s.add_contents(to_send, &[&storage_log_bytes, src_addr.bytes()]);
                    s.is_overflowing() && !s.is_backpressured()
                })

            },

            ToSend::OldContents(to_send, storage_loc) => {
                let mut storage_log_bytes: [u8; 8] = [0; 8];
                LittleEndian::write_u64(&mut storage_log_bytes, storage_loc);
                streams.mutate(send_token, |s| {
                    s.add_contents(to_send, &[&storage_log_bytes, src_addr.bytes()]);
                    s.is_overflowing() && !s.is_backpressured()
                })
            }

            ToSend::Slice(to_send) => {
                let storage_loc_bytes: [u8; 8] = [0; 8];
                streams.mutate(send_token, |s| {
                    s.add_writes(&[to_send, &storage_loc_bytes, src_addr.bytes()]);
                    s.is_overflowing() && !s.is_backpressured()
                })
            }

            ToSend::StaticSlice(to_send) => {
                let storage_loc_bytes: [u8; 8] = [0; 8];
                streams.mutate(send_token, |s| {
                    s.add_writes(&[to_send, &storage_loc_bytes, src_addr.bytes()]);
                    s.is_overflowing() && !s.is_backpressured()
                })
            }

            ToSend::Read(_to_send) => unreachable!(),
        }.expect("downstream dead")
    }

    fn send_to_client(
        &mut self,
        streams: &mut IoState<PerStream>,
        send_token: mio::Token,
        _src_addr: Ipv4SocketAddr,
        to_send: ToSend,
    )  -> bool {
        let needs_backpressure = match to_send {
            ToSend::Nothing => return false,
            ToSend::OldReplication(..) => unreachable!(),

            ToSend::Contents(to_send) | ToSend::OldContents(to_send, _) =>
                streams.mutate(send_token, |c| {
                    c.add_contents(to_send, &[]);
                    c.is_overflowing() && !c.is_backpressured()
                }),

            ToSend::Slice(to_send) =>
                streams.mutate(send_token, |c| {
                    c.add_writes(&[to_send]);
                    c.is_overflowing() && !c.is_backpressured()
                }),

            ToSend::StaticSlice(to_send) =>
                streams.mutate(send_token, |c| {
                    c.add_writes(&[to_send]);
                    c.is_overflowing() && !c.is_backpressured()
                }),

            ToSend::Read(to_send) =>
                streams.mutate(send_token, |c| {
                    c.add_writes(&[to_send]);
                    c.is_overflowing() && !c.is_backpressured()
                }),
        };
        // needs_backpressure.unwrap_or_else(|| false)
        match needs_backpressure {
            Some(nb) => nb,
            None => {
                error!("client {:?} dead", _src_addr);
                false
            },
        }
    }

    fn handle_from_dist(&mut self, streams: &mut IoState<PerStream>) {
        let next_token = |start_token: &mut usize| { let n = *start_token; *start_token += 1; n };
        loop {
            match self.from_dist.try_recv() {
                None => return,

                Some(DistToWorker::NewClient(tok, upstream, downstream, client_addr)) => {
                    debug_assert!(tok.0 >= FIRST_CLIENT_TOKEN.0);
                    self.print_data.from_dist_N(1);
                    let upstream_token = next_token(&mut self.next_token).into();
                    let downstream_token = downstream.map(|(_, stream)|{
                        trace!("Worker {} {} got downstream {:?} => {:?}",
                            self.worker_num, self.has_downstream,
                            stream.local_addr(), stream.peer_addr());
                        let token = next_token(&mut self.next_token).into();
                        let mut stream = per_socket::new_stream(
                            stream, token, false, Some(upstream_token)
                        );
                        stream.ignore_backpressure();
                        let _ = streams.add_stream(token, stream);
                        token
                    });
                    trace!("Worker {} {} got upstream {:?} => {:?}",
                            self.worker_num, self.has_downstream, upstream.local_addr(), upstream.peer_addr());

                    let mut stream = per_socket::new_stream(
                        upstream, upstream_token, self.has_upstream, None
                    );
                    stream.ignore_backpressure();
                    let _ = streams.add_stream(upstream_token, stream);

                    trace!("WORKER {} recv from dist {:?}.",
                        self.worker_num, (tok, client_addr));
                    let downstream_token = downstream_token.unwrap_or(upstream_token);
                    self.downstream_for_addr.insert(client_addr, downstream_token);
                },

                Some(DistToWorker::FenceOff(_token, buffer)) => {
                    //TODO we're not actually going to fence off clients in this branch
                    //     but here's where we'd do it
                    self.to_dist.send(WorkerToDist::ClientFenced(buffer)).unwrap();
                },

                Some(DistToWorker::FinishedFence(token, buffer)) => {
                    streams.mutate(token, move |s| {
                        s.add_writes(&[buffer.entry_slice()]);
                        s.return_buffer(buffer);
                    });
                    streams.wake(token);
                },

            }
        }
    }

    pub fn handle_message(
        &mut self,
        socket_state: &mut TcpWriter,
        token: mio::Token,
        msg: Buffer,
        addr: Ipv4SocketAddr,
        storage_loc: Option<u64>,
    ) -> Result<(), ()> {
        match storage_loc {
            Some(storage_loc) =>
                self.send_replication_to_log(token, msg, storage_loc, addr),
            None => self.send_to_log(token, msg, addr, socket_state),
        };
        Ok(())
    }

    fn send_to_log(
        &mut self,
        token: mio::Token,
        mut buffer: Buffer,
        src_addr: Ipv4SocketAddr,
        socket_state: &mut TcpWriter,
    ) {
        let worker_num = self.worker_num;
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
                        Ok(to_send) => socket_state.add_bytes_to_write(&[to_send]),
                        Err(to_send) => per_socket::add_contents(socket_state, to_send),
                    }
                });
                // socket_state.return_buffer(buffer);
                return
            },

            EntryLayout::Multiput | EntryLayout::Sentinel => {
                let (size, senti_size, num_locs, has_senti, is_unlock, is_direct_write) = {
                    let e = buffer.contents();
                    let locs = e.locs();
                    let num_locs = locs.len();
                    //FIXME
                    let has_senti = locs.contains(&OrderIndex(0.into(), 0.into()))
                        || !e.flag().contains(EntryFlag::TakeLock);
                    (
                        e.len(),
                        e.sentinel_entry_size(),
                        num_locs,
                        has_senti,
                        e.flag().contains(EntryFlag::Unlock),
                        e.flag().contains(EntryFlag::DirectWrite),
                    )
                };
                if is_unlock {
                    Troption::None
                } else if is_direct_write {
                    let storage = {
                        let m = RcSlice::with_len(size);
                        let s = RcSlice::with_len(senti_size);
                        Box::new((m, s))
                    };
                    let t = (worker_num, token, src_addr);
                    let to_send = ToLog::Replication(ToReplicate::Multi(buffer, storage), t);
                    self.print_data.to_log(1);
                    //self.waiting_for_log += 1;
                    return self.to_log.send(to_send).expect("log gone")
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

            EntryLayout::Snapshot => {
                let (size, num_locs, is_unlock) = {
                    let e = buffer.contents();
                    let locs = e.locs();
                    (e.len(), locs.len(), e.flag().contains(EntryFlag::Unlock))
                };
                if is_unlock {
                    Troption::None
                } else {
                    let mut storage = SkeensMultiStorage::new(num_locs, size, None);
                    storage.fill_from(&mut buffer);
                    Troption::Left(storage)
                }

            }

            EntryLayout::GC => {
                //TODO send downstream first?
                Troption::None
            },
            EntryLayout::Data if f.contains(EntryFlag::DirectWrite) => {
                let t = (worker_num, token, src_addr);
                let tr = ToReplicate::Data(buffer, ::std::u64::MAX);
                let to_send = ToLog::Replication(tr, t);
                self.print_data.to_log(1);
                //self.waiting_for_log += 1;
                return self.to_log.send(to_send).expect("log gone")
            }
            _ => Troption::None,
        };
        self.print_data.new_to_log(1);
        self.print_data.to_log(1);
        //self.waiting_for_log += 1;
        let to_send = ToLog::New(buffer, storage, (worker_num, token, src_addr));
        self.to_log.send(to_send).expect("log gone")
    }

    fn send_replication_to_log(
        &mut self,
        token: mio::Token,
        buffer: Buffer,
        storage_addr: u64,
        src_addr: Ipv4SocketAddr,
    ) {
        use packets::EntryKind;
        let worker_num = self.worker_num;
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
                    let has_senti = locs.contains(&OrderIndex(0.into(), 0.into()))
                        || !e.flag().contains(EntryFlag::TakeLock);
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
                    let has_senti = locs.contains(&OrderIndex(0.into(), 0.into()))
                        || !e.flag().contains(EntryFlag::TakeLock);
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
            EntryKind::SnapshotToReplica => {
                let (size, num_locs) = {
                    let e = buffer.contents();
                    let locs = e.locs();
                    (e.len(), locs.len())
                };
                let mut storage = SkeensMultiStorage::new(num_locs, size, None);
                let mut buffer = buffer;
                storage.fill_from(&mut buffer);
                ToReplicate::SnapshotSkeens1(buffer, storage)
            }
            EntryKind::GC => {
                //TODO send downstream first?
                ToReplicate::GC(buffer)
            },
            e => unreachable!("{:?}", e),
        };
        self.print_data.rep_to_log(1);
        self.print_data.to_log(1);
        //self.waiting_for_log += 1;
        let to_send = ToLog::Replication(to_send, (worker_num, token, src_addr));
        self.to_log.send(to_send).expect("log gone 2");
    }

    pub fn end_backpressure(&mut self, token: mio::Token) {
        self.remove_backpressure.push_back(token)
    }
}

impl DistributeToWorkers<(usize, mio::Token, Ipv4SocketAddr)>
for Vec<spsc::Sender<ToWorker<(usize, mio::Token, Ipv4SocketAddr)>>> {
    #[inline(always)]
    fn send_to_worker(&mut self, msg: ToWorker<(usize, mio::Token, Ipv4SocketAddr)>) {
        let (which_queue, _token, _) = msg.get_associated_data();
        // trace!("SERVER   sending to worker {} {:?} ", which_queue, token);
        self[which_queue].send(msg)
    }
}
