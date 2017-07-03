
use std::borrow::Borrow;
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::net::SocketAddr;
use std::thread;
use std::sync::{mpsc, Arc, Mutex};

use mio;

use hash::HashMap;

use async::fuzzy_log::{
    self,
    Message,
    ThreadLog,
    FinshedReadQueue,
    FinshedReadRecv,
    FinshedWriteQueue,
    FinshedWriteRecv,
};
use async::fuzzy_log::FromClient::*;
use packets::{
    entry,
    EntryContents,
    OrderIndex,
    Uuid,
    Storeable,
    UnStoreable,
    order,
    bytes_as_entry,
    data_to_slice,
    slice_to_data,
    EntryFlag,
};

pub struct LogHandle<V: ?Sized> {
    _pd: PhantomData<Box<V>>,
    num_snapshots: usize,
    num_async_writes: usize,
    to_log: mpsc::Sender<Message>,
    ready_reads: FinshedReadRecv,
    finished_writes: FinshedWriteRecv,
    //TODO finished_writes: ..
    curr_entry: Vec<u8>,
    last_seen_entries: HashMap<order, entry>,
    num_errors: u64,
}

impl<V: ?Sized> Drop for LogHandle<V> {
    fn drop(&mut self) {
        let _ = self.to_log.send(Message::FromClient(Shutdown));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GetRes {
    NothingReady,
    Done,
    IoErr(io::ErrorKind, usize),
    AlreadyGCd(order, entry),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TryWaitRes {
    NothingReady,
    IoErr(io::ErrorKind, usize),
}

impl<V> LogHandle<[V]>
where V: Storeable {

    //FIXME this should be unnecessary
    pub fn spawn_tcp_log2<S, C>(
        lock_server: SocketAddr,
        chain_servers: S,
        interesting_chains: C
    )-> Self
    where S: IntoIterator<Item=SocketAddr>,
          C: IntoIterator<Item=order>,
    {
        //TODO this currently leaks the other threads, maybe store the handles so they get
        //     collected when this dies?
        use std::thread;
        use std::sync::{Arc, Mutex};

        let to_store_m = Arc::new(Mutex::new(None));
        let tsm = to_store_m.clone();
        let (to_log, from_outside) = mpsc::channel();
        let client = to_log.clone();
        let (ready_reads_s, ready_reads_r) = mpsc::channel();
        let (finished_writes_s, finished_writes_r) = mpsc::channel();
        let chain_servers = chain_servers.into_iter().collect();
        thread::spawn(move || {
            run_store(lock_server, chain_servers, client, tsm)
        });

        let to_store;
        loop {
            let ts = mem::replace(&mut *to_store_m.lock().unwrap(), None);
            if let Some(s) = ts {
                to_store = s;
                break
            }
        }
        let interesting_chains = interesting_chains.into_iter().collect();
        thread::spawn(move || {
            run_log(to_store, from_outside, ready_reads_s, finished_writes_s,
                interesting_chains)
        });

        #[inline(never)]
        fn run_store(
            lock_server: SocketAddr,
            chain_servers: Vec<SocketAddr>,
            client: mpsc::Sender<Message>,
            tsm: Arc<Mutex<Option<mio::channel::Sender<Vec<u8>>>>>
        ) {
            let mut event_loop = mio::Poll::new().unwrap();
            let (store, to_store) = ::async::store::AsyncTcpStore::tcp(
                lock_server,
                chain_servers,
                client, &mut event_loop
            ).expect("");
            *tsm.lock().unwrap() = Some(to_store);
            store.run(event_loop);
        }

        #[inline(never)]
        fn run_log(
            to_store: mio::channel::Sender<Vec<u8>>,
            from_outside: mpsc::Receiver<Message>,
            ready_reads_s: FinshedReadQueue,
            finished_writes_s: FinshedWriteQueue,
            interesting_chains: Vec<order>,
        ) {
            let log = ThreadLog::new(
                to_store, from_outside,
                ready_reads_s,
                finished_writes_s,
                interesting_chains
            );
            log.run()
        }

        LogHandle::new(to_log, ready_reads_r, finished_writes_r)
    }
}

#[derive(Debug)]
pub struct LogBuilder<V: ?Sized> {
    servers: Servers,
    chains: Vec<order>,
    reads_my_writes: bool,
    _pd: PhantomData<Box<V>>,
}

#[derive(Debug)]
enum Servers {
    Unreplicated(Vec<SocketAddr>),
    Replicated(Vec<(SocketAddr, SocketAddr)>),
    OldUnreplicated(SocketAddr, Vec<SocketAddr>),
    OldReplicated(SocketAddr, Vec<(SocketAddr, SocketAddr)>)
}

impl<V: ?Sized> LogBuilder<V>
where V: Storeable {
    pub fn chains<C, O>(self, chains: C) -> Self
    where
        C: IntoIterator<Item=O>,
        O: Borrow<order>, {
        let chains = chains.into_iter().map(|o| o.borrow().clone()).collect();
        LogBuilder{ chains: chains, .. self}
    }

    pub fn reads_my_writes(self) -> Self {
        LogBuilder{ reads_my_writes: true, .. self}
    }

    pub fn build(self) -> LogHandle<V> {
        let LogBuilder {
            servers, chains, reads_my_writes, _pd,
        } = self;

        let make_store = |client| {
            let to_store_m = Arc::new(Mutex::new(None));
            let tsm = to_store_m.clone();
            let _ = thread::spawn(move || {
                let mut event_loop = mio::Poll::new().unwrap();
                match servers {
                    Servers::Unreplicated(servers) => {
                        let (mut store, to_store) =
                            ::async::store::AsyncTcpStore::new_tcp(
                                servers.into_iter(),
                                client,
                                &mut event_loop
                            ).expect("could not start store.");
                        *tsm.lock().unwrap() = Some(to_store);
                        store.set_reads_my_writes(reads_my_writes);
                        store.run(event_loop);
                    },
                    Servers::Replicated(servers) => {
                        let (mut store, to_store) =
                            ::async::store::AsyncTcpStore::replicated_new_tcp(
                                servers.into_iter(),
                                client,
                                &mut event_loop
                            ).expect("could not start store.");
                        *tsm.lock().unwrap() = Some(to_store);
                        store.set_reads_my_writes(reads_my_writes);
                        store.run(event_loop);
                    },
                    _ => unimplemented!(),
                }
            });
            let to_store;
            loop {
                let ts = mem::replace(&mut *to_store_m.lock().unwrap(), None);
                if let Some(s) = ts {
                    to_store = s;
                    break
                }
            }
            to_store
        };

        LogHandle::with_store(chains, make_store)
    }
}

//TODO I kinda get the feeling that this should send writes directly to the store without
//     the AsyncLog getting in the middle
//     Also, I think if I can send associated data with the wites I could do multiplexing
//     over different writers very easily
impl<V: ?Sized> LogHandle<V>
where V: Storeable {

    pub fn unreplicated_with_servers<S, A>(servers: S) -> LogBuilder<V>
    where
        S: IntoIterator<Item=A>,
        A: Borrow<SocketAddr>, {
        let servers = servers.into_iter().map(|s| s.borrow().clone()).collect();
        LogBuilder {
            servers: Servers::Unreplicated(servers),
            chains: vec![],
            reads_my_writes: false,
            _pd: PhantomData,
        }
    }

    pub fn with_store<C, F>(
        interesting_chains: C,
        store_builder: F,
    ) -> Self
    where C: IntoIterator<Item=order>,
          F: FnOnce(mpsc::Sender<Message>) -> mio::channel::Sender<Vec<u8>> {
        let (to_log, from_outside) = mpsc::channel();
        let to_store = store_builder(to_log.clone());
        let (ready_reads_s, ready_reads_r) = mpsc::channel();
        let interesting_chains: Vec<_> = interesting_chains
            .into_iter()
            .inspect(|c| assert!(c != &0.into(), "Don't register interest in color 0."))
            .collect();
        let (finished_writes_s, finished_writes_r) = mpsc::channel();
        ::std::thread::spawn(move || {
            ThreadLog::new(
                to_store, from_outside,
                ready_reads_s,
                finished_writes_s,
                interesting_chains
            ).run()
        });

        LogHandle::new(to_log, ready_reads_r, finished_writes_r)
    }

    pub fn tcp_log_with_replication<S, C>(
        lock_server: Option<SocketAddr>,
        chain_servers: S,
        interesting_chains: C
    ) -> Self
    where S: IntoIterator<Item=(SocketAddr, SocketAddr)>,
          C: IntoIterator<Item=order>,
    {
        use std::thread;
        use std::sync::{Arc, Mutex};

        let chain_servers: Vec<_> = chain_servers.into_iter().collect();

        let make_store = |client| {
            let to_store_m = Arc::new(Mutex::new(None));
            let tsm = to_store_m.clone();
            thread::spawn(move || {
                let mut event_loop = mio::Poll::new().unwrap();
                let (store, to_store) = ::async::store::AsyncTcpStore::replicated_tcp(
                    lock_server,
                    chain_servers.into_iter(),
                    client,
                    &mut event_loop).expect("could not start store.");
                    *tsm.lock().unwrap() = Some(to_store);
                    store.run(event_loop);
                });
            let to_store;
            loop {
                let ts = mem::replace(&mut *to_store_m.lock().unwrap(), None);
                if let Some(s) = ts {
                    to_store = s;
                    break
                }
            }
            to_store
        };

        LogHandle::with_store(interesting_chains, make_store)
    }

    pub fn new_tcp_log<S, C>(
        chain_servers: S,
        interesting_chains: C
    ) -> Self
    where S: IntoIterator<Item=SocketAddr>,
          C: IntoIterator<Item=order>,
    {
        use std::thread;
        use std::sync::{Arc, Mutex};

        let chain_servers: Vec<_> = chain_servers.into_iter().collect();
        trace!("LogHandle for servers {:?}", chain_servers);

        let make_store = |client| {
            let to_store_m = Arc::new(Mutex::new(None));
            let tsm = to_store_m.clone();
            thread::spawn(move || {
                let mut event_loop = mio::Poll::new().unwrap();
                let (store, to_store) = ::async::store::AsyncTcpStore::new_tcp(
                    chain_servers.into_iter(),
                    client,
                    &mut event_loop
                ).expect("could not start store.");
                *tsm.lock().unwrap() = Some(to_store);
                store.run(event_loop);
            });
            let to_store;
            loop {
                let ts = mem::replace(&mut *to_store_m.lock().unwrap(), None);
                if let Some(s) = ts {
                    to_store = s;
                    break
                }
            }
            to_store
        };

        LogHandle::with_store(interesting_chains, make_store)
    }

    pub fn new_tcp_log_with_replication<S, C>(
        chain_servers: S,
        interesting_chains: C,
    ) -> Self
    where
        S: IntoIterator<Item=(SocketAddr, SocketAddr)>,
        C: IntoIterator<Item=order>,
    {
        use std::thread;
        use std::sync::{Arc, Mutex};

        let chain_servers: Vec<_> = chain_servers.into_iter().collect();
        trace!("LogHandle for servers {:?}", chain_servers);

        let make_store = |client| {
            let to_store_m = Arc::new(Mutex::new(None));
            let tsm = to_store_m.clone();
            thread::spawn(move || {
                let mut event_loop = mio::Poll::new().unwrap();
                let (store, to_store) = ::async::store::AsyncTcpStore::replicated_new_tcp(
                    chain_servers.into_iter(),
                    client,
                    &mut event_loop
                ).expect("could not start store.");
                *tsm.lock().unwrap() = Some(to_store);
                store.run(event_loop);
            });
            let to_store;
            loop {
                let ts = mem::replace(&mut *to_store_m.lock().unwrap(), None);
                if let Some(s) = ts {
                    to_store = s;
                    break
                }
            }
            to_store
        };

        LogHandle::with_store(interesting_chains, make_store)
    }

    pub fn spawn_tcp_log<S, C>(
        lock_server: SocketAddr,
        chain_servers: S,
        interesting_chains: C
    )-> Self
    where S: IntoIterator<Item=SocketAddr>,
          C: IntoIterator<Item=order>,
    {
        //TODO this currently leaks the other threads, maybe store the handles so they get
        //     collected when this dies?
        use std::thread;
        use std::sync::{Arc, Mutex};

        let to_store_m = Arc::new(Mutex::new(None));
        let tsm = to_store_m.clone();
        let (to_log, from_outside) = mpsc::channel();
        let client = to_log.clone();
        let (ready_reads_s, ready_reads_r) = mpsc::channel();
        let (finished_writes_s, finished_writes_r) = mpsc::channel();
        let chain_servers = chain_servers.into_iter().collect();
        thread::spawn(move || {
            run_store(lock_server, chain_servers, client, tsm)
        });

        let to_store;
        loop {
            thread::yield_now();
            let ts = mem::replace(&mut *to_store_m.lock().unwrap(), None);
            if let Some(s) = ts {
                to_store = s;
                break
            }
        }
        let interesting_chains = interesting_chains.into_iter().collect();
        thread::spawn(move || {
            run_log(to_store, from_outside, ready_reads_s, finished_writes_s,
                interesting_chains)
        });

        #[inline(never)]
        fn run_store(
            lock_server: SocketAddr,
            chain_servers: Vec<SocketAddr>,
            client: mpsc::Sender<Message>,
            tsm: Arc<Mutex<Option<mio::channel::Sender<Vec<u8>>>>>
        ) {
            let mut event_loop = mio::Poll::new().unwrap();
            let (store, to_store) = ::async::store::AsyncTcpStore::tcp(
                lock_server,
                chain_servers,
                client, &mut event_loop
            ).expect("");
            *tsm.lock().unwrap() = Some(to_store);
            store.run(event_loop)
        }

        #[inline(never)]
        fn run_log(
            to_store: mio::channel::Sender<Vec<u8>>,
            from_outside: mpsc::Receiver<Message>,
            ready_reads_s: FinshedReadQueue,
            finished_writes_s: FinshedWriteQueue,
            interesting_chains: Vec<order>,
        ) {
            let log = ThreadLog::new(
                to_store, from_outside,
                ready_reads_s,
                finished_writes_s,
                interesting_chains
            );
            log.run()
        }

        LogHandle::new(to_log, ready_reads_r, finished_writes_r)
    }

    pub fn new(
        to_log: mpsc::Sender<Message>,
        ready_reads: FinshedReadRecv,
        finished_writes: FinshedWriteRecv,
    ) -> Self {
        LogHandle {
            to_log: to_log,
            ready_reads: ready_reads,
            finished_writes: finished_writes,
            _pd: Default::default(),
            curr_entry: Default::default(),
            num_snapshots: 0,
            num_async_writes: 0,
            last_seen_entries: Default::default(),
            num_errors: 0,
        }
    }

    pub fn snapshot(&mut self, chain: order) {
        self.num_snapshots = self.num_snapshots.saturating_add(1);
        self.to_log.send(Message::FromClient(SnapshotAndPrefetch(chain)))
            .unwrap();
    }

    pub fn snapshot_colors(&mut self, colors: &[order]) {
        trace!("HANDLE send snap {:?}.", colors);
        let colors = colors.to_vec();
        self.num_snapshots = self.num_snapshots.saturating_add(1);
        self.to_log.send(Message::FromClient(MultiSnapshotAndPrefetch(colors))).unwrap();
    }

    pub fn take_snapshot(&mut self) {
        trace!("HANDLE send all snap.");
        self.num_snapshots = self.num_snapshots.saturating_add(1);
        self.to_log.send(Message::FromClient(SnapshotAndPrefetch(0.into())))
            .unwrap();
    }

    //TODO return two/three slices?
    pub fn get_next(&mut self) -> Result<(&V, &[OrderIndex]), GetRes>
    where V: UnStoreable {
        if self.num_snapshots == 0 {
            trace!("HANDLE read with no snap.");
            return Err(GetRes::Done)
        }

        'recv: loop {
            //TODO use recv_timeout in real version
            let read = self.ready_reads.recv().expect("log hung up");
            let read = match read.map_err(|e| self.make_read_error(e)) {
                Ok(v) => v,
                //TODO Gc err
                Err(Some(e)) => return Err(e),
                Err(None) => continue 'recv,
            };
            let old = mem::replace(&mut self.curr_entry, read);
            if old.capacity() > 0 {
                self.to_log.send(Message::FromClient(ReturnBuffer(old))).expect("cannot send");
            }
            if self.curr_entry.len() != 0 {
                break 'recv
            }

            trace!("HANDLE finished snap.");
            assert!(self.num_snapshots > 0);
            self.num_snapshots = self.num_snapshots.checked_sub(1).unwrap();
            if self.num_snapshots == 0 {
                trace!("HANDLE finished all snaps.");
                return Err(GetRes::Done)
            }
        }

        trace!("HANDLE got val.");
        let (val, locs) = {
            let e = bytes_as_entry(&self.curr_entry);
            (slice_to_data(e.data()), e.locs())
        };
        for &OrderIndex(o, i) in locs {
            let e = self.last_seen_entries.entry(o).or_insert(0.into());
            if *e < i {
                *e = i
            }
        }
        Ok((val, locs))
    }

    pub fn try_get_next(&mut self) -> Result<(&V, &[OrderIndex]), GetRes>
    where V: UnStoreable {
        if self.num_snapshots == 0 {
            trace!("HANDLE read with no snap.");
            return Err(GetRes::Done)
        }

        'recv: loop {
            //TODO use recv_timeout in real version
            let read = self.ready_reads.try_recv()
                .or_else(|_| Err(GetRes::NothingReady))?;
            let read = match read.map_err(|e| self.make_read_error(e)) {
                Ok(v) => v,
                //TODO Gc err
                Err(Some(e)) => return Err(e),
                Err(None) => continue 'recv,
            };
            let old = mem::replace(&mut self.curr_entry, read);
            if old.capacity() > 0 {
                self.to_log.send(Message::FromClient(ReturnBuffer(old))).expect("cannot send");
            }
            if self.curr_entry.len() != 0 {
                break 'recv
            }

            trace!("HANDLE finished snap.");
            assert!(self.num_snapshots > 0);
            self.num_snapshots = self.num_snapshots.checked_sub(1).unwrap();
            if self.num_snapshots == 0 {
                trace!("HANDLE finished all snaps.");
                return Err(GetRes::Done)
            }
        }

        trace!("HANDLE got val.");
        let (val, locs) = {
            let e = bytes_as_entry(&self.curr_entry);
            (slice_to_data(e.data()), e.locs())
        };
        for &OrderIndex(o, i) in locs {
            let e = self.last_seen_entries.entry(o).or_insert(0.into());
            if *e < i {
                *e = i
            }
        }
        Ok((val, locs))
    }

    pub fn try_get_next2(&mut self) -> Result<(&V, &[OrderIndex], &Uuid), GetRes>
    where V: UnStoreable {
        if self.num_snapshots == 0 {
            trace!("HANDLE read with no snap.");
            return Err(GetRes::Done)
        }

        'recv: loop {
            //TODO use recv_timeout in real version
            let read = self.ready_reads.try_recv()
                .or_else(|_| Err(GetRes::NothingReady))?;
            let read = match read.map_err(|e| self.make_read_error(e)) {
                Ok(v) => v,
                //TODO Gc err
                Err(Some(e)) => return Err(e),
                Err(None) => continue 'recv,
            };
            let old = mem::replace(&mut self.curr_entry, read);
            if old.capacity() > 0 {
                self.to_log.send(Message::FromClient(ReturnBuffer(old))).expect("cannot send");
            }
            if self.curr_entry.len() != 0 {
                break 'recv
            }

            trace!("HANDLE finished snap.");
            assert!(self.num_snapshots > 0);
            self.num_snapshots = self.num_snapshots.checked_sub(1).unwrap();
            if self.num_snapshots == 0 {
                trace!("HANDLE finished all snaps.");
                return Err(GetRes::Done)
            }
        }

        trace!("HANDLE got val.");
        let (val, locs, id) = {
            let e = bytes_as_entry(&self.curr_entry);
            (slice_to_data(e.data()), e.locs(), e.id())
        };
        for &OrderIndex(o, i) in locs {
            let e = self.last_seen_entries.entry(o).or_insert(0.into());
            if *e < i {
                *e = i
            }
        }
        Ok((val, locs, id))
    }

    pub fn color_append(
        &mut self,
        data: &V,
        inhabits: &mut [order],
        depends_on: &mut [order],
        async: bool,
    ) -> (Uuid, Result<(), TryWaitRes>) {
        //TODO get rid of gratuitous copies
        assert!(inhabits.len() > 0);
        trace!("color append");
        trace!("inhabits   {:?}", inhabits);
        trace!("depends_on {:?}", depends_on);
        inhabits.sort();
        assert!(
            inhabits.binary_search(&order::from(0)).is_err(),
            "color 0 should not be used;it is special cased for legacy reasons."
        );
        depends_on.sort();
        assert!(
            depends_on.binary_search(&order::from(0)).is_err(),
            "color 0 should not be used;it is special cased for legacy reasons."
        );
        let _no_snapshot = inhabits == depends_on || depends_on.len() == 0;
        // if we're performing a single colour append we might be able fuzzy_log.append
        // instead of multiappend
        let id = if depends_on.len() > 0 {
            trace!("dependent append");
            self.async_dependent_multiappend(&*inhabits, &*depends_on, data, &[])
        }
        else if inhabits.len() == 1 {
            trace!("single append");
            self.async_append(inhabits[0].into(), data, &[])
        }
        else {
            trace!("multi  append");
            self.async_multiappend(&*inhabits, data, &[])
        };
        let res = if !async {
            self.wait_for_a_specific_append(id).map(|_| ())
        } else {
            Ok(())
        };
        (id, res)
    }

    pub fn color_no_remote_append(
        &mut self,
        data: &V,
        inhabits: &mut [order],
        deps: &mut [OrderIndex],
        async: bool,
    ) -> (Uuid, Result<OrderIndex, TryWaitRes>) {
        if inhabits.len() == 0 {
            return (Uuid::nil(), Err(TryWaitRes::NothingReady))
        }

        inhabits.sort();
        assert!(
            inhabits.binary_search(&order::from(0)).is_err(),
            "color 0 should not be used;it is special cased for legacy reasons."
        );
        let id = if inhabits.len() == 1 {
            self.async_append(inhabits[0].into(), data, deps)
        } else {
            self.async_no_remote_multiappend(inhabits, data, deps)
        };
        let mut loc = Err(TryWaitRes::NothingReady);
        if !async {
            loc = self.wait_for_a_specific_append(id).map(|v| v[0]);
        }
        (id, loc)
    }

    pub fn causal_color_append(
        &mut self,
        data: &V,
        inhabits: &mut [order],
        depends_on: &mut [order],
        happens_after: &mut [OrderIndex],
    ) -> Uuid {
        if inhabits.len() == 0 {
            return Uuid::nil()
        }

        inhabits.sort();
        assert!(
            inhabits.binary_search(&order::from(0)).is_err(),
            "color 0 should not be used;it is special cased for legacy reasons."
        );
        depends_on.sort();
        assert!(
            depends_on.binary_search(&order::from(0)).is_err(),
            "color 0 should not be used;it is special cased for legacy reasons."
        );
        if depends_on.len() > 0 {
            trace!("causal dependent append");
            self.async_dependent_multiappend(&*inhabits, &*depends_on, data, happens_after)
        }
        else if inhabits.len() == 1 {
            trace!("causal single append");
            self.async_append(inhabits[0].into(), data, happens_after)
        }
        else {
            trace!("causal multi append");
            self.async_multiappend(&*inhabits, data, happens_after)
        }
    }

    pub fn simple_causal_append(
        &mut self,
        data: &V,
        inhabits: &mut [order],
        happens_after: &mut [order],
    ) -> Uuid {
        if inhabits.len() == 0 {
            return Uuid::nil()
        }

        inhabits.sort();
        assert!(
            inhabits.binary_search(&order::from(0)).is_err(),
            "color 0 should not be used;it is special cased for legacy reasons."
        );

        let mut happens_after_entries = Vec::with_capacity(happens_after.len());

        for o in happens_after {
            let horizon = self.last_seen_entries.get(o).cloned().unwrap_or(0.into());
            if horizon > entry::from(0) && inhabits.binary_search(o).is_err() {
                happens_after_entries.push(OrderIndex(*o, horizon));
            }
        }

        self.causal_color_append(data, inhabits, &mut [], &mut happens_after_entries)
    }

    pub fn wait_for_all_appends(&mut self) -> Result<(), TryWaitRes> {
        trace!("HANDLE waiting for {} appends", self.num_async_writes);
        for _ in 0..self.num_async_writes {
            self.wait_for_any_append()?;
        }
        Ok(())
    }

    pub fn wait_for_a_specific_append(&mut self, write_id: Uuid)
    -> Result<Vec<OrderIndex>, TryWaitRes> {
        for _ in 0..self.num_async_writes {
            let (id, locs) = self.wait_for_any_append()?;
            if id == write_id {
                return Ok(locs)
            }
        }
        Err(TryWaitRes::NothingReady)
    }

    pub fn wait_for_any_append(&mut self) -> Result<(Uuid, Vec<OrderIndex>), TryWaitRes> {
        //FIXME need to know the number of lost writes so we don't freeze?
        if self.num_async_writes > 0 {
            //TODO return buffers here and cache them?
            loop {
                let res = self.recv_write().unwrap();
                match res {
                    Ok(write) => {
                        self.num_async_writes -= 1;
                        return Ok(write)
                    },
                    Err(err) => if let Some(err) = self.to_wait_error(err) {
                        return Err(err)
                    },
                }
            }
        }
        Err(TryWaitRes::NothingReady)
    }

    pub fn try_wait_for_any_append(&mut self)
    -> Result<(Uuid, Vec<OrderIndex>), TryWaitRes> {
        if self.num_async_writes > 0 {
            let ret = self.finished_writes.try_recv()
                .or_else(|_| Err(TryWaitRes::NothingReady))
                .and_then(|res| res.map_err(|err|
                    match self.to_wait_error(err) {
                        Some(err) => err,
                        None => TryWaitRes::NothingReady,
                    }
                ));
            if let Ok(&(_, ref locs)) = ret.as_ref() {
                self.num_async_writes -= 1;
                for &OrderIndex(o, i) in locs {
                    let e = self.last_seen_entries.entry(o).or_insert(0.into());
                    if *e < i {
                        *e = i
                    }
                }
            }
            //TODO return buffers here and cache them?
            ret
        } else {
            Err(TryWaitRes::NothingReady)
        }
    }

    pub fn flush_completed_appends(&mut self) -> Result<(), (io::ErrorKind, usize)> {
        if self.num_async_writes > 0 {
            let last_seen_entries = &mut self.last_seen_entries;
            for res in self.finished_writes.try_iter() {
                match res {
                    Ok((_, locs)) => {
                        self.num_async_writes -= 1;
                        for &OrderIndex(o, i) in &locs {
                            let e = last_seen_entries.entry(o).or_insert(0.into());
                            if *e < i {
                                *e = i
                            }
                        }
                    },
                    Err(fuzzy_log::Error{server, error_num, error}) =>
                        if self.num_errors < error_num {
                            assert!(self.num_errors + 1 == error_num);
                            self.num_errors += 1;
                            return Err((error, server))
                        },
                }
            }
        }
        Ok(())
    }

    fn recv_write(&mut self)
    -> Result<Result<(Uuid, Vec<OrderIndex>), fuzzy_log::Error>, ::std::sync::mpsc::RecvError> {
        self.finished_writes.recv().map(|res| res.map(|(id, locs)| {
            for &OrderIndex(o, i) in &locs {
                let e = self.last_seen_entries.entry(o).or_insert(0.into());
                if *e < i {
                    *e = i
                }
            }
            (id, locs)
        }))
    }

    //TODO add wait_and_snapshot(..)
    //TODO add append_and_wait(..) ?

    pub fn append(&mut self, chain: order, data: &V, deps: &[OrderIndex])
    -> Vec<OrderIndex> {
        let id = self.async_append(chain, data, deps);
        self.wait_for_a_specific_append(id).unwrap()
    }

    pub fn async_append(&mut self, chain: order, data: &V, deps: &[OrderIndex]) -> Uuid {
        //TODO no-alloc?
        let id = Uuid::new_v4();
        let mut buffer = Vec::new();
        EntryContents::Single {
            id: &id,
            flags: &EntryFlag::Nothing,
            loc: &OrderIndex(chain, 0.into()),
            deps: deps,
            data: data_to_slice(data),
        }.fill_vec(&mut buffer);
        self.to_log.send(Message::FromClient(PerformAppend(buffer))).unwrap();
        self.num_async_writes += 1;
        id
    }

    pub fn multiappend(&mut self, chains: &[order], data: &V, deps: &[OrderIndex])
    -> Vec<OrderIndex> {
        //TODO no-alloc?
        let id = self.async_multiappend(chains, data, deps);
        self.wait_for_a_specific_append(id).unwrap()
    }

    pub fn async_multiappend(&mut self, chains: &[order], data: &V, deps: &[OrderIndex])
    -> Uuid {
        //TODO no-alloc?
        assert!(chains.len() > 1);
        let mut locs: Vec<_> = chains.into_iter().map(|&o| OrderIndex(o, 0.into())).collect();
        locs.sort();
        locs.dedup();
        let id = Uuid::new_v4();
        let mut buffer = Vec::new();
        EntryContents::Multi {
            id: &id,
            flags: &EntryFlag::Nothing,
            lock: &0,
            locs: &locs,
            deps: deps,
            data: data_to_slice(data),
        }.fill_vec(&mut buffer);
        self.to_log.send(Message::FromClient(PerformAppend(buffer))).unwrap();
        self.num_async_writes += 1;
        id
    }

    // A multiappend which does not induce a read dependency on the foreign chain
    pub fn no_remote_multiappend(&mut self, chains: &[order], data: &V, deps: &[OrderIndex])
    -> Vec<OrderIndex> {
        //TODO no-alloc?
        let id = self.async_no_remote_multiappend(chains, data, deps);
        self.wait_for_a_specific_append(id).unwrap()
    }

    pub fn async_no_remote_multiappend(&mut self, chains: &[order], data: &V, deps: &[OrderIndex])
    -> Uuid {
        //TODO no-alloc?
        assert!(chains.len() > 1);
        let mut locs: Vec<_> = chains.into_iter().map(|&o| OrderIndex(o, 0.into())).collect();
        locs.sort();
        locs.dedup();
        let id = Uuid::new_v4();
        let mut buffer = Vec::new();
        EntryContents::Multi {
            id: &id,
            flags: &EntryFlag::NoRemote,
            lock: &0,
            locs: &locs,
            deps: deps,
            data: data_to_slice(data),
        }.fill_vec(&mut buffer);
        self.to_log.send(Message::FromClient(PerformAppend(buffer))).unwrap();
        self.num_async_writes += 1;
        id
    }

    //TODO return two vecs
    pub fn dependent_multiappend(&mut self,
        chains: &[order],
        depends_on: &[order],
        data: &V,
        deps: &[OrderIndex])
    -> Vec<OrderIndex> {
        let id = self.async_dependent_multiappend(chains, depends_on, data, deps);
        self.wait_for_a_specific_append(id).unwrap()
    }

    //TODO return two vecs
    pub fn async_dependent_multiappend(&mut self,
        chains: &[order],
        depends_on: &[order],
        data: &V,
        deps: &[OrderIndex])
    -> Uuid {
        assert!(depends_on.len() > 0);
        let mut mchains: Vec<_> = chains.into_iter()
            .map(|&c| OrderIndex(c, 0.into()))
            .chain(::std::iter::once(OrderIndex(0.into(), 0.into())))
            .chain(depends_on.iter().map(|&c| OrderIndex(c, 0.into())))
            .collect();
        {

            let (chains, deps) = mchains.split_at_mut(chains.len());
            chains.sort();
            deps[1..].sort();
        }
        //FIXME ensure there are no chains which are also in depends_on
        mchains.dedup();
        assert!(mchains[chains.len()] == OrderIndex(0.into(), 0.into()));
        debug_assert!(mchains[..chains.len()].iter()
            .all(|&OrderIndex(o, _)| chains.contains(&o)));
        debug_assert!(mchains[(chains.len() + 1)..]
            .iter().all(|&OrderIndex(o, _)| depends_on.contains(&o)));
        let id = Uuid::new_v4();
        let mut buffer = Vec::new();
        EntryContents::Multi {
            id: &id,
            flags: &EntryFlag::Nothing,
            lock: &0,
            locs: &mchains,
            deps: deps,
            data: data_to_slice(data),
        }.fill_vec(&mut buffer);
        self.to_log.send(Message::FromClient(PerformAppend(buffer))).unwrap();
        self.num_async_writes += 1;
        id
    }

    fn to_wait_error(&mut self, fuzzy_log::Error{server, error_num, error}: fuzzy_log::Error)
    -> Option<TryWaitRes> {
        if self.num_errors < error_num {
            assert!(self.num_errors + 1 == error_num);
            self.num_errors += 1;
            Some(TryWaitRes::IoErr(error, server))
        } else {
            None
        }
    }

    fn make_read_error(&mut self, fuzzy_log::Error{server, error_num, error}: fuzzy_log::Error)
    -> Option<GetRes> {
        if self.num_errors < error_num {
            assert!(self.num_errors + 1 == error_num);
            self.num_errors += 1;
            Some(GetRes::IoErr(error, server))
        } else {
            None
        }
    }
}

impl<V: ?Sized> LogHandle<V> {
    pub fn read_until(&mut self, loc: OrderIndex) {
        self.to_log.send(Message::FromClient(ReadUntil(loc))).unwrap();
        self.num_snapshots = self.num_snapshots.saturating_add(1);
    }

    pub fn fastforward(&mut self, loc: OrderIndex) {
        self.to_log.send(Message::FromClient(Fastforward(loc))).unwrap();
    }
}
