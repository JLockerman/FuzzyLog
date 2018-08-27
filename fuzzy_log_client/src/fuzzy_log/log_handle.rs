
use std::borrow::Borrow;
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::net::SocketAddr;
use std::thread;
use std::sync::{mpsc, Arc, Mutex};

pub use hash::HashMap;
use hash::HashSet;

use fuzzy_log::{
    self,
    Message,
    ThreadLog,
    FinshedReadQueue,
    FinshedReadRecv,
    FinshedWriteQueue,
    FinshedWriteRecv,
};
use fuzzy_log_util::socket_addr::Ipv4SocketAddr;
use store;
use fuzzy_log::FromClient::*;
pub use packets::{
    order,
    entry,
    OrderIndex,
    Uuid,
};
use packets::{
    EntryContents,
    Storeable,
    UnStoreable,
    bytes_as_entry,
    data_to_slice,
    slice_to_data,
    EntryFlag,
};

pub struct LogHandle<V: ?Sized> {
    read_handle: ReadHandle<V>,
    write_handle: AtomicWriteHandle<V>,
    finished_writes: FinshedWriteRecv,
    num_async_writes: Option<usize>,
}

impl<V: ?Sized> Drop for LogHandle<V> {
    fn drop(&mut self) {
        let _ = self.read_handle.to_log.send(Message::FromClient(Shutdown));
    }
}

pub struct ReadHandle<V: ?Sized> {
    _pd: PhantomData<Box<V>>,
    num_snapshots: usize,
    to_log: mpsc::Sender<Message>,
    ready_reads: FinshedReadRecv,
    curr_entry: Vec<u8>,
    num_errors: u64,
    last_dropped: Arc<()>,
}

pub struct AtomicWriteHandle<V: ?Sized> {
    _pd: PhantomData<Box<V>>,
    to_log: mpsc::Sender<Message>,
    last_dropped: Arc<()>,
}

impl<V: ?Sized> Drop for ReadHandle<V> {
    fn drop(&mut self) {
        if let Some(..) = Arc::get_mut(&mut self.last_dropped) {
            let _ = self.to_log.send(Message::FromClient(Shutdown));
        }
    }
}

impl<V: ?Sized> Drop for AtomicWriteHandle<V> {
    fn drop(&mut self) {
        if let Some(..) = Arc::get_mut(&mut self.last_dropped) {
            let _ = self.to_log.send(Message::FromClient(Shutdown));
        }
    }
}

impl<V: ?Sized> Clone for AtomicWriteHandle<V> {
    fn clone(&self) -> Self {
        let &AtomicWriteHandle{ref _pd, ref to_log, ref last_dropped} = self;
        AtomicWriteHandle {
            _pd: _pd.clone(),
            to_log: to_log.clone(),
            last_dropped:last_dropped.clone()
        }
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

    #[deprecated]
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
            _lock_server: SocketAddr,
            _chain_servers: Vec<SocketAddr>,
            _client: mpsc::Sender<Message>,
            _tsm: Arc<Mutex<Option<store::ToSelf>>>
        ) {
            unimplemented!()
        }

        #[inline(never)]
        fn run_log(
            to_store: store::ToSelf,
            from_outside: mpsc::Receiver<Message>,
            ready_reads_s: FinshedReadQueue,
            finished_writes_s: FinshedWriteQueue,
            interesting_chains: Vec<order>,
        ) {
            let log = ThreadLog::new(
                to_store, from_outside,
                ready_reads_s,
                finished_writes_s,
                true,
                true,
                interesting_chains
            );
            log.run()
        }

        LogHandle::new(to_log, ready_reads_r, finished_writes_r, true)
    }
}

#[derive(Debug)]
pub struct LogBuilder<V: ?Sized> {
    servers: Servers,
    chains: Vec<order>,
    reads_my_writes: bool,
    fetch_boring_multis: bool,
    id: Option<Ipv4SocketAddr>,
    ack_writes: bool,
    my_colors_chains: Option<HashSet<order>>,
    _pd: PhantomData<Box<V>>,
}

#[derive(Debug)]
enum Servers {
    Unreplicated(Vec<SocketAddr>),
    Replicated(Vec<(SocketAddr, SocketAddr)>),
}

impl<V: ?Sized> LogBuilder<V>
where V: Storeable {

    fn from_servers(servers: Servers) -> Self {
        LogBuilder {
            servers: servers,
            chains: vec![],
            reads_my_writes: false,
            fetch_boring_multis: false,
            id: None,
            ack_writes: true,
            my_colors_chains: None,
            _pd: PhantomData,
        }
    }

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

    pub fn fetch_boring_multis(self) -> Self {
        LogBuilder{ fetch_boring_multis: true, .. self}
    }

    pub fn id(self, bytes: [u8; 16]) -> Self {
        LogBuilder{ id: Some(Ipv4SocketAddr::from_bytes(bytes)), .. self }
    }

    pub fn client_num(self, id: u64) -> Self {
        LogBuilder{ id: Some(Ipv4SocketAddr::from_u64(id)), .. self }
    }

    pub fn my_colors_chains(self, chains: HashSet<order>) -> Self {
        let builder = self.chains(chains.iter().cloned());
        LogBuilder{ my_colors_chains: Some(chains), .. builder }
    }

    pub fn do_not_ack_writes(self) -> Self {
        LogBuilder{ack_writes: false, ..self}
    }

    pub fn build(self) -> LogHandle<V> {
        let LogBuilder {
            servers, chains, reads_my_writes, fetch_boring_multis, ack_writes, id, my_colors_chains, _pd,
        } = self;

        let make_store = |client| {
            let to_store_m = Arc::new(Mutex::new(None));
            let tsm = to_store_m.clone();
            let _ = thread::spawn(move || {
                match servers {
                    Servers::Unreplicated(servers) => {
                        let (mut store, to_store) =
                            ::store::AsyncTcpStore::new_tcp(
                                id.unwrap_or_else(Ipv4SocketAddr::random),
                                servers.into_iter(),
                                client,
                            ).expect("could not start store.");
                        *tsm.lock().unwrap() = Some(to_store);
                        store.set_reads_my_writes(reads_my_writes);
                        store.run();
                    },
                    Servers::Replicated(servers) => {
                        let (mut store, to_store) =
                            ::store::AsyncTcpStore::replicated_new_tcp(
                                id.unwrap_or_else(Ipv4SocketAddr::random),
                                servers.into_iter(),
                                client,
                            ).expect("could not start store.");
                        *tsm.lock().unwrap() = Some(to_store);
                        store.set_reads_my_writes(reads_my_writes);
                        store.run();
                    },
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

        LogHandle::build_with_store(
            chains,
            fetch_boring_multis,
            ack_writes,
            my_colors_chains,
            make_store
        )
    }

    pub fn build_handles(self) -> (ReadHandle<V>, AtomicWriteHandle<V>) {
        let handle = self.do_not_ack_writes().build();
        handle.split_atomic()
    }
}

//TODO I kinda get the feeling that this should send writes directly to the store without
//     the AsyncLog getting in the middle
//     Also, I think if I can send associated data with the wites I could do multiplexing
//     over different writers very easily
impl<V: ?Sized> LogHandle<V>
where V: Storeable {

    fn split_atomic(mut self) -> (ReadHandle<V>, AtomicWriteHandle<V>) {
        macro_rules! move_fields {
            ($($field:ident),* $(,)*; $_self:ident) => (
                $(
                    let $field = mem::replace(&mut $_self.$field, mem::uninitialized());
                )*
                mem::forget(self);
            );
        }
        unsafe {
            move_fields!(
                read_handle,
                write_handle;
                self
            );
            (read_handle, write_handle)
        }
    }

    pub fn split(mut self) -> (ReadHandle<V>, AtomicWriteHandle<V>) {
        let _ = self.read_handle.to_log.send(Message::FromClient(StopAckingWrites));
        let _ = self.flush_completed_appends();
        self.split_atomic()
    }

    pub fn unreplicated_with_servers<S, A>(servers: S) -> LogBuilder<V>
    where
        S: IntoIterator<Item=A>,
        A: Borrow<SocketAddr>, {
        let servers = servers.into_iter().map(|s| s.borrow().clone()).collect();
        LogBuilder::from_servers(Servers::Unreplicated(servers))
    }

    pub fn replicated_with_servers<S, A>(servers: S) -> LogBuilder<V>
    where
        S: IntoIterator<Item=A>,
        A: Borrow<(SocketAddr, SocketAddr)>, {
        let servers = servers.into_iter().map(|s| s.borrow().clone()).collect();
        LogBuilder::from_servers(Servers::Replicated(servers))
    }

    pub fn build_with_store<C, F>(
        interesting_chains: C,
        fetch_boring_multis: bool,
        ack_writes: bool,
        my_colors_chains: Option<HashSet<order>>,
        store_builder: F,
    ) -> Self
    where C: IntoIterator<Item=order>,
          F: FnOnce(mpsc::Sender<Message>) -> store::ToSelf {
        let (to_log, from_outside) = mpsc::channel();
        let to_store = store_builder(to_log.clone());
        let (ready_reads_s, ready_reads_r) = mpsc::channel();
        let interesting_chains: Vec<_> = interesting_chains
            .into_iter()
            .inspect(|c| assert!(c != &0.into(), "Don't register interest in color 0."))
            .collect();
        let (finished_writes_s, finished_writes_r) = mpsc::channel();
        thread::spawn(move || {
            let builder = ThreadLog::builder(to_store, from_outside, ready_reads_s)
                .set_fetch_boring_multis(fetch_boring_multis)
                .chains(interesting_chains);
            let builder = match my_colors_chains {
                Some(my_colors_chains) => builder.my_colors_chains(my_colors_chains),
                None => builder,
            };
            match ack_writes {
                true => builder.ack_writes(finished_writes_s).build().run(),
                false => builder.build().run(),
            };
        });

        LogHandle::new(to_log, ready_reads_r, finished_writes_r, ack_writes)
    }

    pub fn with_store<C, F>(
        interesting_chains: C,
        fetch_boring_multis: bool,
        store_builder: F,
    ) -> Self
    where C: IntoIterator<Item=order>,
          F: FnOnce(mpsc::Sender<Message>) -> store::ToSelf {
        let (to_log, from_outside) = mpsc::channel();
        let to_store = store_builder(to_log.clone());
        let (ready_reads_s, ready_reads_r) = mpsc::channel();
        let interesting_chains: Vec<_> = interesting_chains
            .into_iter()
            .inspect(|c| assert!(c != &0.into(), "Don't register interest in color 0."))
            .collect();
        let (finished_writes_s, finished_writes_r) = mpsc::channel();
        thread::spawn(move || {
            ThreadLog::new(
                to_store, from_outside,
                ready_reads_s,
                finished_writes_s,
                fetch_boring_multis,
                true,
                interesting_chains
            ).run()
        });

        LogHandle::new(to_log, ready_reads_r, finished_writes_r, Default::default())
    }

    #[deprecated]
    pub fn tcp_log_with_replication<S, C>(
        _lock_server: Option<SocketAddr>,
        _chain_servers: S,
        _interesting_chains: C
    ) -> Self
    where S: IntoIterator<Item=(SocketAddr, SocketAddr)>,
          C: IntoIterator<Item=order>,
    {
        unimplemented!()
    }

    /// Spawns a `LogHandle` which connects to TCP servers located at `chain_servers`
    /// which reads chains `interesting_chains`.
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
                let (store, to_store) = ::store::AsyncTcpStore::new_tcp(
                    Ipv4SocketAddr::random(),
                    chain_servers.into_iter(),
                    client,
                ).expect("could not start store.");
                *tsm.lock().unwrap() = Some(to_store);
                store.run();
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
        LogHandle::with_store(interesting_chains, true, make_store)
    }

    /// Spawns a `LogHandle` which connects to replicated TCP servers
    /// whose (head, tail) addresses are `chain_servers`
    /// which reads chains `interesting_chains`.
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
                let (store, to_store) = ::store::AsyncTcpStore::replicated_new_tcp(
                    Ipv4SocketAddr::random(),
                    chain_servers.into_iter(),
                    client,
                ).expect("could not start store.");
                *tsm.lock().unwrap() = Some(to_store);
                store.run();
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

        LogHandle::with_store(interesting_chains, true, make_store)
    }

    #[deprecated]
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
            _lock_server: SocketAddr,
            _chain_servers: Vec<SocketAddr>,
            _client: mpsc::Sender<Message>,
            _tsm: Arc<Mutex<Option<store::ToSelf>>>
        ) {
            unimplemented!()
        }

        #[inline(never)]
        fn run_log(
            to_store: store::ToSelf,
            from_outside: mpsc::Receiver<Message>,
            ready_reads_s: FinshedReadQueue,
            finished_writes_s: FinshedWriteQueue,
            interesting_chains: Vec<order>,
        ) {
            let log = ThreadLog::new(
                to_store, from_outside,
                ready_reads_s,
                finished_writes_s,
                true,
                true,
                interesting_chains
            );
            log.run()
        }

        LogHandle::new(to_log, ready_reads_r, finished_writes_r, Default::default())
    }

    pub fn new(
        to_log: mpsc::Sender<Message>,
        ready_reads: FinshedReadRecv,
        finished_writes: FinshedWriteRecv,
        ack_writes: bool,
    ) -> Self {
        let last_dropped = Arc::new(());
        LogHandle {
            read_handle: ReadHandle::new(to_log.clone(), ready_reads, last_dropped.clone()),
            write_handle: AtomicWriteHandle::new(to_log, last_dropped),
            finished_writes: finished_writes,
            num_async_writes: if ack_writes { Some(0) } else { None },
        }
    }

    /// Take a snapshot of a supplied interesting color and start prefetching.
    pub fn snapshot(&mut self, chain: order) {
        self.read_handle.snapshot(chain)
    }

    /// Take a snapshot of a set of interesting colors and start prefetching.
    pub fn snapshot_colors(&mut self, colors: &[order]) {
        self.read_handle.snapshot_colors(colors)
    }

    /// Take a linearizable snapshot of a set of interesting colors and start prefetching.
    pub fn strong_snapshot(&mut self, colors: &[order]) {
        self.read_handle.strong_snapshot(colors)
    }

    /// Take a snapshot of all interesting colors and start prefetching.
    pub fn take_snapshot(&mut self) {
        self.read_handle.take_snapshot()
    }

    /// Wait until an event is ready, then returns the contents.
    pub fn get_next(&mut self) -> Result<(&V, &[OrderIndex]), GetRes>
    where V: UnStoreable {
        self.read_handle.get_next()
    }

    pub fn get_next2(&mut self) -> Result<(&V, &[OrderIndex], &Uuid), GetRes>
    where V: UnStoreable {
        self.read_handle.get_next2()
    }

    pub fn sync<F>(&mut self, per_event: F)
    -> Result<HashMap<order, entry>, GetRes>
    where V: UnStoreable, F: FnMut(&V, &[OrderIndex], &Uuid) {
        self.read_handle.sync(per_event)
    }

    pub fn sync_chain<F>(&mut self, chain: order, per_event: F)
    -> Result<HashMap<order, entry>, GetRes>
    where V: UnStoreable, F: FnMut(&V, &[OrderIndex], &Uuid) {
        self.read_handle.sync_chain(chain, per_event)
    }

    /// Returns an event if one is ready.
    pub fn try_get_next(&mut self) -> Result<(&V, &[OrderIndex]), GetRes>
    where V: UnStoreable {
        self.read_handle.try_get_next()
    }

    pub fn try_get_next2(&mut self) -> Result<(&V, &[OrderIndex], &Uuid), GetRes>
    where V: UnStoreable {
        self.read_handle.try_get_next2()
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
        _happens_after: &mut [order],
    ) -> Uuid {
        if inhabits.len() == 0 {
            return Uuid::nil()
        }

        inhabits.sort();
        assert!(
            inhabits.binary_search(&order::from(0)).is_err(),
            "color 0 should not be used;it is special cased for legacy reasons."
        );

        self.causal_color_append(data, inhabits, &mut [], &mut [])
    }

    pub fn simpler_causal_append(
        &mut self,
        data: &V,
        inhabits: &mut [order],
    ) -> Uuid {
        if inhabits.len() == 0 {
            return Uuid::nil()
        }

        inhabits.sort();
        assert!(
            inhabits.binary_search(&order::from(0)).is_err(),
            "color 0 should not be used;it is special cased for legacy reasons."
        );

        self.causal_color_append(data, inhabits, &mut [], &mut [])
    }

    //TODO add wait_and_snapshot(..)
    //TODO add append_and_wait(..) ?

    pub fn append(&mut self, chain: order, data: &V, deps: &[OrderIndex])
    -> Vec<OrderIndex> {
        let id = self.async_append(chain, data, deps);
        self.wait_for_a_specific_append(id).unwrap()
    }

    pub fn async_append(&mut self, chain: order, data: &V, deps: &[OrderIndex]) -> Uuid {
        let id = self.write_handle.async_append(chain, data, deps);
        self.num_async_writes.as_mut().map(|n| *n += 1);
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
        let id = self.write_handle.async_multiappend(chains, data, deps);
        self.num_async_writes.as_mut().map(|n| *n += 1);
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
        let id = self.write_handle.async_no_remote_multiappend(chains, data, deps);
        self.num_async_writes.as_mut().map(|n| *n += 1);
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
        let id = self.write_handle.async_dependent_multiappend(chains, depends_on, data, deps);
        self.num_async_writes.as_mut().map(|n| *n += 1);
        id
    }
}

impl<V: ?Sized> LogHandle<V> {

    //FIXME better error checking is no waiting is possible

    pub fn wait_for_all_appends(&mut self) -> Result<(), TryWaitRes> {
        trace!("HANDLE waiting for {:?} appends", self.num_async_writes);
        for _ in 0..self.num_async_writes
            .expect("cannot wait for everything with multiple write handles") {
            self.wait_for_any_append()?;
        }
        Ok(())
    }

    pub fn wait_for_a_specific_append(&mut self, write_id: Uuid)
    -> Result<Vec<OrderIndex>, TryWaitRes> {
        for _ in 0..self.num_async_writes
            .expect("cannot wait for a specific append with multiple write handles") {
            let (id, locs) = self.wait_for_any_append()?;
            if id == write_id {
                return Ok(locs)
            }
        }
        Err(TryWaitRes::NothingReady)
    }

    pub fn wait_for_any_append(&mut self) -> Result<(Uuid, Vec<OrderIndex>), TryWaitRes> {
        //FIXME need to know the number of lost writes so we don't freeze?
        match self.num_async_writes {
            Some(0) => return Err(TryWaitRes::NothingReady),
            None => self.try_wait_for_any_append(),
            Some(_) => {
                //TODO return buffers here and cache them?
                loop {
                    let res = self.recv_write().unwrap();
                    match res {
                        Ok(write) => {
                            self.num_async_writes.as_mut().map(|n| *n -= 1);
                            return Ok(write)
                        },
                        Err(err) => if let Some(err) = self.to_wait_error(err) {
                            return Err(err)
                        },
                    }
                }
            },
        }
    }

    pub fn try_wait_for_any_append(&mut self)
    -> Result<(Uuid, Vec<OrderIndex>), TryWaitRes> {
        match self.num_async_writes {
            Some(0) => return Err(TryWaitRes::NothingReady),
            _ => {
                let ret = self.finished_writes.try_recv()
                    .or_else(|_| Err(TryWaitRes::NothingReady))
                    .and_then(|res| res.map_err(|err|
                        match self.to_wait_error(err) {
                            Some(err) => err,
                            None => TryWaitRes::NothingReady,
                        }
                    ));
                if ret.is_ok() {
                    self.num_async_writes.as_mut().map(|n| *n -= 1);
                }
                //TODO return buffers here and cache them?
                ret
            }
        }
    }

    pub fn flush_completed_appends(&mut self) -> Result<usize, (io::ErrorKind, usize)> {
        match self.num_async_writes {
            Some(0) => return Ok(0),
            _ => {
                let num_errors = &mut self.read_handle.num_errors;
                let mut flushed = 0;
                for res in self.finished_writes.try_iter() {
                    match res {
                        Ok(..) => {
                            flushed += 1;
                            self.num_async_writes.as_mut().map(|n| *n -= 1);
                        },
                        Err(fuzzy_log::Error{server, error_num, error}) =>
                            //TODO return incremental count
                            if *num_errors < error_num {
                                assert!(*num_errors + 1 == error_num);
                                *num_errors += 1;
                                return Err((error, server));
                            },
                    }
                }
                Ok(flushed)
            },
        }
    }

    fn to_wait_error(&mut self, fuzzy_log::Error{server, error_num, error}: fuzzy_log::Error)
    -> Option<TryWaitRes> {
        if self.read_handle.num_errors < error_num {
            assert!(self.read_handle.num_errors + 1 == error_num);
            self.read_handle.num_errors += 1;
            Some(TryWaitRes::IoErr(error, server))
        } else {
            None
        }
    }

    fn recv_write(&mut self)
    -> Result<Result<(Uuid, Vec<OrderIndex>), fuzzy_log::Error>, ::std::sync::mpsc::RecvError> {
        self.finished_writes.recv().map(|res| res.map(|(id, locs)| {
            (id, locs)
        }))
    }

    pub fn read_until(&mut self, loc: OrderIndex) {
        self.read_handle.read_until(loc)
    }

    pub fn fastforward(&mut self, loc: OrderIndex) {
        self.read_handle.fastforward(loc)
    }

    pub fn rewind(&mut self, loc: OrderIndex) {
        self.read_handle.rewind(loc)
    }
}

impl<V: ?Sized> ReadHandle<V> {

    fn new(
        to_log: mpsc::Sender<Message>,
        ready_reads: FinshedReadRecv,
        last_dropped: Arc<()>,
    ) -> Self {
        Self {
            _pd: Default::default(),
            to_log,
            ready_reads,
            curr_entry: Default::default(),
            num_snapshots: 0,
            num_errors: 0,
            last_dropped,
        }
    }

    /// Take a snapshot of a supplied interesting color and start prefetching.
    pub fn snapshot(&mut self, chain: order) {
        self.num_snapshots = self.num_snapshots.saturating_add(1);
        self.to_log.send(Message::FromClient(SnapshotAndPrefetch(chain)))
            .unwrap();
    }

    /// Take a snapshot of a set of interesting colors and start prefetching.
    pub fn snapshot_colors(&mut self, colors: &[order]) {
        trace!("HANDLE send snap {:?}.", colors);
        let colors = colors.to_vec();
        self.num_snapshots = self.num_snapshots.saturating_add(1);
        self.to_log.send(Message::FromClient(MultiSnapshotAndPrefetch(colors))).unwrap();
    }

    /// Take a linearizable snapshot of a set of interesting colors and start prefetching.
    pub fn strong_snapshot(&mut self, colors: &[order]) {
        trace!("HANDLE send snap {:?}.", colors);
        let mut c = Vec::with_capacity(colors.len());
        c.extend(colors.into_iter().map(|&o| OrderIndex(o, entry::from(0))));
        self.num_snapshots = self.num_snapshots.saturating_add(1);
        self.to_log.send(Message::FromClient(StrongSnapshotAndPrefetch(c))).unwrap();
    }

    /// Take a snapshot of all interesting colors and start prefetching.
    pub fn take_snapshot(&mut self) {
        trace!("HANDLE send all snap.");
        self.num_snapshots = self.num_snapshots.saturating_add(1);
        self.to_log.send(Message::FromClient(SnapshotAndPrefetch(0.into())))
            .unwrap();
    }

    pub fn sync<F>(&mut self, per_event: F)
    -> Result<HashMap<order, entry>, GetRes>
    where V: UnStoreable, F: FnMut(&V, &[OrderIndex], &Uuid) {
        self.take_snapshot();
        self.do_sync(per_event)
    }

    pub fn sync_chain<F>(&mut self, chain: order, per_event: F)
    -> Result<HashMap<order, entry>, GetRes>
    where V: UnStoreable, F: FnMut(&V, &[OrderIndex], &Uuid) {
        self.snapshot(chain);
        self.do_sync(per_event)
    }

    fn do_sync<F>(&mut self, mut per_event: F)
    -> Result<HashMap<order, entry>, GetRes>
    where V: UnStoreable, F: FnMut(&V, &[OrderIndex], &Uuid) {
        let mut entries_seen = HashMap::default();
        loop {
            match self.get_next2() {
                Ok((v, locs, id)) => {
                    for &OrderIndex(o, i) in locs {
                        let last = entries_seen.entry(o).or_insert(i);
                        if *last <= i {
                            *last = i
                        }
                    }
                    per_event(v, locs, id);
                },
                Err(GetRes::Done) => return Ok(entries_seen),
                Err(e) => return Err(e),
            }

        }
    }

    /// Wait until an event is ready, then returns the contents.
    pub fn get_next(&mut self) -> Result<(&V, &[OrderIndex]), GetRes>
    where V: UnStoreable {
        self.get_next2().map(|(v, l, _)| (v, l))
    }

    pub fn get_next2(&mut self) -> Result<(&V, &[OrderIndex], &Uuid), GetRes>
    where V: UnStoreable {
        if self.num_snapshots == 0 {
            trace!("HANDLE read with no snap.");
            return Err(GetRes::Done)
        }

        'recv: loop {
            //TODO use recv_timeout in real version
            let read = self.ready_reads.recv().expect("no log");
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
        Ok((val, locs, id))
    }


    pub fn try_get_next(&mut self) -> Result<(&V, &[OrderIndex]), GetRes>
    where V: UnStoreable {
        self.try_get_next2().map(|(v, l, _)| (v, l))
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
        Ok((val, locs, id))
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

    pub fn read_until(&mut self, loc: OrderIndex) {
        self.to_log.send(Message::FromClient(ReadUntil(loc))).unwrap();
        self.num_snapshots = self.num_snapshots.saturating_add(1);
    }

    pub fn fastforward(&mut self, loc: OrderIndex) {
        self.to_log.send(Message::FromClient(Fastforward(loc))).unwrap();
    }

    pub fn rewind(&mut self, loc: OrderIndex) {
        self.to_log.send(Message::FromClient(Rewind(loc))).unwrap();
    }
}

impl<V: ?Sized> AtomicWriteHandle<V>
where V: Storeable {

    fn new(to_log: mpsc::Sender<Message>, last_dropped: Arc<()>) -> Self {
        Self { to_log, last_dropped, _pd: Default::default() }
    }

    pub fn simple_async_append(&self, data: &V, inhabits: &[order]) -> Uuid {
        if inhabits.len() == 1 {
            trace!("causal single append");
            self.async_append(inhabits[0].into(), data, &[])
        }
        else {
            trace!("causal multi append");
            self.async_multiappend(&*inhabits, data, &[])
        }
    }

    pub fn async_append(&self, chain: order, data: &V, deps: &[OrderIndex]) -> Uuid {
        //TODO no-alloc?
        let id = Uuid::new_v4();
        let mut buffer = Vec::new();
        EntryContents::Single {
            id: &id,
            flags: &EntryFlag::Nothing,
            loc: &OrderIndex(chain, 0.into()),
            deps: deps,
            data: data_to_slice(data),
            timestamp: &0, //TODO
        }.fill_vec(&mut buffer);
        self.to_log.send(Message::FromClient(PerformAppend(buffer))).unwrap();
        id
    }

    pub fn async_multiappend(&self, chains: &[order], data: &V, deps: &[OrderIndex])
    -> Uuid {
        //TODO no-alloc?
        assert!(chains.len() > 1);
        let mut locs: Vec<_> = chains.into_iter().map(|&o| OrderIndex(o, 0.into())).collect();
        locs.sort();
        locs.dedup();
        assert!(locs.len() >= 1);
        if locs.len() == 1 {
            return self.async_append(locs[0].0, data, deps)
        }
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
        id
    }

    pub fn async_no_remote_multiappend(&self, chains: &[order], data: &V, deps: &[OrderIndex])
    -> Uuid {
        //TODO no-alloc?
        assert!(chains.len() > 1);
        let mut locs: Vec<_> = chains.into_iter().map(|&o| OrderIndex(o, 0.into())).collect();
        locs.sort();
        locs.dedup();
        assert!(locs.len() >= 1);
        if locs.len() == 1 {
            return self.async_append(locs[0].0, data, deps)
        }
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
        id
    }

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
        id
    }
}


pub fn append_message<V: ?Sized>(chain: order, data: &V, deps: &[OrderIndex]) -> Vec<u8>
where V: Storeable {
    //TODO no-alloc?
    let id = Uuid::new_v4();
    let mut buffer = Vec::new();
    EntryContents::Single {
        id: &id,
        flags: &EntryFlag::Nothing,
        loc: &OrderIndex(chain, 0.into()),
        deps: deps,
        data: data_to_slice(data),
        timestamp: &0, //TODO
    }.fill_vec(&mut buffer);
    buffer
}

pub fn multiappend_message<V: ?Sized>(chains: &[order], data: &V, deps: &[OrderIndex]) -> Vec<u8>
where V: Storeable {
    //TODO no-alloc?
    assert!(chains.len() > 1);
    let mut locs: Vec<_> = chains.into_iter().map(|&o| OrderIndex(o, 0.into())).collect();
    locs.sort();
    locs.dedup();
    let id = Uuid::new_v4();
    let mut buffer = Vec::new();
    EntryContents::Multi {
        id: &id,
        flags: &(EntryFlag::NewMultiPut | EntryFlag::NoRemote),
        lock: &0,
        locs: &locs,
        deps: deps,
        data: data_to_slice(data),
    }.fill_vec(&mut buffer);
    buffer
}

