
use std::marker::PhantomData;
use std::mem;
use std::net::SocketAddr;
use std::sync::mpsc;

use mio;

use async::fuzzy_log::{Message, ThreadLog};
use async::fuzzy_log::FromClient::*;
use packets::{
    Entry,
    EntryContents,
    OrderIndex,
    Uuid,
    Storeable,
    order,
    entry,
    bytes_as_entry_mut
};

pub struct LogHandle<V: ?Sized> {
    _pd: PhantomData<Box<V>>,
    num_snapshots: usize,
    num_async_writes: usize,
    to_log: mpsc::Sender<Message>,
    ready_reads: mpsc::Receiver<Vec<u8>>,
    finished_writes: mpsc::Receiver<(Uuid, Vec<OrderIndex>)>,
    //TODO finished_writes: ..
    curr_entry: Vec<u8>,
}

impl<V: ?Sized> Drop for LogHandle<V> {
    fn drop(&mut self) {
        let _ = self.to_log.send(Message::FromClient(Shutdown));
    }
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
            ready_reads_s: mpsc::Sender<Vec<u8>>,
            finished_writes_s: mpsc::Sender<(Uuid, Vec<OrderIndex>)>,
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

//TODO I kinda get the feeling that this should send writes directly to the store without
//     the AsyncLog getting in the middle
//     Also, I think if I can send associated data with the wites I could do multiplexing
//     over different writers very easily
impl<V: ?Sized> LogHandle<V>
where V: Storeable {

    pub fn with_store<C, F>(
        interesting_chains: C,
        store_builder: F,
    ) -> Self
    where C: IntoIterator<Item=order>,
          F: FnOnce(mpsc::Sender<Message>) -> mio::channel::Sender<Vec<u8>> {
        let (to_log, from_outside) = mpsc::channel();
        let to_store = store_builder(to_log.clone());
        let (ready_reads_s, ready_reads_r) = mpsc::channel();
        let interesting_chains: Vec<_> = interesting_chains.into_iter().collect();
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

/*TODO
    pub fn tcp_log_with_replication<S, C>(
        lock_server: Option<SocketAddr>,
        chain_servers: S,
        interesting_chains: C
    ) -> Self
    where S: IntoIterator<Item=(SocketAddr, SocketAddr)>,
          C: IntoIterator<Item=order>,
    {
        let to_store_m = Arc::new(Mutex::new(None));
        let tsm = to_store_m.clone();
        let (to_log, from_outside) = mpsc::channel();
        let client = to_log.clone();
        let (ready_reads_s, ready_reads_r) = mpsc::channel();
        let (finished_writes_s, finished_writes_r) = mpsc::channel();
        thread::spawn(move || {
            let mut event_loop = mio::Poll::new().unwrap();
            let (store, to_store) = AsyncTcpStore::udp(addr_str.parse().unwrap(),
                    iter::once(addr_str).map(|s| s.parse().unwrap()),
                    client, &mut event_loop).expect("");
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
        thread::spawn(move || {
            let log = ThreadLog::new(to_store, from_outside, ready_reads_s,
                finished_writes_s, interesting_chains.into_iter());
            log.run()
        });

        LogHandle::new(to_log, ready_reads_r, finished_writes_r)
    }
*/
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
            ready_reads_s: mpsc::Sender<Vec<u8>>,
            finished_writes_s: mpsc::Sender<(Uuid, Vec<OrderIndex>)>,
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
        ready_reads: mpsc::Receiver<Vec<u8>>,
        finished_writes: mpsc::Receiver<(Uuid, Vec<OrderIndex>)>
    ) -> Self {
        LogHandle {
            to_log: to_log,
            ready_reads: ready_reads,
            finished_writes: finished_writes,
            _pd: Default::default(),
            curr_entry: Default::default(),
            num_snapshots: 0,
            num_async_writes: 0,
        }
    }

    pub fn snapshot(&mut self, chain: order) {
        self.num_snapshots = self.num_snapshots.saturating_add(1);
        self.to_log.send(Message::FromClient(SnapshotAndPrefetch(chain)))
            .unwrap();
    }

    pub fn take_snapshot(&mut self) {
        self.num_snapshots = self.num_snapshots.saturating_add(1);
        self.to_log.send(Message::FromClient(SnapshotAndPrefetch(0.into())))
            .unwrap();
    }

    //TODO return two/three slices?
    pub fn get_next(&mut self) -> Option<(&V, &[OrderIndex])> {
        if self.num_snapshots == 0 {
            return None
        }

        'recv: loop {
            //TODO use recv_timeout in real version
            let old = mem::replace(&mut self.curr_entry, self.ready_reads.recv().unwrap());
            if old.capacity() > 0 {
                self.to_log.send(Message::FromClient(ReturnBuffer(old)));
            }
            if self.curr_entry.len() != 0 {
                break 'recv
            }

            self.num_snapshots = self.num_snapshots.checked_sub(1).unwrap();
            if self.num_snapshots == 0 {
                return None
            }
        }

        let (val, locs, _) = Entry::<V>::wrap_bytes(&self.curr_entry).val_locs_and_deps();
        Some((val, locs))
    }

    pub fn color_append(
        &mut self,
        data: &V,
        inhabits: &[order],
        depends_on: &[order],
        async: bool,
    ) -> Uuid {
        //TODO get rid of gratuitous copies
        assert!(inhabits.len() > 0);
        let mut inhabits = inhabits.to_vec();
        let mut depends_on = depends_on.to_vec();
        trace!("color append");
        trace!("inhabits   {:?}", inhabits);
        trace!("depends_on {:?}", depends_on);
        inhabits.sort();
        depends_on.sort();
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
        if !async {
            self.wait_for_a_specific_append(id);
        }
        id
    }

    pub fn wait_for_all_appends(&mut self) {
        trace!("HANDLE waiting for {} appends", self.num_async_writes);
        for i in 0..self.num_async_writes {
            //TODO return buffers here and cache them?
            self.finished_writes.recv().unwrap();
            trace!("HANDLE got {}", i);
        }
        trace!("HANDLE got all appends");
        self.num_async_writes = 0;
    }

    pub fn wait_for_a_specific_append(&mut self, write_id: Uuid) -> Option<Vec<OrderIndex>> {
        for _ in 0..self.num_async_writes {
            //TODO return buffers here and cache them?
            let (id, locs) = self.finished_writes.recv().unwrap();
            self.num_async_writes -= 1;
            if id == write_id {
                return Some(locs)
            }
        }
        return None
    }

    pub fn wait_for_any_append(&mut self) -> Option<(Uuid, Vec<OrderIndex>)> {
        if self.num_async_writes > 0 {
            self.num_async_writes -= 1;
            //TODO return buffers here and cache them?
            return Some(self.finished_writes.recv().unwrap())
        }
        None
    }

    pub fn flush_completed_appends(&mut self) {
        while let Ok(..) = self.finished_writes.try_recv() {
            self.num_async_writes -= 1;
        }
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
        let mut buffer = EntryContents::Data(data, &deps).clone_bytes();
        let id = Uuid::new_v4();
        {
            //TODO I should make a better entry builder
            let e = bytes_as_entry_mut(&mut buffer);
            e.id = id;
            e.locs_mut()[0] = OrderIndex(chain, 0.into());
        }
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
        let buffer = EntryContents::Multiput {
            data: data,
            uuid: &id,
            columns: &locs,
            deps: deps,
        }.clone_bytes();
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
        let buffer = EntryContents::Multiput {
            data: data,
            uuid: &id,
            columns: &mchains,
            deps: deps,
        }.clone_bytes();
        self.to_log.send(Message::FromClient(PerformAppend(buffer))).unwrap();
        self.num_async_writes += 1;
        id
    }
}
