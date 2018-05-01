
use std::cmp::max;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::mpsc;
use std::thread;

use fuzzy_log_util::socket_addr::Ipv4SocketAddr as ClientAddr;

use fuzzy_log::{
    self,
    Message,
    ThreadLog,
    FinshedReadRecv,
};

use fuzzy_log::FromClient::*;

use store;

pub use packets::{
    bytes_as_entry,
    bytes_as_entry_mut,
    order,
    entry,
    OrderIndex,
    Uuid,
    EntryFlag,
    EntryLayout,
};

use hash::HashMap;

#[derive(Debug)]
pub struct Builder {
    local_servers: Servers,
    remote_servers: Servers,
    chains: Vec<order>,
    atomic_multi_appends: bool,
}

#[derive(Debug)]
pub enum Servers {
    Unreplicated(Vec<SocketAddr>),
    Replicated(Vec<(SocketAddr, SocketAddr)>),
}

impl Builder {
    pub fn from_servers(local_servers: Servers, remote_servers: Servers) -> Self {
        Builder {
            local_servers,
            remote_servers,
            chains: vec![],
            atomic_multi_appends: false,
        }
    }

    pub fn atomic_multi_appends(self) -> Self {
        Builder{ atomic_multi_appends: true, ..self}
    }

    pub fn chains<C>(self, chains: C) -> Self
    where
        C: IntoIterator<Item=order> {
        let chains = chains.into_iter().collect();
        Builder{ chains: chains, .. self}
    }

    pub fn build(self) -> Replicator {

        let Builder{
            local_servers, remote_servers, chains, atomic_multi_appends,
        } = self;

        let (to_local_log, local_from_outside) = mpsc::channel();
        let (local_ready_reads, local_reads) = mpsc::channel();
        let (local_write_ack, from_logs) = mpsc::channel();
        let remote_ready_reads = local_write_ack.clone();
        let to_local_store = make_store(local_servers, to_local_log.clone());
        thread::spawn(move || {
            ThreadLog::builder(to_local_store, local_from_outside, local_ready_reads)
                .ack_writes(local_write_ack)
                .build()
                .run();
        });

        let (to_remote_log, remote_from_outside) = mpsc::channel();
        let to_remote_store = make_store(remote_servers, to_remote_log.clone());
        thread::spawn(move || {
            let mut log = ThreadLog::builder(
                to_remote_store, remote_from_outside, remote_ready_reads)
                .chains(chains)
                .return_snapshots();
            if atomic_multi_appends {
                log = log.atomic__no_remotes()//TODO fetch_boring_multis?
            }
            log.build().run()
        });

        Replicator {
            from_logs,

            inner: Inner {
                to_remote_log,

                local_reads,

                horizon: HashMap::default(),
                blocked: HashMap::default(),

                num_snapshots: 0,

                appender: Appender {
                    to_local_log,
                    outstanding_appends: 0,
                }
            }
        }
    }
}

fn make_store(servers: Servers, to_client: mpsc::Sender<Message>) -> store::ToSelf {
    let (s, r) = mpsc::channel();
    thread::spawn(move || {
        let (store, to_store) = match servers {
            Servers::Unreplicated(servers) => {
                store::AsyncTcpStore::new_tcp(
                    ClientAddr::random(),
                    servers.into_iter(),
                    to_client,
                ).expect("could not start store.")
            },
            Servers::Replicated(servers) => {
                ::store::AsyncTcpStore::replicated_new_tcp(
                    ClientAddr::random(),
                    servers.into_iter(),
                    to_client,
                ).expect("could not start store.")
            },
        };
        s.send(to_store).unwrap();
        drop(s);
        store.run();
    });
    r.recv().unwrap()
}

pub struct Replicator {
    from_logs: mpsc::Receiver<fuzzy_log::Response>, //remote reads and local writes

    inner: Inner,
}

struct Inner {
    to_remote_log: mpsc::Sender<Message>,

    local_reads: FinshedReadRecv,

    horizon: HashMap<order, entry>,
    blocked: HashMap<OrderIndex, VecDeque<Rc<Vec<u8>>>>,

    num_snapshots: u64,

    appender: Appender,
}

struct Appender {
    to_local_log: mpsc::Sender<Message>,
    outstanding_appends: u64,
}

impl Replicator {
    pub fn with_replicated_servers<S0, S1>(local_servers: S0, remote_servers: S1) -> Builder
    where
        S0: IntoIterator<Item=(SocketAddr, SocketAddr)>,
        S1: IntoIterator<Item=(SocketAddr, SocketAddr)>, {
        let local_servers = local_servers.into_iter().collect();
        let remote_servers = remote_servers.into_iter().collect();
        Builder::from_servers(
            Servers::Replicated(local_servers), Servers::Replicated(remote_servers))
    }

    pub fn with_unreplicated_servers<S0, S1>(local_servers: S0, remote_servers: S1) -> Builder
    where
        S0: IntoIterator<Item=SocketAddr>,
        S1: IntoIterator<Item=SocketAddr>, {
        let local_servers = local_servers.into_iter().collect();
        let remote_servers = remote_servers.into_iter().collect();
        Builder::from_servers(
            Servers::Unreplicated(local_servers), Servers::Unreplicated(remote_servers))
    }

    pub fn run(mut self) -> ! {
        loop {
            self.snapshot_remote();
            for message in self.from_logs.iter() {
                let done = self.inner.handle_message(message);
                if done { break }
            }
        }
    }

    fn snapshot_remote(&mut self) {
        self.inner.snapshot_remote()
    }
}

impl Inner {
    fn handle_message(&mut self, message: fuzzy_log::Response) -> bool {
        match message {
            fuzzy_log::Response::Err(err) =>
                //TODO better error handling
                error!("{:?}", err),

            fuzzy_log::Response::Wrote(_id, locs) => {
                self.appender.got_append();
                for OrderIndex(o, i) in locs {
                    self.after_write_ack(o, i);
                }
            }

            fuzzy_log::Response::Read(mut message) => {
                if message.len() == 0 {
                    return self.finished_snapshot();
                }

                #[derive(Debug)]
                enum Next {
                    Continue,
                    Append,
                    Buffer,
                }

                let next = {
                    let mut entry = bytes_as_entry_mut(&mut *message);
                    match entry.as_ref().kind().layout() {
                        EntryLayout::Snapshot | EntryLayout::Lock | EntryLayout::GC  =>
                            unreachable!(),

                        EntryLayout::Read => {
                            let chain = entry.as_ref().locs()[0].0;
                            self.re_snapshot(chain);
                            Next::Continue
                        },

                        EntryLayout::Data => {
                            //FIXME wait for write ACK from deps before sending next
                            entry.flag_mut().insert(EntryFlag::DirectWrite);
                            let horizon = &mut self.horizon;
                            let can_append = entry.as_ref().dependencies()
                                .into_iter().all(|&OrderIndex(o, i)| {
                                horizon.get(&o).map(|&h| h >= i).unwrap_or(false)
                            });
                            if can_append {
                                Next::Append
                            } else {
                                Next::Buffer
                            }
                        },

                        EntryLayout::Multiput => {
                            entry.flag_mut().insert(EntryFlag::DirectWrite);
                            let horizon = &mut self.horizon;
                            let can_append = entry.as_ref().dependencies()
                                .into_iter().all(|&OrderIndex(o, i)| {
                                horizon.get(&o).map(|&h| h >= i).unwrap_or(false)
                            });
                            //TODO stall until chains are ready?
                            if can_append {
                                Next::Append
                            } else {
                                Next::Buffer
                            }
                        },

                        EntryLayout::Sentinel => {
                            unimplemented!("should never need to replicate a sentinel on its own")
                        },
                    }
                };
                match next {
                    Next::Continue => {},
                    Next::Append =>
                        self.appender.append(message.clone()),

                    Next::Buffer =>
                        self.block(message.clone()),
                }
                self.to_remote_log
                    .send(Message::FromClient(ReturnBuffer(message)))
                    .unwrap();
            }
        }
        false
    }

    fn finished_snapshot(&mut self) -> bool {
        self.num_snapshots = self.num_snapshots.checked_sub(1).unwrap();
        if self.num_snapshots == 0 {
            // finished all our current snapshots
            return true
        }
        false
    }

    fn re_snapshot(&mut self, chain: order) {
        self.to_remote_log
            .send(Message::FromClient(SnapshotAndPrefetch(chain)))
            .unwrap();
        self.num_snapshots += 1;
    }

    fn after_write_ack(&mut self, chain: order, index: entry) {
        use std::collections::hash_map::Entry;
        {
            let val = self.horizon.entry(chain).or_insert(index);
            *val = max(*val, index);
        }
        if let Entry::Occupied(entry) = self.blocked.entry((chain, index).into()) {
            let mut blocked = entry.remove();
            for node in blocked.drain(..) {
                if let Ok(node) = Rc::try_unwrap(node) {
                    self.appender.append(node);
                }
            }
        }
    }

    fn block(&mut self, message: Vec<u8>) {
        let node = Rc::new(message);

        for &OrderIndex(o, i) in bytes_as_entry(&**node).dependencies() {
            let needs_block = self.horizon.get(&o).map(|&h| h < i).unwrap_or(true);
            if needs_block {
                self.blocked.entry((o, i).into())
                    .or_insert_with(VecDeque::new).push_back(node.clone());
            }
        }

        if let Ok(node) = Rc::try_unwrap(node) {
            self.appender.append(node);
        }
    }

    fn snapshot_remote(&mut self) {
        self.to_remote_log.send(Message::FromClient(SnapshotAndPrefetch(0.into()))).unwrap();
        self.num_snapshots += 1;
    }
}

impl Appender {
    fn append(&mut self, message: Vec<u8>) {
        self.to_local_log.send(Message::FromClient(
            PerformAppend(message)
        )).unwrap();
        self.outstanding_appends += 1;
    }

    fn got_append(&mut self) {
        self.outstanding_appends -= 1;
    }
}
