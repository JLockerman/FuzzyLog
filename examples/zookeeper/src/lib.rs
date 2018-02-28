#![allow(dead_code)]
#![allow(unused_variables)]

// Try multiple partitions per client
// add a optional return snapshot, which returns what the snapshotted OrderIndex is
// use to know when we can return observations and resnapshot for a partition

extern crate bincode;
extern crate fuzzy_log_client;
pub extern crate serde;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
extern crate env_logger;

#[cfg(test)]
extern crate fuzzy_log_server;
#[cfg(test)]
#[macro_use]
extern crate matches;

use std::collections::{HashMap, VecDeque};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::Duration;

pub use fuzzy_log_client::fuzzy_log::log_handle::{entry, order, AtomicWriteHandle, GetRes,
                                                  LogHandle, OrderIndex, ReadHandle, TryWaitRes,
                                                  Uuid};

use bincode::{deserialize, serialize_into, Infinite};

pub use serde::{Deserialize, Serialize};

use message::*;
use files::*;

pub use message::CreateMode;

mod message;
mod files;

pub struct Client {
    to_materializer: Sender<MessageFromClient>,

    to_server: AtomicWriteHandle<[u8]>,
    color: order,

    serialize_cache: Vec<u8>,

    roots: HashMap<OsString, order>,
    my_root: OsString,
}

impl Client {
    pub fn new(
        reader: ReadHandle<[u8]>,
        writer: AtomicWriteHandle<[u8]>,
        color: order,
        my_root: OsString,
        roots: HashMap<OsString, order>,
    ) -> Self {
        use std::thread::spawn;
        let handle = reader;
        assert!(roots.iter().all(|(_, &o)| o != order::from(0)));
        let (to_materializer, from_client) = channel();
        let loopback = to_materializer.clone();
        let to_server = writer.clone();
        let my_root1 = my_root.clone();
        let roots1 = roots.clone();
        spawn(move || {
            Materializer::new(
                to_server,
                from_client,
                loopback,
                handle,
                color,
                my_root1,
                roots1,
            ).run()
        });

        let to_server = writer;
        Client {
            to_materializer,
            to_server,
            color,
            serialize_cache: vec![],
            roots,
            my_root,
        }
    }

    pub fn create(
        &mut self,
        path: PathBuf,
        data: Vec<u8>,
        create_mode: CreateMode,
        callback: Box<for<'a, 'b> FnMut(Result<(&'a Path, &'b Path), ()>) + Send>,
    ) {
        let id = Id::new();
        let msg = Mutation::Create {
            id,
            create_mode,
            path,
            data,
        };
        self.send_msg(id, msg, MutationCallback::Path(callback));
    }

    pub fn delete(
        &mut self,
        path: PathBuf,
        version: Version,
        callback: Box<for<'a, 'b> FnMut(Result<(&'a Path), ()>) + Send>,
    ) {
        let id = Id::new();
        let msg = Mutation::Delete { id, path, version };
        self.send_msg(id, msg, MutationCallback::Void(callback));
    }

    pub fn set_data(
        &mut self,
        path: PathBuf,
        data: Vec<u8>,
        version: Version,
        callback: Box<for<'a, 'b> FnMut(Result<(&'a Path, &'b Stat), ()>) + Send>,
    ) {
        let id = Id::new();
        let msg = Mutation::Set {
            id,
            path,
            data,
            version,
        };
        self.send_msg(id, msg, MutationCallback::Stat(callback));
    }

    pub fn rename(
        &mut self,
        old_path: PathBuf,
        new_path: PathBuf,
        callback: Box<for<'a> FnMut(Result<&'a Path, ()>) + Send>,
    ) {
        let id = Id::new();
        let old_mine = old_path.starts_with(&*self.my_root);
        let new_mine = new_path.starts_with(&*self.my_root);
        let other = if !old_mine {
            assert!(new_mine);
            let root = old_path.components().skip(1).next().unwrap();
            Some(self.roots[root.as_ref()])
        } else if !new_mine {
            let root = new_path.components().skip(1).next().unwrap();
            Some(self.roots[root.as_ref()])
        } else {
            None
        };
        let msg = Mutation::RenamePart1 {
            id,
            old_path,
            new_path,
        };
        match other {
            Some(other) => self.send_multi(other, id, msg, MutationCallback::Void(callback)),
            None => self.send_msg(id, msg, MutationCallback::Void(callback)),
        }
    }

    fn send_msg(&mut self, id: Id, msg: Mutation, callback: MutationCallback) {
        self.to_materializer
            .send(MessageFromClient::Mut(id, callback))
            .expect("mat dead");
        serialize_into(&mut self.serialize_cache, &msg, Infinite).expect("cannot serialize");
        self.to_server
            .async_append(self.color, &*self.serialize_cache, &[]);
        self.serialize_cache.clear();
    }

    fn send_multi(&mut self, other: order, id: Id, msg: Mutation, callback: MutationCallback) {
        assert!(other != 0.into());
        self.to_materializer
            .send(MessageFromClient::Mut(id, callback))
            .expect("mat dead");
        serialize_into(&mut self.serialize_cache, &msg, Infinite).expect("cannot serialize");
        self.to_server.async_no_remote_multiappend(
            &[self.color, other],
            &*self.serialize_cache,
            &[],
        );
        self.serialize_cache.clear();
    }

    //TODO fn get_data, get_children
    pub fn exists(
        &mut self,
        path: PathBuf,
        watch: bool,
        callback: Box<for<'a> FnMut(Result<(&Path, &Stat), ()>) + Send>,
    ) {
        let id = Id::new();
        let obs = Observation::Exists {
            id,
            path,
            watch,
            callback,
        };
        self.send_observation(obs);
    }

    pub fn get_data(
        &mut self,
        path: PathBuf,
        watch: bool,
        callback: Box<for<'a> FnMut(Result<(&Path, &[u8], &Stat), ()>) + Send>,
    ) {
        let id = Id::new();
        let obs = Observation::GetData {
            id,
            path,
            watch,
            callback,
        };
        self.send_observation(obs);
    }

    pub fn get_children(
        &mut self,
        path: PathBuf,
        watch: bool,
        callback: Box<for<'a> FnMut(Result<(&Path, &Iterator<Item = &Path>), ()>) + Send>,
    ) {
        let id = Id::new();
        let obs = Observation::GetChildren {
            id,
            path,
            watch,
            callback,
        };
        self.send_observation(obs);
    }

    fn send_observation(&mut self, observation: Observation) {
        self.to_materializer
            .send(MessageFromClient::Obs(observation))
            .expect("mat dead");
    }
}

struct Materializer {
    data: FileSystem,

    to_server: AtomicWriteHandle<[u8]>,

    from_client: Receiver<MessageFromClient>,
    loopback: Sender<MessageFromClient>,

    //TODO (Id, index) and multiversion?
    early_mutations: HashMap<Id, Result<(Arc<Path>, Stat), ()>>,
    waiting_mutations: HashMap<Id, MutationCallback>,
    waiting_observations: VecDeque<Observation>,

    handle: ReadHandle<[u8]>,
    color: order,

    serialize_cache: Vec<u8>,
}

impl Materializer {
    fn new(
        to_server: AtomicWriteHandle<[u8]>,
        from_client: Receiver<MessageFromClient>,
        loopback: Sender<MessageFromClient>,
        handle: ReadHandle<[u8]>,
        color: order,
        my_root: OsString,
        roots: HashMap<OsString, order>,
    ) -> Self {
        Materializer {
            data: FileSystem::new(my_root, roots),
            to_server,
            from_client,
            loopback,
            early_mutations: Default::default(),
            waiting_mutations: Default::default(),
            waiting_observations: Default::default(),
            handle,
            color,
            serialize_cache: Default::default(),
        }
    }

    pub fn run(&mut self) -> ! {
        loop {
            //sleep, waiting for work to be needed
            let msg = self.from_client.recv_timeout(Duration::from_millis(1)).ok();
            self.handle_ops(msg);
            while !(self.waiting_mutations.is_empty() && self.waiting_observations.is_empty()) {
                self.handle_ops(None);
            }
        }
    }

    pub fn handle_ops(&mut self, first_op: Option<MessageFromClient>) {
        let _ = self.loopback.send(MessageFromClient::EndOfSnapshot);
        self.snapshot();
        self.drain_pending_ops(first_op);
        self.play_log();
        self.handle_observations();
    }

    fn snapshot(&mut self) {
        self.handle.snapshot(self.color);
    }

    fn drain_pending_ops(&mut self, first_op: Option<MessageFromClient>) {
        let waiting_observations = &mut self.waiting_observations;
        let waiting_mutations = &mut self.waiting_mutations;
        let early_mutations = &mut self.early_mutations;
        let mut handle_message = |msg: MessageFromClient| {
            use MessageFromClient::*;
            match msg {
                EndOfSnapshot => return true,
                Obs(observation) => waiting_observations.push_back(observation),
                Mut(mutation_id, callback) => {
                    let early = early_mutations.remove(&mutation_id);
                    match early {
                        Some(res) => {
                            do_callback(
                                callback,
                                res.as_ref().map(|&(ref p, ref s)| (p, s)).map_err(|_| ()),
                            );
                        }
                        None => {
                            waiting_mutations.insert(mutation_id, callback);
                        }
                    }
                }
            }
            return false;
        };
        if let Some(msg) = first_op {
            handle_message(msg);
        }
        for msg in self.from_client.try_iter() {
            let done = handle_message(msg);
            if done {
                break;
            }
        }
    }

    fn play_log(&mut self) {
        'play: loop {
            match self.handle.get_next() {
                Err(GetRes::Done) => break 'play,
                Err(e) => panic!(e),
                Ok((bytes, ..)) => {
                    let &mut Materializer {
                        ref mut data,
                        ref mut serialize_cache,
                        ref mut to_server,
                        ref mut loopback,
                        ref mut waiting_mutations,
                        ref mut early_mutations,
                        color,
                        ..
                    } = self;
                    let msg = deserialize(bytes).expect("bad msg");
                    data.apply_mutation(msg, |id, result, msg1, msg2| {
                        for (chain, ref new_msg) in msg1.into_iter().chain(msg2) {
                            let &id = new_msg.id();
                            serialize_into(serialize_cache, &new_msg, Infinite)
                                .expect("cannot serialize");
                            assert!(chain != 0.into());
                            if chain != color {
                                to_server.async_no_remote_multiappend(
                                    &[color, chain],
                                    &*serialize_cache,
                                    &[],
                                );
                            } else {
                                to_server.async_append(color, &*serialize_cache, &[]);
                            }

                            serialize_cache.clear();
                            let _ =
                                loopback.send(MessageFromClient::Mut(id, MutationCallback::None));
                        }
                        if id.client != client_id() {
                            return;
                        }
                        let waiting = waiting_mutations.remove(&id);
                        match waiting {
                            Some(callback) => {
                                do_callback(callback, result);
                            }
                            None => {
                                let result = result.map(|(p, s)| (p.clone(), s.clone()));
                                early_mutations.insert(id, result);
                            }
                        }
                    });
                }
            }
        }
    }

    fn handle_observations(&mut self) {
        for observation in self.waiting_observations.drain(..) {
            //FIXME observation thread?
            self.data.observe(observation);
            // let res = self.data.observe(observation);
            // if let Some(observation, data) = res {
            //     do_observation(observation, data)
            // }
        }
    }
}

fn do_callback(callback: MutationCallback, result: Result<(&Arc<Path>, &Stat), ()>) {
    use MutationCallback::*;
    match callback {
        Stat(mut callback) => callback(result.map(|(p, s)| (&**p, s))),
        Path(mut callback) => callback(result.map(|(p, _)| (&**p, &**p))),
        Void(mut callback) => callback(result.map(|(p, _)| &**p)),
        None => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuzzy_log_server::tcp::run_server;
    use std::sync::mpsc::channel;
    use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};

    #[test]
    fn regression() {
        static STARTED: AtomicUsize = ATOMIC_USIZE_INIT;

        ::std::thread::spawn(|| {
            run_server(
                "0.0.0.0:14005".parse().unwrap(),
                0,
                1,
                None,
                None,
                2,
                &STARTED,
            )
        });
        while STARTED.load(Ordering::Relaxed) == 0 {
            ::std::thread::yield_now()
        }

        let chain = order::from(101);

        let (reader, writer) = LogHandle::<[u8]>::unreplicated_with_servers(&[
            "127.0.0.1:14005".parse().unwrap(),
        ]).chains(&[chain])
            .reads_my_writes()
            .build_handles();

        let my_root = "/abcd/".into();
        let mut roots = HashMap::new();
        roots.insert("/abcd/".into(), chain);
        let mut client = Client::new(reader, writer, chain, my_root, roots);
        println!("start test.");

        // let u = |done: Receiver<Result<PathBuf, _>>| done.recv().unwrap().unwrap();

        fn path<T: AsRef<Path>>(t: &T) -> &Path {
            t.as_ref()
        }

        let create = |client: &mut Client, name, data| {
            let (finished, done) = channel();
            client.create(
                name,
                data,
                CreateMode::persistent(),
                Box::new(move |res| finished.send(res.map(|(p, _)| p.to_path_buf())).unwrap()),
            );
            done.recv().unwrap().unwrap()
        };

        let set_data = |client: &mut Client, name, data| {
            let (finished, done) = channel();
            client.set_data(
                name,
                data,
                -1,
                Box::new(move |res| finished.send(res.map(|_| ())).unwrap()),
            );
            done.recv().unwrap().is_ok()
        };

        let exists = |client: &mut Client, name| {
            let (finished, done) = channel();
            client.exists(
                name,
                false,
                Box::new(move |res| finished.send(res.map(|_| ())).unwrap()),
            );
            done.recv().unwrap().is_ok()
        };

        let get_data = |client: &mut Client, name| {
            let (finished, done) = channel();
            client.get_data(
                name,
                false,
                Box::new(move |res| finished.send(res.map(|(_, d, _)| d.to_vec())).unwrap()),
            );
            done.recv().unwrap().unwrap()
        };

        let name = create(&mut client, "/abcd".into(), vec![]);
        assert_eq!(&*name, path(&"/abcd"));

        for i in 0..100 {
            let name = format!("/abcd/{}", i);
            let created = create(&mut client, name.clone().into(), vec![1, 2, 3, i]);
            assert_eq!(&*created, path(&name));
            assert!(exists(&mut client, name.clone().into()));
            assert_eq!(get_data(&mut client, name.clone().into()), vec![1, 2, 3, i]);
            if i % 2 == 0 {
                let set = set_data(&mut client, name.clone().into(), vec![5, 5, 5, 5, 5, i]);
                assert!(set);
                assert_eq!(get_data(&mut client, name.clone().into()), vec![5, 5, 5, 5, 5, i]);
            }
        }
        for i in 0..100 {
            if i % 10 != 0 {
                let name = format!("/abcd/{}", i);
                assert!(exists(&mut client, name.clone().into()));
                let data = if i % 2 == 0 {
                    vec![5, 5, 5, 5, 5, i]
                } else {
                    vec![1, 2, 3, i]
                };
                assert_eq!(get_data(&mut client, name.clone().into()), data);
            } else {
                //TODO
            }
        }
        println!("test done.");
    }
}
