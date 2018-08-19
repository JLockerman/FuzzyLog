#![allow(deprecated)]

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use mio;
use mio::tcp::{TcpListener, TcpStream};

use rand::{XorShiftRng as Rand, SeedableRng, Rng};

pub use zookeeper::{CreateMode, Stat, Version, order};
use zookeeper::{Client as ZKClient, LogHandle, Observer as ZKObserver};

use reactor::{
    Reactor,
    Handler,
    IoState,
    TcpHandler,
    TcpWriter,
    MessageReader,
    MessageReaderError,
    MessageHandler,
    Wakeable,
};

use ::ServerAddrs;
use msg::{Request, Response};

pub struct View {
    reactor: Reactor<(), ClientInner>,
}

impl View {
    pub fn new(
        client_num: usize,
        num_clients: usize,
        do_transactions: Option<u32>,

        window: usize,

        my_port: u16,
        ServerAddrs(servers): ServerAddrs,

        color: order,
        balance_num: u64,

        my_root: String,
        roots: HashMap<String, order>,

        completed_writes: Arc<AtomicUsize>,
    ) -> Result<Self, io::Error> {
        let replicated = servers[0].0 != servers[0].1;
        let (reader, writer) = if replicated {
            LogHandle::<[u8]>::replicated_with_servers(&servers[..])
        } else {
            LogHandle::<[u8]>::unreplicated_with_servers(servers.iter().map(|&(a, _)| a))
        }.chains(&[color])
            .reads_my_writes()
            .client_num(balance_num)
            .build_handles();
        let roots = roots.into_iter().map(|(k, v)| (k.into(), v)).collect();

        let mut client = ZKClient::new(reader, writer, color, my_root.into(), roots);


        let mut to_threads = vec![];
        for _ in 0..2 {
            let (sender, recver) = ::mio::channel::channel();
            let (to_thread, listener) = ::mio::channel::channel();
            let observer = client.observer();
            to_threads.push(to_thread);
            thread::spawn(|| {
                let per_thread_inner = PerThreadInner {
                    observer,

                    listener,
                    listener_token: 1.into(),

                    sender,
                    recver,

                    recver_token: 2.into(),

                    last_token: 9,
                };
                let mut reactor: Reactor<_, _> = Reactor::with_inner(1.into(), per_thread_inner)
                    .expect("cannot start view reactor");
                let _ = reactor.run();
            });
        }


        let (sender, recver) = ::mio::channel::channel();

        let my_dir = client_num;

        let mut file_num = 0;

        for _ in 0..window {
            let sender = sender.clone();
            client.create(
                format!("/foo{}/{}", my_dir, file_num).into(),
                (&b"AAA"[..]).into(),
                CreateMode::persistent(),
                Box::new(move |res| {
                    match res {
                        Ok(_) => (),
                        Err(e) => panic!("Create error!? {:?}", e),
                    };
                    let _ = sender.send(true);
                }),
            );
            file_num += 1;
        }

        let (do_transactions, one_in) = match do_transactions {
            Some(one_in) => (true, one_in),
            None => (false, 1_000_000_000),
        };

        let reactor = Reactor::with_inner(1.into(), ClientInner {
            listener: TcpListener::bind(&SocketAddr::new([0, 0, 0, 0].into(), my_port))?,
            listener_token: 1.into(),

            rand: Rand::from_seed([111, 234, u32::from(color), 1010]),

            client_num,
            num_clients,

            my_dir,

            file_num,
            rename_num: 0,

            created_files: 0,

            do_transactions,
            one_in,

            client,
            sender,

            recver,
            recver_token: 2.into(),

            completed_writes,

            next_thread: 0,
            to_threads,
        })?;

        Ok(Self { reactor, })
    }

    pub fn run(mut self) {
        self.reactor.run().expect("cannot run view")
    }
}

///////////////////////////////////////

struct ClientInner {
    client: ZKClient,

    client_num: usize,
    num_clients: usize,

    rand: Rand,
    file_num: usize,
    rename_num: usize,
    created_files: usize,

    my_dir: usize,

    do_transactions: bool,
    one_in: u32,

    listener: TcpListener,
    listener_token: mio::Token,

    sender: mio::channel::Sender<bool>,

    recver: mio::channel::Receiver<bool>,
    recver_token: mio::Token,

    completed_writes: Arc<AtomicUsize>,

    next_thread: usize,
    to_threads: Vec<mio::channel::Sender<TcpStream>>,
}

impl Wakeable for ClientInner {
    fn init(&mut self, token: mio::Token, poll: &mut mio::Poll) {
        poll.register(
            &self.listener,
            token,
            mio::Ready::readable(),
            mio::PollOpt::level(),
        ).unwrap();

        poll.register(
            &self.recver,
            (usize::from(token)+1).into(),
            mio::Ready::readable(),
            mio::PollOpt::level(),
        ).unwrap();
    }

    fn needs_to_mark_as_staying_awake(&mut self, _token: mio::Token) -> bool {
        false
    }

    fn mark_as_staying_awake(&mut self, _token: mio::Token) {}

    fn is_marked_as_staying_awake(&self, _token: mio::Token) -> bool {
        true
    }
}

impl Handler<IoState<()>> for ClientInner {
    type Error = ::std::io::Error;

    fn on_event(&mut self, inner: &mut IoState<()>, token: mio::Token, _event: mio::Event)
    -> Result<(), Self::Error> {
        self.on_poll(inner, token)
    }

    fn on_poll(&mut self, _inner: &mut IoState<()>, token: mio::Token)
    -> Result<(), Self::Error> {
        if token == self.recver_token {
            let mut completed = 0;
            while let Ok(create) = self.recver.try_recv() {
                completed += 1;
                if create {
                    self.created_files += 1
                }
            }
            self.completed_writes.fetch_add(completed, Ordering::Relaxed);
            for _ in 0..completed {
                if self.do_transactions
                    && self.rand.gen_weighted_bool(self.one_in as _)
                    /*&& self.rename_num < self.created_files*/ {
                    let mut rename_dir = self.rand.gen_range(0, self.num_clients);
                    while rename_dir == self.my_dir {
                        rename_dir = self.rand.gen_range(0, self.num_clients)
                    }
                    let sender = self.sender.clone();
                    self.client.rename(
                        format!("/foo{}/{}", self.my_dir, self.rename_num).into(),
                        format!("/foo{}/r{}_{}", rename_dir, self.client_num, self.rename_num).into(),
                        Box::new(move |res| {
                            match res {
                                Ok(_) => (),
                                Err(e) => panic!("Rename error!? {:?}", e),

                            }
                            let _ = sender.send(false);
                        }),
                    );
                    self.rename_num += 1;
                } else {
                    let sender = self.sender.clone();
                    self.client.create(
                        format!("/foo{}/{}", self.my_dir, self.file_num).into(),
                        (&b"AAA"[..]).into(),
                        CreateMode::persistent(),
                        Box::new(move |res| {
                            match res {
                                Ok(_) => (),
                                Err(e) => panic!("Create error!? {:?}", e),
                            };
                            let _ = sender.send(true);
                        }),
                    );
                    self.file_num += 1;
                }
            }

            Ok(())

        } else if token == self.listener_token {

            let (new_client, _) = self.listener.accept()?;
            let next_client = self.next_thread % self.to_threads.len();
            self.next_thread += 1;
            let _ = self.to_threads[next_client].send(new_client);
            Ok(())

        } else {
            Ok(())
        }
    }

    fn on_error(&mut self, _error: Self::Error, _poll: &mut mio::Poll) -> bool {
        false
    }
}

///////////////////////////////////////

struct PerThreadInner {
    observer: ZKObserver,

    listener: mio::channel::Receiver<TcpStream>,
    listener_token: mio::Token,

    sender: mio::channel::Sender<(Response, mio::Token)>,

    recver: mio::channel::Receiver<(Response, mio::Token)>,
    recver_token: mio::Token,

    last_token: usize,
}

impl PerThreadInner {
    fn next_token(&mut self) -> mio::Token {
        self.last_token += 1;
        self.last_token.into()
    }
}

type PerStream = TcpHandler<ZKReader, ZKHandler>;

impl Wakeable for PerThreadInner {
    fn init(&mut self, token: mio::Token, poll: &mut mio::Poll) {
        poll.register(
            &self.listener,
            token,
            mio::Ready::readable(),
            mio::PollOpt::level(),
        ).unwrap();

        poll.register(
            &self.recver,
            (usize::from(token)+1).into(),
            mio::Ready::readable(),
            mio::PollOpt::level(),
        ).unwrap();
    }

    fn needs_to_mark_as_staying_awake(&mut self, _token: mio::Token) -> bool {
        false
    }

    fn mark_as_staying_awake(&mut self, _token: mio::Token) {}

    fn is_marked_as_staying_awake(&self, _token: mio::Token) -> bool {
        true
    }
}

impl Handler<IoState<PerStream>> for PerThreadInner {
    type Error = ::std::io::Error;

    fn on_event(&mut self, inner: &mut IoState<PerStream>, token: mio::Token, _event: mio::Event)
    -> Result<(), Self::Error> {
        self.on_poll(inner, token)
    }

    fn on_poll(&mut self, inner: &mut IoState<PerStream>, token: mio::Token)
    -> Result<(), Self::Error> {
        if token == self.listener_token {

            while let Ok(new_client) = self.listener.try_recv() {
                let next_token = self.next_token();
                let _ = new_client.set_nodelay(true);
                inner.add_stream(
                    next_token, TcpHandler::new(new_client, ZKReader, ZKHandler(next_token))
                ).unwrap();
            }
            Ok(())

        } else if token == self.recver_token {
            while let Ok((response, client_token)) = self.recver.try_recv() {
                {
                    use bincode::{serialize, Infinite};
                    inner.get_mut_for(client_token).map(|client|
                        client.add_writes(&[&*serialize(&response, Infinite).unwrap()])
                    );
                }
                inner.wake(client_token);
            }
            Ok(())

        } else {
            Ok(())
        }
    }

    fn on_error(&mut self, _error: Self::Error, _poll: &mut mio::Poll) -> bool {
        false
    }
}

///////////////////////////////////////

#[derive(Debug)]
struct ZKHandler(mio::Token);

impl<'s> MessageHandler<PerThreadInner, Request<'s>> for ZKHandler {
    fn handle_message(&mut self, _: &mut TcpWriter, inner: &mut PerThreadInner, message: Request)
    -> Result<(), ()> {
        let sender = inner.sender.clone();
        let token = self.0;
        match message {
            Request::Create{..}  => unimplemented!(),

            Request::Delete{..}  => unimplemented!(),

            Request::SetData{..}  => unimplemented!(),

            Request::Exists(id, path) => {
                inner.observer.exists(path.into_owned().into(), false, Box::new(move |res| {
                    let resp = match res {
                        Ok((path, &stat)) => {
                            let path = path.to_string_lossy().to_string();
                            Response::Ok(id, path, vec![], stat, vec![])
                        },
                        Err(..) => Response::Err,
                    };
                    let _ = sender.send((resp, token));
                }));
            },
            Request::GetData(id, path) => {
                inner.observer.get_data(path.into_owned().into(), false, Box::new(move |res| {
                    let resp = match res {
                        Ok((path, data, &stat)) => {
                            let path = path.to_string_lossy().to_string();
                            Response::Ok(id, path, data.to_vec(), stat, vec![])
                        },
                        Err(..) => Response::Err,
                    };
                    let _ = sender.send((resp, token));
                }));

            },
            Request::GetChildren(id, path)  => {
                inner.observer.get_children(path.into_owned().into(), false, Box::new(move |res| {
                    let resp = match res {
                        Ok((path, children)) => {
                            let path = path.to_string_lossy().to_string();
                            let children = children.map(|c| c.to_string_lossy().to_string())
                                .collect();
                            Response::Ok(id, path, vec![], Stat::new(0), children)
                        },
                        Err(..) => Response::Err,
                    };
                    let _ = sender.send((resp, token));
                }));
            },
            Request::Done => unimplemented!(),
        }
        Ok(())
    }
}

///////////////////////////////////////

#[derive(Debug)]
struct ZKReader;

impl MessageReader for ZKReader {
    type Message = Request<'static>;
    type Error = Box<::bincode::ErrorKind>;

    fn deserialize_message(
        &mut self,
        bytes: &[u8]
    ) -> Result<(Self::Message, usize), MessageReaderError<Self::Error>> {
        use ::std::io::Cursor;
        // use ::bincode::ErrorKind::
        let mut cursor = Cursor::new(bytes);
        let limit = ::bincode::Bounded(bytes.len() as u64);
        let res = ::bincode::deserialize_from(&mut cursor, limit);
        match res {
            Ok(val) => Ok((val, cursor.position() as usize)),
            Err(error) => {
                if let &::bincode::ErrorKind::SizeLimit = &*error {
                    return Err(MessageReaderError::NeedMoreBytes(1))
                }
                Err(MessageReaderError::Other(error))
            },
        }
    }
}

///////////////////////////////////////
