#![allow(deprecated)]

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
// use std::thread;

use mio;
use mio::tcp::TcpListener;

pub use zookeeper::{CreateMode, Stat, Version, order};
use zookeeper::{Client as ZKClient, LogHandle};

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
    reactor: Reactor<PerStream, ClientInner>,
}


struct ClientInner {
    client: ZKClient,

    listener: TcpListener,
    listener_token: mio::Token,

    sender: mio::channel::Sender<(Response, mio::Token)>,

    recver: mio::channel::Receiver<(Response, mio::Token)>,
    recver_token: mio::Token,

    last_token: usize,
}

impl View {
    pub fn new(
        my_port: u16,
        ServerAddrs(servers): ServerAddrs,
        color: order,
        my_root: String,
        roots: HashMap<String, order>,
    ) -> Result<Self, io::Error> {
        let replicated = servers[0].0 != servers[0].1;
        let (reader, writer) = if replicated {
            LogHandle::<[u8]>::replicated_with_servers(&servers[..])
        } else {
            LogHandle::<[u8]>::unreplicated_with_servers(servers.iter().map(|&(a, _)| a))
        }.chains(&[color])
            .reads_my_writes()
            .build_handles();
        let roots = roots.into_iter().map(|(k, v)| (k.into(), v)).collect();
        #[allow(deprecated)]
        let (sender, recver) = ::mio::channel::channel();
        Ok(Self {
            reactor: Reactor::with_inner(1.into(), ClientInner {
                listener: TcpListener::bind(&SocketAddr::new([0, 0, 0, 0].into(), my_port))?,
                listener_token: 1.into(),

                client: ZKClient::new(reader, writer, color, my_root.into(), roots),
                sender,

                recver,
                recver_token: 2.into(),

                last_token: 9,
            })?,
        })
    }

    pub fn run(mut self) {
        self.reactor.run().unwrap()
    }
}

///////////////////////////////////////

impl ClientInner {
    fn next_token(&mut self) -> mio::Token {
        self.last_token += 1;
        self.last_token.into()
    }
}

type PerStream = TcpHandler<ZKReader, ZKHandler>;

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

impl Handler<IoState<PerStream>> for ClientInner {
    type Error = ::std::io::Error;

    fn on_event(&mut self, inner: &mut IoState<PerStream>, token: mio::Token, _event: mio::Event)
    -> Result<(), Self::Error> {
        self.on_poll(inner, token)
    }

    fn on_poll(&mut self, inner: &mut IoState<PerStream>, token: mio::Token)
    -> Result<(), Self::Error> {
        if token == self.listener_token {

            let (new_client, _) = self.listener.accept()?;
            let next_token = self.next_token();
            inner.add_stream(
                next_token, TcpHandler::new(new_client, ZKReader, ZKHandler(next_token))
            ).unwrap();
            Ok(())

        } else if token == self.recver_token {

            while let Ok((response, client_token)) = self.recver.try_recv() {
                {
                    use bincode::{serialize, Infinite};
                    let client = inner.get_mut_for(client_token).unwrap();
                    client.add_writes(&[&*serialize(&response, Infinite).unwrap()])
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

#[allow(deprecated)]
impl<'s> MessageHandler<ClientInner, Request<'s>> for ZKHandler {
    fn handle_message(&mut self, _: &mut TcpWriter, inner: &mut ClientInner, message: Request)
    -> Result<(), ()> {
        let sender = inner.sender.clone();
        let token = self.0;
        match message {
            Request::Create { id, path, data, create_mode, }  => {
                inner.client.create(path.into_owned().into(), data, create_mode,
                    Box::new(move |res| {
                    let resp = match res {
                        Ok(path) => {
                            let path = path.0.to_string_lossy().to_string();
                            Response::Ok(id, path, vec![], Stat::new(0), vec![])
                        },
                        Err(..) => Response::Err,
                    };
                    let _ = sender.send((resp, token));
                }));
            },

            Request::Delete { id, path, version, } => {
                inner.client.delete(path.into_owned().into(), version, Box::new(move |res| {
                    let resp = match res {
                        Ok(path) => {
                            let path = path.to_string_lossy().to_string();
                            Response::Ok(id, path, vec![], Stat::new(0), vec![])
                        },
                        Err(..) => Response::Err,
                    };
                    let _ = sender.send((resp, token));
                }));
            },

            Request::SetData { id, path, data, version, } => {
                inner.client.set_data(path.into_owned().into(), data, version, Box::new(move |res| {
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

            Request::Exists(id, path) => {
                inner.client.exists(path.into_owned().into(), false, Box::new(move |res| {
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
                inner.client.get_data(path.into_owned().into(), false, Box::new(move |res| {
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
                inner.client.get_children(path.into_owned().into(), false, Box::new(move |res| {
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
