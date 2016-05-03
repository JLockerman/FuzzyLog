
#[macro_use]
extern crate log;

extern crate env_logger;
extern crate fnv;
extern crate fuzzy_log;
extern crate mio;
extern crate nix;

use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::env;
use std::hash::BuildHasherDefault;
use std::io::{self, Read, Write};
use std::mem;
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;

use fnv::FnvHasher;

use fuzzy_log::prelude::*;

use mio::prelude::*;
use mio::tcp::*;

use nix::sys::socket::setsockopt;
use nix::sys::socket::sockopt::TcpNoDelay;

const LISTENER_TOKEN: mio::Token = mio::Token(0);

const ADDR_STR: &'static str = "0.0.0.0:13265";

pub fn main() {
    let _ = env_logger::init();
    let args_len = env::args().len();
    let addr = match (args_len, env::args().next()) {
        (i, Some(ref s)) if i > 1 => ("0.0.0.0".to_owned() + &*s).parse().expect("invalid inet address"),
        _ => ADDR_STR.parse().expect("invalid inet address"),
    };
    let mut event_loop = EventLoop::new().unwrap();
    let mut server = Server::new(&addr, &mut event_loop).unwrap();
    trace!("starting server");
    let _ = event_loop.run(&mut server);
}

pub struct Server {
    log: ServerLog,
    acceptor: TcpListener,
    clients: HashMap<mio::Token, PerClient, BuildHasherDefault<FnvHasher>>,
}

struct PerClient {
    stream: TcpStream,
    buffer: Box<Entry<()>>,
    sent_bytes: usize,
}

struct ServerLog {
    log: HashMap<OrderIndex, Rc<Entry<()>>>,
    horizon: HashMap<order, entry, BuildHasherDefault<FnvHasher>>,
}

impl Server {
    pub fn new(server_addr: &SocketAddr, event_loop: &mut EventLoop<Self>) -> io::Result<Self> {
        let acceptor = try!(TcpListener::bind(server_addr));
        try!(event_loop.register(&acceptor,
                                 mio::Token(0),
                                 mio::EventSet::readable(),
                                 mio::PollOpt::level()));
        Ok(Server {
            log: ServerLog::new(),
            acceptor: acceptor,
            clients: HashMap::with_hasher(Default::default()),
        })
    }
}

impl mio::Handler for Server {
    type Timeout = ();
    type Message = ();

    fn ready(&mut self,
             event_loop: &mut EventLoop<Self>,
             token: mio::Token,
             events: mio::EventSet) {
        match token {
            LISTENER_TOKEN => {
                assert!(events.is_readable());
                trace!("got accept");
                match self.acceptor.accept() {
                    Ok(None) => trace!("false accept"),
                    Err(e) => panic!("error {}", e),
                    Ok(Some((socket, addr))) => {
                        let _ = socket.set_keepalive(Some(1));
                        let _ = setsockopt(socket.as_raw_fd(), TcpNoDelay, &true);
                        //let _ = socket.set_tcp_nodelay(true);
                        let next_client_id = self.clients.len() + 1;
                        let client_token = mio::Token(next_client_id);
                        trace!("new client {:?}, {:?}", next_client_id, addr);
                        let client = PerClient::new(socket);
                        let client_socket = &match self.clients.entry(client_token) {
                                                 Vacant(v) => v.insert(client),
                                                 _ => panic!("re-accept client {:?}", client_token),
                                             }
                                             .stream;
                        event_loop.register(client_socket,
                                            client_token,
                                            mio::EventSet::readable() | mio::EventSet::error(),
                                            mio::PollOpt::edge() | mio::PollOpt::oneshot())
                                  .expect("could not register client socket")
                    }
                }
            }
            client_token => {
                // trace!("got client event");
                if events.is_error() {
                    let err = self.clients.get_mut(&client_token)
                        .ok_or(std::io::Error::new(std::io::ErrorKind::Other, "socket does not exist"))
                        .map(|s| s.stream.take_socket_error());
                    if err.is_err() {
                        trace!("dropping client {:?} due to", client_token);
                        self.clients.remove(&client_token);
                    }
                    return;
                }
                let client = self.clients.get_mut(&client_token).unwrap();
                let next_interest = if events.is_readable() {
                    // trace!("gonna read from client");
                    let finished_read = client.read_packet();
                    if finished_read {
                        trace!("finished read from client {:?}", client_token);
                        self.log.handle_op(&mut *client.buffer);
                        mio::EventSet::writable()
                    } else {
                        // trace!("keep reading from client");
                        mio::EventSet::readable()
                    }
                } else if events.is_writable() {
                    trace!("gonna write from client {:?}", client_token);
                    let finished_write = client.write_packet();
                    if finished_write {
                        trace!("finished write from client {:?}", client_token);
                        mio::EventSet::readable()
                    } else {
                        trace!("keep writing from client {:?}", client_token);
                        mio::EventSet::writable()
                    }
                } else {
                    panic!("invalid event {:?}", events);
                };
                event_loop.reregister(&client.stream,
                                      client_token,
                                      next_interest | mio::EventSet::error(),
                                      mio::PollOpt::edge() | mio::PollOpt::oneshot())
                          .expect("could not reregister client socket")
            }
        }
    }
}

impl PerClient {
    fn new(stream: TcpStream) -> Self {
        PerClient {
            stream: stream,
            buffer: Box::new(unsafe { mem::zeroed() }),
            sent_bytes: 0,
        }
    }

    fn read_packet(&mut self) -> bool {
        let finished_read = self.try_read_packet();
        if let Ok(0) = finished_read {
            self.sent_bytes = 0;
            return true;
        }
        false
    }

    fn try_read_packet(&mut self) -> io::Result<usize> {
        if self.sent_bytes < base_header_size() {
            let read = try!(self.stream.read(&mut self.buffer.bytes_mut()[self.sent_bytes..]));
            self.sent_bytes += read;
            if self.sent_bytes < base_header_size() {
                return Ok(1);
            }
        }

        let header_size = self.buffer.header_size();
        assert!(header_size >= base_header_size());
        if self.sent_bytes < header_size {
            let read = try!(self.stream.read(&mut self.buffer
                .bytes_mut()[self.sent_bytes..]));
            self.sent_bytes += read;
            if self.sent_bytes < header_size {
                return Ok(1);
            }
        }

        let size = self.buffer.entry_size();
        if self.sent_bytes < size {
            try!(self.stream.read(&mut self.buffer
                .bytes_mut()[self.sent_bytes..]));
            if self.sent_bytes < size {
                return Ok(1);
            }
        }
        // assert!(payload_size >= header_size);

        Ok(0)
    }

    fn write_packet(&mut self) -> bool {
        match self.try_send_packet() {
            Ok(s) => {
                trace!("wrote {} bytes", s);
                self.sent_bytes += s;
                if self.sent_bytes >= self.buffer.entry_size() {
                    trace!("finished write");
                    self.sent_bytes = 0;
                    let _ = self.stream.flush();
                    return true;
                }
                self.sent_bytes >= self.buffer.entry_size()
            }
            Err(e) => {
                trace!("write err {:?}", e);
                false
            }
        }
    }

    fn try_send_packet(&mut self) -> io::Result<usize> {
        let send_size = self.buffer.entry_size() - self.sent_bytes;
        // trace!("server send size {}", send_size);
        self.stream
            .write(&self.buffer.bytes()[self.sent_bytes..send_size])
    }
}

impl ServerLog {
    fn new() -> Self {
        ServerLog {
            log: HashMap::new(),
            horizon: HashMap::with_hasher(Default::default()),
        }
    }

    fn handle_op(&mut self, val: &mut Entry<()>) {
        let kind = val.kind;
        if let EntryKind::Multiput = kind & EntryKind::Layout {
            //trace!("multiput {:?}", val);
            trace!("multiput");
            {
                val.kind = kind | EntryKind::ReadSuccess;
                let locs = val.locs_mut();
                for i in 0..locs.len() {
                    let hor: entry = self.horizon
                                         .get(&locs[i].0)
                                         .cloned()
                                         .unwrap_or(0.into()) +
                                     1;
                    locs[i].1 = hor;
                    self.horizon.insert(locs[i].0, hor);
                }
            }
            trace!("appending at {:?}", val.locs());
            let contents = Rc::new(val.clone());
            for &loc in val.locs() {
                self.log.insert(loc, contents.clone());
                //trace!("appended at {:?}", loc);
            }
        } else {
            // trace!("single {:?}", val);
            let loc = {
                let loc = unsafe { &mut val.as_data_entry_mut().flex.loc };
                trace!("loc {:?}", loc);
                if kind & EntryKind::Layout == EntryKind::Data {
                    trace!("is write");
                    let hor = self.horizon.get(&loc.0).cloned().unwrap_or(0.into()) + 1;
                    *loc = (loc.0, hor);
                } else {
                    trace!("is read");
                }
                trace!("loc {:?}", loc);
                *loc
            };

            match self.log.entry(loc) {
                Vacant(e) => {
                    trace!("Vacant entry {:?}", loc);
                    assert_eq!(unsafe { val.as_data_entry().flex.loc }, loc);
                    match kind & EntryKind::Layout {
                        EntryKind::Data => {
                            trace!("writing");
                            val.kind = kind | EntryKind::ReadSuccess;
                            let packet = Rc::new(val.clone());
                            e.insert(packet);
                            self.horizon.insert(loc.0, loc.1);
                        }
                        _ => trace!("empty read {:?}", loc),
                    }
                }
                Occupied(mut e) => {
                    trace!("Occupied entry {:?}", loc);
                    let packet = e.get_mut();
                    *val = (**packet).clone();
                    // trace!("returning {:?}", packet);
                }
            }
        }
    }
}
