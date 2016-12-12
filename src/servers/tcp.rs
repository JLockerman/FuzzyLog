use std::collections::HashMap;
use std::collections::hash_map::Entry::Vacant;
use std::io::{self, Read, Write};
use std::mem;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;

use prelude::*;
use servers::ServerLog;

use mio;
use mio::deprecated::{EventLoop, Handler as MioHandler};
use mio::tcp::*;

use nix::sys::socket::setsockopt;
use nix::sys::socket::sockopt::TcpNoDelay;

use buffer::Buffer;

const LISTENER_TOKEN: mio::Token = mio::Token(0);

pub struct Server {
    log: ServerLog,
    acceptor: TcpListener,
    clients: HashMap<mio::Token, PerClient>,
}

struct PerClient {
    stream: TcpStream,
    buffer: Buffer,
    sent_bytes: usize,
}

impl Server {
    pub fn new(server_addr: &SocketAddr, this_server_num: u32, total_chain_servers: u32, event_loop: &mut EventLoop<Self>) -> io::Result<Self> {
        let acceptor = try!(TcpListener::bind(server_addr));
        try!(event_loop.register(&acceptor,
                                 mio::Token(0),
                                 mio::Ready::readable(),
                                 mio::PollOpt::level()));
        Ok(Server {
            log: ServerLog::new(this_server_num, total_chain_servers),
            acceptor: acceptor,
            clients: HashMap::new(),
        })
    }
}

impl MioHandler for Server {
    type Timeout = ();
    type Message = ();

    fn ready(&mut self,
             event_loop: &mut EventLoop<Self>,
             token: mio::Token,
             events: mio::Ready) {
        match token {
            LISTENER_TOKEN => {
                assert!(events.is_readable());
                trace!("SERVER {:?} got accept", self.log.server_num());
                match self.acceptor.accept() {
                    Err(e) => panic!("error {}", e),
                    Ok((socket, addr)) => {
                        let _ = socket.set_keepalive_ms(Some(1000));
                        let _ = socket.set_nodelay(true);
                        let next_client_id = self.clients.len() + 1;
                        let client_token = mio::Token(next_client_id);
                        trace!("SERVER {} new client {:?}, {:?}",
                            self.log.server_num(), next_client_id, addr);
                        let client = PerClient::new(socket);
                        let client_socket = &match self.clients.entry(client_token) {
                                                 Vacant(v) => v.insert(client),
                                                 _ => {
                                                     panic!("re-accept client {:?}",
                                                            client_token)
                                                 }
                                             }
                                             .stream;
                        //TODO edge or level?
                        event_loop.register(client_socket,
                                            client_token,
                                            mio::Ready::readable() | mio::Ready::error(),
                                            mio::PollOpt::edge() | mio::PollOpt::oneshot())
                                  .expect("could not register client socket")
                    }
                }
            }
            client_token => {
                // trace!("SERVER got client event");
                if events.is_error() {
                    let err = self.clients
                                  .get_mut(&client_token)
                                  .ok_or(::std::io::Error::new(::std::io::ErrorKind::Other,
                                                               "socket does not exist"))
                                  .map(|s| s.stream.take_error());
                    if err.is_err() {
                        trace!("SERVER {} dropping client {:?} due to",
                            self.log.server_num(), client_token);
                        self.clients.remove(&client_token);
                    }
                    return;
                }
                let client = self.clients.get_mut(&client_token).unwrap();
                let next_interest = if events.is_readable() {
                    // trace!("SERVER gonna read from client");

                    let unwound = catch_unwind(AssertUnwindSafe(|| client.read_packet()));
                    trace!("SERVER {} reading from client {:?}...",
                        self.log.server_num(), client_token);
                    let finished_read = match unwound {
                        Err(cause) => {
                            trace!("SERVER {} dropping client {:?} due to panic {:?}",
                                self.log.server_num(), client_token, cause);
                            trace!("SERVER {} had read {:?}",
                                self.log.server_num(), &client.buffer[..client.sent_bytes]);
                            //TODO self.clients.remove(&client_token);
                            //client.sent_bytes = 0;
                            //event_loop.reregister(&client.stream, client_token,
                            //    mio::Ready::readable() | mio::Ready::error(),
                            //    mio::PollOpt::edge() | mio::PollOpt::oneshot())
                            //          .expect("could not reregister client socket");
                            return
                        }
                        Ok(f) => f,
                    };
                    if finished_read {
                        trace!("SERVER {} finished read from client {:?}",
                            self.log.server_num(), client_token);
                        let need_to_respond = self.log.handle_op(client.buffer.entry_mut());
                        //TODO
                        //if need_to_respond {
                        //    mio::Ready::writable()
                        //}
                        //else {
                        //    mio::Ready::readable()
                        //}
                        mio::Ready::writable()
                    } else {
                        // trace!("SERVER keep reading from client");
                        mio::Ready::readable()
                    }
                } else if events.is_writable() {
                    //trace!("SERVER gonna write from client {:?}", client_token);
                    let unwound = catch_unwind(AssertUnwindSafe(|| client.write_packet()));
                    let finished_write = match unwound {
                        Err(cause) => {
                            trace!("SERVER {} dropping client {:?} due to {:?}",
                                self.log.server_num(), client_token, cause);
                            //TODO self.clients.remove(&client_token);
                            return
                        }
                        Ok(f) => f,
                    };
                    if finished_write {
                        trace!("SERVER {} finished write from client {:?}",
                            self.log.server_num(), client_token);
                        //TODO is this, or setting options to edge needed?
                        //let finished_read = client.read_packet();
                        //if finished_read {
                        //    trace!("SERVER read after write");
                        //    let need_to_respond = self.log.handle_op(&mut *client.buffer);
                        //    mio::Ready::writable()
                        //}
                        //else {
                        //    trace!("SERVER wait for read");
                        //    mio::Ready::readable()
                        //}
                        mio::Ready::readable()
                    } else {
                        //trace!("SERVER keep writing from client {:?}", client_token);
                        mio::Ready::writable()
                    }
                } else {
                    panic!("SERVER {} invalid event {:?}", self.log.server_num(), events);
                };
                event_loop.reregister(&client.stream,
                                      client_token,
                                      next_interest | mio::Ready::error(),
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
            buffer: Buffer::new(),
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
            let read = try!(self.stream.read(&mut
                self.buffer[self.sent_bytes..base_header_size()]));
            self.sent_bytes += read;
            if self.sent_bytes < base_header_size() {
                return Ok(1);
            }
        }

        let header_size = self.buffer.entry().header_size();
        assert!(header_size >= base_header_size());
        if self.sent_bytes < header_size {
            let read = try!(self.stream.read(&mut self.buffer[self.sent_bytes..header_size]));
            self.sent_bytes += read;
            if self.sent_bytes < header_size {
                return Ok(1);
            }
        }

        let size = self.buffer.entry().entry_size();
        if self.sent_bytes < size {
            let read = try!(self.stream.read(&mut self.buffer[self.sent_bytes..size]));
            self.sent_bytes += read;
            if self.sent_bytes < size {
                return Ok(1);
            }
        }
        debug_assert!(self.buffer.packet_fits());
        // assert!(payload_size >= header_size);
        Ok(0)
    }

    fn write_packet(&mut self) -> bool {
        match self.try_send_packet() {
            Ok(s) => {
                //trace!("SERVER wrote {} bytes", s);
                self.sent_bytes += s;
                if self.sent_bytes >= self.buffer.entry().entry_size() {
                    trace!("SERVER finished write");
                    self.sent_bytes = 0;
                    let _ = self.stream.flush();
                    return true;
                }
                self.sent_bytes >= self.buffer.entry().entry_size()
            }
            Err(e) => {
                trace!("SERVER write err {:?}", e);
                false
            }
        }
    }

    fn try_send_packet(&mut self) -> io::Result<usize> {
        let send_size = self.buffer.entry().entry_size() - self.sent_bytes;
        // trace!("SERVER server send size {}", send_size);
        self.stream.write(&self.buffer[self.sent_bytes..send_size])
    }
}
