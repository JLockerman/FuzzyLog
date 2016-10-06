use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::io::{self, Read, Write};
use std::mem;
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::rc::Rc;

use prelude::*;
use packets::EntryKind::EntryLayout;

use mio;
use mio::prelude::*;
use mio::tcp::*;

use nix::sys::socket::setsockopt;
use nix::sys::socket::sockopt::TcpNoDelay;


const LISTENER_TOKEN: mio::Token = mio::Token(0);

pub struct Server {
    log: ServerLog,
    acceptor: TcpListener,
    clients: HashMap<mio::Token, PerClient>,
}

struct PerClient {
    stream: TcpStream,
    buffer: Box<Entry<()>>,
    sent_bytes: usize,
}

struct ServerLog {
    log: HashMap<OrderIndex, Rc<Entry<()>>>,
    horizon: HashMap<order, entry>,
    last_lock: u64,
    last_unlock: u64,
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
            clients: HashMap::new(),
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
                trace!("SERVER got accept");
                match self.acceptor.accept() {
                    Ok(None) => trace!("SERVER false accept"),
                    Err(e) => panic!("error {}", e),
                    Ok(Some((socket, addr))) => {
                        let _ = socket.set_keepalive(Some(1));
                        let _ = setsockopt(socket.as_raw_fd(), TcpNoDelay, &true);
                        // let _ = socket.set_tcp_nodelay(true);
                        let next_client_id = self.clients.len() + 1;
                        let client_token = mio::Token(next_client_id);
                        trace!("SERVER new client {:?}, {:?}", next_client_id, addr);
                        let client = PerClient::new(socket);
                        let client_socket = &match self.clients.entry(client_token) {
                                                 Vacant(v) => v.insert(client),
                                                 _ => {
                                                     panic!("re-accept client {:?}",
                                                            client_token)
                                                 }
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
                // trace!("SERVER got client event");
                if events.is_error() {
                    let err = self.clients
                                  .get_mut(&client_token)
                                  .ok_or(::std::io::Error::new(::std::io::ErrorKind::Other,
                                                               "socket does not exist"))
                                  .map(|s| s.stream.take_socket_error());
                    if err.is_err() {
                        trace!("SERVER dropping client {:?} due to", client_token);
                        self.clients.remove(&client_token);
                    }
                    return;
                }
                let client = self.clients.get_mut(&client_token).unwrap();
                let next_interest = if events.is_readable() {
                    // trace!("SERVER gonna read from client");
                    let finished_read = client.read_packet();
                    if finished_read {
                        trace!("SERVER finished read from client {:?}", client_token);
                        let need_to_respond = self.log.handle_op(&mut *client.buffer);
                        if need_to_respond {
                            mio::EventSet::writable()
                        }
                        else {
                            mio::EventSet::readable()
                        }
                    } else {
                        // trace!("SERVER keep reading from client");
                        mio::EventSet::readable()
                    }
                } else if events.is_writable() {
                    //trace!("SERVER gonna write from client {:?}", client_token);
                    let finished_write = client.write_packet();
                    if finished_write {
                        trace!("SERVER finished write from client {:?}", client_token);
                        mio::EventSet::readable()
                    } else {
                        //trace!("SERVER keep writing from client {:?}", client_token);
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
            let read = try!(self.stream.read(&mut self.buffer.sized_bytes_mut()[self.sent_bytes..]));
            self.sent_bytes += read;
            if self.sent_bytes < base_header_size() {
                return Ok(1);
            }
        }

        let header_size = self.buffer.header_size();
        assert!(header_size >= base_header_size());
        if self.sent_bytes < header_size {
            let read = try!(self.stream.read(&mut self.buffer
                .sized_bytes_mut()[self.sent_bytes..]));
            self.sent_bytes += read;
            if self.sent_bytes < header_size {
                return Ok(1);
            }
        }

        let size = self.buffer.entry_size();
        if self.sent_bytes < size {
            try!(self.stream.read(&mut self.buffer
                .sized_bytes_mut()[self.sent_bytes..]));
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
                //trace!("SERVER wrote {} bytes", s);
                self.sent_bytes += s;
                if self.sent_bytes >= self.buffer.entry_size() {
                    trace!("SERVER finished write");
                    self.sent_bytes = 0;
                    let _ = self.stream.flush();
                    return true;
                }
                self.sent_bytes >= self.buffer.entry_size()
            }
            Err(e) => {
                trace!("SERVER write err {:?}", e);
                false
            }
        }
    }

    fn try_send_packet(&mut self) -> io::Result<usize> {
        let send_size = self.buffer.entry_size() - self.sent_bytes;
        // trace!("SERVER server send size {}", send_size);
        self.stream
            .write(&self.buffer.bytes()[self.sent_bytes..send_size])
    }
}

impl ServerLog {
    fn new() -> Self {
        ServerLog {
            log: HashMap::new(),
            horizon: HashMap::new(),
            last_lock: 0,
            last_unlock: 0,
        }
    }

    fn handle_op(&mut self, val: &mut Entry<()>) -> bool {
        let kind = val.kind;
        match kind.layout() {
            EntryLayout::Multiput | EntryLayout::Sentinel => {
                trace!("SERVER multiput");
                //TODO handle sentinels
                if self.try_lock(val.lock_num()) {
                    {
                        val.kind = kind | EntryKind::ReadSuccess;
                        let locs = val.locs_mut();
                        trace!("SERVER locs {:?}", locs);
                        'update_horizon: for i in 0..locs.len() {
                            if locs[i] == (0.into(), 0.into()) {
                                break 'update_horizon
                            }
                            let hor: entry = self.horizon
                                                 .get(&locs[i].0)
                                                 .cloned()
                                                 .unwrap_or(0.into()) +
                                             1;
                            locs[i].1 = hor;
                            self.horizon.insert(locs[i].0, hor);
                        }
                    }
                    trace!("SERVER appending at {:?}", val.locs());
                    let contents = Rc::new(val.clone());
                    'emplace: for &loc in val.locs() {
                        if loc == (0.into(), 0.into()) {
                            break 'emplace
                        }
                        self.log.insert(loc, contents.clone());
                        // trace!("SERVER appended at {:?}", loc);
                    }
                } else {
                    trace!("SERVER wrong lock {} @ ({},{})",
                        val.lock_num(), self.last_lock, self.last_unlock);
                }
            }
            EntryLayout::Read => {
                trace!("SERVER Read");
                let loc = unsafe { val.as_data_entry_mut().flex.loc };
                match self.log.entry(loc) {
                    Vacant(..) => {
                        trace!("SERVER Read Vacant entry {:?}", loc);
                        if val.kind == EntryKind::Read {
                            //TODO validate lock
                            let l = loc.0;
                            let last_entry = self.horizon.get(&l).cloned().unwrap_or(0.into());
                            let (old_id, old_loc) = unsafe {
                                (val.id, val.as_data_entry().flex.loc)
                            };
                            *val = EntryContents::Data(&(), &[(l, last_entry)]).clone_entry();
                            val.id = old_id;
                            val.kind = EntryKind::NoValue;
                            unsafe {
                                val.as_data_entry_mut().flex.loc = old_loc;
                            }
                            trace!("SERVER empty read {:?}", loc)
                        }
                        else {
                            trace!("SERVER nop {:?}", val.kind)
                        }
                    }
                    Occupied(mut e) => {
                        trace!("SERVER Read Occupied entry {:?}", loc);
                        let packet = e.get_mut();
                        *val = (**packet).clone();
                        // trace!("SERVER returning {:?}", packet);
                    }
                }
            }

            EntryLayout::Data => {
                trace!("SERVER Append");
                let loc = {
                    let l = unsafe { &mut val.as_data_entry_mut().flex.loc };
                    let hor = self.horizon.get(&l.0).cloned().unwrap_or(0.into()) + 1;
                    l.1 = hor;
                    *l
                };

                match self.log.entry(loc) {
                    Vacant(e) => {
                        trace!("SERVER Writing vacant entry {:?}", loc);
                        assert_eq!(unsafe { val.as_data_entry().flex.loc }, loc);
                        //TODO validate lock
                        val.kind = kind | EntryKind::ReadSuccess;
                        let packet = Rc::new(val.clone());
                        e.insert(packet);
                        self.horizon.insert(loc.0, loc.1);
                    }
                    _ => {
                        unreachable!("SERVER Occupied entry {:?}", loc)
                        //*val = (**packet).clone();
                    }
                }
            }

            EntryLayout::Lock => {
                trace!("SERVER Lock");
                let lock_num = unsafe { val.as_lock_entry().lock };
                if kind.is_taking_lock() {
                    let acquired_loc = self.try_lock(lock_num);
                    if acquired_loc {
                        val.kind = kind | EntryKind::ReadSuccess;
                    }
                    else {
                        unsafe { val.as_lock_entry_mut().lock = self.last_lock };
                    }
                    return true
                }
                else {
                    if lock_num == self.last_lock {
                        self.last_unlock = self.last_lock;
                    }
                    return false
                }
            }
        }
        true
    }

    fn try_lock(&mut self, lock_num: u64) -> bool {
        if self.is_unlocked()
            && (lock_num == 0 || lock_num == self.last_lock + 1) {
            self.last_lock = lock_num;
            true
        }
        else {
            false
        }
    }

    fn is_unlocked(&self) -> bool {
        self.last_lock == self.last_unlock
    }
}
