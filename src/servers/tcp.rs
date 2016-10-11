use std::collections::{HashMap, HashSet};
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
    total_servers: u32,
    this_server_num: u32,
    _seen_ids: HashSet<Uuid>,
}

impl Server {
    pub fn new(server_addr: &SocketAddr, this_server_num: u32, total_chain_servers: u32, event_loop: &mut EventLoop<Self>) -> io::Result<Self> {
        let acceptor = try!(TcpListener::bind(server_addr));
        try!(event_loop.register(&acceptor,
                                 mio::Token(0),
                                 mio::EventSet::readable(),
                                 mio::PollOpt::level()));
        Ok(Server {
            log: ServerLog::new(this_server_num, total_chain_servers),
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
                        //TODO edge or level?
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
                        //TODO
                        //if need_to_respond {
                        //    mio::EventSet::writable()
                        //}
                        //else {
                        //    mio::EventSet::readable()
                        //}
                        mio::EventSet::writable()
                    } else {
                        // trace!("SERVER keep reading from client");
                        mio::EventSet::readable()
                    }
                } else if events.is_writable() {
                    //trace!("SERVER gonna write from client {:?}", client_token);
                    let finished_write = client.write_packet();
                    if finished_write {
                        trace!("SERVER finished write from client {:?}", client_token);
                        //TODO is this, or setting options to edge needed?
                        //let finished_read = client.read_packet();
                        //if finished_read {
                        //    trace!("SERVER read after write");
                        //    let need_to_respond = self.log.handle_op(&mut *client.buffer);
                        //    mio::EventSet::writable()
                        //}
                        //else {
                        //    trace!("SERVER wait for read");
                        //    mio::EventSet::readable()
                        //}
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
            let read = try!(self.stream.read(&mut self.buffer.sized_bytes_mut()[self.sent_bytes..base_header_size()]));
            self.sent_bytes += read;
            if self.sent_bytes < base_header_size() {
                return Ok(1);
            }
        }

        let header_size = self.buffer.header_size();
        assert!(header_size >= base_header_size());
        if self.sent_bytes < header_size {
            let read = try!(self.stream.read(&mut self.buffer
                .sized_bytes_mut()[self.sent_bytes..header_size]));
            self.sent_bytes += read;
            if self.sent_bytes < header_size {
                return Ok(1);
            }
        }

        let size = self.buffer.entry_size();
        if self.sent_bytes < size {
            let read = try!(self.stream.read(&mut self.buffer
                .sized_bytes_mut()[self.sent_bytes..size]));
            self.sent_bytes += read;
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
    fn new(this_server_num: u32, total_servers: u32) -> Self {
        //TODO
        //assert!(this_server_num > 0);
        assert!(this_server_num <= total_servers);
        ServerLog {
            log: HashMap::new(),
            horizon: HashMap::new(),
            last_lock: 0,
            last_unlock: 0,
            _seen_ids: HashSet::new(),
            this_server_num: this_server_num,
            total_servers: total_servers,
        }
    }

    fn handle_op(&mut self, val: &mut Entry<()>) -> bool {
        let kind = val.kind;
        match kind.layout() {
            EntryLayout::Multiput | EntryLayout::Sentinel => {
                trace!("SERVER multiput");
                //TODO handle sentinels
                //TODO check TakeLock flag?
                if self.try_lock(val.lock_num()) {
                    assert!(self._seen_ids.insert(val.id));
                    let mut sentinel_start_index = None;
                    {
                        val.kind.insert(EntryKind::ReadSuccess);
                        let locs = val.locs_mut();
                        //TODO select only relevent chains
                        //trace!("SERVER Horizon A {:?}", self.horizon);
                        'update_append_horizon: for i in 0..locs.len() {
                            if locs[i] == (0.into(), 0.into()) {
                                sentinel_start_index = Some(i + 1);
                                break 'update_append_horizon
                            }
                            let chain = locs[i].0;
                            if self.stores_chain(chain) {
                                let hor: entry = self.increment_horizon(chain);
                                locs[i].1 = hor;
                            }
                        }
                        if let Some(ssi) = sentinel_start_index {
                            for i in ssi..locs.len() {
                                assert!(locs[i] != (0.into(), 0.into()));
                                if self.stores_chain(locs[i].0) {
                                    let hor: entry = self.increment_horizon(locs[i].0);
                                    locs[i].1 = hor;
                                }
                            }
                        }
                        //trace!("SERVER Horizon B {:?}", self.horizon);
                        trace!("SERVER locs {:?}", locs);
                        trace!("SERVER ssi {:?}", sentinel_start_index);
                    }
                    trace!("SERVER appending at {:?}", val.locs());
                    let contents = Rc::new(val.clone());
                    'emplace: for &loc in val.locs() {
                        if loc == (0.into(), 0.into()) {
                            break 'emplace
                        }
                        if self.stores_chain(loc.0) {
                            self.log.insert(loc, contents.clone());
                        }
                        // trace!("SERVER appended at {:?}", loc);
                    }
                    if let Some(ssi) = sentinel_start_index {
                        val.kind.remove(EntryKind::Multiput);
                        val.kind.insert(EntryKind::Sentinel);
                        let contents = Rc::new(val.clone());
                        trace!("SERVER sentinal locs {:?}", &val.locs()[ssi..]);
                        for &loc in &val.locs()[ssi..] {
                            if self.stores_chain(loc.0) {
                                self.log.insert(loc, contents.clone());
                            }
                            // trace!("SERVER appended at {:?}", loc);
                        }
                        val.kind.remove(EntryKind::Sentinel);
                        val.kind.insert(EntryKind::Multiput);
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
                            assert!(last_entry == 0.into() || last_entry < loc.1,
                                "{:?} >= {:?}", last_entry, loc);
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
                        trace!("SERVER Read Occupied entry {:?} {:?}", loc, e.get().id);
                        let packet = e.get_mut();
                        *val = (**packet).clone();
                        // trace!("SERVER returning {:?}", packet);
                    }
                }
            }

            EntryLayout::Data => {
                trace!("SERVER Append");
                //TODO locks
                let loc = {
                    let l = unsafe { &mut val.as_data_entry_mut().flex.loc };
                    debug_assert!(self.stores_chain(l.0),
                        "tried to store {:?} at server {:?} of {:?}",
                        l, self.this_server_num, self.total_servers);
                    let hor = self.increment_horizon(l.0);
                    l.1 = hor;
                    *l
                };

                match self.log.entry(loc) {
                    Vacant(e) => {
                        trace!("SERVER Writing vacant entry {:?}", loc);
                        assert_eq!(unsafe { val.as_data_entry().flex.loc }, loc);
                        //TODO validate lock
                        val.kind.insert(EntryKind::ReadSuccess);
                        let packet = Rc::new(val.clone());
                        e.insert(packet);
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
                    trace!("SERVER TakeLock");
                    let acquired_loc = self.try_lock(lock_num);
                    if acquired_loc {
                        val.kind.insert(EntryKind::ReadSuccess);
                    }
                    else {
                        unsafe { val.as_lock_entry_mut().lock = self.last_lock };
                    }
                    return true
                }
                else {
                    trace!("SERVER UnLock");
                    if lock_num == self.last_lock {
                        trace!("SERVER Success");
                        val.kind.insert(EntryKind::ReadSuccess);
                        self.last_unlock = self.last_lock;
                    }
                    return false
                }
            }
        }
        true
    }

    fn stores_chain(&self, chain: order) -> bool {
        chain % self.total_servers == self.this_server_num.into()
    }

    fn increment_horizon(&mut self, chain: order) -> entry {
        let h = self.horizon.entry(chain).or_insert(0.into());
        *h = *h + 1;
        *h
    }

    fn try_lock(&mut self, lock_num: u64) -> bool {
        if self.is_unlocked() {
            if lock_num == self.last_lock + 1 {
                trace!("SERVER Lock {:?}", lock_num);
                self.last_lock = lock_num;
            }
            else if lock_num == 0 {
                trace!("SERVER NoLock");
            }
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
