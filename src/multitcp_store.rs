
use prelude::*;

use std::fmt::Debug;
// use std::marker::{Unsize, PhantomData};
use std::io::{Read, Write, Result as IoResult};
use std::marker::PhantomData;
use std::mem;
use std::net::{TcpStream, ToSocketAddrs};
// use std::ops::CoerceUnsized;

use net2::TcpStreamExt;

// use mio::buf::{SliceBuf, MutSliceBuf};
// use mio::udp::UdpSocket;
// use mio::unix;

use time::precise_time_ns;

// #[derive(Debug)]
pub struct TcpStore<V: ?Sized> {
    sockets: Vec<TcpStream>,
    lock_socket: TcpStream,
    receive_buffer: Box<Entry<V>>,
    send_buffer: Box<Entry<V>>,
    rtt: i64,
    dev: i64,
    _pd: PhantomData<V>,
}

const SLEEP_NANOS: u32 = 8000; //TODO user settable
const RTT: i64 = 80000;

impl<V: Storeable + ?Sized> TcpStore<V> {
    pub fn new<P, Q>(lock_server_addr: P, server_addrs: Q) -> IoResult<Self>
        where P: ToSocketAddrs,
              Q: ToSocketAddrs
    {
        unsafe {
            Ok(TcpStore {
                // TcpStream::connect(server_addr).expect("unable to open store"),
                lock_socket: try!(try!(lock_server_addr.to_socket_addrs())
                                      .next()
                                      .map(TcpStream::connect)
                                      .unwrap()),
                sockets: try!(try!(server_addrs.to_socket_addrs().map(|i| {
                    i.map(TcpStream::connect).collect::<Result<Vec<_>, _>>()
                }))),
                receive_buffer: Box::new(mem::zeroed()),
                send_buffer: Box::new(mem::zeroed()),
                _pd: Default::default(),
                rtt: RTT,
                dev: 0,
            })
        }
    }

    fn read_packet(&mut self, socket_id: usize) {
        let mut bytes_read = 0;
        //trace!("client read start base header");
        while bytes_read < base_header_size() {
            bytes_read += self.sockets[socket_id]
                              .read(&mut self.receive_buffer.bytes_mut()[bytes_read..])
                              .expect("cannot read");
        }
        //trace!("client read base header");
        let header_size = self.receive_buffer.header_size();
        trace!("header size {}", header_size);
        while bytes_read < header_size {
            bytes_read +=
                self.sockets[socket_id]
                    .read(&mut self.receive_buffer
                                   .bytes_mut()[bytes_read..])
                    .expect("cannot read");
        }
        let end = self.receive_buffer.entry_size();
        //trace!("client read more header, entry size {}", end);
        while bytes_read < end {
            bytes_read +=
                self.sockets[socket_id]
                    .read(&mut self.receive_buffer
                                   .bytes_mut()[bytes_read..])
                    .expect("cannot read");
        }
    }

    fn send_packet(&mut self, socket_id: usize) {
        let send_size = self.send_buffer.entry_size();
        //trace!("send size {}", send_size);
        self.sockets[socket_id]
            .write_all(&self.send_buffer.bytes()[..send_size])
            .expect("cannot send");
        // self.socket.flush();
    }

    fn socket_id(&self, chain: order) -> usize {
        <u32 as From<order>>::from(chain) as usize % self.sockets.len()
    }

    fn single_node_multiappend(&mut self, request_id: &Uuid, socket_id: usize) -> InsertResult {
        trace!("sn multi_append from {:?}", self.sockets[0].local_addr());
        // TODO find server
        let start_time = precise_time_ns() as i64;
        'send: loop {
            self.send_packet(socket_id);

            'receive: loop {
                self.read_packet(socket_id);
                trace!("got packet");
                match self.receive_buffer.kind & EntryKind::Layout {
                    EntryKind::Multiput => {
                        // TODO types?
                        trace!("correct response");
                        trace!("id {:?}", self.receive_buffer.id);
                        if self.receive_buffer.id == *request_id {
                            trace!("multiappend success");
                            let sample_rtt = precise_time_ns() as i64 - start_time;
                            let diff = sample_rtt - self.rtt;
                            self.dev = self.dev + (diff.abs() - self.dev) / 4;
                            self.rtt = self.rtt + (diff * 4 / 5);
                            return Ok((0.into(), 0.into())); //TODO
                        } else {
                            // trace!("?? packet {:?}", self.receive_buffer);
                            continue 'receive;
                        }
                    }
                    v => {
                        trace!("invalid response {:?}", v);
                        continue 'receive;
                    }
                }
            }
        }
    }

    fn multi_node_multiappend(&mut self, request_id: &Uuid, chains: &[OrderIndex]) -> InsertResult {
        trace!("mn multi_append from {:?}", self.sockets[0].local_addr());

        let indices = self.get_lock_inidices(chains);

        for &(socket_id, lock_num) in &*indices {
            self.resend_multi_node_multiappend(socket_id, lock_num)
        }

        'validate: for &(socket_id, lock_num) in &*indices {
            'receive: loop {
                self.read_packet(<u32 as From<_>>::from(socket_id) as usize);
                trace!("got packet");
                match self.receive_buffer.kind & EntryKind::Layout {
                    EntryKind::Multiput => {
                        // TODO should also have lock-success
                        // TODO types?
                        trace!("correct response");
                        trace!("id {:?}", self.receive_buffer.id);
                        if self.receive_buffer.id == *request_id {
                            if self.receive_buffer.kind & EntryKind::ReadSuccess !=
                            EntryKind::ReadSuccess {
                                self.resend_multi_node_multiappend(socket_id, lock_num);
                                continue 'receive
                            }
                            trace!("multiappend success");
                            continue 'validate;
                        } else {
                            // trace!("?? packet {:?}", self.receive_buffer);
                            continue 'receive;
                        }
                    }
                    v => {
                        trace!("invalid response {:?}", v);
                        continue 'receive;
                    }
                }
            }
        }
        return Ok((0.into(), 0.into()));
    }

    fn resend_multi_node_multiappend(&mut self, socket_id: order, lock_num: entry) {
        trace!("sending mnm actual");
        unsafe {
            self.send_buffer.as_multi_entry_mut().flex.lock =
                <u32 as From<_>>::from(lock_num) as u64;
            assert!(self.send_buffer.as_multi_entry_mut().flex.lock != 0u32.into());
        }
        self.send_packet(<u32 as From<_>>::from(socket_id) as usize);
    }

    fn get_lock_inidices(&mut self, chains: &[OrderIndex]) -> Vec<OrderIndex> {
        let send_buffer_size = self.send_buffer.entry_size() as usize;
        let mut nodes_to_lock: Vec<_> = (0..self.sockets.len()).map(|_| false).collect();
        for &(o, _) in chains {
            nodes_to_lock[self.socket_id(o)] = true;
        }
        let mut lock_chains: Vec<_> = nodes_to_lock.into_iter()
                                                   .enumerate()
                                                   .filter_map(|(i, present)| if present {
                                                       Some(((i as u32).into(), 0.into()))
                                                   } else {
                                                       None
                                                   })
                                                   .collect();
        let lock_id = Uuid::new_v4();
        let lock_request = EntryContents::Multiput {
                               data: &self.send_buffer.bytes()[..send_buffer_size],
                               uuid: &lock_id,
                               columns: &lock_chains[..],
                               deps: &[],
                           }
                           .clone_bytes();
        self.lock_socket.write_all(&lock_request[..]).expect("cannot send");
        'receive: loop {
            self.read_lock_packet();
            trace!("got packet");
            match self.receive_buffer.kind & EntryKind::Layout {
                EntryKind::Multiput => {
                    // TODO types?
                    trace!("correct response");
                    trace!("id {:?}", self.receive_buffer.id);
                    if self.receive_buffer.id == lock_id {
                        trace!("multiappend success");
                        let locs = self.receive_buffer.locs();
                        assert_eq!(locs.len(), lock_chains.len());
                        for i in 0..locs.len() {
                            lock_chains[i] = locs[i];
                        }
                        return lock_chains;
                    } else {
                        // trace!("?? packet {:?}", self.receive_buffer);
                        continue 'receive;
                    }
                }
                v => {
                    trace!("invalid response {:?}", v);
                    continue 'receive;
                }
            }
        }
    }

    fn read_lock_packet(&mut self) {
        let mut bytes_read = 0;
        trace!("client read start base header");
        while bytes_read < base_header_size() {
            bytes_read += self.lock_socket
                              .read(&mut self.receive_buffer.bytes_mut()[bytes_read..])
                              .expect("cannot read");
        }
        //trace!("client read base header");
        let header_size = self.receive_buffer.header_size();
        //trace!("header size {}", header_size);
        while bytes_read < header_size {
            bytes_read +=
                self.lock_socket
                    .read(&mut self.receive_buffer
                                   .bytes_mut()[bytes_read..])
                    .expect("cannot read");
        }
        let end = self.receive_buffer.entry_size();
        //trace!("client read more header, entry size {}", end);
        while bytes_read < end {
            bytes_read +=
                self.lock_socket
                    .read(&mut self.receive_buffer
                                   .bytes_mut()[bytes_read..])
                    .expect("cannot read");
        }
    }
}

impl<V: Storeable + ?Sized + Debug> Store<V> for TcpStore<V> {
    fn insert(&mut self, key: OrderIndex, val: EntryContents<V>) -> InsertResult {
        let request_id = Uuid::new_v4();
        *self.send_buffer = val.clone_entry();
        assert_eq!(self.send_buffer.kind & EntryKind::Layout, EntryKind::Data);
        {
            let entr = unsafe { self.send_buffer.as_data_entry_mut() };
            entr.flex.loc = key;
            entr.id = request_id.clone();
        }
        let socket_id = self.socket_id(key.0);
        // let request_id = $request_id;
        trace!("packet {:#?}", self.send_buffer);
        trace!("at {:?}", self.sockets[socket_id].local_addr());
        let start_time = precise_time_ns() as i64;
        while let Err(..) = self.sockets[socket_id].set_read_timeout(None) {} //TODO
        'send: loop {
            trace!("sending append");
            self.send_packet(socket_id);
            trace!("append sent");

            'receive: loop {
                self.read_packet(socket_id);
                trace!("got packet");
                // if self.receive_buffer.kind & EntryKind::ReadSuccess == EntryKind::ReadSuccess {
                //    trace!("invalid response r ReadSuccess at insert");
                //    continue 'receive
                // }
                match self.receive_buffer.kind & EntryKind::Layout {
                    EntryKind::Data => {
                        // TODO types?
                        trace!("correct response");
                        let entr = unsafe { self.receive_buffer.as_data_entry() };
                        if entr.id == request_id {
                            // let rtt = precise_time_ns() as i64 - start_time;
                            // self.rtt = ((self.rtt * 4) / 5) + (rtt / 5);
                            let sample_rtt = precise_time_ns() as i64 - start_time;
                            let diff = sample_rtt - self.rtt;
                            self.dev = self.dev + (diff.abs() - self.dev) / 4;
                            self.rtt = self.rtt + (diff * 4 / 5);
                            if entr.id == request_id {
                                trace!("write success @ {:?}", entr.flex.loc);
                                trace!("wrote packet {:#?}", self.receive_buffer);
                                return Ok(entr.flex.loc);
                            }
                            trace!("already written");
                            return Err(InsertErr::AlreadyWritten);
                        } else {
                            println!("packet {:?}", self.receive_buffer);
                            continue 'receive;
                        }
                    }
                    EntryKind::Multiput => {
                        match self.receive_buffer.contents() {
                            EntryContents::Multiput { columns, .. } => {
                                if columns.contains(&key) {
                                    return Err(InsertErr::AlreadyWritten);
                                }
                                continue 'receive;
                            }
                            _ => unreachable!(),
                        };
                    }
                    v => {
                        trace!("invalid response {:?}", v);
                        continue 'receive;
                    }
                }
            }
        }
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        // assert!(Storeable::size<V>() <= MAX_DATA_LEN); //TODO size

        // let request_id = Uuid::new_v4();
        self.send_buffer.kind = EntryKind::Read;
        unsafe {
            self.send_buffer.as_data_entry_mut().flex.loc = key;
            self.send_buffer.id = mem::zeroed();
        };
        let socket_id = self.socket_id(key.0);
        // self.send_buffer.id = request_id.clone();

        trace!("at {:?}", self.sockets[socket_id].local_addr());
        // while let Err(..) = self.socket.set_read_timeout(Some(Duration::new(0, RTT as u32))) {} //TODO
        'send: loop {
            trace!("sending get");
            self.send_packet(socket_id);

            // thread::sleep(Duration::new(0, SLEEP_NANOS)); //TODO

            self.read_packet(socket_id);
            trace!("correct addr");
            match self.receive_buffer.kind {
                EntryKind::ReadData => {
                    // TODO validate...
                    // TODO base on loc instead?
                    let loc = unsafe { self.receive_buffer.as_data_entry_mut().flex.loc };
                    if loc == key {
                        trace!("correct response");
                        trace!("packet {:#?}", self.receive_buffer);
                        return Ok(*self.receive_buffer.clone());
                    }
                    trace!("wrong loc {:?}, {:?} expected {:?}",
                           self.receive_buffer,
                           loc,
                           key);
                    continue 'send;
                }
                EntryKind::ReadMulti => {
                    // TODO base on loc instead?
                    if unsafe {
                        self.receive_buffer
                            .as_multi_entry_mut()
                            .multi_contents_mut()
                            .columns
                            .contains(&key)
                    } {
                        trace!("correct response");
                        return Ok(*self.receive_buffer.clone());
                    }
                    trace!("wrong loc {:?}, expected {:?}", self.receive_buffer, key);
                    continue 'send;
                }
                EntryKind::NoValue => {
                    if unsafe { self.receive_buffer.as_data_entry_mut().flex.loc } == key {
                        trace!("correct response");
                        return Err(GetErr::NoValue);
                    }
                    trace!("wrong loc {:?}, expected {:?}", self.receive_buffer, key);
                    continue 'send;
                }
                k => {
                    trace!("invalid response, {:?}", k);
                    continue 'send;
                }
            }
        }
    }

    fn multi_append(&mut self,
                    chains: &[OrderIndex],
                    data: &V,
                    deps: &[OrderIndex])
                    -> InsertResult {
        let request_id = Uuid::new_v4();

        *self.send_buffer = EntryContents::Multiput {
                                data: data,
                                uuid: &request_id,
                                columns: chains,
                                deps: deps,
                            }
                            .clone_entry();
        // self.send_buffer.kind = EntryKind::Multiput;
        self.send_buffer.id = request_id.clone();
        trace!("Tpacket {:#?}", self.send_buffer);

        let socket_id = self.socket_id(chains[0].0);
        let single_node = chains.iter()
                                .fold(true, |a, &(o, _)| a & (self.socket_id(o) == socket_id));
        if single_node {
            self.single_node_multiappend(&request_id, socket_id)
        } else {
            self.multi_node_multiappend(&request_id, chains)
        }
    }
}

impl<V: Storeable + ?Sized> Clone for TcpStore<V> {
    // TODO should not actually be clone...
    fn clone(&self) -> Self {
        let &TcpStore { ref lock_socket,
                        ref sockets,
                        ref receive_buffer,
                        ref send_buffer,
                        _pd,
                        rtt,
                        dev,
                        .. } = self;
        TcpStore {
            lock_socket: TcpStream::connect(lock_socket.peer_addr().expect("cannot get addr"))
                             .expect("cannot clone"),
            sockets: sockets.iter()
                            .map(|s| {
                                TcpStream::connect(s.peer_addr().expect("cannot get addr"))
                                    .expect("cannot clone")
                            })
                            .collect(),
            receive_buffer: receive_buffer.clone(),
            send_buffer: send_buffer.clone(),
            rtt: rtt,
            dev: dev,
            _pd: _pd,
        }
    }
}

impl<V: ?Sized> Drop for TcpStore<V> {
    fn drop(&mut self) {
        use std::net::Shutdown::Both;
        for _ in self.sockets.iter_mut().map(|s| s.shutdown(Both)) {}
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use prelude::*;

    use std::cell::RefCell;
    use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
    use std::collections::HashMap;
    use std::collections::hash_map::Entry::{Occupied, Vacant};
    use std::io::{self, Read, Write};
    use std::mem;
    use std::net::SocketAddr;
    use std::os::unix::io::AsRawFd;
    use std::thread;
    use std::rc::Rc;

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
                    trace!("got accept");
                    match self.acceptor.accept() {
                        Ok(None) => trace!("false accept"),
                        Err(e) => panic!("error {}", e),
                        Ok(Some((socket, addr))) => {
                            let _ = socket.set_keepalive(Some(1));
                            let _ = setsockopt(socket.as_raw_fd(), TcpNoDelay, &true);
                            // let _ = socket.set_tcp_nodelay(true);
                            let next_client_id = self.clients.len() + 1;
                            let client_token = mio::Token(next_client_id);
                            trace!("new client {:?}, {:?}", next_client_id, addr);
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
                    // trace!("got client event");
                    if events.is_error() {
                        let err = self.clients
                                      .get_mut(&client_token)
                                      .ok_or(::std::io::Error::new(::std::io::ErrorKind::Other,
                                                                   "socket does not exist"))
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
                        //trace!("gonna write from client {:?}", client_token);
                        let finished_write = client.write_packet();
                        if finished_write {
                            trace!("finished write from client {:?}", client_token);
                            mio::EventSet::readable()
                        } else {
                            //trace!("keep writing from client {:?}", client_token);
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
                    //trace!("wrote {} bytes", s);
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
                horizon: HashMap::new(),
                last_lock: 0,
            }
        }

        fn handle_op(&mut self, val: &mut Entry<()>) {
            let kind = val.kind;
            if let EntryKind::Multiput = kind & EntryKind::Layout {
                // trace!("multiput {:?}", val);
                trace!("multiput");
                if val.lock_num() == 0 || val.lock_num() == self.last_lock + 1 {
                    {
                        if val.lock_num() == self.last_lock + 1 {
                            trace!("right lock {}", val.lock_num());
                            self.last_lock += 1;
                        }
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
                        // trace!("appended at {:?}", loc);
                    }
                } else { trace!("wrong lock {} expected {}", val.lock_num(), self.last_lock + 1); }
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


    #[allow(non_upper_case_globals)]
    fn new_store<V: ::std::fmt::Debug + Storeable>(_: Vec<OrderIndex>) -> TcpStore<V>
        where V: Clone
    {
        static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

        let handle = thread::spawn(move || {
            const addr_str: &'static str = "0.0.0.0:13266";
            let addr = addr_str.parse().expect("invalid inet address");
            let mut event_loop = EventLoop::new().unwrap();
            let server = Server::new(&addr, &mut event_loop);
            if let Ok(mut server) = server {
                SERVERS_READY.fetch_add(1, Ordering::Release);
                trace!("starting server");
                event_loop.run(&mut server);
            }
            trace!("server already started");
            return;
        });
        mem::forget(handle);

        while SERVERS_READY.load(Ordering::Acquire) < 1 {}

        // const addr_str: &'static str = "172.28.229.152:13265";
        // const addr_str: &'static str = "10.21.7.4:13265";
        const addr_str: &'static str = "127.0.0.1:13266";
        let store = TcpStore::new(addr_str, addr_str).expect("cannot create store");
        trace!("store @ {:?} connected to {:?}",
               store.sockets[0].local_addr(),
               store.sockets.iter().map(|s| s.peer_addr()).collect::<Vec<_>>());
        store
    }

    general_tests!(super::new_store);

    #[test]
    fn multi_server_dep() {
        let store = init_multi_servers();
        let horizon = HashMap::new();
        let map = Rc::new(RefCell::new(HashMap::new()));
        let mut upcalls: HashMap<_, Box<for<'o, 'r> Fn(&'o _, &'r _) -> bool>> = HashMap::new();
        let re = map.clone();
        upcalls.insert(6.into(),
                       Box::new(move |_, &(k, v)| {
                           re.borrow_mut().insert(k, v);
                           true
                       }));
        upcalls.insert(7.into(), Box::new(|_, _| false));
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        let e1 = log.append(6.into(), &(0, 1), &*vec![]);
        assert_eq!(e1, (6.into(), 1.into()));
        let e2 = log.append(6.into(), &(1, 17), &*vec![]);
        assert_eq!(e2, (6.into(), 2.into()));
        let last_index = log.append(6.into(), &(32, 5), &*vec![]);
        assert_eq!(last_index, (6.into(), 3.into()));
        let en = log.append(7.into(), &(0, 0), &*vec![last_index]);
        assert_eq!(en, (7.into(), 1.into()));
        log.play_foward(7.into());
        assert_eq!(*map.borrow(),
                   [(0, 1), (1, 17), (32, 5)].into_iter().cloned().collect());
    }

    #[test]
    fn multi_server_threaded_multiput() {
        extern crate env_logger;
        let _ = env_logger::init();
        let store = init_multi_servers();
        let store2 = store.clone();
        let s = store.clone();
        let s1 = store.clone();
        let horizon = HashMap::new();
        let horizon2 = HashMap::new();
        let h = horizon.clone();
        let h1 = horizon.clone();
        let mut upcalls: HashMap<_, Box<for<'o, 'r> Fn(&'o _, &'r _) -> bool>> = HashMap::new();
        let mut upcalls2: HashMap<_, Box<for<'o, 'r> Fn(&'o _, &'r _) -> bool>> = HashMap::new();
        let populate_upcalls = |up: &mut HashMap<_, Box<for<'o, 'r> Fn(&'o _, &'r _) -> bool>>| {
            let map = Rc::new(RefCell::new(HashMap::new()));
            let re1 = map.clone();
            let re2 = map.clone();
            up.insert(17.into(), Box::new(move |_, &(k, v)| {
                trace!("MapEntry({:?}, {:?})", k, v);
                re1.borrow_mut().insert(k, v);
                true
            }));
            up.insert(18.into(), Box::new(move |_, &(k, v)| {
                trace!("MapEntry({:?}, {:?})", k, v);
                re2.borrow_mut().insert(k, v);
                true
            }));
            map
        };
        let map1 = populate_upcalls(&mut upcalls);
        let map2 = populate_upcalls(&mut upcalls2);

        let mut log = FuzzyLog::new(store, horizon, upcalls);
        let mut log2 = FuzzyLog::new(store2, horizon2, upcalls2);
        //try_multiput(&mut self, offset: u32, mut columns: Vec<(order, V)>, deps: Vec<OrderIndex>)

        let join = thread::spawn(move || {
            let mut log = FuzzyLog::new(s, h, HashMap::new());
            log.append(17.into(), &(31, 36), &[]);
            for i in 0..10 {
                let change = &*vec![17.into(), 18.into()];
                let data = &(i * 2, i * 2);
                log.multiappend(change.clone(), data, &[])
            }
        });
        let join1 = thread::spawn(|| {
            let mut log = FuzzyLog::new(s1, h1, HashMap::new());
            log.append(17.into(), &(72, 21), &[]);
            for i in 0..10 {
                let change = &*vec![17.into(), 18.into()];
                let data = &(i * 2 + 1, i * 2 + 1);
                log.multiappend(change, data, &[]);
            }
        });
        join1.join().unwrap();
        join.join().unwrap();

        log.play_foward(17.into());
        log2.play_foward(18.into());

        let cannonical_map = {
            let mut map = HashMap::new();
            for i in 0..20 {
                map.insert(i, i);
            }
            map.insert(72, 21);
            map.insert(31, 36);
            map
        };
        assert_eq!(*map1.borrow(), cannonical_map);
        assert_eq!(*map2.borrow(), *map1.borrow());
    }

    fn init_multi_servers<V: ?Sized + ::std::fmt::Debug + Storeable>() -> TcpStore<V> {
        static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

        const lock_addr_str: &'static str = "0.0.0.0:13271";
        let handle = thread::spawn(move || {
            let addr = lock_addr_str.parse().expect("invalid inet address");
            let mut event_loop = EventLoop::new().unwrap();
            let server = Server::new(&addr, &mut event_loop);
            if let Ok(mut server) = server {
                SERVERS_READY.fetch_add(1, Ordering::Release);
                trace!("starting server @ {:?}", addr);
                event_loop.run(&mut server);
            }
            return;
        });
        mem::forget(handle);

        const addr_strs: &'static [&'static str] = &["0.0.0.0:13272", "0.0.0.0:13273"];

        for addr in addr_strs {
            let handle = thread::spawn(move || {
                let addr = addr.parse().expect("invalid inet address");
                let mut event_loop = EventLoop::new().unwrap();
                let server = Server::new(&addr, &mut event_loop);
                if let Ok(mut server) = server {
                    trace!("starting server @ {:?}", addr);
                    SERVERS_READY.fetch_add(1, Ordering::Release);
                    event_loop.run(&mut server);
                }
                trace!("server @ {:?} already started", addr);
                return;
            });
            mem::forget(handle);
        }


        while SERVERS_READY.load(Ordering::Acquire) < addr_strs.len() + 1 {}

        TcpStore::new(lock_addr_str,
                      &*addr_strs.iter()
                                 .map(|s| s.parse().expect(""))
                                 .collect::<Vec<SocketAddr>>())
            .expect("cannot create store")
    }

}
