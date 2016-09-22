#![allow(unused_imports)]

use prelude::*;

use std::fmt::Debug;
// use std::marker::{Unsize, PhantomData};
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::mem::{self};
use std::net::{SocketAddr, TcpStream};
// use std::ops::CoerceUnsized;

use net2::TcpStreamExt;

// use mio::buf::{SliceBuf, MutSliceBuf};
// use mio::udp::UdpSocket;
// use mio::unix;

use time::precise_time_ns;

// #[derive(Debug)]
pub struct TcpStore<V: ?Sized> {
    socket: TcpStream,
    receive_buffer: Box<Entry<V>>,
    send_buffer: Box<Entry<V>>,
    rtt: i64,
    dev: i64,
    _pd: PhantomData<V>,
}

//const SLEEP_NANOS: u32 = 8000; //TODO user settable
const RTT: i64 = 80000;

impl<V: Storeable + ?Sized> TcpStore<V> {
    pub fn new(server_addr: SocketAddr) -> Self {
        unsafe {
            TcpStore {
                socket: TcpStream::connect(server_addr).expect("unable to open store"),
                receive_buffer: Box::new(mem::zeroed()),
                send_buffer: Box::new(mem::zeroed()),
                _pd: Default::default(),
                rtt: RTT,
                dev: 0,
            }
        }
    }

    fn read_packet(&mut self) {
        let mut bytes_read = 0;
        trace!("client read start base header");
        while bytes_read < base_header_size() {
            bytes_read += self.socket
                .read(&mut self.receive_buffer.bytes_mut()[bytes_read..])
                .expect("cannot read");
        }
        trace!("client read base header");
        let header_size = self.receive_buffer.header_size();
        trace!("header size {}", header_size);
        while bytes_read < header_size {
            bytes_read += self.socket.read(&mut self.receive_buffer
                .bytes_mut()[bytes_read..])
                .expect("cannot read");
        }
        let end = self.receive_buffer.entry_size();
        trace!("client read more header, entry size {}", end);
        while bytes_read < end {
            bytes_read += self.socket.read(&mut self.receive_buffer
                .bytes_mut()[bytes_read..])
                .expect("cannot read");
        }
    }

    fn send_packet(&mut self) {
        let send_size = self.send_buffer.entry_size();
        trace!("send size {}", send_size);
        self.socket
            .write_all(&self.send_buffer.bytes()[..send_size])
            .expect("cannot send");
        //self.socket.flush();
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
        // let request_id = $request_id;
        trace!("packet {:#?}", self.send_buffer);
        trace!("at {:?}", self.socket.local_addr());
        let start_time = precise_time_ns() as i64;
        while let Err(..) = self.socket.set_read_timeout(None) {} //TODO
        'send: loop {
            trace!("sending append");
            self.send_packet();
            trace!("append sent");

            'receive: loop {
                self.read_packet();
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
        // self.send_buffer.id = request_id.clone();

        trace!("at {:?}", self.socket.local_addr());
        // while let Err(..) = self.socket.set_read_timeout(Some(Duration::new(0, RTT as u32))) {} //TODO
        'send: loop {
            trace!("sending get");
            self.send_packet();

            // thread::sleep(Duration::new(0, SLEEP_NANOS)); //TODO

            self.read_packet();
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
                    trace!("wrong loc {:?}, {:?} expected {:?}", self.receive_buffer, loc, key);
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

        // TODO find server

        trace!("multi_append from {:?}", self.socket.local_addr());
        let start_time = precise_time_ns() as i64;
        'send: loop {
            trace!("sending");
            self.send_packet();

            'receive: loop {
                self.read_packet();
                trace!("got packet");
                match self.receive_buffer.kind & EntryKind::Layout {
                    EntryKind::Multiput => {
                        // TODO types?
                        trace!("correct response");
                        trace!("id {:?}", self.receive_buffer.id);
                        if self.receive_buffer.id == request_id {
                            trace!("multiappend success");
                            let sample_rtt = precise_time_ns() as i64 - start_time;
                            let diff = sample_rtt - self.rtt;
                            self.dev = self.dev + (diff.abs() - self.dev) / 4;
                            self.rtt = self.rtt + (diff * 4 / 5);
                            return Ok((0.into(), 0.into())); //TODO
                        } else {
                            trace!("?? packet {:?}", self.receive_buffer);
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
}

impl<V: Storeable + ?Sized> Clone for TcpStore<V> {
    // TODO should not actually be clone...
    fn clone(&self) -> Self {
        let &TcpStore { ref socket, ref receive_buffer, ref send_buffer, _pd, rtt, dev, .. } = self;
        TcpStore {
            socket: TcpStream::connect(socket.peer_addr().expect("cannot get addr"))
                        .expect("cannot clone"), // TODO
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
        let _ = self.socket.shutdown(Both);
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use prelude::*;

    use std::sync::atomic::{AtomicIsize, ATOMIC_ISIZE_INIT, Ordering};
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
                            .ok_or(::std::io::Error::new(::std::io::ErrorKind::Other, "socket does not exist"))
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
                horizon: HashMap::new(),
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


    #[allow(non_upper_case_globals)]
    pub fn new_store<V: ::std::fmt::Debug>(_: Vec<OrderIndex>) -> TcpStore<V>
        where V: Clone
    {
        static SERVERS_READY: AtomicIsize = ATOMIC_ISIZE_INIT;

        let addr = if let Ok(addr_str) = ::std::env::var("DELOS_TCP_TEST_ADDR") {
            SERVERS_READY.fetch_add(1, Ordering::Release);
            addr_str.parse().expect("invalid inet address")
        } else {
            const addr_str: &'static str = "0.0.0.0:13265";
            let handle = thread::spawn(move || {
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
            addr_str.parse().expect("invalid inet address")
        };

        while SERVERS_READY.load(Ordering::Acquire) < 1 {}

        let store = TcpStore::new(addr);
        trace!("store @ {:?} connected to {:?}", store.socket.local_addr(), store.socket.peer_addr());
        store
    }

    general_tests!(super::new_store);

    // #[bench]
    // fn many_writes(b: &mut Bencher) {
        // let deps = &[];
        // let data = &48u64;
        // let entr = EntryContents::Data(data, deps);
        // let mut store = new_store(vec![]);
        // let mut i = 0;
        // b.iter(|| {
            // store.insert((21.into(), i.into()), entr.clone());
            // i.wrapping_add(1);
        // });
    // }

    // #[test]
    // fn test_external_write() {
    // let mut store = new_store(vec![]);
    // let mut send: Box<Packet<u64>> = Box::new(unsafe { mem::zeroed() });
    // send.data = EntryContents::Data(&48u64, &*vec![]).clone_entry();
    // let mut recv: Box<Packet<u64>> = Box::new(unsafe { mem::zeroed() });
    //
    // let res = store.insert_ref((1.into(), 1.into()), &mut *send, &mut *recv);
    // println!("res {:?}", res);
    // }
    //
    // #[bench]
    // fn external_write(b: &mut Bencher) {
    // let mut store = new_store(vec![]);
    // let mut send: Box<Packet<u64>> = Box::new(unsafe { mem::zeroed() });
    // send.data = EntryContents::Data(&48u64, &*vec![]).clone_entry();
    // let mut recv: Box<Packet<u64>> = Box::new(unsafe { mem::zeroed() });
    // b.iter(|| {
    // store.insert_ref((1.into(), 1.into()), &mut *send, &mut *recv)
    // });
    // }
    //
    // #[bench]
    // fn many_writes(b: &mut Bencher) {
    // let mut store = new_store(vec![]);
    // let mut i = 0;
    // b.iter(|| {
    // let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
    // store.insert((17.into(), i.into()), entr);
    // i.wrapping_add(1);
    // });
    // }
    //
    // #[bench]
    // fn bench_write(b: &mut Bencher) {
    // let mut store = new_store(vec![]);
    // b.iter(|| {
    // let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
    // store.insert((1.into(), 0.into()), entr)
    // });
    // }
    //
    // #[bench]
    // fn bench_sequential_writes(b: &mut Bencher) {
    // let mut store = new_store(vec![]);
    // b.iter(|| {
    // let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
    // let a = store.insert((0.into(), 0.into()), entr);
    // let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
    // let b = store.insert((0.into(), 1.into()), entr);
    // let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
    // let c = store.insert((0.into(), 2.into()), entr);
    // (a, b, c)
    // });
    // }
    //
    // #[bench]
    // fn bench_multistore_writes(b: &mut Bencher) {
    // let mut store_a = new_store(vec![]);
    // let mut store_b = new_store(vec![]);
    // let mut store_c = new_store(vec![]);
    // b.iter(|| {
    // let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
    // let a = store_a.insert((0.into(), 0.into()), entr);
    // let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
    // let b = store_b.insert((0.into(), 1.into()), entr);
    // let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
    // let c = store_c.insert((0.into(), 2.into()), entr);
    // (a, b, c)
    // });
    // }
    //
    // #[bench]
    // fn bench_rtt(b: &mut Bencher) {
    // use std::mem;
    // use mio::udp::UdpSocket;
    // const addr_str: &'static str = "10.21.7.4:13265";
    // let client = UdpSocket::v4().expect("unable to open client");
    // let addr = addr_str.parse().expect("invalid inet address");
    // let buff = Box::new([0u8; 4]);
    // let mut recv_buff = Box::new([0u8; 4096]);
    // b.iter(|| {
    // let a = {
    // let buff: &[u8] = &buff[..];
    // let buf = &mut SliceBuf::wrap(buff);
    // client.send_to(buf, &addr)
    // };
    // let mut recv = client.recv_from(&mut MutSliceBuf::wrap(&mut recv_buff[..]));
    // while let Ok(None) = recv {
    // recv = client.recv_from(&mut MutSliceBuf::wrap(&mut recv_buff[..]))
    // }
    // println!("rec");
    // a
    // });
    // }
    //
    // #[bench]
    // fn bench_mio_write(b: &mut Bencher) {
    // use mio::udp::UdpSocket;
    // let handle = spawn(move || {
    // const addr_str: &'static str = "0.0.0.0:13269";
    // let addr = addr_str.parse().expect("invalid inet address");
    // let receive = if let Ok(socket) =
    // UdpSocket::bound(&addr) {
    // socket
    // } else {
    // return
    // };
    // let mut buff = Box::new([0;4]);
    // 'server: loop {
    // let res = receive.recv_from(&mut MutSliceBuf::wrap(&mut buff[..]));
    // match res {
    // Err(e) => panic!("{}", e),
    // Ok(None) => {
    // continue 'server
    // }
    // Ok(Some(sa)) => {
    // let slice = &mut SliceBuf::wrap(&buff[..]);
    // receive.send_to(slice, &sa).expect("unable to ack");
    // }
    // }
    // }
    // });*/
    //
    // const addr_str: &'static str = "172.28.229.152:13266";
    // let client = UdpSocket::v4().expect("unable to open client");
    // let addr = addr_str.parse().expect("invalid inet address");
    // let mut buff = Box::new([0;4096]);
    // b.iter(|| {
    // let a = client.send_to(&mut SliceBuf::wrap(&buff[..]), &addr);
    //   let mut recv = client.recv_from(&mut MutSliceBuf::wrap(&mut buff[..]));
    //   while let Ok(None) = recv {
    //       recv = client.recv_from(&mut MutSliceBuf::wrap(&mut buff[..]))
    //   }
    // a
    // });
    // }
}
