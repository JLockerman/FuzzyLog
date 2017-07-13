
use prelude::*;

use std::cmp;
use std::collections::HashSet;
use std::fmt::Debug;
// use std::marker::{Unsize, PhantomData};
use std::io::{Read, Write, Result as IoResult};
use std::marker::PhantomData;
use std::mem;
use std::net::{TcpStream, ToSocketAddrs};
// use std::ops::CoerceUnsized;

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

//const SLEEP_NANOS: u32 = 8000; //TODO user settable
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
                .read(&mut self.receive_buffer
                    .sized_bytes_mut()[bytes_read..base_header_size()])
                .expect("cannot read");
        }
        //trace!("client read base header");
        let header_size = self.receive_buffer.header_size();
        trace!("header size {}", header_size);
        while bytes_read < header_size {
            bytes_read +=
                self.sockets[socket_id]
                    .read(&mut self.receive_buffer
                                   .sized_bytes_mut()[bytes_read..header_size])
                    .expect("cannot read");
        }
        let end = self.receive_buffer.entry_size();
        //trace!("client read more header, entry size {}", end);
        while bytes_read < end {
            bytes_read +=
                self.sockets[socket_id]
                    .read(&mut self.receive_buffer
                                   .sized_bytes_mut()[bytes_read..end])
                    .expect("cannot read");
        }
    }

    fn send_packet(&mut self, socket_id: usize) {
        let send_size = self.send_buffer.entry_size();
        //trace!("send size {}", send_size);
        self.sockets[socket_id]
            .write_all(&self.send_buffer.bytes()[..send_size])
            .expect("cannot send");
        self.sockets[socket_id].write_all(&[0u8; 6]).expect("cannot send");
        // self.socket.flush();
    }

    fn socket_id(&self, chain: order) -> usize {
        <u32 as From<order>>::from(chain) as usize % self.sockets.len()
    }

    fn unlock(&mut self, socket_id: usize) {
        self.send_buffer.kind.remove(EntryKind::Multiput);
        self.send_buffer.kind.insert(EntryKind::Sentinel);
        self.send_buffer.kind.insert(EntryKind::Unlock);
        self.send_buffer.data_bytes = 0;
        self.send_buffer.dependency_bytes = 0;
        self.sockets[socket_id].write_all(self.send_buffer.bytes()).expect("cannot send");
        self.sockets[socket_id].write_all(&[0u8; 6]).expect("cannot send");
    }

    fn emplace_multi_node(&mut self, socket_id: usize) {
        trace!("sending mnm actual {:?}", self.send_buffer.id);
        assert!(self.send_buffer.kind.layout() == EntryLayout::Multiput
            || self.send_buffer.kind.layout() == EntryLayout::Sentinel);
        self.send_packet(socket_id);
    }

    fn sockets_for_multi(&self) -> HashSet<usize> {
        let mut nodes_to_lock: Vec<_> = (0..self.sockets.len()).map(|_| false).collect();
        {
            let chains = self.send_buffer.locs();
            for &OrderIndex(o, _) in chains {
                nodes_to_lock[self.socket_id(o)] = true;
                trace!("{:?} => socket {:?}", o, self.socket_id(o));
            }
        }

        nodes_to_lock.into_iter()
            .enumerate()
            .filter_map(|(i, present)| if present { Some(i) } else { None })
            .collect()
    }

    fn send_skeens1(&mut self, socket: usize) {
        trace!("skeens1 sock {:?}", socket);
        assert!(self.send_buffer.kind.contains(EntryKind::TakeLock | EntryKind::NewMultiPut));

        self.emplace_multi_node(socket)
    }

    fn recv_skeens1(&mut self, socket: usize) -> u64 {
        'receive: loop {
            self.read_packet(socket);
            trace!("got packet");
            match self.receive_buffer.kind.layout() {
                EntryLayout::Sentinel => {
                    // TODO types?
                    trace!("correct response");
                    trace!("id {:?}", self.receive_buffer.id);
                    if self.receive_buffer.id == self.send_buffer.id {
                        assert!(self.receive_buffer.kind.contains(EntryKind::Skeens1Queued));
                        let max_ts = self.receive_buffer.locs()
                            .iter()
                            .map(|&OrderIndex(_, i)| u32::from(i) as u64)
                            .max()
                            .unwrap();
                        trace!("skeens1 success @ {:?} max_ts {:?}", socket, max_ts);
                        return max_ts;
                    } else {
                        // trace!("?? packet {:?}", self.receive_buffer);
                        continue 'receive;
                    }
                }
                v => {
                    trace!("M invalid response {:?}", v);
                    continue 'receive;
                }
            }
        }
    }

    fn send_skeens2(&mut self, socket: usize) {
        trace!("skeens2 sock {:?}", socket);
        assert!(self.send_buffer.kind.contains(
            EntryKind::TakeLock | EntryKind::NewMultiPut | EntryKind::Unlock));

        self.emplace_multi_node(socket)
    }

    fn recv_skeens2(&mut self, socket_id: usize, request_id: Uuid) {
        'receive: loop {
            self.read_packet(socket_id);
            trace!("M got packet");
            match self.receive_buffer.kind.layout() {
                EntryLayout::Multiput | EntryLayout::Sentinel => {
                    // TODO should also have lock-success
                    // TODO types?
                    trace!("M correct response");
                    trace!("M id {:?}", self.receive_buffer.id);
                    if self.receive_buffer.id == request_id {
                        if !self.receive_buffer.kind.contains(EntryKind::ReadSuccess) {
                            //trace!("M lock fail, resend");
                            //self.emplace_multi_node(socket_id);
                            //continue 'receive
                            panic!("Failure")
                        }
                        trace!("M multiappend success");
                        return
                    } else {
                        // trace!("?? packet {:?}", self.receive_buffer);
                        continue 'receive;
                    }
                }
                v => {
                    trace!("M invalid response {:?}", v);
                    continue 'receive;
                }
            }
        }
    }

    fn is_single_node_append<I: IntoIterator<Item=order>>(&self, chains: I) -> bool {
        let mut single = true;
        let mut socket_id = None;
        let chains = chains.into_iter().collect::<Vec<_>>();
        trace!("loop start {:?}", chains);
        for c in chains {
            trace!("{:?}, {:?}, {:?}, {:?}", single, c, socket_id, self.socket_id(c));
            if let Some(socket_id) = socket_id {
                single &= self.socket_id(c) == socket_id
            }
            else {
                socket_id = Some(self.socket_id(c))
            }
        }
        single
    }
}

impl<V: Storeable + ?Sized + Debug> Store<V> for TcpStore<V> {
    fn insert(&mut self, key: OrderIndex, val: EntryContents<V>) -> InsertResult {
        let request_id = Uuid::new_v4();
        *self.send_buffer = val.clone_entry();
        assert_eq!(self.send_buffer.kind, EntryKind::Data);
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
                // if self.receive_buffer.kind.contains(EntryKind::ReadSuccess) {
                //    trace!("invalid response r ReadSuccess at insert");
                //    continue 'receive
                // }
                match self.receive_buffer.kind.layout() {
                    EntryLayout::Data => {
                        // TODO types?
                        trace!("A correct response");
                        let entr = unsafe { self.receive_buffer.as_data_entry() };
                        if entr.id == request_id {
                            //FIXME currently we assume servers don't fail
                            //      so on finding the chain is locked we simply retry
                            //TODO multiserver op finish code
                            if !self.receive_buffer.kind.contains(EntryKind::ReadSuccess) {
                                trace!("A server is locked");
                                continue 'send
                            }
                            // let rtt = precise_time_ns() as i64 - start_time;
                            // self.rtt = ((self.rtt * 4) / 5) + (rtt / 5);
                            let sample_rtt = precise_time_ns() as i64 - start_time;
                            let diff = sample_rtt - self.rtt;
                            self.dev = self.dev + (diff.abs() - self.dev) / 4;
                            self.rtt = self.rtt + (diff * 4 / 5);
                            if entr.id == request_id {
                                trace!("A write success @ {:?}", entr.flex.loc);
                                trace!("A wrote packet {:#?}", self.receive_buffer);
                                return Ok(entr.flex.loc);
                            }
                            trace!("A already written");
                            return Err(InsertErr::AlreadyWritten);
                        } else {
                            println!("packet {:?}", self.receive_buffer);
                            continue 'receive;
                        }
                    }
                    EntryLayout::Multiput => {
                        trace!("got multi?");
                        match self.receive_buffer.contents() {
                            EntryContents::Multiput { columns, .. } => {
                                if columns.contains(&key) {
                                    return Err(InsertErr::AlreadyWritten);
                                }
                                trace!("irrelevent multi");
                                continue 'receive;
                            }
                            _ => unreachable!(),
                        };
                    }
                    _ => {
                        trace!("A invalid response {:?}", self.receive_buffer.kind);
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
            //TODO
            self.receive_buffer.kind.remove(EntryKind::TakeLock);
            if !self.receive_buffer.kind.contains(EntryKind::ReadSuccess) {
                if self.receive_buffer.kind == EntryKind::NoValue {
                    if unsafe { self.receive_buffer.as_data_entry().flex.loc } == key {
                        let last_valid_loc = self.receive_buffer.dependencies()[0].1;
                        trace!("{:?}", self.receive_buffer);
                        trace!("last entry at {:?}", self.receive_buffer.dependencies());
                        return Err(GetErr::NoValue(last_valid_loc));
                    }
                    trace!("wrong loc {:?}, expected {:?}", self.receive_buffer, key);
                    continue 'send;
                } else {
                    trace!("G failure, {:?}", self.receive_buffer.kind);
                    continue 'send;
                }
            }
            match self.receive_buffer.kind.layout() {
                EntryLayout::Data => {
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
                k @ EntryLayout::Multiput | k @ EntryLayout::Sentinel  => {
                    // TODO base on loc instead
                    if self.receive_buffer.locs().contains(&key) {
                        trace!("correct response {:?}", k);
                        trace!("packet {:#?}", self.receive_buffer);
                        return Ok(*self.receive_buffer.clone());
                    }
                    trace!("wrong loc {:?}, expected {:?}", self.receive_buffer, key);
                    continue 'send;
                }
                _ => {
                    trace!("G invalid response, {:?}", self.receive_buffer.kind);
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

        let single_node = self.is_single_node_append(chains.iter().map(|&OrderIndex(o, _)|  o));
        let indices =
            if single_node {
                trace!("M single");
                let mut ack_sockets = Vec::new();
                let socket_id = self.socket_id(chains[0].0);
                self.emplace_multi_node(socket_id);
                ack_sockets.push(socket_id);
                ack_sockets
            }
            else {
                self.send_buffer.kind.insert(EntryKind::TakeLock | EntryKind::NewMultiPut);
                let sockets: Vec<_> = self.sockets_for_multi().into_iter().collect();
                sockets.iter().fold((), |_, &socket| self.send_skeens1(socket));
                let mut max_ts = sockets.iter().fold(0, |max_ts, &socket|
                    cmp::max(max_ts, self.recv_skeens1(socket)));
                trace!("M skeens {:?}", max_ts);
                self.send_buffer.kind.remove(EntryKind::Multiput);
                self.send_buffer.kind.insert(EntryKind::Sentinel | EntryKind::Unlock);
                unsafe { self.send_buffer.as_multi_entry_mut().flex.lock = max_ts };
                sockets.iter().fold((), |_, &socket| self.send_skeens2(socket));
                sockets
            };

        indices.into_iter().fold((), |_, socket_id| self.recv_skeens2(socket_id, request_id));

        trace!("M done");

        return Ok(OrderIndex(0.into(), 0.into()));
    }

    fn dependent_multi_append(&mut self, chains: &[order],
        depends_on: &[order], data: &V,
        deps: &[OrderIndex]) -> InsertResult {
        let request_id = Uuid::new_v4();

        let mchains: Vec<_> = chains.into_iter()
            .map(|&c| OrderIndex(c, 0.into()))
            .chain(::std::iter::once(OrderIndex(0.into(), 0.into())))
            .chain(depends_on.iter().map(|&c| OrderIndex(c, 0.into())))
            .collect();

        *self.send_buffer = EntryContents::Multiput {
            data: data,
            uuid: &request_id,
            columns: &mchains,
            deps: deps,
        }.clone_entry();
        // self.send_buffer.kind = EntryKind::Multiput;
        self.send_buffer.id = request_id.clone();
        trace!("Tpacket {:#?}", self.send_buffer);

        trace!("D multi_append from {:?}", self.sockets[0].local_addr());

        let single_node = self.is_single_node_append(
            chains.into_iter().cloned().chain(depends_on.iter().cloned()));
        let indices =
            if single_node {
                trace!("D single s");
                self.send_buffer.kind.remove(EntryKind::TakeLock);
                let mut ack_sockets = Vec::new();
                let socket_id = self.socket_id(chains[0]);
                self.emplace_multi_node(socket_id);
                ack_sockets.push(socket_id);
                ack_sockets
            }
            else {
                self.send_buffer.kind.insert(EntryKind::TakeLock | EntryKind::NewMultiPut);
                let sockets = self.sockets_for_multi();
                let to_send = sockets.clone();
                let to_send = chains.iter().fold(to_send, |mut to_send, &chain| {
                    let socket_id = self.socket_id(chain);
                    if to_send.remove(&socket_id) {
                        self.emplace_multi_node(socket_id);
                    }
                    to_send
                });
                self.send_buffer.kind.remove(EntryKind::Multiput);
                self.send_buffer.kind.insert(EntryKind::Sentinel);
                to_send.iter().fold((), |_, &socket| self.send_skeens1(socket));
                let mut max_ts = sockets.iter().fold(0, |max_ts, &socket|
                    cmp::max(max_ts, self.recv_skeens1(socket)));
                trace!("D skeens {:?}", max_ts);
                self.send_buffer.kind.insert(EntryKind::Unlock);
                unsafe { self.send_buffer.as_multi_entry_mut().flex.lock = max_ts };
                sockets.iter().fold((), |_, &socket| self.send_skeens2(socket));
                sockets.into_iter().collect()
            };

        indices.into_iter().fold((), |_, socket_id| self.recv_skeens2(socket_id, request_id));

        trace!("D done");

        return Ok(OrderIndex(0.into(), 0.into()));
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

    use std::cell::RefCell;
    use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::thread;
    use std::rc::Rc;

    use mio;

    #[test]
    fn multi_server_dep() {
        let store = init_multi_servers();
        let horizon = HashMap::new();
        let map = Rc::new(RefCell::new(HashMap::new()));
        let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u _, &'o _, &'r _) -> bool>> = HashMap::new();
        let re = map.clone();
        upcalls.insert(56.into(),
                       Box::new(move |_, _, &(k, v)| {
                           re.borrow_mut().insert(k, v);
                           true
                       }));
        upcalls.insert(57.into(), Box::new(|_, _, _| false));
        let mut log = FuzzyLog::new(store, horizon, upcalls);
        let e1 = log.append(56.into(), &(0, 1), &*vec![]);
        assert_eq!(e1, OrderIndex(56.into(), 1.into()));
        let e2 = log.append(56.into(), &(1, 17), &*vec![]);
        assert_eq!(e2, OrderIndex(56.into(), 2.into()));
        let last_index = log.append(56.into(), &(32, 5), &*vec![]);
        assert_eq!(last_index, OrderIndex(56.into(), 3.into()));
        let en = log.append(57.into(), &(0, 0), &*vec![last_index]);
        assert_eq!(en, OrderIndex(57.into(), 1.into()));
        log.play_foward(57.into());
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
        let mut upcalls: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u _, &'o _, &'r _) -> bool>> = HashMap::new();
        let mut upcalls2: HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u _, &'o _, &'r _) -> bool>> = HashMap::new();
        let populate_upcalls = |up: &mut HashMap<_, Box<for<'u, 'o, 'r> Fn(&'u _, &'o _, &'r _) -> bool>>| {
            let map = Rc::new(RefCell::new(HashMap::new()));
            let re1 = map.clone();
            let re2 = map.clone();
            up.insert(67.into(), Box::new(move |_, _, &(k, v)| {
                trace!("MapEntry({:?}, {:?})", k, v);
                re1.borrow_mut().insert(k, v);
                true
            }));
            up.insert(68.into(), Box::new(move |_, _, &(k, v)| {
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
            log.append(67.into(), &(31, 36), &[]);
            for i in 0..10 {
                let change = &*vec![67.into(), 68.into()];
                let data = &(i * 2, i * 2);
                log.multiappend(change.clone(), data, &[])
            }
        });
        let join1 = thread::spawn(|| {
            let mut log = FuzzyLog::new(s1, h1, HashMap::new());
            log.append(67.into(), &(72, 21), &[]);
            for i in 0..10 {
                let change = &*vec![67.into(), 68.into()];
                let data = &(i * 2 + 1, i * 2 + 1);
                log.multiappend(change, data, &[]);
            }
        });
        join1.join().unwrap();
        join.join().unwrap();

        log.play_foward(67.into());
        log2.play_foward(68.into());

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

        #[allow(non_upper_case_globals)]
        const lock_addr_str: &'static str = "0.0.0.0:13271";
        let _ = thread::spawn(move || {
            let addr = lock_addr_str.parse().expect("invalid inet address");
            let acceptor = mio::tcp::TcpListener::bind(&addr);
            if let Ok(acceptor) = acceptor {
                trace!("starting server @ {:?}", addr);
                ::servers2::tcp::run(acceptor, 0, 1, 2, &SERVERS_READY)
            }
            trace!("server @ {:?} already started", addr);
            return;
        });

        #[allow(non_upper_case_globals)]
        const addr_strs: &'static [&'static str] = &["0.0.0.0:13272", "0.0.0.0:13273"];

        for (i, addr) in addr_strs.iter().enumerate() {
            let _ = thread::spawn(move || {
                let i = i as u32;
                let num_servers = addr_strs.len() as u32;
                let addr = addr.parse().expect("invalid inet address");
                let acceptor = mio::tcp::TcpListener::bind(&addr);
                if let Ok(acceptor) = acceptor {
                    trace!("starting server @ {:?}", addr);
                    ::servers2::tcp::run(acceptor, i, num_servers, 2, &SERVERS_READY)
                }
                trace!("server @ {:?} already started", addr);
                return;
            });
        }


        while SERVERS_READY.load(Ordering::Acquire) < addr_strs.len() + 1 {}

        TcpStore::new(lock_addr_str,
                      &*addr_strs.iter()
                                 .map(|s| s.parse().expect(""))
                                 .collect::<Vec<SocketAddr>>())
            .expect("cannot create store")
    }

    #[allow(non_upper_case_globals)]
    fn new_store<V: ::std::fmt::Debug + Storeable>(_: Vec<OrderIndex>) -> TcpStore<V>
        where V: Clone {
        init_multi_servers::<V>()
    }

    general_tests!(super::new_store);
}
