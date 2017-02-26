#![allow(unused_imports)]

use prelude::*;

use std::fmt::Debug;
// use std::marker::{Unsize, PhantomData};
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::mem::{self};
use std::net::{SocketAddr, TcpStream};
// use std::ops::CoerceUnsized;

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
                .read(&mut self.receive_buffer.sized_bytes_mut()[bytes_read..])
                .expect("cannot read");
        }
        trace!("client read base header");
        let header_size = self.receive_buffer.header_size();
        trace!("header size {}", header_size);
        while bytes_read < header_size {
            bytes_read += self.socket.read(&mut self.receive_buffer
                .sized_bytes_mut()[bytes_read..])
                .expect("cannot read");
        }
        let end = self.receive_buffer.entry_size();
        trace!("client read more header, entry size {}", end);
        while bytes_read < end {
            bytes_read += self.socket.read(&mut self.receive_buffer
                .sized_bytes_mut()[bytes_read..])
                .expect("cannot read");
        }
    }

    fn send_packet(&mut self) {
        let send_size = self.send_buffer.entry_size();
        trace!("send size {}", send_size);
        self.socket
            .write_all(&self.send_buffer.bytes()[..send_size])
            .expect("cannot send");
        //self.socket.write_all(&[0u8; 6]).expect("cannot send");
        //self.socket.flush();
    }
}

impl<V: Storeable + ?Sized + Debug> Store<V> for TcpStore<V> {
    fn insert(&mut self, key: OrderIndex, val: EntryContents<V>) -> InsertResult {
        let request_id = Uuid::new_v4();
        *self.send_buffer = val.clone_entry();
        assert_eq!(self.send_buffer.kind.layout(), EntryLayout::Data);
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
                // if self.receive_buffer.kind.contains(EntryKind::ReadSuccess) {
                //    trace!("invalid response r ReadSuccess at insert");
                //    continue 'receive
                // }
                match self.receive_buffer.kind.layout() {
                    EntryLayout::Data => {
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
                    EntryLayout::Multiput => {
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
                EntryKind::ReadMulti | EntryKind::ReadSenti=> {
                    // TODO base on loc instead?
                    if self.receive_buffer.locs().contains(&key) {
                        trace!("correct response");
                        return Ok(*self.receive_buffer.clone());
                    }
                    trace!("wrong loc {:?}, expected {:?}", self.receive_buffer, key);
                    continue 'send;
                }
                EntryKind::NoValue => {
                    if unsafe { self.receive_buffer.as_data_entry_mut().flex.loc } == key {
                        let last_valid_loc = self.receive_buffer.dependencies()[0].1;
                        trace!("correct response");
                        return Err(GetErr::NoValue(last_valid_loc));
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
                match self.receive_buffer.kind.layout() {
                    EntryLayout::Multiput => {
                        // TODO types?
                        trace!("correct response");
                        trace!("id {:?}", self.receive_buffer.id);
                        if self.receive_buffer.id == request_id {
                            trace!("multiappend success");
                            let sample_rtt = precise_time_ns() as i64 - start_time;
                            let diff = sample_rtt - self.rtt;
                            self.dev = self.dev + (diff.abs() - self.dev) / 4;
                            self.rtt = self.rtt + (diff * 4 / 5);
                            return Ok(OrderIndex(0.into(), 0.into())); //TODO
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
        self.send_buffer.id = request_id.clone();
        trace!("Tpacket {:#?}", self.send_buffer);

        let start_time = precise_time_ns() as i64;
        'send: loop {
            trace!("sending");
            self.send_packet();

            'receive: loop {
                self.read_packet();
                trace!("got packet");
                match self.receive_buffer.kind.layout() {
                    EntryLayout::Multiput => {
                        // TODO types?
                        trace!("correct response");
                        trace!("id {:?}", self.receive_buffer.id);
                        if self.receive_buffer.id == request_id {
                            trace!("multiappend success");
                            let sample_rtt = precise_time_ns() as i64 - start_time;
                            let diff = sample_rtt - self.rtt;
                            self.dev = self.dev + (diff.abs() - self.dev) / 4;
                            self.rtt = self.rtt + (diff * 4 / 5);
                            return Ok(OrderIndex(0.into(), 0.into())); //TODO
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
pub mod t_test {
    use super::*;

    use std::sync::atomic::{AtomicIsize, ATOMIC_ISIZE_INIT, Ordering};
    use std::mem;
    use std::thread;

    use mio::deprecated::EventLoop;

    use servers::tcp::Server;

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
                let server = Server::new(&addr, 0, 1, &mut event_loop);
                if let Ok(mut server) = server {
                    SERVERS_READY.fetch_add(1, Ordering::Release);
                    trace!("starting server");
                    event_loop.run(&mut server).unwrap();
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
