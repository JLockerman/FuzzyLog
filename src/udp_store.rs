#![allow(unused_imports)]

use prelude::*;

use std::fmt::Debug;
//use std::marker::{Unsize, PhantomData};
use std::marker::PhantomData;
use std::mem;
use std::net::{SocketAddr, UdpSocket};
// use std::ops::CoerceUnsized;
//use std::time::Duration;

// use mio::buf::{SliceBuf, MutSliceBuf};
// use mio::udp::UdpSocket;
// use mio::unix;

use time::precise_time_ns;

// #[derive(Debug)]
pub struct UdpStore<V: ?Sized> {
    socket: UdpSocket,
    server_addr: SocketAddr,
    receive_buffer: Box<Entry<V>>,
    send_buffer: Box<Entry<V>>,
    rtt: i64,
    dev: i64,
    _pd: PhantomData<V>,
}

//const SLEEP_NANOS: u32 = 8000; //TODO user settable
const RTT: i64 = 80000;

impl<V: Storeable + ?Sized> UdpStore<V> {
    pub fn new(server_addr: SocketAddr) -> UdpStore<V> {
        unsafe {
            use std::marker::PhantomData;
            UdpStore {
                socket: UdpSocket::bind("0.0.0.0:0").expect("unable to open store"),
                server_addr: server_addr,
                receive_buffer: Box::new(mem::zeroed()),
                send_buffer: Box::new(mem::zeroed()),
                _pd: PhantomData,
                rtt: RTT,
                dev: 0,
            }
        }
    }
}

impl<V: Storeable + ?Sized + Debug> Store<V> for UdpStore<V> {
    fn insert(&mut self, key: OrderIndex, val: EntryContents<V>) -> InsertResult {
        let (this, key, val) = (self, key, val);

        let request_id = Uuid::new_v4();
        *this.send_buffer = val.clone_entry();
        assert_eq!(this.send_buffer.kind.layout(), EntryLayout::Data);
        {
            let entr = unsafe { this.send_buffer.as_data_entry_mut() };
            entr.flex.loc = key;
            entr.id = request_id.clone();
        }
        // let request_id = $request_id;
        trace!("packet {:#?}", this.send_buffer);
        trace!("at {:?}", this.socket.local_addr());
        let start_time = precise_time_ns() as i64;
        while let Err(..) = this.socket.set_read_timeout(None) {} //TODO
        'send: loop {
            {
                trace!("sending append");
                this.socket
                    .send_to(this.send_buffer.bytes(), &this.server_addr)
                    .expect("cannot send insert"); //TODO
            }

            'receive: loop {
                let (_, addr) = {
                    this.socket
                        .recv_from(this.receive_buffer.sized_bytes_mut())
                        .expect("unable to receive ack") //TODO
                        //precise_time_ns() as i64 - start_time < this.rtt + 4 * this.dev
                };
                trace!("got packet");
                if addr == this.server_addr {
                    // if this.receive_buffer.kind & EntryKind::ReadSuccess == EntryKind::ReadSuccess {
                    //    trace!("invalid response r ReadSuccess at insert");
                    //    continue 'receive
                    // }
                    match this.receive_buffer.kind.layout() {
                        EntryLayout::Data => {
                            // TODO types?
                            trace!("correct response");
                            let entr = unsafe { this.receive_buffer.as_data_entry() };
                            if entr.id == request_id {
                                // let rtt = precise_time_ns() as i64 - start_time;
                                // this.rtt = ((this.rtt * 4) / 5) + (rtt / 5);
                                let sample_rtt = precise_time_ns() as i64 - start_time;
                                let diff = sample_rtt - this.rtt;
                                this.dev = this.dev + (diff.abs() - this.dev) / 4;
                                this.rtt = this.rtt + (diff * 4 / 5);
                                if entr.id == request_id {
                                    trace!("write success @ {:?}", entr.flex.loc);
                                    trace!("packet {:#?}", this.receive_buffer);
                                    return Ok(entr.flex.loc);
                                }
                                trace!("already written");
                                return Err(InsertErr::AlreadyWritten);
                            } else {
                                trace!("wrong packet {:?}", this.receive_buffer);
                                continue 'receive;
                            }
                        }
                        EntryLayout::Multiput => {
                            match this.receive_buffer.contents() {
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
                } else {
                    trace!("unexpected addr {:?}, expected {:?}",
                           addr,
                           this.server_addr);
                    continue 'receive;
                }
            }
        }
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        let (this, key) = (self, key);
        // assert!(Storeable::size<V>() <= MAX_DATA_LEN); //TODO size

        // let request_id = Uuid::new_v4();
        this.send_buffer.kind = EntryKind::Read;
        unsafe {
            this.send_buffer.as_data_entry_mut().flex.loc = key;
            this.send_buffer.id = mem::zeroed();
        };
        // this.send_buffer.id = request_id.clone();

        trace!("at {:?}", this.socket.local_addr());
        // while let Err(..) = this.socket.set_read_timeout(Some(Duration::new(0, RTT as u32))) {} //TODO
        'send: loop {
            {
                trace!("sending get");
                this.socket
                    .send_to(this.send_buffer.bytes(), &this.server_addr)
                    .expect("cannot send get"); //TODO
            }

            // thread::sleep(Duration::new(0, SLEEP_NANOS)); //TODO

            let (_, addr) = {
                this.socket
                    .recv_from(this.receive_buffer.sized_bytes_mut())
                    .expect("unable to receive ack") //TODO
            };
            if addr == this.server_addr {
                trace!("correct addr");
                match this.receive_buffer.kind {
                    EntryKind::ReadData => {
                        // TODO validate...
                        // TODO base on loc instead?
                        if unsafe { this.receive_buffer.as_data_entry_mut().flex.loc } == key {
                            trace!("correct response");
                            trace!("packet {:#?}", this.receive_buffer);
                            return Ok(*this.receive_buffer.clone());
                        }
                        trace!("wrong loc {:?}, expected {:?}", this.receive_buffer, key);
                        continue 'send;
                    }
                    EntryKind::ReadMulti | EntryKind::ReadSenti => {
                        // TODO base on loc instead?
                        if this.receive_buffer.locs().contains(&key) {
                            trace!("correct response");
                            return Ok(*this.receive_buffer.clone());
                        }
                        trace!("wrong loc {:?}, expected {:?}", this.receive_buffer, key);
                        continue 'send;
                    }
                    EntryKind::NoValue => {
                        if unsafe { this.receive_buffer.as_data_entry_mut().flex.loc } == key {
                            let last_valid_loc = this.receive_buffer.dependencies()[0].1;
                            trace!("correct response");
                            return Err(GetErr::NoValue(last_valid_loc));
                        }
                        trace!("wrong loc {:?}, expected {:?}", this.receive_buffer, key);
                        continue 'send;
                    }
                    k => {
                        trace!("invalid response, {:?}", k);
                        continue 'send;
                    }
                }
            } else {
                trace!("unexpected addr {:?}, expected {:?}",
                       addr,
                       this.server_addr);
                continue 'send;
            }
        }
    }

    fn multi_append(&mut self,
                    chains: &[OrderIndex],
                    data: &V,
                    deps: &[OrderIndex])
                    -> InsertResult {
        let (this, chains, data, deps) = (self, chains, data, deps);

        let request_id = Uuid::new_v4();

        *this.send_buffer = EntryContents::Multiput {
                                data: data,
                                uuid: &request_id,
                                columns: chains,
                                deps: deps,
                            }
                            .clone_entry();
        // this.send_buffer.kind = EntryKind::Multiput;
        this.send_buffer.id = request_id.clone();
        trace!("Tpacket {:#?}", this.send_buffer);

        {
            // let fd = this.socket.as_raw_fd();
        }

        // TODO find server

        trace!("multi_append from {:?}", this.socket.local_addr());
        let start_time = precise_time_ns() as i64;
        'send: loop {
            {
                trace!("sending multi");
                this.socket
                    .send_to(this.send_buffer.bytes(), &this.server_addr)
                    .expect("cannot send insert"); //TODO
            }

            'receive: loop {
                let (_size, addr) = {
                    this.socket
                        .recv_from(this.receive_buffer.sized_bytes_mut())
                        .expect("unable to receive ack") //TODO
                        //precise_time_ns() as i64 - start_time < this.rtt + 4 * this.dev
                };
                trace!("got packet");
                if addr == this.server_addr {
                    match this.receive_buffer.kind.layout() {
                        EntryLayout::Multiput => {
                            // TODO types?
                            trace!("correct response");
                            trace!("id {:?}", this.receive_buffer.id);
                            if this.receive_buffer.id == request_id {
                                trace!("multiappend success");
                                let sample_rtt = precise_time_ns() as i64 - start_time;
                                let diff = sample_rtt - this.rtt;
                                this.dev = this.dev + (diff.abs() - this.dev) / 4;
                                this.rtt = this.rtt + (diff * 4 / 5);
                                return Ok((0.into(), 0.into())); //TODO
                            } else {
                                trace!("?? packet {:?}", this.receive_buffer);
                                continue 'receive;
                            }
                        }
                        v => {
                            trace!("invalid response {:?}", v);
                            continue 'receive;
                        }
                    }
                } else {
                    trace!("unexpected addr {:?}, expected {:?}",
                           addr,
                           this.server_addr);
                    continue 'receive;
                }
            }
        }
    }

    fn dependent_multi_append(&mut self, chains: &[order],
        depends_on: &[order], data: &V,
        deps: &[OrderIndex]) -> InsertResult {

        let request_id = Uuid::new_v4();

        let mchains: Vec<_> = chains.into_iter()
            .map(|&c| (c, 0.into()))
            .chain(::std::iter::once((0.into(), 0.into())))
            .chain(depends_on.iter().map(|&c| (c, 0.into())))
            .collect();

        *self.send_buffer = EntryContents::Multiput {
            data: data,
            uuid: &request_id,
            columns: &mchains,
            deps: deps,
        }.clone_entry();
        self.send_buffer.id = request_id.clone();

        'send: loop {
            {
                trace!("sending dmulti");
                self.socket
                    .send_to(self.send_buffer.bytes(), &self.server_addr)
                    .expect("cannot send insert"); //TODO
            }

            let start_time = precise_time_ns() as i64;
            'receive: loop {
                let (_size, addr) = {
                    self.socket
                        .recv_from(self.receive_buffer.sized_bytes_mut())
                        .expect("unable to receive ack") //TODO
                        //precise_time_ns() as i64 - start_time < this.rtt + 4 * this.dev
                };
                trace!("got packet");
                if addr == self.server_addr {
                    match self.receive_buffer.kind.layout() {
                        EntryLayout::Multiput => {
                            // TODO types?
                            trace!("correct response");
                            trace!("id {:?}", self.receive_buffer.id);
                            if self.receive_buffer.id == request_id {
                                trace!("dmultiappend success");
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
                } else {
                    trace!("unexpected addr {:?}, expected {:?}",
                           addr, self.server_addr);
                    continue 'receive;
                }
            }
        }
    }
}

impl<V: Storeable + ?Sized> Clone for UdpStore<V> {
    fn clone(&self) -> Self {
        let &UdpStore {ref server_addr, ref receive_buffer, ref send_buffer, _pd, rtt, dev, ..} = self;
        UdpStore {
            socket: UdpSocket::bind("0.0.0.0:0").expect("cannot clone"), // TODO
            server_addr: server_addr.clone(),
            receive_buffer: receive_buffer.clone(),
            send_buffer: send_buffer.clone(),
            rtt: rtt,
            dev: dev,
            _pd: _pd,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use prelude::*;

    use std::collections::HashMap;
    use std::collections::hash_map::Entry::{Occupied, Vacant};
    use std::mem::{self, forget, transmute};
    use std::net::UdpSocket;
    use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
    use std::thread::spawn;

    use servers::udp::Server;
    // use test::Bencher;

    // use mio::buf::{MutSliceBuf, SliceBuf};
    // use mio::udp::UdpSocket;

    #[allow(non_upper_case_globals)]
    fn new_store<V: ::std::fmt::Debug>(_: Vec<OrderIndex>) -> UdpStore<V>
        where V: Clone
    {
        static SERVERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

        let handle = spawn(move || {
            const addr_str: &'static str = "0.0.0.0:13265";
            let mut server = if let Ok(server) = Server::new(&addr_str.parse().unwrap()) {
                server
            } else {
                trace!("socket in use");
                return;
            };
            SERVERS_READY.fetch_add(1, Ordering::Release);
            server.run()
        });
        forget(handle);

        // const addr_str: &'static str = "172.28.229.152:13265";
        // const addr_str: &'static str = "10.21.7.4:13265";
        const addr_str: &'static str = "127.0.0.1:13265";
        while SERVERS_READY.load(Ordering::Acquire) < 1 {}

        unsafe {
            use std::marker::PhantomData;
            UdpStore {
                socket: UdpSocket::bind("0.0.0.0:0").expect("unable to open store"),
                server_addr: addr_str.parse().expect("invalid inet address"),
                receive_buffer: Box::new(mem::zeroed()),
                send_buffer: Box::new(mem::zeroed()),
                _pd: PhantomData,
                rtt: super::RTT,
                dev: 0,
            }
        }
    }

    //general_tests!(super::new_store);

    #[cfg(False)]
    #[bench]
    fn many_writes(b: &mut Bencher) {
        let mut store = new_store(vec![]);
        let mut i = 0;
        let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
        b.iter(|| {
            store.insert((21.into(), i.into()), entr.clone());
            i.wrapping_add(1);
        });
    }
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
