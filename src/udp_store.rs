
use prelude::*;

//use std::marker::{Unsize, PhantomData};
use std::marker::{PhantomData};
use std::mem::{self, size_of};
use std::net::SocketAddr;
//use std::ops::CoerceUnsized;
use std::slice;
use std::thread;

use mio::buf::{SliceBuf, MutSliceBuf};
use mio::udp::UdpSocket;
use mio::unix;

use time::precise_time_ns;

use uuid::Uuid;

#[derive(Debug)]
pub struct UdpStore<V> {
    socket: UdpSocket,
    server_addr: SocketAddr,
    receive_buffer: Box<Packet<V>>,
    send_buffer: Box<Packet<V>>,
    rtt: i64,
    dev: i64,
    _pd: PhantomData<V>,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[repr(C)]
pub struct Header {
	kind: Kind,
    id: Uuid,
    loc: OrderIndex,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[repr(C)]
pub struct TransactionHeader {
	kind: Kind,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[repr(u32)]
pub enum Kind {
    Ack, Write, Read, Lock,
    Written, AlreadyWritten,
    Value, NoValue,
    Locked, AlreadLocked,
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(C)]
pub struct PacketBuilder<D: ?Sized> {
    header: Header,
    data: D,
}

pub type Packet<V> = PacketBuilder<Entry<V>>;

#[test]
fn test_packet_size() {
    use std::mem::size_of;
    assert_eq!(size_of::<Packet<()>>(), 4096);
}

/*#[repr(C)]
pub struct Packet {
    header: Header,
    //data: [u8; 4096 - 8 - 16],
    data: [u8],
}*/

const SLEEP_NANOS: u32 = 8000; //TODO user settable
const RTT: i64 = 80000;

impl<V: Copy> Store<V> for UdpStore<V> {

    fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult {
        use self::Kind::*;

        let request_id = Uuid::new_v4();

        *self.send_buffer = PacketBuilder {
            header: Header {
                loc: key,
                id: request_id.clone(),
                kind: Write,
            },
            data: val.clone(),
        };

        {
            //let fd = self.socket.as_raw_fd();

        }

        trace!("at {:?}", self.socket.local_addr());
        let start_time = precise_time_ns() as i64;
        'send: loop {
            {
                trace!("sending");
                let buf = &mut SliceBuf::wrap(&self.send_buffer.as_bytes());
                self.socket.send_to(buf, &self.server_addr).expect("cannot send insert"); //TODO
            }

            'receive: loop {
                let response = {
                    let borrow = &mut self.receive_buffer.as_bytes_mut();
                    let buf = &mut MutSliceBuf::wrap(borrow);
                    self.socket.recv_from(buf).expect("unable to receive ack") //TODO
                };
                match response { //TODO loop?
                    Some(addr) if addr == self.server_addr => {
                        match self.receive_buffer.header.kind {
                            Written | AlreadyWritten => { //TODO types?
                                trace!("correct response");
                                if self.receive_buffer.header.loc == key {
                                    //let rtt = precise_time_ns() as i64 - start_time;
                                    //self.rtt = ((self.rtt * 4) / 5) + (rtt / 5);
                                    let sample_rtt = precise_time_ns() as i64 - start_time;
                                    let diff = sample_rtt - self.rtt;
                                    self.dev = self.dev + (diff.abs() - self.dev) / 4;
                                    self.rtt = self.rtt + (diff * 4 / 5);
                                    if self.receive_buffer.header.id == request_id {
                                        trace!("write success");
                                        return Ok(())
                                    }
                                    return Err(InsertErr::AlreadyWritten)
                                }
                                else {
                                    continue 'receive
                                }
                            }
                            v => {
                                trace!("invalid response {:?}", v);
                                continue 'receive
                            }
                        }
                    }
                    Some(addr) => {
                        trace!("unexpected addr {:?}, expected {:?}", addr, self.server_addr);
                        continue 'receive
                    }
                    _ => {
                        trace!("no response");
                        if true {//precise_time_ns() as i64 - start_time < self.rtt + 4 * self.dev {
                            continue 'receive
                        }
                        else {
                        //TODO wait for rtt before resend
                            continue 'send
                        }
                    }
                }
            }
        }
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        use self::Kind::*;
        assert!(size_of::<V>() <= MAX_DATA_LEN);

        let request_id = Uuid::new_v4();
        self.send_buffer.header = Header {
            loc: key,
            id: request_id.clone(),
            kind: Read,
        };

        trace!("at {:?}", self.socket.local_addr());
        'send: loop {
            {
                trace!("sending");
                let buf = &mut SliceBuf::wrap(&self.send_buffer.as_bytes());
                self.socket.send_to(buf, &self.server_addr).expect("cannot send get"); //TODO
            }

            //thread::sleep(Duration::new(0, SLEEP_NANOS)); //TODO

            let response = {
                let borrow = &mut self.receive_buffer.as_bytes_mut();
                let buf = &mut MutSliceBuf::wrap(borrow);
                self.socket.recv_from(buf).expect("unable to receive ack") //TODO
            };

            match response { //TODO loop
                Some(addr) if addr == self.server_addr => {
                    trace!("correct addr");
                    match self.receive_buffer.header.kind {
                        Value => {
                            //TODO validate...
                            //TODO base on loc instead?
                            if self.receive_buffer.header.loc == key {
                                trace!("correct response");
                                return Ok(self.receive_buffer.data.clone())
                            }
                            trace!("wrong loc {:?}, expected {:?}",
                                self.receive_buffer.header.loc, key);
                            continue 'send
                        }
                        NoValue => {
                            //TODO base on loc instead?
                            if self.receive_buffer.header.loc == key {
                                trace!("correct response");
                                return Err(GetErr::NoValue)
                            }
                            trace!("wrong loc {:?}, expected {:?}",
                                self.receive_buffer.header.loc, key);
                            continue 'send
                        }
                        k => {
                            trace!("invalid response, {:?}", k);
                            continue 'send
                        }
                    }
                }
                Some(addr) => {
                    trace!("unexpected addr {:?}, expected {:?}", addr, self.server_addr);
                    continue 'send
                }
                _ => {
                    trace!("no response");
                    continue 'send
                }
            }
        }
    }

    fn multi_append(&mut self, chains: &[order], data: V, deps: &[OrderIndex]) -> InsertResult {
        panic!()
    }
}

impl<V: Clone> Clone for UdpStore<V> {
    fn clone(&self) -> Self {
        let &UdpStore {ref server_addr, ref receive_buffer, ref send_buffer, _pd, rtt, dev, ..} = self;
        UdpStore {
            socket: UdpSocket::v4().expect("cannot clone"), //TODO
            server_addr: server_addr.clone(),
            receive_buffer: receive_buffer.clone(),
            send_buffer: send_buffer.clone(),
            rtt: rtt,
            dev: dev,
            _pd: _pd,
        }
    }
}

impl<V> Packet<V> { //TODO validation

    pub fn wrap_bytes(bytes: &[u8]) -> &Self {
        let start = &bytes[0];
        unsafe {
            mem::transmute(start)
        }
        /*unsafe {
            let start: *const _ = &bytes[0];
            let len = bytes.len() - size_of::<Header>();
            let ptr: *const _ = slice::from_raw_parts(start, len);
            mem::transmute(ptr)
        }*/
    }

    pub fn wrap_bytes_mut(bytes: &mut [u8]) -> &mut Self {
        assert_eq!(bytes.len(), size_of::<Self>());
        let start = &mut bytes[0];
        unsafe {
            mem::transmute(start)
        }
        /*unsafe {
            let start: *mut _ = &mut bytes[0];
            let len = bytes.len() - size_of::<Header>();
            let ptr: *mut _ = slice::from_raw_parts_mut(start, len);
            mem::transmute(ptr)
        }*/
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            let ptr: *const _ = self as *const _ as *const _;
            //TODO slice::from_raw_parts(ptr, size_of::<Header>() + self.data.len())
            slice::from_raw_parts(ptr, size_of::<Self>())
        }
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            let ptr: *mut _ = self as *mut _ as *mut _;
            slice::from_raw_parts_mut(ptr, size_of::<Self>())
        }
    }

    #[inline]
    pub fn get_kind(&self) -> Kind {
        self.header.kind
    }

    #[inline]
    pub fn set_kind(&mut self, kind: Kind) {
        self.header.kind = kind
    }

    #[inline]
    pub fn get_loc(&self) -> OrderIndex {
        self.header.loc
    }
    
    #[inline]
    pub fn get_header(&self) -> &Header {
    	&self.header
    }
}

/*impl<T> PacketBuilder<T>
where T: Unsize<[u8]> {
    pub fn as_packet(&self) -> &Packet {
        let packet: &PacketBuilder<[u8]> = self;
        unsafe { mem::transmute(packet) }
    }
}*/

#[cfg(test)]
mod test {
    use super::*;
    use prelude::*;

    use std::collections::HashMap;
    use std::collections::hash_map::Entry::{Occupied, Vacant};
    use std::mem::{self, forget, transmute};
    use std::thread::spawn;

    use test::Bencher;

    use mio::buf::{MutSliceBuf, SliceBuf};
    use mio::udp::UdpSocket;

    #[cfg(False)]
    #[test]
    fn test_packet_as_bytes() {
        let packet = PacketBuilder {
            header: Header {
                id: Uuid::default(),
                loc: (0xdeadbeef.into(), 0xc0ffeee.into()),
                kind: Kind::Write,
            },
            data: [0x10; 4],
        };
        let packet: &PacketBuilder<[u8]> = &packet;
        assert_eq!(packet, Packet::wrap_bytes(packet.as_bytes()));
    }

    #[cfg(False)]
    #[test]
    fn test_packet_as_bytes_mut() {
        let mut packet = PacketBuilder {
            header: Header {
                id: Uuid::default(),
                loc: (0xdeadbeef.into(), 0xc0ffeee.into()),
                kind: Kind::Write,
            },
            data: [0x10; 4],
        };
        let mut packet2 = PacketBuilder {
            header: Header {
                id: Uuid::default(),
                loc: (0xdeadbeef.into(), 0xc0ffeee.into()),
                kind: Kind::Write,
            },
            data: [0x10; 4],
        };
        let packet: &mut Packet = &mut packet;
        let packet2: &mut Packet = &mut packet2;
        assert_eq!(packet2, Packet::wrap_bytes_mut(packet.as_bytes_mut()));
    }

    #[cfg(False)]
    #[allow(dead_code)]
    fn test_builder_to_bytes() {
        /*let packet = PacketBuilder {
            header: Header {
                id: Uuid::default(),
                loc: (0xdeadbeef.into(), 0xc0ffeee.into()),
            },
            data: [0x10; 4096],
        };
        let packet: &PacketBuilder<[u8]> = &packet;*/
        let packet_box = Box::new(
            PacketBuilder {
                header: Header {
                    id: Uuid::default(),
                    loc: (0xdeadbeef.into(), 0xc0ffeee.into()),
                    kind: Kind::Write,
                },
                data: [0x10; 4096],
            }
        );
        let packet_box: Box<Packet> = packet_box;
        println!("{:?}", packet_box);
    }

    fn occupied_response(request_kind: Kind) -> Kind {
        match request_kind {
            Kind::Write => Kind::AlreadyWritten,
            Kind::Read => Kind::Value,
            _ => Kind::Ack,
        }
    }

    fn unoccupied_response(request_kind: Kind) -> Kind {
        match request_kind {
            Kind::Write => Kind::Written,
            Kind::Read => Kind::NoValue,
            _ => Kind::Ack,
        }
    }

    #[allow(non_upper_case_globals)]
    fn new_store<V>(_: Vec<OrderIndex>) -> UdpStore<V>
    where V: Clone {
        let handle = spawn(move || {
            const addr_str: &'static str = "0.0.0.0:13265";
            let addr = addr_str.parse().expect("invalid inet address");
            let receive = if let Ok(socket) =
                UdpSocket::bound(&addr) {
                socket
            } else {
                trace!("socket in use");
                return
            };
            let mut log = HashMap::with_capacity(10);
            let mut buff = Box::new([0; 4096]);
            trace!("starting server thread");
            'server: loop {
                let res = receive.recv_from(&mut MutSliceBuf::wrap(&mut buff[..]));
                match res {
                    Err(e) => panic!("{}", e),
                    Ok(Some(sa)) => {
                        trace!("recieved from {:?}", sa);

                        let (kind, loc) = {
                            let packet: &Packet<V> =  Packet::wrap_bytes(&buff[..]);
                            (packet.header.kind, packet.header.loc)
                        };

                        match log.entry(loc) {
                            Vacant(e) => {
                                trace!("Vacant entry {:?}", loc);
                                match kind {
                                    Kind::Write => {
                                        trace!("writing");
                                        let packet = mem::replace(&mut buff, Box::new([0; 4096]));
                                        let packet: &mut Box<Packet<V>> =
                                            e.insert(unsafe { transmute(packet) } );
                                        packet.header.kind = Kind::Written;
                                        let slice = &mut SliceBuf::wrap(packet.as_bytes());
                                        let o = receive.send_to(slice, &sa).expect("unable to ack");
                                    }
                                    _ => {
                                        let packet: &mut Packet<V> =
                                            Packet::wrap_bytes_mut(&mut buff[..]);
                                        packet.header.kind = unoccupied_response(kind);
                                        let slice = &mut SliceBuf::wrap(packet.as_bytes());
                                        receive.send_to(slice, &sa).expect("unable to ack");
                                    }
                                }
                            }
                            Occupied(mut e) => {
                                trace!("Occupied entry {:?}", loc);
                                let packet = e.get_mut();
                                packet.header.kind = occupied_response(kind);
                                let slice = &mut SliceBuf::wrap(packet.as_bytes());
                                receive.send_to(slice, &sa).expect("unable to ack");
                            }
                        };
                        //receive.send_to(&mut ByteBuf::from_slice(&[1]), &sa).expect("unable to ack");
                        //send.send_to(&mut ByteBuf::from_slice(&[1]), &sa).expect("unable to ack");
                    }
                    _ => continue 'server,
                }
            }
        });
        forget(handle);

        //const addr_str: &'static str = "172.28.229.151:13265";
        const addr_str: &'static str = "127.0.0.1:13265";

        unsafe {
            UdpStore {
                socket: UdpSocket::v4().expect("unable to open store"),
                server_addr: addr_str.parse().expect("invalid inet address"),
                receive_buffer: Box::new(mem::zeroed()),
                send_buffer: Box::new(mem::zeroed()),
                _pd: Default::default(),
                rtt: super::RTT,
                dev: 0,
            }
        }
    }

    general_tests!(super::new_store);


    #[bench]
    fn bench_write(b: &mut Bencher) {
        let mut store = new_store(vec![]);
        b.iter(|| {
            let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
            store.insert((1.into(), 0.into()), entr)
        });
    }

    #[bench]
    fn bench_sequential_writes(b: &mut Bencher) {
        let mut store = new_store(vec![]);
        b.iter(|| {
            let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
            let a = store.insert((0.into(), 0.into()), entr);
            let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
            let b = store.insert((0.into(), 1.into()), entr);
            let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
            let c = store.insert((0.into(), 2.into()), entr);
            (a, b, c)
        });
    }

    #[bench]
    fn bench_multistore_writes(b: &mut Bencher) {
        let mut store_a = new_store(vec![]);
        let mut store_b = new_store(vec![]);
        let mut store_c = new_store(vec![]);
        b.iter(|| {
            let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
            let a = store_a.insert((0.into(), 0.into()), entr);
            let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
            let b = store_b.insert((0.into(), 1.into()), entr);
            let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
            let c = store_c.insert((0.into(), 2.into()), entr);
            (a, b, c)
        });
    }

    #[bench]
    fn bench_mio_udp_write(b: &mut Bencher) {
        /*let handle = spawn(move || {
            const addr_str: &'static str = "0.0.0.0:13269";
            let addr = addr_str.parse().expect("invalid inet address");
            let receive = if let Ok(socket) =
                UdpSocket::bound(&addr) {
                socket
            } else {
                return
            };
            let mut buff = Box::new([0;4]);
            'server: loop {
                let res = receive.recv_from(&mut MutSliceBuf::wrap(&mut buff[..]));
                match res {
                    Err(e) => panic!("{}", e),
                    Ok(None) => {
                        continue 'server
                    }
                    Ok(Some(sa)) => {
                        let slice = &mut SliceBuf::wrap(&buff[..]);
                        receive.send_to(slice, &sa).expect("unable to ack");
                    }
                }
            }
        });*/

        const addr_str: &'static str = "127.0.0.1:13270";
        let client = UdpSocket::v4().expect("unable to open client");
        let addr = addr_str.parse().expect("invalid inet address");
        let mut buff = Box::new([0;4096]);
        b.iter(|| {
            let a = client.send_to(&mut SliceBuf::wrap(&[]), &addr);
         //   let mut recv = client.recv_from(&mut MutSliceBuf::wrap(&mut buff[..]));
         //   while let Ok(None) = recv {
         //       recv = client.recv_from(&mut MutSliceBuf::wrap(&mut buff[..]))
         //   }
            a
        });
    }

    #[bench]
    fn bench_std_udp_write(b: &mut Bencher) {
        use std::net::{SocketAddr, UdpSocket};

        const local_addr: &'static str = "0.0.0.0:13269";
        let client = UdpSocket::bind(local_addr).unwrap();
        const addr_str: &'static str = "127.0.0.1:13265";
        let addr: SocketAddr = addr_str.parse().expect("invalid inet address");
        b.iter(|| {
            let a = client.send_to(&[], addr);
            a
        });
    }
}
