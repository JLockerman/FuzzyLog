
use prelude::*;

use std::fmt::Debug;
//use std::marker::{Unsize, PhantomData};
use std::marker::{PhantomData};
use std::mem::{self, size_of};
use std::net::{SocketAddr, UdpSocket};
//use std::ops::CoerceUnsized;
use std::slice;
use std::thread;

//use mio::buf::{SliceBuf, MutSliceBuf};
//use mio::udp::UdpSocket;
//use mio::unix;

use time::precise_time_ns;

use uuid::Uuid;

//#[derive(Debug)]
pub struct UdpStore<V> {
    socket: UdpSocket,
    server_addr: SocketAddr,
    receive_buffer: Box<Entry<V>>,
    send_buffer: Box<Entry<V>>,
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

#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(C)]
pub struct TransactionPacket<D: ?Sized> {
    header: TransactionHeader,
    data: D,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[repr(u32)]
pub enum Kind {
    Ack, Write, Read, Multiput,
    Written, AlreadyWritten,
    Value, NoValue,
    MultiputSuccess, MultiputFailed,
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[repr(C)]
pub struct PacketBuilder<D: ?Sized> {
    header: Header,
    data: D,
}

pub type Packet<V> = PacketBuilder<Entry<V>>;

/*#[repr(C)]
pub struct Packet {
    header: Header,
    //data: [u8; 4096 - 8 - 16],
    data: [u8],
}*/

const SLEEP_NANOS: u32 = 8000; //TODO user settable
const RTT: i64 = 80000;

impl<V: Copy + Debug> UdpStore<V> {
    #[inline(always)]
    fn insert_ref(&mut self, key: OrderIndex, val: &mut Packet<V>, recv: &mut Packet<V>) -> InsertResult {
        use self::Kind::*;

        let request_id = Uuid::new_v4();
        val.header = Header {
            loc: key,
            id: request_id.clone(),
            kind: Write,
        };
        let val = &*val;
        self.socket.send_to(val.as_bytes(), &self.server_addr)
            .expect("cannot send insert");

        //println!("sent");
        'receive: loop {
            let (size, addr) = self.socket.recv_from(recv.as_bytes_mut())
                .expect("unable to receive ack");
            if addr == self.server_addr {
                match recv.header.kind {
                    Written | AlreadyWritten => {
                        if recv.header.loc == key {
                            if recv.header.id == request_id {
                                return Ok(())
                            }
                            return Err(InsertErr::AlreadyWritten)
                        }
                        else {
                            //println!("key: {:?}\nloc: {:?}\nrecv\n{:#?}", key, self.receive_buffer.header.loc, recv);
                            continue 'receive
                        }
                    }
                    _ => {
                        //println!("v {:?}", v);
                        continue 'receive
                    }
                }
            }
            else {
                //println!("packet {:?}", recv);
                continue 'receive
            }
        }
    }
}

impl<V: Copy + Debug + Eq> Store<V> for UdpStore<V> {

    fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult {
        use self::Kind::*;

        let request_id = Uuid::new_v4();
        *self.send_buffer = val;
        assert_eq!(self.send_buffer.kind & EntryKind::Layout, EntryKind::Data);
        {
            let entr = unsafe { self.send_buffer.as_data_entry_mut() };
            entr.flex.loc = key;
            entr.id = request_id.clone();
        }
        trace!("packet {:#?}", self.send_buffer);

        trace!("at {:?}", self.socket.local_addr());
        let start_time = precise_time_ns() as i64;
        'send: loop {
            {
                trace!("sending");
                self.socket.send_to(self.send_buffer.bytes(), &self.server_addr)
                    .expect("cannot send insert"); //TODO
            }

            'receive: loop {
                let (size, addr) = {
                    self.socket.recv_from(self.receive_buffer.bytes_mut())
                        .expect("unable to receive ack") //TODO
                        //precise_time_ns() as i64 - start_time < self.rtt + 4 * self.dev
                };
                trace!("got packet");
                if addr == self.server_addr {
                    //if self.receive_buffer.kind & EntryKind::ReadSuccess == EntryKind::ReadSuccess {
                    //    trace!("invalid response r ReadSuccess at insert");
                    //    continue 'receive
                    //}
                    match self.receive_buffer.kind & EntryKind::Layout {
                        EntryKind::Data => { //TODO types?
                            trace!("correct response");
                            let entr = unsafe { self.receive_buffer.as_data_entry() };
                            if entr.flex.loc == key {
                                //let rtt = precise_time_ns() as i64 - start_time;
                                //self.rtt = ((self.rtt * 4) / 5) + (rtt / 5);
                                let sample_rtt = precise_time_ns() as i64 - start_time;
                                let diff = sample_rtt - self.rtt;
                                self.dev = self.dev + (diff.abs() - self.dev) / 4;
                                self.rtt = self.rtt + (diff * 4 / 5);
                                if entr.id == request_id {
                                    trace!("write success");
                                    return Ok(())
                                }
                                trace!("already written");
                                return Err(InsertErr::AlreadyWritten)
                            }
                            else {
                                println!("packet {:?}", self.receive_buffer);
                                continue 'receive
                            }
                        }
                        EntryKind::Multiput => {
                            match self.receive_buffer.contents() {
                                EntryContents::Multiput{columns, ..} => {
                                    if columns.contains(&key) {
                                        return Err(InsertErr::AlreadyWritten)
                                    }
                                    continue 'receive
                                }
                                _ => unreachable!(),
                            };
                        }
                        v => {
                            trace!("invalid response {:?}", v);
                            continue 'receive
                        }
                    }
                }
                else {
                    trace!("unexpected addr {:?}, expected {:?}", addr, self.server_addr);
                    continue 'receive
                }
            }
        }
    }

    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        assert!(size_of::<V>() <= MAX_DATA_LEN);

        //let request_id = Uuid::new_v4();
        self.send_buffer.kind = EntryKind::Read;
        unsafe {
            self.send_buffer.as_data_entry_mut().flex.loc = key;
            self.send_buffer.id = mem::zeroed();
        };
        //self.send_buffer.id = request_id.clone();

        trace!("at {:?}", self.socket.local_addr());
        'send: loop {
            {
                trace!("sending");
                self.socket.send_to(self.send_buffer.bytes(), &self.server_addr)
                    .expect("cannot send get"); //TODO
            }

            //thread::sleep(Duration::new(0, SLEEP_NANOS)); //TODO

            let (_, addr) = {
                self.socket.recv_from(self.receive_buffer.bytes_mut())
                    .expect("unable to receive ack") //TODO
            };
            if addr == self.server_addr {
                trace!("correct addr");
                match self.receive_buffer.kind {
                    EntryKind::ReadData => {
                        //TODO validate...
                        //TODO base on loc instead?
                        if unsafe { self.receive_buffer.as_data_entry_mut().flex.loc } == key {
                            trace!("correct response");
                            return Ok(*self.receive_buffer.clone())
                        }
                        trace!("wrong loc {:?}, expected {:?}",
                            self.receive_buffer, key);
                        continue 'send
                    }
                    EntryKind::ReadMulti => {
                        //TODO base on loc instead?
                        if unsafe { self.receive_buffer.as_multi_entry_mut().multi_contents_mut()
                            .columns.contains(&key) } {
                            trace!("correct response");
                            return Ok(*self.receive_buffer.clone())
                        }
                        trace!("wrong loc {:?}, expected {:?}",
                            self.receive_buffer, key);
                        continue 'send
                    }
                    EntryKind::NoValue => {
                        if unsafe { self.receive_buffer.as_data_entry_mut().flex.loc } == key {
                            trace!("correct response");
                            return Err(GetErr::NoValue)
                        }
                        trace!("wrong loc {:?}, expected {:?}",
                            self.receive_buffer, key);
                        continue 'send
                    }
                    k => {
                        trace!("invalid response, {:?}", k);
                        continue 'send
                    }
                }
            }
            else {
                trace!("unexpected addr {:?}, expected {:?}", addr, self.server_addr);
                continue 'send
            }
        }
    }

    fn multi_append(&mut self, chains: &[OrderIndex], data: V, deps: &[OrderIndex]) -> InsertResult {
        let request_id = Uuid::new_v4();

        let contents = EntryContents::Multiput {
            data: &data,
            uuid: &request_id,
            columns: chains,
            deps: deps,
        };

        *self.send_buffer = EntryContents::Multiput {
            data: &data,
            uuid: &request_id,
            columns: chains,
            deps: deps,
        }.clone_entry();
        //self.send_buffer.kind = EntryKind::Multiput;
        self.send_buffer.id = request_id.clone();
        trace!("Tpacket {:#?}", self.send_buffer);

        {
            //let fd = self.socket.as_raw_fd();

        }

        //TODO find server

        trace!("multi_append from {:?}", self.socket.local_addr());
        let start_time = precise_time_ns() as i64;
        'send: loop {
            {
                trace!("sending");
                self.socket.send_to(self.send_buffer.bytes(), &self.server_addr)
                    .expect("cannot send insert"); //TODO
            }

            'receive: loop {
                let (size, addr) = {
                    self.socket.recv_from(self.receive_buffer.bytes_mut())
                        .expect("unable to receive ack") //TODO
                        //precise_time_ns() as i64 - start_time < self.rtt + 4 * self.dev
                };
                trace!("got packet");
                if addr == self.server_addr {
                    match self.receive_buffer.kind & EntryKind::Layout {
                        EntryKind::Multiput => { //TODO types?
                            trace!("correct response");
                            trace!("id {:?}", self.receive_buffer.id);
                            if self.receive_buffer.id == request_id {
                                trace!("multiappend success");
                                let sample_rtt = precise_time_ns() as i64 - start_time;
                                let diff = sample_rtt - self.rtt;
                                self.dev = self.dev + (diff.abs() - self.dev) / 4;
                                self.rtt = self.rtt + (diff * 4 / 5);
                                return Ok(())
                            }
                            else {
                                trace!("?? packet {:?}", self.receive_buffer);
                                continue 'receive
                            }
                        }
                        v => {
                            trace!("invalid response {:?}", v);
                            continue 'receive
                        }
                    }
                }
                else {
                    trace!("unexpected addr {:?}, expected {:?}", addr, self.server_addr);
                    continue 'receive
                }
            }
        }
    }
}

impl<V: Clone> Clone for UdpStore<V> {
    fn clone(&self) -> Self {
        let &UdpStore {ref server_addr, ref receive_buffer, ref send_buffer, _pd, rtt, dev, ..} = self;
        UdpStore {
            socket: UdpSocket::bind("0.0.0.0:0").expect("cannot clone"), //TODO
            server_addr: server_addr.clone(),
            receive_buffer: receive_buffer.clone(),
            send_buffer: send_buffer.clone(),
            rtt: rtt,
            dev: dev,
            _pd: _pd,
        }
    }
}

pub struct Buffer<V> {
    _pd: PhantomData<V>,
    buff: Box<[u8; 4096]>,
}

impl<V> Buffer<V> {
    fn zeroed() -> Self {
        unsafe {
            Buffer {
                _pd: PhantomData,
                buff: Box::new(mem::zeroed()),
            }
        }
    }
}

impl<V> ::std::ops::Deref for Buffer<V> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &*self.buff
    }
}

impl<V> ::std::ops::DerefMut for Buffer<V> {

    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.buff
    }
}

impl<V> Clone for Buffer<V> {
    fn clone(&self) -> Self {
        use std::ptr;
        unsafe {
            let mut other = Buffer {
                _pd: PhantomData,
                buff: Box::new(mem::uninitialized()),
            };
            ptr::copy::<[u8; 4096]>(&*self.buff, &mut *other.buff, 1);
            other
        }
    }
}

trait Packeteable<V> {
    fn as_packet(&self) -> &Packet<V>;
    fn as_packet_mut(&mut self) -> &mut Packet<V>;
}

trait TPacketeable<V> {
    fn as_tpacket(&self) -> &TransactionPacket<Entry<V>>;
    fn as_tpacket_mut(&mut self) -> &mut TransactionPacket<Entry<V>>;
}

impl<V> Packeteable<V> for Buffer<V> {
    fn as_packet(&self) -> &Packet<V> {
        Packet::wrap_bytes(&self.buff[..])
    }

    fn as_packet_mut(&mut self) -> &mut Packet<V> {
        Packet::wrap_bytes_mut(&mut *self.buff)
    }
}

impl<V> TPacketeable<V> for Buffer<V> {
    fn as_tpacket(&self) -> &TransactionPacket<Entry<V>> {
        let start = &self.buff[0];
        unsafe {
            mem::transmute(start)
        }
    }

    fn as_tpacket_mut(&mut self) -> &mut TransactionPacket<Entry<V>> {
        TransactionPacket::wrap_bytes_mut(&mut *self.buff)
    }
}

#[derive(Debug)]
struct Multiput<'e, V: 'e>{
    data: &'e mut V,
    uuid: &'e mut Uuid,
    columns: &'e mut [order],
    deps: &'e mut [OrderIndex],
}

impl<V> TransactionPacket<Entry<V>> {
    fn get_id(&self) ->&Uuid {
        match self.data.contents() {
            EntryContents::Multiput{uuid, ..} => uuid,
            _ => unreachable!(),
        }
    }

    pub fn wrap_bytes_mut(bytes: &mut [u8]) -> &mut Self {
        assert!(bytes.as_mut().len() >= size_of::<Self>());
        let start = &mut bytes.as_mut()[0];
        unsafe {
            mem::transmute(start)
        }
    }

    fn as_multiput(&mut self) -> Multiput<V> {
        unsafe {
            match self.data.contents_mut() {
                //EntryContentsMut::Multiput{data, uuid, columns, deps} => {
                //    Multiput{data: data, uuid: uuid, columns: columns, deps: deps}
                //}
                _ => unreachable!(),
            }
            /*assert_eq!(self.data.kind, EntryKind::Multiput);
            let contents_ptr: *mut u8 = &mut self.data as *mut _ as *mut u8;
            let data_ptr = contents_ptr.offset((size_of::<Uuid>() + self.data.cols as usize * size_of::<order>())
                as isize);
            let dep_ptr:*mut OrderIndex = data_ptr.offset(self.data.data_bytes as isize)
                as *mut _;
            let num_deps = (self.data.dependency_bytes as usize)
                .checked_div(size_of::<OrderIndex>()).unwrap();
                let deps = slice::from_raw_parts_mut(dep_ptr, num_deps);

            let id_ptr: *mut Uuid = contents_ptr as *mut Uuid;
            let cols_ptr: *mut order = contents_ptr.offset(size_of::<Uuid>() as isize)
                as *mut _;

            let data = (data_ptr as *mut _).as_mut().unwrap();
            let uuid = id_ptr.as_mut().unwrap();
            let cols = slice::from_raw_parts_mut(cols_ptr, self.data.cols as usize);
            Multiput{data: data, uuid: uuid, columns: cols, deps: deps}*/
        }
    }
}

impl<V> Packet<V> { //TODO validation

    pub fn wrap_bytes(bytes: &[u8]) -> &Self {
        let start = &bytes.as_ref()[0];
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
        assert_eq!(bytes.as_mut().len(), size_of::<Self>());
        let start = &mut bytes.as_mut()[0];
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

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            let ptr: *const _ = self as *const _ as *const _;
            //TODO slice::from_raw_parts(ptr, size_of::<Header>() + self.data.len())
            slice::from_raw_parts(ptr, size_of::<Self>())
        }
    }

    #[inline(always)]
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            let ptr: *mut _ = self as *mut _ as *mut _;
            slice::from_raw_parts_mut(ptr, size_of::<Self>())
        }
    }

    #[inline(always)]
    pub fn as_tansaction_packet(&self) -> &TransactionPacket<Entry<V>> {
        unsafe {
            mem::transmute(self)
        }
    }

    #[inline(always)]
    pub fn as_tansaction_packet_mut(&mut self) -> &mut TransactionPacket<Entry<V>> {
        unsafe {
            mem::transmute(self)
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
    use std::net::UdpSocket;
    use std::thread::spawn;

    use test::Bencher;

    use mio::buf::{MutSliceBuf, SliceBuf};
    //use mio::udp::UdpSocket;

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
    fn new_store<V: ::std::fmt::Debug>(_: Vec<OrderIndex>) -> UdpStore<V>
    where V: Clone {
        let handle = spawn(move || {
            use mio::udp::UdpSocket;
            const addr_str: &'static str = "0.0.0.0:13265";
            let addr = addr_str.parse().expect("invalid inet address");
            //return; TODO
            let receive = if let Ok(socket) =
                UdpSocket::bound(&addr) {
                socket
            } else {
                trace!("socket in use");
                return
            };
            let mut log: HashMap<_, Box<Entry<V>>> = HashMap::with_capacity(10);
            let mut horizon = HashMap::with_capacity(10);
            let mut buff = Box::new(unsafe {mem::zeroed::<Entry<V>>()});
            trace!("starting server thread");
            'server: loop {
                let res = receive.recv_from(&mut MutSliceBuf::wrap(buff.bytes_mut()));
                match res {
                    Err(e) => panic!("{}", e),
                    Ok(Some(sa)) => {
                        trace!("server recieved from {:?}", sa);

                        let kind = {
                            buff.kind
                        };

                        trace!("server recieved {:?}", buff);
                        if let EntryKind::Multiput = kind & EntryKind::Layout {
                            {
                                let cols = {
                                    let entr = unsafe { buff.as_multi_entry_mut() };
                                    let packet = entr.multi_contents_mut();
                                    //trace!("multiput {:?}", packet);
                                    Vec::from(&*packet.columns)
                                };
                                let cols = unsafe {
                                    let packet = buff.as_multi_entry_mut().multi_contents_mut();
                                    for i in 0..cols.len() {
                                        let hor: entry = horizon.get(&cols[i].0).cloned().unwrap_or(0.into()) + 1;
                                        packet.columns[i].1 = hor;

                                        horizon.insert(packet.columns[i].0, hor);
                                    }
                                    Vec::from(&*packet.columns)
                                };
                                trace!("appending at {:?}", cols);
                                for loc in cols {
                                    let b = buff.clone();
                                    log.insert(loc,  b);
                                    //trace!("appended at {:?}", loc);
                                }
                            }
                            let slice = &mut SliceBuf::wrap(buff.bytes());
                            let _ = receive.send_to(slice, &sa).expect("unable to ack");
                        }
                        else {
                            let loc = unsafe { buff.as_data_entry().flex.loc };

                            match log.entry(loc) {
                                Vacant(e) => {
                                    trace!("Vacant entry {:?}", loc);
                                    match kind & EntryKind::Layout {
                                        EntryKind::Data => {
                                            trace!("writing");
                                            let packet = mem::replace(&mut buff, Box::new(unsafe {mem::zeroed::<Entry<V>>()}));
                                            let packet: &mut Box<Entry<V>> =
                                                unsafe { transmute_ref_mut(e.insert(packet)) };
                                            horizon.insert(loc.0, loc.1);
                                            //packet.header.kind = Kind::Written;
                                            let slice = &mut SliceBuf::wrap(packet.bytes());
                                            let o = receive.send_to(slice, &sa).expect("unable to ack");
                                        }
                                        _ => {
                                            //buff.kind = unoccupied_response(kind);
                                            let slice = &mut SliceBuf::wrap(buff.bytes());
                                            receive.send_to(slice, &sa).expect("unable to ack");
                                        }
                                    }
                                }
                                Occupied(mut e) => {
                                    trace!("Occupied entry {:?}", loc);
                                    let packet = e.get_mut();
                                    packet.kind = packet.kind | EntryKind::ReadSuccess;
                                    trace!("returning {:?}", packet);
                                    let slice = &mut SliceBuf::wrap(packet.bytes());
                                    receive.send_to(slice, &sa).expect("unable to ack");
                                }
                            };
                        }
                        //receive.send_to(&mut ByteBuf::from_slice(&[1]), &sa).expect("unable to ack");
                        //send.send_to(&mut ByteBuf::from_slice(&[1]), &sa).expect("unable to ack");
                    }
                    _ => continue 'server,
                }
            }
        });
        forget(handle);

        //const addr_str: &'static str = "172.28.229.152:13265";
        //const addr_str: &'static str = "10.21.7.4:13265";
        const addr_str: &'static str = "127.0.0.1:13265";

        unsafe {
            UdpStore {
                socket: UdpSocket::bind("0.0.0.0:0").expect("unable to open store"),
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

    unsafe fn transmute_ref_mut<T, U>(t: &mut T) -> &mut U {
        assert_eq!(mem::size_of::<T>(), mem::size_of::<U>());
        mem::transmute(t)
    }
/*
    #[test]
    fn test_external_write() {
        let mut store = new_store(vec![]);
        let mut send: Box<Packet<u64>> = Box::new(unsafe { mem::zeroed() });
        send.data = EntryContents::Data(&48u64, &*vec![]).clone_entry();
        let mut recv: Box<Packet<u64>> = Box::new(unsafe { mem::zeroed() });

        let res = store.insert_ref((1.into(), 1.into()), &mut *send, &mut *recv);
        println!("res {:?}", res);
    }

    #[bench]
    fn external_write(b: &mut Bencher) {
        let mut store = new_store(vec![]);
        let mut send: Box<Packet<u64>> = Box::new(unsafe { mem::zeroed() });
        send.data = EntryContents::Data(&48u64, &*vec![]).clone_entry();
        let mut recv: Box<Packet<u64>> = Box::new(unsafe { mem::zeroed() });
        b.iter(|| {
            store.insert_ref((1.into(), 1.into()), &mut *send, &mut *recv)
        });
    }

    #[bench]
    fn many_writes(b: &mut Bencher) {
        let mut store = new_store(vec![]);
        let mut i = 0;
        b.iter(|| {
            let entr = EntryContents::Data(&48u64, &*vec![]).clone_entry();
            store.insert((17.into(), i.into()), entr);
            i.wrapping_add(1);
        });
    }

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

    //#[bench]
    fn bench_rtt(b: &mut Bencher) {
        use std::mem;
        use mio::udp::UdpSocket;
        const addr_str: &'static str = "10.21.7.4:13265";
        let client = UdpSocket::v4().expect("unable to open client");
        let addr = addr_str.parse().expect("invalid inet address");
        let buff = Box::new([0u8; 4]);
        let mut recv_buff = Box::new([0u8; 4096]);
        b.iter(|| {
            let a = {
                let buff: &[u8] = &buff[..];
                let buf = &mut SliceBuf::wrap(buff);
                client.send_to(buf, &addr)
            };
            let mut recv = client.recv_from(&mut MutSliceBuf::wrap(&mut recv_buff[..]));
            while let Ok(None) = recv {
                recv = client.recv_from(&mut MutSliceBuf::wrap(&mut recv_buff[..]))
            }
            //println!("rec");
            a
        });
    }

    #[bench]
    fn bench_mio_write(b: &mut Bencher) {
        use mio::udp::UdpSocket;
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

        const addr_str: &'static str = "172.28.229.152:13266";
        let client = UdpSocket::v4().expect("unable to open client");
        let addr = addr_str.parse().expect("invalid inet address");
        let mut buff = Box::new([0;4096]);
        b.iter(|| {
            let a = client.send_to(&mut SliceBuf::wrap(&buff[..]), &addr);
         //   let mut recv = client.recv_from(&mut MutSliceBuf::wrap(&mut buff[..]));
         //   while let Ok(None) = recv {
         //       recv = client.recv_from(&mut MutSliceBuf::wrap(&mut buff[..]))
         //   }
            a
        });
    }*/
}
