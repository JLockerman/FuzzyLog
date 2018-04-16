// use prelude::*;

use std::collections::{/*LinkedList,*/ VecDeque};
use std::io::{Read, Write, ErrorKind};
use std::mem;
use socket_addr::Ipv4SocketAddr;

use byteorder::{ByteOrder, LittleEndian};

use mio::tcp::*;

use buffer::Buffer;

use packets::EntryContents;
use packets::double_buffer::DoubleBuffer;

use super::*;

/*
struct PerSocket {
    buffer: IoBuffer,
    stream: TcpStream,
    bytes_handled: usize,
    is_from_server: bool,
}
*/

type ShouldContinue = bool;

const WRITE_BUFFER_SIZE: usize = 1024 * 8;

#[derive(Debug)]
pub struct PerSocket {
    buffer_cache: VecDeque<Buffer>,
    upstream: Option<Reader>,
    downstream: Option<Reader>,
    being_written: DoubleBuffer,
    bytes_written: usize,
    pending: VecDeque<Vec<u8>>,
    needs_to_stay_awake: bool,
    is_staying_awake: bool,

    has_upstream: bool,

    print_data: PerSocketData,
}

#[derive(Debug)]
struct Reader {
    buffer: Option<Buffer>,
    bytes_read: usize,
    stream: TcpStream,
}

counters! {
    struct PerSocketData {
        packets_recvd: u64,
        bytes_recvd: u64,
        bytes_sent: u64,
        sends: u64,
        sends_added: u64,
        bytes_to_send: u64,
        read_buffers_sent: u64,
        read_buffers_returned: u64,
    }
}

// #[derive(Copy, Clone, Debug, PartialEq, Eq)]
// pub enum PerSocketKind {
//     Upstream,
//     Downstream,
//     Client,
// }

pub enum RecvPacket {
    Err,
    Pending,
    FromUpstream(Buffer, Ipv4SocketAddr, u64),
    FromClient(Buffer, Ipv4SocketAddr),
}

impl Reader {
    fn new(stream: TcpStream) -> Self {
        Reader {
            buffer: None,
            bytes_read: 0,
            stream,
        }
    }
}

impl PerSocket {

    pub fn client(upstream: TcpStream, downstream: Option<(mio::Token, TcpStream)>, has_upstream: bool) -> Self {
        Self {
            buffer_cache: (0..NUMBER_READ_BUFFERS).map(|_| Buffer::empty()).collect(),
            upstream: Some(Reader::new(upstream)),
            downstream: downstream.map(|(_, down)| Reader::new(down)),
            being_written: DoubleBuffer::with_first_buffer_capacity(WRITE_BUFFER_SIZE),
            bytes_written: 0,
            pending: Default::default(),
            needs_to_stay_awake: false,
            is_staying_awake: true,

            has_upstream,

            print_data: Default::default(),
        }
    }

    //TODO recv burst
    //FIXME make sure read from client don't deadlock write from upstream
    pub fn recv_packet(&mut self) -> RecvPacket {
        // trace!("SOCKET try recv");
        let &mut Self{
            ref mut buffer_cache,
            ref mut upstream,
            ref mut needs_to_stay_awake,
            ref mut print_data, has_upstream,
            ..
        } = self;
        let &mut Reader{ref mut buffer, ref mut bytes_read, stream: ref mut upstream} =
            match upstream.as_mut() {
                None => return RecvPacket::Pending,
                Some(up) => up,
            };
        if has_upstream {
            if let &mut None = buffer {
                *buffer = buffer_cache.pop_front();
            }
            let mut read_buffer = match buffer.take() {
                None => return RecvPacket::Pending,
                Some(buffer) => buffer,
            };
            // trace!("SOCKET recv actual");
            //TODO audit
            let recv = recv_packet(&mut read_buffer, upstream, *bytes_read, mem::size_of::<u64>(), needs_to_stay_awake, print_data);
            match recv {
                //TODO send to log
                RecvRes::Done(src_addr) => {
                    print_data.packets_recvd(1);
                    *bytes_read = 0;
                    // trace!("SOCKET recevd replication for {}.", src_addr);
                    *needs_to_stay_awake = true;
                    let entry_size = read_buffer.entry_size();
                    let end = entry_size + mem::size_of::<u64>();
                    let storage_loc = LittleEndian::read_u64(&read_buffer[entry_size..end]);
                    print_data.read_buffers_sent(1);
                    RecvPacket::FromUpstream(read_buffer, src_addr, storage_loc)
                },
                //FIXME remove from map
                RecvRes::Error => {
                    *bytes_read = 0;
                    error!("upstream; returned buffer now @ {}", buffer_cache.len());
                    buffer_cache.push_front(read_buffer);
                    RecvPacket::Err
                },

                RecvRes::NeedsMore(total_read) => {
                    *bytes_read = total_read;
                    *buffer = Some(read_buffer);
                    RecvPacket::Pending
                },
            }
        } else {
            if let &mut None = buffer {
                *buffer = buffer_cache.pop_front();
            }
            let mut read_buffer = match buffer.take() {
                None => return RecvPacket::Pending,
                Some(buffer) => buffer,
            };
            let recv = recv_packet(&mut read_buffer, upstream, *bytes_read, 0,  needs_to_stay_awake, print_data);
            match recv {
                //TODO send to log
                RecvRes::Done(src_addr) => {
                    print_data.packets_recvd(1);
                    *bytes_read = 0;
                    // trace!("SOCKET recevd for {}.", src_addr);
                    *needs_to_stay_awake = true;
                    print_data.read_buffers_sent(1);
                    RecvPacket::FromClient(read_buffer, src_addr)
                },
                //FIXME remove from map
                RecvRes::Error => {
                    *bytes_read = 0;
                    // trace!("error; returned buffer now @ {}", buffer_cache.len());
                    buffer_cache.push_front(read_buffer);
                    RecvPacket::Err
                },

                RecvRes::NeedsMore(total_read) => {
                    *bytes_read = total_read;
                    *buffer = Some(read_buffer);
                    RecvPacket::Pending
                },
            }
        }
    }

    pub fn recv_packet_from_down(&mut self) -> RecvPacket {
        // trace!("SOCKET try recv");
        let &mut Self{
            ref mut buffer_cache,
            ref mut downstream,
            ref mut needs_to_stay_awake,
            ref mut print_data,
            ..
        } = self;
        let &mut Reader{ref mut buffer, ref mut bytes_read, stream: ref mut downstream} =
            match downstream.as_mut() {
                None => return RecvPacket::Pending,
                Some(down) => down,
            };
        if let &mut None = buffer {
            *buffer = buffer_cache.pop_front();
        }
        let mut read_buffer = match buffer.take() {
            None => return RecvPacket::Pending,
            Some(buffer) => buffer,
        };
        let recv = recv_packet(&mut read_buffer, downstream, *bytes_read, 0,  needs_to_stay_awake, print_data);
        match recv {
            //TODO send to log
            RecvRes::Done(src_addr) => {
                print_data.packets_recvd(1);
                *bytes_read = 0;
                // trace!("SOCKET recevd for {}.", src_addr);
                *needs_to_stay_awake = true;
                print_data.read_buffers_sent(1);
                RecvPacket::FromClient(read_buffer, src_addr)
            },
            //FIXME remove from map
            RecvRes::Error => {
                *bytes_read = 0;
                // trace!("error; returned buffer now @ {}", buffer_cache.len());
                buffer_cache.push_front(read_buffer);
                RecvPacket::Err
            },

            RecvRes::NeedsMore(total_read) => {
                *bytes_read = total_read;
                *buffer = Some(read_buffer);
                RecvPacket::Pending
            },
        }
    }

    pub fn send_burst(&mut self) -> Result<ShouldContinue, ()> {
        let &mut Self{
            ref mut being_written,
            ref mut bytes_written,
            ref mut upstream,
            ref mut downstream,
            ref mut needs_to_stay_awake,
            ref mut print_data,
        ..} = self;
        //TODO check who to send to
        let stream = match downstream {
            &mut Some(ref mut down) => &mut down.stream,
            &mut None => match upstream {
                &mut Some(ref mut up) => &mut up.stream,
                &mut None => return Err(()),
            },
        };
        // trace!("SOCKET send actual.");
        if being_written.is_empty() {
            //debug_assert!(pending.iter().all(|p| p.is_empty()));
            //TODO remove
            //assert!(pending.iter().all(|p| p.is_empty()));
            // trace!("SOCKET empty write.");
            //FIXME: this removes the hang? while slowing the server...
            //*needs_to_stay_awake = true;
            return Ok(false)
        }
        assert!(!being_written.first_bytes().is_empty());
        match stream.write(&being_written.first_bytes()[*bytes_written..]) {
            Err(e) =>
                if e.kind() == ErrorKind::WouldBlock { return Ok(false) }
                else {
                    //error!("send error {:?}", e);
                    return Err(())
                },
            Ok(i) => {
                *needs_to_stay_awake = true;
                print_data.bytes_sent(i as u64);
                *bytes_written = *bytes_written + i;
                // trace!("SOCKET sent {}B.", bytes_written);
            },
        }

        if *bytes_written < being_written.first_bytes().len() {
            return Ok(false)
        }

        // trace!("SOCKET finished sending burst {}B.", *bytes_written);
        *bytes_written = 0;
        being_written.swap_if_needed();
        print_data.sends(1);
        //Done with burst check if more bursts to be sent
        Ok(true)
    }

    pub fn add_downstream_send(&mut self, to_write: &[u8]) {
        //TODO Is there some maximum size at which we shouldn't buffer?
        //TODO can we simply write directly from the trie?
        //FIXME split up/down awake
        self.stay_awake();
        // trace!("SOCKET add downstream send");
        self.needs_to_stay_awake = true;
        // trace!("SOCKET send down {}B", to_write.len());
        self.print_data.sends_added(1);
        self.print_data.bytes_to_send(to_write.len() as u64);
        self.being_written.fill(&[to_write]);
    }

    pub fn add_downstream_send3(&mut self, to_write0: &[u8], to_write1: &[u8], to_write2: &[u8]) {
        //TODO Is there some maximum size at which we shouldn't buffer?
        //TODO can we simply write directly from the trie?
        //FIXME split up/down awake
        self.stay_awake();
        self.needs_to_stay_awake = true;
        self.print_data.sends_added(1);
        let write_len = to_write0.len() + to_write1.len() + to_write2.len();
        // trace!("SOCKET send down {}B", write_len);
        self.print_data.bytes_to_send(write_len as u64);
        self.being_written.fill(&[to_write0, to_write1, to_write2]);
    }

    pub fn add_downstream_contents(&mut self, to_write: EntryContents) {
        //FIXME split up/down awake
        self.stay_awake();
        self.needs_to_stay_awake = true;
        let write_len = to_write.len();
        // trace!("SOCKET send down contents {}B", write_len);
        self.print_data.sends_added(1);
        self.print_data.bytes_to_send(write_len as u64);
        self.being_written.fill_from_contents(to_write, &[]);
    }

    #[allow(dead_code)]
    pub fn add_downstream_contents2(
        &mut self, to_write: EntryContents, addr: &[u8]
    ) {
        //FIXME split up/down awake
        self.stay_awake();
        self.needs_to_stay_awake = true;
        let write_len = to_write.len() + addr.len();
        // trace!("SOCKET send down contents2 {}B", write_len);
        self.print_data.sends_added(1);
        self.print_data.bytes_to_send(write_len as u64);
        self.being_written.fill_from_contents(to_write, &[addr]);
    }

    pub fn add_downstream_contents3(
        &mut self, to_write: EntryContents, to_write1: &[u8], to_write2: &[u8]
    ) {
        //FIXME split up/down awake
        self.stay_awake();
        self.needs_to_stay_awake = true;
        let write_len = to_write.len() + to_write1.len() + to_write2.len();
        // trace!("SOCKET send down contents3 {}B", write_len);
        self.print_data.sends_added(1);
        self.print_data.bytes_to_send(write_len as u64);
        self.being_written.fill_from_contents(to_write, &[to_write1, to_write2]);
    }


    pub fn return_buffer(&mut self, buffer: Buffer) {
        //FIXME split up/down awake
        self.stay_awake();
        self.print_data.read_buffers_returned(1);
        self.buffer_cache.push_back(buffer);
        // trace!("returned buffer now @ {}", buffer_cache.len());
        assert!(self.buffer_cache.len() <= NUMBER_READ_BUFFERS);
    }

    //FIXME
    pub fn upstream(&self) -> Option<&TcpStream> {
        self.upstream.as_ref().map(|up| &up.stream)
    }
    pub fn downstream(&self) -> Option<&TcpStream> {
        self.downstream.as_ref().map(|down| &down.stream)
    }

    //FIXME
    pub fn is_backpressured(&self) -> bool {
        self.buffer_cache.is_empty() || self.being_written.has_pending()
    }

    //FIXME split up/down?
    pub fn stay_awake(&mut self) {
        self.needs_to_stay_awake = true;
    }

    //FIXME split up/down?
    pub fn should_be_awake(&self) -> bool {
        self.needs_to_stay_awake || self.is_staying_awake
    }

    //FIXME split up/down?
    pub fn needs_to_stay_awake(&self) -> bool {
        self.needs_to_stay_awake && !self.is_staying_awake
    }

    //FIXME split up/down?
    pub fn is_staying_awake(&mut self) {
        self.is_staying_awake = true;
    }

    //FIXME split up/down!?
    pub fn wake(&mut self) {
        self.needs_to_stay_awake = false;
        self.is_staying_awake = false;
    }

    #[allow(dead_code)]
    pub fn print_data(&self) -> &PerSocketData {
        &self.print_data
    }

    #[allow(dead_code)]
    pub fn more_data(&self) -> MoreData {
        MoreData::new(
            self.buffer_cache.len(),
            self.upstream.as_ref().map(|up| up.bytes_read).unwrap_or(0)
            + self.downstream.as_ref().map(|up| up.bytes_read).unwrap_or(0),
            self.being_written.pending_len(),
            self.bytes_written,
            self.pending.len(),
            self.needs_to_stay_awake,
        )
    }
}

#[derive(Debug)]
pub struct MoreData {
    #[cfg(feature = "print_stats")]
    num_read_buffers: usize,
    #[cfg(feature = "print_stats")]
    bytes_read: usize,
    #[cfg(feature = "print_stats")]
    being_written_first_len: usize,
    #[cfg(feature = "print_stats")]
    bytes_written: usize,
    #[cfg(feature = "print_stats")]
    num_pending: usize,
    #[cfg(feature = "print_stats")]
    needs_to_stay_awake: bool,
}

#[allow(dead_code)]
impl MoreData {
    #[cfg(feature = "print_stats")]
    fn new(
        num_read_buffers: usize,
        bytes_read: usize,
        being_written_first_len: usize,
        bytes_written: usize,
        num_pending: usize,
        needs_to_stay_awake: bool
    ) -> Self {
        MoreData {
            num_read_buffers: num_read_buffers,
            bytes_read: bytes_read,
            being_written_first_len: being_written_first_len,
            bytes_written: bytes_written,
            num_pending: num_pending,
            needs_to_stay_awake: needs_to_stay_awake
        }
    }

    #[cfg(not(feature = "print_stats"))]
    fn new(
        _: usize,
        _: usize,
        _: usize,
        _: usize,
        _: usize,
        _: bool
    ) -> Self { MoreData {} }
}

/*enum SendRes {
    Done,
    Error,
    NeedsMore(usize),
}

fn send_packet(buffer: &Buffer, mut stream: &TcpStream, sent: usize) -> SendRes {
    let bytes_to_write = buffer.entry_slice().len();
    match stream.write(&buffer.entry_slice()[sent..]) {
       Ok(i) if (sent + i) < bytes_to_write => SendRes::NeedsMore(sent + i),
       Ok(..) => SendRes::Done,
       Err(e) => if e.kind() == ErrorKind::WouldBlock { SendRes::NeedsMore(sent) }
                 else { error!("send error {:?}", e); SendRes::Error },
   }
}*/

enum RecvRes {
    Done(Ipv4SocketAddr),
    Error,
    NeedsMore(usize),
}

#[inline(always)]
fn recv_packet(
    buffer: &mut Buffer,
    mut stream: &TcpStream,
    mut read: usize,
    extra: usize,
    stay_awake: &mut bool,
    print_data: &mut PerSocketData,
)  -> RecvRes {
    use packets::Packet::WrapErr;
    loop {
        // trace!("WORKER recved {} bytes extra {}.", read, extra);
        let to_read = buffer.finished_at(read);
        let size = match to_read {
            Err(WrapErr::NotEnoughBytes(needs)) =>
                needs + mem::size_of::<Ipv4SocketAddr>() + extra,
            Err(err) => panic!("{:?}, {:?} {:?} @ {:?} => {:?}", err, extra, buffer, stream.local_addr(), stream.peer_addr()),
            Ok(size) if read < size + extra + mem::size_of::<Ipv4SocketAddr>() =>
                size + extra + mem::size_of::<Ipv4SocketAddr>(),
            Ok(..) => {
                debug_assert_eq!(
                    read, buffer.entry_size() + mem::size_of::<Ipv4SocketAddr>() + extra,//TODO if is_write { mem::size_of::<Ipv4SocketAddr>() } else { 0 },
                    "entry_size {}", buffer.entry_size()
                );
                //let src_addr = Ipv4SocketAddr::from_slice(
                //    &buffer[read-mem::size_of::<Ipv4SocketAddr>()-extra..read-extra]);
                let src_addr = Ipv4SocketAddr::from_slice(
                   &buffer[read-mem::size_of::<Ipv4SocketAddr>()..read]);
                // trace!("WORKER finished recv {} bytes for {}.", read, src_addr);
                //TODO ( if is_read { Some((receive_addr)) } else { None } )
                return RecvRes::Done(src_addr)
            },
        //TODO if is_write { mem::size_of::<Ipv4SocketAddr>() } else { 0 };
        };
        let r = stream.read(&mut buffer[read..size]);
        match r {
            Ok(0) => {
                *stay_awake = true;
                return RecvRes::NeedsMore(read)
            },
            Ok(i) => {
                print_data.bytes_recvd(i as u64);
                *stay_awake = true;
                read += i
            },
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => return RecvRes::NeedsMore(read),
            Err(ref e) if e.kind() == ErrorKind::Interrupted => return RecvRes::NeedsMore(read),
            Err(_) => {
                // error!("recv error {:?}", e);
                return RecvRes::Error
            },
        }
    }
}
