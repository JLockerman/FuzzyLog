// use prelude::*;

use std::collections::{LinkedList, VecDeque};
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

const WRITE_BUFFER_SIZE: usize = 4096;

#[derive(Debug)]
pub enum PerSocket {
    Upstream {
        being_read: VecDeque<Buffer>,
        bytes_read: usize,
        stream: TcpStream,
        needs_to_stay_awake: bool,
        is_staying_awake: bool,

        print_data: PerSocketData,
    },
    Downstream {
        being_written: DoubleBuffer,
        bytes_written: usize,
        stream: TcpStream,
        // pending: VecDeque<Vec<u8>>,
        needs_to_stay_awake: bool,
        is_staying_awake: bool,

        print_data: PerSocketData,
    },
    //FIXME Client should be divided into reader and writer?
    Client {
        being_read: VecDeque<Buffer>,
        bytes_read: usize,
        stream: TcpStream,
        being_written: DoubleBuffer,
        bytes_written: usize,
        pending: VecDeque<Vec<u8>>,
        needs_to_stay_awake: bool,
        is_staying_awake: bool,

        print_data: PerSocketData,
    }
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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PerSocketKind {
    Upstream,
    Downstream,
    Client,
}
/*
struct Upstream {
    being_read: Buffer,
    stream: TcpStream,
    bytes_handled: usize,
}

struct Downstream {
    being_written: Vec<u8>,
    bytes_written: usize,
    stream: TcpStream,
    pending: VecDeque<Vec<u8>>,
}

struct Client {
    being_read: Buffer,
    bytes_read: usize,
    being_written: Vec<u8>,
    bytes_written: usize,
    stream: TcpStream,
    pending: VecDeque<Vec<u8>>,
}
*/

pub enum RecvPacket {
    Err,
    Pending,
    FromUpstream(Buffer, Ipv4SocketAddr, u64),
    FromClient(Buffer, Ipv4SocketAddr),
}

impl PerSocket {
    /*
    Upstream {
        being_read: Buffer,
        bytes_read: usize,
        stream: TcpStream,
    },
    Downstream {
        being_written: Vec<u8>,
        bytes_written: usize,
        stream: TcpStream,
        pending: VecDeque<Vec<u8>>,
    },
    Client {
        being_read: Buffer,
        bytes_read: usize,
        stream: TcpStream,
        being_written: Vec<u8>,
        bytes_written: usize,
        pending: VecDeque<Vec<u8>>,
    }
    */
    pub fn client(stream: TcpStream) -> Self {
        PerSocket::Client {
            being_read: (0..NUMBER_READ_BUFFERS).map(|_| Buffer::empty()).collect(),
            bytes_read: 0,
            stream: stream,
            being_written: DoubleBuffer::with_first_buffer_capacity(WRITE_BUFFER_SIZE),
            bytes_written: 0,
            pending: Default::default(),
            needs_to_stay_awake: false,
            is_staying_awake: true,

            print_data: Default::default(),
        }
    }

    pub fn upstream(stream: TcpStream) -> Self {
        PerSocket::Upstream {
            being_read: (0..NUMBER_READ_BUFFERS).map(|_| Buffer::empty()).collect(),
            bytes_read: 0,
            stream: stream,
            needs_to_stay_awake: false,
            is_staying_awake: true,
            print_data: Default::default(),
        }
    }

    pub fn downstream(stream: TcpStream) -> Self {
        PerSocket::Downstream {
            being_written: DoubleBuffer::with_first_buffer_capacity(WRITE_BUFFER_SIZE),
            bytes_written: 0,
            // pending: Default::default(),
            stream: stream,
            needs_to_stay_awake: false,
            is_staying_awake: true,
            print_data: Default::default(),
        }
    }

    #[allow(dead_code)]
    pub fn kind(&self) -> PerSocketKind {
        match self {
            &PerSocket::Upstream{..} => PerSocketKind::Upstream,
            &PerSocket::Downstream{..} => PerSocketKind::Downstream,
            &PerSocket::Client{..} => PerSocketKind::Client,
        }
    }

    //TODO recv burst
    pub fn recv_packet(&mut self) -> RecvPacket {
        use self::PerSocket::*;
        // trace!("SOCKET try recv");
        match self {
            &mut Upstream {ref mut being_read, ref mut bytes_read, ref stream,
                ref mut needs_to_stay_awake, ref mut print_data, ..} => {
                if let Some(mut read_buffer) = being_read.pop_front() {
                    // trace!("SOCKET recv actual");
                    //TODO audit
                    let recv = recv_packet(&mut read_buffer, stream, *bytes_read, mem::size_of::<u64>(), needs_to_stay_awake, print_data);
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
                            error!("upstream; returned buffer now @ {}", being_read.len());
                            being_read.push_front(read_buffer);
                            RecvPacket::Err
                        },

                        RecvRes::NeedsMore(total_read) => {
                            *bytes_read = total_read;
                            being_read.push_front(read_buffer);
                            RecvPacket::Pending
                        },
                    }
                }
                else {
                    // trace!("SOCKET Upstream recv no buffer");
                    RecvPacket::Pending
                }
            }
            &mut Client {ref mut being_read, ref mut bytes_read, ref stream,
                ref mut needs_to_stay_awake, ref mut print_data, ..} => {
                if let Some(mut read_buffer) = being_read.pop_front() {
                    // trace!("SOCKET recv actual");
                    let recv = recv_packet(&mut read_buffer, stream, *bytes_read, 0,  needs_to_stay_awake, print_data);
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
                            // trace!("error; returned buffer now @ {}", being_read.len());
                            being_read.push_front(read_buffer);
                            RecvPacket::Err
                        },

                        RecvRes::NeedsMore(total_read) => {
                            *bytes_read = total_read;
                            being_read.push_front(read_buffer);
                            RecvPacket::Pending
                        },
                    }
                }
                else {
                    // trace!("SOCKET Client recv no buffer");
                    RecvPacket::Pending
                }
            }
            _ => unreachable!()
        }
    }

    pub fn send_burst(&mut self) -> Result<ShouldContinue, ()> {
        use self::PerSocket::*;
        match self {
            &mut Downstream {ref mut being_written, ref mut bytes_written, ref mut stream, ref mut needs_to_stay_awake, ref mut print_data, ..}
            | &mut Client {ref mut being_written, ref mut bytes_written, ref mut stream, ref mut needs_to_stay_awake, ref mut print_data, ..} => {
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
            },
            _ => unreachable!()
        }
    }

    pub fn add_downstream_send(&mut self, to_write: &[u8]) {
        //TODO Is there some maximum size at which we shouldn't buffer?
        //TODO can we simply write directly from the trie?
        use self::PerSocket::*;
        // trace!("SOCKET add downstream send");
        self.stay_awake();
        match self {
            &mut Downstream {ref mut being_written, ref mut print_data,
                ref mut needs_to_stay_awake, ..}
            | &mut Client {ref mut being_written, ref mut print_data,
                ref mut needs_to_stay_awake, ..} => {
                *needs_to_stay_awake = true;
                // trace!("SOCKET send down {}B", to_write.len());
                print_data.sends_added(1);
                print_data.bytes_to_send(to_write.len() as u64);
                being_written.fill(&[to_write]);
            }
            _ => unreachable!()
        }
    }

    pub fn add_downstream_send3(&mut self, to_write0: &[u8], to_write1: &[u8], to_write2: &[u8]) {
        //TODO Is there some maximum size at which we shouldn't buffer?
        //TODO can we simply write directly from the trie?
        use self::PerSocket::*;
        // trace!("SOCKET add downstream send");
        self.stay_awake();
        match self {
            &mut Downstream {ref mut being_written, ref mut print_data, ref mut needs_to_stay_awake, ..}
            | &mut Client {ref mut being_written, ref mut print_data,
                ref mut needs_to_stay_awake, ..} => {
                *needs_to_stay_awake = true;
                print_data.sends_added(1);
                let write_len = to_write0.len() + to_write1.len() + to_write2.len();
                // trace!("SOCKET send down {}B", write_len);
                print_data.bytes_to_send(write_len as u64);
                being_written.fill(&[to_write0, to_write1, to_write2]);
            }
            _ => unreachable!()
        }
    }

    pub fn add_downstream_contents(&mut self, to_write: EntryContents) {
        use self::PerSocket::*;
        // trace!("SOCKET add downstream send");
        self.stay_awake();
        match self {
            &mut Downstream {ref mut being_written, ref mut print_data,
                ref mut needs_to_stay_awake, ..}
            | &mut Client {ref mut being_written, ref mut print_data,
                ref mut needs_to_stay_awake, ..} => {
                *needs_to_stay_awake = true;
                let write_len = to_write.len();
                // trace!("SOCKET send down contents {}B", write_len);
                print_data.sends_added(1);
                print_data.bytes_to_send(write_len as u64);
                being_written.fill_from_contents(to_write, &[]);
            }
            _ => unreachable!()
        }
    }

    #[allow(dead_code)]
    pub fn add_downstream_contents2(
        &mut self, to_write: EntryContents, addr: &[u8]
    ) {
        use self::PerSocket::*;
        // trace!("SOCKET add downstream send");
        self.stay_awake();
        match self {
            &mut Downstream {ref mut being_written, ref mut print_data,
                ref mut needs_to_stay_awake, ..}
            | &mut Client {ref mut being_written, ref mut print_data,
                ref mut needs_to_stay_awake, ..} => {
                *needs_to_stay_awake = true;
                let write_len = to_write.len() + addr.len();
                // trace!("SOCKET send down contents2 {}B", write_len);
                print_data.sends_added(1);
                print_data.bytes_to_send(write_len as u64);
                being_written.fill_from_contents(to_write, &[addr]);
            }
            _ => unreachable!()
        }
    }

    pub fn add_downstream_contents3(
        &mut self, to_write: EntryContents, to_write1: &[u8], to_write2: &[u8]
    ) {
        use self::PerSocket::*;
        // trace!("SOCKET add downstream send");
        self.stay_awake();
        match self {
            &mut Downstream {ref mut being_written, ref mut print_data,
                ref mut needs_to_stay_awake, ..}
            | &mut Client {ref mut being_written, ref mut print_data,
                ref mut needs_to_stay_awake, ..} => {
                *needs_to_stay_awake = true;
                let write_len = to_write.len() + to_write1.len() + to_write2.len();
                // trace!("SOCKET send down contents3 {}B", write_len);
                print_data.sends_added(1);
                print_data.bytes_to_send(write_len as u64);
                being_written.fill_from_contents(to_write, &[to_write1, to_write2]);
            }
            _ => unreachable!()
        }
    }


    pub fn return_buffer(&mut self, buffer: Buffer) {
        use self::PerSocket::*;
        self.stay_awake();
        match self {
            &mut Client {ref mut being_read, ref mut print_data, ..}
            | &mut Upstream {ref mut being_read, ref mut print_data, ..} => {
                print_data.read_buffers_returned(1);
                being_read.push_back(buffer);
                // trace!("returned buffer now @ {}", being_read.len());
                assert!(being_read.len() <= NUMBER_READ_BUFFERS);

            },
            _ => unreachable!(),
        }
    }

    pub fn stream(&self) -> &TcpStream {
        use self::PerSocket::*;
        match self {
            &Downstream {ref stream, ..} | &Client {ref stream, ..} | &Upstream {ref stream, ..} =>
                stream,
        }
    }

    pub fn is_backpressured(&self) -> bool {
        use self::PerSocket::*;
        match self {
            &Client {ref being_read, ..}
            | &Upstream {ref being_read, ..} => {
                being_read.is_empty()

            },
            _ => false,
        }
    }

    fn stay_awake(&mut self) {
        use self::PerSocket::*;
        match self {
            &mut Downstream {ref mut needs_to_stay_awake, ..}
            | &mut Client {ref mut needs_to_stay_awake, ..}
            | &mut Upstream {ref mut needs_to_stay_awake, ..} => {
                *needs_to_stay_awake = true;
            }
        }
    }

    pub fn should_be_awake(&self) -> bool {
        use self::PerSocket::*;
        match self {
            &Downstream {needs_to_stay_awake, is_staying_awake, ..}
            | &Client {needs_to_stay_awake, is_staying_awake, ..}
            | &Upstream {needs_to_stay_awake, is_staying_awake, ..} => {
                needs_to_stay_awake || is_staying_awake
            }
        }
    }

    pub fn needs_to_stay_awake(&self) -> bool {
        use self::PerSocket::*;
        match self {
            &Downstream {needs_to_stay_awake, is_staying_awake, ..}
            | &Client {needs_to_stay_awake, is_staying_awake, ..}
            | &Upstream {needs_to_stay_awake, is_staying_awake, ..} => {
                needs_to_stay_awake && !is_staying_awake
            }
        }
    }

    pub fn is_staying_awake(&mut self) {
        use self::PerSocket::*;
        match self {
            &mut Downstream {ref mut is_staying_awake, ..}
            | &mut Client {ref mut is_staying_awake, ..}
            | &mut Upstream {ref mut is_staying_awake, ..} => {
                *is_staying_awake = true;
            }
        }
    }

    pub fn wake(&mut self) {
        use self::PerSocket::*;
        match self {
            &mut Downstream {ref mut needs_to_stay_awake, ref mut is_staying_awake, ..}
            | &mut Client {ref mut needs_to_stay_awake, ref mut is_staying_awake, ..}
            | &mut Upstream {ref mut needs_to_stay_awake, ref mut is_staying_awake, ..} => {
                *needs_to_stay_awake = false;
                *is_staying_awake = false;
            }
        }
    }

    #[allow(dead_code)]
    pub fn print_data(&self) -> &PerSocketData {
        use self::PerSocket::*;
        match self {
            &Downstream {ref print_data, ..}
            | &Client {ref print_data, ..}
            | &Upstream {ref print_data, ..} => {
                print_data
            }
        }
    }

    #[allow(dead_code)]
    pub fn more_data(&self) -> MoreData {
        use self::PerSocket::*;
        match self {
            &Upstream {
                ref being_read,
                ref bytes_read,
                ref needs_to_stay_awake,
                ..
            } =>
                MoreData::new(
                    being_read.len(),
                    *bytes_read,
                    0, 0, 0,
                    *needs_to_stay_awake
                ),

            &Downstream {
                ref being_written,
                ref bytes_written,
                ref needs_to_stay_awake,
                ..
            } =>
                MoreData::new(
                    0, 0,
                    being_written.first_bytes().len(),
                    *bytes_written,
                    being_written.pending_len(),
                    *needs_to_stay_awake
                ),

            &Client {
                ref being_read,
                ref bytes_read,
                ref being_written,
                ref bytes_written,
                ref pending,
                ref needs_to_stay_awake,
                ..
            } =>
                MoreData::new(
                    being_read.len(),
                    *bytes_read,
                    being_written.pending_len(),
                    *bytes_written,
                    pending.len(),
                    *needs_to_stay_awake
                ),
        }
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
            Err(err) => panic!("{:?}, {:?}", err, extra),
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
            Err(ref e) if e.kind() == ErrorKind::NotConnected => return RecvRes::NeedsMore(read),
            Err(_) => {
                // error!("recv error {:?}", e);
                return RecvRes::Error
            },
        }
    }
}
