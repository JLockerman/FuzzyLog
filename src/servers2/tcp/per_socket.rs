use prelude::*;

use std::collections::VecDeque;
use std::io::{Read, Write, ErrorKind};
use std::mem;
use socket_addr::Ipv4SocketAddr;

use byteorder::{ByteOrder, LittleEndian};

use mio::tcp::*;

use buffer::Buffer;

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

const WRITE_BUFFER_SIZE: usize = 40000;

#[derive(Debug)]
pub enum PerSocket {
    Upstream {
        being_read: VecDeque<Buffer>,
        bytes_read: usize,
        stream: TcpStream,
        needs_to_stay_awake: bool,

        print_data: PerSocketData,
    },
    Downstream {
        being_written: Vec<u8>,
        bytes_written: usize,
        stream: TcpStream,
        pending: VecDeque<Vec<u8>>,
        needs_to_stay_awake: bool,

        print_data: PerSocketData,
    },
    //FIXME Client should be divided into reader and writer?
    Client {
        being_read: VecDeque<Buffer>,
        bytes_read: usize,
        stream: TcpStream,
        being_written: Vec<u8>,
        bytes_written: usize,
        pending: VecDeque<Vec<u8>>,
        needs_to_stay_awake: bool,

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
            being_read: (0..NUMBER_READ_BUFFERS).map(|_| Buffer::no_drop()).collect(),
            bytes_read: 0,
            stream: stream,
            being_written: Vec::with_capacity(WRITE_BUFFER_SIZE),
            bytes_written: 0,
            pending: Default::default(),
            needs_to_stay_awake: true,
            print_data: Default::default(),
        }
    }

    pub fn upstream(stream: TcpStream) -> Self {
        PerSocket::Upstream {
            being_read: (0..NUMBER_READ_BUFFERS).map(|_| Buffer::no_drop()).collect(),
            bytes_read: 0,
            stream: stream,
            needs_to_stay_awake: true,
            print_data: Default::default(),
        }
    }

    pub fn downstream(stream: TcpStream) -> Self {
        PerSocket::Downstream {
            being_written: Vec::with_capacity(WRITE_BUFFER_SIZE),
            bytes_written: 0,
            pending: Default::default(),
            stream: stream,
            needs_to_stay_awake: true,
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
        trace!("SOCKET try recv");
        match self {
            &mut Upstream {ref mut being_read, ref mut bytes_read, ref stream,
                ref mut needs_to_stay_awake, ref mut print_data} => {
                if let Some(mut read_buffer) = being_read.pop_front() {
                    trace!("SOCKET recv actual");
                    //TODO audit
                    let recv = recv_packet(&mut read_buffer, stream, *bytes_read, mem::size_of::<u64>(), print_data);
                    match recv {
                        //TODO send to log
                        RecvRes::Done(src_addr) => {
                            print_data.packets_recvd(1);
                            *bytes_read = 0;
                            trace!("SOCKET recevd replication for {}.", src_addr);
                            let entry_size = read_buffer.entry_size();
                            let end = entry_size + mem::size_of::<u64>();
                            let storage_loc = LittleEndian::read_u64(&read_buffer[entry_size..end]);;
                            *needs_to_stay_awake = true;
                            RecvPacket::FromUpstream(read_buffer, src_addr, storage_loc)
                        },
                        //FIXME remove from map
                        RecvRes::Error => {
                            *bytes_read = 0;
                            trace!("error; returned buffer now @ {}", being_read.len());
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
                    trace!("SOCKET Upstream recv no buffer");
                    RecvPacket::Pending
                }
            }
            &mut Client {ref mut being_read, ref mut bytes_read, ref stream,
                ref mut needs_to_stay_awake, ref mut print_data, ..} => {
                if let Some(mut read_buffer) = being_read.pop_front() {
                    trace!("SOCKET recv actual");
                    let recv = recv_packet(&mut read_buffer, stream, *bytes_read, 0, print_data);
                    match recv {
                        //TODO send to log
                        RecvRes::Done(src_addr) => {
                            print_data.packets_recvd(1);
                            print_data.bytes_recvd(*bytes_read as u64);
                            *bytes_read = 0;
                            trace!("SOCKET recevd for {}.", src_addr);
                            *needs_to_stay_awake = true;
                            RecvPacket::FromClient(read_buffer, src_addr)
                        },
                        //FIXME remove from map
                        RecvRes::Error => {
                            *bytes_read = 0;
                            trace!("error; returned buffer now @ {}", being_read.len());
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
                    trace!("SOCKET Client recv no buffer");
                    RecvPacket::Pending
                }
            }
            _ => unreachable!()
        }
    }

    pub fn send_burst(&mut self) -> Result<ShouldContinue, ()> {
        use self::PerSocket::*;
        match self {
            &mut Downstream {ref mut being_written, ref mut bytes_written, ref mut stream, ref mut pending, ref mut needs_to_stay_awake, ref mut print_data}
            | &mut Client {ref mut being_written, ref mut bytes_written, ref mut stream, ref mut pending, ref mut needs_to_stay_awake, ref mut print_data, ..} => {
                trace!("SOCKET send actual.");
                let to_write = being_written.len();
                if to_write == 0 {
                    for p in pending.iter() {
                        assert!(p.is_empty())
                    }
                    trace!("SOCKET empty write.");
                    return Ok(false)
                }
                match stream.write(&being_written[*bytes_written..]) {
                    Err(e) => if e.kind() == ErrorKind::WouldBlock { Ok(false) }
                        else {
                            error!("send error {:?}", e);
                            Err(())
                        },
                    Ok(i) if (*bytes_written + i) < to_write => {
                        *bytes_written = *bytes_written + i;
                        print_data.bytes_sent(i as u64);
                        trace!("SOCKET sent {}B.", bytes_written);
                        Ok(false)
                    },
                    Ok(i) => {
                        trace!("SOCKET finished sending burst {}B.", *bytes_written + i);
                        *needs_to_stay_awake = true;
                        //Done with burst check if more bursts to be sent
                        debug_assert_eq!(*bytes_written + i, being_written.len());
                        print_data.sends(1);
                        print_data.bytes_sent(i as u64);
                        being_written.clear();
                        *bytes_written = 0;
                        if let Some(buffer) = pending.pop_front() {
                            if !buffer.is_empty() {
                                trace!("SOCKET swap buffer.");
                                let old_buffer = mem::replace(being_written, buffer);
                                if pending.is_empty() {
                                    pending.push_back(old_buffer);
                                    return Ok(true)
                                }
                                let (should_replace, add_anyway) = pending.back().map(|v| {
                                    let should_replace =
                                        v.is_empty()
                                        && v.capacity() <= old_buffer.capacity();
                                    let add_anyway =
                                        !v.is_empty() &&
                                        old_buffer.capacity() > WRITE_BUFFER_SIZE / 4;
                                    (should_replace, add_anyway)
                                }).unwrap_or((false, false));
                                if should_replace {
                                    pending.back_mut().map(|v| *v = old_buffer);
                                    return Ok(true)
                                }
                                if add_anyway {
                                    pending.push_back(old_buffer)
                                }
                                return Ok(true)
                            } else {
                                debug_assert!(pending.front().map(|v| v.is_empty()).unwrap_or(true));
                                debug_assert!(pending.back().map(|v| v.is_empty()).unwrap_or(true));
                                pending.push_front(buffer)
                            }

                        }
                        Ok(true)
                    },
                }
            },
            _ => unreachable!()
        }
    }

    pub fn add_downstream_send(&mut self, to_write: &[u8]) {
        //TODO Is there some maximum size at which we shouldn't buffer?
        //TODO can we simply write directly from the trie?
        use self::PerSocket::*;
        trace!("SOCKET add downstream send");
        self.stay_awake();
        match self {
            &mut Downstream {ref mut being_written, ref mut pending, ref mut print_data, ..}
            | &mut Client {ref mut being_written, ref mut pending, ref mut print_data, ..} => {
                trace!("SOCKET send down {}B", to_write.len());
                print_data.sends_added(1);
                print_data.bytes_to_send(to_write.len() as u64);
                //FIXME send src_addr
                //TODO if being_written is empty try to send immediately, place remainder
                let no_other_inhabited_buffer =
                    pending.front().map(|v| v.is_empty()).unwrap_or(true);
                if no_other_inhabited_buffer && being_written.capacity() - being_written.len() >= to_write.len() {
                    debug_assert!(
                        pending.front().unwrap_or(&Vec::new()).is_empty(),
                        "being written {{cap: {}, len: {}}}, >= to_write {{len : {}}},\npending {:?}",
                        being_written.capacity(), being_written.len(), to_write.len(),
                        pending
                    );
                    being_written.extend_from_slice(to_write);
                    return
                }
                if let Some(buffer) = pending.back_mut() {
                    if buffer.capacity() - buffer.len() >= to_write.len()
                    || buffer.capacity() < WRITE_BUFFER_SIZE {
                        buffer.extend_from_slice(&to_write[..]);
                        return
                    }
                };
                debug_assert!(
                    pending.front().map(|v| !v.is_empty()).unwrap_or(true),
                    "pending {:?}", pending
                 );
                pending.push_back(to_write[..].to_vec())
            }
            _ => unreachable!()
        }
    }

    pub fn return_buffer(&mut self, buffer: Buffer) {
        use self::PerSocket::*;
        self.stay_awake();
        match self {
            &mut Client {ref mut being_read, ..}
            | &mut Upstream {ref mut being_read, ..} => {
                being_read.push_back(buffer);
                trace!("returned buffer now @ {}", being_read.len());
                debug_assert!(being_read.len() <= NUMBER_READ_BUFFERS);

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

    pub fn needs_to_stay_awake(&self) -> bool {
        use self::PerSocket::*;
        match self {
            &Downstream {needs_to_stay_awake, ..}
            | &Client {needs_to_stay_awake, ..}
            | &Upstream {needs_to_stay_awake, ..} => {
                needs_to_stay_awake
            }
        }
    }

    pub fn wake(&mut self) {
        use self::PerSocket::*;
        match self {
            &mut Downstream {ref mut needs_to_stay_awake, ..}
            | &mut Client {ref mut needs_to_stay_awake, ..}
            | &mut Upstream {ref mut needs_to_stay_awake, ..} => {
                *needs_to_stay_awake = false;
            }
        }
    }

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

fn recv_packet(
    buffer: &mut Buffer,
    mut stream: &TcpStream,
    mut read: usize,
    extra: usize,
    print_data: &mut PerSocketData,
)  -> RecvRes {
    let bhs = base_header_size();
    if read < bhs {
        let r = stream.read(&mut buffer[read..bhs])
            .or_else(|e| if e.kind() == ErrorKind::WouldBlock { Ok(0) } else {
                error!("recv error {:?}", e);
                Err(e)
            })
            .ok();
        match r {
            Some(i) => {
                print_data.bytes_recvd(i as u64);
                read += i
            },
            None => return RecvRes::Error,
        }
        if read < bhs {
            return RecvRes::NeedsMore(read)
        }
    }
    trace!("WORKER recved {} bytes.", read);
    let (header_size, _is_write) = {
        let e = buffer.entry();
        (e.header_size(), e.kind.layout().is_write())
    };
    assert!(header_size >= base_header_size());
    if read < header_size {
        let r = stream.read(&mut buffer[read..header_size])
            .or_else(|e| if e.kind() == ErrorKind::WouldBlock { Ok(0) } else {
                error!("recv error {:?}", e);
                Err(e)
            } )
            .ok();
        match r {
            Some(i) => {
                print_data.bytes_recvd(i as u64);
                read += i
            },
            None => return RecvRes::Error,
        }
        if read < header_size {
            return RecvRes::NeedsMore(read)
        }
    }

    let size = buffer.entry().entry_size() + mem::size_of::<Ipv4SocketAddr>() + extra;//TODO if is_write { mem::size_of::<Ipv4SocketAddr>() } else { 0 };
    //let size = buffer.entry_size() + 6;
    debug_assert!(read <= size);
    if read < size {
        let r = stream.read(&mut buffer[read..size])
            .or_else(|e| if e.kind() == ErrorKind::WouldBlock { Ok(0) } else {
                error!("recv error {:?}", e);
                Err(e)
            } )
            .ok();
        match r {
            Some(i) => {
                debug_assert!(i <= (size - read),
                    "read {} wanted to read {}: {} - {}",
                    i, (size - read), size, read
                );
                print_data.bytes_recvd(i as u64);
                read += i
            },
            None => return RecvRes::Error,
        }
        if read < size {
            return RecvRes::NeedsMore(read);
        }
    }
    debug_assert!(buffer.packet_fits());
    // assert!(payload_size >= header_size);
    debug_assert_eq!(
        read, buffer.entry_size() + mem::size_of::<Ipv4SocketAddr>() + extra,//TODO if is_write { mem::size_of::<Ipv4SocketAddr>() } else { 0 },
        "entry_size {}", buffer.entry().entry_size()
    );
    let src_addr = Ipv4SocketAddr::from_slice(&buffer[read-6-extra..read-extra]);
    trace!("WORKER finished recv {} bytes for {}.", read, src_addr);
    RecvRes::Done(src_addr) //TODO ( if is_read { Some((receive_addr)) } else { None } )
}
