// use prelude::*;

use std::collections::{/*LinkedList,*/ VecDeque};
use std::io::{self, ErrorKind};
use std::mem;
use socket_addr::Ipv4SocketAddr;

use byteorder::{ByteOrder, LittleEndian};

use mio::tcp::*;

use buffer::Buffer;

use packets::EntryContents;

use super::*;

use self::worker::WorkerInner;

use reactor::*;

pub type PerStream = TcpHandler<PacketReader, PacketHandler, WriteHandler>;

#[derive(Debug)]
pub struct WriteHandler {
    upstream: Option<mio::Token>,
}

#[derive(Debug)]
pub struct PacketHandler {
    token: mio::Token,
}

pub fn new_stream(
    stream: TcpStream, token: mio::Token, is_replica: bool, upstream: Option<mio::Token>
) -> PerStream {
    let reader = PacketReader{
        buffer_cache: Default::default(),
        is_replica,
    };
    let ps = (stream, reader, PacketHandler{ token }, WriteHandler{ upstream }).into();
    let _: &Handler<_, Error=io::Error> = &ps as &Handler<_, Error=io::Error>;
    ps
}

impl MessageHandler<WorkerInner, (Buffer, Ipv4SocketAddr, Option<u64>)> for PacketHandler {
    fn handle_message(
        &mut self,
        io: &mut TcpWriter,
        inner: &mut WorkerInner,
        (msg, addr, storage_loc): (Buffer, Ipv4SocketAddr, Option<u64>),
    ) -> Result<(), ()> {
        trace!("{} {:?}", addr, storage_loc);
        inner.handle_message(io, self.token, msg, addr, storage_loc)
    }
}

impl AfterWrite<WorkerInner> for WriteHandler {
    fn after_write(
        &mut self,
        io: &mut TcpWriter,
        inner: &mut WorkerInner,
        _token: mio::Token,
        _wrote: usize,
    ) {
        if let (Some(upstream), false) = (self.upstream, io.is_overflowing()) {
            inner.end_backpressure(upstream)
        }
    }
}

pub trait PerSocket {
    fn return_buffer(&mut self, buffer: Buffer);
}

impl PerSocket for PerStream {
    fn return_buffer(&mut self, _buffer: Buffer) {}
}

pub fn add_contents(io: &mut TcpWriter, contents: EntryContents) {
    io.add_contents_to_write(contents, &[])
}

pub struct PacketReader {
    buffer_cache: VecDeque<Buffer>,
    is_replica: bool,
}

impl MessageReader for PacketReader {
    type Message = (Buffer, Ipv4SocketAddr, Option<u64>);
    type Error = ErrorKind;

    fn deserialize_message(
        &mut self,
        bytes: &[u8]
    ) -> Result<(Self::Message, usize), MessageReaderError<Self::Error>> {
        use self::MessageReaderError::*;
        use packets::Packet::WrapErr;

        let extra = if self.is_replica { mem::size_of::<u64>() } else { 0 };

        let to_read = unsafe { EntryContents::try_ref(bytes).map(|(c, _)| c.len()) };
        let size = to_read.map_err(|e| match e {
            WrapErr::NotEnoughBytes(needs) =>
                NeedMoreBytes(needs + mem::size_of::<Ipv4SocketAddr>() + extra),
            _ => Other(ErrorKind::InvalidInput),
        })?;

        if bytes.len() < size + extra + mem::size_of::<Ipv4SocketAddr>() {
            let needs = size + extra + mem::size_of::<Ipv4SocketAddr>() - bytes.len();
            return Err(NeedMoreBytes(needs))?
        }


        //FIXME buffer cache
        let buffer = Buffer::wrap_vec(bytes[..size].to_vec());
        let storage_loc = match self.is_replica {
            false => {
                assert_eq!(extra, 0);
                None
            },
            true => {
                let end = size + extra;
                let storage_loc = LittleEndian::read_u64(&bytes[size..end]);
                Some(storage_loc)
            },
        };
        let src_addr = Ipv4SocketAddr::from_slice(&bytes[(size + extra)..]);
        let size_read = size + extra + mem::size_of::<Ipv4SocketAddr>();
        Ok(((buffer, src_addr, storage_loc), size_read))
    }

}
