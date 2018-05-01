
extern crate fuzzy_log_packets;
extern crate fuzzy_log_util;

#[macro_use] extern crate log;

extern crate mio;

use std::collections::VecDeque;
use std::collections::hash_map::Entry as HashEntry;
use std::{io, mem};
use std::io::{Read, Write};
use std::time::Duration;

use fuzzy_log_packets::EntryContents;
use fuzzy_log_packets::double_buffer::DoubleBuffer;

use fuzzy_log_util::hash::HashMap;

use mio::tcp::TcpStream;

pub type ShouldRemove = bool;

pub trait Wakeable {
    fn init(&mut self, token: mio::Token, poll: &mut mio::Poll);
    fn needs_to_mark_as_staying_awake(&mut self, token: mio::Token) -> bool;
    fn mark_as_staying_awake(&mut self, token: mio::Token);
    fn is_marked_as_staying_awake(&self, token: mio::Token) -> bool;
}

pub trait Handler<Inner>: Wakeable {
    type Error;

    fn on_event(&mut self, inner: &mut Inner, token: mio::Token, event: mio::Event)
    -> Result<(), Self::Error>;

    fn on_poll(&mut self, inner: &mut Inner, token: mio::Token) -> Result<(), Self::Error>;

    fn on_error(&mut self, error: Self::Error, poll: &mut mio::Poll) -> ShouldRemove;

    //FIXME should be own trait
    fn after_work(&mut self, _inner: &mut Inner) {}
}

///////////////////////////////////////

#[derive(Debug)]
pub struct Reactor<PerStream, Inner> {
    events: mio::Events,
    io_state: IoState<PerStream>,
    inner: Inner,
}

#[derive(Debug)]
pub struct IoState<PerStream> {
    streams: HashMap<mio::Token, PerStream>,
    awake: VecDeque<mio::Token>,
    poll: mio::Poll,
}

impl<PerStream, Inner> Reactor<PerStream, Inner>
where Inner: Handler<IoState<PerStream>> {

    pub fn with_inner(token: mio::Token, inner: Inner) -> io::Result<Self> {
        let mut this = Self {
            events: mio::Events::with_capacity(1024),
            io_state: IoState {
                streams: HashMap::default(),
                awake: VecDeque::default(),
                poll: mio::Poll::new()?,
            },

            inner,
        };
        this.inner.init(token, &mut this.io_state.poll);
        if this.inner.needs_to_mark_as_staying_awake(token) {
            this.io_state.awake.push_back(token);
            this.inner.mark_as_staying_awake(token);
        }
        Ok(this)
    }

    pub fn inner(&mut self) -> &mut Inner {
        &mut self.inner
    }
}

impl<PerStream, Inner> Reactor<PerStream, Inner>
where
    PerStream: Handler<Inner>, {

    pub fn add_stream(&mut self, token: mio::Token, stream: PerStream) -> Option<PerStream> {
        let stream = match self.io_state.streams.entry(token) {
            HashEntry::Occupied(..) => return Some(stream),
            HashEntry::Vacant(v) => v.insert(stream),
        };
        stream.init(token, &mut self.io_state.poll);
        self.io_state.awake.push_back(token);
        None
    }
}

impl<PerStream, Inner> Reactor<PerStream, Inner>
where
    PerStream: Handler<Inner>,
    Inner: Handler<IoState<PerStream>> {

    pub fn run(&mut self) -> Result<(), io::Error> {
        // let mut events = mio::Events::with_capacity(1024);
        let mut running = VecDeque::new();
        loop {
            self.io_state.poll.poll(&mut self.events, None)?;

            self.handle_new_events()?;

            'work: loop {
                mem::swap(&mut running, &mut self.io_state.awake);
                if running.is_empty() { break 'work }
                for token in running.drain(..) {
                    let handled;
                    match self.io_state.streams.entry(token) {
                        HashEntry::Vacant(..) => handled = false,
                        HashEntry::Occupied(mut o) => {
                            handled = true;
                            let error = o.get_mut().on_poll(&mut self.inner, token);

                            if let Err(e) = error {
                                if o.get_mut().on_error(e, &mut self.io_state.poll) {
                                    o.remove();
                                    continue
                                }
                            }

                            let mut o = o.get_mut();
                            if o.needs_to_mark_as_staying_awake(token) {
                                self.io_state.awake.push_back(token);
                                o.mark_as_staying_awake(token);
                            }
                        },
                    }
                    if !handled {
                        let error = self.inner.on_poll(&mut self.io_state, token);
                        if let Err(e) = error {
                            if self.inner.on_error(e, &mut self.io_state.poll) {
                                //FIXME we need to put the rest of running into awake
                                #[allow(unreachable_code)]
                                return Err(unimplemented!())
                            }
                        }

                        if self.inner.needs_to_mark_as_staying_awake(token) {
                            self.io_state.awake.push_back(token);
                            self.inner.mark_as_staying_awake(token);
                        }
                    }
                    self.io_state.poll.poll(&mut self.events, Some(Duration::from_secs(0)))?;
                    self.handle_new_events()?;
                }
                self.inner.after_work(&mut self.io_state);
            }
        }
    }

    fn handle_new_events(&mut self) -> Result<(), io::Error> {
        for event in &self.events {
            let token = event.token();
            let handled;
            match self.io_state.streams.entry(token) {
                HashEntry::Vacant(..) => handled = false,
                HashEntry::Occupied(mut o) => {
                    handled = true;
                    let error = o.get_mut().on_event(&mut self.inner, token, event);

                    if let Err(e) = error {
                        if o.get_mut().on_error(e, &mut self.io_state.poll) {
                            o.remove();
                            continue
                        }
                    }

                    let mut o = o.get_mut();
                    if o.needs_to_mark_as_staying_awake(token) {
                        self.io_state.awake.push_back(token);
                        o.mark_as_staying_awake(token);
                    }

                },
            }

            if !handled {
                let error = self.inner.on_event(&mut self.io_state, token, event);
                if let Err(e) = error {
                    if self.inner.on_error(e, &mut self.io_state.poll) {
                        //FIXME
                        #[allow(unreachable_code)]
                        return Err(unimplemented!())
                    }
                }

                if self.inner.needs_to_mark_as_staying_awake(token) {
                    self.io_state.awake.push_back(token);
                    self.inner.mark_as_staying_awake(token);
                }
            }
        }
        Ok(())
    }
}

impl<PerStream> IoState<PerStream> {
    pub fn get_for(&self, token: mio::Token) -> Option<&PerStream> {
        self.streams.get(&token)
    }

    #[deprecated]
    pub fn get_mut_for(&mut self, token: mio::Token) -> Option<&mut PerStream> {
        self.streams.get_mut(&token)
    }

    pub fn mutate<F, T>(&mut self, token: mio::Token, f: F) -> Option<T>
    where
        F: FnOnce(&mut PerStream) -> T,
        PerStream: Wakeable, {

        let awake = &mut self.awake;
        self.streams.get_mut(&token).map(|stream| {
            let ret = f(stream);
            if !stream.is_marked_as_staying_awake(token) {
                awake.push_back(token);
                stream.mark_as_staying_awake(token);
            }
            ret
        })
    }

    pub fn wake(&mut self, token: mio::Token) -> Option<bool>
    where PerStream: Wakeable {
        let per_stream = self.streams.get_mut(&token);
        let awake = &mut self.awake;
        per_stream.map(|ps| {
            if ps.is_marked_as_staying_awake(token) {
                return false
            }
            awake.push_back(token);
            ps.mark_as_staying_awake(token);
            true
        })
    }

    pub fn add_stream(&mut self, token: mio::Token, ps: PerStream)
    -> Result<&mut PerStream, PerStream>
    where PerStream: Wakeable {
        let stream = match self.streams.entry(token) {
            HashEntry::Occupied(..) => return Err(ps)?,
            HashEntry::Vacant(v) => v.insert(ps),
        };
        stream.init(token, &mut self.poll);
        if stream.needs_to_mark_as_staying_awake(token) {
            self.awake.push_back(token);
            stream.mark_as_staying_awake(token);
        }
        Ok(stream)
    }
}

///////////////////////////////////////

impl Wakeable for () {
    fn init(&mut self, _token: mio::Token, _poll: &mut mio::Poll) {}
    fn needs_to_mark_as_staying_awake(&mut self, _token: mio::Token) -> bool {
        false
    }
    fn mark_as_staying_awake(&mut self, _token: mio::Token) {}
    fn is_marked_as_staying_awake(&self, _token: mio::Token) -> bool {
        true
    }
}

impl<Inner> Handler<Inner> for () {
    type Error = ();

    fn on_event(&mut self, _inner: &mut Inner, _token: mio::Token, _event: mio::Event)
    -> Result<(), Self::Error> {
        Ok(())
    }

    fn on_poll(&mut self, _inner: &mut Inner, _token: mio::Token) -> Result<(), Self::Error> {
        Ok(())
    }

    fn on_error(&mut self, _error: Self::Error, _poll: &mut mio::Poll) -> ShouldRemove {
        false
    }
}

///////////////////////////////////////
///////////////////////////////////////
///////////////////////////////////////

#[derive(Debug)]
pub struct TcpHandler<PacketReader, PacketHandler, WriteHandler=()> {
    io: TcpIo,
    pub reader: PacketReader,
    pub handler: PacketHandler,
    pub after_write: WriteHandler
}

impl<PacketReader, PacketHandler> TcpHandler<PacketReader, PacketHandler, ()>
where
    PacketReader: MessageReader, {

    pub fn new(stream: TcpStream, reader: PacketReader, handler: PacketHandler)
    -> Self {
        Self {
            io: TcpIo::new(stream),
            reader,
            handler,
            after_write: (),
        }
    }
}

impl<PacketReader, PacketHandler>
    From<(TcpStream, PacketReader, PacketHandler)>
for TcpHandler<PacketReader, PacketHandler, ()> {
    fn from(
        (stream, reader, handler): (TcpStream, PacketReader, PacketHandler))
    -> Self {
        Self {
            io: TcpIo::new(stream),
            reader,
            handler,
            after_write: (),
        }
    }
}

impl<PacketReader, PacketHandler, WriteHandler>
    From<(TcpStream, PacketReader, PacketHandler, WriteHandler)>
for TcpHandler<PacketReader, PacketHandler, WriteHandler>
where PacketReader: MessageReader {

    fn from(
        (stream, reader, handler, after_write): (TcpStream, PacketReader, PacketHandler, WriteHandler))
    -> Self {
        Self {
            io: TcpIo::new(stream),
            reader,
            handler,
            after_write: after_write,
        }
    }

}

impl<PacketReader, PacketHandler, WriteHandler> Wakeable
for TcpHandler<PacketReader, PacketHandler, WriteHandler> {
    fn init(&mut self, token: mio::Token, poll: &mut mio::Poll) {
        self.io.register_to(token, poll).unwrap();
    }

    fn needs_to_mark_as_staying_awake(&mut self, _token: mio::Token) -> bool {
        self.io.needs_to_mark_as_staying_awake()
    }

    fn mark_as_staying_awake(&mut self, _token: mio::Token) {
        self.io.mark_as_staying_awake()
    }

    fn is_marked_as_staying_awake(&self, _token: mio::Token) -> bool {
        self.io.is_marked_as_staying_awake()
    }
}

impl<Inner, PacketReader, PacketHandler, WriteHandler> Handler<Inner>
for TcpHandler<PacketReader, PacketHandler, WriteHandler>
where
    PacketReader: MessageReader,
    PacketHandler: MessageHandler<Inner, PacketReader::Message>,
    WriteHandler: AfterWrite<Inner> {

    type Error = io::Error;

    fn on_event(&mut self, inner: &mut Inner, token: mio::Token, event: mio::Event)
    -> Result<(), Self::Error> {
        self.io.on_wake();
        if event.readiness().is_readable() {
            self.io.set_polling_read()
        }
        if event.readiness().is_writable() {
            self.io.set_polling_write()
        }
        self.on_poll(inner, token)
    }

    fn on_poll(&mut self, inner: &mut Inner, token: mio::Token) -> Result<(), Self::Error> {
        use MessageReaderError::*;

        self.io.on_wake();

        if self.io.is_polling_write() {
            let wrote = self.io.write()?;
            let mut writer = TcpWriter {
                write_buffer: &mut self.io.write_buffer,
                polling_write: &mut self.io.polling_write,
            };
            self.after_write.after_write(&mut writer, inner, token, wrote);
        }
        if self.io.is_polling_read() {
            self.io.read()?;
            let mut used = 0;
            let mut additional_needed = 0;
            {
                let read_bytes = &self.io.read_buffer[..self.io.bytes_read];
                let mut writer = TcpWriter {
                    write_buffer: &mut self.io.write_buffer,
                    polling_write: &mut self.io.polling_write,
                };
                for message in self.reader.deserialize_messages(read_bytes, &mut used) {
                    match message {
                        Err(NeedMoreBytes(more)) => additional_needed += more,
                        Err(e) => panic!("{:?}", e),
                        Ok(msg) => {
                            let err = self.handler.handle_message(&mut writer, inner, msg);
                            if let Err(()) = err {
                                //FIXME
                                #[allow(unreachable_code)]
                                return Err(unimplemented!())
                            }
                        }
                    }
                }
            }


            self.io.consume_bytes(used);
            self.io.ensure_additional_read(additional_needed);
        }

        Ok(())
    }

    fn on_error(&mut self, _error: Self::Error, poll: &mut mio::Poll) -> ShouldRemove {
        error!("tcp error {} @ {:#?}", _error, self.io.stream);
        self.io.on_error(poll)
    }
}

impl<PacketReader, PacketHandler, WriteHandler>
TcpHandler<PacketReader, PacketHandler, WriteHandler> {

    pub fn ignore_backpressure(&mut self) {
        self.io.ignore_backpressure();
    }

    pub fn add_writes(&mut self, bytes: &[&[u8]]) {
        self.io.add_bytes_to_write(bytes)
    }

    pub fn add_contents(&mut self, contents: EntryContents, extra: &[&[u8]]) {
        self.io.add_contents_to_write(contents, extra)
    }

    pub fn is_overflowing(&self) -> bool {
        self.io.is_overflowing()
    }

    pub fn is_backpressured(&mut self) -> bool {
        self.io.is_backpressured()
    }

    pub fn mark_as_backpressured(&mut self) {
        self.io.mark_as_backpressured()
    }

    pub fn mark_as_not_backpressured(&mut self) {
        self.io.mark_as_not_backpressured()
    }
}

///////////////////

pub trait MessageHandler<Inner, Message> {
    fn handle_message(&mut self, io: &mut TcpWriter, inner: &mut Inner, msg: Message)
    -> Result<(), ()>;
}

///////////////////

#[derive(Debug)]
pub enum MessageReaderError<OtherError> {
    NeedMoreBytes(usize),
    Other(OtherError)
}

pub trait MessageReader {
    type Message;

    type Error: ::std::fmt::Debug;

    fn deserialize_message(&mut self, bytes: &[u8])
    -> Result<(Self::Message, usize), MessageReaderError<Self::Error>>;

    fn deserialize_messages<'s>(&'s mut self, bytes: &'s [u8], size_out: &'s mut usize)
    -> Messages<'s, Self> {
        Messages {
            bytes,
            read: 0,
            size_out,
            reader: self,
            done: false,
        }
    }
}

///////////////////

pub trait AfterWrite<Inner> {
    fn after_write(
        &mut self,
        io: &mut TcpWriter,
        inner: &mut Inner,
        token: mio::Token,
        wrote: usize
    );
}

impl<Inner> AfterWrite<Inner> for () {
    fn after_write(&mut self, _: &mut TcpWriter, _: &mut Inner, _: mio::Token, _: usize) {}
}

///////////////////

#[derive(Debug)]
pub struct Messages<'r, Reader: 'r + ?Sized> {
    bytes: &'r [u8],
    read: usize,
    size_out: &'r mut usize,

    reader: &'r mut Reader,
    done: bool,
}

impl<'r, Reader> Iterator for Messages<'r, Reader>
where Reader: MessageReader {
    type Item = Result<Reader::Message, MessageReaderError<Reader::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done { return None }
        let bytes = &self.bytes[self.read..];
        let msg = self.reader.deserialize_message(bytes);
        match msg {
            Ok((msg, read)) => {
                self.read += read;
                *self.size_out += read;
                Some(Ok(msg))
            },
            Err(e) => {
                self.done = true;
                Some(Err(e))
            },
        }
    }
}


///////////////////////////////////////

#[derive(Debug)]
pub struct TcpIo {
    stream: TcpStream,

    read_buffer: Vec<u8>,
    bytes_read: usize,


    write_buffer: DoubleBuffer,
    bytes_written: usize,

    polling_read: bool,
    polling_write: bool,

    is_marked_as_staying_awake: bool,
    writes_backpressure_reads: bool,
    ignore_backpressure: bool,
    is_marked_as_backpressured: bool,
}

impl TcpIo {

    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,

            read_buffer: vec![0; 128],
            bytes_read: 0,

            write_buffer: DoubleBuffer::with_first_buffer_capacity(4096),
            bytes_written: 0,

            polling_read: true,
            polling_write: false,

            is_marked_as_staying_awake: false,
            writes_backpressure_reads: false,
            ignore_backpressure: false,
            is_marked_as_backpressured: false,
        }
    }

    pub fn set_writes_backpressure_reads(&mut self, wbr: bool) {
        self.writes_backpressure_reads = wbr
    }

    pub fn ignore_backpressure(&mut self) {
        self.ignore_backpressure = true;
    }

    pub fn register_to(&mut self, token: mio::Token, poll: &mut mio::Poll) -> io::Result<()> {
        poll.register(
            &self.stream,
            token,
            mio::Ready::readable() | mio::Ready::writable(),
            mio::PollOpt::edge(),
        )
    }

    pub fn read(&mut self) -> Result<usize, io::Error> {
        let buffer = &mut self.read_buffer[self.bytes_read..];
        if buffer.len() == 0 {
            return Ok(0)
        }
        let res = self.stream.read(buffer);
        match res {
            Ok(size) => {
                self.polling_read = true;
                self.bytes_read += size;
                Ok(size)
            },
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.polling_read = false;
                Ok(0)
            },
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => Ok(0),
            Err(e) => Err(e),
        }
    }

    pub fn consume_bytes(&mut self, bytes: usize) {
        assert!(bytes <= self.bytes_read, "{} <= {}", bytes, self.bytes_read);
        self.bytes_read -= bytes;
        self.read_buffer.drain(..bytes).for_each(|_| {});
        let len = self.read_buffer.len();
        let cap = self.read_buffer.capacity();
        if len < cap {
            self.read_buffer.extend((len..cap).map(|_| 0))
        }
    }

    pub fn read_additional(&mut self, bytes: usize) {
        self.read_buffer.extend((0..bytes).map(|_| 0));
        let len = self.read_buffer.len();
        let cap = self.read_buffer.capacity();
        if len < cap {
            self.read_buffer.extend((len..cap).map(|_| 0))
        }
    }

    pub fn ensure_additional_read(&mut self, bytes: usize) {
        if bytes <= self.read_buffer[self.bytes_read..].len() {
            return
        }
        let additional_needed = bytes - self.read_buffer[self.bytes_read..].len();
        self.read_additional(additional_needed)
    }


    ///////////////////////////////////

    pub fn write(&mut self) -> Result<usize, io::Error> {
        if self.write_buffer.is_empty() {
            self.polling_write = false;
            return Ok(0)
        }

        debug_assert!(!self.write_buffer.first_bytes().is_empty());
        let bytes_written = self.bytes_written;
        let res = self.stream.write(&self.write_buffer.first_bytes()[bytes_written..]);
        let wrote = match res {
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.polling_write = false;
                return Ok(0)
            },
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => return Ok(0),
            Err(e) => return Err(e),
            Ok(size) => size,
        };

        self.bytes_written += wrote;

        self.polling_write = true;

        if self.bytes_written < self.write_buffer.first_bytes().len() {
            return Ok(wrote)
        }

        self.bytes_written = 0;
        self.write_buffer.swap_if_needed();

        Ok(wrote)
    }

    ///////////////////////////////////

    pub fn needs_to_mark_as_staying_awake(&mut self) -> bool {
        (self.is_polling_read() || self.polling_write) && !self.is_marked_as_staying_awake()
    }

    pub fn mark_as_staying_awake(&mut self) {
        self.is_marked_as_staying_awake = true;
    }

    pub fn is_marked_as_staying_awake(&self) -> bool {
        self.is_marked_as_staying_awake
    }

    pub fn on_wake(&mut self) {
        self.is_marked_as_staying_awake = false;
    }

    ///////////////////////////////////

    pub fn on_error(&mut self, poll: &mut mio::Poll) -> ShouldRemove {
        //TODO
        let _ = poll.deregister(&self.stream);
        true
    }

    ///////////////////////////////////

    pub fn set_polling_read(&mut self) {
        self.polling_read = true
    }

    pub fn set_polling_write(&mut self) {
        self.polling_write = true
    }

    pub fn is_polling_read(&mut self) -> bool {
        self.polling_read && !self.is_backpressured()
    }

    pub fn is_polling_write(&mut self) -> bool {
        self.polling_write
    }

    ///////////////////////////////////

    pub fn is_overflowing(&self) -> bool {
        self.write_buffer.has_pending()
    }

    pub fn is_backpressured(&mut self) -> bool {
        (self.is_marked_as_backpressured || self.write_buffer.has_pending()) &&
        !self.ignore_backpressure
    }

    pub fn mark_as_backpressured(&mut self) {
        self.is_marked_as_backpressured = true;
    }

    pub fn mark_as_not_backpressured(&mut self) {
        self.is_marked_as_backpressured = false;
    }

    ///////////////////////////////////

    pub fn read_bytes(&self) -> &[u8] {
        &self.read_buffer[..self.bytes_read]
    }

    pub fn add_bytes_to_write(&mut self, bytes: &[&[u8]]) {
        self.polling_write = true;
        self.write_buffer.fill(bytes);
    }

    pub fn add_contents_to_write(&mut self, contents: EntryContents, extra: &[&[u8]]) {
        self.polling_write = true;
        self.write_buffer.fill_from_contents(contents, extra);
    }

    pub fn to_read_and_write_halves(&mut self) -> (&[u8], TcpWriter) {
        (&self.read_buffer[..self.bytes_read], TcpWriter {
            write_buffer: &mut self.write_buffer,
            polling_write: &mut self.polling_write,
        })
    }
}

#[derive(Debug)]
pub struct TcpWriter<'s> {
    write_buffer: &'s mut DoubleBuffer,
    polling_write: &'s mut bool,
}

impl<'s> TcpWriter<'s> {
    pub fn add_bytes_to_write(&mut self, bytes: &[&[u8]]) {
        *self.polling_write = true;
        self.write_buffer.fill(bytes);
    }

    pub fn add_contents_to_write(&mut self, contents: EntryContents, extra: &[&[u8]]) {
        *self.polling_write = true;
        self.write_buffer.fill_from_contents(contents, extra);
    }

    pub fn is_overflowing(&self) -> bool {
        self.write_buffer.has_pending()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
