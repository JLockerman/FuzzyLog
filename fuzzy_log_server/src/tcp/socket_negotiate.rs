
use std::cell::RefCell;
use std::io::{self, Read};
use std::mem;
use std::net::SocketAddr;
use std::rc::Rc;

use mio;
use mio::tcp::*;

use socket_addr::Ipv4SocketAddr as ClientId;

use hash::{HashMap, IdHashMap};

use self::Position::*;

// state machine
//           I'm alone -> Done
//          /
// got client - I'm tail, connect upstream -> GotDownClient - got Id -> Done
//          \
//           I'm head, wait for down -> GotUpClient - got down, send id -> Done
//
//          I'm head, wait for client -> GotDown - got client -> Done
//         /
// got down - I'm mid -> send up -> WaitForUpAck - send down -> Done

// connection
// 1. server writes 0
// 2. down sends 1, client sends 2
// 3. down/client sends id
// 4. server sends id

#[derive(Debug)]
pub struct NegotiateState {
    downstream: TcpStream,
    token: mio::Token,
    server_ready: Reader<[u8; 1]>,
    id: IdRead,
    down_type: DownRead,
    up: Option<UpState>
}

#[derive(Debug)]
struct UpState {
    upstream: TcpStream,
    server_ready: Reader<[u8; 1]>,
    id2: IdRead,
    token: mio::Token,
}

impl NegotiateState {
    fn into_new_client(self) -> NewClient {
        use std::os::unix::io::{IntoRawFd, FromRawFd};
        let token = self.token;
        unsafe {
            let up = self.up.map(|up| {
                FromRawFd::from_raw_fd(up.upstream.into_raw_fd())
            });
            let downstream = FromRawFd::from_raw_fd(self.downstream.into_raw_fd());
            match up {
                None => (self.id.unwrap(), token, downstream, None),
                Some(upstream) => (self.id.unwrap(), token, upstream, Some((token, downstream))),
            }
        }
    }
}

#[derive(Debug)]
enum IdRead {
    Pending(IdReader),
    Done(ClientId),
}

impl IdRead {
    fn try_read_from<R: Read>(&mut self, read: R) -> Result<Option<(ClientId, bool)>, io::Error> {
        let id = match self {
            &mut IdRead::Done(id) => return Ok(Some((id, false))),
            &mut IdRead::Pending(ref mut reader) => {
                let id = reader.try_read_from(read)?;
                match id {
                    None => return Ok(None),
                    Some(id) => id,
                }
            }
        };
        *self = IdRead::Done(id);
        Ok(Some((id, true)))
    }

    fn unwrap(self) -> ClientId {
        match self {
            IdRead::Done(id) => id,
            _ => unreachable!(),
        }
    }
}

impl Default for IdRead {
    fn default() -> Self {
        IdRead::Pending(Default::default())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum DownRead {
    Pending(ClientTypeReader),
    Client,
    Server,
}

impl DownRead {
    fn try_read_from<R: Read>(&mut self, read: R) -> Result<Option<ClientType>, io::Error> {
        let kind = match self {
            &mut DownRead::Client => return Ok(Some(ClientType::Client)),
            &mut DownRead::Server => return Ok(Some(ClientType::Server)),
            &mut DownRead::Pending(ref mut reader) => {
                let kind = reader.try_read_from(read)?;
                match kind {
                    None => return Ok(None),
                    Some(kind) => kind,
                }
            }
        };
        *self = match kind {
            ClientType::Client => DownRead::Client,
            ClientType::Server => DownRead::Server,
        };
        Ok(Some(kind))
    }
}

impl Default for DownRead {
    fn default() -> Self {
        DownRead::Pending(Default::default())
    }
}

type R<T> = Rc<RefCell<T>>;

pub type NewClient = (ClientId, mio::Token, TcpStream, Option<(mio::Token, TcpStream)>);

#[derive(Debug)]
pub struct Negotiator {
    for_id: IdHashMap<ClientId, R<NegotiateState>>,
    for_token: HashMap<mio::Token, R<NegotiateState>>,
    position: Position,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Position {
    Solo, Head, Tail(SocketAddr), Mid(SocketAddr),
}

#[derive(Debug, Copy, Clone)]
pub struct NegotiateNotDone;

impl From<io::Error> for NegotiateNotDone {
    fn from(_: io::Error) -> Self {
        NegotiateNotDone
    }
}

impl From<()> for NegotiateNotDone {
    fn from(_: ()) -> Self {
        NegotiateNotDone
    }
}

impl Negotiator {
    pub fn new(position: Position) -> Self {
        Self {
            for_id: Default::default(),
            for_token: Default::default(),
            position,
        }
    }

    pub fn got_connection<NextToken>(
        &mut self, mut socket: TcpStream, poll: &mut mio::Poll, mut get_next_token: NextToken
    ) -> Result<NewClient, NegotiateNotDone>
    where NextToken: FnMut() -> mio::Token {
        super::blocking_write(&mut socket, &[0]).unwrap();
        let token = get_next_token();
        poll.register(&socket, token, mio::Ready::readable(), mio::PollOpt::level()).unwrap();
        self.for_token.insert(token, Rc::new(NegotiateState {
            downstream: socket,
            token,
            server_ready: Reader{ buffer: [0; 1], read: 1},
            down_type: Default::default(),
            id: Default::default(),
            up: None,
        }.into()));
        self.handle_event(token, poll)
    }

    pub fn handle_event(
        &mut self, token: mio::Token, poll: &mut mio::Poll,
    ) -> Result<NewClient, NegotiateNotDone> {
        use ::std::collections::hash_map::Entry;

        let mut negotiation_ref = self.for_token.get_mut(&token).ok_or(())?.clone();
        let kind;
        let (id, first) = {
            let mut negotiation = negotiation_ref.borrow_mut();
            let negotiation = &mut *negotiation;
            let ready = negotiation.server_ready.try_fill_from(&mut negotiation.downstream)?;
            if !ready { return Err(())? }
            kind = negotiation.down_type.try_read_from(&mut negotiation.downstream)?.ok_or(())?;
            negotiation.id.try_read_from(&mut negotiation.downstream)?.ok_or(())?
        };
        {
            let other = self.for_id.entry(id);
            match other {
                Entry::Vacant(v) => {
                    v.insert(negotiation_ref.clone());
                    if let (ClientType::Server, Head) = (kind, self.position) {
                        let mut negotiation = negotiation_ref.borrow_mut();
                        super::blocking_write(&mut negotiation.downstream, id.bytes()).unwrap();
                    }
                },

                Entry::Occupied(o) => {
                    let other_ref = o.into_mut();
                    if first && !Rc::ptr_eq(&negotiation_ref, other_ref) {
                        assert_eq!(self.position, Head);
                        // in some cases we must remove one that got here earlier and make it upstream
                        // @Head: server is downstream, client is upstream
                        match kind {
                            // we're the server, other must be client
                            ClientType::Server => {
                                {
                                    let mut negotiation = negotiation_ref.borrow_mut();
                                    super::blocking_write(&mut negotiation.downstream, id.bytes()).unwrap();
                                }
                                match self.position {
                                    // at the head upstream is the client
                                    Head => mem::swap(other_ref, &mut negotiation_ref),
                                    other => unreachable!("{:?}", other),
                                }
                            },

                            // we're the client, other must be server
                            ClientType::Client => {
                                // at the tail upstream is server
                                // if let Tail(..) = self.position {
                                //     mem::swap(other_ref, &mut negotiation_ref)
                                // }
                                match self.position {
                                    //everything is right
                                    Head => {},
                                    other => unreachable!("{:?}", other),
                                }
                            },
                        }
                        copy_to(other_ref, negotiation_ref, &mut self.for_token);
                        negotiation_ref = other_ref.clone();
                        debug_assert_eq!(negotiation_ref.borrow().down_type, DownRead::Server);

                        fn copy_to(
                            to: &R<NegotiateState>,
                            from_ref: R<NegotiateState>,
                            for_token: &mut HashMap<mio::Token, R<NegotiateState>>
                        ) {
                            {
                                let f = from_ref.borrow();
                                let t = to.borrow();
                                drop(for_token.insert(f.token, to.clone()));
                                drop(for_token.insert(t.token, to.clone()));
                            }
                            let from = Rc::try_unwrap(from_ref).unwrap().into_inner();
                            let mut to = to.borrow_mut();
                            // println!("{:?} => {:?}", from, to);
                            debug_assert_eq!(from.down_type, DownRead::Client);
                            debug_assert_eq!(to.down_type, DownRead::Server);
                            debug_assert!(to.up.is_none());
                            to.up = Some(UpState {
                                upstream: from.downstream,
                                server_ready: from.server_ready,
                                id2: from.id,
                                token: from.token,
                            });
                        }
                    }
                }
            }
        };
        {
            let mut negotiation = negotiation_ref.borrow_mut();
            let negotiation = &mut *negotiation;

            match &self.position {
                &Solo => {
                    super::blocking_write(&mut negotiation.downstream, id.bytes()).unwrap();
                },
                &Tail(upstream_addr) | &Mid(upstream_addr) => {
                    match &self.position {
                        &Tail(..) => assert_eq!(kind, ClientType::Client),
                        &Mid(..) => assert_eq!(kind, ClientType::Server),
                        _ => unreachable!()
                    }
                    if let None = negotiation.up {
                        let mut upstream = TcpStream::connect(&upstream_addr).unwrap();
                        let _ = upstream.set_keepalive_ms(Some(1000));
                        let _ = upstream.set_nodelay(true);
                        trace!("connect up {:?} => {:?}", upstream.local_addr(), upstream.peer_addr());
                        poll.register(&upstream, token, mio::Ready::readable(), mio::PollOpt::level()).unwrap();
                        negotiation.up = Some(UpState {
                            upstream,
                            server_ready: Default::default(),
                            id2: Default::default(),
                            token: token,
                        });
                    }
                    let up = negotiation.up.as_mut().unwrap();
                    if !up.server_ready.try_fill_from(&up.upstream)? {
                        return Err(())?
                    }
                    super::blocking_write(&mut up.upstream, &mut [1]).unwrap();
                    super::blocking_write(&mut up.upstream, id.bytes()).unwrap();
                    let (id2, _) = up.id2.try_read_from(&up.upstream)?.ok_or(())?;
                    assert_eq!(id2, id);
                    match &self.position {
                        &Tail(..) => assert_eq!(kind, ClientType::Client),
                        &Mid(..) => assert_eq!(kind, ClientType::Server),
                        _ => unreachable!()
                    }
                    super::blocking_write(&mut negotiation.downstream, id.bytes()).unwrap();
                },
                &Head => {
                    let up = negotiation.up.as_mut().ok_or(())?;
                    assert_eq!(negotiation.down_type, DownRead::Server);
                    let (id2, _) = up.id2.try_read_from(&up.upstream)?.ok_or(())?;
                    assert_eq!(id2, id);
                    super::blocking_write(&mut up.upstream, id.bytes()).unwrap();
                }
            }
        }
        drop(self.for_id.remove(&id));
        {
            let c = negotiation_ref.borrow();
            drop(self.for_token.remove(&c.token));
            if let Some(up) = c.up.as_ref() {
                drop(self.for_token.remove(&up.token));
            }
        }
        let state = match Rc::try_unwrap(negotiation_ref) {
            Ok(state) => state,
            Err(r) => panic!("lost reference {:#?}, in {:#?}", r, self),
        };
        let state = state.into_inner();
        let _ = poll.deregister(&state.downstream);
        if let Some(up) = state.up.as_ref() {
            let _ = poll.deregister(&up.upstream);
        }
        Ok(state.into_new_client())
    }
}
/////////////////////////////////////////////////////////

#[derive(Debug, Default)]
struct IdReader {
    buffer: Reader<[u8; 16]>,
}

impl IdReader {
    fn try_read_from<R: Read>(&mut self, read: R) -> Result<Option<ClientId>, io::Error> {
        if self.buffer.try_fill_from(read)? {
            Ok(Some(ClientId::from_bytes(self.buffer.buffer)))
        } else {
            Ok(None)
        }
    }
}

////////////////////////////

#[derive(Debug, Default, PartialEq, Eq)]
struct ClientTypeReader {
    buffer: Reader<[u8; 1]>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ClientType {
    Client,
    Server,
}

impl ClientTypeReader {
    fn try_read_from<R: Read>(&mut self, read: R) -> Result<Option<ClientType>, io::Error> {
        let done = self.buffer.try_fill_from(read)?;
        if !done {
            return Ok(None)
        }
        match self.buffer.buffer[0] {
            1 => Ok(Some(ClientType::Server)),
            2 => Ok(Some(ClientType::Client)),
            other => unreachable!("{:?}", other),
        }
    }
}

////////////////////////////

#[derive(Debug, PartialEq, Eq)]
struct Reader<Buffer> {
    buffer: Buffer,
    read: u8,
}

impl<Buffer: Default> Default for Reader<Buffer> {
    fn default() -> Self {
        Self {
            buffer: Default::default(),
            read: 0,
        }
    }
}

impl<Buffer: AsMut<[u8]>> Reader<Buffer> {
    fn try_fill_from<R: Read>(&mut self, mut read: R) -> Result<bool, io::Error> {
        let len = {
            let bytes = &mut self.buffer.as_mut()[self.read as usize..];
            if bytes.is_empty() {
                return Ok(true)
            }
            read.read(bytes)?
        };
        self.read += len as u8;
        return Ok(self.buffer.as_mut()[self.read as usize..].is_empty())
    }
}
