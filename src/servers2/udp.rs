
use prelude::*;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::mem;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use mio;
use mio::deprecated::{EventLoop, Handler as MioHandler};
use mio::udp::UdpSocket;

use servers2::{self, ServerLog, ToWorker, DistributeToWorkers};
use buffer::Buffer;

const SOCKET_TOKEN: mio::Token = mio::Token(0);

pub struct Server {
    log: ServerLog<(), Rc<RefCell<VecDeque<ToWorker<()>>>>>,
    buffer: Buffer,
    from_log: Rc<RefCell<VecDeque<ToWorker<()>>>>,
    socket: UdpSocket,
}

impl MioHandler for Server {
    type Timeout = ();
    type Message = ();

    fn ready(&mut self,
             event_loop: &mut EventLoop<Self>,
             token: mio::Token,
             events: mio::Ready) {
        match token {
            SOCKET_TOKEN => {
                // trace!("SERVER got client event");
                if events.is_error() {
                    trace!("SERVER error");
                    return
                }
                if events.is_readable() {
                    // trace!("SERVER gonna read from client");
                    //TODO currently we assume we read an entire packet
                    if let Some(addr) = self.read_packet() {
                        trace!("SERVER finished read from {:?}", addr);
                        let buff = mem::replace(&mut self.buffer, Buffer::empty());
                        let storage = match buff.entry().kind.layout() {
                            EntryLayout::Multiput | EntryLayout::Sentinel => {
                                let (size, senti_size) = {
                                    let e = buff.entry();
                                    (e.entry_size(), e.sentinel_entry_size())
                                };
                                unsafe {
                                    let mut m = Vec::with_capacity(size);
                                    let mut s = Vec::with_capacity(senti_size);
                                    m.set_len(size);
                                    s.set_len(senti_size);
                                    Some(Box::new((m.into_boxed_slice(), s.into_boxed_slice())))
                                }
                            }
                            _ => None,
                        };
                        self.log.handle_op(buff, storage, ());
                        let to_pop = self.from_log.borrow().len();
                        for _ in 0..to_pop {
                            let wrk = self.from_log.borrow_mut().pop_front().unwrap();
                            let (buffer, ..) = servers2::handle_to_worker(wrk, 0);
                            mem::replace(&mut self.buffer, buffer);
                            self.write_packet(&addr);
                            trace!("SERVER finished write to {:?}", addr);
                        }
                    }
                } else {
                    panic!("invalid event {:?}", events);
                };
                event_loop.reregister(&self.socket,
                                      SOCKET_TOKEN,
                                      mio::Ready::readable() | mio::Ready::error(),
                                      mio::PollOpt::edge() | mio::PollOpt::oneshot())
                          .expect("could not reregister client socket")
            }
            t => panic!("SERVER invalid tokem {:?}.", t)
        }
    }
}

impl Server {
    pub fn new(server_addr: &SocketAddr) -> Result<Self, ::std::io::Error> {
        let socket = try!(UdpSocket::bind(&server_addr));
        let from_log: Rc<RefCell<_>> = Default::default();
        Ok(Server {
            socket: socket,
            //TODO
            log: ServerLog::new(0, 1, from_log.clone()),
            from_log: from_log,
            buffer: Buffer::new(),
        })
    }

    pub fn run(&mut self, start_flag: &AtomicUsize) -> ! {
        let mut event_loop = EventLoop::new().unwrap();
        event_loop.register(&self.socket, SOCKET_TOKEN,
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::edge() | mio::PollOpt::oneshot())
            .expect("could not register server");
        trace!("SERVER started.");
        start_flag.fetch_add(1, Ordering::Release);
        let res = event_loop.run(self);
        panic!("Server stopped with {:?}", res)
    }

    fn read_packet(&mut self) -> Option<SocketAddr> {
        let finished_read = self.try_read_packet();
        finished_read.map(|(read, addr)| {
            assert_eq!(read, self.buffer.entry_size());
            addr
        })
    }

    fn try_read_packet(&mut self) -> Option<(usize, SocketAddr)> {
        //TODO ick
        self.socket.recv_from(&mut self.buffer[..]).expect("socket error")
    }

    fn write_packet(&mut self, addr: &SocketAddr) {
        //TODO ick
        let finished_write = self.socket.send_to(&self.buffer.entry_slice(), addr)
            .expect("socket error");
        finished_write.map(|written| {
            assert_eq!(written, self.buffer.entry_size());
        });
    }
}

impl DistributeToWorkers<()>
for Rc<RefCell<VecDeque<ToWorker<()>>>> {
    fn send_to_worker(&mut self, msg: ToWorker<()>) {
        trace!("SERVER sending to worker");
        self.borrow_mut().push_back(msg);
    }
}
