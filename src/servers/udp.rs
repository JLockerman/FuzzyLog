
use prelude::*;

use std::mem;
use std::net::SocketAddr;

use mio;
use mio::deprecated::{EventLoop, Handler as MioHandler};
use mio::udp::UdpSocket;

use servers::ServerLog;

const SOCKET_TOKEN: mio::Token = mio::Token(0);

pub struct Server {
    log: ServerLog,
    buffer: Box<Entry<()>>,
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
                        let _need_to_respond = self.log.handle_op(&mut *self.buffer);
                        //TODO
                        //if need_to_respond {
                        self.write_packet(&addr);
                        trace!("SERVER finished write to {:?}", addr);
                        //}
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
        Ok(Server {
            socket: socket,
            //TODO
            log: ServerLog::new(0, 1),
            buffer: Box::new(unsafe { mem::zeroed::<Entry<()>>() })
        })
    }

    pub fn run(&mut self) -> ! {
        let mut event_loop = EventLoop::new().unwrap();
        event_loop.register(&self.socket, SOCKET_TOKEN,
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::edge() | mio::PollOpt::oneshot())
            .expect("could not register server");
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
        self.socket.recv_from(&mut self.buffer.sized_bytes_mut()).expect("socket error")
    }

    fn write_packet(&mut self, addr: &SocketAddr) {
        //TODO ick
        let finished_write = self.socket.send_to(self.buffer.bytes(), addr)
            .expect("socket error");
        finished_write.map(|written| {
            assert_eq!(written, self.buffer.entry_size());
        });
    }
}
