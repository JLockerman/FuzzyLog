
use prelude::*;

use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::mem::{self, forget, transmute};
use std::net::SocketAddr;

use mio::udp::UdpSocket;

pub struct Server {
    receive: UdpSocket,
    log: HashMap<OrderIndex, Box<Entry<()>>>,
    horizon: HashMap<order, entry>,
    buff: Box<Entry<()>>,
}

impl Server {
    pub fn new(server_addr: &SocketAddr) -> Result<Self, ::std::io::Error> {
        let receive = try!(UdpSocket::bound(&server_addr));
        Ok(Server {
            receive: receive,
            log: HashMap::new(),
            horizon: HashMap::new(),
            buff: Box::new(unsafe { mem::zeroed::<Entry<()>>() })
        })
    }

    pub fn run(&mut self) -> ! {
        let &mut Server {ref mut receive, ref mut log, ref mut horizon, ref mut buff} = self;
        trace!("starting server");
        'server: loop {
            let res = receive.recv_from(buff.sized_bytes_mut());
            match res {
                Err(e) => panic!("{}", e),
                Ok(Some((_, sa))) => {
                    trace!("server recieved from {:?}", sa);

                    let kind = {
                        buff.kind
                    };

                    trace!("server recieved a {:?}", kind);
                    if let EntryKind::Multiput = kind & EntryKind::Layout {
                        {
                            {
                                buff.kind = kind | EntryKind::ReadSuccess;
                                let locs = buff.locs_mut();
                                for i in 0..locs.len() {
                                    let hor: entry = horizon
                                                         .get(&locs[i].0)
                                                         .cloned()
                                                         .unwrap_or(0.into()) +
                                                     1;
                                    locs[i].1 = hor;
                                    horizon.insert(locs[i].0, hor);
                                }
                            }
                            let locs = buff.locs();
                            trace!("appending at {:?}", locs);
                            for &loc in locs {
                                let b = buff.clone();
                                log.insert(loc, b);
                                // trace!("appended at {:?}", loc);
                            }
                        }
                        let _ = receive.send_to(buff.bytes(), &sa).expect("unable to ack");
                    } else {
                        let loc = {
                            let loc = unsafe { &mut buff.as_data_entry_mut().flex.loc };
                            trace!("loc {:?}", loc);
                            if kind & EntryKind::Layout == EntryKind::Data {
                                trace!("is write");
                                let hor = horizon.get(&loc.0).cloned().unwrap_or(0.into()) + 1;
                                *loc = (loc.0, hor);
                            } else {
                                trace!("is read");
                            }
                            trace!("loc {:?}", loc);
                            *loc
                        };

                        match log.entry(loc) {
                            Vacant(e) => {
                                trace!("Vacant entry {:?}", loc);
                                match kind & EntryKind::Layout {
                                    EntryKind::Data => {
                                        trace!("writing");
                                        let packet = mem::replace(buff,
                                            Box::new(unsafe { mem::zeroed::<Entry<()>>() }));
                                        let packet: &mut Box<Entry<()>> = unsafe {
                                            transmute_ref_mut(e.insert(packet))
                                        };
                                        horizon.insert(loc.0, loc.1);
                                        // packet.header.kind = Kind::Written;
                                        receive.send_to(packet.bytes(), &sa)
                                               .expect("unable to ack");
                                    }
                                    EntryKind::Read => {
                                        let last_entry = horizon.get(&loc.0).cloned().unwrap_or(0.into());
                                        let (old_id, old_loc) = unsafe {
                                            (buff.id, buff.as_data_entry().flex.loc)
                                        };
                                        **buff = EntryContents::Data(&(), &[(loc.0, last_entry)]).clone_entry();
                                        buff.id = old_id;
                                        buff.kind = EntryKind::NoValue;
                                        unsafe {
                                            buff.as_data_entry_mut().flex.loc = old_loc;
                                        }
                                        receive.send_to(buff.bytes(), &sa)
                                               .expect("unable to ack");
                                    }
                                    _ => {
                                        // buff.kind = unoccupied_response(kind);
                                        receive.send_to(buff.bytes(), &sa)
                                               .expect("unable to ack");
                                    }
                                }
                            }
                            Occupied(mut e) => {
                                trace!("Occupied entry {:?}", loc);
                                let packet = e.get_mut();
                                packet.kind = packet.kind | EntryKind::ReadSuccess;
                                //trace!("returning {:?}", packet);
                                receive.send_to(packet.bytes(), &sa).expect("unable to ack");
                            }
                        };
                    }
                }
                _ => continue 'server,
            }
        }
    }
}

unsafe fn transmute_ref_mut<T, U>(t: &mut T) -> &mut U {
    assert_eq!(mem::size_of::<T>(), mem::size_of::<U>());
    mem::transmute(t)
}
