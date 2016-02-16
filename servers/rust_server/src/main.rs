
#[macro_use] extern crate log;

extern crate env_logger;
extern crate fuzzy_log;
extern crate mio;

use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::mem;

use fuzzy_log::prelude::*;

use mio::udp::UdpSocket;

#[allow(non_upper_case_globals)]
pub fn main() {
    const addr_str: &'static str = "0.0.0.0:13265";
    let _ = env_logger::init();
    let addr = addr_str.parse().expect("invalid inet address");
    let receive = if let Ok(socket) =
        UdpSocket::bound(&addr) {
        socket
    } else {
        trace!("socket in use");
        return
    };
    let mut log: HashMap<_, Box<Entry<()>>> = HashMap::with_capacity(10);
    let mut horizon = HashMap::with_capacity(10);
    let mut buff = Box::new(unsafe {mem::zeroed::<Entry<()>>()});
    trace!("starting server");
    'server: loop {
        let res = receive.recv_from(buff.bytes_mut());
        match res {
            Err(e) => panic!("{}", e),
            Ok(Some((_, sa))) => {
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
                    let _ = receive.send_to(buff.bytes(), &sa).expect("unable to ack");
                }
                else {
                    let loc = unsafe { buff.as_data_entry().flex.loc };

                    match log.entry(loc) {
                        Vacant(e) => {
                            trace!("Vacant entry {:?}", loc);
                            match kind & EntryKind::Layout {
                                EntryKind::Data => {
                                    trace!("writing");
                                    let packet = mem::replace(&mut buff, Box::new(unsafe {mem::zeroed::<Entry<()>>()}));
                                    let packet: &mut Box<Entry<()>> =
                                        unsafe { transmute_ref_mut(e.insert(packet)) };
                                    horizon.insert(loc.0, loc.1);
                                    //packet.header.kind = Kind::Written;
                                    receive.send_to(packet.bytes(), &sa).expect("unable to ack");
                                }
                                _ => {
                                    //buff.kind = unoccupied_response(kind);
                                    receive.send_to(buff.bytes(), &sa).expect("unable to ack");
                                }
                            }
                        }
                        Occupied(mut e) => {
                            trace!("Occupied entry {:?}", loc);
                            let packet = e.get_mut();
                            packet.kind = packet.kind | EntryKind::ReadSuccess;
                            trace!("returning {:?}", packet);
                            receive.send_to(packet.bytes(), &sa).expect("unable to ack");
                        }
                    };
                }
            }
            _ => continue 'server,
        }
    }
}

unsafe fn transmute_ref_mut<T, U>(t: &mut T) -> &mut U {
    assert_eq!(mem::size_of::<T>(), mem::size_of::<U>());
    mem::transmute(t)
}
