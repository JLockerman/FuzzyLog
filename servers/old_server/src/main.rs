
#[macro_use] extern crate log;

extern crate env_logger;
extern crate fuzzy_log;
extern crate mio;

use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
#[cfg(False)]
use std::ffi::CString;
use std::mem::{self, transmute};

use fuzzy_log::prelude::*;
use fuzzy_log::udp_store::*;

use mio::buf::{SliceBuf, MutSliceBuf};
use mio::udp::UdpSocket;


pub fn main() {
    #[allow(non_upper_case_globals)]
    const addr_str: &'static str = "0.0.0.0:13265";
    let _ = env_logger::init();
    let addr = addr_str.parse().expect("invalid inet address");
    let receive = if let Ok(socket) =
        UdpSocket::bound(&addr) {
        socket
    } else {
        return
    };
    let mut log = HashMap::with_capacity(10);
    let mut buff = Box::new([0; 4096]);
    trace!("starting server");
    'server: loop {
        let res = receive.recv_from(&mut MutSliceBuf::wrap(&mut buff[..]));
        match res {
            Err(e) => panic!("{}", e),
            Ok(Some(sa)) => {
                trace!("recieved from {:?}", sa);

                let (kind, loc) = {
                    let packet: &Packet<()> =  Packet::wrap_bytes(&buff[..]);
                    (packet.get_kind(), packet.get_loc())
                };

                if let Kind::Ack = kind {
                    use std::mem;
                    let slice = &mut SliceBuf::wrap(&buff[..mem::size_of::<Header>()]);
                    let _ = receive.send_to(slice, &sa).expect("unable to ack");
                }

                match log.entry(loc) {
                    Vacant(e) => {
                        trace!("Vacant entry {:?}", loc);
                        match kind {
                            Kind::Write => {
                                trace!("writing");
                                let packet = mem::replace(&mut buff, Box::new([0; 4096]));
                                let packet: &mut Box<Packet<()>> =
                                    e.insert(unsafe { transmute(packet) } );
                                packet.set_kind(Kind::Written);
                                let slice = &mut SliceBuf::wrap(packet.as_bytes());
                                let _ = receive.send_to(slice, &sa).expect("unable to ack");
                            }
                            _ => {
                                let packet: &mut Packet<()> =
                                    Packet::wrap_bytes_mut(&mut buff[..]);
                                packet.set_kind(unoccupied_response(kind));
                                let slice = &mut SliceBuf::wrap(packet.as_bytes());
                                receive.send_to(slice, &sa).expect("unable to ack");
                            }
                        }
                    }
                    Occupied(mut e) => {
                        trace!("Occupied entry {:?}", loc);
                        let packet = e.get_mut();
                        packet.set_kind(occupied_response(kind));
                        let slice = &mut SliceBuf::wrap(packet.as_bytes());
                        receive.send_to(slice, &sa).expect("unable to ack");
                    }
                };
                //receive.send_to(&mut ByteBuf::from_slice(&[1]), &sa).expect("unable to ack");
                //send.send_to(&mut ByteBuf::from_slice(&[1]), &sa).expect("unable to ack");
            }
            _ => continue 'server,
        }
    }
}

fn occupied_response(request_kind: Kind) -> Kind {
    match request_kind {
        Kind::Write => Kind::AlreadyWritten,
        Kind::Read => Kind::Value,
        _ => Kind::Ack,
    }
}

fn unoccupied_response(request_kind: Kind) -> Kind {
    match request_kind {
        Kind::Write => Kind::Written,
        Kind::Read => Kind::NoValue,
        _ => Kind::Ack,
    }
}

#[cfg(False)]
fn old_main() {
    //let addr_str = "127.0.0.1:13265";
    let addr_str = "0.0.0.0:13265";
    let addr = addr_str.parse().expect("invalid inet address");
    let receive = UdpSocket::bound(&addr).expect("unable to open receive");
    let log = HashMap::new();
    let mut buff = ByteBuf::mut_with_capacity(4096);
    println!("server starting");
    'server: loop {
        let res = receive.recv_from(&mut buff);
        match res {
            Err(e) => panic!("{}", e),
            Ok(Some(sa)) => {
                println!("recieved {:?}", sa);
                receive.send_to(&mut ByteBuf::from_slice(&[1]), &sa).expect("unable to ack");
                //send.send_to(&mut ByteBuf::from_slice(&[1]), &sa).expect("unable to ack");
            }
            _ => continue 'server,
        }
        {
            print!("got ");
            let string = buff.bytes();
            'print: for i in 0..string.len() {
                if string[i] == 0 {
                    break 'print
                }
                print!("{}", string[i] as char);
            }
            print!("\n");
        }
        buff.clear();
    }
}

#[cfg(False)]
#[test]
fn test_main() {
    use std::thread;

    thread::spawn(main);

    //let addr_str = "127.0.0.1:13265";
    //let local_addr = "0.0.0.0:14266";
    let addr_str = "169.254.3.180:13265";
    let addr = addr_str.parse().expect("invalid inet address");
    let sender = UdpSocket::v4().expect("unable to open socket");
    for i in 0..25 {
        'send: loop {
            println!("sending {}", i);
            let s = format!("val {}", i);
            let to_send = CString::new(&*s).unwrap();
            let res = sender.send_to(&mut ByteBuf::from_slice(to_send.as_bytes_with_nul()), &addr);
            res.unwrap();
            thread::sleep_ms(1);
            let addr = sender.recv_from(&mut ByteBuf::mut_with_capacity(1))
                .expect("unable to receive ack");
            if let Some(addr) = addr {
                println!("acked from {:?} for {}", addr, i);
                break 'send
            }
        }
    }
}
