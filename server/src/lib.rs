
#[macro_use] extern crate log;

extern crate env_logger;
extern crate fuzzy_log;

use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::{mem, ptr};

use fuzzy_log::prelude::*;
use fuzzy_log::udp_store::*;

#[no_mangle]
pub extern "C" fn handle_packet(log: &mut HashMap<OrderIndex, Box<Packet<[u8; MAX_DATA_LEN]>>>, packet: &mut Packet<[u8; MAX_DATA_LEN]>)
{
	trace!("{:#?}", packet.get_header());
    let (kind, loc) = {
        (packet.get_kind(), packet.get_loc())
    };

    if let Kind::Ack = kind {
    	trace!("Got ACK");
        return
    }

    match log.entry(loc) {
        Vacant(e) => {
            trace!("Vacant entry {:?}", loc);
            match kind {
                Kind::Write => {
                    trace!("writing");
                    packet.set_kind(Kind::Written);
                    let data: Box<Packet<_>> = unsafe {
                    	let mut ptr = Box::new(mem::uninitialized());
                    	ptr::copy_nonoverlapping(packet, &mut *ptr, 1);
                    	ptr
                    };
                    e.insert(data);
                }
                _ => {
                	trace!("not write");
                    packet.set_kind(unoccupied_response(kind));
                }
            }
        }
        Occupied(e) => {
            trace!("Occupied entry {:?}", loc);
            unsafe {
            	ptr::copy_nonoverlapping::<Packet<_>>(&**e.get(), packet, 1);
            }
            //*packet = *e.get().clone();
            packet.set_kind(occupied_response(kind));;
        }
    }
    trace!("=============>\n{:#?}", packet.get_header());
}

#[no_mangle]
pub extern "C" fn init_log() -> Box<HashMap<OrderIndex, Box<Packet<()>>>> {
	assert_eq!(mem::size_of::<Box<HashMap<OrderIndex, Box<Packet<()>>>>>(), mem::size_of::<*mut u8>());
	let log = Box::new(HashMap::new());
	trace!("logging start.");
	trace!("log init as {:?}.", &*log as *const _);
	log
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

/*
extern crate rust_dpdk;
#[no_mangle]
pub unsafe extern "C" fn echo(port: u8, queue: u16) -> ! {
	if rte_eth_dev_socket_id(port) > 0 && 
		rte_eth_dev_socket_id(port) != rte_socket_id() as i32 {
		println!("WARNING, port {} is on remote NUMA node {} to polling thread on {}.",
			port, rte_eth_dev_socket_id(port), rte_socket_id());
		println!("\ttPerformance willnot be optimal.")
	}
	
	println!("Starting echo.");
	'receive: loop {
		let mut bufs = [ptr::null_mut(); BURST_SIZE as usize];
		let nb_rx = rte_eth_rx_burst(port, queue, &mut bufs[..], BURST_SIZE);
		if nb_rx == 0 { continue 'receive }
		
		for i in 0..nb_rx {
			let mbuf = bufs[i as usize];
			{
				let ether: *mut Struct_ether_hdr = rte_pktmbuf_mtod(mbuf);
				mem::swap(&mut (*ether).s_addr, &mut (*ether).d_addr)
			}
			{
				let ip: *mut Struct_ipv4_hdr = rte_pktmbuf_mtod_offset(mbuf,
					mem::size_of::<Struct_ether_hdr>());
				mem::swap(&mut (*ip).dst_addr, &mut (*ip).src_addr)
			}
			{
				let udp: *mut Struct_udp_hdr = rte_pktmbuf_mtod_offset(mbuf,
					mem::size_of::<Struct_ether_hdr>() + mem::size_of::<Struct_ipv4_hdr>());
				mem::swap(&mut (*udp).dst_port, &mut (*udp).src_port)
			}
		}
		
		
	}
}*/
