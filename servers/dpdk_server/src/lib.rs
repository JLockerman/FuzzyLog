
#[macro_use] extern crate log;

extern crate env_logger;
extern crate fuzzy_log;

//use std::collections::HashMap;
//use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::{mem, ptr};

use std::collections::BTreeMap as HashMap;
use std::collections::btree_map::Entry::{Occupied, Vacant};


use std::collections::HashSet;
use std::sync::Mutex;

use fuzzy_log::prelude::*;
//use fuzzy_log::udp_store::*;

#[no_mangle]
pub extern "C" fn handle_packet(log: &mut HashMap<OrderIndex, Box<Entry<(), DataFlex>>>, packet: &mut Entry<(), DataFlex>)
{
    println!("{:#?}", packet.kind);
    let (kind, loc) = {
        (packet.kind, packet.flex.loc)
    };

    if kind & EntryKind::Layout == EntryKind::Multiput {
        println!("bad packet");
        return
    }

    match log.entry(loc) {
        Vacant(e) => {
            println!("Vacant entry {:?}", loc);
            match kind & EntryKind::Layout {
                EntryKind::Data => {
                    println!("writing");
                    //packet.set_kind(Kind::Written);
                    let data: Box<Entry<_, DataFlex>> = unsafe {
                        let mut ptr = Box::new(mem::uninitialized());
                        ptr::copy_nonoverlapping(packet, &mut *ptr, 1);
                        ptr.kind = kind | EntryKind::ReadSuccess;
                        ptr
                    };
                    e.insert(data);
                }
                _ => {
                    println!("not write");
                    //packet.set_kind(unoccupied_response(kind));
                }
            }
        }
        Occupied(e) => {
            println!("Occupied entry {:?}", loc);
            unsafe {
            	ptr::copy_nonoverlapping::<Entry<_, _>>(&**e.get(), packet, 1);
            }
            //*packet = *e.get().clone();
            //packet.set_kind(occupied_response(kind));;
        }
    }
    //println!("=============>\n{:#?}", packet.contents());
}

#[no_mangle]
pub extern "C" fn init_log() -> Box<HashMap<OrderIndex, Box<Entry<()>>>> {
	assert_eq!(mem::size_of::<Box<HashMap<OrderIndex, Box<Entry<()>>>>>(), mem::size_of::<*mut u8>());
	let log = Box::new(HashMap::new());
	trace!("logging start.");
	trace!("log init as {:?}.", &*log as *const _);
	log
}

#[no_mangle]
pub extern "C" fn rss_log(core: u32, chain: u32, set: &Mutex<HashSet<(u32, u32)>>) -> u32 {
	let mut set = set.lock().unwrap();
	let other_core = if core == 0 { 1 } else { 0 };
	set.insert((core, chain));
	return if set.contains(&(other_core, chain)) { println!("err at {:?}", (core, chain)); 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn rss_log_init() -> Box<Mutex<HashSet<(u32, u16)>>> {
	Box::new(Mutex::new(HashSet::new()))
}

