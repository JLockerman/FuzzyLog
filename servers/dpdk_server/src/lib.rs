#![feature(type_ascription)]
#![feature(unique)]
#![feature(std_panic)]
#![feature(panic_handler)]
#![feature(core_intrinsics)]
#![feature(asm)]

#![feature(test)]
//#![cfg_attr(test, feature(test))]

//#[cfg(test)]
extern crate test as test_crate;

#[macro_use] extern crate log;

extern crate env_logger;
extern crate fuzzy_log;

use std::collections::HashMap;
//use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::{mem, ptr, panic, intrinsics};

//use trie::Trie;
//use trie::Entry::{Occupied, Vacant};
use mempool_trie2::Trie;
use mempool_trie2::Entry::{Occupied, Vacant};

//use std::collections::BTreeMap;
//use std::collections::btree_map::Entry::{Occupied, Vacant};

use std::collections::HashSet;
use std::sync::Mutex;

use fuzzy_log::prelude::*;
//use fuzzy_log::udp_store::*;

//mod trie;
//mod mempool_trie;
mod mempool_trie2;

#[repr(C)]
pub struct Log<F=[u8; MAX_DATA_LEN2]> {
    log: Trie<Trie<Entry<(), F>>>,//TODO
//    horizon: HashMap<order, entry>,
    //log: HashMap<OrderIndex, Box<Entry<(), F>>>,
}

extern "C" {
    fn ualloc_seg() -> *mut u8;
    fn cmemcpy(dst: *mut u8, src: *const u8, count: usize);
    fn cappend_size(log: &mut Trie<Entry<(), DataFlex>>, val: &Entry<(), DataFlex>, count: isize) -> (u32, &mut Entry<(), DataFlex>);
}

#[no_mangle]
pub extern "C" fn handle_packet(log: &mut Log<DataFlex>, packet: &mut Entry<(), DataFlex>, chain_shift: u8)
{
    trace!("handle {:#?}", packet.kind);
    //TODO
    let (kind, loc) = {
        (packet.kind, packet.flex.loc)
    };

    if kind & EntryKind::Layout == EntryKind::Data { // Write
        packet.kind = kind | EntryKind::ReadSuccess;
        let (ent, ptr) = log.log.entry(loc.0.into(): u32 >> chain_shift)
            .or_insert_with(|| { let mut t =  Trie::new(); t.append_size(packet, 0); t } )
            .append_size(packet, data_entry_size(packet));
        ptr.flex.loc.1 = ent.into(); //TODO
        packet.flex.loc.1 = ent.into();
        trace!("Write at {:?}", packet.flex.loc);
    }
    else { // Read
        match log.log.entry(loc.0.into(): u32 >> chain_shift).or_insert(Trie::new()).entry(loc.1.into()) { //TODO shift
            Occupied(e) => {
                trace!("Occupied entry {:?}", loc);
                unsafe {
                    let val = e.get();
                    assert!(val.kind == (EntryKind::Data | EntryKind::ReadSuccess) ||
                        val.kind == (EntryKind::Multiput | EntryKind::ReadSuccess));
                    assert!(val.kind != EntryKind::Read);
                    //ptr::copy_nonoverlapping((&*val) as *const _ as *const u8,
                    //    packet as *mut _ as *mut u8, entry_size(packet));
                    cmemcpy(packet as *mut _ as *mut u8, (&*val) as *const _ as *const u8, entry_size(&*val));
                    packet.kind = packet.kind | EntryKind::ReadSuccess;
                }
            }
            _ => {
                trace!("?@ {:?}", loc);
                unsafe { asm!("":::: "volatile"); } //otherwise test_get_none does not pass 
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn handle_multiappend(core_id: u32, ring_mask: u32,
    log: &mut Log<MultiFlex>, packet: *mut Entry<(), MultiFlex>, chain_shift: u8)
{
    unsafe {
        assert!((*packet).kind == EntryKind::Multiput ||
            (*packet).kind == (EntryKind::Multiput | EntryKind::ReadSuccess))
    };
    let num_cols = (*packet).flex.cols; //TODO len
    let mut cols = &mut (*packet).flex.data as *mut _ as *mut OrderIndex;
    //packet.kind = kind | EntryKind::ReadSuccess;
    let mut offset = 0;
    for _ in 0..num_cols {
        trace!("append? {:?} & {:?} == {:?} ?= {:?}, {:?}", (*cols).0, ring_mask, (*cols).0 & ring_mask, core_id, (*cols).0 & ring_mask == core_id.into());
        if (*cols).0 & ring_mask == core_id.into() {
            unsafe {
                //use std::intrinsics::atomic_store_rel;
                let (ent, ptr) = unsafe { log.log.entry((*cols).0.into(): u32 >> chain_shift)
                    .or_insert_with(|| { let mut t =  Trie::new(); t.append_size(&*packet, 0); t } )
                    .append_size(&*packet, multi_entry_size(&*packet)) };
                ptr::write(&mut(*cols).1, ent.into()); //TODO
                trace!("appended at {:?}", *cols);
                //atomic_store_rel(&mut(*cols).1, ent.into());
                ptr.kind = ptr.kind | EntryKind::ReadSuccess;
                (*(&mut (*ptr).flex.data as *mut _ as *mut OrderIndex).offset(offset)).1 = ent.into();
            }
        }
        offset += 1;
        cols = cols.offset(1)
    }
}

#[inline(always)]
fn entry_size<V, F>(e: &Entry<V, F>) -> usize {
    use fuzzy_log::prelude::EntryKind;
    unsafe {
        if e.kind & EntryKind::Layout == EntryKind::Data {
            trace!("found data entry");
            data_entry_size(mem::transmute::<&Entry<V, F>, &Entry<V, _>>(e))
        }
        else if e.kind & EntryKind::Layout == EntryKind::Multiput {
            trace!("found multi entry");
            multi_entry_size(mem::transmute::<&Entry<V, F>, &Entry<V, _>>(e))
        }
        else {
            panic!("invalid layout {:?}", e.kind);
        }
    }
}

#[inline(always)]
fn data_entry_size<V>(e: &Entry<V, DataFlex>) -> usize {
    assert!(e.kind & EntryKind::Layout == EntryKind::Data);
    let size = mem::size_of::<Entry<(), DataFlex<()>>>() + e.data_bytes as usize
        + e.dependency_bytes as usize;
    assert!(size <= 8192);
    size
}

#[inline(always)]
fn multi_entry_size<V>(e: &Entry<V, MultiFlex>) -> usize {
    assert!(e.kind & EntryKind::Layout == EntryKind::Multiput);
    let size = mem::size_of::<Entry<(), MultiFlex<()>>>() + e.data_bytes as usize
        + e.dependency_bytes as usize + (e.flex.cols as usize * mem::size_of::<OrderIndex>());
    assert!(size <= 8192);
    size
}

#[cfg(False)]
#[no_mangle]
pub extern "C" fn handle_packet(log: &mut Log<DataFlex>, packet: &mut Entry<(), DataFlex>)
{
    trace!("{:#?}", packet.kind);
    //TODO
    let (kind, loc) = {
        (packet.kind, packet.flex.loc)
    };

    //if kind & EntryKind::Layout == EntryKind::Multiput {
    //    trace!("bad packet");
    //    return
    //}

    //match log.log.entry(loc) {
    match log.log.entry(order_index_to_u64(loc)) {
        Vacant(e) => {
            trace!("Vacant entry {:?}", loc);
            match kind & EntryKind::Layout {
                EntryKind::Data => {
                    let seg = unsafe { ualloc_seg() };
                    //trace!("writing");
                    unsafe {
                        packet.kind = kind | EntryKind::ReadSuccess;
                        //let mut ptr = e.insert_with(|| packet.clone(), seg);
                        let mut ptr = e.insert_with(|| mem::uninitialized(), seg);
                        ptr::copy_nonoverlapping::<Entry<_, _>>(packet, ptr, 1)
                    }
                }
                _ => {
                    //if loc.0 == 0.into() {
                    //   println!("not write @ {:?}", loc);
                    //}
                    trace!("not write @ {:?}", loc);
                }
            }
        }
        Occupied(e) => {
            trace!("Occupied entry {:?}", loc);
            unsafe {
                ptr::copy_nonoverlapping::<Entry<_, _>>(&*e.get(), packet, 1);
                packet.kind = packet.kind | EntryKind::ReadSuccess;
            }
            //*packet = *e.get().clone();
            //packet.set_kind(occupied_response(kind));;
        }
    }
    //trace!("=============>\n{:#?}", packet.contents());
}

#[no_mangle]
#[cfg(False)]
pub unsafe extern "C" fn handle_multiappend(core_id: u32, ring_mask: u32,
    log: &mut Log<MultiFlex>, packet: *mut Entry<(), MultiFlex>)
{
    assert_eq!((*packet).kind, EntryKind::Multiput);
    assert!(packet != ptr::null_mut());
    trace!("multiappend! {}", core_id);

    let num_cols = (*packet).flex.cols; //TODO len
    let mut cols = &mut (*packet).flex.data as *mut _ as *mut OrderIndex;

    for _ in 0..num_cols {
        trace!("append? {:?} & {:?} == {:?} ?= {:?}", (*cols).0, ring_mask, (*cols).0 & ring_mask, core_id);
        if (*cols).0 & ring_mask == core_id.into() {
            (*cols).1 = {
                let old_horizon = log.horizon.entry((*cols).0).or_insert(0.into());
                *old_horizon = *old_horizon + 1;
                *old_horizon
            };
            trace!("appending at {:?}", *cols);
            //let data: Box<Entry<_, MultiFlex>> = {
            //    let mut ptr = Box::new(mem::uninitialized()); //TODO where to copy?
            //    ptr::copy_nonoverlapping(packet, &mut *ptr, 1);
            //    ptr.kind = ptr.kind | EntryKind::ReadSuccess; //TODO why does this not suffice?
            //    ptr
            //};
            //trace!("multiappend at {:?}", *cols);
            //log.log.insert(*cols, data);
            //log.log.insert(order_index_to_u64(*cols), packet); //TODO
            unsafe {
                let ptr = log.log.entry(order_index_to_u64(*cols)).insert_with(|| (*packet).clone());
                //ptr::copy_nonoverlapping::<Entry<_, _>>(packet, ptr, 1);
                ptr.kind = ptr.kind | EntryKind::ReadSuccess;
            }
        }
        cols = cols.offset(1)
    }
}

#[no_mangle]
pub extern "C" fn init_log() -> Box<Log> {
    panic::set_handler(|p| {
            println!("{:?} at {:?}:{:?}", p.payload(), p.location().unwrap().file(), p.location().unwrap().line());
    });
	assert_eq!(mem::size_of::<Box<Log>>(), mem::size_of::<*mut u8>());
	let log = Box::new(Log{
	        //horizon: HashMap::new(),
	        //log: HashMap::new(),
	        //log: HashMap::with_capacity(12000000),
	        log: Trie::new(),
	});
	trace!("logging start.");
	trace!("log init as {:?}.", &*log as *const _);
	//from_hash_map::more_entry_insert();
	//from_hash_map::more_insert();
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

