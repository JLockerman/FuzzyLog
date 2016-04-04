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

//struct entry_pointer_and_index get_next_loc(void *, size_t, uint32_t);
//char *get_val(uint32_t, uint32_t);

#[repr(C)]
pub struct entry_pointer_and_index {
    pub entry: *mut *mut u8,
    pub ptr: *mut u8,
    pub index: u32,
}

#[no_mangle]
pub extern "C" fn get_next_loc(log: &mut Log<DataFlex>, data_size: usize, chain: u32) -> entry_pointer_and_index
{
	log.log.entry(chain.into())
	    .or_insert_with(|| { let mut t =  Trie::new(); t.next_entry(0); t })
	    .next_entry(data_size)
}

#[no_mangle]
pub extern "C" fn get_val(log: &mut Log<DataFlex>, chain: u32, index: u32) -> *const u8 {
    log.log.entry(chain.into())
        .or_insert_with(|| { let mut t =  Trie::new(); t.next_entry(0); t })
        .get(index.into()).map_or(ptr::null_mut(), |p| p as *const _ as *const _)
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

