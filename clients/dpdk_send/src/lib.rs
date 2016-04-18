#![feature(asm)]
#![feature(recover)]
#![feature(std_panic)]
#![feature(type_ascription)]

extern crate fuzzy_log;
extern crate rand;

use types::{Mbuf, MessageBuffer};
use scheduler::Scheduler;
use async_log::AsyncLog;
use clients::SingleColumnMapClient as Client;

use std::{mem, slice};
use std::panic::{self, AssertRecoverSafe};

pub mod types;
pub mod scheduler;
pub mod async_log;
pub mod clients;

///////////////////////////////////////

pub type Sched = Scheduler<AsyncLog<Client>>;

#[no_mangle]
pub extern "C" fn alloc_clients(num_clients: u16) -> Box<Sched> {
    assert_eq!(mem::size_of::<Box<Sched>>(), mem::size_of::<*mut u8>());
    let cons = |port| {
        AsyncLog::new(Client::new(67) /*TODO*/, port)
    };
    Box::new(Scheduler::new(num_clients, cons))
}

#[no_mangle]
pub extern "C" fn get_packets_to_send(sched: &mut Sched, out: *mut *mut Mbuf, len: u16) -> u16 {
    let mut sched = AssertRecoverSafe::new(sched);
    let res = panic::recover(move || {
        assert_eq!(mem::size_of::<&mut Sched>(), mem::size_of::<*mut u8>());
        let out = unsafe { slice::from_raw_parts_mut(out, len as usize) };
        let packets = sched.get_packets_to_send(len);
        let to_send = packets.len() as u16;
        for (i, buffer) in packets.drain(..).enumerate() {
            out[i] = buffer.buffer;
            mem::forget(buffer);
        }
        to_send
    });
    match res {
        Ok(r) => r,
        Err(e) => {
            println!("AHHHHHHHHHHHHHHHH!");
            println!("error {:?}", e);
            unsafe { rust_exit() }
        }

    }
}

#[no_mangle]
pub extern "C" fn handle_received_packets(sched: &mut Sched, recv_time: u64, packets: *mut *mut Mbuf, len: u16) {
    let mut sched = AssertRecoverSafe::new(sched);
    let res = panic::recover(move || {
        assert_eq!(mem::size_of::<&mut Sched>(), mem::size_of::<*mut u8>());
        let packets = unsafe { slice::from_raw_parts_mut(packets, len as usize) };
        sched.handle_received_packets(packets, recv_time)
    });
    match res {
        Ok(r) => r,
        Err(e) => {
            println!("AHHHHHHHHHHHHHHHH!");
            println!("error {:?}", e);
            unsafe { rust_exit() }
        }
    }
}

#[no_mangle]
pub extern "C" fn clients_finished(_: Box<Sched>) {}

extern "C" {
    fn rust_exit() -> !;
}

///////////////////////////////////////

/*
struct DpdkStore;
impl<V> Store<V> for DpdkStore {
    fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult {
        unsafe {
            let buf = alloc_mbuf();
            let size = {
                let ent = (*buf).as_write_packet();
                *ent = val; //TODO copy size
                //TODO id
                entry_size(ent) as u16
            };
            prep_mbuf(buf, size);
            let recv_buf = tx_mbuf(buf); //TODO timeout
            if recv_buf != ptr::null_mut() {
                Ok((*recv_buf).flex.loc)
            }
            else {
                panic!("TODO")
            }
        }
    }


    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>> {
        unsafe {
            let buf = alloc_mbuf();
            let size = mem::size_of::<Entry<(), DataFlex<()>>>();
            {
                let ent = mbuf_as_read_packet(buf);
                (*ent).flex.loc = key;
                
            };
            prep_mbuf(buf, size);
            let recv_buf = tx_mbuf(buf); //TODO timeout
            panic!()
        }
    }

    fn multi_append(&mut self, chains: &[OrderIndex], data: V, deps: &[OrderIndex]) -> InsertResult {
        panic!()
    }
}*/


#[cfg(test)]
mod test {
    use std::mem::size_of;

    use scheduler::Scheduler;
    use async_log::AsyncLog;
    use clients::SingleColumnMapClient;

    #[test]
    fn test_sizes() {
        assert_eq!(size_of::<Box<Scheduler<AsyncLog<SingleColumnMapClient>>>>(), size_of::<*mut u8>());
        assert_eq!(size_of::<&mut Scheduler<AsyncLog<SingleColumnMapClient>>>(), size_of::<*mut u8>());
    }
}
