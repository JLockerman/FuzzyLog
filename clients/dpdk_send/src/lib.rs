#![feature(asm)]
#![feature(type_ascription)]

#[macro_use] extern crate log;

extern crate env_logger;

extern crate fuzzy_log;
extern crate rand;

use std::rc::Rc;
use std::cell::RefCell;

use types::{Mbuf};
use scheduler::Scheduler;
use async_log::AsyncLog;
//use clients::SCMapClient as Client;
use clients::causal::CausalClient as Client;

use std::{mem, slice};
use std::panic::{self, AssertUnwindSafe};

pub mod types;
pub mod scheduler;
pub mod async_log;
pub mod clients;

///////////////////////////////////////

pub type Log = Rc<RefCell<AsyncLog<Client>>>;
pub type Sched = Scheduler<Log>;

thread_local!(static LOG: RefCell<Option<Log>> = RefCell::new(None));

#[no_mangle]
pub extern "C" fn alloc_clients(iteration: u32, core_id: u16, num_clients: u16) -> Box<Sched> {
    assert_eq!(mem::size_of::<Box<Sched>>(), mem::size_of::<*mut u8>());
    //let cons = |port| {
    //    AsyncLog::new(Client::new(67, 5) /*TODO*/, port)
    //};
    Box::new(Scheduler::new(num_clients, |_| LOG.with(|r| {
        let mut r = r.borrow_mut();
        if r.is_none() {
            assert_eq!(iteration, 1);
            if core_id & 1 == 0 {
                *r = Some(Rc::new(RefCell::new(AsyncLog::new(Client::new(68, 71) , 1))))
            }
            else {
                *r = Some(Rc::new(RefCell::new(AsyncLog::new(Client::new(71, 68) , 1))))
            }
        }
        else {
            let mut r = r.as_ref().unwrap().borrow_mut();
            r.client.reset_write_counter();
            r.client.reset_read_counter();
            match iteration {
                a if 10 < a && a <= 20 => r.client.assume_partitioned(0),
                _ =>  r.client.assume_connected(),
            }
        }
        r.as_ref().unwrap().clone()
    })))
}

#[no_mangle]
pub extern "C" fn get_packets_to_send(sched: &mut Sched, out: *mut *mut Mbuf, len: u16) -> u16 {
    let mut sched = AssertUnwindSafe(sched);
    let res = panic::catch_unwind(move || {
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
            trace!("AHHHHHHHHHHHHHHHH!");
            trace!("error {:?}", e);
            unsafe { rust_exit() }
        }

    }
}

#[no_mangle]
pub extern "C" fn handle_received_packets(sched: &mut Sched, recv_time: u64, packets: *mut *mut Mbuf, len: u16) {
    let mut sched = AssertUnwindSafe(sched);
    let res = panic::catch_unwind(move || {
        assert_eq!(mem::size_of::<&mut Sched>(), mem::size_of::<*mut u8>());
        let packets = unsafe { slice::from_raw_parts_mut(packets, len as usize) };
        sched.handle_received_packets(packets, recv_time)
    });
    match res {
        Ok(r) => r,
        Err(e) => {
            trace!("AHHHHHHHHHHHHHHHH!");
            trace!("error {:?}", e);
            unsafe { rust_exit() }
        }
    }
}

#[no_mangle]
pub extern "C" fn clients_finished(s: Box<Sched>) {
    let res = panic::catch_unwind(AssertUnwindSafe(move || {
        let r = LOG.with(|r| {
            r.borrow().as_ref().unwrap().clone()
        });
        let r = r.borrow();
        println!("{:?}: {}, {}", Box::into_raw(s), r.client.num_writes(), r.client.num_reads());
    }));
    match res {
        Ok(r) => r,
        Err(e) => {
            trace!("AHHHHHHHHHHHHHHHH!");
            trace!("error {:?}", e);
            unsafe { rust_exit() }
        }
    }
}

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
    use clients::SCMapClient;

    #[test]
    fn test_sizes() {
        assert_eq!(size_of::<Box<Scheduler<AsyncLog<SCMapClient>>>>(), size_of::<*mut u8>());
        assert_eq!(size_of::<&mut Scheduler<AsyncLog<SCMapClient>>>(), size_of::<*mut u8>());
    }
}
