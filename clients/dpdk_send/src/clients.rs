
use std::collections::HashMap;

use rand::{self, Rng};

use fuzzy_log::prelude::*;
use types::*;
use async_log::AsyncLogClient;

pub struct SingleColumnMapClient {
    map: HashMap<u32, u32>,
    cannonical_map: HashMap<u32, u32>,
    col: u32,
    last_written_entry: u32,
    op_count: u16,
    total_ops: u64,
    last_op_was_write: bool,
}

impl SingleColumnMapClient {
    pub fn new(column: u32) -> Self {
        SingleColumnMapClient {
            map: HashMap::new(),
            cannonical_map: HashMap::new(),
            col: column,
            last_written_entry: 0,
            op_count: 0,
            total_ops: 0,
            last_op_was_write: false,
        }
    }
}

impl AsyncLogClient for SingleColumnMapClient {
    type V = [(u32, u32)];

    fn on_new_entry(&mut self, val: &[(u32, u32)], locs: &[OrderIndex], _: &[OrderIndex]) -> bool {
        println!("new entry");
        for &(o, i) in locs {
            if self.col == o.into() {
                for &(k, v) in val {
                    self.map.insert(k, v);
                }
                return i == self.last_written_entry.into()
            }
        }
        return false
    }

    fn on_start(&mut self, buf: MessageBuffer) -> MessageBuffer {
        println!("client start");
        self.after_op(buf)
    }

    fn after_op(&mut self, mut buf: MessageBuffer) -> MessageBuffer {
        println!("after op");
        if self.last_op_was_write {
            self.last_written_entry = buf.locs()[0].1.into()
        }

        if self.op_count < 10 {
            println!("gonna write {}", self.op_count);
            let val = rand::thread_rng().gen();
            buf.as_append(self.col.into(), &[val, val] ,&[]);
            self.cannonical_map.insert(val, val);
            self.op_count += 1;
            self.last_op_was_write = true;
            buf 
        }
        else if self.last_op_was_write {
            println!("gonna read");
            //TODO
            buf.as_read((self.col.into(), 0.into()));
            self.last_op_was_write = false;
            buf
        }
        else {
            println!("gonna validate");
            assert_eq!(self.map, self.cannonical_map);
            self.total_ops += self.op_count as u64;
            self.op_count = 0;
            println!("good after {} writes.\n\tmap len {}", self.total_ops, self.map.len());
            self.after_op(buf)
        }
    }
}

#[cfg(test)]
mod test {

    use std::mem;
    use std::collections::HashMap;

    use fuzzy_log::prelude::*;

    use types::*;
    use scheduler::{Scheduler, AsyncClient, curr_time};
    use async_log::AsyncLog;

    mod single_col {

        use std::collections::HashMap;

        use types::*;
        use scheduler::{Scheduler, curr_time};
        use async_log::AsyncLog;
        use clients::SingleColumnMapClient as Client;

        use fuzzy_log::prelude::*;

        use super::*;

        #[test]
        fn single_client_local() {
            const BURST_SIZE: u16 = 8;
            const ITERS: usize = 100;
            let mut chains = HashMap::with_capacity(ITERS);
            let mut recvs = Vec::with_capacity(BURST_SIZE as usize);
            let cons = |port| {
                AsyncLog::new(Client::new(3) /*TODO*/, port)
            };
            let mut sched = Box::new(Scheduler::new(1, cons));
            println!("start");
            for _ in 0..ITERS {
                {
                    let sends = sched.get_packets_to_send(BURST_SIZE);
                    for b in sends.drain(..) {
                        let &key = b.wait_key();
                        let entr = unsafe { &mut *mbuf_as_packet(b.buffer) };
                        match entr.kind & EntryKind::Layout {
                            EntryKind::Multiput => {
                                panic!("should not be")
                            }
                            EntryKind::Data => {
                                let chain_num = unsafe { entr.as_data_entry().flex.loc.0 };
                                let chain = chains.entry(chain_num).or_insert_with(|| { 
                                    let mut v = Vec::new();
                                    v.push(MessageBuffer { buffer: alloc_mbuf() });
                                    v
                                });//TODO start @ 1
                                let next = chain.len();
                                {
                                    let entr = unsafe { entr.as_data_entry_mut() };
                                    entr.flex.loc.1 = (next as u32).into();
                                    entr.kind = entr.kind | EntryKind::ReadSuccess;
                                }
                                println!("Write @ ({:?}, {:?})", chain_num, next);
                                chain.push(b.clone());
                                recvs.push(b);
                            }
                            EntryKind::Read => {
                                let loc = unsafe { entr.as_data_entry().flex.loc };
                                println!("Read @ {:?}", loc);
                                let chain = chains.get(&loc.0);
                                if let Some(entr) = chain.and_then(|chain| chain.get(loc.1.into() : u32 as usize)) {
                                    unsafe {
                                        copy_mbuf(b.buffer, &*entr.buffer);
                                        mbuf_set_src_port(b.buffer, key);
                                    };
                                }
                                else {
                                    let entr = unsafe { entr.as_data_entry_mut() };
                                    entr.kind = EntryKind::NoValue;
                                }
                                recvs.push(b);
                            }
                            _ => unreachable!(),
                        }
                    }
                }
                let mut recvs: Vec<_> = recvs.drain(..).map(|MessageBuffer{ buffer }| buffer).collect();
                sched.handle_received_packets(&mut recvs[..], curr_time())
            }
        }

        #[test]
        fn single_client_local2() {
            super::local_run(1, |port| AsyncLog::new(Client::new(3) /*TODO*/, port));
        }
    }

    fn local_run<C, F: FnMut(u16) -> C>(clients: u16, constructor: F)
    where C: AsyncClient {
        const BURST_SIZE: u16 = 8;
        const ITERS: usize = 100;
        let mut chains = HashMap::with_capacity(ITERS);
        let mut recvs = Vec::with_capacity(BURST_SIZE as usize);
        let mut sched = Box::new(Scheduler::new(clients, constructor));
        println!("start");
        for _ in 0..ITERS {
            {
                let sends = sched.get_packets_to_send(BURST_SIZE);
                println!("{} sent packets", sends.len());
                for b in sends.drain(..) {
                    let &key = b.wait_key();
                    let entr = unsafe { &mut *mbuf_as_packet(b.buffer) };
                    match entr.kind & EntryKind::Layout {
                        EntryKind::Multiput => {
                            panic!("should not be")
                        }
                        EntryKind::Data => {
                            let chain_num = unsafe { entr.as_data_entry().flex.loc.0 };
                            let chain = chains.entry(chain_num).or_insert_with(|| { 
                                let mut v = Vec::new();
                                v.push(MessageBuffer { buffer: alloc_mbuf() });
                                v
                            });//TODO start @ 1
                            let next = chain.len();
                            {
                                let entr = unsafe { entr.as_data_entry_mut() };
                                entr.flex.loc.1 = (next as u32).into();
                                entr.kind = entr.kind | EntryKind::ReadSuccess;
                            }
                            println!("Write @ ({:?}, {:?})", chain_num, next);
                            chain.push(b.clone());
                            recvs.push(b);
                        }
                        EntryKind::Read => {
                            let loc = unsafe { entr.as_data_entry().flex.loc };
                            println!("Read @ {:?}", loc);
                            let chain = chains.get(&loc.0);
                            if let Some(entr) = chain.and_then(|chain| chain.get(loc.1.into() : u32 as usize)) {
                                unsafe {
                                    copy_mbuf(b.buffer, &*entr.buffer);
                                    mbuf_set_src_port(b.buffer, key);
                                };
                            }
                            else {
                                let entr = unsafe { entr.as_data_entry_mut() };
                                entr.kind = EntryKind::NoValue;
                            }
                            recvs.push(b);
                        }
                        _ => unreachable!(),
                    }
                }
            }
            let mut recvs: Vec<_> = recvs.drain(..).map(|MessageBuffer{ buffer }| buffer).collect();
            sched.handle_received_packets(&mut recvs[..], curr_time())
        }
    }

    #[derive(Clone)]
    pub struct Buf {
        wait_key: WaitKey,
        entr: Entry<()>,
    }

    impl Clone for MessageBuffer {
        fn clone(&self) -> Self {
            MessageBuffer {
                buffer: unsafe { (*self.buffer).clone_ptr() },
            }
        }
    }

    impl Mbuf {
        pub fn as_buf_mut(&mut self) -> &mut Buf {
            unsafe { mem::transmute(self) }
        }

        pub fn as_buf(&self) -> &Buf {
            unsafe { mem::transmute(self) }
        }

        pub unsafe fn to_box_buf(&mut self) -> Box<Buf> {
            Box::from_raw(self.as_buf_mut())
        }

        pub fn clone_ptr(&self) -> *mut Self {
            let ptr = Box::into_raw(Box::new(self.as_buf().clone()));
            ptr as *mut _
        }
    }

    #[allow(private_no_mangle_fns, dead_code)]
    #[no_mangle]
    pub extern "C" fn alloc_mbuf() -> *mut Mbuf {
        unsafe {
            let b: Box<Buf> = Box::new(mem::zeroed());
            let ptr = Box::into_raw(b) as *mut _;
            assert!(ptr != ::std::ptr::null_mut());
            assert!(ptr as usize != 1);
            ptr
        }
    }

    #[allow(private_no_mangle_fns, dead_code)]
    #[no_mangle]
    pub extern "C" fn mbuf_set_src_port(mbuf: *mut Mbuf, port: u16) {
        assert!(mbuf != ::std::ptr::null_mut());
        assert!(mbuf as usize != 1);
        unsafe { (*mbuf).as_buf_mut().wait_key = port };
    }

    #[allow(private_no_mangle_fns, dead_code)]
    #[no_mangle]
    pub extern "C" fn copy_mbuf(dst: *mut Mbuf, src: *const Mbuf) {
        assert!(dst != ::std::ptr::null_mut());
        assert!(dst as usize != 1);
        assert!(src != ::std::ptr::null());
        assert!(src as usize != 1);
        unsafe { *(*dst).as_buf_mut() = (*src).as_buf().clone()};
    }

    #[allow(private_no_mangle_fns, dead_code)]
    #[no_mangle]
    pub extern "C" fn dpdk_free_mbuf(mbuf: *mut Mbuf) {
        //panic!("{:x}", mbuf as usize);
        assert!(mbuf != ::std::ptr::null_mut());
        assert!(mbuf as usize != 1);
        //unsafe { (*mbuf).to_box_buf() };
    }

    #[allow(private_no_mangle_fns, dead_code)]
    #[no_mangle]
    pub extern "C" fn prep_mbuf(mbuf: *mut Mbuf, _: u16, src_port: u16) {
        assert!(mbuf != ::std::ptr::null_mut());
        assert!(mbuf as usize != 1);
        unsafe {
            (*mbuf).as_buf_mut().wait_key = src_port;
        }
    }

    #[allow(private_no_mangle_fns, dead_code)]
    #[no_mangle]
    pub extern "C" fn mbuf_get_src_port_ptr(mbuf: &Mbuf) -> &u16 { //TODO should be dest port
        assert!(mbuf as *const _ as usize != 0);
        assert!(mbuf as *const _ as usize != 1);
        &(*mbuf).as_buf().wait_key
    }

    #[allow(private_no_mangle_fns, dead_code)]
    #[no_mangle]
    pub extern "C" fn mbuf_into_write_packet(mbuf :*mut Mbuf) -> *mut Entry<(), DataFlex> {
        assert!(mbuf != ::std::ptr::null_mut());
        assert!(mbuf as usize != 1);
        unsafe {
            let entr = &mut (*mbuf).as_buf_mut().entr;
            entr.kind = EntryKind::Data;
            entr.as_data_entry_mut()
        }
    }

    #[allow(private_no_mangle_fns, dead_code)]
    #[no_mangle]
    pub extern "C" fn mbuf_into_read_packet(mbuf :*mut Mbuf) -> *mut Entry<(), DataFlex> {
        assert!(mbuf != ::std::ptr::null_mut());
        assert!(mbuf as usize != 1);
        unsafe {
            let entr = &mut (*mbuf).as_buf_mut().entr;
            entr.kind = EntryKind::Read;
            entr.as_data_entry_mut()
        }
    }

    #[allow(private_no_mangle_fns, dead_code)]
    #[no_mangle]
    pub extern "C" fn mbuf_into_multi_packet(mbuf :*mut Mbuf) -> *mut Entry<(), MultiFlex> {
        assert!(mbuf != ::std::ptr::null_mut());
        assert!(mbuf as usize != 1);
        unsafe {
            let entr = &mut (*mbuf).as_buf_mut().entr;
            entr.kind = EntryKind::Multiput;
            entr.as_multi_entry_mut()
        }
    }

    #[allow(private_no_mangle_fns, dead_code)]
    #[no_mangle]
    pub extern "C" fn mbuf_as_packet(mbuf :*mut Mbuf) -> *mut Entry<()> {
        assert!(mbuf != ::std::ptr::null_mut());
        assert!(mbuf as usize != 1);
        unsafe {
            &mut (*mbuf).as_buf_mut().entr
        }
    }

}
