//#![cfg_attr(test, feature(test))]

#[macro_use] extern crate bitflags;
#[macro_use] extern crate custom_derive;
#[cfg(test)] #[macro_use] extern crate grabbag_macros;
#[macro_use] extern crate log;
#[macro_use] extern crate newtype_derive;
#[macro_use] extern crate packet_macro_impl;
#[macro_use] extern crate packet_macro2;

#[cfg(feature = "dynamodb_tests")]
extern crate hyper;
#[cfg(feature = "dynamodb_tests")]
extern crate rusoto;

//#[cfg(test)]
//extern crate test;

extern crate bit_set;
extern crate byteorder;
extern crate deque;
extern crate rustc_serialize;
extern crate mio;
extern crate nix;
extern crate net2;
extern crate time;
extern crate rand;
extern crate uuid;
extern crate libc;
extern crate lazycell;
extern crate env_logger;

//FIXME only needed until repeated multiput returns is fixed
extern crate linked_hash_map;

#[macro_use] mod counter_macro;

#[macro_use]
mod general_tests;

pub mod storeables;
pub mod packets;
//pub mod prelude;
/*pub mod local_store;
pub mod udp_store;
pub mod tcp_store;
pub mod multitcp_store;
pub mod skeens_store;
pub mod servers;*/
pub mod servers2;
//pub mod color_api;
#[cfg(FIXME)]
pub mod async;
mod hash;
pub mod socket_addr;
//TODO only for testing, should be private
pub mod buffer;

#[cfg(FIXME)]
pub mod c_binidings {

    // use prelude::*;
    use packets::*;
    //use tcp_store::TcpStore;
    // use multitcp_store::TcpStore;
    use async::fuzzy_log::log_handle::LogHandle;

    use std::collections::HashMap;
    use std::{mem, ptr, slice};

    use std::ffi::CStr;
    use std::net::SocketAddr;
    use std::os::raw::c_char;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use mio;

    use byteorder::{ByteOrder, NativeEndian};

    pub type DAG = LogHandle<[u8]>;
    pub type ColorID = u32;

    #[repr(C)]
    pub struct colors {
        numcolors: usize,
        mycolors: *mut ColorID,
    }

    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct WriteId {
        p1: u64,
        p2: u64,
    }

    #[repr(C)]
    pub struct WriteLocations {
        pub num_locs: usize,
        pub locs: *mut OrderIndex,
    }

    #[repr(C)]
    pub struct WriteIdAndLocs {
        pub write_id: WriteId,
        pub locs: WriteLocations,
    }

    impl WriteId {
        fn from_uuid(id: Uuid) -> Self {
            let bytes = id.as_bytes();
            WriteId {
                p1: NativeEndian::read_u64(&bytes[0..8]),
                p2: NativeEndian::read_u64(&bytes[8..16]),
            }
        }

        fn to_uuid(self) -> Uuid {
            let WriteId {p1, p2} = self;
            let mut bytes = [0u8; 16];
            NativeEndian::write_u64(&mut bytes[0..8], p1);
            NativeEndian::write_u64(&mut bytes[8..16], p2);
            Uuid::from_bytes(&bytes[..]).unwrap()
        }

        #[allow(dead_code)]
        fn nil() -> Self {
            WriteId::from_uuid(Uuid::nil())
        }
    }

    #[no_mangle]
    pub extern "C" fn new_dag_handle(lock_server_ip: *const c_char,
        num_chain_ips: usize, chain_server_ips: *const *const c_char,
        color: *const colors) -> Box<DAG> {
        assert_eq!(mem::size_of::<Box<DAG>>(), mem::size_of::<*mut u8>());
        //assert_eq!(num_ips, 1, "Multiple servers are not yet supported via the C API");
        assert!(chain_server_ips != ptr::null());
        assert!(unsafe {*chain_server_ips != ptr::null()});
        assert!(num_chain_ips >= 1);
        assert!(color != ptr::null());
        assert!(colors_valid(color));
        let _ = ::env_logger::init();
        let (lock_server_addr, server_addrs) = unsafe {
            let addrs = slice::from_raw_parts(chain_server_ips, num_chain_ips)
                .into_iter().map(|&s|
                    CStr::from_ptr(s).to_str().expect("invalid IP string")
                        .parse().expect("invalid IP addr")
                    ).collect::<Vec<SocketAddr>>();
            if lock_server_ip != ptr::null() {
                let lock_server_addr = CStr::from_ptr(lock_server_ip).to_str()
                    .expect("invalid IP string")
                    .parse().expect("invalid IP addr");
                (lock_server_addr, addrs)
            }
            else {
                (addrs[0], addrs)
            }
        };
        let colors = unsafe {slice::from_raw_parts((*color).mycolors, (*color).numcolors)};
        Box::new(LogHandle::spawn_tcp_log2(lock_server_addr, server_addrs.into_iter(),
            colors.into_iter().cloned().map(order::from)))
    }

    #[no_mangle]
    pub unsafe extern "C" fn new_dag_handle_with_skeens(
        num_chain_ips: usize, chain_server_ips: *const *const c_char, color: *const colors
    ) -> Box<DAG> {
        assert_eq!(mem::size_of::<Box<DAG>>(), mem::size_of::<*mut u8>());
        //assert_eq!(num_ips, 1, "Multiple servers are not yet supported via the C API");
        assert!(chain_server_ips != ptr::null());
        assert!(unsafe {*chain_server_ips != ptr::null()});
        assert!(num_chain_ips >= 1);
        assert!(color != ptr::null());
        assert!(colors_valid(color));
        let _ = ::env_logger::init();
        trace!("Lib num chain servers {:?}", num_chain_ips);
        let server_addrs = slice::from_raw_parts(chain_server_ips, num_chain_ips)
            .into_iter().map(|&s|
                CStr::from_ptr(s).to_str().expect("invalid IP string")
                    .parse().expect("invalid IP addr")
            ).collect::<Vec<SocketAddr>>();
        let colors = unsafe {slice::from_raw_parts((*color).mycolors, (*color).numcolors)};
        Box::new(LogHandle::new_tcp_log(server_addrs.into_iter(),
            colors.into_iter().cloned().map(order::from)))
    }

    //NOTE currently can only use 31bits of return value
    #[no_mangle]
    pub extern "C" fn do_append(dag: *mut DAG, data: *const u8, data_size: usize,
        inhabits: *mut colors, depends_on: *mut colors, async: u8) -> WriteId {
        assert!(data_size == 0 || data != ptr::null());
        assert!(inhabits != ptr::null_mut());
        assert!(colors_valid(inhabits));
        assert!(data_size <= 8000);

        let (dag, data, inhabits) = unsafe {
            let s = slice::from_raw_parts_mut((*inhabits).mycolors, (*inhabits).numcolors);
            (dag.as_mut().expect("need to provide a valid DAGHandle"),
                slice::from_raw_parts(data, data_size),
                mem::transmute(s))
        };
        let depends_on: &mut [order] = unsafe {
            if depends_on != ptr::null_mut() {
                assert!(colors_valid(depends_on));
                let s = slice::from_raw_parts_mut((*depends_on).mycolors, (*depends_on).numcolors);
                mem::transmute(s)
            }
            else {
                &mut []
            }
        };
        let id = dag.color_append(data, inhabits, depends_on, async != 0);
        WriteId::from_uuid(id)
    }

    fn colors_valid(c: *const colors) -> bool {
        unsafe { c != ptr::null() &&
            ((*c).numcolors == 0 || (*c).mycolors != ptr::null_mut()) }
    }

    #[no_mangle]
    pub extern "C" fn async_no_remote_append(
        dag: *mut DAG,
        data: *const u8,
        data_size: usize,
        inhabits: *mut colors,
        deps: *mut OrderIndex,
        num_deps: usize,
    ) -> WriteId {
        assert!(data_size == 0 || data != ptr::null());
        assert!(inhabits != ptr::null_mut());
        assert!(colors_valid(inhabits));
        assert!(data_size <= 8000);

        let (dag, data, inhabits, deps) = unsafe {
            let colors: *mut order = (*inhabits).mycolors as *mut _;
            let s = slice::from_raw_parts_mut(colors, (*inhabits).numcolors);
            let d = slice::from_raw_parts_mut(deps, num_deps);
            (dag.as_mut().expect("need to provide a valid DAGHandle"),
                slice::from_raw_parts(data, data_size),
                s,
                d)
        };
        let id = dag.color_no_remote_append(data, inhabits, deps, true).0;
        WriteId::from_uuid(id)
    }

    #[no_mangle]
    pub extern "C" fn no_remote_append(
        dag: *mut DAG,
        data: *const u8,
        data_size: usize,
        inhabits: *mut colors,
        deps: *mut OrderIndex,
        num_deps: usize,
    ) -> OrderIndex {
        assert!(data_size == 0 || data != ptr::null());
        assert!(inhabits != ptr::null_mut());
        assert!(colors_valid(inhabits));
        assert!(data_size <= 8000);

        let (dag, data, inhabits, deps) = unsafe {
            let colors: *mut order = (*inhabits).mycolors as *mut _;
            let s = slice::from_raw_parts_mut(colors, (*inhabits).numcolors);
            let d = slice::from_raw_parts_mut(deps, num_deps);
            (dag.as_mut().expect("need to provide a valid DAGHandle"),
                slice::from_raw_parts(data, data_size),
                s,
                d)
        };
        let loc = dag.color_no_remote_append(data, inhabits, deps, false).1;
        loc.unwrap_or(OrderIndex(0.into(), 0.into()))
    }

    #[no_mangle]
    pub extern "C" fn async_causal_append(
        dag: *mut DAG,
        data: *const u8,
        data_size: usize,
        inhabits: *mut colors,
        depends_on: *mut colors,
        happens_after: *mut OrderIndex,
        num_happens_after: usize,
    ) -> WriteId {
        assert!(data_size == 0 || data != ptr::null());
        assert!(inhabits != ptr::null_mut());
        assert!(colors_valid(inhabits));

        let (dag, data, inhabits, happens_after) = unsafe {
            let colors: *mut order = (*inhabits).mycolors as *mut _;
            let s = slice::from_raw_parts_mut(colors, (*inhabits).numcolors);
            let h = slice::from_raw_parts_mut(happens_after, num_happens_after);
            (dag.as_mut().expect("need to provide a valid DAGHandle"),
                slice::from_raw_parts(data, data_size),
                s,
                h)
        };
        let depends_on: &mut [order] = unsafe {
            if depends_on != ptr::null_mut() {
                assert!(colors_valid(depends_on));
                let s = slice::from_raw_parts_mut((*depends_on).mycolors, (*depends_on).numcolors);
                mem::transmute(s)
            }
            else {
                &mut []
            }
        };

        let id = dag.causal_color_append(data, inhabits, depends_on, happens_after);
        WriteId::from_uuid(id)
    }

    #[no_mangle]
    pub unsafe extern "C" fn async_simple_causal_append(
        dag: *mut DAG,
        data: *const u8,
        data_size: usize,
        inhabits: *mut colors,
        happens_after: *mut colors,
    ) -> WriteId {
        assert!(data_size == 0 || data != ptr::null());
        assert!(inhabits != ptr::null_mut());
        assert!(colors_valid(inhabits));

        let (dag, data, inhabits) = {
            let colors: *mut order = (*inhabits).mycolors as *mut _;
            let s = slice::from_raw_parts_mut(colors, (*inhabits).numcolors);
            (
                dag.as_mut().expect("need to provide a valid DAGHandle"),
                slice::from_raw_parts(data, data_size),
                s,
            )
        };
        let happens_after = {
            if happens_after != ptr::null_mut() {
                assert!(colors_valid(happens_after));
                slice::from_raw_parts_mut((*happens_after).mycolors as *mut _, (*happens_after).numcolors)
            } else {
                &mut []
            }
        };

        let id = dag.simple_causal_append(data, inhabits, happens_after);
        WriteId::from_uuid(id)
    }

    #[no_mangle]
    pub unsafe extern "C" fn wait_for_all_appends(dag: *mut DAG) {
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        dag.wait_for_all_appends();
    }

    #[no_mangle]
    pub unsafe extern "C" fn wait_for_a_specific_append(dag: *mut DAG, write_id: WriteId) {
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        dag.wait_for_a_specific_append(write_id.to_uuid());
    }

    #[no_mangle]
    pub unsafe extern "C" fn wait_for_any_append(dag: *mut DAG) -> WriteId {
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        let id = dag.wait_for_any_append().map(|t| t.0).unwrap_or(Uuid::nil());
        WriteId::from_uuid(id)
    }

    #[no_mangle]
    pub unsafe extern "C" fn wait_for_a_specific_append_and_locations(
        dag: *mut DAG, write_id: WriteId
    ) -> WriteLocations {
        let locs = dag.as_mut().expect("need to provide a valid DAGHandle")
            .wait_for_a_specific_append(write_id.to_uuid()).unwrap();
        build_write_locs(locs)
    }

    #[no_mangle]
    pub unsafe extern "C" fn try_wait_for_any_append(dag: *mut DAG) -> WriteId {
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        let id = dag.try_wait_for_any_append().map(|t| t.0).unwrap_or(Uuid::nil());
        WriteId::from_uuid(id)
    }

    #[no_mangle]
    pub unsafe extern "C" fn try_wait_for_any_append_and_location(dag: *mut DAG)
    -> WriteIdAndLocs {
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        match dag.try_wait_for_any_append() {
            None => WriteIdAndLocs {
                write_id: WriteId::nil(),
                locs: WriteLocations { num_locs: 0, locs: ptr::null_mut() },
            },
            Some((id, locs)) => WriteIdAndLocs {
                write_id: WriteId::from_uuid(id),
                locs: build_write_locs(locs),
            },
        }

    }

    #[no_mangle]
    pub unsafe extern "C" fn flush_completed_appends(dag: *mut DAG) {
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        dag.flush_completed_appends();
    }

    unsafe fn build_write_locs(locs: Vec<OrderIndex>) -> WriteLocations {
        let num_locs = locs.len();
        let my_locs = ::libc::malloc(mem::size_of::<OrderIndex>() * num_locs) as *mut _;
        assert!(my_locs != ptr::null_mut());
        let s = slice::from_raw_parts_mut(my_locs, num_locs);
        for i in 0..num_locs {
            s[i] = locs[i];
        }
        WriteLocations {
            num_locs: num_locs,
            locs: my_locs,
        }
    }


    //NOTE we need either a way to specify data size, or to pass out a pointer
    // this version simple assumes that no data+metadat passed in or out will be
    // greater than DELOS_MAX_DATA_SIZE
    #[no_mangle]
    pub extern "C" fn get_next(dag: *mut DAG, data_out: *mut u8, data_read: *mut usize,
        inhabits_out: *mut colors) {
        assert!(data_out != ptr::null_mut());
        assert!(data_read != ptr::null_mut());
        assert!(inhabits_out != ptr::null_mut());

        let dag = unsafe {dag.as_mut().expect("need to provide a valid DAGHandle")};
        let data_out = unsafe { slice::from_raw_parts_mut(data_out, 8000)};
        let data_read = unsafe {data_read.as_mut().expect("must provide valid data_out")};
        let val = dag.get_next();
        unsafe {
            let (mycolors, numcolors) = match val {
                Some((data, inhabited_colors)) => {
                    *data_read = <[u8] as Storeable>::copy_to_mut(data, data_out);
                    let numcolors = inhabited_colors.len();
                    let mycolors = ::libc::malloc(mem::size_of::<ColorID>() * numcolors) as *mut _;
                    let s = slice::from_raw_parts_mut(mycolors, numcolors);
                    for i in 0..numcolors {
                        let e: order = inhabited_colors[i].0;
                        s[i] = e.into();
                    }
                    //ptr::copy_nonoverlapping(&inhabited_colors[0], mycolors, numcolors);
                    (mycolors, numcolors)
                }
                None => {
                    (ptr::null_mut(), 0)
                }
            };

            ptr::write(inhabits_out, colors{ numcolors: numcolors, mycolors: mycolors});
        }
    }

    #[repr(C)]
    pub struct Vals {
        data: *const u8,
        locs: *const OrderIndex,
    }

    #[no_mangle]
    pub unsafe extern "C" fn get_next2(
        dag: *mut DAG,
        data_read: *mut usize,
        num_locs: *mut usize,
    ) -> Vals {
        assert!(data_read != ptr::null_mut());
        let dag = unsafe {dag.as_mut().expect("need to provide a valid DAGHandle")};
        let val = dag.get_next();
        let (data, locs) = val.unwrap_or((&[], &[]));

        ptr::write(data_read, data.len());
        ptr::write(num_locs, locs.len());

        Vals { data: data.as_ptr(), locs: locs.as_ptr() }
    }

    #[no_mangle]
    pub extern "C" fn snapshot(dag: *mut DAG) {
        let dag = unsafe {dag.as_mut().expect("need to provide a valid DAGHandle")};
        dag.take_snapshot();
    }

    #[no_mangle]
    pub unsafe extern "C" fn snapshot_colors(dag: *mut DAG, colors: *mut colors) {
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        assert!(colors != ptr::null_mut());
        assert!(colors_valid(colors));
        let colors = {
            let num_colors = (*colors).numcolors;
            let colors: *mut order = (*colors).mycolors as *mut _;
            slice::from_raw_parts_mut(colors, num_colors)
        };
        dag.snapshot_colors(colors);
    }

    #[no_mangle]
    pub unsafe extern "C" fn close_dag_handle(dag: *mut DAG) {
        assert!(dag != ptr::null_mut());
        Box::from_raw(dag);
    }
    ////////////////////////////////////
    //         Server bindings        //
    ////////////////////////////////////

    #[no_mangle]
    pub extern "C" fn start_fuzzy_log_server(server_ip: *const c_char) -> ! {
        start_fuzzy_log_server_for_group(server_ip, 0, 1)
    }

    #[no_mangle]
    pub extern "C" fn start_fuzzy_log_server_thread(server_ip: *const c_char) {
        start_fuzzy_log_server_thread_from_group(server_ip, 0, 1)
    }

    #[no_mangle]
    pub extern "C" fn start_fuzzy_log_server_for_group(server_ip: *const c_char,
        server_number: u32, total_servers_in_group: u32) -> ! {
        start_server(server_ip, server_number,
            total_servers_in_group, &AtomicUsize::new(0));
    }

    #[no_mangle]
    pub extern "C" fn start_fuzzy_log_server_thread_from_group(server_ip: *const c_char,
        server_number: u32, total_servers_in_group: u32) {
            assert!(server_ip != ptr::null());
            let server_started = AtomicUsize::new(0);
            let (started, server_ip) = unsafe {
                //This should be safe since the while loop at the of the function
                //prevents it from exiting until the server is started and
                //server_started is no longer used
                (extend_lifetime(&server_started), &*server_ip)
            };
            let handle = ::std::thread::spawn(move || {
                start_server(server_ip, server_number, total_servers_in_group, &started)
            });
            while !server_started.load(Ordering::SeqCst) < 1 {}
            mem::forget(handle);
            mem::drop(server_ip);

            unsafe fn extend_lifetime<'a, 'b, T>(r: &'a T) -> &'b T {
                ::std::mem::transmute(r)
            }
    }

    fn start_server(server_ip: *const c_char,
        server_num: u32, total_num_servers: u32, servers_ready: &AtomicUsize) -> ! {
        let server_addr_str = unsafe {
            CStr::from_ptr(server_ip).to_str().expect("invalid IP string")
        };
        let ip_addr = server_addr_str.parse().expect("invalid IP addr");
        let acceptor = mio::tcp::TcpListener::bind(&ip_addr).expect("cannot start server");
        ::servers2::tcp::run(
            acceptor,
            server_num,
            total_num_servers,
            1,
            &servers_ready,
        )
    }

    ////////////////////////////////////
    //    Old fuzzy log C bindings    //
    ////////////////////////////////////

    type Log = ();

    #[no_mangle]
    pub extern "C" fn fuzzy_log_new(server_addr: *const c_char, relevent_chains: *const u32,
        num_relevent_chains: u16, callback: extern fn(*const u8, u16) -> u8) -> Box<Log> {
        unimplemented!()
    }

    #[no_mangle]
    pub extern "C" fn fuzzy_log_append(log: &mut Log,
        chain: u32, val: *const u8, len: u16, deps: *const OrderIndex, num_deps: u16) -> OrderIndex {
        unimplemented!()
    }

    #[no_mangle]
    pub extern "C" fn fuzzy_log_multiappend(log: &mut Log,
        chains: *mut OrderIndex, num_chains: u16,
        val: *const u8, len: u16, deps: *const OrderIndex, num_deps: u16) {
        unimplemented!()
    }

    #[no_mangle]
    pub extern "C" fn fuzzy_log_play_forward(log: &mut Log, chain: u32) -> OrderIndex {
        unimplemented!()
    }

}
