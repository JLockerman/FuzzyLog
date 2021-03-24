/*!

This crate contains a combined version of the fuzzy log client and server code.

*/
//TODO we are using an old version of mio, update
#![allow(deprecated)]

#[macro_use] extern crate log;

extern crate byteorder;
extern crate mio;
extern crate toml;
extern crate libc;
extern crate env_logger;

extern crate fuzzy_log_util;
pub extern crate fuzzy_log_packets;
pub extern crate fuzzy_log_server;
pub extern crate fuzzy_log_client;

pub use fuzzy_log_packets as packets;
pub use packets::storeables as storeables;

/// Libraries to assist in the creation of fuzzy log servers.
pub use fuzzy_log_server as servers2;

pub use packets::{order, entry, OrderIndex};

/// The fuzzy log client.
pub use fuzzy_log_client as async;
pub use async::fuzzy_log::log_handle::{LogHandle, LogBuilder};

#[cfg(test)] mod tests;
#[cfg(test)] mod replication_tests;

/// Start a fuzzy log TCP server.
///
/// This function takes over the current thread and never returns.
/// It will spawn at least two additional threads, one to perform ordering and
/// at least one worker.
///
/// # Args
///  * `addr` the address on which the server should accept connection.
///  * `server_num` the the number which this server is in it's server group
///                 must be in `0..group_size`.
///  * `group_size` the number of servers in this servers server-group,
///                 must be at least 1.
///  * `prev_server` the server which precedes this one in the replication chain,
///                  if it exists.
///  * `next_server` the server which comes after this one in the replication chain,
///                  if it exists.
///  * `num_worker_threads` the number of workers that should be spawned,
///                         must be at least 1.
///  * `started` an atomic counter which will be incremented once the server starts.
///
pub fn run_server(
    addr: std::net::SocketAddr,
    server_num: u32,
    group_size: u32,
    prev_server: Option<std::net::SocketAddr>,
    next_server: Option<std::net::IpAddr>,
    num_worker_threads: usize,
    started: &std::sync::atomic::AtomicUsize,
) -> ! {
    let acceptor = mio::tcp::TcpListener::bind(&addr)
        .expect("Bind error");
    servers2::tcp::run_with_replication(
        acceptor, server_num, group_size, prev_server, next_server, num_worker_threads, started
    )
}

pub fn start_server_thread(server_ip: &str) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let server_started = AtomicUsize::new(0);
    let started = unsafe {
        //This should be safe since the while loop at the of the function
        //prevents it from exiting until the server is started and
        //server_started is no longer used
        (extend_lifetime(&server_started))
    };

    let addr = server_ip.parse().unwrap();
    ::std::thread::spawn(move || {
        run_server(addr, 0, 1, None, None, 1, &started)
    });
    while !server_started.load(Ordering::SeqCst) < 1 {}

    unsafe fn extend_lifetime<'a, 'b, T>(r: &'a T) -> &'b T {
        ::std::mem::transmute(r)
    }
}

pub mod c_binidings {

    use packets::*;
    use async::fuzzy_log::log_handle::{
        HashMap,
        LogHandle,
        ReadHandle,
        WriteHandle,
        GetRes,
        TryWaitRes
    };

    //use std::collections::HashMap;
    use std::{mem, ptr, slice};

    use std::ffi::{CStr, CString};
    use std::net::SocketAddr;
    use std::os::raw::{c_char, c_void};

    use std::sync::atomic::{AtomicUsize, Ordering};

    use mio;

    pub type DAG = LogHandle<[u8]>;
    pub type ColorID = u64;

    #[repr(C)]
    pub struct ReaderAndWriter {
        reader: Box<ReadHandle<[u8]>>,
        writer: Box<WriteHandle<[u8]>>,
    }

    #[repr(C)]
    pub struct colors {
        numcolors: usize,
        mycolors: *mut ColorID,
    }

    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct WriteId {
        bytes: [u8; 16],
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
            let bytes = *id.as_bytes();
            WriteId { bytes, }
        }

        fn to_uuid(self) -> Uuid {
            let WriteId {bytes} = self;
            Uuid::from_bytes(&bytes[..]).unwrap()
        }

        #[allow(dead_code)]
        fn nil() -> Self {
            WriteId::from_uuid(Uuid::nil())
        }
    }


    /// A `ColorSpec` describes the layout of a color.
    ///
    /// members:
    ///   local_chain: the local chain for this color
    ///   num_remote_chains: The number of remote chains which make up the color
    ///   retmote_chains: an array of size `numchains` consisting of the chains for a
    ///           color.
    ///
    ///   NOTE `local_chain` _may_ be included in `remote_chains` as well,
    ///   though it is not necessary to do so.
    #[repr(C)]
    pub struct ColorSpec {
        local_chain: u64,
        num_remote_chains: usize,
        remote_chains: *mut u64,
    }

    impl ColorSpec {
        fn is_valid(&self) -> bool {
            self.num_remote_chains == 0 || !self.remote_chains.is_null()
        }
    }

    /// The specification for a static FuzzyLog server configuration.
    /// Since we're using chain-replication, in a replicated setup the client
    /// needs to know of both the head and tail of each replication chain. In an
    /// non-replicated setup tail_ips should be NULL. Each element of an ip
    /// array should be in the form `<ip-addr>:<port>`. Currently only ipv4 is
    /// supported.
    ///
    /// members:
    ///   num_ips: the number of IP addresses in each array
    ///   head_ips: an array of `"<ip-addr>:<port>"` describing the heads of the
    ///             FuzzyLog replication chain.
    ///   tail_ips: an array of `"<ip-addr>:<port>"` describing the heads of the
    ///             FuzzyLog replication chain, or `NULL` if the FuzzyLog is not
    ///             replicated/
    #[repr(C)]
    pub struct ServerSpec {
        num_ips: usize,
        head_ips: *mut *mut c_char,
        tail_ips: *mut *mut c_char,
    }

    impl ServerSpec {
        fn is_valid(&self) -> bool {
            self.num_ips != 0 && !self.head_ips.is_null()
        }
    }

    pub type SnapBody = HashMap<order, entry>;

    pub type SnapId = *mut SnapBody;
    pub type FLPtr = *mut DAG;

    /// Start a new FuzzyLog client instance, and connect it so the supplied
    /// server(s).
    ///
    /// args:
    ///   servers: a `ServerSpec` describing the servers to connect to.
    ///   color: a `ColorSpec` for the color this client reads.
    ///   snap: a SnapId that the client should start `sync`ing from,
    ///         or NULL if the client should start from the beginning of its
    ///         color
    #[no_mangle]
    pub unsafe extern "C" fn new_fuzzylog_instance(
        servers: ServerSpec, color: ColorSpec, snap: SnapId) -> FLPtr {
        assert!(servers.is_valid());
        assert!(color.is_valid());

        let _ = ::env_logger::init();

        let parse_ip = |&s: &*mut c_char| {
            let ip_string = CStr::from_ptr(s).to_str().expect("invalid IP string") ;
            let ip_parse = ip_string.parse();
            match ip_parse {
                Err(e) => panic!("invalid IP addr \"{}\": {}", ip_string, e),
                Ok(addr) => addr,

            }
        };

        let remote_chains =
            slice::from_raw_parts(color.remote_chains, color.num_remote_chains);
        let chains = remote_chains.iter()
            .map(|&c| order::from(c))
            .chain(if color.local_chain != 0 {
                Some(order::from(color.local_chain))
            } else {
                None
            });

        let ServerSpec{ num_ips, head_ips, tail_ips } = servers;
        let heads: &[*mut c_char] = slice::from_raw_parts(head_ips, num_ips);
        let heads = heads.into_iter().map(parse_ip);

        let builder = if tail_ips.is_null() {
            LogHandle::unreplicated_with_servers(heads)
        } else {
            let tails = slice::from_raw_parts(tail_ips, num_ips).into_iter().map(parse_ip);
            LogHandle::replicated_with_servers(heads.zip(tails))
        };

        let mut handle = builder
            .my_colors_chains(chains)
            .build();

        if !snap.is_null() {
            for (&o, &i) in &*snap {
                handle.fastforward((o, i).into())
            }
        }

        Box::into_raw(handle.into())
    }

    /// Append a node to the FuzzyLog
    ///
    /// args:
    ///   handle: the client handle which will perform the append
    ///
    ///   data: the data to be contained in the new node
    ///   data_size: the number of bytes in `data`
    ///
    ///   colors: the colors the new node should inhabit. Note that only
    ///           `local_color` will be read from these colors.
    ///   num_colors: the number of colors in `colors`
    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_append(
        handle: FLPtr,
        data: *const c_char,
        data_size: usize,
        colors: *const ColorSpec,
        num_colors: usize,
    ) -> i32 {
        assert!(
            !data.is_null() || data_size == 0,
            "args: data = {:?}, data_size = {} is not valid\nEither data = NULL or data_size > 0"
        );
        assert!(!colors.is_null());
        assert!((&*colors).is_valid());

        let handle = handle.as_mut().expect("need to provide a valid DAGHandle");
        let data = slice::from_raw_parts(data as *const u8, data_size);
        let colors = slice::from_raw_parts(colors, num_colors);

        let mut colors: Vec<_> = (&*colors).iter()
            .map(|c| order::from(c.local_chain))
            .collect();

        let id = handle.simpler_causal_append(data, &mut colors);
        handle.wait_for_a_specific_append(id).unwrap();
        1
    }

    /// Asynchronously append a node to the FuzzyLog.
    /// Unlike `fuzzylog_append` this function does not wait for the append to
    /// be ack'd by the server, but rather return immediately with a WriteId
    /// which can be used to wait for the ack at a latter point in time.
    ///
    /// args:
    ///   handle: the client handle which will perform the append
    ///
    ///   data: the data to be contained in the new node
    ///   data_size: the number of bytes in `data`
    ///
    ///   colors: the colors the new node should inhabit. Note that only
    ///           `local_color` will be read from these colors.
    ///   num_colors: the number of colors in `colors`
    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_async_append(
        handle: FLPtr,
        data: *const c_char,
        data_size: usize,
        colors: *const ColorSpec,
        num_colors: usize,
    ) -> WriteId {
        assert!(
            !data.is_null() || data_size == 0,
            "args: data = {:?}, data_size = {} is not valid\nEither data = NULL or data_size > 0"
        );
        assert!(!colors.is_null());
        assert!((&*colors).is_valid());

        let handle = handle.as_mut().expect("need to provide a valid DAGHandle");
        let data = slice::from_raw_parts(data as *const u8, data_size);
        let colors = slice::from_raw_parts(colors, num_colors);

        let mut colors: Vec<_> = (&*colors).iter()
            .map(|c| order::from(c.local_chain))
            .collect();

        let id = handle.simpler_causal_append(data, &mut colors);
        WriteId::from_uuid(id)
    }

    /// Sync a local view with the FuzzyLog.
    ///
    /// args:
    ///   handle: the client handle which will perform the sync
    ///   callback: a callback which will be called on every new event.
    ///   args are: the passed in `callback_state`, the event's `data`,
    ///             the events `data_size`
    ///   callback_state: a pointer passed as the first argument to callback.
    ///                   May be `NULL`.
    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_sync(
        handle: FLPtr,
        callback: fn(*mut c_void, *const c_char, usize) -> (),
        callback_state: *mut c_void,
    ) -> SnapId {
        let handle = handle.as_mut().expect("need to provide a valid DAGHandle");
        let entries_seen =
            handle.sync(|data, _, _| callback(
                callback_state,
                data.as_ptr() as *const i8,
                data.len()
            ));
        match entries_seen {
            Ok(entries) => Box::into_raw(entries.into()),
            Err(..) => ptr::null_mut(),
        }
    }

    #[repr(C)]
    pub struct FuzzyLogEvent {
        pub id: *const WriteId,
        pub data: *const c_char,
        pub data_size: usize,
        inhabits: *const c_void,
        inhabits_len: usize,
    }

    /// Sync a local view with the FuzzyLog.
    ///
    /// args:
    ///   handle: the client handle which will perform the sync
    ///   callback: a callback which will be called on every new event.
    ///   args are: the passed in `callback_state`, the event's `data`,
    ///             the events `data_size`
    ///   callback_state: a pointer passed as the first argument to callback.
    ///                   May be `NULL`.
    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_sync_events(
        handle: FLPtr,
        callback: fn(*mut c_void, FuzzyLogEvent) -> (),
        callback_state: *mut c_void,
    ) -> SnapId {
        let handle = handle.as_mut().expect("need to provide a valid DAGHandle");
        let entries_seen = handle.sync_events(|e| {
            let event = FuzzyLogEvent {
                id: e.id as *const Uuid as *const WriteId,
                data: e.data.as_ptr() as *const c_char,
                data_size: e.data.len(),
                inhabits: e.inhabits.as_ptr() as _,
                inhabits_len: e.inhabits.len(),
            };
            callback(callback_state, event);
        });
        match entries_seen {
            Ok(entries) => Box::into_raw(entries.into()),
            Err(..) => ptr::null_mut(),
        }
    }

    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_sync_chain(
        handle: FLPtr,
        chain: u64,
        callback: fn(*mut c_void, FuzzyLogEvent) -> (),
        callback_state: *mut c_void,
    ) -> SnapId {
        let handle = handle.as_mut().expect("need to provide a valid DAGHandle");
        let entries_seen = handle.sync_events_for_chain(chain.into(), |e| {
            let event = FuzzyLogEvent {
                id: e.id as *const Uuid as *const WriteId,
                data: e.data.as_ptr() as *const c_char,
                data_size: e.data.len(),
                inhabits: e.inhabits.as_ptr() as _,
                inhabits_len: e.inhabits.len(),
            };
            callback(callback_state, event);
        });
        match entries_seen {
            Ok(entries) => Box::into_raw(entries.into()),
            Err(..) => ptr::null_mut(),
        }
    }

    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_event_inhabits_chain(
        event: FuzzyLogEvent,
        chain: u64,
    ) -> bool {
        let inhabits = slice::from_raw_parts(
            event.inhabits as *const OrderIndex, event.inhabits_len
        );
        for OrderIndex(o, _) in inhabits {
            if *o == order::from(chain) { return true }
        }
        false
    }


    /// Wait for all outstanding appends to be ack'd by the server.
    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_wait_for_all_appends(handle: FLPtr) {
        wait_for_all_appends(handle)
    }

    /// Wait for a specific append sent by this client to be ack'd by the server.
    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_wait_for_a_specific_append(handle: FLPtr, write_id: WriteId) {
        wait_for_a_specific_append(handle, write_id)
    }

    /// Wait for any append sent by this client to be ack'd by the server.
    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_wait_for_any_append(handle: FLPtr) -> WriteId {
        wait_for_any_append(handle)
    }

    /// Check if any append written by this client has been ack'd by the server
    /// return WriteId{0} if no such append exists.
    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_try_wait_for_any_append(handle: FLPtr) -> WriteId {
        try_wait_for_any_append(handle)
    }

    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_trim(handle: FLPtr, snap: SnapId) {
        unimplemented!()
    }

    #[no_mangle]
    pub unsafe extern "C" fn fuzzylog_close(handle: FLPtr) {
        close_dag_handle(handle)
    }

    #[no_mangle]
    pub unsafe extern "C" fn delete_snap_id(snap: SnapId) {
        let _ = Box::from_raw(snap);
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
        assert!(*chain_server_ips != ptr::null());
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
        let colors = slice::from_raw_parts((*color).mycolors, (*color).numcolors);
        let colors: Vec<_> = colors.into_iter().cloned().map(order::from).collect();
        //Box::new(LogHandle::new_tcp_log(server_addrs.into_iter(), colors))
        let handle = LogHandle::unreplicated_with_servers(server_addrs)
            .chains(colors)
            .reads_my_writes()
            .build();
        Box::new(handle)
    }

    #[no_mangle]
    pub unsafe extern "C" fn new_dag_handle_with_replication(
        num_chain_ips: usize,
        chain_server_head_ips: *const *const c_char,
        chain_server_tail_ips: *const *const c_char,
        color: *const colors
    ) -> Box<DAG> {
        assert_eq!(mem::size_of::<Box<DAG>>(), mem::size_of::<*mut u8>());

        assert!(chain_server_head_ips != ptr::null());
        assert!(chain_server_tail_ips != ptr::null());
        assert!(*chain_server_head_ips != ptr::null());
        assert!(*chain_server_tail_ips != ptr::null());
        assert!(num_chain_ips >= 1);

        assert!(color != ptr::null());
        assert!(colors_valid(color));

        let _ = ::env_logger::init();
        trace!("Lib num chain servers {:?}", num_chain_ips);
        let server_heads = slice::from_raw_parts(chain_server_head_ips, num_chain_ips)
            .into_iter().map(|&s|
                CStr::from_ptr(s).to_str().expect("invalid IP string")
                    .parse().expect("invalid IP addr")
            );
        let server_tails = slice::from_raw_parts(chain_server_tail_ips, num_chain_ips)
            .into_iter().map(|&s|
                CStr::from_ptr(s).to_str().expect("invalid IP string")
                    .parse().expect("invalid IP addr")
            );
        let colors = slice::from_raw_parts((*color).mycolors, (*color).numcolors);
        Box::new(LogHandle::new_tcp_log_with_replication(
            server_heads.zip(server_tails),
            colors.into_iter().cloned().map(order::from)
        ))
    }

    #[no_mangle]
    pub extern "C" fn new_dag_handle_from_config(
        config_filename: *const c_char, color: *const colors
    ) -> Box<DAG> {
        assert_eq!(mem::size_of::<Box<DAG>>(), mem::size_of::<*mut u8>());
        assert!(color != ptr::null());
        assert!(colors_valid(color));
        let _ = ::env_logger::init();
        let (_, chain_server_addrs, chain_server_tails) = read_config_file(config_filename);
        let server_addrs = chain_server_addrs.into_iter()
            .map(|s| s.parse().expect("Invalid server addr"))
            .collect::<Vec<SocketAddr>>();
        let chain_server_tails = chain_server_tails.into_iter()
            .map(|s| s.parse().expect("Invalid server addr"))
            .collect::<Vec<SocketAddr>>();
        let colors = unsafe {slice::from_raw_parts((*color).mycolors, (*color).numcolors)};
        if chain_server_tails.is_empty() {
            Box::new(
                LogHandle::new_tcp_log(
                    server_addrs.into_iter(),
                    colors.into_iter().cloned().map(order::from)
            ))
        } else {
            assert_eq!(
                server_addrs.len(), chain_server_tails.len(),
                "Must have a tail server for each head server.");
            Box::new(
                LogHandle::new_tcp_log_with_replication(
                    server_addrs.into_iter().zip(chain_server_tails),
                    colors.into_iter().cloned().map(order::from)
            ))
        }
    }

    #[no_mangle]
    pub extern "C" fn split_dag_handle(dag: *mut DAG) -> ReaderAndWriter {
        assert!(dag != ptr::null_mut());
        let dag = unsafe { Box::from_raw(dag) };
        let (reader, writer) = dag.split();
        let reader = Box::new(reader);
        let writer = Box::new(writer);
        ReaderAndWriter { reader, writer }
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
        let (id, error) = dag.color_append(data, inhabits, depends_on, async != 0);
        match error {
            Ok(()) => {}
            //TODO set errno?
            Err(e) => panic!("IO error {:?}", e),
        }
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
    pub unsafe extern "C" fn wait_for_all_appends(handle: FLPtr) {
        let dag = handle.as_mut().expect("need to provide a valid DAGHandle");
        dag.wait_for_all_appends().unwrap();
    }

    #[no_mangle]
    pub unsafe extern "C" fn wait_for_a_specific_append(handle: FLPtr, write_id: WriteId) {
        let dag = handle.as_mut().expect("need to provide a valid DAGHandle");
        dag.wait_for_a_specific_append(write_id.to_uuid()).unwrap();
    }

    #[no_mangle]
    pub unsafe extern "C" fn wait_for_any_append(handle: FLPtr) -> WriteId {
        let dag = handle.as_mut().expect("need to provide a valid DAGHandle");
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
    pub unsafe extern "C" fn try_wait_for_any_append(handle: FLPtr) -> WriteId {
        let dag = handle.as_mut().expect("need to provide a valid DAGHandle");
        let id = dag.try_wait_for_any_append().map(|t| t.0).unwrap_or(Uuid::nil());
        WriteId::from_uuid(id)
    }

    #[no_mangle]
    pub unsafe extern "C" fn try_wait_for_any_append_and_location(dag: *mut DAG)
    -> WriteIdAndLocs {
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        match dag.try_wait_for_any_append() {
            Err(TryWaitRes::NothingReady) => WriteIdAndLocs {
                write_id: WriteId::nil(),
                locs: WriteLocations { num_locs: 0, locs: ptr::null_mut() },
            },
            //TODO what to do with error number?
            Err(TryWaitRes::IoErr(_kind, server)) => WriteIdAndLocs {
                write_id: WriteId::nil(),
                locs: WriteLocations { num_locs: server, locs: ptr::null_mut() },
            },
            Ok((id, locs)) => WriteIdAndLocs {
                write_id: WriteId::from_uuid(id),
                locs: build_write_locs(locs),
            },
        }

    }

    #[no_mangle]
    pub unsafe extern "C" fn flush_completed_appends(dag: *mut DAG) {
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        dag.flush_completed_appends().unwrap();
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
                Ok((data, inhabited_colors)) => {
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
                Err(GetRes::Done) => {
                    (ptr::null_mut(), 0)
                }
                _ => unimplemented!(),
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
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        let val = dag.get_next();
        let (data, locs) = val.unwrap_or((&[], &[]));

        ptr::write(data_read, data.len());
        ptr::write(num_locs, locs.len());

        Vals { data: data.as_ptr(), locs: locs.as_ptr() }
    }

    #[no_mangle]
    pub unsafe extern "C" fn async_get_next2(
        dag: *mut DAG,
        data_read: *mut usize,
        num_locs: *mut usize,
    ) -> Vals {
        assert!(data_read != ptr::null_mut());
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        let val = dag.try_get_next();
        let (data, locs): (&[u8], &[OrderIndex]) = match val {
            Ok((data, locs)) => (data, locs),
            Err(GetRes::NothingReady) => (&[], &[]),
            Err(GetRes::Done) => {
                ptr::write(data_read, 0);
                ptr::write(num_locs, 0);
                return Vals { data: ptr::null(), locs: ptr::null() }
            },
            _ => unimplemented!(),
        };

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


    ///////////////////////////////////////////////////
    //         Read and Write Handle bindings        //
    ///////////////////////////////////////////////////

    #[no_mangle]
    pub extern "C" fn wh_async_append(
        dag: *mut WriteHandle<[u8]>,
        data: *const u8,
        data_size: usize,
        inhabits: ColorID,
        // deps: *mut OrderIndex,
        // num_deps: usize,
    ) -> WriteId {
        assert!(data_size == 0 || data != ptr::null());
        assert!(data != ptr::null());
        let (dag, data, deps) = unsafe {
            // let d = slice::from_raw_parts_mut(deps, num_deps);
            (dag.as_mut().expect("need to provide a valid DAGHandle"),
                slice::from_raw_parts(data, data_size),
                &mut [])
        };
        let id = dag.async_append(inhabits.into(), data, deps);
        WriteId::from_uuid(id)
    }

    #[no_mangle]
    pub extern "C" fn wh_async_multiappend(
        dag: *mut WriteHandle<[u8]>,
        data: *const u8,
        data_size: usize,
        inhabits: *mut colors,
        // deps: *mut OrderIndex,
        // num_deps: usize,
    ) -> WriteId {
        assert!(data_size == 0 || data != ptr::null());
        assert!(inhabits != ptr::null_mut());
        assert!(colors_valid(inhabits));
        assert!(data_size <= 8000);

        let (dag, data, inhabits, deps) = unsafe {
            let colors: *mut order = (*inhabits).mycolors as *mut _;
            let s = slice::from_raw_parts_mut(colors, (*inhabits).numcolors);
            // let d = slice::from_raw_parts_mut(deps, num_deps);
            (dag.as_mut().expect("need to provide a valid DAGHandle"),
                slice::from_raw_parts(data, data_size),
                s,
                &mut [])
        };
        inhabits.sort();
        assert!(
            inhabits.binary_search(&order::from(0)).is_err(),
            "color 0 should not be used;it is special cased for legacy reasons."
        );
        let id = dag.async_multiappend(inhabits, data, deps);
        WriteId::from_uuid(id)
    }

    #[no_mangle]
    pub extern "C" fn wh_async_no_remote_multiappend(
        dag: *mut WriteHandle<[u8]>,
        data: *const u8,
        data_size: usize,
        inhabits: *mut colors,
        // deps: *mut OrderIndex,
        // num_deps: usize,
    ) -> WriteId {
        assert!(data_size == 0 || data != ptr::null());
        assert!(inhabits != ptr::null_mut());
        assert!(colors_valid(inhabits));
        assert!(data_size <= 8000);

        let (dag, data, inhabits, deps) = unsafe {
            let colors: *mut order = (*inhabits).mycolors as *mut _;
            let s = slice::from_raw_parts_mut(colors, (*inhabits).numcolors);
            // let d = slice::from_raw_parts_mut(deps, num_deps);
            (dag.as_mut().expect("need to provide a valid DAGHandle"),
                slice::from_raw_parts(data, data_size),
                s,
                &mut [])
        };
        inhabits.sort();
        assert!(
            inhabits.binary_search(&order::from(0)).is_err(),
            "color 0 should not be used;it is special cased for legacy reasons."
        );
        let id = dag.async_no_remote_multiappend(inhabits, data, deps);
        WriteId::from_uuid(id)
    }

    #[no_mangle]
    pub unsafe extern "C" fn wh_flush_completed_appends(dag: *mut WriteHandle<[u8]>) {
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        dag.flush_completed_appends().unwrap();
    }

    #[no_mangle]
    pub unsafe extern "C" fn wh_wait_for_any_append(dag: *mut WriteHandle<[u8]>) -> WriteId {
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        let id = dag.wait_for_any_append().map(|t| t.0).unwrap_or(Uuid::nil());
        WriteId::from_uuid(id)
    }

    #[no_mangle]
    pub extern "C" fn rh_snapshot(dag: *mut ReadHandle<[u8]>) {
        let dag = unsafe {dag.as_mut().expect("need to provide a valid DAGHandle")};
        dag.take_snapshot();
    }

    #[no_mangle]
    pub unsafe extern "C" fn rh_snapshot_colors(dag: *mut ReadHandle<[u8]>, colors: *mut colors) {
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
    pub unsafe extern "C" fn rh_get_next2(
        dag: *mut ReadHandle<[u8]>,
        data_read: *mut usize,
        num_locs: *mut usize,
    ) -> Vals {
        assert!(data_read != ptr::null_mut());
        let dag = dag.as_mut().expect("need to provide a valid DAGHandle");
        let val = dag.get_next();
        let (data, locs) = val.unwrap_or((&[], &[]));

        ptr::write(data_read, data.len());
        ptr::write(num_locs, locs.len());

        Vals { data: data.as_ptr(), locs: locs.as_ptr() }
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
        assert!(server_ip != ptr::null());
        let server_ip = unsafe {
            CStr::from_ptr(server_ip).to_str().expect("invalid IP string")
        };
        start_server(server_ip, server_number,
            total_servers_in_group, &AtomicUsize::new(0));
    }

    #[no_mangle]
    pub extern "C" fn start_fuzzy_log_server_thread_from_group(server_ip: *const c_char,
        server_number: u32, total_servers_in_group: u32) {
            assert!(server_ip != ptr::null());
            let server_started = AtomicUsize::new(0);
            let started = unsafe {
                //This should be safe since the while loop at the of the function
                //prevents it from exiting until the server is started and
                //server_started is no longer used
                (extend_lifetime(&server_started))
            };
            let server_ip = unsafe {
                CStr::from_ptr(server_ip).to_str().expect("invalid IP string")
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

    fn start_server(server_ip: &str,
        server_num: u32, total_num_servers: u32, servers_ready: &AtomicUsize) -> ! {
        let ip_addr = server_ip.parse().expect("invalid IP addr");
        let acceptor = mio::tcp::TcpListener::bind(&ip_addr).expect("cannot start server");
        ::servers2::tcp::run(
            acceptor,
            server_num,
            total_num_servers,
            1,
            &servers_ready,
        )
    }

    #[no_mangle]
    pub extern "C" fn start_servers_from_config(file_name: *const c_char) {
        assert!(file_name != ptr::null());
        let (lock_server_addr, chain_server_addrs, _) = read_config_file(file_name);
        if let Some(addr) = lock_server_addr {
            let addr = CString::new(addr).unwrap();
            start_fuzzy_log_server_thread_from_group(addr.into_raw(), 0, 1);
        }
        let total_chain_servers = chain_server_addrs.len() as u32;
        for (i, addr) in chain_server_addrs.into_iter().enumerate() {
            let addr = CString::new(addr).unwrap();
            start_fuzzy_log_server_thread_from_group(addr.into_raw(), i as u32, total_chain_servers);
        }
    }

    ////////////////////////////////////
    //           Config I/O           //
    ////////////////////////////////////

    fn read_config_file(file_name: *const c_char)
    -> (Option<String>, Vec<String>, Vec<String>) {
        use std::fs::File;
        use std::io::Read;
        use toml::{self, Value};

        let file_name = unsafe { CStr::from_ptr(file_name) }
            .to_str().expect("Can only hanlde utf-8 filenames.");
        let mut config_string = String::new();
        {
            let mut config = File::open(file_name)
                .expect("Could not open config file.");
            config.read_to_string(&mut config_string)
                .expect("Invalid config file encoding.");
        }
        let mut vals = toml::Parser::new(&config_string)
            .parse().expect("Invalid config file format.");
        let lock_server_str =
            if let Some(Value::String(s)) = vals.remove("DELOS_LOCK_SERVER") {
                Some(s)
            } else { None };
        let css = vals.remove("DELOS_CHAIN_SERVERS")
            .expect("Must provide at least one chain server addr.");
        let chain_server_strings = if let Value::String(s) = css {
                s.split_whitespace().map(|s| s.to_string()).collect()
            } else { panic!("Must provide at least one chain server addr.") };
        let csst = match vals.remove("DELOS_CHAIN_SERVERS_TAILS") {
            Some(Value::String(s)) => s.split_whitespace().map(|s| s.to_string()).collect(),
            _ => vec![],
        };
        (lock_server_str, chain_server_strings, csst)
    }
}
