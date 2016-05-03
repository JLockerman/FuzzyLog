#![cfg_attr(test, feature(test))]

#[macro_use] extern crate bitflags;
#[macro_use] extern crate custom_derive;
#[cfg(test)] #[macro_use] extern crate grabbag_macros;
#[macro_use] extern crate log;
#[macro_use] extern crate newtype_derive;

#[cfg(feature = "dynamodb_tests")]
extern crate hyper;
#[cfg(feature = "dynamodb_tests")]
extern crate rusoto;

#[cfg(test)]
extern crate test;

extern crate rustc_serialize;
extern crate mio;
extern crate nix;
extern crate net2;
extern crate time;
extern crate uuid;

#[macro_use]
mod general_tests;

pub mod prelude;
pub mod local_store;
pub mod udp_store;
pub mod tcp_store;

#[cfg(feature = "dynamodb_tests")]
pub mod dynamo_store;

pub mod c_binidings {

    use prelude::*;
    use local_store::LocalHorizon;
    use udp_store::UdpStore;

    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::collections::HashMap;
    use std::slice;

    pub type Log = FuzzyLog<[u8], UdpStore<[u8]>, LocalHorizon>;

    #[no_mangle]
    pub extern "C" fn fuzzy_log_new(server_ip_addr: u32, server_port: u16, relevent_chains: *const u32,
        num_relevent_chains: u16, callback: extern fn(*const u8, u16) -> u8) -> Box<Log> {
        let mut callbacks = HashMap::new();
        let relevent_chains = unsafe { slice::from_raw_parts(relevent_chains, num_relevent_chains as usize) };
        for &chain in relevent_chains {
            let callback: Box<Fn(&[u8]) -> bool> = Box::new(move |val| { callback(&val[0], val.len() as u16) != 0 });
            callbacks.insert(chain.into(), callback);
        }
        let ip_addr: Ipv4Addr = server_ip_addr.into();
        let server_addr = SocketAddr::new(IpAddr::V4(ip_addr), server_port);
        let log = FuzzyLog::new(UdpStore::new(server_addr), LocalHorizon::new(), callbacks);
        Box::new(log)
    }

    #[no_mangle]
    pub extern "C" fn fuzzy_log_append(log: &mut Log,
        chain: u32, val: *const u8, len: u16, deps: *const OrderIndex, num_deps: u16) -> OrderIndex {
        unsafe {
            let val = slice::from_raw_parts(val, len as usize);
            let deps = slice::from_raw_parts(deps, num_deps as usize);
            log.append(chain.into(), val, deps)
        }
    }

    #[no_mangle]
    pub extern "C" fn fuzzy_log_multiappend(log: &mut Log,
        chains: *const order, num_chains: u16,
        val: *const u8, len: u16, deps: *const OrderIndex, num_deps: u16) {
        assert!(num_chains > 1);
        unsafe {
            let val = slice::from_raw_parts(val, len as usize);
            let deps = slice::from_raw_parts(deps, num_deps as usize);
            let chains = slice::from_raw_parts(chains, num_chains as usize);
            log.multiappend(chains, val, deps);
        }
    }

    #[no_mangle]
    pub extern "C" fn fuzzy_log_play_forward(log: &mut Log, chain: u32) -> OrderIndex {
        if let Some(oi) = log.play_foward(order::from(chain)) {
            oi
        }
        else {
            (0.into(), 0.into())
        }
    }

}
