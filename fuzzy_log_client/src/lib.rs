#![allow(deprecated)] //TODO we're using an old mio, need to move queues in tree and update

pub extern crate fuzzy_log_packets as packets;
#[macro_use] extern crate fuzzy_log_util;

#[macro_use] extern crate log;
pub extern crate mio;
extern crate reactor;

pub use fuzzy_log_util::hash;

pub mod fuzzy_log;
pub mod colors;
pub mod store;
pub mod replicator;
