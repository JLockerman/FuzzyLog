#![allow(deprecated)] //TODO we're using an old mio, need to move queues in tree and update

extern crate fuzzy_log_packets as packets;
#[macro_use] extern crate fuzzy_log_util;

#[macro_use] extern crate log;
extern crate mio;

use fuzzy_log_util::hash;

pub mod fuzzy_log;
pub mod colors;
pub mod store;