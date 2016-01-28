#![feature(ptr_as_ref)]
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
extern crate uuid;
extern crate mio;
extern crate time;

#[macro_use]
mod general_tests;

pub mod prelude;
pub mod local_store;
pub mod udp_store;

#[cfg(feature = "dynamodb_tests")]
pub mod dynamo_store;
