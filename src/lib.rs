
#[macro_use] extern crate bitflags;
#[macro_use] extern crate custom_derive;
#[macro_use] extern crate grabbag_macros;
#[macro_use] extern crate log;
#[macro_use] extern crate newtype_derive;

extern crate rustc_serialize;
extern crate uuid;

#[macro_use]
mod general_tests;

pub mod prelude;
pub mod local_store;
