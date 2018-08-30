# Rust Hello World Example

This directory contains a simple example of running the FuzzyLog client from rust.  
For the C equivalent see [examples/hello_c](../hello_c).

## Building the Example

Make sure you have `rust` intalled (run `curl https://sh.rustup.rs -sSf | sh` if not)
and `cargo run`.  

This will run the example. It starts up its own instance of the FuzzyLog server
at `127.0.0.1:13229` to run against, starts a client, performs some appends and
reads them back.
