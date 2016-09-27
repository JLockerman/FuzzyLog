# delos-rust
##To Build
Download this repository.
Download [Rust](https://www.rust-lang.org).
To run local tests

    cargo test --release

**NOTE:** The first build will be much slower than subsequent ones
as it needs to download dependencies.

##Directory Outline
`src` fuzzy log client library  
`examples` sample code which uses the client library to perform vaious tasks of note is  
`examples/c_linking` shows how to use the C API to interface with the fuzzy log client library  
`servers` various servers which the client library can run against including  
`servers/tcp_server` a TCP based sever  
`clients` varous DPDK based clients for use in testing  

[![Build Status](https://travis-ci.com/ProjectDelos/delos-rust.svg?token=RaEyYb9eyzdWqhSpjYxi&branch=mahesh_look_at_this)](https://travis-ci.com/ProjectDelos/delos-rust)
