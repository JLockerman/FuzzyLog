_This work is funded by [NSF grant 1637385](https://nsf.gov/awardsearch/showAward?AWD_ID=1637385)_. Any opinions, findings, and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the National Science Foundation.


# delos-rust

[![Build Status](https://travis-ci.com/ProjectDelos/delos-rust.svg?token=RaEyYb9eyzdWqhSpjYxi&branch=mahesh_look_at_this)](https://travis-ci.com/ProjectDelos/delos-rust)

This repository contains the unified code for the clients and servers for the FuzzyLog project;
an experiment in partially ordered SMR.

## To Build
Download and install [rust](https://www.rust-lang.org) (easiest way is `curl https://sh.rustup.rs -sSf | sh`).  
Clone this repository.  
To run local tests

    cargo test --release

**NOTE:** The first build will be much slower than subsequent ones
as it needs to download dependencies.

## Servers

A CLI binding for starting FuzzyLog servers can be found in [servers/tcp_server](servers/tcp_server).

## C Bindings

C bindings and documentation are currently located in [`fuzzy_log.h`](examples/c_linking/fuzzy_log.h)
in [examples/c_linking](examples/c_linking).  
Examples and build instructions for C applications can be found that directory.

## Directory Outline
[src](src) FuzzyLog library  
[examples](examples) sample code which uses the client library to perform vaious tasks of note is  
[examples/c_linking](examples/c_linking) shows how to use the C API to interface with the FuzzyLog client library  
[servers/tcp_server](servers/tcp_server) a TCP based FuzzyLog sever  
[clients](clients) varous DPDK based clients for use in testing (obsolescent)  
