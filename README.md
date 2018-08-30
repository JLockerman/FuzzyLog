_This work is funded by [NSF grant 1637385](https://nsf.gov/awardsearch/showAward?AWD_ID=1637385)_. Any opinions, findings, and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the National Science Foundation.


# FuzzyLog

This repository contains the unified code for the clients and servers for the FuzzyLog project;
an experiment in partially ordered SMR.
A [companion repository](https://github.com/JLockerman/delos-apps) contains the remaining benchmark code.

## C Bindings

C bindings and documentation are currently located in [`fuzzy_log.h`](fuzzy_log.h).  

## To Build
Download and install [rust](https://www.rust-lang.org) (easiest way is `curl https://sh.rustup.rs -sSf | sh`).  
Clone this repository.  
To run local tests

    cargo test --release

**NOTE:** The first build will be much slower than subsequent ones
as it needs to download dependencies.

## Servers

A CLI binding for starting FuzzyLog servers can be found in [servers/tcp_server](servers/tcp_server).

## Directory Outline
[src](src) fuzzy log library.  
[examples](examples) sample code which uses the client library to perform vaious tasks, of note are:  
[examples/hello_c](examples/hello_c) a C helloworld example.
[servers/tcp_server](servers/tcp_server) a TCP based fuzzy log sever.  
[clients](clients) various DPDK based clients for use in testing (obsolescent).  
[fuzzy_log_server](fuzzy_log_server) the fuzzy log server implementation library.  
[fuzzy_log_client](fuzzy_log_client) the fuzzy log client implementation library.  
