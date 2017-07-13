# C Linking Example
This directory contains a header file which can be used to link
C programs to the Fuzzy Log client API along with an example
of such a program, and a Makefile which hopefully makes such
linking simple.  

## Building the Example

Make sure you have `rust` intalled
(run `curl https://sh.rustup.rs -sSf | sh` if not)
and `make`.

_MAC OS Note_: due to differences in linking on MAC OS from linux
the Makfile provides a seperate set of linker flags for MAC OS.
If you wish to run this example on mac you must first replace
`LINK_FLAGS` with `MAC_LINK_FLAGS` in the Makefile.

Run `make` this will ouput the example binary in `./out/start`.  
This binary is standalone, it starts up its own instance of the
Fuzzy Log server to run against, and can be run directly.

## Starting you own project

If you wish to create your own project which uses the fuzzy log
outside of the examples directory, the easiest thing to do is
use the command

    make new_dir DIR_NAME=<your directory name>

which will copy the relevent files to <your directory name>.
Then set the enviroment variable `DELOS_RUST_LOC` to the location
of your local copy of the delos-rust repo.
(In the examples folder a simple copy-paste will suffice).

See `fuzzy_log.h` for the fuzzy log functions currently available.

The Makefile currently creates a a binary `out/start` which, when
run will start a fuzzy log server at `127.0.0.1:13229` run some appends
against it, and, if successful, exit.
