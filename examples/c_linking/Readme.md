# C Linking Example
This directory contains a header file [`fuzzy_log.h`](fuzzy_log.h)
which can be used to link C programs to the Fuzzy Log client API,
along with an example of such a program, and a Makefile which
hopefully makes such linking simple.  

## Building the Example

Make sure you have `rust` intalled
(run `curl https://sh.rustup.rs -sSf | sh` if not)
and `make`.

**MAC OS Note**: Due to differences in linking on MAC OS and linux
the Makfile provides a seperate set of linker flags for MAC OS.
If you wish to run this example on mac you must first replace
`LINK_FLAGS` with `MAC_LINK_FLAGS` in the Makefile.

Run `make` this will ouput the example binary in `./out/start`.  
This binary is standalone, it starts up its own instance of the
Fuzzy Log server at `127.0.0.1:13229` to run against,
starts a client, performs some appends and reads them back.

## Starting you own project

To create your own fuzzy log project, use the command

    make new_dir DIR_NAME=<your directory name>

which will copy the relevent files to <your directory name>.
Then set the enviroment variable `DELOS_RUST_LOC` to the location
of your local copy of the `delos-rust` repo.
(In the examples folder a simple copy-paste will suffice).

**MAC OS Note**: Due to differences in linking on MAC OS and linux
the Makfile provides a seperate set of linker flags for MAC OS.
If you wish to run your code on mac you must first replace
`LINK_FLAGS` with `MAC_LINK_FLAGS` in the Makefile.

An example of a project wich was created using this method can be
found in [examples/c_multi_server](examples/c_multi_server).

See [`fuzzy_log.h`](fuzzy_log.h) for the fuzzy log functions currently available.
