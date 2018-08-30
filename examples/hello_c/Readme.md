# C Hello World Example

This directory contains an example of running the FuzzyLog client from C, along
with a Makefile which hopefully makes building such projects simple. 

## Building the Example

Make sure you have `rust` intalled (run `curl https://sh.rustup.rs -sSf | sh` if not)
and `make`.

Running `make` this will output the example binary in `./out/start`.  
This binary is standalone. It starts up its own instance of the
FuzzyLog server at `127.0.0.1:13229` to run against,
starts a client, performs some appends and reads them back.

## Starting you own project

To create your own FuzzyLog project, use the command

    make new_dir DIR_NAME=<your directory name>

which will copy the relevant files to <your directory name>.
Then set the environment variable `FUZZYLOG_SRC_LOC` to the location of your
local copy of the FuzzyLog source code.
(In the examples folder a simple copy-paste will suffice).

See [`fuzzylog.h`](../../fuzzylog.h) for the FuzzyLog client C header.
