#C Linking Example
This directory contains an incomplete example of linking to the
C API of the fuzzy log client library (which is also incomplete).
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
