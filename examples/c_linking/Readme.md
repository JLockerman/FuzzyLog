#C Linking Example
This directory contains an incomplete example of linking to the
C API of the fuzzy log client library (which is also incomplete).
If you wish to create your own project which uses the fuzzy log
outside of the examples directory, the easiests thing to do is
use the command

    make new_dir DIR_NAME=<your directory name>

which will copy the relevent files to <your directory name>
with the relevent links updated. (in the examples folder a simple
copy-paste will suffice).

See `fuzzy_log.h` for the fuzzy log functions currently available.

The Makefile currently creates a a binary `out/start` which, when
run will attempt to connect to a fuzzy log server at `127.0.0.1:13229`
and, if successful, exit.
