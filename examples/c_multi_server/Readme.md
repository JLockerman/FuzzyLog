# C Multi-Server Example

This example demonstrates the output from the `new_dir`
command from the [c_linking Makefile](../c_linking/Makefile),
as well as starting clients via a config file.

## `new_dir` Output.

The `new_dir` command copies `.gitignore`, `Makefile`,
`Readme.md`, and `start.c` to a new directory.
The Makefile is setup so that it will work from any location
as long as `DELOS_RUST_LOC` is set to a delos-rust directory.
It comes preconfigured to link in the needed system libraries
and the delos-rust library.

## Config File

FuzzyLog servers are replicated using chain replication for
reliability.
The function `new_dag_handle_from_config` can start a dag handle
for a server-group from a config file with the format

    DELOS_CHAIN_SERVERS="<ipaddr>:<port>+"
    DELOS_CHAIN_SERVERS_TAILS="<ipaddr>:<port>+"

where `DELOS_CHAIN_SERVERS` contains the `<ipaddr>:<port>`s of the
heads of the replication chains, and `DELOS_CHAIN_SERVERS_TAILS` are
the ones for the tails of the replication chain.
If for some reason you wish to run without replcation,
`DELOS_CHAIN_SERVERS_TAILS` can be left out.
