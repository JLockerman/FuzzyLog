# TCP Server
CLI bindings to the Fuzzy Log TCP server.  
The Fuzzy Log shards its partial order into an number of
total orders wich are distributed across the Fuzzy Log servers
in a server-group. These servers decided amongst themselves
where in the partial order new entries are placed.

To run use

    cargo run --release -- <server port> [-ig | --in-group <server num>:<num servers in group>] [--workers <num worker threads>]

it will accept requests for any IP address at that port.  
By deafult the server will start up a number of worker threads
such that the server is running 1 thread per core.
