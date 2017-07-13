# TCP Server
CLI bindings to the Fuzzy Log TCP server.  
The Fuzzy Log shards its partial order into an number of
total orders wich are distributed across the Fuzzy Log servers
in a server-group. These servers decided amongst themselves
where in the partial order new entries are placed.

To run use

    cargo run --release -- <server port> [-ig | --in-group <server num>:<num servers in group>] [--workers <num worker threads>]

it will accept requests for any IP address at that port.  
By default the server will start up a number of worker threads
such that the server is running 1 thread per core.  

For instance to run one, solo, server on port `8192` use

    cargo run --release -- 8192
    
to run 3 servers in in a group on ports `8193`, `8194`, and `3333` respectively use

    cargo run --release -- 8193 -ig 0:3
    cargo run --release -- 8194 -ig 1:3
    cargo run --release -- 3333 -ig 2:3
    
to run a server on port `5432` with only 1 worker thread (the minimum)

    cargo run --release -- 5432 -w 1
