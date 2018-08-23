# TCP Server
CLI bindings to the Fuzzy Log TCP server.  
The Fuzzy Log shards its partial order into an number of
total orders wich are distributed across the Fuzzy Log servers
in a server-group. These servers decided amongst themselves
where in the partial order new entries are placed.

To run use

    cargo run --release -- <server port> [-ig | --in-group <server num>:<num servers in group>] [--workers <num worker threads>] [-up | --upstream <ip addr>:<port>] [-dwn | --downstream <ip addr>]

it will accept requests for any IP address on port `<server port>`.  
By default the server will start up a number of worker threads
such that the server is running 1 thread per core.  
Fuzzy Log servers use chain replication for reliability. Each server
in a replication chain needs to know of the previous and next servers
in its chain, if they exist. The servers connect to each other from
the tail of the replication chain to the head,
so the downstream servers need to know the IP and port of their predecessor
while the upstreams merely need to know which IP to expect a connection from.  

For instance to run one, unreplicated, solo, server on port `8192` use

    cargo run --release -- 8192
    
to run 3 servers in in a group on ports `8193`, `8194`, and `3333` respectively use

    cargo run --release -- 8193 -ig 0:3
    cargo run --release -- 8194 -ig 1:3
    cargo run --release -- 3333 -ig 2:3
    
to run a server on port `5432` with only 1 worker thread (the minimum)

    cargo run --release -- 5432 -w 1

to run a replication chain consisting of servers `127.0.0.2:3334`, `127.0.0.3:3335`, `127.0.0.4:3336`


    cargo run --release -- 3334 -dwn 127.0.0.3
    cargo run --release -- 3335 -up 127.0.0.3:3334, -dwn 127.0.0.4
    cargo run --release -- 3336 -up 127.0.0.3:3335
    
all of these flags can be combined as needed.
