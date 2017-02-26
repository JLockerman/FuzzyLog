
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use std::sync::atomic::AtomicUsize;

use mio;

use fuzzy_log::servers2;

pub fn run(
    port: u16,
    workers: usize,
    upstream: Option<SocketAddr>,
    downstream: Option<IpAddr>,
    group: Option<(u32, u32)>,
) -> ! {
    let a = AtomicUsize::new(0);
    let (server_num, group_size) = group.unwrap_or((0, 1));
    let replicated = upstream.is_some() || downstream.is_some();
    let ip_addr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
    let addr = SocketAddr::new(ip_addr, port);
    let acceptor = mio::tcp::TcpListener::bind(&addr);
    match acceptor {
        Ok(accept) => if replicated {
            servers2::tcp::run_with_replication(accept, server_num, group_size,
                upstream, downstream, workers, &a)
        } else {
            servers2::tcp::run(accept, server_num, group_size, workers, &a)
        },
        Err(e) => panic!("Could not start server due to {}.", e),
    }
}
