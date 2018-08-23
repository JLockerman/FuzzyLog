
pub extern crate bincode;
extern crate mio;
extern crate reactor;
extern crate structopt;
pub extern crate serde;
pub extern crate zookeeper;

#[macro_use] extern crate serde_derive;
#[macro_use] extern crate structopt_derive;


use std::net::SocketAddr;
use std::str::FromStr;

use structopt::StructOpt;

use view::View;

pub mod msg;
pub mod view;

#[derive(StructOpt, Debug)]
#[structopt(name = "zk_test", about = "")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: ServerAddrs,
    my_port: u16,
    client_num: usize,
    window: usize,
}

fn main() {
    let args: Args = StructOpt::from_args();

    zookeeper::message::set_client_id(args.client_num as u32);

    let my_root = "/foo".to_owned();

    let roots = Some(("/foo".to_owned(), 3.into())).into_iter().collect();

    let view = View::new(args.my_port, args.servers, 3.into(), my_root, roots);
    view.unwrap().run()
}


#[derive(Debug, Clone)]
pub struct ServerAddrs(Vec<(SocketAddr, SocketAddr)>);

impl FromStr for ServerAddrs {
    type Err = std::string::ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        println!("{}", s);
        Ok(ServerAddrs(
            s.split('^').map(|t|{
                let mut addrs = t.split('#').map(|s| {
                    match SocketAddr::from_str(s) {
                        Ok(addr) => addr,
                        Err(e) => panic!("head parse err {} @ {}", e, s),
                    }
                });
                let head = addrs.next().expect("no head");
                let tail = if let Some(addr) = addrs.next() {
                    addr
                } else {
                    head
                };
                assert!(addrs.next().is_none());
                (head, tail)
            }).collect()
        ))
    }
}

impl From<Vec<(SocketAddr, SocketAddr)>> for ServerAddrs {
    fn from(v: Vec<(SocketAddr, SocketAddr)>) -> Self {
        ServerAddrs(v)
    }
}

#[cfg(test)]
mod tests {
    extern crate fuzzy_log_server;

    use super::*;
    use self::fuzzy_log_server::tcp::run_server;
    use msg::{Request, Response};
    use view::{CreateMode, Stat};

    use std::net::{TcpStream, SocketAddr};

    use std::sync::mpsc::channel;
    use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
    use std::thread;

    use bincode::{serialize_into, deserialize_from, Infinite};



    #[test]
    fn name() {
        static STARTED: AtomicUsize = ATOMIC_USIZE_INIT;

        thread::spawn(|| {
            run_server(
                "0.0.0.0:14005".parse().unwrap(),
                0,
                1,
                None,
                None,
                2,
                &STARTED,
            )
        });

        while STARTED.load(Ordering::Relaxed) == 0 {
            ::std::thread::yield_now()
        }

        let (starting_view, view_started) = channel();
        thread::spawn(move || {
            zookeeper::message::set_client_id(13);
            let addr = "127.0.0.1:14005".parse().unwrap();
            let servers = vec![(addr, addr)];
            let color = 3.into();
            let my_root = "/foo".to_owned();
            let roots = Some(("/foo".to_owned(), color)).into_iter().collect();
            let view = View::new(13333, servers.into(), color, my_root, roots)
                .unwrap();
            starting_view.send(()).unwrap();
            view.run();
        });
        view_started.recv().unwrap();

        let addr: SocketAddr = "127.0.0.1:13333".parse().unwrap();
        let mut stream = TcpStream::connect(addr).unwrap();

        let value = Request::Create{
            id: 1,
            path: "/foo/1".into(),
            data: vec![0,1,0],
            create_mode: CreateMode::persistent(),
        };
        serialize_into(&mut stream, &value, Infinite).unwrap();
        let response: Response = deserialize_from(&mut stream, Infinite).unwrap();
        assert_eq!(response, Response::Ok(1, "/foo/1".to_owned(), vec![], Stat::new(0), vec![]));

        let value = Request::GetData{
            0: 2,
            1: "/foo/1".into(),
        };
        serialize_into(&mut stream, &value, Infinite).unwrap();
        let response: Response = deserialize_from(&mut stream, Infinite).unwrap();
        assert_eq!(
            response,
            Response::Ok(2, "/foo/1".to_owned(), vec![0, 1, 0], Stat::new(0), vec![])
        );

        let value = Request::SetData{
            id: 3,
            path: "/foo/1".into(),
            data: vec![2,2,1],
            version: -1,
        };
        serialize_into(&mut stream, &value, Infinite).unwrap();
        let _response: Response = deserialize_from(&mut stream, Infinite).unwrap();
        // assert_eq!(
        //     response,
        //     Response::Ok(3, "/foo/1".to_owned(), vec![2, 2, 1], Stat::new(0), vec![])
        // );

        let value = Request::GetData{
            0: 2,
            1: "/foo/1".into(),
        };
        serialize_into(&mut stream, &value, Infinite).unwrap();
        let response: Response = deserialize_from(&mut stream, Infinite).unwrap();
        assert_eq!(
            response,
            Response::Ok(2, "/foo/1".to_owned(), vec![2, 2, 1], Stat::new(0), vec![])
        );
    }
}

