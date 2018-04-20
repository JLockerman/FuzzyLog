
extern crate structopt;
extern crate zk_view;

pub use zk_view::{bincode, serde, zookeeper, ServerAddrs};

#[macro_use] extern crate structopt_derive;

use std::io::{self, Read, Write, BufReader, BufWriter};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "zk_traffic", about = "")]
#[allow(non_camel_case_types)]
enum Args {
    write{
        servers: ServerAddrs,
    },
    read{
        client_num: usize,
        view_addr: SocketAddr,
        num_threads: usize,
        window: usize,
    },
}

fn main() {
    let args = Args::from_args();
    match args {
        Args::write{servers} => mutate(servers),
        Args::read{client_num, view_addr, num_threads, window} =>
            (0..num_threads)
                .map(|i| thread::spawn(move || work(client_num, i, view_addr, window)))
                .for_each(|h| h.join().unwrap()),
    }
}

fn work(client_num: usize, thread_num: usize, addr: SocketAddr, window: usize) {
    use bincode::Infinite;

    let stream = TcpStream::connect(addr).unwrap();
    let (reader, writer) = split_tcp_stream(stream);
    let window = Arc::new(AtomicUsize::new(window));
    let window_availible = window.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let write_stop = stop.clone();

    let completed_reads = Arc::new(AtomicUsize::new(0));
    let read = completed_reads.clone();

    thread::spawn(move || {
        let mut i = 0;
        let mut writer = BufWriter::new(writer);
        while !write_stop.load(Relaxed) {
            let mut to_send = window_availible.swap(0, Relaxed);
            if to_send == 0 {
                writer.flush().unwrap();
                to_send = window_availible.swap(0, Relaxed);
                while to_send == 0 {
                    thread::yield_now();
                    to_send = window_availible.swap(0, Relaxed);
                }
            }
            for _ in 0..to_send {
                let op = zk_view::Request::GetData(i, "/foo/1".into());
                bincode::serialize_into(&mut writer, &op, Infinite).unwrap();
                i += 1;
            }
        }
    });

    thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        while !stop.load(Relaxed) {
            let _response: zk_view::Response =
                bincode::deserialize_from(&mut reader, Infinite).unwrap();
            //TODO check?
            window.fetch_add(1, Relaxed);
            read.fetch_add(1, Relaxed);
        }
    });

    thread::sleep(Duration::from_secs(1));
    let mut ops_completed = [0; 10];
    for i in 0..10 {
        thread::sleep(Duration::from_secs(1));
        ops_completed[i] = completed_reads.swap(0, Relaxed);
    }
    thread::sleep(Duration::from_secs(1));
    println!("{:?}.{:?}: {:?}", client_num, thread_num, ops_completed);
    let total_ops: usize = ops_completed.iter().sum();
    let ops_s = total_ops / 10;
    println!("> {:?}.{:?}: {:?}", client_num, thread_num, ops_s);
}

fn mutate(ServerAddrs(servers): ServerAddrs) {
    use zookeeper::CreateMode;
    use zookeeper::LogHandle;
    zookeeper::set_client_id(1);
    let my_chain = 3.into();
    let replicated = servers[0].0 != servers[0].1;
    let (reader, writer) =
        if replicated {
            LogHandle::<[u8]>::replicated_with_servers(&servers[..])
        } else {
            LogHandle::<[u8]>::unreplicated_with_servers(servers.iter().map(|&(a, _)| a))
        }
        .reads_my_writes()
        .chains(Some(my_chain).into_iter())
        .build_handles();
    let my_root = "/foo".to_owned();
    let roots = Some((my_root.clone().into(), my_chain)).into_iter().collect();
    let mut client = zookeeper::Client::new(reader, writer, my_chain, my_root.into(), roots);

    let (s, r) = channel();
    client.create("/foo/1".into(), vec![], CreateMode::persistent(), Box::new(move |res| {
        res.unwrap();
        s.send(()).unwrap()
    }));
    r.recv().unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let stop = done.clone();

    thread::spawn(move || {
        thread::sleep(Duration::from_secs(12));
        stop.store(true, Relaxed);
    });

    let mut i = 0;
    while !done.load(Relaxed) {
        client.set_data("/foo/1".into(), vec![i], -1, Box::new(|_| {}));
        i = i.wrapping_add(1);
        thread::sleep(Duration::new(0, 8_500));
    }
}

///////////////////////////////////////
///////////////////////////////////////
///////////////////////////////////////

fn split_tcp_stream(stream: TcpStream) -> (ReadHalf, WriteHalf) {
    let stream = Arc::new(stream);
    (ReadHalf(stream.clone()), WriteHalf(stream))
}

struct ReadHalf(Arc<TcpStream>);
struct WriteHalf(Arc<TcpStream>);

impl Read for ReadHalf {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&*self.0).read(buf)
    }
}

impl Write for WriteHalf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&*self.0).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&*self.0).flush()
    }
}
