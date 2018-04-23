
extern crate mio;
extern crate reactor;
extern crate structopt;
extern crate zk_view;

#[macro_use] extern crate structopt_derive;

pub use zk_view::{bincode, serde, zookeeper, ServerAddrs};

use bincode::Infinite;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

use structopt::StructOpt;

use reactor::*;

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
        Args::read{client_num, view_addr, num_threads, window} => {
            let completed_reads: Vec<_> = (0..num_threads)
                .map(|_| Arc::new(AtomicUsize::new(0)))
                .collect();

            (0..num_threads)
                .for_each(|i| {
                    let read = completed_reads[i].clone();
                    thread::spawn(move || work(view_addr, window, read));
                });

            thread::sleep(Duration::from_secs(1));

            let mut ops_completed = vec![[0; 10]; num_threads];

            for i in 0..10 {
                thread::sleep(Duration::from_secs(1));
                for j in 0..num_threads {
                    ops_completed[j][i] = completed_reads[j].swap(0, Relaxed);
                }
            }

            thread::sleep(Duration::from_secs(1));
            for j in 0..num_threads {
                println!("{:?}.{:?}: {:?}", client_num, j, ops_completed[j]);
                let total_ops: usize = ops_completed[j].iter().sum();
                let ops_s = total_ops / 10;
                println!("> {:?}.{:?}: {:?}", client_num, j, ops_s);
            }

        },
    }
}

fn work(addr: SocketAddr, window: usize, read: Arc<AtomicUsize>) {

    let stream = mio::net::TcpStream::connect(&addr).unwrap();
    let window = window as u64;

    let mut handler = TcpHandler::new(
        stream,
        ResponseReader,
        ResponseHandler(vec![], window, read)
    );
    for i in 0..window {
        let op = zk_view::Request::GetData(i, "/foo/1".into());
        let bytes = bincode::serialize(&op, Infinite).unwrap();
        handler.add_writes(&[&*bytes])
    }
    let mut generator = Generator::with_inner(1.into(), ()).unwrap();
    generator.add_stream(2.into(), handler);
    generator.run().unwrap();
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

type Generator = Reactor<PerStream, ()>;
type PerStream = TcpHandler<ResponseReader, ResponseHandler>;

///////////////////

struct ResponseHandler(Vec<u8>, u64, Arc<AtomicUsize>);

impl MessageHandler<(), zk_view::Response> for ResponseHandler {
    fn handle_message(&mut self, io: &mut TcpWriter, _: &mut (), _message: zk_view::Response)
    -> Result<(), ()> {
        self.2.fetch_add(1, Relaxed);

        let i = self.1;
        self.1 += 1;
        let op = zk_view::Request::GetData(i, "/foo/1".into());

        self.0.clear();
        bincode::serialize_into(&mut self.0, &op, Infinite).unwrap();
        io.add_bytes_to_write(&[&*self.0]);
        Ok(())
    }
}

///////////////////

struct ResponseReader;

impl MessageReader for ResponseReader {
    type Message = zk_view::Response;
    type Error = Box<::bincode::ErrorKind>;

    fn deserialize_message(
        &mut self,
        bytes: &[u8]
    ) -> Result<(Self::Message, usize), MessageReaderError<Self::Error>> {
        use ::std::io::Cursor;

        let mut cursor = Cursor::new(bytes);
        let limit = ::bincode::Bounded(bytes.len() as u64);
        let res = ::bincode::deserialize_from(&mut cursor, limit);
        match res {
            Ok(val) => Ok((val, cursor.position() as usize)),
            Err(error) => {
                if let &::bincode::ErrorKind::SizeLimit = &*error {
                    return Err(MessageReaderError::NeedMoreBytes(1))
                }
                Err(MessageReaderError::Other(error))
            },
        }
    }
}

///////////////////////////////////////
///////////////////////////////////////
///////////////////////////////////////

// fn split_tcp_stream(stream: TcpStream) -> (ReadHalf, WriteHalf) {
//     let stream = Arc::new(stream);
//     (ReadHalf(stream.clone()), WriteHalf(stream))
// }

// struct ReadHalf(Arc<TcpStream>);
// struct WriteHalf(Arc<TcpStream>);

// impl Read for ReadHalf {
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//         (&*self.0).read(buf)
//     }
// }

// impl Write for WriteHalf {
//     fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
//         (&*self.0).write(buf)
//     }

//     fn flush(&mut self) -> io::Result<()> {
//         (&*self.0).flush()
//     }
// }
