#![feature(test)]

extern crate fuzzy_log;
extern crate mio;
extern crate nix;
extern crate env_logger;
extern crate test;


use mio::deprecated::{EventLoop, Handler as MioHandler, Sender as MioSender};
use mio::tcp::*;

use nix::sys::socket::setsockopt;
use nix::sys::socket::sockopt::TcpNoDelay;

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::collections::hash_map::Entry::Vacant;
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::{env, mem, ops, thread};
use std::time::{Instant};

use fuzzy_log::prelude::*;

use test::black_box;

use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
use std::{iter};
use std::sync::{Arc, Mutex, mpsc};

use fuzzy_log::servers::tcp::Server as TcpServer;

use fuzzy_log::async::fuzzy_log::{LogHandle, ThreadLog, Message};
use fuzzy_log::async::store::AsyncTcpStore;

const LISTENER_TOKEN: mio::Token = mio::Token(0);

fn main() {
    let args = parse_args();
    let _ = env_logger::init();

    static SERVER_READY: AtomicUsize = ATOMIC_USIZE_INIT;

    let (addr, num_clients, _h) = match args {
        Args::Server(addr) => run_server(addr),
        Args::Client(addr, num_clients) => (addr, num_clients, None),
        Args::TrivialServer(addr) => run_trivial_server(addr, &SERVER_READY),
        Args::TrivialClient(addr, num_clients) => run_trivial_client(addr, num_clients),
        Args::LocalTest => {
            let addr = "0.0.0.0:13669".parse().expect("invalid inet address");
            let h = thread::spawn(move || run_trivial_server(addr, &SERVER_READY));
            while SERVER_READY.load(Ordering::Acquire) < 1 {}
            //TODO num clients
            (addr, 1, Some(h))
        }
    };

    static CLIENTS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

    let joins: Vec<_> = (0..num_clients).map(|client_num| {
        thread::spawn(move || {
            let mut log_handle = LogHandle::<()>::spawn_tcp_log(addr, iter::once(addr),
                [order::from(5), order::from(6), order::from(7)].into_iter().cloned());

            CLIENTS_READY.fetch_add(1, Ordering::SeqCst);
            while CLIENTS_READY.load(Ordering::SeqCst) < num_clients {}

            log_handle.snapshot(order::from(5));
            while let Some(..) = log_handle.get_next() { }

            let start = Instant::now();
            log_handle.snapshot(order::from(6));
            black_box(log_handle.get_next());
            let first_fetch_latency = start.elapsed();
            let mut fetched_packets: u64 = 1;
            while let Some(..) = log_handle.get_next() {
                fetched_packets += 1;
            }
            let time = start.elapsed();

            log_handle.snapshot(order::from(7));
            while let Some(..) = log_handle.get_next() { }

            let s = time.as_secs() as f64 + (time.subsec_nanos() as f64 * 10.0f64.powi(-9));
            let hz = fetched_packets as f64 / s;
            println!("client {}: time for {} reads {:?}, {}s, {:.3} Hz, ff latency {:?}",
                client_num, fetched_packets, time, s, hz, first_fetch_latency);
        })
    }).collect();

    for h in joins {
        h.join().ok();
    }

    std::process::exit(0)
/*
    let to_store_m = Arc::new(Mutex::new(None));
    let tsm = to_store_m.clone();
    let (to_log, from_outside) = mpsc::channel();
    let client = to_log.clone();
    let (ready_reads_s, ready_reads_r) = mpsc::channel();
    let (finished_writes_s, finished_writes_r) = mpsc::channel();
    thread::spawn(move || {
        run_store(addr, client, tsm)
    });
    let to_store;
    loop {
        let ts = mem::replace(&mut *to_store_m.lock().unwrap(), None);
        if let Some(s) = ts {
            to_store = s;
            break
        }
    }
    thread::spawn(move || {
        run_log(to_store, from_outside, ready_reads_s, finished_writes_s)
    });

    let mut log_handle = LogHandle::<()>::new(to_log, ready_reads_r, finished_writes_r);
    log_handle.snapshot(order::from(5));

    let start = Instant::now();
    black_box(log_handle.get_next());
    let first_fetch_latency = start.elapsed();
    for _ in 0..1000000 {
        black_box(log_handle.get_next());
    }
    let time = start.elapsed();
    let s = time.as_secs() as f64 + (time.subsec_nanos() as f64 * 10.0f64.powi(-9));
    let hz = 1000000.0 / s;
    println!("elapsed time for 1000000 reads {:?}, {}s, {:.3} Hz, ff latency {:?}",
        time, s, hz, first_fetch_latency);
    */
}

enum Args {
    LocalTest,
    Server(SocketAddr),
    Client(SocketAddr, usize),
    TrivialServer(SocketAddr),
    TrivialClient(SocketAddr, usize),
}

fn parse_args() -> Args {
    let mut args = env::args().skip(1).take(3);
    let arg0 = args.next();
    let arg1 = args.next();
    match (arg0.as_ref().map(|s| s.as_ref()), arg1.as_ref().map(|s| s.as_ref())) {
        (Some("-c"), Some(addr)) => {
            println!("connecting to remote server @ {}.", addr);
            let num_clients =
                if let Some(n) = args.next() { n.parse().unwrap() }
                else { 1 };
            let addr: &str = addr;
            Args::Client(addr.parse().expect("invalid addr"), num_clients)
        }
        (Some("-s"), Some(port)) => {
            println!("starting server on port {}.", port);
            let mut addr = String::from("0.0.0.0:");
            addr.push_str(port);
            Args::Server(addr.parse().expect("invalid port"))
        }
        (Some("-l"), None) => {
            println!("starting local test.");
            Args::LocalTest
        }
        (Some("-ts"), Some(port)) => {
            println!("starting trivial server on port {}.", port);
            let mut addr = String::from("0.0.0.0:");
            addr.push_str(port);
            Args::TrivialServer(addr.parse().expect("invalid port"))
        }
        (Some("-tc"), Some(addr)) => {
            let num_clients =
                if let Some(n) = args.next() { n.parse().unwrap() }
                else { 1 };
            println!("{:?} trivial client(s) connecting to remote server @ {}.", num_clients, addr);
            let addr: &str = addr;
            Args::TrivialClient(addr.parse().expect("invalid addr"), num_clients)
        }
        _ => unimplemented!()
    }
}

#[inline(never)]
pub fn run_trivial_server(addr: SocketAddr, server_ready: &AtomicUsize) -> ! {
    //let mut event_loop = EventLoop::new().unwrap();
    //let server = Server::new(&addr, &mut event_loop);
    //if let Ok(mut server) = server {
    //    server_ready.fetch_add(1, Ordering::Release);
    //    event_loop.run(&mut server).expect("should never return");
    //    panic!("server should never return")
    //}
    //else { panic!("socket in use") }
    Server::run(&addr)
}

#[inline(never)]
pub fn run_server(addr: SocketAddr) -> ! {
    let mut event_loop = EventLoop::new().unwrap();
    let server = TcpServer::new(&addr, 0, 1, &mut event_loop);
    if let Ok(mut server) = server {
        let _ = event_loop.run(&mut server);
        panic!("server should never return")
    }
    else { panic!("socket in use") }
}

#[inline(never)]
pub fn run_store(
    addr: SocketAddr,
    client: mpsc::Sender<Message>,
    tsm: Arc<Mutex<Option<MioSender<Vec<u8>>>>>
) {
    let mut event_loop = EventLoop::new().unwrap();
    let to_store = event_loop.channel();
    *tsm.lock().unwrap() = Some(to_store);
    let mut store = AsyncTcpStore::tcp(addr,
        iter::once(addr),
        client, &mut event_loop).expect("");
    event_loop.run(&mut store).expect("should never return")
}

#[inline(never)]
pub fn run_log(
    to_store: MioSender<Vec<u8>>,
    from_outside: mpsc::Receiver<Message>,
    ready_reads_s: mpsc::Sender<Vec<u8>>,
    finished_writes_s: mpsc::Sender<(Uuid, Vec<OrderIndex>)>,
) {
    let log = ThreadLog::new(to_store, from_outside, ready_reads_s, finished_writes_s,
        [order::from(5)].into_iter().cloned());
    log.run()
}

fn run_trivial_client(server_addr: SocketAddr, num_clients: usize) -> ! {
    use std::io::{Read, Write};
    let joins: Vec<_> = (0..num_clients).map(|client_num| {
        thread::spawn(move || {
            let stream = Arc::new(std::net::TcpStream::connect(server_addr).unwrap());
            let s1 = stream.clone();
            let _h = thread::spawn(move || {
                let mut stream = &*s1;
                let mut buffer = Vec::new();
                {
                    let e = EntryContents::Data(&(), &[]).fill_vec(&mut buffer);
                    e.kind = EntryKind::Read;
                    e.locs_mut()[0] = (5.into(), 3.into());
                }
                for _ in 0..3000001 {
                    let _ = black_box(stream.write_all(&mut buffer));
                }
            });

            let mut stream = &*stream;
            let mut buffer = vec![0u8; mem::size_of::<Entry<(), DataFlex<()>>>()];
            let _ = black_box(stream.read_exact(&mut buffer));
            for _ in 0..1000000 {
                let _ = black_box(stream.read_exact(&mut buffer));
            }
            let start = Instant::now();
            for _ in 0..1000000 {
                let _ = black_box(stream.read_exact(&mut buffer));
            }
            let time = start.elapsed();
            let s = time.as_secs() as f64 + (time.subsec_nanos() as f64 * 10.0f64.powi(-9));
            let hz = 1000000.0 / s;
            for _ in 0..1000000 {
                let _ = black_box(stream.read_exact(&mut buffer));
            }
            println!("client {:?} elapsed time for 1000000 reads {:?}, {}s, {:.3} Hz",
                client_num, time, s, hz);
        })
    }).collect();

    for h in joins {
        h.join().ok();
    }

    std::process::exit(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    use test::Bencher;

    #[bench]
    fn bench_read(b: &mut Bencher) {
        let _ = env_logger::init();
        static SERVER_READY: AtomicUsize = ATOMIC_USIZE_INIT;

        let handle = thread::spawn(|| {
            run_trivial_server(&SERVER_READY)
        });

        while SERVER_READY.load(Ordering::Acquire) < 1 {}

        let to_store_m = Arc::new(Mutex::new(None));
        let tsm = to_store_m.clone();
        let (to_log, from_outside) = mpsc::channel();
        let client = to_log.clone();
        let (ready_reads_s, ready_reads_r) = mpsc::channel();
        let (finished_writes_s, finished_writes_r) = mpsc::channel();
        thread::spawn(move || {
            run_store(client, tsm)
        });
        let to_store;
        loop {
            let ts = mem::replace(&mut *to_store_m.lock().unwrap(), None);
            if let Some(s) = ts {
                to_store = s;
                break
            }
        }
        thread::spawn(move || {
            run_log(to_store, from_outside, ready_reads_s, finished_writes_s)
        });

        let mut log_handle = LogHandle::<()>::new(to_log, ready_reads_r, finished_writes_r);
        log_handle.snapshot(order::from(5));
        b.iter(move|| {black_box(log_handle.get_next());})
    }
}


struct Server {
    acceptor: TcpListener,
    clients: HashMap<mio::Token, PerClient>,
    next_token: usize,
}

struct PerClient {
    buffer: Buffer,
    stream: TcpStream,
    is_reading: bool,
    bytes_read: usize,
    bytes_written: usize,
}

struct Buffer {
    inner: Vec<u8>,
}

impl Server {
    pub fn new(server_addr: &SocketAddr, event_loop: &mut EventLoop<Self>
    ) -> io::Result<Self> {
        let acceptor = try!(TcpListener::bind(server_addr));
        try!(event_loop.register(&acceptor, mio::Token(0), mio::Ready::readable(),
            mio::PollOpt::level()));
        Ok(Server { acceptor: acceptor, clients: HashMap::new(), next_token: 1})
    }

    pub fn run(server_addr: &SocketAddr) -> ! {
        use mio;
        let poll = mio::Poll::new().unwrap();
        let acceptor = TcpListener::bind(server_addr).unwrap();
        let _ = poll.register(&acceptor,
            mio::Token(0),
            mio::Ready::readable(),
            mio::PollOpt::level()).unwrap();
        let mut events = mio::Events::with_capacity(127);
        loop {
            poll.poll(&mut events, None).unwrap();
            for event in events.iter() {
                if let mio::Token(0) = event.token() {
                    match acceptor.accept() {
                        Err(e) => panic!("error {}", e),
                        Ok((socket, addr)) => {
                            thread::spawn(move || {
                                let _ = socket.set_keepalive_ms(Some(1000));
                                let _ = socket.set_nodelay(true);
                                PerClient::new(socket).run();
                            });
                        }
                    }
                }
            }
        }
    }
}

impl MioHandler for Server {
    type Timeout = ();
    type Message = ();

    fn ready(
        &mut self,
        event_loop: &mut EventLoop<Self>,
        token: mio::Token,
        events: mio::Ready
    ) {
        match token {
            LISTENER_TOKEN => {
                assert!(events.is_readable());
                match self.acceptor.accept() {
                    Err(e) => panic!("error {}", e),
                    Ok((socket, addr)) => {
                        let next_client_id = self.next_token;
                        self.next_token += 1;
                        thread::spawn(move || {
                            let _ = socket.set_keepalive_ms(Some(1000));
                            //let _ = setsockopt(socket.as_raw_fd(), TcpNoDelay, &true);
                            let _ = socket.set_nodelay(true);
                            let client_token = mio::Token(next_client_id);
                            let mut per_client = PerClient::new(socket);
                            let mut client_loop = EventLoop::new().unwrap();
                            client_loop.register(&per_client.stream,
                                    client_token,
                                    mio::Ready::readable() | mio::Ready::error(),
                                    mio::PollOpt::edge() | mio::PollOpt::oneshot())
                            .expect("could not register client socket");
                            let _ = client_loop.run(&mut per_client)
                                .expect(" should never halt");
                        });
                        /*let _ = socket.set_keepalive(Some(1));
                        let _ = setsockopt(socket.as_raw_fd(), TcpNoDelay, &true);
                        // let _ = socket.set_tcp_nodelay(true);
                        let client_token = mio::Token(next_client_id);
                        let client_socket = &match self.clients.entry(client_token) {
                            Vacant(v) => v.insert(PerClient::new(socket)),
                            _ => panic!("re-accept client {:?}", client_token),
                        }.stream;
                        //TODO edge or level?
                        event_loop.register(client_socket,
                                client_token,
                                mio::Ready::readable() | mio::Ready::error(),
                                mio::PollOpt::edge() | mio::PollOpt::oneshot())
                        .expect("could not register client socket")*/
                    }
                }
            }
            client_token => {
                if events.is_error() {
                    self.clients.remove(&client_token);
                    return;
                }

                let client = self.clients.get_mut(&client_token).unwrap();
                let finished_read =
                    if client.is_reading && events.is_readable() { client.read_packet() }
                    else { false };

                let (finished_write, needs_write) =
                    if !client.is_reading && events.is_writable() { client.write_packet() }
                    else { (false, !client.is_reading) };

                let next_interest = match (finished_read, finished_write) {
                    (true, true) => mio::Ready::readable(),
                    (true, false) => mio::Ready::writable(),
                    (false, true) => mio::Ready::readable(),
                    (false, false) if needs_write => mio::Ready::writable(),
                    (false, false) => mio::Ready::readable(),
                };
                event_loop.reregister(
                    &client.stream,
                    client_token,
                    next_interest | mio::Ready::error(),
                    mio::PollOpt::edge() | mio::PollOpt::oneshot())
                .expect("could not reregister client socket")
            }
        }
    }
}

impl MioHandler for PerClient {
    type Timeout = ();
    type Message = ();

    fn ready(
        &mut self,
        event_loop: &mut EventLoop<Self>,
        token: mio::Token,
        events: mio::Ready
    ) {
        print!("iter start");
        if events.is_error() {
            panic!("error {:?}", self.stream.take_error())
        }

        let finished_read =
            if self.is_reading && events.is_readable() { self.read_packet() }
            else { false };

        let (finished_write, needs_write) =
            if !self.is_reading && events.is_writable() { self.write_packet() }
            else { (false, !self.is_reading) };

        let next_interest = match (finished_read, finished_write) {
            (true, true) => mio::Ready::readable(),
            (true, false) => mio::Ready::writable(),
            (false, true) => mio::Ready::readable(),
            (false, false) if needs_write => mio::Ready::writable(),
            (false, false) => mio::Ready::readable(),
        };
        event_loop.reregister(
            &self.stream,
            token,
            next_interest | mio::Ready::error(),
            mio::PollOpt::edge() | mio::PollOpt::oneshot())
        .expect("could not reregister client socket")
    }
}

impl PerClient {
    fn new(stream: TcpStream) -> Self {
        PerClient {
            stream: stream,
            buffer: Buffer::new(),
            bytes_read: 0,
            bytes_written: 0,
            is_reading: true,
        }
    }

    fn run(mut self) -> ! {
        use mio;
        let poll = mio::Poll::new().unwrap();
        let _ = poll.register(&self.stream,
            mio::Token(0),
            mio::Ready::readable() | mio::Ready::error(),
            mio::PollOpt::edge() | mio::PollOpt::oneshot()).unwrap();
        let mut events = mio::Events::with_capacity(127);

        loop {
            poll.poll(&mut events, None).unwrap();
            let event = events.get(0).unwrap();
            let events = event.kind();
            let finished_read =
                if self.is_reading && events.is_readable() { self.read_packet() }
                else { false };

            let (finished_write, needs_write) =
                if !self.is_reading && events.is_writable() { self.write_packet() }
                else { (false, !self.is_reading) };

            let next_interest = match (finished_read, finished_write) {
                (true, true) => mio::Ready::readable(),
                (true, false) => mio::Ready::writable(),
                (false, true) => mio::Ready::readable(),
                (false, false) if needs_write => mio::Ready::writable(),
                (false, false) => mio::Ready::readable(),
            };
            poll.reregister(
                &self.stream,
                mio::Token(0),
                next_interest | mio::Ready::error(),
                mio::PollOpt::edge() | mio::PollOpt::oneshot())
            .expect("could not reregister client socket")
        }
    }

    fn read_packet(&mut self) -> bool {
        let size = mem::size_of::<Entry<(), DataFlex<()>>>();
        if self.bytes_read < size {
            let read = self.stream.read(&mut self.buffer[self.bytes_read..size])
                .unwrap();
            self.bytes_read += read;
            if self.bytes_read < size {
                return false;
            }
        }
        if self.buffer.entry().locs()[0].1 < entry::from(::std::u32::MAX) {
            self.buffer.entry_mut().kind.insert(EntryKind::ReadSuccess);
            self.buffer.entry_mut().kind = EntryKind::ReadData;
        }
        else {
            let packet = self.buffer.entry_mut();
            let (old_id, old_loc) =  (packet.id, packet.locs()[0]);
            let chain: order = old_loc.0;
            *packet = EntryContents::Data(&(), &[(chain, entry::from(10000000))]).clone_entry();
            packet.id = old_id;
            packet.kind = EntryKind::NoValue;
            unsafe {
                packet.as_data_entry_mut().flex.loc = old_loc;
            }
        }
        self.is_reading = false;
        true
    }

    fn write_packet(&mut self) -> (bool, bool) {
        let len = self.buffer.entry().entry_size();
        //println!("SERVER writing {:?}", self.buffer.entry().locs()[0]);
        self.bytes_written += self.stream.write(&self.buffer[self.bytes_written..len]).unwrap();
        if self.bytes_written == len {
            //println!("SERVER finished write {:?}", self.buffer.entry().locs()[0]);
            self.bytes_written = 0;
            self.bytes_read = 0;
            self.is_reading = true;
            (true, false)
        }
        else {
            (false, !self.is_reading)
        }
    }
}

impl Buffer {
    fn new() -> Self {
        let size = mem::size_of::<Entry<(), DataFlex<()>>>() * 2;
        let mut inner = Vec::with_capacity(size);
        unsafe { inner.set_len(size) }
        Buffer { inner: inner}
    }

    fn entry(&self) -> &Entry<()> {
        Entry::<()>::wrap_bytes(&self[..])
    }

    fn entry_mut(&mut self) -> &mut Entry<()> {
        Entry::<()>::wrap_bytes_mut(&mut self[..])
    }
}

//FIXME impl AsRef for Buffer...
impl ops::Index<ops::Range<usize>> for Buffer {
    type Output = [u8];
    fn index(&self, index: ops::Range<usize>) -> &Self::Output {
        &self.inner[index]
    }
}

impl ops::IndexMut<ops::Range<usize>> for Buffer {
    fn index_mut(&mut self, index: ops::Range<usize>) -> &mut Self::Output {
        //TODO should this ensure capacity?
        &mut self.inner[index]
    }
}

impl ops::Index<ops::RangeFrom<usize>> for Buffer {
    type Output = [u8];
    fn index(&self, index: ops::RangeFrom<usize>) -> &Self::Output {
        &self.inner[index]
    }
}

impl ops::Index<ops::RangeFull> for Buffer {
    type Output = [u8];
    fn index(&self, index: ops::RangeFull) -> &Self::Output {
        &self.inner[index]
    }
}

impl ops::IndexMut<ops::RangeFull> for Buffer {
    fn index_mut(&mut self, index: ops::RangeFull) -> &mut Self::Output {
        &mut self.inner[index]
    }
}
