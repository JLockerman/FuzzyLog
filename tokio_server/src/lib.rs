extern crate futures;
extern crate tokio;
extern crate tokio_io;

extern crate fuzzy_log_packets;
extern crate fuzzy_log_server;
extern crate fuzzy_log_util;

use std::io::{Error as IoError, ErrorKind};
use std::mem;
use std::net::SocketAddr;
use std::thread;

use futures::{future, stream, Future, Sink, Stream};
use futures::sync::{mpsc, oneshot};

use tokio::executor::current_thread;
use tokio::net::{TcpListener, TcpStream};

use tokio_io::{io, AsyncRead};

use fuzzy_log_server::*;
use fuzzy_log_server::shared_slice::RcSlice;

use fuzzy_log_packets::*;
use fuzzy_log_packets::Packet::WrapErr;
use fuzzy_log_packets::buffer::Buffer;
use fuzzy_log_util::socket_addr::Ipv4SocketAddr as ClientId;

use fuzzy_log_util::hash::IdHashMap;

use buffer_stream::BufferStream;

pub use read_buffer::ReadBuffer;
pub use batch_read_buffer::BatchReadBuffer;

// mod buf_writer;
// mod doorbell;
mod batch_read_buffer;
mod buffer_stream;
mod read_buffer;
mod vec_stream;
mod worker;

pub enum ToServer {
    NewClient(u64, mpsc::Sender<ReadBuffer>),
    Message(u64, ReadBuffer),
}

pub fn run_unreplicated_server<CallBack: FnOnce()>(
    addr: &SocketAddr, this_server_num: u32, total_chain_servers: u32, callback: CallBack
) -> Result<(), IoError> {
    let (store, log_reader) = fuzzy_log_server::new_chain_store_and_reader();
    let to_log = start_log_thread(this_server_num, total_chain_servers, store);
    let listener = TcpListener::bind(&addr)?;
    callback();
    Ok(run(listener, to_log, log_reader, Some(false)))
}

pub fn run(
    listener: TcpListener,
    sender: mpsc::Sender<ToLog<u64>>,
    log_reader: ChainReader<u64>,
    replication: Option<bool>,
) {
    let mut next = 0;
    let server = listener
        .incoming()
        .for_each(move |tcp| {
            use ::std::time::Duration;
            let _ = tcp.set_nodelay(true);
            let _ = tcp.set_keepalive(Some(Duration::from_secs(1)));
            let client = next;
            next += 1;
            let (to_client, receiver) = mpsc::channel(100);
            //FIXME no wait?
            let sender = sender.clone();
            let sender = sender
                .send(ToLog::NewClient(client, to_client))
                .wait()
                .unwrap();
            let log_reader = log_reader.clone();
            thread::spawn(move || {

                let tcp = match replication {
                    None => tcp,
                    Some(true) => unimplemented!(),
                    Some(false) => {
                        let mut ack = [0; 16];
                        io::read_exact(tcp, ack)
                        .and_then(|(tcp, ack)|
                            io::write_all(tcp, ack)
                        ).wait().unwrap().0
                    },
                };

                let (reader, writer) = tcp.split();

                //FIXME read client_id
                //FIXME send up/downstream

                let batch = worker::Batcher::new(client, log_reader);
                let write_buffer = BufferStream::default();
                let needs_writing = write_buffer.clone();
                let read = stream::repeat(())
                    .fold(
                        (
                            sender,
                            reader,
                            BatchReadBuffer::with_capacity(8192),
                            batch,
                            needs_writing,
                        ),
                        move |(sender, reader, buffer, mut batch, mut needs_writing), _| {
                            io::read(reader, buffer).and_then(move |(reader, mut buffer, len)| {
                                buffer.freeze_additional(len);
                                let needed;
                                //TODO do read_fixed batch, send, future instead?
                                loop {
                                    let len = match unsafe { Packet::Ref::try_ref(&buffer[..]) } {
                                        Ok((packet, bytes)) if bytes.len() >= client_id_size() => {
                                            Ok(packet.len() + client_id_size())
                                        }
                                        Ok((packet, _)) => Err(packet.len() + client_id_size()),
                                        Err(WrapErr::NotEnoughBytes(len)) => {
                                            Err(len + client_id_size())
                                        }
                                        Err(e) => panic!("{:?}", e), //panic!("{:?} @ {:?}", e, buffer),
                                    };
                                    match len {
                                        Ok(len) => {
                                            //TODO cache Vecs?
                                            let buff = Buffer::wrap_vec(buffer.split_off(len));
                                            batch.add_client_msg(buff);
                                        }
                                        Err(len) => {
                                            needed = Some(len);
                                            break;
                                        }
                                    }
                                }
                                buffer.shift_back();
                                if let Some(len) = needed {
                                    buffer.ensure_fits(len)
                                }
                                sender
                                    .send_all(batch.log_batch())
                                    .map_err(|_| IoError::from(ErrorKind::BrokenPipe))
                                    .map(move |(sender, batch)| {
                                        let mut batcher = batch.batcher();
                                        batcher.handle_buffered_reads(|res| match res {
                                            Ok(bytes) => needs_writing.add_slice(bytes),
                                            Err(contents) => needs_writing.add_contents(contents),
                                        });
                                        (sender, reader, buffer, batcher, needs_writing)
                                    })
                            })
                        },
                    )
                    .map(|_| ())
                    .map_err(|err: IoError| println!("Recv error {:?}", err));

                let needs_writing = write_buffer.clone();
                let buffer_cache = write_buffer.clone();

                //TODO backpressure
                let add_buffer = receiver
                    .fold(write_buffer, move |mut write_buffer, new_send| {
                        //TODO replication
                        worker::add_response(new_send, &mut write_buffer, client);
                        future::ok(write_buffer)
                    })
                    .map(|_| ())
                    .map_err(|_| ());

                let write = needs_writing
                    .map_err(|_| IoError::from(ErrorKind::BrokenPipe))
                    .fold(
                        (writer, buffer_cache),
                        |(writer, buffer_cache), write_buffer| {
                            io::write_all(writer, write_buffer).map(|(writer, buffer)| {
                                buffer_cache.return_buffer(buffer);
                                (writer, buffer_cache)
                            })
                        },
                    )
                    .map(|_| ())
                    .map_err(|err: IoError| println!("Send error {:?}", err));

                current_thread::run(|_| {
                    current_thread::spawn(read);
                    current_thread::spawn(add_buffer);
                    current_thread::spawn(write);
                });
            });
            future::ok(())
        })
        .map_err(|err| {
            println!("server error {:?}", err);
        });

    current_thread::run(|_| {
        current_thread::spawn(server);
    });
}

pub enum ToLog<T> {
    New(
        Buffer,
        Troption<SkeensMultiStorage, Box<(RcSlice, RcSlice)>>,
        T,
    ),
    Replication(ToReplicate, T),

    #[allow(dead_code)]
    Recovery(Recovery, T),

    NewClient(u64, mpsc::Sender<ToWorker<u64>>),
}

pub fn start_log_thread(
    this_server_num: u32,
    total_chain_servers: u32,
    chains: ChainStore<u64>,
) -> mpsc::Sender<ToLog<u64>> {
    let (p, c) = oneshot::channel();
    thread::spawn(move || {

        let to_workers = ToWorkers::default();
        let log = ServerLog::new(this_server_num, total_chain_servers, to_workers, chains);
        let (to_log, from_clients) = mpsc::channel(100);
        p.send(to_log).unwrap_or_else(|_| panic!());
        let log = from_clients
            .fold(log, |mut log, msg| {
                match msg {
                    ToLog::New(buffer, storage, st) => log.handle_op(buffer, storage, st),
                    ToLog::Replication(tr, st) => log.handle_replication(tr, st),
                    ToLog::Recovery(r, st) => log.handle_recovery(r, st),
                    ToLog::NewClient(client, channel) => {
                        log.to_workers.0.insert(client, channel);
                    }
                }
                future::ok(log)
            })
            .map(|_| ());
        current_thread::run(|_| {
            current_thread::spawn(log);
        });
    });
    c.wait().unwrap()
}

#[derive(Default)]
struct ToWorkers(IdHashMap<u64, mpsc::Sender<ToWorker<u64>>>);

impl DistributeToWorkers<u64> for ToWorkers {
    fn send_to_worker(&mut self, msg: ToWorker<u64>) {
        //TODO buffer?
        self.0.get_mut(&msg.get_associated_data()).map(move |send| {
            use futures::{Async, AsyncSink};
            let mut res = Ok(AsyncSink::NotReady(msg));
            while let Ok(AsyncSink::NotReady(msg)) = res {
                res = send.start_send(msg);
                //TODO yield?
            }
            while let Ok(Async::NotReady) = send.poll_complete() {}
        });
    }
}

pub fn run_pool(
    listener: TcpListener,
    sender: mpsc::Sender<ToServer>,
    num_threads: usize,
    // receiver: Box<FnMut(usize) -> mpsc::UnboundedReceiver<Vec<u8>> + Send + Sync>,
) {
    let mut to_workers = Vec::with_capacity(num_threads);
    for _ in 0..num_threads {
        let (to_thread, from_acceptor) = mpsc::channel(10);
        to_workers.push(Some(to_thread));
        thread::spawn(move || {
            let accept_new = from_acceptor.for_each(|connection| {
                let (socket, to_log, from_log, client): (
                    TcpStream,
                    mpsc::Sender<_>,
                    mpsc::Receiver<ReadBuffer>,
                    _,
                ) = connection;
                let (reader, writer) = socket.split();

                let read = stream::repeat(())
                    .fold((to_log, reader), move |(to_log, reader), _| {
                        io::read_exact::<_, ReadBuffer>(
                            reader,
                            ReadBuffer::with_capacity(min_recv_size()),
                        ).then(|res| match res {
                            Ok((reader, mut buffer)) => {
                                buffer.freeze_read();
                                let len = unsafe {
                                    match Packet::Ref::try_ref(&buffer[..]) {
                                        Ok((packet, _)) => packet.len() + client_id_size(),
                                        Err(WrapErr::NotEnoughBytes(len)) => len + client_id_size(),
                                        Err(_) => return Err(ErrorKind::InvalidInput.into()),
                                    }
                                };
                                buffer.will_read_to(len);
                                Ok((reader, buffer))
                            }
                            e => e,
                        })
                            .and_then(|(reader, buffer)| {
                                io::read_exact::<_, ReadBuffer>(reader, buffer)
                            })
                            .and_then(move |(reader, mut buffer)| {
                                buffer.freeze_read();
                                //TODO start_send?
                                //TODO splitoff ClientId
                                to_log
                                    .send(ToServer::Message(client, buffer))
                                    .map(|to_log| (to_log, reader))
                                    .map_err(|_| IoError::from(ErrorKind::BrokenPipe))
                            })
                    })
                    .map(|_| ())
                    .map_err(|err: IoError| println!("Recv error {:?}", err));

                let write_buffer = BufferStream::default();
                let downstream_buffer = BufferStream::default();
                let needs_writing = write_buffer.clone();
                let buffer_cache = write_buffer.clone();

                let has_downstream = false;
                let add_buffer = from_log
                    .fold(
                        (write_buffer, downstream_buffer, has_downstream),
                        |(write_buffer, downstream_buffer, has_downstream), new_send| {
                            if has_downstream {
                                downstream_buffer.add_slice(&*new_send);
                            } else {
                                write_buffer.add_slice(&*new_send);
                            }
                            future::ok((write_buffer, downstream_buffer, has_downstream))
                        },
                    )
                    .map(|_| ())
                    .map_err(|_| ());

                let write = needs_writing
                    .map_err(|_| IoError::from(ErrorKind::BrokenPipe))
                    .fold(
                        (writer, buffer_cache),
                        |(writer, buffer_cache), write_buffer| {
                            io::write_all(writer, write_buffer).map(|(writer, buffer)| {
                                buffer_cache.return_buffer(buffer);
                                (writer, buffer_cache)
                            })
                        },
                    )
                    .map(|_| ())
                    .map_err(|err: IoError| println!("Send error {:?}", err));

                current_thread::spawn(read);
                current_thread::spawn(add_buffer);
                current_thread::spawn(write);
                future::ok(())
            });

            current_thread::run(|_| {
                current_thread::spawn(accept_new);
            });
        });
    }
    let mut next = 0;
    let server = listener
        .incoming()
        .fold(to_workers, move |mut to_workers, tcp| {
            let client = next;
            next += 1;
            let (to_client, receiver) = mpsc::channel(10);
            //FIXME no clone?
            let sender = sender.clone();
            sender
                .send(ToServer::NewClient(client, to_client))
                .map_err(|e| panic!("{:?}", e))
                .and_then(move |sender| {
                    //FIXME use hash distribution
                    let worker_num = (client % to_workers.len() as u64) as usize;
                    println!("worker {:?}", worker_num);
                    //TODO conditional remove?
                    mem::replace(&mut to_workers[worker_num], None)
                        .expect("cannot send to worker")
                        .send((tcp, sender, receiver, client))
                        .map(move |to_worker| {
                            to_workers[worker_num] = Some(to_worker);
                            to_workers
                        })
                })
                .map_err(|e| {
                    println!("dist error {:?}", e);
                    ErrorKind::ConnectionRefused
                })
        })
        .map(|_| ())
        .map_err(|err| println!("server error {:?}", err));

    current_thread::run(|_| {
        current_thread::spawn(server);
    });
}

#[inline(always)]
fn min_recv_size() -> usize {
    Packet::min_len() + mem::size_of::<ClientId>()
}

#[inline(always)]
fn client_id_size() -> usize {
    mem::size_of::<ClientId>()
}

#[cfg(test)]
mod tests {
    use super::*;
    // use fuzzy_log_packets::*;

    use std::collections::HashMap;

    use tokio::net::TcpStream;

    macro_rules! read_packet {
        ($order: expr, $index:expr) => (
            EntryContents::Read {
                id: &Uuid::nil(),
                flags: &EntryFlag::Nothing,
                data_bytes: &0,
                dependency_bytes: &0,
                loc: &OrderIndex($order.into(), $index.into()),
                horizon: &OrderIndex($order.into(), 0.into()),
                min: &OrderIndex($order.into(), 0.into())
            }
        )
    }

    pub fn echo(listener: TcpListener, sender: mpsc::Sender<ToServer>) {
        let mut next = 0;
        let server = listener
            .incoming()
            .for_each(move |tcp| {
                let client = next;
                next += 1;
                let (to_client, receiver) = mpsc::channel(100);
                //FIXME no wait?
                let sender = sender.clone();
                let sender = sender
                    .send(ToServer::NewClient(client, to_client))
                    .wait()
                    .unwrap();
                thread::spawn(move || {
                    let (reader, writer) = tcp.split();

                    let read = stream::repeat(())
                        .fold(
                            (
                                sender,
                                reader,
                                BatchReadBuffer::with_capacity(8192),
                                Vec::with_capacity(8),
                            ),
                            move |(sender, reader, buffer, mut batch), _| {
                                io::read(reader, buffer).and_then(
                                    move |(reader, mut buffer, len)| {
                                        buffer.freeze_additional(len);
                                        let mut needed = None;
                                        for _ in 0..(batch.capacity() - 1) {
                                            let len = match unsafe {
                                                Packet::Ref::try_ref(&buffer[..])
                                            } {
                                                Ok((packet, bytes))
                                                    if bytes.len() >= client_id_size() =>
                                                {
                                                    Ok(packet.len() + client_id_size())
                                                }
                                                Ok((packet, _)) => {
                                                    Err(packet.len() + client_id_size())
                                                }
                                                Err(WrapErr::NotEnoughBytes(len)) => {
                                                    Err(len + client_id_size())
                                                }
                                                Err(e) => panic!("{:?} @ {:?}", e, buffer),
                                            };
                                            match len {
                                                Ok(len) => {
                                                    //TODO cache Vecs?
                                                    let buff =
                                                        ReadBuffer::from_vec(buffer.split_off(len));
                                                    batch.push(ToServer::Message(client, buff))
                                                }
                                                Err(len) => {
                                                    needed = Some(len);
                                                    break;
                                                }
                                            }
                                        }
                                        buffer.shift_back();
                                        if let Some(len) = needed {
                                            buffer.ensure_fits(len)
                                        }
                                        sender
                                            .send_all(vec_stream::build(batch))
                                            .map(move |(sender, batch)| {
                                                (sender, reader, buffer, batch.into_vec())
                                            })
                                            .map_err(|_| IoError::from(ErrorKind::BrokenPipe))
                                    },
                                )
                            },
                        )
                        .map(|_| ())
                        .map_err(|err: IoError| println!("Recv error {:?}", err));

                    let write_buffer = BufferStream::default();
                    let needs_writing = write_buffer.clone();
                    let buffer_cache = write_buffer.clone();

                    //TODO backpressure
                    let add_buffer = receiver
                        .fold(write_buffer, |write_buffer, new_send| {
                            write_buffer.add_slice(&*new_send);
                            future::ok(write_buffer)
                        })
                        .map(|_| ())
                        .map_err(|_| ());

                    let write = needs_writing
                        .map_err(|_| IoError::from(ErrorKind::BrokenPipe))
                        .fold(
                            (writer, buffer_cache),
                            |(writer, buffer_cache), write_buffer| {
                                io::write_all(writer, write_buffer).map(|(writer, buffer)| {
                                    buffer_cache.return_buffer(buffer);
                                    (writer, buffer_cache)
                                })
                            },
                        )
                        .map(|_| ())
                        .map_err(|err: IoError| println!("Send error {:?}", err));

                    current_thread::run(|_| {
                        current_thread::spawn(read);
                        current_thread::spawn(add_buffer);
                        current_thread::spawn(write);
                    });
                });
                future::ok(())
            })
            .map_err(|err| {
                println!("server error {:?}", err);
            });

        current_thread::run(|_| {
            current_thread::spawn(server);
        });
    }

    #[test]
    fn server() {
        let listener = TcpListener::bind(&"0.0.0.0:13288".parse().unwrap()).unwrap();
        let (to_log, from_client) = mpsc::channel(10);
        let (store, reader) = fuzzy_log_server::new_chain_store_and_reader();
        thread::spawn(move || run(listener, to_log, reader, None));

        thread::spawn(move || {
            let handle = from_client
                .fold(HashMap::new(), |mut clients, msg| {
                    use ToLog::*;
                    match msg {
                        NewClient(client, sender) => {
                            clients.insert(client, sender);
                            future::Either::B(future::ok(clients))
                        }
                        //FIXME
                        New(msg, _storage, client) => {
                            let msg = msg.contents().to_vec();
                            let msg = Buffer::wrap_vec(msg);
                            future::Either::A(
                                clients
                                    .remove(&client)
                                    .expect("no client")
                                    .send(ToWorker::Reply(msg, client))
                                    .map(move |sender| {
                                        clients.insert(client, sender);
                                        clients
                                    })
                                    .map_err(|e| panic!("{:?}", e)),
                            )
                        },
                        _ => unimplemented!(),
                    }
                })
                .map(|_| ());

            current_thread::run(|_| {
                current_thread::spawn(handle);
            });
        });

        // clients("127.0.0.1:13288")
        let read_len = make_read_packet(0, 0).len();

        let mut clients: Vec<_> = (1..6)
            .map(|client| {
                //FIXME test writes?
                let id = ClientId::random();
                TcpStream::connect(&"127.0.0.1:13288".parse().unwrap())
                    .and_then(move |stream| {
                        let (reader, writer) = stream.split();
                        let writer = io::write_all(writer, make_read_packet(client, 1))
                            .and_then(move |(writer, _)| io::write_all(writer, id))
                            .and_then(move |(writer, _)| {
                                io::write_all(writer, make_read_packet(client, 2))
                                    .and_then(move |(writer, _)| io::write_all(writer, id))
                            })
                            .and_then(move |(writer, _)| {
                                io::write_all(writer, make_read_packet(client, 3))
                                    .and_then(move |(writer, _)| io::write_all(writer, id))
                            })
                            .and_then(move |(writer, _)| {
                                io::write_all(writer, make_read_packet(client, 2))
                                    .and_then(move |(writer, _)| io::write_all(writer, id))
                            })
                            .and_then(move |(writer, _)| {
                                io::write_all(writer, make_read_packet(client, 1))
                                    .and_then(move |(writer, _)| io::write_all(writer, id))
                            })
                            .and_then(move |(writer, _)| {
                                io::write_all(writer, make_read_packet(client, client))
                                    .and_then(move |(writer, _)| io::write_all(writer, id))
                            });

                        let reader = unsafe {
                            io::read_exact(reader, vec![0; read_len])
                                .and_then(move |(reader, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, 1), &[][..])
                                    );
                                    io::read_exact(reader, vec![0; read.len()])
                                })
                                .and_then(move |(reader, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, 2), &[][..])
                                    );
                                    io::read_exact(reader, vec![0; read_len])
                                })
                                .and_then(move |(reader, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, 3), &[][..])
                                    );
                                    io::read_exact(reader, vec![0; read_len])
                                })
                                .and_then(move |(reader, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, 2), &[][..])
                                    );
                                    io::read_exact(reader, vec![0; read_len])
                                })
                                .and_then(move |(reader, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, 1), &[][..])
                                    );
                                    io::read_exact(reader, vec![0; read_len])
                                })
                                .and_then(move |(_, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, client), &[][..])
                                    );
                                    future::ok(())
                                })
                        };

                        reader.join(writer)
                    })
                    .map(|_| ())
                    .map_err(|e| panic!("{:?}", e))
            })
            .collect();

        current_thread::run(|_| {
            clients
                .drain(..)
                .for_each(|client| current_thread::spawn(client));
        });
    }

    #[test]
    fn thread_per_client() {
        let listener = TcpListener::bind(&"0.0.0.0:13289".parse().unwrap()).unwrap();
        let (to_log, from_client) = mpsc::channel(10);
        thread::spawn(move || echo(listener, to_log));

        run_test("127.0.0.1:13289", from_client)
    }

    #[test]
    fn fixed_threads() {
        let listener = TcpListener::bind(&"0.0.0.0:13291".parse().unwrap()).unwrap();
        let (to_log, from_client) = mpsc::channel(10);
        thread::spawn(move || run_pool(listener, to_log, 2));

        run_test("127.0.0.1:13291", from_client)
    }

    fn run_test(addr: &str, from_client: mpsc::Receiver<ToServer>) {
        thread::spawn(move || {
            let handle = from_client
                .fold(HashMap::new(), |mut clients, msg| {
                    use ToServer::*;
                    match msg {
                        NewClient(client, sender) => {
                            clients.insert(client, sender);
                            future::Either::B(future::ok(clients))
                        }
                        //FIXME
                        Message(client, mut msg) => {
                            msg.pop_bytes(client_id_size());
                            future::Either::A(
                                clients
                                    .remove(&client)
                                    .expect("no client")
                                    .send(msg)
                                    .map(move |sender| {
                                        clients.insert(client, sender);
                                        clients
                                    })
                                    .map_err(|e| panic!("{:?}", e)),
                            )
                        }
                    }
                })
                .map(|_| ());

            current_thread::run(|_| {
                current_thread::spawn(handle);
            });
        });

        clients(addr)
    }

    fn clients(addr: &str) {
        let read_len = make_read_packet(0, 0).len();

        let mut clients: Vec<_> = (1..6)
            .map(|client| {
                let id = ClientId::random();
                TcpStream::connect(&addr.parse().unwrap())
                    .and_then(move |stream| {
                        let (reader, writer) = stream.split();
                        let writer = io::write_all(writer, make_read_packet(client, 1))
                            .and_then(move |(writer, _)| io::write_all(writer, id))
                            .and_then(move |(writer, _)| {
                                io::write_all(writer, make_read_packet(client, 2))
                                    .and_then(move |(writer, _)| io::write_all(writer, id))
                            })
                            .and_then(move |(writer, _)| {
                                io::write_all(writer, make_read_packet(client, 3))
                                    .and_then(move |(writer, _)| io::write_all(writer, id))
                            })
                            .and_then(move |(writer, _)| {
                                io::write_all(writer, make_read_packet(client, 2))
                                    .and_then(move |(writer, _)| io::write_all(writer, id))
                            })
                            .and_then(move |(writer, _)| {
                                io::write_all(writer, make_read_packet(client, 1))
                                    .and_then(move |(writer, _)| io::write_all(writer, id))
                            })
                            .and_then(move |(writer, _)| {
                                io::write_all(writer, make_read_packet(client, client))
                                    .and_then(move |(writer, _)| io::write_all(writer, id))
                            });

                        let reader = unsafe {
                            io::read_exact(reader, vec![0; read_len])
                                .and_then(move |(reader, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, 1), &[][..])
                                    );
                                    io::read_exact(reader, vec![0; read.len()])
                                })
                                .and_then(move |(reader, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, 2), &[][..])
                                    );
                                    io::read_exact(reader, vec![0; read_len])
                                })
                                .and_then(move |(reader, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, 3), &[][..])
                                    );
                                    io::read_exact(reader, vec![0; read_len])
                                })
                                .and_then(move |(reader, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, 2), &[][..])
                                    );
                                    io::read_exact(reader, vec![0; read_len])
                                })
                                .and_then(move |(reader, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, 1), &[][..])
                                    );
                                    io::read_exact(reader, vec![0; read_len])
                                })
                                .and_then(move |(_, read)| {
                                    assert_eq!(
                                        Packet::Ref::try_ref(&*read).unwrap(),
                                        (read_packet!(client, client), &[][..])
                                    );
                                    future::ok(())
                                })
                        };

                        reader.join(writer)
                    })
                    .map(|_| ())
                    .map_err(|e| panic!("{:?}", e))
            })
            .collect();

        current_thread::run(|_| {
            clients
                .drain(..)
                .for_each(|client| current_thread::spawn(client));
        });
    }

    fn make_read_packet(chain: u32, index: u32) -> Vec<u8> {
        let mut buffer = vec![];
        read_packet!(chain, index).fill_vec(&mut buffer);
        buffer
    }

    // fn make_read_response(chain: u32, index: u32) -> Vec<u8> {
    //     let mut buffer = vec![];
    //     read_packet!(chain, index).fill_vec(&mut buffer);
    //     buffer
    // }
}
