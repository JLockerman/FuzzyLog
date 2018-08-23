use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::thread;
use std::time::Duration;

use ::{bincode, mio, msg};

use bincode::Infinite;

use reactor::*;

pub fn work(addr: SocketAddr, window: usize, read: Arc<AtomicUsize>) {
    use std::io::ErrorKind;

    thread::sleep(Duration::from_secs(1));

    // let mut stream = mio::net::TcpStream::connect(&addr);
    // let stream = loop {
    //     let reconnect = match stream {
    //         Ok(s) => break s,
    //         Err(ref e) if e.kind() != ErrorKind::WouldBlock =>
    //             true,
    //         Err(..) => {thread::yield_now(); false},
    //     };
    //     if reconnect {
    //         stream = mio::net::TcpStream::connect(&addr)
    //     }

    // };

    let stream = mio::net::TcpStream::connect(&addr).expect("work gen cannot connect");
    let window = window as u64;

    let mut handler = TcpHandler::new(
        stream,
        ResponseReader,
        ResponseHandler(vec![], window, read)
    );
    for i in 0..window {
        let op = msg::Request::GetData(i, "/".into());
        let bytes = bincode::serialize(&op, Infinite).unwrap();
        handler.add_writes(&[&*bytes])
    }
    let mut generator = Generator::with_inner(1.into(), ()).expect("cannot start gen");
    generator.add_stream(2.into(), handler);
    generator.run().expect("cannot run gen");
}

///////////////////////////////////////

type Generator = Reactor<PerStream, ()>;
type PerStream = TcpHandler<ResponseReader, ResponseHandler>;

///////////////////

struct ResponseHandler(Vec<u8>, u64, Arc<AtomicUsize>);

impl MessageHandler<(), msg::Response> for ResponseHandler {
    fn handle_message(&mut self, io: &mut TcpWriter, _: &mut (), _message: msg::Response)
    -> Result<(), ()> {
        self.2.fetch_add(1, Relaxed);

        let i = self.1;
        self.1 += 1;
        let op = msg::Request::GetData(i, "/".into());

        self.0.clear();
        bincode::serialize_into(&mut self.0, &op, Infinite).unwrap();
        io.add_bytes_to_write(&[&*self.0]);
        Ok(())
    }
}

///////////////////

struct ResponseReader;

impl MessageReader for ResponseReader {
    type Message = msg::Response;
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
