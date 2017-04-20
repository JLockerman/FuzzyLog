
extern crate mio;
extern crate fuzzy_log;

use fuzzy_log::packets::*;
use fuzzy_log::buffer::Buffer;

use mio::net::TcpStream;
//use mio::Poll;

use std::io::{self, Write, Read};
use std::time::{Duration, Instant};
use std::thread;

fn main() {
    let mut args = ::std::env::args().skip(1);
    let mut to_server0 = TcpStream::connect(&args.next().unwrap().parse().unwrap()).unwrap();
    let mut to_server1 = TcpStream::connect(&args.next().unwrap().parse().unwrap()).unwrap();
    to_server0.set_nodelay(true).unwrap();
    to_server1.set_nodelay(true).unwrap();
    // let poll = Poll::new().unwrap();
    // poll.register(
    //     &to_server0,
    //     mio::Token(1),
    //     mio::Ready::readable() | mio::Ready::writable(),
    //     mio::PollOpt::edge(),
    // ).unwrap();
    // poll.register(
    //     &to_server1,
    //     mio::Token(1),
    //     mio::Ready::readable() | mio::Ready::writable(),
    //     mio::PollOpt::edge()
    // ).unwrap();

    let client_id = Uuid::new_v4();
    // println!("client {:?}\n", client_id);
    // println!("write  {:?}", write_id);

    blocking_write(&mut to_server0, &*client_id.as_bytes()).unwrap();
    blocking_write(&mut to_server1, &*client_id.as_bytes()).unwrap();

    let mut run_exp = || {
        let write_id = Uuid::new_v4();

        let mut skeens = vec![];
        EntryContents::Multi{
            id: &write_id,
            flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock),
            lock: &0,
            locs: &[OrderIndex(2.into(), 0.into()), OrderIndex(3.into(), 0.into())],
            deps: &[],
            data: &[94, 49, 0xff],
        }.fill_vec(&mut skeens);
        skeens.extend_from_slice(&*client_id.as_bytes());

        let mut fence0_and_update = vec![];
        EntryContents::FenceClient{
            fencing_write: &write_id,
            client_to_fence: &client_id,
            fencing_client: &client_id,
        }.fill_vec(&mut fence0_and_update);
        fence0_and_update.extend_from_slice(&*client_id.as_bytes());
        EntryContents::UpdateRecovery {
            old_recoverer: &Uuid::nil(),
            write_id: &write_id,
            flags: &EntryFlag::Nothing,
            lock: &0,
            locs: &[OrderIndex(2.into(), 0.into()), OrderIndex(3.into(), 0.into())],
        }.fill_vec(&mut fence0_and_update);
        fence0_and_update.extend_from_slice(&*client_id.as_bytes());

        let mut fence1 = vec![];
        EntryContents::FenceClient{
            fencing_write: &write_id,
            client_to_fence: &client_id,
            fencing_client: &client_id,
        }.fill_vec(&mut fence1);
        fence1.extend_from_slice(&*client_id.as_bytes());

        let mut check_skeens1 = vec![];
        EntryContents::CheckSkeens1 {
            id: &write_id,
            flags: &EntryFlag::Nothing,
            data_bytes: &0,
            dependency_bytes: &0,
            loc: &OrderIndex(0.into(), 0.into()),
        }.fill_vec(&mut check_skeens1);
        check_skeens1.extend_from_slice(&*client_id.as_bytes());

        let mut buffer = Buffer::new();

        blocking_write(&mut to_server0, &skeens).unwrap();
        recv_packet(&mut buffer, &to_server0);

        // println!("a");

        let start = Instant::now();

        let reason = buffer.contents().locs()[0];
        let ts0: u32 = reason.1.into();
        assert!(ts0 > 0);

        bytes_as_entry_mut(&mut check_skeens1).locs_mut()[0] = reason;

        blocking_write(&mut to_server0, &fence0_and_update).unwrap();
        blocking_write(&mut to_server1, &fence1).unwrap();

        // println!("b");

        recv_packet(&mut buffer, &to_server0);
        match buffer.contents() {
            EntryContents::FenceClient{fencing_write, client_to_fence, fencing_client,} => {
                assert_eq!(fencing_write, &write_id);
                assert_eq!(client_to_fence, &client_id);
                assert_eq!(fencing_client, &client_id);
            }
            EntryContents::UpdateRecovery{flags, ..} =>
                assert!(flags.contains(EntryFlag::ReadSuccess)),
            c => panic!("{:?}", c),
        }

        // println!("c");

        recv_packet(&mut buffer, &to_server1);
        match buffer.contents() {
            EntryContents::FenceClient{fencing_write, client_to_fence, fencing_client,} => {
                assert_eq!(fencing_write, &write_id);
                assert_eq!(client_to_fence, &client_id);
                assert_eq!(fencing_client, &client_id);
            }
            _ => unreachable!(),
        }

        // println!("d");

        recv_packet(&mut buffer, &to_server0);
        match buffer.contents() {
            EntryContents::UpdateRecovery{flags, ..} =>
                assert!(flags.contains(EntryFlag::ReadSuccess)),
            EntryContents::FenceClient{fencing_write, client_to_fence, fencing_client,} => {
                assert_eq!(fencing_write, &write_id);
                assert_eq!(client_to_fence, &client_id);
                assert_eq!(fencing_client, &client_id);
            }
            _ => unreachable!(),
        }

        // println!("e");

        blocking_write(&mut to_server0, &check_skeens1).unwrap();
        recv_packet(&mut buffer, &to_server0);
        match buffer.contents() {
            EntryContents::CheckSkeens1{id, flags, loc, ..} => {
                assert_eq!(id, &write_id);
                assert_eq!(loc, &reason);
                assert!(flags.contains(EntryFlag::ReadSuccess));
            }
            _ => unreachable!(),
        }

        // println!("f");
        //skeens1 is idempotent so we can use it as a TAS
        blocking_write(&mut to_server1, &skeens).unwrap();
        recv_packet(&mut buffer, &to_server1);
        let ts1: u32 = buffer.contents().locs()[1].1.into();
        assert!(ts1 > 0);

        // println!("g");

        {
            let mut e = bytes_as_entry_mut(&mut skeens);
            *e.lock_mut() = ::std::cmp::max(ts0 as u64, ts1 as u64);
            e.flag_mut().insert(EntryFlag::Unlock);
        }
        blocking_write(&mut to_server0, &skeens).unwrap();
        blocking_write(&mut to_server1, &skeens).unwrap();

        recv_packet(&mut buffer, &to_server0);
        // println!("h");
        recv_packet(&mut buffer, &to_server1);

        // println!("i");

        let elapsed = start.elapsed();
        println!("{:?}", elapsed);
        elapsed
    };

    let mut data_points = Vec::with_capacity(1000);
    for _ in 0..data_points.capacity() {
        let point = run_exp();
        data_points.push(point);
    }

    let sum: Duration = data_points.iter().sum();
    let avg: Duration = sum / (data_points.len() as u32);
    println!("avg of {:?} runs\n\t {:?}", data_points.len(), avg);
}

fn blocking_write<W: Write>(w: &mut W, mut buffer: &[u8]) -> io::Result<()> {
    //like Write::write_all but doesn't die on WouldBlock
    'recv: while !buffer.is_empty() {
        match w.write(buffer) {
            Ok(i) => { let tmp = buffer; buffer = &tmp[i..]; }
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => {
                    thread::yield_now();
                    continue 'recv
                },
                _ => { return Err(e) }
            }
        }
    }
    if !buffer.is_empty() {
        return Err(io::Error::new(io::ErrorKind::WriteZero,
            "failed to fill whole buffer"))
    }
    else {
        return Ok(())
    }
}

fn recv_packet(buffer: &mut Buffer, mut stream: &TcpStream) {
    use fuzzy_log::packets::Packet::WrapErr;
    let mut read = 0;
    loop {
        let to_read = buffer.finished_at(read);
        let size = match to_read {
            Err(WrapErr::NotEnoughBytes(needs)) => needs,
            Err(err) => panic!("{:?}", err),
            Ok(size) if read < size => size,
            Ok(..) => return,
        };
        let r = stream.read(&mut buffer[read..size]);
        match r {
            Ok(i) => read += i,

            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => {
                    thread::yield_now();
                    continue
                },
                _ => panic!("recv error {:?}", e),
            }
        }
    }
}
