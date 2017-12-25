#![allow(deprecated)]

extern crate aho_corasick;
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log;
extern crate memmap;
extern crate mio;
extern crate rand;
extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Write, BufWriter, Read, BufReader};
use std::mem::forget;
use std::net::{SocketAddr, TcpStream};
use std::process::Command;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{sleep, yield_now};
use std::time::Duration;

use std::os::unix::io::{AsRawFd, FromRawFd};

use aho_corasick::{AcAutomaton, Automaton, Match};

use memmap::{Mmap, Protection};

use rand::{OsRng, Rng};

use structopt::StructOpt;

use fuzzy_log::async::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log::packets::order;

// mod buffered;

#[derive(StructOpt, Debug)]
#[structopt(name = "ml", about = "ml benchmark.")]
struct Args {
    #[structopt(help = "head of the FuzzyLog server chain to run against.")]
    head_server: SocketAddr,

    #[structopt(help = "Mode to run tester instance.")]
    mode: Mode,

    #[structopt(short="v", long="vw_port", help = "vowpal wabbit port.")]
    vw_port: Option<String>,


    #[structopt(short="i", long="input_file", help = "file of training data.")]
    input_file: Option<String>,

    #[structopt(short="w", long="write_window", help = "", default_value = "30")]
    write_window: usize,

    #[structopt(short="t", long="time_per_round", help = "number of milliseconds per round.", default_value = "1000")]
    ms_per_round: usize,

    #[structopt(short="r", long="rounds", help = "number of rounds.", default_value = "10")]
    num_rounds: usize,

    #[structopt(short="n", long="num_writers", help = "num_writers.")]
    num_handles: usize,

    #[structopt(short="m", long="multiplier", help = "logical writers per writer.")]
    multiplier: usize,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum Mode {
    Write,
    ReadA,
    ReadM,
    PostA,
    PostB,
    BothB,
}

impl FromStr for Mode {
    type Err = ::std::string::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "write" => Ok(Mode::Write),
            "read_a" => Ok(Mode::ReadA),
            "read_m" => Ok(Mode::ReadM),
            "post" => Ok(Mode::PostA),
            "postB" => Ok(Mode::PostB),
            "both" => Ok(Mode::BothB),
            _ => {
                let level: Result<usize, _> = s.parse();
                match level {
                    Ok(0) => Ok(Mode::ReadA),
                    Ok(1) => Ok(Mode::ReadM),
                    Ok(2) => Ok(Mode::PostA),
                    Ok(3) => Ok(Mode::PostB),
                    Ok(4) => Ok(Mode::BothB),
                    _ => panic!("{} is not a valid mode, need one of [write, read_a, read_m, post]", s),
                }

            },
        }
    }
}

fn main() {
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();
    if args.mode != Mode::Write {
        Command::new("./vw")
            .args([
                &b"--daemon"[..],
                &b"--port"[..], &args.vw_port.as_ref().expect("need vw port").as_bytes(),
                &b"--cb_explore_adf"[..],
                &b"--rank_all"[..],
                &b"--interact"[..], &b"u\x87"[..],//0x87
                &b"--cb_type"[..], &b"ips"[..],
                &b"-l"[..],  &b"0.005"[..],
                &b"--dictionary_path"[..], &b"."[..],
                &b"--ignore"[..], &b"d"[..],
                // "--epsilon", "0.1",
                &b"--quiet"[..],
            ].into_iter().map(|b| OsStr::from_bytes(b)))
            .status()
            .expect("could not start vw");
        println!("started vw");
    }

    sleep(Duration::from_millis(10));

    // let stream = TcpStream::connect(&("127.0.0.1:".to_string() + &args.vw_port.as_ref().unwrap()).parse().unwrap())
    //     .expect("could not connect to vw");

    // let mut ack = String::with_capacity(64);
    // let mut reader = BufReader::new(&stream);
    //let mut writer = &stream;
    // for _ in 0..100 {
    //     writer.write(b"| l1:20 l2:10 \n").expect("cannot write");
    //     reader.read_line(&mut ack).expect("cannot read");
    //     println!("ack: {:?}", ack);
    //     ack.clear();
    // }

    let start_all = AtomicBool::new(false);

    let done = AtomicBool::new(false);

    let (start_all, done) = (&start_all, &done);

    let args = &args;
    let to_vw = match args.mode {
        Mode::ReadA | Mode::ReadM | Mode::PostA | Mode::PostB | Mode::BothB => {
            let vw_addr = "127.0.0.1:".to_string() + &args.vw_port.as_ref().unwrap();
            let vw_stream = TcpStream::connect(vw_addr).expect("could not connect to vw");
            // vw_stream.set_read_timeout(Some(Duration::from_millis(1))).unwrap();
            Some(vw_stream)
        },
        Mode::Write => None,
    };
    let to_vw = to_vw;
    let from_vw = to_vw.as_ref().map(|sock| unsafe { TcpStream::from_raw_fd(sock.as_raw_fd()) });
    from_vw.as_ref().map(|f| f.set_read_timeout(Some(Duration::from_millis(100))).unwrap());

    crossbeam::scope(move |scope| {
        let handle1;
        let mut handle2 = None;

        match args.mode {
            Mode::Write => {
                handle1 = scope.spawn(move || writer(start_all, done, args));
                for _ in 1..args.multiplier {
                   scope.spawn(move || writer(start_all, done, args));
                }
                // println!("all write start");
            },
            Mode::ReadA | Mode::ReadM | Mode::PostA | Mode::PostB | Mode::BothB => {
                handle1 = scope.spawn(move ||
                    // reader(BufWriter::new(to_vw.unwrap()), start_all, done, args));
                    reader(to_vw.unwrap(), start_all, done, args));
                handle2 = Some(scope.spawn(move ||
                    // ack_vw(BufReader::new(from_vw.unwrap()), done)));
                    ack_vw(from_vw.unwrap(), done)));
            },
        }

        while !start_all.load(Ordering::Relaxed) {
            yield_now()
        }

        for _ in 0..args.num_rounds {
            sleep(Duration::from_millis(args.ms_per_round as u64));
        }

        if args.mode == Mode::Write {
            sleep(Duration::from_millis(args.ms_per_round as u64));
        }

        if args.mode == Mode::BothB {
            println!("{:?} Done", Mode::BothB);
        }

        done.store(true, Ordering::Relaxed);

        let (events_seen, mut syncs) = handle1.join();
        println!("{:?} {:?}", args.mode, (events_seen, syncs.len()));
        handle2.map(|h| {
            let mut completed_examples = h.join();
            println!("{:?} {:?}", args.mode, completed_examples);
            let mut completed_syncs = 0;
            while completed_examples > 0 {
                match syncs.pop_front() {
                    Some(sync_size) => {
                        if completed_examples < sync_size as u64 { break }
                        completed_examples -= sync_size as u64;
                        completed_syncs += 1;
                    },
                    None => break,
                }
            }
            let microseconds_per_sync = (10_000_000.0 / completed_syncs as f64) as f64;
            println!("> {:?} {:?}", args.mode, microseconds_per_sync);
        })
    });
    sleep(Duration::from_millis(10));
}

fn num_handles(args: &Args) -> usize {
    (args.num_handles - 5) * args.multiplier + 5
}

#[allow(dead_code)]
fn writer(start_all: &AtomicBool, done: &AtomicBool, args: &Args) -> (u64, VecDeque<u32>) {
    let input = Mmap::open_path(args.input_file.as_ref().expect("no input file"), Protection::Read)
        .expect("cannot open input file.");

    let end_of_example = &[b"\r\n\r\n"];
    let searcher = AcAutomaton::new(end_of_example).into_full();

    let input = unsafe { input.as_slice() };

    let start = {
        let start = OsRng::new().unwrap().gen_range(0, 20) * 50_000 * 8318;
        let input = &input[start..];
        let mut finds = searcher.find(input);
        let Match {end,..} = finds.next().unwrap();
        start + end
    };

    let input = &input[start..];


    let mut handle = LogHandle::unreplicated_with_servers(&[args.head_server])
        .chains(&[order::from(WAIT_CHAIN)])
        .build();

    wait_for_clients_start(&mut handle, num_handles(args));
    start_all.store(true, Ordering::Relaxed);

    let mut outstanding = 0;
    let mut sent = 0;
    let mut start = 0;
    'send: while !done.load(Ordering::Relaxed) {
        for Match{end, ..} in searcher.find(input) {
            let ackd = handle.flush_completed_appends().expect("cannot flush");
            outstanding -= ackd;
            sent += ackd as u64;
            if done.load(Ordering::Relaxed) { break 'send }
            while outstanding > args.write_window {
                let ackd = handle.flush_completed_appends().expect("cannot flush");
                outstanding -= ackd;
                sent += ackd as u64;
            }
            let filter = input[start];
            let chains = if filter == b'A' {
                [order::from(1), order::from(2)]
            } else if filter == b'M' {
                [order::from(1), order::from(3)]
            } else {
                panic!("bad tag {} @ {}", input[start] as char, start)
            };
            handle.async_no_remote_multiappend(&chains, &input[start+1..end], &[]);
            outstanding += 1;
            start = end;
        }
        // if !done.load(Ordering::Relaxed) { println!("loop"); }
    }
    sent += handle.flush_completed_appends().expect("cannot flush") as u64;
    let total_send_time = args.num_rounds as u64 + 10;
    let hz = sent / total_send_time;
    println!("> w {:?}", hz);
    (sent, VecDeque::new())
}

#[allow(dead_code)]
fn reader<W: Write>(
    mut to_vw: W,
    start_all: &AtomicBool,
    done: &AtomicBool,
    args: &Args) -> (u64, VecDeque<u32>)
{
    let mut syncs = VecDeque::with_capacity(1_000_000);
    let mut chains = match args.mode {
        // Mode::PostA | Mode::BothB => [order::from(1), 2.into(), 3.into()].to_vec(),
        Mode::PostA | Mode::PostB | Mode::BothB => [order::from(2), 3.into()].to_vec(),
        Mode::ReadA => [2.into()].to_vec(),
        Mode::ReadM => [3.into()].to_vec(),

        Mode::Write => unreachable!(),
    };
    chains.push(WAIT_CHAIN.into());
    let mut handle = LogHandle::<[u8]>::unreplicated_with_servers(&[args.head_server])
        .chains(&chains[..])
        .build();

    wait_for_clients_start(&mut handle, num_handles(args));

    //FIXME
    let mut started = false;
    let mut events_recvd = 0u64;
    'work: while !done.load(Ordering::Relaxed) {
        match args.mode {
            Mode::PostA | Mode::PostB | Mode::BothB => handle.take_snapshot(),
            Mode::ReadA => handle.snapshot(2.into()),
            Mode::ReadM => handle.snapshot(3.into()),
            Mode::Write => unreachable!(),
        }
        let mut num_events = 0u32;
        'recv: loop {
            let next = handle.get_next();
            if !started && next.is_ok() {
                started = true;
                start_all.store(true, Ordering::Relaxed);
            };
            match next {
                Ok((event, locs)) if args.mode == Mode::PostA => {
                    assert_eq!(locs.len(), 2, "non multi? {:?}", locs);
                    if locs[1].0 == order::from(2) {
                        let finished_write = write_all(&mut to_vw, event, done).expect("lost vw");
                        if !finished_write { break 'recv }
                        num_events += 1;
                    }
                },
                Ok((event, locs)) if args.mode == Mode::PostB => if locs[1].0 == order::from(3) {
                    let finished_write = write_all(&mut to_vw, event, done).expect("lost vw");
                    if !finished_write { break 'recv }
                    num_events += 1;
                },
                Ok((event, ..)) => {
                    let finished_write = write_all(&mut to_vw, event, done).expect("lost vw");
                    if !finished_write { break 'recv }
                    num_events += 1;
                },
                Err(GetRes::Done) => break 'recv,
                Err(r) => panic!("{:?}", r),
            }
        }
        let _ = to_vw.flush();
        events_recvd += num_events as u64;
        syncs.push_front(num_events);
        if done.load(Ordering::Relaxed) { break 'work }
    }
    forget(to_vw);
    (events_recvd, syncs)
}

fn ack_vw(mut from_vw: TcpStream,  done: &AtomicBool) -> u64 {
    let mut finished_rounds = 0u64;

    // let mut buffer = String::with_capacity(1024);
    let mut buffer = vec![0u8; 1024].into_boxed_slice();
    let mut pos = 0;
    let mut len = 0;
    let end_of_ack = &[b"\n\n"];
    let searcher = AcAutomaton::new(end_of_ack).into_full();
    while !done.load(Ordering::Relaxed) {
        for Match{end, ..} in searcher.find(&buffer[pos..len]) {
            pos = end;
            finished_rounds += 1
        }
        if pos >= len {
            pos = 0;
            len = match from_vw.read(&mut buffer) {
                Ok(s) => s,
                Err(ref e) if e.kind() == IoErrorKind::Interrupted => 0,
                Err(ref e) if e.kind() == IoErrorKind::WouldBlock => 0,
                Err(e) => panic!("vw read {:?}", e),
            };
        } else if len > 0 {
            buffer[0] = buffer[len - 1];
            pos = 0;
            len = 1 + match from_vw.read(&mut buffer[1..]) {
                Ok(s) => s,
                Err(ref e) if e.kind() == IoErrorKind::Interrupted => 0,
                Err(ref e) if e.kind() == IoErrorKind::WouldBlock => 0,
                Err(e) => panic!("vw read {:?}", e),
            };
        } else {
            pos = 0;
            len = match from_vw.read(&mut buffer) {
                Ok(s) => s,
                Err(ref e) if e.kind() == IoErrorKind::Interrupted => 0,
                Err(ref e) if e.kind() == IoErrorKind::WouldBlock => 0,
                Err(e) => panic!("vw read {:?}", e),
            };
        }
        // let read = from_vw.read_line(&mut buffer).expect("lost vw");
        // if read == 1 { finished_rounds += 1 }
        // buffer.clear();
    }
    forget(from_vw);

    finished_rounds
}

fn write_all<W: Write>(w: &mut W, mut buf: &[u8], done: &AtomicBool) -> Result<bool, IoError> {
    let mut finished_normal = true;
    while !buf.is_empty() {
        if done.load(Ordering::Relaxed) {
            finished_normal = false;
            break
        }
        match w.write(buf) {
            Ok(0) => return Err(IoError::new(IoErrorKind::WriteZero, "failed to write whole buffer")),
            Ok(n) => buf = &buf[n..],
            Err(ref e) if e.kind() == IoErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(finished_normal)
}

/////////////////////////

const WAIT_CHAIN: u32 = 10_000;

fn wait_for_clients_start(
    log: &mut LogHandle<[u8]>, num_clients: usize
) {
    println!("waiting for {} handles", num_clients);
    log.append(WAIT_CHAIN.into(), &[], &[]);
    let mut clients_started = 0;
    while clients_started < num_clients {
        log.snapshot(WAIT_CHAIN.into());
        while let Ok(..) = log.get_next() { clients_started += 1 }
    }
}
