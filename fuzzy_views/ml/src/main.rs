#![allow(deprecated)]

extern crate aho_corasick;
extern crate crossbeam;
extern crate env_logger;
extern crate fuzzy_log;
extern crate memmap;
extern crate mio;
extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::io::{self, Write, Read};
use std::collections::LinkedList;
use std::mem::forget;
use std::net::SocketAddr;
use std::process::Command;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{sleep, yield_now};
use std::time::{Instant, Duration};

use std::os::unix::io::{AsRawFd, FromRawFd};

use aho_corasick::{AcAutomaton, Automaton, Match};

use memmap::{Mmap, Protection};

use mio::net::TcpStream;
use mio::channel::{Sender, Receiver};

use structopt::StructOpt;

use fuzzy_log::async::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log::packets::order;

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
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum Mode {
    Write,
    ReadA,
    ReadM,
    PostA,
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
            "both" => Ok(Mode::BothB),
            _ => panic!("{} is not a valid mode, need one of [write, read_a, read_m, post]", s),
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
        Mode::ReadA | Mode::ReadM | Mode::PostA | Mode::BothB => {
            let vw_addr = "127.0.0.1:".to_string() + &args.vw_port.as_ref().unwrap();
            let vw_stream = Some(TcpStream::connect(&vw_addr.parse().unwrap())
                .expect("could not connect to vw"));
            vw_stream
        },
        Mode::Write => None,
    };
    let to_vw = to_vw;
    let from_vw = to_vw.as_ref().map(|sock| unsafe { TcpStream::from_raw_fd(sock.as_raw_fd()) });

    crossbeam::scope(move |scope| {

        let (to_acker, from_reader) = mio::channel::channel();
        let handle1;
        let mut handle2 = None;

        match args.mode {
            Mode::Write => {
                handle1 = scope.spawn(move || writer(done, args));
                scope.spawn(move || writer(done, args));
                scope.spawn(move || writer(done, args));
                scope.spawn(move || writer(done, args));
                scope.spawn(move || writer(done, args));
                scope.spawn(move || writer(done, args));
                println!("all write start");
            },
            Mode::ReadA | Mode::ReadM | Mode::PostA | Mode::BothB => {
                handle1 = scope.spawn(move ||
                    reader(to_vw.unwrap(), to_acker, start_all, done, args));
                handle2 = Some(scope.spawn(move ||
                    ack_vw(from_vw.unwrap(), from_reader, done)));
            },
        }

        while !start_all.load(Ordering::Relaxed) {
            yield_now()
        }
        println!("start");

        for _ in 0..args.num_rounds {
            sleep(Duration::from_millis(args.ms_per_round as u64));
        }
        if args.mode == Mode::Write {
            sleep(Duration::from_millis(10 * args.ms_per_round as u64));
        }

        done.store(true, Ordering::Relaxed);

        let events_seen = handle1.join();
        handle2.map(|h|{
            let out = h.join();
            print!("latency {:?} = [", args.mode);
            for round in &out {
                print!("{}, ", round.0.subsec_nanos());
            }
            println!("");
            print!("events {:?} = [", args.mode);
            for round in &out {
                print!("{}, ", round.1);
            }
            println!("]");
            println!("events_seen  {:?} = {:10}", args.mode, events_seen);
            let total_latency: u64 = out.iter().map(|r| r.0.subsec_nanos() as u64).sum();
            let avg_latency = total_latency / out.len() as u64;
            println!("avg_latency  {:?} = {:10} ns", args.mode, avg_latency);
            let total_events: i64 = out.iter().map(|r| r.1).sum();
            let total_ms = (args.ms_per_round * args.num_rounds) as f64;
            let avg_events = 1000.0 * (total_events as f64) / total_ms;
            println!("avg_events   {:?} = {:10.0} Hz", args.mode, avg_events);
            println!("total events {:?} = {:10}", args.mode, total_events);
        })
    });
    sleep(Duration::from_millis(10));
}

#[allow(dead_code)]
fn writer(done: &AtomicBool, args: &Args) -> u64 {
    let input = Mmap::open_path(args.input_file.as_ref().expect("no input file"), Protection::Read)
        .expect("cannot open input file.");
    let input = unsafe { input.as_slice() };
    let end_of_example = &[b"\r\n\r\n"];
    let searcher = AcAutomaton::new(end_of_example).into_full();
    let mut handle = LogHandle::unreplicated_with_servers(&[args.head_server])
        .chains(&[order::from(WAIT_CHAIN)])
        .build();

    wait_for_clients_start(&mut handle, args.num_handles);

    let mut outstanding = 0;
    let mut start = 0;
    let mut sent = 0;
    for Match{end, ..} in searcher.find(input) {
        if done.load(Ordering::Relaxed) {
            break
        }
        while outstanding > args.write_window {
            outstanding -= handle.flush_completed_appends().expect("cannot flush");
        }
        let filter = input[start];
        let chains = if filter == b'A' {
            [order::from(1), order::from(2)]
        } else if filter == b'M' {
            [order::from(1), order::from(3)]
        } else {
            panic!("bad tag {}", input[start] as char)
        };
        handle.async_no_remote_multiappend(&chains, &input[start+1..end], &[]);
        outstanding += 1;
        sent += 1;
        start = end;
    }
    handle.wait_for_all_appends().expect("log dies");
    sent
}

#[allow(dead_code)]
fn reader(
    mut to_vw: TcpStream,
    to_acker: Sender<(Instant, i64)>,
    start_all: &AtomicBool,
    done: &AtomicBool,
    args: &Args) -> u64
{
    let mut chains = match args.mode {
        // Mode::PostA | Mode::BothB => [order::from(1), 2.into(), 3.into()].to_vec(),
        Mode::PostA | Mode::BothB => [order::from(2), 3.into()].to_vec(),
        Mode::ReadA => [2.into()].to_vec(),
        Mode::ReadM => [3.into()].to_vec(),

        Mode::Write => unreachable!(),
    };
    chains.push(WAIT_CHAIN.into());
    let mut handle = LogHandle::<[u8]>::unreplicated_with_servers(&[args.head_server])
        .chains(&chains[..])
        .build();

    wait_for_clients_start(&mut handle, args.num_handles);

    //FIXME
    let mut started = false;
    let post_filter = args.mode == Mode::PostA;
    let mut events_recvd = 0;
    'work: while !done.load(Ordering::Relaxed) {
        let start_time = Instant::now();
        match args.mode {
            Mode::PostA | Mode::BothB => handle.take_snapshot(),
            Mode::ReadA => handle.snapshot(2.into()),
            Mode::ReadM => handle.snapshot(3.into()),
            Mode::Write => unreachable!(),
        }
        let mut num_events = 0;
        'recv: loop {
            let next = handle.get_next();
            if !started && next.is_ok() {
                started = true;
                start_all.store(true, Ordering::Relaxed);
            };
            match next {
                Ok((event, locs)) if post_filter => if locs[1].0 == order::from(2) {
                    blocking_write(&mut to_vw, event, done).expect("lost vw");
                    num_events += 1;
                },
                Ok((event, ..)) => {
                    blocking_write(&mut to_vw, event, done).expect("lost vw");
                    num_events += 1;
                },
                Err(GetRes::Done) => break 'recv,
                Err(r) => panic!("{:?}", r),
            }
        }
        let _ = to_vw.flush();
        events_recvd += num_events;
        if done.load(Ordering::Relaxed) { break 'work }
        if num_events > 0 {
            to_acker.send((start_time, num_events)).expect("lost acker");
        }
        // let latency = start_time.elapsed().subsec_nanos();
        if started {
            /* TODO
            completed_ops += 1;
            avg_latency.add_point(latency as usize);
            avg_latencies[level].store(avg_latency.val(), Ordering::Relaxed);
            */
        }
    }
    forget((to_vw, to_acker));
    events_recvd as u64
}

fn ack_vw(mut from_vw: TcpStream, from_reader: Receiver<(Instant, i64)>, done: &AtomicBool)
-> LinkedList<(Duration, i64)> {
    let poll = mio::Poll::new().expect("cannot poll");

    const FROM_VW: mio::Token = mio::Token(0);
    const FROM_READER: mio::Token = mio::Token(1);

    poll.register(&from_vw, FROM_VW, mio::Ready::readable(), mio::PollOpt::level())
        .expect("cannot register vw");
    poll.register(&from_reader, FROM_READER, mio::Ready::all(), mio::PollOpt::level())
        .expect("cannot register from_reader");

    let mut events = mio::Events::with_capacity(128);
    let mut acked_events = 0i64;
    let mut current_rounds: LinkedList<(Instant, i64)> = LinkedList::new();
    let mut finished_rounds = LinkedList::new();

    {
        /*let mut check_round_finished = |acked_events: &mut i64, current_round: &mut LinkedList<(Instant, i64)>| {
            while current_round.front().map_or(false, |&(_, events_needed)| {
                *acked_events >= events_needed
            }) {
                let (start_time, events_needed) = current_round.pop_front().unwrap();
                *acked_events = *acked_events - events_needed;
                let elapsed = start_time.elapsed();
                finished_rounds.push_back((elapsed, events_needed));
            }
        };*/

        let end_of_example = &[b"\n\n"];
        let searcher = AcAutomaton::new(end_of_example).into_full();

        let mut buffer = vec![0; 1024];
        let mut last_n = false;
        'poll_ack: while !done.load(Ordering::Relaxed) {
            poll.poll(&mut events, Some(Duration::from_millis(1))).expect("cannot poll");
            for event in &events {
                if done.load(Ordering::Relaxed) { break 'poll_ack }
                match event.token() {
                    FROM_VW => {
                        let chars = from_vw.read(&mut buffer[..]).unwrap_or(0);
                        if last_n && chars > 0 && buffer[0] == b'\n' {
                            acked_events += 1;
                        }
                        let mut last_end = None;
                        for Match{end, ..} in searcher.find(&buffer[..chars]) {
                            acked_events += 1;
                            last_end = Some(end);
                        }
                        match last_end.map(|e| e < chars && chars > 0 &&
                            !last_n && buffer[0] == b'\n') {
                            Some(true) => last_n = true,
                            Some(false) => last_n = false,
                            None => {},
                        }
                        // check_round_finished(&mut acked_events, &mut current_rounds);
                    },
                    FROM_READER => {
                        if let Ok(round) = from_reader.try_recv() {
                            // current_rounds.push_back(round);
                            // check_round_finished(&mut acked_events, &mut current_rounds);
                            let (start_time, events_needed) = round;
                            finished_rounds.push_back((start_time.elapsed(), events_needed));
                        }
                    },
                    _ => unimplemented!(),
                }
            }
        }
    }
    forget((poll, from_vw, from_reader));
    finished_rounds
}

fn blocking_write<W: Write>(w: &mut W, mut buffer: &[u8], done: &AtomicBool) -> io::Result<()> {
    //like Write::write_all but doesn't die on WouldBlock
    'recv: while !buffer.is_empty() {
        if done.load(Ordering::Relaxed) { return Ok(()) }
        match w.write(buffer) {
            Ok(i) => { let tmp = buffer; buffer = &tmp[i..]; }
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => {
                    yield_now();
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

/////////////////////////

const WAIT_CHAIN: u32 = 10_000;

fn wait_for_clients_start(
    log: &mut LogHandle<[u8]>, num_clients: usize
) {
    log.append(WAIT_CHAIN.into(), &[], &[]);
    let mut clients_started = 0;
    while clients_started < num_clients {
        log.snapshot(WAIT_CHAIN.into());
        while let Ok(..) = log.get_next() { clients_started += 1 }
    }
}
