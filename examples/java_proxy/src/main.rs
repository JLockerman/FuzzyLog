extern crate byteorder;
extern crate env_logger;
extern crate fuzzy_log_client;
extern crate mio;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::io::{Read, Write, BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener};

use byteorder::*;

use fuzzy_log_client::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log_client::packets::order;

//use mio::net::TcpStream;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "proxy", about = "")]
struct Args {
    #[structopt(help = "FuzzyLog servers to run against.")]
    servers: Vec<SocketAddr>,

    #[structopt(short="p", long="port", help = "port to listen on.", default_value="13336")]
    append_port: u16,

    #[structopt(short="y", long="sync", help = "number of clients to wait for.")]
    sync_clients: Option<usize>,
}

fn main() {
    let _ = env_logger::init();
    let args @ Args{..} = StructOpt::from_args();

    let addr = SocketAddr::from(([127, 0, 0, 1], args.append_port));
    let listener = TcpListener::bind(addr).expect("could not listen");
    let mut append = BufReader::new(listener.accept().expect("cannot accept append").0);
    let recv = listener.accept().expect("cannot accept recv").0;
    //let mut read_recv = BufReader::with_capacity(512, &recv);
    let mut read_recv = &recv;
    let mut write_recv = BufWriter::new(&recv);
    // let mut recv = BufReader::with_capacity(512, listener.accept().unwrap().0);

    let num_chains = append.read_i32::<BigEndian>().expect("cannot get num interesting chains")
        as usize;
    let mut chains = Vec::with_capacity(num_chains);

    for _ in 0..num_chains {
        let chain = append.read_i32::<BigEndian>().expect("cannot get interesting chain") as u32;
        chains.push(chain.into());
    }
    chains.push(10_001.into());


    //FIXME
    let snap_chain = chains[0];
    let handle = LogHandle::<[u8]>::unreplicated_with_servers(&args.servers[..])
        .chains(&chains[..])
        .reads_my_writes()
        .build();

    let (mut reader, mut writer) = handle.split();

    if let Some(num_sync_clients) = args.sync_clients {
        writer.async_append(10_001.into(), &[], &[]);
        let mut clients_started = 0;
        while clients_started < num_sync_clients {
            reader.snapshot(10_001.into());
            while let Ok(..) = reader.get_next() {
                clients_started += 1
            }
        }
        write_recv.get_mut().write_u8(0).expect("cannot write sync signal");
        let _ = write_recv.get_mut().flush();
        let _ = writer.flush_completed_appends();
    }

    let mut read_buffer = vec![0u8; 1024];
    let mut append_chains: Vec<order> = Vec::with_capacity(128);
    ::std::thread::spawn(move || loop {
        let num_chains = append.read_i32::<BigEndian>().expect("cannot get num chains");
        if num_chains > 0 {
            let size = append.read_i32::<BigEndian>().expect("cannot get size") as usize;
            append.read_exact(&mut read_buffer[..size]).expect("cannot get data");

            writer.async_append(order::from(num_chains as u32), &read_buffer[..size], &[]);
        } else {
            let num_chains = -num_chains as usize;
            for _ in 0..num_chains {
                let chain = append.read_i32::<BigEndian>().expect("M cannot get chain") as u32;
                append_chains.push(chain.into());
            }
            let size = append.read_i32::<BigEndian>().expect("M cannot get size") as usize;
            append.read_exact(&mut read_buffer[..size]).expect("M cannot get data");
            assert!(append_chains.len() > 0);
            if append_chains.len() > 1 {
                writer.async_no_remote_multiappend(&append_chains[..], &read_buffer[..size], &[]);
            } else {
                writer.async_append(append_chains[0], &read_buffer[..size], &[]);
            }
            append_chains.clear();
        }
        let _ = writer.flush_completed_appends();
    });

    loop {
        read_recv.read_u8().expect("cannot get snap signal");
        reader.snapshot(snap_chain);

        'recv: loop {
            {
                let data = reader.get_next();
                match data {
                    Ok((data, ..)) => {
                        write_recv.get_mut().write_i32::<BigEndian>(data.len() as i32)
                            .expect("cannot send data size");
                        write_recv.get_mut().write_all(data).expect("cannot send data");
                    }
                    Err(GetRes::NothingReady) => continue 'recv,
                    Err(GetRes::Done) => break 'recv,
                    e @ Err(GetRes::IoErr(..)) | e @ Err(GetRes::AlreadyGCd(..)) =>
                        panic!("{:?}", e),
                }
            }
            'poll: loop {
                let data2 = reader.try_get_next();
                match data2 {
                    Ok((data, ..)) => {
                        write_recv.get_mut().write_i32::<BigEndian>(data.len() as i32)
                            .expect("P cannot send data size");
                        write_recv.get_mut().write_all(data).expect("P cannot send data");
                    }
                    Err(GetRes::NothingReady) => break 'poll,
                    Err(GetRes::Done) => break 'recv,

                    e @ Err(GetRes::IoErr(..)) | e @ Err(GetRes::AlreadyGCd(..)) =>
                        panic!("{:?}", e),
                }
            }
            let _ = write_recv.get_mut().flush();
        }
        write_recv.get_mut().write_i32::<BigEndian>(0).expect("cannot write get done");
        let _ = write_recv.get_mut().flush();
    }
}
