use fuzzy_log::async::fuzzy_log::log_handle::{LogHandle, GetRes};
use fuzzy_log::packets::SingletonBuilder;

use rand::{SeedableRng, XorShiftRng as RandGen, Rng};
use rand::distributions::Sample;
use rand::distributions::range::Range as RandRange;

use std::iter;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
use std::time::Instant;
use std::thread;
use std::u32;

///////////////////////////////////////

pub fn run_unreplicated_write_read(
    server_addrs: Box<[SocketAddr]>,
    clients_to_run: usize,
    total_clients: usize,
    jobsize: usize,
    num_writes: u32,
    write_window: u32,
    random_seed: Option<[u32; 4]>
) {
    println!(
        "# Starting {}/{} clients with jobsize {} for {} iterations running against:\n#\t{:?}",
        clients_to_run, total_clients, jobsize, num_writes, server_addrs,
    );

    if let Some(seed) = random_seed {
        println!("# with random spray, seed: {:?}.", seed);
    }

    let clients = (0..clients_to_run).map(|_| {
        let admin_chain = (u32::MAX-1).into();
        let num_chains = total_clients as u32;
        let chains = (1..(num_chains*3 + 1)).map(Into::into);
        let chains = chains.chain(iter::once(admin_chain));
        if server_addrs.len() > 1 {
                LogHandle::spawn_tcp_log(
                server_addrs[0],
                server_addrs[1..].iter().cloned(),
                chains,
            )
        } else {
            //FIXME let lock_server be optional
            LogHandle::spawn_tcp_log(
                server_addrs[0],
                server_addrs.iter().cloned(),
                chains,
            )
        }
    }).collect();
    write_read(clients, total_clients, jobsize, num_writes, write_window, random_seed)
}

pub fn run_replicated_write_read(
    lock_addr: Option<SocketAddr>,
    server_addrs: Vec<(SocketAddr, SocketAddr)>,
    clients_to_run: usize,
    total_clients: usize,
    jobsize: usize,
    num_writes: u32,
    write_window: u32,
    random_seed: Option<[u32; 4]>
) {
    println!(
        "# Starting {}/{} clients with jobsize {} for {} iterations running against:\n#\t{:?}",
        clients_to_run, total_clients, jobsize, num_writes, server_addrs,
    );

    if let Some(seed) = random_seed {
        println!("# with random spray, seed: {:?}.", seed);
    }

    let clients = (0..clients_to_run).map(|_| {
        let admin_chain = (u32::MAX-1).into();
        let num_chains = total_clients as u32;
        let chains = (1..(num_chains*3 + 1)).map(Into::into);
        let chains = chains.chain(iter::once(admin_chain));
        LogHandle::tcp_log_with_replication(
            lock_addr,
            server_addrs.iter().cloned(),
            chains,
        )
    }).collect();
    write_read(clients, total_clients, jobsize, num_writes, write_window, random_seed)
}

//TODO pass in data gathering function(s)
fn write_read(
    mut clients: Vec<LogHandle<[u8]>>,
    total_clients: usize,
    jobsize: usize,
    num_writes: u32,
    write_window: u32,
    random_seed: Option<[u32; 4]>
) {
    let clients_to_run = clients.len();
    static WRITERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;
    static READERS_READY: AtomicUsize = ATOMIC_USIZE_INIT;

    let start = Instant::now();
    let joins: Vec<_> = clients.drain(..).enumerate().map(|(client_num, handle)| {
        thread::spawn(move || {
            let mut handle = handle;
            let admin_chain = (u32::MAX-1).into();
            let num_chains = total_clients as u32;
            let data = vec![0u8; jobsize];

            let mut rand = random_seed.map(RandGen::from_seed);
            let range = RandRange::new(1, num_chains+1);
            let mut get_write_chain = move |mut range: RandRange<u32>|
                if let Some(ref mut rand) = rand {
                     let sample: u32 = range.sample(rand);
                     sample.into()
                } else {
                    ((client_num + 1) as u32).into()
                };
            let read_chain = ((client_num + 1) as u32).into();

            if total_clients > clients_to_run {
                //TODO
                let mut num_clients_started = 0;
                handle.append(admin_chain, &[][..], &[]);
                while num_clients_started < total_clients {
                    handle.snapshot(admin_chain);
                    while handle.get_next().is_ok() {
                        num_clients_started += 1;
                    }
                }
            }

            WRITERS_READY.fetch_add(1, Ordering::SeqCst);
            while WRITERS_READY.load(Ordering::SeqCst) < clients_to_run {
                thread::yield_now()
            }

            trace!("client {} starting write.", client_num);

            let mut sent = 0;
            let mut current_writes = 0;
            while sent < num_writes {
                if current_writes < write_window {
                    handle.async_append(
                        get_write_chain(range),
                        &*data,
                        &[]
                    );
                    sent += 1;
                    current_writes += 1;
                }
                current_writes -= handle.flush_completed_appends().unwrap() as u32;
            }
            let _ = handle.wait_for_all_appends();

            let write_start = Instant::now();
            let mut sent = 0;
            let mut current_writes = 0;
            while sent < num_writes {
                if current_writes < write_window {
                    handle.async_append(
                        get_write_chain(range) + num_chains,
                        &*data,
                        &[]
                    );
                    sent += 1;
                    current_writes += 1;
                }
                current_writes -= handle.flush_completed_appends().unwrap() as u32;
            }
            let _ = handle.wait_for_all_appends();

            let write_time = write_start.elapsed();
            let mut sent = 0;
            let mut current_writes = 0;
            while sent < num_writes {
                if current_writes < write_window {
                    handle.async_append(
                        get_write_chain(range),
                        &*data,
                        &[]
                    );
                    sent += 1;
                    current_writes += 1;
                }
                current_writes -= handle.flush_completed_appends().unwrap() as u32;
            }
            let _ = handle.wait_for_all_appends();

            trace!("client {} finished write.", client_num);

            ////////////////////////////////////////

            if total_clients > clients_to_run {
                //TODO
                let mut num_clients_started = 0;
                handle.append(admin_chain, &[][..], &[]);
                while num_clients_started < total_clients {
                    handle.snapshot(admin_chain);
                    while handle.get_next().is_ok() {
                        num_clients_started += 1;
                    }
                }
            }

            READERS_READY.fetch_add(1, Ordering::SeqCst);
            while READERS_READY.load(Ordering::SeqCst) < clients_to_run {
                thread::yield_now()
            }

            trace!("client {} starting read.", client_num);

            handle.snapshot(read_chain);
            while let Ok(..) = handle.get_next() {}

            let read_start = Instant::now();
            handle.snapshot(read_chain + num_chains);
            let mut num_entries = 0usize;
            while let Ok(..) = handle.get_next() {
                num_entries += 1;
            }
            let read_time = read_start.elapsed();

            handle.snapshot(read_chain + 2 * num_chains);
            while let Ok(..) = handle.get_next() {}

            trace!("client {} finished read.", client_num);

            let write_s = write_time.as_secs() as f64 + (write_time.subsec_nanos() as f64 * 10.0f64.powi(-9));
            let read_s = read_time.as_secs() as f64 + (read_time.subsec_nanos() as f64 * 10.0f64.powi(-9));
            let write_hz = num_writes as f64 / write_s;
            let read_hz = num_entries as f64 / read_s;
            let write_bits = write_hz * (data.len() * 8) as f64;
            let read_bits = read_hz * (data.len() * 8) as f64;
            println!("# client {:?} elapsed time for {} writes {:?}, {}s, {:.3} Hz, {}b/s",
                client_num, sent, write_time, write_s, write_hz, write_bits);
            println!("# client {:?} elapsed time for {} reads {:?}, {}s, {:.3} Hz, {}b/s",
                client_num, num_entries, read_time, read_s, read_hz, read_bits);
            (write_hz, read_hz)
        })
    }).collect();

    println!("#All clients started");
    let (total_write_hz, total_read_hz): (f64, f64) = joins.into_iter().map(|j| j.join().unwrap()).fold((0.0, 0.0), |(write_total, read_total), (write, read)|
            (write_total + write, read_total + read)
    );
    let packetsize = packetsize_for_jobsize(jobsize);
    let end = start.elapsed();
    println!("#elapsed time {}s", end.as_secs());
    println!("#local clients | total clients | jobsize | write Hz | read Hz | write b/s | read b/s | packetsize");
    println!("{}\t{}\t{}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t{}",
        clients_to_run,
        total_clients,
        jobsize,
        total_write_hz,
        total_read_hz,
        total_write_hz * packetsize as f64 * 8.0,
        total_read_hz * packetsize as f64 * 8.0,
        packetsize
    );
}

///////////////////////////////////////

///////////////////////////////////////

pub fn dependent_cost(
    server_addr: SocketAddr,
    jobsize: usize,
    num_writes: u32,
    non_dep_portion: u32,
    random_seed: Option<[u32; 4]>
) {
    println!(
        "# Starting dependent cost with jobsize {} for {} iterations running against:\n#\t{:?}",
        jobsize, num_writes, server_addr,
    );

    if let Some(seed) = random_seed {
        println!("# with random spray, seed: {:?}.", seed);
    }

    let mut handle = {
        let chains = (1..3).map(Into::into);
        LogHandle::new_tcp_log(iter::once(server_addr), chains)

    };

    let start = Instant::now();
    {
        let data = vec![0u8; jobsize];

        let mut rand = random_seed.map(RandGen::from_seed).unwrap_or_else(RandGen::new_unseeded);

        trace!("client starting writes.");

        let mut sent = 0;
        while sent < num_writes {
            if rand.gen_weighted_bool(non_dep_portion) {
                handle.async_dependent_multiappend(
                    &[2.into()],
                    &[1.into()],
                    &*data,
                    &[]
                );
            } else {
                handle.async_append(1.into(), &*data, &[]);
            }
            sent += 1;
            handle.flush_completed_appends().unwrap();
        }
        handle.wait_for_all_appends().unwrap();

        trace!("client finished writes, starting read.");

        ////////////////////////////////////////

        let read_start = Instant::now();
        handle.snapshot(1.into());
        let mut num_entries = 0usize;
        'recv: loop {
            match handle.get_next() {
                Ok(..) => num_entries += 1,
                Err(GetRes::Done) => break 'recv,
                Err(e) => panic!("{:?}", e),
            }
        }
        let read_time = read_start.elapsed();

        trace!("client finished read.");

        let read_s = read_time.as_secs() as f64 + (read_time.subsec_nanos() as f64 * 10.0f64.powi(-9));
        let read_hz = num_entries as f64 / read_s;
        let read_bits = read_hz * (data.len() * 8) as f64;
        println!(
            "elapsed time for {} reads 1/{} non_dependent",
            num_entries, non_dep_portion
        );
        println!("{:?}\n{}s\t{:.3} Hz\t{}b/s", read_time, read_s, read_hz, read_bits);
    }

    let end = start.elapsed();
    println!("#elapsed time {}s", end.as_secs());
}

///////////////////////////////////////

fn packetsize_for_jobsize(jobsize: usize) -> usize {
    let data = vec![0u8; jobsize];
    SingletonBuilder(&data[..], &[]).clone_entry().entry_size()
}

///////////////////////////////////////

// fn leak<T: ?Sized>(b: Box<T>) -> &'static T {
    // unsafe { &*Box::into_raw(b) }
// }
