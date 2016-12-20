#![feature(test)]

extern crate fuzzy_log;
extern crate mio;
extern crate nix;
extern crate env_logger;
extern crate test;
extern crate libc;

use std::mem;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
use std::sync::mpsc;
use std::thread;

use test::{Bencher, black_box};

use fuzzy_log::servers2::spmc;

macro_rules! bench_queue {
    ($bench_name:ident, $channel_creator:path) => (
        #[bench]
        fn $bench_name(b: &mut Bencher) {
            static SERVER_READY: AtomicUsize = ATOMIC_USIZE_INIT;
            set_priority();
            set_cpu(0);
            let (send1, recv1) = $channel_creator();
            let (send2, recv2) = $channel_creator();

            thread::spawn(move || {
                set_cpu(1);
                SERVER_READY.fetch_add(1, Ordering::SeqCst);
                loop {
                    let mut msg;
                    'recv: loop {
                        msg = recv1.try_recv().unwrap_or(0);
                        if msg != 0 { break 'recv }
                    }
                    let _ = send2.send(black_box(msg));
                }
            });

            while SERVER_READY.load(Ordering::Acquire) < 1 {}

            b.iter(move || {
                let sum = 0u64;
                for _ in 0..100 {
                    let _ = black_box(send1.send(0xff10bc1u64));
                }
                for _ in 0..100 {
                    let mut msg;
                    'recv: loop {
                        msg = recv2.try_recv().unwrap_or(0);
                        if msg != 0 { break 'recv }
                    }
                    sum.wrapping_add(black_box(msg));
                }
                black_box(sum)
            })
        }
    );
}

bench_queue!(bench_mio, mio::channel::channel);
bench_queue!(bench_mio_sync, mio_sync_channel);
bench_queue!(bench_sync, sync_channel);
bench_queue!(bench_mpsc, mpsc::channel);
bench_queue!(bench_spmc, spmc::channel);

fn sync_channel<T: Send>() -> (mpsc::SyncSender<T>, mpsc::Receiver<T>) {
    mpsc::sync_channel(1024)
}

fn mio_sync_channel<T: Send>() -> (mio::channel::SyncSender<T>, mio::channel::Receiver<T>) {
    mio::channel::sync_channel(1024)
}

fn set_cpu(cpu_num: usize) {
    unsafe {
        let mut cpu_set = mem::zeroed();
        libc::CPU_ZERO(&mut cpu_set);
        libc::CPU_SET(cpu_num, &mut cpu_set);
        let thread_id = libc::pthread_self();
        let cpu_set_size = mem::size_of::<libc::cpu_set_t>();
        let err = libc::pthread_setaffinity_np(thread_id, cpu_set_size, &cpu_set);
        if err != 0 {
            panic!("cpu set failed for cpu {}, thread_id {}", cpu_num, thread_id)
        }
    }
}

fn set_priority() {
    unsafe {
        let err = libc::setpriority(libc::PRIO_PROCESS as u32, 0, -20);
        if err != 0 {
            panic!("set priority failed")
        }
    }
}

fn main() {
    println!("Only use with bench");
}
