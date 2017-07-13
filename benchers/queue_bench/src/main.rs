#![feature(test)]

extern crate fuzzy_log;
extern crate mio;
extern crate nix;
extern crate env_logger;
extern crate test;
extern crate libc;

use std::mem;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering, AtomicPtr};
use std::sync::{mpsc, Arc};
use std::thread;

use test::{Bencher, black_box};

use fuzzy_log::servers2::spmc;

//based on http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue

struct Node {
     next: AtomicPtr<Node>,
     data: u64,
}

struct Receiver {
    tail: AtomicPtr<Node>,
}

#[derive(Clone)]
struct Sender {
    head: Arc<AtomicPtr<Node>>,
}


fn channel() -> (Sender, Receiver) {
    let stub = Box::new(Node{ next: AtomicPtr::default(), data: 0 });
    let stub = Box::into_raw(stub);
    let head = AtomicPtr::new(stub);
    let tail = AtomicPtr::new(stub);
    (Sender { head: Arc::new(head), }, Receiver { tail: tail, })
}

impl Sender {
    fn send(&self, data: u64) {
        let node = Box::new(Node { next: AtomicPtr::default(), data: data });
        let n = Box::into_raw(node);
        let prev = self.head.swap(n, Ordering::AcqRel);
        unsafe {
            (&*prev).next.store(n, Ordering::Release)
        }
    }
}

impl Receiver {
    fn try_recv(&mut self) -> Option<u64> {
        unsafe {
            let tail = *self.tail.get_mut();
            let next = (&*tail).next.load(Ordering::Acquire);
            next.as_mut().map(|next| {
                *self.tail.get_mut() = next;
                next.data
            })
        }
    }
}

macro_rules! bench_queue {
    ($bench_name:ident, $channel_creator:path) => (
        #[bench]
        fn $bench_name(b: &mut Bencher) {
            static SERVER_READY: AtomicUsize = ATOMIC_USIZE_INIT;
            set_priority();
            set_cpu(0);
            let (send1, mut recv1) = $channel_creator();
            let (send2, mut recv2) = $channel_creator();
            let (send11, send21) = (send1.clone(), send2.clone());
            send1.send(1);
            send2.send(1);
            recv1.try_recv();
            recv2.try_recv();

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
            });
            drop((send11, send21));
        }
    );
}

bench_queue!(bench_mio, mio::channel::channel);
bench_queue!(a_bench_mio_sync, mio_sync_channel);
bench_queue!(a_bench_sync, sync_channel);
bench_queue!(bench_mpsc, mpsc::channel);
//bench_queue!(bench_spmc, spmc::channel);
bench_queue!(new, channel);

fn sync_channel<T: Send>() -> (mpsc::SyncSender<T>, mpsc::Receiver<T>) {
    mpsc::sync_channel(1024)
}

fn mio_sync_channel<T: Send>() -> (mio::channel::SyncSender<T>, mio::channel::Receiver<T>) {
    mio::channel::sync_channel(1024)
}

fn set_cpu(cpu_num: usize) {
    #[cfg(linux)]
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
    #[cfg(linux)]
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
