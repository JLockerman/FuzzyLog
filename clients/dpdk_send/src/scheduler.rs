
use std::collections::{HashMap, VecDeque, BinaryHeap};
use std::mem;

use fuzzy_log::prelude::*;
use types::*;

use types::ReceiveEvent::*;

use self::NextActivity::*;

pub trait AsyncClient {
    fn gen_event(&mut self, MessageBuffer) -> NextActivity;
    fn handle_event(&mut self, ReceiveEvent) -> NextActivity;
}

pub enum NextActivity {
    Send(MessageBuffer, WaitKey, ns),
    Ready,
    KeepWaiting,
    Done,
}

impl NextActivity {
    pub fn map_send<F>(self, f: F) -> NextActivity
    where F: FnOnce(MessageBuffer, WaitKey, ns) -> NextActivity {
        match self {
            Send(m, w, d) => f(m, w, d),
            other => other,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct TimeoutSlot(ns, WaitKey);

impl PartialOrd for TimeoutSlot {
    fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
        let &TimeoutSlot(d, _) = self;
        let &TimeoutSlot(ref other, _) = other;
        Some(d.cmp(other).reverse())
    }
}

impl Ord for TimeoutSlot {
    fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct Scheduler<C>
where C: AsyncClient {
    ready: VecDeque<C>,
    waiting: HashMap<WaitKey, (ns, C)>,
    to_send: Vec<MessageBuffer>,
    timeouts: BinaryHeap<TimeoutSlot>, //TODO
}

impl<C> Scheduler<C>
where C: AsyncClient {
    pub fn new<F>(clients: u16, constructor: F) -> Self
    where F: FnMut(u16) -> C { //TODO alloc: &Allocator
        assert_eq!(mem::size_of::<*mut Mbuf>(), mem::size_of::<*mut u8>());
        assert_eq!(mem::size_of::<Mbuf>(), 0);
        Scheduler {
            ready: (1..clients+1).map(constructor).collect(),
            waiting: HashMap::new(),
            to_send: Vec::with_capacity(clients as usize),
            timeouts: BinaryHeap::with_capacity(clients as usize),
        }
    }

    pub fn get_packets_to_send(&mut self, burst_size: u16) -> &mut Vec<MessageBuffer> {
        while self.to_send.len() < burst_size as usize && !self.ready.is_empty() {
            self.gen_event();
        }
        if self.to_send.len() > 0 { println!("to send {} packets", self.to_send.len()) }
        &mut self.to_send
    }

    pub fn handle_received_packets(&mut self, received: &mut [*mut Mbuf], recv_time: u64) {
        println!("got {} packets @ {}", received.len(), recv_time);
        for &mut msg in received {//TODO
            println!("delivering packet");
            self.send_event_to_client(Message(MessageBuffer{ buffer: msg }));
        }
        'timeout: loop {
            match self.timeouts.peek() {
                Some(&TimeoutSlot(deadline, key)) if recv_time > deadline => {
                    let _ = self.timeouts.pop();
                    self.send_timeout_to_client(deadline, key);
                }
                _ => break 'timeout,
            }
        }
    }

    //pub fn event_loop(&mut self, runtime: u64, burst_size: u16) {
    //    let mut received = Vec::with_capacity(self.ready.len()); //TODO move to init
    //    while curr_time() - start_time < runtime {
    //        while self.to_send.len() < burst_size as usize && !self.ready.is_empty() {
    //            self.gen_event();
    //        }
    //        send(&mut self.to_send);
    //        received = try_recv(received, burst_size);
    //        let recv_time = curr_time();
    //        for msg in received.drain(..) {//TODO
    //            self.send_event_to_client(Message(msg));
    //        }
    //        'timeout: loop {
    //            match self.timeouts.peek() {
    //                Some(&(deadline, key)) if recv_time > deadline => {
    //                    let _ = self.timeouts.pop();
    //                    let buffer = self.alloc_packet();
    //                    self.send_event_to_client(Timeout(buffer, key));
    //                }
    //                _ => break 'timeout,
    //            }
    //        }
    //    }
    //}

    fn gen_event(&mut self) {
         if let Some(mut client) = self.ready.pop_front() {
             match client.gen_event(self.alloc_packet()) {
                 Send(b, w, d) => self.prep_message(client, (b, w, d)),
                 Ready => self.ready.push_back(client),
                 KeepWaiting => panic!("cannot keep waiting when not waiting"), //TODO this should not be
                 Done => mem::drop(client), //TODO add to finished queue?
             }
         }
    }

    fn prep_message(&mut self, client: C, (buf, key, timeout): (MessageBuffer, WaitKey, ns)) {
        let deadline = curr_time() + timeout;
        println!("next deadline {}", deadline);
        self.to_send.push(buf);
        let old = self.waiting.insert(key, (deadline, client));
        assert!(old.is_none());
        self.timeouts.push(TimeoutSlot(deadline, key));
    }

    fn send_event_to_client(&mut self, event: ReceiveEvent) {
        println!("wait key: {}", event.wait_key());
        let waiter = self.waiting.remove(event.wait_key());
        println!("waiting for key {}? {}", event.wait_key(), waiter.is_some());
        if let Some((d, mut client)) = waiter {
            let key = event.wait_key().clone();
            match client.handle_event(event) {
                Send(b, w, d) => {
                    println!("got next event");
                    self.prep_message(client, (b, w, d))
                }
                Ready => self.ready.push_back(client),
                KeepWaiting => {
                    println!("still waiting");
                    self.waiting.insert(key, (d, client));
                }
                Done => mem::drop(client), //TODO add to finished queue?
            }
        }
    }

    fn send_timeout_to_client(&mut self, deadline: u64, wait_key: WaitKey) {
        if self.waiting.get(&wait_key).map_or(false, |&(d, _)| {
            //println!("timeout {}, deadline {}", d, deadline);
            d == deadline
        }) {
            println!("Timeout {} @ {}", deadline, wait_key);
            let (_, mut client) = self.waiting.remove(&wait_key).unwrap();
            let buffer = MessageBuffer { buffer: unsafe { alloc_mbuf() } };
            match client.handle_event(Timeout(buffer, wait_key)) {
                Send(b, w, d) => {
                    println!("got next event");
                    self.prep_message(client, (b, w, d))
                }
                Ready => self.ready.push_back(client),
                KeepWaiting => {
                    println!("still waiting");
                    self.waiting.insert(wait_key, (deadline, client));
                }
                Done => mem::drop(client), //TODO add to finished queue?
            }
        }
    }

    fn alloc_packet(&mut self) -> MessageBuffer {
        let buffer = unsafe { alloc_mbuf() };
        assert!(buffer != ::std::ptr::null_mut());
        assert!(buffer as usize != 1);
        MessageBuffer {
            buffer: buffer,
        }
    }
}

extern "C" {
     fn alloc_mbuf() -> *mut Mbuf;
}

//fn try_recv(mut out: Vec<MessageBuffer>, burst_size: u16) -> Vec<MessageBuffer> {
//    assert!(out.capacity() >= burst_size as usize);
//    unsafe {
//        out.set_len(burst_size as usize);
//        let recv_count = dpdk_rx_burst(&mut out.as_mut_slice()[0].buffer, burst_size);
//        out.set_len(recv_count as usize);
//        out
//    }
//}

//fn send(mut to_send: &mut Vec<MessageBuffer>) {
//    unsafe {
//        dpdk_tx_burst(&mut to_send.as_mut_slice()[0].buffer, to_send.len() as u16);
//        to_send.set_len(0);
//    }
//}



#[inline(always)]
pub fn curr_time() -> ns {
    //unsafe { rdtsc() }
    unsafe {
        let (low, high): (u32, u32);
        asm!("rdtsc\n\tmovl %edx, $0\n\tmovl %eax, $1\n\t"
            : "=r"(high), "=r"(low) :: "{rax}", "{rdx}" : "volatile" );
        ((high as u64) << 32) | (low as u64)
    }
}