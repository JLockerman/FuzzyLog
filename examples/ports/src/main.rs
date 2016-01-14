#![cfg_attr(test, feature(test))]

#[cfg(test)]
extern crate test;

extern crate time;

use std::io;
use std::mem::{self, size_of};
use std::net::{SocketAddr, UdpSocket};
use std::slice;

#[cfg(test)]
use test::Bencher;

use time::precise_time_ns;


fn main() {
    const CLIENT_ADDR: &'static str = "0.0.0.0:0";
    let client1 = UdpSocket::bind(CLIENT_ADDR).unwrap();
    let client2 = UdpSocket::bind(CLIENT_ADDR).unwrap();
    let mut buf = [0; 4];
    for i in 0..14 {
        let server_addr = format!("10.21.7.4:{}", 13265+i);
        print!("sending to {:?} ... ", server_addr);
        client1.send_to(&buf[..], &*server_addr).unwrap();
        client2.send_to(&buf[..], &*server_addr).unwrap();
        println!("complete!")
    }
    

}

fn send(bytes: &[u8]) {
    const SERVER_ADDR: &'static str = "10.21.7.4:13265";
    const CLIENT_ADDR: &'static str = "0.0.0.0:0";
    let client = UdpSocket::bind(CLIENT_ADDR).unwrap();
    let mut buf: Vec<_> = (0..bytes.len()).map(|_| 0).collect();
    //unsafe {
    //    println!("pre-send buf {:x}", mem::transmute_copy::<_, u32>(&*buf));
    //}
    client.send_to(&bytes[..], SERVER_ADDR).unwrap();
    
    let (recv, addr) = client.recv_from(&mut buf[..]).unwrap();
    //unsafe {
    //    println!("post-recv buf {:x}", mem::transmute_copy::<_, u32>(&*buf));
    //}
    //println!("recv from {:?}", addr);
    assert_eq!(bytes.len(), recv);
    for i in 0..bytes.len() {
        if bytes[i] != buf[i] {
            panic!("send {}: {} != {} at {}", bytes.len(), bytes[i], buf[i], i)
        }
        assert_eq!(bytes[i], buf[i]);
    }
}

fn simple_send(len: usize) {
    const SERVER_ADDR: &'static str = "10.21.7.4:13265";
    const CLIENT_ADDR: &'static str = "0.0.0.0:0";
    let client = UdpSocket::bind(CLIENT_ADDR).unwrap();
    let mut buf: Vec<_> = (0..len).map(|_| 0).collect();
    client.send_to(&buf[..], SERVER_ADDR).unwrap();
    let (recv, addr) = client.recv_from(&mut buf[..]).unwrap();
    assert_eq!(recv, len);
}

#[bench]
fn rtt(b: &mut Bencher) {
    const SERVER_ADDR: &'static str = "10.21.7.4:13265";
    const CLIENT_ADDR: &'static str = "0.0.0.0:0";
    let client = UdpSocket::bind(CLIENT_ADDR).unwrap();
    let mut buf: Vec<u8> = (0..4).map(|i| i).collect();
    let server: SocketAddr = SERVER_ADDR.parse().unwrap();
    b.iter(|| {
        client.send_to(&buf[..], server).unwrap();
        client.recv_from(&mut buf[..]).unwrap()
    })
}

