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
    for i in 0..12 {
        print!("send size {}... ", 1<<i);
        //let bytes = (0u32..1<<i).map(|i| i as u8).collect::<Vec<_>>();
        //send(&bytes[..]);
        simple_send(1<<i);
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

#[test]
fn echo() {
    main()
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

#[bench]
fn big_rtt(b: &mut Bencher) {
    const SERVER_ADDR: &'static str = "10.21.7.4:13265";
    const CLIENT_ADDR: &'static str = "0.0.0.0:0";
    let client = UdpSocket::bind(CLIENT_ADDR).unwrap();
    let mut buf: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
    let server: SocketAddr = SERVER_ADDR.parse().unwrap();
    b.iter(|| {
        let s = client.send_to(&buf[..], server).unwrap();
        let r = client.recv_from(&mut buf[..]).unwrap();
        test::black_box((s, r));
        test::black_box(&mut buf[..]);
   })
}

#[bench]
fn big_rtt2(b: &mut Bencher) {
    const SERVER_ADDR: &'static str = "10.21.7.4:13265";
    const CLIENT_ADDR: &'static str = "0.0.0.0:0";
    let client = UdpSocket::bind(CLIENT_ADDR).unwrap();
    let mut buf: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
    let mut buf2: Vec<u8> = (0..4096u32).map(|i| i as u8).collect();
    assert_eq!(4096, buf.len());
    assert_eq!(buf.len(), buf2.len());
    let server: SocketAddr = SERVER_ADDR.parse().unwrap();
    b.iter(|| {
        let s = client.send_to(&buf[..], server).unwrap();
        let r = client.recv_from(&mut buf2[..]).unwrap();
        test::black_box((s, r));
        test::black_box(buf2[0] == 0);
        test::black_box((&buf[..], &mut buf2[..]));
   })
}

