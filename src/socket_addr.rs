use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use std::fmt;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Ipv4SocketAddr {
    bytes: [u8; 6],
}

impl Ipv4SocketAddr {

    pub fn from_bytes(bytes: [u8; 6]) -> Self {
        Ipv4SocketAddr { bytes: bytes }
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        let slice = &slice[..6];
        Ipv4SocketAddr::from_bytes([slice[0], slice[1], slice[2], slice[3], slice[4], slice[5]])
    }

    pub fn nil() -> Self {
        Ipv4SocketAddr { bytes: [0; 6] }
    }

    pub fn from_addr_and_port(addr: Ipv4Addr, port: u16) -> Self {
        let mut bytes = [0; 6];
        let addr_bytes = addr.octets();
        bytes[0] = addr_bytes[0];
        bytes[1] = addr_bytes[1];
        bytes[2] = addr_bytes[2];
        bytes[3] = addr_bytes[3];
        let port = port.to_le();
        bytes[4] = (port & 0xff) as u8;
        bytes[5] = ((port >> 8) & 0xff) as u8;
        Ipv4SocketAddr { bytes: bytes }
    }

    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(addr) =>
                Ipv4SocketAddr::from_addr_and_port(*addr.ip(), addr.port()),
            _ => unimplemented!()
        }
    }

    fn to_addr_and_port(&self) -> (Ipv4Addr, u16) {
        let &Ipv4SocketAddr{ bytes } = self;
        let addr = Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
        let port = bytes[4] as u16 | ((bytes[5] as u16) << 8);
        (addr, u16::from_le(port))
    }

    fn to_socket_addr(&self) -> SocketAddr {
        let (ip_addr, port) = self.to_addr_and_port();
        SocketAddr::new(IpAddr::V4(ip_addr), port)
    }

    pub fn bytes(&self) -> &[u8; 6] {
        &self.bytes
    }
}

impl fmt::Debug for Ipv4SocketAddr {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let (addr, port) = self.to_addr_and_port();
        Ok(try!(fmt.write_fmt(format_args!("{:?}:{:?}", addr, port))))
    }
}

impl fmt::Display for Ipv4SocketAddr {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let (addr, port) = self.to_addr_and_port();
        Ok(try!(fmt.write_fmt(format_args!("{}:{}", addr, port))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::Ipv4Addr;
    use std::mem;

    #[test]
    fn test_ip_convert() {
        let ip_addr = Ipv4Addr::new(127, 0, 1,3);
        let port = 15343;
        let sa = Ipv4SocketAddr::from_addr_and_port(ip_addr, port);
        assert_eq!(sa.to_addr_and_port(), (ip_addr, port));
    }

    #[test]
    fn test_size() {
        assert_eq!(mem::size_of::<Ipv4SocketAddr>(), 6);
    }

    #[test]
    fn test_align() {
        assert_eq!(mem::align_of::<Ipv4SocketAddr>(), 1);
    }

    #[test]
    fn test_debug() {
        let ip_addr = Ipv4Addr::new(127, 0, 1,3);
        let port = 15343;
        let sa = Ipv4SocketAddr::from_addr_and_port(ip_addr, port);
        assert_eq!("127.0.1.3:15343", format!("{:?}", sa))
    }

    #[test]
    fn test_display() {
        let ip_addr = Ipv4Addr::new(127, 0, 1,3);
        let port = 15343;
        let sa = Ipv4SocketAddr::from_addr_and_port(ip_addr, port);
        assert_eq!("127.0.1.3:15343", format!("{}", sa))
    }
}
