use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use std::fmt;

use uuid::Uuid;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Ipv4SocketAddr {
    bytes: [u8; 16],
}

impl Ipv4SocketAddr {

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Ipv4SocketAddr { bytes: bytes }
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        let slice = &slice[..16];
        let mut bytes = [0; 16];
        bytes.copy_from_slice(slice);
        Ipv4SocketAddr { bytes: bytes }
    }

    pub fn nil() -> Self {
        Ipv4SocketAddr { bytes: [0; 16] }
    }

    pub fn bytes(&self) -> &[u8; 16] {
        &self.bytes
    }

    pub fn to_uuid(&self) -> Uuid {
        Uuid::from_bytes(&self.bytes).unwrap()
    }
}

impl fmt::Debug for Ipv4SocketAddr {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        Ok(try!(fmt.write_fmt(format_args!("{:?}", Uuid::from_bytes(&*self.bytes())))))
    }
}

impl fmt::Display for Ipv4SocketAddr {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        Ok(try!(fmt.write_fmt(format_args!("{}", &Uuid::from_bytes(&*self.bytes()).unwrap()))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::mem;

    #[test]
    fn test_size() {
        assert_eq!(mem::size_of::<Ipv4SocketAddr>(), 16);
    }

    #[test]
    fn test_align() {
        assert_eq!(mem::align_of::<Ipv4SocketAddr>(), 1);
    }
}
