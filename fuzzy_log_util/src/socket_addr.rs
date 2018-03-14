use std::fmt;

use uuid::Uuid;

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct Ipv4SocketAddr {
    bytes: [u8; 16],
}

impl Ipv4SocketAddr {

    pub fn random() -> Self {
        Self::from_uuid(&Uuid::new_v4())
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Ipv4SocketAddr { bytes: bytes }
    }

    pub fn from_u64(num: u64) -> Self {
        use ::byteorder::{LittleEndian, ByteOrder};
        let mut bytes = [0; 16];
        LittleEndian::write_u64(&mut bytes, num);
        Ipv4SocketAddr { bytes: bytes }
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        let slice = &slice[..16];
        let mut bytes = [0; 16];
        bytes.copy_from_slice(slice);
        Ipv4SocketAddr { bytes: bytes }
    }

    pub fn from_uuid(id: &Uuid) -> Self {
        Self::from_bytes(*id.as_bytes())
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

impl From<Uuid> for Ipv4SocketAddr {
    fn from(id: Uuid) -> Self {
        Self::from_uuid(&id)
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

    #[test]
    fn test_from_u64_id_hash() {
        use std::hash::{Hash, Hasher};
        use hash::{UuidHasher, IdHasher};
        for i in 0..100 {
            let mut hasher = IdHasher::default();
            let addr = Ipv4SocketAddr::from_u64(i);
            addr.hash(&mut hasher);
            assert_eq!(hasher.finish(), i);
        }

        for i in 0..100 {
            let mut hasher = UuidHasher::default();
            let addr = Ipv4SocketAddr::from_u64(i);
            addr.hash(&mut hasher);
            assert_eq!(hasher.finish(), i);
        }
    }
}
