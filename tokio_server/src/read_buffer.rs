use std::mem::replace;
use std::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct ReadBuffer {
    end: usize,
    cursor: usize,
    start: usize,
    bytes: Box<[u8]>,
}

impl ReadBuffer {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn from_vec(mut bytes: Vec<u8>) -> Self {
        let start = 0;
        let cursor = bytes.len();
        let end = bytes.capacity();
        unsafe {
            bytes.set_len(end);
        }
        ReadBuffer {
            start,
            cursor,
            end,
            bytes: bytes.into_boxed_slice(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut vec = Vec::with_capacity(capacity);
        let cap = vec.capacity();
        unsafe { vec.set_len(cap) }
        ReadBuffer {
            end: capacity,
            cursor: 0,
            start: 0,
            bytes: vec.into_boxed_slice(),
        }
    }

    pub fn will_read_to(&mut self, len: usize) {
        self.ensure_fits(len);
        self.end = len;
    }

    pub fn will_read_end(&mut self) {
        self.end = self.bytes.len();
    }

    pub fn ensure_fits(&mut self, len: usize) {
        if self.bytes.len() < len {
            let buffer = replace(&mut self.bytes, vec![].into_boxed_slice());
            let mut buffer: Vec<_> = buffer.into();
            let needed = len.saturating_sub(buffer.capacity());
            buffer.reserve(needed);
            let cap = buffer.capacity();
            unsafe { buffer.set_len(cap) }
            self.bytes = buffer.into_boxed_slice();
        }
    }

    pub fn freeze(&mut self, len: usize) {
        self.ensure_fits(len);
        self.cursor = len;
    }

    pub fn freeze_read(&mut self) {
        self.cursor = self.end;
    }

    pub fn clear(&mut self) {
        self.cursor = 0;
        self.end = self.bytes.len();
    }

    pub fn pop_bytes(&mut self, len: usize) {
        self.cursor = self.cursor.saturating_sub(len);
        self.end = self.end.saturating_sub(len);
    }

    pub fn into_vec(self) -> Vec<u8> {
        let ReadBuffer { bytes, .. } = self;
        From::<Box<[u8]>>::from(bytes)
    }

    // pub fn split_off_at(&mut self, point: usize) -> Vec<u8> {
    //     use std::ptr;
    //     assert!(point <= self.cursor);
    //     let to_remove = self.cursor - point;
    //     unsafe {
    //         let read = if to_remove < self.bytes.len() - point {
    //             let read = self.bytes[..point].to_vec();
    //             let copy_back = self.cursor - point;
    //             ptr::copy(
    //                 self.bytes[..point].as_mut_ptr(),
    //                 self.bytes[point..self.cursor].as_mut_ptr(),
    //                 copy_back,
    //             );
    //             read
    //         } else {
    //             let new_bytes = self.bytes[point..self.end].to_vec().into_boxed_slice();
    //             let read = ::std::mem::replace(&mut self.bytes, new_bytes);
    //             let mut read: Vec<_> = read.into();
    //             read.set_len(point);
    //             read
    //         };
    //         self.cursor = 0;
    //         self.end = self.end.saturating_sub(point);
    //         read
    //     }
    // }

    pub fn split_off_at(&mut self, point: usize) -> Vec<u8> {
        assert!(self.start + point <= self.cursor);
        let read = self.bytes[self.start..point].to_vec();
        self.start += point;
        read
    }

    pub fn shift_back(&mut self) {
        use std::ptr;
        let copy_len = self.end - self.start;
        unsafe {
            let copy_from = self.bytes[self.start..self.end].as_mut_ptr();
            let copy_to = self.bytes[0..].as_mut_ptr();
            ptr::copy(copy_from, copy_to, copy_len);
        }
        self.cursor -= self.start;
        self.end -= self.start;
        assert_eq!(self.end, copy_len);
        self.start = 0;
    }
}

// impl AsRef<[u8]> for ReadBuffer {
//     fn as_ref(&self) -> &[u8] {
//         &self.bytes[..self.start]
//     }
// }

impl AsMut<[u8]> for ReadBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[self.cursor..self.end]
    }
}

impl Deref for ReadBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes[self.start..self.cursor]
    }
}

impl DerefMut for ReadBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes[self.start..self.cursor]
    }
}
