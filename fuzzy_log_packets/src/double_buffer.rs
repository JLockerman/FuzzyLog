use std::collections::LinkedList;
use std::mem;

use EntryContents;

//TODO remove Debug
#[derive(Debug)]
pub struct DoubleBuffer {
    first: Vec<u8>,
    second: Vec<u8>,
    //TODO or VecDeque
    pending: LinkedList<Vec<u8>>,
}

const MAX_WRITE_BUFFER_SIZE: usize = 1024 * 10;

//TODO move this and the one in async/store into a single file
impl DoubleBuffer {

    pub fn new() -> Self {
        DoubleBuffer {
            first: Vec::new(),
            second: Vec::new(),
            pending: Default::default(),
        }
    }

    pub fn with_first_buffer_capacity(cap: usize) -> Self {
        DoubleBuffer {
            first: Vec::with_capacity(cap),
            second: Vec::with_capacity(cap),
            pending: Default::default(),
        }
    }

    pub fn first_bytes(&self) -> &[u8] {
        &self.first[..]
    }

    pub fn swap_if_needed(&mut self) {
        self.first.clear();
        if self.second.len() > 0 {
            mem::swap(&mut self.first, &mut self.second)
        }
        self.flush_pending();
    }

    pub fn can_hold_bytes(&self, bytes: usize) -> bool {
        (self.is_filling_first() && buffer_can_hold_bytes(&self.first, bytes))
        || buffer_can_hold_bytes(&self.second, bytes)
    }

    #[inline(always)]
    pub fn fill(&mut self, bytes: &[&[u8]]) -> bool {
        let write_len = bytes.iter().map(|b| b.len()).sum();
        if !self.pending.is_empty() || !self.can_hold_bytes(write_len) {
            let mut pend = Vec::with_capacity(write_len);
            for byte in bytes {
                pend.extend_from_slice(byte);
            }
            self.pending.push_back(pend);
            // if self.first.is_empty() && !self.second.is_empty() || !self.pending.is_empty() {
            //     self.swap_if_needed()
            // }
            return false
        }

        let mut _added = true;
        for byte in bytes {
            _added &= self.try_fill_buffer(byte);
        }
        assert!(_added);
        _added
    }

    #[inline(always)]
    pub fn fill_from_contents(&mut self, contents: EntryContents, additional_bytes: &[&[u8]]) -> bool {
        let additional_len: usize = additional_bytes.iter().map(|b| b.len()).sum();
        let write_len = contents.len() + additional_len;
        if !self.pending.is_empty() || !self.can_hold_bytes(write_len) {
            let mut pend = Vec::with_capacity(write_len);
            contents.fill_vec(&mut pend);
            for byte in additional_bytes {
                pend.extend_from_slice(byte);
            }
            self.pending.push_back(pend);
            // if self.first.is_empty() && !self.second.is_empty() || !self.pending.is_empty() {
            //     self.swap_if_needed()
            // }
            return false
        }
        let mut _added = self.try_fill_buffer_from_contents(contents);
        for byte in additional_bytes {
            _added &= self.try_fill_buffer(byte);
        }
        assert!(_added);
        _added
    }

    fn try_fill_buffer(&mut self, bytes: &[u8]) -> bool {
        if self.is_filling_first() {
            if buffer_can_hold_bytes(&self.first, bytes.len())
            || self.first.is_empty() {
                self.first.extend_from_slice(bytes);
                return true
            }
        }

        if buffer_can_hold_bytes(&self.second, bytes.len())
        || self.second.capacity() < MAX_WRITE_BUFFER_SIZE {
            self.second.extend_from_slice(bytes);
            return true
        }

        return false
    }

    fn try_fill_buffer_from_contents(&mut self, contents: EntryContents) -> bool {
        let contents_len = contents.len();
        if self.is_filling_first() {
            if buffer_can_hold_bytes(&self.first, contents_len)
            || self.first.is_empty() {
                let _old_len = self.first.len();
                contents.fill_vec(&mut self.first);
                assert!(
                    self.first.len() > _old_len,
                    "{:?} > {:?}, {:?}",
                    _old_len, self.first.len(), contents_len
                );
                return true
            }
        }

        if buffer_can_hold_bytes(&self.second, contents_len)
        || self.second.capacity() < MAX_WRITE_BUFFER_SIZE {
            let _old_len = self.second.len();
            contents.fill_vec(&mut self.second);
            assert!(self.second.len() > _old_len);
            return true
        }

        return false
    }


    fn is_filling_first(&self) -> bool {
        self.second.len() == 0
    }

    pub fn is_empty(&self) -> bool {
        self.first.is_empty() && self.second.is_empty() && self.pending.is_empty()
    }

    fn flush_pending(&mut self) {
        let mut pending = mem::replace(&mut self.pending, Default::default());
        while !pending.is_empty() {
            let added = self.try_fill_buffer(&*pending.front().unwrap());
            if !added { break }
            drop(pending.pop_front())
        }
        self.pending = pending;
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub fn clear(&mut self) {
        self.first = vec![];
        self.second = vec![];
        self.pending.clear();
    }
}

fn buffer_can_hold_bytes(buffer: &Vec<u8>, bytes: usize) -> bool {
    buffer.capacity() - buffer.len() >= bytes
}
