use storeables::Storeable;
use packets::{Entry, EntryContents};
use hash::HashMap;

//FIXME merge this buffer with the one in store and put in it's own mod
pub struct Buffer {
    inner: Vec<u8>,
}

impl Buffer {
    pub fn new() -> Self {
        Buffer { inner: vec![0u8; 8192] }
    }

    pub fn empty() -> Self {
        Buffer { inner: Vec::new() }
    }

    pub fn fill_from_entry_contents<V>(&mut self, contents: EntryContents<V>) -> &mut Entry<V>
    where V: Storeable {
        //FIXME Entry::fill_vec sets the vec len to be the size of the packet
        //      while Buffer has an invariant that len == cacpacity
        //      it might be better to pass a lambda into this function
        //      so we can reset the len at the end
        contents.fill_vec(&mut self.inner)
    }

    pub fn ensure_capacity(&mut self, capacity: usize) {
        if self.inner.capacity() < capacity {
            let curr_cap = self.inner.capacity();
            self.inner.reserve_exact(capacity - curr_cap);
            unsafe { self.inner.set_len(capacity) }
        }
        else if self.inner.len() < capacity {
            unsafe { self.inner.set_len(capacity) }
        }
    }

    pub fn ensure_len(&mut self) {
        unsafe {
            let capacity = self.inner.capacity();
            self.inner.set_len(capacity);
        }
    }

    pub fn entry_slice(&self) -> &[u8] {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        let size = self.entry_size();
        &self[..size]
    }

    pub fn entry_size(&self) -> usize {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        self.entry().entry_size()
    }

    pub fn entry(&self) -> &Entry<()> {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        Entry::<()>::wrap_bytes(&self[..])
    }

    pub fn entry_mut(&mut self) -> &mut Entry<()> {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        debug_assert!(self.packet_fits(), "packet size {}, buffer len {}", self.entry_size(), self.inner.len());
        Entry::<()>::wrap_bytes_mut(&mut self[..])
    }

    pub fn packet_fits(&self) -> bool {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        self.inner.len() >= Entry::<()>::wrap_bytes(&self[..]).entry_size()
    }

    pub fn get_lock_nums(&self) -> HashMap<usize, u64> {
        use packets::OrderIndex;
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        self.entry()
            .locs()
            .into_iter()
            .cloned()
            .map(|OrderIndex(o, i)| {
                let (o, i): (u32, u32) = ((o - 1).into(), i.into());
                (o as usize, i as u64)
            })
            .collect()
    }
}


//FIXME impl AsRef for Buffer...
impl ::std::ops::Index<::std::ops::Range<usize>> for Buffer {
    type Output = [u8];
    fn index(&self, index: ::std::ops::Range<usize>) -> &Self::Output {
        &self.inner[index]
    }
}

impl ::std::ops::IndexMut<::std::ops::Range<usize>> for Buffer {
    fn index_mut(&mut self, index: ::std::ops::Range<usize>) -> &mut Self::Output {
        //TODO should this ensure capacity?
        self.ensure_capacity(index.end);
        &mut self.inner[index]
    }
}

impl ::std::ops::Index<::std::ops::RangeFrom<usize>> for Buffer {
    type Output = [u8];
    fn index(&self, index: ::std::ops::RangeFrom<usize>) -> &Self::Output {
        &self.inner[index]
    }
}

impl ::std::ops::IndexMut<::std::ops::RangeFrom<usize>> for Buffer {
    fn index_mut(&mut self, index: ::std::ops::RangeFrom<usize>) -> &mut Self::Output {
        //TODO should this ensure capacity?
        self.ensure_capacity(index.start);
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        &mut self.inner[index]
    }
}

impl ::std::ops::Index<::std::ops::RangeTo<usize>> for Buffer {
    type Output = [u8];
    fn index(&self, index: ::std::ops::RangeTo<usize>) -> &Self::Output {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        &self.inner[index]
    }
}

impl ::std::ops::IndexMut<::std::ops::RangeTo<usize>> for Buffer {
    fn index_mut(&mut self, index: ::std::ops::RangeTo<usize>) -> &mut Self::Output {
        //TODO should this ensure capacity?
        self.ensure_capacity(index.end);
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        &mut self.inner[index]
    }
}

impl ::std::ops::Index<::std::ops::RangeFull> for Buffer {
    type Output = [u8];
    fn index(&self, index: ::std::ops::RangeFull) -> &Self::Output {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        &self.inner[index]
    }
}

impl ::std::ops::IndexMut<::std::ops::RangeFull> for Buffer {
    fn index_mut(&mut self, index: ::std::ops::RangeFull) -> &mut Self::Output {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        //TODO should this ensure capacity?
        &mut self.inner[index]
    }
}
