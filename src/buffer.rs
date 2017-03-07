use storeables::Storeable;
use packets::{Entry, EntryContents};
use hash::HashMap;

#[must_use]
#[derive(Debug, Clone)]
pub struct Buffer {
    inner: Vec<u8>,
    #[cfg(any(debug_assertions, feature="debug_no_drop"))]
    no_drop: bool,
}

impl Buffer {
    #[cfg(any(debug_assertions, feature="debug_no_drop"))]
    pub fn new() -> Self {
        Buffer { inner: vec![0u8; 8192], no_drop: false }
    }

    #[cfg(not(any(debug_assertions, feature="debug_no_drop")))]
    pub fn new() -> Self {
        Buffer { inner: vec![0u8; 8192] }
    }

    #[cfg(any(debug_assertions, feature="debug_no_drop"))]
    pub fn empty() -> Self {
        Buffer { inner: Vec::new(), no_drop: false }
    }

    #[cfg(not(any(debug_assertions, feature="debug_no_drop")))]
    pub fn empty() -> Self {
        Buffer { inner: Vec::new(), }
    }

    #[cfg(any(debug_assertions, feature="debug_no_drop"))]
    pub fn no_drop() -> Self {
        Buffer { inner: Vec::new(), no_drop: true }
    }

    #[cfg(not(any(debug_assertions, feature="debug_no_drop")))]
    pub fn no_drop() -> Self {
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

    pub fn buffer_len(&self) -> usize {
        self.inner.len()
    }
}

#[cfg(any(debug_assertions, feature="debug_no_drop"))]
impl Drop for Buffer {
    fn drop(&mut self) {
        if !::std::thread::panicking() && self.no_drop {
            panic!("Dropped NoDropBuffer.")
        }
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

#[derive(Debug, Clone)]
pub struct NoDropBuffer {
    inner: Buffer,
}

impl NoDropBuffer {
    pub fn empty() -> Self {
        NoDropBuffer { inner: Buffer::empty() }
    }
}

impl ::std::ops::Deref for NoDropBuffer {
    type Target = Buffer;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ::std::ops::DerefMut for NoDropBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Drop for NoDropBuffer {
    fn drop(&mut self) {
        if !::std::thread::panicking() {
            panic!("Dropped NoDropBuffer.")
        }
    }
}
