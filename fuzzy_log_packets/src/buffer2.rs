//use storeables::Storeable;
use super::EntryContents;
use super::Packet::WrapErr;

#[must_use]
#[derive(Debug, Clone)]
pub struct Buffer {
    inner: Vec<u8>,
    start: usize,
    #[cfg(any(debug_assertions, feature="debug_no_drop"))]
    no_drop: bool,
}

impl Buffer {
/*
    #[cfg(any(debug_assertions, feature="debug_no_drop"))]
    pub fn wrap_vec(mut inner: Vec<u8>) -> Self {
        let cap = inner.len();
        unsafe { inner.set_len(cap) };
        Buffer { inner: inner, no_drop: false, start: 0, }
    }

    #[cfg(not(any(debug_assertions, feature="debug_no_drop")))]
    pub fn wrap_vec(mut inner: Vec<u8>) -> Self {
        let cap = inner.len();
        unsafe { inner.set_len(cap) };
        Buffer { inner: inner, start: 0, }
    }*/

    #[cfg(any(debug_assertions, feature="debug_no_drop"))]
    pub fn new() -> Self {
        Buffer { inner: vec![0u8; 8192], no_drop: false, start: 0, }
    }

    #[cfg(not(any(debug_assertions, feature="debug_no_drop")))]
    pub fn new() -> Self {
        Buffer { inner: vec![0u8; 8192], start: 0, }
    }

    #[cfg(any(debug_assertions, feature="debug_no_drop"))]
    pub fn empty() -> Self {
        Buffer { inner: Vec::new(), no_drop: false, start: 0, }
    }

    #[cfg(not(any(debug_assertions, feature="debug_no_drop")))]
    pub fn empty() -> Self {
        Buffer { inner: Vec::new(), start: 0, }
    }

    pub fn clear(&mut self) {
        self.inner = vec![];
        self.start = 0;
    }
/*
    #[cfg(any(debug_assertions, feature="debug_no_drop"))]
    pub fn no_drop() -> Self {
        Buffer { inner: Vec::new(), no_drop: true, start: 0, }
    }

    #[cfg(not(any(debug_assertions, feature="debug_no_drop")))]
    pub fn no_drop() -> Self {
        Buffer { inner: Vec::new(), start: 0, }
    }

    pub fn fill_from_entry_contents(&mut self, contents: EntryContents) -> MutEntry {
        //FIXME Entry::fill_vec sets the vec len to be the size of the packet
        //      while Buffer has an invariant that len == cacpacity
        //      it might be better to pass a lambda into this function
        //      so we can reset the len at the end
        unsafe { self.inner.set_len(0) }
        contents.fill_vec(&mut self.inner);
        if self.inner.len() < self.inner.capacity() {
            let cap = self.inner.capacity();
            unsafe { self.inner.set_len(cap) }
        }
        self.entry_mut()
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
    }*/

    /*pub fn entry(&self) -> Entry {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        Entry::wrap_slice(&self.entry_slice())
    }

    pub fn entry_mut(&mut self) -> MutEntry {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        debug_assert!(self.packet_fits(), "packet size {}, buffer len {}", self.entry_size(), self.inner.len());
        let size = self.entry_size();
        MutEntry::wrap_slice(&mut self[..size])
    }*/

    pub fn try_contents_until(&self, len: usize) -> Result<(EntryContents, &[u8]), WrapErr> {
        unsafe { EntryContents::try_ref(&self.inner[self.start..len]) }
    }

    pub fn try_contents(&self) -> Result<(EntryContents, &[u8]), WrapErr> {
        self.try_contents_until(self.inner.len())
    }

    pub fn entry_slice(&self) -> &[u8] {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        let size = self.entry_size();
        &self.inner[self.start..self.start+size]
    }

    pub fn contents(&self) -> EntryContents {
        self.try_contents().unwrap().0
    }

    pub fn entry_size(&self) -> usize {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        self.contents().len()
    }

    pub fn ensure_capacity(&mut self, capacity: usize) -> usize {
        let effective_cap = self.inner.len() - self.start;
        if effective_cap >= capacity {
            return 0
        }

        let drained = self.start;
        self.inner.drain(0..drained).fold((), |_, _| ());
        self.start = 0;

        if self.inner.capacity() < capacity {
            let curr_cap = self.inner.capacity();
            self.inner.reserve_exact(capacity - curr_cap);
            unsafe { self.inner.set_len(capacity) }
        } else {
            let cap = self.inner.capacity();
            unsafe { self.inner.set_len(cap) }
        }
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        drained
    }

    pub fn finished_entry(&mut self) {
        self.start += self.entry_size()
    }

    /*
    pub fn contents_mut(&mut self) -> EntryContentsMut {
        unsafe { EntryContentsMut::try_mut(&mut self.inner[..]).unwrap().0 }
    }
*/

    pub fn finished_at(&self, len: usize) -> Result<usize, WrapErr> {
        self.try_contents_until(len).map(|(c, _)| c.len())
    }

    pub fn packet_fits(&self) -> bool {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        //self.inner.len() >= Entry::<()>::wrap_bytes(&self[..]).entry_size()
        self.try_contents_until(self.inner.len()).is_ok()
    }
/*
    pub fn to_sentinel(&mut self) -> bool {
        //FIXME
        //assert_eq!(EntryKind::from_bits(self.inner[0]), EntryKind::Multiput);
        packets::slice_to_sentinel(&mut self.inner[..])
        //self.inner[0] = unsafe { ::std::mem::transmute(EntryKind::Sentinel) }
    }

    pub fn from_sentinel(&mut self, was_multi: bool) {
        if was_multi {
            packets::slice_to_multi(&mut self.inner[..])
        }
    }

    //pub fn to_read(&mut self) {
        //FIXME
        //assert_eq!(EntryKind::from_bits(self.inner[0]), EntryKind::Multiput);
     //   self.inner[0] = unsafe { ::std::mem::transmute(EntryKind::Read) }
    //}

    pub fn get_lock_nums(&self) -> HashMap<usize, u64> {
        unimplemented!()
        // use packets::OrderIndex;
        // debug_assert_eq!(self.inner.len(), self.inner.capacity());
        // self.entry()
        //     .locs()
        //     .into_iter()
        //     .cloned()
        //     .map(|OrderIndex(o, i)| {
        //         let (o, i): (u32, u32) = ((o - 1).into(), i.into());
        //         (o as usize, i as u64)
        //     })
        //     .collect()
    }

    pub fn buffer_len(&self) -> usize {
        self.inner.len()
    }

    pub fn into_vec(mut self) -> Vec<u8> {
        let inner = unsafe { ::std::ptr::read(&mut self.inner) };
        ::std::mem::forget(self);
        inner

    }

    pub fn clear_data(&mut self) {
        self[..].iter_mut().fold((), |_, i| *i = 0);
    }*/
}

#[cfg(any(debug_assertions, feature="debug_no_drop"))]
impl Drop for Buffer {
    fn drop(&mut self) {
        if !::std::thread::panicking() && self.no_drop {
            panic!("Dropped NoDropBuffer.")
        }
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
        &mut self.inner[index]
    }
}

/*
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
*/
