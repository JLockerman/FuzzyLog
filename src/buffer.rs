//use storeables::Storeable;
use packets::{self, Entry, MutEntry, EntryContents, EntryContentsMut};
use packets::Packet::WrapErr;
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
    pub fn wrap_vec(mut inner: Vec<u8>) -> Self {
        let cap = inner.len();
        unsafe { inner.set_len(cap) };
        Buffer { inner: inner, no_drop: false }
    }

    #[cfg(not(any(debug_assertions, feature="debug_no_drop")))]
    pub fn wrap_vec(mut inner: Vec<u8>) -> Self {
        let cap = inner.len();
        unsafe { inner.set_len(cap) };
        Buffer { inner: inner, }
    }

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
        self.contents().len()
    }

    pub fn entry(&self) -> Entry {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        Entry::wrap_slice(&self.entry_slice())
    }

    pub fn entry_mut(&mut self) -> MutEntry {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        debug_assert!(self.packet_fits(), "packet size {}, buffer len {}", self.entry_size(), self.inner.len());
        let size = self.entry_size();
        MutEntry::wrap_slice(&mut self[..size])
    }

    pub fn contents(&self) -> EntryContents {
        unsafe { EntryContents::try_ref(&self.inner[..]).unwrap().0 }
    }

    pub fn contents_mut(&mut self) -> EntryContentsMut {
        unsafe { EntryContentsMut::try_mut(&mut self.inner[..]).unwrap().0 }
    }

    pub fn finished_at(&self, len: usize) -> Result<usize, WrapErr> {
        unsafe { EntryContents::try_ref(&self.inner[..len]).map(|(c, _)| c.len()) }
    }

    pub fn packet_fits(&self) -> bool {
        debug_assert_eq!(self.inner.len(), self.inner.capacity());
        //self.inner.len() >= Entry::<()>::wrap_bytes(&self[..]).entry_size()
        unsafe { EntryContents::try_ref(&self.inner[..]).is_ok() }
    }

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

    pub fn skeens1_rep_from_sentinel(&mut self, was_multi: bool) {
        if was_multi {
            packets::slice_to_skeens1_multirep(&mut self.inner[..])
        } else {
            packets::slice_to_skeens1_sentirep(&mut self.inner[..])
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
