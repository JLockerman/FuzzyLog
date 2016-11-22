
use std::{mem, ptr, slice};

//TODO require !Drop?
pub trait Storeable {
    fn size(&self) -> usize;
    unsafe fn ref_to_bytes(&self) -> &u8;
    unsafe fn bytes_to_ref(&u8, usize) -> &Self;
    unsafe fn bytes_to_mut(&mut u8, usize) -> &mut Self;
    unsafe fn clone_box(&self) -> Box<Self>;
    unsafe fn copy_to_mut(&self, &mut Self) -> usize;
}

impl<V> Storeable for V { //TODO should V be Copy/Clone?
    fn size(&self) -> usize {
        mem::size_of::<Self>()
    }

    unsafe fn ref_to_bytes(&self) -> &u8 {
        mem::transmute(self)
    }

    unsafe fn bytes_to_ref(val: &u8, size: usize) -> &Self {
        //TODO assert_eq!(size, mem::size_of::<Self>());
        mem::transmute(val)
    }

    unsafe fn bytes_to_mut(val: &mut u8, size: usize) -> &mut Self {
        assert_eq!(size, mem::size_of::<Self>());
        mem::transmute(val)
    }

    unsafe fn clone_box(&self) -> Box<Self> {
        let mut b = Box::new(mem::uninitialized());
        ptr::copy_nonoverlapping(self, &mut *b, 1);
        b
    }

    unsafe fn copy_to_mut(&self, out: &mut Self) -> usize {
        ptr::copy(self, out, 1);
        mem::size_of::<Self>()
    }
}

impl<V> Storeable for [V] {
    fn size(&self) -> usize {
        mem::size_of::<V>() * self.len()
    }

    unsafe fn ref_to_bytes(&self) -> &u8 {
        mem::transmute(&self[0])
    }

    unsafe fn bytes_to_ref(val: &u8, size: usize) -> &Self {
        assert_eq!(size % mem::size_of::<V>(), 0);
        slice::from_raw_parts(val as *const _ as *const _, size / mem::size_of::<V>())
    }

    unsafe fn bytes_to_mut(val: &mut u8, size: usize) -> &mut Self {
        assert_eq!(size % mem::size_of::<V>(), 0);
        slice::from_raw_parts_mut(val as *mut _ as *mut _, size / mem::size_of::<V>())
    }

    unsafe fn clone_box(&self) -> Box<Self> {
        let mut v = Vec::with_capacity(self.len());
        v.set_len(self.len());
        let mut b = v.into_boxed_slice();
        ptr::copy_nonoverlapping(&self[0], &mut b[0], self.len());
        b
    }

    unsafe fn copy_to_mut(&self, out: &mut Self) -> usize {
        let to_copy = ::std::cmp::min(self.len(), out.len());
        ptr::copy(&self[0], &mut out[0], to_copy);
        to_copy * mem::size_of::<V>()
    }
}
