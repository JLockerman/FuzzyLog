//FIXME replace with regular RC one Rc<[u8]> can be implemented stably
use std::mem::{drop, forget, size_of, size_of_val, transmute};
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::sync::atomic::{self, AtomicUsize, Ordering};

pub struct RcSlice {
    //FIXME should be Owned<..>/Shared<..>
    ptr: &'static mut RcSliceStorage
}

struct RcSliceStorage {
    count: AtomicUsize,
    data: [u8],
}

unsafe impl Send for RcSlice {}
unsafe impl Sync for RcSlice {}

impl RcSlice {
    // based on https://github.com/rust-lang/rfcs/blob/master/text/1845-shared-from-slice.md#theres-already-an-implementation
    pub fn with_len(len: usize) -> Self {
        let aligned_len = aligned_len(len);
        let mut vec = Vec::<AtomicUsize>::with_capacity(aligned_len);
         unsafe {
            vec.set_len(aligned_len);
            let ptr = vec.as_mut_ptr();
            forget(vec);
            let slice: RcSlice = transmute([ptr as usize, len]);
            *slice.ptr.count.get_mut() = 1;
            assert_eq!(slice.len(), len);
            assert_eq!(aligned_len * size_of::<AtomicUsize>(), size_of_val(&*slice.ptr));
            slice
        }
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        let mut s = Self::with_len(slice.len());
        s.copy_from_slice(slice);
        s
    }

    pub fn count(&self) -> usize {
        self.ptr.count.load(Ordering::Relaxed)
    }

    pub fn into_ptr(self) -> *const u8 {
        let ptr = self.as_ptr();
        forget(self);
        ptr
    }

    pub unsafe fn from_ptr(ptr: *const u8, len: usize) -> Self {
        let ptr = ptr.offset(-(size_of::<AtomicUsize>() as isize));
        transmute([ptr as usize, len])
    }

    #[inline(never)]
    unsafe fn drop_slow(&mut self) {
        let aligned_len = aligned_len(self.len());
        let ptr = self.ptr as *mut RcSliceStorage as *mut AtomicUsize;
        drop(Vec::from_raw_parts(ptr, aligned_len, aligned_len));
    }

    #[allow(dead_code)]
    unsafe fn get_mut(&mut self) -> &mut [u8] {
        &mut self.ptr.data
    }
}

fn aligned_len(len: usize) -> usize {
    1 + (len + size_of::<AtomicUsize>() - 1) / size_of::<AtomicUsize>()
}

//Clone and Drop based on Arc
impl Clone for RcSlice {
    fn clone(&self) -> Self {
        self.ptr.count.fetch_add(1, Ordering::Relaxed);
        unsafe { ptr::read(self) }
    }
}

impl Drop for RcSlice {
    fn drop(&mut self) {
        unsafe {
            if self.ptr.count.fetch_sub(1, Ordering::Release) != 1 {
                return
            }

            atomic::fence(Ordering::Acquire);
            self.drop_slow()
        }
    }
}

impl Deref for RcSlice {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.ptr.data
    }
}

impl DerefMut for RcSlice {
    fn deref_mut(&mut self) -> &mut [u8] {
        //FIXME this is UB, we need a more principled way to do this
        //      (we update the of each skeens shard incrementally, this should be atomic)
        //assert!(self.ptr.count.load(Ordering::Relaxed) == 1);
        &mut self.ptr.data
    }
}

impl ::std::fmt::Debug for RcSlice {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error> {
        f.debug_tuple("RcSlice")
            .field(&(self.ptr as *const _))
            .finish()
    }
}

impl Default for RcSlice {
    fn default() -> Self {
        RcSlice::with_len(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::slice;

    #[test]
    fn new() {
        let a = RcSlice::from_slice(&[1, 2, 3, 4]);
        for i in 0..4 {
            assert_eq!(a[i as usize], i+1);
        }
    }

    #[test]
    fn clone() {
        let a = RcSlice::from_slice(&[5, 6, 7, 8, 9]);
        assert_eq!(a.count(), 1);
        let b = a.clone();
        assert_eq!(a.count(), 2);
        assert_eq!(b.count(), 2);
        for i in 0..5 {
            assert_eq!(a[i as usize], i+5);
        }
        for i in 0..5 {
            assert_eq!(b[i], a[i]);
        }
        assert_eq!(&*b, &*a);
        assert_eq!(a.count(), 2);
        drop(a);
        assert_eq!(b.count(), 1);
    }

    #[test]
    fn into_ptr() {
        let slice: &[u8] = &[7, 8, 9, 10, 11, 12];
        let r = RcSlice::from_slice(slice);
        let r2 = r.clone();
        let ptr = r.into_ptr();
        let s2 = unsafe { slice::from_raw_parts(ptr, slice.len()) };
        assert_eq!(s2, slice);
        assert_eq!(s2, &*r2);
    }

    #[test]
    fn from_ptr() {
        let slice: &[u8] = &[13, 14, 15, 16, 17, 18, 19, 20];
        let r = RcSlice::from_slice(slice);
        let r2 = r.clone();
        let ptr = r.into_ptr();
        let len = r2.len();
        let r3 = unsafe { RcSlice::from_ptr(ptr, len) };
        assert_eq!(&r3.ptr.count as *const _ as usize, &r2.ptr.count as *const _ as usize);
        assert_eq!(&*r3, &*r2);
    }
}
