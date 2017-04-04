
use std::{mem, ptr, slice};

//TODO require !Drop?
//FIXME should methods take &u8 or *u8?
//TODO make methods default
pub trait Storeable {
    fn size(&self) -> usize;
    unsafe fn ref_to_bytes(&self) -> &u8;
    unsafe fn ref_to_slice(&self) -> &[u8];
    unsafe fn bytes_to_ref(&u8, usize) -> &Self;
    unsafe fn bytes_to_mut(&mut u8, usize) -> &mut Self;
    unsafe fn clone_box(&self) -> Box<Self>;
    unsafe fn copy_to_mut(&self, &mut Self) -> usize;
}

pub trait UnStoreable: Storeable {
    fn size_from_bytes(bytes: &u8) -> usize;

    unsafe fn unstore(bytes: &u8) -> &Self {
        let size = <Self as UnStoreable>::size_from_bytes(bytes);
        <Self as Storeable>::bytes_to_ref(bytes, size)
    }

    unsafe fn unstore_mut(bytes: &mut u8) -> &mut Self {
        let size = <Self as UnStoreable>::size_from_bytes(bytes);
        <Self as Storeable>::bytes_to_mut(bytes, size)
    }
}

impl<V> Storeable for V { //TODO should V be Copy/Clone?
    fn size(&self) -> usize {
        mem::size_of::<Self>()
    }

    unsafe fn ref_to_bytes(&self) -> &u8 {
        mem::transmute(self)
    }

    unsafe fn ref_to_slice(&self) -> &[u8] {
        let ptr = self as *const V as *const u8;
        let size = self.size();
        slice::from_raw_parts(ptr, size)
    }

    unsafe fn bytes_to_ref(val: &u8, _size: usize) -> &Self {
        //TODO assert_eq!(_size, mem::size_of::<Self>());
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
        mem::transmute(self.as_ptr())
    }

    unsafe fn ref_to_slice(&self) -> &[u8] {
        let ptr = self as *const _ as *const u8;
        let size = self.size();
        slice::from_raw_parts(ptr, size)
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
        ptr::copy_nonoverlapping(self.as_ptr(), b.as_mut_ptr(), self.len());
        b
    }

    unsafe fn copy_to_mut(&self, out: &mut Self) -> usize {
        let to_copy = ::std::cmp::min(self.len(), out.len());
        ptr::copy(self.as_ptr(), out.as_mut_ptr(), to_copy);
        to_copy * mem::size_of::<V>()
    }
}

impl<V> UnStoreable for V { //TODO should V be Copy/Clone?
    fn size_from_bytes(_: &u8) -> usize {
        mem::size_of::<Self>()
    }
}

#[test]
fn store_u8() {
    unsafe {
        assert_eq!(32u8.ref_to_slice(), &[32]);
        assert_eq!(0u8.ref_to_slice(), &[0]);
        assert_eq!(255u8.ref_to_slice(), &[255]);
    }
}

#[test]
fn round_u32() {
    unsafe {
        assert_eq!(32u32.ref_to_slice().len(), 4);
        assert_eq!(0u32.ref_to_slice().len(), 4);
        assert_eq!(255u32.ref_to_slice().len(), 4);
        assert_eq!(0xff0fbedu32.ref_to_slice().len(), 4);

        assert_eq!(u32::unstore(&32u32.ref_to_slice()[0]), &32);
        assert_eq!(u32::unstore(&0u32.ref_to_slice()[0]), &0);
        assert_eq!(u32::unstore(&255u32.ref_to_slice()[0]), &255);
        assert_eq!(u32::unstore(&0xff0fbedu32.ref_to_slice()[0]), &0xff0fbedu32);
    }
}
