// use std::borrow::Borrow;
// use std::fmt;
use std::{mem, ptr};
// use std::ops::{Deref, DerefMut};
use std::ptr::Unique;
use std::ops::{Deref, DerefMut};

use ::entry_pointer_and_index;

pub struct Trie<V> {
    root: RootEdge<V>, // is an array
}

// type RootEdge<V> = Option<Unique<[L1Edge<V>; ARRAY_SIZE]>>;
type RootEdge<V> = Option<P<RootTable<V>>>;
type L1Edge<V> = Option<P<[L2Edge<V>; ARRAY_SIZE]>>;
type L2Edge<V> = Option<P<[L3Edge<V>; ARRAY_SIZE]>>;
type L3Edge<V> = Option<P<[ValEdge<V>; ARRAY_SIZE]>>;
type ValEdge<V> = Option<P<V>>;

struct P<V>(Unique<V>);

impl<V> P<V> {
    unsafe fn new(ptr: *mut V) -> P<V> {
        P(Unique::new(ptr))
    }

    unsafe fn increment(&mut self, count: isize) {
        self.0 = Unique::new(self.0.offset(count));
    }

    unsafe fn increment_by(&mut self, count: isize) {
        self.0 = Unique::new((*self.0 as *mut u8).offset(count) as *mut _);
    }

    //TODO
    fn as_ptr(&mut self) -> *mut V {
        unsafe { *(self.0) }
    }
}

impl<V> Deref for P<V> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        unsafe { &**(self.0) }
    }
}
impl<V> DerefMut for P<V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut **(self.0) }
    }
}

impl<V> Clone for P<V> {
    fn clone(&self) -> Self {
        unsafe { P::new(*self.0) }
    }
}

struct RootTable<V> {
    l3: P<ValEdge<V>>,
    l2: P<L3Edge<V>>,
    l1: P<L2Edge<V>>,
    array: [L1Edge<V>; 4],
    next_entry: u32,
    alloc: P<V>,
    alloc_rem: isize, //should be ptrdiff_t?
}

impl<V> Deref for RootTable<V> {
    type Target = [L1Edge<V>; 4];

    fn deref(&self) -> &Self::Target {
        let &RootTable {ref array, ..} = self;
        array
    }
}
impl<V> DerefMut for RootTable<V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let &mut RootTable {ref mut array, ..} = self;
        array
    }
}

const ARRAY_SIZE: usize = 8192 / 8;
const MASK: u32 = ARRAY_SIZE as u32 - 1;
const SHIFT_LEN: u8 = 10;
// const MAX_DEPTH: u8 = 7;
const ROOT_SHIFT: u8 = 30;
// 30, 20, 10, 0
//  R,  1,  2,  3,  4,  5, v
// 1...7

macro_rules! index {
    ($array:ident, $k:expr, $depth:expr) => {
        {
            let index = (($k >> (ROOT_SHIFT - (SHIFT_LEN * $depth))) & MASK) as usize;
            assert!(index < ARRAY_SIZE, "index: {}", index);
            match *$array {
                None => return None,
                Some(ref ptr) => &(**ptr)[index],
            }
        }
    };
}

macro_rules! insert {
    ($array:ident, $k:expr, $depth:expr) => {
        {
            let index = (($k >> (ROOT_SHIFT - (SHIFT_LEN * $depth))) & MASK) as usize;
            assert!(index < ARRAY_SIZE);
            let l = &mut match *$array {
                Some(ref mut ptr) => &mut **ptr,
                ref mut slot => {
                    //*slot = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))));
                    *slot = Some(P::new(alloc_seg() as *mut _));
                    &mut **slot.as_mut().unwrap()
                }
            }[index];
            l
            //if (*$array).is_none() {
            //    *$array = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))))
            //}
            //let index = (($k >> (SHIFT_LEN * (7 - $depth))) & MASK) as usize;
            //assert!(index < ARRAY_SIZE, "index: {}", index);
            //&mut (***(*$array).as_mut().unwrap())[index]
        }
    };
}

macro_rules! entry {
    ($array:ident, $k:expr, $depth:expr, $constructor:ident) => {
        {
            let index = (($k >> (ROOT_SHIFT - (SHIFT_LEN * $depth))) & MASK) as usize;
            assert!(index < ARRAY_SIZE);
            match *$array {
                Some(ref mut ptr) => &mut (**ptr)[index],
                ref mut none => {
                    return Entry::Vacant(VacantEntry($k, Vacancy::$constructor(none)))
                }
            }
        }
    };
}

impl<V> Trie<V> {
    pub fn new() -> Self {
        unsafe {
            let ptr = alloc_seg() as *mut _;
            Trie { root: Some(P::new(ptr)) }
        }
    }

    pub fn next_entry(&mut self, data_size: usize) -> entry_pointer_and_index {
        unsafe {
            let root: &mut RootTable<_> = match self.root {
                Some(ref mut root) => &mut *root,
                ref mut none => {
                    //let ptr = alloc_seg() as *mut _;
                    //*ptr = mem::zeroed();
                    //*none = Some(P::new(ptr));
                    //&mut *ptr
                    unreachable!()
                }
            };
            let storage_size = data_size as isize; // FIXME
            let mut ptr = if root.alloc_rem >= storage_size {
                let ptr = root.alloc.clone();
                root.alloc.increment_by(storage_size);
                //root.alloc_rem = root.alloc_rem.saturating_sub(storage_size);
                root.alloc_rem -= storage_size;
                ptr
            } else {
                root.alloc = P::new(ualloc_seg() as *mut _);
                root.alloc_rem = 8192 - storage_size; //TODO
                let ptr = root.alloc.clone();
                root.alloc.increment_by(storage_size);
                ptr
            };
            let next_entry = root.next_entry;
            if next_entry & 0x3FFFFFFF == 0 {
                // new l2
                let l1_ptr: *mut [L2Edge<V>; ARRAY_SIZE] = alloc_seg() as *mut _;
                root.l1 = P::new(&mut (*l1_ptr)[0]);
                let index = (next_entry >> ROOT_SHIFT) & MASK;
                assert!(index < 4);
                (*root)[index as usize] = Some(P::new(l1_ptr));
            }
            if next_entry & 0xfffff == 0 {
                // new l3
                let l2_ptr: *mut [L3Edge<V>; ARRAY_SIZE] = alloc_seg() as *mut _;
                root.l2 = P::new(&mut (*l2_ptr)[0]);
                *root.l1 = Some(P::new(l2_ptr));
                root.l1.increment(1);
            }
            if next_entry & 0x3ff == 0 {
                // new val
                let l3_ptr: *mut [ValEdge<V>; ARRAY_SIZE] = alloc_seg() as *mut _;
                root.l3 = P::new(&mut (*l3_ptr)[0]);
                *root.l2 = Some(P::new(l3_ptr));
                root.l2.increment(1);
            }
            // fill val
            let entry: *mut *mut u8 = mem::transmute_copy::<P<ValEdge<V>>, *mut *mut u8>(&root.l3);
            root.l3.increment(1);
            root.next_entry += 1;
            entry_pointer_and_index {
                entry: entry,
                ptr: mem::transmute::<P<V>, *mut u8>(ptr),
                index: next_entry,
            }
        }
    }

    #[allow(dead_code)]
    fn insert(&mut self, k: u32, v: V) -> Option<V> {
        unsafe {
            let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
            assert!(root_index < 3);
            let l1 = &mut match self.root {
                Some(ref mut ptr) => &mut ***ptr,
                ref mut slot => {
                    // *slot = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))));
                    *slot = Some(P::new(alloc_seg() as *mut _));
                    &mut ***slot.as_mut().unwrap()
                }
            }[root_index];
            // if self.array.is_none() {
            //    self.array = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))))
            // };
            // let l1 = &mut (***self.array.as_mut().unwrap())[root_index];
            let l2 = insert!(l1, k, 1);
            let l3 = insert!(l2, k, 2);
            let val_ptr = insert!(l3, k, 3);
            if (*val_ptr).is_none() {
                // let ptr = Box::new(v);
                let ptr = alloc_seg(); //TODO
                ptr::copy_nonoverlapping(&v, ptr as *mut _, 1);
                // *val_ptr = Some(Unique::new(Box::into_raw(ptr)));
                *val_ptr = Some(P::new(ptr as *mut _));
                None
            } else if let Some(ref mut val) = *val_ptr {
                Some(mem::replace(&mut **val, v))
            } else {
                unreachable!()
            }
        }
    }

    pub fn get(&self, k: u32) -> Option<&V> {
        unsafe {
            // let root = self.array;
            // let l1_ptr = index!(root, k, 1);
            let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
            assert!(root_index < 3);
            let l1 = match self.root {
                None => return None,
                Some(ref ptr) => &(***ptr)[root_index],
            };

            let l2 = index!(l1, k, 1);
            let l3 = index!(l2, k, 2);
            let val_ptr = index!(l3, k, 3);
            match &*val_ptr {
                &None => None,
                &Some(ref v) => Some(&**v),
            }
        }
    }

    #[inline(always)]
    pub fn entry(&mut self, k: u32) -> Entry<V> {
        unsafe {
            let root_index = (((k & 0xffffffff) >> ROOT_SHIFT) & MASK) as usize;
            assert!(root_index <= 3);
            let l1 = match self.root {
                Some(ref mut ptr) => &mut (***ptr)[root_index],
                ref mut none => return Entry::Vacant(VacantEntry(k, Vacancy::L0(none))),
            };

            let l2 = entry!(l1, k, 1, L1);
            let l3 = entry!(l2, k, 2, L2);
            let val_ptr = entry!(l3, k, 3, L3);
            match *val_ptr {
                Some(ref mut ptr) => Entry::Occupied(OccupiedEntry(&mut **ptr)),
                ref mut none => Entry::Vacant(VacantEntry(k, Vacancy::Val(none))),
            }
        }
    }
}

pub enum Entry<'a, V: 'a> {
    Occupied(OccupiedEntry<'a, V>),
    Vacant(VacantEntry<'a, V>),
}

pub struct OccupiedEntry<'a, V: 'a>(&'a mut V);
pub struct VacantEntry<'a, V: 'a>(u32, Vacancy<'a, V>);

impl<'a, V: 'a> Entry<'a, V> {
    pub fn or_insert(self, default: V) -> &'a mut V {
        use self::Entry::*;
        match self {
            Occupied(e) => e.into_mut(),
            Vacant(e) => e.insert(default),
        }
    }

    pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        use self::Entry::*;
        match self {
            Occupied(e) => e.into_mut(),
            Vacant(e) => unsafe { e.insert_with(default, ualloc_seg()) },
        }
    }

    pub fn insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        use self::Entry::*;
        match self {
            Occupied(e) => e.insert_with(default),
            Vacant(e) => unsafe { e.insert_with(default, ualloc_seg()) },
        }
    }
}

impl<'a, V: 'a> OccupiedEntry<'a, V> {
    pub fn get(&self) -> &V {
        &*self.0
    }

    pub fn get_mut(&mut self) -> &mut V {
        &mut *self.0
    }

    pub fn into_mut(self) -> &'a mut V {
        self.0
    }

    pub fn insert(&mut self, v: V) -> V {
        mem::replace(self.0, v)
    }

    pub fn insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        *self.0 = default();
        self.into_mut()
    }
}

enum Vacancy<'a, V: 'a> {
    L0(&'a mut RootEdge<V>),
    L1(&'a mut L1Edge<V>),
    L2(&'a mut L2Edge<V>),
    L3(&'a mut L3Edge<V>),
    Val(&'a mut ValEdge<V>),
}

macro_rules! fill_entry {
    ($array:ident, $k:expr, $depth:expr, body) => {
        {
            let index = (($k >> (ROOT_SHIFT - (SHIFT_LEN * $depth))) & MASK) as usize;
// println!("{}: depth: {}, shift: {}, index: {}", $k, $depth, (ROOT_SHIFT - (SHIFT_LEN * $depth)), index);
            assert!(index < ARRAY_SIZE);
            let l = &mut match *$array {
                Some(ref mut ptr) => &mut **ptr,
                ref mut slot => {
// *slot = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))));
                    *slot = Some(P::new(alloc_seg() as *mut _));
                    &mut **slot.as_mut().unwrap()
                }
            }[index];
            l
        }
    };
    ($array:ident, $k:expr, $v:expr, 1, $val_loc:ident) => {
        {
            let l: &mut L2Edge<_> = fill_entry!($array, $k, 1, body);
            fill_entry!(l, $k, $v, 2, $val_loc)
        }
    };
    ($array:ident, $k:expr, $v:expr, 2, $val_loc:ident) => {
        {
            let l: &mut L3Edge<_> = fill_entry!($array, $k, 2, body);
            fill_entry!(l, $k, $v, 3, $val_loc)
        }
    };
    ($array:ident, $k:expr, $v:expr, 3, $val_loc:ident) => {
        {
            let slot: &mut ValEdge<_> = fill_entry!($array, $k, 3, body);
            *$val_loc = $v();
            *slot = Some(P::new($val_loc));
            &mut **slot.as_mut().unwrap()
        }
    };
    ($array:ident, $k:expr, $v:expr, 4, $val_loc:ident) => {
        {
            let l: &mut L5Edge<_> = fill_entry!($array, $k, 4, body);
            fill_entry!(l, $k, $v, 5, $val_loc)
        }
    };
    ($array:ident, $k:expr, $v:expr, 5, $val_loc:ident) => {
        {
            let l: &mut L6Edge<_> = fill_entry!($array, $k, 5, body);
            fill_entry!(l, $k, $v, 6, $val_loc)
        }
    };
    ($array:ident, $k:expr, $v:expr, 6, $val_loc:ident) => {
        {
            let slot: &mut ValEdge<_> = fill_entry!($array, $k, 6, body);
// TODO
// *slot = Some(Unique::new(Box::into_raw(Box::new($v))));

        }
    };
}

impl<'a, V: 'a> VacantEntry<'a, V> {
    #[inline(always)]
    pub fn insert_with<F: FnOnce() -> V>(self, v: F, seg: *mut u8) -> &'a mut V {
        use self::Vacancy::*;
        assert!(mem::size_of::<V>() < 8192);
        assert!(seg != ptr::null_mut());
        let VacantEntry(k, entry) = self;
        unsafe {
            let val_loc = seg as *mut _;
            match entry {
                Val(slot) => {
                    // TODO
                    // *slot = Some(Unique::new(Box::into_raw(Box::new(v))));
                    // let ptr = ualloc_seg() as *mut _;
                    *val_loc = v();
                    *slot = Some(P::new(val_loc));
                    return &mut **slot.as_mut().unwrap();
                }
                L0(slot) => {
                    let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
                    assert!(root_index < 3);
                    let l1 = &mut match *slot {
                        Some(ref mut ptr) => &mut ***ptr,
                        ref mut slot => {
                            // *slot = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))));
                            *slot = Some(P::new(alloc_seg() as *mut _));
                            &mut ***slot.as_mut().unwrap()
                        }
                    }[root_index];
                    fill_entry!(l1, k, v, 1, val_loc)
                }
                L1(slot) => fill_entry!(slot, k, v, 1, val_loc),
                L2(slot) => fill_entry!(slot, k, v, 2, val_loc),
                L3(slot) => fill_entry!(slot, k, v, 3, val_loc),
            }
        }
    }

    pub fn insert(self, v: V) -> &'a mut V {
        unsafe { self.insert_with(|| v, ualloc_seg()) }
    }
}

#[test]
fn non_zero_opt() {
    assert_eq!(mem::size_of::<Trie<u8>>(), mem::size_of::<*const u8>());
    assert_eq!(mem::size_of::<L1Edge<u8>>(), mem::size_of::<*const u8>());
    assert_eq!(mem::size_of::<L2Edge<u8>>(), mem::size_of::<*const u8>());
    assert_eq!(mem::size_of::<L3Edge<u8>>(), mem::size_of::<*const u8>());
}

#[test]
pub fn size() {
    assert_eq!(mem::size_of::<[Trie<u8>; ARRAY_SIZE]>(), 8192);
    assert!(mem::size_of::<RootTable<u8>>() < 8192);
}

extern "C" {
    fn alloc_seg() -> *mut u8;
    fn ualloc_seg() -> *mut u8;
    fn cmemcpy(dst: *mut u8, src: *const u8, count: usize);
}

#[cfg(test)]
pub mod test {

    use super::*;

    use std::mem;

    #[no_mangle]
    pub extern "C" fn alloc_seg() -> *mut u8 {
        Box::into_raw(Box::new([0u8; super::ARRAY_SIZE * 8])) as *mut _ //TODO
    }

    #[no_mangle]
    pub extern "C" fn ualloc_seg() -> *mut u8 {
        unsafe {
            Box::into_raw(Box::new([mem::uninitialized::<u8>(); super::ARRAY_SIZE * 8])) as *mut _ //TODO
        }
    }

    #[test]
    pub fn empty() {
        let t: Trie<()> = Trie::new();
        assert!(t.get(0).is_none());
        assert!(t.get(10).is_none());
        assert!(t.get(1).is_none());
        assert!(t.get(0xffff).is_none());
        assert!(t.get(0x0).is_none());
    }

    #[test]
    pub fn insert() {
        let mut t = Trie::new();
        assert!(t.get(0).is_none());
        assert!(t.insert(0, 5i32).is_none());
        assert!(t.get(10).is_none());
        assert!(t.insert(10, 7i32).is_none());
        assert_eq!(t.get(10).unwrap(), &7);
        assert!(t.get(1).is_none());
        assert!(t.get(0xffff).is_none());
        assert_eq!(t.get(0x0).unwrap(), &5);
    }

    #[test]
    pub fn entry_insert() {
        let mut t = Trie::new();
        assert!(t.get(0).is_none());
        assert_eq!(t.entry(0).or_insert(5i32), &mut 5);
        assert!(t.get(10).is_none());
        assert_eq!(t.entry(10).or_insert(7), &mut 7);
        assert_eq!(t.get(10).unwrap(), &7);
        assert!(t.get(1).is_none());
        assert!(t.get(0xffff).is_none());
        assert_eq!(t.get(0x0).unwrap(), &5);
    }

    pub mod from_hash_map {
        use super::super::*;

        use test_crate::Bencher;

        #[test]
        pub fn test_insert() {
            let mut m = Trie::new();
            // assert_eq!(m.len(), 0);
            assert!(m.insert(1, 2).is_none());
            // assert_eq!(m.len(), 1);
            assert!(m.insert(2, 4).is_none());
            // assert_eq!(m.len(), 2);
            assert_eq!(*m.get(1).unwrap(), 2);
            assert_eq!(*m.get(2).unwrap(), 4);
        }

        #[test]
        pub fn more_insert() {
            let mut m = Trie::new();
            for i in 1..1001 {
                assert!(m.insert(i, i).is_none());
                // println!("{:#?}", m);
                // assert_eq!(m.get(&i).unwrap(), &i);

                for j in 1..i + 1 {
                    let r = m.get(j);
                    assert_eq!(r, Some(&j));
                }

                for j in i + 1..1001 {
                    let r = m.get(j);
                    assert_eq!(r, None);
                }
            }
        }

        #[test]
        pub fn more_append() {
            let mut m = Trie::new();
            for i in 0..1001 {
                assert_eq!(m.append(i).0, i);
                // println!("{:#?}", m);
                // assert_eq!(m.get(&i).unwrap(), &i);

                for j in 0..i + 1 {
                    let r = m.get(j);
                    assert_eq!(r, Some(&j));
                }

                for j in i + 1..1001 {
                    let r = m.get(j);
                    assert_eq!(r, None);
                }
            }
        }

        #[test]
        pub fn even_more_append() {
            let mut m = Trie::new();
            for i in 0..0x18000 {
                assert_eq!(m.append(i).0, i);
                // println!("{:#?}", m);
                // assert_eq!(m.get(&i).unwrap(), &i);

                for j in 0..i + 1 {
                    let r = m.get(j);
                    assert_eq!(r, Some(&j));
                }

                for j in i + 1..0x18000 {
                    let r = m.get(j);
                    assert_eq!(r, None);
                }
            }
        }

        // #[test]
        pub fn more_entry_insert() {
            let mut m = Trie::new();
            for i in 1..1001 {
                assert_eq!(*m.entry(i).or_insert(i), i);
                // println!("{:#?}", m);
                // assert_eq!(m.get(&i).unwrap(), &i);

                for j in 1..i + 1 {
                    let r = m.get(j);
                    assert_eq!(r, Some(&j));
                }

                for j in i + 1..1001 {
                    let r = m.get(j);
                    assert_eq!(r, None);
                }
            }
        }

        #[bench]
        fn grow_by_insertion(b: &mut Bencher) {
            let mut m = Trie::new();

            let mut k = 1000001u32;
            for i in 0..k {
                m.append(i);
            }

            b.iter(|| {
                m.append(k);
                k += 1;
            });
        }

        #[bench]
        fn hash_map_grow_by_insertion(b: &mut Bencher) {
            use std::collections::HashMap;

            let mut m = HashMap::new();

            let mut k = 1000001u32;
            for i in 1..k {
                m.insert(i, i);
            }

            b.iter(|| {
                m.insert(k, k);
                k += 1;
            });
        }

        #[bench]
        fn find_existing(b: &mut Bencher) {
            let mut m = Trie::new();

            for i in 0..0x1800 {
                m.append(i);
            }

            let mut i = 0;
            let mut a = true;
            b.iter(|| {
                a = a ^ m.get(i).is_some();
                i += 1;
                i &= 0x17ff;
                a
            });
        }

        #[bench]
        fn hash_map_find_existing(b: &mut Bencher) {
            use std::collections::HashMap;
            let mut m = HashMap::new();

            for i in 1..0x18000 {
                m.insert(i, i);
            }

            let mut i = 0;
            let mut a = true;
            b.iter(|| {
                a = a ^ m.get(&i).is_some();
                i += 1;
                i &= 0x17fff;
                a
            });
        }

        // #[bench]
        // fn hash_map_find_existing(b: &mut Bencher) {
        //    use std::collections::HashMap;

        // let mut m = HashMap::new();

        //    for i in 1..100001u32 {
        //        m.insert(i, i);
        //    }

        //    let mut a = true;
        //    b.iter(|| {
        //        for i in 1..100001 {
        //            a = a ^ m.contains_key(&i);
        //        }
        //        a
        //    });
        // }
    }
}
