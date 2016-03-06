// use std::borrow::Borrow;
// use std::fmt;
use std::{mem, ptr};
use std::cell::UnsafeCell;
// use std::ops::{Deref, DerefMut};
use std::ptr::Unique;

pub struct Trie<V> {
    array: RootEdge<V>, // is an array
}

type RootEdge<V> = Option<Unique<[L1Edge<V>; ARRAY_SIZE]>>;
type L1Edge<V> = Option<Unique<[L2Edge<V>; ARRAY_SIZE]>>;
type L2Edge<V> = Option<Unique<[L3Edge<V>; ARRAY_SIZE]>>;
type L3Edge<V> = Option<Unique<[L4Edge<V>; ARRAY_SIZE]>>;
type L4Edge<V> = Option<Unique<[L5Edge<V>; ARRAY_SIZE]>>;
type L5Edge<V> = Option<Unique<[L6Edge<V>; ARRAY_SIZE]>>;
type L6Edge<V> = Option<Unique<[ValEdge<V>; ARRAY_SIZE]>>;
type ValEdge<V> = Option<Unique<V>>;

const ARRAY_SIZE: usize = 8192 / 8;
const MASK: u64 = ARRAY_SIZE as u64 - 1;
const SHIFT_LEN: u8 = 10;
const MAX_DEPTH: u8 = 7;
const ROOT_SHIFT: u8 = 60;
//const MAX_SHIFT: u8 = 54;
// 54, 44, 34, 24, 14, 4
// 60, 50, 40, 30, 20, 10, 0
//  R,  1,  2,  3,  4,  5, v
// 1...7

macro_rules! index {
    ($array:ident, $k:expr, $depth:expr) => {
        {
            let index = (($k >> (ROOT_SHIFT - (SHIFT_LEN * $depth))) & MASK) as usize;
            assert!(index < ARRAY_SIZE, "index: {}", index);
            match *$array {
                None => return None,
                Some(ref ptr) => &(***ptr)[index],
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
                Some(ref mut ptr) => &mut ***ptr,
                ref mut slot => {
                    //*slot = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))));
                    *slot = Some(Unique::new(alloc_seg() as *mut _));
                    &mut ***slot.as_mut().unwrap()
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
                Some(ref mut ptr) => &mut (***ptr)[index],
                ref mut none => {
                    return Entry::Vacant(VacantEntry($k, Vacancy::$constructor(none)))
                }
            }
        }
    };
}

impl<V> Trie<V> {
    pub fn new() -> Self {
        Trie { array: None }
    }

    pub fn insert(&mut self, k: u64, v: V) -> Option<V> {
        // 70
        unsafe {
            let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
            assert!(root_index < ARRAY_SIZE);
            let l1 = &mut match self.array {
                Some(ref mut ptr) => &mut ***ptr,
                ref mut slot => {
                    //*slot = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))));
                    *slot = Some(Unique::new(alloc_seg() as *mut _));
                    &mut ***slot.as_mut().unwrap()
                }
            }[root_index];
            // if self.array.is_none() {
            //    self.array = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))))
            // };
            // let l1 = &mut (***self.array.as_mut().unwrap())[root_index];
            let l2 = insert!(l1, k, 1);
            let l3 = insert!(l2, k, 2);
            let l4 = insert!(l3, k, 3);
            let l5 = insert!(l4, k, 4);
            let l6 = insert!(l5, k, 5);
            let val_ptr = insert!(l6, k, 6);
            if (*val_ptr).is_none() {
                //let ptr = Box::new(v);
                let ptr = alloc_seg(); //TODO
                ptr::copy_nonoverlapping(&v, ptr as *mut _, 1);
                //*val_ptr = Some(Unique::new(Box::into_raw(ptr)));
                *val_ptr = Some(Unique::new(ptr as *mut _));
                None
            } else if let Some(ref mut val) = *val_ptr {
                Some(mem::replace(&mut ***val, v))
            } else {
                unreachable!()
            }
        }
    }

    pub fn get(&self, k: u64) -> Option<&V> {
        unsafe {
            // let root = self.array;
            // let l1_ptr = index!(root, k, 1);
            let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
            assert!(root_index < ARRAY_SIZE);
            let l1 = match self.array {
                None => return None,
                Some(ref ptr) => &(***ptr)[root_index],
            };

            let l2 = index!(l1, k, 1);
            let l3 = index!(l2, k, 2);
            let l4 = index!(l3, k, 3);
            let l5 = index!(l4, k, 4);
            let l6 = index!(l5, k, 5);
            let val_ptr = index!(l6, k, 6);
            match &*val_ptr {
                &None => None,
                &Some(ref v) => Some(&***v),
            }
        }
    }

    pub fn entry(&mut self, k: u64) -> Entry<V> {
        unsafe {
            let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
            assert!(root_index < ARRAY_SIZE);
            let l1 = match self.array {
                Some(ref mut ptr) => &mut (***ptr)[root_index],
                ref mut none => return Entry::Vacant(VacantEntry(k, Vacancy::L0(none))),
            };

            let l2 = entry!(l1, k, 1, L1);
            let l3 = entry!(l2, k, 2, L2);
            let l4 = entry!(l3, k, 3, L3);
            let l5 = entry!(l4, k, 4, L4);
            let l6 = entry!(l5, k, 5, L5);
            let val_ptr = entry!(l6, k, 6, L6);
            match *val_ptr {
                Some(ref mut ptr) => Entry::Occupied(OccupiedEntry(&mut ***ptr)),
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
pub struct VacantEntry<'a, V: 'a>(u64, Vacancy<'a, V>);

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
            Vacant(e) => e.insert_with(default)
        }
    }

    pub fn insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        use self::Entry::*;
        match self {
            Occupied(e) => e.insert_with(default),
            Vacant(e) => e.insert_with(default),
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
    L4(&'a mut L4Edge<V>),
    L5(&'a mut L5Edge<V>),
    L6(&'a mut L6Edge<V>),
    Val(&'a mut ValEdge<V>),
}

macro_rules! fill_entry {
    ($array:ident, $k:expr, $depth:expr, body) => {
        {
            let index = (($k >> (ROOT_SHIFT - (SHIFT_LEN * $depth))) & MASK) as usize;
            //println!("{}: depth: {}, shift: {}, index: {}", $k, $depth, (ROOT_SHIFT - (SHIFT_LEN * $depth)), index);
            assert!(index < ARRAY_SIZE);
            let l = &mut match *$array {
                Some(ref mut ptr) => &mut ***ptr,
                ref mut slot => {
                    //*slot = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))));
                    *slot = Some(Unique::new(alloc_seg() as *mut _));
                    &mut ***slot.as_mut().unwrap()
                }
            }[index];
            l
        }
    };
    ($array:ident, $k:expr, $v:expr, 1) => {
        {
            let l: &mut L2Edge<_> = fill_entry!($array, $k, 1, body);
            fill_entry!(l, $k, $v, 2)
        }
    };
    ($array:ident, $k:expr, $v:expr, 2) => {
        {
            let l: &mut L3Edge<_> = fill_entry!($array, $k, 2, body);
            fill_entry!(l, $k, $v, 3)
        }
    };
    ($array:ident, $k:expr, $v:expr, 3) => {
        {
            let l: &mut L4Edge<_> = fill_entry!($array, $k, 3, body);
            fill_entry!(l, $k, $v, 4)
        }
    };
    ($array:ident, $k:expr, $v:expr, 4) => {
        {
            let l: &mut L5Edge<_> = fill_entry!($array, $k, 4, body);
            fill_entry!(l, $k, $v, 5)
        }
    };
    ($array:ident, $k:expr, $v:expr, 5) => {
        {
            let l: &mut L6Edge<_> = fill_entry!($array, $k, 5, body);
            fill_entry!(l, $k, $v, 6)
        }
    };
    ($array:ident, $k:expr, $v:expr, 6) => {
        {
            let slot: &mut ValEdge<_> = fill_entry!($array, $k, 6, body);
            //TODO
            //*slot = Some(Unique::new(Box::into_raw(Box::new($v))));
            let ptr = ualloc_seg() as *mut _;
            *ptr = $v();
            *slot = Some(Unique::new(ptr));
            &mut ***slot.as_mut().unwrap()
        }
    };
}

pub fn insert_with<'a, V, F: FnOnce() -> V>(s: VacantEntry<'a, V>, v: F) -> &'a mut V {
    s.insert_with(v)
}

impl<'a, V: 'a> VacantEntry<'a, V> {
    pub fn insert_with<F: FnOnce() -> V>(self, v: F) -> &'a mut V {
        use self::Vacancy::*;
        assert!(mem::size_of::<V>() < 8192);
        let VacantEntry(k, entry) = self;
        unsafe {
            match entry {
                Val(slot) => {
                    // TODO
                    //*slot = Some(Unique::new(Box::into_raw(Box::new(v))));
                    let ptr = ualloc_seg() as *mut _;
                    *ptr = v();
                    *slot = Some(Unique::new(ptr));
                    return &mut ***slot.as_mut().unwrap();
                }
                L0(slot) => {
                    let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
                    assert!(root_index < ARRAY_SIZE);
                    let l1 = &mut match *slot {
                        Some(ref mut ptr) => &mut ***ptr,
                        ref mut slot => {
                            //*slot = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))));
                            *slot = Some(Unique::new(alloc_seg() as *mut _));
                            &mut ***slot.as_mut().unwrap()
                        }
                    }[root_index];
                    fill_entry!(l1, k, v, 1)
                }
                L1(slot) => fill_entry!(slot, k, v, 1),
                L2(slot) => fill_entry!(slot, k, v, 2),
                L3(slot) => fill_entry!(slot, k, v, 3),
                L4(slot) => fill_entry!(slot, k, v, 4),
                L5(slot) => fill_entry!(slot, k, v, 5),
                L6(slot) => fill_entry!(slot, k, v, 6),
            }
        }
    }

    pub fn insert(self, v: V) -> &'a mut V {
        self.insert_with(|| v)
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
}

extern "C" {
    fn alloc_seg() -> *mut u8;
    fn ualloc_seg() -> *mut u8;
}

//#[cfg(test)]
pub mod test {

    use super::*;

    use std::mem;

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

        //#[test]
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

        //#[test]
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

            let mut k = 1000001u64;
            for i in 1..k {
                m.insert(i, i);
            }

            b.iter(|| {
                m.insert(k, k);
                k += 1;
            });
        }

        #[bench]
        fn hash_map_grow_by_insertion(b: &mut Bencher) {
            use std::collections::HashMap;

            let mut m = HashMap::new();

            let mut k = 1000001u64;
            for i in 1..k {
                m.insert(i, i);
            }

            b.iter(|| {
                m.insert(k, k);
                k += 1;
            });
        }

        #[bench]
        fn grow_by_entry_insertion(b: &mut Bencher) {
            let mut m = Trie::new();

            let mut k = 1000001u64;
            for i in 1..k {
                m.entry(i).or_insert(i);
            }

            b.iter(|| {
                m.entry(k).or_insert(k);
                k += 1;
            });
        }

        #[bench]
        fn hash_map_grow_by_entry_insertion(b: &mut Bencher) {
            use std::collections::HashMap;

            let mut m = HashMap::new();

            let mut k = 1000001u64;
            for i in 1..k {
                m.entry(i).or_insert(i);
            }

            b.iter(|| {
                m.entry(k).or_insert(k);
                k += 1;
            });
        }

        #[bench]
        fn find_existing(b: &mut Bencher) {
            let mut m = Trie::new();

            for i in 1..0x18000 {
                m.insert(i, i);
            }

            let mut i = 0;
            let mut a = true;
            b.iter(|| {
                a = a ^ m.get(i).is_some();
                i += 1;
                i &= 0x17fff;
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

        //    for i in 1..100001u64 {
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
