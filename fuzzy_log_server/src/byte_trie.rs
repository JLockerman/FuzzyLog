#![allow(dead_code)]
#![allow(unused_imports)]
use std::{mem, ptr, slice};

use std::marker::PhantomData;

use storeables::Storeable;
use packets::Entry as Packet;

pub struct Trie {
    //TODO should this be boxed?
    root: RootEdge,
}

type RootEdge = Box<RootTable>;

struct RootTable {
    stored_bytes: u64,
    free_front_blocks: u64,
    storage_val: Shortcut<u8>,
    storage_l4: Shortcut<ValEdge>,
    storage_l3: Shortcut<L4Edge>,
    storage_l2: Shortcut<L3Edge>,
    storage_l1: Shortcut<L2Edge>,
    storage_l0: Shortcut<L1Edge>,
    storage_root: L0Edge,
}

type L0Edge = Option<Box<[L1Edge; ARRAY_SIZE]>>;
type L1Edge = Option<Box<[L2Edge; ARRAY_SIZE]>>;
type L2Edge = Option<Box<[L3Edge; ARRAY_SIZE]>>;
type L3Edge = Option<Box<[L4Edge; ARRAY_SIZE]>>;
type L4Edge = Option<Box<[ValEdge; ARRAY_SIZE]>>;
type ValEdge = Option<Box<Val>>;

struct Val([u8; LEVEL_BYTES], [&'static u8; 0]);

impl ::std::ops::Deref for Val {
    type Target = [u8; LEVEL_BYTES];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ::std::ops::DerefMut for Val {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub const LEVEL_BYTES: usize = 8192;
const ARRAY_SIZE: usize = 8192 / 8;
const MASK: u64 = ARRAY_SIZE as u64 - 1;
const SHIFT_LEN: u8 = 10;
const ROOT_SHIFT: u8 = 63;

const L1_MASK:  u64 = 0x001FFFFFFFFFFFFF;
const L2_MASK:  u64 = 0x000007FFFFFFFFFF;
const L3_MASK:  u64 = 0x00000001FFFFFFFF;
const L4_MASK:  u64 = 0x00000000007FFFFF;
const VAL_MASK: u64 = 0x0000000000001FFF;

const SHIFTS: [u64; 6] = [53, 43, 33, 23, 13, 0];
const MASKS: [u64; 6] = [0x3FF, 0x3FF, 0x3FF, 0x3FF, 0x3FF, 0x1FFF];

struct Shortcut<V>(*mut [V]);

// Basically a weak ref which can point at the interior of an array
impl<V> Shortcut<V> {
    unsafe fn new(data: &mut [V]) -> Self {
        Shortcut(data as *mut [V])
    }

    fn append(&mut self, data: V) -> &mut V {
        let (place, rem) = unsafe {&mut *self.0}.split_first_mut().unwrap();
        *place = data;
        self.0 = rem;
        place
    }

    fn cannot_append(&mut self) -> bool {
        unsafe {&mut *self.0}.len() == 0
    }

    fn len(&self) -> usize {
        unsafe {&mut *self.0}.len()
    }
}

impl Shortcut<u8> {
    fn reserve(&mut self, size: usize) -> &mut [u8] {
        let (place, rem) = unsafe {&mut *self.0}.split_at_mut(size);
        self.0 = rem;
        place
    }
}

macro_rules! index {
    ($array:ident, $k:expr, $depth:expr) => {
        (($k >> (SHIFTS[$depth])) & MASKS[$depth]) as usize
    }
}

macro_rules! get {
    ($array:ident, $k:expr, $depth:expr) => {
        {
            let index = index!($array, $k, $depth);
            match *$array {
                None => return None,
                Some(ref ptr) => &(**ptr)[index],
            }
        }
    };
}

macro_rules! get_mut {
    ($array:ident, $k:expr, $depth:expr) => {
        {
            let index = index!($array, $k, $depth);
            match *$array {
                None => return None,
                Some(ref mut ptr) => &mut (**ptr)[index],
            }
        }
    };
}

macro_rules! insert {
    ($array:ident, $k:expr, $depth:expr) => {
        {
            let index = index!($array, $k, $depth);
            match $array {
                &mut Some(ref mut ptr) => &mut (**ptr)[index],
                slot => {
                    *slot = Some(alloc_seg());
                    &mut slot.as_mut().unwrap()[index]
                },
            }
        }
    };
}

impl Trie  {
    pub fn new() -> Self {
        unsafe {
            let mut s = Trie { root: Box::new(mem::zeroed()) };
            {
                s.root.storage_root = Some(alloc_seg());
                s.root.storage_l0 = Shortcut::new(&mut s.root.storage_root.as_mut().unwrap()[..]);
            }
            s
        }
    }

    pub unsafe fn append_at(&mut self, at: u64, size: usize) -> &mut [u8] {
        self.root.append_at(at, size)
    }

    pub unsafe fn get_mut(&mut self, loc: u64, size: usize) -> Option<&mut [u8]> {
        self.root.get_mut(loc, size)
    }

    pub unsafe fn get(&self, loc: u64, size: usize) -> Option<&[u8]> {
        self.root.get(loc, size)
    }

    pub fn append(&mut self, size: usize) -> (&mut [u8], u64) {
        self.root.append(size)
    }

    pub fn len(&self) -> u64 {
        self.root.stored_bytes
    }

    pub fn free_first(&mut self, num_blocks: usize) {
        macro_rules! iter {
            ($slot:expr) => ($slot.as_mut().into_iter().flat_map(|v| v.iter_mut()))
        }

        macro_rules! walk {
            (1 $new:ident in $old:ident $body:tt) => {
                walk!(0 $new, $old, $body; 0)
            };
            (0 $new:ident, $old:ident, $body:tt; {{{{0}}}}) => {
                for $new in iter!($old) $body;
            };
            (0 $new:ident, $old:ident, $body:tt; $depth:tt) => {
                for slot in iter!($old) {
                    walk!(0 $new, slot, $body; {$depth});
                    *slot = None
                }
            };
        }

        let mut next = 0;
        let root = &mut self.root.storage_root;
        walk!(1 slotv in root {
            if next >= num_blocks {
                return
            }
            let slotv: &mut ValEdge = slotv;
            *slotv = None;
            next += 1
        });
    }
}

impl RootTable {
    pub unsafe fn append_at(&mut self, at: u64, size: usize) -> &mut [u8] {
        // use std::cmp::Ordering::*;
        // match at.cmp(&self.stored_bytes) {
        //     Equal => self.append(size).0,
        //     Greater => {
        //         self.reserve_until(at);
        //         self.append(size).0
        //     }
        //     Less => self.get_mut(at, size).unwrap(),
        // }
        self.insert(at, size)
    }

    pub unsafe fn insert(&mut self, loc: u64, size: usize) -> &mut [u8] {
        let root = &mut self.storage_root;
        if loc + size as u64 > self.stored_bytes {
            self.stored_bytes = loc + size as u64;
            //FIXME update shortcut here or at switch to be head?
        }
        let l1 = insert!(root, loc, 0);
        let l2 = insert!(l1, loc, 1);
        let l3 = insert!(l2, loc, 2);
        let l4 = insert!(l3, loc, 3);
        let lv = insert!(l4, loc, 4);
        let val_start = index!(lv, loc, 5);
        let vals = match lv {
            &mut Some(ref mut vals) => vals,
            slot => {
                *slot = Some(alloc_val_level());
                slot.as_mut().unwrap()
            },
        };
        &mut vals[val_start..val_start+size]
    }

    pub unsafe fn get_mut(&mut self, loc: u64, size: usize) -> Option<&mut [u8]> {
        let root = &mut self.storage_root;
        let l1 = get_mut!(root, loc, 0);
        let l2 = get_mut!(l1, loc, 1);
        let l3 = get_mut!(l2, loc, 2);
        let l4 = get_mut!(l3, loc, 3);
        let lv = get_mut!(l4, loc, 4);
        let val_start = index!(lv, loc, 5);
        lv.as_mut().map(|lv| &mut lv[val_start..val_start+size])
    }

    pub unsafe fn get(&self, loc: u64, size: usize) -> Option<&[u8]> {
        let root = &self.storage_root;
        let l1 = get!(root, loc, 0);
        let l2 = get!(l1, loc, 1);
        let l3 = get!(l2, loc, 2);
        let l4 = get!(l3, loc, 3);
        let lv = get!(l4, loc, 4);
        let val_start = index!(lv, loc, 5);
        lv.as_ref().map(|lv| &lv[val_start..val_start+size])
    }

    fn reserve_until(&mut self, end_loc: u64) {
        let start_byte = self.stored_bytes;
        //if they're not in the same segment
        if start_byte & !VAL_MASK != end_loc & !VAL_MASK {
            let remaining = VAL_MASK - (start_byte & VAL_MASK);
            self.append(remaining as usize);
            debug_assert_eq!(self.stored_bytes, start_byte + remaining);
            while self.stored_bytes & !VAL_MASK != end_loc & !VAL_MASK {
                self.append(LEVEL_BYTES);
            }
        }
        //start and end are now in the same segment
        let remaining = end_loc - self.stored_bytes;
        self.append(remaining as usize);
        debug_assert_eq!(self.stored_bytes, end_loc);
    }

    pub fn append(&mut self, size: usize) -> (&mut [u8], u64) {
        unsafe {
            assert!(size <= LEVEL_BYTES);
            let mut start_byte = self.stored_bytes;
            let end_byte = (start_byte + size as u64) - 1;
            // ensure that start and end are in the same segment
            if size > 1 && start_byte & !VAL_MASK != end_byte & !VAL_MASK {
                /*assert_eq!(
                    start_byte & !VAL_MASK,
                    end_byte & !VAL_MASK,
                    "{:x}: {:x} != {:x} :{:x}",
                    start_byte,
                    start_byte & !VAL_MASK,
                    end_byte & !VAL_MASK,
                    end_byte
                );*/
                let new_start = round_up_to_next(start_byte, LEVEL_BYTES as u64);
                //println!(
                //    "byte_trie cannot append {}B @ {}.",
                //    size, start_byte
                //);
                //println!("          filling in remaining {}.", (new_start - start_byte) as usize);
                self.append((new_start - start_byte) as usize);
                //self.stored_bytes += new_start - start_byte;
                start_byte = new_start
            }
            //if we're out of address space there's nothing we can do...
            debug_assert!(self.stored_bytes != 0xFFFFFFFFFFFFFFFF);
            if start_byte & L1_MASK == 0 {
                debug_assert!(self.storage_l1.cannot_append());
                // new l1
                let new_chunk = self.storage_l0.append(Some(alloc_seg()));
                self.storage_l1 = Shortcut::new(&mut new_chunk.as_mut().unwrap()[..]);
            }
            if start_byte & L2_MASK == 0 {
                debug_assert!(self.storage_l2.cannot_append());
                // new l2
                let new_chunk = self.storage_l1.append(Some(alloc_seg()));
                self.storage_l2 = Shortcut::new(&mut new_chunk.as_mut().unwrap()[..]);
            }
            if start_byte & L3_MASK == 0 {
                debug_assert!(self.storage_l3.cannot_append());
                // new l3
                let new_chunk = self.storage_l2.append(Some(alloc_seg()));
                self.storage_l3 = Shortcut::new(&mut new_chunk.as_mut().unwrap()[..]);
            }
            if start_byte & L4_MASK == 0 {
                debug_assert!(self.storage_l4.cannot_append());
                // new l4
                let new_chunk = self.storage_l3.append(Some(alloc_seg()));
                self.storage_l4 = Shortcut::new(&mut new_chunk.as_mut().unwrap()[..]);
            }
            if start_byte & VAL_MASK == 0 {
                debug_assert!(
                    self.storage_val.cannot_append(),
                    "storage_val len: {}",
                    self.storage_val.len()
                );
                // new val
                //TODO let new_chunk = self.storage_l4.append(Some(alloc_level()));
                let new_chunk = self.storage_l4.append(Some(alloc_val_level()));
                let (val, shortcut) = new_chunk.as_mut().unwrap().split_at_mut(size);
                self.stored_bytes += size as u64;
                self.storage_val = Shortcut::new(shortcut);
                (val, start_byte)
            }
            else {
                self.stored_bytes += size as u64;
                (self.storage_val.reserve(size), start_byte)
            }
        }
    }
}

//from rust hashmap
#[inline]
fn round_up_to_next(unrounded: u64, target_alignment: u64) -> u64 {
    assert!(target_alignment.is_power_of_two());
    (unrounded + target_alignment - 1) & !(target_alignment - 1)
}

//TODO abstract over alloc place
unsafe fn alloc_seg<V>() -> Box<[V; ARRAY_SIZE]> {
    assert_eq!(mem::size_of::<[V; ARRAY_SIZE]>(), LEVEL_BYTES);
    Box::new(mem::zeroed())
}

fn alloc_level() -> Box<[u8; LEVEL_BYTES]> {
    unsafe { Box::new(mem::zeroed()) }
}

fn alloc_val_level() -> Box<Val> {
    unsafe { Box::new(mem::zeroed()) }
}

#[cfg(test)]
pub mod test {

    use super::*;

    use packets::SingletonBuilder as Data;

    use packets::{Entry as Packet, OrderIndex};

    #[test]
    pub fn empty() {
        unsafe {
            let mut t = Trie::new();
            assert!(t.get_mut(0, 1).is_none());
            assert!(t.get_mut(10, 1024).is_none());
            assert!(t.get_mut(1, 77).is_none());
            assert!(t.get_mut(0xffff, 2).is_none());
            assert!(t.get_mut(0x0, 0xffffffffffffffff).is_none());
        }
    }

    #[test]
    pub fn append() {
        unsafe {
            let mut m = Trie::new();
            for i in 0..255u8 {
                {
                    let (s, l) = m.append(1);
                    s[0] = i;
                    assert_eq!(l, i as u64);
                    assert_eq!(s, &mut [i][..], "@ {}", i);
                }

                for j in 0..i + 1 {
                    let r = m.get_mut(j as u64, 1);
                    assert_eq!(r, Some(&mut [j][..]), "@ {}", j);
                }

                for j in i + 1..255 {
                    let r = m.get_mut(j as u64, 1);
                    assert_eq!(r, Some(&mut [0][..]), "@ {}", j);
                }
            }
        }
    }

    #[test]
    pub fn more_append() {
        unsafe {
            let mut trie = Trie::new();
            for i in 0..0x18000u64 {
                assert_eq!(trie.len(), i);
                {
                    let (s, l) = trie.append(1);
                    s[0] = i as u8;
                    assert_eq!(l, i);
                    assert_eq!(s, &mut [i as u8][..], "@ {}", i);
                }
                assert_eq!(trie.len(), i+1);
            }

            for j in 0..0x18000u64 {
                assert_eq!(trie.len(), 0x18000);
                assert_eq!(trie.get_mut(j, 1), Some(&mut [j as u8][..]), "@ {}", j);
                assert_eq!(trie.get(j, 1), Some(&[j as u8][..]), "@ {}", j);
                assert_eq!(trie.len(), 0x18000);
            }

            for j in 0..0x18000u64 {
                assert_eq!(trie.len(), 0x18000);
                assert_eq!(trie.get_mut(j, 1), Some(&mut [j as u8][..]), "@ {}", j);
                assert_eq!(trie.len(), 0x18000);
            }

            assert_eq!(trie.len(), 0x18000);

            for j in 0x18000..0x28000 {
                assert_eq!(trie.get_mut(j as u64, 1), None, "@ {}", j);
                assert_eq!(trie.len(), 0x18000);
            }

            {
                let (s, _l) = trie.append(2);
                s[0] = 7;
                s[1] = 9;
            }

            {
                let r = trie.get_mut(0x18000 as u64, 1);
                assert_eq!(r, Some(&mut [7][..]), "@ {}", 0x18000);
            }

            {
                let r = trie.get_mut(0x18001 as u64, 1);
                assert_eq!(r, Some(&mut [9][..]), "@ {}", 0x18000);
            }

            let _r = trie.get_mut(0x18002 as u64, 8190);
            //TODO no longer valid now that we use unimplemented for the last level of the trie
            //assert_eq!(_r, Some(&mut [0; 8190][..]), "@ {}", 0x18002);
        }
    }

    #[test]
    pub fn insert() {
        let mut m = Trie::new();
        for i in 0..255u8 {
            let data = [i, i, i];
            unsafe {
                m.append_at(i as u64 * 3, data.len()).copy_from_slice(&data);
            }
            // println!("{:#?}", m);
            // assert_eq!(m.get(&i).unwrap(), &i);

            for j in 0..i + 1 {
                unsafe {
                    assert_eq!(m.get(j as u64 * 3, data.len()),
                        Some(&[j, j, j][..]),
                        "failed at {:?} in {:?}", j, i,
                    );
                }
            }
        }
    }

/*
    #[test]
    pub fn insert() {
        let mut p = Data(&0, &[OrderIndex(7.into(), 11.into())]).clone_entry();
        let mut m = Trie::new();
        for i in 0..255u8 {
            unsafe { Data(&i, &[OrderIndex(7.into(), (i as u32).into())]).fill_entry(&mut p) }
            unsafe {
                let size = p.entry_size();
                let slot = m.partial_insert(i as u64, size);
                slot.finish_append(&p);
            }
            // println!("{:#?}", m);
            // assert_eq!(m.get(&i).unwrap(), &i);

            for j in 0..i + 1 {
                let r = m.get(j as u64);
                assert_eq!(r.map(|e| e.contents()),
                    Some(Data(&j, &[OrderIndex(7.into(), (j as u32).into())])));
            }

            for j in i + 1..255 {
                let r = m.get(j as u64);
                assert_eq!(r, None);
            }
        }
    }

    #[cfg(TODO)]
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

    #[test]
    pub fn multi_part_insert() {
        let mut p = Data(&32i64, &[OrderIndex(5.into(), 6.into())]).clone_entry();
        unsafe {
            let mut t = Trie::new();
            assert!(t.get(0).is_none());
            assert_eq!(t.len(), 0);
            let slot0 = t.partial_append(p.entry_size());
            assert!(t.get(0).is_none());
            assert_eq!(t.len(), 1);
            let slot1 = t.partial_append(p.entry_size());
            assert!(t.get(0).is_none());
            assert!(t.get(1).is_none());
            assert_eq!(t.len(), 2);
            let slot2 = t.partial_append(p.entry_size());
            assert!(t.get(0).is_none());
            assert!(t.get(1).is_none());
            assert!(t.get(2).is_none());
            assert_eq!(t.len(), 3);
            slot1.finish_append(&p);
            assert!(t.get(0).is_none());
            assert_eq!(t.get(1).map(|e| e.contents()),
                Some(Data(&32i64, &[OrderIndex(5.into(), 6.into())])));
            assert!(t.get(2).is_none());
            assert_eq!(t.len(), 3);
            Data(&1, &[OrderIndex(5.into(), (7 as u32).into())]).fill_entry(&mut p);
            slot0.finish_append(&p);
            assert_eq!(t.get(0).map(|e| e.contents()),
                Some(Data(&1, &[OrderIndex(5.into(), (7 as u32).into())])));
            assert_eq!(t.get(1).map(|e| e.contents()),
                Some(Data(&32i64, &[OrderIndex(5.into(), 6.into())])));
            assert!(t.get(2).is_none());
            assert_eq!(t.len(), 3);
            Data(&-7, &[OrderIndex(5.into(), (92 as u32).into())]).fill_entry(&mut p);
            slot2.finish_append(&p);
            assert_eq!(t.get(0).map(|e| e.contents()),
                Some(Data(&1, &[OrderIndex(5.into(), (7 as u32).into())])));
            assert_eq!(t.get(1).map(|e| e.contents()),
                Some(Data(&32i64, &[OrderIndex(5.into(), 6.into())])));
            assert_eq!(t.get(2).map(|e| e.contents()),
                Some(Data(&-7, &[OrderIndex(5.into(), (92 as u32).into())])));
            assert_eq!(t.len(), 3);
        }
    }

    #[cfg(TODO)]
    pub mod from_hash_map {
        use super::super::*;

        #[test]
        pub fn more_append() {
            let mut m = Trie::new();
            for i in 0..1001 {
                assert_eq!(m.append(&i), i);
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
                assert_eq!(m.append(&i), i);
                //println!("{:#?}", m);
                //println!("{:#?}", i);
                if i > 0 {
                    assert_eq!(m.get(i - 1), Some(&(i - 1)));
                }
                if i >= 3 {
                    assert_eq!(m.get(i - 3), Some(&(i - 3)));
                }
                if i >= 1000 {
                    assert_eq!(m.get(i - 1000), Some(&(i - 1000)));
                }
                assert_eq!(m.get(i), Some(&i));
                assert_eq!(m.get(i + 1), None);
            }

            for j in 0..0x18000 {
                let r = m.get(j);
                assert_eq!(r, Some(&j));
            }

            for j in 0x18000..0x28000 {
                let r = m.get(j);
                assert_eq!(r, None);
            }
        }

        #[test]
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
    }
*/
}
