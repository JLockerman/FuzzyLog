
use std::{mem, ptr};
use std::cell::RefCell;

use storeables::Storeable;

//XXX UGH this is going to be wildly unsafe...


pub struct Trie<V> {
    root: RootEdge<V>, // is an array
}

type RootEdge<V> = Box<RootTable<V>>;

struct RootTable<V> {
    l3: Shortcut<ValEdge<V>>,
    l2: Shortcut<L3Edge<V>>,
    l1: Shortcut<L2Edge<V>>,
    array: [L1Edge<V>; 4],
    next_entry: u32,
    alloc: AllocPtr<u8>,
}

type L1Edge<V> = Option<Box<[L2Edge<V>; ARRAY_SIZE]>>;
type L2Edge<V> = Option<Box<[L3Edge<V>; ARRAY_SIZE]>>;
type L3Edge<V> = Option<Box<[ValEdge<V>; ARRAY_SIZE]>>;
type ValEdge<V> = *const V;

const LEVEL_BYTES: usize = 8192;
const ARRAY_SIZE: usize = 8192 / 8;
const MASK: u32 = ARRAY_SIZE as u32 - 1;
const SHIFT_LEN: u8 = 10;
const ROOT_SHIFT: u8 = 30;

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
}

/*struct P<V>(*mut V);

impl<V> P<[V; ARRAY_SIZE]> {

    fn new() -> Self {
        //TODO
        P(Box::into_raw(alloc_seg()))
    }

    unsafe fn increment(&mut self) {
        self.0 = self.0.offset(1);
    }
}*/

struct AllocPtr<V> {
    ptr: *mut [V],
    alloc_rem: usize, //should be ptrdiff_t?
}

impl AllocPtr<u8> {

    fn new() -> Self {
        unsafe { mem::zeroed() }
    }

    fn append(&mut self, data: &[u8]) -> *const u8 {
        let storage_size = data.len(); // FIXME
        if self.alloc_rem < storage_size {
            if storage_size > LEVEL_BYTES {
                let mut storage = Vec::with_capacity(storage_size);
                storage.extend_from_slice(data);
                return &mut unsafe { (*Box::into_raw(storage.into_boxed_slice()))[0] }
            }
            self.ptr = unsafe { Box::into_raw(Box::new([0; LEVEL_BYTES])) };
            //TODO
            self.alloc_rem = LEVEL_BYTES;
        }
        let (append_to, rem) = unsafe {&mut *self.ptr}.split_at_mut(storage_size);
        self.ptr = rem;
        let append_to = &mut (*append_to)[0];
        //TODO storage_size * mem::size_of::<V>()?
        self.alloc_rem -= storage_size;
        //safe do to if self.alloc_rem < storage_size at the second line of the function
        unsafe { ptr::copy_nonoverlapping(&data[0], append_to, data.len()) };
        append_to
    }
}

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

#[repr(C)]
pub struct entry_pointer_and_index {
    pub entry: *mut *mut u8,
    pub ptr: *mut u8,
    pub index: u32,
}

impl Trie<u8> {
    pub fn new() -> Self {
        unsafe {
            //FIXME
            Trie { root: Box::new(mem::zeroed()) }
        }
    }

    pub fn append(&mut self, data: &[u8]) -> u32 {
        let root: &mut RootTable<_> =&mut *self.root;
        let val_ptr = root.alloc.append(data);
        let next_entry = root.next_entry;
        if next_entry & 0x3FFFFFFF == 0 {
            debug_assert!(root.l1.cannot_append());
            // new l2
            let index = (next_entry >> ROOT_SHIFT) & MASK;
            assert!(index < 4);
            let loc = &mut root.array[index as usize];
            *loc = unsafe { Some(alloc_seg()) };
            root.l1 = unsafe {Shortcut::new(&mut loc.as_mut().unwrap()[..])};
        }
        if next_entry & 0xfffff == 0 {
            debug_assert!(root.l2.cannot_append());
            // new l3
            let new_chunk = root.l1.append(unsafe { Some(alloc_seg()) });
            root.l2 = unsafe {Shortcut::new(&mut new_chunk.as_mut().unwrap()[..])};
        }
        if next_entry & 0x3ff == 0 {
            debug_assert!(root.l3.cannot_append());
            // new val
            let new_chunk = root.l2.append(unsafe { Some(alloc_seg()) });
            root.l3 = unsafe {Shortcut::new(&mut new_chunk.as_mut().unwrap()[..])};
        }
        // fill val
        root.l3.append(val_ptr);
        root.next_entry += 1;
        next_entry
    }

    #[cfg(FALSE)]
    fn insert(&mut self, k: u32, v: &[u8]) -> Option<&u8> {
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

    pub fn get(&self, k: u32) -> Option<&u8> {
        unsafe {
            // let root = self.array;
            // let l1_ptr = index!(root, k, 1);
            let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
            assert!(root_index < 3);
            let l1 = &self.root.array[root_index];
            let l2 = index!(l1, k, 1);
            let l3 = index!(l2, k, 2);
            let val_ptr = index!(l3, k, 3);
            let val_ptr = unsafe { val_ptr.as_ref() };
            match val_ptr {
                None => None,
                Some(v) => Some(v),
            }
        }
    }

    #[inline(always)]
    pub fn entry<'s>(&'s mut self, k: u32) -> Entry<'s, u8> {
        unsafe {
            let root_index = (((k & 0xffffffff) >> ROOT_SHIFT) & MASK) as usize;
            assert!(root_index <= 3);
            //let l2 = match &mut self.root.array[root_index] {
            //    &mut Some(ref mut ptr) => &mut (**ptr)[root_index],
            //    none => return Entry::Vacant(VacantEntry(k, Vacancy::L1(none))),
            //};
            let l1 = &mut self.root.array[root_index];
            let l2 = entry!(l1, k, 1, L1);
            let l3 = entry!(l2, k, 2, L2);
            let val_ptr = entry!(l3, k, 3, L3);
            if val_ptr.is_null() {
                return Entry::Vacant(VacantEntry(k, Vacancy::Val(val_ptr)))
            }
            let val_ptr: *const u8 = *val_ptr;
            let val_ptr: *mut u8 = val_ptr as *mut _;
            Entry::Occupied(OccupiedEntry(unsafe {&mut *val_ptr }))
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
            //TODO
            Vacant(e) => unsafe { e.insert_with(default, alloc_seg2()) },
        }
    }

    pub fn insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        use self::Entry::*;
        match self {
            Occupied(e) => e.insert_with(default),
            //TODO
            Vacant(e) => unsafe { e.insert_with(default, alloc_seg2()) },
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
                    *slot = Some(alloc_seg());
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
            //FIXME
            *$val_loc = $v();
            *slot = $val_loc;
            let slot: *mut V = (*slot) as *mut _;
            slot.as_mut().unwrap()
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
            //FIXME
            let val_loc = seg as *mut _;
            match entry {
                Val(slot) => {
                    // TODO
                    // *slot = Some(Unique::new(Box::into_raw(Box::new(v))));
                    // let ptr = ualloc_seg() as *mut _;
                    //TODO this is fill_entry!(slot, k, v, 3, val_loc),
                    //FIXME
                    //*val_loc = v();
                    *val_loc = unimplemented!();
                    *slot = val_loc;
                    let slot: *mut V = (*slot) as *mut _;
                    return slot.as_mut().unwrap();
                }
                L0(slot) => {
                    unreachable!()
                    /*let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
                    assert!(root_index < 3);
                    let l1 = &mut match slot {
                        &mut Some(ref mut ptr) => &mut ***ptr,
                        slot => {
                            // *slot = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))));
                            *slot = Some(P::new(alloc_seg() as *mut _));
                            &mut ***slot.as_mut().unwrap()
                        }
                    }[root_index];
                    fill_entry!(l1, k, v, 1, val_loc)*/
                }
                L1(slot) => fill_entry!(slot, k, v, 1, val_loc),
                L2(slot) => fill_entry!(slot, k, v, 2, val_loc),
                L3(slot) => fill_entry!(slot, k, v, 3, val_loc),
            }
        }
    }

    pub fn insert(self, v: V) -> &'a mut V {
        unsafe { self.insert_with(|| v, alloc_seg2()) }
    }
}

//TODO abstract over alloc place
unsafe fn alloc_seg<V>() -> Box<[V; ARRAY_SIZE]> {
    assert_eq!(mem::size_of::<[V; ARRAY_SIZE]>(), LEVEL_BYTES);
    Box::new(mem::zeroed())
}

fn alloc_seg2() -> *mut u8 {
    let b: Box<[u8; LEVEL_BYTES]> = Box::new([0; LEVEL_BYTES]);
    let b = Box::into_raw(b);
    unsafe { &mut (*b)[0] }
}

#[cfg(test)]
pub mod test {

    use super::*;

    use std::mem;

    #[test]
    pub fn empty() {
        let t: Trie<u8> = Trie::new();
        assert!(t.get(0).is_none());
        assert!(t.get(10).is_none());
        assert!(t.get(1).is_none());
        assert!(t.get(0xffff).is_none());
        assert!(t.get(0x0).is_none());
    }

    #[test]
    pub fn append() {
        let mut m = Trie::new();
        for i in 0..255u8 {
            assert_eq!(m.append(&[i]), i as u32);
            // println!("{:#?}", m);
            // assert_eq!(m.get(&i).unwrap(), &i);

            for j in 0..i + 1 {
                let r = m.get(j as u32);
                assert_eq!(r, Some(&j));
            }

            for j in i + 1..1001 {
                let r = m.get(j as u32);
                assert_eq!(r, None);
            }
        }
    }

    pub mod from_hash_map {
        use super::super::*;

        /*#[test]
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
        }*/
    }
}
