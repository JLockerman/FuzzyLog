
use std::marker::PhantomData;
use std::{mem, ptr};

//use storeables::Storeable;
//FIXME
use packets::MutEntry as MutPacket;
use packets::Entry as Packet;

use servers2::byte_trie::Trie as Alloc;

//XXX UGH this is going to be wildly unsafe...


pub struct Trie {
    //TODO should this be boxed?
    root: RootEdge,
}

type RootEdge = Box<RootTable>;

//TODO it would be nice to intersperse the shortcuts of the two tries
//     but it is to annoying to code.
struct RootTable {
    l6: Shortcut<ValEdge>,
    next_entry: u64,
    alloc: AllocPtr,
    last_lock: u64,
    //FIXME if we want to do lock handoff we need to know until where we can read...
    last_unlock: u64,
    l5: Shortcut<L6Edge>,
    l4: Shortcut<L5Edge>,
    l3: Shortcut<L4Edge>,
    l2: Shortcut<L3Edge>,
    l1: Shortcut<L2Edge>,
    array: [L1Edge; 16],
}

pub type ByteLoc = u64;
pub type TrieIndex = u64;

//Remaining Address => 60 Bits
type L1Edge = Option<Box<[L2Edge; ARRAY_SIZE]>>;
///Remaining Address => 50 Bits
type L2Edge = Option<Box<[L3Edge; ARRAY_SIZE]>>;
//Remaining Address => 40 Bits
type L3Edge = Option<Box<[L4Edge; ARRAY_SIZE]>>;
//Remaining Address => 30 Bits
type L4Edge = Option<Box<[L5Edge; ARRAY_SIZE]>>;
//Remaining Address => 20 Bits
type L5Edge = Option<Box<[L6Edge; ARRAY_SIZE]>>;
//Remaining Address => 10 Bits
type L6Edge = Option<Box<[ValEdge; ARRAY_SIZE]>>;
//Remaining Address => 0 Bits
type ValEdge = *const u8;

/*
after each level
64 bits: 0xFFFFFFFFFFFFFFFF
root 4b: 0x0FFFFFFFFFFFFFFF
l1  10b: 0x0003FFFFFFFFFFFF
l2  10b: 0x000000FFFFFFFFFF
l3  10b: 0x000000003FFFFFFF
l4  10b: 0x00000000000FFFFF
l5  10b: 0x00000000000003FF
l6  10b: 0x0000000000000000
*/

/*
TODO if we have the last use 14b to index (16384Bytes) we don't need the root table...
TODO we can probably do something more clever than byte addressing
for alloc, last level uses last 13b: 0x1FFF
51 bits: 0xFFFFFFFFFFFFFFFF
root 1b: 0x7FFFFFFFFFFFFFFF
l1  10b: 0x001FFFFFFFFFFFFF
L2  10b: 0x000007FFFFFFFFFF
L3  10b: 0x00000001FFFFFFFF
L4  10b: 0x00000000007FFFFF
L5  10b: 0x0000000000001FFF
*/

const LEVEL_BYTES: usize = 8192;
const ARRAY_SIZE: usize = 8192 / 8;
const MASK: u64 = ARRAY_SIZE as u64 - 1;
const SHIFT_LEN: u8 = 10;
const ROOT_SHIFT: u8 = 60;

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

struct AllocPtr {
    alloc: Alloc,
}

impl AllocPtr {

    #[allow(dead_code)]
    fn new() -> Self {
        AllocPtr { alloc: Alloc::new() }
    }

    fn append(&mut self, data: Packet) -> (*mut u8, ByteLoc) {
        let bytes = data.bytes();
        let storage_size = bytes.len(); // FIXME
        let (append_to, loc) = self.prep_append(storage_size);
        //safe do to: if self.alloc_rem < storage_size {..} at first line of fn prep_append
        append_to.copy_from_slice(bytes);
        (append_to.as_mut_ptr(), loc)
    }

    fn prep_append(&mut self, storage_size: usize) -> (&mut [u8], ByteLoc) {
        if storage_size <= LEVEL_BYTES / 2 {
                self.alloc.append(storage_size)
        }
        else {
            let mut storage = Vec::with_capacity(storage_size);
            let ptr = unsafe {
                storage.set_len(storage_size);
                let bx = storage.into_boxed_slice();
                debug_assert_eq!(bx.len(), storage_size);
                &mut *Box::into_raw(bx)
            };
            (ptr, ::std::u64::MAX)
        }
    }

    unsafe fn alloc_at(&mut self, start: ByteLoc, size: usize) -> *mut u8 {
        self.alloc.append_at(start, size).as_mut_ptr()
    }

    unsafe fn get_mut(&mut self, loc: ByteLoc, size: usize) -> *mut u8 {
        self.alloc.get_mut(loc, size).unwrap().as_mut_ptr()
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

macro_rules! index_mut {
    ($array:ident, $k:expr, $depth:expr) => {
        {
            let index = (($k >> (ROOT_SHIFT - (SHIFT_LEN * $depth))) & MASK) as usize;
            assert!(index < ARRAY_SIZE, "index: {}", index);
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
            let index = (($k >> (ROOT_SHIFT - (SHIFT_LEN * $depth))) & MASK) as usize;
            assert!(index < ARRAY_SIZE, "index: {}", index);
            match *$array {
                Some(ref mut ptr) => &mut (**ptr)[index],
                ref mut slot => {
                    *slot = Some(alloc_seg());
                    &mut slot.as_mut().unwrap()[index]
                }
            }
        }
    };
}

//
// macro_rules! insert {
    // ($array:ident, $k:expr, $depth:expr) => {
        // {
            // let index = (($k >> (ROOT_SHIFT - (SHIFT_LEN * $depth))) & MASK) as usize;
            // assert!(index < ARRAY_SIZE);
            // let l = &mut match *$array {
                // Some(ref mut ptr) => &mut **ptr,
                // ref mut slot => {
                //    *slot = Some(Unique::new(Box::into_raw(Box::new(mem::zeroed()))));
                    // *slot = Some(P::new(alloc_seg() as *mut _));
                    // &mut **slot.as_mut().unwrap()
                // }
            // }[index];
            // l
        // }
    // };
// }


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

//TODO wildly unsafe
#[must_use]
#[repr(C)]
pub struct AppendSlot<V> {
    trie_entry: *mut *const u8,
    data_ptr: *mut u8,
    data_size: usize,
    storage_loc: ByteLoc,
    _pd: PhantomData<*mut V>,
}

unsafe impl<V> Send for AppendSlot<V> where V: Sync {}

impl<'a> AppendSlot<Packet<'a>> {
    pub unsafe fn finish_append(self, data: Packet) -> Packet {
        use std::sync::atomic::{AtomicPtr, Ordering};

        let AppendSlot {trie_entry, data_ptr, data_size, ..} = self;
        let bytes = data.bytes();
        let storage_size = bytes.len();
        assert_eq!(data_size, storage_size);
        ptr::copy_nonoverlapping::<u8>(bytes.as_ptr(), data_ptr, storage_size);
        //*trie_entry = data_ptr;
        let trie_entry: *mut AtomicPtr<u8> =
            mem::transmute::<*mut *const u8, *mut AtomicPtr<u8>>(trie_entry);
        //TODO mem barrier ordering
        (*trie_entry).store(data_ptr, Ordering::Release);
        Packet::wrap(&*data_ptr)
    }

    pub unsafe fn finish_append_with<F>(self, data: Packet, before_insert: F) -> Packet
    where F: FnOnce(MutPacket) {
        use std::sync::atomic::{AtomicPtr, Ordering};

        let AppendSlot {trie_entry, data_ptr, data_size, ..} = self;
        let bytes = data.bytes();
        assert!(bytes.len() >= data_size);
        ptr::copy_nonoverlapping(bytes.as_ptr(), data_ptr, data_size);
        before_insert(MutPacket::wrap(&mut *data_ptr));
        assert_eq!(Packet::wrap(&*data_ptr).bytes().len(), data_size);
        //*trie_entry = data_ptr;
        let trie_entry: *mut AtomicPtr<u8> = mem::transmute(trie_entry);
        //TODO mem barrier ordering
        (*trie_entry).store(data_ptr, Ordering::Release);
        Packet::wrap(&*data_ptr)
    }

    pub fn loc(&self) -> ByteLoc {
        self.storage_loc
    }

    pub unsafe fn extend_lifetime<'b>(self) -> AppendSlot<Packet<'b>> {
        mem::transmute(self)
    }
}

impl Trie
 {
    pub fn new() -> Self {
        unsafe {
            //FIXME gratuitously unsafe
            let mut t = Trie { root: Box::new(mem::zeroed()) };
            ::std::ptr::write(&mut t.root.alloc, AllocPtr::new());
            t
        }
    }

    pub fn append(&mut self, data: Packet) -> TrieIndex {
        self._append(data).0
    }

    fn _append(&mut self, data: Packet) -> (TrieIndex, &mut u8) {
        let (val_ptr, _) = self.root.alloc.append(data);
        let (entry, _) = unsafe { self.prep_append(val_ptr) };
        (entry, unsafe {val_ptr.as_mut().unwrap()})
    }

    pub unsafe fn partial_append_at(&mut self, key: TrieIndex, storage_start: ByteLoc, storage_size: usize)
    -> AppendSlot<Packet> {
        use std::cmp::Ordering::*;
        match key.cmp(&self.root.next_entry) {
            Equal => self.partial_append(storage_size),
            Less => self.get_append_slot(key, storage_start, storage_size),
            Greater => {
                //TODO
                let trie_entry: *mut *const u8;
                loop {
                    let (index, tentry) = self.prep_append(ptr::null());
                    if index == key {
                        trie_entry = tentry;
                        break
                    }
                }
                let val_ptr = self.root.alloc.alloc_at(storage_start, storage_size);
                AppendSlot { trie_entry: trie_entry, data_ptr: val_ptr, data_size: storage_size, storage_loc: storage_start,
                    _pd: Default::default()}
            }
        }
    }

    pub unsafe fn reserve_space(&mut self, size: usize) -> (*mut u8, u64) {
        let (val_ptr, loc) = self.root.alloc.prep_append(size);
        (val_ptr.as_mut_ptr(), loc)
    }

    pub unsafe fn partial_append(&mut self, size: usize) -> AppendSlot<Packet> {
        let (val_ptr, loc): (*mut u8, ByteLoc) = {
            let (val_ptr, loc) = self.root.alloc.prep_append(size);
            (val_ptr.as_mut_ptr(), loc)
        };
        let (_index, trie_entry) = self.prep_append(ptr::null());
        AppendSlot { trie_entry: trie_entry, data_ptr: val_ptr, data_size: size,
            storage_loc: loc, _pd: Default::default()}
    }

    #[cfg(FALSE)]
    pub unsafe fn append_at_with_storage(
        &mut self,
        key: TrieIndex,
        //TODO what type?
        storage: *mut [u8],
    ) -> AppendSlot<Packet> {
        let trie_entry = self.prep_append_at(key, ptr::null());
        let size = (*storage).len();
        let storage = (*storage).as_mut_ptr();
        AppendSlot {
            trie_entry: trie_entry,
            data_ptr: storage,
            data_size: storage_size,
            _pd: PhantomData
        }
    }

    pub unsafe fn prep_append_at(&mut self, key: TrieIndex, val_ptr: *const u8) -> *mut *const u8 {
        use std::cmp::Ordering::*;
        match key.cmp(&self.root.next_entry) {
            Equal => self.prep_append(val_ptr).1,
            Less => self.get_entry_at(key).unwrap(),
            Greater => {
                //TODO
                let trie_entry: *mut *const u8;
                loop {
                    let (index, tentry) = self.prep_append(ptr::null());
                    if index == key {
                        trie_entry = tentry;
                        break
                    }
                }
                trie_entry
            }
        }
    }

    pub unsafe fn prep_append(&mut self, val_ptr: *const u8) -> (TrieIndex, &mut *const u8) {
        let root: &mut RootTable =&mut *self.root;
        let next_entry = root.next_entry;
        //if we're out of address space there's nothing we can do...
        debug_assert!(next_entry != 0xFFFFFFFFFFFFFFFF);
        if next_entry & 0x0FFFFFFFFFFFFFFF == 0 {
            debug_assert!(root.l1.cannot_append());
            // new l2
            let index = (next_entry >> ROOT_SHIFT) & MASK;
            assert!(index < 4);
            let loc = &mut root.array[index as usize];
            *loc = Some(alloc_seg());
            root.l1 = Shortcut::new(&mut loc.as_mut().unwrap()[..]);
        }
        if next_entry & 0x0003FFFFFFFFFFFF == 0 {
            debug_assert!(root.l2.cannot_append());
            // new l3
            let new_chunk = root.l1.append(Some(alloc_seg()));
            root.l2 = Shortcut::new(&mut new_chunk.as_mut().unwrap()[..]);
        }
        if next_entry & 0x000000FFFFFFFFFF == 0 {
            debug_assert!(root.l3.cannot_append());
            // new l4
            let new_chunk = root.l2.append(Some(alloc_seg()));
            root.l3 = Shortcut::new(&mut new_chunk.as_mut().unwrap()[..]);
        }
        if next_entry & 0x000000003FFFFFFF == 0 {
            debug_assert!(root.l4.cannot_append());
            // new l5
            let new_chunk = root.l3.append(Some(alloc_seg()));
            root.l4 = Shortcut::new(&mut new_chunk.as_mut().unwrap()[..]);
        }
        if next_entry & 0x00000000000FFFFF == 0 {
            debug_assert!(root.l5.cannot_append());
            // new l5
            let new_chunk = root.l4.append(Some(alloc_seg()));
            root.l5 = Shortcut::new(&mut new_chunk.as_mut().unwrap()[..]);
        }
        if next_entry & 0x00000000000003FF == 0 {
            debug_assert!(root.l6.cannot_append());
            // new val
            let new_chunk = root.l5.append(Some(alloc_seg()));
            root.l6 = Shortcut::new(&mut new_chunk.as_mut().unwrap()[..]);
        }
        // fill val
        root.next_entry += 1;
        (next_entry, root.l6.append(val_ptr))
    }

    fn get_append_slot(&mut self, k: TrieIndex, storage_start: ByteLoc, size: usize)
    -> AppendSlot<Packet> {
        unsafe {
            let trie_entry: *mut *const u8 = self.get_entry_at(k).unwrap();
            //TODO these should be interleaved...
            let data_ptr = self.root.alloc.get_mut(storage_start, size);
            AppendSlot { trie_entry: trie_entry, data_ptr: data_ptr, data_size: size,
                storage_loc: storage_start, _pd: Default::default()}
        }
    }

    pub fn cannot_lock(&self, lock_num: u64) -> bool {
        self.is_locked() || lock_num != self.root.last_unlock + 1
    }

    pub fn increment_lock(&mut self) {
        self.root.last_lock += 1;
        debug_assert_eq!(self.root.last_lock, self.root.last_unlock + 1);
    }

    pub fn unlock(&mut self, lock_num: u64) -> bool {
        if lock_num == self.root.last_lock {
            debug_assert!(self.root.last_unlock <= self.root.last_lock);
            self.root.last_unlock = lock_num;
            true
        } else {
            false
        }
    }

    pub fn is_locked(&self) -> bool {
        self.root.last_lock != self.root.last_unlock
    }

    pub fn lock_pair(&self) -> (u64, u64) {
        (self.root.last_lock, self.root.last_unlock)
    }

    #[inline]
    fn get_entry_at(&mut self, k: TrieIndex) -> Option<&mut *const u8> {
        let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
        let l1 = &mut self.root.array[root_index];
        let l2 = index_mut!(l1, k, 1);
        let l3 = index_mut!(l2, k, 2);
        let l4 = index_mut!(l3, k, 3);
        let l5 = index_mut!(l4, k, 4);
        let l6 = index_mut!(l5, k, 5);
        Some(index_mut!(l6, k, 6))
    }

    #[cfg(FALSE)]
    pub unsafe fn partial_insert(&mut self, k: u64, size: usize) -> AppendSlot<Packet> {
        let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
        //assert!(root_index <= 3, "root index: {:?} <= 3", root_index);
        let l1 = &mut self.root.array[root_index];
        let l2 = insert!(l1, k, 1);
        let l3 = insert!(l2, k, 2);
        let l4 = insert!(l3, k, 3);
        let l5 = insert!(l4, k, 4);
        let l6 = insert!(l5, k, 5);
        let trie_entry = insert!(l6, k, 6);

        let val_ptr: *mut u8 = unsafe {
            let mut v = Vec::with_capacity(size);
            v.set_len(size);
            (*Box::into_raw(v.into_boxed_slice())).as_mut_ptr()
        };
        AppendSlot { trie_entry: trie_entry, data_ptr: val_ptr, data_size: size,
            _pd: Default::default()}
    }

    #[cfg(FALSE)]
    fn insert(&mut self, k: u32, v: &[u8]) -> Option<&u8> {
        unsafe {
            let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
            //assert!(root_index <= 3, "root index: {:?} <= 3", root_index);
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
}

impl Trie {
    pub fn len(&self) -> u64 {
        self.root.next_entry
    }
}

impl Trie
 {

    pub fn get(&self, k: u64) -> Option<Packet> {
        unsafe {
            // let root = self.array;
            // let l1_ptr = index!(root, k, 1);
            let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
            //assert!(root_index <= 3, "root index: {:?} <= 3", root_index);
            let l1 = &self.root.array[root_index];
            let l2 = index!(l1, k, 1);
            let l3 = index!(l2, k, 2);
            let l4 = index!(l3, k, 3);
            let l5 = index!(l4, k, 4);
            let l6 = index!(l5, k, 5);
            let val_ptr = index!(l6, k, 6);
            let val_ptr = val_ptr.as_ref();
            match val_ptr {
                None => None,
                Some(v) => {
                    //let size = <V as UnStoreable>::size_from_bytes(v);
                    //Some(<V as Storeable>::bytes_to_ref(v, size))
                    Some(Packet::wrap(v))
                }
            }
        }
    }

    /*#[allow(dead_code)]
    #[inline(always)]
    pub fn entry<'s>(&'s mut self, k: u64) -> Entry<'s> {
        //FIXME specialize for the case when k in next
        if k == self.root.next_entry {
            return Entry::Vacant(VacantEntry(k, Vacancy::Next(self)));
        }
        unsafe {
            let root_index = ((k >> ROOT_SHIFT) & MASK) as usize;
            assert!(root_index <= 3, "root index: {:?} <= 3", root_index);
            //let l2 = match &mut self.root.array[root_index] {
            //    &mut Some(ref mut ptr) => &mut (**ptr)[root_index],
            //    none => return Entry::Vacant(VacantEntry(k, Vacancy::L1(none))),
            //};
            let l1 = &mut self.root.array[root_index];
            let l2 = entry!(l1, k, 1, L1);
            let l3 = entry!(l2, k, 2, L2);
            let l4 = entry!(l3, k, 3, L3);
            let l5 = entry!(l4, k, 4, L4);
            let l6 = entry!(l5, k, 5, L5);
            let val_ptr = entry!(l6, k, 6, L6);
            if val_ptr.is_null() {
                return Entry::Vacant(VacantEntry(k, Vacancy::Val(val_ptr)))
            }
            let val_ptr: *mut u8 = *val_ptr as *mut _;
            let val = Packet::wrap_mut(&mut *val_ptr);
            Entry::Occupied(OccupiedEntry(val))
        }
    }*/
}
/*
pub enum Entry<'a> {
    Occupied(OccupiedEntry<'a>),
    Vacant(VacantEntry<'a>),
}

pub struct OccupiedEntry<'a>(Packet<'a>);
pub struct VacantEntry<'a>(u64, Vacancy<'a>);

impl<'a> Entry<'a>
 {

    #[allow(dead_code)]
    pub fn or_insert(self, default: Packet) -> Packet {
        use self::Entry::*;
        match self {
            Occupied(e) => e.into_mut(),
            Vacant(e) => e.insert(default),
        }
    }

    /*#[allow(dead_code)]
    pub fn or_insert_with<F: FnOnce() -> Packet>(self, default: F) -> &'a mut Packet {
        use self::Entry::*;
        match self {
            Occupied(e) => e.into_mut(),
            //FIXME
            Vacant(e) => e.insert_with(default, alloc_seg2()),
        }
    }

    #[allow(dead_code)]
    pub fn insert_with<F: FnOnce() -> Packet>(self, default: F) -> &'a mut Packet {
        use self::Entry::*;
        match self {
            Occupied(e) => e.insert_with(default),
            //FIXME
            Vacant(e) => e.insert_with(default, alloc_seg2()),
        }
    }*/
}

impl<'a> OccupiedEntry<'a>
 {
    #[allow(dead_code)]
    pub fn get(&self) -> &Packet {
        &*self.0
    }

    #[allow(dead_code)]
    pub fn get_mut(&mut self) -> &mut Packet {
        &mut *self.0
    }

    #[allow(dead_code)]
    pub fn into_mut(self) -> &'a mut Packet {
        self.0
    }

    #[allow(dead_code)]
    pub fn insert(&mut self, v: Packet) -> Packet {
        mem::replace(self.0, v)
    }

    /*#[allow(dead_code)]
    pub fn insert_with<F: for<'a> FnOnce() -> Packet>(self, default: F) -> &'a mut Packet {
        *self.0 = default();
        self.into_mut()
    }*/
}

enum Vacancy<'a> {
    Next(&'a mut Trie),
    L1(&'a mut L1Edge),
    L2(&'a mut L2Edge),
    L3(&'a mut L3Edge),
    L4(&'a mut L4Edge),
    L5(&'a mut L5Edge),
    L6(&'a mut L6Edge),
    Val(&'a mut ValEdge),
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
            let l: &mut L2Edge = fill_entry!($array, $k, 1, body);
            fill_entry!(l, $k, $v, 2, $val_loc)
        }
    };
    ($array:ident, $k:expr, $v:expr, 2, $val_loc:ident) => {
        {
            let l: &mut L3Edge = fill_entry!($array, $k, 2, body);
            fill_entry!(l, $k, $v, 3, $val_loc)
        }
    };
    ($array:ident, $k:expr, $v:expr, 3, $val_loc:ident) => {
        {
            let l: &mut L4Edge = fill_entry!($array, $k, 3, body);
            fill_entry!(l, $k, $v, 4, $val_loc)
        }
    };
    ($array:ident, $k:expr, $v:expr, 4, $val_loc:ident) => {
        {
            let l: &mut L5Edge = fill_entry!($array, $k, 4, body);
            fill_entry!(l, $k, $v, 5, $val_loc)
        }
    };
    ($array:ident, $k:expr, $v:expr, 5, $val_loc:ident) => {
        {
            let l: &mut L6Edge = fill_entry!($array, $k, 5, body);
            fill_entry!(l, $k, $v, 6, $val_loc)
        }
    };
    ($array:ident, $k:expr, $v:expr, 6, $val_loc:ident) => {
        {
            let slot: &mut ValEdge = fill_entry!($array, $k, 6, body);
            fill_entry!(val: $v, $val_loc, slot)
        }
    };
    (val: $v:expr, $val_loc:ident, $slot: expr) => {
        {
            let val = $v();
            let size = val.entry_size();
            let val = &val as *const _ as *const u8;
            //FIXME bounds check
            ptr::copy_nonoverlapping(val, $val_loc, size);
            let slot: &mut ValEdge = $slot;
            *slot = $val_loc;
            let slot: *mut Packet<_> = (*slot) as *mut _;
            slot.as_mut().unwrap()
        }
    };
    ($array:ident, $k:expr, $v:expr, 4, $val_loc:ident) => {
        {
            let l: &mut L5Edge = fill_entry!($array, $k, 4, body);
            fill_entry!(l, $k, $v, 5, $val_loc)
        }
    };
    ($array:ident, $k:expr, $v:expr, 5, $val_loc:ident) => {
        {
            let l: &mut L6Edge = fill_entry!($array, $k, 5, body);
            fill_entry!(l, $k, $v, 6, $val_loc)
        }
    };
    ($array:ident, $k:expr, $v:expr, 6, $val_loc:ident) => {
        {
            let slot: &mut ValEdge = fill_entry!($array, $k, 6, body);
// TODO
// *slot = Some(Unique::new(Box::into_raw(Box::new($v))));

        }
    };
}

impl<'a> VacantEntry<'a>
 {
    #[inline(always)]
    pub fn insert_with<F: FnOnce() -> Packet>(self, v: F, seg: *mut u8)
    -> &'a mut Packet {
        use self::Vacancy::*;
        assert!(seg != ptr::null_mut());
        let VacantEntry(k, entry) = self;
        unsafe {
            //FIXME
            let val_loc = seg;
            match entry {
                Val(slot) => return fill_entry!(val: v, val_loc, slot),
                L1(slot) => fill_entry!(slot, k, v, 1, val_loc),
                L2(slot) => fill_entry!(slot, k, v, 2, val_loc),
                L3(slot) => fill_entry!(slot, k, v, 3, val_loc),
                L4(slot) => fill_entry!(slot, k, v, 4, val_loc),
                L5(slot) => fill_entry!(slot, k, v, 5, val_loc),
                L6(slot) => fill_entry!(slot, k, v, 6, val_loc),
                Next(root) => {
                    let val = root._append(&v()).1;
                    Packet::wrap_mut(val)
                }
            }
        }
    }

    #[allow(dead_code)]
    pub fn insert(self, v: Packet) -> &'a mut Packet {
        self.insert_with(|| v, alloc_seg2())
    }
}*/

//TODO abstract over alloc place
unsafe fn alloc_seg<V>() -> Box<[V; ARRAY_SIZE]> {
    assert_eq!(mem::size_of::<[V; ARRAY_SIZE]>(), LEVEL_BYTES);
    Box::new(mem::zeroed())
}

#[allow(dead_code)]
fn alloc_seg2() -> *mut u8 {
    let b: Box<[u8; LEVEL_BYTES]> = Box::new([0; LEVEL_BYTES]);
    let b = Box::into_raw(b);
    unsafe { &mut (*b)[0] }
}

#[cfg(test)]
pub mod test {

    use super::*;

    use packets::SingletonBuilder as Data;

    use packets::{Entry as Packet, OrderIndex};

    #[test]
    pub fn empty() {
        let t = Trie::new();
        assert!(t.get(0).is_none());
        assert!(t.get(10).is_none());
        assert!(t.get(1).is_none());
        assert!(t.get(0xffff).is_none());
        assert!(t.get(0x0).is_none());
    }

    #[test]
    pub fn append() {
        let mut p = Data(&0u8, &[OrderIndex(5.into(), 6.into())]).clone_entry();
        let mut m = Trie::new();
        for i in 0..255u8 {
            unsafe { Data(&(i as u8), &[OrderIndex(5.into(), (i as u32).into())])
                .fill_entry(&mut p) }
            assert_eq!(p.contents().into_singleton_builder(),
                Data(&i, &[OrderIndex(5.into(), (i as u32).into())]));
            assert_eq!(m.append(p.entry()), i as u64);
            // println!("{:#?}", m);
            // assert_eq!(m.get(&i).unwrap(), &i);

            for j in 0..i + 1 {
                let r = m.get(j as u64);
                assert_eq!(r.map(|e| e.contents().into_singleton_builder()),
                    Some(Data(&(j as u8), &[OrderIndex(5.into(), (j as u32).into())])));
            }

            for j in i + 1..255 {
                let r = m.get(j as u64);
                assert_eq!(r.map(Packet::contents), None);
            }
        }
    }

    #[test]
    pub fn insert() {
        let mut p = Data(&0u32, &[OrderIndex(7.into(), 11.into())]).clone_entry();
        let mut m = Trie::new();
        for i in 0..255u8 {
            unsafe { Data(&(i as u32), &[OrderIndex(7.into(), (i as u32).into())])
                .fill_entry(&mut p) }
            unsafe {
                let size = p.entry_size();
                let slot = m.partial_append_at(i as u64, i as u64 * size as u64, size);
                slot.finish_append(p.entry());
            }
            // println!("{:#?}", m);
            // assert_eq!(m.get(&i).unwrap(), &i);

            for j in 0..i + 1 {
                let r = m.get(j as u64);
                assert_eq!(r.map(|e| e.contents().into_singleton_builder()),
                    Some(Data(&(j as u32), &[OrderIndex(7.into(), (j as u32).into())])));
            }

            for j in i + 1..255 {
                let r = m.get(j as u64);
                assert_eq!(r.map(Packet::contents), None);
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
            let slot0 = t.partial_append(p.entry_size()).extend_lifetime();
            assert!(t.get(0).is_none());
            assert_eq!(t.len(), 1);
            let slot1 = t.partial_append(p.entry_size()).extend_lifetime();
            assert!(t.get(0).is_none());
            assert!(t.get(1).is_none());
            assert_eq!(t.len(), 2);
            let slot2 = t.partial_append(p.entry_size()).extend_lifetime();
            assert!(t.get(0).is_none());
            assert!(t.get(1).is_none());
            assert!(t.get(2).is_none());
            assert_eq!(t.len(), 3);
            slot1.finish_append(p.entry());
            assert!(t.get(0).is_none());
            assert_eq!(t.get(1).map(|e| e.contents().into_singleton_builder()),
                Some(Data(&32i64, &[OrderIndex(5.into(), 6.into())])));
            assert!(t.get(2).is_none());
            assert_eq!(t.len(), 3);
            Data(&1i64, &[OrderIndex(5.into(), (7 as u32).into())]).fill_entry(&mut p);
            slot0.finish_append(p.entry());
            assert_eq!(t.get(0).map(|e| e.contents().into_singleton_builder()),
                Some(Data(&1i64, &[OrderIndex(5.into(), (7 as u32).into())])));
            assert_eq!(t.get(1).map(|e| e.contents().into_singleton_builder()),
                Some(Data(&32i64, &[OrderIndex(5.into(), 6.into())])));
            assert!(t.get(2).is_none());
            assert_eq!(t.len(), 3);
            Data(&-7i64, &[OrderIndex(5.into(), (92 as u32).into())]).fill_entry(&mut p);
            slot2.finish_append(p.entry());
            assert_eq!(t.get(0).map(|e| e.contents().into_singleton_builder()),
                Some(Data(&1i64, &[OrderIndex(5.into(), (7 as u32).into())])));
            assert_eq!(t.get(1).map(|e| e.contents().into_singleton_builder()),
                Some(Data(&32i64, &[OrderIndex(5.into(), 6.into())])));
            assert_eq!(t.get(2).map(|e| e.contents().into_singleton_builder()),
                Some(Data(&-7i64, &[OrderIndex(5.into(), (92 as u32).into())])));
            assert_eq!(t.len(), 3);
        }
    }

    #[test]
    pub fn even_more_append() {
        let mut p = Data(&32u64, &[OrderIndex(5.into(), 6.into())]).clone_entry();
        let entry_size = p.entry_size();
        let mut m = Trie::new();
        for i in 0..0x18000u64 {
            assert_eq!(m.len(), i);
            unsafe { Data(&i, &[OrderIndex(5.into(), (i as u32).into())]).fill_entry(&mut p) }
            assert_eq!(m.append(p.entry()), i);
            //println!("{:#?}", m);
            //println!("{:#?}", i);
            if i > 0 {
                assert_eq!(m.get(i - 1).map(|e| e.contents().into_singleton_builder()),
                    Some(Data(&(i - 1), &[OrderIndex(5.into(), ((i-1) as u32).into())])));
            }
            if i >= 3 {
                assert_eq!(m.get(i - 3).map(|e| e.contents().into_singleton_builder()),
                    Some(Data(&(i - 3), &[OrderIndex(5.into(), ((i-3) as u32).into())])));
            }
            if i >= 1000 {
                assert_eq!(m.get(i - 1000).map(|e| e.contents().into_singleton_builder()),
                    Some(Data(&(i - 1000), &[OrderIndex(5.into(), ((i-1000) as u32).into())])));
            }
            assert_eq!(m.get(i).map(|e| e.contents().into_singleton_builder()),
                Some(Data(&i, &[OrderIndex(5.into(), (i as u32).into())])));
            assert!(m.get(i + 1).is_none());
            assert_eq!(m.len(), i + 1);
        }

        assert_eq!(m.len(), 0x18000u64);

        for j in 0..0x18000u64 {
            assert_eq!(m.get(j).map(|e| e.contents().into_singleton_builder()),
                Some(Data(&j, &[OrderIndex(5.into(), (j as u32).into())])));
        }

        assert_eq!(m.len(), 0x18000u64);

        for j in 0x18000..0x28000u64 {
            assert!(m.get(j).is_none());
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
            for i in 0..0x18000u64 {
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

            for j in 0..0x18000u64 {
                let r = m.get(j);
                assert_eq!(r, Some(&j));
            }

            for j in 0x18000..0x28000u64 {
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
}
