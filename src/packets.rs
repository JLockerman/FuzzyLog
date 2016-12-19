use std::fmt;
use std::marker::PhantomData;
use std::mem::{self, size_of};
use std::ptr;
use std::slice;

pub use uuid::Uuid;

pub use storeables::{Storeable, UnStoreable};

use self::EntryContents::*;

pub use self::EntryKind::EntryLayout;

custom_derive! {
    #[derive(Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Clone, Copy, Default, RustcDecodable, RustcEncodable, NewtypeFrom, NewtypeBitAnd(u32), NewtypeAdd(u32), NewtypeSub(u32), NewtypeMul(u32), NewtypeRem(u32))]
    #[allow(non_camel_case_types)]
    pub struct order(u32);
}

custom_derive! {
    #[derive(Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Clone, Copy, Default, RustcDecodable, RustcEncodable, NewtypeFrom, NewtypeAdd(u32), NewtypeSub(u32), NewtypeMul(u32), NewtypeRem(u32))]
    #[allow(non_camel_case_types)]
    pub struct entry(u32);
}

pub fn order_index_to_u64(o: OrderIndex) -> u64 {
    let hig: u32 = o.0.into();
    let low: u32 = o.1.into();
    let hig = (hig as u64) << 32;
    let low = low as u64;
    hig | low
}

pub fn u64_to_order_index(u: u64) -> OrderIndex {
    let ord = (u & 0xFFFFFFFF00000000) >> 32;
    let ent = u & 0x00000000FFFFFFFF;
    let ord: u32 = ord as u32;
    let ent: u32 = ent as u32;
    (ord.into(), ent.into())
}

pub type OrderIndex = (order, entry);

///////////////////////////////////////
///////////////////////////////////////
///////////////////////////////////////
///////////////////////////////////////

// kinds of packets:
// single chain append
//   needs location, data, deps
// multi chain append
//   needs locations, data, deps, id?
// multi server append
//   needs locations, data, deps, id, lock number
// multi server append
//   needs above + dependent chains
// sentinel
//   needs id, locations?
// lock
//   needs lock number

#[allow(non_snake_case)]
pub mod EntryKind {
    #![allow(non_upper_case_globals)]
    bitflags! {
        #[derive(RustcDecodable, RustcEncodable)]
        flags Kind: u8 {
            const Invalid = 0x0,
            const Data = 0x1,
            const Multiput = 0x2,
            const Lock = 0x4,
            const Sentinel = Lock.bits | Multiput.bits,
            const Read = Data.bits | Multiput.bits,
            const Layout = Data.bits | Multiput.bits | Sentinel.bits,

            const TakeLock = 0x8,

            const ReadSuccess = 0x80,

            const ReadData = Data.bits | ReadSuccess.bits,
            const ReadMulti = Multiput.bits | ReadSuccess.bits,
            const ReadSenti = Sentinel.bits | ReadSuccess.bits,
            //const GotValue = Read.bits | Success.bits,
            const NoValue = Read.bits,
        }
    }

    #[derive(Copy, Clone, PartialEq, Eq, Debug)]
    pub enum EntryLayout {
        Data,
        Multiput,
        Lock,
        Sentinel,
        Read,
    }

    impl EntryLayout {
        pub fn kind(&self) -> Kind {
            match self {
                &EntryLayout::Data => Data,
                &EntryLayout::Multiput => Multiput,
                &EntryLayout::Lock => Lock,
                &EntryLayout::Sentinel => Sentinel,
                &EntryLayout::Read => Read,
            }
        }
    }

    impl Kind {
        pub fn layout(&self) -> EntryLayout {
            match *self & Layout {
                Data => EntryLayout::Data,
                Multiput => EntryLayout::Multiput,
                Lock => EntryLayout::Lock,
                Sentinel => EntryLayout::Sentinel,
                Read => EntryLayout::Read,
                Invalid => panic!("Empty Layout"),
                _ => unreachable!("no layout {:x}", self.bits()),
            }
        }

        pub fn is_taking_lock(&self) -> bool {
            self.contains(TakeLock)
        }
    }
}

pub const MAX_DATA_LEN: usize = 4096 - 8 - (4 + 8 + 16); //TODO

pub const MAX_DATA_LEN2: usize = 4096 - 24; //TODO

//TODO as much as possible we want fields in fixed locations to reduce
//     branchieness and to allow to make it easier to FPGAify
//     All writes need an ID, and num locations, and num bytes

#[repr(C)] //TODO
pub struct Entry<V: ?Sized, F: ?Sized = [u8; MAX_DATA_LEN2]> {
    pub _pd: PhantomData<V>,
    pub id: Uuid,
    pub kind: EntryKind::Kind,
    pub _padding: [u8; 3],
    pub data_bytes: u16,
    pub dependency_bytes: u16,
    pub flex: F,
}

pub const MAX_DATA_DATA_LEN: usize = MAX_DATA_LEN2 - 8; //TODO

#[repr(C)]
pub struct DataFlex<D: ?Sized = [u8; MAX_DATA_DATA_LEN]> {
    pub loc: OrderIndex,
    pub data: D,
}

pub const MAX_MULTI_DATA_LEN: usize = MAX_DATA_LEN2 - 8 - 6 - 2; //TODO

#[repr(C)]
pub struct MultiFlex<D: ?Sized = [u8; MAX_MULTI_DATA_LEN]> {
    pub lock: u64, //TODO should be in multiflex but padding
    pub _padding: [u8; 6],
    pub cols: u16,
    pub data: D,
}

#[repr(C)]
pub struct Lock {
    pub id: Uuid,
    pub kind: EntryKind::Kind,
    pub _padding: [u8; 3],
    pub lock: u64,
}


#[derive(Debug, PartialEq, Eq)]
pub enum EntryContents<'e, V:'e + ?Sized> {
    Data(&'e V, &'e [OrderIndex]),
    // Data:
    // | Kind: 8 | data_bytes: u16 | dependency_bytes: u16 | data: data_bytes | dependencies: dependency_bytes
    Multiput{data: &'e V, uuid: &'e Uuid, columns: &'e [OrderIndex], deps: &'e [OrderIndex]}, //TODO id? committed?
    // Multiput
    // | 8 | cols: u16 (from padding) | 16 | 16 | uuid | start_entries: [order; cols] | data | deps
    Sentinel(&'e Uuid),
}

#[derive(Debug, PartialEq, Eq)]
pub enum EntryContentsMut<'e, V:'e + ?Sized> {
    Data(&'e mut V, &'e mut [OrderIndex]),
    // Data:
    // | Kind: 8 | data_bytes: u16 | dependency_bytes: u16 | data: data_bytes | dependencies: dependency_bytes
    // Data section: data: data_bytes | dependencies: dependency_bytes
    Multiput{data: &'e mut V, uuid: &'e mut Uuid, columns: &'e mut [OrderIndex], deps: &'e mut [OrderIndex]}, //TODO id? committed?
    // Multiput
    // | 8 | cols: u16 (from padding) | 16 | 16 | uuid | start_entries: [order; cols] | data | deps
    // Data section: [OrderIndex; cols] | data: data_bytes | dependencies: dependency_bytes
    Sentinel(&'e mut Uuid),
}

pub struct MultiputContentsMut<'e, V: 'e + ?Sized> {
    pub data: &'e mut V,
    pub uuid: &'e mut Uuid,
    pub columns: &'e mut [OrderIndex],
    pub deps: &'e mut [OrderIndex],
}

impl<V: Storeable + ?Sized, F> Entry<V, F> {

    pub fn contents<'s>(&'s self) -> EntryContents<'s, V> {
        //TODO I should really not need this
        assert!(self as *const _ != ptr::null());
        if self.kind & EntryKind::Layout == EntryKind::Sentinel {
            return Sentinel(&self.id)
        }
        unsafe {
            let data_bytes = self.data_bytes;
            let dependency_bytes = self.dependency_bytes;
            let uuid = &self.id;
            match self.kind & EntryKind::Layout {
                EntryKind::Data => {
                    let r = self.as_data_entry();
                    assert_eq!(r as *const _ as *const u8,
                        self as *const _ as *const u8);
                    r.data_contents(data_bytes)
                }

                EntryKind::Multiput => {
                    let r = self.as_multi_entry();
                    assert_eq!(r as *const _ as *const u8,
                        self as *const _ as *const u8);
                    assert!(r as *const _ != ptr::null());
                    r.multi_contents(data_bytes, dependency_bytes, uuid)
                }
                EntryKind::Lock => panic!("unimpl"),
                _ => unreachable!()
            }
        }
    }

    fn data_and_deps<'s, D: Storeable + ?Sized>(&'s self, contents_ptr: *const u8, data_bytes: u16, data_offset: isize)
    -> (&'s D, &'s [OrderIndex]) {
        unsafe {
            //assert_eq!(data_bytes as usize, mem::size_of::<V>());
            let data_ptr = contents_ptr.offset(data_offset);
            let dep_ptr:*const OrderIndex = data_ptr.offset(self.data_bytes as isize) as *const _;

            //let data = Storeable::bytes_to_ref(data_ptr.as_ref().unwrap(), data_bytes as usize);
            let data = Storeable::bytes_to_ref(mem::transmute(data_ptr), data_bytes as usize);

            let num_deps = (self.dependency_bytes as usize)
                .checked_div(mem::size_of::<OrderIndex>()).unwrap();
            let deps = slice::from_raw_parts(dep_ptr, num_deps);
            (data, deps)
        }
    }

    /*pub fn contents_mut<'s>(&'s mut self) -> EntryContentsMut<'s, V> {
        unsafe {
            use self::EntryContentsMut::*;
            //let contents_ptr: *mut u8 = &self.data as *mut _;
            //TODO this might be invalid...
            let contents_ptr: *mut u8 = &mut self.data as *mut _ as *mut u8;
            let data_ptr = contents_ptr.offset(self.data_start_offset());
            let dep_ptr:*mut OrderIndex = data_ptr.offset(self.data_bytes as isize)
                as *mut _;
            //println!("datap {:?} depp {:?}", data_ptr, dep_ptr);
            let num_deps = (self.dependency_bytes as usize)
                .checked_div(size_of::<OrderIndex>()).unwrap();
            let deps = slice::from_raw_parts_mut(dep_ptr, num_deps);
            match self.kind & EntryKind::Layout {
                EntryKind::Data => {
                    //TODO assert_eq!(self.data_bytes as usize, size_of::<V>());
                    let data = (data_ptr as *mut _).as_mut().unwrap();
                    Data(data, deps)
                }
                EntryKind::Multiput => {
                	let id_ptr: *mut Uuid = contents_ptr as *mut Uuid;
                	let cols_ptr: *mut order = contents_ptr.offset(size_of::<Uuid>() as isize)
                		as *mut _;

                	let data = (data_ptr as *mut _).as_mut().unwrap();
                	let uuid = id_ptr.as_mut().unwrap();
                	let cols = slice::from_raw_parts_mut(cols_ptr, self.cols as usize);
                	Multiput{data: data, uuid: uuid, columns: cols, deps: deps}
                }
                o => unreachable!("{:?}", o),
            }
        }
        panic!()
    }*/

    unsafe fn read_deps(&self) -> &[OrderIndex] {
        assert_eq!(self.kind, EntryKind::NoValue);
        let s = mem::transmute::<_, &Entry<(), DataFlex>>(self);
        let contents_ptr: *const u8 = &s.flex.data as *const _ as *const u8;
        let dep_ptr:*const OrderIndex = contents_ptr as *const _;

        let num_deps = (self.dependency_bytes as usize)
            .checked_div(mem::size_of::<OrderIndex>()).unwrap();
        slice::from_raw_parts(dep_ptr, num_deps)
    }

    pub fn dependencies<'s>(&'s self) -> &'s [OrderIndex] {
        if self.kind == EntryKind::NoValue {
            return unsafe { self.read_deps() };
        }
        if self.is_sentinel() {
            return &[]
        }
        match self.contents() {
            Data(_, ref deps) => &deps,
            Multiput{ref deps, ..} => deps,
            Sentinel(..) => unreachable!(),
        }
    }

    pub unsafe fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), mem::size_of::<Self>());
        let mut entr = mem::uninitialized::<Self>();
        let ptr: *mut _ = &mut entr;
        let ptr: *mut u8 = ptr as *mut _;
        ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, mem::size_of::<Self>());
        entr
    }

    pub unsafe fn wrap_byte(byte: &u8) -> &Self {
        mem::transmute(byte)
    }

    pub unsafe fn wrap_mut(byte: &mut u8) -> &mut Self {
        mem::transmute(byte)
    }

    //FIXME wow this is unsafe
    pub fn wrap_bytes(bytes: &[u8]) -> &Self {
        //assert_eq!(bytes.len(), mem::size_of::<Self>());
        unsafe {
            mem::transmute(&bytes[0])
        }
    }

    //FIXME wow this is unsafe
    pub fn wrap_bytes_mut(bytes: &mut [u8]) -> &mut Self {
        //assert_eq!(bytes.len(), mem::size_of::<Self>());
        unsafe {
            mem::transmute(&mut bytes[0])
        }
    }

    pub unsafe fn as_data_entry(&self) -> &Entry<V, DataFlex> {
        mem::transmute(self)
    }

    pub unsafe fn as_data_entry_mut(&mut self) -> &mut Entry<V, DataFlex> {
        mem::transmute(self)
    }

    pub unsafe fn as_multi_entry(&self) -> &Entry<V, MultiFlex> {
        mem::transmute(self)
    }

    pub unsafe fn as_multi_entry_mut(&mut self) -> &mut Entry<V, MultiFlex> {
        mem::transmute(self)
    }

    pub unsafe fn as_sentinel_entry(&self) -> &Entry<V, MultiFlex> {
        mem::transmute(self)
    }

    pub unsafe fn as_sentinel_entry_mut(&mut self) -> &mut Entry<V, MultiFlex> {
        mem::transmute(self)
    }

    pub unsafe fn as_lock_entry(&self) -> &Lock {
        mem::transmute(self)
    }

    pub unsafe fn as_lock_entry_mut(&mut self) -> &mut Lock {
        mem::transmute(self)
    }

    pub fn bytes(&self) -> &[u8] {
        unsafe {
            let ptr: *const _ = self;
            let ptr: *const u8 = ptr as *const _;
            let size = self.entry_size();
            slice::from_raw_parts(ptr, size)
        }
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            let ptr: *mut _ = self;
            let ptr: *mut u8 = ptr as *mut _;
            let size = self.entry_size();
            slice::from_raw_parts_mut(ptr, size)
        }
    }

    //TODO remove after switching servers and clients to use [u8] as partial packets?
    pub fn sized_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            let ptr: *mut _ = self;
            let ptr: *mut u8 = ptr as *mut _;
            slice::from_raw_parts_mut(ptr, mem::size_of::<Self>())
        }
    }

    pub fn lock_num(&self) -> u64 {
        match self.kind & EntryKind::Layout {
            EntryKind::Multiput => unsafe {
                self.as_multi_entry().flex.lock
            },
            EntryKind::Sentinel => unsafe {
                //TODO currently we treat a Sentinel like a multiappend with no data
                //     nor deps
                self.as_multi_entry().flex.lock
            },
            EntryKind::Lock => unsafe {
                self.as_lock_entry().lock
            },
            _ => unreachable!(),
        }
    }

    pub fn locs(&self) -> &[OrderIndex] {
        unsafe {
            match self.kind & EntryKind::Layout {
                EntryKind::Read | EntryKind::Data =>
                    slice::from_raw_parts(&self.as_data_entry().flex.loc, 1),
                EntryKind::Multiput => self.as_multi_entry().mlocs(),
                EntryKind::Sentinel => self.as_multi_entry().mlocs(),
                EntryKind::Lock => &[],
                _ => unreachable!()
            }
        }
    }

    pub fn locs_mut(&mut self) -> &mut [OrderIndex] {
        unsafe {
            match self.kind & EntryKind::Layout {
                EntryKind::Read | EntryKind::Data =>
                    slice::from_raw_parts_mut(&mut self.as_data_entry_mut().flex.loc, 1),
                EntryKind::Multiput => self.as_multi_entry_mut().mlocs_mut(),
                EntryKind::Sentinel => self.as_multi_entry_mut().mlocs_mut(),
                EntryKind::Lock => &mut [],
                _ => unreachable!()
            }
        }
    }

    pub fn val_locs_and_deps(&self) -> (&V, &[OrderIndex], &[OrderIndex]) {
        match self.contents() {
            Data(ref data, ref deps) => (data, self.locs(), deps),
            Multiput{ref data, ref columns, ref deps, ..} => {
                (data, columns, deps)
            }
            Sentinel(..) => panic!("a sentinel has no value"),
        }
    }

    pub fn header_size(&self) -> usize {
        match self.kind & EntryKind::Layout {
            EntryKind::Data | EntryKind::Read => mem::size_of::<Entry<(), DataFlex<()>>>(),
            EntryKind::Multiput => mem::size_of::<Entry<(), MultiFlex<()>>>(),
            EntryKind::Sentinel => {
                //TODO currently we treat a Sentinel like a multiappend with no data
                //     nor deps
                mem::size_of::<Entry<(), MultiFlex<()>>>()
            }
            EntryKind::Lock => mem::size_of::<Lock>(),
            _ => panic!("header_size: invalid layout {:x} {:?}", self.kind.bits(), self.kind),
        }
    }

    pub fn payload_size(&self) -> usize {
        if self.is_lock() {
            return 0
        }
        let mut size = self.data_bytes as usize + self.dependency_bytes as usize;
        if self.is_multi() || self.is_sentinel() {
            //TODO currently we treat a Sentinel like a multiappend with no data
            //     nor deps
            let header = unsafe { mem::transmute::<_, &Entry<V, MultiFlex<()>>>(self) };
            size += header.flex.cols as usize * mem::size_of::<OrderIndex>();
        }
        assert!(size + mem::size_of::<Entry<(), ()>>() <= 8192);
        size
    }

    pub fn entry_size(&self) -> usize {
        let size = match self.kind & EntryKind::Layout {
            EntryKind::Data | EntryKind::Read => self.data_entry_size(),
            EntryKind::Multiput => self.multi_entry_size(),
            EntryKind::Sentinel => self.sentinel_entry_size(),
            EntryKind::Lock => mem::size_of::<Lock>(),
            _ => panic!("invalid layout {:?}", self.kind & EntryKind::Layout),
        };
        assert!(size >= base_header_size());
        size
    }

    fn data_entry_size(&self) -> usize {
        assert!(self.kind & EntryKind::Layout == EntryKind::Data
            || self.kind & EntryKind::Layout == EntryKind::Read);
        let size = mem::size_of::<Entry<(), DataFlex<()>>>()
            + self.data_bytes as usize
            + self.dependency_bytes as usize;
        //trace!("data size {}", size);
        assert!(size <= 8192);
        size
    }

    fn multi_entry_size(&self) -> usize {
        assert!(self.kind & EntryKind::Layout == EntryKind::Multiput);
        unsafe {
            let header = self.as_multi_entry();
            let size = mem::size_of::<Entry<(), MultiFlex<()>>>()
                + header.data_bytes as usize
                + header.dependency_bytes as usize
                + (header.flex.cols as usize * mem::size_of::<OrderIndex>());
            //trace!("multi size {}", size);
            assert!(size <= 8192);
            size
        }
    }

    pub fn sentinel_entry_size(&self) -> usize {
        assert!(self.kind & EntryKind::Layout == EntryKind::Sentinel
            || self.kind & EntryKind::Layout == EntryKind::Multiput);
        //TODO currently we treat a Sentinel like a multiappend with no data
        //     nor deps
        unsafe {
            let header = self.as_multi_entry();
            let size = mem::size_of::<Entry<(), MultiFlex<()>>>()
                + (header.flex.cols as usize * mem::size_of::<OrderIndex>());
            //trace!("multi size {}", size);
            assert!(size <= 8192);
            size
        }
    }

    pub fn is_multi(&self) -> bool {
        self.kind & EntryKind::Layout == EntryKind::Multiput
    }

    pub fn is_data(&self) -> bool {
        self.kind & EntryKind::Layout == EntryKind::Data
    }

    pub fn is_sentinel(&self) -> bool {
        self.kind & EntryKind::Layout == EntryKind::Sentinel
    }

    pub fn is_lock(&self) -> bool {
        self.kind &EntryKind::Layout == EntryKind::Lock
    }
}

impl<V: Storeable + ?Sized> Entry<V, DataFlex> {
    fn data_contents<'s>(&'s self, data_bytes: u16) -> EntryContents<'s, V> { //TODO to DataContents<..>?
        //TODO I _really_ should not need to do this...
        let contents_ptr: *const u8 = &self.flex.data as *const _ as *const u8;
        let (data, deps) = self.data_and_deps(contents_ptr, data_bytes, 0);
        Data(data, deps)
    }
}

impl<V: Storeable + ?Sized> Entry<V, MultiFlex> {
    pub fn multi_contents_mut<'s>(&'s mut self) -> MultiputContentsMut<'s, V> {
        unsafe {
            //let data_bytes = self.data_bytes;
            //let dependency_bytes = self.dependency_bytes;
            let uuid = &mut self.id;
            let contents_ptr: *mut u8 = &mut self.flex.data as *mut _ as *mut u8;
            let cols_ptr = contents_ptr;


            let cols_ptr = cols_ptr as *mut _;
            let num_cols = self.flex.cols;
            let cols = slice::from_raw_parts_mut(cols_ptr, num_cols as usize);
            assert!(cols.len() > 0);

            let data_offset = (self.flex.cols as usize * mem::size_of::<OrderIndex>()) as isize;
            let data_ptr = contents_ptr.offset(data_offset);
            let dep_ptr:*mut OrderIndex = data_ptr.offset(self.data_bytes as isize) as *mut _;
            //let data = Storeable::bytes_to_mut(data_ptr.as_mut().unwrap(), self.data_bytes as usize);
            let data = Storeable::bytes_to_mut(mem::transmute(data_ptr), self.data_bytes as usize);

            let num_deps = (self.dependency_bytes as usize)
                .checked_div(mem::size_of::<OrderIndex>()).unwrap();
            let deps = slice::from_raw_parts_mut(dep_ptr, num_deps);
            MultiputContentsMut{data: data, uuid: uuid, columns: cols, deps: deps}
        }
    }

    fn mlocs(&self) -> &[OrderIndex] {
        unsafe {
            let contents_ptr: *const u8 = &self.flex.data as *const _ as *const u8;
            let cols_ptr = contents_ptr;
            let cols_ptr = cols_ptr as *const _;
            let num_cols = match self.kind & EntryKind::Layout {
                EntryKind::Multiput | EntryKind::Sentinel => self.flex.cols,
                _ => unreachable!()
            };
            assert!(num_cols > 0);
            slice::from_raw_parts(cols_ptr, num_cols as usize)
        }
    }

    fn mlocs_mut(&mut self) -> &mut [OrderIndex] {
        unsafe {
            let contents_ptr: *mut u8 = &mut self.flex.data as *mut _ as *mut u8;
            let cols_ptr = contents_ptr;
            let cols_ptr = cols_ptr as *mut _;
            let num_cols = match self.kind & EntryKind::Layout {
                EntryKind::Multiput | EntryKind::Sentinel => self.flex.cols,
                EntryKind::Lock => panic!("unimpl"), //TODO
                _ => unreachable!()
            };
            assert!(num_cols > 0);
            slice::from_raw_parts_mut(cols_ptr, num_cols as usize)
        }
    }

    fn multi_contents<'s>(&'s self, data_bytes: u16, _: u16, uuid: &'s Uuid)
    -> EntryContents<'s, V> { //TODO to DataContents<..>?
        unsafe {
            let contents_ptr: *const u8 = &self.flex.data as *const _ as *const u8;
            let cols_ptr = contents_ptr;


            let cols_ptr = cols_ptr as *const _;
            let num_cols = self.flex.cols;
            assert!(num_cols > 0);
            let cols = slice::from_raw_parts(cols_ptr, num_cols as usize);
            assert!(cols.len() > 0);

            let data_offset = (self.flex.cols as usize * mem::size_of::<OrderIndex>()) as isize;
            let (data, deps) = self.data_and_deps(contents_ptr, data_bytes, data_offset);
            Multiput{data: data, uuid: uuid, columns: cols, deps: deps}
        }
    }
}

impl Lock {
    pub fn bytes(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self as *const _ as *const u8, mem::size_of::<Self>())
        }
    }
}

pub fn base_header_size() -> usize {
    mem::size_of::<Entry<(), ()>>()
}

impl<V: PartialEq + Storeable + ?Sized> PartialEq for Entry<V> {
    fn eq(&self, other: &Self) -> bool {
        //TODO
        self.contents() == other.contents()
    }
}

impl<V: Eq + Storeable + ?Sized> Eq for Entry<V> {}

impl<V: fmt::Debug + Storeable + ?Sized> fmt::Debug for Entry<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if self.kind == EntryKind::Invalid {
            panic!("invalid entry")
        }
        else if self.kind == EntryKind::Read {
            let s = unsafe { self.as_data_entry() };
            f.debug_struct("Entry")
                .field("kind", &"Read")
                .field("id", &s.id)
                .field("loc", &s.flex.loc)
            .finish()
        }
        else {
            write!(f, "{:?} with id: {:?}", self.contents(), self.id)
        }
    }
}

impl<V: ?Sized> Clone for Entry<V> {
    fn clone(&self) -> Self {
        //TODO
        unsafe {
            let mut entr: Entry<V> = mem::uninitialized();
            ptr::copy(self as *const _ as *const u8, &mut entr as *mut _ as *mut u8, self.size());
            entr
        }
        //self.contents().clone_entry()
    }
}

impl<'e, V: Storeable + ?Sized> Clone for EntryContents<'e, V> {
    fn clone(&self) -> Self {
        match self {
            &Data(ref data, ref deps) => Data(data.clone(), deps.clone()),
            &Multiput{ref data, ref uuid, ref columns, ref deps} => {
                Multiput{data: data.clone(), uuid: uuid.clone(), columns: columns.clone(), deps: deps.clone()}
            }
            &Sentinel(ref id) => Sentinel(id.clone()),
        }
    }
}

impl<'e, V: Storeable + ?Sized> EntryContents<'e, V> {
    pub fn dependencies<'s>(&'s self) -> &'s [OrderIndex] {
        match self {
            &Data(_, ref deps) => &deps,
            &Multiput{ref deps, ..} => deps,
            &Sentinel(..) => &[],
        }
    }
}

impl <'e, V: ?Sized> EntryContents<'e, V> {
    fn kind(&self) -> EntryKind::Kind {
        match self {
            &Data(..) => EntryKind::Data,
            &Multiput{..} => EntryKind::Multiput,
            &Sentinel(..) => EntryKind::Sentinel,
        }
    }
}

impl<'e, V: Storeable + ?Sized> EntryContents<'e, V> {

    pub fn clone_entry(&self) -> Entry<V> {
        unsafe {
            let mut entr = mem::uninitialized::<Entry<V>>();
            self.fill_entry(&mut entr);
            entr
        }
    }

    pub unsafe fn fill_entry(&self, e: &mut Entry<V>) {
        use std::u16;
        e.kind = self.kind();
        let (data_ptr, data, deps) = match self {
            &Data(data, deps) => {
                assert!(deps.len() < u16::MAX as usize);
                let e = e.as_data_entry_mut();
                let data_ptr: *mut u8 = &mut (&mut e.flex.data)[0];
                (data_ptr, data, deps)
            }
            &Multiput{data, uuid, columns, deps} => {
                assert!(deps.len() < u16::MAX as usize);
                assert!(columns.len() < u16::MAX as usize);
                assert!(columns.len() > 0);
                let e = e.as_multi_entry_mut();
                let cols_ptr: *mut OrderIndex = (&mut e.flex.data[0]) as *mut _ as *mut _;
                e.flex.cols = columns.len() as u16;
                assert!(e.flex.cols > 0);
                e.id = uuid.clone();

                ptr::copy(columns.as_ptr() as *mut _, cols_ptr, columns.len());

                let data_ptr = cols_ptr.offset(columns.len() as isize) as *mut u8;
                e.flex.lock = 0;
                (data_ptr, data, deps)
            }
            &Sentinel(id) => {
                e.id = id.clone();
                return
            }
        };
        assert!(Storeable::size(data) < u16::MAX as usize);
        assert!(deps.len() * size_of::<OrderIndex>() < u16::MAX as usize);
        e.data_bytes = Storeable::size(data) as u16;
        e.dependency_bytes = (deps.len() * size_of::<OrderIndex>()) as u16;
        let dep_ptr = data_ptr.offset(e.data_bytes as isize);

        //trace!("assembing {:?} entry: base {:?}, data @ {:?} len {:?}, deps @ {:?} len {:?}",
        //    self.kind(), e as *mut _, data_ptr, Storeable::size(data), dep_ptr, deps.len());

        //ptr::write(data_ptr as *mut _, data.clone()); //TODO why did this fail?
        ptr::copy(Storeable::ref_to_bytes(data), data_ptr as *mut _, e.data_bytes as usize);
        ptr::copy(deps.as_ptr(), dep_ptr as *mut _, deps.len());
        if e.kind == EntryKind::Multiput {
            let e = e.as_multi_entry_mut();
            assert!(e.flex.cols > 0);
        }
    }

    pub fn clone_bytes(&self) -> Vec<u8> {
        let size = match self {
            &Data(data, deps) =>
                mem::size_of::<Entry<(), DataFlex<()>>>()
                 + Storeable::size(data) + (deps.len() * size_of::<OrderIndex>()),
            &Multiput{ data, columns, deps, ..} =>
                mem::size_of::<Entry<(), MultiFlex<()>>>()
                + Storeable::size(data) + (deps.len() * size_of::<OrderIndex>())
                + (columns.len() * size_of::<OrderIndex>()),
            &Sentinel(..) => {
                //TODO currently we treat a Sentinel like a multiappend with no data
                //     nor deps
                mem::size_of::<Entry<(), MultiFlex<()>>>()
                + size_of::<OrderIndex>()
            }
        };
        //FIXME
        //assert!(size <= 4096);
        //assert!(size <= 8192);
        let mut buffer = Vec::with_capacity(size);
        unsafe {
            buffer.set_len(size);
            self.fill_entry(Entry::wrap_bytes_mut(&mut buffer));
        }
        buffer
    }

    pub fn fill_vec<'s, 'a>(&'s self, vec: &'a mut Vec<u8>) -> &'a mut Entry<V> {
        let size = match self {
            &Data(data, deps) =>
                mem::size_of::<Entry<(), DataFlex<()>>>()
                 + Storeable::size(data) + (deps.len() * size_of::<OrderIndex>()),
            &Multiput{ data, columns, deps, ..} =>
                mem::size_of::<Entry<(), MultiFlex<()>>>()
                + Storeable::size(data) + (deps.len() * size_of::<OrderIndex>())
                + (columns.len() * size_of::<OrderIndex>()),
            &Sentinel(..) => {
                //TODO currently we treat a Sentinel like a multiappend with no data
                //     nor deps
                mem::size_of::<Entry<(), MultiFlex<()>>>()
                + size_of::<OrderIndex>()
            }
        };
        if vec.capacity() < size {
            let add_cap = size - vec.capacity();
            vec.reserve_exact(add_cap)
        }
        unsafe {
            vec.set_len(size);
            let e = Entry::wrap_bytes_mut(&mut *vec);
            self.fill_entry(e);
            e
        }
    }
}

pub fn bytes_as_entry(bytes: &[u8]) -> &Entry<()> {
    Entry::<()>::wrap_bytes(bytes)
}

pub fn bytes_as_entry_mut(bytes: &mut [u8]) -> &mut Entry<()> {
    Entry::<()>::wrap_bytes_mut(bytes)
}

///////////////////////////////////////
///////////////////////////////////////
///////////////////////////////////////
///////////////////////////////////////

#[cfg(test)]
mod test {

    use super::*;
    use super::EntryContents::*;

    use std::marker::PhantomData;


    #[test]
    fn test_entry2_header_size() {
        use std::mem::size_of;
        assert_eq!(size_of::<Entry<(), ()>>(), 24);
    }

    #[test]
    fn test_entry2_data_header_size() {
        use std::mem::size_of;
        assert_eq!(size_of::<DataFlex<()>>(), 8);
    }

    #[test]
    fn test_entry2_multi_header_size() {
        use std::mem::size_of;
        assert_eq!(size_of::<MultiFlex<()>>(), 16);
    }

    #[repr(C)] //TODO
    pub struct PrimEntry<V, F: ?Sized = [u8; MAX_DATA_LEN2]> {
        _pd: PhantomData<V>,
        pub id: [u64; 2],
        pub kind: EntryKind::Kind,
        pub _padding: [u8; 1],
        pub data_bytes: u16,
        pub dependency_bytes: u16,
        pub flex: F,
    }

    #[test]
    fn test_entry_primitive() {
        use std::mem::size_of;
        assert_eq!(size_of::<Entry<()>>(), size_of::<PrimEntry<()>>());
    }

    #[test]
    fn test_entry2_data_header() {
        use std::mem::size_of;
        assert_eq!(size_of::<Entry<(), DataFlex<()>>>(),
            size_of::<Entry<(), ()>>() + size_of::<DataFlex<()>>());
    }

    #[test]
    fn test_entry2_multi_header() {
        use std::mem::size_of;
        assert_eq!(size_of::<Entry<(), MultiFlex<()>>>(), size_of::<Entry<(), ()>>() + size_of::<MultiFlex<()>>());
    }

    #[test]
    fn test_entry2_size() {
        use std::mem::size_of;
        assert_eq!(size_of::<Entry<()>>(), 4096);
    }

    #[test]
    fn test_entry2_data_size() {
        use std::mem::size_of;
        assert_eq!(size_of::<Entry<(), DataFlex>>(), 4096);
    }

    #[test]
    fn test_entry2_multi_size() {
        use std::mem::size_of;
        assert_eq!(size_of::<Entry<(), MultiFlex>>(), 4096);
    }

    #[test]
    fn multiput_entry_convert() {
        use std::fmt::Debug;
        //test_(1231123, &[(01.into(), 10.into()), (02.into(), 201.into())], &[]);
        //test_(3334, &[], &[]);
        //test_(1231123u64, &[(01.into(), 10.into()), (02.into(), 201.into())], &[]);
        //test_(3334u64, &[], &[]);
        //test_(3334u64, &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[]);
        //test_((3334u64, 1231123), &[], &[]);
        //test_((3334u64, 1231123), &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[]);

        test_(1231123, &[(01.into(), 10.into()), (02.into(), 201.into())], &[(32.into(), 0.into()), (402.into(), 5.into())]);
        test_(3334, &[], &[(32.into(), 0.into()), (402.into(), 5.into())]);
        test_(1231123u64, &[(01.into(), 10.into()), (02.into(), 201.into())], &[(32.into(), 0.into()), (402.into(), 5.into())]);
        test_(3334u64, &[], &[(32.into(), 0.into()), (402.into(), 5.into())]);
        test_(3334u64, &[(01.into(), 10.into())], &[(02.into(), 9.into()), (201.into(), 0.into()), (02.into(), 57.into()), (201.into(), 0xffffffff.into()), (02.into(), 0xdeadbeef.into()), (201.into(), 2.into()), (02.into(), 6.into()), (201.into(), 201.into())]);
        test_(3334u64, &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[(32.into(), 0.into()), (402.into(), 5.into())]);
        test_((3334u64, 1231123), &[], &[(32.into(), 0.into()), (402.into(), 5.into())]);
        test_((3334u64, 1231123), &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[(32.into(), 0.into()), (402.into(), 5.into())]);

        fn test_<T: Clone + Debug + Eq>(data: T, deps: &[OrderIndex], cols: &[OrderIndex]) {
            let id = Uuid::new_v4();
            let ent1 = Multiput{
            	data: &data,
                uuid: &id,
                deps: &deps,
                columns: cols,
            };
            let entr = Box::new(ent1.clone_entry());
            let ent2 = entr.contents();
            assert_eq!(ent1, ent2);
        }
    }

    #[cfg(test)]
    #[test]
    fn data_entry_convert() {
        use std::fmt::Debug;
        test_(1231123, &[(01.into(), 10.into()), (02.into(), 201.into())]);
        test_(3334, &[]);
        test_(1231123u64, &[(01.into(), 10.into()), (02.into(), 201.into())]);
        test_(3334u64, &[]);
        test_(3334u64, &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())]);
        test_((3334u64, 1231123), &[]);
        test_((3334u64, 1231123), &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())]);

        fn test_<T: Clone + Debug + Eq>(data: T, deps: &[OrderIndex]) {
            let ent1 = Data(&data, &deps);
            let entr = Box::new(ent1.clone_entry());
            let ent2 = entr.contents();
            assert_eq!(ent1, ent2);
        }
    }
}
