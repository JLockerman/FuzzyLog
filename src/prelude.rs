
use std::collections::HashMap;
use std::convert::Into;
use std::fmt;
use std::marker::PhantomData;
use std::mem::{self, size_of};
use std::ptr;
use std::slice;
pub use uuid::Uuid;

use self::EntryContents::*;

//TODO FIX
pub trait Store<V: ?Sized> {
    //type Entry: IsEntry<V>;

    fn insert(&mut self, key: OrderIndex, val: EntryContents<V>) -> InsertResult; //TODO nocopy
    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>>;

    fn multi_append(&mut self, chains: &[OrderIndex], data: &V, deps: &[OrderIndex]) -> InsertResult; //TODO -> MultiAppebdResult
}

pub trait IsEntry<V> {
    fn contents<'s>(&'s self) -> EntryContents<'s, V>;
}

pub type OrderIndex = (order, entry);

pub type InsertResult = Result<OrderIndex, InsertErr>;
pub type GetResult<T> = Result<T, GetErr>;

/*#[repr(u8)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, RustcDecodable, RustcEncodable)]
pub enum EntryKind {
    Data = 1,
    Multiput = 2,
}*/

pub trait Storeable {
    fn size(&self) -> usize;
    unsafe fn ref_to_bytes(&self) -> &u8;
    unsafe fn bytes_to_ref(&u8, usize) -> &Self;
    unsafe fn bytes_to_mut(&mut u8, usize) -> &mut Self;
    unsafe fn clone_box(&self) -> Box<Self>;
}

impl<V> Storeable for V { //TODO should V be Copy/Clone?
    fn size(&self) -> usize {
        mem::size_of::<Self>()
    }

    unsafe fn ref_to_bytes(&self) -> &u8 {
        mem::transmute(self)
    }


    unsafe fn bytes_to_ref(val: &u8, size: usize) -> &Self {
        assert_eq!(size, mem::size_of::<Self>());
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
}

#[allow(non_snake_case)]
pub mod EntryKind {
    #![allow(non_upper_case_globals)]
    bitflags! {
        #[derive(RustcDecodable, RustcEncodable)]
        flags Kind: u8 {
            const Invalid = 0,
            const Data = 1,
            const Multiput = 2,
            const Layout = Data.bits | Multiput.bits,
            const Read = Data.bits | Multiput.bits,
            const ReadSuccess = 0x80,

            const ReadData = Data.bits | ReadSuccess.bits,
            const ReadMulti = Multiput.bits | ReadSuccess.bits,
            //const GotValue = Read.bits | Success.bits,
            const NoValue = Read.bits,
        }
    }
}

pub const MAX_DATA_LEN: usize = 4096 - 8 - (4 + 8 + 16); //TODO

#[repr(C)] //TODO
pub struct OLD<V, D: ?Sized = [u8; MAX_DATA_LEN]> {
    _pd: PhantomData<V>,
    pub kind: EntryKind::Kind,
    pub _padding: [u8; 1],
    pub cols: u16, //padding for non-multiputs
    pub data_bytes: u16,
    pub dependency_bytes: u16,
    pub data: D
    // layout Optional uuid, [u8; data_bytes] [OrderIndex; dependency_bytes/size<OrderIndex>]
}

pub const MAX_DATA_LEN2: usize = 4096 - 24; //TODO

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

/*
#[repr(C)] //TODO
pub struct EntryU<V, D: ?Sized = [u8; MAX_DATA_LEN]> {
    _pd: PhantomData<V>,
    pub id: Uuid,
    pub kind: EntryKind::Kind,
    pub _padding: [u8; 1],
    pub data_bytes: u16,
    pub dependency_bytes: u16,
    union {
        data {
            pub _padding: [u8; ?8],
            pub loc: u64,
            pub data: D
        }
        multi {
            pub cols: u16,
            pub data: D
        }
    }
}
*/

#[derive(Debug, PartialEq, Eq)]
pub enum EntryContents<'e, V:'e + ?Sized> {
    Data(&'e V, &'e [OrderIndex]),
    // Data:
    // | Kind: 8 | data_bytes: u16 | dependency_bytes: u16 | data: data_bytes | dependencies: dependency_bytes
    Multiput{data: &'e V, uuid: &'e Uuid, columns: &'e [OrderIndex], deps: &'e [OrderIndex]}, //TODO id? committed?
    // Multiput
    // | 8 | cols: u16 (from padding) | 16 | 16 | uuid | start_entries: [order; cols] | data | deps
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
}

pub struct MultiputContentsMut<'e, V: 'e + ?Sized> {
    pub data: &'e mut V,
    pub uuid: &'e mut Uuid,
    pub columns: &'e mut [OrderIndex],
    pub deps: &'e mut [OrderIndex],
}

impl<V: Storeable + ?Sized> Entry<V, DataFlex> {
    fn data_contents<'s>(&'s self, data_bytes: u16, _: u16) -> EntryContents<'s, V> { //TODO to DataContents<..>?
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

            let data_offset = (self.flex.cols as usize * size_of::<OrderIndex>()) as isize;
            let data_ptr = contents_ptr.offset(data_offset);
            let dep_ptr:*mut OrderIndex = data_ptr.offset(self.data_bytes as isize) as *mut _;
            //let data = Storeable::bytes_to_mut(data_ptr.as_mut().unwrap(), self.data_bytes as usize);
            let data = Storeable::bytes_to_mut(mem::transmute(data_ptr), self.data_bytes as usize);

            let num_deps = (self.dependency_bytes as usize)
                .checked_div(size_of::<OrderIndex>()).unwrap();
            let deps = slice::from_raw_parts_mut(dep_ptr, num_deps);
            MultiputContentsMut{data: data, uuid: uuid, columns: cols, deps: deps}
        }
    }

    fn mlocs(&self) -> &[OrderIndex] {
        unsafe {
            let contents_ptr: *const u8 = &self.flex.data as *const _ as *const u8;
            let cols_ptr = contents_ptr;
            let cols_ptr = cols_ptr as *const _;
            let num_cols = self.flex.cols;
            slice::from_raw_parts(cols_ptr, num_cols as usize)
        }
    }

    fn mlocs_mut(&mut self) -> &mut [OrderIndex] {
        unsafe {
            let contents_ptr: *mut u8 = &mut self.flex.data as *mut _ as *mut u8;
            let cols_ptr = contents_ptr;
            let cols_ptr = cols_ptr as *mut _;
            let num_cols = self.flex.cols;
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

            let data_offset = (self.flex.cols as usize * size_of::<OrderIndex>()) as isize;
            let (data, deps) = self.data_and_deps(contents_ptr, data_bytes, data_offset);
            Multiput{data: data, uuid: uuid, columns: cols, deps: deps}
        }
    }
}

impl<V: Storeable + ?Sized, F> Entry<V, F> {

    pub fn contents<'s>(&'s self) -> EntryContents<'s, V> {
        unsafe {
            let data_bytes = self.data_bytes;
            let dependency_bytes = self.dependency_bytes;
            let uuid = &self.id;
            match self.kind & EntryKind::Layout {
                EntryKind::Data =>
                        mem::transmute::<_, &Entry<V, DataFlex>>(self)
                            .data_contents(data_bytes, dependency_bytes),
                EntryKind::Multiput => mem::transmute::<_, &Entry<V, MultiFlex>>(self)
                    .multi_contents(data_bytes, dependency_bytes, uuid),
                _ => unreachable!(),
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
                .checked_div(size_of::<OrderIndex>()).unwrap();
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

    fn dependencies<'s>(&'s self) -> &'s [OrderIndex] {
        match self.contents() {
            Data(_, ref deps) => &deps,
            Multiput{ref deps, ..} => deps,
        }
    }

    pub unsafe fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), size_of::<Self>());
        let mut entr = mem::uninitialized::<Self>();
        let ptr: *mut _ = &mut entr;
        let ptr: *mut u8 = ptr as *mut _;
        ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, size_of::<Self>());
        entr
    }

    pub fn wrap_bytes(bytes: &[u8]) -> &OLD<[u8; MAX_DATA_LEN]> {
        assert_eq!(bytes.len(), size_of::<Self>());
        unsafe {
            mem::transmute(&bytes[0])
        }
    }
}

impl<V: ?Sized + Storeable, F> Entry<V, F> {
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

    pub fn bytes(&self) -> &[u8] {
        unsafe {
            let ptr: *const _ = self;
            let ptr: *const u8 = ptr as *const _;
            slice::from_raw_parts(ptr, size_of::<Self>())
        }
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            let ptr: *mut _ = self;
            let ptr: *mut u8 = ptr as *mut _;
            slice::from_raw_parts_mut(ptr, size_of::<Self>())
        }
    }

    pub fn lock_num(&self) -> u64 {
        match self.kind & EntryKind::Layout {
            EntryKind::Multiput => unsafe {
                self.as_multi_entry().flex.lock
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
        }
    }

    pub fn header_size(&self) -> usize {
        match self.kind & EntryKind::Layout {
            EntryKind::Data | EntryKind::Read => mem::size_of::<Entry<(), DataFlex<()>>>(),
            EntryKind::Multiput => mem::size_of::<Entry<(), MultiFlex<()>>>(),
            _ => panic!("invalid layout {:?}", self.kind & EntryKind::Layout),
        }
    }

    pub fn payload_size(&self) -> usize {
        let mut size = self.data_bytes as usize + self.dependency_bytes as usize;
        if self.is_multi() {
            let header = unsafe { mem::transmute::<_, &Entry<V, MultiFlex<()>>>(self) };
            size += header.flex.cols as usize * mem::size_of::<OrderIndex>();
        }
        assert!(size + mem::size_of::<Entry<(), ()>>() <= 8192);
        size
    }

    pub fn entry_size(&self) -> usize {
        let size = match self.kind & EntryKind::Layout {
            EntryKind::Data => self.data_entry_size(),
            EntryKind::Multiput => self.multi_entry_size(),
            EntryKind::Read => {
                //trace!("read size {}", mem::size_of::<Entry<(), ()>>());
                mem::size_of::<Entry<(), DataFlex<()>>>()
            }
            _ => panic!("invalid layout {:?}", self.kind & EntryKind::Layout),
        };
        assert!(size >= base_header_size());
        size
    }

    fn data_entry_size(&self) -> usize {
        assert!(self.kind & EntryKind::Layout == EntryKind::Data);
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
            let header = mem::transmute::<_, &Entry<V, MultiFlex>>(self);
            let size = mem::size_of::<Entry<(), MultiFlex<()>>>()
                + header.data_bytes as usize
                + header.dependency_bytes as usize
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
        }
    }
}

impl<'e, V: Storeable + ?Sized> EntryContents<'e, V> {
    pub fn dependencies<'s>(&'s self) -> &'s [OrderIndex] {
        match self {
            &Data(_, ref deps) => &deps,
            &Multiput{ref deps, ..} => deps,
        }
    }
}

impl <'e, V: ?Sized> EntryContents<'e, V> {
    fn kind(&self) -> EntryKind::Kind {
        match self {
            &Data(..) => EntryKind::Data,
            &Multiput{..} => EntryKind::Multiput,
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

    unsafe fn fill_entry(&self, e: &mut Entry<V>) {
        use std::u16;
        e.kind = self.kind();
        let (data_ptr, data, deps) = match self {
            &Data(data, deps) => {
                assert!(deps.len() < u16::MAX as usize);
                let e = transmute_ref_mut::<_, Entry<V, DataFlex>>(e);
                let data_ptr: *mut u8 = &mut (&mut e.flex.data)[0];
                (data_ptr, data, deps)
            }
            &Multiput{data, uuid, columns, deps} => {
                assert!(deps.len() < u16::MAX as usize);
                assert!(columns.len() < u16::MAX as usize);
                assert!(columns.len() > 0);
                let e = transmute_ref_mut::<_, Entry<V, MultiFlex>>(e);
                let cols_ptr: *mut OrderIndex = (&mut e.flex.data[0]) as *mut _ as *mut _;
                e.flex.cols = columns.len() as u16;
                assert!(e.flex.cols > 0);
                e.id = uuid.clone();

                ptr::copy(columns.as_ptr() as *mut _, cols_ptr, columns.len());

                let data_ptr = cols_ptr.offset(columns.len() as isize) as *mut u8;
                e.flex.lock = 0;
                (data_ptr, data, deps)
            }
        };
        assert!(Storeable::size(data) < u16::MAX as usize);
        e.data_bytes = Storeable::size(data) as u16;
        e.dependency_bytes = (deps.len() * size_of::<OrderIndex>()) as u16;
        let dep_ptr = data_ptr.offset(e.data_bytes as isize);

        //ptr::write(data_ptr as *mut _, data.clone()); //TODO why did this fail?
        ptr::copy(Storeable::ref_to_bytes(data), data_ptr as *mut _, e.data_bytes as usize);
        ptr::copy(deps.as_ptr(), dep_ptr as *mut _, deps.len());
        if e.kind == EntryKind::Multiput {
            let e = transmute_ref_mut::<_, Entry<V, MultiFlex>>(e);
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
                + (columns.len()  * size_of::<OrderIndex>()),
        };
        assert!(size <= 4096);
        let mut buffer = Vec::with_capacity(size);
        unsafe {
            buffer.set_len(size);
            self.fill_entry(transmute_ref_mut(&mut buffer[0]));
        }
        buffer
    }
}

unsafe fn transmute_ref_mut<'s, T, U>(t: &'s mut T) -> &'s mut U {
    mem::transmute(t)
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum InsertErr {
    AlreadyWritten
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum GetErr {
    NoValue
}

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

pub trait Horizon {
    fn get_horizon(&mut self, order) -> entry;
    fn update_horizon(&mut self, order, entry) -> entry;
}

pub type LogResult = Result<(), ()>;
pub type ApplyResult = Result<(), ()>;

pub struct FuzzyLog<V: ?Sized, S, H>
where V: Storeable, S: Store<V>, H: Horizon {
    pub store: S,
    pub horizon: H,
    local_horizon: HashMap<order, entry>,
    upcalls: HashMap<order, Box<for<'r> Fn(&'r V) -> bool>>,
}

//TODO should impl some trait FuzzyLog instead of providing methods directly to allow for better sharing?
//TODO allow dynamic register of new upcalls?
impl<V: ?Sized, S, H> FuzzyLog<V, S, H>
where V: Storeable, S: Store<V>, H: Horizon{
    pub fn new(store: S, horizon: H, upcalls: HashMap<order, Box<Fn(&V) -> bool>>) -> Self {
        FuzzyLog {
            store: store,
            horizon: horizon,
            local_horizon: HashMap::new(),
            upcalls: upcalls,
        }
    }

    pub fn append(&mut self, column: order, data: &V, deps: &[OrderIndex]) -> OrderIndex {
        self.append_entry(column, Data(data, &*deps))
    }

    pub fn try_append(&mut self, column: order, data: &V, deps: &[OrderIndex]) -> Option<OrderIndex> {
        let next_entry = self.horizon.get_horizon(column);
        let insert_loc = (column, next_entry);
        self.store.insert(insert_loc, Data(data, &*deps)).ok().map(|loc| {
            self.horizon.update_horizon(column, loc.1);
            loc
        })
    }

    fn append_entry(&mut self, column: order, ent: EntryContents<V>) -> OrderIndex {
        let mut inserted = false;
        let mut insert_loc = (column, 0.into());
        let mut next_entry = self.horizon.get_horizon(column);
        while !inserted {
            next_entry = next_entry + 1; //TODO jump ahead
            insert_loc = (column, next_entry);
            inserted = match self.store.insert(insert_loc, ent.clone()) {
                Err(..) => false,
                Ok(loc) => {
                    insert_loc = loc;
                    true
                }
            }
        }
        self.horizon.update_horizon(column, insert_loc.1);
        insert_loc
    }

    pub fn multiappend(&mut self, columns: &[order], data: &V, deps: &[OrderIndex]) {
        let columns: Vec<OrderIndex> = columns.into_iter().map(|i| (*i, 0.into())).collect();
        self.store.multi_append(&columns[..], data, &deps[..]); //TODO error handling
        for &(column, _) in &*columns {
            let next_entry = self.horizon.get_horizon(column) + 1; //TODO
            self.horizon.update_horizon(column, next_entry); //TODO
        }
    }

    pub fn get_next_unseen(&mut self, column: order) -> Option<OrderIndex> {
        let index = self.local_horizon.get(&column).cloned().unwrap_or(0.into()) + 1;
        trace!("next unseen: {:?}", (column, index));
        let ent = self.store.get((column, index)).clone();
        let ent = match ent { Err(GetErr::NoValue) => return None, Ok(e) => e };
        self.play_deps(ent.dependencies());
        match ent.contents() {
            Multiput{data, uuid, columns, deps} => {
                //TODO
                trace!("Multiput {:?}", deps);
                self.read_multiput(column, data, uuid, columns);
            }
            Data(data, deps) => {
                trace!("Data {:?}", deps);
                self.upcalls.get(&column).map(|f| f(data.clone())); //TODO clone
            }
        }
        self.local_horizon.insert(column, index);
        Some((column, index))
    }

    fn read_multiput(&mut self, first_seen_column: order, data: &V, put_id: &Uuid,
        columns: &[OrderIndex]) {

        for &(column, _) in columns { //TODO only relevent cols
            trace!("play multiput for col {:?}", column);
            self.play_until_multiput(column, put_id);
            self.upcalls.get(&column).map(|f| f(data));
        }

        //XXX TODO note multiserver validation happens at the store layer?
        self.upcalls.get(&first_seen_column).map(|f| f(data));
    }

    fn play_until_multiput(&mut self, column: order, put_id: &Uuid) {
        //TODO instead, just mark all interesting columns not in the
        //     transaction as stale, and only read the interesting
        //     columns of the transaction
        'search: loop {
            let index = self.local_horizon.get(&column).cloned().unwrap_or(0.into()) + 1;
            trace!("seatching for multiput {:?}\n\tat: {:?}", put_id, (column, index));
            let ent = self.store.get((column, index)).clone();
            let ent = match ent {
                Err(GetErr::NoValue) => panic!("invalid multiput."),
                Ok(e) => e
            };
            self.play_deps(ent.dependencies());
            match ent.contents() {
                Multiput{uuid, ..} if uuid == put_id => {
                    trace!("found multiput {:?} for {:?} at: {:?}", put_id, column, index);
                    self.local_horizon.insert(column, index);
                    break 'search
                }
                Multiput{data, uuid, columns, ..} => {
                    //TODO
                    trace!("Multiput");
                    self.read_multiput(column, data, uuid, columns);
                    self.local_horizon.insert(column, index);
                }
                Data(data, _) => {
                    trace!("Data");
                    self.upcalls.get(&column).map(|f| f(data)); //TODO clone
                    self.local_horizon.insert(column, index);
                }
            }
        }
	}

    fn play_deps(&mut self, deps: &[OrderIndex]) {
        for &dep in deps {
            self.play_until(dep)
        }
    }

    pub fn play_until(&mut self, dep: OrderIndex) {
        //TODO end if run out?
        while self.local_horizon.get(&dep.0).cloned().unwrap_or(0.into()) < dep.1 {
            self.get_next_unseen(dep.0);
        }
    }

    pub fn play_foward(&mut self, column: order) -> Option<OrderIndex> {
        trace!("play_foward");
        //let index = self.horizon.get_horizon(column);
        //trace!("play until {:?}", index);
        //if index == 0.into() { return None }//TODO
        //self.play_until((column, index));
        //Some((column, index))
        let mut res = None;
        while let Some(index) = self.get_next_unseen(column) {
            res = Some(index);
        }
        res
    }

    pub fn local_horizon(&self) -> &HashMap<order, entry> {
        &self.local_horizon
    }
}

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
    fn test_entry_header() {
        use std::mem::size_of;
        assert_eq!(size_of::<OLD<(), ()>>(), 8);
    }

    #[test]
    fn test_entry_size() {
        use std::mem::size_of;
        assert_eq!(size_of::<OLD<()>>(), 4096 - (4 + 8 + 16));
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
