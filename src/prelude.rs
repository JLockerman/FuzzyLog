
use std::collections::HashMap;
use std::convert::Into;
use std::fmt;
use std::marker::PhantomData;
use std::mem::{self, size_of};
use std::ptr;
use std::slice;
use std::time::Duration;
use std::thread;
use uuid::Uuid;

use self::EntryContents::*;

//TODO FIX
pub trait Store<V> {
    //type Entry: IsEntry<V>;

    fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult; //TODO nocopy
    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>>;

    fn multi_append(&mut self, chains: &[OrderIndex], data: V, deps: &[OrderIndex]) -> InsertResult; //TODO -> MultiAppebdResult
}

pub trait IsEntry<V> {
    fn contents<'s>(&'s self) -> EntryContents<'s, V>;
}

pub type OrderIndex = (order, entry);

pub type InsertResult = Result<(), InsertErr>;
pub type GetResult<T> = Result<T, GetErr>;

/*#[repr(u8)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, RustcDecodable, RustcEncodable)]
pub enum EntryKind {
    Data = 1,
    Multiput = 2,
}*/

#[allow(non_snake_case)]
pub mod EntryKind {
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

pub const MAX_DATA_LEN2: usize = 4096 - 22; //TODO

#[repr(C)] //TODO
pub struct Entry<V, F: ?Sized = [u8; MAX_DATA_LEN2]> {
    _pd: PhantomData<V>,
    pub id: Uuid,
    pub kind: EntryKind::Kind,
    pub _padding: [u8; 1],
    pub data_bytes: u16,
    pub dependency_bytes: u16,
    pub flex: F,
}

pub const MAX_DATA_DATA_LEN: usize = MAX_DATA_LEN2 - 12; //TODO

pub struct DataFlex<D: ?Sized = [u8; MAX_DATA_DATA_LEN]> {
    pub loc: OrderIndex,
    pub data: D,
}

pub const MAX_MULTI_DATA_LEN: usize = MAX_DATA_LEN2 - 2; //TODO

pub struct MultiFlex<D: ?Sized = [u8; MAX_MULTI_DATA_LEN]> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

pub struct MultiputContentsMut<'e, V: 'e> {
    pub data: &'e mut V,
    pub uuid: &'e mut Uuid,
    pub columns: &'e mut [OrderIndex],
    pub deps: &'e mut [OrderIndex],
}

impl<V, D: ?Sized> OLD<V, D> {
    fn data_start_offset(&self) -> isize {
        match self.kind & EntryKind::Layout {
            EntryKind::Data => 0 as isize,
            EntryKind::Multiput => (size_of::<Uuid>() + self.cols as usize * size_of::<order>())
            	as isize,
            o => unreachable!("{:?}", o),
        }
    }

    pub fn kind(&self) -> EntryKind::Kind {
        self.kind
    }
}

impl<V> Entry<V, DataFlex> {
    fn data_contents<'s>(&'s self, data_bytes: u16, dependency_bytes: u16) -> EntryContents<'s, V> { //TODO to DataContents<..>?
        let contents_ptr: *const u8 = &self.flex.data as *const _ as *const u8;
        let (data, deps) = self.data_and_deps(contents_ptr, 0);
        Data(data, deps)
    }
}

impl<V> Entry<V, MultiFlex> {
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
            let data = (data_ptr as *mut _).as_mut().unwrap();

            let num_deps = (self.dependency_bytes as usize)
                .checked_div(size_of::<OrderIndex>()).unwrap();
            let deps = slice::from_raw_parts_mut(dep_ptr, num_deps);
            MultiputContentsMut{data: data, uuid: uuid, columns: cols, deps: deps}
        }
    }

    fn multi_contents<'s>(&'s self, data_bytes: u16, dependency_bytes: u16, uuid: &'s Uuid)
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
            let (data, deps) = self.data_and_deps(contents_ptr, data_offset);
            Multiput{data: data, uuid: uuid, columns: cols, deps: deps}
        }
    }
}

impl<V, F> Entry<V, F> {

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

    fn data_and_deps<'s, D>(&'s self, contents_ptr: *const u8, data_offset: isize)
    -> (&'s D, &'s [OrderIndex]) {
        unsafe {
            let data_ptr = contents_ptr.offset(data_offset);
            let dep_ptr:*const OrderIndex = data_ptr.offset(self.data_bytes as isize) as *const _;
            let data = (data_ptr as *const _).as_ref().unwrap();

            let num_deps = (self.dependency_bytes as usize)
                .checked_div(size_of::<OrderIndex>()).unwrap();
            let deps = slice::from_raw_parts(dep_ptr, num_deps);
            (data, deps)
        }
    }

    pub fn contents_mut<'s>(&'s mut self) -> EntryContentsMut<'s, V> {
        /*unsafe {
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
        }*/
        panic!()
    }

    pub unsafe fn as_data_entry(&self) -> &Entry<V, DataFlex> {
        mem::transmute(self)
    }

    pub unsafe fn as_data_entry_mut(&mut self) -> &mut Entry<V, DataFlex> {
        transmute_ref_mut(self)
    }

    pub unsafe fn as_multi_entry(&self) -> &Entry<V, MultiFlex> {
        mem::transmute(self)
    }

    pub unsafe fn as_multi_entry_mut(&mut self) -> &mut Entry<V, MultiFlex> {
        transmute_ref_mut(self)
    }

    fn dependencies<'s>(&'s self) -> &'s [OrderIndex] {
        match self.contents() {
            Data(_, ref deps) => &deps,
            Multiput{ref deps, ..} => deps,
        }
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

impl<V: PartialEq> PartialEq for Entry<V> {
    fn eq(&self, other: &Self) -> bool {
        //TODO
        self.contents() == other.contents()
    }
}

impl<V: Eq> Eq for Entry<V> {}

impl<V: fmt::Debug> fmt::Debug for Entry<V> {
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

impl<V> Clone for Entry<V>
where V: Clone {
    fn clone(&self) -> Self {
        //TODO
        unsafe {
            let mut entr: Entry<V> = mem::uninitialized();
            ptr::copy(self as *const _, &mut entr as *mut _, 1);
            entr
        }
        //self.contents().clone_entry()
    }
}

//TODO impl<V> Entry<[V]>
impl<V, D> OLD<V, D> {

    pub fn contents<'s>(&'s self) -> EntryContents<'s, V> {
        /*unsafe {
            //let contents_ptr: *const u8 = &self.data as *const _;
            //TODO this might be invalid...
            let contents_ptr: *const u8 = &self.data as *const _ as *const u8;
            let data_ptr = contents_ptr.offset(self.data_start_offset());
            let dep_ptr:*const OrderIndex = data_ptr.offset(self.data_bytes as isize)
                as *const _;
            //println!("datap {:?} depp {:?}", data_ptr, dep_ptr);
            let num_deps = (self.dependency_bytes as usize)
                .checked_div(size_of::<OrderIndex>()).unwrap();
            let deps = slice::from_raw_parts(dep_ptr, num_deps);
            match self.kind & EntryKind::Layout {
                EntryKind::Data => {
                    //TODO assert_eq!(self.data_bytes as usize, size_of::<V>());
                    let data = (data_ptr as *const _).as_ref().unwrap();
                    Data(data, deps)
                }
                EntryKind::Multiput => {
                	let id_ptr: *const Uuid = contents_ptr as *const Uuid;
                	let cols_ptr: *const order = contents_ptr.offset(size_of::<Uuid>() as isize)
                		as *const _;

                	let data = (data_ptr as *const _).as_ref().unwrap();
                	let uuid = id_ptr.as_ref().unwrap();
                	let cols = slice::from_raw_parts(cols_ptr, self.cols as usize);
                	Multiput{data: data, uuid: uuid, columns: cols, deps: deps}
                }
                o => unreachable!("{:?}", o),
            }
        }*/
        panic!()
    }

    pub fn contents_mut<'s>(&'s mut self) -> EntryContentsMut<'s, V> {
        /*unsafe {
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

        }*/
        panic!()
    }

    fn dependencies<'s>(&'s self) -> &'s [OrderIndex] {
        match self.contents() {
            Data(_, ref deps) => &deps,
            Multiput{ref deps, ..} => deps,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        unsafe {
            let ptr: *const _ = self;
            let ptr: *const u8 = ptr as *const _;
            slice::from_raw_parts(ptr, size_of::<Self>())
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

impl<V: PartialEq> PartialEq for OLD<V> {
    fn eq(&self, other: &Self) -> bool {
        //TODO
        self.contents() == other.contents()
    }
}

impl<V: Eq> Eq for OLD<V> {}

impl<V: fmt::Debug> fmt::Debug for OLD<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{:?}", self.contents())
    }
}

impl<V, D> Clone for OLD<V, D>
where V: Clone {
    fn clone(&self) -> Self {
        //TODO
        self.contents().clone_entry_old()
    }
}

impl<'e, V> EntryContents<'e, V> {
    pub fn dependencies<'s>(&'s self) -> &'s [OrderIndex] {
        match self {
            &Data(_, ref deps) => &deps,
            &Multiput{ref deps, ..} => deps,
        }
    }
}

impl <'e, V: ?Sized> EntryContents<'e, V> {
    fn data_start_offset(&self) -> isize { //TODO call EntryKind.data start offset
        match self {
            &Data(..) => 0 as isize,
            &Multiput{columns, ..} =>  (size_of::<Uuid>() + columns.len() * size_of::<order>())
            	as isize,
        }
    }

    fn kind(&self) -> EntryKind::Kind {
        match self {
            &Data(..) => EntryKind::Data,
            &Multiput{..} => EntryKind::Multiput,
        }
    }
}

impl<'e, V: Clone> EntryContents<'e, V> {

    //TODO temporary
    pub fn clone_entry_old<D = [u8; MAX_DATA_LEN]>(&self) -> OLD<V, D> {
        use std::u16;
        assert!(size_of::<V>() < u16::MAX as usize);
        unsafe {
            let mut entr = mem::uninitialized::<OLD<V, D>>();
            let contents_ptr: *mut u8 = &mut entr.data as *mut _ as *mut u8;
            let data_ptr = contents_ptr.offset(self.data_start_offset());
            entr.kind = self.kind();
            match self {
                &Data(data, deps) => {
                    // Data:
                    // | data: data_bytes | dependencies: dependency_bytes | Kind: 8 | data_bytes: u16 | dependency_bytes: u16
                    assert!(deps.len() < u16::MAX as usize);
                    let data_ptr = data_ptr as *mut V;
                    ptr::write(data_ptr, data.clone());
                    entr.data_bytes = size_of::<V>() as u16;
                    let dep_ptr = (data_ptr as *mut u8).offset(entr.data_bytes as isize);
                    let dep_ptr = dep_ptr as *mut _;
                    ptr::copy(deps.as_ptr(), dep_ptr, deps.len());
                    entr.dependency_bytes = (deps.len() * size_of::<OrderIndex>())
                        as u16;
                    //println!("root {:?}\n datap {:?} depp {:?}\n datab {:?} depb {:?}", (&mut entr) as *mut _, data_ptr, dep_ptr, entr.data_bytes, entr.dependency_bytes);
                    //println!("depp[0] {:?}", *dep_ptr);
                }
                &Multiput{data, uuid, columns, deps} => {
                	// | 8 | cols: u16 (from padding) | 16 | 16 | uuid | start_entries: [order; cols] | data | deps
                	assert!(deps.len() < u16::MAX as usize);
                	assert!(columns.len() < u16::MAX as usize);

                	entr.data_bytes = size_of::<V>() as u16;
                	entr.cols = columns.len() as u16;
                	entr.dependency_bytes = (deps.len() * size_of::<OrderIndex>())
                        as u16;

                	let dep_ptr: *mut u8 = data_ptr.offset(entr.data_bytes as isize);

                	let dep_ptr = dep_ptr as *mut OrderIndex;
                	let uuid_ptr = contents_ptr as *mut Uuid;
                	let cols_ptr = contents_ptr.offset(size_of::<Uuid>() as isize) as *mut _;
                	let data_ptr = data_ptr as *mut V;

					ptr::write(uuid_ptr, uuid.clone());
                	ptr::write(data_ptr, data.clone());
                	ptr::copy(deps.as_ptr(), dep_ptr, deps.len());
                	ptr::copy(columns.as_ptr(), cols_ptr, columns.len());
                }
            }
            //TODO let dep_ptr = data_ptr.offset(self.data_bytes as isize) as *const _;
            //TODO let num_deps = (self.dependency_bytes as usize).checked_div(size_of::<OrderIndex>()).unwrap();
            //TODO let deps = slice::from_raw_parts(dep_ptr, num_deps);
            entr
        }
    }

    pub fn clone_entry(&self) -> Entry<V> {
        use std::u16;
        assert!(size_of::<V>() < u16::MAX as usize);
        unsafe {
            let mut entr = mem::uninitialized::<Entry<V>>();
            {
                let e = &mut entr;
                e.data_bytes = size_of::<V>() as u16;
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
                        (data_ptr, data, deps)
                    }
                };
                e.dependency_bytes = (deps.len() * size_of::<OrderIndex>()) as u16;
                let dep_ptr = data_ptr.offset(e.data_bytes as isize);

                ptr::write(data_ptr as *mut _, data.clone());
                ptr::copy(deps.as_ptr(), dep_ptr as *mut _, deps.len());
                if e.kind == EntryKind::Multiput {
                    let e = transmute_ref_mut::<_, Entry<V, MultiFlex>>(e);
                    assert!(e.flex.cols > 0);
                }
            }
            entr
        }
    }

    /*
    fn data_and_deps<'s, D>(&'s self, contents_ptr: *const u8, data_offset: isize)
    -> (&'s D, &'s [OrderIndex]) {
        unsafe {
            let data_ptr = contents_ptr.offset(data_offset);
            let dep_ptr:*const OrderIndex = data_ptr.offset(self.data_bytes as isize) as *const _;
            let data = (data_ptr as *const _).as_ref().unwrap();

            let num_deps = (self.dependency_bytes as usize)
                .checked_div(size_of::<OrderIndex>()).unwrap();
            let deps = slice::from_raw_parts(dep_ptr, num_deps);
            (data, deps)
        }
    }
    impl<V> Entry<V, DataFlex> {
        fn data_contents<'s>(&'s self, data_bytes: u16, dependency_bytes: u16) -> EntryContents<'s, V> { //TODO to DataContents<..>?
            let contents_ptr: *const u8 = &self.flex.data as *const _ as *const u8;
            let (data, deps) = self.data_and_deps(contents_ptr, 0);
            Data(data, deps)
        }
    }
    fn multi_contents<'s>(&'s self, data_bytes: u16, dependency_bytes: u16, uuid: &'s Uuid)
    -> EntryContents<'s, V> { //TODO to DataContents<..>?
        unsafe {
            let contents_ptr: *const u8 = &self.flex.data as *const _ as *const u8;
            let cols_ptr = contents_ptr;


            let cols_ptr = cols_ptr as *const _;
            let num_cols = self.flex.cols;
            let cols = slice::from_raw_parts(cols_ptr, num_cols as usize);

            let data_offest = (self.flex.cols as usize * size_of::<OrderIndex>()) as isize;
            let (data, deps) = self.data_and_deps(contents_ptr, data_offest);
            Multiput{data: data, uuid: uuid, columns: cols, deps: deps}
        }
    }*/
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
    #[derive(Debug, Hash, PartialOrd, Ord, PartialEq, Eq, Clone, Copy, Default, RustcDecodable, RustcEncodable, NewtypeFrom, NewtypeAdd(u32), NewtypeSub(u32), NewtypeMul(u32), NewtypeRem(u32))]
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

pub struct FuzzyLog<V, S, H>
where V: Copy, S: Store<V>, H: Horizon {
    pub store: S,
    pub horizon: H,
    local_horizon: HashMap<order, entry>,
    upcalls: HashMap<order, Box<Fn(V) -> bool>>,
}

//TODO should impl some trait FuzzyLog instead of providing methods directly to allow for better sharing?
//TODO allow dynamic register of new upcalls?
impl<V, S, H> FuzzyLog<V, S, H>
where V: Copy, S: Store<V>, H: Horizon{
    pub fn new(store: S, horizon: H, upcalls: HashMap<order, Box<Fn(V) -> bool>>) -> Self {
        FuzzyLog {
            store: store,
            horizon: horizon,
            local_horizon: HashMap::new(),
            upcalls: upcalls,
        }
    }

    pub fn append(&mut self, column: order, data: V, deps: Vec<OrderIndex>) -> OrderIndex {
        self.append_entry(column, Data(&data, &*deps))
    }

    pub fn try_append(&mut self, column: order, data: V, deps: Vec<OrderIndex>) -> Option<OrderIndex> {
        let next_entry = self.horizon.get_horizon(column);
        let insert_loc = (column, next_entry);
        self.store.insert(insert_loc, Data(&data, &*deps).clone_entry()).ok().map(|_| {
            self.horizon.update_horizon(column, next_entry);
            insert_loc
        })
    }

    fn append_entry(&mut self, column: order, ent: EntryContents<V>) -> OrderIndex {
        let mut inserted = false;
        let mut insert_loc = (column, 0.into());
        let mut next_entry = self.horizon.get_horizon(column);
        while !inserted {
            next_entry = next_entry + 1; //TODO jump ahead
            insert_loc = (column, next_entry);
            inserted = self.store.insert(insert_loc, ent.clone_entry()).is_ok();
        }
        self.horizon.update_horizon(column, next_entry);
        insert_loc
    }

    pub fn multiappend(&mut self, columns: Vec<order>, data: V, deps: Vec<OrderIndex>) {
        let columns: Vec<OrderIndex> = columns.into_iter().map(|i| (i, 0.into())).collect();
        self.store.multi_append(&columns[..], data, &deps[..]); //TODO error handling
        for &(column, _) in &*columns {
            let next_entry =  self.horizon.get_horizon(column) + 1; //TODO
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
                trace!("Multiput");
                self.read_multiput(column, data, uuid, columns);
            }
            Data(data, _) => {
                trace!("Data");
                self.upcalls.get(&column).map(|f| f(data.clone())); //TODO clone
            }
        }
        self.local_horizon.insert(column, index);
        Some((column, index))
    }

    fn read_multiput(&mut self, first_seen_column: order, data: &V, put_id: &Uuid,
        columns: &[OrderIndex]) {

        //XXX note multiserver validation happens at the store layer
        self.upcalls.get(&first_seen_column).map(|f| f(data.clone())); //TODO clone

        for &(column, _) in columns { //TODO only relevent cols
            trace!("play multiput for col {:?}", column);
            self.play_until_multiput(column, put_id);
            self.upcalls.get(&column).map(|f| f(data.clone())); //TODO clone
        }
    }

    fn play_until_multiput(&mut self, column: order, put_id: &Uuid) {
        //TODO instead, just mark all interesting columns not in the
        //     transaction as stale, and only read the interesting
        //     columns of the transaction
        let mut index = self.local_horizon.get(&column).cloned().unwrap_or(0.into()) + 1;
        'search: loop {
            trace!("seatching for multiput {:?}\n\tat: {:?}", put_id, (column, index));
            let ent = self.store.get((column, index)).clone();
            let ent = match ent {
                Err(GetErr::NoValue) => panic!("invalid multiput."),
                Ok(e) => e
            };
            self.play_deps(ent.dependencies());
            match ent.contents() {
                Multiput{uuid, ..} if uuid == put_id => break 'search,
                Multiput{data, uuid, columns, deps} => {
                    //TODO
                    trace!("Multiput");
                    self.read_multiput(column, data, uuid, columns);
                }
                Data(data, _) => {
                    trace!("Data");
                    self.upcalls.get(&column).map(|f| f(data.clone())); //TODO clone
                }
            }
        }
        trace!("found multiput {:?} for {:?} at: {:?}", put_id, column, index);
        self.local_horizon.insert(column, index);
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
        let index = self.horizon.get_horizon(column);
        if index == 0.into() { return None }//TODO
        self.play_until((column, index));
        Some((column, index))
    }

    pub fn local_horizon(&self) -> &HashMap<order, entry> {
        &self.local_horizon
    }
}

mod test {

    use super::*;
    use super::EntryContents::*;
    use uuid::Uuid;


    #[test]
    fn test_entry2_header() {
        use std::mem::size_of;
        assert_eq!(size_of::<Entry<(), ()>>(), 22);
    }

    #[test]
    fn test_entry2_data_header() {
        use std::mem::size_of;
        assert_eq!(size_of::<Entry<(), DataFlex<()>>>(),
            size_of::<Entry<(), ()>>() + size_of::<DataFlex<()>>() + 2);
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
