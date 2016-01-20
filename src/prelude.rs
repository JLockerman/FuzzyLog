
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

pub trait Store<V> {
    fn insert(&mut self, key: OrderIndex, val: Entry<V>) -> InsertResult; //TODO nocopy
    fn get(&mut self, key: OrderIndex) -> GetResult<Entry<V>>;

    fn multi_append(&mut self, chains: &[order], data: V, deps: &[OrderIndex]) -> InsertResult; //TODO -> MultiAppebdResult
}

pub type OrderIndex = (order, entry);

pub type InsertResult = Result<(), InsertErr>;
pub type GetResult<T> = Result<T, GetErr>;

#[repr(u8)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, RustcDecodable, RustcEncodable)]
pub enum EntryKind {
    Data = 1,
    Multiput,
    TransactionCommit,
    TransactionStart,
    TransactionAbort,
}

pub const MAX_DATA_LEN: usize = 4096 - 8 - (4 + 8 + 16); //TODO

#[repr(C)] //TODO
pub struct Entry<V, D: ?Sized = [u8; MAX_DATA_LEN]> {
    _pd: PhantomData<V>,
    kind: EntryKind,
    _padding: [u8; 1],
    cols: u16, //padding for non-multiputs
    data_bytes: u16,
    dependency_bytes: u16,
    data: D
    // layout Optional uuid, [u8; data_bytes] [OrderIndex; dependency_bytes/size<OrderIndex>]
}

#[test]
fn test_entry_size() {
    use std::mem::size_of;
    assert_eq!(size_of::<Entry<()>>(), 4096 - (4 + 8 + 16));
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryContents<'e, V:'e + ?Sized> {
    Data(&'e V, &'e [OrderIndex]),
    // Data:
    // | Kind: 8 | data_bytes: u16 | dependency_bytes: u16 | data: data_bytes | dependencies: dependency_bytes
    TransactionCommit{uuid: &'e Uuid, start_entries: &'e [OrderIndex],
        deps: &'e [OrderIndex]}, //TODO do commits need dependencies?
    // Commit:
    // | 8 | u16 | u16 | uuid: size Uuid | start_entries: data_bytes / size OrderIndex | deps: dependency_bytes / size OrderIndex
    TransactionStart(&'e V,  order,  &'e Uuid), //TODO do Starts need dependencies?
    // Start:
    // | 8 | u16 | u16 = 0 | uuid: size Uuid | order: size order | data: data_bytes |
    TransactionAbort(&'e Uuid), //TODO do Aborts need dependencies?
    // Abort:
    // u16 = 0 | u16 = 0 | uuid: size Uuid | ... | 8 |
    Multiput{data: &'e V, uuid: &'e Uuid, columns: &'e [order], deps: &'e [OrderIndex]}, //TODO id? committed?
    // Multiput
    // | 8 | cols: u16 (from padding) | 16 | 16 | uuid | start_entries: [order; cols] | data | deps
}

impl<V, D: ?Sized> Entry<V, D> {
    fn data_start_offset(&self) -> isize {
        match self.kind {
            EntryKind::Data => 0 as isize,
            EntryKind::TransactionAbort => size_of::<Uuid>() as isize,
            EntryKind::TransactionCommit => size_of::<Uuid>() as isize,
            EntryKind::TransactionStart => (size_of::<Uuid>() + size_of::<order>())
                as isize,
            EntryKind::Multiput => (size_of::<Uuid>() + self.cols as usize * size_of::<order>())
            	as isize,
        }
    }

    pub fn kind(&self) -> EntryKind {
        self.kind
    }
}

//TODO impl<V> Entry<[V]>
impl<V, D> Entry<V, D> {

    pub fn contents<'s>(&'s self) -> EntryContents<'s, V> {
        unsafe {
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
            match self.kind {
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
                EntryKind::TransactionAbort => {
                    /*assert_eq!(self.data_bytes as usize, size_of::<Uuid>());
                    assert_eq!(self.dependency_bytes, 0);
                    let uuid = (contents_ptr as *const _).as_ref().unwrap();
                    TransactionAbort(uuid)*/
                    panic!()
                }
                EntryKind::TransactionCommit => {
                    /*let uuid = (contents_ptr as *const _).as_ref().unwrap();
                    let starts_ptr = contents_ptr.offset(size_of::<Uuid>() as isize) as *const _;
                    let starts_len = self.data_bytes as usize;
                    let num_starts = starts_len.checked_div(size_of::<OrderIndex>()).unwrap();
                    let start_entries = slice::from_raw_parts(starts_ptr, num_starts);
                    TransactionCommit{uuid: uuid, start_entries: start_entries, deps: deps}*/
                    panic!()
                }
                EntryKind::TransactionStart => {
                    /*assert!(self.data_bytes as usize <=
                        MAX_DATA_LEN as usize - (size_of::<order>() + size_of::<Uuid>()));
                    let order_ptr = contents_ptr.offset(size_of::<Uuid>() as isize);
                    let uuid = (contents_ptr as *const _).as_ref().unwrap();
                    let ord = *(order_ptr as *const _);
                    let data = (data_ptr as *const _).as_ref().unwrap();
                    TransactionStart(data, ord, uuid)*/
                    panic!()
                }
            }
        }
    }

    fn dependencies<'s>(&'s self) -> &'s [OrderIndex] {
        match self.contents() {
            Data(_, ref deps) => &deps,
            TransactionCommit{ref deps, ..} => &deps,
            TransactionAbort(..) => &[],
            TransactionStart(_, _, _) => &[],
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

    pub fn wrap_bytes(bytes: &[u8]) -> &Entry<[u8; MAX_DATA_LEN]> {
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
        write!(f, "{:?}", self.contents())
    }
}

impl<V, D> Clone for Entry<V, D>
where V: Clone {
    fn clone(&self) -> Self {
        //TODO
        self.contents().clone_entry()
    }
}

impl<'e, V> EntryContents<'e, V> {
    pub fn dependencies<'s>(&'s self) -> &'s [OrderIndex] {
        match self {
            &Data(_, ref deps) => &deps,
            &TransactionCommit{ref deps, ..} => &deps,
            &TransactionAbort(..) => &[],
            &TransactionStart(_, _, _) => &[],
            &Multiput{ref deps, ..} => deps,
        }
    }
}

impl <'e, V: ?Sized> EntryContents<'e, V> {
    fn data_start_offset(&self) -> isize { //TODO call EntryKind.data start offset
        match self {
            &Data(..) => 0 as isize,
            &TransactionAbort(..) => size_of::<Uuid>() as isize,
            &TransactionCommit{..} => size_of::<Uuid>() as isize,
            &TransactionStart(..) => (size_of::<Uuid>() + size_of::<order>())
                as isize,
            &Multiput{columns, ..} =>  (size_of::<Uuid>() + columns.len() * size_of::<order>())
            	as isize,
        }
    }

    fn kind(&self) -> EntryKind {
        match self {
            &Data(..) => EntryKind::Data,
            &TransactionAbort(..) => EntryKind::TransactionAbort,
            &TransactionCommit{..} => EntryKind::TransactionCommit,
            &TransactionStart(..) => EntryKind::TransactionStart,
            &Multiput{..} => EntryKind::Multiput,
        }
    }
}

impl<'e, V: Clone> EntryContents<'e, V> {

    //TODO temporary
    pub fn clone_entry<D = [u8; MAX_DATA_LEN]>(&self) -> Entry<V, D> {
        use std::u16;
        assert!(size_of::<V>() < u16::MAX as usize);
        unsafe {
            let mut entr = mem::uninitialized::<Entry<V, D>>();
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
                /*&TransactionCommit{uuid, start_entries, deps} => {
                    // Commit:
                    // | uuid: size Uuid | start_entries: data_bytes / size OrderIndex | deps: dependency_bytes / size OrderIndex | 8 | u16 | u16
                    //let uuid = (contents_ptr as *const _).as_ref().unwrap();
                    /*assert!(deps.len() < u16::MAX as usize);
                    let uuid_ptr = contents_ptr as *mut Uuid;
                    ptr::write(uuid_ptr, *uuid);
                    let starts_ptr = contents_ptr.offset(size_of::<Uuid>()
                        as isize) as *mut OrderIndex;
                    ptr::copy(start_entries.as_ptr(), starts_ptr, start_entries.len());
                    entr.data_bytes = (start_entries.len() * size_of::<OrderIndex>()) as u16; //TODO overflow
                    let dep_ptr = (data_ptr as *mut u8).offset(entr.data_bytes as isize);
                    let dep_ptr = dep_ptr as *mut _;
                    ptr::copy(deps.as_ptr(), dep_ptr, deps.len());
                    entr.dependency_bytes = (deps.len() * size_of::<OrderIndex>())
                        as u16;*/
                    panic!()
                }
                &TransactionStart(data, commit_col, uuid) => {
                    // Start:
                    // | uuid: size Uuid | order: size order | data: data_bytes | ... | 8 | u16 | u16 = 0
                    /*let uuid_ptr = contents_ptr as *mut Uuid;
                    let order_ptr = contents_ptr.offset(size_of::<Uuid>() as isize)
                        as *mut order;
                    let data_ptr = data_ptr as *mut V;
                    ptr::write(uuid_ptr, uuid.clone());
                    ptr::write(order_ptr, commit_col.clone());
                    ptr::write(data_ptr, data.clone());
                    entr.data_bytes = size_of::<V>() as u16;
                    entr.dependency_bytes = 0;*/
                    panic!()
                }
                &TransactionAbort(uuid) => {
                    // Abort:
                    // | uuid: size Uuid | ... | 8 | u16 = 0 | u16 = 0
                    //assert_eq!(self.data_bytes as usize, size_of::<Uuid>());
                    //assert_eq!(self.dependency_bytes, 0);
                    //let uuid = (contents_ptr as *const _).as_ref().unwrap();
                    //TransactionAbort(uuid)
                    /*let uuid_ptr = contents_ptr as *mut Uuid;
                    ptr::write(uuid_ptr, uuid.clone());
                    entr.data_bytes = size_of::<Uuid>() as u16;
                    entr.dependency_bytes = 0;*/
                    panic!()
                }*/
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
                	let cols_ptr = contents_ptr.offset(size_of::<Uuid>() as isize) as *mut order;
                	let data_ptr = data_ptr as *mut V;

					ptr::write(uuid_ptr, uuid.clone());
                	ptr::write(data_ptr, data.clone());
                	ptr::copy(deps.as_ptr(), dep_ptr, deps.len());
                	ptr::copy(columns.as_ptr(), cols_ptr, columns.len());
                }
                _ => panic!()
            }
            //TODO let dep_ptr = data_ptr.offset(self.data_bytes as isize) as *const _;
            //TODO let num_deps = (self.dependency_bytes as usize).checked_div(size_of::<OrderIndex>()).unwrap();
            //TODO let deps = slice::from_raw_parts(dep_ptr, num_deps);
            entr
        }
    }
}

#[cfg(False)]
#[test]
fn start_entry_convert() {
    use std::fmt::Debug;
    test_(1231123, 32.into());
    test_(3334, 16.into());
    test_(1231123u64, 43.into());
    test_((3334u64, 1231123), 87.into());

    fn test_<T: Clone + Debug + Eq>(data: T, col: order) {
        let id = Uuid::new_v4();
        let ent1 = TransactionStart(&data, col, &id);
        let entr = Box::new(ent1.clone_entry());
        let ent2 = entr.contents();
        assert_eq!(ent1, ent2);
    }
}

#[cfg(False)]
#[test]
fn abort_entry_convert() {
    use std::fmt::Debug;
    test_::<()>();
    test_::<i32>();
    test_::<u64>();

    fn test_<T: Clone + Debug + Eq>() {
        let id = Uuid::new_v4();
        let ent1: EntryContents<_> = TransactionAbort::<T>(&id);
        let entr = Box::new(ent1.clone_entry());
        let ent2: EntryContents<_> = entr.contents();
        assert_eq!(ent1, ent2);
    }
}

#[cfg(False)]
#[test]
fn commit_entry_convert() {
    use std::fmt::Debug;
    test_(1231123, &[(01.into(), 10.into()), (02.into(), 201.into())], &[]);
    test_(3334, &[], &[]);
    test_(1231123u64, &[(01.into(), 10.into()), (02.into(), 201.into())], &[]);
    test_(3334u64, &[], &[]);
    test_(3334u64, &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[]);
    test_((3334u64, 1231123), &[], &[]);
    test_((3334u64, 1231123), &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[]);

    test_(1231123, &[(01.into(), 10.into()), (02.into(), 201.into())], &[(32.into(), 10.into()), (402.into(), 5111.into())]);
    test_(3334, &[], &[(32.into(), 10.into()), (402.into(), 5111.into())]);
    test_(1231123u64, &[(01.into(), 10.into()), (02.into(), 201.into())], &[(32.into(), 10.into()), (402.into(), 5111.into())]);
    test_(3334u64, &[], &[(32.into(), 10.into()), (402.into(), 5111.into())]);
    test_(3334u64, &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[(32.into(), 10.into()), (402.into(), 5111.into())]);
    test_((3334u64, 1231123), &[], &[(32.into(), 10.into()), (402.into(), 5111.into())]);
    test_((3334u64, 1231123), &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[(32.into(), 10.into()), (402.into(), 5111.into())]);

    fn test_<T: Clone + Debug + Eq>(_: T, deps: &[OrderIndex], start_entries: &[OrderIndex]) {
        let id = Uuid::new_v4();
        let ent1 = TransactionCommit::<T>{
            uuid: &id,
            start_entries: &start_entries,
            deps: &deps
        };
        let entr = Box::new(ent1.clone_entry());
        let ent2 = entr.contents();
        assert_eq!(ent1, ent2);
    }
}

#[test]
fn multiput_entry_convert() {
    use std::fmt::Debug;
    test_(1231123, &[(01.into(), 10.into()), (02.into(), 201.into())], &[]);
    test_(3334, &[], &[]);
    test_(1231123u64, &[(01.into(), 10.into()), (02.into(), 201.into())], &[]);
    test_(3334u64, &[], &[]);
    test_(3334u64, &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[]);
    test_((3334u64, 1231123), &[], &[]);
    test_((3334u64, 1231123), &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[]);

    test_(1231123, &[(01.into(), 10.into()), (02.into(), 201.into())], &[32.into(), 402.into()]);
    test_(3334, &[], &[32.into(), 402.into()]);
    test_(1231123u64, &[(01.into(), 10.into()), (02.into(), 201.into())], &[32.into(), 402.into()]);
    test_(3334u64, &[], &[32.into(), 402.into()]);
    test_(3334u64, &[(01.into(), 10.into())], &[02.into(), 201.into(), 02.into(), 201.into(), 02.into(), 201.into(), 02.into(), 201.into()]);
    test_(3334u64, &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[32.into(), 402.into()]);
    test_((3334u64, 1231123), &[], &[32.into(), 402.into()]);
    test_((3334u64, 1231123), &[(01.into(), 10.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into()), (02.into(), 201.into())], &[32.into(), 402.into()]);

    fn test_<T: Clone + Debug + Eq>(data: T, deps: &[OrderIndex], cols: &[order]) {
        let id = Uuid::new_v4();
        let ent1 = Multiput{
        	data: &data,
            uuid: &id,
            deps: &deps,
            columns: cols,
        };
        let entr = Box::new(ent1.clone_entry::<[u8; MAX_DATA_LEN]>());
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
        let entr = Box::new(ent1.clone_entry::<[u8; MAX_DATA_LEN]>());
        let ent2 = entr.contents();
        assert_eq!(ent1, ent2);
    }
}

/*#[derive(Debug, Hash, PartialEq, Eq, Clone, RustcDecodable, RustcEncodable)]
pub enum Entry<V> {
    Data(V, Vec<OrderIndex>),
    TransactionCommit{uuid: Uuid, start_entries: Vec<OrderIndex>}, //TODO do commits need dependencies?
    TransactionStart(V, order, Uuid, Vec<OrderIndex>), //TODO do Starts need dependencies?
    TransactionAbort(Uuid), //TODO do Starts need dependencies?
    Multiput{data: V, columns: Vec<order>, deps: Vec<OrderIndex>},
}

impl<V> Entry<V> {
    pub fn dependencies(&self) -> &[OrderIndex] {
        match self {
            &Entry::Data(_, ref deps) => &deps,
            &Entry::TransactionCommit{..} => &[],
            &Entry::TransactionAbort(..) => &[],
            &Entry::TransactionStart(_, _, _, ref deps) => &deps,
            &Entry::Multiput{ref deps, ..} => &deps,
        }
    }
}*/

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
        self.store.multi_append(&columns[..], data, &deps[..]); //TODO error handling
        for &column in &*columns {
            let next_entry =  self.horizon.get_horizon(column) + 1;
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
            TransactionStart(data, commit_column, uuid) => {
                trace!("TransactionStart");
                //TODO clone
                self.play_transaction((column, index), commit_column, uuid.clone(), data.clone());
            }
            Multiput{data, uuid, columns, deps} => {
                //TODO
                trace!("Multiput");
                self.read_multiput(column, data, uuid, columns);
            }
            TransactionCommit{..} => { trace!("TransactionCommit"); } //TODO skip?
            TransactionAbort(..) => { trace!("TransactionAbort"); } //TODO skip?

            Data(data, _) => {
                trace!("Data");
                self.upcalls.get(&column).map(|f| f(data.clone())); //TODO clone
            }
        }
        self.local_horizon.insert(column, index);
        Some((column, index))
    }

    fn read_multiput(&mut self, first_seen_column: order, data: &V, put_id: &Uuid,
        columns: &[order]) {

        //XXX note multiserver validation happens at the store layer
        self.upcalls.get(&first_seen_column).map(|f| f(data.clone())); //TODO clone

        for column in columns { //TODO only relevent cols
            trace!("play multiput for col {:?}", column);
            self.play_until_multiput(*column, put_id);
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
                TransactionStart(data, commit_column, uuid) => {
                     trace!("TransactionStart");
                     //TODO clone
                     self.play_transaction((column, index), commit_column, uuid.clone(), data.clone());
                }
                Multiput{uuid, ..} if uuid == put_id => break 'search,
                Multiput{data, uuid, columns, deps} => {
                    //TODO
                    trace!("Multiput");
                    self.read_multiput(column, data, uuid, columns);
                }
                TransactionCommit{..} => { trace!("TransactionCommit"); } //TODO skip?
                TransactionAbort(..) => { trace!("TransactionAbort"); } //TODO skip?
                Data(data, _) => {
                    trace!("Data");
                    self.upcalls.get(&column).map(|f| f(data.clone())); //TODO clone
                }
            }
        }
        trace!("found multiput {:?} for {:?} at: {:?}", put_id, column, index);
        self.local_horizon.insert(column, index);
	}

    fn play_transaction(&mut self, (start_column, _): OrderIndex, commit_column: order, start_uuid: Uuid, data: V) {
        let mut next_entry = self.local_horizon.get(&commit_column).cloned()
            .unwrap_or(0.into()) + 1;

        let transaction_start_entries;
        let mut timed_out = false;
        'find_commit: loop {
            trace!("transaction reading: {:?}", (commit_column, next_entry));
            let next = self.store.get((commit_column, next_entry)).clone();
            match next {
                Err(GetErr::NoValue) if timed_out => {
                    let inserted = self.store.insert((commit_column, next_entry),
                        TransactionAbort(&start_uuid).clone_entry());
                    if let Ok(..) = inserted {
                        return
                    }
                }
                Err(GetErr::NoValue) => {
                    //TODO estimate based on RTT
                    thread::sleep(Duration::from_millis(100));
                    timed_out = true;
                    continue 'find_commit
                }
                Ok(entr) => {
                    match entr.contents() {
                        TransactionCommit{uuid, start_entries, ..} =>
                            if uuid == &start_uuid {
                                transaction_start_entries = Vec::from(start_entries);
                                break 'find_commit
                            },
                        TransactionAbort(uuid) =>
                            if uuid == &start_uuid {
                                return //local_horizon is updated in get_next_unseen
                            },
                        _ => {}
                    }
                }
            }
            next_entry = next_entry + 1;
            timed_out = false;
            continue 'find_commit
        }

        self.upcalls.get(&start_column).map(|f| f(data));

        //TODO instead, just mark all interesting columns not in the
        //     transaction as stale, and only read the interesting
        //     columns of the transaction
        for (column, index) in transaction_start_entries {
            if column != start_column {
                self.play_until((column, index - 1)); //TODO underflow
                let start_entry = self.store.get((column, index)).clone().expect("invalid commit entry");
                if let TransactionStart(data, commit_col, uuid) =
                    start_entry.contents() {
                    assert_eq!(commit_column, commit_col);
                    assert_eq!(&start_uuid, uuid);
                    //self.play_deps(&deps); TODO
                    self.upcalls.get(&column).map(|f| f(data.clone())); //TODO clone
                    self.local_horizon.insert(column, index);
                }
                else {
                    panic!("invalid start entry {:?} or commit entry", (column, index))
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
        let index = self.horizon.get_horizon(column);
        if index == 0.into() { return None }//TODO
        self.play_until((column, index));
        Some((column, index))
    }

    pub fn try_multiput(&mut self, offset: u32, mut columns: Vec<(order, V)>, deps: Vec<OrderIndex>) ->
    Option<Vec<OrderIndex>>
    {
        /*use std::iter::FromIterator;
        columns.sort_by(|a, b| a.0.cmp(&b.0));
        let row = columns.iter().fold(1.into(), |old, c| {
            let row = self.horizon.get_horizon(c.0);
            if old < row { old } else { row }
        }) + offset + 1;
        trace!("multiput row {:?}", row);
        let cols = Vec::from_iter(columns.iter().map(|&(c, _)| c));
        let mut puts = Vec::new();
        for &(column, val) in &columns {
            let res = self.store.insert((column, row),
                Multiput{data: &val, columns: &*cols, deps: &*deps}.clone_entry());
            match res {
                Err(InsertErr::AlreadyWritten) => {
                    trace!("multiput {:?} {:?} failed", row, column);
                    return None
                }
                Ok(()) => {
                    trace!("multiput {:?} {:?} success", row, column);
                    self.horizon.update_horizon(column, row);
                    puts.push((column, row));
                }
            }
            //TODO fillin
            //self.local_horizon.insert(column, row);
        }
        Some(puts)*/
        panic!()
    }

    pub fn start_transaction(&mut self, mut columns: Vec<(order, V)>, deps: Vec<OrderIndex>) -> Transaction<V, S, H> {
        columns.sort_by(|a, b| a.0.cmp(&b.0));
        //TODO assert columns.dedup()
        let min = columns[0].0;
        let mut start_entries = Vec::new();
        let transaction_id = Uuid::new_v4();
        for &(column, val) in &columns {
            let loc = self.append_entry(column,
                TransactionStart(&val, min, &transaction_id));
            start_entries.push(loc)
        }
        Transaction {
            log: self,
            start_entries: Some(start_entries),
            uuid: transaction_id,
        }
    }

    pub fn local_horizon(&self) -> &HashMap<order, entry> {
        &self.local_horizon
    }
}

#[must_use]
pub struct Transaction<'t, V, S, H>
where V: 't + Copy, S: 't + Store<V>, H: 't + Horizon {
    log: &'t mut FuzzyLog<V, S, H>,
    start_entries: Option<Vec<OrderIndex>>,
    uuid: Uuid,
}

impl<'t, V, S, H> Transaction<'t, V, S, H>
where V: 't + Copy, S: 't + Store<V>, H: 't + Horizon {
    pub fn commit(mut self) -> (OrderIndex, Vec<OrderIndex>) {
        let start_entries = self.start_entries.take().expect("Double committed transaction");
        (self.log.append_entry(start_entries[0].0,
            TransactionCommit {
                uuid: &self.uuid,
                start_entries: &*start_entries,
                deps: panic!(), //TODO
            }),
        start_entries)
    }

    //TODO pub fn add(&mut self, column, val)
    //TODO pub fn abort(self)
}

impl<'t, V, S, H> Drop for Transaction<'t, V, S, H>
where V: 't + Copy, S: 't + Store<V>, H: 't + Horizon {
    fn drop(&mut self) {
        let start_entries = self.start_entries.take();
        if let Some(entries) = start_entries {
            self.log.append_entry(entries[0].0,
                TransactionAbort(&self.uuid));
        }
    }
}
