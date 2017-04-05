use std::fmt;
use std::mem;
use std::slice;

pub use uuid::Uuid;

pub use storeables::{Storeable, UnStoreable};

pub use self::Packet::Ref as EntryContents;
pub use self::Packet::Mut as EntryContentsMut;
pub use self::EntryKind::EntryLayout;

use self::EntryFlag::Flag;

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
    OrderIndex(ord.into(), ent.into())
}

//pub type OrderIndex = (order, entry);
#[repr(C)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Copy, Clone, Hash, Default)]
pub struct OrderIndex(pub order, pub entry);

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
        flags Kind: u8 {
            const Invalid = 0x0,
            const Data = 0x1,
            const Multiput = 0x2,
            const Read = 0x3,
            const Lock = 0x4,
            const Sentinel = 0x6,
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

        pub fn is_write(&self) -> bool {
            match self {
                &EntryLayout::Data | &EntryLayout::Multiput | &EntryLayout::Sentinel => true,
                _ => false,
            }
        }
    }

    impl Kind {
        pub fn layout(&self) -> EntryLayout {
            match *self {
                Data => EntryLayout::Data,
                Multiput => EntryLayout::Multiput,
                Lock => EntryLayout::Lock,
                Sentinel => EntryLayout::Sentinel,
                Read => EntryLayout::Read,
                Invalid => panic!("Empty Layout"),
                _ => unreachable!("no layout {:x}", self.bits()),
            }
        }
    }
}

#[allow(non_snake_case)]
pub mod EntryFlag {
    #![allow(non_upper_case_globals)]
    bitflags! {
        flags Flag: u8 {
            const Nothing = 0x0,
            const ReadSuccess = 0x1,
            const NewMultiPut = 0x2,
            const Skeens1Queued = 0x4,
            const TakeLock = 0x8,
            const Unlock = 0x10,
            const NoRemote = 0x20,
        }
    }

    impl Flag {
        pub fn is_taking_lock(&self) -> bool {
            self.contains(TakeLock)
        }
    }
}

define_packet!{
    enum Packet {
        tag: EntryKind::Kind,
        Read: EntryKind::Read => { // 1
            id: Uuid, // 17
            flags: EntryFlag::Flag, // 19
            data_bytes: u16, // 21
            dependency_bytes: u16, // 23
            loc: OrderIndex, // 39
            horizon: OrderIndex,
        },
        Single: EntryKind::Data => {
            id: Uuid,
            flags: EntryFlag::Flag,
            data_bytes: u32,
            num_deps: u16,
            loc: OrderIndex,
            deps: [OrderIndex | num_deps],
            data: [u8 | data_bytes],
        },
        Multi: EntryKind::Multiput => {
            id: Uuid,
            flags: EntryFlag::Flag,
            data_bytes: u32,
            num_deps: u16,
            cols: u16,
            lock: u64,
            locs: [OrderIndex | cols],
            deps: [OrderIndex | num_deps],
            data: [u8 | data_bytes],
        },
        Senti: EntryKind::Sentinel => {
            id: Uuid,
            flags: EntryFlag::Flag,
            data_bytes: u32,
            num_deps: u16,
            cols: u16,
            lock: u64,
            locs: [OrderIndex | cols],
            deps: [OrderIndex | num_deps],
        },
    }
}

impl<'a> Packet::Ref<'a> {
    pub fn flag(self) -> &'a Flag {
        use self::Packet::Ref::*;
        match self {
            Read{flags, ..} | Single{flags, ..} | Multi{flags, ..} | Senti{flags, ..} =>
                flags,
        }
    }

    pub fn kind(self) -> EntryKind::Kind {
        use self::Packet::Ref::*;
        match self {
            Read{..} => EntryKind::Read,
            Single{..} => EntryKind::Data,
            Multi{..} => EntryKind::Multiput,
            Senti{..} => EntryKind::Sentinel,
        }
    }

    pub fn layout(self) -> EntryLayout {
        self.kind().layout()
    }

    pub fn id(self) -> &'a Uuid {
        use self::Packet::Ref::*;
        match self {
            Read{id, ..} | Single{id, ..} | Multi{id, ..} | Senti{id, ..} => id,
        }
    }

    pub fn locs(self) -> &'a [OrderIndex] {
        use self::Packet::Ref::*;
        match self {
            Read{loc, ..} | Single{loc, ..} => unsafe { slice::from_raw_parts(loc, 1) },
            Multi{locs, ..} | Senti{locs, ..} => locs,
        }
    }

    pub fn data(self) -> &'a [u8] {
        use self::Packet::Ref::*;
        match self {
            Multi{data, ..} | Single{data, ..} => data,
            Read{..} | Senti{..} => unreachable!(),
        }
    }

    pub fn sentinel_entry_size(self) -> usize {
        use self::Packet::Ref::*;
        let len = self.len();
        match self {
            Multi{data, ..} => len - data.len(),
            Senti{..} => len,
            Read{..} | Single{..} => unreachable!(),
        }
    }

    pub fn lock_num(self) -> u64 {
        use self::Packet::Ref::*;
        match self {
            Multi{lock, ..} | Senti{lock, ..} => *lock,
            Read{..} | Single{..} => unreachable!(),
        }
    }

    pub fn into_singleton_builder<V: ?Sized>(self) -> SingletonBuilder<'a, V>
    where V: UnStoreable {
        use self::Packet::Ref::*;
        match self {
            Read{..} | Senti{..} => unreachable!(),
            Multi{deps, data, ..} | Single{deps, data, ..} => {
                let data = unsafe { V::unstore(data) };
                SingletonBuilder(data, deps)
            },
        }
    }

    pub fn dependencies(self) -> &'a [OrderIndex] {
        use self::Packet::Ref::*;
        match self {
            Single{deps, ..} | Multi{deps, ..} | Senti{deps, ..} => deps,
            Read{..} => unreachable!(),
        }
    }

    pub fn horizon(self) -> OrderIndex {
        use self::Packet::Ref::*;
        match self {
            Single{..} | Multi{..} | Senti{..} => unreachable!(),
            Read{horizon, ..} => *horizon,
        }
    }
}

impl<'a> Packet::Mut<'a> {
    pub fn flag_mut(&mut self) -> &mut EntryFlag::Flag {
        use self::Packet::Mut::*;
        match self {
            &mut Read{ref mut flags, ..}
            | &mut Single{ref mut flags, ..}
            | &mut Multi{ref mut flags, ..}
            | &mut Senti{ref mut flags, ..} =>
                &mut **flags,
        }
    }

    pub fn flag_mut_a(&'a mut self) -> &'a mut EntryFlag::Flag {
        use self::Packet::Mut::*;
        match self {
            &mut Read{ref mut flags, ..}
            | &mut Single{ref mut flags, ..}
            | &mut Multi{ref mut flags, ..}
            | &mut Senti{ref mut flags, ..} =>
                &mut **flags,
        }
    }

    pub fn locs_mut(&mut self) -> &mut [OrderIndex] {
        use self::Packet::Mut::*;
        match self {
            &mut Read{ref mut loc, ..} | &mut Single{ref mut loc, ..} => unsafe {
                slice::from_raw_parts_mut(&mut **loc, 1)
            },
            &mut Multi{ref mut locs, ..} | &mut Senti{ref mut locs, ..} => &mut *locs,
        }
    }

    pub fn lock_mut(&mut self) -> &mut u64 {
        use self::Packet::Mut::*;
        match self {
            &mut Multi{ref mut lock, ..} | &mut Senti{ref mut lock, ..} => &mut *lock,
            &mut Read{..} | &mut Single{..} => unreachable!(),
        }
    }
}

#[derive(Copy, Clone)]
pub struct Entry<'a> {
    inner: &'a u8,
}

pub struct MutEntry<'a> {
    inner: &'a mut u8,
}

impl<'a> Entry<'a> {
    pub unsafe fn wrap(byte: &'a u8) -> Self {
        Entry { inner: byte }
    }

    pub fn bytes(self) -> &'a [u8] {
        //FIXME
        let mut size = Packet::min_len();
        loop {
            let slice = unsafe { slice::from_raw_parts(self.inner, size) };
            match unsafe { Packet::Ref::try_ref(slice) } {
                Err(Packet::WrapErr::NotEnoughBytes(n)) => size = n,
                Err(e) => panic!("{:?}", e),
                Ok((c, _)) => {
                    debug_assert_eq!(slice.len(), c.len());
                    return slice
                }
            }
        }
    }

    pub fn contents(self) -> EntryContents<'a> {
        unsafe { EntryContents::try_ref(self.bytes()).unwrap().0 }
    }

    pub fn flag(self) -> &'a EntryFlag::Flag {
        self.contents().flag()
    }

    pub unsafe fn extend_lifetime<'b>(self) -> Entry<'b> {
        mem::transmute(self)
    }
}

impl<'a> fmt::Debug for Entry<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.contents().fmt(f)
    }
}

#[allow(dead_code)]
impl<'a> MutEntry<'a> {
    pub unsafe fn wrap(byte: &'a mut u8) -> Self {
        MutEntry { inner: byte }
    }

    pub fn bytes(&mut self) -> &mut [u8] {
        //FIXME
        let mut size = Packet::min_len();
        loop {
            let slice = unsafe { slice::from_raw_parts_mut(self.inner, size) };
            match unsafe { Packet::Mut::try_mut(slice) } {
                Err(Packet::WrapErr::NotEnoughBytes(n)) => size = n,
                Err(e) => panic!("{:?}", e),
                Ok((c, _)) => {
                    debug_assert_eq!(slice.len(), c.as_ref().len());
                    return slice
                }
            }
        }
    }
    pub fn bytes_a(&'a mut self) -> &'a mut [u8] {
        //FIXME
        let mut size = Packet::min_len();
        loop {
            let slice = unsafe { slice::from_raw_parts_mut(self.inner, size) };
            match unsafe { Packet::Mut::try_mut(slice) } {
                Err(Packet::WrapErr::NotEnoughBytes(n)) => size += n,
                Err(e) => panic!("{:?}", e),
                Ok(..) => return slice,
            }
        }
    }

    pub fn contents(&mut self) -> EntryContentsMut {
        unsafe { EntryContentsMut::try_mut(self.bytes()).unwrap().0 }
    }

    pub fn into_contents(mut self) -> EntryContentsMut<'a> {
        unsafe { EntryContentsMut::try_mut(self.bytes()).unwrap().0 }
    }

    pub fn contents_a(&'a mut self) -> EntryContentsMut<'a> {
        unsafe { EntryContentsMut::try_mut(self.bytes_a()).unwrap().0 }
    }

    //pub fn flag(&'a mut self) -> &'a mut EntryFlag::Flag {
    //    self.contents_a().flag_mut_a()
    //}
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct SingletonBuilder<'a, V:'a + ?Sized>(pub &'a V, pub &'a [OrderIndex]);

impl<'a, V:'a + ?Sized> SingletonBuilder<'a, V> {
    pub fn dependencies(self) -> &'a [OrderIndex] {
        self.1
    }
}

impl<'a, V:'a> SingletonBuilder<'a, V> {
    pub fn clone_entry(self) -> ::buffer::Buffer
    where V: Storeable {
        let SingletonBuilder(data, deps) = self;
        let mut buffer = ::buffer::Buffer::empty();
        let daya = unsafe { V::ref_to_slice(data) };
        buffer.fill_from_entry_contents(
            EntryContents::Single{
                id: &Uuid::new_v4(),
                flags: &EntryFlag::Nothing,
                loc: &OrderIndex(0.into(), 0.into()),
                deps: deps,
                data: daya,
            }
        );
        buffer
    }

    pub fn fill_entry(self, buffer: &mut ::buffer::Buffer) {
        let SingletonBuilder(data, deps) = self;
        let daya = unsafe { V::ref_to_slice(data) };
        buffer.fill_from_entry_contents(
            EntryContents::Single{
                id: &Uuid::new_v4(),
                flags: &EntryFlag::Nothing,
                loc: &OrderIndex(0.into(), 0.into()),
                deps: deps,
                data: daya,
            }
        );
    }
}

impl<'a, V:'a> SingletonBuilder<'a, [V]> {
    pub fn clone_entry(self) -> ::buffer::Buffer
    where V: Storeable {
        let SingletonBuilder(data, deps) = self;
        let mut buffer = ::buffer::Buffer::empty();
        let daya = unsafe { <[V]>::ref_to_slice(data) };
        buffer.fill_from_entry_contents(
            EntryContents::Single{
                id: &Uuid::new_v4(),
                flags: &EntryFlag::Nothing,
                loc: &OrderIndex(0.into(), 0.into()),
                deps: deps,
                data: daya,
            }
        );
        buffer
    }

    pub fn fill_entry(self, buffer: &mut ::buffer::Buffer) {
        let SingletonBuilder(data, deps) = self;
        let daya = unsafe { <[V]>::ref_to_slice(data) };
        buffer.fill_from_entry_contents(
            EntryContents::Single{
                id: &Uuid::new_v4(),
                flags: &EntryFlag::Nothing,
                loc: &OrderIndex(0.into(), 0.into()),
                deps: deps,
                data: daya,
            }
        );
    }
}

pub fn data_to_slice<V: ?Sized + Storeable>(data: &V) -> &[u8] {
    unsafe { <V as Storeable>::ref_to_slice(data) }
}

pub fn slice_to_data<V: ?Sized + UnStoreable + Storeable>(data: &[u8]) -> &V {
    unsafe { <V as UnStoreable>::unstore(data) }
}

pub fn bytes_as_entry(bytes: &[u8]) -> EntryContents {
    unsafe { Packet::Ref::try_ref(bytes).unwrap().0 }
}

pub fn bytes_as_entry_mut(bytes: &mut [u8]) -> EntryContentsMut {
    unsafe { Packet::Mut::try_mut(bytes).unwrap().0 }
}

pub unsafe fn data_bytes(bytes: &[u8]) -> &[u8] {
    /*match Entry::<[u8]>::wrap_bytes(bytes).contents() {
        EntryContents::Data(data, ..) | EntryContents::Multiput{data, ..} => data,
        EntryContents::Sentinel(..) => &[],
    }*/
    use self::Packet::Ref::*;
    match bytes_as_entry(bytes) {
        Read{..} | Senti{..} => unreachable!(),
        Single{data, ..} | Multi{data, ..} => data,
    }
}

pub fn slice_to_sentinel(bytes: &mut [u8]) {
    bytes[0] = unsafe { ::std::mem::transmute(EntryKind::Sentinel) };
    unsafe { debug_assert!(EntryContents::try_ref(bytes).is_ok()) }
}

pub fn slice_to_multi(bytes: &mut [u8]) {
    bytes[0] = unsafe { ::std::mem::transmute(EntryKind::Multiput) };
    unsafe { debug_assert!(EntryContents::try_ref(bytes).is_ok()) }
}

///////////////////////////////////////
///////////////////////////////////////
///////////////////////////////////////
///////////////////////////////////////

#[cfg(test)]
mod test {

    use super::*;

    use std::marker::PhantomData;


    /*#[test]
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

        test_(1231123, &[OrderIndex(01.into(), 10.into()), OrderIndex(02.into(), 201.into())], &[OrderIndex(32.into(), 0.into()), OrderIndex(402.into(), 5.into())]);
        test_(3334, &[], &[OrderIndex(32.into(), 0.into()), OrderIndex(402.into(), 5.into())]);
        test_(1231123u64, &[OrderIndex(01.into(), 10.into()), OrderIndex(02.into(), 201.into())], &[OrderIndex(32.into(), 0.into()), OrderIndex(402.into(), 5.into())]);
        test_(3334u64, &[], &[OrderIndex(32.into(), 0.into()), OrderIndex(402.into(), 5.into())]);
        test_(3334u64, &[OrderIndex(01.into(), 10.into())], &[OrderIndex(02.into(), 9.into()), OrderIndex(201.into(), 0.into()), OrderIndex(02.into(), 57.into()), OrderIndex(201.into(), 0xffffffff.into()), OrderIndex(02.into(), 0xdeadbeef.into()), OrderIndex(201.into(), 2.into()), OrderIndex(02.into(), 6.into()), OrderIndex(201.into(), 201.into())]);
        test_(3334u64, &[OrderIndex(01.into(), 10.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into())], &[OrderIndex(32.into(), 0.into()), OrderIndex(402.into(), 5.into())]);
        test_((3334u64, 1231123), &[], &[OrderIndex(32.into(), 0.into()), OrderIndex(402.into(), 5.into())]);
        test_((3334u64, 1231123), &[OrderIndex(01.into(), 10.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into())], &[OrderIndex(32.into(), 0.into()), OrderIndex(402.into(), 5.into())]);

        fn test_<T: Clone + Debug + Eq>(data: T, deps: &[OrderIndex], cols: &[OrderIndex]) {
            let id = Uuid::new_v4();
            let ent1 = EntryContents::Multiput{
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
        test_(1231123, &[OrderIndex(01.into(), 10.into()), OrderIndex(02.into(), 201.into())]);
        test_(3334, &[]);
        test_(1231123u64, &[OrderIndex(01.into(), 10.into()), OrderIndex(02.into(), 201.into())]);
        test_(3334u64, &[]);
        test_(3334u64, &[OrderIndex(01.into(), 10.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into())]);
        test_((3334u64, 1231123), &[]);
        test_((3334u64, 1231123), &[OrderIndex(01.into(), 10.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into()), OrderIndex(02.into(), 201.into())]);

        fn test_<T: Clone + Debug + Eq>(data: T, deps: &[OrderIndex]) {
            let ent1 = EntryContents::Data(&data, &deps);
            let entr = Box::new(ent1.clone_entry());
            let ent2 = entr.contents();
            assert_eq!(ent1, ent2);
        }
    }*/

/*
    #[test]
    fn new_packets_multi() {
        test_(
            &[12, 31,123],
            &[OrderIndex(01.into(), 10.into()), OrderIndex(02.into(), 201.into())],
            &[OrderIndex(32.into(), 0.into()), OrderIndex(402.into(), 5.into())]
        );
        test_(
            &[33,34],
            &[],
            &[OrderIndex(32.into(), 0.into()), OrderIndex(402.into(), 5.into())]
        );
        fn test_(data0: &[u8], deps0: &[OrderIndex], cols0: &[OrderIndex]) {
            let id0 = Uuid::new_v4();
            let mut ent1 = Vec::new();
            EntryContents::Multiput{
                data: data0,
                uuid: &id0,
                deps: deps0,
                columns: cols0,
            }.fill_vec(&mut ent1);
            ent1[0] |= 0x80;
            let r = unsafe { ServerToClient::Ref::wrap(&*ent1) };
            println!("{:?}", r);
            match r {
                ServerToClient::Ref::MultiRead{
                    id,
                    _padding,
                    data_bytes,
                    dependency_bytes,
                    lock,
                    cols,
                    locs,
                    data,
                    deps,
                    ..
                } => {
                    println!("{:?}", ent1);
                    assert_eq!(id, &id0);
                    assert_eq!(*data_bytes as usize, data.len());
                    assert_eq!(
                        *dependency_bytes as usize,
                        deps0.len() * mem::size_of::<OrderIndex>()
                    );
                    assert_eq!(*cols as usize, cols0.len());
                    assert_eq!(locs, cols0);
                    assert_eq!(data, data0);
                    let d: &[u8] = unsafe {
                        let len = deps0.len() * mem::size_of::<OrderIndex>();
                        slice::from_raw_parts(deps0.as_ptr() as *const _, len)
                    };
                    assert_eq!(deps, d);
                },
                _ => unreachable!(),
            }
        }
    }
    */
}
