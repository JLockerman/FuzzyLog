
#[macro_use] extern crate bitflags;
#[macro_use] extern crate custom_derive;
#[macro_use] extern crate packet_macro_impl;
#[macro_use] extern crate packet_macro2;
#[macro_use] extern crate newtype_derive;

extern crate rustc_serialize;
extern crate uuid;

#[cfg(test)] extern crate byteorder;


use std::fmt;
use std::mem;
use std::slice;

pub use uuid::Uuid;

pub use storeables::{Storeable, UnStoreable};

pub use self::Packet::Ref as EntryContents;
pub use self::Packet::Mut as EntryContentsMut;
pub use self::Packet::Var as EntryVar;
pub use self::EntryKind::EntryLayout;

use self::EntryFlag::Flag;

pub mod buffer;
pub mod buffer2;
pub mod storeables;
pub mod double_buffer;

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

impl From<(u32, u32)> for OrderIndex {
    fn from((o, e): (u32, u32)) -> Self {
        OrderIndex(o.into(), e.into())
    }
}

impl From<(order, entry)> for OrderIndex {
    fn from((o, e): (order, entry)) -> Self {
        OrderIndex(o, e)
    }
}

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
            const Skeens2ToReplica = 0x8,

            const SingleToReplica = Data.bits | ToReplica.bits,
            const MultiputToReplica = Multiput.bits | ToReplica.bits,
            const SentinelToReplica = Sentinel.bits | ToReplica.bits,

            const ToReplica = 0x10,

            const FenceClient = 0x20,
            const UpdateRecovery = 0x30,
            const CheckSkeens1 = 0x40,
            const GC = 0x50,

            const Snapshot = 0x60,
            const SnapshotToReplica = 0x70,
        }
    }

    #[derive(Copy, Clone, PartialEq, Eq, Debug)]
    pub enum EntryLayout {
        Data,
        Multiput,
        Lock,
        Sentinel,
        Read,
        GC,
        Snapshot,
    }

    impl EntryLayout {
        pub fn kind(&self) -> Kind {
            match self {
                &EntryLayout::Data => Data,
                &EntryLayout::Multiput => Multiput,
                &EntryLayout::Lock => Lock,
                &EntryLayout::Sentinel => Sentinel,
                &EntryLayout::Read => Read,
                &EntryLayout::GC => GC,
                &EntryLayout::Snapshot => Snapshot,
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
                Data | SingleToReplica => EntryLayout::Data,
                Multiput | MultiputToReplica => EntryLayout::Multiput,
                Sentinel | SentinelToReplica => EntryLayout::Sentinel,
                Lock => EntryLayout::Lock,
                Read => EntryLayout::Read,
                GC => EntryLayout::GC,
                Snapshot => EntryLayout::Snapshot,
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
            const NewMultiPut = 0x2, //TODO this flag should always be on, remove?
            const Skeens1Queued = 0x4,
            const TakeLock = 0x8,
            const Unlock = 0x10,
            const NoRemote = 0x20,
            const DirectWrite = 0x40,
            const SnapshotAndFetch = 0x80,
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
        Read: EntryKind::Read => {
            id: Uuid,
            flags: EntryFlag::Flag,
            data_bytes: u16,
            dependency_bytes: u16,
            loc: OrderIndex,
            horizon: OrderIndex,
            min:  OrderIndex,
        },
        Single: EntryKind::Data => {
            id: Uuid,
            flags: EntryFlag::Flag,
            data_bytes: u32,
            num_deps: u16,
            loc: OrderIndex,
            deps: [OrderIndex | num_deps],
            data: [u8 | data_bytes],
            timestamp: u64,
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

        SingleToReplica: EntryKind::SingleToReplica => {
            id: Uuid,
            flags: EntryFlag::Flag,
            data_bytes: u32,
            num_deps: u16,
            loc: OrderIndex,
            deps: [OrderIndex | num_deps],
            data: [u8 | data_bytes],
            timestamp: u64,
            queue_num: u64,
        },
        MultiToReplica: EntryKind::MultiputToReplica => {
            id: Uuid,
            flags: EntryFlag::Flag,
            data_bytes: u32,
            num_deps: u16,
            cols: u16,
            lock: u64,
            locs: [OrderIndex | cols],
            deps: [OrderIndex | num_deps],
            data: [u8 | data_bytes],
            queue_nums: [u64 | cols],
        },
        SentiToReplica: EntryKind::SentinelToReplica => {
            id: Uuid,
            flags: EntryFlag::Flag,
            data_bytes: u32,
            num_deps: u16,
            cols: u16,
            lock: u64,
            locs: [OrderIndex | cols],
            deps: [OrderIndex | num_deps],
            queue_nums: [u64 | cols],
        },

        Skeens2ToReplica: EntryKind::Skeens2ToReplica => {
            id: Uuid,
            lock: u64,
            loc: OrderIndex,
        },


        GC: EntryKind::GC => {
            id: Uuid,
            flags: EntryFlag::Flag,
            cols: u16,
            locs: [OrderIndex | cols],
        },
        FenceClient: EntryKind::FenceClient => {
            fencing_write: Uuid,
            client_to_fence: Uuid,
            fencing_client: Uuid,
        },

        /*?
        TakeOverRecovery: EntryKind::FenceClient => {
            fencing_write: Uuid,
            flags: EntryFlag,
            old_recoverer: Uuid,
            new_recoverer: Uuid,
        }
        */

        UpdateRecovery: EntryKind::UpdateRecovery => {
            old_recoverer: Uuid,
            write_id: Uuid,
            flags: EntryFlag::Flag,
            cols: u16,
            lock: u64,
            locs: [OrderIndex | cols],
        },

        CheckSkeens1: EntryKind::CheckSkeens1 => {
            id: Uuid,
            flags: EntryFlag::Flag,
            data_bytes: u16,
            dependency_bytes: u16,
            loc: OrderIndex,
        },

        Snapshot: EntryKind::Snapshot => {
            id: Uuid,
            flags: EntryFlag::Flag,
            data_bytes: u32,
            num_deps: u16,
            cols: u16,
            lock: u64,
            locs: [OrderIndex | cols],
        },
        SnapshotToReplica: EntryKind::SnapshotToReplica => {
            id: Uuid,
            flags: EntryFlag::Flag,
            data_bytes: u32,
            num_deps: u16,
            cols: u16,
            lock: u64,
            locs: [OrderIndex | cols],
            queue_nums: [u64 | cols],
        },
    }
}

impl<'a> Packet::Ref<'a> {
    pub fn read(loc: &'a OrderIndex) -> Self {
        static NO_FLAG: &'static EntryFlag::Flag = &EntryFlag::Nothing;
        static ZERO_BYTES: &'static u16 = &0;
        static ID: &'static Uuid = &::uuid::NAMESPACE_DNS;

        Packet::Ref::Read {
            id: ID,
            flags: NO_FLAG,
            data_bytes: ZERO_BYTES,
            dependency_bytes: ZERO_BYTES,
            loc: &loc,
            horizon: &loc,
            min: &loc,
        }
    }

    pub fn flag(self) -> &'a Flag {
        use self::Packet::Ref::*;
        match self {
            Read{flags, ..}
            | Single{flags, ..} | SingleToReplica{flags, ..}
            | Multi{flags, ..} | MultiToReplica{flags, ..}
            | Senti{flags, ..} | SentiToReplica{flags, ..}
            | GC{flags, ..}
            | UpdateRecovery{flags, ..} | CheckSkeens1{flags, ..}
            | Snapshot{flags, ..} | SnapshotToReplica{flags, ..} =>
                flags,

            FenceClient{..} => {
                static NO_FLAG: EntryFlag::Flag = EntryFlag::Nothing;
                &NO_FLAG
            },

            Skeens2ToReplica{..} => unreachable!(),
        }
    }

    pub fn kind(self) -> EntryKind::Kind {
        use self::Packet::Ref::*;
        match self {
            Read{..} => EntryKind::Read,
            Single{..} => EntryKind::Data,
            Multi{..} => EntryKind::Multiput,
            Senti{..} => EntryKind::Sentinel,
            SingleToReplica{..} => EntryKind::SingleToReplica,
            MultiToReplica{..} => EntryKind::MultiputToReplica,
            SentiToReplica{..} => EntryKind::SentinelToReplica,
            Skeens2ToReplica{..} => EntryKind::Skeens2ToReplica,
            GC{..} => EntryKind::GC,
            UpdateRecovery{..} => EntryKind::UpdateRecovery,
            FenceClient{..} => EntryKind::FenceClient,
            CheckSkeens1{..} => EntryKind::CheckSkeens1,
            Snapshot{..} => EntryKind::Snapshot,
            SnapshotToReplica{..} => EntryKind::SnapshotToReplica,
        }
    }

    pub fn layout(self) -> EntryLayout {
        self.kind().layout()
    }

    pub fn id(self) -> &'a Uuid {
        use self::Packet::Ref::*;
        match self {
            Read{id, ..} | Single{id, ..} | Multi{id, ..} | Senti{id, ..}
            | SingleToReplica{id, ..}
            | MultiToReplica{id, ..} | SentiToReplica{id, ..}
            | Skeens2ToReplica{id, ..}
            | GC{id, ..}
            | CheckSkeens1{id, ..} => id,

            UpdateRecovery{write_id, ..} => write_id,
            FenceClient{fencing_write, ..} => fencing_write,

            Snapshot{id, ..} | SnapshotToReplica{id, ..} => id
        }
    }

    pub fn locs(self) -> &'a [OrderIndex] {
        use self::Packet::Ref::*;
        match self {
            Read{loc, ..} | Single{loc, ..} | SingleToReplica{loc, ..}
            | Skeens2ToReplica{loc, ..} | CheckSkeens1{loc, ..} => unsafe {
                slice::from_raw_parts(loc, 1)
            },

            Multi{locs, ..} | Senti{locs, ..}
            | MultiToReplica{locs, ..} | SentiToReplica{locs, ..}
            | GC{locs, ..}
            | UpdateRecovery{locs, ..}
            | Snapshot{locs, ..}
            | SnapshotToReplica{locs, ..} => locs,

            FenceClient{..} => unreachable!(),
        }
    }

    pub fn locs_and_node_nums(self) ->
        ::std::iter::Zip<::std::slice::Iter<'a, OrderIndex>, ::std::slice::Iter<'a, u64>> {
        use self::Packet::Ref::*;
        match self {
            SingleToReplica{loc, queue_num, ..} => unsafe {
                slice::from_raw_parts(loc, 1).iter().zip(
                    slice::from_raw_parts(queue_num, 1)
                )
            },

            MultiToReplica{locs, queue_nums, ..} | SentiToReplica{locs, queue_nums, ..}
            | SnapshotToReplica{locs, queue_nums, ..}  => {
                locs.iter().zip(queue_nums.iter())
            },

            Read{..} | Single{..} | Multi{..} | Senti{..} | Skeens2ToReplica{..}
            | GC{..}
            | UpdateRecovery{..} | FenceClient{..} | CheckSkeens1{..}
            | Snapshot{..}  => unreachable!(),
        }
    }

    pub fn data(self) -> &'a [u8] {
        use self::Packet::Ref::*;
        match self {
            Multi{data, ..} | Single{data, ..}
            | MultiToReplica{data, ..} | SingleToReplica{data, ..} => data,

            Read{..} | Senti{..} | SentiToReplica{..} | Skeens2ToReplica{..}
            | GC{..}
            | UpdateRecovery{..} | FenceClient{..}
            | CheckSkeens1{..}
            | Snapshot{..} | SnapshotToReplica{..} => unreachable!(),
        }
    }

    pub fn sentinel_entry_size(self) -> usize {
        use self::Packet::Ref::*;
        match self {
            MultiToReplica{ id, flags, lock, locs, deps, ..} =>
                Senti{
                    id:id, flags:flags, data_bytes:&0, lock:lock, locs:locs, deps:deps
                }.len(),

            SentiToReplica{ id, flags, data_bytes, lock, locs, deps, ..} =>
                Senti{
                    id:id, flags:flags, data_bytes:data_bytes, lock:lock, locs:locs, deps:deps,
                }.len(),

            Multi{ id, flags, lock, locs, deps, ..} =>
                Senti{
                    id:id, flags:flags, data_bytes:&0, lock:lock, locs:locs, deps:deps
                }.len(),

            s @ Senti{..} => s.len(),

            Read{..} | Single{..} | SingleToReplica{..} | Skeens2ToReplica{..}
            | GC{..}
            | UpdateRecovery{..} | FenceClient{..} | CheckSkeens1{..}
            |Snapshot{..} | SnapshotToReplica{..} => unreachable!(),
        }
    }

    pub fn lock_num(self) -> u64 {
        use self::Packet::Ref::*;
        match self {
            Multi{lock, ..} | Senti{lock, ..}
            | MultiToReplica{lock, ..}
            | SentiToReplica{lock, ..}
            | Skeens2ToReplica{lock, ..}
            | UpdateRecovery{lock, ..}
            | Snapshot{lock, ..} | SnapshotToReplica{lock, ..}  => *lock,
            SingleToReplica{timestamp, ..}| Single{timestamp, ..} => *timestamp,

            Read{..} => 0,

            GC{..}
            | FenceClient{..} | CheckSkeens1{..}  => unreachable!(),
        }
    }

    pub fn into_singleton_builder<V: ?Sized>(self) -> SingletonBuilder<'a, V>
    where V: UnStoreable {
        use self::Packet::Ref::*;
        match self {
            Read{..} | Senti{..} | SentiToReplica{..} | Skeens2ToReplica{..}
            | GC{..}
            | UpdateRecovery{..} | FenceClient{..} | CheckSkeens1{..}
            | Snapshot{..} | SnapshotToReplica{..} => unreachable!(),

            SingleToReplica{deps, data, ..}
            | MultiToReplica{deps, data, ..}
            | Multi{deps, data, ..}
            | Single{deps, data, ..} => {
                let data = unsafe { V::unstore(data) };
                SingletonBuilder(data, deps)
            },
        }
    }

    pub fn dependencies(self) -> &'a [OrderIndex] {
        use self::Packet::Ref::*;
        match self {
            Single{deps, ..} | Multi{deps, ..} | Senti{deps, ..}
            | SingleToReplica{deps, ..} | MultiToReplica{deps, ..} | SentiToReplica{deps, ..} =>
                deps,

            Read{..} | Skeens2ToReplica{..}| GC{..} | UpdateRecovery{..} | FenceClient{..}
            |CheckSkeens1{..}
            | Snapshot{..} | SnapshotToReplica{..} =>
                unreachable!(),
        }
    }

    pub fn horizon(self) -> OrderIndex {
        use self::Packet::Ref::*;
        match self {
            Single{..} | Multi{..} | Senti{..}
            | SingleToReplica{..} | MultiToReplica{..} | SentiToReplica{..}
            | Skeens2ToReplica{..} | GC{..}
            | UpdateRecovery{..} | FenceClient{..}
            | CheckSkeens1{..}
            | Snapshot{..} | SnapshotToReplica{..} =>
                unreachable!(),
            Read{horizon, ..} => *horizon,
        }
    }

    pub fn non_replicated_len(self) -> usize {
        use self::Packet::Ref::*;
        match self {
            c @ Read {..} | c @ Single {..} | c @ Multi{..} | c @Senti{..} | c @ GC{..}
            | c @ UpdateRecovery{..} | c @ CheckSkeens1{..}
            | c @ Snapshot{..} | c @ SnapshotToReplica{..} => c.len(),

            SingleToReplica{ id, flags, loc, deps, data, timestamp, ..} =>
                Single{id: id, flags: flags, loc: loc, deps: deps, data: data, timestamp}.len(),

            MultiToReplica{ id, flags, lock, locs, deps, data, ..} =>
                Multi{
                    id:id, flags:flags, lock:lock, locs:locs, deps:deps, data:data,
                }.len(),

            SentiToReplica{ id, flags, data_bytes, lock, locs, deps, ..} =>
                Senti{
                    id:id, flags:flags, data_bytes:data_bytes, lock:lock, locs:locs, deps:deps,
                }.len(),

            Skeens2ToReplica{..} => unreachable!(),
            FenceClient{..} => unreachable!(),
        }
    }

    pub fn single_skeens_to_replication(self, time: &'a u64, queue_num: &'a u64) -> Self {
        use self::Packet::Ref::*;
        match self {
            Single{id, flags, loc, deps, data, ..} => {
                SingleToReplica{
                    id: id,
                    flags: flags,
                    loc: loc,
                    deps: deps,
                    data: data,
                    timestamp: time,
                    queue_num: queue_num,
                }
            }

            o => panic!("tried to turn {:?} into a singleton skeens replica.", o),
        }
    }

    pub fn multi_skeens_to_replication(self, queue_nums: &'a [u64]) -> Self {
        use self::Packet::Ref::*;
        match self {
            Multi{id, flags, lock, locs, deps, data} => {
                MultiToReplica{
                    id: id,
                    flags: flags,
                    lock: lock,
                    locs: locs,
                    deps: deps,
                    data: data,
                    queue_nums: queue_nums,
                }
            }

            Senti{id, flags, data_bytes, lock, locs, deps} => {
                SentiToReplica{
                    id: id,
                    flags: flags,
                    data_bytes: data_bytes,
                    lock: lock,
                    locs: locs,
                    deps: deps,
                    queue_nums: queue_nums,
                }
            }

            Snapshot{id, flags, data_bytes, num_deps, lock, locs} => {
                SnapshotToReplica{
                    id,
                    flags,
                    data_bytes,
                    num_deps,
                    lock,
                    locs,
                    queue_nums,
                }
            }

            o => panic!("tried to turn {:?} into a skeens replica.", o),
        }
    }

    pub fn to_unreplica(self) -> Self {
        use self::Packet::Ref::*;
        match self {
            MultiToReplica{id, flags, lock, locs, deps, data, ..} => {
                Multi{ id, flags, lock, locs, deps, data }
            }

            SentiToReplica{ id, flags, data_bytes, lock, locs, deps, ..} => {
                Senti{ id, flags, data_bytes, lock, locs, deps }
            }

            SnapshotToReplica{id, flags, data_bytes, num_deps, lock, locs, ..} => {
                Snapshot{ id, flags, data_bytes, num_deps, lock, locs }
            }

            SingleToReplica{id, flags, loc, deps, data, timestamp, ..} => {
                Single{id, flags, loc, deps, data, timestamp }
            },

            o => panic!("tried to turn {:?} into a singleton skeens replica.", o),
        }
    }

    pub fn queue_nums(self) -> &'a [u64] {
        use self::Packet::Ref::*;
        match self {
            MultiToReplica{queue_nums, ..}
            | SentiToReplica{queue_nums, ..}
            | SnapshotToReplica{queue_nums, ..} =>
                queue_nums,

            o => panic!("tried to get queue_nums from {:?}.", o),
        }
    }

    pub fn to_vec(self) -> Vec<u8> {
        let mut v = Vec::new();
        self.fill_vec(&mut v);
        v
    }

    pub fn write_id_and_old_recoverer(self) -> (&'a Uuid, &'a Uuid) {
        use self::Packet::Ref::*;
        match self {
            UpdateRecovery{write_id, old_recoverer, ..} =>
                (write_id, old_recoverer),

            _ => unreachable!(),
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
            | &mut Senti{ref mut flags, ..}
            | &mut SingleToReplica{ref mut flags, ..}
            | &mut MultiToReplica{ref mut flags, ..}
            | &mut SentiToReplica{ref mut flags, ..}
            | &mut GC{ref mut flags, ..}
            | &mut UpdateRecovery{ref mut flags, ..}
            | &mut CheckSkeens1{ref mut flags, ..}
            | &mut Snapshot{ref mut flags, ..}
            | &mut SnapshotToReplica{ref mut flags, ..} =>
                &mut **flags,

            &mut Skeens2ToReplica{..} | &mut FenceClient{..} => unreachable!(),
        }
    }

    pub fn flag_mut_a(&'a mut self) -> &'a mut EntryFlag::Flag {
        use self::Packet::Mut::*;
        match self {
            &mut Read{ref mut flags, ..}
            | &mut Single{ref mut flags, ..}
            | &mut Multi{ref mut flags, ..}
            | &mut Senti{ref mut flags, ..}
            | &mut SingleToReplica{ref mut flags, ..}
            | &mut MultiToReplica{ref mut flags, ..}
            | &mut SentiToReplica{ref mut flags, ..}
            | &mut GC{ref mut flags, ..}
            | &mut UpdateRecovery{ref mut flags, ..}
            | &mut CheckSkeens1{ref mut flags, ..}
            | &mut Snapshot{ref mut flags, ..}
            | &mut SnapshotToReplica{ref mut flags, ..} =>
                &mut **flags,

            &mut Skeens2ToReplica{..} | &mut FenceClient{..} => unreachable!(),
        }
    }

    pub fn locs_mut(&mut self) -> &mut [OrderIndex] {
        use self::Packet::Mut::*;
        match self {
            &mut Read{ref mut loc, ..}
            | &mut Single{ref mut loc, ..}
            | &mut SingleToReplica{ref mut loc, ..}
            | &mut Skeens2ToReplica{ref mut loc, ..}
            | &mut CheckSkeens1{ref mut loc, ..} => unsafe {
                slice::from_raw_parts_mut(&mut **loc, 1)
            },

            &mut Multi{ref mut locs, ..}
            | &mut Senti{ref mut locs, ..}
            | &mut MultiToReplica{ref mut locs, ..}
            | &mut SentiToReplica{ref mut locs, ..}
            | &mut GC{ref mut locs, ..}
            | &mut UpdateRecovery{ref mut locs, ..}
            | &mut Snapshot{ref mut locs, ..}
            | &mut SnapshotToReplica{ref mut locs, ..} => &mut *locs,

            &mut FenceClient{..} => unreachable!(),
        }
    }

    pub fn lock_mut(&mut self) -> &mut u64 {
        use self::Packet::Mut::*;
        match self {
            &mut Multi{ref mut lock, ..} | &mut Senti{ref mut lock, ..}
            | &mut Skeens2ToReplica {ref mut lock, .. } => &mut *lock,

            &mut MultiToReplica{ref mut lock, ..} | &mut SentiToReplica{ref mut lock, ..}
            |&mut UpdateRecovery{ref mut lock, ..} => &mut *lock,

            &mut Snapshot{ref mut lock, ..}
            | &mut SnapshotToReplica{ref mut lock, ..} => &mut *lock,

            &mut Single{ref mut timestamp, ..} | &mut SingleToReplica{ref mut timestamp, ..} => &mut *timestamp,

            &mut Read{..}
            | &mut GC{..}
            | &mut FenceClient{..}
            | &mut CheckSkeens1{..} => unreachable!(),
        }
    }

    pub fn recoverer_is(&mut self, id: Uuid) {
        use self::Packet::Mut::*;
        match self {
            &mut UpdateRecovery{ref mut old_recoverer, ..} => **old_recoverer = id,
            _ => unreachable!(),
        }
    }
}

//FIXME make slice
#[derive(Copy, Clone)]
pub struct Entry<'a> {
    inner: &'a [u8],
}

//FIXME make slice
pub struct MutEntry<'a> {
    inner: &'a mut [u8],
}

impl<'a> Entry<'a> {

    pub unsafe fn wrap_bytes(byte: *const u8) -> Self {
        let mut size = Packet::min_len();
        loop {
            let slice = slice::from_raw_parts(byte, size);
            match Packet::Ref::try_ref(slice) {
                Err(Packet::WrapErr::NotEnoughBytes(n)) => size = n,
                Err(e) => panic!("{:?}", e),
                Ok((c, _)) => {
                    debug_assert_eq!(slice.len(), c.len());
                    return Entry { inner: slice }
                }
            }
        }

    }

    pub fn wrap_slice(slice: &'a [u8]) -> Self {
        Entry { inner: slice }
    }

    pub fn bytes(self) -> &'a [u8] {
        self.inner
    }

    pub fn contents(self) -> EntryContents<'a> {
        unsafe { EntryContents::try_ref(self.inner).unwrap().0 }
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

    pub unsafe fn wrap_bytes(byte: *mut u8) -> Self {
        let mut size = Packet::min_len();
        loop {
            let slice = slice::from_raw_parts_mut(byte, size);
            match Packet::Mut::try_mut(slice) {
                Err(Packet::WrapErr::NotEnoughBytes(n)) => size = n,
                Err(e) => panic!("{:?}", e),
                Ok((c, _)) => {
                    debug_assert_eq!(slice.len(), c.as_ref().len());
                    return MutEntry { inner: slice }
                }
            }
        }
    }

    pub fn wrap_slice(slice: &'a mut [u8]) -> Self {
        MutEntry { inner: slice }
    }

    pub fn to_non_replicated(&mut self) {
        unsafe {
            let mut kind: EntryKind::Kind = mem::transmute::<u8, _>(self.inner[0]);
            let () = kind.remove(EntryKind::ToReplica);
            self.inner[0] = mem::transmute::<EntryKind::Kind, u8>(kind);
        }
    }

    pub fn to_replicated(&mut self) {
        unsafe {
            let mut kind: EntryKind::Kind = mem::transmute::<u8, _>(self.inner[0]);
            let () = kind.insert(EntryKind::ToReplica);
            self.inner[0] = mem::transmute::<EntryKind::Kind, u8>(kind);
        }
    }

    pub fn bytes(&mut self) -> &mut [u8] {
        &mut *self.inner
    }
    pub fn bytes_a(&'a mut self) -> &'a mut [u8] {
        &mut *self.inner
    }

    pub fn contents(&mut self) -> EntryContentsMut {
        unsafe { EntryContentsMut::try_mut(self.bytes()).unwrap().0 }
    }

    pub fn into_contents(mut self) -> EntryContentsMut<'a> {
        unsafe { EntryContentsMut::try_mut(self.bytes()).unwrap().0 }
    }

    pub fn contents_a(&'a mut self) -> EntryContentsMut<'a> {
        unsafe { EntryContentsMut::try_mut(self.inner).unwrap().0 }
    }
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
                timestamp: &0, //FIXME
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
                timestamp: &0, //FIXME
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
                timestamp: &0, //FIXME
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
                timestamp: &0, //FIXME
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
        Read{..} | Senti{..} | SentiToReplica{..} | Skeens2ToReplica{..}
        | GC{..}
        | FenceClient{..} | UpdateRecovery{..} | CheckSkeens1{..}
        | Snapshot{..}  | SnapshotToReplica{..} => unreachable!(),

        Single{data, ..} | Multi{data, ..}
        | SingleToReplica{data, ..} | MultiToReplica{data, ..} => data,
    }
}

pub fn slice_to_sentinel(bytes: &mut [u8]) -> bool {
    let old_kind: EntryKind::Kind = unsafe { ::std::mem::transmute(bytes[0]) };
    bytes[0] = unsafe { ::std::mem::transmute(EntryKind::Sentinel) };
    unsafe { debug_assert!(EntryContents::try_ref(bytes).is_ok()) }
    old_kind == EntryKind::Multiput || old_kind == EntryKind::MultiputToReplica
}

pub fn slice_to_multi(bytes: &mut [u8]) {
    bytes[0] = unsafe { ::std::mem::transmute(EntryKind::Multiput) };
    unsafe { debug_assert!(EntryContents::try_ref(bytes).is_ok()) }
}

pub fn slice_to_skeens1_multirep(bytes: &mut [u8]) {
    bytes[0] = unsafe { ::std::mem::transmute(EntryKind::MultiputToReplica) };
    unsafe { debug_assert!(EntryContents::try_ref(bytes).is_ok()) }
}

pub fn slice_to_skeens1_sentirep(bytes: &mut [u8]) {
    bytes[0] = unsafe { ::std::mem::transmute(EntryKind::SentinelToReplica) };
    unsafe { debug_assert!(EntryContents::try_ref(bytes).is_ok()) }
}

///////////////////////////////////////
///////////////////////////////////////
///////////////////////////////////////
///////////////////////////////////////

#[cfg(test)]
mod test {

    use super::*;

    use byteorder::{ByteOrder, LittleEndian};

    //use std::marker::PhantomData;

    //#[test]
    // fn packet_size_check() {
    //     let b = Packet::Ref::Single {
    //         id: &Uuid::nil(),
    //         flags: &EntryFlag::Nothing,
    //         loc: &OrderIndex(0.into(), 0.into()),
    //         deps: &[],
    //         data: &[],
    //     }.len();
    //     assert!(
    //         Packet::min_len() >= b,
    //         "{} >= {}",
    //         Packet::min_len(), b,
    //     );
    //     /*let b = Packet::Ref::Multi {
    //         id: &Uuid::nil(),
    //         flags: &EntryFlag::Nothing,
    //         lock: &0,
    //         locs: &[],
    //         deps: &[],
    //         data: &[],
    //     }.len();
    //     assert!(
    //         Packet::min_len() >= b,
    //         "{} >= {}",
    //         Packet::min_len(), b,
    //     );*/
    // }

    #[test]
    fn round_trip() {
        let id = Uuid::new_v4();
        let flag = EntryFlag::ReadSuccess;
        let data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 99, 10];
        // let deps = [OrderIndex(1.into(), 1.into()), OrderIndex(2.into(), 1.into()), OrderIndex(3.into(), 3.into())];

        let timestamp = 0xdeadbeef;
        let contents = EntryContents::Single {
            id: &id,
            flags: &flag,
            timestamp: &timestamp,
            deps: &[],
            loc: &(1, 13).into(),
            data: &data,
        };
        let mut bytes = vec![];
        contents.fill_vec(&mut bytes);
        assert_eq!(bytes.len(), contents.len());
        assert_eq!(bytes_as_entry(&bytes), contents);
        assert_eq!(bytes_as_entry_mut(&mut bytes).as_ref(), contents);
    }

    #[test]
    fn packet_sanity_check() {
        let id = Uuid::new_v4();
        let flag = EntryFlag::ReadSuccess;
        let data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 99, 10];
        let deps = [OrderIndex(1.into(), 1.into()), OrderIndex(2.into(), 1.into()), OrderIndex(3.into(), 3.into())];
        let locs = [OrderIndex(5.into(), 55.into()), OrderIndex(37.into(), 8.into()),
            OrderIndex(1000000000.into(), 0xffffffff.into())];
        let lock = 0xdeadbeef;
        let mut bytes = Vec::new();
        EntryContents::Multi {
            id: &id,
            flags: &flag,
            //data_bytes: u32,
            //num_deps: u16,
            //cols: u16,
            lock: &lock,
            locs: &locs,
            deps: &deps,
            data: &data,
        }.fill_vec(&mut bytes);
        let mut data_bytes = [0u8; 4];
        LittleEndian::write_u32(&mut data_bytes, data.len() as u32);
        let mut num_deps = [0u8; 2];
        LittleEndian::write_u16(&mut num_deps, deps.len() as u16);
        let mut cols = [0u8; 2];
        LittleEndian::write_u16(&mut cols, locs.len() as u16);
        let mut lock_bytes = [0u8; 8];
        LittleEndian::write_u64(&mut lock_bytes, lock);

        let locs_as_bytes = unsafe { slice::from_raw_parts(
            &locs as *const _ as *const u8, locs.len() * mem::size_of::<OrderIndex>()) };
        let deps_as_bytes = unsafe { slice::from_raw_parts(
            &deps as *const _ as *const u8, deps.len() * mem::size_of::<OrderIndex>()) };

        let mut cannonical = Vec::new();
        cannonical.extend_from_slice(id.as_bytes());
        unsafe { cannonical.push(mem::transmute(flag)) };
        cannonical.extend_from_slice(&data_bytes);
        cannonical.extend_from_slice(&num_deps);
        cannonical.extend_from_slice(&cols);
        cannonical.extend_from_slice(&lock_bytes);
        cannonical.extend_from_slice(&locs_as_bytes);
        cannonical.extend_from_slice(&deps_as_bytes);
        cannonical.extend_from_slice(&data);


        assert_eq!(&bytes[1..], &cannonical[..]);
    }


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
