
extern crate env_logger;

use super::*;
use packets::*;

fn new_log() -> ServerLog<(), VecDeque<ToWorker<()>>> {
    let (store, _reader) = ::new_chain_store_and_reader();
    ServerLog::new(0, 2, Default::default(), store)
}

fn read_from_log(
    server: &ServerLog<(), VecDeque<ToWorker<()>>>,
    loc: OrderIndex,
    f: &mut for<'a> FnMut(Result<&'a [u8], EntryContents<'a>>) -> ()
) {
    let mut buffer = Buffer::empty();
    buffer.fill_from_entry_contents(EntryContents::read(&loc));

    worker_thread::handle_read(&*server.log, &buffer, 0, f)
}

fn multi_append_buffer(id: &Uuid, locs: &[OrderIndex], multi_server: bool) -> Buffer {
    let mut buffer = Buffer::empty();
    buffer.fill_from_entry_contents(EntryContents::Multi {
        id: id,
        flags: &if multi_server { (EntryFlag::TakeLock | EntryFlag::NewMultiPut) }
            else { EntryFlag::NewMultiPut },
        locs: locs,
        lock: &1,
        deps: &[],
        data: &[],
    });
    buffer
}

fn snapshot_buffer(locs: &[OrderIndex], multi_server: bool) -> Buffer {
    let mut buffer = Buffer::empty();
    buffer.fill_from_entry_contents(EntryContents::Snapshot {
        id: &Uuid::nil(),
        flags: &if multi_server { (EntryFlag::TakeLock | EntryFlag::NewMultiPut) }
            else { EntryFlag::NewMultiPut },
        data_bytes: &0,
        num_deps: &0,
        locs: locs,
        lock: &1,
    });
    buffer
}

fn make_storage(buffer: &Buffer) -> SkeensMultiStorage {
    SkeensMultiStorage::new(
        buffer.contents().locs().len(),
        buffer.entry_size(),
        if buffer.contents().locs().contains(&OrderIndex(0.into(), 0.into()))
            || !buffer.contents().flag().contains(EntryFlag::TakeLock) {
            Some(buffer.entry_size())
        } else {
            None
        },
    )
}

fn skeens2_buffer(id: &Uuid, locs: &[OrderIndex], max_ts: u64) -> Buffer {
    let mut buffer = Buffer::empty();
    buffer.fill_from_entry_contents(EntryContents::Multi {
        id: &id,
        flags: &(EntryFlag::NewMultiPut | EntryFlag::TakeLock | EntryFlag::Unlock),
        locs: &locs,
        lock: &max_ts,
        deps: &[],
        data: &[],
    });
    buffer
}

fn singe_append_buffer(id: &Uuid, loc: order) -> Buffer {
    let mut buffer = Buffer::empty();
    buffer.fill_from_entry_contents(EntryContents::Single {
        id: id,
        flags: &EntryFlag::Nothing,
        loc: &OrderIndex(loc, 0.into()),
        deps: &[],
        data: &[],
        timestamp: &1,
    });
    buffer
}

fn handle_op(
    server: &mut ServerLog<(), VecDeque<ToWorker<()>>>,
    buffer: BufferSlice,
    storage: Troption<SkeensMultiStorage, Box<(RcSlice, RcSlice)>>,
) -> Option<BufferSlice> {
    server.handle_op(buffer, storage, ());
    let mut buffer = None;
    while let Some(msg) = server.to_workers.pop_front() {
        let (b, u) = handle_to_worker2(msg, 0, false, |_, _, _| {});
        if let Some(b) = b {
            buffer = Some(b)
        }
    }
    buffer
}

#[test]
fn read_empty() {
    let mut server = new_log();

    read_from_log(&server, OrderIndex(1.into(), 1.into()), &mut |res| {
        match res {
            Ok(bytes) => panic!("Read empty @ {:#?}", unsafe { EntryContents::try_ref(bytes)} ),
            Err(EntryContents::Read{ loc, horizon, min, ..}) => {
                assert_eq!(loc, &OrderIndex(1.into(), 1.into()));
                assert_eq!(horizon, &OrderIndex(1.into(), 0.into()));
                assert_eq!(min, &OrderIndex(1.into(), 0.into()));
            },
            Err(e) => panic!("bad return {:#?}", e),
        }
    })
}

#[test]
fn single_append() {
    let _ = env_logger::init();
    let mut server = new_log();
    let mut buffer = Buffer::empty();
    let wid = Uuid::new_v4();
    buffer.fill_from_entry_contents(EntryContents::Single {
        id: &wid,
        flags: &EntryFlag::Nothing,
        loc: &OrderIndex(2.into(), 0.into()),
        deps: &[],
        data: &[],
        timestamp: &1,
    });
    buffer = handle_op(&mut server, buffer, Troption::None).unwrap();

    read_from_log(&server, OrderIndex(2.into(), 1.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Single{ id, flags, loc, deps, data, timestamp } => {
                        assert_eq!(id, &wid);
                        assert_eq!(flags, &EntryFlag::ReadSuccess);
                        assert_eq!(loc, &OrderIndex(2.into(), 1.into()));
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                        assert_eq!(timestamp, &1);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });
}

#[test]
fn single_append2() {
    let _ = env_logger::init();
    let mut server = new_log();
    let mut buffer = Buffer::empty();
    let wid = Uuid::new_v4();
    buffer.fill_from_entry_contents(EntryContents::Single {
        id: &wid,
        flags: &EntryFlag::Nothing,
        loc: &OrderIndex(2.into(), 0.into()),
        deps: &[],
        data: &[],
        timestamp: &7,
    });
    buffer = handle_op(&mut server, buffer, Troption::None).unwrap();

    read_from_log(&server, OrderIndex(2.into(), 1.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Single{ id, flags, loc, deps, data, timestamp } => {
                        assert_eq!(id, &wid);
                        assert_eq!(flags, &EntryFlag::ReadSuccess);
                        assert_eq!(loc, &OrderIndex(2.into(), 1.into()));
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                        assert_eq!(timestamp, &7);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });
}

#[test]
fn snapshot() {
    let _ = env_logger::init();
    let mut server = new_log();


    let mut buffer = snapshot_buffer(
        &[OrderIndex(2.into(), 0.into()), OrderIndex(4.into(), 0.into())],
        false,
    );

    buffer = handle_op(&mut server, buffer, Troption::None).unwrap();
    assert_eq!(
        buffer.contents().locs(),
        &[OrderIndex(2.into(), 0.into()), OrderIndex(4.into(), 0.into())]
    );

    let wid = Uuid::new_v4();
    buffer.fill_from_entry_contents(EntryContents::Single {
        id: &wid,
        flags: &EntryFlag::Nothing,
        loc: &OrderIndex(2.into(), 0.into()),
        deps: &[],
        data: &[],
        timestamp: &1
    });
    buffer = handle_op(&mut server, buffer, Troption::None).unwrap();

    let wid = Uuid::new_v4();
    buffer.fill_from_entry_contents(EntryContents::Single {
        id: &wid,
        flags: &EntryFlag::Nothing,
        loc: &OrderIndex(4.into(), 0.into()),
        deps: &[],
        data: &[],
        timestamp: &1
    });
    buffer = handle_op(&mut server, buffer, Troption::None).unwrap();

    buffer = snapshot_buffer(
        &[OrderIndex(2.into(), 0.into()), OrderIndex(4.into(), 0.into())],
        false,
    );

    buffer = handle_op(&mut server, buffer, Troption::None).unwrap();
    assert_eq!(
        buffer.contents().locs(),
        &[OrderIndex(2.into(), 1.into()), OrderIndex(4.into(), 1.into())]
    );
}

#[test]
fn multi_append() {
    let _ = env_logger::init();
    let mut server = new_log();
    let wid = Uuid::new_v4();
    let mut buffer = multi_append_buffer(
        &wid,
        &[OrderIndex(2.into(), 0.into()), OrderIndex(4.into(), 0.into())],
        false,
    );
    let storage = make_storage(&buffer);
    buffer = handle_op(&mut server, buffer, Troption::Left(storage)).unwrap();

    read_from_log(&server, OrderIndex(2.into(), 1.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Multi{ id, flags, locs, deps, data, lock } => {
                        let _ = lock;
                        assert_eq!(id, &wid);
                        assert_eq!(flags, &(EntryFlag::NewMultiPut | EntryFlag::ReadSuccess));
                        assert_eq!(
                            locs,
                            &[OrderIndex(2.into(), 1.into()), OrderIndex(4.into(), 1.into())]
                        );
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });

    read_from_log(&server, OrderIndex(4.into(), 1.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Multi{ id, flags, locs, deps, data, lock } => {
                        let _ = lock;
                        assert_eq!(id, &wid);
                        assert_eq!(flags, &(EntryFlag::NewMultiPut | EntryFlag::ReadSuccess));
                        assert_eq!(
                            locs,
                            &[OrderIndex(2.into(), 1.into()), OrderIndex(4.into(), 1.into())]
                        );
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });
}

//FIXME when depending on the an empty chain, add a sentinel
#[test]
fn dependent_multi_append() {
    let _ = env_logger::init();
    let mut server = new_log();
    // let mut buffer = Buffer::empty();
    let wid = Uuid::new_v4();
    let mut buffer = multi_append_buffer(
        &wid,
        &[OrderIndex(2.into(), 0.into()),
            OrderIndex(0.into(), 0.into()),
            OrderIndex(4.into(), 0.into())],
        false,
    );
    let storage = make_storage(&buffer);
    buffer = handle_op(&mut server, buffer, Troption::Left(storage)).unwrap();

    read_from_log(&server, OrderIndex(2.into(), 1.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Multi{ id, flags, locs, deps, data, lock } => {
                        let _ = lock;
                        assert_eq!(id, &wid);
                        assert_eq!(flags, &(EntryFlag::NewMultiPut | EntryFlag::ReadSuccess));
                        assert_eq!(
                            locs,
                            &[OrderIndex(2.into(), 1.into()), OrderIndex(0.into(), 0.into()), OrderIndex(4.into(), 1.into())]
                        );
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });
}

#[test]
fn dependent_multi_append2() {
    let _ = env_logger::init();
    let mut server = new_log();
    let wid = Uuid::new_v4();
    let buffer = singe_append_buffer(&Uuid::nil(), 4.into());
    handle_op(&mut server, buffer, Troption::None).unwrap();
    let mut buffer = multi_append_buffer(
        &wid,
        &[OrderIndex(2.into(), 0.into()),
            OrderIndex(0.into(), 0.into()),
            OrderIndex(4.into(), 0.into())],
        false,
    );
    let storage = make_storage(&buffer);
    buffer = handle_op(&mut server, buffer, Troption::Left(storage)).unwrap();

    read_from_log(&server, OrderIndex(2.into(), 1.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Multi{ id, flags, locs, deps, data, lock } => {
                        let _ = lock;
                        assert_eq!(id, &wid);
                        assert_eq!(flags, &(EntryFlag::NewMultiPut | EntryFlag::ReadSuccess));
                        assert_eq!(
                            locs,
                            &[OrderIndex(2.into(), 1.into()), OrderIndex(0.into(), 0.into()), OrderIndex(4.into(), 1.into())]
                        );
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });

    read_from_log(&server, OrderIndex(4.into(), 1.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Single{ flags, loc, deps, data, ..} => {
                        assert_eq!(flags, &EntryFlag::ReadSuccess);
                        assert_eq!(loc, &OrderIndex(4.into(), 1.into()));
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });

    read_from_log(&server, OrderIndex(4.into(), 2.into()), &mut |res| {
        match res {
            Ok(bytes) => panic!("Read empty @ {:#?}", unsafe { EntryContents::try_ref(bytes)} ),
            Err(EntryContents::Read{ loc, horizon, min, ..}) => {
                assert_eq!(loc, &OrderIndex(4.into(), 2.into()));
                assert_eq!(horizon, &OrderIndex(4.into(), 1.into()));
                assert_eq!(min, &OrderIndex(4.into(), 0.into()));
            },
            Err(e) => panic!("bad return {:#?}", e),
        }
    })
}

#[test]
fn skeens_multi_append() {
    let _ = env_logger::init();
    let mut server = new_log();
    let wid = Uuid::new_v4();
    let locs = &[OrderIndex(2.into(), 0.into()), OrderIndex(3.into(), 0.into())];
    let buffer = multi_append_buffer(&wid, locs, true);
    let storage = make_storage(&buffer);
    handle_op(&mut server, buffer, Troption::Left(storage)).unwrap();
    let buffer = skeens2_buffer(&wid, locs, 1);
    handle_op(&mut server, buffer, Troption::None).unwrap();

    read_from_log(&server, OrderIndex(2.into(), 1.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Multi{ id, flags, locs, deps, data, lock } => {
                        assert_eq!(id, &wid);
                        assert_eq!(flags,
                            &(EntryFlag::NewMultiPut
                                | EntryFlag::ReadSuccess
                                | EntryFlag::TakeLock));
                        assert_eq!(
                            locs,
                            &[OrderIndex(2.into(), 1.into()), OrderIndex(3.into(), 0.into())]
                        );
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                        assert_eq!(lock, &1);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });
}

#[test]
fn single_after_skeens() {
    let _ = env_logger::init();
    let mut server = new_log();
    let wid = Uuid::new_v4();
    let locs = &[OrderIndex(2.into(), 0.into()), OrderIndex(3.into(), 0.into())];
    let buffer = multi_append_buffer(&wid, locs, true);
    let storage = make_storage(&buffer);
    handle_op(&mut server, buffer, Troption::Left(storage)).unwrap();
    let buffer = skeens2_buffer(&wid, locs, 1);
    handle_op(&mut server, buffer, Troption::None).unwrap();

    read_from_log(&server, OrderIndex(2.into(), 1.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Multi{ id, flags, locs, deps, data, lock } => {
                        let _ = lock;
                        assert_eq!(id, &wid);
                        assert_eq!(flags,
                            &(EntryFlag::NewMultiPut
                                | EntryFlag::ReadSuccess
                                | EntryFlag::TakeLock));
                        assert_eq!(
                            locs,
                            &[OrderIndex(2.into(), 1.into()), OrderIndex(3.into(), 0.into())]
                        );
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                        assert_eq!(lock, &1);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });

    let mut buffer = Buffer::empty();
    let wid = Uuid::new_v4();
    buffer.fill_from_entry_contents(EntryContents::Single {
        id: &wid,
        flags: &EntryFlag::Nothing,
        loc: &OrderIndex(2.into(), 0.into()),
        deps: &[],
        data: &[],
        timestamp: &1,
    });
    buffer = handle_op(&mut server, buffer, Troption::None).unwrap();

    read_from_log(&server, OrderIndex(2.into(), 2.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Single{ id, flags, loc, deps, data, timestamp } => {
                        assert_eq!(id, &wid);
                        assert_eq!(flags, &EntryFlag::ReadSuccess);
                        assert_eq!(loc, &OrderIndex(2.into(), 2.into()));
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                        assert_eq!(timestamp, &1);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });
}

#[test]
fn single_after_partial_skeens() {
    let _ = env_logger::init();
    let mut server = new_log();
    let wid = Uuid::new_v4();
    let locs = &[OrderIndex(2.into(), 0.into()), OrderIndex(3.into(), 0.into())];
    let buffer = multi_append_buffer(&wid, locs, true);
    let storage = make_storage(&buffer);
    handle_op(&mut server, buffer, Troption::Left(storage)).unwrap();

    let mut buffer = Buffer::empty();
    let sid = Uuid::new_v4();
    buffer.fill_from_entry_contents(EntryContents::Single {
        id: &sid,
        flags: &EntryFlag::Nothing,
        loc: &OrderIndex(2.into(), 0.into()),
        deps: &[],
        data: &[],
        timestamp: &1,
    });
    buffer = handle_op(&mut server, buffer, Troption::None).unwrap();

    let buffer = skeens2_buffer(&wid, locs, 1);
    handle_op(&mut server, buffer, Troption::None).unwrap();

    read_from_log(&server, OrderIndex(2.into(), 1.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Multi{ id, flags, locs, deps, data, lock } => {
                        let _ = lock;
                        assert_eq!(id, &wid);
                        assert_eq!(flags,
                            &(EntryFlag::NewMultiPut
                                | EntryFlag::ReadSuccess
                                | EntryFlag::TakeLock));
                        assert_eq!(
                            locs,
                            &[OrderIndex(2.into(), 1.into()), OrderIndex(3.into(), 0.into())]
                        );
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                        assert_eq!(lock, &1);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });

    read_from_log(&server, OrderIndex(2.into(), 2.into()), &mut |res| {
        match res {
            Err(e) => panic!("bad return {:#?}", e),
            Ok(bytes) => unsafe {
                let (e, _) = EntryContents::try_ref(bytes).unwrap();
                match e {
                    EntryContents::Single{ id, flags, loc, deps, data, timestamp } => {
                        assert_eq!(id, &sid);
                        assert_eq!(flags, &EntryFlag::ReadSuccess);
                        assert_eq!(loc, &OrderIndex(2.into(), 2.into()));
                        assert_eq!(deps, &[]);
                        assert_eq!(data, &[0u8; 0]);
                        assert_eq!(timestamp, &1);
                    }
                    e => panic!("wrong read {:#?}", e)
                }
            },
        }
    });
}
