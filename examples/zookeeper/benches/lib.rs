#![feature(test)]

extern crate test;

extern crate zookeeper;

use std::collections::HashMap;

use test::{black_box, Bencher};

use zookeeper::files::FileSystem;
use zookeeper::WhichPath;
use zookeeper::message::{self, CreateMode, Id};
use zookeeper::message::Mutation::*;

#[bench]
pub fn create(b: &mut Bencher) {
    message::set_client_id(101);
    let my_root = "/abcd/".into();
    let mut roots = HashMap::new();
    roots.insert("abcd".into(), 101.into());
    let mut files = FileSystem::new(my_root, roots);
    let mut i = 0u64;
    b.iter(|| {
        files.apply_mutation(
            Create {
                id: Id::new(),
                create_mode: CreateMode::persistent(),
                path: format!("/abcd/{}", i).into(),
                data: vec![0, 1, 2, i as u8],
            },
            WhichPath::Path1,
            |id, r, m0, m1| {
                black_box((id, r, m0, m1));
                i += 1
            },
        );
    })
}

#[bench]
pub fn create_rename(b: &mut Bencher) {
    let my_root = "/abcd/".into();
    let mut roots = HashMap::new();
    roots.insert("abcd".into(), 101.into());
    let mut files = FileSystem::new(my_root, roots);
    let mut i = 0u64;
    b.iter(|| {
        files.apply_mutation(
            Create {
                id: Id::new(),
                create_mode: CreateMode::persistent(),
                path: format!("/abcd/{}", i).into(),
                data: vec![0, 1, 2, i as u8],
            },
            WhichPath::Path1,
            |id, r, m0, m1| {
                black_box((id, r, m0, m1));
            },
        );
        let rename_id = Id::new();
        files.apply_mutation(
            RenamePart1 {
                id: rename_id,
                old_path: format!("/abcd/{}", i).into(),
                new_path: format!("/abcd/{}_{}", i, i).into(),
            },
            WhichPath::Both,
            |id, r, m0, m1| {
                black_box((id, r, m0, m1));
            },
        );
        files.apply_mutation(
            RenameNewEmpty {
                id: Id::new(),
                part1_id: rename_id,
                old_path: format!("/abcd/{}", i).into(),
                new_path: format!("/abcd/{}_{}", i, i).into(),
            },
            WhichPath::Both,
            |id, r, m0, m1| {
                black_box((id, r, m0, m1));
            },
        );
        files.apply_mutation(
            RenameOldExists {
                id: Id::new(),
                create_mode: CreateMode::persistent(),
                data: vec![0, 1, 2, i as u8],
                part1_id: rename_id,
                old_path: format!("/abcd/{}", i).into(),
                new_path: format!("/abcd/{}_{}", i, i).into(),
            },
            WhichPath::Both,
            |id, r, m0, m1| {
                black_box((id, r, m0, m1));
            },
        );
        i += 1;
    })
}
