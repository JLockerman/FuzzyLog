
use std::borrow::Cow;

use zookeeper::{CreateMode, Version, Stat};

pub type Id = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum Request<'s> {
    Create {
        id: Id,
        path: Cow<'s, str>,
        data: Vec<u8>,
        create_mode: CreateMode,
    },

    Delete {
        id: Id,
        path: Cow<'s, str>,
        version: Version,
    },

    SetData {
        id: Id,
        path: Cow<'s, str>,
        data: Vec<u8>,
        version: Version,
    },

    // Rename {
    //     oldPath: Cow<'s, str>,
    //     newPath: Cow<'s, str>,
    // },

    Exists(Id, Cow<'s, str>),
    GetData(Id, Cow<'s, str>),
    GetChildren(Id, Cow<'s, str>),
    Done,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Response {
    Ok(Id, String, Vec<u8>, Stat, Vec<String>),
    Err,
}
