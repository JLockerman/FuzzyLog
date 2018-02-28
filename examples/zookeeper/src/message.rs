use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering::Relaxed;

#[derive(Debug)]
pub enum MessageFromClient {
    Mut(Id, MutationCallback),
    Obs(Observation),
    EndOfSnapshot,
}

impl MessageFromClient {
    pub fn is_end_of_snapshot(&self) -> bool {
        if let &MessageFromClient::EndOfSnapshot = self {
            return true;
        }
        false
    }
}

pub enum MutationCallback {
    Stat(Box<for<'a, 'b> FnMut(Result<(&'a Path, &'b Stat), ()>) + Send>),
    Path(Box<for<'a, 'b> FnMut(Result<(&'a Path, &'b Path), ()>) + Send>),
    Void(Box<for<'a> FnMut(Result<&'a Path, ()>) + Send>),
    None,
}

impl fmt::Debug for MutationCallback {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use MutationCallback::*;
        match self {
            &Stat(..) => f.debug_tuple("Stat").finish(),
            &Path(..) => f.debug_tuple("Path").finish(),
            &Void(..) => f.debug_tuple("Void").finish(),
            &None => f.debug_tuple("None").finish(),
        }
    }
}

pub type ClientId = u64;
pub type Count = u64;
pub type Version = i64;

pub static CLIENT_ID: AtomicUsize = ATOMIC_USIZE_INIT;
static COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;

pub fn client_id() -> u64 {
    CLIENT_ID.load(Relaxed) as u64
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Id {
    pub client: ClientId,
    pub count: Count,
}

impl Id {
    pub fn new() -> Self {
        Id {
            client: client_id(),
            count: COUNTER.fetch_add(1, Relaxed) as u64,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Mutation {
    Create {
        id: Id,
        create_mode: CreateMode,
        path: PathBuf,
        data: Vec<u8>,
        //TODO acl: Vec<ACL>,
    },

    Delete {
        id: Id,
        path: PathBuf,
        version: Version,
    },

    Set {
        id: Id,
        path: PathBuf,
        data: Vec<u8>,
        version: Version,
    },

    RenamePart1 {
        id: Id,
        old_path: PathBuf,
        new_path: PathBuf,
    },

    RenameOldExists {
        id: Id,
        part1_id: Id,
        create_mode: CreateMode,
        old_path: PathBuf,
        new_path: PathBuf,
        data: Vec<u8>,
        //TODO acl: Vec<ACL>,
    },

    RenameNewEmpty {
        id: Id,
        part1_id: Id,
        old_path: PathBuf,
        new_path: PathBuf,
    },

    RenameNack {
        id: Id,
        part1_id: Id,
        old_path: PathBuf,
        new_path: PathBuf,
    },
}

impl Mutation {
    pub fn id(&self) -> &Id {
        use self::Mutation::*;
        match self {
            &Create { ref id, .. }
            | &Delete { ref id, .. }
            | &Set { ref id, .. }
            | &RenamePart1 { ref id, .. }
            | &RenameOldExists { ref id, .. }
            | &RenameNewEmpty { ref id, .. }
            | &RenameNack { ref id, .. } => id,
        }
    }

    pub fn path(&self) -> &PathBuf {
        use self::Mutation::*;
        match self {
            &Create { ref path, .. } | &Delete { ref path, .. } | &Set { ref path, .. } => &path,
            &RenamePart1 { ref old_path, .. }
            | &RenameOldExists { ref old_path, .. }
            | &RenameNewEmpty { ref old_path, .. }
            | &RenameNack { ref old_path, .. } => &old_path,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
pub struct CreateMode(u32);

impl CreateMode {
    pub fn ephemeral() -> Self {
        CreateMode(2)
    }
    pub fn ephemeral_sequential() -> Self {
        CreateMode(3)
    }
    pub fn persistent() -> Self {
        CreateMode(4)
    }
    pub fn persistent_sequential() -> Self {
        CreateMode(5)
    }

    pub fn is_ephemeral(&self) -> bool {
        self.0 <= 3
    }
    pub fn is_sequential(&self) -> bool {
        self.0 & 1 == 1
    }
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
pub struct ACL;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct Stat {
    pub version: Version,
    pub create_time: u64,
    pub mutate_time: u64,
}

impl Stat {
    pub fn new(create_time: u64) -> Self {
        Stat {
            create_time,
            version: 0,
            mutate_time: 0,
        }
    }
}

pub enum Observation {
    Exists {
        id: Id,
        path: PathBuf,
        watch: bool,
        callback: Box<FnMut(Result<(&Path, &Stat), ()>) + Send>,
    },

    GetData {
        id: Id,
        path: PathBuf,
        watch: bool,
        callback: Box<FnMut(Result<(&Path, &[u8], &Stat), ()>) + Send>,
    },

    GetChildren {
        id: Id,
        path: PathBuf,
        watch: bool,
        callback: Box<FnMut(Result<(&Path, &Iterator<Item = &Path>), ()>) + Send>,
    },
}

impl Observation {
    pub fn id(&self) -> &Id {
        use self::Observation::*;
        match self {
            &Exists { ref id, .. } | &GetData { ref id, .. } | &GetChildren { ref id, .. } => id,
        }
    }

    pub fn path(&self) -> &PathBuf {
        use self::Observation::*;
        match self {
            &Exists { ref path, .. }
            | &GetData { ref path, .. }
            | &GetChildren { ref path, .. } => &path,
        }
    }
}

impl fmt::Debug for Observation {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use Observation::*;
        match self {
            &Exists {
                ref id,
                ref path,
                ref watch,
                ..
            } => f.debug_struct("Exists")
                .field("id", id)
                .field("path", path)
                .field("watch", watch)
                .finish(),

            &GetData {
                ref id,
                ref path,
                ref watch,
                ..
            } => f.debug_struct("GetData")
                .field("id", id)
                .field("path", path)
                .field("watch", watch)
                .finish(),

            &GetChildren {
                ref id,
                ref path,
                ref watch,
                ..
            } => f.debug_struct("GetChildren")
                .field("id", id)
                .field("path", path)
                .field("watch", watch)
                .finish(),
        }
    }
}
