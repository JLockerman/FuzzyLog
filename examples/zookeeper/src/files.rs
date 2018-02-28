use std::cmp::{Eq, PartialEq};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsString;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use message::*;

use order;

#[derive(Debug)]
pub struct FileSystem {
    //FIXME PathBuf isn't cross platform
    files: HashMap<Arc<Path>, Box<FileNode>>,
    // watches
    my_root: OsString,
    roots: HashMap<OsString, order>,
    num_entries: u64,
}

#[derive(Debug)]
struct FileNode {
    path: Arc<Path>,
    stat: Stat,
    data: Arc<[u8]>,
    ephemeral: bool,
    children: HashSet<Arc<Path>>,
    sequential_counters: HashMap<Arc<Path>, i64>,
    pending_rename: Option<Box<PendingRename>>,
    pending_ops: VecDeque<Mutation>,
}

impl Hash for FileNode {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        (&*self.path).hash(state);
    }
}

impl PartialEq for FileNode {
    fn eq(&self, other: &Self) -> bool {
        &*self.path == &*other.path
    }
}

impl Eq for FileNode {}

/////////////////

#[derive(Debug)]
struct PendingRename {
    id: Id,
    old_path: Arc<PathBuf>,
    new_path: Arc<PathBuf>,
    old_exists: bool,
    new_empty: bool,
    nacked: bool,
}

/////////////////

impl FileSystem {
    pub fn new(my_root: OsString, roots: HashMap<OsString, order>) -> Self {
        let mut system = FileSystem {
            my_root,
            roots,
            files: Default::default(),
            num_entries: 0,
        };
        let root_path: &Path = "/".as_ref();
        let root_dir: Arc<Path> = root_path.to_owned().into_boxed_path().into();
        let data = Arc::from(&[0u8; 0][..]);
        system
            .files
            .insert(root_dir.clone(), FileNode::new(root_dir, data, 0).into());
        system
    }

    pub fn apply_mutation<CB>(&mut self, mutation: Mutation, mut mutation_callback: CB)
    where
        CB: FnMut(
            Id,
            Result<(&Arc<Path>, &Stat), ()>,
            Option<(order, Mutation)>,
            Option<(order, Mutation)>,
        ),
    {
        use Mutation::*;

        static EMPTY_STAT: &Stat = &Stat {
            version: 0,
            create_time: 0,
            mutate_time: 0,
        };

        enum RenamePart2 {
            OldExists {
                id: Id,
                part1_id: Id,
                create_mode: CreateMode,
                old_path: PathBuf,
                new_path: PathBuf,
                data: Vec<u8>,
            },
            NewEmpty {
                id: Id,
                part1_id: Id,
                old_path: PathBuf,
                new_path: PathBuf,
            },
            Nack {
                id: Id,
                part1_id: Id,
                old_path: PathBuf,
                new_path: PathBuf,
            },
        }

        let mutation = {
            let mut file = self.files.get_mut(&**mutation.path()); //TODO other path?
            match file {
                None => Ok(mutation),
                Some(mut file) => if file.pending_rename.is_some() {
                    match mutation {
                        RenameOldExists {
                            id,
                            part1_id,
                            create_mode,
                            old_path,
                            new_path,
                            data,
                        } => Err(RenamePart2::OldExists {
                            id,
                            part1_id,
                            create_mode,
                            old_path,
                            new_path,
                            data,
                        }),

                        RenameNewEmpty {
                            id,
                            part1_id,
                            old_path,
                            new_path,
                        } => Err(RenamePart2::NewEmpty {
                            id,
                            part1_id,
                            old_path,
                            new_path,
                        }),

                        RenameNack {
                            id,
                            part1_id,
                            old_path,
                            new_path,
                        } => Err(RenamePart2::Nack {
                            id,
                            part1_id,
                            old_path,
                            new_path,
                        }),

                        op => {
                            file.pending_ops.push_back(op);
                            return;
                        }
                    }
                } else {
                    Ok(mutation)
                },
            }
        };

        match mutation {
            Ok(Create {
                id,
                create_mode,
                path,
                data,
            }) => {
                let res = self.create(create_mode, path, data);
                let res = res.map(|path| (path, EMPTY_STAT));
                mutation_callback(id, res, None, None)
            }

            Ok(Delete { id, path, version }) => {
                let res = self.delete(path, version);
                let res = res.as_ref().map_err(|_| ());
                let res = res.map(|path| (path, EMPTY_STAT));
                mutation_callback(id, Err(()), None, None)
            }

            Ok(Set {
                id,
                path,
                data,
                version,
            }) => {
                let res = self.set(path, version, data.into_boxed_slice());
                mutation_callback(id, res, None, None)
            }

            Ok(RenameOldExists {
                id,
                part1_id,
                create_mode,
                old_path,
                new_path,
                data,
            }) => mutation_callback(id, Err(()), None, None),

            Ok(RenameNewEmpty {
                id,
                part1_id,
                old_path,
                new_path,
            }) => mutation_callback(id, Err(()), None, None),

            Ok(RenameNack {
                id,
                part1_id,
                old_path,
                new_path,
            }) => mutation_callback(id, Err(()), None, None),

            Ok(RenamePart1 {
                id,
                old_path,
                new_path,
            }) => {
                //FIXME
                let msg0 = match old_path.starts_with(&*self.my_root) {
                    false => None,
                    true => {
                        let root = old_path.components().skip(1).next().unwrap();
                        let chain = self.roots[root.as_ref()];
                        Some((
                            chain,
                            RenameOldExists {
                                id: Id::new(),
                                part1_id: id,
                                old_path: old_path.clone(),
                                new_path: new_path.clone(),
                                create_mode: CreateMode::persistent(),
                                data: vec![],
                            },
                        ))
                    }
                };

                //FIXME
                let msg1 = match new_path.starts_with(&*self.my_root) {
                    false => None,
                    true => {
                        let chain = {
                            let root = new_path.components().skip(1).next().unwrap();
                            self.roots[root.as_ref()]
                        };
                        Some((
                            chain,
                            RenameNewEmpty {
                                id: Id::new(),
                                part1_id: id,
                                old_path,
                                new_path,
                            },
                        ))
                    }
                };
                mutation_callback(id, Err(()), msg0, msg1)
            }
            Err(..) => unimplemented!(),
        }
    }

    fn create(
        &mut self,
        create_mode: CreateMode,
        path: PathBuf,
        data: Vec<u8>,
    ) -> Result<&Arc<Path>, ()> {
        //TODO Err type
        match self.files.get_mut(&*path) {
            Some(..) => return Err(()),
            None => (),
        }

        let path: Arc<Path> = Arc::from(path.into_boxed_path());
        //TODO unwrap
        let path: Arc<Path> = match self.files.get_mut(path.parent().unwrap()) {
            None => return Err(()),
            Some(parent) => {
                let actual_path: Arc<Path> = if create_mode.is_sequential() {
                    let counter = parent.sequential_counters.entry(path.clone()).or_insert(0);
                    let count = *counter;
                    *counter += 1;
                    //FIXME this isn't a great way to do multiversion
                    //      doesn't work with version checks in delete / set
                    let real_path = format!("{}{}", path.to_string_lossy(), count);
                    //FIXME why doesn't From work directly?
                    Arc::from(Box::<Path>::from(Path::new(&real_path)))
                } else {
                    path.clone()
                };
                parent.children.insert(actual_path.clone());
                actual_path
            }
        };

        let new_node = FileNode::new(
            path.clone(),
            data.into_boxed_slice().into(),
            self.num_entries,
        );
        self.num_entries += 1;

        //TODO self.triggerwatches
        let path = &self.files.entry(path).or_insert(new_node.into()).path;
        return Ok(path);
    }

    fn delete(&mut self, path: PathBuf, version: Version) -> Result<Arc<Path>, ()> {
        match self.files.get_mut(path.as_path()) {
            None => return Err(()),
            Some(file) => {
                if version != -1 && file.stat.version != version {
                    return Err(());
                }
                if file.children.len() > 0 {
                    return Err(());
                }
            }
        }

        {
            let parent = self.files.get_mut(path.parent().unwrap());
            parent.unwrap().children.remove(path.as_path());
            //TODO triggerwatches
        }

        let path = self.files.remove(path.as_path()).unwrap().path;
        Ok(path)
    }

    fn set(
        &mut self,
        path: PathBuf,
        version: Version,
        data: Box<[u8]>,
    ) -> Result<(&Arc<Path>, &Stat), ()> {
        match self.files.get_mut(path.as_path()) {
            None => return Err(()),
            Some(file) => {
                if version != -1 && file.stat.version != version {
                    return Err(());
                }
                file.data = data.into();
                file.stat.mutate_time = self.num_entries;
                self.num_entries += 1;
                //TODO triggerwatches
                return Ok((&file.path, &file.stat));
            }
        }
    }

    ////////////

    pub fn observe(&mut self, observation: Observation) {
        //FIXME observation thread?
        let observation = {
            let mut file = self.files.get_mut(&**observation.path()); //TODO other path?
            match file {
                None => observation,
                Some(mut file) => if file.pending_rename.is_some() {
                    //FIXME pending ops
                    unimplemented!()
                } else {
                    observation
                },
            }
        };

        self.handle_observation(observation)
    }

    fn handle_observation(&mut self, observation: Observation) {
        use Observation::*;
        match observation {
            Exists {
                id,
                path,
                watch,
                mut callback,
            } => {
                match self.files.get_mut(&*path) {
                    None => {
                        //TODO watches
                        callback(Err(()))
                    }
                    Some(file) => {
                        //TODO watches //TODO callback thread
                        callback(Ok((&*file.path, &file.stat)))
                    }
                }
            }

            GetData {
                id,
                path,
                watch,
                mut callback,
            } => {
                match self.files.get_mut(&*path) {
                    None => {
                        //TODO watches
                        callback(Err(()))
                    }
                    Some(file) => {
                        //TODO watches //TODO callback thread
                        callback(Ok((&*file.path, &file.data, &file.stat)))
                    }
                }
            }

            GetChildren {
                id,
                path,
                watch,
                mut callback,
            } => {
                match self.files.get_mut(&*path) {
                    None => {
                        //TODO watches
                        callback(Err(()))
                    }
                    Some(file) => {
                        //TODO watches //TODO callback thread
                        callback(Ok((&*file.path, &file.children.iter().map(|p| &**p))))
                    }
                }
            }
        }
    }
}

impl FileNode {
    fn new(path: Arc<Path>, data: Arc<[u8]>, create_time: u64) -> Self {
        FileNode {
            path,
            stat: Stat::new(create_time),
            data,
            ephemeral: false, //TODO
            children: Default::default(),
            sequential_counters: Default::default(),
            pending_rename: None,
            pending_ops: Default::default(),
        }
    }
}
