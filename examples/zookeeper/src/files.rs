use std::cmp::{Eq, PartialEq};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ffi::OsString;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use message::*;
use super::WhichPath;

use order;//

#[derive(Debug)]
pub struct FileSystem {
    //FIXME PathBuf isn't cross platform
    // watches
    my_root: OsString,
    num_entries: u64,
    files: HashMap<Arc<Path>, FileNode>,
    roots: HashMap<OsString, order>,
    empty_path: Arc<Path>,
    // seen_ids: HashSet<Id>,
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
    old_path: Arc<Path>,
    new_path: Arc<Path>,
    data: Vec<u8>,
    old_exists: CommitState,
    new_empty: CommitState,
    pending_ops: VecDeque<Operation>,
    check_new: bool,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum CommitState {
    Ok,
    Abort,
    Pending,
}

impl PendingRename {
    fn new(id: Id, old_path: Arc<Path>, new_path: Arc<Path>, check_new: bool) -> Self {
        PendingRename {
            id,
            old_path,
            new_path,
            data: vec![],
            old_exists: CommitState::Pending,
            new_empty: CommitState::Pending,
            pending_ops: Default::default(),
            check_new,
        }
    }

    fn state(&self) -> CommitState {
        use self::CommitState::*;
        match (self.old_exists, self.new_empty, self.check_new) {
            (old, _, false) => unreachable!(),
            (_, Pending, true) => Pending,
            (Pending, _, _) => Pending,
            (Ok, new, true) => new,
            // (Abort, Pending, _) => Pending,
            // (Pending, Abort, _) => Pending,
            (Abort, Ok, _) => Abort,
            (Abort, Abort, _) => Abort,
            // (_, Pending, true) => Pending,
        }
    }
}

#[derive(Debug)]
enum Operation {
    Mut(Arc<Mutation>, WhichPath),
    Obs(Observation),
}

impl From<(Mutation, WhichPath)> for Operation {
    fn from((mutation, which_path): (Mutation, WhichPath)) -> Self {
        Operation::Mut(Arc::new(mutation), which_path)
    }
}

impl From<Observation> for Operation {
    fn from(observation: Observation) -> Self {
        Operation::Obs(observation)
    }
}

enum Next {
    Finish,
    Abort(Box<PendingRename>),
    Nothing,
}

/////////////////

impl FileSystem {
    pub fn new(my_root: OsString, roots: HashMap<OsString, order>) -> Self {
        let empty_path = PathBuf::new().into_boxed_path().into();
        let my_root_path: Arc<Path> = PathBuf::from(&*my_root).into_boxed_path().into();
        let mut system = FileSystem {
            my_root,
            roots,
            files: Default::default(),
            num_entries: 0,
            empty_path,
            // seen_ids: Default::default(),
        };
        let root_path: &Path = "/".as_ref();
        let root_dir: Arc<Path> = root_path.to_owned().into_boxed_path().into();
        let data: Arc<[u8]> = Arc::from(&[0u8; 0][..]);
        system.files.insert(
            root_dir.clone(),
            FileNode::new(root_dir, data.clone(), 0).into(),
        );
        system.files.insert(
            my_root_path.clone(),
            FileNode::new(my_root_path, data, 0).into(),
        );
        system
    }

    pub fn apply_mutation<CB>(
        &mut self,
        mutation: Mutation,
        which_path: WhichPath,
        mut mutation_callback: CB,
    ) where
        CB: FnMut(Id, Result<(&Arc<Path>, &Stat), u32>, Option<Mutation>, Option<Mutation>),
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
                due_to_old: bool,
            },
        }

        let mutation = {
            let mut file = self.files.get_mut(&**mutation.path()); //TODO other path?
            match file {
                None => Ok(mutation),
                Some(mut file) => match &mut file.pending_rename {
                    &mut Some(ref mut rename) => match mutation {
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
                            due_to_old,
                        } => Err(RenamePart2::Nack {
                            id,
                            part1_id,
                            old_path,
                            new_path,
                            due_to_old,
                        }),

                        op => {
                            rename.pending_ops.push_back((op, which_path).into());
                            return;
                        }
                    },
                    &mut None => Ok(mutation),
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
                assert!(res.is_ok());
                mutation_callback(id, res, None, None)
            }

            Ok(Delete { id, path, version }) => {
                let res = self.delete(path, version);
                let res = res.as_ref().map_err(|&u| u);
                let res = res.map(|path| (path, EMPTY_STAT));
                mutation_callback(id, res, None, None)
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
            })
            | Err(RenamePart2::OldExists {
                id,
                part1_id,
                create_mode,
                old_path,
                new_path,
                data,
            }) => {
                let (flush1, flush2) =
                    self.rename_old_exists(part1_id, old_path, new_path, data, which_path);
                mutation_callback(id, Ok((&self.empty_path, EMPTY_STAT)), None, None);
                if let Some(ops) = flush1 {
                    self.flush_operations(ops, &mut mutation_callback)
                }
                if let Some(ops) = flush2 {
                    self.flush_operations(ops, &mut mutation_callback)
                }
            }

            Ok(RenameNewEmpty {
                id,
                part1_id,
                old_path,
                new_path,
            })
            | Err(RenamePart2::NewEmpty {
                id,
                part1_id,
                old_path,
                new_path,
            }) => {
                let (flush1, flush2) =
                    self.rename_new_empty(part1_id, old_path, new_path, which_path);
                mutation_callback(id, Ok((&self.empty_path, EMPTY_STAT)), None, None);
                if let Some(ops) = flush1 {
                    self.flush_operations(ops, &mut mutation_callback)
                }
                if let Some(ops) = flush2 {
                    self.flush_operations(ops, &mut mutation_callback)
                }
            }

            Ok(RenameNack {
                id,
                part1_id,
                old_path,
                new_path,
                due_to_old,
            })
            | Err(RenamePart2::Nack {
                id,
                part1_id,
                old_path,
                new_path,
                due_to_old,
            }) => {
                let (flush1, flush2) =
                    self.rename_nack(part1_id, old_path, new_path, which_path, due_to_old);
                mutation_callback(id, Ok((&self.empty_path, EMPTY_STAT)), None, None);
                if let Some(ops) = flush1 {
                    self.flush_operations(ops, &mut mutation_callback)
                }
                if let Some(ops) = flush2 {
                    self.flush_operations(ops, &mut mutation_callback)
                }
            }

            Ok(RenamePart1 {
                id,
                old_path,
                new_path,
                check_new,
            }) => {
                // let new = self.seen_ids.insert(id);
                // assert!(new);
                let (res, msg0, msg1) = self.rename_part1(id, old_path, new_path, which_path, check_new);
                mutation_callback(id, res.map(|p| (p, EMPTY_STAT)), msg0, msg1)
            }
        }
    }

    fn flush_operations(
        &mut self,
        mut ops: VecDeque<Operation>,
        mutation_callback: &mut FnMut(
            Id,
            Result<(&Arc<Path>, &Stat), u32>,
            Option<Mutation>,
            Option<Mutation>,
        ),
    ) {
        use self::Operation::*;
        for op in ops.drain(..) {
            match op {
                Mut(mutation, which_path) => {
                    let mutation = match Arc::try_unwrap(mutation) {
                        Ok(mutation) => mutation,
                        Err(arc) => (&*arc).clone(),
                    };
                    self.apply_mutation(mutation, which_path, &mut *mutation_callback)
                }
                Obs(observation) => self.handle_observation(observation),
            }
        }
    }

    /////////////

    pub fn create(
        &mut self,
        create_mode: CreateMode,
        path: PathBuf,
        data: Vec<u8>,
    ) -> Result<&Arc<Path>, u32> {
        //TODO Err type
        match self.files.get_mut(&*path) {
            Some(file) => return Err(line!()),
            None => (),
        }

        let path: Arc<Path> = Arc::from(path.into_boxed_path());
        //TODO unwrap
        let path: Arc<Path> = match self.files.get_mut(path.parent().unwrap()) {
            None => return Err(line!()),
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

    fn delete(&mut self, path: PathBuf, version: Version) -> Result<Arc<Path>, u32> {
        match self.files.get_mut(path.as_path()) {
            None => return Err(line!()),
            Some(file) => {
                if version != -1 && file.stat.version != version {
                    return Err(line!());
                }
                if file.children.len() > 0 {
                    return Err(line!());
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
    ) -> Result<(&Arc<Path>, &Stat), u32> {
        match self.files.get_mut(path.as_path()) {
            None => return Err(line!()),
            Some(file) => {
                if version != -1 && file.stat.version != version {
                    return Err(line!());
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

    fn rename_part1(
        &mut self,
        id: Id,
        old_path: PathBuf,
        new_path: PathBuf,
        which_path: WhichPath,
        check_new: bool,
    ) -> (Result<&Arc<Path>, u32>, Option<Mutation>, Option<Mutation>) {
        use Mutation::{RenameNack, RenameNewEmpty, RenameOldExists};
        let old_path: Arc<Path> = old_path.into_boxed_path().into();
        let new_path: Arc<Path> = new_path.into_boxed_path().into();
        let build_nack = |due_to_old| RenameNack {
            id: Id::new(),
            part1_id: id,
            old_path: old_path.to_path_buf(),
            new_path: new_path.to_path_buf(),
            due_to_old,
        };
        let (mut msg0, mut msg1) = (None, None);
        let handle_old = old_path.starts_with(&self.my_root)
            && matches!(which_path, WhichPath::Path1 | WhichPath::Both);
        let handle_new = new_path.starts_with(&self.my_root)
            && matches!(which_path, WhichPath::Path2 | WhichPath::Both);
        // let mut ret = Err(line!());
        if handle_old {
            let mut error = false;
            msg0 = match self.files.get_mut(&*old_path) {
                //FIXME dummy node to handle nacks?
                //FIXME Err(line!()) condition
                None => {
                    error = true;
                    Some(build_nack(true))
                },
                Some(file) => {
                    if file.pending_rename.is_some() {
                        //FIXME just buffer?
                        unimplemented!()
                    }
                    file.pending_rename =
                        Some(PendingRename::new(id, old_path.clone(), new_path.clone(), check_new).into());
                    Some(RenameOldExists {
                        id: Id::new(),
                        part1_id: id,
                        create_mode: CreateMode::persistent(), //FIXME file.create_mode,
                        old_path: old_path.to_path_buf(),
                        new_path: new_path.to_path_buf(),
                        data: file.data.clone().to_vec(),
                    })
                }
            };
            if error {
                let ne = self.num_entries;
                let mut new_node = FileNode::new(
                    old_path.clone(),
                    vec![].into_boxed_slice().into(),
                    ne,
                );
                new_node.pending_rename =
                    Some(PendingRename::new(id, old_path.clone(), new_path.clone(), check_new).into());
                self.files.insert(old_path.clone(), new_node);
            }
        }
        if handle_new {
            let (add_node, nack) = match self.files.get_mut(&*new_path) {
                None => (true, false),
                Some(file) => {
                    if file.pending_rename.is_some() {
                        //FIXME just buffer?
                        if check_new {
                            unimplemented!()
                        }
                        (true, false)

                    } else {
                        file.pending_rename =
                            Some(PendingRename::new(id, old_path.clone(), new_path.clone(), check_new).into());
                        (false, true)
                    }
                }
            };
            if add_node {
                let mut new_node = FileNode::new(
                    new_path.clone(),
                    vec![].into_boxed_slice().into(),
                    self.num_entries,
                );
                self.num_entries += 1;

                new_node.pending_rename =
                    Some(PendingRename::new(id, old_path.clone(), new_path.clone(), check_new).into());
                //TODO self.triggerwatches
                &self.files.insert(new_path.clone(), new_node.into());
            }
            if nack {
                msg1 = Some(build_nack(false));
                return (Err(line!()), msg0, msg1);
            }
            //TODO unwrap
            match self.files.get(new_path.parent().unwrap()) {
                None => {
                    msg1 = Some(build_nack(false));
                    return (Err(line!()), msg0, msg1);
                }
                Some(file) => {
                    if file.ephemeral {
                        msg1 = Some(build_nack(false));
                        return (Err(line!()), msg0, msg1);
                    }
                }
            }
            if check_new {
                msg1 = Some(RenameNewEmpty {
                    id: Id::new(),
                    part1_id: id,
                    old_path: old_path.to_path_buf(),
                    new_path: new_path.to_path_buf(),
                });
            }
        }
        let path = if handle_old {
            match self.files.get(&*old_path) {
                Some(file) => Ok(&file.path),
                None => Err(line!()),
            }
        } else if let (Some(file), true) = (self.files.get(&*new_path), handle_new) {
            Ok(&file.path)
        } else if handle_new {
            Err(line!())
        } else {
            Err(line!())
        };
        return (path, msg0, msg1);
    }

    fn rename_new_empty(
        &mut self,
        id: Id,
        old_path: PathBuf,
        new_path: PathBuf,
        which_path: WhichPath,
    ) -> (Option<VecDeque<Operation>>, Option<VecDeque<Operation>>) {
        // assert!(self.seen_ids.contains(&id), "rne didn't see rename {:?}: {:?} => {:?}", id, old_path, new_path);
        // if !self.seen_ids.contains(&id) {
        //     println!("panic rne didn't see rename {:?}: {:?} => {:?}", id, old_path, new_path);
        // }
        let handle_old = old_path.starts_with(&self.my_root)
            && matches!(which_path, WhichPath::Path1 | WhichPath::Both);
        let handle_new = new_path.starts_with(&self.my_root)
            && matches!(which_path, WhichPath::Path2 | WhichPath::Both);
        let mut old_next = Next::Nothing;
        let mut new_next = Next::Nothing;
        if handle_old {
            match self.files.get_mut(&*old_path) {
                None => unreachable!(),
                Some(file) => {
                    let state = {
                        if file.pending_rename.is_none() {
                            return (None, None) //TODO
                            // panic!("no pending rename {:?} => {:?}", old_path, new_path);
                        }
                        let pending = file.pending_rename.as_mut().expect("rne second after first");
                        if id != pending.id {
                            unimplemented!() //TODO buffer op?
                        }
                        pending.new_empty = CommitState::Ok;
                        pending.state()
                    };

                    old_next = match state {
                        CommitState::Pending => Next::Nothing,
                        CommitState::Abort => Next::Abort(file.pending_rename.take().expect("rne old cannot abort no transaction")),
                        CommitState::Ok => Next::Finish,
                    }
                }
            }
        }
        if handle_new {
            match self.files.get_mut(&*new_path) {
                None => unreachable!("{:?}", &*new_path),
                Some(file) => {
                    let &mut FileNode {
                        ref mut data,
                        ref mut pending_rename,
                        ..
                    } = &mut *file;
                    let state = {
                        let pending = pending_rename.as_mut().unwrap();
                        if id != pending.id {
                            unimplemented!() //TODO buffer op?
                        }
                        pending.new_empty = CommitState::Ok;
                        pending.state()
                    };

                    new_next = match state {
                        CommitState::Pending => Next::Nothing,
                        CommitState::Abort => Next::Abort(pending_rename.take().expect("rne new cannot abort no transaction")),
                        CommitState::Ok => Next::Finish,
                    }
                }
            }
        }
        let flush0 = match old_next {
            Next::Nothing => None,
            Next::Abort(pending) => Some(self.abort(pending)),
            Next::Finish => Some(self.finish_rename_remove(old_path)),
        };
        let flush1 = match new_next {
            Next::Nothing => None,
            Next::Abort(pending) => Some(self.abort(pending)),
            Next::Finish => Some(self.finish_rename_create(new_path)),
        };
        (flush0, flush1)
    }

    fn rename_old_exists(
        &mut self,
        id: Id,
        old_path: PathBuf,
        new_path: PathBuf,
        new_data: Vec<u8>,
        which_path: WhichPath,
    ) -> (Option<VecDeque<Operation>>, Option<VecDeque<Operation>>) {
        // assert!(self.seen_ids.contains(&id), "roe didn't see rename {:?} => {:?}", old_path, new_path);
        let handle_old = old_path.starts_with(&self.my_root)
            && matches!(which_path, WhichPath::Path1 | WhichPath::Both);
        let handle_new = new_path.starts_with(&self.my_root)
            && matches!(which_path, WhichPath::Path2 | WhichPath::Both);
        let mut old_next = Next::Nothing;
        let mut new_next = Next::Nothing;
        if handle_old {
            match self.files.get_mut(&*old_path) {
                None => unreachable!(),
                Some(file) => {
                    let state = {
                        let pending = file.pending_rename.as_mut().unwrap();
                        if id != pending.id {
                            unimplemented!() //TODO buffer op?
                        }
                        pending.old_exists = CommitState::Ok;
                        pending.state()
                    };

                    old_next = match state {
                        CommitState::Pending => Next::Nothing,
                        CommitState::Abort => Next::Abort(file.pending_rename.take().unwrap()),
                        CommitState::Ok => Next::Finish,
                    }
                }
            }
        }
        if handle_new {
            match self.files.get_mut(&*new_path) {
                None => unreachable!(),
                Some(file) => {
                    let &mut FileNode {
                        ref mut data,
                        ref mut pending_rename,
                        ..
                    } = &mut *file;
                    let state = {
                        let pending = pending_rename.as_mut().unwrap();
                        if id != pending.id {
                            unimplemented!() //TODO buffer op?
                        }
                        pending.data = new_data;
                        pending.old_exists = CommitState::Ok;
                        pending.state()
                    };

                    new_next = match state {
                        CommitState::Pending => Next::Nothing,
                        CommitState::Abort => Next::Abort(pending_rename.take().unwrap()),
                        CommitState::Ok => Next::Finish,
                    }
                }
            }
        }
        let flush0 = match old_next {
            Next::Nothing => None,
            Next::Abort(pending) => Some(self.abort(pending)),
            Next::Finish => Some(self.finish_rename_remove(old_path)),
        };
        let flush1 = match new_next {
            Next::Nothing => None,
            Next::Abort(pending) => Some(self.abort(pending)),
            Next::Finish => Some(self.finish_rename_create(new_path)),
        };
        (flush0, flush1)
    }

    fn rename_nack(
        &mut self,
        id: Id,
        old_path: PathBuf,
        new_path: PathBuf,
        which_path: WhichPath,
        due_to_old: bool,
    ) -> (Option<VecDeque<Operation>>, Option<VecDeque<Operation>>) {
        // assert!(self.seen_ids.contains(&id), "rn didn't see rename {:?} => {:?}", old_path, new_path);
        let handle_old = old_path.starts_with(&self.my_root)
            && matches!(which_path, WhichPath::Path1 | WhichPath::Both);
        let handle_new = new_path.starts_with(&self.my_root)
            && matches!(which_path, WhichPath::Path2 | WhichPath::Both);
        let mut old_next = Next::Nothing;
        let mut new_next = Next::Nothing;
        if handle_old {
            match self.files.get_mut(&*old_path) {
                None => unreachable!("{:?} => {:?}, {:?}", old_path, new_path, due_to_old),
                Some(file) => {
                    let next = {
                        let pending = file.pending_rename.as_mut().unwrap();
                        if id != pending.id {
                            unimplemented!() //TODO buffer op?
                        }
                        if due_to_old {
                            pending.old_exists = CommitState::Abort;
                        } else {
                            pending.new_empty = CommitState::Abort;
                        }
                        pending.state()
                    };

                    old_next = match next {
                        CommitState::Pending => Next::Nothing,
                        CommitState::Abort => Next::Abort(file.pending_rename.take().unwrap()),
                        CommitState::Ok => unreachable!(),
                    }
                }
            }
        }
        if handle_new {
            match self.files.get_mut(&*new_path) {
                None => unreachable!(),
                Some(file) => {
                    let &mut FileNode {
                        ref mut data,
                        ref mut pending_rename,
                        ..
                    } = &mut *file;
                    let next = {
                        let pending = pending_rename.as_mut().unwrap();
                        if id != pending.id {
                            unimplemented!() //TODO buffer op?
                        }
                        if due_to_old {
                            pending.old_exists = CommitState::Abort;
                        } else {
                            pending.new_empty = CommitState::Abort;
                        }
                        pending.state()
                    };

                    new_next = match next {
                        CommitState::Pending => Next::Nothing,
                        CommitState::Abort => Next::Abort(pending_rename.take().unwrap()),
                        CommitState::Ok => unreachable!(),
                    }
                }
            }
        }
        let flush0 = match old_next {
            Next::Nothing => None,
            Next::Abort(pending) => Some(self.abort(pending)),
            Next::Finish => unreachable!(),
        };
        let flush1 = match new_next {
            Next::Nothing => None,
            Next::Abort(pending) => Some(self.abort(pending)),
            Next::Finish => unreachable!(),
        };
        (flush0, flush1)
    }

    fn abort(&mut self, rename: Box<PendingRename>) -> VecDeque<Operation> {
        //TODO remove file here?
        if rename.new_empty != CommitState::Abort {
            self.files.remove(&*rename.new_path);
        }
        if rename.old_exists == CommitState::Abort {
            self.files.remove(&*rename.old_path);
        }
        rename.pending_ops
    }

    fn finish_rename_remove(&mut self, old_path: PathBuf) -> VecDeque<Operation> {
        match self.files.get_mut(old_path.parent().unwrap()) {
            None => unreachable!(),
            Some(parent) => {
                parent.children.remove(&*old_path);
                //TODO trigger watches
            }
        }
        let mut old = self.files.remove(&*old_path).unwrap();
        old.pending_rename.take().unwrap().pending_ops
    }

    fn finish_rename_create(&mut self, new_path: PathBuf) -> VecDeque<Operation> {
        //TODO set data here?
        //TODO for all parents ensure exists
        let (path, flush) = match self.files.get_mut(&*new_path) {
            None => unreachable!(),
            Some(new_node) => {
                new_node.stat.create_time = self.num_entries;
                //TODO trigger watches
                let mut rename = new_node.pending_rename.take().unwrap();
                let new_data = ::std::mem::replace(&mut rename.data, vec![]);
                let flush = rename.pending_ops;
                // let (new_data, flush) = (rename.data, rename.pending_ops);
                new_node.data = new_data.into_boxed_slice().into();
                (new_node.path.clone(), flush)
            }
        };
        match self.files.get_mut(new_path.parent().unwrap()) {
            None => unreachable!(),
            Some(parent) => {
                parent.children.insert(path);
                //TODO trigger watches
            }
        }
        flush
    }

    ////////////

    pub fn observe(&mut self, observation: Observation) {
        //FIXME observation thread?
        let observation = {
            let mut file = self.files.get_mut(&**observation.path()); //TODO other path?
            match file {
                None => observation,
                Some(mut file) => match &mut file.pending_rename {
                    &mut Some(ref mut rename) => {
                        rename.pending_ops.push_back(observation.into());
                        return;
                    }
                    &mut None => observation,
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
                        callback(Err(line!()))
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
                        callback(Err(line!()))
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
                        callback(Err(line!()))
                    }
                    Some(file) => {
                        //TODO watches //TODO callback thread
                        callback(Ok((&*file.path, &mut file.children.iter().map(|p| &**p))))
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
        }
    }
}

fn box_path(path: &Path) -> Box<Path> {
    path.to_path_buf().into_boxed_path()
}
