
use prelude::*;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use linked_hash_map::LinkedHashMap;

//TODO there is exactly one chain per colour.
#[allow(non_camel_case_types)]
pub type color = u32;

//TODO is the 'static really necessary?
pub struct DAGHandle<V: ?Sized + 'static, S, H>
where V: Storeable, S: Store<V>, H: Horizon {
    log: FuzzyLog<V,S,H>,
    //TODO I currently return multiappends multiple times, once for each color
    //     I believe I can coalece them, since the reads should be sequential
    //     but I need to check with Mahesh which API we perfer. Notably, if
    //     we're going to add a read(color) method, I believe multiple returns
    //     to be ergonomic
    //TODO If we wish for a read(color) method we'll need to break this up by
    //     color
    //read_buffer: Rc<RefCell<LinkedList<(Uuid, Vec<color>, Box<V>)>>>,
    //TODO switch back to linked list
    read_buffer: Rc<RefCell<LinkedHashMap<Uuid, (Vec<color>, Box<V>)>>>,
    interesting_colors: HashSet<color>,
    snapshot: Vec<OrderIndex>,
}

impl<V: ?Sized, S, H> DAGHandle<V, S, H>
where V: Storeable, S: Store<V>, H: Horizon {
    pub fn new<'c, I: IntoIterator<Item=&'c color>>(store: S, horizon: H, interesting_colors: I) -> Self {
        let read_buffer: Rc<RefCell<LinkedHashMap<Uuid, (Vec<color>, Box<V>)>>> = Default::default();
        let interesting_colors: HashSet<_> = interesting_colors.into_iter().cloned().collect();
        let mut upcalls: HashMap<order, Box<for<'u, 'o, 'r> Fn(&'u Uuid, &'o OrderIndex, &'r V) -> bool + 'static>> = HashMap::new();

        for &chain in &interesting_colors {
            upcalls.entry(chain.into()).or_insert_with(|| { let b = read_buffer.clone();
                Box::new(move |i, &OrderIndex(c, e), v| {
                    unsafe {
                        //FIXME multiappends return multiple times (once per chain +1)
                        //      currently I'm deduplicating here, but really I should
                        //      fix this a layer down...
                        let mut l = b.borrow_mut();
                        if i != &Uuid::nil() {
                            //FIXME figure out why some chains get repeated
                            if let Some(&mut (ref mut cs, _)) = l.get_refresh(i) {
                                if !cs.contains(&c.into()) {
                                    cs.push(c.into())
                                }
                                trace!("coalesce {:?} into {:?}\tid {:?}", (c, e), cs, i);
                                return true
                            }
                        }
                        trace!("new val at {:?} id {:?}", (c, e), i);
                        //FIXME we need to ensure non-multiputs have unique ids
                        //      so we generate them I guess...
                        let id = if i == &Uuid::nil() {
                            Uuid::new_v4()
                        } else { *i };

                        l.insert(id, ([c.into()].to_vec(), v.clone_box()));
                    }
                    true
                }) });
        }
        let num_color = interesting_colors.len();
        DAGHandle {
            log: FuzzyLog::new(store, horizon, upcalls),
            read_buffer: read_buffer,
            interesting_colors: interesting_colors,
            snapshot: Vec::with_capacity(num_color),
        }
    }

    pub fn append(&mut self, data: &V, inhabits: &[color], depends_on: &[color]) {
        //TODO get rid of gratuitous copies
        assert!(inhabits.len() > 0);
        let mut inhabits = inhabits.to_vec();
        let mut depends_on = depends_on.to_vec();
        trace!("color append");
        trace!("inhabits   {:?}", inhabits);
        trace!("depends_on {:?}", depends_on);
        inhabits.sort();
        depends_on.sort();
        let no_snapshot = inhabits == depends_on || depends_on.len() == 0;
        // if we're performing a single colour append we might be able fuzzy_log.append
        // instead of multiappend
        let happens_after = if no_snapshot {
            vec![]
        } else {
            //TODO we should do this in a better way
            depends_on.retain(|c| !inhabits.contains(c));
            self.dependency_snapshot(depends_on)
        };

        if inhabits.len() == 1 {
            trace!("single append");
            self.log.append(inhabits[0].into(), data, &*happens_after);
        }
        else {
            trace!("multi  append");
            let inhabited_chains: Vec<_> = inhabits.into_iter().map(|c| c.into()).collect();
            self.log.multiappend(&*inhabited_chains, data, &*happens_after);
        }
    }

    fn dependency_snapshot(&mut self, depends_on: Vec<color>) -> Vec<OrderIndex> {
        //FIXME we really need read locks to do this correctly
        //      until I implement those, I'm just going to take
        //      a non-linearizeable snapshot and use that
        let mut snapshot = Vec::with_capacity(depends_on.len());
        for &color in &depends_on[..] {
            //TODO should be in parallel
            snapshot.push(OrderIndex(color.into(),
                self.take_snapshot_of(color).unwrap_or(0.into())));
        }
        snapshot
    }

    //NOTE I believe this may return multi-chain appends multiple times, need to test
    //TODO kinda ugly, clean?
    //TODO need some way to do timeout if no update...
    pub fn get_next(&mut self, data_out: &mut V, data_read: &mut usize) -> Vec<color> {
        let mut write_data = |out: &mut _, data: Box<V>| unsafe {
            //TODO we have no way to finish a read if incomplete...
            //TODO this assumes the size info is statically known or stored in the pointer
            //     while currenlty valid, it may not always be so...
            *out = <V as Storeable>::copy_to_mut(&*data, data_out);
        };
        let next_node = self.read_buffer.borrow_mut().pop_front();
        if let Some((_, (color, data))) = next_node {
            write_data(data_read, data);
            return color
        }

        //The buffer is empty we may need to read
        if let Some(to_read_until) = self.snapshot.pop() {
            trace!("reading until {:?}", to_read_until);
            self.log.play_until(to_read_until);
        }

        let next_node = self.read_buffer.borrow_mut().pop_front();
        if let Some((_, (color, data))) = next_node {
            write_data(data_read, data);
            return color
        }
        trace!("no current snapshot");
        *data_read = 0;
        return Vec::new();
    }

    pub fn take_snapshot(&mut self) -> bool {
        if self.snapshot.is_empty() {
            //TODO once asynchrony is setup it probably pays to snapshot
            //     all interesting chains in parallel
            let snap: Vec<_> = self.interesting_colors.iter().cloned().collect();
            let tmp = snap.into_iter()
                .map(|c| (c.into(), self.take_snapshot_of(c)))
                .flat_map(|(c, oe)| oe.map(|e| OrderIndex(c,e)))
                .collect::<Vec<_>>();
            self.snapshot.extend(tmp);
        }
        return !self.snapshot.is_empty()
    }

    pub fn take_snapshot_of(&mut self, color: color) -> Option<entry> {
        let last_unread_entry = self.log.snapshot(color.into());
        if let Some(e) = last_unread_entry {
            Some(e)
            //TODO should start prefetching here
        }
        else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;

    use super::*;

    use std::collections::HashMap;

    //TODO seperate out tcp server
    //TODO switch to multitcp server
    use tcp_store::t_test::new_store;

    #[test]
    fn single_color() {
        let _ = env_logger::init();
        let store = new_store(vec![]);
        let horizon = HashMap::new();
        let mut dag = DAGHandle::new(store, horizon, &[100]);
        {
            dag.append(&32i32, &[100], &[]);
            dag.append(&47i32, &[100], &[]);
            dag.append(&56i32, &[100], &[]);
        }
        let mut out = -1;
        let mut data_read = 0;
        {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec![]);
            assert_eq!(data_read, 0);
        }
        dag.take_snapshot();
        {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec!(100));
            assert_eq!(out, 32);
            assert_eq!(data_read, 4);
        }
        {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec!(100));
            assert_eq!(data_read, 4);
        }
        {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec!(100));
            assert_eq!(data_read, 4);
        }
        {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec![]);
            assert_eq!(data_read, 0);
        }
    }

    #[test]
    fn more_single_color() {
        let _ = env_logger::init();
        let store = new_store(vec![]);
        let horizon = HashMap::new();
        let mut  dag = DAGHandle::new(store, horizon, &[101]);
        for i in 1..50 {
            dag.append(&i, &[101], &[]);
        }
        let mut out = -1;
        let mut data_read = 0;
        let mut sum = 1;
        {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec![]);
            assert_eq!(data_read, 0);
        }

        dag.take_snapshot();
        while out != 49 {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec!(101));
            assert_eq!(data_read, 4);
            assert_eq!(sum, out);
            sum += 1;
        }
        for i in 50..100 {
            dag.append(&i, &[101], &[]);
        }
        {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec![]);
            assert_eq!(data_read, 0);
        }

        dag.take_snapshot();
        while out != 99 {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec!(101));
            assert_eq!(data_read, 4);
            assert_eq!(sum, out);
            sum += 1;
        }
        {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec![]);
            assert_eq!(data_read, 0);
        }
    }

    #[test]
    fn more_multi_color() {
        let _ = env_logger::init();
        let store = new_store(vec![]);
        let horizon = HashMap::new();
        let mut  dag = DAGHandle::new(store, horizon, &[102, 103]);
        for i in 1..50 {
            dag.append(&i, &[102, 103], &[]);
        }

        let mut out = -1;
        let mut data_read = 0;
        let mut sum1 = 1;
        let mut sum2 = 1;
        {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec![]);
            assert_eq!(data_read, 0);
        }

        dag.take_snapshot();
        while !(sum1 == 50 && sum2 == 50) {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(data_read, 4);
            assert_eq!(read, vec![102, 103], "at {:?}", out);
            assert_eq!(sum1, out);
            assert_eq!(sum2, out);
            sum1 += 1;
            sum2 += 1;
        }
        {
            let read = dag.get_next(&mut out, &mut data_read);
            assert_eq!(read, vec![]);
            assert_eq!(data_read, 0);
        }
    }

    #[test]
    fn empty_snapshot() {
        let _ = env_logger::init();
        let store = new_store(vec![]);
        let horizon = HashMap::new();
        let mut dag = DAGHandle::new(store, horizon, &[104, 105]);
        assert_eq!(dag.take_snapshot(), false);
        dag.append(&247, &[105], &[]);
        assert_eq!(dag.take_snapshot(), true);
        assert_eq!(dag.take_snapshot(), true);
        let mut out = -1;
        let mut data_read = 0;
        let read = dag.get_next(&mut out, &mut data_read);
        assert_eq!(&*read, &[105]);
        assert_eq!(data_read, 4);
        assert_eq!(out, 247);
        assert_eq!(dag.take_snapshot(), false);
    }
}
