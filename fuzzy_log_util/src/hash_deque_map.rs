#![allow(dead_code)]

use hash::HashMap;
use std::ops::{Index, IndexMut};

#[derive(Clone, Debug)]
pub struct VecDequeMap<V> {
    //TODO it may be better to use a bitmask instead of Option<V>s
    //     say, have VecDequeMap<V> {
    //         masks: VecDeque<u8>,
    //         data: VecDeque<ManuallyDrop<V>>, ..
    //     }
    data: HashMap<u64, V>,
    start: u64,
    next: u64,
}


impl<V> VecDequeMap<V> {
    pub fn new() -> Self {
        VecDequeMap {
            data: Default::default(),
            start: 0,
            next: 0,
        }
    }

    pub fn push_back(&mut self, val: V) -> u64 {
        let key = self.next;
        self.data.insert(key, val);
        self.next += 1;
        key
    }

    pub fn pop_front(&mut self) -> Option<V> {
        if self.data.is_empty() {
            return None
        }

        let val = self.data.remove(&self.start);
        self.start += 1;
        //FIXME skip elems until Some(..)?
        val
    }

    pub fn insert(&mut self, key: u64, val: V) -> Option<V> {
        if key < self.start {
            self.start = key;
        }
        if key >= self.next {
            self.next = key + 1;
        }
        self.data.insert(key, val)
    }

    pub fn get(&self, key: u64) -> Option<&V> {
        let val = self.data.get(&key);
        if key < self.start || key >= self.next {
            assert!(val.is_none());
        }
        val
    }

    pub fn get_mut(&mut self, key: u64) -> Option<&mut V> {
        let val = self.data.get_mut(&key);
        if key < self.start || key >= self.next {
            assert!(val.is_none());
        }
        val
    }

    pub fn front(&self) -> Option<&V> {
        self.data.get(&self.start)
    }

    pub fn front_mut(&mut self) -> Option<&mut V> {
        self.data.get_mut(&self.start)
    }

    pub fn start_index(&self) -> u64 {
        self.start
    }

    pub fn push_index(&self) -> u64 {
        self.next
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn increment_start(&mut self) {
        assert!(self.front().is_none());
        self.start += 1;
    }
}

impl<V> Default for VecDequeMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A> Index<u64> for VecDequeMap<A> {
    type Output = A;

    fn index(&self, index: u64) -> &A {
        self.get(index).unwrap()
    }
}

impl<A> IndexMut<u64> for VecDequeMap<A> {
    fn index_mut(&mut self, index: u64) -> &mut A {
        self.get_mut(index).unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::VecDequeMap;

    #[test]
    fn test_push() {
        let mut m = VecDequeMap::new();
        m.push_back(3);
        assert_eq!(m[0], 3);
        assert_eq!(m.start_index(), 0);
        assert_eq!(m.push_index(), 1);
        m.push_back(7);
        assert_eq!(m[0], 3);
        assert_eq!(m[1], 7);
        assert_eq!(m.start_index(), 0);
        assert_eq!(m.push_index(), 2);
        m.push_back(9);
        assert_eq!(m[0], 3);
        assert_eq!(m[1], 7);
        assert_eq!(m[2], 9);
        assert_eq!(m.start_index(), 0);
        assert_eq!(m.push_index(), 3);
    }

    #[test]
    fn test_pop() {
        let mut m = VecDequeMap::new();
        m.push_back(3);
        assert_eq!(m[0], 3);
        assert_eq!(m.start_index(), 0);
        assert_eq!(m.push_index(), 1);
        m.push_back(7);
        assert_eq!(m[0], 3);
        assert_eq!(m[1], 7);
        assert_eq!(m.start_index(), 0);
        assert_eq!(m.push_index(), 2);
        m.push_back(9);
        assert_eq!(m[0], 3);
        assert_eq!(m[1], 7);
        assert_eq!(m[2], 9);
        assert_eq!(m.start_index(), 0);
        assert_eq!(m.push_index(), 3);
        assert_eq!(m.pop_front(), Some(3));
        assert_eq!(m.get(0), None);
        assert_eq!(m[1], 7);
        assert_eq!(m[2], 9);
        assert_eq!(m.start_index(), 1);
        assert_eq!(m.push_index(), 3);
        m.push_back(11);
        assert_eq!(m.get(0), None);
        assert_eq!(m[1], 7);
        assert_eq!(m[2], 9);
        assert_eq!(m[3], 11);
        assert_eq!(m.start_index(), 1);
        assert_eq!(m.push_index(), 4);
        assert_eq!(m.pop_front(), Some(7));
        assert_eq!(m.pop_front(), Some(9));
        assert_eq!(m.pop_front(), Some(11));
        assert_eq!(m.get(0), None);
        assert_eq!(m.get(1), None);
        assert_eq!(m.get(2), None);
        assert_eq!(m.get(3), None);
        assert_eq!(m.start_index(), 4);
        assert_eq!(m.push_index(), 4);
        assert!(m.is_empty());
    }

    #[test]
    fn big_map() {
        //bad insert key: 512, start: 63, old_len: 449, new_len: 450, index: 449
        let mut m = VecDequeMap::new();
        for i in 0..512 {
            assert_eq!(m.insert(i, i), None);
        }
        for i in 0..63 {
            assert_eq!(m.pop_front(), Some(i));
        }
        assert_eq!(m.start_index(), 63);
        assert_eq!(m.push_index(), 512);
        assert_eq!(m.insert(512, 512), None);
    }

    mod from_vec_map {
        //from contain-rs/vec-map
        use super::super::VecDequeMap;

         #[test]
        fn test_get_mut() {
            let mut m = VecDequeMap::new();
            assert!(m.insert(1, 12).is_none());
            assert!(m.insert(2, 8).is_none());
            assert!(m.insert(5, 14).is_none());
            let new = 100;
            match m.get_mut(5) {
                None => panic!(), Some(x) => *x = new
            }
            assert_eq!(m.get(5), Some(&new));
        }

        #[test]
        fn test_insert() {
            let mut m = VecDequeMap::new();
            assert_eq!(m.insert(1, 2), None);
            assert_eq!(m.insert(1, 3), Some(2));
            assert_eq!(m.insert(1, 4), Some(3));
        }

        #[test]
        fn test_index() {
            let mut map = VecDequeMap::new();

            map.insert(1, 2);
            map.insert(2, 1);
            map.insert(3, 4);

            assert_eq!(map[3], 4);
        }

        #[test]
        #[should_panic]
        fn test_index_nonexistent() {
            let mut map = VecDequeMap::new();

            map.insert(1, 2);
            map.insert(2, 1);
            map.insert(3, 4);

            map[4];
        }
    }

    #[test]
        fn test_insert() {
            let mut m = VecDequeMap::new();
            assert_eq!(m.insert(2, 2), None);
            assert_eq!(m.insert(2, 3), Some(2));
            assert_eq!(m.insert(2, 4), Some(3));
            assert_eq!(m.insert(1, 1), None);
            assert_eq!(m.insert(2, 5), Some(4));
            assert_eq!(m.insert(1, 2), Some(1));
        }
}
