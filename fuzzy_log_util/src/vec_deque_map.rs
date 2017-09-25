#![allow(dead_code)]

use std::collections::VecDeque;
use std::fmt;
use std::mem;
use std::ops::{Index, IndexMut};

#[derive(Clone)]
pub struct VecDequeMap<V> {
    //TODO it may be better to use a bitmask instead of Option<V>s
    //     say, have VecDequeMap<V> {
    //         masks: VecDeque<u8>,
    //         data: VecDeque<ManuallyDrop<V>>, ..
    //     }
    data: VecDeque<Option<V>>,
    start: u64,
}


impl<V> VecDequeMap<V> {
    pub fn new() -> Self {
        VecDequeMap {
            data: VecDeque::new(),
            start: 0,
        }
    }

    pub fn push_back(&mut self, val: V) -> u64 {
        let key = self.data.len();
        self.data.push_back(Some(val));
        key as u64
    }

    pub fn pop_front(&mut self) -> Option<V> {
        if self.data.is_empty() {
            return None
        }

        self.start += 1;
        //FIXME skip elems until Some(..)?
        self.data.pop_front().unwrap_or(None)
    }

    pub fn insert(&mut self, key: u64, val: V) -> Option<V> {
        if key >= self.start {
            let index = (key - self.start) as usize;
            if index >= self.data.len() {
                let additional = (index - self.data.len()) + 1;
                self.data.reserve_exact(additional);
                self.data.extend((0..additional).map(|_| None));
            }

            mem::replace(&mut self.data[index], Some(val))
        } else {
            let additional = (key - self.start) as usize;
            self.data.reserve_exact(additional);
            for _ in 0..additional {
                self.data.push_front(None)
            }

            mem::replace(&mut self.data[0], Some(val))
        }
    }

    pub fn get(&self, key: u64) -> Option<&V> {
        if key < self.start || self.data.is_empty() {
            return None
        }

        let index = (key - self.start) as usize;
        self.data[index].as_ref()
    }

    pub fn get_mut(&mut self, key: u64) -> Option<&mut V> {
        if key < self.start || self.data.is_empty() {
            return None
        }

        let index = (key - self.start) as usize;
        self.data[index].as_mut()
    }

    pub fn front(&self) -> Option<&V> {
        self.data.front().map(Option::as_ref).unwrap_or(None)
    }

    pub fn front_mut(&mut self) -> Option<&mut V> {
        self.data.front_mut().map(Option::as_mut).unwrap_or(None)
    }

    pub fn start_index(&self) -> u64 {
        self.start
    }

    pub fn push_index(&self) -> u64 {
        self.start + self.data.len() as u64
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

impl<V: fmt::Debug> fmt::Debug for VecDequeMap<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_map()
        .entries(self.data.iter().enumerate().map(|(i, v)|
            ((i as u64 + self.start), v.as_ref().unwrap())))
        .finish()
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
}
