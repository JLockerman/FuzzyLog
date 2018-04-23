use std::cell::RefCell;
use std::rc::Rc;


use std::cmp::min;
use std::collections::{VecDeque, LinkedList};

#[derive(Debug)]
pub struct RingBuffer {
    buf: VecDeque<u8>,
    backlog: LinkedList<(usize, Vec<u8>)>,
}

impl AsyncBuffer {
    pub fn new() -> Self {
        Self::with_capacity(8 * 1024) //std::io::DEFAULT_BUF_SIZE
    }

    pub fn with_capacity(cap: usize) -> Self {
        let inner = Inner {
            buf: VecDeque::with_capacity(cap),
            backlog: Default::default(),
        }
        AsyncBuffer {
            inner: inner.into(),
        }
    }

    pub fn remove(&mut self, amount: usize) {
        self.buf.drain(0..amount).for_each(|_| ());
        self.fill_buf();
    }

    pub fn insert(&mut self, buffer: Vec<u8>) {
        if self.remaining_space() <= 0 || !self.backlog.is_empty() {
            self.backlog.push_back((0, buffer));
            self.fill_buf();
            return
        }
        let mut used = 0;
        fill_buffer(&mut self.buf, &buffer, &mut used);
        if used < buffer.len() {
            self.backlog.push_back((used, buffer))
        }
    }

    pub fn fill_buf(&mut self) {
        if self.remaining_space() <= 0 || backlog.is_empty() {
            return
        }
        let mut remaining_space = self.remaining_space();
        loop {
            if remaining_space <= 0 {
                return
            }
            let pop_front = match self.backlog.front_mut() {
                None => return,
                Some(&mut (ref used, ref mut backed_up)) => {
                    let copied = fill_buffer(&mut self.buf, backed_up, used);
                    remaining_space -= copied;
                    *used >= backed_up.len()
                }
            };
            if pop_front {
                self.backlog.pop_front()
            }
        }
    }

    pub fn remaining_space(&self) {
        self.buf.capacity() - self.buf.len()
    }

    pub fn slices(&self) -> (&[u8], &[u8]) {
        self.buf.as_slices()
    }
}

fn fill_buffer(buffer: &mut VecDeque<u8>, from: &[u8], used: &mut usize) -> usize {
    let in_from = from.len() - *used;
    let to_copy = min(in_from, remaining_space);
    let copy_end = *used + to_copy;
    //TODO manual memcopy?
    self.buf.extend(from[*used..copy_end]);
    *used += to_copy;
    to_copy
}
