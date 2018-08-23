use std::cell::RefCell;
use std::collections::{LinkedList, VecDeque};
use std::rc::Rc;

use futures::{Async, Poll, Stream, Sink};
use futures::task::{self, Task};

use fuzzy_log_packets::Packet;

#[derive(Default, Clone)]
pub struct BufferStream {
    inner: Rc<RefCell<Inner>>,
}

#[derive(Default)]
struct Inner {
    stream: LinkedList<Vec<u8>>,
    blocked: Option<Task>,
    returned: VecDeque<Vec<u8>>,
}

const MAX_BUFFER_SIZE: usize = 8 * 1024;

impl BufferStream {

    //FIXME add backpressure
    pub fn add_buffer(&self, buffer: Vec<u8>) {
        let mut inner = self.inner.borrow_mut();
        let added = inner.try_append_bytes(&buffer);
        if !added {
            inner.append_buffer(buffer);
        }
        if let Some(task) = inner.blocked.take() {
            task.notify();
        }
    }

    pub fn add_slice(&self, bytes: &[u8]) {
        let mut inner = self.inner.borrow_mut();
        let added = inner.try_append_bytes(bytes);
        if !added {
            let buffer = inner.into_vec(bytes);
            inner.append_buffer(buffer);
        }
        if let Some(task) = inner.blocked.take() {
            task.notify();
        }
    }

    pub fn add_contents(&mut self, contents: Packet::Ref) {
        let mut inner = self.inner.borrow_mut();
        inner.add_contents(contents);
        if let Some(task) = inner.blocked.take() {
            task.notify();
        }
    }

    pub fn return_buffer(&self, buffer: Vec<u8>) {
        let mut inner = self.inner.borrow_mut();
        inner.return_buffer(buffer)
    }
}

impl Inner {
    fn try_append_bytes(&mut self, bytes: &[u8]) -> bool {
        match self.stream.back_mut() {
            None => return false,
            Some(buffer) => {
                if buffer.len() + bytes.len() > MAX_BUFFER_SIZE {
                    return false;
                }
                buffer.extend_from_slice(bytes);
                return true;
            }
        }
    }

    fn append_buffer(&mut self, buffer: Vec<u8>) {
        self.stream.push_back(buffer)
    }

    fn add_contents(&mut self, contents: Packet::Ref) {
        let contents_len = contents.len();
        match self.stream.back_mut() {
            None => (),
            Some(ref mut buffer) => if buffer.len() + contents_len <= MAX_BUFFER_SIZE {
                contents.fill_vec(buffer);
                return;
            },
        };
        let mut buffer = if self.returned.is_empty() {
            vec![]
        } else if contents_len > self.returned[0].capacity() {
            vec![]
        } else {
            self.returned.pop_front().unwrap()
        };

        contents.fill_vec(&mut buffer);
        self.stream.push_back(buffer);
    }

    fn into_vec(&mut self, bytes: &[u8]) -> Vec<u8> {
        if self.returned.is_empty() {
            return bytes.to_vec();
        }

        if self.returned.front().unwrap().capacity() < bytes.len() {
            return bytes.to_vec();
        }

        let mut buffer = self.returned.pop_front().unwrap();

        buffer.extend_from_slice(bytes);
        buffer
    }

    fn return_buffer(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        let buffer = buffer;
        if self.returned.is_empty() {
            self.returned.push_back(buffer);
            return;
        }

        if self.returned.len() == 1 {
            if self.returned[0].capacity() < buffer.capacity() {
                self.returned.push_front(buffer);
            } else {
                self.returned.push_back(buffer);
            }
            return;
        }

        if self.returned[0].capacity() < buffer.capacity() {
            self.returned.pop_back();
            self.returned.push_front(buffer);
            return;
        }

        if self.returned[1].capacity() < buffer.capacity() {
            self.returned.pop_back();
            self.returned.push_back(buffer);
            return;
        }
        ::std::mem::drop(buffer)
    }
}

impl Stream for BufferStream {
    type Item = Vec<u8>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut inner = self.inner.borrow_mut();
        match inner.stream.pop_front() {
            None => {
                inner.blocked = Some(task::current());
                Ok(Async::NotReady)
            }
            Some(buffer) => Ok(Async::Ready(Some(buffer))),
        }
    }
}
