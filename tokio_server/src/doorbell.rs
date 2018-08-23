use std::cell::Cell;
use std::rc::Rc;

use futures::Stream;
use futures::task::{self, Task};

#[derive(Debug, Clone)]
pub struct Doorbell {
    inner: Rc<Inner>,
}

struct Inner {
    count: Cell<u64>,
    blocked: Cell<Option<Task>>,
}

impl Doorbell {
    pub fn new() -> Self {
        let inner = Inner {
            count: 0.into(),
            blocked: None.into(),
        };
        Doorbell {
            inner: inner.into(),
        }

    }

    pub fn ring() {
        let old = self.inner.count.get();
        self.inner.set(old + 1);
        if let Some(task) = self.inner.blocked.replace(None) {
            task.notify();
        }
    }
}

impl Stream for Doorbell {
    type Item = u64;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let wakes = self.inner.count.replace(0);
        if wakes > 0 {
            Ok(Async::Ready(wakes))
        } else {
            self.inner.blocked.set(Some(task::current()));
            Ok(Async::NotReady)
        }
}
