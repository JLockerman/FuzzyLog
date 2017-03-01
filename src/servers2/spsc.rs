use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};


use deque::{self, Stolen, Worker, Stealer};

use mio::{self, Evented, Poll, PollOpt, Registration, SetReadiness, Token};

use lazycell::{LazyCell, AtomicLazyCell};

// ctl based on mio::channel

/*pub fn channels<V: Send>(num_receivers: usize) -> (Sender<V>, Vec<Receiver<V>>) {
    let (worker, stealer) = deque::new();
    let notifiers = (0..num_receivers).map(|_| LazyCell::new()).collect();
    let ctl = Arc::new(Ctl { pending: AtomicUsize::new(0), notifiers: notifiers });
    let receivers = (0..num_receivers).map(|i|
        Receiver { inner: stealer.clone(), receiver_num: i, ctl: ctl.clone() }
    ).collect();
    let sender = Sender { inner: worker, ctl: ctl };
    (sender, receivers)
}

pub struct Sender<V: Send> {
    inner: Worker<V>,
    ctl: Arc<Ctl>,
}

pub struct Receiver<V: Send> {
    inner: Stealer<V>,
    receiver_num: usize,
    ctl: Arc<Ctl>,
}

struct Ctl {
    pending: AtomicUsize,
    notifiers: Vec<Notifier>,
}*/

pub fn channel<V: Send>() -> (Sender<V>, Receiver<V>) {
    let (worker, stealer) = deque::new();
    let ctl = Arc::new(Ctl {
        pending: AtomicUsize::new(0),
        set_readiness: AtomicLazyCell::new()
    });
    let receiver = Receiver {
        inner: stealer,
        registration: LazyCell::new(),
        ctl: ctl.clone()
    };
    let sender = Sender { inner: worker, ctl: ctl };
    (sender, receiver)
}

pub struct Sender<V: Send> {
    inner: Worker<V>,
    ctl: Arc<Ctl>,
}

pub struct Receiver<V: Send> {
    inner: Stealer<V>,
    registration: LazyCell<Registration>,
    ctl: Arc<Ctl>,
}

struct Ctl {
    pending: AtomicUsize,
    set_readiness: AtomicLazyCell<SetReadiness>,
}

impl<V> Sender<V>
where V: Send {
    pub fn send(&self, v: V) {
        self.inner.push(v);
        self.ctl.inc();
    }
}

impl<V> Receiver<V>
where V: Send {
    pub fn try_recv(&self) -> Option<V> {
        loop {
            match self.inner.steal() {
                Stolen::Empty => return None,
                Stolen::Abort => { ::std::thread::yield_now() },
                Stolen::Data(v) => {
                    self.ctl.dec();
                    return Some(v)
                },
            }
        }
    }
}

impl<V: Send> Evented for Receiver<V> {
    fn register(&self, poll: &Poll, token: Token, interest: mio::Ready, opts: PollOpt) -> io::Result<()> {
        if self.registration.borrow().is_some() {
            return Err(io::Error::new(io::ErrorKind::Other, "receiver already registered"));
        }

        let (registration, set_readiness) = Registration::new(poll, token, interest, opts);


        if self.ctl.pending.load(Ordering::Relaxed) > 0 {
            // TODO: Don't drop readiness
            let _ = set_readiness.set_readiness(mio::Ready::readable());
        }

        self.registration.fill(registration).ok().expect("unexpected state encountered");
        self.ctl.set_readiness.fill(set_readiness).ok().expect("unexpected state encountered");

        Ok(())
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: mio::Ready, opts: PollOpt) -> io::Result<()> {
        match self.registration.borrow() {
            Some(registration) => registration.update(poll, token, interest, opts),
            None => Err(io::Error::new(io::ErrorKind::Other, "receiver not registered")),
        }
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        match self.registration.borrow() {
            Some(registration) => registration.deregister(poll),
            None => Err(io::Error::new(io::ErrorKind::Other, "receiver not registered")),
        }
    }
}

impl Ctl {
    fn inc(&self) {
        let cnt = self.pending.fetch_add(1, Ordering::Acquire);

        if 0 == cnt {
            // Toggle readiness to readable
            if let Some(set_readiness) = self.set_readiness.borrow() {
                //FIXME handle errors
                let _ = set_readiness.set_readiness(mio::Ready::readable());
            }
        }
    }

    fn dec(&self) {
        let first = self.pending.load(Ordering::Acquire);

        if first == 1 {
            // Unset readiness
            if let Some(set_readiness) = self.set_readiness.borrow() {
                //FIXME handle errors
                let _ = set_readiness.set_readiness(mio::Ready::none());
            }
        }

        // Decrement
        let second = self.pending.fetch_sub(1, Ordering::AcqRel);

        if first == 1 && second > 1 {
            // There are still pending messages. Since readiness was
            // previously unset, it must be reset here
            if let Some(set_readiness) = self.set_readiness.borrow() {
                //FIXME handle errors
                let _ = set_readiness.set_readiness(mio::Ready::readable());
            }
        }
    }
}

