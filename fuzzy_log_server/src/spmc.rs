use std::io;
use std::sync::Arc;
use std::sync::mpsc;

use deque::{self, Stolen, Worker, Stealer};

use mio::{self, Evented, Poll, PollOpt, Registration, SetReadiness, Token};

use lazycell::{LazyCell, AtomicLazyCell};

// XXX XXX THIS DOES NOT WORK

// based heavily on mio::channel

pub fn channel<V: Send>() -> (Sender<V>, Receiver<V>) {
    let (worker, stealer) = deque::new();
    let (send, recv) = mpsc::channel();
    let sender = Sender { inner: worker, sleepers: recv };
    let reciever = Receiver {
        inner: stealer,
        registration: LazyCell::new(),
        notifier: Arc::new(AtomicLazyCell::new()),
        sleepers: send
    };
    (sender, reciever)
}

pub struct Sender<V: Send> {
    inner: Worker<V>,
    sleepers: mpsc::Receiver<Arc<Notifier>>,
}

pub struct Receiver<V: Send> {
    inner: Stealer<V>,
    registration: LazyCell<Registration>,
    notifier: Arc<Notifier>,
    sleepers: mpsc::Sender<Arc<Notifier>>,
}

type Notifier = AtomicLazyCell<SetReadiness>;

impl<V> Sender<V>
where V: Send {
    pub fn send(&self, v: V) {
        self.inner.push(v);
        if let Ok(note) = self.sleepers.try_recv() {
            if let Some(set_readiness) = note.borrow() {
                set_readiness.set_readiness(mio::Ready::readable()).unwrap();
            }
        }
    }
}

impl<V> Receiver<V>
where V: Send {
    pub fn steal(&self) -> Stolen<V> {
        self.inner.steal()
    }

    pub fn steal_or_set_unready(&self) -> Stolen<V> {
        match self.inner.steal() {
            s @ Stolen::Abort | s @ Stolen::Data(..) => return s,
            s @ Stolen::Empty => {
                if let Some(notifier) = self.notifier.borrow() {
                    //FIXME is this right?
                    if notifier.readiness().is_readable() {
                        notifier.set_readiness(mio::Ready::none()).unwrap();
                        let _ = self.sleepers.send(self.notifier.clone());
                    }
                }
                return s
            },
        }
    }

    pub fn try_recv(&self) -> Option<V> {
        loop {
            match self.steal_or_set_unready() {
                Stolen::Empty => return None,
                Stolen::Data(v) => return Some(v),
                Stolen::Abort => continue,
            }
        }
    }
}

impl<V: Send> Clone for Receiver<V> {
    fn clone(&self) -> Self {
        Receiver {
            inner: self.inner.clone(),
            registration: LazyCell::new(),
            notifier: Arc::new(AtomicLazyCell::new()),
            sleepers: self.sleepers.clone(),
        }
    }
}

impl<V: Send> Evented for Receiver<V> {
    fn register(&self, poll: &Poll, token: Token, interest: mio::Ready, opts: PollOpt) -> io::Result<()> {
        if self.registration.borrow().is_some() {
            return Err(io::Error::new(io::ErrorKind::Other, "receiver already registered"));
        }

        let (registration, set_readiness) = Registration::new(poll, token, interest, opts);

        let _ = set_readiness.set_readiness(mio::Ready::readable());

        self.registration.fill(registration).ok().expect("unexpected state encountered");
        self.notifier.fill(set_readiness).ok().expect("unexpected state encountered");

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
