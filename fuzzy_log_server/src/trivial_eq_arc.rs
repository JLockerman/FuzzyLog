use std::cell::UnsafeCell;
use std::sync::Arc;

#[derive(Debug)]
pub struct TrivialEqArc<T: ?Sized>(Arc<UnsafeCell<T>>);

impl<T> TrivialEqArc<T> {
    pub fn new(t: T) -> Self {
        TrivialEqArc(Arc::new(UnsafeCell::new(t)))
    }

    pub fn get(this: &Self) -> &T {
        unsafe { &*this.0.get() }
    }
}

impl<T: ?Sized> ::std::ops::Deref for TrivialEqArc<T> {
    type Target = Arc<UnsafeCell<T>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized> ::std::ops::DerefMut for TrivialEqArc<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: ?Sized> PartialEq for TrivialEqArc<T> {
    fn eq(&self, other: &TrivialEqArc<T>) -> bool {
        Arc::ptr_eq(self, other)
    }
}

impl<T: ?Sized> Clone for TrivialEqArc<T> {
    fn clone(&self) -> Self {
        TrivialEqArc(Arc::clone(&self.0))
    }
}

impl<T: ?Sized> Eq for TrivialEqArc<T> {}
unsafe impl<T: ?Sized + Sync> Send for TrivialEqArc<T> {}
