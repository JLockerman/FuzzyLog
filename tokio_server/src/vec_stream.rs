use std::collections::VecDeque;
use std::marker::PhantomData;

use futures::{Async, Poll};
use stream::Stream;

#[derive(Debug)]
pub struct VecStream<I, E> {
    vec: VecDeque<I>,
    _pd: PhantomData<fn() -> E>,
}

pub fn build<I, E>(vec: Vec<I>) -> VecStream<I, E> {
    VecStream {
        vec: vec.into(),
        _pd: PhantomData,
    }
}

impl<I, E> VecStream<I, E> {
    pub fn into_vec(self) -> Vec<I> {
        self.vec.into()
    }
}

impl<I, E> Stream for VecStream<I, E> {
    type Item = I;
    type Error = E;

    fn poll(&mut self) -> Poll<Option<I>, E> {
        Ok(Async::Ready(self.vec.pop_front()))
    }
}
