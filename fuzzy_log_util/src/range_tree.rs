use std::collections::BTreeMap;

use std::cmp::{Ord, PartialOrd, PartialEq, Ordering};
use std::cmp::Ordering::*;

use std::u64;

use std::collections::btree_map::Iter;
use std::collections::btree_map::Entry::Occupied;

#[derive(Debug)]
pub struct RangeTree<V> {
    inner: BTreeMap<Range, V>,
}

pub trait Mergeable: Sized {
    fn is_prior_to(&self, other: &Self) -> bool;
    fn can_merge_with_prior(self, other: &Self) -> Result<Self, Self>;
    fn can_merge_with_next(self, other: &Self) -> Result<Self, Self>;
}

impl<V> RangeTree<V> {
    pub fn with_default_val(v: V) -> Self {
        let mut map = BTreeMap::new();
        map.insert(Range::new(0, u64::MAX.into()), v);
        RangeTree {
            inner: map,
        }
    }

    pub fn get(&self, point: u64) -> Option<&V> {
        self.inner.get(&Range::point(point))
    }

    pub fn iter(&self) -> Iter<Range, V> {
        self.inner.iter()
    }
}

impl<V: Mergeable + Eq + Clone> RangeTree<V> {
    fn set_point_as(&mut self, point: u64, new_kind: V) -> bool {
        let (old_range, old_kind) = remove_from_map(&mut self.inner, Range::point(point));
        if !old_kind.is_prior_to(&new_kind) {
            self.inner.insert(old_range, old_kind);
            return false
        }

        match old_range.split_at(point) {
            (Some(pre), Some(post)) => {
                self.inner.insert(Range::point(point), new_kind);
                self.inner.insert(pre, old_kind.clone());
                self.inner.insert(post, old_kind);
            },

            (Some(pre), None) => {
                self.inner.insert(pre, old_kind);
                let new_range = Range::point(point);
                let (new_range, new_kind) =
                    try_merge_with_next(&mut self.inner, new_range, new_kind);
                self.inner.insert(new_range, new_kind);
            },

            (None, Some(post)) => {
                self.inner.insert(post, old_kind);
                let new_range = Range::point(point);
                let (new_range, new_kind) =
                    try_merge_with_prior(&mut self.inner, new_range, new_kind);
                self.inner.insert(new_range, new_kind);
            },

            (None, None) => {
                let new_range = Range::point(point);
                let (new_range, new_kind) =
                    try_merge_with_next(&mut self.inner, new_range, new_kind);
                let (new_range, new_kind) =
                    try_merge_with_prior(&mut self.inner, new_range, new_kind);
                self.inner.insert(new_range, new_kind);
            },
        }

        true
    }

    pub fn set_range_as(&mut self, low: u64, high: u64, new_kind: V) {
        debug_assert!(self.tree_invariant(), "invariant failed");
        let new_range = Range::new(low, high);
        let (old_range, old_kind) = remove_from_map(&mut self.inner, Range::point(low));
        assert!(old_range.spans(&new_range));

        match old_range.remove(&new_range) {
            (Some(pre), Some(post)) => {
                self.inner.insert(new_range, new_kind);
                self.inner.insert(pre, old_kind.clone());
                self.inner.insert(post, old_kind);
            },

            (Some(pre), None) => {
                self.inner.insert(pre, old_kind);
                let (new_range, new_kind) =
                    try_merge_with_next(&mut self.inner, new_range, new_kind);
                self.inner.insert(new_range, new_kind);
            },

            (None, Some(post)) => {
                self.inner.insert(post, old_kind);
                let (new_range, new_kind) =
                    try_merge_with_prior(&mut self.inner, new_range, new_kind);
                self.inner.insert(new_range, new_kind);
            },

            (None, None) => {
                let (new_range, new_kind) =
                    try_merge_with_next(&mut self.inner, new_range, new_kind);
                let (new_range, new_kind) =
                    try_merge_with_prior(&mut self.inner, new_range, new_kind);
                self.inner.insert(new_range, new_kind);
            },
        }
        debug_assert!(self.tree_invariant(), "invariant failed");
    }

    pub fn first_with(&self, v: &V) -> Option<u64> {
        self.iter()
            .filter(|&(_, k)| k == v)
            .next()
            .map(|(r, _)| r.first())
    }

    #[allow(dead_code)]
    fn tree_invariant(&self) -> bool {
        let (_, _, valid) = self.iter()
            .fold((0, None, true),
            |(prev, prev_kind, prev_valid), (r, k)| {
            if let Some(prev_kind) = prev_kind {
                let valid = prev_valid && r.first() == prev + 1 && k != prev_kind;
                (r.last(), Some(k), valid)
            }
            else {
                let valid = prev_valid && r.first() == 0;
                (r.last(), Some(k), valid)
            }
        });
        valid
    }
}

fn remove_from_map<V>(map: &mut BTreeMap<Range, V>, key: Range) -> (Range, V) {
    match map.entry(key.clone()) {
        Occupied(o) => Some(o.remove_entry()),
        _ => None,
    }.unwrap_or_else(|| panic!("Tried to remove bad range {:?}", key))
}

fn try_merge_with_next<V: Mergeable>(
    inner: &mut BTreeMap<Range, V>, current: Range, our_kind: V
) -> (Range, V) {
    if current.last() >= u64::MAX {
        return (current, our_kind)
    }

    match inner.entry(Range::point(current.last() + 1)) {
        Occupied(entr) => {
            match our_kind.can_merge_with_next(entr.get()) {
                Err(our_kind) => (current, our_kind),
                Ok(new_kind) => {
                    let merge = entr.remove_entry().0;
                    (current.merge_with_next(merge), new_kind)
                }
            }
        },
        _ => (current, our_kind)
    }
}

fn try_merge_with_prior<V: Mergeable>(
    inner: &mut BTreeMap<Range, V>, current: Range, our_kind: V
) -> (Range, V) {
    if current.first() <= 0 {
        return (current, our_kind)
    }

    match inner.entry(Range::point(current.first() - 1)) {
        Occupied(entr) => {
            match our_kind.can_merge_with_prior(entr.get()) {
                Err(our_kind) => (current, our_kind),
                Ok(new_kind) => {
                    let merge = entr.remove_entry().0;
                    (current.merge_with_prior(merge), new_kind)
                }
            }
        },
        _ => (current, our_kind)
    }
}

//XXX this is inconsistent if there are overlapping ranges

#[derive(Eq, Clone, Debug)]
pub struct Range(u64, u64);

impl Range {
    fn new(first: u64, last: u64) -> Self {
        assert!(first <= last, "{:?} >= {:?}", first, last);
        Range(first, last)
    }

    fn point(point: u64) -> Self {
        Range(point, point)
    }

    fn merge_with_next(self, next: Range) -> Range {
        debug_assert_eq!(self.last() + 1, next.first());
        let Range(first, _) = self;
        let Range(_, last) = next;
        Range(first, last)
    }

    fn merge_with_prior(self, prior: Range) -> Range {
        debug_assert_eq!(prior.last() + 1, self.first());
        let Range(first, _) = prior;
        let Range(_, last) = self;
        Range(first, last)
    }

    /*
    fn extend_end(&mut self, amount: u64) {
        let &mut Range(_, ref mut last) = self;
        *last = *last + amount;
    }

    fn extend_beginning(&mut self, amount: u64) {
        let &mut Range(ref mut first, _) = self;
        *first = *first - amount;
    }*/

    fn split_at(self, point: u64) -> (Option<Range>, Option<Range>) {
        let Range(first, last) = self;
        match (first.cmp(&point), last.cmp(&point)) {
            (Equal, Equal) => (None, None),
            (Less, Equal) => (Some(Range(first, point - 1)), None),
            (Equal, Greater) => (None, Some(Range(point + 1, last))),
            (Less, Greater) => (Some(Range(first, point - 1)),
                Some(Range(point + 1, last))),

            (_, Less) => (Some(Range(first, last)), None),
            (Greater, _) => (None, Some(Range(first, last))),
        }
    }

    fn remove(self, other: &Range) -> (Option<Range>, Option<Range>) {
        let Range(my_first, my_last) = self;
        let &Range(ref other_first, ref other_last) = other;
        match (my_first.cmp(other_first), my_last.cmp(other_last)) {
            (Equal, Equal) => (None, None),
            (Less, Equal) => (Some(Range(my_first, *other_first - 1)), None),
            (Equal, Greater) => (None, Some(Range(*other_last + 1, my_last))),
            (Less, Greater) => (Some(Range(my_first, *other_first - 1)),
                Some(Range(*other_last + 1, my_last))),

            _ => unreachable!(),
        }
    }

    fn spans(&self, other: &Range) -> bool {
        let &Range(ref my_first, ref my_last) = self;
        let &Range(ref other_first, ref other_last) = other;
        my_first <= other_first && other_last <= my_last
    }

    fn first(&self) -> u64 {
        let &Range(first, _) = self;
        first
    }

    fn last(&self) -> u64 {
        let &Range(_, last) = self;
        last
    }

    fn len(&self) -> u64 {
        let &Range(first, last) = self;
        (last - first) + 1
    }
}

impl Ord for Range {
    fn cmp(&self, other: &Self) -> Ordering {
        let &Range(ref my_first, ref my_last) = self;
        let &Range(ref other_first, ref other_last) = other;
        match (my_first.cmp(other_last), my_last.cmp(other_first)) {
            (_, Less) => Less,
            (Greater, _) => Greater,
            (_, _) => Equal,
        }
    }
}

impl PartialOrd for Range {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Range {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Equal
    }
}

#[cfg(test)]
mod test {
    use super::*;

}
