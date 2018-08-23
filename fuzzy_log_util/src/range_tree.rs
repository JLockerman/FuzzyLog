use std::collections::BTreeMap;

use std::cmp::{Ord, PartialOrd, PartialEq, Ordering};
use std::cmp::Ordering::*;

use std::u64;

use std::collections::btree_map::Iter;
use std::collections::btree_map::Entry::Occupied;

use std::rc::Rc;

#[derive(Debug)]
pub struct RangeTree<V> {
    inner: BTreeMap<Range, V>,
}

pub trait Mergeable: Sized + Eq {
    #[allow(unused_variables)]
    fn can_be_replaced_with(&self, other: &Self) -> bool { true }

    fn can_merge_with_prior(self, other: &Self) -> Result<Self, Self> {
        if &self == other { Ok(self) } else { Err(self) }
    }

    fn can_merge_with_next(self, other: &Self) -> Result<Self, Self> {
        self.can_merge_with_prior(other)
    }
}

impl<V> RangeTree<V> {
    pub fn with_default_val(v: V) -> Self {
        let mut map = BTreeMap::new();
        map.insert(Range::new(0, u64::MAX.into()), v);
        RangeTree {
            inner: map,
        }
    }

    pub fn get(&self, point: u64) -> &V {
        self.inner.get(&Range::point(point)).unwrap()
    }

    pub fn iter(&self) -> Iter<Range, V> {
        self.inner.iter()
    }
}

impl<V: Mergeable + Eq + Clone> RangeTree<V> {

    //FIXME merge logic is wrong for constraints other than equals
    pub fn set_point_as(&mut self, point: u64, new_kind: V) -> bool {
        //let (old_range, old_kind) = remove_from_map(&mut self.inner, Range::point(point));
        let old = remove_from_map_if(
            &mut self.inner, Range::point(point),
            |_, old_kind| old_kind != &new_kind && old_kind.can_be_replaced_with(&new_kind)
        );
        let (old_range, old_kind) = match old {
            Some(old) => old, None => return false,
        };

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

    //FIXME merge logic is wrong for constraints other than equals
    pub fn set_range_as(&mut self, low: u64, high: u64, new_kind: V) {
        debug_assert!(self.tree_invariant(), "invariant failed");
        let new_range = Range::new(low, high);
        let old = remove_from_map_if(
            &mut self.inner, Range::point(low),
            |old_range, old_kind|
                !(old_range.spans(&new_range) && old_kind == &new_kind)
                && old_kind.can_be_replaced_with(&new_kind)
        );
        let (mut old_range, mut old_kind) = match old {
            Some(old) => old, None => return,
        };
        if old_range.spans(&new_range) {
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
        } else {
            let mut accumulator = Range::point(low);
            loop {
                if old_kind == new_kind {
                    accumulator = accumulator.merge_with(old_range);
                } else {
                    match old_range.trim_out(&new_range) {
                        (None, range, None) => accumulator = accumulator.merge_with(range),
                        (Some(prior), range, None) => {
                            self.inner.insert(prior, old_kind);
                            accumulator = accumulator.merge_with(range)
                        },
                        (prior, range, Some(post)) => {
                            if let Some(prior) = prior {
                                self.inner.insert(prior, old_kind.clone());
                            }
                            accumulator = accumulator.merge_with(range);
                            self.inner.insert(post, old_kind);
                            break
                        },
                    }
                }

                if accumulator.last() >= high {
                    break
                }

                let (or, ok) = remove_from_map(
                    &mut self.inner, Range::point(accumulator.last() + 1)
                );
                old_range = or;
                old_kind = ok;
            }

            let new_range = accumulator;
            let (new_range, new_kind) =
                try_merge_with_next(&mut self.inner, new_range, new_kind);
            let (new_range, new_kind) =
                try_merge_with_prior(&mut self.inner, new_range, new_kind);
            self.inner.insert(new_range, new_kind);
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

fn remove_from_map_if<V, If>(map: &mut BTreeMap<Range, V>, key: Range, f: If)
-> Option<(Range, V)>
where If: FnOnce(&Range, &V) -> bool {
    match map.entry(key.clone()) {
        Occupied(o) => {
            if f(o.key(), o.get()) {
                Some(o.remove_entry())
            } else {
                None
            }
        },
        _ => panic!("Tried to remove bad range {:?}", key),
    }
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

    fn merge_with(self, other: Range) -> Range {
        let Range(my_first, my_last) = self;
        let Range(other_first, other_last) = other;

        match (my_first.cmp(&other_first), my_last.cmp(&other_last)) {
            // [{a ... b}]
            (Equal, Equal) => Range(my_first, my_last),

            // [a ... {b ... c}] => [a ... c]
            (Less, Equal) => Range(my_first, my_last),

            // {[a ... b]... c} => [a ... c]
            (Equal, Less) => Range(my_first, other_last),

            // {a ... [b ... c]} => [a ... c]
            (Greater, Equal) => Range(other_first, my_last),

            // [{a ... b} ... c] => [a ... c]
            (Equal, Greater) => Range(my_first, my_last),

            // [a ... {b ... c} ... d] => [a ... d]
            (Less, Greater) => Range(my_first, my_last),

            // [a ... {b ... c] ... d} => [a ... d]
            (Less, Less) => {
                assert!(other_first <= my_last + 1); // ! [a ... b] ... {c ... d}
                Range(my_first, other_last)
            },

            // {a ... [b ... c} ... d] => [a ... d]
            (Greater, Greater) => {
                assert!(my_first <= other_last + 1);// ! {a ... b} ... [c ... d]
                Range(other_first, my_last)
            },

            // {a ... [b ... c] ... d} => [a ... d]
            (Greater, Less) => Range(other_first, other_last)
        }
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

    //TODO replace with trim?
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

    // given ranges with partial overlap [a ... {b ... c] ... d} divides into two ranges
    // [a...b-1] {b...c}
    // given a range covered by this one, [a ... {b ... c} ... d] divides into three
    // [a...b-1] {b...c} [c+1...d]
    fn trim_out(self, other: &Range)  -> (Option<Range>, Range, Option<Range>) {
        let Range(my_first, my_last) = self;
        let &Range(other_first, other_last) = other;
        match (my_first.cmp(&other_first), my_last.cmp(&other_last)) {
            (Equal, Equal) => (None, Range(my_first, my_last), None),

            // [a ... {b ... c}] => [a ... b-1] {b ... c}
            (Less, Equal) =>
                (Some(Range(my_first, other_first - 1)), Range(other_first, my_last), None),

            // {[a ... b]... c} => {a ... b}
            (Equal, Less) => (None, Range(my_first, my_last), None),

            // {a ... [b ... c]} => {b ... c}
            (Greater, Equal) => (None, Range(my_first, my_last), None),

            // [{a ... b} ... c] => {a ... b} [b+1 ... c]
            (Equal, Greater) =>
                (None,
                    Range(other_first, other_last),
                    Some(Range(other_last + 1, my_last))),

            // [a ... {b ... c} ... d] => [a ... b-1] {b ... c} [c+1 ... d]
            (Less, Greater) =>
                (Some(Range(my_first, other_first - 1)),
                    Range(other_first, other_last),
                    Some(Range(other_first + 1, my_last))),

            // [a ... {b ... c] ... d} => [a ... b-1] {b ... c}
            (Less, Less) => {
                assert!(other_first <= my_last); // ! [a ... b]{b+1 ... d}
                (Some(Range(my_first, other_first - 1)),
                    Range(other_first, my_last),
                    None)
            },

            // {a ... [b ... c} ... d] => {b ... c} [c+1 ... d]
            (Greater, Greater) => {
                assert!(my_first <= other_last);// ! {a ... b}[b+1 ... d]
                (None,
                    Range(my_first, other_last),
                    Some(Range(other_last+1, my_last)))
            },

            // {a ... [b ... c] ... d} => {b ... c}
            (Greater, Less) => (None, Range(my_first, my_last), None),
        }
    }

    fn spans(&self, other: &Range) -> bool {
        let &Range(ref my_first, ref my_last) = self;
        let &Range(ref other_first, ref other_last) = other;
        my_first <= other_first && other_last <= my_last
    }

    pub fn first(&self) -> u64 {
        let &Range(first, _) = self;
        first
    }

    pub fn last(&self) -> u64 {
        let &Range(_, last) = self;
        last
    }

    pub fn len(&self) -> u64 {
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

macro_rules! impl_mergeable {
    ($($type:path),* $(,)*) => (
        $(
            impl Mergeable for $type {}
        )*
    )
}

impl_mergeable!(bool, i8, u8, i16, u16, i32, u32, i64, u64);

impl Mergeable for Vec<u8> {}

impl<T> Mergeable for Rc<T>
where T: Eq {
    fn can_merge_with_prior(self, other: &Self) -> Result<Self, Self> {
        if Rc::ptr_eq(&self, other) {
            return Ok(self)
        }
        if &self == other {
            if Rc::strong_count(&self) >= Rc::strong_count(other) {
                Ok(self)
            } else {
                Ok(other.clone())
            }
        } else {
            Err(self)
        }
    }

    fn can_merge_with_next(self, other: &Self) -> Result<Self, Self> {
        self.can_merge_with_prior(other)
    }
}

#[cfg(test)]
#[test]
fn test_range_trim() {
    // [a ... {b ... c} ... d] => [a ... b-1] {b ... c} [c+1 ... d]
    assert_eq!(
        Range(100, 200).trim_out(&Range(150, 175)),
        (Some(Range(100, 149)), Range(150, 175), Some(Range(176, 200)))
    );

    // [a ... {b ... c}] => [a ... b-1] {b ... c}
    assert_eq!(
        Range(100, 200).trim_out(&Range(150, 200)),
        (Some(Range(100, 149)), Range(150, 200), None)
    );

    // {[a ... b]... c} => {a ... b}
    assert_eq!(
        Range(100, 200).trim_out(&Range(100, 300)),
        (None, Range(100, 200), None)
    );

    // {a ... [b ... c]} => {b ... c}
    assert_eq!(
        Range(100, 200).trim_out(&Range(50, 200)),
        (None, Range(100, 200), None)
    );

    // [{a ... b} ... c] => {a ... b} [b+1 ... c]
    assert_eq!(
        Range(100, 200).trim_out(&Range(100, 150)),
        (None, Range(100, 150), Some(Range(151, 200)))
    );

    // [a ... {b ... c] ... d} => [a ... b-1] {b ... c}
    assert_eq!(
        Range(100, 200).trim_out(&Range(150, 300)),
        (Some(Range(100, 149)), Range(150, 200), None)
    );

    // {a ... [b ... c} ... d] => {b ... c} [c+1 ... d]
    assert_eq!(
        Range(100, 200).trim_out(&Range(50, 150)),
        (None, Range(100, 150), Some(Range(151, 200)))
    );

    // {a ... [b ... c] ... d} => {b ... c}
    assert_eq!(
        Range(100, 200).trim_out(&Range(50, 300)),
        (None, Range(100, 200), None)
    );
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        let mut tree = RangeTree::with_default_val(false);
        println!("{:?}", tree);
        {
            let mut iter = tree.iter();
            assert_eq!(iter.next(), Some((&Range::new(0, u64::MAX), &false)));
            assert_eq!(iter.next(), None);
        }
        assert_eq!(tree.get(0), &false);
        assert_eq!(tree.get(200), &false);
        assert_eq!(tree.get(201), &false);

        tree.set_point_as(200, true);

        println!("{:?}", tree);

        assert_eq!(tree.get(0), &false);
        assert_eq!(tree.get(200), &true);
        assert_eq!(tree.get(201), &false);
        {
            let mut iter = tree.iter();
            assert_eq!(iter.next(), Some((&Range::new(0, 199), &false)));
            assert_eq!(iter.next(), Some((&Range::new(200, 200), &true)));
            assert_eq!(iter.next(), Some((&Range::new(201, u64::MAX), &false)));
            assert_eq!(iter.next(), None);
        }

        tree.set_range_as(100, 300, true);

        for i in 0..400 {
            if i < 100 || i > 300 {
                assert_eq!(tree.get(i), &false, "{}", i);
            } else {
                assert_eq!(tree.get(i), &true);
            }
        }

        println!("{:?}", tree);
    }
}
