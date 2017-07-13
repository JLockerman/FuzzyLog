use std::collections::BTreeMap;

use std::cmp::{Ord, PartialOrd, PartialEq, Ordering};
use std::cmp::Ordering::*;

use std::u32;

use std::collections::btree_map::Iter;
use std::collections::btree_map::Entry::Occupied;

use packets::entry;

#[derive(Debug)]
pub struct RangeTree {
    inner: BTreeMap<Range, Kind>,
    num_outstanding: usize,
    num_buffered: usize,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Kind {
    None,
    SentToServer,
    GottenFromServer,
    ReturnedToClient,
    Skip,
}

impl Kind {
    fn is_prior(self, other: Self) -> bool {
        match (self, other) {
            (Kind::None, _) => true,
            (Kind::SentToServer, _) => true,
            (Kind::GottenFromServer, Kind::None) => true,
            (Kind::GottenFromServer, Kind::ReturnedToClient) => true,
            (Kind::GottenFromServer, Kind::Skip) => true,
            (Kind::Skip, Kind::ReturnedToClient) => true,
            (_, _) => false,
        }
    }
}

impl RangeTree {

    pub fn new() -> Self {
        let mut map = BTreeMap::new();
        map.insert(Range::new(0.into(), 0.into()), Kind::ReturnedToClient);
        map.insert(Range::new(1.into(), u32::MAX.into()), Kind::None);
        RangeTree {
            inner: map,
            num_outstanding: 0,
            num_buffered: 0,
        }
    }

    pub fn set_point_as_none(&mut self, point: entry) {
        debug_assert!(self.tree_invariant(), "invariant failed @ {:#?}", self);
        self.set_point_as(point, Kind::None);
        debug_assert!(self.tree_invariant(), "invariant failed @ {:#?}", self);
    }

    pub fn set_point_as_skip(&mut self, point: entry) {
        debug_assert!(self.tree_invariant(), "invariant failed @ {:#?}", self);
        self.set_point_as(point, Kind::Skip);
        debug_assert!(self.tree_invariant(), "invariant failed @ {:#?}", self);
    }

    /*
    pub fn set_point_as_sent(&mut self, point: entry) {
        self.set_point_as(point, Kind::SentToServer);
        self.num_outstanding += 1;
    }*/

    pub fn set_point_as_recvd(&mut self, point: entry) -> bool {
        debug_assert!(self.tree_invariant(), "invariant failed @ {:#?}", self);
        let r = self.set_point_as(point, Kind::GottenFromServer);
        debug_assert!(self.tree_invariant(), "invariant failed @ {:#?}", self);
        r
    }

    pub fn set_point_as_returned(&mut self, point: entry) {
        debug_assert!(self.tree_invariant(), "invariant failed @ {:#?}", self);
        let worked = self.set_point_as(point, Kind::ReturnedToClient);
        assert!(worked, "bad return @ {:?} in {:#?}", point, self);
        debug_assert!(self.tree_invariant(), "invariant failed @ {:#?}", self);
    }

    fn set_point_as(&mut self, point: entry, new_kind: Kind) -> bool {
        let (old_range, old_kind) = remove_from_map(&mut self.inner, Range::point(point));
        if !old_kind.is_prior(new_kind) {
            self.inner.insert(old_range, old_kind);
            return false
        }

        match old_kind {
            Kind::SentToServer => self.num_outstanding -= 1,
            Kind::GottenFromServer => self.num_buffered -= 1,
            _ => {},
        }

        match new_kind {
            Kind::SentToServer => self.num_outstanding += 1,
            Kind::GottenFromServer => self.num_buffered += 1,
            _ => {},
        }

        match old_range.split_at(point) {
            (Some(pre), Some(post)) => {
                self.inner.insert(Range::point(point), new_kind);
                self.inner.insert(pre, old_kind);
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

    pub fn set_range_as_sent(&mut self, low: entry, high: entry) {
        debug_assert!(self.tree_invariant(), "invariant failed @ {:#?}", self);
        let new_range = Range::new(low, high);
        let (old_range, old_kind) = remove_from_map(&mut self.inner, Range::point(low));
        assert_eq!(old_kind, Kind::None);
        assert!(old_range.spans(&new_range));

        self.num_outstanding += new_range.len();

        let new_kind = Kind::SentToServer;
        match old_range.remove(&new_range) {
            (Some(pre), Some(post)) => {
                self.inner.insert(new_range, new_kind);
                self.inner.insert(pre, old_kind);
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
        debug_assert!(self.tree_invariant(), "invariant failed @ {:#?}", self);
    }

    pub fn is_returned(&self, point: entry) -> bool {
        match self.inner.get(&Range::point(point)) {
            Some(&Kind::ReturnedToClient) => true,
            _ => false,
        }
    }

    pub fn next_return_is(&self, point: entry) -> bool {
        match (self.inner.get(&Range::point(point - 1)), self.inner.get(&Range::point(point))) {
            (_, Some(&Kind::ReturnedToClient)) => false,
            (Some(&Kind::ReturnedToClient), _) => true,
            _ => false,
        }
    }

    pub fn num_buffered(&self) -> usize {
        self.num_buffered
    }

    pub fn num_outstanding(&self) -> usize {
        self.num_outstanding
    }

    fn iter(&self) -> Iter<Range, Kind> {
        self.inner.iter()
    }

    pub fn first_outstanding(&self) -> Option<entry> {
        self.iter()
            .filter(|&(_, &k)| k == Kind::SentToServer)
            .next()
            .map(|(r, _)| r.first())
    }

    #[allow(dead_code)]
    pub fn last_outstanding(&self) -> Option<entry> {
        self.iter().rev()
            .filter(|&(_, &k)| k == Kind::SentToServer)
            .next()
            .map(|(r, _)| r.last())
    }

    pub fn min_range_to_fetch(&self) -> (u32, u32) {
        self.iter().filter(|&(_, &k)| k == Kind::None)
            .map(|(r, _)| (r.first().into(), r.last().into()))
            .next().unwrap_or((0, 0))
    }

    /*pub fn add_point(&mut self, point: entry) {
        if self.inner.contains(&Range::point(point)) {
            return
        }

        debug_assert!(point > 0.into());
        if let Some(pre) = self.inner.take(&Range::point(point - 1)) {
            pre.extend_end(1);
            debug_assert!(pre.spans(point));
            debug_assert!(pre.last() == point);
            self.inner.insert(pre);
            return
        }

        //FIXME overflow
        if let Some(post) = self.inner.take(&Range::point(point + 1)) {
            post.extend_beginning(1);
            debug_assert!(post.spans(point));
            debug_assert!(post.first() == point);
            self.inner.insert(post);
            return
        }

        self.inner.insert(Range::point(point));
    }

    pub fn remove_point(&mut self, point: entry) -> bool {
        let old_span = self.inner.take(&Range::point(point));
        old_span.map(|old_span| {
            debug_assert!(old_span.spans(point));
            let (less, greater) = old_span.remove_point(point);
            if let Some(less) = less {
                debug_assert!(less.last() == point - 1);
                self.inner.insert(less);
            }
            if let Some(greater) = greater {
                debug_assert!(greater.first() == point + 1);
                self.inner.insert(greater);
            }
            true
        }).unwrap_or(false)
    }*/

    #[allow(dead_code)]
    fn tree_invariant(&self) -> bool {
        let (_, _, valid, num_outstanding, num_buffered) = self.iter()
            .fold((0.into(), None, true, 0, 0),
            |(prev, prev_kind, prev_valid, mut num_outstanding, mut num_buffered), (r, k)| {
            if let Some(prev_kind) = prev_kind {
                let valid = prev_valid && r.first() == prev + 1 && k != prev_kind;
                match k {
                    &Kind::SentToServer => num_outstanding += r.len(),
                    &Kind::GottenFromServer => num_buffered += r.len(),
                    _ => {},
                }
                (r.last(), Some(k), valid, num_outstanding, num_buffered)
            }
            else {
                let valid = prev_valid && r.first() == 0.into() && k == &Kind::ReturnedToClient;
                (r.last(), Some(k), valid, num_outstanding, num_buffered)
            }
        });
        valid
            && self.num_buffered() == num_buffered
            && self.num_outstanding() == num_outstanding
    }
}

fn remove_from_map(map: &mut BTreeMap<Range, Kind>, key: Range) -> (Range, Kind) {
    match map.entry(key.clone()) {
        Occupied(o) => Some(o.remove_entry()),
        _ => None,
    }.unwrap_or_else(|| panic!("Tried to remove bad range {:?} from {:#?}", key, map))
}

fn try_merge_with_next(
    inner: &mut BTreeMap<Range, Kind>, current: Range, our_kind: Kind
) -> (Range, Kind) {
    use self::Kind::*;
    if current.last() >= u32::MAX.into() {
        return (current, our_kind)
    }

    match inner.entry(Range::point(current.last() + 1)) {
        Occupied(entr) => {
            let other_kind = *entr.get();
            let merge_skip =
                (our_kind == Skip && other_kind == ReturnedToClient)
                || (our_kind == ReturnedToClient && other_kind == Skip);
            if other_kind == our_kind {
                let merge = entr.remove_entry().0;
                (current.merge_with_next(merge), our_kind)
            } else if merge_skip {
                let merge = entr.remove_entry().0;
                (current.merge_with_next(merge), ReturnedToClient)
            } else {
                (current, our_kind)
            }
        },
        _ => (current, our_kind)
    }
}

fn try_merge_with_prior(
    inner: &mut BTreeMap<Range, Kind>, current: Range, our_kind: Kind
) -> (Range, Kind) {
    use self::Kind::*;
    if current.first() <= 0.into() {
        return (current, our_kind)
    }

    match inner.entry(Range::point(current.first() - 1)) {
        Occupied(entr) => {
            let other_kind = *entr.get();
            let merge_skip =
                (our_kind == Skip && other_kind == ReturnedToClient)
                || (our_kind == ReturnedToClient && other_kind == Skip);
            if other_kind == our_kind {
                let merge = entr.remove_entry().0;
                (current.merge_with_prior(merge), our_kind)
            } else if merge_skip {
                let merge = entr.remove_entry().0;
                (current.merge_with_prior(merge), ReturnedToClient)
            } else {
                (current, our_kind)
            }
        },
        _ => (current, our_kind)
    }
}

//XXX this is inconsistent if there are overlapping ranges

#[derive(Eq, Clone, Debug)]
struct Range(entry, entry);

impl Range {
    fn new(first: entry, last: entry) -> Self {
        assert!(first <= last, "{:?} >= {:?}", first, last);
        Range(first, last)
    }

    fn point(point: entry) -> Self {
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
    fn extend_end(&mut self, amount: u32) {
        let &mut Range(_, ref mut last) = self;
        *last = *last + amount;
    }

    fn extend_beginning(&mut self, amount: u32) {
        let &mut Range(ref mut first, _) = self;
        *first = *first - amount;
    }*/

    fn split_at(self, point: entry) -> (Option<Range>, Option<Range>) {
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

    fn first(&self) -> entry {
        let &Range(first, _) = self;
        first
    }

    fn last(&self) -> entry {
        let &Range(_, last) = self;
        last
    }

    fn len(&self) -> usize {
        let &Range(first, last) = self;
        u32::from(last - first.into() + 1) as usize
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

    #[test]
    fn overread() {
        let inner = [
            (Range::new(0.into(), 54.into()), Kind::ReturnedToClient),
            (Range::new(55.into(), 55.into()), Kind::GottenFromServer),
            (Range::new(56.into(), 134.into()), Kind::SentToServer),
            (Range::new(135.into(), 4294967295.into()), Kind::None)
            ].iter().cloned().collect();
        let mut tree = RangeTree {
            inner: inner, num_outstanding: 79, num_buffered: 1
        };
        tree.set_point_as_none(55.into());
        println!("{:?}", tree);
        assert!(false);
    }
}
