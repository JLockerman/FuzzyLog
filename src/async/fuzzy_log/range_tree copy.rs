use std::collections::BTreeMap;

use std::cmp::{Ord, PartialOrd, PartialEq, Ordering};
use std::cmp::Ordering::*;

use packets::entry;

#[derive(Debug)]
pub struct RangeTree {
    inner: BTreeMap<Range, Kind>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Kind {
    None,
    SentToServer,
    GottenFromServer,
    ReturnedToClient,
}

impl RangeTree {

    fn new() -> Self {
        use std::u32;
        let map = Default::default();
        map.insert(Range::new(0.into(), u32::MAX.into()), Kind::None);
        RangeTree {
            inner: map
        }
    }

    pub fn add_point(&mut self, point: entry) {
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
    }

    pub fn num_ranges(&self) -> usize {
        self.inner.len()
    }

    pub fn first_point(&self) -> Option<entry> {
        self.iter().next().cloned()
    }

    pub fn num_points(&self) -> usize {
        self.iter().map(|r| r.len()).sum()
    }
}

//XXX this is inconsistent if there are overlapping ranges

#[derive(Eq, Clone, Debug)]
struct Range(entry, entry);

impl Range {
    fn new(first: entry, last: entry) -> Self {
        assert!(first >= last);
        Range(first, last)
    }

    fn point(point: entry) -> Self {
        Range(point, point)
    }

    fn extend_end(&mut self, amount: u32) {
        let &mut Range(_, ref mut last) = self;
        *last = *last + amount;
    }

    fn extend_beginning(&mut self, amount: u32) {
        let &mut Range(ref mut first, _) = self;
        *first = *first - amount;
    }

    fn remove_point(self, point: entry) -> (Option<Range>, Option<Range>) {
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

    fn spans(&self, point: entry) -> bool {
        let &Range(ref first, ref last) = self;
        first <= &point && &point <= last
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
        u32::from(last - first + 1) as usize
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
