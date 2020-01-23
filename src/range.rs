// Copyright (c) 2018 10x Genomics, Inc. All rights reserved.

use crate::ShardRecord;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Rorder {
    Before,
    Intersects,
    After,
}

/// A range of points over the type `K`, spanning the half-open interval [`start`, `end`). A value
/// of `None` indicates that the interval is unbounded in that direction.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct Range<K> {
    /// Start of interval
    pub start: Option<K>,

    /// Inclusive end of interval
    pub end: Option<K>,
}

impl<K: Ord + Clone> Range<K> {
    /// Create a Range object spanning the half-open interval [`start`, `end)
    pub fn new(start: K, end: K) -> Range<K> {
        Range {
            start: Some(start),
            end: Some(end),
        }
    }

    /// Create a range containing all points greater than or equal to `start`
    pub fn starts_at(start: K) -> Range<K> {
        Range {
            start: Some(start),
            end: None,
        }
    }

    /// Create a range containing all points less than `end`
    pub fn ends_at(end: K) -> Range<K> {
        Range {
            start: None,
            end: Some(end),
        }
    }

    /// Create a range covering all points.
    pub fn all() -> Range<K> {
        Range {
            start: None,
            end: None,
        }
    }

    #[inline]
    /// Test if `point` in contained in the range `self`
    pub fn contains(&self, point: &K) -> bool {
        let after_start = self.start.as_ref().map_or(true, |s| point >= s);
        let before_end = self.end.as_ref().map_or(true, |e| point < e);
        after_start && before_end
    }

    #[inline]
    /// Test if Range `other` intersects Range `self`
    pub fn intersects(&self, other: &Range<K>) -> bool {
        self.start < other.start && Range::se_lt(&other.start, &self.end)
            || self.start >= other.start && Range::se_lt(&self.start, &other.end)
    }

    #[inline]
    /// Test if Range `other` intersects Range `self`
    pub(crate) fn intersects_shard(&self, shard: &ShardRecord<K>) -> bool {
        self.start < Some(shard.start_key.clone())
            && Range::se_lt(&Some(shard.start_key.clone()), &self.end)
            || self.start >= Some(shard.start_key.clone())
                && Range::shard_le(&self.start, &shard.end_key)
    }

    // is `start` position s1 less then inclusive `end` position e2
    #[inline]
    fn shard_le(inclusive_start: &Option<K>, inclusive_end: &K) -> bool {
        match (inclusive_start, inclusive_end) {
            (&Some(ref v1), ref v2) => v1 <= v2,
            (&None, _) => true,
        }
    }

    pub(crate) fn cmp(&self, point: &K) -> Rorder {
        if self.contains(point) {
            Rorder::Intersects
        } else if self.start.as_ref().map_or(false, |ref s| point < &s) {
            Rorder::Before
        } else {
            Rorder::After
        }
    }

    /// Ensure that  start <= end
    pub fn is_valid(&self) -> bool {
        match (&self.start, &self.end) {
            (Some(s), Some(e)) => s <= e,
            _ => true
        }
    }

    // is inclusive `start` position s1 less than exclusive `end` position e2
    #[inline]
    fn se_lt(s1: &Option<K>, e2: &Option<K>) -> bool {
        match (s1, e2) {
            (&Some(ref v1), &Some(ref v2)) => v1 < v2,
            (&Some(_), &None) => true,
            (&None, &Some(_)) => true,
            (&None, &None) => true,
        }
    }
}

#[cfg(test)]
mod range_tests {
    use super::*;

    #[test]
    fn test_range() {
        let r1 = Range::starts_at(10);
        let r2 = Range::ends_at(10);
        assert_eq!(r1.intersects(&r2), false);
        assert_eq!(r2.intersects(&r1), false);

        let r3 = Range::new(5, 10);
        assert_eq!(r1.intersects(&r3), false);
        assert_eq!(r3.intersects(&r1), false);

        let r4 = Range::new(10, 15);
        assert_eq!(r2.intersects(&r4), false);
        assert_eq!(r4.intersects(&r2), false);

        assert_eq!(r3.intersects(&r4), false);
        assert_eq!(r4.intersects(&r3), false);

        assert_eq!(r2.intersects(&r3), true);
        assert_eq!(r3.intersects(&r2), true);
    }

    #[test]
    fn test_shard_cmp() {
        let def = ShardRecord {
            start_key: 0,
            end_key: 0,
            offset: 0,
            len_bytes: 0,
            len_items: 0,
        };

        let r1 = Range::new(5, 10);
        let s1 = ShardRecord {
            start_key: 0,
            end_key: 5,
            ..def
        };
        assert_eq!(r1.intersects_shard(&s1), true);

        let s2 = ShardRecord {
            start_key: 0,
            end_key: 4,
            ..def
        };
        assert_eq!(r1.intersects_shard(&s2), false);

        let s3 = ShardRecord {
            start_key: 10,
            end_key: 12,
            ..def
        };
        assert_eq!(r1.intersects_shard(&s3), false);

        let s4 = ShardRecord {
            start_key: 9,
            end_key: 12,
            ..def
        };
        assert_eq!(r1.intersects_shard(&s4), true);
    }

    #[test]
    fn test_range_cmp() {
        let p = 10;
        let r1 = Range::starts_at(11);
        assert_eq!(r1.cmp(&p), Rorder::Before);

        let p = 10;
        let r1 = Range::starts_at(8);
        assert_eq!(r1.cmp(&p), Rorder::Intersects);

        let p = 10;
        let r1 = Range::ends_at(8);
        assert_eq!(r1.cmp(&p), Rorder::After);
    }
}
