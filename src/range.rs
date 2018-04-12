// Copyright (c) 2018 10x Genomics, Inc. All rights reserved.

/// A range of points over the type `K`, spanning the half-open interval [`start`, `end`). A value
/// of `None` indicates that the interval is unbounded in that direction.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Range<K> {
    pub start: Option<K>,
    pub end: Option<K>,
}


impl<K: Ord> Range<K> {
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
            end: None
        }
    }

    /// Create a range containing all points less than `end`
    pub fn ends_at(end: K) -> Range<K> {
        Range {
            start: None,
            end: Some(end)
        }
    }

    /// Create a range covering all points.
    pub fn all() -> Range<K> {
        Range { start: None, end: None }
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
        self.start <  other.start && Range::se_lt(&other.start, &self.end) ||
           self.start >= other.start && Range::se_lt(&self.start, &other.end) 
    }

    // is `start` position s1 less then `end` position e2
    fn se_lt(s1: &Option<K>, e2 : &Option<K>) -> bool {
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

        assert_eq!(r2.intersects(&r3), true);
        assert_eq!(r3.intersects(&r2), true)
    }
}

