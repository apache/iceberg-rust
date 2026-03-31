// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::ops::BitOrAssign;

use roaring::RoaringTreemap;
use roaring::bitmap::Iter;
use roaring::treemap::BitmapIter;

use crate::{Error, ErrorKind, Result};

#[derive(Debug, Default)]
pub struct DeleteVector {
    inner: RoaringTreemap,
}

impl DeleteVector {
    #[allow(unused)]
    pub fn new(roaring_treemap: RoaringTreemap) -> DeleteVector {
        DeleteVector {
            inner: roaring_treemap,
        }
    }

    pub fn iter(&self) -> DeleteVectorIterator<'_> {
        let outer = self.inner.bitmaps();
        DeleteVectorIterator { outer, inner: None }
    }

    pub fn insert(&mut self, pos: u64) -> bool {
        self.inner.insert(pos)
    }

    /// Inserts all positions in the range [start, end) into the delete vector.
    /// If start == end, this method does nothing and returns 0.
    ///
    /// # Panics
    ///
    /// Panics if start > end (a reversed range indicates a bug in the caller).
    ///
    /// Returns the number of newly inserted positions.
    #[allow(unused)]
    pub fn insert_range(&mut self, start: u64, end: u64) -> u64 {
        assert!(
            start <= end,
            "insert_range requires start <= end, got [{start}, {end})"
        );
        if start == end {
            return 0;
        }
        self.inner.insert_range(start..end)
    }

    /// Marks the given `positions` as deleted and returns the number of elements appended.
    ///
    /// The input slice must be strictly ordered in ascending order, and every value must be greater than all existing values already in the set.
    ///
    /// # Errors
    ///
    /// Returns an error if the precondition is not met.
    #[allow(dead_code)]
    pub fn insert_positions(&mut self, positions: &[u64]) -> Result<usize> {
        if let Err(err) = self.inner.append(positions.iter().copied()) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "failed to marks rows as deleted".to_string(),
            )
            .with_source(err));
        }
        Ok(positions.len())
    }

    /// Returns true if the given position is present in the delete vector.
    #[allow(unused)]
    pub fn contains(&self, pos: u64) -> bool {
        self.inner.contains(pos)
    }

    #[allow(unused)]
    pub fn len(&self) -> u64 {
        self.inner.len()
    }
}

// Ideally, we'd just wrap `roaring::RoaringTreemap`'s iterator, `roaring::treemap::Iter` here.
// But right now, it does not have a corresponding implementation of `roaring::bitmap::Iter::advance_to`,
// which is very handy in ArrowReader::build_deletes_row_selection.
// There is a PR open on roaring to add this (https://github.com/RoaringBitmap/roaring-rs/pull/314)
// and if that gets merged then we can simplify `DeleteVectorIterator` here, refactoring `advance_to`
// to just a wrapper around the underlying iterator's method.
pub struct DeleteVectorIterator<'a> {
    // NB: `BitMapIter` was only exposed publicly in https://github.com/RoaringBitmap/roaring-rs/pull/316
    // which is not yet released. As a consequence our Cargo.toml temporarily uses a git reference for
    // the roaring dependency.
    outer: BitmapIter<'a>,
    inner: Option<DeleteVectorIteratorInner<'a>>,
}

struct DeleteVectorIteratorInner<'a> {
    high_bits: u32,
    bitmap_iter: Iter<'a>,
}

impl Iterator for DeleteVectorIterator<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = &mut self.inner
            && let Some(inner_next) = inner.bitmap_iter.next()
        {
            return Some(u64::from(inner.high_bits) << 32 | u64::from(inner_next));
        }

        if let Some((high_bits, next_bitmap)) = self.outer.next() {
            self.inner = Some(DeleteVectorIteratorInner {
                high_bits,
                bitmap_iter: next_bitmap.iter(),
            })
        } else {
            return None;
        }

        self.next()
    }
}

impl DeleteVectorIterator<'_> {
    pub fn advance_to(&mut self, pos: u64) {
        let hi = (pos >> 32) as u32;
        let lo = pos as u32;

        let Some(ref mut inner) = self.inner else {
            return;
        };

        while inner.high_bits < hi {
            let Some((next_hi, next_bitmap)) = self.outer.next() else {
                return;
            };

            *inner = DeleteVectorIteratorInner {
                high_bits: next_hi,
                bitmap_iter: next_bitmap.iter(),
            }
        }

        inner.bitmap_iter.advance_to(lo);
    }
}

impl BitOrAssign for DeleteVector {
    fn bitor_assign(&mut self, other: Self) {
        self.inner.bitor_assign(&other.inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insertion_and_iteration() {
        let mut dv = DeleteVector::default();
        assert!(dv.insert(42));
        assert!(dv.insert(100));
        assert!(!dv.insert(42));

        let mut items: Vec<u64> = dv.iter().collect();
        items.sort();
        assert_eq!(items, vec![42, 100]);
        assert_eq!(dv.len(), 2);
    }

    #[test]
    fn test_successful_insert_positions() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 2, 3, 1000, 1 << 33];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 5);

        let mut collected: Vec<u64> = dv.iter().collect();
        collected.sort();
        assert_eq!(collected, positions);
    }

    /// Testing scenario: bulk insertion fails because input positions are not strictly increasing.
    #[test]
    fn test_failed_insertion_unsorted_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 4];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have intersection with existing ones.
    #[test]
    fn test_failed_insertion_with_intersection() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5];
        assert_eq!(dv.insert_positions(&positions).unwrap(), 3);

        let res = dv.insert_positions(&[2, 4]);
        assert!(res.is_err());
    }

    /// Testing scenario: bulk insertion fails because input positions have duplicates.
    #[test]
    fn test_failed_insertion_duplicate_elements() {
        let mut dv = DeleteVector::default();
        let positions = vec![1, 3, 5, 5];
        let res = dv.insert_positions(&positions);
        assert!(res.is_err());
    }

    #[test]
    fn test_insert_range_single_key() {
        let mut dv = DeleteVector::default();
        assert_eq!(dv.insert_range(10, 20), 10);
        assert_eq!(dv.len(), 10);
        for pos in 10..20 {
            assert!(dv.iter().any(|p| p == pos), "missing {pos}");
        }
        assert!(!dv.iter().any(|p| p == 9));
        assert!(!dv.iter().any(|p| p == 20));
    }

    #[test]
    fn test_insert_range_single_position() {
        let mut dv = DeleteVector::default();
        assert_eq!(dv.insert_range(42, 43), 1);
        assert_eq!(dv.len(), 1);
        assert!(dv.iter().any(|p| p == 42));
        assert!(!dv.iter().any(|p| p == 41));
        assert!(!dv.iter().any(|p| p == 43));
    }

    #[test]
    fn test_insert_range_across_keys() {
        let mut dv = DeleteVector::default();
        let start = (1u64 << 32) - 5;
        let end = (1u64 << 32) + 5;
        assert_eq!(dv.insert_range(start, end), 10);
        assert_eq!(dv.len(), 10);
        for pos in start..end {
            assert!(dv.iter().any(|p| p == pos), "missing {pos}");
        }
        assert!(!dv.iter().any(|p| p == start - 1));
        assert!(!dv.iter().any(|p| p == end));
    }

    #[test]
    fn test_insert_range_spanning_three_keys() {
        let mut dv = DeleteVector::default();
        let start = 0xFFFFFFF0u64;
        let end = (2u64 << 32) | 0x10;
        let inserted = dv.insert_range(start, end);
        assert_eq!(inserted, end - start);
        assert_eq!(dv.len(), end - start);
        assert!(dv.contains(start));
        assert!(dv.contains(end - 1));
        assert!(dv.contains(1u64 << 32));
        assert!(dv.contains((1u64 << 32) | 0xFFFFFFF0));
        assert!(!dv.contains(start - 1));
        assert!(!dv.contains(end));
    }

    #[test]
    fn test_insert_range_empty_when_start_equals_end() {
        let mut dv = DeleteVector::default();
        assert_eq!(dv.insert_range(100, 100), 0);
        assert_eq!(dv.len(), 0);
    }

    #[test]
    #[should_panic(expected = "insert_range requires start <= end")]
    fn test_insert_range_reversed_panics() {
        let mut dv = DeleteVector::default();
        dv.insert_range(100, 50);
    }

    #[test]
    fn test_insert_range_large_contiguous() {
        let mut dv = DeleteVector::default();
        assert_eq!(dv.insert_range(500, 200_500), 200_000);
        assert_eq!(dv.len(), 200_000);
        assert!(dv.iter().any(|p| p == 500));
        assert!(dv.iter().any(|p| p == 200_499));
        assert!(!dv.iter().any(|p| p == 499));
        assert!(!dv.iter().any(|p| p == 200_500));
    }
}
