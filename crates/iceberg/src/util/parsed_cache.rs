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

//! A bounded cache of values parsed from byte strings.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, PoisonError, RwLock};

use crate::Result;

/// How many parsed values a [`ParsedCache`] keeps. The manifests of a table
/// version repeat a handful of distinct schemas; a long-lived process that
/// reads many tables re-parses occasionally instead of growing without bound.
const MAX_ENTRIES: usize = 32;

/// Values parsed from byte strings, keyed by the exact bytes they came from.
///
/// Keying by the bytes rather than a digest makes a wrong hit impossible, and
/// hashing a few kilobytes costs microseconds against the parse it replaces.
/// Meant for a `static`: the manifests of one table version carry the same
/// schema bytes, and engines that plan each query from a fresh table object
/// would find a per-table cache cold.
#[derive(Debug)]
pub(crate) struct ParsedCache<T> {
    entries: LazyLock<RwLock<HashMap<Vec<u8>, Arc<T>>>>,
}

impl<T> ParsedCache<T> {
    /// An empty cache.
    pub(crate) const fn new() -> Self {
        Self {
            entries: LazyLock::new(Default::default),
        }
    }

    /// The value parsed from `bytes`, from the cache when `bytes` was parsed
    /// before and otherwise from `parse`. A parse error is returned and not
    /// cached.
    pub(crate) fn get_or_parse(
        &self,
        bytes: &[u8],
        parse: impl FnOnce() -> Result<T>,
    ) -> Result<Arc<T>> {
        if let Some(value) = self
            .entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(bytes)
        {
            return Ok(value.clone());
        }
        let value = Arc::new(parse()?);
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        if entries.len() >= MAX_ENTRIES {
            entries.clear();
        }
        // Another thread may have parsed the same bytes meanwhile; either copy
        // will do.
        Ok(entries.entry(bytes.to_vec()).or_insert(value).clone())
    }

    /// Looks `bytes` up twice in a row and returns both results. A `static`
    /// cache is shared by tests running in parallel, so another test's miss
    /// can clear it between the two lookups; the pair is retried until it
    /// agrees so a pointer-identity assertion only fails when the cache never
    /// hits.
    #[cfg(test)]
    pub(crate) fn get_or_parse_twice(
        &self,
        bytes: &[u8],
        parse: impl Fn() -> Result<T>,
    ) -> (Arc<T>, Arc<T>) {
        const ATTEMPTS: usize = 100;
        let mut pair = None;
        for _ in 0..ATTEMPTS {
            let first = self.get_or_parse(bytes, &parse).unwrap();
            let second = self.get_or_parse(bytes, &parse).unwrap();
            let hit = Arc::ptr_eq(&first, &second);
            pair = Some((first, second));
            if hit {
                break;
            }
        }
        pair.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::invalid_data;

    fn parse_len(bytes: &[u8]) -> Result<usize> {
        if bytes.is_empty() {
            return Err(invalid_data!("empty"));
        }
        Ok(bytes.len())
    }

    #[test]
    fn test_get_or_parse_returns_same_value_for_same_bytes() {
        let cache = ParsedCache::new();

        let (first, second) = cache.get_or_parse_twice(b"abc", || parse_len(b"abc"));

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(*first, 3);
    }

    #[test]
    fn test_get_or_parse_keeps_different_bytes_apart() {
        let cache = ParsedCache::new();

        let first = cache.get_or_parse(b"abc", || parse_len(b"abc")).unwrap();
        let second = cache.get_or_parse(b"abcd", || parse_len(b"abcd")).unwrap();

        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!((*first, *second), (3, 4));
    }

    #[test]
    fn test_get_or_parse_does_not_cache_errors() {
        let cache = ParsedCache::new();

        let err = cache.get_or_parse(b"", || parse_len(b"")).unwrap_err();
        let parsed = cache.get_or_parse(b"", || Ok(0)).unwrap();

        assert_eq!(err.kind(), crate::ErrorKind::DataInvalid);
        assert_eq!(*parsed, 0);
    }

    #[test]
    fn test_get_or_parse_clears_at_bound() {
        let cache = ParsedCache::new();
        let keys: Vec<Vec<u8>> = (0..=MAX_ENTRIES).map(|i| vec![0; i + 1]).collect();
        let first = cache
            .get_or_parse(&keys[0], || parse_len(&keys[0]))
            .unwrap();
        for key in &keys[1..MAX_ENTRIES] {
            cache.get_or_parse(key, || parse_len(key)).unwrap();
        }
        assert_eq!(cache.entries.read().unwrap().len(), MAX_ENTRIES);

        cache
            .get_or_parse(&keys[MAX_ENTRIES], || parse_len(&keys[MAX_ENTRIES]))
            .unwrap();

        assert_eq!(cache.entries.read().unwrap().len(), 1);
        let again = cache
            .get_or_parse(&keys[0], || parse_len(&keys[0]))
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &again));
        assert_eq!(first, again);
    }
}
