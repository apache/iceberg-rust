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

use std::hash::Hash;
use std::mem::size_of_val;
use std::sync::Arc;

use iceberg::cache::{ObjectCache, ObjectCacheProvide};
use iceberg::spec::{Manifest, ManifestList};

const DEFAULT_CACHE_SIZE_BYTES: u64 = 32 * 1024 * 1024; // 32MiB

/// A cache whose `max_capacity` is a byte budget rather than an entry count.
///
/// Without a weigher `moka` treats `max_capacity` as a number of entries, so passing a byte
/// figure to `Cache::new` leaves the cache effectively unbounded. This mirrors the weigher
/// `iceberg::io::ObjectCache` uses.
fn byte_bounded_cache<V>(max_capacity_bytes: u64) -> moka::sync::Cache<String, Arc<V>>
where V: Send + Sync + 'static {
    moka::sync::Cache::builder()
        .weigher(|_, value: &Arc<V>| size_of_val(value.as_ref()) as u32)
        .max_capacity(max_capacity_bytes)
        .build()
}

struct MokaObjectCache<K, V>(moka::sync::Cache<K, V>);

impl<K, V> ObjectCache<K, V> for MokaObjectCache<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn get(&self, key: &K) -> Option<V> {
        self.0.get(key)
    }

    fn set(&self, key: K, value: V) {
        self.0.insert(key, value);
    }
}

/// A cache provider that uses Moka for caching objects.
pub struct MokaObjectCacheProvider {
    manifest_cache: MokaObjectCache<String, Arc<Manifest>>,
    manifest_list_cache: MokaObjectCache<String, Arc<ManifestList>>,
}

impl Default for MokaObjectCacheProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MokaObjectCacheProvider {
    /// Creates a new `MokaObjectCacheProvider` with default cache sizes.
    pub fn new() -> Self {
        let manifest_cache = MokaObjectCache(byte_bounded_cache(DEFAULT_CACHE_SIZE_BYTES));
        let manifest_list_cache = MokaObjectCache(byte_bounded_cache(DEFAULT_CACHE_SIZE_BYTES));

        Self {
            manifest_cache,
            manifest_list_cache,
        }
    }

    /// Set the cache for manifests.
    pub fn with_manifest_cache(mut self, cache: moka::sync::Cache<String, Arc<Manifest>>) -> Self {
        self.manifest_cache = MokaObjectCache(cache);
        self
    }

    /// Set the cache for manifest lists.
    pub fn with_manifest_list_cache(
        mut self,
        cache: moka::sync::Cache<String, Arc<ManifestList>>,
    ) -> Self {
        self.manifest_list_cache = MokaObjectCache(cache);
        self
    }
}

impl ObjectCacheProvide for MokaObjectCacheProvider {
    fn manifest_cache(&self) -> &dyn ObjectCache<String, Arc<Manifest>> {
        &self.manifest_cache
    }

    fn manifest_list_cache(&self) -> &dyn ObjectCache<String, Arc<ManifestList>> {
        &self.manifest_list_cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cache with no weigher gives every entry weight 1, so `weighted_size` would be the
    /// entry count and a byte figure for `max_capacity` would admit 33_554_432 manifests.
    #[test]
    fn test_cache_weighs_entries_by_size_not_count() {
        let cache = byte_bounded_cache::<[u8; 512]>(DEFAULT_CACHE_SIZE_BYTES);
        cache.insert("a".to_string(), Arc::new([0u8; 512]));
        cache.insert("b".to_string(), Arc::new([0u8; 512]));
        cache.run_pending_tasks();

        assert_eq!(cache.entry_count(), 2);
        assert_eq!(cache.weighted_size(), 2 * 512);
        assert_eq!(
            cache.policy().max_capacity(),
            Some(DEFAULT_CACHE_SIZE_BYTES)
        );
    }

    #[test]
    fn test_default_provider_caches_are_byte_bounded() {
        let provider = MokaObjectCacheProvider::new();

        for max in [
            provider.manifest_cache.0.policy().max_capacity(),
            provider.manifest_list_cache.0.policy().max_capacity(),
        ] {
            assert_eq!(max, Some(DEFAULT_CACHE_SIZE_BYTES));
        }
    }
}
