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

use std::mem;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use moka::future::{Cache, CacheBuilder};
use tokio::sync::RwLock;

/// A cache for data files retrieved by [`FileIO`].
///
/// Minimizes work by allowing cache [`PartialHit`]s, where a file read with edge(s) covered
/// by the cache only needs to fetch the missing part in the middle.
// !!!! Note: the current implementation is unoptimized and basic, it must be revised
// Before any optimization is done, some kind of benchmarking is needed
#[derive(Clone, Debug)]
pub struct DataCache {
    cache: Cache<String, FileCache>,
}

/// The cache of a single file, with a read/write lock on the content for concurrent use
#[derive(Clone, Debug)]
struct FileCache {
    content: Arc<RwLock<FileContentCache>>,
    current_size: u32,
}

impl DataCache {
    /// Creates a new [`DataCache`] with no entries and the specified size.
    pub fn new(max_size: u64) -> Self {
        Self {
            cache: CacheBuilder::new(max_size)
                .weigher(|path: &String, file_cache: &FileCache| {
                    path.len() as u32 + file_cache.current_size
                })
                .build(),
        }
    }

    /// Tries to get the entire contents of a data file. Will only return the cached content if the
    /// entire file has previously been cached as a whole. If it has been cached in fragments,
    /// we have no way of knowing whether it is complete or not ATM.
    pub async fn get_whole(&self, path: &String) -> Option<Bytes> {
        if let Some(file_cache) = self.cache.get(path).await {
            if let FileContentCache::Complete(bytes) = &*file_cache.content.read().await {
                return Some(bytes.clone());
            }
        }

        None
    }

    /// Caches an entire data file.
    pub async fn set_whole(&self, path: &str, bytes: Bytes) {
        let size = size_of::<Bytes>() + bytes.len();
        self.cache
            .insert(path.to_owned(), FileCache {
                content: Arc::new(RwLock::new(FileContentCache::Complete(bytes))),
                current_size: size as u32,
            })
            .await;
    }

    /// Tries to get a range of bytes from the cache of a data file. Depending on what is currently
    /// availible, this may return
    /// 
    /// - Hit, if the entire range is availible
    /// - PartialHit, if only some of the head and/or the tail is availible
    ///   - Use [`PartialHit::missing_range`] and [`DataCache::fill_partial_hit`] to resolve this
    /// - Miss, if none of the range is availible
    pub async fn get_range(&self, path: &String, range: Range<u64>) -> DataCacheRes {
        if let Some(file_cache) = self.cache.get(path).await {
            match &*file_cache.content.read().await {
                FileContentCache::Complete(bytes) => {
                    let range = (range.start as usize)..(range.end as usize);
                    DataCacheRes::Hit(bytes.slice(range))
                }
                FileContentCache::Fragmented(fragmented_content_cache) => {
                    fragmented_content_cache.get(range)
                }
            }
        } else {
            DataCacheRes::Miss
        }
    }

    /// Caches a fragment of a data file
    pub async fn set_range(&self, path: &String, range: Range<u64>, bytes: Bytes) {
        if let Some(mut file_cache) = self.cache.get(path).await {
            let mut file_content_cache = file_cache.content.write().await;
            match &mut *file_content_cache {
                FileContentCache::Complete(_) => {
                    // do nothing, we already have the entire file cached
                }
                FileContentCache::Fragmented(fragmented_content_cache) => {
                    fragmented_content_cache.set(range, bytes);
                    file_cache.current_size =
                        fragmented_content_cache.size() as u32 + size_of::<u32>() as u32;

                    mem::drop(file_content_cache); // release our lock

                    self.cache.insert(path.clone(), file_cache).await;
                }
            }
        } else {
            let fragmented_content_cache =
                FragmentedContentCache::new_with_first_buf(path.clone(), range, bytes);
            let current_size = fragmented_content_cache.size() as u32;
            let file_cache = FileCache {
                content: Arc::new(RwLock::new(FileContentCache::Fragmented(
                    fragmented_content_cache,
                ))),
                current_size,
            };

            self.cache.insert(path.clone(), file_cache).await;
        }
    }

    /// Fills the missing section of a [`PartialHit`] and returns the complete btyes, even if the
    /// cached head and/or tail have since been purged from the cache
    pub async fn fill_partial_hit(&self, partial_hit: PartialHit, missing_bytes: Bytes) -> Bytes {
        self.set_range(
            &partial_hit.path,
            partial_hit.missing_range,
            missing_bytes.clone(),
        )
        .await;

        if let DataCacheRes::Hit(complete_buf) = self
            .get_range(&partial_hit.path, partial_hit.original_range)
            .await
        {
            complete_buf
        } else {
            // if our file data has been purged from the cache in the meantime, reconstruct the needed buffer ourselves
            Bytes::from(
                [
                    partial_hit.head_bytes.unwrap_or_default(),
                    missing_bytes,
                    partial_hit.tail_bytes.unwrap_or_default(),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>(),
            )
        }
    }
}

/// An atomic reference to a [`DataCache`]
pub type DataCacheRef = Arc<DataCache>;

/// Possible results of a cache search for a range of a file
#[derive(Debug, PartialEq)]
pub enum DataCacheRes {
    Hit(Bytes),
    PartialHit(PartialHit),
    Miss,
}

/// A result of a cache search where some of the head and/or tail of the needed range was in the cache
#[derive(Debug, PartialEq)]
pub struct PartialHit {
    path: String,
    original_range: Range<u64>,
    missing_range: Range<u64>,
    head_bytes: Option<Bytes>,
    tail_bytes: Option<Bytes>,
}

impl PartialHit {
    /// Get the range that still needs to be recovered
    pub fn missing_range(&self) -> Range<u64> {
        self.missing_range.clone()
    }
}

#[derive(Clone, Debug)]
enum FileContentCache {
    Complete(Bytes),
    Fragmented(FragmentedContentCache),
}

#[derive(Clone, Debug)]
struct FragmentedContentCache {
    path: String,
    // it is assumed no buffers overlap or are adjacent (adjacent buffers should be merged)
    buffers: Vec<(Range<u64>, Bytes)>,
}

impl FragmentedContentCache {
    fn new_with_first_buf(path: String, range: Range<u64>, bytes: Bytes) -> Self {
        if range.start == range.end {
            return Self {
                path,
                buffers: vec![],
            };
        }
        Self {
            path,
            buffers: vec![(range, bytes)],
        }
    }

    fn size(&self) -> u64 {
        let vec_size = size_of::<Vec<(Range<u64>, Bytes)>>();
        let buf_sizes = self.buffers.iter().fold(0usize, |sum: usize, (_, buf)| {
            sum + size_of::<Range<u64>>() + size_of::<Bytes>() + buf.len()
        });

        (vec_size + buf_sizes) as u64
    }

    fn get(&self, range: Range<u64>) -> DataCacheRes {
        let mut head: Option<Bytes> = None;
        let mut tail: Option<Bytes> = None;

        for (buf_range, buf) in &self.buffers {
            if buf_range.start <= range.start && range.end <= buf_range.end {
                let offset = (range.start - buf_range.start) as usize;
                let len = (range.end - range.start) as usize;
                return DataCacheRes::Hit(buf.slice(offset..(offset + len)));
            }

            if buf_range.start <= range.start
                && ((range.start + 1)..range.end).contains(&buf_range.end)
            {
                let offset = (range.start - buf_range.start) as usize;
                head = Some(buf.slice(offset..buf.len()));
            }

            if range.contains(&buf_range.start) && range.end <= buf_range.end {
                let cutoff = (range.end - buf_range.start) as usize;
                tail = Some(buf.slice(0..cutoff))
            }
        }

        if head.is_some() || tail.is_some() {
            let offset_start = match &head {
                Some(buf) => buf.len() as u64,
                None => 0,
            };

            let offset_end = match &tail {
                Some(buf) => buf.len() as u64,
                None => 0,
            };

            let missing_range = (range.start + offset_start)..(range.end - offset_end);

            DataCacheRes::PartialHit(PartialHit {
                path: self.path.clone(),
                original_range: range,
                missing_range,
                head_bytes: head,
                tail_bytes: tail,
            })
        } else {
            DataCacheRes::Miss
        }
    }

    fn set(&mut self, range: Range<u64>, bytes: Bytes) {
        if range.end == range.start {
            return;
        }

        let mut head_touching: Option<(Range<u64>, Bytes)> = None;
        let mut tail_touching: Option<(Range<u64>, Bytes)> = None;

        for i in (0..self.buffers.len()).rev() {
            let buf_range = self.buffers[i].0.clone();
            if buf_range.start <= range.start && range.end <= buf_range.end {
                return; // we already have this cached
            }

            if buf_range.start <= range.start
                && ((range.start + 1)..range.end).contains(&buf_range.end)
            {
                head_touching = Some(self.buffers.remove(i));
            }

            if range.contains(&buf_range.start) && range.end <= buf_range.end {
                tail_touching = Some(self.buffers.remove(i));
            }

            if range.start < buf_range.start && buf_range.end < range.end {
                self.buffers.remove(i);
            }
        }

        if head_touching.is_none() && tail_touching.is_none() {
            self.buffers.push((range, bytes))
        } else {
            let (head_range, head_buf) =
                head_touching.unwrap_or((range.start..range.start, Bytes::new()));
            let (tail_range, tail_buf) =
                tail_touching.unwrap_or((range.end..range.end, Bytes::new()));

            let start_offset = (head_range.end - range.start) as usize;
            let end_offset = range.end - tail_range.start;
            let trimmed_end = bytes.len() - end_offset as usize;

            let trimmed_middle_buf = bytes.slice(start_offset..trimmed_end);
            let new_buf = Bytes::from(
                [head_buf, trimmed_middle_buf, tail_buf]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>(),
            );

            // we clear all of the buffers that were totally overpassed by our new buffer
            self.buffers.retain(|(buf_range, _)| {
                !(range.start < buf_range.start && buf_range.end < range.end)
            });

            self.buffers
                .push((head_range.start..tail_range.end, new_buf))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use bytes::Bytes;

    use crate::io::data_cache::PartialHit;

    use super::DataCache;
    use super::DataCacheRes::{Hit, Miss, PartialHit as ParHit};

    const MEGS32: u64 = 32 * 1000 * 1000;
    const TEST_PATH: &str = "/test/path";
    const TEST_CONTENTS: &str = "abcdefghijklmnopqrstuvwxyz";
    const TEST_BYTES: Bytes = Bytes::from_static(TEST_CONTENTS.as_bytes());

    #[tokio::test]
    async fn cache_whole_file() {
        let cache = DataCache::new(MEGS32);

        assert_eq!(None, cache.get_whole(&TEST_PATH.to_owned()).await);
        assert_eq!(Miss, cache.get_range(&TEST_PATH.to_owned(), 5..10).await);

        cache.set_whole(TEST_PATH, TEST_BYTES).await;

        assert_eq!(
            Some(TEST_BYTES),
            cache.get_whole(&TEST_PATH.to_owned()).await
        );
        assert_eq!(
            Hit(TEST_BYTES.slice(5..10)),
            cache.get_range(&TEST_PATH.to_owned(), 5..10).await
        );

        // shouldn't have an effect, we already have this fully cached and the cache shouldn't have filled and purged it
        cache.set_range(&TEST_PATH.to_owned(), 4..8, Bytes::new()).await;

        assert_eq!(
            Some(TEST_BYTES),
            cache.get_whole(&TEST_PATH.to_owned()).await
        );
        assert_eq!(
            Hit(TEST_BYTES.slice(5..10)),
            cache.get_range(&TEST_PATH.to_owned(), 5..10).await
        );
    }

    #[tokio::test]
    async fn cache_one_range_simple() {
        let cache = DataCache::new(MEGS32);

        assert_eq!(None, cache.get_whole(&TEST_PATH.to_owned()).await);
        assert_eq!(Miss, cache.get_range(&TEST_PATH.to_owned(), 0..4).await);
        assert_eq!(Miss, cache.get_range(&TEST_PATH.to_owned(), 9..12).await);
        assert_eq!(Miss, cache.get_range(&TEST_PATH.to_owned(), 20..23).await);

        cache.set_range(&TEST_PATH.to_owned(), 7..15, TEST_BYTES.slice(7..15)).await;

        assert_eq!(None, cache.get_whole(&TEST_PATH.to_owned()).await);
        assert_eq!(Miss, cache.get_range(&TEST_PATH.to_owned(), 0..4).await);
        assert_eq!(Hit(TEST_BYTES.slice(9..12)), cache.get_range(&TEST_PATH.to_owned(), 9..12).await);
        assert_eq!(Miss, cache.get_range(&TEST_PATH.to_owned(), 20..23).await);

        cache.set_whole(TEST_PATH, TEST_BYTES).await;

        assert_eq!(Some(TEST_BYTES), cache.get_whole(&TEST_PATH.to_owned()).await);
        assert_eq!(Hit(TEST_BYTES.slice(0..4)), cache.get_range(&TEST_PATH.to_owned(), 0..4).await);
        assert_eq!(Hit(TEST_BYTES.slice(9..12)), cache.get_range(&TEST_PATH.to_owned(), 9..12).await);
        assert_eq!(Hit(TEST_BYTES.slice(20..23)), cache.get_range(&TEST_PATH.to_owned(), 20..23).await);
    }

    #[tokio::test]
    async fn cache_partial_hit() {
        let cache = DataCache::new(MEGS32);

        assert_eq!(Miss, cache.get_range(&TEST_PATH.to_owned(), 5..15).await);
        assert_eq!(Miss, cache.get_range(&TEST_PATH.to_owned(), 10..20).await);

        cache.set_range(&TEST_PATH.to_owned(), 3..8, TEST_BYTES.slice(3..8)).await;

        assert_eq!(ParHit(PartialHit {
            path: TEST_PATH.to_owned(),
            original_range: 5..15,
            missing_range: 8..15,
            head_bytes: Some(TEST_BYTES.slice(5..8)),
            tail_bytes: None
        }), cache.get_range(&TEST_PATH.to_owned(), 5..15).await);
        assert_eq!(Miss, cache.get_range(&TEST_PATH.to_owned(), 10..20).await);

        cache.set_range(&TEST_PATH.to_owned(), 15..22, TEST_BYTES.slice(15..22)).await;

        assert_eq!(ParHit(PartialHit {
            path: TEST_PATH.to_owned(),
            original_range: 5..15,
            missing_range: 8..15,
            head_bytes: Some(TEST_BYTES.slice(5..8)),
            tail_bytes: None
        }), cache.get_range(&TEST_PATH.to_owned(), 5..15).await);
        assert_eq!(ParHit(PartialHit {
            path: TEST_PATH.to_owned(),
            original_range: 10..20,
            missing_range: 10..15,
            head_bytes: None,
            tail_bytes: Some(TEST_BYTES.slice(15..20))
        }), cache.get_range(&TEST_PATH.to_owned(), 10..20).await);

        cache.set_range(&TEST_PATH.to_owned(), 12..17, TEST_BYTES.slice(12..17)).await;

        assert_eq!(ParHit(PartialHit {
            path: TEST_PATH.to_owned(),
            original_range: 5..15,
            missing_range: 8..12,
            head_bytes: Some(TEST_BYTES.slice(5..8)),
            tail_bytes: Some(TEST_BYTES.slice(12..15))
        }), cache.get_range(&TEST_PATH.to_owned(), 5..15).await);
        assert_eq!(ParHit(PartialHit {
            path: TEST_PATH.to_owned(),
            original_range: 10..20,
            missing_range: 10..12,
            head_bytes: None,
            tail_bytes: Some(TEST_BYTES.slice(12..20))
        }), cache.get_range(&TEST_PATH.to_owned(), 10..20).await);
    }

    #[tokio::test]
    async fn cache_partial_hit_fill() {
        let cache = DataCache::new(MEGS32);

        cache.set_range(&TEST_PATH.to_owned(), 3..8, TEST_BYTES.slice(3..8)).await;

        if let ParHit(partial_hit) = cache.get_range(&TEST_PATH.to_owned(), 5..13).await {
            let missing = partial_hit.missing_range();
            let missing = missing.start as usize..missing.end as usize;
            let missing_bytes = TEST_BYTES.slice(missing);
            assert_eq!(TEST_BYTES.slice(5..13), cache.fill_partial_hit(partial_hit, missing_bytes).await);
        } else {
            panic!("not a partial hit :(")
        }
    }

    #[tokio::test]
    async fn cache_overlapping_ranges() {
        let cache = DataCache::new(MEGS32);

        cache.set_range(&TEST_PATH.to_owned(), 12..18, TEST_BYTES.slice(12..18)).await;
        cache.set_range(&TEST_PATH.to_owned(), 10..20, TEST_BYTES.slice(10..20)).await;
        cache.set_range(&TEST_PATH.to_owned(), 14..16, TEST_BYTES.slice(14..16)).await;
        
        assert_eq!(ParHit(PartialHit {
            path: TEST_PATH.to_owned(),
            original_range: 5..15,
            missing_range: 5..10,
            head_bytes: None,
            tail_bytes: Some(TEST_BYTES.slice(10..15))
        }), cache.get_range(&TEST_PATH.to_owned(), 5..15).await);

        assert_eq!(Hit(TEST_BYTES.slice(11..17)), cache.get_range(&TEST_PATH.to_owned(), 11..17).await)
    }

    #[tokio::test]
    async fn cache_partial_fill_ran_out_of_memory() {
        // enough memory to cache 5 bytes
        let size = size_of::<u32>() + size_of::<Vec<(Range<u64>, Bytes)>>() + size_of::<Range<u64>>() + size_of::<Bytes>() + 5;

        // give a little bit of extra leeway
        let cache = DataCache::new(size as u64 + 2);

        cache.set_range(&TEST_PATH.to_owned(), 10..15, TEST_BYTES.slice(10..15)).await;

        if let ParHit(partial_hit) = cache.get_range(&TEST_PATH.to_owned(), 12..22).await {
            let missing = partial_hit.missing_range();
            let missing = missing.start as usize..missing.end as usize;
            let missing_bytes = TEST_BYTES.slice(missing);
            assert_eq!(TEST_BYTES.slice(12..22), cache.fill_partial_hit(partial_hit, missing_bytes).await);
        } else {
            panic!("not a partial hit :(")
        }
    }
}
