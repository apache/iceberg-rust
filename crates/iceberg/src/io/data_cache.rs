use std::{mem, ops::Range, sync::Arc};

use async_std::sync::RwLock;
use bytes::Bytes;
use moka::future::{Cache, CacheBuilder};

/// A cache for data files retrieved by `FileIO`. 
/// 
/// Minimizes work by allowing "partial" cache hits, where a file read with edge(s) covered 
/// by the cache only needs to fetch the missing part in the middle.
/// 
// !!!! Note: the current implementation is unoptimized and basic, it must be revised
// Before any optimization is done, some kind of benchmarking is needed
#[derive(Clone, Debug)]
pub struct DataCache {
    cache: Cache<String, FileCache>,
}

#[derive(Clone, Debug)]
struct FileCache {
    content: Arc<RwLock<FileContentCache>>,
    current_size: u32
}

impl DataCache {
    pub fn new(max_size: u64) -> Self {
        Self {
            cache: CacheBuilder::new(max_size)
                .weigher(|path: &String, file_cache: &FileCache| path.len() as u32 + file_cache.current_size)
                .build(),
        }
    }

    pub async fn get(&self, path: &String, range: Range<u64>) -> DataCacheRes {
        if let Some(file_cache) = self.cache.get(path).await {
            file_cache.content.read().await.get(range)
        } else {
            DataCacheRes::Miss
        }
    }

    pub async fn set(&self, path: &String, range: Range<u64>, bytes: Bytes) {
        if let Some(mut file_cache) = self.cache.get(path).await {
            let mut file_content_cache = file_cache.content.write().await;

            file_content_cache.set(range, bytes);
            file_cache.current_size = file_content_cache.size() as u32 + size_of::<u32>() as u32;

            mem::drop(file_content_cache); // release our lock

            self.cache.insert(path.clone(), file_cache).await;
        } else {
            let file_content_cache = FileContentCache::new_with_first_buf(path.clone(), range, bytes);
            let current_size = file_content_cache.size() as u32;
            let file_cache = FileCache {
                content: Arc::new(RwLock::new(file_content_cache)),
                current_size
            };

            self.cache.insert(path.clone(), file_cache).await;
        }
    }
    
    pub async fn fill_partial_hit(&self, partial_hit: PartialHit, missing_bytes: Bytes) -> Bytes {
        self.set(&partial_hit.path, partial_hit.missing_range, missing_bytes.clone()).await;
        
        if let DataCacheRes::Hit(complete_buf) = self.get(&partial_hit.path, partial_hit.original_range).await {
            complete_buf
        } else {
            // if our file data has been purged from the cache in the meantime, reconstruct the needed buffer ourselves
            Bytes::from([partial_hit.head_bytes.unwrap_or_default(), missing_bytes, partial_hit.tail_bytes.unwrap_or_default()].into_iter().flatten().collect::<Vec<_>>())
        }
    }
}

pub type DataCacheRef = Arc<DataCache>;

pub enum DataCacheRes {
    Hit(Bytes),
    PartialHit(PartialHit),
    Miss
}

pub struct PartialHit {
    path: String,
    original_range: Range<u64>,
    missing_range: Range<u64>,
    head_bytes: Option<Bytes>,
    tail_bytes: Option<Bytes>
}

impl PartialHit {
    pub fn missing_range(&self) -> Range<u64> {
        self.missing_range.clone()
    }
}


#[derive(Clone, Debug)]
struct FileContentCache {
    path: String,
    // it is assumed no buffers overlap or are adjacent (adjacent buffers should be merged)
    buffers: Vec<(Range<u64>, Bytes)>
}

impl FileContentCache {
    fn new_with_first_buf(path: String, range: Range<u64>, bytes: Bytes) -> Self {
        if range.start == range.end { // TODO: check if this is necessary
            return Self {
                path,
                buffers: vec![]
            }
        }
        Self {
            path,
            buffers: vec![(range, bytes)]
        }
    }

    fn size(&self) -> u64 {
        let vec_size = size_of::<Vec<(Range<u64>, Bytes)>>();
        let buf_sizes = self.buffers.iter().fold(0usize, |sum: usize, (_, buf)| sum + size_of::<Range<u64>>() + size_of::<Bytes>() + buf.len());

        (vec_size + buf_sizes) as u64
    }

    fn get(&self, range: Range<u64>) -> DataCacheRes {
        let mut head: Option<Bytes> = None;
        let mut tail: Option<Bytes> = None;

        for (buf_range, buf) in &self.buffers {
            if buf_range.start <= range.start && range.end <= buf_range.end {
                let offset = (range.start - buf_range.start) as usize;
                let len = (range.end - range.start) as usize;
                return DataCacheRes::Hit(buf.slice(offset..(offset + len)))
            }

            if buf_range.start <= range.start && buf_range.end < range.end {
                let offset = (range.start - buf_range.start) as usize;
                head = Some(buf.slice(offset..buf.len()));
            }

            if range.start < buf_range.start && range.end <= buf_range.end {
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

            return DataCacheRes::PartialHit(PartialHit { path: self.path.clone(), original_range: range, missing_range, head_bytes: head, tail_bytes: tail })
        } else {
            DataCacheRes::Miss
        }
    }

    fn set(&mut self, range: Range<u64>, bytes: Bytes) {
        // TODO: LOCKING THIS IS PROBABLY A GOOD IDEA

        if range.end == range.start { // TODO: check if this necessary
            return;
        }

        let mut head_touching: Option<(Range<u64>, Bytes)> = None;
        let mut tail_touching: Option<(Range<u64>, Bytes)> = None;

        for i in (0..self.buffers.len()).rev() {
            let buf_range = self.buffers[i].0.clone();
            if buf_range.start <= range.start && range.end <= buf_range.end {
                return // we already have this cached
            }

            if buf_range.start <= range.start && buf_range.end < range.end {
                head_touching = Some(self.buffers.remove(i));
            }

            if range.start < buf_range.start && range.end <= buf_range.start {
                tail_touching = Some(self.buffers.remove(i));
            }

            if range.start < buf_range.start && buf_range.end < range.end {
                self.buffers.remove(i);
            }
        }

        if head_touching.is_none() && tail_touching.is_none() {
            self.buffers.push((range, bytes))
        } else {
            let (head_range, head_buf) = head_touching.unwrap_or((range.start..range.start, Bytes::new()));
            let (tail_range, tail_buf) = tail_touching.unwrap_or((range.end..range.end, Bytes::new()));

            let start_offset = (head_range.end - range.start) as usize;
            let end_offset = range.end - tail_range.start;
            let trimmed_end = bytes.len() - end_offset as usize;

            let trimmed_middle_buf = bytes.slice(start_offset..trimmed_end);
            let new_buf = Bytes::from([head_buf, trimmed_middle_buf, tail_buf].into_iter().flatten().collect::<Vec<_>>());
            
            // we clear all of the buffers that were totally overpassed by our new buffer
            self.buffers.retain(|(buf_range, _)| !(range.start < buf_range.start && buf_range.end < range.end));

            self.buffers.push((head_range.start..tail_range.end, new_buf))
        }
    }
}
