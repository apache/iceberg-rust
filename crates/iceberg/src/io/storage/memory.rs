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

//! Pure Rust in-memory storage implementation for testing.
//!
//! This module provides a `MemoryStorage` implementation that stores data
//! in a thread-safe `HashMap`, without any external dependencies.
//! It is primarily intended for unit testing and scenarios where persistent
//! storage is not needed.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::io::{
    FileInfo, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use crate::{Error, ErrorKind, Result};

/// A stored in-memory blob together with the wall-clock time it was last written.
///
/// The timestamp is captured at write time so [`Storage::list`] can report a plausible
/// `created_at_millis` for each entry (mirroring the last-modified value Java's object-store
/// `FileIO` implementations attach to `FileInfo`).
#[derive(Debug, Clone)]
pub(crate) struct MemoryEntry {
    bytes: Bytes,
    created_at_millis: i64,
}

/// Current wall-clock time as milliseconds since the Unix epoch.
///
/// A clock at or before the epoch (unreachable in practice) clamps to `0` so the recorded
/// timestamp stays non-negative.
fn now_millis() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

/// In-memory storage implementation.
///
/// This storage implementation stores all data in a thread-safe `HashMap`,
/// making it suitable for unit tests and scenarios where persistent storage
/// is not needed.
///
/// # Path Normalization
///
/// The storage normalizes paths to handle various formats:
/// - `memory://path/to/file` -> `path/to/file`
/// - `memory:/path/to/file` -> `path/to/file`
/// - `/path/to/file` -> `path/to/file`
/// - `path/to/file` -> `path/to/file`
///
/// # Serialization
///
/// When serialized, `MemoryStorage` serializes to an empty state. When
/// deserialized, it creates a new empty instance. This is intentional
/// because in-memory data cannot be meaningfully serialized across
/// process boundaries.
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryStorage {
    #[serde(skip, default = "default_memory_data")]
    data: Arc<RwLock<HashMap<String, MemoryEntry>>>,
}

fn default_memory_data() -> Arc<RwLock<HashMap<String, MemoryEntry>>> {
    Arc::new(RwLock::new(HashMap::new()))
}

impl MemoryStorage {
    /// Create a new empty `MemoryStorage` instance.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Test-only accessor for an entry's recorded write timestamp.
    #[cfg(test)]
    pub(crate) fn created_at_millis(&self, path: &str) -> Option<i64> {
        let normalized = Self::normalize_path(path);
        self.data
            .read()
            .ok()
            .and_then(|data| data.get(&normalized).map(|entry| entry.created_at_millis))
    }

    /// Normalize a path by removing scheme prefixes and leading slashes.
    ///
    /// This handles the following formats:
    /// - `memory://path` -> `path`
    /// - `memory:/path` -> `path`
    /// - `/path` -> `path`
    /// - `path` -> `path`
    pub(crate) fn normalize_path(path: &str) -> String {
        // Handle memory:// prefix (with double slash)
        let path = path.strip_prefix("memory://").unwrap_or(path);
        // Handle memory:/ prefix (with single slash)
        let path = path.strip_prefix("memory:/").unwrap_or(path);
        // Remove any leading slashes
        path.trim_start_matches('/').to_string()
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for MemoryStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let normalized = Self::normalize_path(path);
        let data = self.data.read().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire read lock: {e}"),
            )
        })?;
        Ok(data.contains_key(&normalized))
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let normalized = Self::normalize_path(path);
        let data = self.data.read().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire read lock: {e}"),
            )
        })?;
        match data.get(&normalized) {
            Some(entry) => Ok(FileMetadata {
                size: entry.bytes.len() as u64,
            }),
            None => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("File not found: {path}"),
            )),
        }
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let normalized = Self::normalize_path(path);
        let data = self.data.read().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire read lock: {e}"),
            )
        })?;
        match data.get(&normalized) {
            Some(entry) => Ok(entry.bytes.clone()),
            None => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("File not found: {path}"),
            )),
        }
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let normalized = Self::normalize_path(path);
        let data = self.data.read().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire read lock: {e}"),
            )
        })?;
        match data.get(&normalized) {
            Some(entry) => Ok(Box::new(MemoryFileRead::new(entry.bytes.clone()))),
            None => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("File not found: {path}"),
            )),
        }
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let normalized = Self::normalize_path(path);
        let mut data = self.data.write().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire write lock: {e}"),
            )
        })?;
        data.insert(normalized, MemoryEntry {
            bytes: bs,
            created_at_millis: now_millis(),
        });
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let normalized = Self::normalize_path(path);
        Ok(Box::new(MemoryFileWrite::new(
            self.data.clone(),
            normalized,
        )))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let normalized = Self::normalize_path(path);
        let mut data = self.data.write().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire write lock: {e}"),
            )
        })?;
        data.remove(&normalized);
        Ok(())
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let normalized = Self::normalize_path(path);
        let prefix = if normalized.ends_with('/') {
            normalized
        } else {
            format!("{normalized}/")
        };

        let mut data = self.data.write().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire write lock: {e}"),
            )
        })?;

        // Collect keys to remove (can't modify while iterating)
        let keys_to_remove: Vec<String> = data
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();

        for key in keys_to_remove {
            data.remove(&key);
        }

        Ok(())
    }

    /// List every blob whose normalized key falls under `prefix`.
    ///
    /// # Prefix semantics: STRING-PREFIX
    ///
    /// Like an object store (Java's `SupportsPrefixOperations` permits arbitrary string
    /// prefixes for key/value stores) and matching this backend's `delete_prefix`, the prefix
    /// is matched against the normalized key with a trailing `/` enforced. Enforcing the `/`
    /// is the boundary guard: prefix `dir` matches `dir/file.txt` but NOT a sibling key
    /// `dir2/file.txt`. (The flat key space has no notion of nested directories, so every
    /// match is already "recursive" — there is nothing to descend into.) A prefix with no
    /// matching keys yields an empty list, never an error.
    ///
    /// The trailing-`/` construction is deliberately identical to `delete_prefix`'s (no
    /// `is_empty()` shortcut): this is what makes `list(p)` and `delete_prefix(p)` agree on the
    /// exact key set for EVERY prefix, including the empty/root prefix — the safety invariant an
    /// orphan-file sweep depends on.
    async fn list(&self, prefix: &str) -> Result<Vec<FileInfo>> {
        let normalized = Self::normalize_path(prefix);
        let prefix = if normalized.ends_with('/') {
            normalized
        } else {
            format!("{normalized}/")
        };

        let data = self.data.read().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire read lock: {e}"),
            )
        })?;

        let files = data
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(key, entry)| {
                FileInfo::new(
                    key.clone(),
                    entry.bytes.len() as u64,
                    entry.created_at_millis,
                )
            })
            .collect();
        Ok(files)
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// Factory for creating `MemoryStorage` instances.
///
/// This factory implements `StorageFactory` and creates `MemoryStorage`
/// instances. Since the factory is explicitly chosen, no scheme validation
/// is performed - the storage will validate paths during operations.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemoryStorageFactory;

#[typetag::serde]
impl StorageFactory for MemoryStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(MemoryStorage::new()))
    }
}

/// File reader for in-memory storage.
#[derive(Debug)]
pub struct MemoryFileRead {
    data: Bytes,
}

impl MemoryFileRead {
    /// Create a new `MemoryFileRead` with the given data.
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }
}

#[async_trait]
impl FileRead for MemoryFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let start = range.start as usize;
        let end = range.end as usize;

        if start > self.data.len() || end > self.data.len() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Range {}..{} is out of bounds for data of length {}",
                    start,
                    end,
                    self.data.len()
                ),
            ));
        }

        Ok(self.data.slice(start..end))
    }
}

/// File writer for in-memory storage.
///
/// This struct implements `FileWrite` for writing to in-memory storage.
/// Data is buffered until `close()` is called, at which point it is
/// flushed to the storage.
#[derive(Debug)]
pub struct MemoryFileWrite {
    data: Arc<RwLock<HashMap<String, MemoryEntry>>>,
    path: String,
    buffer: Vec<u8>,
    closed: bool,
}

impl MemoryFileWrite {
    /// Create a new `MemoryFileWrite` for the given path.
    pub fn new(data: Arc<RwLock<HashMap<String, MemoryEntry>>>, path: String) -> Self {
        Self {
            data,
            path,
            buffer: Vec::new(),
            closed: false,
        }
    }
}

#[async_trait]
impl FileWrite for MemoryFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        if self.closed {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Cannot write to closed file",
            ));
        }
        self.buffer.extend_from_slice(&bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::new(ErrorKind::DataInvalid, "File already closed"));
        }

        let mut data = self.data.write().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire write lock: {e}"),
            )
        })?;

        data.insert(self.path.clone(), MemoryEntry {
            bytes: Bytes::from(std::mem::take(&mut self.buffer)),
            created_at_millis: now_millis(),
        });
        self.closed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        // Test memory:// prefix
        assert_eq!(
            MemoryStorage::normalize_path("memory://path/to/file"),
            "path/to/file"
        );

        // Test memory:/ prefix
        assert_eq!(
            MemoryStorage::normalize_path("memory:/path/to/file"),
            "path/to/file"
        );

        // Test leading slash
        assert_eq!(
            MemoryStorage::normalize_path("/path/to/file"),
            "path/to/file"
        );

        // Test bare path
        assert_eq!(
            MemoryStorage::normalize_path("path/to/file"),
            "path/to/file"
        );

        // Test multiple leading slashes
        assert_eq!(
            MemoryStorage::normalize_path("///path/to/file"),
            "path/to/file"
        );

        // Test memory:// with leading slash in path
        assert_eq!(
            MemoryStorage::normalize_path("memory:///path/to/file"),
            "path/to/file"
        );
    }

    #[tokio::test]
    async fn test_memory_storage_write_read() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";
        let content = Bytes::from("Hello, World!");

        // Write
        storage.write(path, content.clone()).await.unwrap();

        // Read
        let read_content = storage.read(path).await.unwrap();
        assert_eq!(read_content, content);
    }

    #[tokio::test]
    async fn test_memory_storage_exists() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";

        // File doesn't exist initially
        assert!(!storage.exists(path).await.unwrap());

        // Write file
        storage.write(path, Bytes::from("test")).await.unwrap();

        // File exists now
        assert!(storage.exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_storage_metadata() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";
        let content = Bytes::from("Hello, World!");

        storage.write(path, content.clone()).await.unwrap();

        let metadata = storage.metadata(path).await.unwrap();
        assert_eq!(metadata.size, content.len() as u64);
    }

    #[tokio::test]
    async fn test_memory_storage_delete() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";

        storage.write(path, Bytes::from("test")).await.unwrap();
        assert!(storage.exists(path).await.unwrap());

        storage.delete(path).await.unwrap();
        assert!(!storage.exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_storage_delete_prefix() {
        let storage = MemoryStorage::new();

        // Create multiple files
        storage
            .write("memory://dir/file1.txt", Bytes::from("1"))
            .await
            .unwrap();
        storage
            .write("memory://dir/file2.txt", Bytes::from("2"))
            .await
            .unwrap();
        storage
            .write("memory://other/file.txt", Bytes::from("3"))
            .await
            .unwrap();

        // Delete prefix
        storage.delete_prefix("memory://dir").await.unwrap();

        // Files in dir should be deleted
        assert!(!storage.exists("memory://dir/file1.txt").await.unwrap());
        assert!(!storage.exists("memory://dir/file2.txt").await.unwrap());

        // File in other dir should still exist
        assert!(storage.exists("memory://other/file.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_storage_reader() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";
        let content = Bytes::from("Hello, World!");

        storage.write(path, content.clone()).await.unwrap();

        let reader = storage.reader(path).await.unwrap();
        let read_content = reader.read(0..content.len() as u64).await.unwrap();
        assert_eq!(read_content, content);

        // Test partial read
        let partial = reader.read(0..5).await.unwrap();
        assert_eq!(partial, Bytes::from("Hello"));
    }

    #[tokio::test]
    async fn test_memory_storage_writer() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";

        let mut writer = storage.writer(path).await.unwrap();
        writer.write(Bytes::from("Hello, ")).await.unwrap();
        writer.write(Bytes::from("World!")).await.unwrap();
        writer.close().await.unwrap();

        let content = storage.read(path).await.unwrap();
        assert_eq!(content, Bytes::from("Hello, World!"));
    }

    #[tokio::test]
    async fn test_memory_file_write_double_close() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";

        let mut writer = storage.writer(path).await.unwrap();
        writer.write(Bytes::from("test")).await.unwrap();
        writer.close().await.unwrap();

        // Second close should fail
        let result = writer.close().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_file_write_after_close() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";

        let mut writer = storage.writer(path).await.unwrap();
        writer.close().await.unwrap();

        // Write after close should fail
        let result = writer.write(Bytes::from("test")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_file_read_out_of_bounds() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";
        let content = Bytes::from("Hello");

        storage.write(path, content).await.unwrap();

        let reader = storage.reader(path).await.unwrap();
        let result = reader.read(0..100).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_memory_storage_serialization() {
        let storage = MemoryStorage::new();

        // Serialize
        let serialized = serde_json::to_string(&storage).unwrap();

        // Deserialize
        let deserialized: MemoryStorage = serde_json::from_str(&serialized).unwrap();

        // Deserialized storage should be empty (new instance)
        assert!(deserialized.data.read().unwrap().is_empty());
    }

    #[test]
    fn test_memory_storage_factory() {
        let factory = MemoryStorageFactory;
        let config = StorageConfig::new();
        let storage = factory.build(&config).unwrap();

        // Verify we got a valid storage instance
        assert!(format!("{storage:?}").contains("MemoryStorage"));
    }

    #[test]
    fn test_memory_storage_factory_serialization() {
        let factory = MemoryStorageFactory;

        // Serialize
        let serialized = serde_json::to_string(&factory).unwrap();

        // Deserialize
        let deserialized: MemoryStorageFactory = serde_json::from_str(&serialized).unwrap();

        // Verify the deserialized factory works
        let config = StorageConfig::new();
        let storage = deserialized.build(&config).unwrap();
        assert!(format!("{storage:?}").contains("MemoryStorage"));
    }

    /// Risk: a wrong path set, size, or timestamp from the in-memory listing feeds A2 a wrong
    /// orphan set. Pins write -> list round-trip: the full key set, per-file sizes, and
    /// plausible (>0, <= now) timestamps that match the recorded write time.
    #[tokio::test]
    async fn test_list_returns_written_paths_sizes_and_timestamps() {
        let storage = MemoryStorage::new();
        storage
            .write("memory://dir/a.txt", Bytes::from("a"))
            .await
            .unwrap();
        storage
            .write("memory://dir/sub/b.txt", Bytes::from("bb"))
            .await
            .unwrap();
        let after_writes = now_millis();

        let listed = storage.list("memory://dir").await.unwrap();

        // Keys are stored normalized (scheme + leading slash stripped).
        let mut locations: Vec<String> = listed.iter().map(|f| f.location.clone()).collect();
        locations.sort();
        assert_eq!(locations, vec![
            "dir/a.txt".to_string(),
            "dir/sub/b.txt".to_string()
        ]);

        let entry = |location: &str| listed.iter().find(|f| f.location == location).unwrap();
        assert_eq!(entry("dir/a.txt").size, 1);
        assert_eq!(entry("dir/sub/b.txt").size, 2);

        for file in &listed {
            assert!(file.created_at_millis > 0);
            assert!(file.created_at_millis <= after_writes);
            // The listed timestamp is exactly the one recorded at write time.
            let normalized = format!("memory://{}", file.location);
            assert_eq!(
                storage.created_at_millis(&normalized),
                Some(file.created_at_millis)
            );
        }
    }

    /// Risk: over-listing is over-deletion in A2. Prefix `dir` must match `dir/...` but NOT a
    /// sibling key `dir2/...` (the trailing-`/` boundary guard, string-prefix semantics).
    #[tokio::test]
    async fn test_list_prefix_excludes_sibling_key() {
        let storage = MemoryStorage::new();
        storage
            .write("memory://dir/inside.txt", Bytes::from("in"))
            .await
            .unwrap();
        storage
            .write("memory://dir2/outside.txt", Bytes::from("out"))
            .await
            .unwrap();

        let listed = storage.list("memory://dir").await.unwrap();
        let locations: Vec<&str> = listed.iter().map(|f| f.location.as_str()).collect();
        assert_eq!(locations, vec!["dir/inside.txt"]);
        assert!(!locations.contains(&"dir2/outside.txt"));
    }

    /// Risk: a prefix with no matching keys is a legitimate empty answer, not an error.
    #[tokio::test]
    async fn test_list_empty_prefix_is_empty_not_error() {
        let storage = MemoryStorage::new();
        storage
            .write("memory://other/file.txt", Bytes::from("x"))
            .await
            .unwrap();

        let listed = storage.list("memory://nothing-here").await.unwrap();
        assert!(listed.is_empty());
    }

    /// Risk: a writer-path (`writer().close()`) write must also record a timestamp, so files
    /// created via the streaming writer are listable with plausible metadata.
    #[tokio::test]
    async fn test_list_includes_writer_created_files_with_timestamp() {
        let storage = MemoryStorage::new();
        let mut writer = storage.writer("memory://dir/streamed.txt").await.unwrap();
        writer.write(Bytes::from("hello")).await.unwrap();
        writer.close().await.unwrap();
        let after = now_millis();

        let listed = storage.list("memory://dir").await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].location, "dir/streamed.txt");
        assert_eq!(listed[0].size, 5);
        assert!(listed[0].created_at_millis > 0 && listed[0].created_at_millis <= after);
    }

    #[tokio::test]
    async fn test_path_normalization_consistency() {
        let storage = MemoryStorage::new();
        let content = Bytes::from("test content");

        // Write with one format
        storage
            .write("memory://path/to/file", content.clone())
            .await
            .unwrap();

        // Read with different formats - all should work
        assert_eq!(
            storage.read("memory://path/to/file").await.unwrap(),
            content
        );
        assert_eq!(storage.read("memory:/path/to/file").await.unwrap(), content);
        assert_eq!(storage.read("/path/to/file").await.unwrap(), content);
        assert_eq!(storage.read("path/to/file").await.unwrap(), content);
    }

    /// Risk: the increment's load-bearing safety invariant is "`list(prefix)` returns exactly
    /// the set `delete_prefix(prefix)` would remove" — a disagreement is the data-loss class the
    /// orphan sweep is built against. This pins the invariant ACROSS prefix shapes, including the
    /// empty/root prefix (`memory://`) where an earlier `list`-only `is_empty()` special case
    /// made `list` report every key while `delete_prefix` removed none. For each prefix the two
    /// sets must be byte-for-byte equal.
    #[tokio::test]
    async fn test_list_set_equals_delete_prefix_set_including_empty_prefix() {
        let seed_keys = [
            "memory://ab/inside.txt",
            "memory://ab/sub/deep.txt",
            "memory://ab2/sibling.txt",
            "memory://ab", // a key EXACTLY equal to the prefix name (file-as-prefix boundary)
            "memory://other/x.txt",
        ];

        for prefix in ["memory://ab", "memory://ab/", "memory://", "memory://ab2"] {
            let storage = MemoryStorage::new();
            for key in seed_keys {
                storage.write(key, Bytes::from("x")).await.unwrap();
            }

            let listed: std::collections::BTreeSet<String> = storage
                .list(prefix)
                .await
                .unwrap()
                .into_iter()
                .map(|file| file.location)
                .collect();

            // Compute the set delete_prefix actually removes: snapshot-all, delete, diff.
            let snapshot_all = |storage: &MemoryStorage| {
                let data = storage.data.read().unwrap();
                data.keys()
                    .cloned()
                    .collect::<std::collections::BTreeSet<String>>()
            };
            let before = snapshot_all(&storage);
            storage.delete_prefix(prefix).await.unwrap();
            let after = snapshot_all(&storage);
            let removed: std::collections::BTreeSet<String> =
                before.difference(&after).cloned().collect();

            assert_eq!(
                listed, removed,
                "list/delete_prefix disagree for prefix {prefix:?}: list={listed:?} delete={removed:?}"
            );
        }
    }
}
