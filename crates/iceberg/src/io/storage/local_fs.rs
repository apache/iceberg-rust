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

//! Local filesystem storage implementation for testing.
//!
//! This module provides a `LocalFsStorage` implementation that uses standard
//! Rust filesystem operations. It is primarily intended for unit testing
//! scenarios where tests need to read/write files on the local filesystem.

use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::io::{
    FileInfo, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use crate::{Error, ErrorKind, Result};

/// Convert a filesystem modification time into milliseconds since the Unix epoch.
///
/// Mirrors how Java's `HadoopFileIO.listPrefix` reports `FileStatus.getModificationTime()` as
/// the `FileInfo.createdAtMillis` value. A modification time at or before the epoch (only
/// reachable on clocks set far in the past) clamps to `0` so the value stays non-negative.
fn modified_time_to_millis(modified: SystemTime) -> i64 {
    match modified.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

/// Local filesystem storage implementation.
///
/// This storage implementation uses standard Rust filesystem operations,
/// making it suitable for unit tests that need to read/write files on disk.
///
/// # Path Normalization
///
/// The storage normalizes paths to handle various formats:
/// - `file:///path/to/file` -> `/path/to/file`
/// - `file:/path/to/file` -> `/path/to/file`
/// - `/path/to/file` -> `/path/to/file`
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocalFsStorage;

impl LocalFsStorage {
    /// Create a new `LocalFsStorage` instance.
    pub fn new() -> Self {
        Self
    }

    /// Normalize a path by removing scheme prefixes.
    ///
    /// This handles the following formats:
    /// - `file:///path` -> `/path`
    /// - `file://path` -> `/path` (treats as absolute)
    /// - `file:/path` -> `/path`
    /// - `/path` -> `/path`
    pub(crate) fn normalize_path(path: &str) -> PathBuf {
        let path = if let Some(stripped) = path.strip_prefix("file://") {
            // file:///path -> /path or file://path -> /path
            if stripped.starts_with('/') {
                stripped.to_string()
            } else {
                format!("/{stripped}")
            }
        } else if let Some(stripped) = path.strip_prefix("file:") {
            // file:/path -> /path
            if stripped.starts_with('/') {
                stripped.to_string()
            } else {
                format!("/{stripped}")
            }
        } else {
            path.to_string()
        };
        PathBuf::from(path)
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for LocalFsStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let path = Self::normalize_path(path);
        Ok(path.exists())
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let path = Self::normalize_path(path);
        let metadata = fs::metadata(&path).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to get metadata for {}: {}", path.display(), e),
            )
        })?;
        Ok(FileMetadata {
            size: metadata.len(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let path = Self::normalize_path(path);
        let content = fs::read(&path).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to read file {}: {}", path.display(), e),
            )
        })?;
        Ok(Bytes::from(content))
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let path = Self::normalize_path(path);
        let file = fs::File::open(&path).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to open file {}: {}", path.display(), e),
            )
        })?;
        Ok(Box::new(LocalFsFileRead::new(file)))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let path = Self::normalize_path(path);

        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to create directory {}: {}", parent.display(), e),
                )
            })?;
        }

        fs::write(&path, &bs).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to write file {}: {}", path.display(), e),
            )
        })?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let path = Self::normalize_path(path);

        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to create directory {}: {}", parent.display(), e),
                )
            })?;
        }

        let file = fs::File::create(&path).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to create file {}: {}", path.display(), e),
            )
        })?;
        Ok(Box::new(LocalFsFileWrite::new(file)))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let path = Self::normalize_path(path);
        if path.exists() {
            fs::remove_file(&path).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to delete file {}: {}", path.display(), e),
                )
            })?;
        }
        Ok(())
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let path = Self::normalize_path(path);
        if path.is_dir() {
            fs::remove_dir_all(&path).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to delete directory {}: {}", path.display(), e),
                )
            })?;
        }
        Ok(())
    }

    /// Recursively list every file under `prefix`.
    ///
    /// # Prefix semantics: DIRECTORY
    ///
    /// `prefix` names a directory; the listing is the set of files in that directory tree
    /// (matching `delete_prefix`, which removes a directory tree, and Java's
    /// `HadoopFileIO.listPrefix` over a hierarchical filesystem, which walks
    /// `FileSystem.listFiles(prefix, recursive = true)`). Because the prefix is a directory
    /// boundary, a sibling directory `ab2/` is NOT reported for prefix `ab` — only entries
    /// genuinely nested under the `ab` directory are returned. A `prefix` that does not name
    /// an existing directory (a plain file, or a path that does not exist) yields an empty
    /// list, never an error — a legitimately empty listing must not be confused with a
    /// failure.
    ///
    /// Only files are reported; directories themselves are descended into but never emitted
    /// as entries. The walk uses an explicit stack (not recursion) so an arbitrarily deep
    /// tree cannot overflow the call stack.
    ///
    /// # Symlinks are skipped (deliberate, safety-load-bearing)
    ///
    /// Each entry is classified with [`std::fs::DirEntry::metadata`], which does NOT follow
    /// symlinks — so a symlink is neither descended into nor emitted as a file. This is the
    /// conservative posture an orphan-file sweep needs: a symlinked directory cannot form a
    /// walk cycle (`a/loop -> a` terminates), and a symlink that escapes the prefix
    /// (`table/out -> /elsewhere`) cannot pull files OUTSIDE the table root into the listing —
    /// which would otherwise become deletion of live data outside the table. It also matches
    /// `delete_prefix`: `remove_dir_all` removes a directory symlink itself but never follows
    /// it to delete the target, so `list` and `delete_prefix` agree to leave symlink targets
    /// alone. Do NOT change this to follow symlinks without re-opening that hole.
    async fn list(&self, prefix: &str) -> Result<Vec<FileInfo>> {
        let root = Self::normalize_path(prefix);
        if !root.is_dir() {
            return Ok(Vec::new());
        }

        let mut files = Vec::new();
        let mut directories = vec![root];
        while let Some(directory) = directories.pop() {
            let entries = fs::read_dir(&directory).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to read directory {}: {}", directory.display(), e),
                )
            })?;
            for entry in entries {
                let entry = entry.map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "Failed to read directory entry under {}: {}",
                            directory.display(),
                            e
                        ),
                    )
                })?;
                let entry_path = entry.path();
                let metadata = entry.metadata().map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!("Failed to stat {}: {}", entry_path.display(), e),
                    )
                })?;

                if metadata.is_dir() {
                    directories.push(entry_path);
                    continue;
                }
                // Symlinks and other special entries are not regular files; skip them so the
                // listing reports only real data files (Java's listFiles likewise yields file
                // statuses, and a symlink target is not an orphan candidate here).
                if !metadata.is_file() {
                    continue;
                }

                let modified = metadata.modified().map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "Failed to read modification time for {}: {}",
                            entry_path.display(),
                            e
                        ),
                    )
                })?;
                let location = entry_path
                    .to_str()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Path {} is not valid UTF-8", entry_path.display()),
                        )
                    })?
                    .to_string();
                files.push(FileInfo::new(
                    location,
                    metadata.len(),
                    modified_time_to_millis(modified),
                ));
            }
        }
        Ok(files)
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// File reader for local filesystem storage.
#[derive(Debug)]
pub struct LocalFsFileRead {
    file: std::sync::Mutex<fs::File>,
}

impl LocalFsFileRead {
    /// Create a new `LocalFsFileRead` with the given file.
    pub fn new(file: fs::File) -> Self {
        Self {
            file: std::sync::Mutex::new(file),
        }
    }
}

#[async_trait]
impl FileRead for LocalFsFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let mut file = self.file.lock().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire file lock: {e}"),
            )
        })?;

        file.seek(SeekFrom::Start(range.start)).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to seek to position {}: {}", range.start, e),
            )
        })?;

        let len = (range.end - range.start) as usize;
        let mut buffer = vec![0u8; len];
        file.read_exact(&mut buffer).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to read {len} bytes: {e}"),
            )
        })?;

        Ok(Bytes::from(buffer))
    }
}

/// File writer for local filesystem storage.
///
/// This struct implements `FileWrite` for writing to local files.
#[derive(Debug)]
pub struct LocalFsFileWrite {
    file: Option<fs::File>,
}

impl LocalFsFileWrite {
    /// Create a new `LocalFsFileWrite` for the given file.
    pub fn new(file: fs::File) -> Self {
        Self { file: Some(file) }
    }
}

#[async_trait]
impl FileWrite for LocalFsFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Cannot write to closed file"))?;

        file.write_all(&bs).map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to write to file: {e}"),
            )
        })?;

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let file = self
            .file
            .take()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "File already closed"))?;

        file.sync_all()
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to sync file: {e}")))?;

        Ok(())
    }
}

/// Factory for creating `LocalFsStorage` instances.
///
/// This factory implements `StorageFactory` and creates `LocalFsStorage`
/// instances for the "file" scheme.
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::io::{StorageConfig, StorageFactory, LocalFsStorageFactory};
///
/// let factory = LocalFsStorageFactory;
/// let config = StorageConfig::new();
/// let storage = factory.build(&config)?;
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LocalFsStorageFactory;

#[typetag::serde]
impl StorageFactory for LocalFsStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(LocalFsStorage::new()))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    /// Risk: a secs/millis confusion or a wrong epoch base in the mtime->createdAtMillis
    /// conversion would feed A2 wrong timestamps. Pins EXACT values at the boundaries: epoch ->
    /// 0, epoch + 1500 ms -> 1500 (proving milliseconds, not seconds), and a pre-epoch
    /// SystemTime clamps to 0 rather than going negative.
    #[test]
    fn test_modified_time_to_millis_is_exact_and_clamps_pre_epoch() {
        use std::time::Duration;

        assert_eq!(modified_time_to_millis(UNIX_EPOCH), 0);
        assert_eq!(
            modified_time_to_millis(UNIX_EPOCH + Duration::from_millis(1500)),
            1500,
            "must report milliseconds since the epoch, not seconds"
        );
        assert_eq!(
            modified_time_to_millis(UNIX_EPOCH - Duration::from_secs(1)),
            0,
            "a pre-epoch modification time must clamp to 0, never go negative"
        );
    }

    #[test]
    fn test_normalize_path() {
        // Test file:/// prefix
        assert_eq!(
            LocalFsStorage::normalize_path("file:///path/to/file"),
            PathBuf::from("/path/to/file")
        );

        // Test file:// prefix (without leading slash in path)
        assert_eq!(
            LocalFsStorage::normalize_path("file://path/to/file"),
            PathBuf::from("/path/to/file")
        );

        // Test file:/ prefix
        assert_eq!(
            LocalFsStorage::normalize_path("file:/path/to/file"),
            PathBuf::from("/path/to/file")
        );

        // Test bare path
        assert_eq!(
            LocalFsStorage::normalize_path("/path/to/file"),
            PathBuf::from("/path/to/file")
        );
    }

    #[tokio::test]
    async fn test_local_fs_storage_write_read() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let path = tmp_dir.path().join("test.txt");
        let path_str = path.to_str().unwrap();
        let content = Bytes::from("Hello, World!");

        // Write
        storage.write(path_str, content.clone()).await.unwrap();

        // Read
        let read_content = storage.read(path_str).await.unwrap();
        assert_eq!(read_content, content);
    }

    #[tokio::test]
    async fn test_local_fs_storage_exists() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let path = tmp_dir.path().join("test.txt");
        let path_str = path.to_str().unwrap();

        // File doesn't exist initially
        assert!(!storage.exists(path_str).await.unwrap());

        // Write file
        storage.write(path_str, Bytes::from("test")).await.unwrap();

        // File exists now
        assert!(storage.exists(path_str).await.unwrap());
    }

    #[tokio::test]
    async fn test_local_fs_storage_metadata() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let path = tmp_dir.path().join("test.txt");
        let path_str = path.to_str().unwrap();
        let content = Bytes::from("Hello, World!");

        storage.write(path_str, content.clone()).await.unwrap();

        let metadata = storage.metadata(path_str).await.unwrap();
        assert_eq!(metadata.size, content.len() as u64);
    }

    #[tokio::test]
    async fn test_local_fs_storage_delete() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let path = tmp_dir.path().join("test.txt");
        let path_str = path.to_str().unwrap();

        storage.write(path_str, Bytes::from("test")).await.unwrap();
        assert!(storage.exists(path_str).await.unwrap());

        storage.delete(path_str).await.unwrap();
        assert!(!storage.exists(path_str).await.unwrap());
    }

    #[tokio::test]
    async fn test_local_fs_storage_delete_prefix() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let dir_path = tmp_dir.path().join("subdir");
        let file1 = dir_path.join("file1.txt");
        let file2 = dir_path.join("file2.txt");

        // Create files in subdirectory
        storage
            .write(file1.to_str().unwrap(), Bytes::from("1"))
            .await
            .unwrap();
        storage
            .write(file2.to_str().unwrap(), Bytes::from("2"))
            .await
            .unwrap();

        // Delete prefix (directory)
        storage
            .delete_prefix(dir_path.to_str().unwrap())
            .await
            .unwrap();

        // Directory should be deleted
        assert!(!dir_path.exists());
    }

    #[tokio::test]
    async fn test_local_fs_storage_reader() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let path = tmp_dir.path().join("test.txt");
        let path_str = path.to_str().unwrap();
        let content = Bytes::from("Hello, World!");

        storage.write(path_str, content.clone()).await.unwrap();

        let reader = storage.reader(path_str).await.unwrap();
        let read_content = reader.read(0..content.len() as u64).await.unwrap();
        assert_eq!(read_content, content);

        // Test partial read
        let partial = reader.read(0..5).await.unwrap();
        assert_eq!(partial, Bytes::from("Hello"));
    }

    #[tokio::test]
    async fn test_local_fs_storage_writer() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let path = tmp_dir.path().join("test.txt");
        let path_str = path.to_str().unwrap();

        let mut writer = storage.writer(path_str).await.unwrap();
        writer.write(Bytes::from("Hello, ")).await.unwrap();
        writer.write(Bytes::from("World!")).await.unwrap();
        writer.close().await.unwrap();

        let content = storage.read(path_str).await.unwrap();
        assert_eq!(content, Bytes::from("Hello, World!"));
    }

    #[tokio::test]
    async fn test_local_fs_file_write_double_close() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let path = tmp_dir.path().join("test.txt");
        let path_str = path.to_str().unwrap();

        let mut writer = storage.writer(path_str).await.unwrap();
        writer.write(Bytes::from("test")).await.unwrap();
        writer.close().await.unwrap();

        // Second close should fail
        let result = writer.close().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_local_fs_file_write_after_close() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let path = tmp_dir.path().join("test.txt");
        let path_str = path.to_str().unwrap();

        let mut writer = storage.writer(path_str).await.unwrap();
        writer.close().await.unwrap();

        // Write after close should fail
        let result = writer.write(Bytes::from("test")).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_local_fs_storage_factory() {
        let factory = LocalFsStorageFactory;
        let config = StorageConfig::new();
        let storage = factory.build(&config).unwrap();

        // Verify we got a valid storage instance
        assert!(format!("{storage:?}").contains("LocalFsStorage"));
    }

    #[tokio::test]
    async fn test_local_fs_creates_parent_directories() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let path = tmp_dir.path().join("a/b/c/test.txt");
        let path_str = path.to_str().unwrap();

        // Write should create parent directories
        storage.write(path_str, Bytes::from("test")).await.unwrap();

        assert!(path.exists());
    }

    /// Risk: a missed file at depth, or a directory leaking in as a "file", would make A2's
    /// orphan set wrong. Pins the EXACT recursive file set across nested subdirectories,
    /// correct sizes, and plausible (>0, <= now) timestamps.
    #[tokio::test]
    async fn test_list_returns_exact_recursive_file_set_with_sizes_and_times() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let root = tmp_dir.path();

        // Build a nested tree: top-level file + two levels of subdirectories.
        let top = root.join("top.txt");
        let nested = root.join("sub/nested.txt");
        let deep = root.join("sub/deeper/deep.txt");
        storage
            .write(top.to_str().unwrap(), Bytes::from("a"))
            .await
            .unwrap();
        storage
            .write(nested.to_str().unwrap(), Bytes::from("bb"))
            .await
            .unwrap();
        storage
            .write(deep.to_str().unwrap(), Bytes::from("ccc"))
            .await
            .unwrap();
        // An empty directory must contribute nothing (no directories-as-files).
        fs::create_dir_all(root.join("sub/empty_dir")).unwrap();

        let now_millis = modified_time_to_millis(SystemTime::now());
        let listed = storage.list(root.to_str().unwrap()).await.unwrap();

        // Exactly the three files, no directories.
        let mut locations: Vec<String> = listed.iter().map(|f| f.location.clone()).collect();
        locations.sort();
        let mut expected = vec![
            top.to_str().unwrap().to_string(),
            nested.to_str().unwrap().to_string(),
            deep.to_str().unwrap().to_string(),
        ];
        expected.sort();
        assert_eq!(locations, expected);

        // Sizes match the written bytes, by location.
        let size_of = |path: &std::path::Path| {
            listed
                .iter()
                .find(|f| f.location == path.to_str().unwrap())
                .unwrap()
                .size
        };
        assert_eq!(size_of(&top), 1);
        assert_eq!(size_of(&nested), 2);
        assert_eq!(size_of(&deep), 3);

        // Timestamps are plausible: strictly positive and not in the future.
        for file in &listed {
            assert!(
                file.created_at_millis > 0,
                "expected a positive mtime, got {}",
                file.created_at_millis
            );
            assert!(
                file.created_at_millis <= now_millis,
                "mtime {} should not exceed now {now_millis}",
                file.created_at_millis
            );
        }
    }

    /// Risk: over-listing is over-deletion in A2. A sibling directory `ab2/` must NEVER appear
    /// when listing the `ab/` directory prefix (directory-boundary semantics).
    #[tokio::test]
    async fn test_list_excludes_sibling_directory_outside_prefix() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let root = tmp_dir.path();

        let inside = root.join("ab/inside.txt");
        let sibling = root.join("ab2/outside.txt");
        storage
            .write(inside.to_str().unwrap(), Bytes::from("in"))
            .await
            .unwrap();
        storage
            .write(sibling.to_str().unwrap(), Bytes::from("out"))
            .await
            .unwrap();

        let listed = storage
            .list(root.join("ab").to_str().unwrap())
            .await
            .unwrap();

        let locations: Vec<&str> = listed.iter().map(|f| f.location.as_str()).collect();
        assert_eq!(locations, vec![inside.to_str().unwrap()]);
        assert!(
            !locations.contains(&sibling.to_str().unwrap()),
            "sibling ab2/ leaked into the ab/ listing"
        );
    }

    /// Risk: an empty directory must be a legitimate empty answer, not an error (a downstream
    /// caller distinguishes "no files" from "could not list").
    #[tokio::test]
    async fn test_list_empty_directory_is_empty_not_error() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let empty = tmp_dir.path().join("empty");
        fs::create_dir_all(&empty).unwrap();

        let listed = storage.list(empty.to_str().unwrap()).await.unwrap();
        assert!(listed.is_empty());
    }

    /// Risk: a prefix that is not an existing directory (here, never created) must yield an
    /// empty list rather than erroring, matching `delete_prefix`'s no-op-on-missing behavior.
    #[tokio::test]
    async fn test_list_nonexistent_prefix_is_empty() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let missing = tmp_dir.path().join("does_not_exist");

        let listed = storage.list(missing.to_str().unwrap()).await.unwrap();
        assert!(listed.is_empty());
    }

    /// Risk: a plain file given as the prefix must not be reported as if it were its own
    /// directory listing (local_fs uses directory semantics — a file is not a directory).
    #[tokio::test]
    async fn test_list_file_path_as_prefix_is_empty() {
        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let file = tmp_dir.path().join("a.txt");
        storage
            .write(file.to_str().unwrap(), Bytes::from("x"))
            .await
            .unwrap();

        let listed = storage.list(file.to_str().unwrap()).await.unwrap();
        assert!(listed.is_empty());
    }

    /// Risk: a symlinked directory forming a cycle (`a/loop -> a`) must NOT make the walk loop
    /// forever or error — it must terminate. The walk inspects each entry with `lstat`-style
    /// metadata (`DirEntry::metadata`, which does not follow symlinks), so a symlink is neither
    /// descended into nor emitted. Pins termination + that the symlink contributes no entry.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_list_symlink_cycle_terminates_and_skips_symlink() {
        use std::os::unix::fs::symlink;

        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let root = tmp_dir.path();

        let real = root.join("a/real.txt");
        storage
            .write(real.to_str().unwrap(), Bytes::from("x"))
            .await
            .unwrap();
        // a/loop -> a  (a directory symlink that closes a cycle).
        symlink(root.join("a"), root.join("a/loop")).unwrap();

        let listed = storage
            .list(root.join("a").to_str().unwrap())
            .await
            .unwrap();
        let locations: Vec<&str> = listed.iter().map(|f| f.location.as_str()).collect();
        // Only the real file; the cycle did not hang and the symlink emitted nothing.
        assert_eq!(locations, vec![real.to_str().unwrap()]);
    }

    /// Risk: over-listing is over-deletion in A2. A symlink that escapes the prefix — pointing
    /// at a directory OR a file OUTSIDE the table root — must never surface outside paths in the
    /// listing, or the orphan sweep could delete live data outside the table. Pins that neither
    /// a directory-symlink nor a file-symlink to an outside location leaks into the listing.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_list_symlink_escaping_prefix_does_not_leak_outside_files() {
        use std::os::unix::fs::symlink;

        let tmp_dir = TempDir::new().unwrap();
        let outside_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let root = tmp_dir.path();

        let inside = root.join("table/inside.txt");
        storage
            .write(inside.to_str().unwrap(), Bytes::from("in"))
            .await
            .unwrap();
        let outside_file = outside_dir.path().join("secret.txt");
        storage
            .write(outside_file.to_str().unwrap(), Bytes::from("secret"))
            .await
            .unwrap();
        // A directory-symlink and a file-symlink that both escape the `table` prefix.
        symlink(outside_dir.path(), root.join("table/out_dir")).unwrap();
        symlink(&outside_file, root.join("table/out_file")).unwrap();

        let listed = storage
            .list(root.join("table").to_str().unwrap())
            .await
            .unwrap();
        let locations: Vec<&str> = listed.iter().map(|f| f.location.as_str()).collect();
        assert_eq!(locations, vec![inside.to_str().unwrap()]);
        assert!(
            !locations.iter().any(|location| location.contains("secret")),
            "an outside file leaked through a symlink into the listing: {locations:?}"
        );
    }

    /// Risk: a mid-walk failure (here an unreadable subdirectory) must be PROPAGATED, not
    /// silently swallowed — a silent skip makes the listing quietly incomplete, and A2 would
    /// then treat genuinely-live files it never saw as already gone (or, with later inversions,
    /// fail to protect them). Java's Hadoop `RemoteIterator` throws on such I/O errors; this
    /// pins the same loud posture: an `Unexpected` error naming the unreadable directory.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_list_unreadable_subdirectory_errors_loudly_not_silently_skipped() {
        use std::os::unix::fs::PermissionsExt;

        let tmp_dir = TempDir::new().unwrap();
        let storage = LocalFsStorage::new();
        let root = tmp_dir.path();

        storage
            .write(
                root.join("readable.txt").to_str().unwrap(),
                Bytes::from("ok"),
            )
            .await
            .unwrap();
        let locked = root.join("locked");
        storage
            .write(
                locked.join("hidden.txt").to_str().unwrap(),
                Bytes::from("secret"),
            )
            .await
            .unwrap();
        // Drop read+exec so `read_dir` on `locked` fails mid-walk.
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();

        let result = storage.list(root.to_str().unwrap()).await;

        // Restore permissions first so the TempDir can be cleaned up regardless of assertions.
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

        let error = result.expect_err("an unreadable subdirectory must surface as an error");
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(
            error.message().contains("locked"),
            "error must name the unreadable directory, got: {}",
            error.message()
        );
    }
}
