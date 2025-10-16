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

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use opendal::Operator;
use url::Url;

use crate::io::storage_builder::create_storage_builder;
use crate::{Error, ErrorKind, Result};

/// todo doc
pub type InputFileRef = Arc<dyn InputFile>;
/// todo doc
pub type OutputFileRef = Arc<dyn OutputFile>;
/// todo doc
pub type FileReadRef = Arc<dyn FileRead>;
/// todo doc
pub type FileWriteRef = Arc<dyn FileWrite>;

/// Trait for storage operations in Iceberg
#[async_trait]
pub trait Storage: Debug + Send + Sync {
    /// Check if a file exists at the given path
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Delete a file at the given path
    async fn delete(&self, path: &str) -> Result<()>;

    /// Remove a directory and all its contents recursively
    async fn remove_dir_all(&self, path: &str) -> Result<()>;

    /// Create a new input file for reading
    fn new_input(&self, path: &str) -> Result<InputFileRef>;

    /// Create a new output file for writing
    fn new_output(&self, path: &str) -> Result<OutputFileRef>;
}

/// Common interface for all storage builders.
pub trait StorageBuilder: Debug + Send + Sync {
    /// Create a new storage instance with the given properties and extensions.
    fn build(
        &self,
        props: HashMap<String, String>,
        extensions: Extensions,
    ) -> Result<Arc<dyn Storage>>;
}

/// Trait for reading files
#[async_trait]
pub trait InputFile: Send + Sync + Debug {
    /// Get the absolute path to the file
    fn location(&self) -> &str;

    /// Check if the file exists
    async fn exists(&self) -> Result<bool>;

    /// Get metadata about the file
    async fn metadata(&self) -> Result<FileMetadata>;

    /// Read the entire file content
    async fn read(&self) -> Result<Bytes>;

    /// Create a reader for the file
    async fn reader(&self) -> Result<FileReadRef>;
}

/// Trait for writing files
#[async_trait]
pub trait OutputFile: Send + Sync + Debug {
    /// Get the absolute path to the file
    fn location(&self) -> &str;

    /// Check if the file exists
    async fn exists(&self) -> Result<bool>;

    /// Delete the file
    async fn delete(&self) -> Result<()>;

    /// Convert to an input file
    fn to_input_file(self: Arc<Self>) -> InputFileRef;

    /// Write bytes to the file
    async fn write(&self, bs: Bytes) -> Result<()>;

    /// Create a writer for the file
    async fn writer(&self) -> Result<Box<dyn FileWrite>>;
}

/// FileIO implementation, used to manipulate files in underlying storage.
///
/// # Note
///
/// All path passed to `FileIO` must be absolute path starting with scheme string used to construct `FileIO`.
/// For example, if you construct `FileIO` with `s3a` scheme, then all path passed to `FileIO` must start with `s3a://`.
///
/// Supported storages:
///
/// | Storage            | Feature Flag      | Expected Path Format             | Schemes                       |
/// |--------------------|-------------------|----------------------------------| ------------------------------|
/// | Local file system  | `storage-fs`      | `file`                           | `file://path/to/file`         |
/// | Memory             | `storage-memory`  | `memory`                         | `memory://path/to/file`       |
/// | S3                 | `storage-s3`      | `s3`, `s3a`                      | `s3://<bucket>/path/to/file`  |
/// | GCS                | `storage-gcs`     | `gs`, `gcs`                      | `gs://<bucket>/path/to/file`  |
/// | OSS                | `storage-oss`     | `oss`                            | `oss://<bucket>/path/to/file` |
/// | Azure Datalake     | `storage-azdls`   | `abfs`, `abfss`, `wasb`, `wasbs` | `abfs://<filesystem>@<account>.dfs.core.windows.net/path/to/file` or `wasb://<container>@<account>.blob.core.windows.net/path/to/file` |
#[derive(Clone, Debug)]
pub struct FileIO {
    builder: FileIOBuilder,

    inner: Arc<dyn Storage>,
}

impl FileIO {
    /// Convert FileIO into [`FileIOBuilder`] which used to build this FileIO.
    ///
    /// This function is useful when you want serialize and deserialize FileIO across
    /// distributed systems.
    pub fn into_builder(self) -> FileIOBuilder {
        self.builder
    }

    /// Try to infer file io scheme from path. See [`FileIO`] for supported schemes.
    ///
    /// - If it's a valid url, for example `s3://bucket/a`, url scheme will be used, and the rest of the url will be ignored.
    /// - If it's not a valid url, will try to detect if it's a file path.
    ///
    /// Otherwise will return parsing error.
    pub fn from_path(path: impl AsRef<str>) -> Result<FileIOBuilder> {
        let url = Url::parse(path.as_ref())
            .map_err(Error::from)
            .or_else(|e| {
                Url::from_file_path(path.as_ref()).map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Input is neither a valid url nor path",
                    )
                    .with_context("input", path.as_ref().to_string())
                    .with_source(e)
                })
            })?;

        Ok(FileIOBuilder::new(url.scheme()))
    }

    /// Deletes file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        self.inner.delete(path.as_ref()).await
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    #[deprecated(note = "use remove_dir_all instead", since = "0.4.0")]
    pub async fn remove_all(&self, path: impl AsRef<str>) -> Result<()> {
        // todo fix this later
        self.remove_dir_all(path).await
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    ///
    /// # Behavior
    ///
    /// - If the path is a file or not exist, this function will be no-op.
    /// - If the path is a empty directory, this function will remove the directory itself.
    /// - If the path is a non-empty directory, this function will remove the directory and all nested files and directories.
    pub async fn remove_dir_all(&self, path: impl AsRef<str>) -> Result<()> {
        self.inner.remove_dir_all(path.as_ref()).await
    }

    /// Check file exists.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        self.inner.exists(path.as_ref()).await
    }

    /// Creates input file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFileRef> {
        self.inner.new_input(path.as_ref())
    }

    /// Creates output file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFileRef> {
        self.inner.new_output(path.as_ref())
    }
}

/// Container for storing type-safe extensions used to configure underlying FileIO behavior.
#[derive(Clone, Debug, Default)]
pub struct Extensions(HashMap<TypeId, Arc<dyn Any + Send + Sync>>);

impl Extensions {
    /// Add an extension.
    pub fn add<T: Any + Send + Sync>(&mut self, ext: T) {
        self.0.insert(TypeId::of::<T>(), Arc::new(ext));
    }

    /// Extends the current set of extensions with another set of extensions.
    pub fn extend(&mut self, extensions: Extensions) {
        self.0.extend(extensions.0);
    }

    /// Fetch an extension.
    pub fn get<T>(&self) -> Option<Arc<T>>
    where T: 'static + Send + Sync + Clone {
        let type_id = TypeId::of::<T>();
        self.0
            .get(&type_id)
            .and_then(|arc_any| Arc::clone(arc_any).downcast::<T>().ok())
    }
}

/// Builder for [`FileIO`].
#[derive(Clone, Debug)]
pub struct FileIOBuilder {
    /// This is used to infer scheme of operator.
    ///
    /// If this is `None`, then [`FileIOBuilder::build`](FileIOBuilder::build) will build a local file io.
    scheme_str: Option<String>,
    /// Arguments for operator.
    props: HashMap<String, String>,
    /// Extensions to configure the underlying Storage behavior.
    extensions: Extensions,
}

impl FileIOBuilder {
    /// Creates a new builder with scheme.
    /// See [`FileIO`] for supported schemes.
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            props: HashMap::default(),
            extensions: Extensions::default(),
        }
    }

    /// Creates a new builder for local file io.
    pub fn new_fs_io() -> Self {
        Self {
            scheme_str: None,
            props: HashMap::default(),
            extensions: Extensions::default(),
        }
    }

    /// Fetch the scheme string.
    ///
    /// The scheme_str will be empty if it's None.
    pub fn into_parts(self) -> (String, HashMap<String, String>) {
        (self.scheme_str.unwrap_or_default(), self.props)
    }

    /// Add argument for operator.
    pub fn with_prop(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.props.insert(key.to_string(), value.to_string());
        self
    }

    /// Add argument for operator.
    pub fn with_props(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.props
            .extend(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    /// Add an extension to the file IO builder.
    pub fn with_extension<T: Any + Send + Sync>(mut self, ext: T) -> Self {
        self.extensions.add(ext);
        self
    }

    /// Add extensions to the builder.
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions.extend(extensions);
        self
    }

    /// Fetch an extension from the file IO builder.
    pub fn extension<T>(&self) -> Option<Arc<T>>
    where T: 'static + Send + Sync + Clone {
        self.extensions.get::<T>()
    }

    /// Builds [`FileIO`].
    pub fn build(self) -> Result<FileIO> {
        // Use the scheme to determine the storage type
        let scheme = self.scheme_str.clone().unwrap_or_default();
        let builder = create_storage_builder(scheme.as_str())?;

        // Build storage with props and extensions
        let storage = builder.build(self.props.clone(), self.extensions.clone())?;

        Ok(FileIO {
            builder: self,
            inner: storage,
        })
    }
}

/// The struct the represents the metadata of a file.
///
/// TODO: we can add last modified time, content type, etc. in the future.
pub struct FileMetadata {
    /// The size of the file.
    pub size: u64,
}

/// Trait for reading file.
///
/// # TODO
///
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileRead: Send + Sync + Unpin + 'static {
    /// Read file content with given range.
    ///
    /// TODO: we can support reading non-contiguous bytes in the future.
    async fn read(&self, range: Range<u64>) -> Result<Bytes>;
}

#[async_trait::async_trait]
impl FileRead for opendal::Reader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(self, range).await?.to_bytes())
    }
}

/// todo fix visibility qualifier after moving this to opendal-storage
/// Input file is used for reading from files.
#[derive(Debug)]
pub struct OpenDALInputFile {
    /// OpenDAL Operator for file operations
    pub op: Operator,
    /// Absolute path of file.
    pub path: String,
    /// Relative path of file to uri, starts at [`relative_path_pos`]
    pub relative_path_pos: usize,
}

#[async_trait]
impl InputFile for OpenDALInputFile {
    /// Absolute path to root uri.
    fn location(&self) -> &str {
        &self.path
    }

    /// Check if file exists.
    async fn exists(&self) -> Result<bool> {
        Ok(self.op.exists(&self.path[self.relative_path_pos..]).await?)
    }

    /// Fetch and returns metadata of file.
    async fn metadata(&self) -> Result<FileMetadata> {
        let meta = self.op.stat(&self.path[self.relative_path_pos..]).await?;

        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    /// Read and returns whole content of file.
    ///
    /// For continuous reading, use [`Self::reader`] instead.
    async fn read(&self) -> Result<Bytes> {
        Ok(self
            .op
            .read(&self.path[self.relative_path_pos..])
            .await?
            .to_bytes())
    }

    /// Creates [`FileRead`] for continuous reading.
    ///
    /// For one-time reading, use [`Self::read`] instead.
    async fn reader(&self) -> Result<FileReadRef> {
        Ok(Arc::new(
            self.op.reader(&self.path[self.relative_path_pos..]).await?,
        ))
    }
}

/// Trait for writing file.
///
/// # TODO
///
/// It's possible for us to remove the async_trait, but we need to figure
/// out how to handle the object safety.
#[async_trait::async_trait]
pub trait FileWrite: Send + Unpin + 'static {
    /// Write bytes to file.
    ///
    /// TODO: we can support writing non-contiguous bytes in the future.
    async fn write(&mut self, bs: Bytes) -> Result<()>;

    /// Close file.
    ///
    /// Calling close on closed file will generate an error.
    async fn close(&mut self) -> Result<()>;
}

#[async_trait::async_trait]
impl FileWrite for opendal::Writer {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        Ok(opendal::Writer::write(self, bs).await?)
    }

    async fn close(&mut self) -> Result<()> {
        let _ = opendal::Writer::close(self).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl FileWrite for Box<dyn FileWrite> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.as_mut().write(bs).await
    }

    async fn close(&mut self) -> Result<()> {
        self.as_mut().close().await
    }
}

/// todo fix visibility qualifier after moving this to opendal-storage
/// Output file is used for writing to files..
#[derive(Debug)]
pub struct OpenDALOutputFile {
    /// OpenDAL Operator
    pub op: Operator,
    /// Absolution path of file.
    pub path: String,
    /// Relative path of file to uri, starts at [`relative_path_pos`]
    pub relative_path_pos: usize,
}

#[async_trait]
impl OutputFile for OpenDALOutputFile {
    /// Relative path to root uri.
    fn location(&self) -> &str {
        &self.path
    }

    /// Checks if file exists.
    async fn exists(&self) -> Result<bool> {
        Ok(self.op.exists(&self.path[self.relative_path_pos..]).await?)
    }

    /// Deletes file.
    ///
    /// If the file does not exist, it will not return error.
    async fn delete(&self) -> Result<()> {
        Ok(self.op.delete(&self.path[self.relative_path_pos..]).await?)
    }

    /// Converts into [`InputFile`].
    fn to_input_file(self: Arc<Self>) -> InputFileRef {
        Arc::new(OpenDALInputFile {
            op: self.op.clone(),
            path: self.path.clone(),
            relative_path_pos: self.relative_path_pos,
        })
    }

    /// Create a new output file with given bytes.
    ///
    /// # Notes
    ///
    /// Calling `write` will overwrite the file if it exists.
    /// For continuous writing, use [`Self::writer`].
    async fn write(&self, bs: Bytes) -> Result<()> {
        let mut writer = self.writer().await?;
        writer.write(bs).await?;
        writer.close().await
    }

    /// Creates output file for continuous writing.
    ///
    /// # Notes
    ///
    /// For one-time writing, use [`Self::write`] instead.
    async fn writer(&self) -> Result<Box<dyn FileWrite>> {
        Ok(Box::new(
            self.op.writer(&self.path[self.relative_path_pos..]).await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{File, create_dir_all};
    use std::io::Write;
    use std::path::Path;

    use bytes::Bytes;
    use futures::AsyncReadExt;
    use futures::io::AllowStdIo;
    use tempfile::TempDir;

    use super::{FileIO, FileIOBuilder};

    fn create_local_file_io() -> FileIO {
        FileIOBuilder::new_fs_io().build().unwrap()
    }

    fn write_to_file<P: AsRef<Path>>(s: &str, path: P) {
        create_dir_all(path.as_ref().parent().unwrap()).unwrap();
        let mut f = File::create(path).unwrap();
        write!(f, "{s}").unwrap();
    }

    async fn read_from_file<P: AsRef<Path>>(path: P) -> String {
        let mut f = AllowStdIo::new(File::open(path).unwrap());
        let mut s = String::new();
        f.read_to_string(&mut s).await.unwrap();
        s
    }

    #[tokio::test]
    async fn test_local_input_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let content = "Iceberg loves rust.";

        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);
        write_to_file(content, &full_path);

        let file_io = create_local_file_io();
        let input_file = file_io.new_input(&full_path).unwrap();

        assert!(input_file.exists().await.unwrap());
        // Remove heading slash
        assert_eq!(&full_path, input_file.location());
        let read_content = read_from_file(full_path).await;

        assert_eq!(content, &read_content);
    }

    #[tokio::test]
    async fn test_delete_local_file() {
        let tmp_dir = TempDir::new().unwrap();

        let a_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), "a.txt");
        let sub_dir_path = format!("{}/sub", tmp_dir.path().to_str().unwrap());
        let b_path = format!("{}/{}", sub_dir_path, "b.txt");
        let c_path = format!("{}/{}", sub_dir_path, "c.txt");
        write_to_file("Iceberg loves rust.", &a_path);
        write_to_file("Iceberg loves rust.", &b_path);
        write_to_file("Iceberg loves rust.", &c_path);

        let file_io = create_local_file_io();
        assert!(file_io.exists(&a_path).await.unwrap());

        // Remove a file should be no-op.
        file_io.remove_dir_all(&a_path).await.unwrap();
        assert!(file_io.exists(&a_path).await.unwrap());

        // Remove a not exist dir should be no-op.
        file_io.remove_dir_all("not_exists/").await.unwrap();

        // Remove a dir should remove all files in it.
        file_io.remove_dir_all(&sub_dir_path).await.unwrap();
        assert!(!file_io.exists(&b_path).await.unwrap());
        assert!(!file_io.exists(&c_path).await.unwrap());
        assert!(file_io.exists(&a_path).await.unwrap());

        file_io.delete(&a_path).await.unwrap();
        assert!(!file_io.exists(&a_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_non_exist_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let file_io = create_local_file_io();
        assert!(!file_io.exists(&full_path).await.unwrap());
        assert!(file_io.delete(&full_path).await.is_ok());
        assert!(file_io.remove_dir_all(&full_path).await.is_ok());
    }

    #[tokio::test]
    async fn test_local_output_file() {
        let tmp_dir = TempDir::new().unwrap();

        let file_name = "a.txt";
        let content = "Iceberg loves rust.";

        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let file_io = create_local_file_io();
        let output_file = file_io.new_output(&full_path).unwrap();

        assert!(!output_file.exists().await.unwrap());
        {
            output_file.write(content.into()).await.unwrap();
        }

        assert_eq!(&full_path, output_file.location());

        let read_content = read_from_file(full_path).await;

        assert_eq!(content, &read_content);
    }

    #[test]
    fn test_create_file_from_path() {
        let io = FileIO::from_path("/tmp/a").unwrap();
        assert_eq!("file", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("file:/tmp/b").unwrap();
        assert_eq!("file", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("file:///tmp/c").unwrap();
        assert_eq!("file", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("s3://bucket/a").unwrap();
        assert_eq!("s3", io.scheme_str.unwrap().as_str());

        let io = FileIO::from_path("tmp/||c");
        assert!(io.is_err());
    }

    #[tokio::test]
    async fn test_memory_io() {
        let io = FileIOBuilder::new("memory").build().unwrap();

        let path = format!("{}/1.txt", TempDir::new().unwrap().path().to_str().unwrap());

        let output_file = io.new_output(&path).unwrap();
        output_file.write("test".into()).await.unwrap();

        assert!(io.exists(&path.clone()).await.unwrap());
        let input_file = io.new_input(&path).unwrap();
        let content = input_file.read().await.unwrap();
        assert_eq!(content, Bytes::from("test"));

        io.delete(&path).await.unwrap();
        assert!(!io.exists(&path).await.unwrap());
    }
}
