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

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use opendal::Operator;
use url::Url;

use super::storage::Storage;
use crate::{Error, ErrorKind, Result};

/// FileIO implementation, used to manipulate files in underlying storage.
///
/// # Note
///
/// All path passed to `FileIO` must be absolute path starting with scheme string used to construct `FileIO`.
/// For example, if you construct `FileIO` with `s3a` scheme, then all path passed to `FileIO` must start with `s3a://`.
///
/// Supported storages:
///
/// | Storage            | Feature Flag     | Schemes    |
/// |--------------------|-------------------|------------|
/// | Local file system  | `storage-fs`      | `file`     |
/// | Memory             | `storage-memory`  | `memory`   |
/// | S3                 | `storage-s3`      | `s3`, `s3a`|
/// | GCS                | `storage-gcs`     | `gcs`       |
#[derive(Clone, Debug)]
pub struct FileIO {
    inner: Arc<Storage>,
}

impl FileIO {
    /// Try to infer file io scheme from path. See [`FileIO`] for supported schemes.
    ///
    /// - If it's a valid url, for example `s3://bucket/a`, url scheme will be used, and the rest of the url will be ignored.
    /// - If it's not a valid url, will try to detect if it's a file path.
    ///
    /// Otherwise will return parsing error.
    pub fn from_path(path: impl AsRef<str>) -> crate::Result<FileIOBuilder> {
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

    /// TODO: docs
    pub fn from_extension(ext: Arc<dyn FileIOExtension>) -> FileIO {
        Self {
            inner: Arc::new(Storage::Extension(ext)),
        }
    }

    /// Deletes file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        match self.inner.as_ref() {
            Storage::Extension(e) => e.delete(path.as_ref()).await,
            _ => {
                let (op, relative_path) = self.inner.create_operator(&path)?;
                Ok(op.delete(relative_path).await?)
            }
        }
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn remove_all(&self, path: impl AsRef<str>) -> Result<()> {
        match self.inner.as_ref() {
            Storage::Extension(e) => e.remove_all(path.as_ref()).await,
            _ => {
                let (op, relative_path) = self.inner.create_operator(&path)?;
                Ok(op.remove_all(relative_path).await?)
            }
        }
    }

    /// Check file exists.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub async fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        match self.inner.as_ref() {
            Storage::Extension(e) => e.exists(path.as_ref()).await,
            _ => {
                let (op, relative_path) = self.inner.create_operator(&path)?;
                Ok(op.exists(relative_path).await?)
            }
        }
    }

    /// Creates input file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        match self.inner.as_ref() {
            Storage::Extension(e) => e.new_input(path.as_ref()),
            _ => {
                let (op, relative_path) = self.inner.create_operator(&path)?;
                let path = path.as_ref().to_string();
                let relative_path_pos = path.len() - relative_path.len();
                Ok(InputFile::OpenDAL {
                    op,
                    path,
                    relative_path_pos,
                })
            }
        }
    }

    /// Creates output file.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        match self.inner.as_ref() {
            Storage::Extension(e) => e.new_output(path.as_ref()),
            _ => {
                let (op, relative_path) = self.inner.create_operator(&path)?;
                let path = path.as_ref().to_string();
                let relative_path_pos = path.len() - relative_path.len();
                Ok(OutputFile::OpenDAL {
                    op,
                    path,
                    relative_path_pos,
                })
            }
        }
    }
}

/// Builder for [`FileIO`].
#[derive(Debug)]
pub struct FileIOBuilder {
    /// This is used to infer scheme of operator.
    ///
    /// If this is `None`, then [`FileIOBuilder::build`](FileIOBuilder::build) will build a local file io.
    scheme_str: Option<String>,
    /// Arguments for operator.
    props: HashMap<String, String>,
}

impl FileIOBuilder {
    /// Creates a new builder with scheme.
    /// See [`FileIO`] for supported schemes.
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            props: HashMap::default(),
        }
    }

    /// Creates a new builder for local file io.
    pub fn new_fs_io() -> Self {
        Self {
            scheme_str: None,
            props: HashMap::default(),
        }
    }

    /// Fetch the scheme string.
    ///
    /// The scheme_str will be empty if it's None.
    pub(crate) fn into_parts(self) -> (String, HashMap<String, String>) {
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

    /// Builds [`FileIO`].
    pub fn build(self) -> crate::Result<FileIO> {
        let storage = Storage::build(self)?;
        Ok(FileIO {
            inner: Arc::new(storage),
        })
    }
}
/// TODO: docs
#[async_trait::async_trait]
pub trait FileIOExtension: Debug + Send + Sync {
    /// TODO: docs
    fn new_input(&self, path: &str) -> Result<InputFile>;
    /// TODO: docs
    fn new_output(&self, path: &str) -> Result<OutputFile>;
    /// TODO: docs
    async fn delete(&self, path: &str) -> Result<()>;
    /// TODO: docs
    async fn remove_all(&self, path: &str) -> Result<()>;
    /// TODO: docs
    async fn exists(&self, path: &str) -> Result<bool>;
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
pub trait FileRead: Send + Unpin + 'static {
    /// Read file content with given range.
    ///
    /// TODO: we can support reading non-contiguous bytes in the future.
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes>;
}

#[async_trait::async_trait]
impl FileRead for opendal::Reader {
    async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
        Ok(opendal::Reader::read(self, range).await?.to_bytes())
    }
}

/// Input file is used for reading from files.
#[derive(Debug)]
pub enum InputFile {
    /// TODO: docs
    OpenDAL {
        /// TODO: docs
        op: Operator,
        /// Absolution path of file.
        path: String,
        /// Relative path of file to uri, starts at [`relative_path_pos`]
        relative_path_pos: usize,
    },
    /// TODO: docs
    Extension(Arc<dyn InputFileExtension>),
}

/// TODO: docs
#[async_trait::async_trait]
pub trait InputFileExtension: Debug + Send + Sync {
    /// TODO: docs
    fn location(&self) -> &str;
    /// TODO: docs
    async fn exists(&self) -> crate::Result<bool>;
    /// TODO: docs
    async fn metadata(&self) -> crate::Result<FileMetadata>;
    /// TODO: docs
    async fn read(&self) -> crate::Result<Bytes>;
    // NOTE: async fn reader(&self) -> crate::Result<impl FileRead> cannot be implemented, as the
    // return type is not object safe
}

impl InputFile {
    /// Absolute path to root uri.
    pub fn location(&self) -> &str {
        match self {
            Self::OpenDAL {
                op: _,
                path,
                relative_path_pos: _,
            } => path,
            Self::Extension(e) => e.location(),
        }
    }

    /// Check if file exists.
    pub async fn exists(&self) -> crate::Result<bool> {
        match self {
            Self::OpenDAL {
                op,
                path,
                relative_path_pos,
            } => Ok(op.exists(&path[*relative_path_pos..]).await?),
            Self::Extension(e) => e.exists().await,
        }
    }

    /// Fetch and returns metadata of file.
    pub async fn metadata(&self) -> crate::Result<FileMetadata> {
        match self {
            Self::OpenDAL {
                op,
                path,
                relative_path_pos,
            } => {
                let meta = op.stat(&path[*relative_path_pos..]).await?;

                Ok(FileMetadata {
                    size: meta.content_length(),
                })
            }
            Self::Extension(e) => e.metadata().await,
        }
    }

    /// Read and returns whole content of file.
    ///
    /// For continues reading, use [`Self::reader`] instead.
    pub async fn read(&self) -> crate::Result<Bytes> {
        match self {
            Self::OpenDAL {
                op,
                path,
                relative_path_pos,
            } => Ok(op.read(&path[*relative_path_pos..]).await?.to_bytes()),
            Self::Extension(e) => e.read().await,
        }
    }

    /// Creates [`FileRead`] for continues reading.
    ///
    /// For one-time reading, use [`Self::read`] instead.
    pub async fn reader(&self) -> crate::Result<impl FileRead> {
        match self {
            Self::OpenDAL {
                op,
                path,
                relative_path_pos,
            } => Ok(op.reader(&path[*relative_path_pos..]).await?),
            Self::Extension(_) => unimplemented!(),
        }
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
    async fn write(&mut self, bs: Bytes) -> crate::Result<()>;

    /// Close file.
    ///
    /// Calling close on closed file will generate an error.
    async fn close(&mut self) -> crate::Result<()>;
}

#[async_trait::async_trait]
impl FileWrite for opendal::Writer {
    async fn write(&mut self, bs: Bytes) -> crate::Result<()> {
        Ok(opendal::Writer::write(self, bs).await?)
    }

    async fn close(&mut self) -> crate::Result<()> {
        Ok(opendal::Writer::close(self).await?)
    }
}

/// Output file is used for writing to files..
#[derive(Debug)]
pub enum OutputFile {
    /// TODO: docs
    OpenDAL {
        /// TODO: docs
        op: Operator,
        /// Absolution path of file.
        path: String,
        /// Relative path of file to uri, starts at [`relative_path_pos`]
        relative_path_pos: usize,
    },
    /// TODO: docs
    Extension(Arc<dyn OutputFileExtension>),
}

/// TODO: docs
#[async_trait::async_trait]
pub trait OutputFileExtension: Debug + Send + Sync {
    /// TODO: docs
    fn location(&self) -> &str;
    /// TODO: docs
    async fn exists(&self) -> crate::Result<bool>;
    /// TODO: docs
    fn to_input_file(&self) -> InputFile;
    /// TODO: docs
    async fn write(&self, bs: Bytes) -> crate::Result<()>;
    /// TODO: docs
    async fn writer(&self) -> crate::Result<Box<dyn FileWrite>>;
}

impl OutputFile {
    /// Relative path to root uri.
    pub fn location(&self) -> &str {
        match self {
            Self::OpenDAL {
                op: _,
                path,
                relative_path_pos: _,
            } => path,
            Self::Extension(e) => e.location(),
        }
    }

    /// Checks if file exists.
    pub async fn exists(&self) -> crate::Result<bool> {
        match self {
            Self::OpenDAL {
                op,
                path,
                relative_path_pos,
            } => Ok(op.exists(&path[*relative_path_pos..]).await?),
            Self::Extension(e) => e.exists().await,
        }
    }

    /// Converts into [`InputFile`].
    pub fn to_input_file(self) -> InputFile {
        match self {
            Self::OpenDAL {
                op,
                path,
                relative_path_pos,
            } => InputFile::OpenDAL {
                op,
                path,
                relative_path_pos,
            },
            Self::Extension(e) => e.to_input_file(),
        }
    }

    /// Create a new output file with given bytes.
    ///
    /// # Notes
    ///
    /// Calling `write` will overwrite the file if it exists.
    /// For continues writing, use [`Self::writer`].
    pub async fn write(&self, bs: Bytes) -> crate::Result<()> {
        match self {
            Self::OpenDAL { .. } => {
                let mut writer = self.writer().await?;
                writer.write(bs).await?;
                writer.close().await
            }
            Self::Extension(e) => e.write(bs).await,
        }
    }

    /// Creates output file for continues writing.
    ///
    /// # Notes
    ///
    /// For one-time writing, use [`Self::write`] instead.
    pub async fn writer(&self) -> crate::Result<Box<dyn FileWrite>> {
        match self {
            Self::OpenDAL {
                op,
                path,
                relative_path_pos,
            } => Ok(Box::new(op.writer(&path[*relative_path_pos..]).await?)),
            Self::Extension(e) => e.writer().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{create_dir_all, File};
    use std::io::Write;
    use std::path::Path;

    use bytes::Bytes;
    use futures::io::AllowStdIo;
    use futures::AsyncReadExt;
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

        file_io.remove_all(&sub_dir_path).await.unwrap();
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
        assert!(file_io.remove_all(&full_path).await.is_ok());
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
