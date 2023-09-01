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

//! File io implementation.
//!
//! File io is built on top of [`opendal`](https://docs.rs/opendal/latest/opendal/index.html), which already provided an abstraction over all kinds of storage.
//!
//! # How to build `FileIO`
//!
//! We provided a `FileIOBuilder` to build `FileIO` from scratch. For example:
//! ```rust
//! use iceberg::io::{FileIOBuilder, S3_ARGS_BUCKET, S3_ARGS_REGION};
//!
//! let file_io = FileIOBuilder::new("s3")
//!     .with_arg(S3_ARGS_BUCKET, "test_bucket")
//!     .with_arg(S3_ARGS_REGION, "us-east-1")
//!     .build()
//!     .unwrap();
//! ```
//!
//! We also provided a convenient method to build `FileIO` from url:
//!
//! ```rust
//! use iceberg::io::{FileIO, S3_ARGS_REGION};
//! let file_io = FileIO::build_from_url("s3a://test_bucket/warehouse")
//!     .unwrap()
//!     .with_arg(S3_ARGS_REGION, "us-east-1")
//!     .build()
//!     .unwrap();
//! ```
//!
//! # How to use `FileIO`
//!
//! Currently `FileIO` provides simple methods for file operations:
//!
//! - `delete`: Delete file.
//! - `is_exist`: Check if file exists.
//! - `new_input`: Create input file for reading.
//! - `new_output`: Create output file for writing.

use std::{collections::HashMap, sync::Arc};

use crate::{error::Result, Error, ErrorKind};
use futures::{AsyncRead, AsyncSeek, AsyncWrite};
use opendal::{Operator, Scheme};
use url::Url;

/// Following are arguments for s3 operator.
/// s3 root
pub const S3_ARGS_ROOT: &str = "root";
/// s3 bucket
pub const S3_ARGS_BUCKET: &str = "bucket";
/// s3 endpoint
pub const S3_ARGS_ENDPOINT: &str = "endpoint";
/// s3 region
pub const S3_ARGS_REGION: &str = "region";
/// s3 access key
pub const S3_ARGS_ACCESS_KEY_ID: &str = "access_key_id";
/// s3 access secret
pub const S3_ARGS_ACCESS_KEY: &str = "secret_access_key";

const ROOT_PATH: &str = "/";
/// FileIO implementation.
///
/// # Note
///
/// `FileIO` keep a [`root_uri`](FileIO::root_uri) to indicate the root path of file io. The arguments passed to operations should be in one for following format:
/// * Absolute path which starts with [`root_uri`](FileIO::root_uri).
/// * Relative path which is relative to [`root_uri`](FileIO::root_uri).
///
/// For example, if `FileIO` is built from `s3a://test_bucket/warehouse`, then [`root_uri`](FileIO::root_uri) is `s3a://test_bucket/`. And then the following paths are valid:
///
/// * `s3a://test_bucket/warehouse/iceberg/`
/// * `warehouse/iceberg/`, it will be treated same as above.
#[derive(Clone, Debug)]
pub struct FileIO {
    root_uri: Arc<str>,
    op: Operator,
}

/// Builder for [`FileIO`].
///
/// # Note
///
/// We should refer to [`opendal`](https://docs.rs/opendal/0.39.0/opendal/) for what args should be passed to operator.
/// The special `root` argument will always be set to `/` for file io.
pub struct FileIOBuilder {
    /// This is used to infer scheme of operator.
    ///
    /// If this is `None`, then [`FileIOBuilder::build`](FileIOBuilder::build) will build a local file io.
    scheme_str: Option<String>,
    /// Arguments for operator.
    args: HashMap<String, String>,
}

impl FileIOBuilder {
    /// Creates a new builder with scheme.
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            args: HashMap::default(),
        }
    }

    /// Creates a new builder for local file io.
    pub fn new_local_file_io() -> Self {
        Self {
            scheme_str: None,
            args: HashMap::default(),
        }
    }

    /// Add argument for operator.
    pub fn with_arg(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.args.insert(key.to_string(), value.to_string());
        self
    }

    /// Add argument for operator.
    pub fn with_args(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.args
            .extend(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    /// Builds [`FileIO`].
    pub fn build(mut self) -> Result<FileIO> {
        let scheme = FileIO::parse_scheme(self.scheme_str.as_deref())?;
        // Set root to "/" for file io.
        self.args
            .insert(S3_ARGS_ROOT.to_string(), ROOT_PATH.to_string());
        let op = Operator::via_map(scheme, self.args)?;
        let op_info = op.info();

        let root_uri = match self.scheme_str.as_deref() {
            Some(s) if op_info.name().is_empty() => format!("{s}:{ROOT_PATH}"),
            Some(s) => format!("{s}://{}{ROOT_PATH}", op_info.name()),
            None => ROOT_PATH.to_string(),
        };

        Ok(FileIO {
            root_uri: Arc::from(root_uri),
            op,
        })
    }
}

impl FileIO {
    /// Creates builder from url.
    pub fn build_from_url(path: impl AsRef<str>) -> Result<FileIOBuilder> {
        let url = Url::parse(path.as_ref())?;

        let scheme = Self::parse_scheme(Some(url.scheme()))?;

        let bucket = url.host_str().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid s3 url: {}, missing bucket", path.as_ref()),
            )
        });

        match scheme {
            Scheme::Fs => Ok(FileIOBuilder::new(url.scheme())),
            Scheme::S3 => Ok(FileIOBuilder::new(url.scheme()).with_arg(S3_ARGS_BUCKET, bucket?)),
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Constructing file io from scheme: {scheme} not supported now",),
            )),
        }
    }

    /// Parse scheme.
    fn parse_scheme(scheme: Option<&str>) -> Result<Scheme> {
        match scheme {
            Some("file") | None => Ok(Scheme::Fs),
            Some("s3") | Some("s3a") => Ok(Scheme::S3),
            Some(s) => Ok(s.parse::<Scheme>()?),
        }
    }

    /// Deletes file.
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        Ok(self.op.delete(self.relative_path(path.as_ref())).await?)
    }

    /// Check file exists.
    pub async fn is_exist(&self, path: impl AsRef<str>) -> Result<bool> {
        Ok(self.op.is_exist(path.as_ref()).await?)
    }

    /// Creates input file.
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        Ok(InputFile {
            op: self.op.clone(),
            path: self.relative_path(path.as_ref()).to_string(),
        })
    }

    /// Creates output file.
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        Ok(OutputFile {
            op: self.op.clone(),
            path: self.relative_path(path.as_ref()).to_string(),
        })
    }

    fn relative_path<'a>(&self, path: &'a str) -> &'a str {
        if path.starts_with(self.root_uri.as_ref()) {
            &path[self.root_uri.len()..]
        } else {
            path
        }
    }
}

/// Input file implementation.
pub struct InputFile {
    op: Operator,
    path: String,
}

/// Input stream for reading.
pub trait InputStream: AsyncRead + AsyncSeek {}

impl<T> InputStream for T where T: AsyncRead + AsyncSeek {}

impl InputFile {
    /// Relative path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Check if file exists.
    pub async fn exists(&self) -> Result<bool> {
        Ok(self.op.is_exist(&self.path).await?)
    }

    /// Creates [`InputStream`] for reading.
    pub async fn reader(&self) -> Result<impl InputStream> {
        Ok(self.op.reader(&self.path).await?)
    }
}

/// Output file implementation.
pub struct OutputFile {
    op: Operator,
    path: String,
}

impl OutputFile {
    /// Relative path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Checks if file exists.
    pub async fn exists(&self) -> Result<bool> {
        Ok(self.op.is_exist(&self.path).await?)
    }

    /// Converts into [`InputFile`].
    pub fn to_input_file(self) -> InputFile {
        InputFile {
            op: self.op,
            path: self.path,
        }
    }

    /// Creates output file for writing.
    pub async fn writer(&self) -> Result<impl AsyncWrite> {
        Ok(self.op.writer(&self.path).await?)
    }
}

#[cfg(test)]
mod tests {

    use std::io::Write;

    use std::{fs::File, path::Path};

    use futures::io::AllowStdIo;
    use futures::{AsyncReadExt, AsyncWriteExt};

    use tempdir::TempDir;

    use crate::io::S3_ARGS_REGION;

    use super::{FileIO, FileIOBuilder};

    fn create_local_file_io() -> FileIO {
        FileIOBuilder::new_local_file_io().build().unwrap()
    }

    fn write_to_file<P: AsRef<Path>>(s: &str, path: P) {
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
        let tmp_dir = TempDir::new("test").unwrap();

        let file_name = "a.txt";
        let content = "Iceberg loves rust.";

        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);
        write_to_file(content, &full_path);

        let file_io = create_local_file_io();
        let input_file = file_io.new_input(&full_path).unwrap();

        assert!(input_file.exists().await.unwrap());
        // Remove heading slash
        assert_eq!(&full_path[1..], input_file.location());
        let read_content = read_from_file(full_path).await;

        assert_eq!(content, &read_content);
    }

    #[tokio::test]
    async fn test_delete_local_file() {
        let tmp_dir = TempDir::new("test").unwrap();

        let file_name = "a.txt";
        let content = "Iceberg loves rust.";

        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);
        write_to_file(content, &full_path);

        let file_io = create_local_file_io();
        assert!(file_io.is_exist(&full_path).await.unwrap());
        file_io.delete(&full_path).await.unwrap();
        assert!(!file_io.is_exist(&full_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_non_exist_file() {
        let tmp_dir = TempDir::new("test").unwrap();

        let file_name = "a.txt";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let file_io = create_local_file_io();
        assert!(!file_io.is_exist(&full_path).await.unwrap());
        assert!(file_io.delete(&full_path).await.is_ok());
    }

    #[tokio::test]
    async fn test_local_output_file() {
        let tmp_dir = TempDir::new("test").unwrap();

        let file_name = "a.txt";
        let content = "Iceberg loves rust.";

        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let file_io = create_local_file_io();
        let output_file = file_io.new_output(&full_path).unwrap();

        assert!(!output_file.exists().await.unwrap());
        {
            let mut writer = output_file.writer().await.unwrap();
            writer.write_all(content.as_bytes()).await.unwrap();
            writer.close().await.unwrap();
        }

        assert_eq!(&full_path[1..], output_file.location());

        let read_content = read_from_file(full_path).await;

        assert_eq!(content, &read_content);
    }

    #[tokio::test]
    async fn test_create_s3_file_io() {
        let file_io = FileIO::build_from_url("s3a://test_bucket/warehouse")
            .unwrap()
            .with_arg("root", "xx")
            .with_arg(S3_ARGS_REGION, "us-east-1")
            .build()
            .unwrap();

        assert_eq!("s3a://test_bucket/", file_io.root_uri.as_ref());
    }

    #[tokio::test]
    async fn test_create_local_file_io() {
        let file_io = FileIO::build_from_url("file:/warehouse")
            .unwrap()
            .build()
            .unwrap();

        assert_eq!("file:/", file_io.root_uri.as_ref());

        let file_io = FileIO::build_from_url("/warehouse");
        assert!(file_io.is_err());

        let file_io = FileIOBuilder::new_local_file_io().build().unwrap();
        assert_eq!("/", file_io.root_uri.as_ref());
    }
}
