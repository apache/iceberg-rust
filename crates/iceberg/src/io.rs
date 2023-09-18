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
//! # How to build `FileIO`
//!
//! We provided a `FileIOBuilder` to build `FileIO` from scratch. For example:
//! ```rust
//! use iceberg::io::{FileIOBuilder, S3_REGION};
//!
//! let file_io = FileIOBuilder::new("s3")
//!     .with_prop(S3_REGION, "us-east-1")
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
use once_cell::sync::Lazy;
use opendal::{Operator, Scheme};
use url::Url;

/// Following are arguments for s3 file io.
/// S3 endopint.
pub const S3_ENDPOINT: &str = "s3.endpoint";
/// S3 access key id.
pub const S3_ACCESS_KEY_ID: &str = "s3.access-key-id";
/// S3 secret access key.
pub const S3_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
/// S3 region.
pub const S3_REGION: &str = "s3.region";

/// A mapping from iceberg s3 configuration key to [`opendal::Operator`] configuration key.
static S3_CONFIG_MAPPING: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut m = HashMap::with_capacity(4);
    m.insert(S3_ENDPOINT, "endpoint");
    m.insert(S3_ACCESS_KEY_ID, "access_key_id");
    m.insert(S3_SECRET_ACCESS_KEY, "secret_access_key");
    m.insert(S3_REGION, "region");

    m
});

const ROOT_PATH: &str = "/";
/// FileIO implementation, used to manipulate files in underlying storage.
///
/// # Note
///
/// All path passed to `FileIO` must be absolute path starting with scheme string used to construct `FileIO`.
/// For example, if you construct `FileIO` with `s3a` scheme, then all path passed to `FileIO` must start with `s3a://`.
#[derive(Clone, Debug)]
pub struct FileIO {
    inner: Arc<Storage>,
}

/// Builder for [`FileIO`].
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
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            props: HashMap::default(),
        }
    }

    /// Creates a new builder for local file io.
    pub fn new_local_file_io() -> Self {
        Self {
            scheme_str: None,
            props: HashMap::default(),
        }
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
    pub fn build(self) -> Result<FileIO> {
        let storage = Storage::build(self)?;
        Ok(FileIO {
            inner: Arc::new(storage),
        })
    }
}

impl FileIO {
    /// Deletes file.
    pub async fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        let (op, relative_path) = self.inner.create_operator(&path)?;
        Ok(op.delete(relative_path).await?)
    }

    /// Check file exists.
    pub async fn is_exist(&self, path: impl AsRef<str>) -> Result<bool> {
        let (op, relative_path) = self.inner.create_operator(&path)?;
        Ok(op.is_exist(relative_path).await?)
    }

    /// Creates input file.
    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        let (op, relative_path) = self.inner.create_operator(&path)?;
        let path = path.as_ref().to_string();
        let relative_path_pos = path.len() - relative_path.len();
        Ok(InputFile {
            op,
            path,
            relative_path_pos,
        })
    }

    /// Creates output file.
    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        let (op, relative_path) = self.inner.create_operator(&path)?;
        let path = path.as_ref().to_string();
        let relative_path_pos = path.len() - relative_path.len();
        Ok(OutputFile {
            op,
            path,
            relative_path_pos,
        })
    }
}

/// Input file implementation.
#[derive(Debug)]
pub struct InputFile {
    op: Operator,
    // Absolution path of file.
    path: String,
    // Relative path of file to uri, starts at [`relative_path_pos`]
    relative_path_pos: usize,
}

/// Input stream for reading.
pub trait InputStream: AsyncRead + AsyncSeek {}

impl<T> InputStream for T where T: AsyncRead + AsyncSeek {}

impl InputFile {
    /// Absolute path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Check if file exists.
    pub async fn exists(&self) -> Result<bool> {
        Ok(self
            .op
            .is_exist(&self.path[self.relative_path_pos..])
            .await?)
    }

    /// Creates [`InputStream`] for reading.
    pub async fn reader(&self) -> Result<impl InputStream> {
        Ok(self.op.reader(&self.path[self.relative_path_pos..]).await?)
    }
}

/// Output file implementation.
#[derive(Debug)]
pub struct OutputFile {
    op: Operator,
    // Absolution path of file.
    path: String,
    // Relative path of file to uri, starts at [`relative_path_pos`]
    relative_path_pos: usize,
}

impl OutputFile {
    /// Relative path to root uri.
    pub fn location(&self) -> &str {
        &self.path
    }

    /// Checks if file exists.
    pub async fn exists(&self) -> Result<bool> {
        Ok(self
            .op
            .is_exist(&self.path[self.relative_path_pos..])
            .await?)
    }

    /// Converts into [`InputFile`].
    pub fn to_input_file(self) -> InputFile {
        InputFile {
            op: self.op,
            path: self.path,
            relative_path_pos: self.relative_path_pos,
        }
    }

    /// Creates output file for writing.
    pub async fn writer(&self) -> Result<impl AsyncWrite> {
        Ok(self.op.writer(&self.path[self.relative_path_pos..]).await?)
    }
}

// We introduce this because I don't want to handle unsupported `Scheme` in every method.
#[derive(Debug)]
enum Storage {
    LocalFs {
        op: Operator,
    },
    S3 {
        scheme_str: String,
        props: HashMap<String, String>,
    },
}

impl Storage {
    /// Creates operator from path.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    ///
    /// # Returns
    ///
    /// The return value consists of two parts:
    ///
    /// * An [`opendal::Operator`] instance used to operate on file.
    /// * Relative path to the root uri of [`opendal::Operator`].
    ///
    fn create_operator<'a>(&self, path: &'a impl AsRef<str>) -> Result<(Operator, &'a str)> {
        let path = path.as_ref();
        match self {
            Storage::LocalFs { op } => {
                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok((op.clone(), stripped))
                } else {
                    Ok((op.clone(), &path[1..]))
                }
            }
            Storage::S3 { scheme_str, props } => {
                let mut props = props.clone();
                let url = Url::parse(path)?;
                let bucket = url.host_str().ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {}, missing bucket", path),
                    )
                })?;

                props.insert("bucket".to_string(), bucket.to_string());

                let prefix = format!("{}://{}/", scheme_str, bucket);
                if path.starts_with(&prefix) {
                    Ok((Operator::via_map(Scheme::S3, props)?, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {}, should start with {}", path, prefix),
                    ))
                }
            }
        }
    }

    /// Parse scheme.
    fn parse_scheme(scheme: &str) -> Result<Scheme> {
        match scheme {
            "file" | "" => Ok(Scheme::Fs),
            "s3" | "s3a" => Ok(Scheme::S3),
            s => Ok(s.parse::<Scheme>()?),
        }
    }

    /// Convert iceberg config to opendal config.
    fn build(file_io_builder: FileIOBuilder) -> Result<Self> {
        let scheme_str = file_io_builder.scheme_str.unwrap_or("".to_string());
        let scheme = Self::parse_scheme(&scheme_str)?;
        let mut new_props = HashMap::default();
        new_props.insert("root".to_string(), ROOT_PATH.to_string());

        match scheme {
            Scheme::Fs => Ok(Self::LocalFs {
                op: Operator::via_map(Scheme::Fs, new_props)?,
            }),
            Scheme::S3 => {
                for prop in file_io_builder.props {
                    if let Some(op_key) = S3_CONFIG_MAPPING.get(prop.0.as_str()) {
                        new_props.insert(op_key.to_string(), prop.1);
                    }
                }

                Ok(Self::S3 {
                    scheme_str,
                    props: new_props,
                })
            }
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Constructing file io from scheme: {scheme} not supported now",),
            )),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::io::Write;

    use std::{fs::File, path::Path};

    use futures::io::AllowStdIo;
    use futures::{AsyncReadExt, AsyncWriteExt};

    use tempdir::TempDir;

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
        assert_eq!(&full_path, input_file.location());
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

        assert_eq!(&full_path, output_file.location());

        let read_content = read_from_file(full_path).await;

        assert_eq!(content, &read_content);
    }
}
