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

use crate::Result;

/// Storage credentials for accessing cloud storage.
///
/// Contains configuration properties like access keys, tokens, etc.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StorageCredential {
    /// Prefix for which these credentials are valid
    pub prefix: String,
    /// Configuration properties for the storage credentials
    pub config: HashMap<String, String>,
}

/// Newtype wrapper for the metadata location string, used as an extension
/// so that `RefreshableOpenDalStorage` can pass it to `load_credentials`.
#[derive(Debug, Clone)]
pub struct MetadataLocation(pub String);

/// Trait for loading storage credentials dynamically.
///
/// Implementations can fetch credentials from external sources,
/// refresh expired credentials, or implement custom credential logic.
///
/// # Example
///
/// ```rust,no_run
/// use std::collections::HashMap;
/// use std::sync::Arc;
///
/// use iceberg::io::{FileIOBuilder, StorageCredential, StorageCredentialsLoader};
///
/// #[derive(Debug)]
/// struct MyCredentialLoader;
///
/// #[async_trait::async_trait]
/// impl StorageCredentialsLoader for MyCredentialLoader {
///     async fn load_credentials(&self, location: &str) -> iceberg::Result<StorageCredential> {
///         // Fetch fresh credentials from your credential service
///         let mut config = HashMap::new();
///         config.insert("access_key_id".to_string(), "fresh-key".to_string());
///         config.insert("secret_access_key".to_string(), "fresh-secret".to_string());
///
///         Ok(StorageCredential {
///             prefix: "s3://my-bucket/".to_string(),
///             config,
///         })
///     }
/// }
///
/// # async fn example() -> iceberg::Result<()> {
/// // Create FileIO with credential refresh enabled
/// let loader: Arc<dyn StorageCredentialsLoader> = Arc::new(MyCredentialLoader);
/// let file_io = FileIOBuilder::new("s3")
///     .with_prop("bucket", "my-bucket")
///     .with_extension(loader)
///     .build()?;
///
/// // Each operation will refresh credentials automatically
/// let input = file_io.new_input("s3://my-bucket/path/file.parquet")?;
/// # Ok(())
/// # }
/// ```
#[async_trait::async_trait]
pub trait StorageCredentialsLoader: Send + Sync + Debug {
    /// Load storage credentials using custom user-defined logic.
    ///
    /// # Arguments
    /// * `location` - The full path being accessed (e.g., "s3://bucket/path/file.parquet")
    async fn load_credentials(&self, location: &str) -> Result<StorageCredential>;
}
