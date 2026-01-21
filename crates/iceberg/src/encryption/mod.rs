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

//! Encryption module for Apache Iceberg.
//!
//! This module provides core cryptographic primitives for encrypting
//! and decrypting data in Iceberg tables.

mod cache;
mod crypto;
mod key_management;
mod key_metadata;
mod manager;
mod parquet_key_retriever;
mod stream;

pub use cache::KeyCache;
pub use crypto::{AesGcmEncryptor, EncryptionAlgorithm, SecureKey};
pub use key_management::{InMemoryKms, KeyManagementClient};
pub use key_metadata::{EncryptionKeyMetadata, StandardKeyMetadata};
pub use manager::EncryptionManager;
pub use parquet_key_retriever::IcebergKeyRetriever;
pub use stream::AesGcmFileRead;
