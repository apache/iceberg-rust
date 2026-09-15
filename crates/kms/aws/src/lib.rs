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

//! AWS Key Management Service integration for Apache Iceberg client-side encryption.
//!
//! This crate provides [`AwsKmsClientFactory`], which creates AWS-backed
//! implementations of Iceberg's
//! [`KeyManagementClient`](iceberg::encryption::KeyManagementClient). The factory can
//! be supplied to any Iceberg catalog builder through
//! [`CatalogBuilder::with_kms_client_factory`](iceberg::CatalogBuilder::with_kms_client_factory).
//!
//! The KMS key referenced by a table's `encryption.key-id` property requires
//! `kms:Decrypt` for reads. Writes with `SYMMETRIC_DEFAULT` use
//! `kms:GenerateDataKey`. Writes with an asymmetric encryption algorithm
//! generate the key-encryption key locally and require `kms:Encrypt` to wrap it.
//!
//! # Configuration
//!
//! The factory uses the AWS SDK's default credential and region provider chains.
//! The `profile_name`, `region_name`, `aws_access_key_id`,
//! `aws_secret_access_key`, and `aws_session_token` catalog properties can
//! override those defaults, following the conventions used by Iceberg Rust's
//! other AWS integrations.
//!
//! Catalog properties are the properties supplied by the application when the
//! catalog is constructed. Configuration returned later by a REST catalog
//! server is not currently available when the KMS client is created.
//!
//! The following KMS properties match Iceberg Java:
//!
//! - `kms.endpoint` overrides the KMS service endpoint.
//! - `kms.encryption-algorithm-spec` selects the KMS encryption algorithm and
//!   defaults to `SYMMETRIC_DEFAULT`.
//! - `kms.data-key-spec` selects the size of KMS-generated key-encryption keys
//!   and defaults to `AES_256`.
//!
//! `kms.data-key-spec` is independent from the Iceberg table property
//! `encryption.data-key-length`, which controls locally generated file data keys.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::collections::HashMap;
//! use std::sync::Arc;
//!
//! use iceberg::CatalogBuilder;
//! use iceberg_catalog_rest::RestCatalogBuilder;
//! use iceberg_kms_aws::AwsKmsClientFactory;
//!
//! # async fn example() -> iceberg::Result<()> {
//! let catalog = RestCatalogBuilder::default()
//!     .with_kms_client_factory(Arc::new(AwsKmsClientFactory::new()))
//!     .load("rest", HashMap::new())
//!     .await?;
//! # let _ = catalog;
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]

mod client;
mod config;
mod factory;

pub use config::{
    AWS_ACCESS_KEY_ID, AWS_PROFILE_NAME, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN,
    KMS_DATA_KEY_SPEC, KMS_DATA_KEY_SPEC_DEFAULT, KMS_ENCRYPTION_ALGORITHM_SPEC,
    KMS_ENCRYPTION_ALGORITHM_SPEC_DEFAULT, KMS_ENDPOINT,
};
pub use factory::AwsKmsClientFactory;
