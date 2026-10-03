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

//! Key management service integrations for Apache Iceberg client-side encryption.
//!
//! Provider implementations are enabled through crate features. Enable `aws`
//! to use the `aws` module. [`ResolvingKmsClientFactory`] selects an enabled
//! provider from the `encryption.kms-type` catalog property.

#![deny(missing_docs)]

/// AWS Key Management Service integration.
#[cfg(feature = "aws")]
pub mod aws;

mod resolving;

pub use resolving::{
    ENCRYPTION_KMS_TYPE, ENCRYPTION_KMS_TYPE_AWS, ENCRYPTION_KMS_TYPE_AZURE,
    ENCRYPTION_KMS_TYPE_GCP, ResolvingKmsClientFactory,
};
