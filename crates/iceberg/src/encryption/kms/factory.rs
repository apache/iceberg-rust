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

//! Factory trait for creating [`KeyManagementClient`] instances.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;

use super::KeyManagementClient;
use crate::Result;

/// Factory for creating a [`KeyManagementClient`] from catalog properties.
///
/// Replaces Java's reflection-based `encryption.kms-impl` + `initialize(properties)`
/// pattern. Users provide an implementation of this trait to the catalog builder via
/// [`CatalogBuilder::with_kms_client_factory`](crate::CatalogBuilder::with_kms_client_factory).
///
/// The catalog calls [`create_kms_client`](Self::create_kms_client) **once** during
/// catalog initialization with the catalog's properties. The resulting client is
/// shared across all tables in the catalog and passed to each table's
/// [`EncryptionManager`](crate::encryption::EncryptionManager) via
/// `TableBuilder::kms_client(...)`.
#[async_trait]
pub trait KmsClientFactory: Debug + Send + Sync {
    /// Create a [`KeyManagementClient`] from catalog properties.
    ///
    /// Called once during catalog initialization. Properties may include
    /// KMS endpoint, region, credentials, or any backend-specific
    /// configuration needed to construct the client.
    async fn create_kms_client(
        &self,
        properties: &HashMap<String, String>,
    ) -> Result<Arc<dyn KeyManagementClient>>;
}
