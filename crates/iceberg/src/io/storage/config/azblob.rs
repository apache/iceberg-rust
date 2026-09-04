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

//! Azure Blob Storage configuration.
//!
//! This module provides configuration constants and types for Azure Blob Storage.

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::StorageConfig;
use crate::Result;

/// Azure Blob Storage endpoint URL.
pub const AZBLOB_ENDPOINT: &str = "azblob.endpoint";
/// Azure Blob Storage account name.
pub const AZBLOB_ACCOUNT_NAME: &str = "azblob.account-name";
/// Azure Blob Storage account key.
pub const AZBLOB_ACCOUNT_KEY: &str = "azblob.account-key";
/// Azure Blob Storage shared access signature.
pub const AZBLOB_SAS_TOKEN: &str = "azblob.sas-token";

/// Azure Blob Storage configuration.
///
/// This struct contains all the configuration options for connecting to Azure Blob Storage.
/// Use the builder pattern via `AzblobConfig::builder()` to construct instances.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TypedBuilder)]
pub struct AzblobConfig {
    /// Endpoint URL.
    #[builder(default, setter(strip_option, into))]
    pub endpoint: Option<String>,
    /// Account name.
    #[builder(default, setter(strip_option, into))]
    pub account_name: Option<String>,
    /// Account key.
    #[builder(default, setter(strip_option, into))]
    pub account_key: Option<String>,
    /// SAS token.
    #[builder(default, setter(strip_option, into))]
    pub sas_token: Option<String>,
}

impl TryFrom<&StorageConfig> for AzblobConfig {
    type Error = crate::Error;

    fn try_from(config: &StorageConfig) -> Result<Self> {
        let props = config.props();

        let mut cfg = AzblobConfig::default();

        if let Some(endpoint) = props.get(AZBLOB_ENDPOINT) {
            cfg.endpoint = Some(endpoint.clone());
        }
        if let Some(account_name) = props.get(AZBLOB_ACCOUNT_NAME) {
            cfg.account_name = Some(account_name.clone());
        }
        if let Some(account_key) = props.get(AZBLOB_ACCOUNT_KEY) {
            cfg.account_key = Some(account_key.clone());
        }
        if let Some(sas_token) = props.get(AZBLOB_SAS_TOKEN) {
            cfg.sas_token = Some(sas_token.clone());
        }

        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_azblob_config_builder() {
        let config = AzblobConfig::builder()
            .endpoint("https://account.blob.core.windows.net")
            .account_name("myaccount")
            .account_key("my-account-key")
            .build();

        assert_eq!(
            config.endpoint.as_deref(),
            Some("https://account.blob.core.windows.net")
        );
        assert_eq!(config.account_name.as_deref(), Some("myaccount"));
        assert_eq!(config.account_key.as_deref(), Some("my-account-key"));
    }

    #[test]
    fn test_azblob_config_from_storage_config() {
        let storage_config = StorageConfig::new()
            .with_prop(AZBLOB_ENDPOINT, "https://account.blob.core.windows.net")
            .with_prop(AZBLOB_ACCOUNT_NAME, "myaccount")
            .with_prop(AZBLOB_ACCOUNT_KEY, "my-account-key")
            .with_prop(AZBLOB_SAS_TOKEN, "my-sas-token");

        let azblob_config = AzblobConfig::try_from(&storage_config).unwrap();

        assert_eq!(
            azblob_config.endpoint.as_deref(),
            Some("https://account.blob.core.windows.net")
        );
        assert_eq!(azblob_config.account_name.as_deref(), Some("myaccount"));
        assert_eq!(azblob_config.account_key.as_deref(), Some("my-account-key"));
        assert_eq!(azblob_config.sas_token.as_deref(), Some("my-sas-token"));
    }
}
