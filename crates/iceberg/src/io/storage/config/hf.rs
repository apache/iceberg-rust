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

//! HuggingFace Hub storage configuration.

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::StorageConfig;
use crate::Result;

/// HuggingFace Hub authentication token.
pub const HF_TOKEN: &str = "hf.token";
/// HuggingFace Hub endpoint URL. Defaults to `https://huggingface.co`.
pub const HF_ENDPOINT: &str = "hf.endpoint";
/// Default git revision/branch for all paths that don't specify one. Defaults to `main`.
pub const HF_REVISION: &str = "hf.revision";

/// HuggingFace Hub storage configuration.
///
/// Repo type, repo ID, and revision are normally encoded in the file path URI
/// (`hf://<repo_type>/<owner>/<repo>[@<revision>]/<path>`, where `<repo_type>`
/// is one of `models`, `datasets`, `spaces`, or `buckets`).
/// The fields here provide credentials and a default revision fallback.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TypedBuilder)]
pub struct HfConfig {
    /// HuggingFace Hub API token (required for private repos and write access).
    #[builder(default, setter(strip_option, into))]
    pub token: Option<String>,
    /// HuggingFace Hub endpoint. Defaults to `https://huggingface.co`.
    #[builder(default, setter(strip_option, into))]
    pub endpoint: Option<String>,
    /// Default revision to use when a path URI does not specify one.
    #[builder(default, setter(strip_option, into))]
    pub revision: Option<String>,
}

impl TryFrom<&StorageConfig> for HfConfig {
    type Error = crate::Error;

    fn try_from(config: &StorageConfig) -> Result<Self> {
        let props = config.props();
        let mut cfg = HfConfig::default();
        if let Some(token) = props.get(HF_TOKEN) {
            cfg.token = Some(token.clone());
        }
        if let Some(endpoint) = props.get(HF_ENDPOINT) {
            cfg.endpoint = Some(endpoint.clone());
        }
        if let Some(revision) = props.get(HF_REVISION) {
            cfg.revision = Some(revision.clone());
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hf_config_builder() {
        let cfg = HfConfig::builder()
            .token("hf_mytoken")
            .endpoint("https://huggingface.co")
            .revision("dev")
            .build();
        assert_eq!(cfg.token.as_deref(), Some("hf_mytoken"));
        assert_eq!(cfg.endpoint.as_deref(), Some("https://huggingface.co"));
        assert_eq!(cfg.revision.as_deref(), Some("dev"));
    }

    #[test]
    fn test_hf_config_from_storage_config() {
        let storage_config = StorageConfig::new()
            .with_prop(HF_TOKEN, "hf_abc123")
            .with_prop(HF_ENDPOINT, "https://huggingface.co");

        let cfg = HfConfig::try_from(&storage_config).unwrap();
        assert_eq!(cfg.token.as_deref(), Some("hf_abc123"));
        assert_eq!(cfg.endpoint.as_deref(), Some("https://huggingface.co"));
    }

    #[test]
    fn test_hf_config_empty() {
        let cfg = HfConfig::try_from(&StorageConfig::new()).unwrap();
        assert_eq!(cfg.token, None);
        assert_eq!(cfg.endpoint, None);
    }
}
