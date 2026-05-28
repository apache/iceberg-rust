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

//! HuggingFace Hub storage backend.

use std::collections::HashMap;

use iceberg::io::{HF_ENDPOINT, HF_REVISION, HF_TOKEN};
use iceberg::{Error, ErrorKind, Result};
use opendal::{Configurator, Operator, OperatorUri};

use crate::utils::from_opendal_error;

// ---------------------------------------------------------------------------
// Minimal URI parser — extracts only what the caller needs.
// TODO: remove once opendal-service-hf exports its URI parser publicly.
// ---------------------------------------------------------------------------

/// Repository type of a HuggingFace Hub repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HfRepoType {
    /// Model repository (`models/` prefix).
    Model,
    /// Dataset repository (`datasets/` prefix).
    Dataset,
    /// Spaces application repository (`spaces/` prefix).
    Space,
    /// XET-backed object-storage bucket (`buckets/` prefix).
    Bucket,
}

impl HfRepoType {
    /// Parse a repo-type keyword (singular or plural) into the corresponding variant.
    fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().replace(' ', "").as_str() {
            "model" | "models" => Some(Self::Model),
            "dataset" | "datasets" => Some(Self::Dataset),
            "space" | "spaces" => Some(Self::Space),
            "bucket" | "buckets" => Some(Self::Bucket),
            _ => None,
        }
    }

    fn canonical(self) -> &'static str {
        match self {
            Self::Model => "models",
            Self::Dataset => "datasets",
            Self::Space => "spaces",
            Self::Bucket => "buckets",
        }
    }
}

/// Parsed HuggingFace URI: `hf://<repo_type>/<repo_id>[@<revision>][/<path>]`.
///
/// `repo_type` must be explicitly specified — there is no implicit default.
/// Only the fields required by this crate are stored; revision is consumed
/// during parsing but not retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HfUri {
    pub repo_type: HfRepoType,
    /// e.g. `"user/my-repo"`.
    pub repo_id: String,
    /// Path within the repository, e.g. `"train/data.parquet"`. Empty at repo root.
    pub path: String,
}

impl HfUri {
    /// Parse a full `hf://…` URI or the bare path portion (without scheme).
    /// Returns `None` if the URI does not begin with a recognized repo-type prefix
    /// (`models/`, `datasets/`, `spaces/`, or `buckets/`).
    pub(crate) fn parse(full_uri: &str) -> Option<Self> {
        let s = full_uri.strip_prefix("hf://").unwrap_or(full_uri);
        if s.is_empty() {
            return None;
        }

        // Require an explicit repo_type prefix — no implicit default.
        let (first, rest) = s.split_once('/')?;
        let repo_type = HfRepoType::parse(first)?;
        let s = rest;

        // Remaining: `<repo_id>[@<revision>][/<path_in_repo>]`
        let (repo_id, path) = if s.contains('/') {
            // Check if `@` appears in the first two slash-segments (the repo_id portion).
            // This distinguishes "user/repo@rev/file" from "user/repo/path/@file".
            let first_two = s.splitn(3, '/').take(2).collect::<Vec<_>>().join("/");
            if first_two.contains('@') {
                let (repo_id, rev_and_path) = s.split_once('@').unwrap();
                let rev_and_path = rev_and_path.replace("%2F", "/");
                (repo_id.to_string(), path_after_revision(&rev_and_path))
            } else {
                let segs: Vec<_> = s.splitn(3, '/').collect();
                let repo_id = format!("{}/{}", segs[0], segs[1]);
                let path = segs.get(2).copied().unwrap_or("").to_string();
                (repo_id, path)
            }
        } else if let Some((repo_id, _)) = s.split_once('@') {
            (repo_id.to_string(), String::new())
        } else {
            (s.to_string(), String::new())
        };

        Some(Self {
            repo_type,
            repo_id,
            path,
        })
    }
}

/// Given the string after `@`, extract the path-in-repo, correctly skipping
/// multi-segment special refs (`refs/convert/parquet`, `refs/pr/N`).
/// These are the only two multi-segment special ref prefixes in HF's git model.
fn path_after_revision(rev_and_path: &str) -> String {
    if !rev_and_path.contains('/') {
        return String::new();
    }
    if let Some(rest) = rev_and_path.strip_prefix("refs/convert/") {
        return rest
            .find('/')
            .map_or(String::new(), |i| rest[i + 1..].to_string());
    }
    if let Some(rest) = rev_and_path.strip_prefix("refs/pr/") {
        return rest
            .find('/')
            .map_or(String::new(), |i| rest[i + 1..].to_string());
    }
    rev_and_path
        .split_once('/')
        .map(|(_, path)| path.to_string())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Public helpers used by lib.rs
// ---------------------------------------------------------------------------

/// Parse iceberg `StorageConfig` properties into an opendal [`opendal::services::HfConfig`].
pub(crate) fn hf_config_parse(m: HashMap<String, String>) -> Result<opendal::services::HfConfig> {
    let mut cfg = opendal::services::HfConfig::default();
    if let Some(token) = m.get(HF_TOKEN) {
        cfg.token = Some(token.clone());
    }
    if let Some(endpoint) = m.get(HF_ENDPOINT) {
        cfg.endpoint = Some(endpoint.clone());
    }
    if let Some(revision) = m.get(HF_REVISION) {
        cfg.revision = Some(revision.clone());
    }
    Ok(cfg)
}

/// Build an [`Operator`] for the given `hf://…` path and return it together with
/// the relative path-in-repo.
///
/// URI parsing is delegated to opendal's [`HfConfig::from_uri`]. The base config
/// provides fallback values for `revision` and `endpoint`; the `token` is always
/// taken from the base config and never from the URI.
pub(crate) fn hf_config_build<'a>(
    cfg: &opendal::services::HfConfig,
    path: &'a str,
) -> Result<(Operator, &'a str)> {
    let uri = OperatorUri::new(path, Vec::<(String, String)>::new()).map_err(|e| {
        Error::new(ErrorKind::DataInvalid, format!("Invalid hf url: {path}")).with_source(e)
    })?;

    let mut hf_cfg = opendal::services::HfConfig::from_uri(&uri).map_err(|e| {
        Error::new(ErrorKind::DataInvalid, format!("Invalid hf url: {path}")).with_source(e)
    })?;

    // Token must come from config only, never from the URI.
    hf_cfg.token = cfg.token.clone();

    if hf_cfg.endpoint.is_none() {
        hf_cfg.endpoint = cfg.endpoint.clone();
    }
    if hf_cfg.revision.is_none() {
        hf_cfg.revision = cfg.revision.clone();
    }

    let parsed = HfUri::parse(path)
        .ok_or_else(|| Error::new(ErrorKind::DataInvalid, format!("Invalid hf url: {path}")))?;
    let relative_path = &path[path.len() - parsed.path.len()..];

    let op = Operator::from_config(hf_cfg)
        .map_err(from_opendal_error)?
        .finish();
    Ok((op, relative_path))
}

/// Returns a stable cache key for `delete_stream` batching: `"<repo_type>/<repo_id>"`
/// (e.g. `"buckets/user/my-repo"`), without revision.
/// Repo type is included so bucket and dataset paths to the same repo use separate operators.
/// Falls back to the full path so that unparsable paths never share an operator accidentally.
pub(crate) fn hf_batch_key(path: &str) -> String {
    HfUri::parse(path)
        .map(|u| format!("{}/{}", u.repo_type.canonical(), u.repo_id))
        .unwrap_or_else(|| path.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(uri: &str) -> HfUri {
        HfUri::parse(uri).unwrap_or_else(|| panic!("parse failed for {uri:?}"))
    }

    #[test]
    fn test_model_prefix() {
        let u = parse("hf://models/user/my-model/path/to/file.parquet");
        assert_eq!(u.repo_type, HfRepoType::Model);
        assert_eq!(u.repo_id, "user/my-model");
        assert_eq!(u.path, "path/to/file.parquet");
    }

    #[test]
    fn test_dataset_prefix() {
        let u = parse("hf://datasets/user/my-dataset/train/data.parquet");
        assert_eq!(u.repo_type, HfRepoType::Dataset);
        assert_eq!(u.repo_id, "user/my-dataset");
        assert_eq!(u.path, "train/data.parquet");
    }

    #[test]
    fn test_bucket_prefix() {
        let u = parse("hf://buckets/myorg/my-bucket/iceberg/metadata/v1.json");
        assert_eq!(u.repo_type, HfRepoType::Bucket);
        assert_eq!(u.repo_id, "myorg/my-bucket");
        assert_eq!(u.path, "iceberg/metadata/v1.json");
    }

    #[test]
    fn test_revision() {
        let u = parse("hf://datasets/user/my-dataset@main/train/data.parquet");
        assert_eq!(u.repo_type, HfRepoType::Dataset);
        assert_eq!(u.repo_id, "user/my-dataset");
        assert_eq!(u.path, "train/data.parquet");
    }

    #[test]
    fn test_refs_convert_revision() {
        let u = parse("hf://datasets/squad@refs/convert/parquet/data.parquet");
        assert_eq!(u.path, "data.parquet");
    }

    #[test]
    fn test_refs_pr_revision() {
        let u = parse("hf://models/user/repo@refs/pr/10/file.txt");
        assert_eq!(u.path, "file.txt");
    }

    #[test]
    fn test_encoded_revision() {
        let u = parse("hf://models/user/repo@refs%2Fpr%2F10/file.txt");
        assert_eq!(u.path, "file.txt");
    }

    #[test]
    fn test_no_path() {
        let u = parse("hf://models/user/my-model");
        assert_eq!(u.repo_id, "user/my-model");
        assert_eq!(u.path, "");
    }

    #[test]
    fn test_at_in_path_not_revision() {
        let u = parse("hf://models/user/repo/path/@not-a-revision.txt");
        assert_eq!(u.path, "path/@not-a-revision.txt");
    }

    #[test]
    fn test_single_segment_repo_id() {
        // Without revision and path: unambiguous.
        let u = parse("hf://models/gpt2");
        assert_eq!(u.repo_type, HfRepoType::Model);
        assert_eq!(u.repo_id, "gpt2");
        assert_eq!(u.path, "");

        // With explicit revision: single-segment repos with paths are parsed correctly.
        let u = parse("hf://models/gpt2@main/config.json");
        assert_eq!(u.repo_type, HfRepoType::Model);
        assert_eq!(u.repo_id, "gpt2");
        assert_eq!(u.path, "config.json");
    }

    #[test]
    fn test_batch_key() {
        assert_eq!(
            hf_batch_key("hf://datasets/user/repo@main/path/file.parquet"),
            "datasets/user/repo"
        );
        assert_eq!(
            hf_batch_key("hf://buckets/org/bucket/data/file.parquet"),
            "buckets/org/bucket"
        );
        // Same repo_id, different repo_type → different keys.
        assert_ne!(
            hf_batch_key("hf://buckets/user/repo/file"),
            hf_batch_key("hf://datasets/user/repo/file"),
        );
    }

    #[test]
    fn test_invalid_uri() {
        assert!(HfUri::parse("hf://").is_none());
        // bare repo-type, no repo_id
        assert!(HfUri::parse("hf://datasets").is_none());
        // missing repo-type prefix
        assert!(HfUri::parse("hf://user/my-model").is_none());
        assert!(HfUri::parse("hf://gpt2").is_none());
        // unrecognized repo-type prefix
        assert!(HfUri::parse("hf://repos/user/repo/file").is_none());
    }

    #[test]
    fn test_hf_config_build_relative_path() {
        let cfg = opendal::services::HfConfig::default();

        let (_, rel) = hf_config_build(
            &cfg,
            "hf://datasets/user/my-dataset@main/train/data.parquet",
        )
        .unwrap();
        assert_eq!(rel, "train/data.parquet");

        let (_, rel) = hf_config_build(&cfg, "hf://models/user/my-model/config.json").unwrap();
        assert_eq!(rel, "config.json");

        let (_, rel) = hf_config_build(&cfg, "hf://models/user/my-model").unwrap();
        assert_eq!(rel, "");
    }
}
