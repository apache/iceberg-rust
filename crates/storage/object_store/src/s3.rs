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

use std::sync::Arc;

use iceberg::io::S3Config;
use iceberg::{Error, ErrorKind, Result};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use url::Url;

/// Parse an absolute S3 URL into (scheme, bucket, relative_path).
///
/// Accepts `s3://` and `s3a://` schemes.
pub(crate) fn parse_s3_url(path: &str) -> Result<(&str, &str, &str)> {
    let url = Url::parse(path).map_err(|e| {
        Error::new(ErrorKind::DataInvalid, format!("Invalid URL: {path}")).with_source(e)
    })?;

    let scheme = &path[..url.scheme().len()];
    match scheme {
        "s3" | "s3a" => {}
        _ => {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unsupported S3 scheme: {scheme} in url: {path}"),
            ));
        }
    }

    let bucket_str = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid s3 url: {path}, missing bucket"),
        )
    })?;

    let prefix_len = scheme.len() + "://".len() + bucket_str.len() + "/".len();
    let relative = if path.len() > prefix_len {
        &path[prefix_len..]
    } else {
        ""
    };

    let bucket_start = scheme.len() + "://".len();
    let bucket = &path[bucket_start..bucket_start + bucket_str.len()];

    Ok((scheme, bucket, relative))
}

/// Build an `AmazonS3` store from iceberg's `S3Config` for a given bucket.
pub(crate) fn build_s3_store(config: &S3Config, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

    if let Some(ref endpoint) = config.endpoint {
        builder = builder.with_endpoint(endpoint);
        if endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
    }
    if let Some(ref access_key_id) = config.access_key_id {
        builder = builder.with_access_key_id(access_key_id);
    }
    if let Some(ref secret_access_key) = config.secret_access_key {
        builder = builder.with_secret_access_key(secret_access_key);
    }
    if let Some(ref session_token) = config.session_token {
        builder = builder.with_token(session_token);
    }
    if let Some(ref region) = config.region {
        builder = builder.with_region(region);
    }
    if config.enable_virtual_host_style {
        builder = builder.with_virtual_hosted_style_request(true);
    }
    if config.allow_anonymous {
        builder = builder.with_skip_signature(true);
    }

    let store = builder.build().map_err(|e| {
        Error::new(ErrorKind::Unexpected, "Failed to build S3 object store").with_source(e)
    })?;
    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url() {
        let (scheme, bucket, relative) =
            parse_s3_url("s3://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(scheme, "s3");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(relative, "path/to/file.parquet");
    }

    #[test]
    fn test_parse_s3a_url() {
        let (scheme, bucket, relative) =
            parse_s3_url("s3a://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(scheme, "s3a");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(relative, "path/to/file.parquet");
    }

    #[test]
    fn test_parse_s3_url_unsupported_scheme() {
        assert!(parse_s3_url("gs://my-bucket/file.parquet").is_err());
    }

    #[test]
    fn test_parse_s3_url_bucket_only() {
        let (scheme, bucket, relative) = parse_s3_url("s3://my-bucket/").unwrap();
        assert_eq!(scheme, "s3");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(relative, "");
    }
}
