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

use iceberg::io::{
    OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, S3_ACCESS_KEY_ID, S3_ENDPOINT,
    S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::services::OssConfig;
use opendal::{Configurator, Operator};
use url::Url;

use crate::utils::from_opendal_error;

/// Parse iceberg props to oss config.
///
/// Without any `oss.*` property, reads the `s3.*` ones, which REST catalogs
/// vend for OSS. The two are never mixed, so credentials only reach the
/// endpoint they were configured with.
pub(crate) fn oss_config_parse(mut m: HashMap<String, String>) -> Result<OssConfig> {
    let mut cfg: OssConfig = OssConfig::default();
    let has_oss_props = [OSS_ENDPOINT, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET]
        .iter()
        .any(|key| m.contains_key(*key));
    if has_oss_props {
        cfg.endpoint = m.remove(OSS_ENDPOINT);
        cfg.access_key_id = m.remove(OSS_ACCESS_KEY_ID);
        cfg.access_key_secret = m.remove(OSS_ACCESS_KEY_SECRET);
    } else {
        cfg.endpoint = m.remove(S3_ENDPOINT);
        cfg.access_key_id = m.remove(S3_ACCESS_KEY_ID);
        cfg.access_key_secret = m.remove(S3_SECRET_ACCESS_KEY);
        cfg.security_token = m.remove(S3_SESSION_TOKEN);
    }

    Ok(cfg)
}

/// Build new opendal operator from give path.
pub(crate) fn oss_config_build(cfg: &OssConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path)?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid oss url: {path}, missing bucket"),
        )
    })?;

    let builder = cfg.clone().into_builder().bucket(bucket);

    Operator::new(builder).map_err(from_opendal_error)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const VENDED_S3_PROPS: [(&str, &str); 4] = [
        (S3_ENDPOINT, "https://oss-cn-hangzhou.aliyuncs.com"),
        (S3_ACCESS_KEY_ID, "STS.key"),
        (S3_SECRET_ACCESS_KEY, "secret"),
        (S3_SESSION_TOKEN, "token"),
    ];

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_oss_config_parse_reads_s3_props() {
        let mut expected = OssConfig::default();
        expected.endpoint = Some("https://oss-cn-hangzhou.aliyuncs.com".to_string());
        expected.access_key_id = Some("STS.key".to_string());
        expected.access_key_secret = Some("secret".to_string());
        expected.security_token = Some("token".to_string());

        assert_eq!(oss_config_parse(props(&VENDED_S3_PROPS)).unwrap(), expected);
    }

    #[test]
    fn test_oss_config_parse_ignores_s3_props_with_any_oss_prop() {
        for key in [OSS_ENDPOINT, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET] {
            let oss_only = props(&[(key, "value")]);
            let mut with_s3 = props(&VENDED_S3_PROPS);
            with_s3.extend(oss_only.clone());

            assert_eq!(
                oss_config_parse(with_s3).unwrap(),
                oss_config_parse(oss_only).unwrap(),
                "{key}"
            );
        }
    }

    #[test]
    fn test_oss_config_parse_reads_oss_props() {
        let mut expected = OssConfig::default();
        expected.endpoint = Some("https://oss-cn-beijing.aliyuncs.com".to_string());
        expected.access_key_id = Some("key".to_string());
        expected.access_key_secret = Some("secret".to_string());

        let config = oss_config_parse(props(&[
            (OSS_ENDPOINT, "https://oss-cn-beijing.aliyuncs.com"),
            (OSS_ACCESS_KEY_ID, "key"),
            (OSS_ACCESS_KEY_SECRET, "secret"),
        ]))
        .unwrap();

        assert_eq!(config, expected);
    }

    #[test]
    fn test_oss_config_build_rejects_s3_credentials_without_an_endpoint() {
        let config = oss_config_parse(props(&VENDED_S3_PROPS[1..])).unwrap();

        let err = oss_config_build(&config, "oss://bucket/key").unwrap_err();

        assert!(err.to_string().contains("endpoint is empty"), "{err}");
    }

    #[tokio::test]
    async fn test_s3_session_credentials_sign_native_oss_requests() {
        let config = oss_config_parse(props(&VENDED_S3_PROPS)).unwrap();
        let operator = oss_config_build(&config, "oss://bucket/key").unwrap();

        let request = operator
            .presign_read("key", Duration::from_secs(60))
            .await
            .unwrap();

        let url = Url::parse(&request.uri().to_string()).unwrap();
        let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(
            (
                url.host_str(),
                query.get("OSSAccessKeyId").map(String::as_str),
                query.get("security-token").map(String::as_str),
            ),
            (
                Some("bucket.oss-cn-hangzhou.aliyuncs.com"),
                Some("STS.key"),
                Some("token"),
            )
        );
    }
}
