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

//! Azure Blob Storage properties.

use std::collections::HashMap;

use iceberg::io::{AZBLOB_ACCOUNT_KEY, AZBLOB_ACCOUNT_NAME, AZBLOB_ENDPOINT, AZBLOB_SAS_TOKEN};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::services::AzblobConfig;
use url::Url;

use crate::utils::from_opendal_error;

/// Parse azblob.* prefixed configuration properties.
pub(crate) fn azblob_config_parse(mut properties: HashMap<String, String>) -> Result<AzblobConfig> {
    let mut config = AzblobConfig::default();

    if let Some(endpoint) = properties.remove(AZBLOB_ENDPOINT) {
        config.endpoint = Some(endpoint);
    }
    if let Some(account_name) = properties.remove(AZBLOB_ACCOUNT_NAME) {
        config.account_name = Some(account_name);
    }
    if let Some(account_key) = properties.remove(AZBLOB_ACCOUNT_KEY) {
        config.account_key = Some(account_key);
    }
    if let Some(sas_token) = properties.remove(AZBLOB_SAS_TOKEN) {
        config.sas_token = Some(sas_token);
    }

    Ok(config)
}

/// Build an OpenDAL operator for an `azblob://<container>/<path>` URL.
pub(crate) fn azblob_create_operator<'a>(
    path: &'a str,
    config: &AzblobConfig,
) -> Result<(Operator, &'a str)> {
    let (container, relative_path) = parse_azblob_path(path)?;
    let mut config = config.clone();
    config.container = container;

    let operator = Operator::from_config(config)
        .map_err(from_opendal_error)?
        .finish();
    Ok((operator, relative_path))
}

/// Extract the path relative to the Azure Blob container.
pub(crate) fn azblob_relative_path(path: &str) -> Result<&str> {
    Ok(parse_azblob_path(path)?.1)
}

fn parse_azblob_path(path: &str) -> Result<(String, &str)> {
    let url = Url::parse(path)?;
    let container = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid azblob url: {path}, container is required"),
        )
    })?;
    let prefix = format!("azblob://{container}/");
    let relative_path = path.strip_prefix(&prefix).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid azblob url: {path}, should start with {prefix}"),
        )
    })?;

    Ok((container.to_string(), relative_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_azblob_config_parse() {
        let config = azblob_config_parse(HashMap::from([
            (
                AZBLOB_ENDPOINT.to_string(),
                "https://account.blob.core.windows.net".to_string(),
            ),
            (AZBLOB_ACCOUNT_NAME.to_string(), "account".to_string()),
            (AZBLOB_ACCOUNT_KEY.to_string(), "key".to_string()),
            (AZBLOB_SAS_TOKEN.to_string(), "token".to_string()),
        ]))
        .unwrap();

        assert_eq!(
            config.endpoint.as_deref(),
            Some("https://account.blob.core.windows.net")
        );
        assert_eq!(config.account_name.as_deref(), Some("account"));
        assert_eq!(config.account_key.as_deref(), Some("key"));
        assert_eq!(config.sas_token.as_deref(), Some("token"));
    }

    #[test]
    fn test_azblob_create_operator() {
        let config = AzblobConfig {
            endpoint: Some("https://account.blob.core.windows.net".to_string()),
            account_name: Some("account".to_string()),
            ..Default::default()
        };

        let (operator, relative_path) =
            azblob_create_operator("azblob://container/path/to/file.parquet", &config).unwrap();

        assert_eq!(operator.info().name(), "container");
        assert_eq!(relative_path, "path/to/file.parquet");
    }

    #[test]
    fn test_azblob_relative_path_root() {
        assert_eq!(azblob_relative_path("azblob://container/").unwrap(), "");
    }

    #[test]
    fn test_azblob_relative_path_rejects_invalid_url() {
        assert!(azblob_relative_path("s3://container/path").is_err());
        assert!(azblob_relative_path("azblob:///path").is_err());
    }
}
