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

//! Iceberg Glue Catalog implementation.

use std::collections::HashMap;

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_glue::{config::Credentials, types::DatabaseInput};
use iceberg::{Error, ErrorKind, NamespaceIdent, Result};

use crate::error::from_build_error;

const _GLUE_SKIP_ARCHIVE: &str = "glue.skip-archive";
const _GLUE_SKIP_ARCHIVE_DEFAULT: bool = true;
/// Property aws profile name
pub const AWS_PROFILE_NAME: &str = "profile_name";
/// Property aws region
pub const AWS_REGION_NAME: &str = "region_name";
/// Property aws access key
pub const AWS_ACCESS_KEY_ID: &str = "aws_access_key_id";
/// Property aws secret access key
pub const AWS_SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
/// Property aws session token
pub const AWS_SESSION_TOKEN: &str = "aws_session_token";
/// Parameter namespace description
const DESCRIPTION: &str = "description";

/// Creates an AWS SDK configuration (SdkConfig) based on
/// provided properties and an optional endpoint URL.
pub(crate) async fn create_sdk_config(
    properties: &HashMap<String, String>,
    endpoint_url: Option<&String>,
) -> SdkConfig {
    let mut config = aws_config::defaults(BehaviorVersion::latest());

    if let Some(endpoint) = endpoint_url {
        config = config.endpoint_url(endpoint)
    };

    if properties.is_empty() {
        return config.load().await;
    }

    if let (Some(access_key), Some(secret_key)) = (
        properties.get(AWS_ACCESS_KEY_ID),
        properties.get(AWS_SECRET_ACCESS_KEY),
    ) {
        let session_token = properties.get(AWS_SESSION_TOKEN).cloned();
        let credentials_provider =
            Credentials::new(access_key, secret_key, session_token, None, "properties");

        config = config.credentials_provider(credentials_provider)
    };

    if let Some(profile_name) = properties.get(AWS_PROFILE_NAME) {
        config = config.profile_name(profile_name);
    }

    if let Some(region_name) = properties.get(AWS_REGION_NAME) {
        let region = Region::new(region_name.clone());
        config = config.region(region);
    }

    config.load().await
}

/// Create `DatabaseInput` from name, uri and properties
pub(crate) fn convert_to_database(
    namespace: &NamespaceIdent,
    location_uri: &Option<String>,
    properties: &HashMap<String, String>,
) -> Result<DatabaseInput> {
    let db_name = validate_namespace(namespace)?;
    let mut builder = DatabaseInput::builder().name(db_name);

    if let Some(location_uri) = location_uri {
        builder = builder.location_uri(location_uri);
    }

    for (k, v) in properties.iter() {
        match k.as_ref() {
            DESCRIPTION => {
                builder = builder.description(v);
            }
            _ => {
                builder = builder.parameters(k, v);
            }
        }
    }

    builder.build().map_err(from_build_error)
}

/// Checks if provided `NamespaceIdent` is valid.
pub(crate) fn validate_namespace(namespace: &NamespaceIdent) -> Result<String> {
    let name = namespace.as_ref();

    if name.len() != 1 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid database name: {:?}, hierarchical namespaces are not supported",
                namespace
            ),
        ));
    }

    let name = name[0].clone();

    if name.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Invalid database, provided namespace is empty.",
        ));
    }

    Ok(name)
}

#[cfg(test)]
mod tests {
    use aws_sdk_glue::config::ProvideCredentials;
    use iceberg::{Namespace, Result};

    use super::*;

    #[test]
    fn test_convert_to_database() -> Result<()> {
        let namespace = NamespaceIdent::new("my_database".to_string());
        let location_uri = Some("my_location".to_string());
        let properties = HashMap::new();

        let result = convert_to_database(&namespace, &location_uri, &properties)?;

        assert_eq!("my_database", result.name());
        assert_eq!(location_uri, result.location_uri);

        Ok(())
    }

    #[test]
    fn test_validate_namespace() {
        let valid_ns = Namespace::new(NamespaceIdent::new("ns".to_string()));
        let empty_ns = Namespace::new(NamespaceIdent::new("".to_string()));
        let hierarchical_ns = Namespace::new(
            NamespaceIdent::from_vec(vec!["level1".to_string(), "level2".to_string()]).unwrap(),
        );

        let valid = validate_namespace(valid_ns.name());
        let empty = validate_namespace(empty_ns.name());
        let hierarchical = validate_namespace(hierarchical_ns.name());

        assert!(valid.is_ok());
        assert!(empty.is_err());
        assert!(hierarchical.is_err());
    }

    #[tokio::test]
    async fn test_config_with_custom_endpoint() {
        let properties = HashMap::new();
        let endpoint_url = "http://custom_url:5000";

        let sdk_config = create_sdk_config(&properties, Some(&endpoint_url.to_string())).await;

        let result = sdk_config.endpoint_url().unwrap();

        assert_eq!(result, endpoint_url);
    }

    #[tokio::test]
    async fn test_config_with_properties() {
        let properties = HashMap::from([
            (AWS_PROFILE_NAME.to_string(), "my_profile".to_string()),
            (AWS_REGION_NAME.to_string(), "us-east-1".to_string()),
            (AWS_ACCESS_KEY_ID.to_string(), "my-access-id".to_string()),
            (
                AWS_SECRET_ACCESS_KEY.to_string(),
                "my-secret-key".to_string(),
            ),
            (AWS_SESSION_TOKEN.to_string(), "my-token".to_string()),
        ]);

        let sdk_config = create_sdk_config(&properties, None).await;

        let region = sdk_config.region().unwrap().as_ref();
        let credentials = sdk_config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        assert_eq!("us-east-1", region);
        assert_eq!("my-access-id", credentials.access_key_id());
        assert_eq!("my-secret-key", credentials.secret_access_key());
        assert_eq!("my-token", credentials.session_token().unwrap());
    }
}
