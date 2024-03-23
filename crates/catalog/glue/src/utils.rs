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
use aws_sdk_glue::config::Credentials;

const _GLUE_ID: &str = "glue.id";
const _GLUE_SKIP_ARCHIVE: &str = "glue.skip-archive";
const _GLUE_SKIP_ARCHIVE_DEFAULT: bool = true;
/// Property aws profile name
const PROFILE_NAME: &str = "profile_name";
/// Property aws region
const REGION_NAME: &str = "region_name";
/// Property aws access key
const ACCESS_KEY_ID: &str = "aws_access_key_id";
/// Property aws secret access key
const SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
/// Property aws session token
const SESSION_TOKEN: &str = "aws_session_token";

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
        properties.get(ACCESS_KEY_ID),
        properties.get(SECRET_ACCESS_KEY),
    ) {
        let session_token = properties.get(SESSION_TOKEN).cloned();
        let credentials_provider =
            Credentials::new(access_key, secret_key, session_token, None, "properties");

        config = config.credentials_provider(credentials_provider)
    };

    if let Some(profile_name) = properties.get(PROFILE_NAME) {
        config = config.profile_name(profile_name);
    }

    if let Some(region_name) = properties.get(REGION_NAME) {
        let region = Region::new(region_name.clone());
        config = config.region(region);
    }

    config.load().await
}

#[cfg(test)]
mod tests {
    use aws_sdk_glue::config::ProvideCredentials;

    use super::*;

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
            (PROFILE_NAME.to_string(), "my_profile".to_string()),
            (REGION_NAME.to_string(), "us-east-1".to_string()),
            (ACCESS_KEY_ID.to_string(), "my-access-id".to_string()),
            (SECRET_ACCESS_KEY.to_string(), "my-secret-key".to_string()),
            (SESSION_TOKEN.to_string(), "my-token".to_string()),
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
