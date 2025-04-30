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

use aws_config::{BehaviorVersion, Region, SdkConfig};
use iceberg::NamespaceIdent;
use iceberg::{Error, ErrorKind, Namespace, Result};
use uuid::Uuid;

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

/// Creates an aws sdk configuration based on
/// provided properties and an optional endpoint URL.
pub(crate) async fn create_sdk_config(
    properties: &HashMap<String, String>,
    endpoint_url: Option<String>,
) -> SdkConfig {
    let mut config = aws_config::defaults(BehaviorVersion::latest());

    if properties.is_empty() {
        return config.load().await;
    }

    if let Some(endpoint_url) = endpoint_url {
        config = config.endpoint_url(endpoint_url);
    }

    if let Some(profile_name) = properties.get(AWS_PROFILE_NAME) {
        config = config.profile_name(profile_name);
    }

    if let Some(region_name) = properties.get(AWS_REGION_NAME) {
        let region = Region::new(region_name.clone());
        config = config.region(region);
    }

    config.load().await
}

/// Create metadata location from `location` and `version`
pub(crate) fn create_metadata_location(
    warehouse_location: impl AsRef<str>,
    version: i32,
) -> Result<String> {
    if version < 0 {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Table metadata version: '{}' must be a non-negative integer",
                version
            ),
        ));
    };

    let version = format!("{:0>5}", version);
    let id = Uuid::new_v4();
    let metadata_location = format!(
        "{}/metadata/{}-{}.metadata.json",
        warehouse_location.as_ref(),
        version,
        id
    );

    Ok(metadata_location)
}

pub(crate) fn valid_s3_namespaces(namespace: &NamespaceIdent) -> Result<bool> {
    for name in namespace.iter() {
        if name.len() < 3 || name.len() > 63 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Namespace name must be between 3 and 63 characters long, but got {}",
                    name.len()
                ),
            ));
        }
        if !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Namespace name can only contain lowercase letters, numbers, and underscores, but got {}",
                    &name
                ),
            ));
        }
        if name.starts_with('-') || name.ends_with('-') {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Namespace name cannot start or end with a hyphen, but got {}",
                    &name
                ),
            ));
        }
    }

    Ok(true)
}

/// Get default table location from `Namespace` properties
pub(crate) fn get_default_table_location(
    namespace: &NamespaceIdent,
    table_name: impl AsRef<str>,
    warehouse: impl AsRef<str>,
) -> String {
    return format!(
        "{}/{}/{}",
        warehouse.as_ref(),
        namespace.join("/"),
        table_name.as_ref()
    );
}
