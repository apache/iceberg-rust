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
const PROFILE_NAME: &str = "profile_name";
const REGION_NAME: &str = "region_name";

const ACCESS_KEY_ID: &str = "aws_access_key_id";
const SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
const SESSION_TOKEN: &str = "aws_session_token";

pub(crate) async fn get_sdk_config(
    properties: &HashMap<String, String>,
    endpoint_url: &Option<String>,
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
