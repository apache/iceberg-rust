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

//! AWS SDK configuration utilities.

use std::collections::HashMap;

use aws_config::sts::AssumeRoleProvider;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_sts::config::Credentials;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ASSUME_ROLE_ARN, S3_ASSUME_ROLE_EXTERNAL_ID, S3_ASSUME_ROLE_SESSION_NAME,
    S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};

use crate::{
    AWS_ACCESS_KEY_ID, AWS_ASSUME_ROLE_ARN, AWS_ASSUME_ROLE_EXTERNAL_ID,
    AWS_ASSUME_ROLE_SESSION_NAME, AWS_PROFILE_NAME, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
};

/// Creates an aws sdk configuration based on
/// provided properties and an optional endpoint URL.
///
/// When `client.assume-role.arn` is set, the function first builds a base
/// configuration using any static credentials or profile provided, then uses
/// AWS STS to assume the specified role. The resulting temporary credentials
/// are used for all subsequent AWS API calls.
pub async fn create_sdk_config(
    properties: &HashMap<String, String>,
    endpoint_uri: Option<&String>,
) -> SdkConfig {
    let mut config = aws_config::defaults(BehaviorVersion::latest());

    if let Some(endpoint) = endpoint_uri {
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

    // If a role ARN is provided, assume that role via STS and use the
    // resulting temporary credentials for all AWS SDK calls.
    if let Some(role_arn) = properties.get(AWS_ASSUME_ROLE_ARN) {
        let base_config = config.load().await;

        let session_name = properties
            .get(AWS_ASSUME_ROLE_SESSION_NAME)
            .cloned()
            .unwrap_or_else(|| "iceberg-aws-catalog".to_string());

        let mut assume_role_builder =
            AssumeRoleProvider::builder(role_arn).session_name(session_name);

        if let Some(external_id) = properties.get(AWS_ASSUME_ROLE_EXTERNAL_ID) {
            assume_role_builder = assume_role_builder.external_id(external_id);
        }

        let assume_role_provider = assume_role_builder
            .build_from_provider(
                base_config
                    .credentials_provider()
                    .expect("base credentials provider must be set for STS assume role"),
            )
            .await;

        return aws_config::defaults(BehaviorVersion::latest())
            .credentials_provider(assume_role_provider)
            .region(base_config.region().cloned())
            .endpoint_url(endpoint_uri.map(|s| s.as_str()).unwrap_or_default())
            .load()
            .await;
    }

    config.load().await
}

/// Maps generic AWS properties to S3-specific properties.
///
/// This is used to propagate catalog-level AWS configurations (like static credentials
/// or assume-role settings) to the S3 FileIO.
pub fn map_aws_to_s3_properties(
    properties: &HashMap<String, String>,
    endpoint_uri: Option<&String>,
) -> HashMap<String, String> {
    let mut s3_props = properties.clone();

    if !s3_props.contains_key(S3_ACCESS_KEY_ID)
        && let Some(access_key_id) = s3_props.get(AWS_ACCESS_KEY_ID)
    {
        s3_props.insert(S3_ACCESS_KEY_ID.to_string(), access_key_id.to_string());
    }
    if !s3_props.contains_key(S3_SECRET_ACCESS_KEY)
        && let Some(secret_access_key) = s3_props.get(AWS_SECRET_ACCESS_KEY)
    {
        s3_props.insert(
            S3_SECRET_ACCESS_KEY.to_string(),
            secret_access_key.to_string(),
        );
    }
    if !s3_props.contains_key(S3_REGION)
        && let Some(region) = s3_props.get(AWS_REGION_NAME)
    {
        s3_props.insert(S3_REGION.to_string(), region.to_string());
    }
    if !s3_props.contains_key(S3_SESSION_TOKEN)
        && let Some(session_token) = s3_props.get(AWS_SESSION_TOKEN)
    {
        s3_props.insert(S3_SESSION_TOKEN.to_string(), session_token.to_string());
    }
    if !s3_props.contains_key(S3_ENDPOINT)
        && let Some(aws_endpoint) = endpoint_uri
    {
        s3_props.insert(S3_ENDPOINT.to_string(), aws_endpoint.to_string());
    }

    // STS assume-role properties
    if !s3_props.contains_key(S3_ASSUME_ROLE_ARN)
        && let Some(role_arn) = s3_props.get(AWS_ASSUME_ROLE_ARN)
    {
        s3_props.insert(S3_ASSUME_ROLE_ARN.to_string(), role_arn.to_string());
    }
    if !s3_props.contains_key(S3_ASSUME_ROLE_EXTERNAL_ID)
        && let Some(external_id) = s3_props.get(AWS_ASSUME_ROLE_EXTERNAL_ID)
    {
        s3_props.insert(
            S3_ASSUME_ROLE_EXTERNAL_ID.to_string(),
            external_id.to_string(),
        );
    }
    if !s3_props.contains_key(S3_ASSUME_ROLE_SESSION_NAME)
        && let Some(session_name) = s3_props.get(AWS_ASSUME_ROLE_SESSION_NAME)
    {
        s3_props.insert(
            S3_ASSUME_ROLE_SESSION_NAME.to_string(),
            session_name.to_string(),
        );
    }

    s3_props
}
