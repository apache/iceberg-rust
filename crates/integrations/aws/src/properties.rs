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
use std::fmt::{Debug, Formatter};

use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ASSUME_ROLE_ARN, S3_ASSUME_ROLE_EXTERNAL_ID, S3_ASSUME_ROLE_SESSION_NAME,
    S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use iceberg_property_macro::properties_view;

/// AWS profile name property key.
pub const AWS_PROFILE_NAME: &str = AwsProperties::PROPERTY_PROFILE_NAME;
/// AWS region name property key.
pub const AWS_REGION_NAME: &str = AwsProperties::PROPERTY_REGION_NAME;
/// Static AWS access key ID property key.
pub const AWS_ACCESS_KEY_ID: &str = AwsProperties::PROPERTY_ACCESS_KEY_ID;
/// Static AWS secret access key property key.
pub const AWS_SECRET_ACCESS_KEY: &str = AwsProperties::PROPERTY_SECRET_ACCESS_KEY;
/// Temporary AWS session token property key.
pub const AWS_SESSION_TOKEN: &str = AwsProperties::PROPERTY_SESSION_TOKEN;
/// IAM role ARN to assume via AWS STS property key.
pub const AWS_ASSUME_ROLE_ARN: &str = AwsProperties::PROPERTY_ASSUME_ROLE_ARN;
/// External ID passed to AWS STS AssumeRole property key.
pub const AWS_ASSUME_ROLE_EXTERNAL_ID: &str = AwsProperties::PROPERTY_ASSUME_ROLE_EXTERNAL_ID;
/// Session name passed to AWS STS AssumeRole property key.
pub const AWS_ASSUME_ROLE_SESSION_NAME: &str = AwsProperties::PROPERTY_ASSUME_ROLE_SESSION_NAME;

properties_view! {
    /// Typed view over common AWS catalog and client properties.
    pub struct AwsProperties {
        /// AWS profile name used to load credentials and configuration.
        #[property(key = Self::PROPERTY_PROFILE_NAME, default = None, getter)]
        profile_name: Option<String>,
        /// AWS region name for SDK and storage requests.
        #[property(key = Self::PROPERTY_REGION_NAME, default = None, getter)]
        region_name: Option<String>,
        /// Static AWS access key ID.
        #[property(key = Self::PROPERTY_ACCESS_KEY_ID, default = None, getter)]
        access_key_id: Option<String>,
        /// Static AWS secret access key.
        #[property(key = Self::PROPERTY_SECRET_ACCESS_KEY, default = None, getter)]
        secret_access_key: Option<String>,
        /// Temporary AWS session token accompanying static credentials.
        #[property(key = Self::PROPERTY_SESSION_TOKEN, default = None, getter)]
        session_token: Option<String>,
        /// IAM role ARN to assume via AWS STS.
        #[property(key = Self::PROPERTY_ASSUME_ROLE_ARN, default = None, getter)]
        assume_role_arn: Option<String>,
        /// External ID passed to AWS STS AssumeRole for cross-account access.
        #[property(key = Self::PROPERTY_ASSUME_ROLE_EXTERNAL_ID, default = None, getter)]
        assume_role_external_id: Option<String>,
        /// Session name passed to AWS STS AssumeRole.
        #[property(key = Self::PROPERTY_ASSUME_ROLE_SESSION_NAME, default = None, getter)]
        assume_role_session_name: Option<String>,
    }
}

impl Debug for AwsProperties<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let redacted: HashMap<&str, &str> = self
            .properties
            .iter()
            .map(|(k, v)| {
                let value = if Self::is_sensitive_property(k) {
                    "[REDACTED]"
                } else {
                    v.as_str()
                };
                (k.as_str(), value)
            })
            .collect();
        f.debug_struct("AwsProperties")
            .field("properties", &redacted)
            .finish()
    }
}

impl AwsProperties<'_> {
    /// AWS profile name property key.
    pub const PROPERTY_PROFILE_NAME: &'static str = "profile_name";
    /// AWS region name property key.
    pub const PROPERTY_REGION_NAME: &'static str = "region_name";
    /// Static AWS access key ID property key.
    pub const PROPERTY_ACCESS_KEY_ID: &'static str = "aws_access_key_id";
    /// Static AWS secret access key property key.
    pub const PROPERTY_SECRET_ACCESS_KEY: &'static str = "aws_secret_access_key";
    /// Temporary AWS session token property key.
    pub const PROPERTY_SESSION_TOKEN: &'static str = "aws_session_token";
    /// IAM role ARN to assume via AWS STS property key.
    pub const PROPERTY_ASSUME_ROLE_ARN: &'static str = "client.assume-role.arn";
    /// External ID passed to AWS STS AssumeRole property key.
    pub const PROPERTY_ASSUME_ROLE_EXTERNAL_ID: &'static str = "client.assume-role.external-id";
    /// Session name passed to AWS STS AssumeRole property key.
    pub const PROPERTY_ASSUME_ROLE_SESSION_NAME: &'static str = "client.assume-role.session-name";

    /// Returns whether the caller supplied S3-specific static credentials.
    pub fn has_explicit_s3_credentials(&self) -> bool {
        self.properties.contains_key(S3_ACCESS_KEY_ID)
            || self.properties.contains_key(S3_SECRET_ACCESS_KEY)
            || self.properties.contains_key(S3_SESSION_TOKEN)
    }

    /// Maps AWS properties to S3 `FileIO` properties, preserving explicit `s3.*` overrides.
    pub fn s3_file_io_properties(
        &self,
        endpoint_uri: Option<&str>,
        default_session_name: &str,
        resolved_region: Option<&str>,
    ) -> HashMap<String, String> {
        let mut s3_props = self.properties.clone();

        if !s3_props.contains_key(S3_ACCESS_KEY_ID)
            && let Some(access_key_id) = s3_props.get(Self::PROPERTY_ACCESS_KEY_ID)
        {
            s3_props.insert(S3_ACCESS_KEY_ID.to_string(), access_key_id.to_string());
        }
        if !s3_props.contains_key(S3_SECRET_ACCESS_KEY)
            && let Some(secret_access_key) = s3_props.get(Self::PROPERTY_SECRET_ACCESS_KEY)
        {
            s3_props.insert(
                S3_SECRET_ACCESS_KEY.to_string(),
                secret_access_key.to_string(),
            );
        }
        if !s3_props.contains_key(S3_REGION) {
            if let Some(region) = s3_props.get(Self::PROPERTY_REGION_NAME) {
                s3_props.insert(S3_REGION.to_string(), region.to_string());
            } else if let Some(region) = resolved_region {
                s3_props.insert(S3_REGION.to_string(), region.to_string());
            }
        }
        if !s3_props.contains_key(S3_SESSION_TOKEN)
            && let Some(session_token) = s3_props.get(Self::PROPERTY_SESSION_TOKEN)
        {
            s3_props.insert(S3_SESSION_TOKEN.to_string(), session_token.to_string());
        }
        if !s3_props.contains_key(S3_ENDPOINT)
            && let Some(aws_endpoint) = endpoint_uri
        {
            s3_props.insert(S3_ENDPOINT.to_string(), aws_endpoint.to_string());
        }
        if s3_props.contains_key(Self::PROPERTY_ASSUME_ROLE_ARN)
            && !s3_props.contains_key(S3_ASSUME_ROLE_SESSION_NAME)
        {
            s3_props.insert(
                S3_ASSUME_ROLE_SESSION_NAME.to_string(),
                default_session_name.to_string(),
            );
        }

        s3_props
    }

    /// Returns whether `key` holds a secret value that should be redacted in debug output.
    pub fn is_sensitive_property(key: &str) -> bool {
        let lower = key.to_ascii_lowercase();
        lower.contains("secret")
            || lower.contains("token")
            || lower.contains("password")
            || lower.contains("credential")
            || lower.contains("key")
    }
}

#[allow(dead_code)]
pub(crate) fn remove_assume_role_properties(properties: &mut HashMap<String, String>) {
    properties.remove(S3_ASSUME_ROLE_ARN);
    properties.remove(S3_ASSUME_ROLE_EXTERNAL_ID);
    properties.remove(S3_ASSUME_ROLE_SESSION_NAME);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::io::{
        S3_ACCESS_KEY_ID, S3_ASSUME_ROLE_ARN, S3_ASSUME_ROLE_EXTERNAL_ID,
        S3_ASSUME_ROLE_SESSION_NAME, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
        S3_SESSION_TOKEN,
    };

    use super::*;

    const DEFAULT_SESSION_NAME: &str = "iceberg-glue-catalog";

    fn assume_role_properties() -> HashMap<String, String> {
        HashMap::from([
            (AWS_ACCESS_KEY_ID.to_string(), "base-access-key".to_string()),
            (
                AWS_SECRET_ACCESS_KEY.to_string(),
                "base-secret-key".to_string(),
            ),
            (AWS_REGION_NAME.to_string(), "ap-southeast-2".to_string()),
            (
                AWS_ASSUME_ROLE_ARN.to_string(),
                "arn:aws:iam::123456789012:role/TestRole".to_string(),
            ),
        ])
    }

    #[test]
    fn aws_properties_getters_and_defaults() {
        let empty = HashMap::new();
        let empty_view = AwsProperties::new(&empty);
        assert_eq!(empty_view.profile_name().unwrap(), None);
        assert_eq!(empty_view.region_name().unwrap(), None);
        assert_eq!(empty_view.access_key_id().unwrap(), None);
        assert_eq!(empty_view.secret_access_key().unwrap(), None);
        assert_eq!(empty_view.session_token().unwrap(), None);
        assert_eq!(empty_view.assume_role_arn().unwrap(), None);
        assert_eq!(empty_view.assume_role_external_id().unwrap(), None);
        assert_eq!(empty_view.assume_role_session_name().unwrap(), None);

        let populated = HashMap::from([
            (AWS_PROFILE_NAME.to_string(), "dev-profile".to_string()),
            (AWS_REGION_NAME.to_string(), "us-east-1".to_string()),
            (AWS_ACCESS_KEY_ID.to_string(), "akid".to_string()),
            (AWS_SECRET_ACCESS_KEY.to_string(), "secret".to_string()),
            (AWS_SESSION_TOKEN.to_string(), "token".to_string()),
            (
                AWS_ASSUME_ROLE_ARN.to_string(),
                "arn:aws:iam::123456789012:role/TestRole".to_string(),
            ),
            (
                AWS_ASSUME_ROLE_EXTERNAL_ID.to_string(),
                "ext-123".to_string(),
            ),
            (
                AWS_ASSUME_ROLE_SESSION_NAME.to_string(),
                "custom-session".to_string(),
            ),
        ]);
        let view = AwsProperties::new(&populated);
        assert_eq!(
            view.profile_name().unwrap(),
            Some("dev-profile".to_string())
        );
        assert_eq!(view.region_name().unwrap(), Some("us-east-1".to_string()));
        assert_eq!(view.access_key_id().unwrap(), Some("akid".to_string()));
        assert_eq!(
            view.secret_access_key().unwrap(),
            Some("secret".to_string())
        );
        assert_eq!(view.session_token().unwrap(), Some("token".to_string()));
        assert_eq!(
            view.assume_role_arn().unwrap(),
            Some("arn:aws:iam::123456789012:role/TestRole".to_string())
        );
        assert_eq!(
            view.assume_role_external_id().unwrap(),
            Some("ext-123".to_string())
        );
        assert_eq!(
            view.assume_role_session_name().unwrap(),
            Some("custom-session".to_string())
        );
    }

    #[test]
    fn maps_properties_with_explicit_s3_precedence_and_defaults() {
        let mut properties = assume_role_properties();
        properties.insert(S3_ACCESS_KEY_ID.to_string(), "s3-access-key".to_string());
        properties.insert(
            S3_SECRET_ACCESS_KEY.to_string(),
            "s3-secret-key".to_string(),
        );
        properties.insert(S3_SESSION_TOKEN.to_string(), "s3-token".to_string());
        properties.insert(S3_REGION.to_string(), "us-west-2".to_string());
        properties.insert(S3_ENDPOINT.to_string(), "http://s3.example".to_string());

        let view = AwsProperties::new(&properties);
        let mapped = view.s3_file_io_properties(
            Some("http://catalog.example"),
            DEFAULT_SESSION_NAME,
            Some("eu-central-1"),
        );

        assert_eq!(mapped.get(S3_ACCESS_KEY_ID).unwrap(), "s3-access-key");
        assert_eq!(mapped.get(S3_SECRET_ACCESS_KEY).unwrap(), "s3-secret-key");
        assert_eq!(mapped.get(S3_SESSION_TOKEN).unwrap(), "s3-token");
        assert_eq!(mapped.get(S3_REGION).unwrap(), "us-west-2");
        assert_eq!(mapped.get(S3_ENDPOINT).unwrap(), "http://s3.example");
        assert_eq!(
            mapped.get(S3_ASSUME_ROLE_SESSION_NAME).unwrap(),
            DEFAULT_SESSION_NAME
        );
        assert!(view.has_explicit_s3_credentials());
    }

    #[test]
    fn preserves_explicit_assume_role_session_name() {
        let mut properties = assume_role_properties();
        properties.insert(
            S3_ASSUME_ROLE_SESSION_NAME.to_string(),
            "caller-session".to_string(),
        );

        let view = AwsProperties::new(&properties);
        let mapped = view.s3_file_io_properties(None, DEFAULT_SESSION_NAME, None);

        assert_eq!(
            mapped.get(S3_ASSUME_ROLE_SESSION_NAME).map(String::as_str),
            Some("caller-session")
        );
    }

    #[test]
    fn region_precedence_s3_over_region_name_over_resolved() {
        let mut props_with_s3 = assume_role_properties();
        props_with_s3.insert(S3_REGION.to_string(), "us-west-2".to_string());
        let mapped_s3 = AwsProperties::new(&props_with_s3).s3_file_io_properties(
            None,
            DEFAULT_SESSION_NAME,
            Some("eu-central-1"),
        );
        assert_eq!(
            mapped_s3.get(S3_REGION).map(String::as_str),
            Some("us-west-2")
        );

        let props_with_region_name = assume_role_properties();
        let mapped_region_name = AwsProperties::new(&props_with_region_name).s3_file_io_properties(
            None,
            DEFAULT_SESSION_NAME,
            Some("eu-central-1"),
        );
        assert_eq!(
            mapped_region_name.get(S3_REGION).map(String::as_str),
            Some("ap-southeast-2")
        );

        let mut props_without_region = assume_role_properties();
        props_without_region.remove(AWS_REGION_NAME);
        let mapped_resolved = AwsProperties::new(&props_without_region).s3_file_io_properties(
            None,
            DEFAULT_SESSION_NAME,
            Some("eu-central-1"),
        );
        assert_eq!(
            mapped_resolved.get(S3_REGION).map(String::as_str),
            Some("eu-central-1")
        );

        let mapped_absent = AwsProperties::new(&props_without_region).s3_file_io_properties(
            None,
            DEFAULT_SESSION_NAME,
            None,
        );
        assert!(!mapped_absent.contains_key(S3_REGION));
    }

    #[test]
    fn debug_redacts_sensitive_properties() {
        let props = HashMap::from([
            (AWS_PROFILE_NAME.to_string(), "my-profile".to_string()),
            (AWS_REGION_NAME.to_string(), "us-east-1".to_string()),
            (
                AWS_ACCESS_KEY_ID.to_string(),
                "AKIAIOSFODNN7EXAMPLE".to_string(),
            ),
            (
                AWS_SECRET_ACCESS_KEY.to_string(),
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            ),
            (
                AWS_SESSION_TOKEN.to_string(),
                "AQoDYXdzEJr...secret-token".to_string(),
            ),
        ]);

        let debug_output = format!("{:?}", AwsProperties::new(&props));

        assert!(debug_output.contains("my-profile"));
        assert!(debug_output.contains("us-east-1"));
        assert!(debug_output.contains("[REDACTED]"));
        assert!(!debug_output.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(!debug_output.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));
        assert!(!debug_output.contains("AQoDYXdzEJr...secret-token"));
    }

    #[test]
    fn removes_assume_role_properties_for_shared_provider() {
        let mut mapped = AwsProperties::new(&assume_role_properties()).s3_file_io_properties(
            None,
            DEFAULT_SESSION_NAME,
            None,
        );
        mapped.insert(
            S3_ASSUME_ROLE_EXTERNAL_ID.to_string(),
            "external-id".to_string(),
        );

        remove_assume_role_properties(&mut mapped);

        assert!(!mapped.contains_key(S3_ASSUME_ROLE_ARN));
        assert!(!mapped.contains_key(S3_ASSUME_ROLE_EXTERNAL_ID));
        assert!(!mapped.contains_key(S3_ASSUME_ROLE_SESSION_NAME));
    }
}
