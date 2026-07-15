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
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::sts::AssumeRoleProvider;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials as AwsSdkCredentials;
use aws_credential_types::provider::{
    ProvideCredentials, SharedCredentialsProvider, future as credentials_future,
};
use aws_sdk_sts::config::Credentials;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ASSUME_ROLE_ARN, S3_ASSUME_ROLE_EXTERNAL_ID, S3_ASSUME_ROLE_SESSION_NAME,
    S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
};
use reqsign_aws_v4::Credential as AwsCredential;
use reqsign_core::time::Timestamp;
use reqsign_core::{Context, Error as ReqsignError, ProvideCredential};

use crate::{
    AWS_ACCESS_KEY_ID, AWS_ASSUME_ROLE_ARN, AWS_ASSUME_ROLE_EXTERNAL_ID,
    AWS_ASSUME_ROLE_SESSION_NAME, AWS_PROFILE_NAME, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
};

/// Creates an AWS SDK configuration from catalog properties.
///
/// When `client.assume-role.arn` is set, the configured base credentials,
/// profile, region, and runtime settings are used for the STS request. The
/// returned configuration uses the refreshable assumed-role provider.
pub async fn create_sdk_config(
    properties: &HashMap<String, String>,
    endpoint_uri: Option<&str>,
    default_session_name: &str,
) -> SdkConfig {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());

    if let Some(endpoint) = endpoint_uri {
        loader = loader.endpoint_url(endpoint);
    }

    if let (Some(access_key), Some(secret_key)) = (
        properties.get(AWS_ACCESS_KEY_ID),
        properties.get(AWS_SECRET_ACCESS_KEY),
    ) {
        let session_token = properties.get(AWS_SESSION_TOKEN).cloned();
        let credentials_provider =
            Credentials::new(access_key, secret_key, session_token, None, "properties");
        loader = loader.credentials_provider(credentials_provider);
    }

    if let Some(profile_name) = properties.get(AWS_PROFILE_NAME) {
        loader = loader.profile_name(profile_name);
    }

    if let Some(region_name) = properties.get(AWS_REGION_NAME) {
        loader = loader.region(Region::new(region_name.clone()));
    }

    let base_config = loader.load().await;
    let Some(role_arn) = properties.get(AWS_ASSUME_ROLE_ARN) else {
        return base_config;
    };

    let session_name = properties
        .get(AWS_ASSUME_ROLE_SESSION_NAME)
        .map(String::as_str)
        .unwrap_or(default_session_name);
    let mut assume_role_builder = AssumeRoleProvider::builder(role_arn)
        .session_name(session_name)
        .configure(&base_config);

    if let Some(external_id) = properties.get(AWS_ASSUME_ROLE_EXTERNAL_ID) {
        assume_role_builder = assume_role_builder.external_id(external_id);
    }

    let assume_role_provider = SharedCredentialsProvider::new(RefreshingCredentialsProvider {
        inner: assume_role_builder.build().await,
        cached: tokio::sync::Mutex::new(None),
    });
    base_config
        .into_builder()
        .credentials_provider(assume_role_provider)
        .build()
}

#[derive(Debug)]
struct RefreshingCredentialsProvider {
    inner: AssumeRoleProvider,
    cached: tokio::sync::Mutex<Option<AwsSdkCredentials>>,
}

impl RefreshingCredentialsProvider {
    async fn credentials(&self) -> aws_credential_types::provider::Result {
        let mut cached = self.cached.lock().await;
        if let Some(credentials) = cached
            .as_ref()
            .filter(|credentials| credentials_are_fresh(credentials, SystemTime::now()))
        {
            return Ok(credentials.clone());
        }

        let credentials = self.inner.provide_credentials().await?;
        *cached = Some(credentials.clone());
        Ok(credentials)
    }
}

impl ProvideCredentials for RefreshingCredentialsProvider {
    fn provide_credentials<'a>(&'a self) -> credentials_future::ProvideCredentials<'a>
    where Self: 'a {
        credentials_future::ProvideCredentials::new(self.credentials())
    }
}

fn credentials_are_fresh(credentials: &AwsSdkCredentials, now: SystemTime) -> bool {
    credentials
        .expiry()
        .is_none_or(|expiry| expiry > now + Duration::from_secs(300))
}

/// A reqsign credential provider backed by an AWS SDK credential provider.
///
/// Clones share the SDK provider, including its assumed-role credential cache
/// and refresh behavior.
#[derive(Clone, Debug)]
pub struct AwsSdkCredentialProvider {
    provider: SharedCredentialsProvider,
}

impl AwsSdkCredentialProvider {
    /// Creates an adapter from the credentials provider in `config`.
    pub fn from_sdk_config(config: &SdkConfig) -> Option<Self> {
        config
            .credentials_provider()
            .map(|provider| Self { provider })
    }
}

impl ProvideCredential for AwsSdkCredentialProvider {
    type Credential = AwsCredential;

    async fn provide_credential(
        &self,
        _ctx: &Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let credentials = self.provider.provide_credentials().await.map_err(|error| {
            ReqsignError::credential_invalid("failed to load AWS SDK credentials")
                .with_source(error)
        })?;

        let expires_in = credentials
            .expiry()
            .map(|expiry| {
                let duration = expiry.duration_since(UNIX_EPOCH).map_err(|error| {
                    ReqsignError::credential_invalid("AWS credential expiry predates Unix epoch")
                        .with_source(error)
                })?;
                let millis = i64::try_from(duration.as_millis()).map_err(|error| {
                    ReqsignError::credential_invalid("AWS credential expiry is out of range")
                        .with_source(error)
                })?;
                Timestamp::from_millisecond(millis)
            })
            .transpose()?;

        Ok(Some(AwsCredential {
            access_key_id: credentials.access_key_id().to_string(),
            secret_access_key: credentials.secret_access_key().to_string(),
            session_token: credentials.session_token().map(ToString::to_string),
            expires_in,
        }))
    }
}

/// Returns whether the caller supplied S3-specific static credentials.
pub fn has_explicit_s3_credentials(properties: &HashMap<String, String>) -> bool {
    properties.contains_key(S3_ACCESS_KEY_ID)
        || properties.contains_key(S3_SECRET_ACCESS_KEY)
        || properties.contains_key(S3_SESSION_TOKEN)
}

/// Removes AssumeRole properties after FileIO has been given the SDK provider.
///
/// This prevents OpenDAL from performing a second AssumeRole request.
pub fn remove_assume_role_properties(properties: &mut HashMap<String, String>) {
    properties.remove(S3_ASSUME_ROLE_ARN);
    properties.remove(S3_ASSUME_ROLE_EXTERNAL_ID);
    properties.remove(S3_ASSUME_ROLE_SESSION_NAME);
}

/// Maps generic AWS properties to S3-specific properties.
///
/// Explicit S3 properties take precedence. When AssumeRole is enabled, the
/// service-specific default session name is materialized for FileIO.
pub fn map_aws_to_s3_properties(
    properties: &HashMap<String, String>,
    endpoint_uri: Option<&str>,
    default_session_name: &str,
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
    if s3_props.contains_key(AWS_ASSUME_ROLE_ARN)
        && !s3_props.contains_key(S3_ASSUME_ROLE_SESSION_NAME)
    {
        s3_props.insert(
            S3_ASSUME_ROLE_SESSION_NAME.to_string(),
            default_session_name.to_string(),
        );
    }

    s3_props
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use aws_credential_types::provider::ProvideCredentials;
    use iceberg::io::{
        S3_ACCESS_KEY_ID, S3_ASSUME_ROLE_ARN, S3_ASSUME_ROLE_EXTERNAL_ID,
        S3_ASSUME_ROLE_SESSION_NAME, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
        S3_SESSION_TOKEN,
    };
    use mockito::Matcher;
    use reqsign_core::{Context, ProvideCredential};

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

    fn assume_role_response() -> &'static str {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>assumed-access-key</AccessKeyId>
      <SecretAccessKey>assumed-secret-key</SecretAccessKey>
      <SessionToken>assumed-session-token</SessionToken>
      <Expiration>2035-01-01T00:00:00Z</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::123456789012:assumed-role/TestRole/iceberg</Arn>
      <AssumedRoleId>AROAEXAMPLE:iceberg</AssumedRoleId>
    </AssumedRoleUser>
  </AssumeRoleResult>
  <ResponseMetadata><RequestId>request-id</RequestId></ResponseMetadata>
</AssumeRoleResponse>"#
    }

    #[tokio::test]
    async fn assume_role_uses_base_config_and_shared_provider() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .match_header(
                "authorization",
                Matcher::Regex(
                    "Credential=base-access-key/.*/ap-southeast-2/sts/aws4_request".into(),
                ),
            )
            .match_body(Matcher::AllOf(vec![
                Matcher::Regex("RoleSessionName=iceberg-glue-catalog".into()),
                Matcher::Regex(
                    "RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2FTestRole".into(),
                ),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(assume_role_response())
            .expect(1)
            .create_async()
            .await;

        let sdk_config = create_sdk_config(
            &assume_role_properties(),
            Some(&server.url()),
            DEFAULT_SESSION_NAME,
        )
        .await;
        let sdk_provider = sdk_config.credentials_provider().unwrap();
        let sdk_credentials = sdk_provider.provide_credentials().await.unwrap();
        let reqsign_credentials = AwsSdkCredentialProvider::from_sdk_config(&sdk_config)
            .unwrap()
            .provide_credential(&Context::default())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(sdk_credentials.access_key_id(), "assumed-access-key");
        assert_eq!(sdk_credentials.secret_access_key(), "assumed-secret-key");
        assert_eq!(
            sdk_credentials.session_token(),
            Some("assumed-session-token")
        );
        assert_eq!(reqsign_credentials.access_key_id, "assumed-access-key");
        assert_eq!(reqsign_credentials.secret_access_key, "assumed-secret-key");
        assert_eq!(
            reqsign_credentials.session_token.as_deref(),
            Some("assumed-session-token")
        );
        assert!(reqsign_credentials.expires_in.is_some());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn assume_role_sends_custom_session_name_and_external_id() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .match_body(Matcher::AllOf(vec![
                Matcher::Regex("RoleSessionName=custom-session".into()),
                Matcher::Regex("ExternalId=external-id".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(assume_role_response())
            .expect(1)
            .create_async()
            .await;
        let mut properties = assume_role_properties();
        properties.insert(
            AWS_ASSUME_ROLE_SESSION_NAME.to_string(),
            "custom-session".to_string(),
        );
        properties.insert(
            AWS_ASSUME_ROLE_EXTERNAL_ID.to_string(),
            "external-id".to_string(),
        );

        let config =
            create_sdk_config(&properties, Some(&server.url()), DEFAULT_SESSION_NAME).await;
        config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        mock.assert_async().await;
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

        let mapped = map_aws_to_s3_properties(
            &properties,
            Some("http://catalog.example"),
            DEFAULT_SESSION_NAME,
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
        assert!(has_explicit_s3_credentials(&properties));
    }

    #[test]
    fn removes_assume_role_properties_for_shared_provider() {
        let mut mapped =
            map_aws_to_s3_properties(&assume_role_properties(), None, DEFAULT_SESSION_NAME);
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
