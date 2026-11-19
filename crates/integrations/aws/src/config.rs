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

use aws_config::sts::AssumeRoleProvider;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_sts::config::Credentials;

use crate::AwsProperties;
use crate::credentials::RefreshingCredentialsProvider;

pub(crate) async fn create_sdk_config(
    aws_props: &AwsProperties<'_>,
    endpoint_uri: Option<&str>,
    default_session_name: &str,
) -> SdkConfig {
    let base_config = load_base_sdk_config(aws_props).await;
    create_sdk_config_from_base_config(aws_props, endpoint_uri, default_session_name, base_config)
        .await
}

pub(crate) async fn load_base_sdk_config(aws_props: &AwsProperties<'_>) -> SdkConfig {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());

    if let (Ok(Some(access_key)), Ok(Some(secret_key))) =
        (aws_props.access_key_id(), aws_props.secret_access_key())
    {
        let session_token = aws_props.session_token().ok().flatten();
        let credentials_provider =
            Credentials::new(access_key, secret_key, session_token, None, "properties");
        loader = loader.credentials_provider(credentials_provider);
    }

    if let Ok(Some(profile_name)) = aws_props.profile_name() {
        loader = loader.profile_name(profile_name);
    }

    if let Ok(Some(region_name)) = aws_props.region_name() {
        loader = loader.region(Region::new(region_name));
    }

    loader.load().await
}

pub(crate) async fn create_sdk_config_from_base_config(
    aws_props: &AwsProperties<'_>,
    endpoint_uri: Option<&str>,
    default_session_name: &str,
    base_config: SdkConfig,
) -> SdkConfig {
    let Ok(Some(role_arn)) = aws_props.assume_role_arn() else {
        return match endpoint_uri {
            Some(endpoint) => base_config.into_builder().endpoint_url(endpoint).build(),
            None => base_config,
        };
    };

    let custom_session_name = aws_props.assume_role_session_name().ok().flatten();
    let session_name = custom_session_name
        .as_deref()
        .unwrap_or(default_session_name);
    let mut assume_role_builder = AssumeRoleProvider::builder(role_arn)
        .session_name(session_name)
        .configure(&base_config);

    if let Ok(Some(external_id)) = aws_props.assume_role_external_id() {
        assume_role_builder = assume_role_builder.external_id(external_id);
    }

    let assume_role_provider = SharedCredentialsProvider::new(RefreshingCredentialsProvider::new(
        assume_role_builder.build().await,
    ));

    // Only apply the catalog endpoint override to the catalog's own service config,
    // never to `base_config` used by the STS `AssumeRole` provider above.
    let mut builder = base_config
        .into_builder()
        .credentials_provider(assume_role_provider);
    if let Some(endpoint) = endpoint_uri {
        builder = builder.endpoint_url(endpoint);
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use aws_credential_types::provider::ProvideCredentials;
    use mockito::Matcher;
    use reqsign_core::{Context, ProvideCredential};

    use super::*;
    use crate::{
        AWS_ACCESS_KEY_ID, AWS_ASSUME_ROLE_ARN, AWS_ASSUME_ROLE_EXTERNAL_ID,
        AWS_ASSUME_ROLE_SESSION_NAME, AWS_PROFILE_NAME, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY,
        AWS_SESSION_TOKEN, AwsSdkCredentialProvider,
    };

    const DEFAULT_SESSION_NAME: &str = "iceberg-glue-catalog";
    const CATALOG_ENDPOINT: &str = "http://192.0.2.1:1234";

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

    async fn create_sdk_config_with_sts_endpoint_for_test(
        properties: &HashMap<String, String>,
        catalog_endpoint_uri: Option<&str>,
        default_session_name: &str,
        sts_endpoint_uri: &str,
    ) -> SdkConfig {
        let aws_props = AwsProperties::new(properties);
        let base_config = load_base_sdk_config(&aws_props)
            .await
            .into_builder()
            .endpoint_url(sts_endpoint_uri)
            .build();
        create_sdk_config_from_base_config(
            &aws_props,
            catalog_endpoint_uri,
            default_session_name,
            base_config,
        )
        .await
    }

    #[tokio::test]
    async fn config_with_static_properties_and_custom_endpoint() {
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
        let endpoint_url = "http://custom_url:5001";

        let sdk_config = AwsProperties::new(&properties)
            .create_sdk_config(Some(endpoint_url), DEFAULT_SESSION_NAME)
            .await;

        let region = sdk_config.region().unwrap().as_ref();
        let credentials = sdk_config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        assert_eq!(sdk_config.endpoint_url(), Some(endpoint_url));
        assert_eq!("us-east-1", region);
        assert_eq!("my-access-id", credentials.access_key_id());
        assert_eq!("my-secret-key", credentials.secret_access_key());
        assert_eq!(Some("my-token"), credentials.session_token());
    }

    #[tokio::test]
    async fn ignores_partial_static_credentials() {
        let properties = HashMap::from([
            (
                AWS_ACCESS_KEY_ID.to_string(),
                "orphan-access-id".to_string(),
            ),
            (AWS_REGION_NAME.to_string(), "us-east-1".to_string()),
        ]);

        let sdk_config = AwsProperties::new(&properties)
            .create_sdk_config(None, DEFAULT_SESSION_NAME)
            .await;

        assert_eq!(
            sdk_config.region().map(AsRef::as_ref),
            Some("us-east-1"),
            "region should still be resolved when static credentials are partial"
        );
        if let Some(provider) = sdk_config.credentials_provider()
            && let Ok(credentials) = provider.provide_credentials().await
        {
            assert_ne!(
                credentials.access_key_id(),
                "orphan-access-id",
                "partial static credentials must not install a broken static provider"
            );
        }
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

        let sdk_config = create_sdk_config_with_sts_endpoint_for_test(
            &assume_role_properties(),
            Some(CATALOG_ENDPOINT),
            DEFAULT_SESSION_NAME,
            &server.url(),
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
        assert_eq!(sdk_config.endpoint_url(), Some(CATALOG_ENDPOINT));
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

        let config = create_sdk_config_with_sts_endpoint_for_test(
            &properties,
            Some(CATALOG_ENDPOINT),
            DEFAULT_SESSION_NAME,
            &server.url(),
        )
        .await;
        config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn assume_role_never_targets_the_catalog_endpoint() {
        let mut catalog_server = mockito::Server::new_async().await;
        let catalog_mock = catalog_server
            .mock("POST", "/")
            .expect(0)
            .create_async()
            .await;

        let mut sts_server = mockito::Server::new_async().await;
        let sts_mock = sts_server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(assume_role_response())
            .expect(1)
            .create_async()
            .await;

        let sdk_config = create_sdk_config_with_sts_endpoint_for_test(
            &assume_role_properties(),
            Some(&catalog_server.url()),
            DEFAULT_SESSION_NAME,
            &sts_server.url(),
        )
        .await;
        sdk_config
            .credentials_provider()
            .unwrap()
            .provide_credentials()
            .await
            .unwrap();

        assert_eq!(
            sdk_config.endpoint_url(),
            Some(catalog_server.url().as_str())
        );
        sts_mock.assert_async().await;
        catalog_mock.assert_async().await;
    }
}
