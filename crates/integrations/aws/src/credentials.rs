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

use std::fmt::{Debug, Formatter};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_config::SdkConfig;
use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::Credentials as AwsSdkCredentials;
use aws_credential_types::provider::{
    ProvideCredentials, SharedCredentialsProvider, future as credentials_future,
};
use reqsign_aws_v4::Credential as AwsCredential;
use reqsign_core::time::Timestamp;
use reqsign_core::{Context, Error as ReqsignError, ProvideCredential};
use tokio::sync::RwLock;

const REFRESH_RETRY_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub(crate) struct RefreshingCredentialsProvider {
    inner: AssumeRoleProvider,
    cached: RwLock<CachedCredentialsState>,
}

#[derive(Default)]
struct CachedCredentialsState {
    credentials: Option<AwsSdkCredentials>,
    retry_not_before: Option<SystemTime>,
}

impl Debug for CachedCredentialsState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedCredentialsState")
            .field("has_credentials", &self.credentials.is_some())
            .field("retry_not_before", &self.retry_not_before)
            .finish()
    }
}

impl CachedCredentialsState {
    fn reusable_credentials(&self, now: SystemTime) -> Option<AwsSdkCredentials> {
        if let Some(credentials) = self
            .credentials
            .as_ref()
            .filter(|credentials| credentials_are_fresh(credentials, now))
        {
            return Some(credentials.clone());
        }

        if self.retry_not_before.is_some_and(|retry_at| now < retry_at)
            && let Some(credentials) = self
                .credentials
                .as_ref()
                .filter(|credentials| credentials_are_unexpired(credentials, now))
        {
            return Some(credentials.clone());
        }

        None
    }
}

impl RefreshingCredentialsProvider {
    #[allow(dead_code)]
    pub(crate) fn new(inner: AssumeRoleProvider) -> Self {
        Self {
            inner,
            cached: RwLock::new(CachedCredentialsState::default()),
        }
    }

    async fn credentials(&self) -> aws_credential_types::provider::Result {
        {
            let state = self.cached.read().await;
            let now = SystemTime::now();
            if let Some(credentials) = state.reusable_credentials(now) {
                return Ok(credentials);
            }
        }

        let mut state = self.cached.write().await;
        let now = SystemTime::now();
        if let Some(credentials) = state.reusable_credentials(now) {
            return Ok(credentials);
        }

        match self.inner.provide_credentials().await {
            Ok(credentials) => {
                state.credentials = Some(credentials.clone());
                state.retry_not_before = None;
                Ok(credentials)
            }
            Err(error) => {
                state.retry_not_before = Some(now + REFRESH_RETRY_BACKOFF);
                if let Some(credentials) = state
                    .credentials
                    .as_ref()
                    .filter(|credentials| credentials_are_unexpired(credentials, now))
                {
                    tracing::warn!(
                        "failed to refresh AssumeRole credentials, falling back to cached \
                         credentials that have not yet expired: {error}"
                    );
                    return Ok(credentials.clone());
                }
                Err(error)
            }
        }
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

fn credentials_are_unexpired(credentials: &AwsSdkCredentials, now: SystemTime) -> bool {
    credentials.expiry().is_none_or(|expiry| expiry > now)
}

/// Adapts an AWS SDK credentials provider to [`reqsign_core::ProvideCredential`].
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use aws_config::{BehaviorVersion, Region};
    use aws_credential_types::provider::ProvideCredentials;
    use aws_sdk_sts::config::Credentials;
    use tracing::instrument::WithSubscriber;

    use super::*;

    /// With a single live dispatcher, tracing resolves callsite interest from the
    /// registering thread's default, so a concurrent test can disable a callsite for
    /// the capturing subscriber. A second dispatcher avoids that.
    static KEEP_ALIVE: std::sync::LazyLock<tracing::Dispatch> = std::sync::LazyLock::new(|| {
        tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default())
    });

    #[derive(Clone, Default)]
    struct CapturedLog(Arc<std::sync::Mutex<String>>);

    impl tracing::field::Visit for CapturedLog {
        fn record_debug(&mut self, _: &tracing::field::Field, value: &dyn Debug) {
            self.0.lock().unwrap().push_str(&format!("{value:?}"));
        }
    }

    impl tracing::Subscriber for CapturedLog {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::Id {
            tracing::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            event.record(&mut self.clone());
        }
        fn enter(&self, _: &tracing::Id) {}
        fn exit(&self, _: &tracing::Id) {}
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

    fn assume_role_error_response() -> &'static str {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error>
    <Type>Sender</Type>
    <Code>AccessDenied</Code>
    <Message>not authorized</Message>
  </Error>
  <RequestId>request-id</RequestId>
</ErrorResponse>"#
    }

    async fn build_assume_role_provider(sts_endpoint_uri: &str) -> AssumeRoleProvider {
        let base_config = aws_config::defaults(BehaviorVersion::latest())
            .credentials_provider(Credentials::new(
                "base-access-key",
                "base-secret-key",
                None,
                None,
                "test",
            ))
            .region(Region::new("ap-southeast-2"))
            .endpoint_url(sts_endpoint_uri)
            .load()
            .await;

        AssumeRoleProvider::builder("arn:aws:iam::123456789012:role/TestRole")
            .session_name("test-session")
            .configure(&base_config)
            .build()
            .await
    }

    #[tokio::test]
    async fn falls_back_to_cached_credentials_when_refresh_fails_but_not_yet_expired() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(403)
            .with_header("content-type", "text/xml")
            .with_body(assume_role_error_response())
            .expect(1)
            .create_async()
            .await;

        let provider = RefreshingCredentialsProvider {
            inner: build_assume_role_provider(&server.url()).await,
            cached: RwLock::new(CachedCredentialsState {
                credentials: Some(AwsSdkCredentials::new(
                    "cached-access-key",
                    "cached-secret-key",
                    None,
                    Some(SystemTime::now() + Duration::from_secs(60)),
                    "test",
                )),
                retry_not_before: None,
            }),
        };

        std::sync::LazyLock::force(&KEEP_ALIVE);
        let log = CapturedLog::default();
        let credentials = provider
            .credentials()
            .with_subscriber(log.clone())
            .await
            .expect("should fall back to the still-valid cached credentials");
        assert_eq!(credentials.access_key_id(), "cached-access-key");
        mock.assert_async().await;

        let captured = log.0.lock().unwrap().clone();
        assert!(
            captured.contains("falling back to cached credentials that have not yet expired"),
            "expected log message about fallback, got: {captured}"
        );
    }

    #[tokio::test]
    async fn propagates_error_when_cached_credentials_are_actually_expired() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(403)
            .with_header("content-type", "text/xml")
            .with_body(assume_role_error_response())
            .expect(1)
            .create_async()
            .await;

        let provider = RefreshingCredentialsProvider {
            inner: build_assume_role_provider(&server.url()).await,
            cached: RwLock::new(CachedCredentialsState {
                credentials: Some(AwsSdkCredentials::new(
                    "cached-access-key",
                    "cached-secret-key",
                    None,
                    Some(SystemTime::now() - Duration::from_secs(10)),
                    "test",
                )),
                retry_not_before: None,
            }),
        };

        let result = provider.credentials().await;
        assert!(
            result.is_err(),
            "actually-expired cached credentials must not mask a refresh failure"
        );
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn propagates_error_when_no_cached_credentials_exist() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(403)
            .with_header("content-type", "text/xml")
            .with_body(assume_role_error_response())
            .expect(1)
            .create_async()
            .await;

        let provider =
            RefreshingCredentialsProvider::new(build_assume_role_provider(&server.url()).await);

        let result = provider.credentials().await;
        assert!(result.is_err());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn does_not_retry_sts_again_within_the_backoff_window_after_a_failed_refresh() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(403)
            .with_header("content-type", "text/xml")
            .with_body(assume_role_error_response())
            .expect(1)
            .create_async()
            .await;

        let provider = RefreshingCredentialsProvider {
            inner: build_assume_role_provider(&server.url()).await,
            cached: RwLock::new(CachedCredentialsState {
                credentials: Some(AwsSdkCredentials::new(
                    "cached-access-key",
                    "cached-secret-key",
                    None,
                    Some(SystemTime::now() + Duration::from_secs(60)),
                    "test",
                )),
                retry_not_before: None,
            }),
        };

        let first = provider
            .credentials()
            .await
            .expect("first call falls back to cache after the failed refresh");
        assert_eq!(first.access_key_id(), "cached-access-key");

        let second = provider
            .credentials()
            .await
            .expect("second call must not re-hit STS within the backoff window");
        assert_eq!(second.access_key_id(), "cached-access-key");

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn concurrent_callers_share_single_sts_refresh() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(assume_role_response())
            .expect(1)
            .create_async()
            .await;

        let provider = Arc::new(RefreshingCredentialsProvider {
            inner: build_assume_role_provider(&server.url()).await,
            cached: RwLock::new(CachedCredentialsState {
                credentials: Some(AwsSdkCredentials::new(
                    "stale-access-key",
                    "stale-secret-key",
                    None,
                    Some(SystemTime::now() + Duration::from_secs(60)),
                    "test",
                )),
                retry_not_before: None,
            }),
        });

        let p1 = Arc::clone(&provider);
        let p2 = Arc::clone(&provider);
        let (res1, res2) = tokio::join!(p1.provide_credentials(), p2.provide_credentials());

        assert_eq!(res1.unwrap().access_key_id(), "assumed-access-key");
        assert_eq!(res2.unwrap().access_key_id(), "assumed-access-key");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn debug_does_not_leak_cached_credentials() {
        let server = mockito::Server::new_async().await;
        let provider = RefreshingCredentialsProvider {
            inner: build_assume_role_provider(&server.url()).await,
            cached: RwLock::new(CachedCredentialsState {
                credentials: Some(AwsSdkCredentials::new(
                    "cached-access-key",
                    "cached-secret-key",
                    Some("cached-session-token".to_string()),
                    Some(SystemTime::now() + Duration::from_secs(600)),
                    "test",
                )),
                retry_not_before: None,
            }),
        };

        let formatted = format!("{provider:?}");
        assert!(!formatted.contains("cached-secret-key"));
        assert!(!formatted.contains("cached-access-key"));
        assert!(!formatted.contains("cached-session-token"));
        assert!(formatted.contains("has_credentials: true"));
    }
}
