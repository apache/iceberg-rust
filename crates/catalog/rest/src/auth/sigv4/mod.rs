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
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::Result;
use reqwest::header::{AUTHORIZATION, HeaderName};

mod signer;

pub use signer::{AwsCredentials, PayloadHashMode, SigV4Signer};

use super::{AuthManager, AuthSession, HttpRequest};
use crate::catalog::sigv4_signer_from_props;
use crate::client::HttpClient;

/// Header the delegate's `Authorization` is relocated to before signing, so
/// token-based auth composes with SigV4, which needs `Authorization` for the
/// signature itself. Iceberg Java relocates with the `Original-` prefix.
const RELOCATED_AUTH_HEADER: HeaderName = HeaderName::from_static("original-authorization");

/// [`AuthManager`] that SigV4-signs every request, wrapping a delegate
/// manager whose authentication (e.g. an OAuth2 bearer token) is relocated to
/// `Original-Authorization` and included in the signature.
///
/// An injected HTTP client must not follow redirects (a signed request can't
/// be transparently re-followed) nor set default headers that change the
/// signed set (e.g. `Host`, `x-amz-*`): those apply after signing.
#[derive(Debug)]
pub struct SigV4AuthManager {
    delegate: Arc<dyn AuthManager>,
    signer: SigV4Signer,
    /// True when the signer was built from catalog properties:
    /// [`Self::catalog_session`] then rebuilds it from the merged props.
    signer_from_config: bool,
}

impl SigV4AuthManager {
    /// Creates a SigV4 manager signing with `signer` on top of `delegate`.
    ///
    /// The signer is kept as-is — its credentials and payload mode exist
    /// nowhere in the properties, so signing properties never replace it.
    pub fn new(delegate: Arc<dyn AuthManager>, signer: SigV4Signer) -> Self {
        Self {
            delegate,
            signer,
            signer_from_config: false,
        }
    }

    /// A manager whose signer derives from catalog properties; the merged
    /// `/v1/config` properties rebuild it.
    pub(crate) fn from_config_signer(delegate: Arc<dyn AuthManager>, signer: SigV4Signer) -> Self {
        Self {
            delegate,
            signer,
            signer_from_config: true,
        }
    }
}

#[async_trait]
impl AuthManager for SigV4AuthManager {
    async fn init_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        Ok(Box::new(SigV4Session {
            delegate: Arc::from(self.delegate.init_session(client, props).await?),
            signer: self.signer.clone(),
        }))
    }

    async fn catalog_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        // A config-built signer follows the merged properties; an injected
        // one is kept as-is (see [`Self::new`]).
        let signer = if self.signer_from_config {
            sigv4_signer_from_props(props)?
        } else {
            self.signer.clone()
        };
        Ok(Arc::new(SigV4Session {
            delegate: self.delegate.catalog_session(client, props).await?,
            signer,
        }))
    }
}

/// [`AuthSession`] applying the delegate's auth, then SigV4-signing.
#[derive(Debug)]
struct SigV4Session {
    delegate: Arc<dyn AuthSession>,
    signer: SigV4Signer,
}

#[async_trait]
impl AuthSession for SigV4Session {
    async fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
        self.delegate.authenticate(request).await?;
        // Every value moves, and an existing `Original-Authorization` is kept:
        // Java groups them into one list rather than replacing.
        let relocated: Vec<_> = request
            .headers()
            .get_all(AUTHORIZATION)
            .iter()
            .cloned()
            .collect();
        if !relocated.is_empty() {
            request.headers_mut().remove(AUTHORIZATION);
            for mut auth in relocated {
                // Force-mark it: a delegate (or `header.authorization`) may
                // have supplied a non-sensitive value.
                auth.set_sensitive(true);
                request.headers_mut().append(RELOCATED_AUTH_HEADER, auth);
            }
        }
        self.signer.sign(request.inner_mut())
    }
}

#[cfg(test)]
mod tests {
    use iceberg::sensitive::SensitiveString;
    use reqwest::header::HeaderValue;

    use super::*;
    use crate::HttpRequest;

    fn test_session(session_token: Option<&str>) -> SigV4Session {
        SigV4Session {
            delegate: Arc::new(crate::auth::NoopSession),
            signer: SigV4Signer::new(
                AwsCredentials {
                    access_key_id: "AKIDEXAMPLE".into(),
                    secret_access_key: "secret".to_string().into(),
                    session_token: session_token.map(|t| SensitiveString::from(t.to_string())),
                },
                "us-east-1".into(),
                "execute-api".into(),
                PayloadHashMode::IcebergRest,
            ),
        }
    }

    fn request_with(headers: &[(&'static str, &str)]) -> HttpRequest {
        let mut builder = reqwest::Client::new().get("https://rest.example.com/v1/config");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        HttpRequest::new(builder.build().unwrap())
    }

    #[tokio::test]
    async fn relocation_keeps_an_existing_original_authorization() {
        // Java collects both values under `Original-Authorization` instead of
        // replacing one with the other.
        let mut request = request_with(&[
            ("original-authorization", "credential-A"),
            ("authorization", "credential-B"),
        ]);

        test_session(None).authenticate(&mut request).await.unwrap();

        let relocated: Vec<_> = request
            .headers()
            .get_all("original-authorization")
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect();
        assert_eq!(relocated, ["credential-A", "credential-B"]);
        // The signature replaces `authorization`, it does not leave the original.
        assert!(
            request
                .headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 ")
        );
    }

    #[tokio::test]
    async fn conflicting_signer_headers_are_relocated_not_dropped() {
        // Java relocates an original whose value differs from the signed one.
        let mut request = request_with(&[
            ("x-amz-date", "19700101T000000Z"),
            ("x-amz-security-token", "caller-token"),
        ]);

        test_session(Some("signer-token"))
            .authenticate(&mut request)
            .await
            .unwrap();

        assert_eq!(
            request.headers().get("original-x-amz-date").unwrap(),
            "19700101T000000Z"
        );
        let token = request
            .headers()
            .get("original-x-amz-security-token")
            .unwrap();
        assert_eq!(token, "caller-token");
        assert!(token.is_sensitive());
        assert_eq!(
            request.headers().get("x-amz-security-token").unwrap(),
            HeaderValue::from_static("signer-token")
        );
    }
}
