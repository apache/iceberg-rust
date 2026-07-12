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
use reqwest::Request;
use reqwest::header::{AUTHORIZATION, HeaderName};

use super::{AuthManager, AuthSession};
use crate::catalog::{
    REST_CATALOG_PROP_SIGNING_NAME, REST_CATALOG_PROP_SIGNING_REGION, sigv4_signer_from_props,
};
use crate::signing::SigV4Signer;

/// Header the delegate's `Authorization` is relocated to before signing, so
/// token-based auth composes with SigV4 (Iceberg Java parity: AWS needs the
/// `Authorization` header for the signature itself).
const RELOCATED_AUTH_HEADER: HeaderName = HeaderName::from_static("x-iceberg-authorization");

/// [`AuthManager`] that SigV4-signs every request, wrapping a delegate
/// manager whose authentication (e.g. an OAuth2 bearer token) is relocated to
/// `X-Iceberg-Authorization` and included in the signature.
#[derive(Debug)]
pub struct SigV4AuthManager {
    delegate: Arc<dyn AuthManager>,
    signer: SigV4Signer,
}

impl SigV4AuthManager {
    /// Creates a SigV4 manager signing with `signer` on top of `delegate`.
    pub fn new(delegate: Arc<dyn AuthManager>, signer: SigV4Signer) -> Self {
        Self { delegate, signer }
    }
}

#[async_trait]
impl AuthManager for SigV4AuthManager {
    async fn init_session(&self) -> Result<Arc<dyn AuthSession>> {
        Ok(Arc::new(SigV4Session {
            delegate: self.delegate.init_session().await?,
            signer: self.signer.clone(),
        }))
    }

    async fn catalog_session(
        &self,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        // The merged properties include the server's defaults/overrides, so a
        // server-supplied `rest.signing-*` is honored here. When the props
        // don't define a signing config (e.g. an injected manager with an
        // explicit signer), keep the configured one.
        let signer = if props.contains_key(REST_CATALOG_PROP_SIGNING_REGION)
            && props.contains_key(REST_CATALOG_PROP_SIGNING_NAME)
        {
            sigv4_signer_from_props(props)?
        } else {
            self.signer.clone()
        };
        Ok(Arc::new(SigV4Session {
            delegate: self.delegate.catalog_session(props).await?,
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
    async fn authenticate(&self, request: &mut Request) -> Result<()> {
        self.delegate.authenticate(request).await?;
        if let Some(auth) = request.headers_mut().remove(AUTHORIZATION) {
            request.headers_mut().insert(RELOCATED_AUTH_HEADER, auth);
        }
        self.signer.sign(request)
    }

    async fn invalidate(&self) -> Result<()> {
        self.delegate.invalidate().await
    }

    async fn refresh(&self) -> Result<()> {
        self.delegate.refresh().await
    }
}
