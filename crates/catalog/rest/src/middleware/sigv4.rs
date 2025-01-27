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

//! Middleware that signs requests using the AWS SigV4 signing process.

use std::time::SystemTime;

use anyhow::anyhow;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use http::Extensions;
use reqwest::{Request, Response};
use reqwest_middleware::{Middleware, Next, Result};
use tokio::sync::OnceCell;

pub(crate) struct SigV4Middleware {
    catalog_uri: String,
    signing_name: String,
    signing_region: Option<String>,
    config: OnceCell<aws_config::SdkConfig>,
}

impl SigV4Middleware {
    pub(crate) fn new(catalog_uri: &str, signing_name: &str, signing_region: Option<&str>) -> Self {
        Self {
            catalog_uri: catalog_uri.to_string(),
            signing_name: signing_name.to_string(),
            signing_region: signing_region.map(|s| s.to_string()),
            config: OnceCell::new(),
        }
    }
}

#[async_trait::async_trait]
impl Middleware for SigV4Middleware {
    async fn handle(
        &self,
        mut req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> Result<Response> {
        // Skip requests not matching the catalog URI prefix
        if !req.url().as_str().starts_with(&self.catalog_uri) {
            return next.run(req, extensions).await;
        }

        let signing_region = self.signing_region.clone();
        let config = self
            .config
            .get_or_init(|| async {
                let mut config_loader = aws_config::defaults(BehaviorVersion::v2024_03_28());
                if let Some(signing_region) = signing_region {
                    config_loader = config_loader.region(Region::new(signing_region));
                }
                config_loader.load().await
            })
            .await;

        let credential_provider = config.credentials_provider().ok_or_else(|| {
            reqwest_middleware::Error::Middleware(anyhow!("No credentials provider found"))
        })?;

        let region: &str = config.region().map(|r| r.as_ref()).unwrap_or("us-east-1");

        // Prepare signing parameters
        let credentials = credential_provider
            .provide_credentials()
            .await
            .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?
            .into();
        let signing_params = v4::SigningParams::builder()
            .identity(&credentials)
            .region(region)
            .name(&self.signing_name)
            .time(SystemTime::now())
            .settings(SigningSettings::default())
            .build()
            .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;

        // In order to sign the request, we need to read the body into bytes.
        let body = match req.body() {
            Some(body) => SignableBody::Bytes(body.as_bytes().ok_or_else(|| {
                reqwest_middleware::Error::Middleware(anyhow!("Unable to read body as bytes"))
            })?),
            None => SignableBody::Bytes(&[]),
        };
        let signable_request = SignableRequest::new(
            req.method().as_str(),
            req.url().as_str(),
            req.headers()
                .iter()
                .map(|(k, v)| (k.as_str(), v.to_str().expect("Invalid header value"))),
            body,
        )
        .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;

        // Sign the request
        let signed_request = sign(signable_request, &signing_params.into())
            .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;

        // Rebuild the reqwest request with signed headers
        let (signed_parts, _signature) = signed_request.into_parts();

        let (new_headers, _) = signed_parts.into_parts();
        for header in new_headers.into_iter() {
            let mut value = http::HeaderValue::from_str(header.value()).unwrap();
            value.set_sensitive(header.sensitive());
            req.headers_mut().insert(header.name(), value);
        }

        next.run(req, extensions).await
    }
}
