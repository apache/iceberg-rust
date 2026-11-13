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

//! AWS SigV4 request signing for the REST catalog.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use chrono::{DateTime, Utc};
use iceberg::sensitive::SensitiveString;
use iceberg::{Error, ErrorKind, Result, SessionContext};
use sha2::{Digest, Sha256};
use typed_builder::TypedBuilder;

use super::{AuthManager, AuthSession, Credentials};
use crate::client::HttpClient;

/// How the payload hash is encoded in the `x-amz-content-sha256` header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PayloadHashMode {
    /// Iceberg Java's style: a base64 header when there is a body, hex when
    /// there is none, and hex in the canonical request. The base64 comes from
    /// how Java configures the AWS SDK, not from SigV4, so a verifier that
    /// trusts the header instead of hashing the body rejects it.
    IcebergRest,
    /// Standard SigV4: hex everywhere.
    StandardAws,
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn base64_encode(bytes: &[u8]) -> String {
    base64::engine::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

/// The content header and canonical payload hash, computed from one digest.
fn payload_hashes(body: Option<&[u8]>, mode: PayloadHashMode) -> (String, String) {
    let digest = Sha256::digest(body.unwrap_or_default());
    let hex = encode_hex(&digest);
    let header = match mode {
        PayloadHashMode::IcebergRest if body.is_some() => base64_encode(&digest),
        _ => hex.clone(),
    };
    (header, hex)
}

/// Signs REST catalog requests the way Iceberg Java's `RESTSigV4AuthSession`
/// does. Carries no credentials, so one signer serves every session.
///
/// Built with [`SigV4Signer::builder`].
#[derive(Clone, Debug, TypedBuilder)]
pub struct SigV4Signer {
    /// The signing region, e.g. `us-east-1`.
    #[builder(setter(into))]
    region: String,
    /// The signing name, e.g. `execute-api`.
    #[builder(setter(into))]
    service: String,
    /// How the payload hash is encoded.
    mode: PayloadHashMode,
}

impl SigV4Signer {
    /// Signs `request` in place. An existing `Authorization` moves to
    /// `Original-Authorization`, and a caller's `x-amz-date`,
    /// `x-amz-content-sha256` or `x-amz-security-token` that the signer
    /// overwrites moves to `Original-<name>` (Java keeps a caller's content
    /// hash when there is a body). Userinfo leaves the URL, and a `+` in the
    /// query becomes `%20`, so write a literal plus as `%2B`.
    ///
    /// Uses `credentials` as given and never refreshes them: resolve temporary
    /// ones from their provider before each call.
    ///
    /// On error, including for a streaming body or a non-UTF-8 header, the
    /// request is left unchanged.
    ///
    /// Send the result through a client that does not follow redirects: a
    /// redirect replays the signature, and across hosts reqwest drops
    /// `Authorization` but keeps `Original-Authorization`.
    ///
    /// `aws_sigv4` traces requests without redacting `Original-Authorization`.
    /// A `tracing` subscriber is muted for the call, but the `log` bridge (no
    /// subscriber, or `log-always`) still forwards those events, so keep
    /// `aws_sigv4` below trace level there.
    pub fn sign(&self, request: &mut crate::HttpRequest, credentials: &Credentials) -> Result<()> {
        self.sign_at(request, credentials, Utc::now())
    }

    fn sign_at(
        &self,
        request: &mut crate::HttpRequest,
        credentials: &Credentials,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let headers = request.headers().clone();
        let url = request.url().clone();
        let signed = self.sign_in_place(request, credentials, now);
        if signed.is_err() {
            *request.headers_mut() = headers;
            *request.url_mut() = url;
        }
        signed
    }

    fn sign_in_place(
        &self,
        request: &mut crate::HttpRequest,
        credentials: &Credentials,
        now: DateTime<Utc>,
    ) -> Result<()> {
        use aws_sigv4::http_request::{SignableBody, SignableRequest, sign};
        use aws_sigv4::sign::v4;
        use tracing::dispatcher::Dispatch;
        use tracing::level_filters::LevelFilter;

        // Before the content hash is displaced, which takes it out of signing.
        signable_headers(request).try_for_each(|header| header.map(|_| ()))?;
        let (content_header, payload_hash) = payload_hashes(signable_body(request)?, self.mode);

        convert_headers(request);

        // Relocated after signing, so the `Original-` copy is not signed.
        let displaced_content_hash: Vec<_> = request
            .headers()
            .get_all(CONTENT_SHA256)
            .iter()
            .filter(|v| v.as_bytes() != content_header.as_bytes())
            .cloned()
            .collect();
        let content_value = content_header.parse().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "invalid x-amz-content-sha256 value").with_source(e)
        })?;
        request.headers_mut().insert(CONTENT_SHA256, content_value);

        rewrite_url_for_signing(request)?;

        let identity = credentials.clone().into();
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(&self.service)
            .time(now.into())
            .settings(signing_settings())
            .build()
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "failed to build SigV4 params").with_source(e)
            })?
            .into();

        let headers = signable_headers(request).collect::<Result<Vec<_>>>()?;
        let signable = SignableRequest::new(
            request.method().as_str(),
            request.url_str(),
            headers.into_iter(),
            SignableBody::Precomputed(payload_hash),
        )
        .map_err(|e| {
            Error::new(ErrorKind::DataInvalid, "request is not signable").with_source(e)
        })?;

        // `aws_sigv4` traces the request, `Original-Authorization` included.
        // Mute it only when a subscriber could record that (the max level is
        // `OFF` until one is registered): `with_default` also sets
        // tracing-core's `EXISTS` flag, which nothing clears, and with the
        // `log` feature tracing then stops forwarding events to `log` for good.
        let signed = if LevelFilter::current() == LevelFilter::TRACE {
            tracing::dispatcher::with_default(&Dispatch::none(), || sign(signable, &params))
        } else {
            sign(signable, &params)
        };
        let (instructions, _signature) = signed
            .map_err(|e| Error::new(ErrorKind::Unexpected, "SigV4 signing failed").with_source(e))?
            .into_parts();

        update_request_headers(request, instructions, displaced_content_hash)
    }
}

/// The body to sign; as in Java, an absent body and an empty one differ.
fn signable_body(request: &crate::HttpRequest) -> Result<Option<&[u8]>> {
    match request.body() {
        crate::HttpRequestBody::Empty => Ok(None),
        crate::HttpRequestBody::Buffered(bytes) => Ok(Some(bytes)),
        crate::HttpRequestBody::Streaming => Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "cannot sign a streaming request body",
        )),
    }
}

/// The headers to sign. A non-UTF-8 one is an error: skipping it would send
/// it unsigned.
fn signable_headers(request: &crate::HttpRequest) -> impl Iterator<Item = Result<(&str, &str)>> {
    request.headers().iter().map(|(n, v)| {
        let v = std::str::from_utf8(v.as_bytes()).map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("cannot sign non-UTF-8 header value for `{n}`"),
            )
            .with_source(e)
        })?;
        Ok((n.as_str(), v))
    })
}

/// Drops userinfo, which the wire `Host` never carries, and rewrites `+` in the
/// query as `%20`: a space to AWS and Java either way, but unambiguous to any
/// verifier.
fn rewrite_url_for_signing(request: &mut crate::HttpRequest) -> Result<()> {
    if !request.url().username().is_empty() || request.url().password().is_some() {
        let url = request.url_mut();
        url.set_username("")
            .and_then(|()| url.set_password(None))
            .map_err(|()| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "cannot strip userinfo from the request URL",
                )
            })?;
    }
    if let Some(query) = request.url().query().filter(|q| q.contains('+')) {
        let unambiguous = query.replace('+', "%20");
        request.url_mut().set_query(Some(&unambiguous));
    }
    Ok(())
}

/// `Aws4Signer`'s settings: normalized double-encoded path, and Java's ignore
/// list.
fn signing_settings() -> aws_sigv4::http_request::SigningSettings {
    use aws_sigv4::http_request::{
        PayloadChecksumKind, PercentEncodingMode, SigningSettings, UriPathNormalizationMode,
    };

    let mut settings = SigningSettings::default();
    settings.percent_encoding_mode = PercentEncodingMode::Double;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Enabled;
    // We set and relocate the content hash ourselves, outside the instructions.
    settings.payload_checksum_kind = PayloadChecksumKind::NoHeader;
    let mut excluded = settings.excluded_headers.take().unwrap_or_default();
    excluded.extend([
        // Java's list, spelled out rather than left to the crate's defaults.
        "connection".into(),
        "expect".into(),
        "transfer-encoding".into(),
        "user-agent".into(),
        "x-amzn-trace-id".into(),
        // Not Java's, but proxies append to it too.
        "x-forwarded-for".into(),
        // Relocation appends to these after signing.
        "original-x-amz-date".into(),
        "original-x-amz-content-sha256".into(),
        "original-x-amz-security-token".into(),
    ]);
    settings.excluded_headers = Some(excluded);
    settings
}

/// Java's `convertHeaders`: moves `Authorization` aside before signing, so the
/// moved copy is signed.
fn convert_headers(request: &mut crate::HttpRequest) {
    let displaced: Vec<_> = request
        .headers()
        .get_all(reqwest::header::AUTHORIZATION)
        .iter()
        .cloned()
        .collect();
    if displaced.is_empty() {
        return;
    }
    request.headers_mut().remove(reqwest::header::AUTHORIZATION);
    for mut value in displaced {
        value.set_sensitive(true);
        request.headers_mut().append(RELOCATED_AUTHORIZATION, value);
    }
}

/// Java's `updateRequestHeaders`: installs the signed headers, moving
/// conflicting caller values aside.
fn update_request_headers(
    request: &mut crate::HttpRequest,
    instructions: aws_sigv4::http_request::SigningInstructions,
    displaced_content_hash: Vec<reqwest::header::HeaderValue>,
) -> Result<()> {
    let (signed_headers, _params) = instructions.into_parts();
    let h = request.headers_mut();
    for mut value in displaced_content_hash {
        // The original may carry a credential.
        value.set_sensitive(true);
        h.append(RELOCATED_CONTENT_SHA256, value);
    }
    for header in signed_headers {
        let name: reqwest::header::HeaderName = header.name().parse().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "invalid signed header name").with_source(e)
        })?;
        if let Some(relocated) = relocated_name(name.as_str()) {
            relocate_conflicting(h, name.as_str(), header.value(), relocated);
        }
        let mut value: reqwest::header::HeaderValue = header.value().parse().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "invalid signed header value").with_source(e)
        })?;
        if name == reqwest::header::AUTHORIZATION || name == SECURITY_TOKEN {
            value.set_sensitive(true);
        }
        h.insert(name, value);
    }
    Ok(())
}

const CONTENT_SHA256: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-amz-content-sha256");
const AMZ_DATE: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-amz-date");
const SECURITY_TOKEN: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-amz-security-token");

const RELOCATED_AUTHORIZATION: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-authorization");
const RELOCATED_AMZ_DATE: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-date");
const RELOCATED_CONTENT_SHA256: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-content-sha256");
const RELOCATED_SECURITY_TOKEN: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("original-x-amz-security-token");

/// The `Original-<name>` counterpart of a header the signer generates.
fn relocated_name(name: &str) -> Option<reqwest::header::HeaderName> {
    match name {
        n if n == AMZ_DATE => Some(RELOCATED_AMZ_DATE),
        n if n == SECURITY_TOKEN => Some(RELOCATED_SECURITY_TOKEN),
        _ => None,
    }
}

/// Moves `name`'s values that differ from `signed` to `relocated`.
fn relocate_conflicting(
    headers: &mut reqwest::header::HeaderMap,
    name: &str,
    signed: &str,
    relocated: reqwest::header::HeaderName,
) {
    let conflicting: Vec<_> = headers
        .get_all(name)
        .iter()
        .filter(|value| value.as_bytes() != signed.as_bytes())
        .cloned()
        .collect();
    for mut value in conflicting {
        // It may carry a credential, e.g. a session token.
        value.set_sensitive(true);
        headers.append(relocated.clone(), value);
    }
}

/// SigV4 signing region.
pub const REST_CATALOG_PROP_SIGNING_REGION: &str = "rest.signing-region";
/// SigV4 signing name; defaults to [`SIGNING_NAME_DEFAULT`].
pub const REST_CATALOG_PROP_SIGNING_NAME: &str = "rest.signing-name";
/// Static SigV4 access key id. Unlike Java, there is no fallback to the AWS
/// default provider chain.
pub const REST_CATALOG_PROP_ACCESS_KEY_ID: &str = "rest.access-key-id";
/// Static SigV4 secret access key (see [`REST_CATALOG_PROP_ACCESS_KEY_ID`]).
pub const REST_CATALOG_PROP_SECRET_ACCESS_KEY: &str = "rest.secret-access-key";
/// Static SigV4 session token (see [`REST_CATALOG_PROP_ACCESS_KEY_ID`]).
pub const REST_CATALOG_PROP_SESSION_TOKEN: &str = "rest.session-token";

/// Java's `REST_SIGNING_NAME_DEFAULT`: API Gateway and Lambda.
pub const SIGNING_NAME_DEFAULT: &str = "execute-api";

const CREDENTIAL_PROPERTIES: [&str; 3] = [
    REST_CATALOG_PROP_ACCESS_KEY_ID,
    REST_CATALOG_PROP_SECRET_ACCESS_KEY,
    REST_CATALOG_PROP_SESSION_TOKEN,
];
const SIGNING_PROPERTIES: [&str; 5] = [
    REST_CATALOG_PROP_SIGNING_REGION,
    REST_CATALOG_PROP_SIGNING_NAME,
    REST_CATALOG_PROP_ACCESS_KEY_ID,
    REST_CATALOG_PROP_SECRET_ACCESS_KEY,
    REST_CATALOG_PROP_SESSION_TOKEN,
];

/// `base` with `overrides` on top. Overriding any credential replaces the
/// base's credentials whole: fields from two sources never form a valid
/// credential.
fn overlay_signing(
    base: &HashMap<String, SensitiveString>,
    overrides: HashMap<String, String>,
) -> HashMap<String, String> {
    let replaces_credentials = CREDENTIAL_PROPERTIES
        .iter()
        .any(|key| overrides.contains_key(*key));
    let mut props: HashMap<_, _> = base
        .iter()
        .filter(|(k, _)| !(replaces_credentials && CREDENTIAL_PROPERTIES.contains(&k.as_str())))
        .map(|(k, v)| (k.clone(), v.expose().to_string()))
        .collect();
    props.extend(overrides);
    props
}

/// The non-blank [`SIGNING_PROPERTIES`] of `props`.
fn signing_overrides(props: &HashMap<String, String>) -> HashMap<String, String> {
    SIGNING_PROPERTIES
        .iter()
        .filter_map(|key| Some((key.to_string(), non_blank(props, key)?)))
        .collect()
}

fn sensitive(props: HashMap<String, String>) -> HashMap<String, SensitiveString> {
    props.into_iter().map(|(k, v)| (k, v.into())).collect()
}

/// Trims a property, treating blank as absent.
fn non_blank(props: &HashMap<String, String>, key: &str) -> Option<String> {
    props
        .get(key)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// Builds a signer and static credentials from Java's `AwsProperties` names.
fn from_props(props: &HashMap<String, String>) -> Result<(SigV4Signer, SharedCredentialsProvider)> {
    let region = non_blank(props, REST_CATALOG_PROP_SIGNING_REGION).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("'{REST_CATALOG_PROP_SIGNING_REGION}' is required for SigV4 signing"),
        )
    })?;
    let name = non_blank(props, REST_CATALOG_PROP_SIGNING_NAME)
        .unwrap_or_else(|| SIGNING_NAME_DEFAULT.into());

    let credentials = credentials_from_props(props)?;
    Ok((
        SigV4Signer::builder()
            .region(region)
            .service(name)
            .mode(PayloadHashMode::IcebergRest)
            .build(),
        credentials,
    ))
}

/// Like Java, branches on the access key id alone; a lone secret is an error.
fn credentials_from_props(props: &HashMap<String, String>) -> Result<SharedCredentialsProvider> {
    let Some(access_key_id) = non_blank(props, REST_CATALOG_PROP_ACCESS_KEY_ID) else {
        if non_blank(props, REST_CATALOG_PROP_SECRET_ACCESS_KEY).is_some() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "'{REST_CATALOG_PROP_SECRET_ACCESS_KEY}' is set without '{REST_CATALOG_PROP_ACCESS_KEY_ID}'"
                ),
            ));
        }
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "no SigV4 credentials: set '{REST_CATALOG_PROP_ACCESS_KEY_ID}' and \
                 '{REST_CATALOG_PROP_SECRET_ACCESS_KEY}', or build the catalog with a \
                 `SigV4AuthManager` carrying your own credentials provider"
            ),
        ));
    };
    let secret_access_key = non_blank(props, REST_CATALOG_PROP_SECRET_ACCESS_KEY).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("'{REST_CATALOG_PROP_ACCESS_KEY_ID}' is set without '{REST_CATALOG_PROP_SECRET_ACCESS_KEY}'"),
        )
    })?;
    Ok(SharedCredentialsProvider::new(Credentials::new(
        access_key_id,
        secret_access_key,
        non_blank(props, REST_CATALOG_PROP_SESSION_TOKEN),
        None,
        "iceberg-rest-properties",
    )))
}

/// [`AuthManager`] that SigV4-signs every request on top of a delegate, as
/// Java's `RESTSigV4AuthManager` does. The delegate's `Authorization` moves to
/// `Original-Authorization` and is signed over.
///
/// Unlike Java, properties configure static credentials only; pass a provider
/// to [`Self::new`] for anything else. Context values override catalog ones,
/// blank context values are ignored, and context credentials replace the
/// catalog's as a set.
///
/// Configure signing-relevant headers as `header.*` properties: an injected
/// client's default headers are added after signing.
#[derive(Debug)]
pub struct SigV4AuthManager {
    delegate: Arc<dyn AuthManager>,
    signing: Signing,
    catalog: OnceLock<SigV4CatalogState>,
}

#[derive(Debug)]
enum Signing {
    Injected {
        signer: SigV4Signer,
        credentials: SharedCredentialsProvider,
    },
    /// The constructor's signing properties, which each session method's
    /// properties override, as Java rebuilds `AwsProperties` in each.
    FromProperties(HashMap<String, SensitiveString>),
}

#[derive(Debug)]
struct SigV4CatalogState {
    session: Arc<SigV4Session>,
    /// The signing properties the catalog session was built from.
    signing_properties: HashMap<String, SensitiveString>,
}

impl SigV4AuthManager {
    /// Signs with `signer` and `credentials`, whatever the properties say.
    pub fn new(
        delegate: Arc<dyn AuthManager>,
        signer: SigV4Signer,
        credentials: SharedCredentialsProvider,
    ) -> Self {
        Self {
            delegate,
            signing: Signing::Injected {
                signer,
                credentials,
            },
            catalog: OnceLock::new(),
        }
    }

    /// Signs as `props` describe. The properties each session method is given
    /// override them, so `/v1/config` overrides apply.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::DataInvalid`] if `props` lack a signing region or
    /// a complete static credential pair.
    pub fn from_properties(
        delegate: Arc<dyn AuthManager>,
        props: &HashMap<String, String>,
    ) -> Result<Self> {
        let base = signing_overrides(props);
        from_props(&base)?;
        Ok(Self {
            delegate,
            signing: Signing::FromProperties(sensitive(base)),
            catalog: OnceLock::new(),
        })
    }

    /// The signer and credentials for `props`, and the signing properties
    /// they came from (empty for an injected signer).
    fn signing_for(
        &self,
        props: &HashMap<String, String>,
    ) -> Result<(
        SigV4Signer,
        SharedCredentialsProvider,
        HashMap<String, SensitiveString>,
    )> {
        match &self.signing {
            Signing::Injected {
                signer,
                credentials,
            } => Ok((signer.clone(), credentials.clone(), HashMap::new())),
            Signing::FromProperties(base) => {
                let props = overlay_signing(base, signing_overrides(props));
                let (signer, credentials) = from_props(&props)?;
                Ok((signer, credentials, sensitive(props)))
            }
        }
    }
}

/// The non-blank [`SIGNING_PROPERTIES`] a context sets, its credentials over
/// its properties.
fn context_signing_overrides(context: &SessionContext) -> HashMap<String, String> {
    SIGNING_PROPERTIES
        .iter()
        .filter_map(|key| {
            let credential = context.credentials().get(*key).map(|v| v.expose());
            let property = context.properties().get(*key).map(String::as_str);
            let value = [credential, property]
                .into_iter()
                .flatten()
                .find(|v| !v.trim().is_empty())?;
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

#[async_trait]
impl AuthManager for SigV4AuthManager {
    async fn init_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Box<dyn AuthSession>> {
        let (signer, credentials, _) = self.signing_for(props)?;
        Ok(Box::new(SigV4Session {
            delegate: Arc::from(self.delegate.init_session(client, props).await?),
            signer,
            credentials,
        }))
    }

    async fn catalog_session(
        &self,
        client: &HttpClient,
        props: &HashMap<String, String>,
    ) -> Result<Arc<dyn AuthSession>> {
        let (signer, credentials, signing_properties) = self.signing_for(props)?;
        let session = Arc::new(SigV4Session {
            delegate: self.delegate.catalog_session(client, props).await?,
            signer,
            credentials,
        });
        self.catalog
            .set(SigV4CatalogState {
                session: session.clone(),
                signing_properties,
            })
            .map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "SigV4 catalog session already initialized",
                )
            })?;
        Ok(session)
    }

    async fn contextual_session(
        &self,
        context: &SessionContext,
        catalog_session: Arc<dyn AuthSession>,
    ) -> Result<Arc<dyn AuthSession>> {
        let catalog = self.catalog.get().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "SigV4 catalog session is not initialized",
            )
        })?;
        let parent: Arc<dyn AuthSession> = catalog.session.clone();
        if !Arc::ptr_eq(&parent, &catalog_session) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "unexpected SigV4 catalog session",
            ));
        }

        let delegate = self
            .delegate
            .contextual_session(context, catalog.session.delegate.clone())
            .await?;
        let overrides = match self.signing {
            Signing::FromProperties(_) => context_signing_overrides(context),
            Signing::Injected { .. } => HashMap::new(),
        };
        if overrides.is_empty() && Arc::ptr_eq(&delegate, &catalog.session.delegate) {
            return Ok(catalog_session);
        }
        let (signer, credentials) = if !overrides.is_empty() {
            from_props(&overlay_signing(&catalog.signing_properties, overrides))?
        } else {
            (
                catalog.session.signer.clone(),
                catalog.session.credentials.clone(),
            )
        };
        Ok(Arc::new(SigV4Session {
            delegate,
            signer,
            credentials,
        }))
    }

    fn signs_requests(&self) -> bool {
        true
    }
}

/// [`AuthSession`] applying the delegate's auth, then SigV4-signing.
#[derive(Debug)]
struct SigV4Session {
    delegate: Arc<dyn AuthSession>,
    signer: SigV4Signer,
    credentials: SharedCredentialsProvider,
}

#[async_trait]
impl AuthSession for SigV4Session {
    async fn authenticate(&self, request: &mut crate::HttpRequest) -> Result<()> {
        self.delegate.authenticate(request).await?;
        // Per request, as in Java; caching is the provider's job.
        let credentials = self.credentials.provide_credentials().await.map_err(|e| {
            Error::new(ErrorKind::Unexpected, "failed to resolve AWS credentials").with_source(e)
        })?;
        self.signer.sign(request, &credentials)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use chrono::TimeZone;

    use super::*;
    use crate::HttpRequest;

    const EMPTY_HEX: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    #[test]
    fn signing_rewrites_an_ambiguous_plus_out_of_the_query() {
        // reqwest writes a space as `+`; signing makes it `%20`.
        let mut request = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/namespaces")
                .query(&[("parent", "my ns")])
                .build()
                .unwrap(),
        );
        assert!(request.url().query().unwrap().contains("my+ns"));

        let signer = test_signer(PayloadHashMode::StandardAws);
        let now = test_time();
        signer
            .sign_at(&mut request, &test_credentials(), now)
            .unwrap();

        let query = request.url().query().unwrap();
        assert!(!query.contains('+'), "{query}");
        assert!(query.contains("my%20ns"), "{query}");
        assert_signature_is(
            &request,
            "b7bb5a323a1ce0ace18454171084deef2dac44c933c3949771fc70179d3cce2b",
        );
    }

    #[test]
    fn payload_hashes_iceberg_mode() {
        let v = payload_hashes(Some(b"hello"), PayloadHashMode::IcebergRest).0;
        assert_eq!(v, "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=");
        let e = payload_hashes(None, PayloadHashMode::IcebergRest).0;
        assert_eq!(e, EMPTY_HEX);
    }

    /// As in Java, an empty body is hashed, unlike an absent one.
    #[test]
    fn signing_separates_an_empty_body_from_an_absent_one() {
        let signer = test_signer(PayloadHashMode::IcebergRest);
        let now = test_time();
        let hash_of = |builder: reqwest::RequestBuilder| {
            let mut req = HttpRequest::new(builder.build().unwrap());
            signer.sign_at(&mut req, &test_credentials(), now).unwrap();
            req.headers()
                .get("x-amz-content-sha256")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        };

        let client = reqwest::Client::new();
        let url = "https://rest.example.com/v1/namespaces";
        assert_eq!(
            hash_of(client.post(url).body("")),
            "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        );
        assert_eq!(hash_of(client.post(url)), EMPTY_HEX);
    }

    #[test]
    fn payload_hashes_standard_mode() {
        let v = payload_hashes(Some(b"hello"), PayloadHashMode::StandardAws).0;
        assert_eq!(
            v,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    /// Pins a signature this crate produced; `signatures_match_iceberg_java`
    /// checks against Java.
    fn assert_signature_is(req: &HttpRequest, expected: &str) {
        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth.ends_with(&format!("Signature={expected}")), "{auth}");
    }

    fn test_signer(mode: PayloadHashMode) -> SigV4Signer {
        SigV4Signer::builder()
            .region("us-east-1")
            .service("execute-api")
            .mode(mode)
            .build()
    }

    fn test_credentials() -> Credentials {
        Credentials::new("ak", "sk", None::<String>, None, "test")
    }

    fn example_credentials(session_token: Option<&str>) -> Credentials {
        Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            session_token.map(str::to_string),
            None,
            "test",
        )
    }

    fn test_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2015, 8, 30, 12, 36, 0).unwrap()
    }

    fn signed_headers(req: &HttpRequest) -> Vec<String> {
        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        let signed = auth.split("SignedHeaders=").nth(1).unwrap();
        let signed = signed.split(',').next().unwrap();
        signed.split(';').map(str::to_string).collect()
    }

    /// Every header value as `(name, value)`, sorted.
    fn header_list(req: &HttpRequest) -> Vec<(String, String)> {
        let mut headers: Vec<_> = req
            .headers()
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_str().unwrap().to_string()))
            .collect();
        headers.sort();
        headers
    }

    fn sorted_pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut pairs: Vec<_> = pairs
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect();
        pairs.sort();
        pairs
    }

    /// Records every event field.
    #[derive(Clone, Default)]
    struct CapturedLog(Arc<Mutex<String>>);

    impl tracing::field::Visit for CapturedLog {
        fn record_debug(&mut self, _: &tracing::field::Field, value: &dyn std::fmt::Debug) {
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

    /// `aws_sigv4` does not redact `Original-Authorization` itself.
    #[test]
    fn signing_does_not_trace_a_relocated_bearer_token() {
        const TOKEN: &str = "Bearer topsecretdelegatetoken";
        let signer = test_signer(PayloadHashMode::IcebergRest);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header(reqwest::header::AUTHORIZATION, TOKEN)
                .build()
                .unwrap(),
        );

        let log = CapturedLog::default();
        tracing::subscriber::with_default(log.clone(), || {
            // `sign_at` mutes only at this level, which is what is under test.
            assert_eq!(
                tracing::level_filters::LevelFilter::current(),
                tracing::level_filters::LevelFilter::TRACE
            );
            signer
                .sign_at(&mut req, &test_credentials(), test_time())
                .unwrap();
            tracing::trace!(canary = "subscriber-is-live");
        });

        let captured = log.0.lock().unwrap().clone();
        assert!(captured.contains("subscriber-is-live"), "captured nothing");
        assert!(!captured.contains(TOKEN), "{captured}");
        assert_eq!(req.headers().get(RELOCATED_AUTHORIZATION).unwrap(), TOKEN);
    }

    #[test]
    fn a_non_utf8_header_is_rejected_without_changing_the_request() {
        let signer = test_signer(PayloadHashMode::IcebergRest);
        for name in ["x-amz-meta-tenant", "authorization", "x-amz-content-sha256"] {
            let mut req = HttpRequest::new(reqwest::Request::new(
                reqwest::Method::GET,
                "https://user:pw@rest.example.com/v1/config?warehouse=my+catalog"
                    .parse()
                    .unwrap(),
            ));
            req.headers_mut()
                .insert("authorization", "Bearer delegate-token".parse().unwrap());
            req.headers_mut()
                .insert("x-amz-content-sha256", "caller-hash".parse().unwrap());
            req.headers_mut().insert(
                name,
                reqwest::header::HeaderValue::from_bytes(b"acme\xfa").unwrap(),
            );
            let headers = req.headers().clone();
            let url = req.url().clone();

            let err = signer
                .sign_at(&mut req, &test_credentials(), test_time())
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(err.message().contains(name), "{err}");
            assert_eq!(req.headers(), &headers, "{name}");
            assert_eq!(req.url(), &url, "{name}");
        }
    }

    #[test]
    fn a_non_ascii_utf8_header_is_signed() {
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(reqwest::Request::new(
            reqwest::Method::GET,
            "https://rest.example.com/v1/config".parse().unwrap(),
        ));
        req.headers_mut().insert(
            "x-tenant",
            reqwest::header::HeaderValue::from_bytes("Zürich".as_bytes()).unwrap(),
        );

        signer
            .sign_at(&mut req, &test_credentials(), test_time())
            .unwrap();

        assert_eq!(signed_headers(&req), [
            "host",
            "x-amz-content-sha256",
            "x-amz-date",
            "x-tenant"
        ]);
    }

    #[test]
    fn a_signing_failure_after_relocation_restores_the_request() {
        let signer = test_signer(PayloadHashMode::IcebergRest);
        // Longer than `http::Uri` accepts, so signing fails after the headers
        // and query were rewritten.
        let url = format!(
            "https://rest.example.com/v1/config?warehouse=my+catalog&pad={}",
            "a".repeat(70_000)
        );
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get(url)
                .header("authorization", "Bearer delegate-token")
                .header("x-amz-content-sha256", "caller-hash")
                .build()
                .unwrap(),
        );
        let headers = req.headers().clone();
        let url = req.url().clone();

        let err = signer
            .sign_at(&mut req, &test_credentials(), test_time())
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid, "{err}");
        assert_eq!(req.headers(), &headers);
        assert_eq!(req.url(), &url);
    }

    /// Relocation appends to a caller's `Original-x-amz-*` after signing, so it
    /// must stay unsigned.
    #[test]
    fn a_caller_supplied_relocation_header_is_not_signed() {
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header("x-amz-content-sha256", "caller-hash")
                .header("original-x-amz-content-sha256", "previous")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(&mut req, &test_credentials(), test_time())
            .unwrap();

        assert_eq!(signed_headers(&req), [
            "host",
            "x-amz-content-sha256",
            "x-amz-date"
        ]);
        let relocated: Vec<_> = req
            .headers()
            .get_all("original-x-amz-content-sha256")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(relocated, ["previous", "caller-hash"]);
    }

    #[test]
    fn userinfo_is_stripped_before_signing() {
        // A hand-built request can carry userinfo; the wire `Host` never does.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(reqwest::Request::new(
            reqwest::Method::GET,
            "https://user:pw@rest.example.com/v1/config"
                .parse()
                .unwrap(),
        ));
        assert_eq!(req.url().username(), "user");
        let now = test_time();

        signer.sign_at(&mut req, &test_credentials(), now).unwrap();

        assert_eq!(req.url().username(), "");
        assert_eq!(req.url().password(), None);
        let mut plain = HttpRequest::new(reqwest::Request::new(
            reqwest::Method::GET,
            "https://rest.example.com/v1/config".parse().unwrap(),
        ));
        signer
            .sign_at(&mut plain, &test_credentials(), now)
            .unwrap();
        assert_eq!(
            req.headers().get("authorization"),
            plain.headers().get("authorization"),
        );
    }

    #[test]
    fn a_doubled_slash_in_the_path_is_normalized() {
        // A trailing slash on the catalog URI gives `//v1/...`, which
        // `Aws4Signer` collapses.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(reqwest::Request::new(
            reqwest::Method::GET,
            "https://rest.example.com//v1//config".parse().unwrap(),
        ));
        let now = test_time();

        signer.sign_at(&mut req, &test_credentials(), now).unwrap();

        let mut plain = HttpRequest::new(reqwest::Request::new(
            reqwest::Method::GET,
            "https://rest.example.com/v1/config".parse().unwrap(),
        ));
        signer
            .sign_at(&mut plain, &test_credentials(), now)
            .unwrap();
        assert_eq!(
            req.headers().get("authorization"),
            plain.headers().get("authorization"),
        );
    }

    #[test]
    fn caller_headers_the_signer_overwrites_are_relocated() {
        // As in Java, conflicting caller values move to `Original-<name>`.
        let creds = Credentials::new(
            "ak".to_string(),
            "sk".to_string(),
            Some("signer-token".to_string()),
            None,
            "test",
        );
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header("authorization", "Bearer caller-token")
                .header("x-amz-date", "19700101T000000Z")
                .header("x-amz-security-token", "caller-session")
                .header("x-amz-content-sha256", "caller-hash")
                .build()
                .unwrap(),
        );

        signer.sign_at(&mut req, &creds, test_time()).unwrap();

        assert_eq!(
            header_list(&req),
            sorted_pairs(&[
                (
                    "authorization",
                    "AWS4-HMAC-SHA256 Credential=ak/20150830/us-east-1/execute-api/aws4_request, \
                     SignedHeaders=host;original-authorization;x-amz-content-sha256;x-amz-date;\
                     x-amz-security-token, \
                     Signature=87560cb735277b284a547d63f57cfe3a721f9bce0b08a9402a58d754d2ed707c"
                ),
                ("original-authorization", "Bearer caller-token"),
                ("original-x-amz-content-sha256", "caller-hash"),
                ("original-x-amz-date", "19700101T000000Z"),
                ("original-x-amz-security-token", "caller-session"),
                ("x-amz-content-sha256", EMPTY_HEX),
                ("x-amz-date", "20150830T123600Z"),
                ("x-amz-security-token", "signer-token"),
            ])
        );
        // Relocated originals may be credentials themselves.
        for relocated in [RELOCATED_SECURITY_TOKEN, RELOCATED_CONTENT_SHA256] {
            assert!(req.headers().get(relocated).unwrap().is_sensitive());
        }
    }

    #[test]
    fn an_existing_authorization_is_never_signed() {
        // The signer replaces `authorization`, and a proxy may rewrite
        // `user-agent`, so neither is signed.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header("authorization", "Bearer caller-token")
                .header("user-agent", "example/1.0")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(&mut req, &test_credentials(), test_time())
            .unwrap();

        assert_eq!(signed_headers(&req), [
            "host",
            "original-authorization",
            "x-amz-content-sha256",
            "x-amz-date"
        ]);
    }

    /// As in Java, every `Authorization` value is relocated, and stays redacted.
    #[test]
    fn every_repeated_authorization_is_relocated_and_kept_sensitive() {
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header("authorization", "Bearer first")
                .header("authorization", "Bearer second")
                .build()
                .unwrap(),
        );

        signer
            .sign_at(&mut req, &test_credentials(), test_time())
            .unwrap();

        let relocated: Vec<_> = req
            .headers()
            .get_all(RELOCATED_AUTHORIZATION)
            .iter()
            .collect();
        assert_eq!(relocated.len(), 2, "{relocated:?}");
        assert_eq!(relocated[0], "Bearer first");
        assert_eq!(relocated[1], "Bearer second");
        assert!(relocated.iter().all(|v| v.is_sensitive()), "{relocated:?}");
    }

    #[test]
    fn hop_by_hop_headers_are_not_signed() {
        // A proxy may drop or rewrite these, so they are not signed.
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .header("expect", "100-continue")
                .header("connection", "keep-alive")
                .header("x-forwarded-for", "203.0.113.7")
                .header("x-tenant", "acme")
                .build()
                .unwrap(),
        );
        let now = test_time();

        signer.sign_at(&mut req, &test_credentials(), now).unwrap();

        assert_eq!(signed_headers(&req), [
            "host",
            "x-amz-content-sha256",
            "x-amz-date",
            "x-tenant"
        ]);
        assert_signature_is(
            &req,
            "f938221412ed6b55cf3db380ce6ded476419ad3d7db4c76d932031c98465ce79",
        );
    }

    #[test]
    fn signed_credentials_are_marked_sensitive() {
        // Both carry a credential, so `Debug` must not print them.
        let creds = Credentials::new(
            "ak".to_string(),
            "sk".to_string(),
            Some("session-token".to_string()),
            None,
            "test",
        );
        let signer = test_signer(PayloadHashMode::StandardAws);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .build()
                .unwrap(),
        );

        signer.sign_at(&mut req, &creds, test_time()).unwrap();

        assert!(req.headers().get("authorization").unwrap().is_sensitive());
        assert!(
            req.headers()
                .get("x-amz-security-token")
                .unwrap()
                .is_sensitive()
        );
        let debug = format!("{:?}", req.headers());
        assert!(!debug.contains("session-token"), "{debug}");
    }

    #[test]
    fn signs_with_a_non_default_service_and_session_token() {
        // As a non-AWS catalog might vend: its own signing name, and STS
        // credentials.
        let creds = Credentials::new(
            "STS.EXAMPLEACCESSKEYID",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("example-session-token".to_string()),
            None,
            "test",
        );
        let signer = SigV4Signer::builder()
            .region("us-east-1")
            .service("custom-service")
            .mode(PayloadHashMode::IcebergRest)
            .build();
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://catalog.example.com/v1/config?warehouse=my-catalog")
                .build()
                .unwrap(),
        );
        let now = Utc.with_ymd_and_hms(2026, 8, 26, 12, 0, 0).unwrap();

        signer.sign_at(&mut req, &creds, now).unwrap();

        assert_signature_is(
            &req,
            "6b7065e5f44da4f5c3654126b8d5fe29599905afb6b98f4de65b6ec6e1be783f",
        );
        let auth = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            auth.contains("/us-east-1/custom-service/aws4_request"),
            "{auth}"
        );
        assert_eq!(
            req.headers().get("x-amz-security-token").unwrap(),
            "example-session-token"
        );
    }

    #[test]
    fn signs_request_iceberg_mode() {
        let creds = example_credentials(Some("SESSIONTOKEN"));
        let signer = SigV4Signer::builder()
            .region("us-east-1")
            .service("glue")
            .mode(PayloadHashMode::IcebergRest)
            .build();
        let client = reqwest::Client::new();
        let mut req = HttpRequest::new(
            client
                .post("https://rest.example.com/v1/namespaces")
                .body("{}")
                .build()
                .unwrap(),
        );

        signer.sign_at(&mut req, &creds, test_time()).unwrap();

        assert_eq!(
            header_list(&req),
            sorted_pairs(&[
                (
                    "authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/glue/aws4_request, \
                     SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, \
                     Signature=effad6acde583dd14ba7aff52b2b83776a54421c010fe82067f166819057cb32"
                ),
                (
                    "x-amz-content-sha256",
                    "RBNvo1WzZ4oRRq0W9+hknpT7T8If536DEMBg9hyq/4o="
                ),
                ("x-amz-date", "20150830T123600Z"),
                ("x-amz-security-token", "SESSIONTOKEN"),
            ])
        );
    }

    /// An empty body hashes to the hex constant, and caller headers are signed
    /// (Java's `authenticateWithoutBody`).
    #[test]
    fn signs_empty_body_and_all_headers() {
        let creds = example_credentials(None);
        let signer = SigV4Signer::builder()
            .region("us-east-1")
            .service("glue")
            .mode(PayloadHashMode::IcebergRest)
            .build();
        let client = reqwest::Client::new();
        let mut req = HttpRequest::new(
            client
                .get("https://rest.example.com/v1/config")
                .header("content-type", "application/json")
                .header("content-encoding", "gzip")
                .build()
                .unwrap(),
        );

        signer.sign_at(&mut req, &creds, test_time()).unwrap();

        assert_eq!(
            header_list(&req),
            sorted_pairs(&[
                (
                    "authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/glue/aws4_request, \
                     SignedHeaders=content-encoding;content-type;host;x-amz-content-sha256;x-amz-date, \
                     Signature=eaef7eb88d9cd810031684748671d8a3c9394ea5168622a212ed041b670b9777"
                ),
                ("content-encoding", "gzip"),
                ("content-type", "application/json"),
                ("x-amz-content-sha256", EMPTY_HEX),
                ("x-amz-date", "20150830T123600Z"),
            ])
        );
    }

    #[test]
    fn iceberg_mode_signs_the_hex_payload_hash_not_the_base64_header() {
        // Needs a body: without one the header is the hex constant too.
        let creds = example_credentials(None);
        let signer = test_signer(PayloadHashMode::IcebergRest);
        let body = br#"{"namespace":["ns"]}"#;
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .post("https://rest.example.com/v1/namespaces")
                .body(body.to_vec())
                .build()
                .unwrap(),
        );
        let now = test_time();

        signer.sign_at(&mut req, &creds, now).unwrap();

        assert_eq!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            payload_hashes(Some(body), PayloadHashMode::IcebergRest)
                .0
                .as_str()
        );
        assert_ne!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            encode_hex(&Sha256::digest(body)).as_str()
        );
        assert_signature_is(
            &req,
            "c68682c26cab6a781256f83b0076f50014f4922c3907f4ff09c204a74d61fc1d",
        );
    }

    /// What Iceberg Java's `RESTSigV4AuthSession` (iceberg-aws 1.10.1) sent for
    /// these requests, given these credentials as `rest.*` properties.
    #[test]
    fn signatures_match_iceberg_java() {
        let creds = example_credentials(Some("example-session-token"));
        let signer = test_signer(PayloadHashMode::IcebergRest);
        let now = Utc.with_ymd_and_hms(2026, 10, 2, 11, 40, 30).unwrap();
        let client = reqwest::Client::new();

        // A body, so the header carries base64, and a token to relocate.
        let mut post = HttpRequest::new(
            client
                .post("https://rest.example.com/v1/namespaces")
                .header("content-type", "application/json")
                .header("authorization", "Bearer delegate-token")
                .body(r#"{"namespace":["a","b"],"properties":{}}"#)
                .build()
                .unwrap(),
        );
        signer.sign_at(&mut post, &creds, now).unwrap();
        assert_eq!(
            post.headers().get("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261002/us-east-1/execute-api/aws4_request, \
             SignedHeaders=content-type;host;original-authorization;\
             x-amz-content-sha256;x-amz-date;x-amz-security-token, \
             Signature=c5d6bfcecd19c5421e8696c67465f6909b5e09398608d8e79a9eb391e04de556"
        );
        assert_eq!(
            post.headers().get("x-amz-content-sha256").unwrap(),
            "n1CB8Yl4L77sBwHTc5qTpUEhlsHHOXFzg0dV0fbOwXs="
        );

        // No body, a multi-level namespace and an encoded query.
        let mut get = HttpRequest::new(
            client
                .get("https://rest.example.com/v1/namespaces/a%1Fb/tables/x,y?pageToken=a%20b%2Fc")
                .build()
                .unwrap(),
        );
        signer.sign_at(&mut get, &creds, now).unwrap();
        assert_eq!(
            get.headers().get("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261002/us-east-1/execute-api/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, \
             Signature=5f6832ec82fc821b08cd3eac3a86ffc3e0c333571fd0a5796e43f2534f23329f"
        );
    }

    /// The signed `host` keeps a non-default port, as on the wire.
    #[test]
    fn signs_host_with_non_default_port() {
        let creds = example_credentials(None);
        let signer = SigV4Signer::builder()
            .region("us-east-1")
            .service("glue")
            .mode(PayloadHashMode::IcebergRest)
            .build();
        let client = reqwest::Client::new();
        let mut req = HttpRequest::new(
            client
                .get("https://rest.example.com:8181/v1/config")
                .build()
                .unwrap(),
        );
        let now = test_time();

        signer.sign_at(&mut req, &creds, now).unwrap();
        assert_signature_is(
            &req,
            "f7801a4ecac5fe6dcc4ee385223428ec4833f21d0c2fc10d2fb00694b2c0def7",
        );
    }

    /// Like `Aws4Signer`, the canonical path is encoded again: `,` becomes `%2C`
    /// and `%2C` becomes `%252C`.
    #[test]
    fn canonical_uri_is_aws_double_encoded() {
        let creds = example_credentials(None);
        let signer = SigV4Signer::builder()
            .region("us-east-1")
            .service("glue")
            .mode(PayloadHashMode::IcebergRest)
            .build();
        let client = reqwest::Client::new();
        let mut req = HttpRequest::new(
            client
                .get("https://rest.example.com/v1/namespaces/a%2Cb/tables/x,y")
                .build()
                .unwrap(),
        );
        let now = test_time();

        signer.sign_at(&mut req, &creds, now).unwrap();
        assert_signature_is(
            &req,
            "4d966b6fc2dfb62be5a603e4e07e5dc85b2af1c7e6a181c5646bfbf38ddfa543",
        );
    }

    /// The AWS SigV4 test suite's `post-x-www-form-urlencoded`, which signs a
    /// hex `x-amz-content-sha256` like this mode.
    #[test]
    fn standard_mode_matches_the_aws_test_suite() {
        let creds = example_credentials(None);
        let signer = SigV4Signer::builder()
            .region("us-east-1")
            .service("service")
            .mode(PayloadHashMode::StandardAws)
            .build();
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .post("https://example.amazonaws.com/")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("content-length", "13")
                .body("Param1=value1")
                .build()
                .unwrap(),
        );

        signer.sign_at(&mut req, &creds, test_time()).unwrap();

        assert_eq!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            "9095672bbd1f56dfc5b65f3e153adc8731a4a654192329106275f4c7b24d0b6e"
        );
        assert_eq!(
            req.headers().get("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=content-length;content-type;host;x-amz-content-sha256;x-amz-date, \
             Signature=d3875051da38690788ef43de4db0d8f280229d82040bfac253562e56c3f20e0b"
        );
    }

    /// The one test on the live clock: everything else pins the time.
    #[test]
    fn sign_stamps_the_current_time() {
        use chrono::SubsecRound;

        let signer = test_signer(PayloadHashMode::IcebergRest);
        let mut req = HttpRequest::new(
            reqwest::Client::new()
                .get("https://rest.example.com/v1/config")
                .build()
                .unwrap(),
        );
        let before = Utc::now();
        signer.sign(&mut req, &test_credentials()).unwrap();
        let after = Utc::now();

        let date = req.headers().get("x-amz-date").unwrap().to_str().unwrap();
        let stamped = chrono::NaiveDateTime::parse_from_str(date, "%Y%m%dT%H%M%SZ")
            .unwrap()
            .and_utc();
        assert_eq!(date.len(), "YYYYMMDDTHHMMSSZ".len(), "{date}");
        // The header drops sub-seconds, so compare against a truncated start.
        assert!(
            before.trunc_subsecs(0) <= stamped && stamped <= after,
            "{date}"
        );
    }

    fn test_credentials_provider() -> SharedCredentialsProvider {
        SharedCredentialsProvider::new(test_credentials())
    }

    fn test_session() -> SigV4Session {
        SigV4Session {
            delegate: Arc::new(crate::auth::NoopSession),
            signer: test_signer(PayloadHashMode::IcebergRest),
            credentials: test_credentials_provider(),
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
        let mut request = request_with(&[
            ("original-authorization", "credential-A"),
            ("authorization", "credential-B"),
        ]);

        test_session().authenticate(&mut request).await.unwrap();

        let relocated: Vec<_> = request
            .headers()
            .get_all("original-authorization")
            .iter()
            .map(|v| v.to_str().unwrap().to_string())
            .collect();
        assert_eq!(relocated, ["credential-A", "credential-B"]);
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
    async fn credentials_are_resolved_once_per_request() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let counted = {
            let calls = calls.clone();
            aws_credential_types::credential_fn::provide_credentials_fn(move || {
                let calls = calls.clone();
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Credentials::new(
                        format!("AKID{n}"),
                        "secret",
                        None::<String>,
                        None,
                        "test",
                    ))
                }
            })
        };
        let session = SigV4Session {
            delegate: Arc::new(crate::auth::NoopSession),
            signer: test_signer(PayloadHashMode::IcebergRest),
            credentials: SharedCredentialsProvider::new(counted),
        };

        let mut first = request_with(&[]);
        session.authenticate(&mut first).await.unwrap();
        let mut second = request_with(&[]);
        session.authenticate(&mut second).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let auth = |r: &HttpRequest| {
            r.headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        };
        assert!(auth(&first).contains("AKID0/"), "{}", auth(&first));
        assert!(auth(&second).contains("AKID1/"), "{}", auth(&second));
    }

    #[tokio::test]
    async fn the_delegate_authenticates_before_signing() {
        #[derive(Debug)]
        struct Bearer;
        #[async_trait]
        impl AuthSession for Bearer {
            async fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
                request
                    .headers_mut()
                    .insert("authorization", "Bearer delegate".parse().unwrap());
                Ok(())
            }
        }

        let session = SigV4Session {
            delegate: Arc::new(Bearer),
            signer: test_signer(PayloadHashMode::IcebergRest),
            credentials: test_credentials_provider(),
        };
        let mut request = request_with(&[]);
        session.authenticate(&mut request).await.unwrap();

        assert_eq!(
            request.headers().get("original-authorization").unwrap(),
            "Bearer delegate"
        );
        let auth = request.headers().get("authorization").unwrap();
        assert!(auth.to_str().unwrap().contains("original-authorization"));
    }

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn static_credentials_come_from_properties() {
        let (signer, credentials) = from_props(&props(&[
            (REST_CATALOG_PROP_SIGNING_REGION, "cn-hangzhou"),
            (REST_CATALOG_PROP_ACCESS_KEY_ID, "AKID"),
            (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "secret"),
            (REST_CATALOG_PROP_SESSION_TOKEN, "token"),
        ]))
        .unwrap();

        assert_eq!(signer.region, "cn-hangzhou");
        assert_eq!(signer.service, SIGNING_NAME_DEFAULT);
        assert_eq!(signer.mode, PayloadHashMode::IcebergRest);
        let resolved = credentials.provide_credentials().await.unwrap();
        assert_eq!(resolved.access_key_id(), "AKID");
        assert_eq!(resolved.session_token(), Some("token"));
    }

    #[tokio::test]
    async fn a_blank_property_counts_as_absent() {
        let err = from_props(&props(&[(REST_CATALOG_PROP_SIGNING_REGION, "   ")])).unwrap_err();
        assert!(
            err.message().contains(REST_CATALOG_PROP_SIGNING_REGION),
            "{err}"
        );
    }

    #[tokio::test]
    async fn half_a_credential_pair_is_rejected_either_way() {
        for (present, missing) in [
            (
                REST_CATALOG_PROP_ACCESS_KEY_ID,
                REST_CATALOG_PROP_SECRET_ACCESS_KEY,
            ),
            (
                REST_CATALOG_PROP_SECRET_ACCESS_KEY,
                REST_CATALOG_PROP_ACCESS_KEY_ID,
            ),
        ] {
            let err = from_props(&props(&[
                (REST_CATALOG_PROP_SIGNING_REGION, "us-east-1"),
                (present, "value"),
            ]))
            .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::DataInvalid, "{err}");
            let message = err.message();
            assert!(message.contains(present), "{err}");
            assert!(message.contains(missing), "{err}");
            assert!(!message.contains("credentials provider"), "{err}");
        }
    }

    #[tokio::test]
    async fn without_credentials_the_error_points_at_the_provider() {
        let err =
            from_props(&props(&[(REST_CATALOG_PROP_SIGNING_REGION, "us-east-1")])).unwrap_err();
        assert!(err.message().contains("credentials provider"), "{err}");
    }

    #[tokio::test]
    async fn the_signing_name_property_overrides_the_default() {
        let (signer, _) = from_props(&props(&[
            (REST_CATALOG_PROP_SIGNING_REGION, "us-east-1"),
            (REST_CATALOG_PROP_SIGNING_NAME, "glue"),
            (REST_CATALOG_PROP_ACCESS_KEY_ID, "AKID"),
            (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "secret"),
        ]))
        .unwrap();
        assert_eq!(signer.service, "glue");
        assert_ne!(signer.service, SIGNING_NAME_DEFAULT);
    }

    #[tokio::test]
    async fn a_padded_property_is_trimmed_not_just_accepted() {
        let (signer, credentials) = from_props(&props(&[
            (REST_CATALOG_PROP_SIGNING_REGION, "  us-east-1  "),
            (REST_CATALOG_PROP_ACCESS_KEY_ID, "  AKID  "),
            (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "secret"),
        ]))
        .unwrap();
        assert_eq!(signer.region, "us-east-1");
        assert_eq!(
            credentials
                .provide_credentials()
                .await
                .unwrap()
                .access_key_id(),
            "AKID"
        );
    }

    #[tokio::test]
    async fn a_failing_delegate_stops_the_request_unsigned() {
        #[derive(Debug)]
        struct Failing;
        #[async_trait]
        impl AuthSession for Failing {
            async fn authenticate(&self, _: &mut HttpRequest) -> Result<()> {
                Err(Error::new(ErrorKind::Unexpected, "token refresh failed"))
            }
        }

        let session = SigV4Session {
            delegate: Arc::new(Failing),
            signer: test_signer(PayloadHashMode::IcebergRest),
            credentials: test_credentials_provider(),
        };
        let mut request = request_with(&[]);
        let err = session.authenticate(&mut request).await.unwrap_err();

        assert!(err.message().contains("token refresh failed"), "{err}");
        assert!(request.headers().get("authorization").is_none());
    }

    /// Records which delegate method ran and with which properties.
    #[derive(Debug, Default)]
    struct RecordingManager {
        calls: Mutex<Vec<(&'static str, HashMap<String, String>)>>,
    }

    #[async_trait]
    impl AuthManager for RecordingManager {
        async fn init_session(
            &self,
            _: &HttpClient,
            props: &HashMap<String, String>,
        ) -> Result<Box<dyn AuthSession>> {
            self.calls.lock().unwrap().push(("init", props.clone()));
            Ok(Box::new(crate::auth::NoopSession))
        }

        async fn catalog_session(
            &self,
            _: &HttpClient,
            props: &HashMap<String, String>,
        ) -> Result<Arc<dyn AuthSession>> {
            self.calls.lock().unwrap().push(("catalog", props.clone()));
            Ok(Arc::new(crate::auth::NoopSession))
        }
    }

    fn test_client() -> HttpClient {
        HttpClient::new(
            &crate::RestCatalogConfig::builder()
                .uri("http://localhost".to_string())
                .build(),
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct ContextDelegate {
        parent: Arc<dyn AuthSession>,
    }

    #[derive(Debug)]
    struct ContextBearer(String);

    #[async_trait]
    impl AuthSession for ContextBearer {
        async fn authenticate(&self, request: &mut HttpRequest) -> Result<()> {
            request.headers_mut().insert(
                "authorization",
                format!("Bearer {}", self.0).parse().unwrap(),
            );
            Ok(())
        }
    }

    #[async_trait]
    impl AuthManager for ContextDelegate {
        async fn init_session(
            &self,
            _: &HttpClient,
            _: &HashMap<String, String>,
        ) -> Result<Box<dyn AuthSession>> {
            Ok(Box::new(crate::auth::NoopSession))
        }

        async fn catalog_session(
            &self,
            _: &HttpClient,
            _: &HashMap<String, String>,
        ) -> Result<Arc<dyn AuthSession>> {
            Ok(self.parent.clone())
        }

        async fn contextual_session(
            &self,
            context: &SessionContext,
            parent: Arc<dyn AuthSession>,
        ) -> Result<Arc<dyn AuthSession>> {
            assert!(Arc::ptr_eq(&parent, &self.parent));
            match context.identity() {
                Some("fail") => Err(Error::new(
                    ErrorKind::DataInvalid,
                    "delegate context failed",
                )),
                Some(identity) => Ok(Arc::new(ContextBearer(identity.to_string()))),
                None => Ok(parent),
            }
        }
    }

    #[tokio::test]
    async fn contextual_sigv4_sessions_use_the_delegate_and_context_credentials() {
        let base = props(&[
            (REST_CATALOG_PROP_SIGNING_REGION, "us-east-1"),
            (REST_CATALOG_PROP_ACCESS_KEY_ID, "CATALOG"),
            (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "catalog-secret"),
        ]);
        let manager = SigV4AuthManager::from_properties(
            Arc::new(ContextDelegate {
                parent: Arc::new(crate::auth::NoopSession),
            }),
            &base,
        )
        .unwrap();
        let parent = manager
            .catalog_session(&test_client(), &base)
            .await
            .unwrap();
        let context = SessionContext::builder()
            .identity("alice".to_string())
            .properties(props(&[
                (REST_CATALOG_PROP_SIGNING_REGION, "eu-west-1"),
                (REST_CATALOG_PROP_ACCESS_KEY_ID, "PROPERTY-KEY"),
            ]))
            .credentials(
                props(&[
                    (REST_CATALOG_PROP_ACCESS_KEY_ID, "CONTEXT-KEY"),
                    (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "context-secret"),
                    (REST_CATALOG_PROP_SESSION_TOKEN, "context-token"),
                ])
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            )
            .build();
        let session = manager
            .contextual_session(&context, parent.clone())
            .await
            .unwrap();
        let mut request = request_with(&[]);
        session.authenticate(&mut request).await.unwrap();
        let authorization = request
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            authorization.contains("Credential=CONTEXT-KEY/"),
            "{authorization}"
        );
        assert!(
            authorization.contains("/eu-west-1/execute-api/"),
            "{authorization}"
        );
        assert_eq!(
            request.headers().get("original-authorization").unwrap(),
            "Bearer alice"
        );
        assert_eq!(
            request.headers().get("x-amz-security-token").unwrap(),
            "context-token"
        );

        let date = request
            .headers()
            .get("x-amz-date")
            .unwrap()
            .to_str()
            .unwrap();
        let now = chrono::NaiveDateTime::parse_from_str(date, "%Y%m%dT%H%M%SZ")
            .unwrap()
            .and_utc();
        let mut expected = request_with(&[("authorization", "Bearer alice")]);
        SigV4Signer::builder()
            .region("eu-west-1")
            .service("execute-api")
            .mode(PayloadHashMode::IcebergRest)
            .build()
            .sign_at(
                &mut expected,
                &Credentials::new(
                    "CONTEXT-KEY",
                    "context-secret",
                    Some("context-token".into()),
                    None,
                    "test",
                ),
                now,
            )
            .unwrap();
        assert_eq!(
            request.headers().get("authorization"),
            expected.headers().get("authorization")
        );

        let unchanged = manager
            .contextual_session(&SessionContext::empty(), parent.clone())
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&parent, &unchanged));
        assert!(scope_of(parent.as_ref()).await.starts_with("CATALOG/"));

        let failed = SessionContext::builder().identity("fail".into()).build();
        let err = manager
            .contextual_session(&failed, parent)
            .await
            .unwrap_err();
        assert_eq!(err.message(), "delegate context failed");
        assert!(!format!("{manager:?}").contains("catalog-secret"));
    }

    #[tokio::test]
    async fn context_credentials_replace_the_catalog_credentials_as_a_set() {
        let base = props(&[
            (REST_CATALOG_PROP_SIGNING_REGION, "us-east-1"),
            (REST_CATALOG_PROP_ACCESS_KEY_ID, "CATALOG"),
            (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "catalog-secret"),
            (REST_CATALOG_PROP_SESSION_TOKEN, "catalog-token"),
        ]);
        let manager =
            SigV4AuthManager::from_properties(Arc::new(RecordingManager::default()), &base)
                .unwrap();
        let parent = manager
            .catalog_session(&test_client(), &base)
            .await
            .unwrap();
        let context = SessionContext::builder()
            .properties(props(&[(REST_CATALOG_PROP_SIGNING_REGION, " ")]))
            .credentials(
                props(&[
                    (REST_CATALOG_PROP_ACCESS_KEY_ID, "CONTEXT"),
                    (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "context-secret"),
                ])
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            )
            .build();

        let session = manager.contextual_session(&context, parent).await.unwrap();

        let mut request = request_with(&[]);
        session.authenticate(&mut request).await.unwrap();
        let mut expected = request_with(&[]);
        let date = request
            .headers()
            .get("x-amz-date")
            .unwrap()
            .to_str()
            .unwrap();
        let now = chrono::NaiveDateTime::parse_from_str(date, "%Y%m%dT%H%M%SZ")
            .unwrap()
            .and_utc();
        test_signer(PayloadHashMode::IcebergRest)
            .sign_at(
                &mut expected,
                &Credentials::new("CONTEXT", "context-secret", None, None, "test"),
                now,
            )
            .unwrap();
        assert_eq!(header_list(&request), header_list(&expected));
    }

    #[tokio::test]
    async fn context_session_token_alone_does_not_reuse_catalog_keys() {
        let base = props(&[
            (REST_CATALOG_PROP_SIGNING_REGION, "us-east-1"),
            (REST_CATALOG_PROP_ACCESS_KEY_ID, "CATALOG"),
            (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "catalog-secret"),
        ]);
        let manager =
            SigV4AuthManager::from_properties(Arc::new(RecordingManager::default()), &base)
                .unwrap();
        let parent = manager
            .catalog_session(&test_client(), &base)
            .await
            .unwrap();
        let context = SessionContext::builder()
            .credentials(
                props(&[(REST_CATALOG_PROP_SESSION_TOKEN, "context-token")])
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            )
            .build();

        let err = manager
            .contextual_session(&context, parent)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid, "{err}");
        assert!(err.message().contains("credentials provider"), "{err}");
    }

    #[tokio::test]
    async fn the_manager_forwards_to_the_matching_delegate_method() {
        let delegate = Arc::new(RecordingManager::default());
        let manager = SigV4AuthManager::new(
            delegate.clone(),
            test_signer(PayloadHashMode::IcebergRest),
            test_credentials_provider(),
        );
        let init_props = props(&[("a", "1")]);
        let catalog_props = props(&[("b", "2")]);

        manager
            .init_session(&test_client(), &init_props)
            .await
            .unwrap();
        manager
            .catalog_session(&test_client(), &catalog_props)
            .await
            .unwrap();

        let calls = delegate.calls.lock().unwrap().clone();
        assert_eq!(calls, vec![
            ("init", init_props),
            ("catalog", catalog_props)
        ]);
    }

    /// The credential scope a session signs with.
    async fn scope_of(session: &dyn AuthSession) -> String {
        let mut request = request_with(&[]);
        session.authenticate(&mut request).await.unwrap();
        let auth = request
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let scope = auth.split("Credential=").nth(1).unwrap();
        scope.split(',').next().unwrap().to_string()
    }

    #[tokio::test]
    async fn a_property_built_manager_rebuilds_from_the_merged_properties() {
        let base = props(&[
            (REST_CATALOG_PROP_SIGNING_REGION, "us-east-1"),
            (REST_CATALOG_PROP_ACCESS_KEY_ID, "AKID"),
            (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "secret"),
        ]);
        let manager =
            SigV4AuthManager::from_properties(Arc::new(RecordingManager::default()), &base)
                .unwrap();

        let mut merged = base.clone();
        merged.insert(REST_CATALOG_PROP_SIGNING_REGION.into(), "eu-west-1".into());
        merged.insert(REST_CATALOG_PROP_SIGNING_NAME.into(), "glue".into());
        merged.insert(REST_CATALOG_PROP_ACCESS_KEY_ID.into(), "ROTATED".into());

        for session in [
            scope_of(
                manager
                    .catalog_session(&test_client(), &merged)
                    .await
                    .unwrap()
                    .as_ref(),
            )
            .await,
            scope_of(
                manager
                    .init_session(&test_client(), &merged)
                    .await
                    .unwrap()
                    .as_ref(),
            )
            .await,
        ] {
            assert!(session.contains("/eu-west-1/glue/"), "{session}");
            assert!(!session.contains("us-east-1"), "{session}");
            assert!(session.starts_with("ROTATED/"), "{session}");
        }
    }

    #[tokio::test]
    async fn constructor_properties_apply_when_session_properties_omit_them() {
        let manager = SigV4AuthManager::from_properties(
            Arc::new(RecordingManager::default()),
            &props(&[
                (REST_CATALOG_PROP_SIGNING_REGION, "us-east-1"),
                (REST_CATALOG_PROP_ACCESS_KEY_ID, "AKID"),
                (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "secret"),
                (REST_CATALOG_PROP_SESSION_TOKEN, "constructor-token"),
            ]),
        )
        .unwrap();
        let catalog_props = props(&[
            ("uri", "http://localhost"),
            (REST_CATALOG_PROP_SIGNING_NAME, "glue"),
            (REST_CATALOG_PROP_ACCESS_KEY_ID, "ROTATED"),
            (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "rotated-secret"),
        ]);

        let init = manager
            .init_session(&test_client(), &props(&[("uri", "http://localhost")]))
            .await
            .unwrap();
        let catalog = manager
            .catalog_session(&test_client(), &catalog_props)
            .await
            .unwrap();

        // The scope's date comes from the live clock.
        let key_region_service = |scope: String| {
            let parts: Vec<_> = scope.split('/').collect();
            format!("{}/{}/{}", parts[0], parts[2], parts[3])
        };
        let mut request = request_with(&[]);
        catalog.authenticate(&mut request).await.unwrap();
        assert_eq!(
            (
                key_region_service(scope_of(init.as_ref()).await),
                key_region_service(scope_of(catalog.as_ref()).await),
                request.headers().get("x-amz-security-token"),
            ),
            (
                "AKID/us-east-1/execute-api".to_string(),
                "ROTATED/us-east-1/glue".to_string(),
                None
            )
        );
    }

    #[tokio::test]
    async fn an_injected_signer_survives_catalog_and_context_properties() {
        let manager = SigV4AuthManager::new(
            Arc::new(RecordingManager::default()),
            SigV4Signer::builder()
                .region("ap-south-1")
                .service("custom")
                .mode(PayloadHashMode::StandardAws)
                .build(),
            test_credentials_provider(),
        );
        let overriding = props(&[
            (REST_CATALOG_PROP_SIGNING_REGION, "eu-west-1"),
            (REST_CATALOG_PROP_SIGNING_NAME, "glue"),
            (REST_CATALOG_PROP_ACCESS_KEY_ID, "OTHER"),
            (REST_CATALOG_PROP_SECRET_ACCESS_KEY, "other"),
        ]);

        let parent = manager
            .catalog_session(&test_client(), &overriding)
            .await
            .unwrap();
        let context = SessionContext::builder()
            .properties(overriding.clone())
            .credentials(overriding.into_iter().map(|(k, v)| (k, v.into())).collect())
            .build();
        let session = manager
            .contextual_session(&context, parent.clone())
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&parent, &session));
        let scope = scope_of(session.as_ref()).await;
        assert!(scope.starts_with("ak/"), "{scope}");
        assert!(scope.contains("/ap-south-1/custom/"), "{scope}");
        assert!(!scope.contains("eu-west-1"), "{scope}");
    }

    #[tokio::test]
    async fn a_request_the_signer_rejects_fails_the_session() {
        let session = SigV4Session {
            delegate: Arc::new(crate::auth::NoopSession),
            signer: test_signer(PayloadHashMode::IcebergRest),
            credentials: test_credentials_provider(),
        };
        let mut request = HttpRequest::new(
            reqwest::Client::new()
                .post("https://rest.example.com/v1/namespaces")
                .body(reqwest::Body::wrap_stream(futures::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"chunk"))
                })))
                .build()
                .unwrap(),
        );

        let err = session.authenticate(&mut request).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported, "{err}");
        assert!(request.headers().get("authorization").is_none());
    }

    #[tokio::test]
    async fn a_failing_credentials_provider_fails_the_session() {
        #[derive(Debug)]
        struct Failing;
        impl ProvideCredentials for Failing {
            fn provide_credentials<'a>(
                &'a self,
            ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
            where Self: 'a {
                aws_credential_types::provider::future::ProvideCredentials::ready(Err(
                    aws_credential_types::provider::error::CredentialsError::not_loaded("no creds"),
                ))
            }
        }

        let session = SigV4Session {
            delegate: Arc::new(crate::auth::NoopSession),
            signer: test_signer(PayloadHashMode::IcebergRest),
            credentials: SharedCredentialsProvider::new(Failing),
        };
        let mut request = request_with(&[]);

        let err = session.authenticate(&mut request).await.unwrap_err();
        assert!(err.message().contains("resolve AWS credentials"), "{err}");
        assert!(request.headers().get("authorization").is_none());
    }
}
