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

pub(crate) fn is_truthy(value: &str) -> bool {
    ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str())
}

/// Convert an opendal error into an iceberg error.
pub(crate) fn from_opendal_error(e: opendal::Error) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        "Failure in doing io operation",
    )
    .with_source(e)
}

/// Convert a [`SystemTime`](std::time::SystemTime) credential expiry into the
/// `reqsign` [`Timestamp`](reqsign_core::time::Timestamp) used on backend
/// credential types (e.g. `AwsCredential::expires_in`, `google::Token::expires_at`).
#[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
pub(crate) fn system_time_to_timestamp(
    time: std::time::SystemTime,
) -> reqsign_core::Result<reqsign_core::time::Timestamp> {
    let millis = time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| {
            reqsign_core::Error::unexpected(format!(
                "credential expiry precedes the UNIX epoch: {e}"
            ))
        })?
        .as_millis();
    let millis = i64::try_from(millis).map_err(|_| {
        reqsign_core::Error::unexpected("credential expiry overflows i64 milliseconds")
    })?;
    reqsign_core::time::Timestamp::from_millisecond(millis)
        .map_err(|e| reqsign_core::Error::unexpected(format!("invalid credential expiry: {e}")))
}

/// Validate that a provider's declared credential prefix covers the path for
/// which the backend requested the credential.
#[cfg(any(feature = "opendal-s3", feature = "opendal-gcs"))]
pub(crate) fn validate_credential_prefix(
    path: &str,
    prefix: Option<&str>,
) -> reqsign_core::Result<()> {
    match prefix {
        Some("") => Err(reqsign_core::Error::unexpected(
            "vended credential has an empty storage prefix",
        )),
        Some(prefix) if !path.starts_with(prefix) => Err(reqsign_core::Error::unexpected(format!(
            "vended credential prefix {prefix:?} does not cover storage location {path:?}"
        ))),
        _ => Ok(()),
    }
}
