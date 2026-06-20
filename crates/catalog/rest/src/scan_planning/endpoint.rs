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

//! Endpoint capability negotiation.
//!
//! The REST server advertises the set of endpoints it supports in the
//! `endpoints` field of the `GET /v1/config` response, each encoded as a
//! `"<HTTP-METHOD> <path-template>"` string (mirroring the Java
//! `org.apache.iceberg.rest.Endpoint`). The client gates optional features —
//! notably server-side scan planning — on the presence of the corresponding
//! [`Endpoint`] in the negotiated set.

use std::collections::HashSet;

use iceberg::{Error, ErrorKind, Result};
use serde::de::{Error as DeError, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// Path templates, matching `org.apache.iceberg.rest.ResourcePaths`. These are
// the *template* forms (with `{prefix}`/`{namespace}`/`{table}`/`{plan-id}`),
// not resolved URLs — the server advertises them verbatim and we compare by
// string equality.
const V1_TABLE_SCAN_PLAN_SUBMIT: &str = "/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan";
const V1_TABLE_SCAN_PLAN: &str =
    "/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}";
const V1_TABLE_SCAN_PLAN_TASKS: &str = "/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks";

/// A server endpoint identified by its HTTP method and path template.
///
/// Serialized as `"<METHOD> <path>"` (e.g. `"POST /v1/{prefix}/.../plan"`),
/// matching the wire form used in the `endpoints` field of the config response.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Endpoint {
    method: String,
    path: String,
}

impl Endpoint {
    fn new(method: &str, path: &str) -> Self {
        Self {
            method: method.to_ascii_uppercase(),
            path: path.to_string(),
        }
    }

    /// `POST .../plan` — submit a table scan for server-side planning.
    pub(crate) fn submit_table_scan_plan() -> Self {
        Self::new("POST", V1_TABLE_SCAN_PLAN_SUBMIT)
    }

    /// `GET .../plan/{plan-id}` — poll an asynchronous planning result.
    pub(crate) fn fetch_table_scan_plan() -> Self {
        Self::new("GET", V1_TABLE_SCAN_PLAN)
    }

    /// `DELETE .../plan/{plan-id}` — cancel an in-progress plan.
    pub(crate) fn cancel_table_scan_plan() -> Self {
        Self::new("DELETE", V1_TABLE_SCAN_PLAN)
    }

    /// `POST .../tasks` — fetch concrete scan tasks for a plan task token.
    pub(crate) fn fetch_table_scan_plan_tasks() -> Self {
        Self::new("POST", V1_TABLE_SCAN_PLAN_TASKS)
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.method, self.path)
    }
}

impl Serialize for Endpoint {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_str(&format!("{} {}", self.method, self.path))
    }
}

impl<'de> Deserialize<'de> for Endpoint {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct EndpointVisitor;
        impl Visitor<'_> for EndpointVisitor {
            type Value = Endpoint;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an endpoint string of the form \"<HTTP-METHOD> <path>\"")
            }

            fn visit_str<E>(self, v: &str) -> std::result::Result<Endpoint, E>
            where E: DeError {
                let (method, path) = v.split_once(' ').ok_or_else(|| {
                    E::custom(format!(
                        "invalid endpoint {v:?}: expected \"<HTTP-METHOD> <path>\""
                    ))
                })?;
                Ok(Endpoint::new(method.trim(), path.trim()))
            }
        }
        deserializer.deserialize_str(EndpointVisitor)
    }
}

/// Returns an error if `endpoint` is absent from the negotiated `supported`
/// set, using [`ErrorKind::FeatureUnsupported`] so callers can fall back.
pub(crate) fn check(supported: &HashSet<Endpoint>, endpoint: &Endpoint) -> Result<()> {
    if supported.contains(endpoint) {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::FeatureUnsupported,
            format!("Server does not support endpoint: {endpoint}"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_string_round_trip() {
        let ep = Endpoint::submit_table_scan_plan();
        let json = serde_json::to_string(&ep).unwrap();
        assert_eq!(
            json,
            "\"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan\""
        );
        let back: Endpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(ep, back);
    }

    #[test]
    fn endpoint_set_parses_from_config_array() {
        let arr = serde_json::json!([
            "GET /v1/{prefix}/namespaces",
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
        ]);
        let set: HashSet<Endpoint> = serde_json::from_value(arr).unwrap();
        assert!(check(&set, &Endpoint::submit_table_scan_plan()).is_ok());
        assert!(check(&set, &Endpoint::fetch_table_scan_plan()).is_ok());
        assert!(check(&set, &Endpoint::fetch_table_scan_plan_tasks()).is_ok());
        // Cancel was not advertised.
        assert!(check(&set, &Endpoint::cancel_table_scan_plan()).is_err());
    }
}
