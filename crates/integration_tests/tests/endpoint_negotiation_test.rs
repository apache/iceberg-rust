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

//! Integration test for REST endpoint capability negotiation, run against the
//! `iceberg-rest-fixture` started by `dev/docker-compose.yaml`.

use std::collections::HashMap;

use iceberg::CatalogBuilder;
use iceberg_catalog_rest::{Endpoint, REST_CATALOG_PROP_URI, RestCatalogBuilder};
use iceberg_test_utils::{get_rest_catalog_endpoint, set_up};

#[tokio::test]
async fn test_supports_endpoint_from_server_config() {
    set_up();

    // Endpoint negotiation only reads `GET /v1/config`, so the catalog needs
    // just the REST URI — no storage configuration is involved.
    let config = HashMap::from([(
        REST_CATALOG_PROP_URI.to_string(),
        get_rest_catalog_endpoint(),
    )]);
    let catalog = RestCatalogBuilder::default()
        .load("rest", config)
        .await
        .unwrap();

    // `HEAD .../tables/{table}` (table-exists) is advertised by the fixture but
    // is NOT part of `DEFAULT_ENDPOINTS`. Asserting it is supported confirms the
    // server's advertised list — not the default fallback — is what gets
    // negotiated (the fallback would report it unsupported).
    let table_exists: Endpoint = "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"
        .parse()
        .unwrap();
    assert!(catalog.supports_endpoint(&table_exists).await.unwrap());

    // A base operation is advertised and supported too.
    let load_table: Endpoint = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"
        .parse()
        .unwrap();
    assert!(catalog.supports_endpoint(&load_table).await.unwrap());

    // A path the fixture does not advertise is reported unsupported.
    let bogus: Endpoint = "GET /v1/{prefix}/does-not-exist".parse().unwrap();
    assert!(!catalog.supports_endpoint(&bogus).await.unwrap());
}
