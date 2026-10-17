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

//! Fast endpoint probe and readiness utilities for integration test emulators.

use std::time::Duration;

use iceberg::io::FileIO;
use tokio::time::sleep;

use crate::harness::StorageHarness;

const DEFAULT_PROBE_TIMEOUT_MS: u64 = 1000;
const DEFAULT_PROBE_RETRIES: usize = 3;
const PROBE_RETRY_INTERVAL_MS: u64 = 200;

fn get_probe_timeout() -> Duration {
    let ms = std::env::var("ICEBERG_PROBE_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PROBE_TIMEOUT_MS);
    Duration::from_millis(ms)
}

/// Fast probe to check if an endpoint service is listening before entering retry loops.
///
/// Note: Any HTTP response from `.send().await.is_ok()` (including 4xx/5xx) is treated
/// as reachable, as it proves the underlying server is up, listening on the port,
/// and actively responding to HTTP requests.
pub async fn is_endpoint_reachable(endpoint: &str) -> bool {
    let Ok(client) = reqwest::Client::builder()
        .timeout(get_probe_timeout())
        .build()
    else {
        return false;
    };

    for attempt in 0..DEFAULT_PROBE_RETRIES {
        if client.get(endpoint).send().await.is_ok() {
            return true;
        }
        if attempt + 1 < DEFAULT_PROBE_RETRIES {
            sleep(Duration::from_millis(PROBE_RETRY_INTERVAL_MS)).await;
        }
    }
    false
}

/// Handle an unreachable storage endpoint by panicking if `ICEBERG_REQUIRE_STORAGE`
/// is set, or returning `None` to skip the test when running without local Docker services.
pub fn handle_unreachable_endpoint(kind: &'static str, endpoint: &str) -> Option<StorageHarness> {
    if std::env::var("ICEBERG_REQUIRE_STORAGE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        panic!(
            "storage backend '{kind}' is required by ICEBERG_REQUIRE_STORAGE, but endpoint '{endpoint}' is unreachable"
        );
    }
    eprintln!("Skipping {kind} storage test: {endpoint} not reachable");
    None
}

/// Polls `file_io.exists(check_path)` until ready or max retries exceeded.
pub async fn wait_until_ready(
    file_io: &FileIO,
    check_path: &str,
    kind: &'static str,
    endpoint: &str,
) {
    let mut retries = 0;
    while retries < 15 {
        if file_io.exists(check_path).await.unwrap_or(false) {
            return;
        }
        sleep(Duration::from_millis(500)).await;
        retries += 1;
    }

    panic!(
        "Storage backend '{kind}' was reachable at '{endpoint}', but failed readiness check on '{check_path}' after 15 retries"
    );
}
