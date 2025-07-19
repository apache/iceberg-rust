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

use base64::Engine;
use iceberg_catalog_rest::auth::{AuthManager, BasicAuthManager, NoopAuthManager};

#[tokio::test]
async fn test_noop_auth_manager() {
    let auth_manager = NoopAuthManager;
    let header = auth_manager.auth_header().await;
    assert!(header.is_none());
}

#[tokio::test]
async fn test_basic_auth_manager() {
    let username = "testuser";
    let password = "testpassword";
    let auth_manager = BasicAuthManager::new(username, password);
    let header = auth_manager.auth_header().await;
    assert!(header.is_some());
    let expected_token = base64::engine::general_purpose::STANDARD
        .encode(format!("{}:{}", username, password).as_bytes());
    assert_eq!(header.unwrap(), format!("Basic {}", expected_token));
}
