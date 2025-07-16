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

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose;

/// Trait for authentication managers that supply authorization headers.
#[async_trait]
pub trait AuthManager: Send + Sync + std::fmt::Debug {
    /// Return the Authorization header value, or None if not applicable.
    async fn auth_header(&self) -> Option<String>;
}

/// An `AuthManager` that performs no authentication.
#[derive(Debug, Default)]
pub struct NoopAuthManager;

#[async_trait]
impl AuthManager for NoopAuthManager {
    async fn auth_header(&self) -> Option<String> {
        None
    }
}

/// An `AuthManager` for basic authentication.
#[derive(Debug)]
pub struct BasicAuthManager {
    token: String,
}

impl BasicAuthManager {
    /// Create a new `BasicAuthManager`.
    pub fn new(username: &str, password: &str) -> Self {
        let credentials = format!("{}:{}", username, password);
        let token = general_purpose::STANDARD.encode(credentials.as_bytes());
        BasicAuthManager { token }
    }
}

#[async_trait]
impl AuthManager for BasicAuthManager {
    async fn auth_header(&self) -> Option<String> {
        Some(format!("Basic {}", self.token))
    }
}
