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

use datafusion::catalog::Session as DFSession;
use datafusion::common::extensions_options;
use iceberg::SessionContext;

extensions_options! {
    /// Iceberg-specific DataFusion options.
    ///
    /// It does deliberately not implement [`ConfigExtension`](datafusion::config::ConfigExtension)
    /// to prevent SQL users from setting unverified authentication properties
    /// such as:
    ///
    /// ```sql
    /// SET iceberg.identity = 'alice';
    /// ```
    pub struct IcebergOptions {
        /// Optional identity used when deriving the Iceberg session context.
        pub identity: Option<String>, default = None
    }
}

/// Derives an Iceberg session context from a DataFusion session and its
/// configured [`IcebergOptions`], if registered.
pub(crate) fn resolve_session_context(session: &dyn DFSession) -> Option<SessionContext> {
    let options = session.config().get_extension::<IcebergOptions>()?;

    let builder = SessionContext::builder().session_id(session.session_id().to_string());

    let context = if let Some(identity) = &options.identity {
        builder.identity(identity.to_string()).build()
    } else {
        builder.build()
    };

    Some(context)
}
