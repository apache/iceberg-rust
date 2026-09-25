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

//! Iceberg REST API implementation.
//!
//! The crate provides two REST catalog APIs:
//!
//! - [`RestCatalog`] implements [`iceberg::Catalog`] by binding one
//!   [`iceberg::SessionContext`] to every operation.
//! - [`RestSessionCatalog`] implements [`iceberg::SessionCatalog`] and accepts a
//!   session context with each operation.
//!
//! # Catalog compatibility API
//!
//! ```rust, no_run
//! use std::collections::HashMap;
//!
//! use iceberg::CatalogBuilder;
//! use iceberg_catalog_rest::{
//!     REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let catalog = RestCatalogBuilder::default()
//!         .load(
//!             "rest",
//!             HashMap::from([
//!                 (
//!                     REST_CATALOG_PROP_URI.to_string(),
//!                     "http://localhost:8181".to_string(),
//!                 ),
//!                 (
//!                     REST_CATALOG_PROP_WAREHOUSE.to_string(),
//!                     "s3://warehouse".to_string(),
//!                 ),
//!             ]),
//!         )
//!         .await
//!         .unwrap();
//! }
//! ```
//!
//! # Pagination
//!
//! Both builders support `with_page_size(1000)` to request bounded namespace
//! and table list responses. Alternatively, set [`REST_CATALOG_PROP_PAGE_SIZE`]
//! in the properties passed to `load`; this takes precedence over the builder
//! method. Server `/v1/config` defaults have lower priority than client settings,
//! and server overrides have the highest priority. No page size is sent if unset.
//! The effective value must be a positive `u32` and is validated after fetching
//! server configuration. List operations fetch all pages and return all results.
//!
//! # Session catalog API
//!
//! ```rust, no_run
//! use std::collections::HashMap;
//!
//! use iceberg::{SessionCatalog, SessionContext};
//! use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestSessionCatalogBuilder};
//!
//! #[tokio::main]
//! async fn main() {
//!     let catalog = RestSessionCatalogBuilder::default()
//!         .load(
//!             "rest",
//!             HashMap::from([(
//!                 REST_CATALOG_PROP_URI.to_string(),
//!                 "http://localhost:8181".to_string(),
//!             )]),
//!         )
//!         .await
//!         .unwrap();
//!     let context = SessionContext::builder()
//!         .identity("user123".to_string())
//!         .build();
//!
//!     let namespaces = catalog.list_namespaces(&context, None).await.unwrap();
//! }
//! ```

#![deny(missing_docs)]

mod auth;
mod catalog;
mod client;
pub use client::HttpClient;
mod request;
pub use request::{HttpRequest, HttpRequestBody};
mod response;
pub use response::HttpResponse;
mod endpoint;
mod types;

pub use auth::*;
pub use catalog::*;
pub use endpoint::Endpoint;
pub use types::*;
