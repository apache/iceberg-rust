<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Iceberg Rest Catalog Official Native Rust Implementation

[![crates.io](https://img.shields.io/crates/v/iceberg.svg)](https://crates.io/crates/iceberg-catalog-rest)
[![docs.rs](https://img.shields.io/docsrs/iceberg.svg)](https://docs.rs/iceberg/latest/iceberg-catalog-rest/)

This crate contains the official Native Rust implementation of Apache Iceberg Rest Catalog.

See the [API documentation](https://docs.rs/iceberg-catalog-rest/latest) for examples and the full API.

## Features

### Middleware Support

The `middleware` feature enables support for custom HTTP middleware using the `reqwest-middleware` crate. This allows you to add custom behavior to HTTP requests, such as:

- Request/response logging
- Retry logic
- Rate limiting
- Custom authentication
- Metrics collection

To enable middleware support, add the feature to your `Cargo.toml`:

```toml
[dependencies]
iceberg-catalog-rest = { version = "0.8", features = ["middleware"] }
reqwest-middleware = "0.4"
```

Example usage:

```rust
use iceberg_catalog_rest::RestCatalogBuilder;
use reqwest_middleware::ClientBuilder;
use reqwest::Client;

// Create a client with middleware
let client = ClientBuilder::new(Client::new())
    // Add your middleware here
    .build();

// Configure the catalog with the middleware client
let catalog = RestCatalogBuilder::new("http://localhost:8080")
    .with_middleware_client(client)
    .build()?;
```
