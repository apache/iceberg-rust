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

use std::env;

use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

fn get_catalog_uri_from_env() -> String {
    env::var("CATALOG_URI").unwrap_or("http://localhost:8080".to_string())
}

pub fn get_rest_catalog() -> RestCatalog {
    let config = RestCatalogConfig::builder()
        .uri(get_catalog_uri_from_env())
        .build();
    RestCatalog::new(config)
}
