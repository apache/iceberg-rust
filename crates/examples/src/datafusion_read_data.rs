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

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use iceberg_datafusion::IcebergCatalogProvider;

mod utils;

#[tokio::main]
async fn main() {
    let iceberg_catalog = utils::get_rest_catalog();

    let client = Arc::new(iceberg_catalog);
    let catalog = Arc::new(IcebergCatalogProvider::try_new(client).await.unwrap());

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);
    let df = ctx.sql("select * from catalog.ns.table1").await.unwrap();
    let data = df.collect().await.unwrap();
    println!("{:?}", data);
}
