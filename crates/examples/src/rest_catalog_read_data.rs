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

use futures::TryStreamExt;
use iceberg::{Catalog, Result, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    let catalog = utils::get_rest_catalog();

    // There should be a table `ns1.t1` pre-created with column name
    let table_id = TableIdent::from_strs(["ns1", "t1"]).unwrap();
    let table = catalog.load_table(&table_id).await?;

    // Build table scan.
    let stream = table
        .scan()
        .select(["a", "b", "c"])
        .build()?
        .to_arrow()
        .await?;

    // Consume this stream like arrow record batch stream.
    let data: Vec<_> = stream.try_collect().await?;
    println!("data: {:?}", data);
    Ok(())
}
