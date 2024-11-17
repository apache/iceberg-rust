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
use iceberg::io::FileIO;
use iceberg::table::{StaticTable, Table};
use iceberg::TableIdent;
use iceberg_datafusion::IcebergTableProvider;

async fn get_test_table_from_metadata_file() -> Table {
    let metadata_file_path = "file:///tmp/warehouse/a/a/metadata/00001-e6e7a74a-ae5c-4201-a2a4-46d979c810a3.metadata.json";
    let file_io = FileIO::from_path(metadata_file_path)
        .unwrap()
        .build()
        .unwrap();
    let static_identifier = TableIdent::from_strs(["static_ns", "static_table"]).unwrap();
    let static_table =
        StaticTable::from_metadata_file(metadata_file_path, static_identifier, file_io)
            .await
            .unwrap();
    static_table.into_table()
}

#[tokio::main]
async fn main() {
    let table = get_test_table_from_metadata_file().await;
    let table_provider = IcebergTableProvider::try_new_from_table(table.clone())
        .await
        .unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("mytable", Arc::new(table_provider))
        .unwrap();
    //let data = ctx.sql("SELECT count(*) FROM mytable").await.unwrap().explain(true, true).unwrap().collect().await.unwrap();
    let data = ctx
        .sql("SELECT name FROM mytable group by 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    println!("{:?}", data);
}
