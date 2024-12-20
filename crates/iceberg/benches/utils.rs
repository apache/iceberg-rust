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

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::vec::IntoIter;

use criterion::black_box;
use futures::stream::StreamExt;
use futures_util::stream::Iter;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::scan::{FileScanTask, FileScanTaskStream, TableScan};
use iceberg::table::Table;
use iceberg::{Catalog, Error};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use iceberg_test_utils::docker::DockerCompose;
use tokio::runtime::Runtime;

pub fn get_docker_compose() -> DockerCompose {
    DockerCompose::new(
        "iceberg-rust-performance",
        format!(
            "{}/../iceberg/testdata/performance",
            env!("CARGO_MANIFEST_DIR")
        ),
    )
    .with_persistence(true)
}

pub async fn build_catalog() -> RestCatalog {
    let docker_compose = get_docker_compose();

    let rest_api_container_ip = docker_compose.get_container_ip("rest");
    let warehouse_container_ip = docker_compose.get_container_ip("haproxy");
    // let warehouse_container_ip = docker_compose.get_container_ip("minio");

    let catalog_uri = format!("http://{}:{}", rest_api_container_ip, 8181);
    let warehouse_uri = format!("http://{}:{}", warehouse_container_ip, 9080);
    // let warehouse_uri = format!("http://{}:{}", warehouse_container_ip, 9000);

    let user_props = HashMap::from_iter(
        vec![
            (S3_ENDPOINT.to_string(), warehouse_uri),
            (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
        ]
        .into_iter(),
    );

    RestCatalog::new(
        RestCatalogConfig::builder()
            .uri(catalog_uri)
            .props(user_props)
            .build(),
    )
}

async fn setup_async() -> Arc<Table> {
    let catalog = build_catalog().await;
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    let table_idents = catalog.list_tables(&namespaces[0]).await.unwrap();
    let table = catalog.load_table(&table_idents[0]).await.unwrap();
    Arc::new(table)
}

pub fn setup(runtime: &Runtime) -> Arc<Table> {
    runtime.block_on(setup_async())
}

#[allow(dead_code)] // not dead, used in table_scan_execute_query
pub fn create_file_plan(runtime: &Runtime, scan: TableScan) -> Arc<Vec<FileScanTask>> {
    let tasks = runtime.block_on(async {
        let stream = scan.plan_files().await.unwrap();
        stream.map(|x| x.unwrap()).collect::<Vec<_>>().await
    });
    Arc::new(tasks)
}

#[allow(dead_code)] // not dead, used in table_scan_execute_query
pub fn create_task_stream(
    tasks: Arc<Vec<FileScanTask>>,
) -> Pin<Box<Iter<IntoIter<iceberg::Result<FileScanTask>>>>> {
    let tasks: Vec<_> = tasks
        .iter()
        .map(|t| Ok::<FileScanTask, Error>(t.clone()))
        .collect();
    Box::pin(futures_util::stream::iter(tasks))
}

#[allow(dead_code)] // not dead, used in table_scan_execute_query
pub async fn exec_plan(table: Arc<Table>, plan: FileScanTaskStream) {
    let reader = table.reader_builder().build();

    let mut record_batch_stream = reader.read(plan).unwrap();
    while let Some(item) = record_batch_stream.next().await {
        black_box(item.unwrap());
    }
}
