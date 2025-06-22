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

use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use benchmarks::{copy_dir_to_fileio, construct_table};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::TryStreamExt;
use iceberg::io::FileIOBuilder;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use iceberg_catalog_sql::{SqlBindStyle, SqlCatalog, SqlCatalogConfig};
use tokio::runtime::Runtime;

pub fn bench_sql_catalog_projection_once(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_catalog_projection_once");
    group.measurement_time(Duration::from_secs(20)).sample_size(50);

    let table_dir = construct_table("sql-catalog-taxicab");
    let mut db_path = table_dir.clone();
    db_path.push("benchmarking-catalog.db");
    let uri = format!("sqlite:{}", db_path.to_str().unwrap());

    group.bench_function(
        "sql_catalog_projection_once",
        // an issue with criterion (https://github.com/bheisler/criterion.rs/issues/751) means we can't do a normal benchmark here,
        // it doesn't let use provide an async setup function to build `FileIO`, etc. instead, we have to use `iter_custom` and do
        // the measurements ourselves
        |b| {
            b.to_async(Runtime::new().unwrap()).iter_custom(async |n| {
                let mut total_elapsed = Duration::default();

                for _ in 0..n {
                    let table = setup_table(table_dir.clone(), uri.clone()).await;

                    let start = Instant::now();
                    let output = scan_table(black_box(table)).await;
                    let dur = start.elapsed();

                    drop(black_box(output));
                    total_elapsed += dur;
                }

                total_elapsed
            })
        },
    );
    group.finish()
}

async fn setup_table(table_dir: PathBuf, uri: String) -> Table {
    let file_io = FileIOBuilder::new("iceberg_benchmarking_storage")
        .build()
        .unwrap();
    copy_dir_to_fileio(table_dir.clone(), &file_io).await;

    let config = SqlCatalogConfig::builder()
        .file_io(file_io)
        .uri(format!("sqlite:{uri}"))
        .name("default".to_owned())
        .sql_bind_style(SqlBindStyle::QMark)
        .warehouse_location(table_dir.to_str().unwrap().to_owned())
        .build();
    let catalog = SqlCatalog::new(config).await.unwrap();
    let table = catalog
        .load_table(&TableIdent::from_strs(["default", "taxi_dataset"]).unwrap())
        .await
        .expect(&format!("table_dir: {table_dir:?}"));
    table
}

async fn scan_table(table: Table) -> Vec<RecordBatch> {
    let stream = table
        .scan()
        .select(["passenger_count", "fare_amount"])
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();

    stream.try_collect().await.unwrap()
}

criterion_group!(benches, bench_sql_catalog_projection_once);
criterion_main!(benches);
