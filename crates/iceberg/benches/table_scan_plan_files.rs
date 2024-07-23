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

use criterion::*;
use futures_util::StreamExt;
use tokio::runtime::Runtime;

mod utils;
use iceberg::expr::Reference;
use iceberg::spec::Datum;
use iceberg::table::Table;
use iceberg::Catalog;
use utils::build_catalog;

async fn setup_async() -> Table {
    let catalog = build_catalog().await;
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    let table_idents = catalog.list_tables(&namespaces[0]).await.unwrap();
    catalog.load_table(&table_idents[0]).await.unwrap()
}

fn setup(runtime: &Runtime) -> Table {
    runtime.block_on(setup_async())
}

async fn all_files_all_rows(table: &Table) {
    let scan = table.scan().build().unwrap();
    let mut stream = scan.plan_files().await.unwrap();

    while let Some(item) = stream.next().await {
        black_box(item.unwrap());
    }
}

async fn one_file_all_rows(table: &Table) {
    let scan = table
        .scan()
        .with_filter(
            Reference::new("tpep_pickup_datetime")
                .greater_than_or_equal_to(
                    Datum::timestamptz_from_str("2024-02-01T00:00:00.000 UTC").unwrap(),
                )
                .and(Reference::new("tpep_pickup_datetime").less_than(
                    Datum::timestamptz_from_str("2024-02-02T00:00:00.000 UTC").unwrap(),
                )),
        )
        .build()
        .unwrap();
    let mut stream = scan.plan_files().await.unwrap();

    while let Some(item) = stream.next().await {
        black_box(item.unwrap());
    }
}

async fn all_files_some_rows(table: &Table) {
    let scan = table
        .scan()
        .with_filter(Reference::new("passenger_count").equal_to(Datum::double(1.0)))
        .build()
        .unwrap();
    let mut stream = scan.plan_files().await.unwrap();

    while let Some(item) = stream.next().await {
        black_box(item.unwrap());
    }
}

async fn one_file_some_rows(table: &Table) {
    let scan =
        table
            .scan()
            .with_filter(
                Reference::new("tpep_pickup_datetime")
                    .greater_than_or_equal_to(
                        Datum::timestamptz_from_str("2024-02-01T00:00:00.000 UTC").unwrap(),
                    )
                    .and(Reference::new("tpep_pickup_datetime").less_than(
                        Datum::timestamptz_from_str("2024-02-02T00:00:00.000 UTC").unwrap(),
                    ))
                    .and(Reference::new("passenger_count").equal_to(Datum::double(1.0))),
            )
            .build()
            .unwrap();
    let mut stream = scan.plan_files().await.unwrap();

    while let Some(item) = stream.next().await {
        black_box(item.unwrap());
    }
}

pub fn bench_all_files_all_rows(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let table = setup(&runtime);
    println!("setup complete");

    c.bench_function("all_files_all_rows", |b| {
        b.to_async(&runtime).iter(|| all_files_all_rows(&table))
    });
}

pub fn bench_one_file_all_rows(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let table = setup(&runtime);
    println!("setup complete");

    c.bench_function("one_file_all_rows", |b| {
        b.to_async(&runtime).iter(|| one_file_all_rows(&table))
    });
}

pub fn bench_all_files_some_rows(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let table = setup(&runtime);
    println!("setup complete");

    c.bench_function("all_files_some_rows", |b| {
        b.to_async(&runtime).iter(|| all_files_some_rows(&table))
    });
}

pub fn bench_one_file_some_rows(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let table = setup(&runtime);
    println!("setup complete");

    c.bench_function("one_file_some_rows", |b| {
        b.to_async(&runtime).iter(|| one_file_some_rows(&table))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_all_files_all_rows, bench_all_files_some_rows, bench_one_file_all_rows, bench_one_file_some_rows
}

criterion_main!(benches);
