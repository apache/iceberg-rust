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
use iceberg::expr::Reference;
use iceberg::spec::Datum;
use tokio::runtime::Runtime;

mod utils;
use utils::{create_file_plan, create_task_stream, exec_plan, setup};

pub fn bench_read_all_files_all_rows(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let table = setup(&runtime);
    let scan = table.scan().build().unwrap();
    let tasks = create_file_plan(&runtime, scan);

    c.bench_function("scan: read (all files, all rows)", |b| {
        b.to_async(&runtime).iter_batched(
            || create_task_stream(tasks.clone()),
            |plan| exec_plan(table.clone(), plan),
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_read_all_files_some_rows(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let table = setup(&runtime);
    let scan = table
        .scan()
        .with_filter(Reference::new("passenger_count").greater_than(Datum::double(1.0)))
        .build()
        .unwrap();
    let tasks = create_file_plan(&runtime, scan);

    c.bench_function("scan: read (all files, some rows)", |b| {
        b.to_async(&runtime).iter_batched(
            || create_task_stream(tasks.clone()),
            |plan| exec_plan(table.clone(), plan),
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_read_some_files_all_rows(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let table = setup(&runtime);
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
    let tasks = create_file_plan(&runtime, scan);

    c.bench_function("scan: read (some files, all rows)", |b| {
        b.to_async(&runtime).iter_batched(
            || create_task_stream(tasks.clone()),
            |plan| exec_plan(table.clone(), plan),
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_read_some_files_some_rows(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let table = setup(&runtime);
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
                    .and(Reference::new("passenger_count").greater_than(Datum::double(1.0))),
            )
            .build()
            .unwrap();
    let tasks = create_file_plan(&runtime, scan);

    c.bench_function("scan: read (some files, some rows)", |b| {
        b.to_async(&runtime).iter_batched(
            || create_task_stream(tasks.clone()),
            |plan| exec_plan(table.clone(), plan),
            BatchSize::SmallInput,
        )
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_read_some_files_some_rows, bench_read_some_files_all_rows, bench_read_all_files_some_rows, bench_read_all_files_all_rows
}

criterion_main!(benches);
