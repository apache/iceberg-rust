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
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

#[path = "../src/arrow/reader/name_mapping_lookup.rs"]
mod name_mapping_lookup;

struct MappedField {
    names: Vec<String>,
    field_id: Option<i32>,
}

fn apply_current_lookup(fields: &[String], mappings: &[MappedField]) -> Vec<Option<i32>> {
    fields
        .iter()
        .map(|field| {
            mappings
                .iter()
                .find(|mapped| mapped.names.contains(&field.to_string()))
                .and_then(|mapped| mapped.field_id)
        })
        .collect()
}

fn apply_borrowed_lookup(fields: &[String], mappings: &[MappedField]) -> Vec<Option<i32>> {
    fields
        .iter()
        .map(|field| {
            mappings
                .iter()
                .find(|mapped| name_mapping_lookup::contains_name(&mapped.names, field))
                .and_then(|mapped| mapped.field_id)
        })
        .collect()
}

fn bench_case(c: &mut Criterion, width: usize) {
    let fields = (0..width)
        .map(|index| format!("column_{index}"))
        .collect::<Vec<_>>();
    let mappings = (0..width)
        .map(|index| MappedField {
            names: vec![format!("column_{index}"), format!("legacy_{index}")],
            field_id: Some(index as i32 + 1),
        })
        .collect::<Vec<_>>();

    let expected = apply_current_lookup(&fields, &mappings);
    assert_eq!(apply_borrowed_lookup(&fields, &mappings), expected);

    let mut group = c.benchmark_group(format!("name_mapping_lookup/{width}_fields"));
    group.throughput(Throughput::Elements(width as u64));
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_secs(2));
    group.bench_function("allocating_contains", |b| {
        b.iter(|| {
            black_box(apply_current_lookup(
                black_box(&fields),
                black_box(&mappings),
            ))
        })
    });
    group.bench_function("borrowed_name_comparison", |b| {
        b.iter(|| {
            black_box(apply_borrowed_lookup(
                black_box(&fields),
                black_box(&mappings),
            ))
        })
    });
    group.finish();
}

fn bench_name_mapping_lookup(c: &mut Criterion) {
    for width in [1, 2, 4, 8, 16, 32, 128, 512, 2_048] {
        bench_case(c, width);
    }
}

criterion_group!(benches, bench_name_mapping_lookup);
criterion_main!(benches);
