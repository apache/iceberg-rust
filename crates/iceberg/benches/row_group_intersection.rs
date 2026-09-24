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

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

#[path = "../src/arrow/reader/row_group_intersection.rs"]
mod row_group_intersection;

fn intersect_with_contains(left: &[usize], right: &[usize]) -> Vec<usize> {
    left.iter()
        .copied()
        .filter(|index| right.contains(index))
        .collect()
}

fn intersect_with_binary_search(left: &[usize], right: &[usize]) -> Vec<usize> {
    left.iter()
        .copied()
        .filter(|index| right.binary_search(index).is_ok())
        .collect()
}

fn bench_intersection_case(c: &mut Criterion, name: &str, left: &[usize], right: &[usize]) {
    let expected = intersect_with_contains(left, right);
    assert_eq!(
        row_group_intersection::intersect_sorted_row_group_indices(left, right),
        expected
    );
    assert_eq!(intersect_with_binary_search(left, right), expected);

    let mut group = c.benchmark_group(format!("row_group_intersection/{name}"));
    group.throughput(Throughput::Elements((left.len() + right.len()) as u64));
    group.bench_function("contains", |b| {
        b.iter(|| black_box(intersect_with_contains(black_box(left), black_box(right))))
    });
    group.bench_function("two_pointer", |b| {
        b.iter(|| {
            black_box(row_group_intersection::intersect_sorted_row_group_indices(
                black_box(left),
                black_box(right),
            ))
        })
    });
    group.bench_function("binary_search", |b| {
        b.iter(|| {
            black_box(intersect_with_binary_search(
                black_box(left),
                black_box(right),
            ))
        })
    });
    group.finish();
}

fn bench_row_group_intersection(c: &mut Criterion) {
    for size in [32, 128, 512, 2_048, 8_192] {
        let left = (0..size).collect::<Vec<usize>>();
        let right = left.clone();
        bench_intersection_case(c, &format!("equal_{size}"), &left, &right);
    }

    for predicate_size in [512, 2_048] {
        let byte_range_filtered = (predicate_size - 16..predicate_size).collect::<Vec<usize>>();
        let predicate_filtered = (0..predicate_size).collect::<Vec<usize>>();
        bench_intersection_case(
            c,
            &format!("byte_range_16_predicate_{predicate_size}"),
            &byte_range_filtered,
            &predicate_filtered,
        );
    }
}

criterion_group!(benches, bench_row_group_intersection);
criterion_main!(benches);
