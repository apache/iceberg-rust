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
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use criterion::{Criterion, criterion_group, criterion_main};
use iceberg::arrow::partition_value_calculator::PartitionValueCalculator;
use iceberg::arrow::record_batch_projector::RecordBatchProjector;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{
    NestedField, NestedFieldRef, PartitionSpecBuilder, PrimitiveType, Schema, Transform, Type,
};

fn make_projector_and_batch(
    projected_columns: usize,
) -> (RecordBatchProjector, PartitionValueCalculator, RecordBatch) {
    let row_count = 65_536;
    let fields: Vec<NestedFieldRef> = (0..64)
        .map(|column| {
            NestedField::required(
                column + 1,
                format!("column_{column}"),
                Type::Primitive(PrimitiveType::Int),
            )
            .into()
        })
        .collect();
    let schema = Arc::new(Schema::builder().with_fields(fields).build().unwrap());
    let field_ids: Vec<i32> = (1..=projected_columns as i32).collect();
    let projector = RecordBatchProjector::from_iceberg_schema(schema.clone(), &field_ids).unwrap();
    let mut partition_spec_builder = PartitionSpecBuilder::new(schema.clone());
    for column in 0..projected_columns {
        partition_spec_builder = partition_spec_builder
            .add_partition_field(
                format!("column_{column}"),
                format!("partition_{column}"),
                Transform::Identity,
            )
            .unwrap();
    }
    let partition_spec = partition_spec_builder.build().unwrap();
    let calculator = PartitionValueCalculator::try_new(&partition_spec, &schema).unwrap();
    let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).unwrap());
    let columns = (0..64)
        .map(|column| {
            Arc::new(Int32Array::from_iter_values(
                (0..row_count).map(|row| row * (column + 1)),
            )) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(arrow_schema, columns).unwrap();

    (projector, calculator, batch)
}

fn bench_projector(criterion: &mut Criterion) {
    for projected_columns in [1, 4, 16, 64] {
        let (projector, calculator, batch) = make_projector_and_batch(projected_columns);
        criterion.bench_function(
            &format!("record_batch_projector/{projected_columns}_top_level_columns"),
            |bencher| {
                bencher.iter(|| {
                    black_box(
                        projector
                            .project_column(black_box(batch.columns()))
                            .unwrap(),
                    )
                })
            },
        );
        criterion.bench_function(
            &format!("partition_value_calculator/{projected_columns}_identity_columns"),
            |bencher| bencher.iter(|| black_box(calculator.calculate(black_box(&batch)).unwrap())),
        );
    }
}

criterion_group!(benches, bench_projector);
criterion_main!(benches);
