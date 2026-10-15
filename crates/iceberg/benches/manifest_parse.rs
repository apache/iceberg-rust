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
use std::hint::black_box;
use std::sync::Arc;

use apache_avro::types::Value;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, Manifest,
    ManifestWriterBuilder, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, Transform,
    Type,
};

const COLUMN_TYPES: [PrimitiveType; 12] = [
    PrimitiveType::Long,
    PrimitiveType::String,
    PrimitiveType::Int,
    PrimitiveType::Long,
    PrimitiveType::Float,
    PrimitiveType::Double,
    PrimitiveType::String,
    PrimitiveType::Boolean,
    PrimitiveType::Date,
    PrimitiveType::Timestamp,
    PrimitiveType::String,
    PrimitiveType::Long,
];

fn columns() -> impl Iterator<Item = (i32, &'static PrimitiveType)> {
    (1..).zip(COLUMN_TYPES.iter())
}

fn bound(ty: &PrimitiveType, v: i64) -> Datum {
    match ty {
        PrimitiveType::Boolean => Datum::bool(v % 2 == 1),
        PrimitiveType::Int => Datum::int(v as i32),
        PrimitiveType::Long => Datum::long(v),
        PrimitiveType::Float => Datum::float(v as f32),
        PrimitiveType::Double => Datum::double(v as f64),
        PrimitiveType::Date => Datum::date(v as i32),
        PrimitiveType::Timestamp => Datum::timestamp_micros(v),
        PrimitiveType::String => Datum::string(format!("{v:016}")),
        _ => unreachable!(),
    }
}

/// Returns a V2 data manifest with `num_entries` files, each with stats for
/// every column and an identity partition on a string column.
fn write_manifest(num_entries: u64) -> Vec<u8> {
    let schema = Arc::new(
        Schema::builder()
            .with_fields(columns().map(|(id, ty)| {
                let name = if id == 2 {
                    "category".to_string()
                } else {
                    format!("c{id}")
                };
                NestedField::optional(id, name, Type::Primitive(ty.clone())).into()
            }))
            .build()
            .unwrap(),
    );
    let partition_spec = PartitionSpec::builder(schema.clone())
        .add_partition_field("category", "category", Transform::Identity)
        .unwrap()
        .build()
        .unwrap();
    let file_io = FileIO::new_with_memory();
    let path = "memory://manifest.avro";
    let mut writer = ManifestWriterBuilder::new(
        file_io.new_output(path).unwrap(),
        Some(1),
        schema,
        partition_spec,
    )
    .build_v2_data();

    for i in 0..num_entries {
        let n = i as i64;
        let category = format!("c{}", i % 10);
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!(
                "s3://bucket/warehouse/db/table/data/category={category}/{i:08}-0-6f1e1a2b-3c4d-5e6f-7a8b-9c0d1e2f3a4b.parquet"
            ))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::from_iter([Some(Literal::string(category))]))
            .record_count(10_000 + i)
            .file_size_in_bytes(1_000_000 + i)
            .column_sizes(columns().map(|(id, _)| (id, 1000 + id as u64 + i)).collect())
            .value_counts(columns().map(|(id, _)| (id, 10_000 + i)).collect())
            .null_value_counts(columns().map(|(id, _)| (id, i % 7)).collect())
            .nan_value_counts(HashMap::from([(5, 0), (6, 0)]))
            .lower_bounds(
                columns()
                    .map(|(id, ty)| (id, bound(ty, n * 13 + id as i64)))
                    .collect(),
            )
            .upper_bounds(
                columns()
                    .map(|(id, ty)| (id, bound(ty, n * 17 + id as i64 + 1000)))
                    .collect(),
            )
            .split_offsets(Some(vec![4]))
            .sort_order_id(0)
            .build()
            .unwrap();
        writer.add_file(data_file, 1).unwrap();
    }

    futures::executor::block_on(async {
        writer.write_manifest_file().await.unwrap();
        file_io.new_input(path).unwrap().read().await.unwrap()
    })
    .to_vec()
}

/// Rewrites `bytes` without the `data_file` fields that PyIceberg doesn't
/// write, so the writer schema differs from the reader schema.
fn without_optional_fields(bytes: &[u8]) -> Vec<u8> {
    const OMITTED: [&str; 3] = ["first_row_id", "content_offset", "content_size_in_bytes"];
    let retain = |fields: &mut Vec<serde_json::Value>| {
        fields.retain(|f| !OMITTED.contains(&f["name"].as_str().unwrap()))
    };

    let reader = apache_avro::Reader::new(bytes).unwrap();
    let mut schema = serde_json::to_value(reader.writer_schema()).unwrap();
    for field in schema["fields"].as_array_mut().unwrap() {
        if field["name"] == "data_file" {
            retain(field["type"]["fields"].as_array_mut().unwrap());
        }
    }
    let schema = apache_avro::Schema::parse(&schema).unwrap();
    let mut writer = apache_avro::Writer::new(&schema, Vec::new()).unwrap();
    for (key, value) in reader.user_metadata() {
        writer.add_user_metadata(key.clone(), value).unwrap();
    }
    for entry in reader {
        let Value::Record(mut entry) = entry.unwrap() else {
            unreachable!()
        };
        for (name, data_file) in &mut entry {
            if let ("data_file", Value::Record(fields)) = (name.as_str(), data_file) {
                fields.retain(|(name, _)| !OMITTED.contains(&name.as_str()));
            }
        }
        writer.append_value(Value::Record(entry)).unwrap();
    }
    writer.into_inner().unwrap()
}

fn bench_parse_avro(c: &mut Criterion) {
    let mut group = c.benchmark_group("manifest_parse_avro");
    group.sample_size(10);
    for num_entries in [1_000, 10_000, 50_000] {
        let bytes = write_manifest(num_entries);
        let cases = [
            ("fewer_writer_fields", without_optional_fields(&bytes)),
            ("same_writer_schema", bytes),
        ];
        assert_eq!(
            Manifest::parse_avro(&cases[0].1).unwrap(),
            Manifest::parse_avro(&cases[1].1).unwrap()
        );
        group.throughput(Throughput::Elements(num_entries));
        for (name, bytes) in &cases {
            group.bench_with_input(BenchmarkId::new(*name, num_entries), bytes, |b, bytes| {
                b.iter(|| Manifest::parse_avro(black_box(bytes)).unwrap())
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_parse_avro);
criterion_main!(benches);
