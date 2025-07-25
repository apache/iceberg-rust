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

//! Integration tests for rest catalog.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};
use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray, MapArray, RecordBatch,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Fields};
use datafusion::common::assert_contains;
use iceberg::arrow::{DEFAULT_MAP_FIELD_NAME, UTC_TIME_ZONE};
use iceberg::spec::{LIST_FIELD_NAME, ListType, MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME, MapType, NestedField, PrimitiveType, Schema, StructType, Type, UnboundPartitionSpec, Transform, Struct, Literal, PrimitiveLiteral, UnboundPartitionField};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCreation};
use iceberg_catalog_rest::RestCatalog;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;
use crate::get_shared_containers;
use crate::shared_tests::random_ns;

#[tokio::test]
async fn test_spark_read_all_type() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());
    let ns = random_ns().await;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .with_fields(vec![
            // test all type
            NestedField::required(1, "int", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "long", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(3, "float", Type::Primitive(PrimitiveType::Float)).into(),
            NestedField::required(4, "double", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::required(
                5,
                "decimal",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 20,
                    scale: 5,
                }),
            )
                .into(),
            NestedField::required(6, "string", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(7, "boolean", Type::Primitive(PrimitiveType::Boolean)).into(),
            NestedField::required(8, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
            NestedField::required(9, "date", Type::Primitive(PrimitiveType::Date)).into(),
            NestedField::required(10, "timestamp", Type::Primitive(PrimitiveType::Timestamp))
                .into(),
            NestedField::required(11, "fixed", Type::Primitive(PrimitiveType::Fixed(10))).into(),
            NestedField::required(12, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(
                13,
                "timestamptz",
                Type::Primitive(PrimitiveType::Timestamptz),
            )
                .into(),
            NestedField::required(
                14,
                "struct",
                Type::Struct(StructType::new(vec![
                    NestedField::required(17, "int", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(18, "string", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])),
            )
                .into(),
            NestedField::required(
                15,
                "list",
                Type::List(ListType::new(
                    NestedField::list_element(19, Type::Primitive(PrimitiveType::Int), true).into(),
                )),
            )
                .into(),
            NestedField::required(
                16,
                "map",
                Type::Map(MapType::new(
                    NestedField::map_key_element(20, Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::map_value_element(
                        21,
                        Type::Primitive(PrimitiveType::String),
                        true,
                    )
                        .into(),
                )),
            )
                .into(),
        ])
        .build()
        .unwrap();


    let table_creation = TableCreation::builder()
        .name("rust_test_all_data_types".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Create the writer and write the data
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
    let mut data_file_writer = data_file_writer_builder.build().await.unwrap();

    // Prepare data
    let col1 = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let col2 = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let col3 = Float32Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
    let col4 = Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
    let col5 = Decimal128Array::from(vec![
        Some(1.into()),
        Some(2.into()),
        Some(3.into()),
        Some(4.into()),
        Some(5.into()),
    ])
        .with_data_type(DataType::Decimal128(20, 5));
    let col6 = StringArray::from(vec!["a", "b", "c", "d", "e"]);
    let col7 = BooleanArray::from(vec![true, false, true, false, true]);
    let col8 = LargeBinaryArray::from_opt_vec(vec![
        Some(b"a"),
        Some(b"b"),
        Some(b"c"),
        Some(b"d"),
        Some(b"e"),
    ]);
    let col9 = Date32Array::from(vec![1, 2, 3, 4, 5]);
    let col10 = TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5]);
    let col11 = FixedSizeBinaryArray::try_from_iter(
        vec![
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        ]
            .into_iter(),
    )
        .unwrap();
    let uuids = vec![
        Uuid::parse_str("0aed1a6e-1f03-473b-b2f8-808df2365106").unwrap(),
        Uuid::parse_str("8657c713-a9dd-41a6-829b-0774e3088808").unwrap(),
        Uuid::parse_str("ad50c31a-5ab5-4d2d-b574-4f60475fe59f").unwrap(),
        Uuid::parse_str("0fd0128b-3d68-48fc-9bef-9daa3bbb004f").unwrap(),
        Uuid::parse_str("1d0b5ee7-9da8-4fa1-b7a1-174c416c5781").unwrap(),
    ];

    let col12 = FixedSizeBinaryArray::try_from_iter(
        uuids.iter().map(|u| u.as_bytes().to_vec()),
    ).unwrap();
    let col13 = TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5]).with_timezone(UTC_TIME_ZONE);
    let col14 = StructArray::from(vec![
        (
            Arc::new(
                Field::new("int", DataType::Int32, false).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    17.to_string(),
                )])),
            ),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
        ),
        (
            Arc::new(
                Field::new("string", DataType::Utf8, false).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    18.to_string(),
                )])),
            ),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
        ),
    ]);
    let col15 = {
        let mut builder = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(
            Field::new(LIST_FIELD_NAME, DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                19.to_string(),
            )])),
        ));
        builder.append_value([Some(1), Some(2), Some(3), Some(4), Some(5)]);
        builder.append_value([Some(1), Some(2), Some(3), Some(4), Some(5)]);
        builder.append_value([Some(1), Some(2), Some(3), Some(4), Some(5)]);
        builder.append_value([Some(1), Some(2), Some(3), Some(4), Some(5)]);
        builder.append_value([Some(1), Some(2), Some(3), Some(4), Some(5)]);
        builder.finish()
    };
    let col16 = {
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::with_capacity(4);
        let mut builder = MapBuilder::new(None, int_builder, string_builder);
        builder.keys().append_value(1);
        builder.values().append_value("a");
        builder.append(true).unwrap();
        builder.keys().append_value(2);
        builder.values().append_value("b");
        builder.append(true).unwrap();
        builder.keys().append_value(3);
        builder.values().append_value("c");
        builder.append(true).unwrap();
        builder.keys().append_value(4);
        builder.values().append_value("d");
        builder.append(true).unwrap();
        builder.keys().append_value(5);
        builder.values().append_value("e");
        builder.append(true).unwrap();
        let array = builder.finish();
        let (_field, offsets, entries, nulls, ordered) = array.into_parts();
        let new_struct_fields = Fields::from(vec![
            Field::new(MAP_KEY_FIELD_NAME, DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), 20.to_string()),
            ])),
            Field::new(MAP_VALUE_FIELD_NAME, DataType::Utf8, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), 21.to_string()),
            ])),
        ]);
        let entries = {
            let (_, arrays, nulls) = entries.into_parts();
            StructArray::new(new_struct_fields.clone(), arrays, nulls)
        };
        let field = Arc::new(Field::new(
            DEFAULT_MAP_FIELD_NAME,
            DataType::Struct(new_struct_fields),
            false,
        ));
        MapArray::new(field, offsets, entries, nulls, ordered)
    };

    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
        Arc::new(col4) as ArrayRef,
        Arc::new(col5) as ArrayRef,
        Arc::new(col6) as ArrayRef,
        Arc::new(col7) as ArrayRef,
        Arc::new(col8) as ArrayRef,
        Arc::new(col9) as ArrayRef,
        Arc::new(col10) as ArrayRef,
        Arc::new(col11) as ArrayRef,
        Arc::new(col12) as ArrayRef,
        Arc::new(col13) as ArrayRef,
        Arc::new(col14) as ArrayRef,
        Arc::new(col15) as ArrayRef,
        Arc::new(col16) as ArrayRef,
    ])
        .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file.clone());
    let tx = append_action.apply(tx).unwrap();
    tx.commit(&rest_catalog).await.unwrap();

    let output = fixture.docker_compose.exec_in_container("spark-iceberg",["python", "./validation.py", "--sql", &format!("SELECT * FROM `{}`.rust_test_all_data_types", ns.name())]);
    let expected = r#"
+---+----+-----+------+-------+------+-------+------+----------+--------------------------+-------------------------------+------------------------------------+--------------------------+------+---------------+--------+
|int|long|float|double|decimal|string|boolean|binary|date      |timestamp                 |fixed                          |uuid                                |timestamptz               |struct|list           |map     |
+---+----+-----+------+-------+------+-------+------+----------+--------------------------+-------------------------------+------------------------------------+--------------------------+------+---------------+--------+
|1  |1   |1.1  |1.1   |0.00001|a     |true   |[61]  |1970-01-02|1970-01-01 00:00:00.000001|[01 02 03 04 05 06 07 08 09 0A]|0aed1a6e-1f03-473b-b2f8-808df2365106|1970-01-01 00:00:00.000001|{1, a}|[1, 2, 3, 4, 5]|{1 -> a}|
|2  |2   |2.2  |2.2   |0.00002|b     |false  |[62]  |1970-01-03|1970-01-01 00:00:00.000002|[01 02 03 04 05 06 07 08 09 0A]|8657c713-a9dd-41a6-829b-0774e3088808|1970-01-01 00:00:00.000002|{2, b}|[1, 2, 3, 4, 5]|{2 -> b}|
|3  |3   |3.3  |3.3   |0.00003|c     |true   |[63]  |1970-01-04|1970-01-01 00:00:00.000003|[01 02 03 04 05 06 07 08 09 0A]|ad50c31a-5ab5-4d2d-b574-4f60475fe59f|1970-01-01 00:00:00.000003|{3, c}|[1, 2, 3, 4, 5]|{3 -> c}|
|4  |4   |4.4  |4.4   |0.00004|d     |false  |[64]  |1970-01-05|1970-01-01 00:00:00.000004|[01 02 03 04 05 06 07 08 09 0A]|0fd0128b-3d68-48fc-9bef-9daa3bbb004f|1970-01-01 00:00:00.000004|{4, d}|[1, 2, 3, 4, 5]|{4 -> d}|
|5  |5   |5.5  |5.5   |0.00005|e     |true   |[65]  |1970-01-06|1970-01-01 00:00:00.000005|[01 02 03 04 05 06 07 08 09 0A]|1d0b5ee7-9da8-4fa1-b7a1-174c416c5781|1970-01-01 00:00:00.000005|{5, e}|[1, 2, 3, 4, 5]|{5 -> e}|
+---+----+-----+------+-------+------+-------+------+----------+--------------------------+-------------------------------+------------------------------------+--------------------------+------+---------------+--------+
    "#;
    assert_eq!(output.trim(), expected.trim());
}

#[tokio::test]
async fn test_spark_read_partitioned() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalog::new(fixture.catalog_config.clone());
    let ns = random_ns().await;
    let schema = crate::shared_tests::test_schema();

    let unbound_partition_spec = UnboundPartitionSpec::builder()
        .add_partition_fields(vec![
            UnboundPartitionField {
                name: "foo_truncate_5".to_string(),
                transform: Transform::Truncate(5),
                source_id: 1,
                field_id: Some(1000),
            },
            UnboundPartitionField {
                name: "bar_bucket_3".to_string(),
                transform: Transform::Bucket(3),
                source_id: 2,
                field_id: Some(1001),
            },
        ])
        .expect("could not add partition fields")
        .build();

    let partition_spec = unbound_partition_spec
        .bind(schema.clone())
        .expect("could not bind to schema");

    let table_creation = TableCreation::builder()
        .name("rust_partitioned_table".to_string())
        .schema(schema.clone())
        .partition_spec(partition_spec)
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );

    let values = vec![
        ("foo1", 100, true),
        ("foo2", 101, false),
        ("foo3", 102, true),
    ];

    for (foo, bar, baz) in values {
        let col1 = StringArray::from(vec![Some(foo)]);
        let col2 = Int32Array::from(vec![Some(bar)]);
        let col3 = BooleanArray::from(vec![Some(baz)]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(col1), Arc::new(col2), Arc::new(col3)],
        )
            .unwrap();

        let partition_struct = Struct::from_iter([
            Some(Literal::Primitive(PrimitiveLiteral::String(foo.to_string()))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(bar))),
        ]);

        let mut writer = DataFileWriterBuilder::new(
            parquet_writer_builder.clone(),
            Some(partition_struct),
            0,
        )
            .build()
            .await
            .unwrap();

        writer.write(batch).await.unwrap();
        let data_file = writer.close().await.unwrap();

        // commit result
        let tx = Transaction::new(&table);
        let append_action = tx.fast_append().add_data_files(data_file.clone());
        let tx = append_action.apply(tx).unwrap();
        tx.commit(&rest_catalog).await.unwrap();
    }

    let output = fixture.docker_compose.exec_in_container("spark-iceberg",["python", "./validation.py", "--sql", &format!("SELECT * FROM `{}`.rust_partitioned_table ORDER BY foo", ns.name())]);
    let data_expected = r#"
+----+---+-----+
|foo |bar|baz  |
+----+---+-----+
|foo1|100|true |
|foo2|101|false|
|foo3|102|true |
+----+---+-----+
    "#;
    assert_eq!(output.trim(), data_expected.trim());

    let output = fixture.docker_compose.exec_in_container(
        "spark-iceberg",
        ["python", "./validation.py", "--sql",
            &format!("SELECT partition, spec_id, record_count FROM `{}`.rust_partitioned_table.partitions ORDER BY partition", ns.name())
        ]);
    let partition_expected = r#"
+-----------+-------+------------+
|partition  |spec_id|record_count|
+-----------+-------+------------+
|{foo1, 100}|0      |1           |
|{foo2, 101}|0      |1           |
|{foo3, 102}|0      |1           |
+-----------+-------+------------+
    "#;
    assert_eq!(output.trim(), partition_expected.trim());

    let output = fixture.docker_compose.exec_in_container(
        "spark-iceberg",
        ["python", "./validation.py", "--sql",
            &format!("DESCRIBE TABLE EXTENDED `{}`.rust_partitioned_table", ns.name())
        ]);
    assert_contains!(output.trim(), "|Part 0                      |truncate(5, foo)");
    assert_contains!(output.trim(), "|Part 1                      |bucket(3, bar)");
    assert_contains!(output.trim(), "|_partition                  |struct<foo_truncate_5:string,bar_bucket_3:int>");
}
