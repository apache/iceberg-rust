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
use std::sync::Arc;

use arrow_schema::TimeUnit;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;
use datafusion::catalog::TableProvider;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;
use iceberg::{Catalog, Result, TableIdent};
use iceberg_datafusion::IcebergTableProvider;
use iceberg_integration_tests::set_test_fixture;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn test_basic_queries() -> Result<()> {
    let fixture = set_test_fixture("datafusion_basic_read").await;

    let catalog = fixture.rest_catalog;

    let table = catalog
        .load_table(&TableIdent::from_strs(["default", "types_test"]).unwrap())
        .await?;

    let ctx = SessionContext::new();

    let table_provider = Arc::new(IcebergTableProvider::try_new_from_table(table).await?);

    let schema = table_provider.schema();

    assert_eq!(
        schema.as_ref(),
        &Schema::new(vec![
            Field::new("cboolean", DataType::Boolean, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("ctinyint", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("csmallint", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
            Field::new("cint", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "4".to_string(),
            )])),
            Field::new("cbigint", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "5".to_string(),
            )])),
            Field::new("cfloat", DataType::Float32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "6".to_string(),
            )])),
            Field::new("cdouble", DataType::Float64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "7".to_string(),
            )])),
            Field::new("cdecimal", DataType::Decimal128(8, 2), true).with_metadata(HashMap::from(
                [(PARQUET_FIELD_ID_META_KEY.to_string(), "8".to_string(),)]
            )),
            Field::new("cdate", DataType::Date32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "9".to_string(),
            )])),
            Field::new(
                "ctimestamp_ntz",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "10".to_string(),
            )])),
            Field::new(
                "ctimestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+00:00"))),
                true
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "11".to_string(),
            )])),
            Field::new("cstring", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "12".to_string(),
            )])),
            Field::new("cbinary", DataType::LargeBinary, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "13".to_string(),
            )])),
        ])
    );

    ctx.register_table("types_table", table_provider).unwrap();

    let batches = ctx
        .sql("SELECT * FROM types_table ORDER BY cbigint LIMIT 3")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let expected = [
        "+----------+----------+-----------+------+---------+--------+---------+----------+------------+---------------------+----------------------+---------+----------+",
        "| cboolean | ctinyint | csmallint | cint | cbigint | cfloat | cdouble | cdecimal | cdate      | ctimestamp_ntz      | ctimestamp           | cstring | cbinary  |",
        "+----------+----------+-----------+------+---------+--------+---------+----------+------------+---------------------+----------------------+---------+----------+",
        "| false    | -128     | 0         | 0    | 0       | 0.0    | 0.0     | 0.00     | 1970-01-01 | 1970-01-01T00:00:00 | 1970-01-01T00:00:00Z | 0       | 00000000 |",
        "| true     | -127     | 1         | 1    | 1       | 1.0    | 1.0     | 0.01     | 1970-01-02 | 1970-01-01T00:00:01 | 1970-01-01T00:00:01Z | 1       | 00000001 |",
        "| false    | -126     | 2         | 2    | 2       | 2.0    | 2.0     | 0.02     | 1970-01-03 | 1970-01-01T00:00:02 | 1970-01-01T00:00:02Z | 2       | 00000002 |",
        "+----------+----------+-----------+------+---------+--------+---------+----------+------------+---------------------+----------------------+---------+----------+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_statistics() -> Result<()> {
    let fixture = set_test_fixture("datafusion_statistics").await;

    let catalog = fixture.rest_catalog;

    // Test table statistics
    let table = catalog
        .load_table(&TableIdent::from_strs([
            "default",
            "test_positional_merge_on_read_double_deletes",
        ])?)
        .await?;

    let table_provider = IcebergTableProvider::try_new_from_table(table)
        .await?
        .with_computed_statistics()
        .await;

    let table_stats = table_provider.statistics();

    assert_eq!(
        table_stats,
        Some(Statistics {
            num_rows: Precision::Inexact(12),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Inexact(0),
                    max_value: Precision::Inexact(ScalarValue::Date32(Some(19428))),
                    min_value: Precision::Inexact(ScalarValue::Date32(Some(19417))),
                    distinct_count: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Inexact(0),
                    max_value: Precision::Inexact(ScalarValue::Int32(Some(12))),
                    min_value: Precision::Inexact(ScalarValue::Int32(Some(1))),
                    distinct_count: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Inexact(0),
                    max_value: Precision::Inexact(ScalarValue::Utf8View(Some("l".to_string()))),
                    min_value: Precision::Inexact(ScalarValue::Utf8View(Some("a".to_string()))),
                    distinct_count: Precision::Absent,
                },
            ],
        })
    );

    // Test plan statistics with filtering
    let ctx = SessionContext::new();
    let scan = table_provider
        .scan(
            &ctx.state(),
            Some(&vec![1]),
            &[col("number").gt(lit(4))],
            None,
        )
        .await
        .unwrap();

    let plan_stats = scan.statistics().unwrap();

    // The estimate for the number of rows and the min value for the column are changed in response
    // to the filtration
    assert_eq!(plan_stats, Statistics {
        num_rows: Precision::Inexact(8),
        total_byte_size: Precision::Absent,
        column_statistics: vec![ColumnStatistics {
            null_count: Precision::Inexact(0),
            max_value: Precision::Inexact(ScalarValue::Int32(Some(12))),
            min_value: Precision::Inexact(ScalarValue::Int32(Some(5))),
            distinct_count: Precision::Absent,
        },],
    });

    Ok(())
}
