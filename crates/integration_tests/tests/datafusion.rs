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
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use iceberg::{Catalog, TableIdent};
use iceberg_datafusion::IcebergTableProvider;
use iceberg_integration_tests::set_test_fixture;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

#[tokio::test]
async fn test_basic_queries() -> Result<(), DataFusionError> {
    let fixture = set_test_fixture("datafusion_basic_read").await;

    let catalog = fixture.rest_catalog;

    let table = catalog
        .load_table(&TableIdent::from_strs(["default", "types_test"]).unwrap())
        .await
        .unwrap();

    let ctx = SessionContext::new();

    let table_provider = Arc::new(
        IcebergTableProvider::try_new_from_table(table)
            .await
            .unwrap(),
    );

    let schema = table_provider.schema();

    assert_eq!(
        schema.as_ref(),
        &Schema::new(vec![
            Field::new("cboolean", DataType::Boolean, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("cint8", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("cint16", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
            Field::new("cint32", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "4".to_string(),
            )])),
            Field::new("cint64", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "5".to_string(),
            )])),
            Field::new("cfloat32", DataType::Float32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "6".to_string(),
            )])),
            Field::new("cfloat64", DataType::Float64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "7".to_string(),
            )])),
            Field::new("cdecimal", DataType::Decimal128(8, 2), true).with_metadata(HashMap::from(
                [(PARQUET_FIELD_ID_META_KEY.to_string(), "8".to_string(),)]
            )),
            Field::new("cdate32", DataType::Date32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "9".to_string(),
            )])),
            Field::new(
                "ctimestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "10".to_string(),
            )])),
            Field::new(
                "ctimestamptz",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+00:00"))),
                true
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "11".to_string(),
            )])),
            Field::new("cutf8", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "12".to_string(),
            )])),
            Field::new("cbinary", DataType::LargeBinary, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "13".to_string(),
            )])),
        ])
    );

    ctx.register_table("types_table", table_provider)?;

    let batches = ctx
        .sql("SELECT * FROM types_table LIMIT 3")
        .await?
        .collect()
        .await?;
    let expected = [
        "+----------+-------+--------+--------+--------+----------+----------+----------+------------+----------------------------+-----------------------------+-------+---------+",
        "| cboolean | cint8 | cint16 | cint32 | cint64 | cfloat32 | cfloat64 | cdecimal | cdate32    | ctimestamp                 | ctimestamptz                | cutf8 | cbinary |",
        "+----------+-------+--------+--------+--------+----------+----------+----------+------------+----------------------------+-----------------------------+-------+---------+",
        "| false    | -128  | 0      | 0      | 0      | 0.0      | 0.0      | 0.00     | 1970-01-01 | 1970-01-01T00:00:00        | 1970-01-01T00:00:00Z        | 0     | 30      |",
        "| true     | -127  | 1      | 1      | 1      | 1.0      | 1.0      | 0.01     | 1970-01-02 | 1970-01-01T00:00:00.000001 | 1970-01-01T00:00:00.000001Z | 1     | 31      |",
        "| false    | -126  | 2      | 2      | 2      | 2.0      | 2.0      | 0.02     | 1970-01-03 | 1970-01-01T00:00:00.000002 | 1970-01-01T00:00:00.000002Z | 2     | 32      |",
        "+----------+-------+--------+--------+--------+----------+----------+----------+------------+----------------------------+-----------------------------+-------+---------+",
    ];
    assert_batches_eq!(expected, &batches);

    // TODO: this isn't OK, and should be fixed with https://github.com/apache/iceberg-rust/issues/813
    let err = ctx
        .sql("SELECT cdecimal FROM types_table WHERE cint16 <= 2")
        .await?
        .collect()
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("Invalid comparison operation: Int16 <= Int32"));

    Ok(())
}
