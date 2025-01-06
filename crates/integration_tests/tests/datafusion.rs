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

use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
use iceberg::{Catalog, Result, TableIdent};
use iceberg_datafusion::compute_statistics;
use iceberg_integration_tests::set_test_fixture;

#[tokio::test]
async fn test_statistics() -> Result<()> {
    let fixture = set_test_fixture("datafusion_statistics").await;

    let catalog = fixture.rest_catalog;

    let table = catalog
        .load_table(
            &TableIdent::from_strs(["default", "test_positional_merge_on_read_double_deletes"])
                .unwrap(),
        )
        .await
        .unwrap();

    let stats = compute_statistics(&table, None).await?;

    assert_eq!(stats, Statistics {
        num_rows: Precision::Inexact(14),
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
    });

    Ok(())
}
