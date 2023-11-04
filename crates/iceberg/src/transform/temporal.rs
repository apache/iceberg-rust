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

use super::TransformFunction;
use crate::{Error, ErrorKind, Result};
use arrow_arith::{
    arity::binary,
    temporal::{month_dyn, year_dyn},
};
use arrow_array::{
    types::Date32Type, Array, ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::Datelike;
use std::sync::Arc;

/// The number of days since unix epoch.
const DAY_SINCE_UNIX_EPOCH: i32 = 719163;
/// Hour in one second.
const HOUR_PER_SECOND: f64 = 1.0_f64 / 3600.0_f64;
/// Day in one second.
const DAY_PER_SECOND: f64 = 1.0_f64 / 24.0_f64 / 3600.0_f64;
/// Year of unix epoch.
const UNIX_EPOCH_YEAR: i32 = 1970;

/// Extract a date or timestamp year, as years from 1970
#[derive(Debug)]
pub struct Year;

impl TransformFunction for Year {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let array =
            year_dyn(&input).map_err(|err| Error::new(ErrorKind::Unexpected, format!("{err}")))?;
        Ok(Arc::<Int32Array>::new(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .unary(|v| v - UNIX_EPOCH_YEAR),
        ))
    }
}

/// Extract a date or timestamp month, as months from 1970-01-01
#[derive(Debug)]
pub struct Month;

impl TransformFunction for Month {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let year_array =
            year_dyn(&input).map_err(|err| Error::new(ErrorKind::Unexpected, format!("{err}")))?;
        let year_array: Int32Array = year_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .unary(|v| 12 * (v - UNIX_EPOCH_YEAR));
        let month_array =
            month_dyn(&input).map_err(|err| Error::new(ErrorKind::Unexpected, format!("{err}")))?;
        Ok(Arc::<Int32Array>::new(
            binary(
                month_array.as_any().downcast_ref::<Int32Array>().unwrap(),
                year_array.as_any().downcast_ref::<Int32Array>().unwrap(),
                // Compute month from 1970-01-01, so minus 1 here.
                |a, b| a + b - 1,
            )
            .unwrap(),
        ))
    }
}

/// Extract a date or timestamp day, as days from 1970-01-01
#[derive(Debug)]
pub struct Day;

impl TransformFunction for Day {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Int32Array = match input.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => input
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .unary(|v| -> i32 { (v as f64 / 1000.0 / 1000.0 * DAY_PER_SECOND) as i32 }),
            DataType::Date32 => {
                input
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        Date32Type::to_naive_date(v).num_days_from_ce() - DAY_SINCE_UNIX_EPOCH
                    })
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Should not call internally for unsupported data type {:?}",
                        input.data_type()
                    ),
                ))
            }
        };
        Ok(Arc::new(res))
    }
}

/// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
#[derive(Debug)]
pub struct Hour;

impl TransformFunction for Hour {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Int32Array = match input.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => input
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .unary(|v| -> i32 { (v as f64 * HOUR_PER_SECOND / 1000.0 / 1000.0) as i32 }),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Should not call internally for unsupported data type {:?}",
                        input.data_type()
                    ),
                ))
            }
        };
        Ok(Arc::new(res))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray};
    use chrono::{NaiveDate, NaiveDateTime};
    use std::sync::Arc;

    use crate::transform::TransformFunction;

    #[test]
    fn test_transform_years() {
        let year = super::Year;

        // Test Date32
        let ori_date = vec![
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2030, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2060, 1, 1).unwrap(),
        ];
        let date_array: ArrayRef = Arc::new(Date32Array::from(
            ori_date
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32
                })
                .collect::<Vec<i32>>(),
        ));
        let res = year.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 4);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30);
        assert_eq!(res.value(2), 60);
        assert_eq!(res.value(3), 90);

        // Test TimestampMicrosecond
        let ori_timestamp = vec![
            NaiveDateTime::parse_from_str("1970-01-01 12:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2000-01-01 19:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2030-01-01 10:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2060-01-01 11:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
        ];
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_timestamp
                .into_iter()
                .map(|timestamp| {
                    timestamp
                        .signed_duration_since(
                            NaiveDateTime::parse_from_str(
                                "1970-01-01 00:00:00.0",
                                "%Y-%m-%d %H:%M:%S.%f",
                            )
                            .unwrap(),
                        )
                        .num_microseconds()
                        .unwrap()
                })
                .collect::<Vec<i64>>(),
        ));
        let res = year.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 4);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30);
        assert_eq!(res.value(2), 60);
        assert_eq!(res.value(3), 90);
    }

    #[test]
    fn test_transform_months() {
        let month = super::Month;

        // Test Date32
        let ori_date = vec![
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2000, 4, 1).unwrap(),
            NaiveDate::from_ymd_opt(2030, 7, 1).unwrap(),
            NaiveDate::from_ymd_opt(2060, 10, 1).unwrap(),
        ];
        let date_array: ArrayRef = Arc::new(Date32Array::from(
            ori_date
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32
                })
                .collect::<Vec<i32>>(),
        ));
        let res = month.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 4);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30 * 12 + 3);
        assert_eq!(res.value(2), 60 * 12 + 6);
        assert_eq!(res.value(3), 90 * 12 + 9);

        // Test TimestampMicrosecond
        let ori_timestamp = vec![
            NaiveDateTime::parse_from_str("1970-01-01 12:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2000-04-01 19:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2030-07-01 10:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2060-10-01 11:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
        ];
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_timestamp
                .into_iter()
                .map(|timestamp| {
                    timestamp
                        .signed_duration_since(
                            NaiveDateTime::parse_from_str(
                                "1970-01-01 00:00:00.0",
                                "%Y-%m-%d %H:%M:%S.%f",
                            )
                            .unwrap(),
                        )
                        .num_microseconds()
                        .unwrap()
                })
                .collect::<Vec<i64>>(),
        ));
        let res = month.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 4);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30 * 12 + 3);
        assert_eq!(res.value(2), 60 * 12 + 6);
        assert_eq!(res.value(3), 90 * 12 + 9);
    }

    #[test]
    fn test_transform_days() {
        let day = super::Day;
        let ori_date = vec![
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2000, 4, 1).unwrap(),
            NaiveDate::from_ymd_opt(2030, 7, 1).unwrap(),
            NaiveDate::from_ymd_opt(2060, 10, 1).unwrap(),
        ];
        let expect_day = ori_date
            .clone()
            .into_iter()
            .map(|data| {
                data.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                    .num_days() as i32
            })
            .collect::<Vec<i32>>();

        // Test Date32
        let date_array: ArrayRef = Arc::new(Date32Array::from(
            ori_date
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32
                })
                .collect::<Vec<i32>>(),
        ));
        let res = day.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 4);
        assert_eq!(res.value(0), expect_day[0]);
        assert_eq!(res.value(1), expect_day[1]);
        assert_eq!(res.value(2), expect_day[2]);
        assert_eq!(res.value(3), expect_day[3]);

        // Test TimestampMicrosecond
        let ori_timestamp = vec![
            NaiveDateTime::parse_from_str("1970-01-01 12:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2000-04-01 19:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2030-07-01 10:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2060-10-01 11:30:42.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
        ];
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_timestamp
                .into_iter()
                .map(|timestamp| {
                    timestamp
                        .signed_duration_since(
                            NaiveDateTime::parse_from_str(
                                "1970-01-01 00:00:00.0",
                                "%Y-%m-%d %H:%M:%S.%f",
                            )
                            .unwrap(),
                        )
                        .num_microseconds()
                        .unwrap()
                })
                .collect::<Vec<i64>>(),
        ));
        let res = day.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 4);
        assert_eq!(res.value(0), expect_day[0]);
        assert_eq!(res.value(1), expect_day[1]);
        assert_eq!(res.value(2), expect_day[2]);
        assert_eq!(res.value(3), expect_day[3]);
    }

    #[test]
    fn test_transform_hours() {
        let hour = super::Hour;
        let ori_timestamp = vec![
            NaiveDateTime::parse_from_str("1970-01-01 19:01:23.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2000-03-01 12:01:23.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2030-10-02 10:01:23.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
            NaiveDateTime::parse_from_str("2060-09-01 05:03:23.123", "%Y-%m-%d %H:%M:%S.%f")
                .unwrap(),
        ];
        let expect_hour = ori_timestamp
            .clone()
            .into_iter()
            .map(|timestamp| {
                timestamp
                    .signed_duration_since(
                        NaiveDateTime::parse_from_str(
                            "1970-01-01 00:00:0.0",
                            "%Y-%m-%d %H:%M:%S.%f",
                        )
                        .unwrap(),
                    )
                    .num_hours() as i32
            })
            .collect::<Vec<i32>>();

        // Test TimestampMicrosecond
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_timestamp
                .into_iter()
                .map(|timestamp| {
                    timestamp
                        .signed_duration_since(
                            NaiveDateTime::parse_from_str(
                                "1970-01-01 00:00:0.0",
                                "%Y-%m-%d %H:%M:%S.%f",
                            )
                            .unwrap(),
                        )
                        .num_microseconds()
                        .unwrap()
                })
                .collect::<Vec<i64>>(),
        ));
        let res = hour.transform(date_array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(res.len(), 4);
        assert_eq!(res.value(0), expect_hour[0]);
        assert_eq!(res.value(1), expect_hour[1]);
        assert_eq!(res.value(2), expect_hour[2]);
        assert_eq!(res.value(3), expect_hour[3]);
    }
}
