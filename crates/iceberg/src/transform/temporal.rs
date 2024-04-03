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
use crate::spec::{Datum, PrimitiveLiteral};
use crate::{Error, ErrorKind, Result};
use arrow_arith::temporal::DatePart;
use arrow_arith::{arity::binary, temporal::date_part};
use arrow_array::{
    types::Date32Type, Array, ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, Datelike, Duration};
use std::sync::Arc;

/// Hour in one second.
const HOUR_PER_SECOND: f64 = 1.0_f64 / 3600.0_f64;
/// Year of unix epoch.
const UNIX_EPOCH_YEAR: i32 = 1970;
/// One second in micros.
const MICROS_PER_SECOND: i64 = 1_000_000;

/// Extract a date or timestamp year, as years from 1970
#[derive(Debug)]
pub struct Year;

impl Year {
    #[inline]
    fn timestamp_to_year(timestamp: i64) -> Result<i32> {
        Ok(DateTime::from_timestamp_micros(timestamp)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Fail to convert timestamp to date in year transform",
                )
            })?
            .year()
            - UNIX_EPOCH_YEAR)
    }
}

impl TransformFunction for Year {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let array = date_part(&input, DatePart::Year)
            .map_err(|err| Error::new(ErrorKind::Unexpected, format!("{err}")))?;
        Ok(Arc::<Int32Array>::new(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .unary(|v| v - UNIX_EPOCH_YEAR),
        ))
    }

    fn transform_literal(&self, input: &crate::spec::Datum) -> Result<Option<crate::spec::Datum>> {
        let val = match input.literal() {
            PrimitiveLiteral::Date(v) => Date32Type::to_naive_date(*v).year() - UNIX_EPOCH_YEAR,
            PrimitiveLiteral::Timestamp(v) => Self::timestamp_to_year(*v)?,
            PrimitiveLiteral::TimestampTZ(v) => Self::timestamp_to_year(*v)?,
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for year transform: {:?}",
                        input.data_type()
                    ),
                ))
            }
        };
        Ok(Some(Datum::int(val)))
    }
}

/// Extract a date or timestamp month, as months from 1970-01-01
#[derive(Debug)]
pub struct Month;

impl Month {
    #[inline]
    fn timestamp_to_month(timestamp: i64) -> Result<i32> {
        // date: aaaa-aa-aa
        // unix epoch date: 1970-01-01
        // if date > unix epoch date, delta month = (aa - 1) + 12 * (aaaa-1970)
        // if date < unix epoch date, delta month = (12 - (aa - 1)) + 12 * (1970-aaaa-1)
        let date = DateTime::from_timestamp_micros(timestamp).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Fail to convert timestamp to date in month transform",
            )
        })?;
        let unix_epoch_date = DateTime::from_timestamp_micros(0)
            .expect("0 timestamp from unix epoch should be valid");
        if date > unix_epoch_date {
            Ok((date.month0() as i32) + 12 * (date.year() - UNIX_EPOCH_YEAR))
        } else {
            let delta = (12 - date.month0() as i32) + 12 * (UNIX_EPOCH_YEAR - date.year() - 1);
            Ok(-delta)
        }
    }
}

impl TransformFunction for Month {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let year_array = date_part(&input, DatePart::Year)
            .map_err(|err| Error::new(ErrorKind::Unexpected, format!("{err}")))?;
        let year_array: Int32Array = year_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .unary(|v| 12 * (v - UNIX_EPOCH_YEAR));
        let month_array = date_part(&input, DatePart::Month)
            .map_err(|err| Error::new(ErrorKind::Unexpected, format!("{err}")))?;
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

    fn transform_literal(&self, input: &crate::spec::Datum) -> Result<Option<crate::spec::Datum>> {
        let val = match input.literal() {
            PrimitiveLiteral::Date(v) => {
                (Date32Type::to_naive_date(*v).year() - UNIX_EPOCH_YEAR) * 12
                    + Date32Type::to_naive_date(*v).month0() as i32
            }
            PrimitiveLiteral::Timestamp(v) => Self::timestamp_to_month(*v)?,
            PrimitiveLiteral::TimestampTZ(v) => Self::timestamp_to_month(*v)?,
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for month transform: {:?}",
                        input.data_type()
                    ),
                ))
            }
        };
        Ok(Some(Datum::int(val)))
    }
}

/// Extract a date or timestamp day, as days from 1970-01-01
#[derive(Debug)]
pub struct Day;

impl Day {
    #[inline]
    fn day_timestamp_micro(v: i64) -> Result<i32> {
        let secs = v / MICROS_PER_SECOND;

        let (nanos, offset) = if v >= 0 {
            let nanos = (v.rem_euclid(MICROS_PER_SECOND) * 1_000) as u32;
            let offset = 0i64;
            (nanos, offset)
        } else {
            let v = v + 1;
            let nanos = (v.rem_euclid(MICROS_PER_SECOND) * 1_000) as u32;
            let offset = 1i64;
            (nanos, offset)
        };

        let delta = Duration::new(secs, nanos).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Failed to create 'TimeDelta' from seconds {} and nanos {}",
                    secs, nanos
                ),
            )
        })?;

        let days = (delta.num_days() - offset) as i32;

        Ok(days)
    }
}

impl TransformFunction for Day {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Int32Array = match input.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => input
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .try_unary(|v| -> Result<i32> { Self::day_timestamp_micro(v) })?,
            DataType::Date32 => input
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .unary(|v| -> i32 { v }),
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

    fn transform_literal(&self, input: &crate::spec::Datum) -> Result<Option<crate::spec::Datum>> {
        let val = match input.literal() {
            PrimitiveLiteral::Date(v) => *v,
            PrimitiveLiteral::Timestamp(v) => Self::day_timestamp_micro(*v)?,
            PrimitiveLiteral::TimestampTZ(v) => Self::day_timestamp_micro(*v)?,
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for day transform: {:?}",
                        input.data_type()
                    ),
                ))
            }
        };
        Ok(Some(Datum::int(val)))
    }
}

/// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
#[derive(Debug)]
pub struct Hour;

impl Hour {
    #[inline]
    fn hour_timestamp_micro(v: i64) -> i32 {
        (v as f64 / 1000.0 / 1000.0 * HOUR_PER_SECOND) as i32
    }
}

impl TransformFunction for Hour {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Int32Array = match input.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => input
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .unary(|v| -> i32 { Self::hour_timestamp_micro(v) }),
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for hour transform: {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Arc::new(res))
    }

    fn transform_literal(&self, input: &crate::spec::Datum) -> Result<Option<crate::spec::Datum>> {
        let val = match input.literal() {
            PrimitiveLiteral::Timestamp(v) => Self::hour_timestamp_micro(*v),
            PrimitiveLiteral::TimestampTZ(v) => Self::hour_timestamp_micro(*v),
            _ => {
                return Err(crate::Error::new(
                    crate::ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for hour transform: {:?}",
                        input.data_type()
                    ),
                ))
            }
        };
        Ok(Some(Datum::int(val)))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray};
    use chrono::{NaiveDate, NaiveDateTime};
    use std::sync::Arc;

    use crate::{
        spec::Datum,
        transform::{BoxedTransformFunction, TransformFunction},
    };

    #[test]
    fn test_transform_years() {
        let year = super::Year;

        // Test Date32
        let ori_date = vec![
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2030, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2060, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(1969, 1, 1).unwrap(),
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
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30);
        assert_eq!(res.value(2), 60);
        assert_eq!(res.value(3), 90);
        assert_eq!(res.value(4), -1);

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
            NaiveDateTime::parse_from_str("1969-01-01 00:00:00.00", "%Y-%m-%d %H:%M:%S.%f")
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
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30);
        assert_eq!(res.value(2), 60);
        assert_eq!(res.value(3), 90);
        assert_eq!(res.value(4), -1);
    }

    fn test_timestamp_and_tz_transform(
        time: &str,
        transform: &BoxedTransformFunction,
        expect: Datum,
    ) {
        let timestamp = Datum::timestamp_micros(
            NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M:%S.%f")
                .unwrap()
                .and_utc()
                .timestamp_micros(),
        );
        let timestamp_tz = Datum::timestamptz_micros(
            NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M:%S.%f")
                .unwrap()
                .and_utc()
                .timestamp_micros(),
        );
        let res = transform.transform_literal(&timestamp).unwrap().unwrap();
        assert_eq!(res, expect);
        let res = transform.transform_literal(&timestamp_tz).unwrap().unwrap();
        assert_eq!(res, expect);
    }

    fn test_timestamp_and_tz_transform_using_i64(
        time: i64,
        transform: &BoxedTransformFunction,
        expect: Datum,
    ) {
        let timestamp = Datum::timestamp_micros(time);
        let timestamp_tz = Datum::timestamptz_micros(time);
        let res = transform.transform_literal(&timestamp).unwrap().unwrap();
        assert_eq!(res, expect);
        let res = transform.transform_literal(&timestamp_tz).unwrap().unwrap();
        assert_eq!(res, expect);
    }

    fn test_date(date: i32, transform: &BoxedTransformFunction, expect: Datum) {
        let date = Datum::date(date);
        let res = transform.transform_literal(&date).unwrap().unwrap();
        assert_eq!(res, expect);
    }

    #[test]
    fn test_transform_year_literal() {
        let year = Box::new(super::Year) as BoxedTransformFunction;

        // Test Date32
        test_date(18628, &year, Datum::int(2021 - super::UNIX_EPOCH_YEAR));
        test_date(-365, &year, Datum::int(-1));

        // Test TimestampMicrosecond
        test_timestamp_and_tz_transform_using_i64(
            186280000000,
            &year,
            Datum::int(1970 - super::UNIX_EPOCH_YEAR),
        );
        test_timestamp_and_tz_transform("1969-01-01 00:00:00.00", &year, Datum::int(-1));
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
            NaiveDate::from_ymd_opt(1969, 12, 1).unwrap(),
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
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30 * 12 + 3);
        assert_eq!(res.value(2), 60 * 12 + 6);
        assert_eq!(res.value(3), 90 * 12 + 9);
        assert_eq!(res.value(4), -1);

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
            NaiveDateTime::parse_from_str("1969-12-01 00:00:00.00", "%Y-%m-%d %H:%M:%S.%f")
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
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), 0);
        assert_eq!(res.value(1), 30 * 12 + 3);
        assert_eq!(res.value(2), 60 * 12 + 6);
        assert_eq!(res.value(3), 90 * 12 + 9);
        assert_eq!(res.value(4), -1);
    }

    #[test]
    fn test_transform_month_literal() {
        let month = Box::new(super::Month) as BoxedTransformFunction;

        // Test Date32
        test_date(
            18628,
            &month,
            Datum::int((2021 - super::UNIX_EPOCH_YEAR) * 12),
        );
        test_date(-31, &month, Datum::int(-1));

        // Test TimestampMicrosecond
        test_timestamp_and_tz_transform_using_i64(
            186280000000,
            &month,
            Datum::int((1970 - super::UNIX_EPOCH_YEAR) * 12),
        );
        test_timestamp_and_tz_transform("1969-12-01 23:00:00.00", &month, Datum::int(-1));
        test_timestamp_and_tz_transform("2017-12-01 00:00:00.00", &month, Datum::int(575));
        test_timestamp_and_tz_transform("1970-01-01 00:00:00.00", &month, Datum::int(0));
        test_timestamp_and_tz_transform("1969-12-31 00:00:00.00", &month, Datum::int(-1));
    }

    #[test]
    fn test_transform_days() {
        let day = super::Day;
        let ori_date = vec![
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2000, 4, 1).unwrap(),
            NaiveDate::from_ymd_opt(2030, 7, 1).unwrap(),
            NaiveDate::from_ymd_opt(2060, 10, 1).unwrap(),
            NaiveDate::from_ymd_opt(1969, 12, 31).unwrap(),
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
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), expect_day[0]);
        assert_eq!(res.value(1), expect_day[1]);
        assert_eq!(res.value(2), expect_day[2]);
        assert_eq!(res.value(3), expect_day[3]);
        assert_eq!(res.value(4), -1);

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
            NaiveDateTime::parse_from_str("1969-12-31 00:00:00.00", "%Y-%m-%d %H:%M:%S.%f")
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
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), expect_day[0]);
        assert_eq!(res.value(1), expect_day[1]);
        assert_eq!(res.value(2), expect_day[2]);
        assert_eq!(res.value(3), expect_day[3]);
        assert_eq!(res.value(4), -1);
    }

    #[test]
    fn test_transform_days_literal() {
        let day = Box::new(super::Day) as BoxedTransformFunction;
        // Test Date32
        test_date(18628, &day, Datum::int(18628));
        test_date(-31, &day, Datum::int(-31));

        // Test TimestampMicrosecond
        test_timestamp_and_tz_transform_using_i64(1512151975038194, &day, Datum::int(17501));
        test_timestamp_and_tz_transform_using_i64(-115200000000, &day, Datum::int(-2));
        test_timestamp_and_tz_transform("2017-12-01 10:30:42.123", &day, Datum::int(17501));
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
            NaiveDateTime::parse_from_str("1969-12-31 23:00:00.00", "%Y-%m-%d %H:%M:%S.%f")
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
        assert_eq!(res.len(), 5);
        assert_eq!(res.value(0), expect_hour[0]);
        assert_eq!(res.value(1), expect_hour[1]);
        assert_eq!(res.value(2), expect_hour[2]);
        assert_eq!(res.value(3), expect_hour[3]);
        assert_eq!(res.value(4), -1);
    }

    #[test]
    fn test_transform_hours_literal() {
        let hour = Box::new(super::Hour) as BoxedTransformFunction;

        // Test TimestampMicrosecond
        test_timestamp_and_tz_transform("2017-12-01 18:00:00.00", &hour, Datum::int(420042));
        test_timestamp_and_tz_transform("1969-12-31 23:00:00.00", &hour, Datum::int(-1));
    }
}
