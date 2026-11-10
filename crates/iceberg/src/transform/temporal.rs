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

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};

use super::TransformFunction;
use crate::spec::{Datum, PrimitiveLiteral, PrimitiveType};
use crate::{Error, ErrorKind, Result};

/// Microseconds in one hour.
const MICROSECONDS_PER_HOUR: i64 = 3_600_000_000;
/// Nanoseconds in one hour.
const NANOSECONDS_PER_HOUR: i64 = 3_600_000_000_000;
/// Year of unix epoch.
const UNIX_EPOCH_YEAR: i32 = 1970;
/// Microseconds in one day.
const MICROSECONDS_PER_DAY: i64 = 86_400_000_000;
/// Nanoseconds in one day.
const NANOSECONDS_PER_DAY: i64 = 86_400_000_000_000;
/// Milliseconds in one day.
const MILLISECONDS_PER_DAY: i64 = 86_400_000;
/// Seconds in one day.
const SECONDS_PER_DAY: i64 = 86_400;

/// Returns the `(year, month0)` of the civil date `days` days after 1970-01-01
/// in the proleptic Gregorian calendar.
///
/// This is defined for every `i32` day offset, unlike `chrono`, which stops
/// around ±262,000 years. `date` values span ±5.9 million years and
/// microsecond timestamps ±292,000 years, so `chrono` cannot represent all
/// inputs of the `year` and `month` transforms.
fn civil_year_month(days: i64) -> (i64, i64) {
    // Howard Hinnant's `civil_from_days`
    // <https://howardhinnant.github.io/date_algorithms.html#civil_from_days>
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };
    // `month0` is zero based, matching `chrono`'s `month0()`.
    (year, month - 1)
}

/// Years since 1970 of the day `days` days after 1970-01-01.
#[inline]
fn year_from_days(days: i64) -> i32 {
    (civil_year_month(days).0 - i64::from(UNIX_EPOCH_YEAR)) as i32
}

/// Months since 1970-01 of the day `days` days after 1970-01-01.
#[inline]
fn month_from_days(days: i64) -> i32 {
    let (year, month0) = civil_year_month(days);
    ((year - i64::from(UNIX_EPOCH_YEAR)) * 12 + month0) as i32
}

/// Extract a date or timestamp year, as years from 1970
#[derive(Debug)]
pub struct Year;

impl Year {
    #[inline]
    fn timestamp_to_year_micros(timestamp: i64) -> i32 {
        year_from_days(timestamp.div_euclid(MICROSECONDS_PER_DAY))
    }

    #[inline]
    fn timestamp_to_year_nanos(timestamp: i64) -> i32 {
        year_from_days(timestamp.div_euclid(NANOSECONDS_PER_DAY))
    }
}

impl TransformFunction for Year {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Int32Array = match input.data_type() {
            DataType::Date32 => input
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .unary(|v| -> i32 { year_from_days(i64::from(v)) }),
            DataType::Timestamp(TimeUnit::Second, _) => input
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap()
                .unary(|v| -> i32 { year_from_days(v.div_euclid(SECONDS_PER_DAY)) }),
            DataType::Timestamp(TimeUnit::Millisecond, _) => input
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .unary(|v| -> i32 { year_from_days(v.div_euclid(MILLISECONDS_PER_DAY)) }),
            DataType::Timestamp(TimeUnit::Microsecond, _) => input
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .unary(|v| -> i32 { Self::timestamp_to_year_micros(v) }),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => input
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .unary(|v| -> i32 { Self::timestamp_to_year_nanos(v) }),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Should not call internally for unsupported data type {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Arc::new(res))
    }

    fn transform_literal(&self, input: &Datum) -> Result<Option<Datum>> {
        let val = match (input.data_type(), input.literal()) {
            (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => year_from_days(i64::from(*v)),
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_year_micros(*v)
            }
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_year_micros(*v)
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_year_nanos(*v)
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_year_nanos(*v)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for year transform: {:?}",
                        input.data_type()
                    ),
                ));
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
    fn timestamp_to_month_micros(timestamp: i64) -> i32 {
        month_from_days(timestamp.div_euclid(MICROSECONDS_PER_DAY))
    }

    #[inline]
    fn timestamp_to_month_nanos(timestamp: i64) -> i32 {
        month_from_days(timestamp.div_euclid(NANOSECONDS_PER_DAY))
    }
}

impl TransformFunction for Month {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Int32Array = match input.data_type() {
            DataType::Date32 => input
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .unary(|v| -> i32 { month_from_days(i64::from(v)) }),
            DataType::Timestamp(TimeUnit::Second, _) => input
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap()
                .unary(|v| -> i32 { month_from_days(v.div_euclid(SECONDS_PER_DAY)) }),
            DataType::Timestamp(TimeUnit::Millisecond, _) => input
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .unary(|v| -> i32 { month_from_days(v.div_euclid(MILLISECONDS_PER_DAY)) }),
            DataType::Timestamp(TimeUnit::Microsecond, _) => input
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .unary(|v| -> i32 { Self::timestamp_to_month_micros(v) }),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => input
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .unary(|v| -> i32 { Self::timestamp_to_month_nanos(v) }),
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Should not call internally for unsupported data type {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Arc::new(res))
    }

    fn transform_literal(&self, input: &Datum) -> Result<Option<Datum>> {
        let val = match (input.data_type(), input.literal()) {
            (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => month_from_days(i64::from(*v)),
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_month_micros(*v)
            }
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_month_micros(*v)
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_month_nanos(*v)
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Self::timestamp_to_month_nanos(*v)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for month transform: {:?}",
                        input.data_type()
                    ),
                ));
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
    fn day_timestamp_micro(v: i64) -> i32 {
        v.div_euclid(MICROSECONDS_PER_DAY) as i32
    }

    #[inline]
    fn day_timestamp_nano(v: i64) -> i32 {
        v.div_euclid(NANOSECONDS_PER_DAY) as i32
    }
}

impl TransformFunction for Day {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Date32Array = match input.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => input
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .unary(|v| -> i32 { Self::day_timestamp_micro(v) }),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => input
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .unary(|v| -> i32 { Self::day_timestamp_nano(v) }),
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
                ));
            }
        };
        Ok(Arc::new(res))
    }

    fn transform_literal(&self, input: &Datum) -> Result<Option<Datum>> {
        let val = match (input.data_type(), input.literal()) {
            (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => *v,
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => Self::day_timestamp_micro(*v),
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Self::day_timestamp_micro(*v)
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => Self::day_timestamp_nano(*v),
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Self::day_timestamp_nano(*v)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for day transform: {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Some(Datum::date(val)))
    }
}

/// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
#[derive(Debug)]
pub struct Hour;

impl Hour {
    #[inline]
    fn hour_timestamp_micro(v: i64) -> i32 {
        v.div_euclid(MICROSECONDS_PER_HOUR) as i32
    }

    #[inline]
    fn hour_timestamp_nano(v: i64) -> i32 {
        v.div_euclid(NANOSECONDS_PER_HOUR) as i32
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
            DataType::Timestamp(TimeUnit::Nanosecond, _) => input
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .unary(|v| -> i32 { Self::hour_timestamp_nano(v) }),
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for hour transform: {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Arc::new(res))
    }

    fn transform_literal(&self, input: &Datum) -> Result<Option<Datum>> {
        let val = match (input.data_type(), input.literal()) {
            (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => Self::hour_timestamp_micro(*v),
            (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => {
                Self::hour_timestamp_micro(*v)
            }
            (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(v)) => {
                Self::hour_timestamp_nano(*v)
            }
            (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(v)) => {
                Self::hour_timestamp_nano(*v)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Unsupported data type for hour transform: {:?}",
                        input.data_type()
                    ),
                ));
            }
        };
        Ok(Some(Datum::int(val)))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray, TimestampNanosecondArray,
    };
    use chrono::{NaiveDate, NaiveDateTime};

    use crate::Result;
    use crate::expr::PredicateOperator;
    use crate::spec::PrimitiveType::{
        Binary, Date, Decimal, Fixed, Int, Long, String as StringType, Time, Timestamp,
        TimestampNs, Timestamptz, TimestamptzNs, Uuid,
    };
    use crate::spec::Type::{Primitive, Struct};
    use crate::spec::{Datum, NestedField, StructType, Transform};
    use crate::transform::test::{TestProjectionFixture, TestTransformFixture};
    use crate::transform::{BoxedTransformFunction, TransformFunction};

    #[test]
    fn test_year_transform() {
        let trans = Transform::Year;

        let fixture = TestTransformFixture {
            display: "year".to_string(),
            json: r#""year""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, false),
                (Transform::Day, false),
                (Transform::Hour, false),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), Some(Primitive(Int))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Int))),
                (Primitive(Timestamptz), Some(Primitive(Int))),
                (Primitive(TimestampNs), Some(Primitive(Int))),
                (Primitive(TimestamptzNs), Some(Primitive(Int))),
                (
                    Struct(StructType::new(vec![
                        NestedField::optional(1, "a", Primitive(Timestamp)).into(),
                    ])),
                    None,
                ),
            ],
        };

        fixture.assert_transform(trans);
    }

    #[test]
    fn test_month_transform() {
        let trans = Transform::Month;

        let fixture = TestTransformFixture {
            display: "month".to_string(),
            json: r#""month""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, true),
                (Transform::Day, false),
                (Transform::Hour, false),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), Some(Primitive(Int))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Int))),
                (Primitive(Timestamptz), Some(Primitive(Int))),
                (Primitive(TimestampNs), Some(Primitive(Int))),
                (Primitive(TimestamptzNs), Some(Primitive(Int))),
                (
                    Struct(StructType::new(vec![
                        NestedField::optional(1, "a", Primitive(Timestamp)).into(),
                    ])),
                    None,
                ),
            ],
        };

        fixture.assert_transform(trans);
    }

    #[test]
    fn test_day_transform() {
        let trans = Transform::Day;

        let fixture = TestTransformFixture {
            display: "day".to_string(),
            json: r#""day""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, true),
                (Transform::Day, true),
                (Transform::Hour, false),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), Some(Primitive(Date))),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Date))),
                (Primitive(Timestamptz), Some(Primitive(Date))),
                (Primitive(TimestampNs), Some(Primitive(Date))),
                (Primitive(TimestamptzNs), Some(Primitive(Date))),
                (
                    Struct(StructType::new(vec![
                        NestedField::optional(1, "a", Primitive(Timestamp)).into(),
                    ])),
                    None,
                ),
            ],
        };

        fixture.assert_transform(trans);
    }

    #[test]
    fn test_hour_transform() {
        let trans = Transform::Hour;

        let fixture = TestTransformFixture {
            display: "hour".to_string(),
            json: r#""hour""#.to_string(),
            dedup_name: "time".to_string(),
            preserves_order: true,
            satisfies_order_of: vec![
                (Transform::Year, true),
                (Transform::Month, true),
                (Transform::Day, true),
                (Transform::Hour, true),
                (Transform::Void, false),
                (Transform::Identity, false),
            ],
            trans_types: vec![
                (Primitive(Binary), None),
                (Primitive(Date), None),
                (
                    Primitive(Decimal {
                        precision: 8,
                        scale: 5,
                    }),
                    None,
                ),
                (Primitive(Fixed(8)), None),
                (Primitive(Int), None),
                (Primitive(Long), None),
                (Primitive(StringType), None),
                (Primitive(Uuid), None),
                (Primitive(Time), None),
                (Primitive(Timestamp), Some(Primitive(Int))),
                (Primitive(Timestamptz), Some(Primitive(Int))),
                (Primitive(TimestampNs), Some(Primitive(Int))),
                (Primitive(TimestamptzNs), Some(Primitive(Int))),
                (
                    Struct(StructType::new(vec![
                        NestedField::optional(1, "a", Primitive(Timestamp)).into(),
                    ])),
                    None,
                ),
            ],
        };

        fixture.assert_transform(trans);
    }

    #[test]
    fn test_projection_timestamp_hour_upper_bound() -> Result<()> {
        // 420034
        let value = "2017-12-01T10:59:59.999999";
        // 412007
        let another = "2016-12-31T23:59:59.999999";

        let fixture = TestProjectionFixture::new(
            Transform::Hour,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 420035"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (420034, 412007)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_hour_lower_bound() -> Result<()> {
        // 420034
        let value = "2017-12-01T10:00:00.000000";
        // 411288
        let another = "2016-12-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Hour,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 420033"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 420034"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (411288, 420034)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_year_upper_bound() -> Result<()> {
        let value = "2017-12-31T23:59:59.999999";
        let another = "2016-12-31T23:59:59.999999";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 48"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (47, 46)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_year_lower_bound() -> Result<()> {
        let value = "2017-01-01T00:00:00.000000";
        let another = "2016-12-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 46"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (47, 46)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_negative_upper_bound() -> Result<()> {
        let value = "1969-12-31T23:59:59.999999";
        let another = "1970-01-01T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= -1"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name IN (-1, 0)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (0, -1)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_upper_bound() -> Result<()> {
        let value = "2017-12-01T23:59:59.999999";
        let another = "2017-11-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (575, 574)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;
        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_negative_lower_bound() -> Result<()> {
        let value = "1969-01-01T00:00:00.000000";
        let another = "1969-03-01T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= -11"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name IN (-12, -11)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (-10, -9, -12, -11)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_month_lower_bound() -> Result<()> {
        let value = "2017-12-01T00:00:00.000000";
        let another = "2017-12-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 574"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (575)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_negative_upper_bound() -> Result<()> {
        // -1
        let value = "1969-12-31T23:59:59.999999";
        // 0
        let another = "1970-01-01T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1969-12-31"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name IN (1969-12-31, 1970-01-01)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (1970-01-01, 1969-12-31)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_types_day_negative() -> Result<()> {
        // 1969-12-30T23:59:59.5
        let micros = -86_400_500_000;
        for (field_type, value) in [
            (Timestamp, Datum::timestamp_micros(micros)),
            (Timestamptz, Datum::timestamptz_micros(micros)),
            (TimestampNs, Datum::timestamp_nanos(micros * 1_000)),
            (TimestamptzNs, Datum::timestamptz_nanos(micros * 1_000)),
        ] {
            let fixture = TestProjectionFixture::new(
                Transform::Day,
                "name",
                NestedField::required(1, "value", Primitive(field_type)),
            );

            fixture.assert_projection(
                &fixture.binary_predicate(PredicateOperator::LessThan, value.clone()),
                Some("name <= 1969-12-31"),
            )?;

            fixture.assert_projection(
                &fixture.binary_predicate(PredicateOperator::LessThanOrEq, value.clone()),
                Some("name <= 1969-12-31"),
            )?;

            fixture.assert_projection(
                &fixture.binary_predicate(PredicateOperator::Eq, value.clone()),
                Some("name IN (1969-12-31, 1969-12-30)"),
            )?;

            fixture.assert_projection(
                &fixture.set_predicate(PredicateOperator::In, vec![value]),
                Some("name IN (1969-12-31, 1969-12-30)"),
            )?;
        }

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_upper_bound() -> Result<()> {
        // 17501
        let value = "2017-12-01T23:59:59.999999";
        // 17502
        let another = "2017-12-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 2017-12-02"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (2017-12-02, 2017-12-01)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_negative_lower_bound() -> Result<()> {
        // -365
        let value = "1969-01-01T00:00:00.000000";
        // -364
        let another = "1969-01-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1969-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1969-01-02"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1969-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1969-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name IN (1969-01-01, 1969-01-02)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (1969-01-02, 1969-01-01, 1969-01-03)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_lower_bound() -> Result<()> {
        // 17501
        let value = "2017-12-01T00:00:00.000000";
        // 17502
        let another = "2017-12-02T00:00:00.000000";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 2017-11-30"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 2017-12-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (2017-12-02, 2017-12-01)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_timestamp_day_epoch() -> Result<()> {
        // 0
        let value = "1970-01-01T00:00:00.00000";
        // 1
        let another = "1970-01-02T00:00:00.00000";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Primitive(Timestamp)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name <= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThan,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::timestamp_from_str(value)?,
            ),
            Some("name >= 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::timestamp_from_str(value)?),
            Some("name = 1970-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::timestamp_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            Some("name IN (1970-01-01, 1970-01-02)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::timestamp_from_str(value)?,
                Datum::timestamp_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_day_negative() -> Result<()> {
        // -2
        let value = "1969-12-30";
        // -4
        let another = "1969-12-28";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Primitive(Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 1969-12-29"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 1969-12-30"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 1969-12-31"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 1969-12-30"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 1969-12-30"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (1969-12-28, 1969-12-30)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_day() -> Result<()> {
        // 17167
        let value = "2017-01-01";
        // 17531
        let another = "2017-12-31";

        let fixture = TestProjectionFixture::new(
            Transform::Day,
            "name",
            NestedField::required(1, "value", Primitive(Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 2016-12-31"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 2017-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 2017-01-02"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 2017-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 2017-01-01"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (2017-01-01, 2017-12-31)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_negative_upper_bound() -> Result<()> {
        // -1 => 1969-12
        let value = "1969-12-31";
        // -12 => 1969-01
        let another = "1969-01-01";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Primitive(Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= -1"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name IN (-1, 0)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (-1, -12, -11, 0)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_upper_bound() -> Result<()> {
        // 575 => 2017-12
        let value = "2017-12-31";
        // 564 => 2017-01
        let another = "2017-01-01";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Primitive(Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 576"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (575, 564)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_negative_lower_bound() -> Result<()> {
        // -12 => 1969-01
        let value = "1969-01-01";
        // -1 => 1969-12
        let another = "1969-12-31";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Primitive(Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= -11"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= -12"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name IN (-12, -11)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (-1, -12, -11, 0)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_lower_bound() -> Result<()> {
        // 575 => 2017-12
        let value = "2017-12-01";
        // 564 => 2017-01
        let another = "2017-01-01";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Primitive(Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 574"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 575"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (575, 564)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_month_epoch() -> Result<()> {
        // 0 => 1970-01
        let value = "1970-01-01";
        // -1 => 1969-12
        let another = "1969-12-31";

        let fixture = TestProjectionFixture::new(
            Transform::Month,
            "name",
            NestedField::required(1, "value", Primitive(Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (0, -1)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_negative_upper_bound() -> Result<()> {
        // -1 => 1969
        let value = "1969-12-31";
        let another = "1969-01-01";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Primitive(Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= -1"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name IN (-1, 0)"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (0, -1)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_upper_bound() -> Result<()> {
        // 47 => 2017
        let value = "2017-12-31";
        // 46 => 2016
        let another = "2016-01-01";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Primitive(Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 48"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (47, 46)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_negative_lower_bound() -> Result<()> {
        // 0 => 1970
        let value = "1970-01-01";
        // -1 => 1969
        let another = "1969-12-31";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Primitive(Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 0"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (0, -1)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_projection_date_year_lower_bound() -> Result<()> {
        // 47 => 2017
        let value = "2017-01-01";
        // 46 => 2016
        let another = "2016-12-31";

        let fixture = TestProjectionFixture::new(
            Transform::Year,
            "name",
            NestedField::required(1, "value", Primitive(Date)),
        );

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::LessThan, Datum::date_from_str(value)?),
            Some("name <= 46"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::LessThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name <= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::GreaterThan, Datum::date_from_str(value)?),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(
                PredicateOperator::GreaterThanOrEq,
                Datum::date_from_str(value)?,
            ),
            Some("name >= 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::Eq, Datum::date_from_str(value)?),
            Some("name = 47"),
        )?;

        fixture.assert_projection(
            &fixture.binary_predicate(PredicateOperator::NotEq, Datum::date_from_str(value)?),
            None,
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::In, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            Some("name IN (47, 46)"),
        )?;

        fixture.assert_projection(
            &fixture.set_predicate(PredicateOperator::NotIn, vec![
                Datum::date_from_str(value)?,
                Datum::date_from_str(another)?,
            ]),
            None,
        )?;

        Ok(())
    }

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
        let timestamp = Datum::timestamp_from_str(time).unwrap();
        let timestamp_tz = Datum::timestamptz_from_str(time.to_owned() + " +00:00").unwrap();
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

    fn test_timestamp_ns_and_tz_transform(
        time: &str,
        transform: &BoxedTransformFunction,
        expect: Datum,
    ) {
        let timestamp_ns = Datum::timestamp_from_str(time).unwrap();
        let timestamptz_ns = Datum::timestamptz_from_str(time.to_owned() + " +00:00").unwrap();
        let res = transform.transform_literal(&timestamp_ns).unwrap().unwrap();
        assert_eq!(res, expect);
        let res = transform
            .transform_literal(&timestamptz_ns)
            .unwrap()
            .unwrap();
        assert_eq!(res, expect);
    }

    fn test_timestamp_ns_and_tz_transform_using_i64(
        time: i64,
        transform: &BoxedTransformFunction,
        expect: Datum,
    ) {
        let timestamp_ns = Datum::timestamp_nanos(time);
        let timestamptz_ns = Datum::timestamptz_nanos(time);
        let res = transform.transform_literal(&timestamp_ns).unwrap().unwrap();
        assert_eq!(res, expect);
        let res = transform
            .transform_literal(&timestamptz_ns)
            .unwrap()
            .unwrap();
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
        test_timestamp_and_tz_transform("1969-01-01T00:00:00.000000", &year, Datum::int(-1));

        // Test TimestampNanosecond
        test_timestamp_ns_and_tz_transform_using_i64(
            186280000000,
            &year,
            Datum::int(1970 - super::UNIX_EPOCH_YEAR),
        );
        test_timestamp_ns_and_tz_transform("1969-01-01T00:00:00.000000", &year, Datum::int(-1));
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
        test_timestamp_and_tz_transform("1969-12-01T23:00:00.000000", &month, Datum::int(-1));
        test_timestamp_and_tz_transform("2017-12-01T00:00:00.000000", &month, Datum::int(575));
        test_timestamp_and_tz_transform("1970-01-01T00:00:00.000000", &month, Datum::int(0));
        test_timestamp_and_tz_transform("1969-12-31T00:00:00.000000", &month, Datum::int(-1));

        // Test TimestampNanosecond
        test_timestamp_ns_and_tz_transform_using_i64(
            186280000000,
            &month,
            Datum::int((1970 - super::UNIX_EPOCH_YEAR) * 12),
        );
        test_timestamp_ns_and_tz_transform("1969-12-01T23:00:00.000000", &month, Datum::int(-1));
        test_timestamp_ns_and_tz_transform("2017-12-01T00:00:00.000000", &month, Datum::int(575));
        test_timestamp_ns_and_tz_transform("1970-01-01T00:00:00.000000", &month, Datum::int(0));
        test_timestamp_ns_and_tz_transform("1969-12-31T00:00:00.000000", &month, Datum::int(-1));
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
        let res = res.as_any().downcast_ref::<Date32Array>().unwrap();
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
        let res = res.as_any().downcast_ref::<Date32Array>().unwrap();
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
        test_date(18628, &day, Datum::date(18628));
        test_date(-31, &day, Datum::date(-31));

        // Test TimestampMicrosecond
        test_timestamp_and_tz_transform_using_i64(1512151975038194, &day, Datum::date(17501));
        test_timestamp_and_tz_transform_using_i64(-115200000000, &day, Datum::date(-2));
        test_timestamp_and_tz_transform("2017-12-01T10:30:42.123000", &day, Datum::date(17501));

        // Test TimestampNanosecond
        test_timestamp_ns_and_tz_transform_using_i64(1512151975038194, &day, Datum::date(17));
        test_timestamp_ns_and_tz_transform_using_i64(-115200000000, &day, Datum::date(-1));
        test_timestamp_ns_and_tz_transform("2017-12-01T10:30:42.123000", &day, Datum::date(17501));
    }

    #[test]
    fn test_transform_days_pre_epoch() {
        let day = Box::new(super::Day) as BoxedTransformFunction;
        let expected = [-1, -2, -2, -2, -2, -366, -365];

        let micros = vec![
            -500_000,            // 1969-12-31T23:59:59.500000
            -86_400_000_001,     // 1969-12-30T23:59:59.999999
            -86_400_000_002,     // 1969-12-30T23:59:59.999998
            -86_400_500_000,     // 1969-12-30T23:59:59.500000
            -86_401_000_000,     // 1969-12-30T23:59:59.000000
            -31_536_000_500_000, // 1968-12-31T23:59:59.500000
            -31_535_999_000_001, // 1969-01-01T00:00:00.999999, Iceberg Java gives -366
        ];
        let res = day
            .transform(Arc::new(TimestampMicrosecondArray::from(micros.clone())))
            .unwrap();
        let res = res.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(res.values(), &expected);
        for (v, d) in micros.into_iter().zip(expected) {
            test_timestamp_and_tz_transform_using_i64(v, &day, Datum::date(d));
        }

        let nanos = vec![
            -500_000_000,            // 1969-12-31T23:59:59.500000000
            -86_400_000_000_001,     // 1969-12-30T23:59:59.999999999
            -86_400_000_000_002,     // 1969-12-30T23:59:59.999999998
            -86_400_500_000_000,     // 1969-12-30T23:59:59.500000000
            -86_401_000_000_000,     // 1969-12-30T23:59:59.000000000
            -31_536_000_500_000_000, // 1968-12-31T23:59:59.500000000
            -31_535_999_000_000_001, // 1969-01-01T00:00:00.999999999, Iceberg Java gives -366
        ];
        let res = day
            .transform(Arc::new(TimestampNanosecondArray::from(nanos.clone())))
            .unwrap();
        let res = res.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(res.values(), &expected);
        for (v, d) in nanos.into_iter().zip(expected) {
            test_timestamp_ns_and_tz_transform_using_i64(v, &day, Datum::date(d));
        }
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

        // Test TimestampNanosecond with and without timezone
        let timestamp_nanos = vec![0, super::NANOSECONDS_PER_HOUR, -1];
        let date_arrays = [
            Arc::new(TimestampNanosecondArray::from(timestamp_nanos.clone())) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(timestamp_nanos).with_timezone_utc())
                as ArrayRef,
        ];
        for date_array in date_arrays {
            let res = hour.transform(date_array).unwrap();
            let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(res.values(), &[0, 1, -1]);
        }
    }

    #[test]
    fn test_transform_hours_literal() {
        let hour = Box::new(super::Hour) as BoxedTransformFunction;

        test_timestamp_and_tz_transform("2017-12-01T18:00:00.000000", &hour, Datum::int(420042));
        test_timestamp_and_tz_transform("1970-01-01T22:01:01.000000", &hour, Datum::int(22));
        test_timestamp_and_tz_transform("1969-12-31T23:00:00.000000", &hour, Datum::int(-1));
        test_timestamp_and_tz_transform("1969-12-31T22:01:01.000000", &hour, Datum::int(-2));
        test_timestamp_and_tz_transform("0022-05-01T22:01:01.000000", &hour, Datum::int(-17072906));

        // Test TimestampNanosecond
        test_timestamp_ns_and_tz_transform(
            "2017-12-01T18:00:00.0000000000",
            &hour,
            Datum::int(420042),
        );
        test_timestamp_ns_and_tz_transform("1969-12-31T23:00:00.0000000000", &hour, Datum::int(-1));
        test_timestamp_ns_and_tz_transform(
            "1900-05-01T22:01:01.0000000000",
            &hour,
            Datum::int(-610706),
        );
    }
}

#[cfg(test)]
mod out_of_range_tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray, TimestampNanosecondArray,
    };

    use super::*;

    fn years(transform: &dyn TransformFunction, array: ArrayRef) -> Vec<i32> {
        let res = transform.transform(array).unwrap();
        let res = res.as_any().downcast_ref::<Int32Array>().unwrap();
        (0..res.len()).map(|i| res.value(i)).collect()
    }

    #[test]
    fn year_and_month_of_date_type_extremes() {
        // `Date32` covers roughly ±5.9 million years, well past the range of
        // `chrono`; the transforms must not return null for these inputs.
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![i32::MAX, i32::MIN, 0]));

        assert_eq!(years(&Year, Arc::clone(&dates)), vec![
            5_879_610, -5_879_611, 0
        ]);
        assert_eq!(years(&Month, dates), vec![70_555_326, -70_555_327, 0]);
    }

    #[test]
    fn year_and_month_of_timestamp_type_extremes() {
        // `i64::MAX` microseconds is 294247-01-10, and `i64::MIN` is
        // -290308-12-22; both are outside the range of `chrono`.
        let timestamps: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX, i64::MIN, 0]));

        assert_eq!(years(&Year, Arc::clone(&timestamps)), vec![
            292_277, -292_278, 0
        ]);
        assert_eq!(years(&Month, timestamps), vec![3_507_324, -3_507_325, 0]);

        // i64 nanoseconds only spans ±292 years, but the same code path is used.
        let nanos: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![i64::MAX, i64::MIN, 0]));
        assert_eq!(years(&Year, Arc::clone(&nanos)), vec![292, -293, 0]);
        assert_eq!(years(&Month, nanos), vec![3_507, -3_508, 0]);
    }

    #[test]
    fn year_and_month_literals_at_type_extremes_do_not_panic() {
        assert_eq!(
            Year.transform_literal(&Datum::date(i32::MAX)).unwrap(),
            Some(Datum::int(5_879_610))
        );
        assert_eq!(
            Month.transform_literal(&Datum::date(i32::MIN)).unwrap(),
            Some(Datum::int(-70_555_327))
        );
        assert_eq!(
            Year.transform_literal(&Datum::timestamp_micros(i64::MAX))
                .unwrap(),
            Some(Datum::int(292_277))
        );
        assert_eq!(
            Month
                .transform_literal(&Datum::timestamptz_micros(i64::MIN))
                .unwrap(),
            Some(Datum::int(-3_507_325))
        );
    }
}
