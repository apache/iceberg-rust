use super::TransformFunction;
use crate::{Error, Result};
use arrow::array::{
    Array, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::binary;
use arrow::datatypes;
use arrow::datatypes::DataType;
use arrow::{
    array::{ArrayRef, Date32Array, Int32Array},
    compute::{month_dyn, year_dyn},
};
use chrono::Datelike;
use std::sync::Arc;

/// 719163 is the number of days from 0000-01-01 to 1970-01-01
const EPOCH_DAY_FROM_CE: i32 = 719163;
const DAY_PER_SECOND: f64 = 0.0000115741;
const HOUR_PER_SECOND: f64 = 1_f64 / 3600.0;

/// Extract a date or timestamp year, as years from 1970
pub struct Year;

impl TransformFunction for Year {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let array = year_dyn(&input).map_err(|err| {
            Error::new(
                crate::ErrorKind::ArrowCompute,
                format!("error in transformfunction: {}", err),
            )
        })?;
        Ok(Arc::<Int32Array>::new(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .unary(|v| v - 1970),
        ))
    }
}

/// Extract a date or timestamp month, as months from 1970-01-01
pub struct Month;

impl TransformFunction for Month {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let year_array = year_dyn(&input)
            .map_err(|err| Error::new(crate::ErrorKind::ArrowCompute, format!("{err}")))?;
        let year_array: Int32Array = year_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .unary(|v| 12 * (v - 1970));
        let month_array = month_dyn(&input)
            .map_err(|err| Error::new(crate::ErrorKind::ArrowCompute, format!("{err}")))?;
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
pub struct Day;

impl TransformFunction for Day {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Int32Array = match input.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                datatypes::TimeUnit::Second => input
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 { (v as f64 * DAY_PER_SECOND) as i32 }),
                datatypes::TimeUnit::Millisecond => input
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 { (v as f64 / 1000.0 * DAY_PER_SECOND) as i32 }),
                datatypes::TimeUnit::Microsecond => input
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 { (v as f64 / 1000.0 / 1000.0 * DAY_PER_SECOND) as i32 }),
                datatypes::TimeUnit::Nanosecond => input
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        (v as f64 / 1000.0 / 1000.0 / 1000.0 * DAY_PER_SECOND) as i32
                    }),
            },
            DataType::Date32 => {
                input
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        datatypes::Date32Type::to_naive_date(v).num_days_from_ce()
                            - EPOCH_DAY_FROM_CE
                    })
            }
            DataType::Date64 => {
                input
                    .as_any()
                    .downcast_ref::<Date64Array>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        datatypes::Date64Type::to_naive_date(v).num_days_from_ce()
                            - EPOCH_DAY_FROM_CE
                    })
            }
            _ => unreachable!(
                "Should not call transform in Day with type {:?}",
                input.data_type()
            ),
        };
        Ok(Arc::new(res))
    }
}

/// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
pub struct Hour;

impl TransformFunction for Hour {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Int32Array = match input.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                datatypes::TimeUnit::Second => input
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        println!("second: {}", v);
                        (v as f64 * HOUR_PER_SECOND) as i32
                    }),
                datatypes::TimeUnit::Millisecond => input
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        println!("mill: {}", v);
                        (v as f64 * HOUR_PER_SECOND / 1000.0) as i32
                    }),
                datatypes::TimeUnit::Microsecond => input
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        println!("micro: {}", v);
                        (v as f64 * HOUR_PER_SECOND / 1000.0 / 1000.0) as i32
                    }),
                datatypes::TimeUnit::Nanosecond => input
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        println!("nano: {}", v);
                        (v as f64 * HOUR_PER_SECOND / 1000.0 / 1000.0 / 1000.0) as i32
                    }),
            },
            _ => unreachable!(
                "Should not call transform in Day with type {:?}",
                input.data_type()
            ),
        };
        Ok(Arc::new(res))
    }
}

#[cfg(test)]
mod test {
    use arrow::array::{
        ArrayRef, Date32Array, Date64Array, Int32Array, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use chrono::NaiveDate;
    use std::sync::Arc;

    use crate::transform::TransformFunction;

    #[test]
    fn test_transform_years() {
        let year = super::Year;
        let ori_date = vec![
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2030, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2060, 1, 1).unwrap(),
        ];

        // Test Date32
        let date_array: ArrayRef = Arc::new(Date32Array::from(
            ori_date
                .clone()
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

        // Test Date64
        let date_array: ArrayRef = Arc::new(Date64Array::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_milliseconds()
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

        // Test TimestampSecond
        let date_array: ArrayRef = Arc::new(TimestampSecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_seconds()
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

        // Test TimestampMillisecond
        let date_array: ArrayRef = Arc::new(TimestampMillisecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_milliseconds()
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

        // Test TimestampMicrosecond
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
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

        // Test TimestampNanosecond
        let date_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(
            ori_date
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_nanoseconds()
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
        let ori_date = vec![
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2000, 4, 1).unwrap(),
            NaiveDate::from_ymd_opt(2030, 7, 1).unwrap(),
            NaiveDate::from_ymd_opt(2060, 10, 1).unwrap(),
        ];

        // Test Date32
        let date_array: ArrayRef = Arc::new(Date32Array::from(
            ori_date
                .clone()
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

        // Test Date64
        let date_array: ArrayRef = Arc::new(Date64Array::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_milliseconds()
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

        // Test TimestampSecond
        let date_array: ArrayRef = Arc::new(TimestampSecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_seconds()
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

        // Test TimestampMillisecond
        let date_array: ArrayRef = Arc::new(TimestampMillisecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_milliseconds()
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

        // Test TimestampMicrosecond
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
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

        // Test TimestampNanosecond
        let date_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(
            ori_date
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_nanoseconds()
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
                .clone()
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

        // Test Date64
        let date_array: ArrayRef = Arc::new(Date64Array::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_milliseconds()
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

        // Test TimestampSecond
        let date_array: ArrayRef = Arc::new(TimestampSecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_seconds()
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

        // Test TimestampMillisecond
        let date_array: ArrayRef = Arc::new(TimestampMillisecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_milliseconds()
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

        // Test TimestampMicrosecond
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
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

        // Test TimestampNanosecond
        let date_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(
            ori_date
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_nanoseconds()
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
        let ori_date = vec![
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2000, 4, 1).unwrap(),
            NaiveDate::from_ymd_opt(2030, 7, 1).unwrap(),
            NaiveDate::from_ymd_opt(2060, 10, 1).unwrap(),
        ];
        let expect_hour = ori_date
            .clone()
            .into_iter()
            .map(|data| {
                data.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                    .num_hours() as i32
            })
            .collect::<Vec<i32>>();

        // Test TimestampSecond
        let date_array: ArrayRef = Arc::new(TimestampSecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_seconds()
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

        // Test TimestampMillisecond
        let date_array: ArrayRef = Arc::new(TimestampMillisecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_milliseconds()
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

        // Test TimestampMicrosecond
        let date_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(
            ori_date
                .clone()
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
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

        // Test TimestampNanosecond
        let date_array: ArrayRef = Arc::new(TimestampNanosecondArray::from(
            ori_date
                .into_iter()
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_nanoseconds()
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
