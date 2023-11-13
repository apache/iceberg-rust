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

use std::fmt::Formatter;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::de;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// A NewType wrapping DateTime<Utc> with millisecond precision
pub struct TimestampMillis(DateTime<Utc>);

impl TimestampMillis {
    /// Create new Timestamp
    pub fn new(milliseconds_since_epoch: i64) -> TimestampMillis {
        let one_second_in_millis = 1_000;
        let whole_seconds_since_epoch = milliseconds_since_epoch / one_second_in_millis;

        let remainder_in_millis = milliseconds_since_epoch % one_second_in_millis;
        let remainder_in_nanos = remainder_in_millis * 1_000_000;

        TimestampMillis(
            Utc.from_utc_datetime(
                // This shouldn't fail until the year 262000
                &NaiveDateTime::from_timestamp_opt(
                    whole_seconds_since_epoch,
                    remainder_in_nanos as u32,
                )
                .unwrap(),
            ),
        )
    }

    /// Returns the DateTime represented by this Timestamp
    pub fn to_date_time(self) -> DateTime<Utc> {
        self.0
    }
}

impl Serialize for TimestampMillis {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.0.timestamp_millis())
    }
}

struct I64Visitor;

impl<'de> Visitor<'de> for I64Visitor {
    type Value = i64;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("an integer between -(2^63) and (2^63)-1")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        i64::try_from(value).map_err(E::custom)
    }
}

impl<'de> Deserialize<'de> for TimestampMillis {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ms_since_epoch = deserializer.deserialize_any(I64Visitor)?;
        Ok(TimestampMillis::new(ms_since_epoch))
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::timestamp_millis::TimestampMillis;
    use chrono::{DateTime, Utc};

    #[test]
    fn new_should_handle_realistic_value() {
        let date_str = "2020-04-12T22:10:57.123+00:00";
        let datetime = DateTime::parse_from_rfc3339(date_str).unwrap();
        let datetime_utc = datetime.with_timezone(&Utc);
        let millis = datetime_utc.timestamp_millis();

        let date_time = TimestampMillis::new(millis).to_date_time();
        assert_eq!(1586729457, date_time.timestamp());
        assert_eq!(123 * 1_000_000, date_time.timestamp_subsec_nanos());
        assert_eq!(Utc, date_time.timezone());
    }

    #[test]
    fn new_should_handle_whole_seconds() {
        let date_time = TimestampMillis::new(1000).to_date_time();
        assert_eq!(1, date_time.timestamp());
        assert_eq!(0, date_time.timestamp_subsec_nanos());
        assert_eq!(Utc, date_time.timezone());
    }

    #[test]
    fn new_should_handle_sub_second() {
        let date_time = TimestampMillis::new(1).to_date_time();
        assert_eq!(0, date_time.timestamp());
        assert_eq!(1_000_000, date_time.timestamp_subsec_nanos());
        assert_eq!(Utc, date_time.timezone());
    }

    #[test]
    fn new_should_handle_millisecond_precision() {
        let date_time = TimestampMillis::new(1001).to_date_time();
        assert_eq!(1, date_time.timestamp());
        assert_eq!(1_000_000, date_time.timestamp_subsec_nanos());
        assert_eq!(Utc, date_time.timezone());
    }

    #[test]
    fn new_should_handle_min_utc_value() {
        let min_utc = DateTime::<Utc>::MIN_UTC;
        let millis = min_utc.timestamp_millis();
        let date_time = TimestampMillis::new(millis).to_date_time();
        assert_eq!(min_utc.timestamp(), date_time.timestamp());
        assert_eq!(
            min_utc.timestamp_subsec_nanos(),
            date_time.timestamp_subsec_nanos()
        );
        assert_eq!(Utc, date_time.timezone());
    }

    #[test]
    fn new_should_panic_if_value_less_than_min_utc_value() {
        let out_of_range_millis = DateTime::<Utc>::MIN_UTC.timestamp_millis() - 1;
        let result =
            std::panic::catch_unwind(|| TimestampMillis::new(out_of_range_millis).to_date_time());
        assert!(result.is_err());
    }

    #[test]
    fn new_should_handle_max_utc_value() {
        let max_utc = DateTime::<Utc>::MAX_UTC;
        let millis = max_utc.timestamp_millis();
        let date_time = TimestampMillis::new(millis).to_date_time();
        assert_eq!(max_utc.timestamp(), date_time.timestamp());
        assert_eq!(
            max_utc.timestamp_subsec_millis() * 1_000_000,
            date_time.timestamp_subsec_nanos()
        );
        assert_eq!(Utc, date_time.timezone());
    }

    #[test]
    fn new_should_panic_if_value_greater_than_max_utc_value() {
        let out_of_range_millis = DateTime::<Utc>::MAX_UTC.timestamp_millis() + 1;
        let result =
            std::panic::catch_unwind(|| TimestampMillis::new(out_of_range_millis).to_date_time());
        assert!(result.is_err());
    }

    #[test]
    fn round_trip_with_serde_json() {
        let timestamp = TimestampMillis::new(3);
        let str = serde_json::to_string(&timestamp).unwrap();
        let deserialized = serde_json::from_str::<TimestampMillis>(&str).unwrap();
        assert_eq!(timestamp, deserialized);
    }
}
