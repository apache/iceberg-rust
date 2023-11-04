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

use arrow_array::ArrayRef;
use arrow_schema::{DataType, TimeUnit};

use super::TransformFunction;

#[derive(Debug)]
pub struct Bucket {
    mod_n: u32,
}

impl Bucket {
    pub fn new(mod_n: u32) -> Self {
        Self { mod_n }
    }
}

impl Bucket {
    /// When switch the hash function, we only need to change this function.
    fn hash_bytes(mut v: &[u8]) -> i32 {
        murmur3::murmur3_32(&mut v, 0).unwrap() as i32
    }

    fn hash_int(v: i32) -> i32 {
        Self::hash_long(v as i64)
    }

    fn hash_long(v: i64) -> i32 {
        Self::hash_bytes(v.to_le_bytes().as_slice())
    }

    /// v is days from unix epoch
    fn hash_date(v: i32) -> i32 {
        Self::hash_int(v)
    }

    /// v is microseconds from midnight
    fn hash_time(v: i64) -> i32 {
        Self::hash_long(v)
    }

    /// v is microseconds from unix epoch
    fn hash_timestamp(v: i64) -> i32 {
        Self::hash_long(v)
    }

    fn hash_str(s: &str) -> i32 {
        Self::hash_bytes(s.as_bytes())
    }

    /// Decimal values are hashed using the minimum number of bytes required to hold the unscaled value as a two’s complement big-endian
    /// ref: https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
    fn hash_decimal(v: i128) -> i32 {
        let bytes = v.to_be_bytes();
        if let Some(start) = bytes.iter().position(|&x| x != 0) {
            Self::hash_bytes(&bytes[start..])
        } else {
            Self::hash_bytes(&[0])
        }
    }

    /// def bucket_N(x) = (murmur3_x86_32_hash(x) & Integer.MAX_VALUE) % N
    /// ref: https://iceberg.apache.org/spec/#partitioning
    fn bucket_n(&self, v: i32) -> i32 {
        (v & i32::MAX) % (self.mod_n as i32)
    }
}

impl TransformFunction for Bucket {
    fn transform(&self, input: ArrayRef) -> crate::Result<ArrayRef> {
        let res: arrow_array::Int32Array = match input.data_type() {
            DataType::Int32 => input
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .unary(|v| self.bucket_n(Self::hash_int(v))),
            DataType::Int64 => input
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .unary(|v| self.bucket_n(Self::hash_long(v))),
            DataType::Decimal128(_, _) => input
                .as_any()
                .downcast_ref::<arrow_array::Decimal128Array>()
                .unwrap()
                .unary(|v| self.bucket_n(Self::hash_decimal(v))),
            DataType::Date32 => input
                .as_any()
                .downcast_ref::<arrow_array::Date32Array>()
                .unwrap()
                .unary(|v| self.bucket_n(Self::hash_date(v))),
            DataType::Time64(TimeUnit::Microsecond) => input
                .as_any()
                .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                .unwrap()
                .unary(|v| self.bucket_n(Self::hash_time(v))),
            DataType::Timestamp(TimeUnit::Microsecond, _) => input
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .unwrap()
                .unary(|v| self.bucket_n(Self::hash_timestamp(v))),
            DataType::Utf8 => arrow_array::Int32Array::from_iter(
                input
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| self.bucket_n(Self::hash_str(v.unwrap()))),
            ),
            DataType::LargeUtf8 => arrow_array::Int32Array::from_iter(
                input
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| self.bucket_n(Self::hash_str(v.unwrap()))),
            ),
            DataType::Binary => arrow_array::Int32Array::from_iter(
                input
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|v| self.bucket_n(Self::hash_bytes(v.unwrap()))),
            ),
            DataType::LargeBinary => arrow_array::Int32Array::from_iter(
                input
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|v| self.bucket_n(Self::hash_bytes(v.unwrap()))),
            ),
            DataType::FixedSizeBinary(_) => arrow_array::Int32Array::from_iter(
                input
                    .as_any()
                    .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|v| self.bucket_n(Self::hash_bytes(v.unwrap()))),
            ),
            _ => unreachable!("Unsupported data type: {:?}", input.data_type()),
        };
        Ok(Arc::new(res))
    }
}

#[cfg(test)]
mod test {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};

    use super::Bucket;
    #[test]
    fn test_hash() {
        // test int
        assert_eq!(Bucket::hash_int(34), 2017239379);
        // test long
        assert_eq!(Bucket::hash_long(34), 2017239379);
        // test decimal
        assert_eq!(Bucket::hash_decimal(1420), -500754589);
        // test date
        let date = NaiveDate::from_ymd_opt(2017, 11, 16).unwrap();
        assert_eq!(
            Bucket::hash_date(
                date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                    .num_days() as i32
            ),
            -653330422
        );
        // test time
        let time = NaiveTime::from_hms_opt(22, 31, 8).unwrap();
        assert_eq!(
            Bucket::hash_time(
                time.signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                    .num_microseconds()
                    .unwrap()
            ),
            -662762989
        );
        // test timestamp
        let timestamp =
            NaiveDateTime::parse_from_str("2017-11-16 22:31:08", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(
            Bucket::hash_timestamp(
                timestamp
                    .signed_duration_since(
                        NaiveDateTime::parse_from_str("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
                            .unwrap()
                    )
                    .num_microseconds()
                    .unwrap()
            ),
            -2047944441
        );
        // test timestamp with tz
        let timestamp = DateTime::parse_from_rfc3339("2017-11-16T14:31:08-08:00").unwrap();
        assert_eq!(
            Bucket::hash_timestamp(
                timestamp
                    .signed_duration_since(
                        DateTime::parse_from_rfc3339("1970-01-01T00:00:00-00:00").unwrap()
                    )
                    .num_microseconds()
                    .unwrap()
            ),
            -2047944441
        );
        // test str
        assert_eq!(Bucket::hash_str("iceberg"), 1210000089);
        // test uuid
        assert_eq!(
            Bucket::hash_bytes(
                [
                    0xF7, 0x9C, 0x3E, 0x09, 0x67, 0x7C, 0x4B, 0xBD, 0xA4, 0x79, 0x3F, 0x34, 0x9C,
                    0xB7, 0x85, 0xE7
                ]
                .as_ref()
            ),
            1488055340
        );
        // test fixed and binary
        assert_eq!(
            Bucket::hash_bytes([0x00, 0x01, 0x02, 0x03].as_ref()),
            -188683207
        );
    }
}
