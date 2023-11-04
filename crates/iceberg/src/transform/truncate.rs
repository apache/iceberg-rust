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
use arrow_schema::DataType;

use crate::Error;

use super::TransformFunction;

#[derive(Debug)]
pub struct Truncate {
    width: u32,
}

impl Truncate {
    pub fn new(width: u32) -> Self {
        Self { width }
    }

    fn truncate_str_by_char(s: &str, max_chars: usize) -> &str {
        match s.char_indices().nth(max_chars) {
            None => s,
            Some((idx, _)) => &s[..idx],
        }
    }
}

impl TransformFunction for Truncate {
    fn transform(&self, input: ArrayRef) -> crate::Result<ArrayRef> {
        match input.data_type() {
            DataType::Int32 => {
                let width: i32 = self.width.try_into().map_err(|_| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "width is failed to convert to i32 when truncate Int32Array",
                    )
                })?;
                let res: arrow_array::Int32Array = input
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap()
                    .unary(|v| v - v.rem_euclid(width));
                Ok(Arc::new(res))
            }
            DataType::Int64 => {
                let width = self.width as i64;
                let res: arrow_array::Int64Array = input
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .unary(|v| v - (((v % width) + width) % width));
                Ok(Arc::new(res))
            }
            DataType::Decimal128(precision, scale) => {
                let width = self.width as i128;
                let res: arrow_array::Decimal128Array = input
                    .as_any()
                    .downcast_ref::<arrow_array::Decimal128Array>()
                    .unwrap()
                    .unary(|v| v - (((v % width) + width) % width))
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|err| Error::new(crate::ErrorKind::Unexpected, format!("{err}")))?;
                Ok(Arc::new(res))
            }
            DataType::Utf8 => {
                let len = self.width as usize;
                let res: arrow_array::StringArray = arrow_array::StringArray::from_iter(
                    input
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap()
                        .iter()
                        .map(|v| v.map(|v| Self::truncate_str_by_char(v, len))),
                );
                Ok(Arc::new(res))
            }
            DataType::LargeUtf8 => {
                let len = self.width as usize;
                let res: arrow_array::LargeStringArray = arrow_array::LargeStringArray::from_iter(
                    input
                        .as_any()
                        .downcast_ref::<arrow_array::LargeStringArray>()
                        .unwrap()
                        .iter()
                        .map(|v| v.map(|v| Self::truncate_str_by_char(v, len))),
                );
                Ok(Arc::new(res))
            }
            _ => unreachable!("Truncate transform only supports (int,long,decimal,string) types"),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{
        builder::PrimitiveBuilder, types::Decimal128Type, Decimal128Array, Int32Array, Int64Array,
    };

    use crate::transform::TransformFunction;

    // Test case ref from: https://iceberg.apache.org/spec/#truncate-transform-details
    #[test]
    fn test_truncate_simple() {
        // test truncate int
        let input = Arc::new(Int32Array::from(vec![1, -1]));
        let res = super::Truncate::new(10).transform(input).unwrap();
        assert_eq!(
            res.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            0
        );
        assert_eq!(
            res.as_any().downcast_ref::<Int32Array>().unwrap().value(1),
            -10
        );

        // test truncate long
        let input = Arc::new(Int64Array::from(vec![1, -1]));
        let res = super::Truncate::new(10).transform(input).unwrap();
        assert_eq!(
            res.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            0
        );
        assert_eq!(
            res.as_any().downcast_ref::<Int64Array>().unwrap().value(1),
            -10
        );

        // test decimal
        let mut buidler = PrimitiveBuilder::<Decimal128Type>::new()
            .with_precision_and_scale(20, 2)
            .unwrap();
        buidler.append_value(1065);
        let input = Arc::new(buidler.finish());
        let res = super::Truncate::new(50).transform(input).unwrap();
        assert_eq!(
            res.as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            1050
        );

        // test string
        let input = Arc::new(arrow_array::StringArray::from(vec!["iceberg"]));
        let res = super::Truncate::new(3).transform(input).unwrap();
        assert_eq!(
            res.as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap()
                .value(0),
            "ice"
        );

        // test large string
        let input = Arc::new(arrow_array::LargeStringArray::from(vec!["iceberg"]));
        let res = super::Truncate::new(3).transform(input).unwrap();
        assert_eq!(
            res.as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .unwrap()
                .value(0),
            "ice"
        );
    }

    #[test]
    fn test_string_truncate() {
        let test1 = "イロハニホヘト";
        let test1_2_expected = "イロ";
        assert_eq!(
            super::Truncate::truncate_str_by_char(test1, 2),
            test1_2_expected
        );

        let test1_3_expected = "イロハ";
        assert_eq!(
            super::Truncate::truncate_str_by_char(test1, 3),
            test1_3_expected
        );

        let test2 = "щщаεはчωいにπάほхεろへσκζ";
        let test2_7_expected = "щщаεはчω";
        assert_eq!(
            super::Truncate::truncate_str_by_char(test2, 7),
            test2_7_expected
        );

        let test3 = "\u{FFFF}\u{FFFF}";
        assert_eq!(super::Truncate::truncate_str_by_char(test3, 2), test3);

        let test4 = "\u{10000}\u{10000}";
        let test4_1_expected = "\u{10000}";
        assert_eq!(
            super::Truncate::truncate_str_by_char(test4, 1),
            test4_1_expected
        );
    }
}
