use arrow::array::types::{Decimal128Type, Decimal256Type, DecimalType};
use arrow::datatypes::i256;
use bigdecimal::BigDecimal;
use half::f16;
use rust_decimal::prelude::*;

/// Represents a constant for NULL string in your database.
pub const NULL_STR: &str = "NULL";

pub(crate) fn bool_to_str(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

pub(crate) fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_string()
    } else {
        value.trim_end_matches('\n').to_string()
    }
}

pub(crate) fn f16_to_str(value: f16) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f16::INFINITY {
        "Infinity".to_string()
    } else if value == f16::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

pub(crate) fn f32_to_str(value: f32) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f32::INFINITY {
        "Infinity".to_string()
    } else if value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

pub(crate) fn f64_to_str(value: f64) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

pub(crate) fn i128_to_str(value: i128, precision: &u8, scale: &i8) -> String {
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal128Type::format_decimal(value, *precision, *scale)).unwrap(),
    )
}

pub(crate) fn i256_to_str(value: i256, precision: &u8, scale: &i8) -> String {
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal256Type::format_decimal(value, *precision, *scale)).unwrap(),
    )
}

pub(crate) fn big_decimal_to_str(value: BigDecimal) -> String {
    value.round(12).normalized().to_string()
}
