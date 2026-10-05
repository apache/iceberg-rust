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

//! Primitive literal types

use std::cmp::Ordering;

use ordered_float::{FloatCore, OrderedFloat};

/// Values present in iceberg type
///
/// `Float` and `Double` compare as Java's `Float.compare` and `Double.compare` do: `-0.0` is
/// less than `0.0`, and every NaN is the same value, greater than all others. iceberg-java
/// compares partition values this way, so `-0.0` and `0.0` are different partitions. The
/// [spec](https://iceberg.apache.org/spec/#scan-planning) states the same rule: floating point
/// partition values are equal if their IEEE 754 bit layouts are equal, with NaNs normalized.
// The derived `Hash` gives `-0.0` and `0.0` the same hash. That is coarser than `eq`, which is
// allowed: equal values still hash alike.
#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Clone, Debug, Hash, Eq)]
pub enum PrimitiveLiteral {
    /// 0x00 for false, non-zero byte for true
    Boolean(bool),
    /// Stored as 4-byte little-endian
    Int(i32),
    /// Stored as 8-byte little-endian
    Long(i64),
    /// Stored as 4-byte little-endian
    Float(OrderedFloat<f32>),
    /// Stored as 8-byte little-endian
    Double(OrderedFloat<f64>),
    /// UTF-8 bytes (without length)
    String(String),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stored as 16-byte big-endian
    Int128(i128),
    /// Stored as 16-byte big-endian
    UInt128(u128),
    /// When a number is larger than it can hold
    AboveMax,
    /// When a number is smaller than it can hold
    BelowMin,
}

impl PartialEq for PrimitiveLiteral {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl PartialOrd for PrimitiveLiteral {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Boolean(a), Self::Boolean(b)) => a.partial_cmp(b),
            (Self::Int(a), Self::Int(b)) => a.partial_cmp(b),
            (Self::Long(a), Self::Long(b)) => a.partial_cmp(b),
            (Self::Float(a), Self::Float(b)) => Some(float_cmp(a, b)),
            (Self::Double(a), Self::Double(b)) => Some(float_cmp(a, b)),
            (Self::String(a), Self::String(b)) => a.partial_cmp(b),
            (Self::Binary(a), Self::Binary(b)) => a.partial_cmp(b),
            (Self::Int128(a), Self::Int128(b)) => a.partial_cmp(b),
            (Self::UInt128(a), Self::UInt128(b)) => a.partial_cmp(b),
            (Self::AboveMax, Self::AboveMax) | (Self::BelowMin, Self::BelowMin) => {
                Some(Ordering::Equal)
            }
            // Different variants order by declaration, as the derived impl did. A variant that
            // lacks an arm above lands here against itself; fail closed instead of calling the
            // two values equal.
            _ => {
                debug_assert_ne!(std::mem::discriminant(self), std::mem::discriminant(other));
                match self.variant_index().cmp(&other.variant_index()) {
                    Ordering::Equal => None,
                    ordering => Some(ordering),
                }
            }
        }
    }
}

/// Compares floats as Java's `Float.compare` and `Double.compare` do. `OrderedFloat` already
/// treats every NaN as one value above all others; it only lacks `-0.0` before `0.0`.
fn float_cmp<T: FloatCore>(a: &OrderedFloat<T>, b: &OrderedFloat<T>) -> Ordering {
    a.cmp(b).then_with(|| {
        if a.is_nan() {
            Ordering::Equal
        } else {
            b.is_sign_negative().cmp(&a.is_sign_negative())
        }
    })
}

impl PrimitiveLiteral {
    /// Must follow the declaration order of the variants: `partial_cmp` uses it to order
    /// different variants the way the derived `PartialOrd` did.
    fn variant_index(&self) -> u8 {
        match self {
            Self::Boolean(_) => 0,
            Self::Int(_) => 1,
            Self::Long(_) => 2,
            Self::Float(_) => 3,
            Self::Double(_) => 4,
            Self::String(_) => 5,
            Self::Binary(_) => 6,
            Self::Int128(_) => 7,
            Self::UInt128(_) => 8,
            Self::AboveMax => 9,
            Self::BelowMin => 10,
        }
    }

    /// Returns true if the Literal represents a primitive type
    /// that can be a NaN, and that it's value is NaN
    pub fn is_nan(&self) -> bool {
        match self {
            PrimitiveLiteral::Double(val) => val.is_nan(),
            PrimitiveLiteral::Float(val) => val.is_nan(),
            _ => false,
        }
    }
}
