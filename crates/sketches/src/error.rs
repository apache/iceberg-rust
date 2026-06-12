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

//! Error type for the theta-sketch crate.
//!
//! This crate is intentionally dependency-free, so it defines its own `std::error::Error` rather
//! than depending on `thiserror`. Every deserialization error carries enough context for an
//! operator to diagnose a malformed or foreign blob from logs alone.

use std::fmt;

/// The result type used throughout the crate.
pub type SketchResult<T> = Result<T, SketchError>;

/// Errors produced while building, serializing, or deserializing a theta sketch.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SketchError {
    /// MurmurHash3 was given a zero-length input. DataSketches' `checkPositive` rejects this.
    EmptyHashInput,

    /// The seed produced a seed hash of zero, which DataSketches forbids.
    ZeroSeedHash {
        /// The offending seed.
        seed: u64,
    },

    /// A serialized sketch was shorter than the bytes its preamble claims to need.
    TruncatedInput {
        /// Bytes required by the parsed header.
        expected: usize,
        /// Bytes actually available.
        actual: usize,
    },

    /// The serialization version byte was not the supported value (3).
    UnsupportedSerialVersion {
        /// The version found on the wire.
        found: u8,
    },

    /// The family id byte was not the theta `COMPACT` family (3).
    UnexpectedFamily {
        /// The family id found on the wire.
        found: u8,
    },

    /// The preamble-longs byte was not one of the legal compact values (1, 2, or 3).
    InvalidPreambleLongs {
        /// The preamble-longs value found on the wire.
        found: u8,
    },

    /// The blob's seed hash does not match the expected seed's hash — the blob was written with a
    /// different seed and its hashes are not comparable to ours.
    SeedHashMismatch {
        /// The seed hash expected for our seed.
        expected: u16,
        /// The seed hash found on the wire.
        found: u16,
    },

    /// A non-compact (update-form) serialized sketch was supplied where a compact one is required.
    NotCompact,
}

impl fmt::Display for SketchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SketchError::EmptyHashInput => {
                write!(
                    formatter,
                    "MurmurHash3 input must be non-empty (DataSketches rejects zero-length arrays)"
                )
            }
            SketchError::ZeroSeedHash { seed } => {
                write!(
                    formatter,
                    "seed {seed} produced a seed hash of zero; choose a different seed"
                )
            }
            SketchError::TruncatedInput { expected, actual } => {
                write!(
                    formatter,
                    "truncated theta sketch: preamble requires {expected} bytes but only {actual} are present"
                )
            }
            SketchError::UnsupportedSerialVersion { found } => {
                write!(
                    formatter,
                    "unsupported theta serialization version {found} (expected 3)"
                )
            }
            SketchError::UnexpectedFamily { found } => {
                write!(
                    formatter,
                    "unexpected sketch family id {found} (expected 3 = COMPACT theta)"
                )
            }
            SketchError::InvalidPreambleLongs { found } => {
                write!(
                    formatter,
                    "invalid compact preamble-longs {found} (expected 1, 2, or 3)"
                )
            }
            SketchError::SeedHashMismatch { expected, found } => {
                write!(
                    formatter,
                    "seed hash mismatch: blob has {found}, expected {expected}"
                )
            }
            SketchError::NotCompact => {
                write!(
                    formatter,
                    "expected a compact theta sketch; the input is an update (mutable) sketch"
                )
            }
        }
    }
}

impl std::error::Error for SketchError {}
