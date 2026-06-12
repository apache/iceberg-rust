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

//! # iceberg-sketches
//!
//! A dependency-free, byte-exact port of the Apache DataSketches **theta** `CompactSketch`
//! serialized format — the payload of Apache Iceberg's `apache-datasketches-theta-v1` Puffin blob
//! used to carry per-column NDV (number of distinct values) statistics.
//!
//! The goal is **cross-engine byte compatibility**: a theta blob this crate writes is readable by
//! Java DataSketches (and therefore Spark/Trino/Flink Iceberg engines), and blobs those engines
//! write are readable here. To guarantee that, the hash ([`hash`]) and the build/serialize paths
//! ([`ThetaSketch`], [`AlphaSketch`]) are ported one-to-one from the DataSketches 3.3.0 jar bytecode,
//! and pinned against Java-generated fixtures (see the crate's `testdata`).
//!
//! ## Two update families
//!
//! - [`ThetaSketch`] ports `HeapQuickSelectSketch` (the `UpdateSketch.builder()` default family).
//! - [`AlphaSketch`] ports `HeapAlphaSketch` — **the family Apache Iceberg's NDV pipeline actually
//!   builds** (`ThetaSketchAgg` → `UpdateSketch.builder.setFamily(Family.ALPHA).build()`). In exact
//!   mode the two are byte-identical; in estimation mode they diverge (different retained set, theta,
//!   bytes, and NDV). Use [`AlphaSketch`] to match what other engines write on high-cardinality
//!   columns.
//!
//! Both families serialize to the SAME family-COMPACT [`CompactThetaSketch`] form — one serialization
//! path, no per-family fork.
//!
//! ## Usage
//!
//! ```
//! use iceberg_sketches::ThetaSketch;
//!
//! let mut sketch = ThetaSketch::new();
//! sketch.update_u64(1);
//! sketch.update_u64(2);
//! sketch.update_u64(2); // duplicate — not counted
//! assert_eq!(sketch.estimate(), 2.0);
//!
//! // The bytes below are the `apache-datasketches-theta-v1` Puffin blob payload.
//! let blob = sketch.serialize_compact().unwrap();
//! let parsed = iceberg_sketches::CompactThetaSketch::deserialize(&blob).unwrap();
//! assert_eq!(parsed.estimate(), 2.0);
//! ```
//!
//! ## Scope
//!
//! This crate is the byte-contract foundation only. Wiring it into `ComputeTableStats` (per-column
//! NDV over a scan, the Puffin `StatisticsFile` write, registration via `UpdateStatisticsAction`)
//! lives in the `iceberg` crate. Note that the Iceberg `ndv` property reads the COMPACT sketch's
//! estimate ([`CompactThetaSketch::estimate`]) — the family-COMPACT standard estimator — NOT the Alpha
//! update sketch's own sampling estimate; the two differ in estimation mode (see [`AlphaSketch`]).

#![deny(missing_docs)]

pub mod alpha;
pub mod error;
pub mod hash;
pub mod theta;

pub use alpha::{ALPHA_MIN_LG_NOM_LONGS, AlphaSketch};
pub use error::{SketchError, SketchResult};
pub use hash::{DEFAULT_UPDATE_SEED, compute_seed_hash, hash_bytes, hash_long, hash_longs};
pub use theta::{CompactThetaSketch, DEFAULT_LG_NOMINAL_LONGS, MAX_THETA, ThetaSketch};
