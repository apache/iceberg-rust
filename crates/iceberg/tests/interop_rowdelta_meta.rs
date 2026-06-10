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

//! METADATA-LEVEL row-delta interop (sprint increment E1) — the snapshot/manifest SEMANTICS proof
//! that complements the data-level scan-execution interop (`interop_scan_exec.rs`, which proved the
//! ROWS match both directions).
//!
//! Both sides build the same CANONICAL "snapshot metadata view" of an on-disk table — the Java side
//! via `InteropOracle` mode `emit-snapshot-meta` (`SnapshotMetaOracle`), the Rust side via
//! [`snapshot_meta_view`] here. The canonicalization (mirrored EXACTLY in the Java emitter):
//!
//! - snapshots ordered by SEQUENCE NUMBER; snapshot ids replaced by ORDINALS (parent too);
//! - `operation` as its own field (the re-parsed summary splits it out of the map on both sides);
//! - summary filtered to the COUNT-key allowlist — `*-files-size` keys are EXCLUDED because parquet
//!   byte sizes legitimately differ between writers;
//! - manifests sorted by `(content, sequence_number, min_sequence_number)`; paths excluded;
//!   `added_snapshot_id` emitted as an ordinal;
//! - entries sorted by the tuple `(status, content, record_count, sequence_number, equality_ids,
//!   partition)`; the `sequence_number` is the POST-INHERITANCE data sequence number (the
//!   merge-on-read applicability input); `file_sequence_number` is NOT emitted (no public Rust
//!   accessor — tracked);
//! - `partition` is `null` for an unpartitioned spec, else the spec's SINGLE-VALUE JSON
//!   serialization (Rust `Literal::try_into_json` ↔ Java `SingleValueParser.toJson` — the one
//!   cross-language-canonical rendering of a partition tuple).
//!
//! THE THREE COMPARISONS (per fixture; driven by `dev/java-interop/run-interop-rowdelta-meta.sh`):
//! 1. Rust's view of the JAVA-written table  == `java_meta.json` (Java's view of its own table) —
//!    READ parity: sequence-number inheritance, summary parsing, operation classification.
//! 2. Rust's view of the RUST-written table  == `java_meta.json` — WRITE parity: Rust's
//!    `fast_append` + `row_delta` commits produce metadata canonically indistinguishable from
//!    Java's `newAppend` + `newRowDelta` for the same logical operations.
//! 3. (script-side) Java's view of the RUST-written table == `java_meta.json`, diffed byte-for-byte
//!    by the run script — Java itself judging Rust's written metadata.
//!
//! GATED on `ICEBERG_INTEROP_META_DIR` (the run script's temp dir containing the per-fixture
//! subdirectories). Unset ⇒ clean no-op, so the offline `cargo test` gate is unaffected.
//!
//! LIMIT (reviewer-flagged): the ordinal scheme assumes DISTINCT sequence numbers. Every V1
//! snapshot has sequence number 0, so a multi-snapshot V1 table would produce
//! iteration-order-dependent ordinals — do NOT extend this oracle to V1 tables without adding a
//! tiebreaker (timestamp, then snapshot id). The three fixtures are V2 row-delta chains, where
//! every commit bumps the sequence number.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value as JsonValue;

mod common;
use common::snapshot_meta_view::snapshot_meta_view;

// ===========================================================================================
// The env-gated tests.
// ===========================================================================================

fn meta_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_META_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// The fixture subdirectories the run script generates. `part_scan` exercises the partitioned
/// rendering; the other two are unpartitioned position- and equality-delete row-delta chains.
const FIXTURES: &[&str] = &["scan_exec", "eq_delete", "part_scan"];

fn load_json(path: &Path) -> JsonValue {
    let raw =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// Direction 1 (READ parity): Rust's canonical view of the JAVA-written table equals Java's own
/// view of it. Pins: sequence-number inheritance on read, operation + summary parsing, the
/// manifest-list structure as Rust sees it.
#[tokio::test]
async fn test_rust_view_of_java_table_matches_java_view() {
    let Some(dir) = meta_dir() else {
        println!(
            "skipping interop_rowdelta_meta — set ICEBERG_INTEROP_META_DIR \
             (run dev/java-interop/run-interop-rowdelta-meta.sh)"
        );
        return;
    };

    for fixture in FIXTURES {
        let fixture_dir = dir.join(fixture);
        let java_view = load_json(&fixture_dir.join("java_meta.json"));
        let rust_view =
            snapshot_meta_view(&fixture_dir.join("table/metadata/final.metadata.json")).await;
        assert_eq!(
            rust_view, java_view,
            "{fixture}: Rust's view of the JAVA-written table diverges from Java's own view"
        );
        println!("{fixture}: Rust view of Java table == Java view OK");
    }
}

/// Direction 2 (WRITE parity — the E1 crown jewel): Rust's canonical view of the RUST-written
/// table ALSO equals Java's view of the JAVA-written table for the same logical operations. With
/// the script-side check (Java's view of the Rust table diffed against `java_meta.json`), this
/// pins that Rust's `fast_append` + `row_delta` emit Java-identical snapshot semantics: operation
/// classification, summary counts, manifest split, and the sequence-number/inheritance chain.
#[tokio::test]
async fn test_rust_written_table_metadata_matches_java_semantics() {
    let Some(dir) = meta_dir() else {
        println!(
            "skipping interop_rowdelta_meta — set ICEBERG_INTEROP_META_DIR \
             (run dev/java-interop/run-interop-rowdelta-meta.sh)"
        );
        return;
    };

    for fixture in FIXTURES {
        let fixture_dir = dir.join(fixture);
        let rust_table_metadata = fixture_dir.join("rust_table/metadata/final.metadata.json");
        assert!(
            rust_table_metadata.exists(),
            "{fixture}: missing {} — run the Rust GEN step of the run script first",
            rust_table_metadata.display()
        );
        let java_view = load_json(&fixture_dir.join("java_meta.json"));
        let rust_view = snapshot_meta_view(&rust_table_metadata).await;
        assert_eq!(
            rust_view, java_view,
            "{fixture}: the RUST-written table's canonical metadata diverges from Java's \
             semantics for the same logical operations"
        );
        println!("{fixture}: Rust-written table metadata == Java semantics OK");
    }
}
