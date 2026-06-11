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

//! Java interop test for DELETION-VECTOR WRITING (Increment D2, Direction 2 — "JAVA reads what
//! RUST writes"). The sibling of [`interop_dv_scan`] (D1, Direction 1).
//!
//! THE CROWN JEWEL. `test_dv_write_gen` writes a REAL Puffin file via the production
//! `DVFileWriter` — one `deletion-vector-v1` blob per referenced data file — plus
//! `rust_dv_expected.json` describing {referenced path → positions + blob coordinates}. The Java
//! oracle's `verify-interop-dv-write` mode (driven by `dev/java-interop/run-interop-dv.sh`) then
//! opens the RUST-written Puffin with Java's REAL reader machinery — `Puffin.read` footer
//! parsing, the ranged blob read of `BaseDeleteLoader.readDV`, and
//! `PositionDeleteIndex.deserialize` (framing + magic + CRC + cardinality validations) — and
//! asserts the decoded positions equal the expected sets. A failure there is a REAL
//! write-incompatibility finding (Rust wrote a DV Java cannot read, the silent-corruption class
//! this increment exists to prevent).
//!
//! THE BYTE-EXACT PIN. The Java oracle ALSO serializes ITS OWN `BitmapPositionDeleteIndex` over
//! the SAME position sets (through the production `BaseDVFileWriter`) and dumps each blob to
//! `java_dv_blob_<i>.bin` (index = the entry's position in `rust_dv_expected.json`).
//! `test_dv_write_blob_bytes_match_java` then asserts the Rust-written blob bytes (sliced out of
//! the Rust Puffin at the recorded coordinates) are BYTE-IDENTICAL to Java's — including the
//! run-length-encoded set, pinning that `RoaringBitmap::optimize()` makes the same
//! run-vs-array/bitmap container choices as Java's `runOptimize()`.
//!
//! THE FIXTURE. Three referenced data files in ONE Puffin:
//! * `interop://data-x.parquet` — positions {0, 5} ∪ [1000, 6000) ∪ {2^32 + 7}: includes
//!   position 0, a 5000-long contiguous run (the run-container probe), and a >2^32 position;
//! * `interop://data-y.parquet` — positions {3, 2^33 + 1}: bitmap keys 0 and 2, so the dense
//!   serialization must carry an EMPTY key-1 gap bitmap (the dense-layout probe);
//! * `interop://data-z.parquet` — positions {0, 1, 2}: the EXACT array/run size tie (array
//!   2·3 = 6 bytes == run 2 + 4·1 = 6 bytes) — both sides must keep the ARRAY container
//!   (strictly-smaller criterion), the byte-compare settles the tie empirically.
//!
//! THE ENV GATE. Both tests are clean NO-OPS (runtime early-return, not `#[ignore]`) unless
//! `ICEBERG_INTEROP_DV_WRITE_DIR` is set non-empty, so the offline `cargo test` gate needs no
//! Java/Maven. With the env var SET, `test_dv_write_blob_bytes_match_java` REQUIRES the Java
//! blob dumps to exist (the script runs Java between the two Rust phases) and fails loudly
//! otherwise — a silent skip there would hollow out the byte pin. Run phases individually via
//! the test-name filters in `run-interop-dv.sh`; do not run the whole binary with the env var
//! set mid-harness.

use std::fs;
use std::path::{Path, PathBuf};

use iceberg::io::FileIO;
use iceberg::writer::base_writer::deletion_vector_writer::DVFileWriter;
use serde::{Deserialize, Serialize};

/// One expected deletion vector: the data file it applies to, the exact deleted positions, and
/// the blob coordinates + sizes the writer recorded in its `DeleteFile` metadata.
#[derive(Debug, Serialize, Deserialize)]
struct ExpectedDeletionVector {
    referenced_data_file: String,
    positions: Vec<u64>,
    content_offset: i64,
    content_size_in_bytes: i64,
    record_count: u64,
    file_size_in_bytes: u64,
}

/// The temp dir shared with the Java oracle. `None` when the env var is unset OR empty
/// (set-but-empty must not flip the gate on).
fn dv_write_dir() -> Option<PathBuf> {
    std::env::var_os("ICEBERG_INTEROP_DV_WRITE_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// The fixture position sets, keyed by referenced data file path (sorted order — the writer
/// emits blobs sorted by path, and the expected JSON entries follow the same order).
fn fixture_position_sets() -> Vec<(String, Vec<u64>)> {
    let mut x_positions: Vec<u64> = vec![0, 5];
    x_positions.extend(1000..6000); // the run-container probe
    x_positions.push((1u64 << 32) + 7); // > 2^32
    let y_positions: Vec<u64> = vec![3, (1u64 << 33) + 1]; // keys 0 + 2: the dense-gap probe
    let z_positions: Vec<u64> = vec![0, 1, 2]; // the exact array/run size tie (6 == 6 bytes)
    vec![
        ("interop://data-x.parquet".to_string(), x_positions),
        ("interop://data-y.parquet".to_string(), y_positions),
        ("interop://data-z.parquet".to_string(), z_positions),
    ]
}

fn read_expected(dir: &Path) -> Vec<ExpectedDeletionVector> {
    let path = dir.join("rust_dv_expected.json");
    let json = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&json).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// GEN phase: write the Puffin deletion-vector file via the PRODUCTION `DVFileWriter` and emit
/// the expected-positions JSON the Java oracle verifies against.
///
/// Risk pinned (with the Java step): the WHOLE Direction-2 write path — dense serialization,
/// framing/CRC, blob properties, footer coordinates — read back by Java's production machinery.
#[tokio::test]
async fn test_dv_write_gen() {
    let Some(dir) = dv_write_dir() else {
        println!(
            "skipping interop_dv_write GEN — set ICEBERG_INTEROP_DV_WRITE_DIR \
             (run dev/java-interop/run-interop-dv.sh)"
        );
        return;
    };
    fs::create_dir_all(&dir).expect("create interop dir");

    let file_io = FileIO::new_with_fs();
    let puffin_path = dir.join("rust_dv.puffin");
    let output_file = file_io
        .new_output(puffin_path.to_str().expect("utf-8 path"))
        .expect("new output file");

    let mut writer = DVFileWriter::new(output_file);
    for (data_file_path, positions) in fixture_position_sets() {
        for position in positions {
            writer
                .delete(&data_file_path, position, None)
                .expect("record deleted position");
        }
    }
    let delete_files = writer.close().await.expect("close DVFileWriter");
    assert_eq!(
        delete_files.len(),
        3,
        "one DeleteFile per referenced data file"
    );

    let expected: Vec<ExpectedDeletionVector> = delete_files
        .iter()
        .zip(fixture_position_sets())
        .map(|(delete_file, (fixture_path, positions))| {
            assert_eq!(
                delete_file.referenced_data_file().as_deref(),
                Some(fixture_path.as_str()),
                "DeleteFiles must come back in sorted referenced-path order"
            );
            ExpectedDeletionVector {
                referenced_data_file: fixture_path,
                positions,
                content_offset: delete_file.content_offset().expect("content_offset"),
                content_size_in_bytes: delete_file
                    .content_size_in_bytes()
                    .expect("content_size_in_bytes"),
                record_count: delete_file.record_count(),
                file_size_in_bytes: delete_file.file_size_in_bytes(),
            }
        })
        .collect();

    let json = serde_json::to_string_pretty(&expected).expect("serialize expected JSON");
    fs::write(dir.join("rust_dv_expected.json"), json).expect("write expected JSON");
    println!(
        "wrote rust_dv.puffin ({} blobs) + rust_dv_expected.json to {}",
        expected.len(),
        dir.display()
    );
}

/// BYTE-PARITY phase (run AFTER the Java oracle): every blob Rust wrote must be BYTE-IDENTICAL
/// to Java's own serialization of the same position set (`java_dv_blob_<i>.bin`, emitted by
/// `verify-interop-dv-write` through the production `BaseDVFileWriter`).
///
/// Risk pinned: any divergence in the dense layout (count, gap bitmaps), the container choice
/// (run vs array vs bitmap — the `runLengthEncode` parity), or the framing/CRC. Identical
/// SEMANTICS with different bytes would pass the positions check but fail here.
#[tokio::test]
async fn test_dv_write_blob_bytes_match_java() {
    let Some(dir) = dv_write_dir() else {
        println!(
            "skipping interop_dv_write byte parity — set ICEBERG_INTEROP_DV_WRITE_DIR \
             (run dev/java-interop/run-interop-dv.sh)"
        );
        return;
    };

    let expected = read_expected(&dir);
    let puffin_bytes = fs::read(dir.join("rust_dv.puffin")).expect("read rust_dv.puffin");

    for (index, entry) in expected.iter().enumerate() {
        let java_blob_path = dir.join(format!("java_dv_blob_{index}.bin"));
        // With the env gate ON, a missing Java dump is a harness-order error, not a skip — a
        // silent pass here would hollow out the byte pin.
        let java_blob = fs::read(&java_blob_path).unwrap_or_else(|error| {
            panic!(
                "read {} (did the Java verify-interop-dv-write step run?): {error}",
                java_blob_path.display()
            )
        });

        let offset = usize::try_from(entry.content_offset).expect("offset fits usize");
        let size = usize::try_from(entry.content_size_in_bytes).expect("size fits usize");
        let rust_blob = &puffin_bytes[offset..offset + size];

        assert_eq!(
            rust_blob,
            java_blob.as_slice(),
            "deletion-vector blob for '{}' must be byte-identical to Java's serialization \
             of the same positions",
            entry.referenced_data_file
        );
        println!(
            "byte-identical blob for '{}' ({} bytes, {} positions)",
            entry.referenced_data_file,
            size,
            entry.positions.len()
        );
    }
}
