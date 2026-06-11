#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# DELETION-VECTOR interop harness, BOTH DIRECTIONS + TABLE level + METADATA level:
#
#   D1 (Direction 1 — "Rust reads what JAVA writes"): proves Rust's scan → Arrow with PUFFIN
#   DELETION-VECTOR application matches Java's OWN read of a JAVA-WRITTEN **V3** table whose
#   merge-on-read deletes are a real `deletion-vector-v1` blob written by Java's production
#   BaseDVFileWriter. This is the sibling of run-interop-scan-exec.sh (parquet position deletes);
#   the DV is the V3 replacement for those.
#
#   D2 (Direction 2 — "JAVA reads what RUST writes", blob level): the Rust GEN test writes a REAL
#   Puffin DV file via the production DVFileWriter (3 referenced data files in one Puffin:
#   position 0, a 5000-long contiguous run, a >2^32 position, a dense-gap key, and the exact
#   array/run container size tie {0,1,2}); Java verifies it with its
#   REAL reader machinery (Puffin footer parse + the BaseDeleteLoader.readDV-style ranged read +
#   PositionDeleteIndex.deserialize) AND emits its own serialization of the SAME position sets;
#   a final Rust test asserts the blobs are BYTE-IDENTICAL (the exact-byte parity pin, run
#   containers included).
#
#   D4 TABLE level (Direction 2, the headline — "JAVA reads a RUST-COMMITTED V3+DV TABLE"): the
#   Rust GEN test (tests/interop_dv_table.rs) commits a COMPLETE V3 table through the production
#   Rust path — two real parquet data files in two identity(category) partitions fast_appended at
#   sequence 1, ONE Puffin holding TWO DVs (DVFileWriter) committed via row_delta at sequence 2 —
#   and Java reads it back with its PRODUCTION scan (IcebergGenerics, which loads both DVs via
#   BaseDeleteLoader.readDV), asserting the live rows {(10,x),(30,z),(50,q)} plus a manifest-API
#   cross-check of the committed DeleteFile metadata (content/format/referenced-data-file/blob
#   coordinates/cardinality + the shared-puffin multi-blob pin).
#
#   D4 METADATA level (the E1-family extension): BOTH sides perform the SAME logical chain
#   {fast_append 2 partitioned data files, row_delta adding 2 DVs} on equivalent V3 tables, and
#   the canonical snapshot-metadata views (SnapshotMetaOracle <-> common/snapshot_meta_view.rs)
#   are compared the established 3 ways: Java's own view (java_meta.json), Java's view of the
#   RUST table byte-diffed against it HERE, and Rust's views of both tables asserted in
#   tests/interop_dv_table.rs. This pins the `added-dvs` summary key, the operation
#   classification, and the manifest count/sequence-number semantics of a DV commit. (The
#   canonical view's entry tuple does not carry the DV-specific referenced/offset fields — those
#   are covered by the table-level manifest cross-check above.)
#
# The D1 run ALSO settles the roaring byte-compatibility question EMPIRICALLY: the oracle emits a
# second, synthetic DV blob (dv_blob.bin) whose positions span the 32-bit key boundary and include
# a run-length-encoded range, and the env-gated Rust LIB test decodes those real Java bytes and
# asserts the exact position set.
#
# This is a TEST-ONLY ORACLE (a dev tool, like dev/spark/) — NOT part of the shipped Rust library
# and NOT part of the offline `cargo test` gate (it needs Java + Maven). Nothing binary is
# committed; the temp table under dev/java-interop/target/ is gitignored.
#
# Methodology (regenerate-and-compare):
#   1. mvn ... -Dexec.args=generate-interop-dv -Dinterop.dv.dir="$TMP"
#        -> The Java oracle writes an unpartitioned V3 table under "$TMP/table": two REAL parquet
#           data files (A: ids 10..50; B: ids 60/70/80) + a REAL Puffin deletion vector deleting
#           positions {1,3} of file A (ids 20/40), committed via newRowDelta().addDeletes(dv). It
#           writes "$TMP/table/metadata/final.metadata.json", materializes Java's OWN
#           merge-on-read READ into "$TMP/java_dv_scan_rows.json" (= {10,30,50,60,70,80}), and
#           emits the synthetic high-bits/run-container blob "$TMP/dv_blob.bin" +
#           "$TMP/dv_blob_expected.json".
#   2. ICEBERG_INTEROP_DV_DIR="$TMP" cargo test ... interop_dv_scan
#        -> The env-gated Rust test scans the SAME table via table.scan().to_arrow() (which loads
#           + applies the DV) and asserts the rows EQUAL Java's read: 20/40 ABSENT, file B intact.
#   3. ICEBERG_INTEROP_DV_DIR="$TMP" cargo test -p iceberg --lib test_dv_blob_decodes_java...
#        -> The env-gated lib test decodes the raw Java-serialized blob bytes (framing + portable
#           64-bit roaring incl. >2^32 positions + run containers) and asserts the position set.
#
# Without ICEBERG_INTEROP_DV_DIR both Rust tests are clean no-ops (the offline gate stays green);
# this script is what flips them into the REAL comparison.
#
# Requirements:
#   - Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64.
#   - A Rust toolchain (the repo's pinned nightly via rust-toolchain.toml).
#   - The FIRST Maven run must be ONLINE if ~/.m2 lacks the oracle deps (iceberg-core/-data/
#     -parquet/hadoop-client-runtime 1.10.0); after that, `mvn -o` works fully offline.
#
# Run from anywhere; paths are resolved relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-dv"
TMP2="${SCRIPT_DIR}/target/interop-dv-write"
TMP3="${SCRIPT_DIR}/target/interop-dv-table"
TMP4="${SCRIPT_DIR}/target/interop-dv-replace"

echo "==> [1/16] Reset the temp dirs: ${TMP} + ${TMP2} + ${TMP3} + ${TMP4}"
rm -rf "${TMP}" "${TMP2}" "${TMP3}" "${TMP4}"
mkdir -p "${TMP}" "${TMP2}" "${TMP3}" "${TMP4}"

echo "==> [2/16] (D1) Java oracle: write a REAL V3 table (parquet data + Puffin deletion vector) + emit java_dv_scan_rows.json + dv_blob.bin"
(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -q compile exec:java \
    -Dexec.args=generate-interop-dv \
    -Dinterop.dv.dir="${TMP}"
)

echo "==> [3/16] (D1) Rust: scan the V3 DV table (merge-on-read) + decode the raw Java DV blob, compare vs Java"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_DV_DIR="${TMP}" \
    cargo test -p iceberg --test interop_dv_scan -- --nocapture
  ICEBERG_INTEROP_DV_DIR="${TMP}" \
    cargo test -p iceberg --lib test_dv_blob_decodes_java_written_blob_when_env_set -- --nocapture
)

echo "==> [4/16] (D2) Rust: write a REAL Puffin DV file via the production DVFileWriter + rust_dv_expected.json"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_DV_WRITE_DIR="${TMP2}" \
    cargo test -p iceberg --test interop_dv_write test_dv_write_gen -- --exact --nocapture
)

echo "==> [5/16] (D2) Java: verify the RUST-written Puffin with the production reader + emit java_dv_blob_<i>.bin"
# NOTE: do NOT trust `mvn -q exec:java`'s exit code for the verdict — depending on the
# exec-maven-plugin/JVM combination the oracle's `System.exit(1)` either vanishes (the D2-era
# observation) or surfaces as a Maven build failure. So capture the output TOLERANTLY (`|| true`
# keeps `set -e` from aborting before the diagnostics are echoed) and assert the success sentinel
# ("...: 0 failures") with no per-check FAIL line — otherwise this script would falsely pass on a
# real Java-read incompatibility.
VERIFY_OUT="$(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=verify-interop-dv-write \
    -Dinterop.dv_write.dir="${TMP2}" 2>&1
)" || true
echo "${VERIFY_OUT}"
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-dv-write: 0 failures'; then
  echo "==> FAILED — Java could not correctly read the Rust-written deletion vectors (a real write-incompatibility finding)."
  exit 1
fi

echo "==> [6/16] (D2) Rust: assert the Rust-written blobs are BYTE-IDENTICAL to Java's serialization of the same positions"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_DV_WRITE_DIR="${TMP2}" \
    cargo test -p iceberg --test interop_dv_write test_dv_write_blob_bytes_match_java -- --exact --nocapture
)

echo "==> [7/16] (D4 table) Rust: COMMIT a complete V3 table (2 partitioned parquet data files + ONE puffin with TWO DVs via fast_append + row_delta) + expected_rows.json + expected_dvs.json"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_DV_TABLE_DIR="${TMP3}" \
    cargo test -p iceberg --test interop_dv_table test_dv_table_gen_rust_writes_java_readable_v3_dv_table -- --exact --nocapture
)

echo "==> [8/16] (D4 table) Java: read the RUST-COMMITTED V3+DV table with the PRODUCTION scan (IcebergGenerics) + manifest-API DeleteFile cross-check"
# Same sentinel rule as step 5: the verdict comes from the OUTPUT (success sentinel present, no
# per-check FAIL line), never from mvn's unreliable exit code; `|| true` keeps `set -e` from
# aborting before the diagnostics are echoed.
TABLE_VERIFY_OUT="$(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=verify-interop-dv-table \
    -Dinterop.dv_table.dir="${TMP3}" 2>&1
)" || true
echo "${TABLE_VERIFY_OUT}"
if echo "${TABLE_VERIFY_OUT}" | grep -q '^FAIL ' || ! echo "${TABLE_VERIFY_OUT}" | grep -q 'verify-interop-dv-table: 0 failures'; then
  echo "==> FAILED — Java could not correctly read the RUST-COMMITTED V3 deletion-vector table (a real table-level write-incompatibility finding)."
  exit 1
fi

echo "==> [9/16] (D4 meta) Java: write the JAVA mirror chain (newFastAppend + BaseDVFileWriter + newRowDelta) on an equivalent V3 table"
(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=generate-interop-dv-table \
    -Dinterop.dv_table.dir="${TMP3}"
)

echo "==> [10/16] (D4 meta) Java: emit the canonical snapshot-metadata views + byte-diff Java's view of the RUST table against Java's own"
(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP3}/table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP3}/java_meta.json"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP3}/rust_table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP3}/java_view_rust_meta.json"
)
if ! diff -u "${TMP3}/java_meta.json" "${TMP3}/java_view_rust_meta.json"; then
  echo "==> FAILED — JAVA's view of the RUST-committed DV chain diverges from Java's own DV-chain semantics (added-dvs / operation / manifest structure)."
  exit 1
fi
echo "    dv_table: Java view of Rust table == Java view of Java table OK"

echo "==> [11/16] (D4 meta) Rust: assert ITS canonical views (of the Java table AND the Rust table) equal java_meta.json"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_DV_TABLE_DIR="${TMP3}" \
    cargo test -p iceberg --test interop_dv_table test_dv_meta_views_match_java -- --exact --nocapture
)

# ---------------------------------------------------------------------------------------------
# Arc-E Increment 2 — the DV REPLACEMENT chain (the BaseDVFileWriter.loadPreviousDeletes merge hook).
# Rust commits {fast_append, row_delta(DV1{1}), row_delta(add writer-merged DV2{1,3} + remove DV1)};
# Java reads the replacement table (rows reflect the merged DV, DV1 absent from the manifests),
# byte-compares the merged blob, and runs the metadata-level 3-way (the first LIVE `removed-dvs`).
# ---------------------------------------------------------------------------------------------

echo "==> [12/16] (replace table) Rust: COMMIT the V3 replacement chain (fast_append + DV1{1} + writer-MERGED DV2{1,3} replacing DV1) + expected_rows.json + expected_dvs.json + rust_merged_dv_blob.bin"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_DV_REPLACE_DIR="${TMP4}" \
    cargo test -p iceberg --test interop_dv_replace test_dv_replace_gen_rust_writes_replacement_table -- --exact --nocapture
)

echo "==> [13/16] (replace table) Java: read the RUST-COMMITTED replacement table with the PRODUCTION scan + manifest cross-check (exactly ONE live DV, DV1 ABSENT) + emit java_merged_dv_blob.bin"
# Same fail-closed sentinel rule (success sentinel present, no FAIL line; verdict from OUTPUT not
# mvn's exit code; `|| true` so set -e does not abort before the diagnostics are echoed).
REPLACE_VERIFY_OUT="$(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=verify-interop-dv-replace \
    -Dinterop.dv_replace.dir="${TMP4}" 2>&1
)" || true
echo "${REPLACE_VERIFY_OUT}"
if echo "${REPLACE_VERIFY_OUT}" | grep -q '^FAIL ' || ! echo "${REPLACE_VERIFY_OUT}" | grep -q 'verify-interop-dv-replace: 0 failures'; then
  echo "==> FAILED — Java could not correctly read the RUST-COMMITTED DV replacement table (a real replacement-incompatibility finding: resurrected rows or a stale/duplicate DV)."
  exit 1
fi

echo "==> [14/16] (replace) Rust: assert the merged DV blob is BYTE-IDENTICAL to Java's merge of the same {1}∪{3} (the Run-store re-serialization pin — array container)"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_DV_REPLACE_DIR="${TMP4}" \
    cargo test -p iceberg --test interop_dv_replace test_dv_replace_merged_blob_bytes -- --exact --nocapture
)

echo "==> [15/16] (replace meta) Java: write the JAVA mirror replacement chain + emit the canonical views + byte-diff Java's view of the RUST table against Java's own"
(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=generate-interop-dv-replace \
    -Dinterop.dv_replace.dir="${TMP4}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP4}/table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP4}/java_meta.json"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP4}/rust_table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP4}/java_view_rust_meta.json"
)
if ! diff -u "${TMP4}/java_meta.json" "${TMP4}/java_view_rust_meta.json"; then
  echo "==> FAILED — JAVA's view of the RUST replacement chain diverges from Java's own (removed-dvs / operation / tombstone structure)."
  exit 1
fi
echo "    dv_replace: Java view of Rust table == Java view of Java table OK"

echo "==> [16/16] (replace meta) Rust: assert ITS canonical views (of the Java table AND the Rust table) equal java_meta.json (the first LIVE removed-dvs comparison)"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_DV_REPLACE_DIR="${TMP4}" \
    cargo test -p iceberg --test interop_dv_replace test_dv_replace_meta_views_match_java -- --exact --nocapture
)

echo "==> DONE — deletion-vector interop passed BOTH directions, table + blob + metadata + REPLACEMENT level:"
echo "    D1: Rust scan == Java read (live rows {10,30,50,60,70,80}); raw blob decode matched incl. >2^32 positions + run containers."
echo "    D2: Java's production reader decoded the Rust-written Puffin DVs exactly; blobs byte-identical to Java's own serialization."
echo "    D4 table: Java's PRODUCTION scan read the RUST-COMMITTED V3 table (one puffin, two DVs, two partitions) -> {(10,x),(30,z),(50,q)}; manifest DeleteFile metadata cross-checked."
echo "    D4 meta: the canonical snapshot-metadata views of the Rust and Java DV chains are IDENTICAL all 3 ways (added-dvs, operation, manifest/seq semantics)."
echo "    REPLACE: the WRITER-merged DV replacement (loadPreviousDeletes) — Java read the Rust replacement table (merged DV applied, DV1 absent), merged blob byte-identical, and the metadata views match all 3 ways incl. the first LIVE removed-dvs."
