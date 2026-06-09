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
# DIRECTION-2 data-level scan-execution interop harness — "JAVA READS WHAT RUST WRITES". The mirror image of
# run-interop-scan-exec.sh (Direction 1, where Java writes and Rust reads): here RUST writes a REAL on-disk
# table via its PRODUCTION write path and JAVA reads it back, proving Rust's written manifests + parquet +
# position-delete are Java-readable. This is the increment that flips the write actions (append / row_delta)
# toward "done" — the DoD is "Java reads what we write".
#
# This is a TEST-ONLY ORACLE (a dev tool, like dev/spark/) — it is NOT part of the shipped Rust library, and
# it is NOT part of the offline `cargo test` gate (it needs Java + Maven). Nothing binary is committed — the
# committed artifacts are the oracle code (InteropOracle.java), the Rust test (interop_scan_exec.rs), and
# this run script. The temp table under dev/java-interop/target/ is gitignored.
#
# Methodology (Rust writes → Java reads-and-verifies):
#   1. ICEBERG_INTEROP_SCAN_GEN_DIR="$TMP2" cargo test ... interop_scan_exec
#        -> The env-gated Rust GEN test builds an unpartitioned V2 table on local disk under
#           "$TMP2/rust_table" via its PRODUCTION write path: a `MemoryCatalog` over `LocalFsStorageFactory`
#           writes a REAL parquet DATA file (5 rows: (10,a)..(50,e)) via ParquetWriterBuilder, a REAL parquet
#           POSITION-DELETE file (deleting positions 1 and 3 = rows 20 and 40) via PositionDeleteFileWriter,
#           then fast_append(dataFile) + row_delta().add_deletes(deleteFile). It writes
#           "$TMP2/rust_table/metadata/final.metadata.json" to a known path. (Without the env var the test is
#           a clean no-op, so the offline gate stays green.)
#   2. mvn ... -Dexec.args=verify-interop-scan-exec -Dinterop.scan_exec.dir="$TMP2"
#        -> The Java oracle loads "$TMP2/rust_table/metadata/final.metadata.json", builds a BaseTable over a
#           LocalFileIO (resolving the bare absolute manifest + parquet paths the Rust commits wrote), reads
#           the rows with IcebergGenerics.read(table).build() (which APPLIES the Rust-written position
#           delete), sorts by id, and asserts the (id,data) rows == {(10,a),(30,c),(50,e)} (20/40 deleted).
#           A FAIL is a REAL write-incompatibility finding; the verify exits non-zero.
#
# Requirements:
#   - Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64.
#   - A Rust toolchain (the repo's pinned nightly via rust-toolchain.toml).
#   - The Maven deps (iceberg-core / iceberg-data / iceberg-parquet / hadoop-client-runtime 1.10.0) are the
#     SAME ones Direction-1 already pulled — no new pom deps. If the local ~/.m2 cache is already populated
#     (Direction-1 has run), `mvn -o` runs fully offline.
#
# Run from anywhere; paths are resolved relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP2="${SCRIPT_DIR}/target/interop-scan-exec-d2"

echo "==> [1/3] Reset the temp table dir: ${TMP2}"
rm -rf "${TMP2}"
mkdir -p "${TMP2}"

echo "==> [2/3] Rust: WRITE a REAL unpartitioned V2 table (parquet data + position-delete) + final.metadata.json"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_SCAN_GEN_DIR="${TMP2}" \
    cargo test -p iceberg --test interop_scan_exec -- --nocapture
)

echo "==> [3/3] Java: load the RUST-written final.metadata.json, read via IcebergGenerics, verify {10,30,50}"
# NOTE: `mvn -q exec:java` runs the oracle in Maven's own JVM and does NOT propagate the verify's
# `System.exit(1)` to the shell exit code. So capture the output and assert the success sentinel
# ("...: 0 failures") with no per-check FAIL line — otherwise this script would falsely pass on a real
# Java-read incompatibility.
VERIFY_OUT="$(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=verify-interop-scan-exec \
    -Dinterop.scan_exec.dir="${TMP2}" 2>&1
)"
echo "${VERIFY_OUT}"
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' || ! echo "${VERIFY_OUT}" | grep -q ': 0 failures'; then
  echo "==> FAILED — Java could not correctly read the Rust-written table (a real write-incompatibility finding)."
  exit 1
fi

echo "==> DONE — Direction-2 round-trip passed (Java read the Rust-written table, live rows {10,30,50}, 20/40 absent)."
