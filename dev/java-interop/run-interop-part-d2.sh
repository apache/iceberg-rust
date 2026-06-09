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
# DIRECTION-2 PARTITIONED scan-execution interop harness — "JAVA READS WHAT RUST WRITES", the partition-WRITE
# proof. The mirror image of run-interop-part.sh (Direction 1, where Java writes and Rust reads): here RUST
# writes a REAL on-disk table PARTITIONED by identity(category) with a PARTITION-SCOPED position delete via
# its PRODUCTION write path and JAVA reads it back, proving Rust's written manifests + per-partition parquet +
# partition-scoped position-delete (with the partition values stamped) are Java-readable.
#
# This is a TEST-ONLY ORACLE (a dev tool, like dev/spark/) — it is NOT part of the shipped Rust library, and
# it is NOT part of the offline `cargo test` gate (it needs Java + Maven). Nothing binary is committed — the
# committed artifacts are the oracle code (InteropOracle.java), the Rust test (interop_scan_exec.rs), and this
# run script. The temp table under dev/java-interop/target/ is gitignored.
#
# THE SEQUENCE ORDERING IS THE CORRECTNESS POINT. A position delete applies to data files with a data-
# sequence-number <= the delete's. The Rust GEN path fast_appends both per-partition DATA files first
# (sequence 1) and row_deltas the partition-scoped position delete SECOND (sequence 2), so the delete (seq 2)
# reaches the cat=a data file (seq 1).
#
# Methodology (Rust writes → Java reads-and-verifies):
#   1. ICEBERG_INTEROP_PART_SCAN_GEN_DIR="$TMP2" cargo test ... interop_scan_exec
#        -> The env-gated Rust GEN test builds a V2 table partitioned by identity(category) on local disk
#           under "$TMP2/rust_table" via its PRODUCTION write path: a `MemoryCatalog` over
#           `LocalFsStorageFactory` writes one REAL parquet DATA file PER PARTITION via the production
#           DataFileWriter built with a PartitionKey (which stamps the partition Struct + spec id 0 onto the
#           DataFile AND routes the parquet under the partition path), fast_appended at sequence 1, plus a
#           REAL PARTITION-SCOPED parquet POSITION-delete in partition a (position 1 = id=20) via
#           PositionDeleteFileWriter built with the cat=a PartitionKey, row_delta'd at sequence 2. It writes
#           "$TMP2/rust_table/metadata/final.metadata.json" to a known path. (Without the env var the test is
#           a clean no-op, so the offline gate stays green.)
#   2. mvn ... -Dexec.args=verify-interop-part-scan -Dinterop.part_scan.dir="$TMP2"
#        -> The Java oracle loads "$TMP2/rust_table/metadata/final.metadata.json", builds a BaseTable over a
#           LocalFileIO (resolving the bare absolute manifest + per-partition parquet paths the Rust commits
#           wrote), reads the rows with IcebergGenerics.read(table).build() (which APPLIES the Rust-written
#           partition-scoped position delete), sorts by id, and asserts the (id,data) rows ==
#           {(10,x),(30,z),(40,p),(50,q)} (id=20 deleted, both partitions otherwise intact). A FAIL is a REAL
#           partition-aware write-incompatibility finding; the verify exits non-zero.
#
# Requirements:
#   - Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64.
#   - A Rust toolchain (the repo's pinned nightly via rust-toolchain.toml).
#   - The Maven deps (iceberg-core / iceberg-data / iceberg-parquet / hadoop-client-runtime 1.10.0) are the
#     SAME ones the unpartitioned scan-exec harness already pulled — no new pom deps. If the local ~/.m2
#     cache is already populated, `mvn -o` runs fully offline.
#
# Run from anywhere; paths are resolved relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP2="${SCRIPT_DIR}/target/interop-part-d2"

echo "==> [1/3] Reset the temp table dir: ${TMP2}"
rm -rf "${TMP2}"
mkdir -p "${TMP2}"

echo "==> [2/3] Rust: WRITE a REAL PARTITIONED V2 table (per-partition parquet data seq 1 + partition-scoped position-delete seq 2) + final.metadata.json"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_PART_SCAN_GEN_DIR="${TMP2}" \
    cargo test -p iceberg --test interop_scan_exec test_part_scan_exec_gen_rust_writes_java_readable_partitioned_table -- --nocapture
)

echo "==> [3/3] Java: load the RUST-written final.metadata.json, read via IcebergGenerics, verify {10,30,40,50}"
# NOTE: `mvn -q exec:java` does NOT propagate the verify's `System.exit(1)` to the shell exit code, so
# capture the output and assert the success sentinel ("...: 0 failures") with no per-check FAIL line.
VERIFY_OUT="$(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=verify-interop-part-scan \
    -Dinterop.part_scan.dir="${TMP2}" 2>&1
)"
echo "${VERIFY_OUT}"
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' || ! echo "${VERIFY_OUT}" | grep -q ': 0 failures'; then
  echo "==> FAILED — Java could not correctly read the Rust-written partitioned table (a real partition-aware write-incompatibility finding)."
  exit 1
fi

echo "==> DONE — Direction-2 partitioned round-trip passed (Java read the Rust-written partitioned table, live rows {10,30,40,50}, id=20 absent, both partitions intact)."
