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
# DATA-LEVEL EQUALITY-DELETE merge-on-read interop harness, DIRECTION 1 — "Rust reads what JAVA writes". The
# sibling of run-interop-scan-exec.sh, but the merge-on-read mechanism is delete-by-VALUE (an equality delete
# keyed on a field id), not delete-by-position. Proves Rust's scan → Arrow with EQUALITY-delete application
# matches Java's OWN read of a JAVA-WRITTEN table containing REAL parquet data + a REAL equality-delete file.
#
# This is a TEST-ONLY ORACLE (a dev tool, like dev/spark/) — it is NOT part of the shipped Rust library, and
# it is NOT part of the offline `cargo test` gate (it needs Java + Maven). Nothing binary is committed — the
# committed artifacts are the oracle code (InteropOracle.java), the Rust test (interop_scan_exec.rs), and this
# run script. The temp table under dev/java-interop/target/ is gitignored.
#
# THE SEQUENCE ORDERING IS THE CORRECTNESS POINT. An equality delete applies to data files with a strictly
# LOWER data-sequence-number than the delete. The Java oracle commits the DATA first (newAppend, sequence 1)
# and the equality delete SECOND (newRowDelta, sequence 2), so the delete (seq 2) reaches the data (seq 1).
#
# Methodology (regenerate-and-compare):
#   1. mvn ... -Dexec.args=generate-interop-eq-delete -Dinterop.eq_delete.dir="$TMP"
#        -> The Java oracle builds an unpartitioned V2 table on local disk under "$TMP/table" via REAL
#           commits: it writes a REAL parquet DATA file (5 rows: (10,a)..(50,e)) via the generic appender,
#           appended at sequence 1, and a REAL parquet EQUALITY-delete file (equality_ids = [1] = the `id`
#           field, deleting rows id=20 and id=40) via the generic equality-delete writer
#           (GenericAppenderFactory.newEqDeleteWriter), committed via newRowDelta at sequence 2. It writes
#           "$TMP/table/metadata/final.metadata.json" and materializes Java's OWN merge-on-read READ (via
#           IcebergGenerics.read(table).build(), which APPLIES the equality delete) into
#           "$TMP/java_eq_scan_rows.json" = [{10,a},{30,c},{50,e}] (the GROUND TRUTH; 20 and 40 are deleted).
#   2. ICEBERG_INTEROP_EQ_SCAN_DIR="$TMP" cargo test ... interop_scan_exec
#        -> The env-gated Rust test loads "$TMP/table/metadata/final.metadata.json", builds a Table over a
#           local-filesystem FileIO (resolving the absolute manifest + parquet paths), runs
#           table.scan().build()?.to_arrow() (which applies the equality delete by VALUE), and asserts the
#           (id,data) rows EQUAL Java's read — exactly 3 rows, 20 and 40 ABSENT, values {10,30,50}.
#
# Without ICEBERG_INTEROP_EQ_SCAN_DIR the Rust test is a clean no-op (it stays green in the offline gate);
# this script is what flips it into the REAL comparison.
#
# Requirements:
#   - Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64.
#   - A Rust toolchain (the repo's pinned nightly via rust-toolchain.toml).
#   - The Maven deps (iceberg-core / iceberg-data / iceberg-parquet / hadoop-client-runtime 1.10.0) are the
#     SAME ones the position-delete scan-exec harness already pulled — no new pom deps. If the local ~/.m2
#     cache is already populated, `mvn -o` runs fully offline.
#
# Run from anywhere; paths are resolved relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-eq-delete"

echo "==> [1/3] Reset the temp table dir: ${TMP}"
rm -rf "${TMP}"
mkdir -p "${TMP}"

echo "==> [2/3] Java oracle: write a REAL unpartitioned V2 table (parquet data seq 1 + equality-delete seq 2) + emit java_eq_scan_rows.json"
(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=generate-interop-eq-delete \
    -Dinterop.eq_delete.dir="${TMP}"
)

echo "==> [3/3] Rust: load final.metadata.json, scan → Arrow (equality merge-on-read), compare vs Java's read"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_EQ_SCAN_DIR="${TMP}" \
    cargo test -p iceberg --test interop_scan_exec test_scan_exec_equality_delete_matches_java_read -- --nocapture
)

echo "==> DONE — equality-delete scan-execution interop passed (Rust scan == Java read, live rows {10,30,50}, 20/40 absent)."
