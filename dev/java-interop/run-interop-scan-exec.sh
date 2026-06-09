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
# DATA-LEVEL scan-execution interop harness — the merge-on-read capstone. Proves Rust's scan → Arrow with
# POSITION-DELETE application matches Java's OWN read of a JAVA-WRITTEN table containing REAL parquet data +
# a REAL position-delete file. Unlike the manifest-reading inspection interop (which reads metadata only),
# this writes the ACTUAL parquet bytes via the generic parquet appender + a real position-delete writer.
#
# This is a TEST-ONLY ORACLE (a dev tool, like dev/spark/) — it is NOT part of the shipped Rust library, and
# it is NOT part of the offline `cargo test` gate (it needs Java + Maven). Nothing binary is committed — the
# committed artifacts are the oracle code (InteropOracle.java), the Rust test (interop_scan_exec.rs), and
# this run script. The temp table under dev/java-interop/target/ is gitignored.
#
# Methodology (regenerate-and-compare):
#   1. mvn ... -Dexec.args=generate-interop-scan-exec -Dinterop.scan_exec.dir="$TMP"
#        -> The Java oracle builds an unpartitioned V2 table on local disk under "$TMP/table" via REAL
#           commits: it writes a REAL parquet DATA file (5 rows: (10,a)..(50,e)) via the generic appender and
#           a REAL parquet POSITION-DELETE file (deleting positions 1 and 3 = rows 20 and 40) via the generic
#           position-delete writer, then newAppend(dataFile) + newRowDelta(deleteFile). It writes
#           "$TMP/table/metadata/final.metadata.json" and materializes Java's OWN merge-on-read READ (via
#           IcebergGenerics.read(table).build(), which APPLIES the position deletes) into
#           "$TMP/java_scan_rows.json" = [{10,a},{30,c},{50,e}] (the GROUND TRUTH; 20 and 40 are deleted).
#   2. ICEBERG_INTEROP_SCAN_DIR="$TMP" cargo test ... interop_scan_exec
#        -> The env-gated Rust test loads "$TMP/table/metadata/final.metadata.json", builds a Table over a
#           local-filesystem FileIO (resolving the absolute manifest + parquet paths), runs
#           table.scan().build()?.to_arrow() (which applies the position deletes), and asserts the (id,data)
#           rows EQUAL Java's read — exactly 3 rows, 20 and 40 ABSENT, values {10,30,50}.
#
# Without ICEBERG_INTEROP_SCAN_DIR the Rust test is a clean no-op (it stays green in the offline gate); this
# script is what flips it into the REAL comparison.
#
# Requirements:
#   - Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64.
#   - A Rust toolchain (the repo's pinned nightly via rust-toolchain.toml).
#   - The FIRST Maven run must be ONLINE to download iceberg-data / iceberg-parquet / hadoop-client-runtime
#     1.10.0 from Maven Central (the oracle pom's new deps for the real parquet appender). After that run has
#     populated the local ~/.m2 cache, swap the `mvn` below for `mvn -o` to run fully offline.
#
# Run from anywhere; paths are resolved relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-scan-exec"

echo "==> [1/3] Reset the temp table dir: ${TMP}"
rm -rf "${TMP}"
mkdir -p "${TMP}"

echo "==> [2/3] Java oracle: write a REAL unpartitioned V2 table (parquet data + position-delete) + emit java_scan_rows.json"
(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -q compile exec:java \
    -Dexec.args=generate-interop-scan-exec \
    -Dinterop.scan_exec.dir="${TMP}"
)

echo "==> [3/3] Rust: load final.metadata.json, scan → Arrow (merge-on-read), compare vs Java's read"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_SCAN_DIR="${TMP}" \
    cargo test -p iceberg --test interop_scan_exec -- --nocapture
)

echo "==> DONE — data-level scan-execution interop passed (Rust scan == Java read, live rows {10,30,50}, 20/40 absent)."
