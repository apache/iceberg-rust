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
# PARTITIONED DATA-LEVEL scan-execution interop harness — the PARTITION-HANDLING proof, DIRECTION 1
# ("Rust reads what JAVA writes"). The sibling of run-interop-scan-exec.sh (UNPARTITIONED), but the table is
# PARTITIONED by identity(category) and the position-delete is PARTITION-SCOPED. Proves Rust's scan → Arrow
# applies a partition-scoped position delete exactly as Java's own read does: only the targeted row in the
# targeted partition is dropped; the other partition is intact.
#
# This is a TEST-ONLY ORACLE (a dev tool, like dev/spark/) — it is NOT part of the shipped Rust library, and
# it is NOT part of the offline `cargo test` gate (it needs Java + Maven). Nothing binary is committed — the
# committed artifacts are the oracle code (InteropOracle.java), the Rust test (interop_scan_exec.rs), and
# this run script. The temp table under dev/java-interop/target/ is gitignored.
#
# Methodology (regenerate-and-compare):
#   1. mvn ... -Dexec.args=generate-interop-part-scan -Dinterop.part_scan.dir="$TMP"
#        -> The Java oracle builds a V2 table partitioned by identity(category) on local disk under
#           "$TMP/table" via REAL commits. It writes one REAL parquet DATA file PER PARTITION via the generic
#           appender (category=a: (10,a,x),(20,a,y),(30,a,z); category=b: (40,b,p),(50,b,q)), each DataFile
#           stamped with its partition value (the category Struct, spec id 0), and a REAL PARTITION-SCOPED
#           parquet POSITION-DELETE file in partition a (deleting position 1 = id=20) via the generic
#           position-delete writer with the partition set. It commits newAppend(A,B) at sequence 1 +
#           newRowDelta(deleteA) at sequence 2, writes "$TMP/table/metadata/final.metadata.json", and
#           materializes Java's OWN merge-on-read READ into "$TMP/java_part_scan_rows.json" =
#           [{10,x},{30,z},{40,p},{50,q}] (the GROUND TRUTH; only id=20 is deleted).
#   2. ICEBERG_INTEROP_PART_SCAN_DIR="$TMP" cargo test ... interop_scan_exec
#        -> The env-gated Rust test loads "$TMP/table/metadata/final.metadata.json", builds a Table over a
#           local-filesystem FileIO (resolving the absolute manifest + per-partition parquet paths), runs
#           table.scan().build()?.to_arrow() (which applies the partition-scoped position delete), and
#           asserts the (id,data) rows EQUAL Java's read — exactly 4 rows, id=20 ABSENT, cat=a survivors
#           10/30 AND cat=b's 40/50 all present.
#
# Without ICEBERG_INTEROP_PART_SCAN_DIR the Rust test is a clean no-op (it stays green in the offline gate);
# this script is what flips it into the REAL comparison.
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
TMP="${SCRIPT_DIR}/target/interop-part"

echo "==> [1/3] Reset the temp table dir: ${TMP}"
rm -rf "${TMP}"
mkdir -p "${TMP}"

echo "==> [2/3] Java oracle: write a REAL PARTITIONED V2 table (per-partition parquet data + partition-scoped position-delete) + emit java_part_scan_rows.json"
(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=generate-interop-part-scan \
    -Dinterop.part_scan.dir="${TMP}"
)

echo "==> [3/3] Rust: load final.metadata.json, scan → Arrow (partition-aware merge-on-read), compare vs Java's read"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_PART_SCAN_DIR="${TMP}" \
    cargo test -p iceberg --test interop_scan_exec test_part_scan_exec_partition_scoped_merge_on_read_matches_java_read -- --nocapture
)

echo "==> DONE — partitioned scan-execution interop passed (Rust scan == Java read, live rows {10,30,40,50}, id=20 absent, both partitions intact)."
