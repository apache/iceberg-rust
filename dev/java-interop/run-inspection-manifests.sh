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
# MANIFEST-READING inspection interop harness — the `files` / `data_files` / `delete_files` tables (A1),
# the `entries` / `manifests` / `partitions` tables (A2), the five cross-snapshot `all_*` tables
# `all_data_files` / `all_delete_files` / `all_files` / `all_entries` / `all_manifests` (A3, over the A2 table),
# AND SCAN PLANNING interop (A4): does Rust plan the SAME data files Java does for a given filter? — over a
# dedicated `table_a4` with four named filter scenarios (no_filter / partition_a / metric_id_gt_15 / combined).
#
# This is a TEST-ONLY ORACLE (a dev tool, like dev/spark/) — it is NOT part of the shipped Rust library, and
# it is NOT part of the offline `cargo test` gate (it needs Java + Maven). Unlike the pure-metadata
# inspection interop (committed JSON, offline), these tables read REAL ON-DISK AVRO manifests, so the oracle
# WRITES REAL TABLES to a temp dir each run and ENV-GATED Rust tests read them. Nothing binary is
# committed — the committed artifacts are the oracle code (InteropOracle.java), the Rust test
# (interop_inspection_manifests.rs), and this run script.
#
# Methodology (regenerate-and-compare):
#   1. mvn ... -Dexec.args=generate-inspection-manifests -Dinterop.inspection_manifests.dir="$TMP"
#        -> The Java oracle builds the A1 partitioned V2 table on local disk under "$TMP/table" via REAL
#           commits (newAppend writes a DATA manifest + manifest-list; newRowDelta writes a DELETE manifest),
#           writes "$TMP/table/metadata/final.metadata.json", and materializes the rows of Java's REAL
#           FilesTable / DataFilesTable / DeleteFilesTable into "$TMP/java_{files,data_files,delete_files}.json".
#           The SAME invocation ALSO builds a SECOND, richer A2 table under "$TMP/table_a2" (append A,B,C,D;
#           row-delta a position-delete for cat=a; delete B) and materializes Java's REAL ManifestEntriesTable
#           / ManifestsTable / PartitionsTable into "$TMP/java_{entries,manifests,partitions}.json", AND a
#           THIRD, dedicated A4 table under "$TMP/table_a4" (append F1/F2/F3 with distinct id metric bounds;
#           row-delta a position-delete for F1) over which it plans four named filter scenarios via Java's
#           REAL table.newScan().filter(expr).planFiles(), emitting "$TMP/java_scan_{no_filter,partition_a,
#           metric_id_gt_15,combined}.json".
#   2. ICEBERG_INTEROP_MANIFEST_DIR="$TMP" cargo test ... interop_inspection_manifests
#        -> The env-gated Rust tests load "$TMP/table[/_a2/_a4]/metadata/final.metadata.json", build a Table
#           over a local-filesystem FileIO (resolving the absolute manifest paths), run
#           inspect().files()/.data_files()/.delete_files() (A1) and .entries()/.manifests()/.partitions()
#           (A2) and the all_* tables (A3) .scan(), and — for A4 — table.scan().with_filter(pred).plan_files(),
#           comparing EVERY column (except the deferred readable_metrics) / the planned data-file SET +
#           delete association + residual-always-true field-for-field, order-independent, against Java.
#
# Without ICEBERG_INTEROP_MANIFEST_DIR the Rust test is a clean no-op (it stays green in the offline gate);
# this script is what flips it into the REAL comparison.
#
# Requirements:
#   - Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64.
#   - A Rust toolchain (the repo's pinned nightly via rust-toolchain.toml).
#   - First Maven run downloads iceberg-core/iceberg-api 1.10.0 from Maven Central.
#
# Run from anywhere; paths are resolved relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-manifests"

echo "==> [1/3] Reset the temp table dir: ${TMP}"
rm -rf "${TMP}"
mkdir -p "${TMP}"

echo "==> [2/3] Java oracle: write REAL partitioned V2 tables (A1 table + A2 table_a2 + A4 table_a4) + emit java_*.json"
(
  cd "${SCRIPT_DIR}"
  JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH \
    /opt/maven/bin/mvn -o -q compile exec:java \
    -Dexec.args=generate-inspection-manifests \
    -Dinterop.inspection_manifests.dir="${TMP}"
)

echo "==> [3/3] Rust: load final.metadata.json (A1) + table_a2/...(A2/A3) + table_a4/...(A4), scan tables + plan scans, compare vs Java"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_MANIFEST_DIR="${TMP}" \
    cargo test -p iceberg --test interop_inspection_manifests -- --nocapture
)

echo "==> DONE — manifest-reading inspection interop passed (A1 files/data_files/delete_files + A2 entries/manifests/partitions + A3 all_* + A4 scan planning)."
