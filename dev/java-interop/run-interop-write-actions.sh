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
# METADATA-LEVEL rewrite-family interop harness (sprint increment E2) — DeleteFiles,
# OverwriteFiles, ReplacePartitions, and RewriteFiles proven in ONE five-commit chain
# (fast-append → delete → overwrite → replace-partitions → rewrite) on a partitioned V2 table,
# through the SAME canonical snapshot-metadata view as E1 (SnapshotMetaOracle ↔
# common/snapshot_meta_view.rs). NO parquet — the actions only read/rewrite manifests, so the
# fixture is pure metadata. Three comparisons:
#
#   1. JAVA performs the chain (WriteActionsOracle) and emits java_meta.json.
#   2. RUST performs the SAME chain via its production write paths (the GEN test in
#      interop_write_actions_meta.rs) -> <dir>/rust_table; JAVA emits its view of it and this
#      script byte-DIFFS the two Java views — Java judging Rust's four write actions.
#   3. RUST asserts its views of BOTH chains equal java_meta.json.
#
# TEST-ONLY oracle; nothing here is in the offline cargo test gate; temp dirs gitignored.
# Requirements: Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64.
# Run from anywhere; paths resolve relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-write-actions"

MVN="/opt/maven/bin/mvn"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:${PATH}"

run_oracle() {
  (cd "${SCRIPT_DIR}" && "${MVN}" -o -q compile exec:java "$@" 2>&1)
}

echo "==> [1/5] Reset the temp dir: ${TMP}"
rm -rf "${TMP}"
mkdir -p "${TMP}"

echo "==> [2/5] Java: perform the five-commit write-actions chain + emit java_meta.json"
run_oracle -Dexec.args=generate-interop-write-actions -Dinterop.write_actions.dir="${TMP}"
run_oracle -Dexec.args=emit-snapshot-meta \
  -Dinterop.meta.metadata="${TMP}/table/metadata/final.metadata.json" \
  -Dinterop.meta.out="${TMP}/java_meta.json"

echo "==> [3/5] Rust: perform the SAME chain via the production write paths (GEN test)"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_WRITE_ACTIONS_GEN_DIR="${TMP}" \
    cargo test -p iceberg --test interop_write_actions_meta -- --nocapture
)

echo "==> [4/5] Java: emit + DIFF its view of the RUST chain against java_meta.json"
run_oracle -Dexec.args=emit-snapshot-meta \
  -Dinterop.meta.metadata="${TMP}/rust_table/metadata/final.metadata.json" \
  -Dinterop.meta.out="${TMP}/java_view_rust_meta.json"
if ! diff -u "${TMP}/java_meta.json" "${TMP}/java_view_rust_meta.json"; then
  echo "==> FAILED — JAVA's view of the RUST write-actions chain diverges from Java's own semantics."
  exit 1
fi
echo "    write_actions: Java view of Rust chain == Java view of Java chain OK"

echo "==> [5/5] Rust: assert ITS canonical views (of the Java chain AND the Rust chain) equal java_meta.json"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_WRITE_ACTIONS_DIR="${TMP}" \
    cargo test -p iceberg --test interop_write_actions_meta -- --nocapture
)

echo "==> DONE — metadata-level write-actions interop passed (DeleteFiles + OverwriteFiles + ReplacePartitions + RewriteFiles, one chain, 3 comparison directions)."
