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
# DATA-LEVEL write-action interop harness (sprint increment S1) —
# MergeAppend (fixture A) and RewriteFiles (fixture B) proven at the DATA level: REAL parquet
# is written and Java's IcebergGenerics production scan reads it back.  Two fixtures, six steps:
#
#   Fixture A (merge_append): fast-append A(cat=a,10/20/30)+B(cat=b,40), set min-count-to-merge=2,
#     merge-append G(cat=a,60) — merge fires into ONE manifest; all 5 rows must survive.
#
#   Fixture B (rewrite_data): fast-append A(cat=a,10/20/30)+B(cat=b,40), pos-delete on A pos 1
#     (id=20), rewrite {A}→{A'} with data_sequence_number=1 — delete must STILL APPLY to A'
#     because A'.data_seq=1 < delete.seq=2; live rows = {10,30,40}.
#
# Per fixture, THREE comparisons (separate from the metadata-level run-interop-write-actions.sh):
#
#   1. JAVA writes the table + emits java_<fixture>_rows.json (ground truth).
#   2. RUST writes the SAME chain via its production paths (GEN tests in interop_write_data.rs)
#      → Java verify-interop-* reads it and asserts the live row set (sentinel grep).
#   3. RUST reads the Java-written table and asserts its scan == java_<fixture>_rows.json.
#
# WHY A NEW SCRIPT (not an extension of run-interop-write-actions.sh):
#   Data-level fixtures require REAL parquet written into their own temp directories;
#   adding data-level steps to the metadata script would conflate two structurally distinct
#   chains (metadata-only vs real-parquet) into one inconsistent multi-step harness.
#
# Test-only oracle; nothing here is in the offline cargo test gate; temp dirs gitignored.
# Requirements: Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64.
# Run from anywhere; paths resolve relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-write-data"
MERGE_DIR="${TMP}/merge_append_data"
REWRITE_DIR="${TMP}/rewrite_data"

MVN="/opt/maven/bin/mvn"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:${PATH}"

run_oracle() {
  (cd "${SCRIPT_DIR}" && "${MVN}" -o -q compile exec:java "$@" 2>&1)
}

echo "==> [1/6] Reset the temp dir: ${TMP}"
rm -rf "${TMP}"
mkdir -p "${MERGE_DIR}" "${REWRITE_DIR}"

echo "==> [2/6] Java: generate both fixtures (real parquet + java_<fixture>_rows.json)"
run_oracle -Dexec.args=generate-interop-merge-append-data \
  -Dinterop.merge_append_data.dir="${MERGE_DIR}"
run_oracle -Dexec.args=generate-interop-rewrite-data \
  -Dinterop.rewrite_data.dir="${REWRITE_DIR}"

echo "==> [3/6] Rust: generate both fixtures via the production write paths (GEN tests)"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_MERGE_APPEND_DATA_GEN_DIR="${MERGE_DIR}" \
  ICEBERG_INTEROP_REWRITE_DATA_GEN_DIR="${REWRITE_DIR}" \
    cargo test -p iceberg --test interop_write_data -- --nocapture
)

echo "==> [4/6] Java: verify-interop-merge-append-data — Java reads the Rust-written merge-append table"
VERIFY_OUT="$(run_oracle -Dexec.args=verify-interop-merge-append-data \
  -Dinterop.merge_append_data.dir="${MERGE_DIR}")" || true
echo "${VERIFY_OUT}"
# Fail-closed two ways (matching run-interop-expire.sh / run-interop-cherrypick.sh): a per-check
# `^FAIL ` line OR the absence of the `0 failures` sentinel. The `^FAIL` guard catches a verify that
# emits a FAIL line but desyncs its count, which the positive-sentinel check alone would miss.
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-merge-append-data: 0 failures'; then
  echo "==> FAILED — verify-interop-merge-append-data emitted a FAIL line or did not emit the '0 failures' sentinel."
  exit 1
fi

echo "==> [5/6] Java: verify-interop-rewrite-data — Java reads the Rust-written rewrite-data table"
VERIFY_OUT="$(run_oracle -Dexec.args=verify-interop-rewrite-data \
  -Dinterop.rewrite_data.dir="${REWRITE_DIR}")" || true
echo "${VERIFY_OUT}"
# Fail-closed two ways (see the merge-append step above).
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-rewrite-data: 0 failures'; then
  echo "==> FAILED — verify-interop-rewrite-data emitted a FAIL line or did not emit the '0 failures' sentinel."
  exit 1
fi

echo "==> [6/6] Rust: read the Java-written tables and assert row equality (comparison tests)"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_MERGE_APPEND_DATA_DIR="${MERGE_DIR}" \
  ICEBERG_INTEROP_REWRITE_DATA_DIR="${REWRITE_DIR}" \
    cargo test -p iceberg --test interop_write_data -- --nocapture
)

echo "==> DONE — data-level write-actions interop passed (MergeAppend fixture A + RewriteFiles fixture B, all 6 steps, 3 comparison directions each)."
