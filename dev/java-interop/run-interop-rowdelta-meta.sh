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
# METADATA-LEVEL row-delta interop harness (sprint increment E1) — the snapshot/manifest SEMANTICS
# proof on top of the data-level scan-exec interop (which proved the ROWS). Three fixtures (the
# unpartitioned position-delete chain, the equality-delete chain, the partitioned chain), each
# compared via the CANONICAL "snapshot metadata view" both sides emit (ordinal snapshots, COUNT-only
# summaries, manifest-list -> entry structure with POST-INHERITANCE sequence numbers; see
# SnapshotMetaOracle in InteropOracle.java and crates/iceberg/tests/interop_rowdelta_meta.rs):
#
#   1. JAVA writes each fixture table (the EXISTING generate-interop-* oracles, reused unchanged)
#      and emits java_meta.json — Java's view of Java's table.
#   2. RUST writes the equivalent table per fixture (the EXISTING env-gated GEN paths in
#      interop_scan_exec.rs, reused unchanged) -> <fixture>/rust_table.
#   3. JAVA emits its view of the RUST-written table (java_view_rust_meta.json) and this script
#      diffs it byte-for-byte against java_meta.json — JAVA judging Rust's written metadata.
#   4. RUST (interop_rowdelta_meta.rs) asserts ITS view of the Java table AND its view of the Rust
#      table both equal java_meta.json — read parity + write parity.
#
# A divergence anywhere in operation classification, summary counts, manifest split, or the
# sequence-number/inheritance chain fails loudly. TEST-ONLY oracle; nothing here is in the offline
# `cargo test` gate; the temp dirs under dev/java-interop/target/ are gitignored.
#
# Requirements: Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64, the
# repo's pinned Rust toolchain. Deps are the same ones the scan-exec scripts already pulled.
#
# Run from anywhere; paths resolve relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-rowdelta-meta"

MVN="/opt/maven/bin/mvn"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:${PATH}"

run_oracle() {
  (cd "${SCRIPT_DIR}" && "${MVN}" -o -q compile exec:java "$@" 2>&1)
}

echo "==> [1/6] Reset the temp dir: ${TMP}"
rm -rf "${TMP}"
mkdir -p "${TMP}/scan_exec" "${TMP}/eq_delete" "${TMP}/part_scan"

echo "==> [2/6] Java: write the three fixture tables (the existing generate oracles, reused)"
run_oracle -Dexec.args=generate-interop-scan-exec -Dinterop.scan_exec.dir="${TMP}/scan_exec"
run_oracle -Dexec.args=generate-interop-eq-delete -Dinterop.eq_delete.dir="${TMP}/eq_delete"
run_oracle -Dexec.args=generate-interop-part-scan -Dinterop.part_scan.dir="${TMP}/part_scan"

echo "==> [3/6] Java: emit java_meta.json — Java's canonical view of each JAVA-written table"
for fixture in scan_exec eq_delete part_scan; do
  run_oracle -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP}/${fixture}/table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP}/${fixture}/java_meta.json"
done

echo "==> [4/6] Rust: write the three equivalent tables via the PRODUCTION write path (existing GEN tests)"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_SCAN_GEN_DIR="${TMP}/scan_exec" \
  ICEBERG_INTEROP_EQ_SCAN_GEN_DIR="${TMP}/eq_delete" \
  ICEBERG_INTEROP_PART_SCAN_GEN_DIR="${TMP}/part_scan" \
    cargo test -p iceberg --test interop_scan_exec -- --nocapture
)

echo "==> [5/6] Java: emit + DIFF its view of each RUST-written table against java_meta.json"
for fixture in scan_exec eq_delete part_scan; do
  run_oracle -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP}/${fixture}/rust_table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP}/${fixture}/java_view_rust_meta.json"
  if ! diff -u "${TMP}/${fixture}/java_meta.json" "${TMP}/${fixture}/java_view_rust_meta.json"; then
    echo "==> FAILED — ${fixture}: JAVA's view of the RUST-written table diverges from Java's own semantics."
    exit 1
  fi
  echo "    ${fixture}: Java view of Rust table == Java view of Java table OK"
done

echo "==> [6/6] Rust: assert ITS canonical views (of the Java table AND the Rust table) equal java_meta.json"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_META_DIR="${TMP}" \
    cargo test -p iceberg --test interop_rowdelta_meta -- --nocapture
)

echo "==> DONE — metadata-level row-delta interop passed (3 fixtures x {Java-reads-Rust, Rust-reads-Java, Rust-self} canonical-view equality)."
