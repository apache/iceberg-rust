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
# CHERRYPICK metadata-level interop harness (increment S2) — proving the Rust CherryPickAction and
# Java 1.10.0 ManageSnapshots.cherrypick produce IDENTICAL canonical snapshot metadata on the SAME
# fixtures, judged by Java. Three fixtures (ff / replay / dedup), both directions.
#
# THE CHAIN:
#
#   1. Reset the temp dir.
#   2. Java: build each fixture (stage a snapshot + cherrypick it), emit java_meta.json +
#      dedup_expected_rejection.json (dedup only).
#   3. Rust: perform the SAME chain (stage + cherry_pick) per fixture, land rust_table/metadata/
#      final.metadata.json.
#   4. Java: emit its canonical view of each RUST-produced table (java_view_rust_meta.json) and
#      BYTE-DIFF it against java_meta.json (Java judging Rust's metadata correctness).
#   5. Java: run verify-interop-cherrypick — asserts (a) canonical view byte-equal, (b) per-fixture
#      facts (FF: 2 snapshots; replay/dedup: 4 snapshots + source-snapshot-id present; dedup: second
#      cherrypick raises CherrypickAncestorCommitException).
#   6. Rust: assert ITS canonical views (of the Java tables) equal java_meta.json.
#
# A divergence anywhere — operation classification, summary counts, manifest/list structure,
# snapshot count, or the source-snapshot-id presence — fails loudly. TEST-ONLY oracle; nothing here
# is in the offline `cargo test` gate; the temp dir under dev/java-interop/target/ is gitignored.
#
# Requirements: Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64, the
# repo's pinned Rust toolchain. Run from anywhere; paths resolve relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-cherrypick"

MVN="/opt/maven/bin/mvn"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:${PATH}"

FIXTURES=(ff replay dedup)

run_oracle() {
  (cd "${SCRIPT_DIR}" && "${MVN}" -o -q compile exec:java "$@" 2>&1)
}

echo "==> [1/6] Reset the temp dir: ${TMP}"
rm -rf "${TMP}"
mkdir -p "${TMP}"

echo "==> [2/6] Java: build each fixture (stage + cherrypick), emit java_meta.json + dedup_expected_rejection.json"
run_oracle -Dexec.args=generate-interop-cherrypick -Dinterop.cherrypick.dir="${TMP}"

echo "==> [3/6] Rust: perform the SAME chain (stage + cherry_pick) per fixture, land rust_table/metadata/final.metadata.json"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_CHERRYPICK_GEN_DIR="${TMP}" \
    cargo test -p iceberg --test interop_cherrypick test_cherrypick_gen_rust_produces_each_fixture \
    -- --exact --nocapture
)

echo "==> [4/6] Java: emit + DIFF its view of each RUST-produced table vs java_meta.json"
for fixture in "${FIXTURES[@]}"; do
  run_oracle -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP}/${fixture}/rust_table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP}/${fixture}/java_view_rust_meta.json"
  if ! diff -u "${TMP}/${fixture}/java_meta.json" "${TMP}/${fixture}/java_view_rust_meta.json"; then
    echo "==> FAILED — ${fixture}: JAVA's view of the RUST-produced table diverges from Java's own semantics."
    exit 1
  fi
  echo "    ${fixture}: Java view of Rust table == Java view of Java table OK"
done

echo "==> [5/6] Java: verify-interop-cherrypick — per-fixture facts + canonical view + dedup rejection"
# The verdict comes from the OUTPUT sentinel ("verify-interop-cherrypick: 0 failures"), never from
# mvn's exit code (`exec:java` does not reliably propagate System.exit). `|| true` keeps set -e from
# aborting before the diagnostics are echoed; the grep below provides the fail-closed guarantee.
CHERRYPICK_VERIFY_OUT="$(
  cd "${SCRIPT_DIR}"
  "${MVN}" -o -q compile exec:java \
    -Dexec.args=verify-interop-cherrypick \
    -Dinterop.cherrypick.dir="${TMP}" 2>&1
)" || true
echo "${CHERRYPICK_VERIFY_OUT}"
if echo "${CHERRYPICK_VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${CHERRYPICK_VERIFY_OUT}" | grep -q 'verify-interop-cherrypick: 0 failures'; then
  echo "==> FAILED — Java rejected the RUST-produced cherrypick tables."
  exit 1
fi

echo "==> [6/6] Rust: assert ITS canonical views (of the Java tables) equal java_meta.json"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_CHERRYPICK_DIR="${TMP}" \
    cargo test -p iceberg --test interop_cherrypick test_rust_view_of_java_cherrypick_matches_java_view \
    -- --exact --nocapture
)

echo "==> DONE — CherryPick interop passed BOTH directions over 3 fixtures (ff / replay / dedup):"
echo "    D1 (Rust acts, Java judges): Java's view of each Rust-produced table == Java's own view;"
echo "        Java's verify confirmed the per-fixture facts (FF snapshot count, source-snapshot-id,"
echo "        dedup rejection via CherrypickAncestorCommitException)."
echo "    D2 (Java acts, Rust verifies): Rust's canonical view of each Java-produced table == Java's view."
