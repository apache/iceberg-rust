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
# EXPIRE-SNAPSHOTS interop harness (increment A3) — proving the Rust ExpireSnapshotsAction (B1
# retention) + ExpireSnapshotsCleanup (B2 ReachableFileCleanup file GC) agree with Java 1.10.0
# RemoveSnapshots + cleanExpiredFiles(true) on the SAME fixtures, judged by Java where possible.
# Five fixtures (linear / tag_protected / stats / deletes / rewrite), each forcing Java down
# ReachableFileCleanup (the strategy the Rust side ports) via a surviving tag.
#
# THE CHAIN (mirrors run-interop-write-actions.sh's structure + the fail-closed sentinel discipline):
#
#   1. JAVA performs each fixture's chain + table.expireSnapshots()...cleanExpiredFiles(true)
#      .deleteWith(collector).commit(), emitting <fixture>/java_deleted.json (a path-independent
#      <funnel>@ord<N> descriptor) + the post-expire final.metadata.json.
#   2. JAVA emits its canonical snapshot-metadata view of each JAVA-expired table (java_meta.json).
#   3. RUST performs the SAME chain + ExpireSnapshotsAction + ExpireSnapshotsCleanup::commit_and_clean
#      with a COLLECTING deleter (collect-only) -> <fixture>/rust_table + rust_deleted.json.
#   4. JAVA emits its view of each RUST-expired table and this script byte-DIFFS the two Java views
#      (Java judging Rust's expired metadata) AND runs verify-interop-expire (Java asserts the
#      surviving-snapshot count + ref names match AND the deleted descriptors are set-equal).
#   5. RUST asserts ITS canonical views (of the Java tables) equal java_meta.json AND its deleted
#      descriptors equal Java's.
#
# A divergence anywhere — operation classification, summary counts, manifest/list structure,
# surviving-snapshot set, or the deleted-file set — fails loudly. TEST-ONLY oracle; nothing here is
# in the offline `cargo test` gate; the temp dir under dev/java-interop/target/ is gitignored.
#
# Requirements: Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64, the
# repo's pinned Rust toolchain. Run from anywhere; paths resolve relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-expire"

MVN="/opt/maven/bin/mvn"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:${PATH}"

FIXTURES=(linear tag_protected stats deletes rewrite)

run_oracle() {
  (cd "${SCRIPT_DIR}" && "${MVN}" -o -q compile exec:java "$@" 2>&1)
}

echo "==> [1/6] Reset the temp dir: ${TMP}"
rm -rf "${TMP}"
mkdir -p "${TMP}"

echo "==> [2/6] Java: build + expire each fixture (deleteWith collector) + emit java_deleted.json + final.metadata.json"
run_oracle -Dexec.args=generate-interop-expire -Dinterop.expire.dir="${TMP}"

echo "==> [3/6] Java: emit java_meta.json — Java's canonical view of each JAVA-expired table"
for fixture in "${FIXTURES[@]}"; do
  run_oracle -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP}/${fixture}/table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP}/${fixture}/java_meta.json"
done

echo "==> [4/6] Rust: perform the SAME chain + ExpireSnapshotsAction + commit_and_clean (collect-only) per fixture"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_EXPIRE_GEN_DIR="${TMP}" \
    cargo test -p iceberg --test interop_expire test_expire_gen_rust_expires_the_fixtures \
    -- --exact --nocapture
)

echo "==> [5/6] Java: emit + DIFF its view of each RUST-expired table vs java_meta.json, then verify both ways"
for fixture in "${FIXTURES[@]}"; do
  run_oracle -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP}/${fixture}/rust_table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP}/${fixture}/java_view_rust_meta.json"
  if ! diff -u "${TMP}/${fixture}/java_meta.json" "${TMP}/${fixture}/java_view_rust_meta.json"; then
    echo "==> FAILED — ${fixture}: JAVA's view of the RUST-expired table diverges from Java's own semantics."
    exit 1
  fi
  echo "    ${fixture}: Java view of Rust table == Java view of Java table OK"
done
# verify-interop-expire: Java asserts (a) surviving-snapshot count + ref names match and (b) the
# Rust-collected deleted descriptor equals Java's. The verdict comes from the OUTPUT sentinel
# ("verify-interop-expire: 0 failures" with no per-fixture FAIL line), never from mvn's exit code
# (machine-dependent for `exec:java` System.exit); `|| true` keeps set -e from aborting before the
# diagnostics are echoed (the run-interop-dv.sh fail-closed rule).
EXPIRE_VERIFY_OUT="$(
  cd "${SCRIPT_DIR}"
  "${MVN}" -o -q compile exec:java \
    -Dexec.args=verify-interop-expire \
    -Dinterop.expire.dir="${TMP}" 2>&1
)" || true
echo "${EXPIRE_VERIFY_OUT}"
if echo "${EXPIRE_VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${EXPIRE_VERIFY_OUT}" | grep -q 'verify-interop-expire: 0 failures'; then
  echo "==> FAILED — Java rejected the RUST-expired tables (surviving snapshots/refs or deleted set diverged)."
  exit 1
fi

echo "==> [6/6] Rust: assert ITS canonical views (of the Java tables) equal java_meta.json AND its deleted descriptors equal Java's"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_EXPIRE_DIR="${TMP}" \
    cargo test -p iceberg --test interop_expire test_rust_view_of_java_expire_matches_java_view \
    -- --exact --nocapture
  ICEBERG_INTEROP_EXPIRE_DIR="${TMP}" \
    cargo test -p iceberg --test interop_expire test_rust_deleted_descriptor_matches_java \
    -- --exact --nocapture
)

echo "==> DONE — ExpireSnapshots interop passed BOTH directions over 5 fixtures:"
echo "    D1 (Rust acts, Java judges): Java's view of each Rust-expired table == Java's own view; Java's"
echo "        verify confirmed the surviving snapshots/refs + the deleted-file descriptor match."
echo "    D2 (Java acts, Rust verifies): Rust's canonical view of each Java-expired table == Java's view,"
echo "        and Rust's cleanup deleted descriptor == Java's deleteWith set (path-independent <funnel>@ord<N>)."
