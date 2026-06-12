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
# VIEW METADATA interop harness (increment I2) — proving bidirectional parity between
# Rust view metadata (via MemoryCatalog + ReplaceViewVersionAction + ViewMetadata::write_to)
# and Java iceberg-core 1.10.0 ViewMetadataParser (fromJson / toJson).
#
# FIXTURE: A single-schema view {1 id long, 2 name string} with two versions:
#   Version 1: SQL "SELECT id, name FROM events WHERE id > 0"   / dialect spark
#   Version 2: SQL "SELECT id, name FROM events WHERE id > 100" / dialect spark
#   (SQL_V1 ≠ SQL_V2 → reuseOrCreateNewViewVersionId produces 2 distinct versions.)
#
# THE CHAIN (6 steps, run twice — chain ×2):
#
#   1. Reset the temp dir.
#   2. Rust: GEN (Direction 1) — test_view_gen creates the two-version view, writes
#      rust_view_metadata.json (via ViewMetadata::write_to) + rust_view_expected.json.
#   3. Java: verify-interop-view (D1) — reads rust_view_metadata.json via PRODUCTION
#      ViewMetadataParser.fromJson, asserts all fields against rust_view_expected.json.
#      0 failures → sentinel "verify-interop-view: 0 failures".
#   4. Java: generate-interop-view-java-to-rust (D2 fixture) — InMemoryCatalog.buildView
#      create + replace, writes java_view_metadata.json + java_view_expected.json.
#   5. Rust: D2 — test_view_d2_rust_reads_java reads java_view_metadata.json via PRODUCTION
#      ViewMetadata::read_from, asserts all fields against java_view_expected.json.
#   6. Sabotage battery + tolerance control:
#        6a: alter the representation SQL in rust_view_metadata.json → Java D1 verify must FAIL
#        6b: drop the required "default-namespace" from a version → parse must FAIL on Rust side
#        6c: change current-version-id to a dangling id → parse must FAIL on Rust side
#        6d: alter the representation SQL in java_view_metadata.json → Rust D2 must detect mismatch
#        6e: TOLERANCE CONTROL — offline test_view_tolerance_controls (field-order + empty-props)
#   Controls: step 3 (clean Java D1 verify) is the control for 6a; step 5 (clean Rust D2) is the
#   control for 6b/6c/6d — all clean passes run before the sabotages in their respective chain.
#   7. Repeat the full chain (steps 1–6) a second time (chain ×2).
#
# Requirements: Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64, the
# repo's pinned Rust toolchain, python3. Run from anywhere; paths resolve relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-view"

MVN="/opt/maven/bin/mvn"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:${PATH}"

run_oracle() {
  (cd "${SCRIPT_DIR}" && "${MVN}" -o -q compile exec:java "$@" 2>&1)
}

run_chain() {
  local chain_num="$1"
  local total_steps=6

  echo "==> [${chain_num}/chains] CHAIN ${chain_num} BEGIN"

  # -----------------------------------------------------------------------------------------
  echo "    [1/${total_steps}] Reset the temp dir: ${TMP}"
  rm -rf "${TMP}"
  mkdir -p "${TMP}"

  # -----------------------------------------------------------------------------------------
  echo "    [2/${total_steps}] Rust: GEN (test_view_gen — create + replace view → rust_view_metadata.json)"
  (
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_VIEW_GEN_DIR="${TMP}" \
      cargo test -p iceberg --test interop_view test_view_gen \
      -- --exact --nocapture
  )

  if [[ ! -f "${TMP}/rust_view_metadata.json" ]]; then
    echo "==> FAILED (chain ${chain_num}): Rust GEN did not emit rust_view_metadata.json"
    exit 1
  fi
  if [[ ! -f "${TMP}/rust_view_expected.json" ]]; then
    echo "==> FAILED (chain ${chain_num}): Rust GEN did not emit rust_view_expected.json"
    exit 1
  fi
  echo "    [2/${total_steps}] Rust GEN: OK (rust_view_metadata.json + rust_view_expected.json)"

  # -----------------------------------------------------------------------------------------
  echo "    [3/${total_steps}] Java: verify-interop-view (D1 — Java reads Rust metadata)"
  VERIFY_D1_OUT="$(
    run_oracle \
      -Dexec.args=verify-interop-view \
      -Dinterop.view.dir="${TMP}"
  )" || true
  echo "${VERIFY_D1_OUT}"
  if echo "${VERIFY_D1_OUT}" | grep -q '^FAIL ' \
    || ! echo "${VERIFY_D1_OUT}" | grep -q 'verify-interop-view: 0 failures'; then
    echo "==> FAILED (chain ${chain_num}): Java rejected the Rust-written view metadata (D1)"
    exit 1
  fi
  echo "    [3/${total_steps}] D1 Java-reads-Rust: PASS"

  # -----------------------------------------------------------------------------------------
  echo "    [4/${total_steps}] Java: generate-interop-view-java-to-rust (D2 fixture)"
  run_oracle \
    -Dexec.args=generate-interop-view-java-to-rust \
    -Dinterop.view.dir="${TMP}"

  if [[ ! -f "${TMP}/java_view_metadata.json" ]]; then
    echo "==> FAILED (chain ${chain_num}): Java D2 generate did not emit java_view_metadata.json"
    exit 1
  fi
  if [[ ! -f "${TMP}/java_view_expected.json" ]]; then
    echo "==> FAILED (chain ${chain_num}): Java D2 generate did not emit java_view_expected.json"
    exit 1
  fi
  echo "    [4/${total_steps}] Java D2 generate: OK (java_view_metadata.json + java_view_expected.json)"

  # -----------------------------------------------------------------------------------------
  echo "    [5/${total_steps}] Rust: D2 — test_view_d2_rust_reads_java"
  (
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_VIEW_DIR="${TMP}" \
      cargo test -p iceberg --test interop_view test_view_d2_rust_reads_java \
      -- --exact --nocapture
  )
  echo "    [5/${total_steps}] D2 Rust-reads-Java: PASS"

  # -----------------------------------------------------------------------------------------
  echo "    [6/${total_steps}] Sabotage battery"

  # ---- 6a: alter the SQL in rust_view_metadata.json → Java D1 verify must FAIL ----
  # Semantic sabotage: change version 1's SQL to a bogus string.
  # Java's verifyRustMetadata checks that version[1].sql == SQL_V1; after the edit it
  # compares the injected string and must emit a FAIL line.
  echo "        6a: alter SQL in rust_view_metadata.json → Java D1 verify must FAIL"
  RUST_META="${TMP}/rust_view_metadata.json"
  cp "${RUST_META}" "${RUST_META}.bak"
  python3 -c "
import json, sys

with open(sys.argv[1]) as f:
    meta = json.load(f)

altered = False
for version in meta.get('versions', []):
    for rep in version.get('representations', []):
        if rep.get('type') == 'sql':
            rep['sql'] = '__SABOTAGE_6A__'
            altered = True
            break
    if altered:
        break

if not altered:
    print('ERROR: could not find a SQL representation to alter', file=sys.stderr)
    sys.exit(1)

with open(sys.argv[1], 'w') as f:
    json.dump(meta, f, indent=2)
print('6a: altered first SQL representation to __SABOTAGE_6A__')
" "${RUST_META}"
  SABOTAGE_6A="$(
    run_oracle \
      -Dexec.args=verify-interop-view \
      -Dinterop.view.dir="${TMP}"
  )" || true
  if echo "${SABOTAGE_6A}" | grep -q 'verify-interop-view: 0 failures'; then
    echo "==> SABOTAGE 6a FAILED (chain ${chain_num}): altered SQL still passed Java D1"
    cp "${RUST_META}.bak" "${RUST_META}"
    exit 1
  fi
  echo "        6a PASS: altered SQL caused Java D1 to fail as expected"
  cp "${RUST_META}.bak" "${RUST_META}"
  echo "        6a: restored"

  # ---- 6b: drop default-namespace from a version → Rust must fail to parse ----
  # The Rust serde for ViewVersionV1 has default_namespace as non-Option (required field).
  # Dropping it from a version in the SOURCE file and re-reading via ViewMetadata::read_from
  # must fail (DataInvalid or serde error) — the Rust D2 test will exit non-zero.
  # We apply the sabotage to java_view_metadata.json (the Java-written file) and re-run D2.
  echo "        6b: drop default-namespace from a version in java_view_metadata.json → Rust D2 must FAIL"
  JAVA_META="${TMP}/java_view_metadata.json"
  cp "${JAVA_META}" "${JAVA_META}.bak"
  python3 -c "
import json, sys

with open(sys.argv[1]) as f:
    meta = json.load(f)

altered = False
for version in meta.get('versions', []):
    if 'default-namespace' in version:
        del version['default-namespace']
        altered = True
        break

if not altered:
    print('ERROR: no version with default-namespace found to drop', file=sys.stderr)
    sys.exit(1)

with open(sys.argv[1], 'w') as f:
    json.dump(meta, f, indent=2)
print('6b: dropped default-namespace from first version')
" "${JAVA_META}"
  SABOTAGE_6B_OUT="$(
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_VIEW_DIR="${TMP}" \
      cargo test -p iceberg --test interop_view \
        test_view_d2_rust_reads_java \
      -- --exact --nocapture 2>&1
  )" || true
  # The Rust test must NOT pass (default-namespace is non-Option → serde parse error).
  if echo "${SABOTAGE_6B_OUT}" | grep -q "^test test_view_d2_rust_reads_java \.\.\. ok$"; then
    echo "==> SABOTAGE 6b FAILED (chain ${chain_num}): missing default-namespace still passed Rust D2"
    cp "${JAVA_META}.bak" "${JAVA_META}"
    exit 1
  fi
  echo "        6b PASS: missing default-namespace caused Rust D2 to fail as expected"
  cp "${JAVA_META}.bak" "${JAVA_META}"
  echo "        6b: restored"

  # ---- 6c: change current-version-id to a dangling id → Rust must FAIL to parse ----
  # Rust's ViewMetadata::validate checks that current-version-id exists in versions.
  # Setting it to 99 (not in the versions list) must make read_from return DataInvalid.
  echo "        6c: set current-version-id to dangling id 99 in java_view_metadata.json → Rust D2 must FAIL"
  cp "${JAVA_META}" "${JAVA_META}.bak"
  python3 -c "
import json, sys

with open(sys.argv[1]) as f:
    meta = json.load(f)

meta['current-version-id'] = 99

with open(sys.argv[1], 'w') as f:
    json.dump(meta, f, indent=2)
print('6c: set current-version-id to dangling id 99')
" "${JAVA_META}"
  SABOTAGE_6C_OUT="$(
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_VIEW_DIR="${TMP}" \
      cargo test -p iceberg --test interop_view \
        test_view_d2_rust_reads_java \
      -- --exact --nocapture 2>&1
  )" || true
  if echo "${SABOTAGE_6C_OUT}" | grep -q "^test test_view_d2_rust_reads_java \.\.\. ok$"; then
    echo "==> SABOTAGE 6c FAILED (chain ${chain_num}): dangling current-version-id still passed Rust D2"
    cp "${JAVA_META}.bak" "${JAVA_META}"
    exit 1
  fi
  echo "        6c PASS: dangling current-version-id caused Rust D2 to fail as expected"
  cp "${JAVA_META}.bak" "${JAVA_META}"
  echo "        6c: restored"

  # ---- 6d: alter SQL in java_view_metadata.json → Rust D2 must detect mismatch ----
  # The Rust D2 test compares the parsed SQL against the expected JSON.
  # Corrupting the SQL in the metadata (not the expected) causes the assert_eq! to fail.
  echo "        6d: alter SQL in java_view_metadata.json → Rust D2 assert_eq! must FAIL"
  cp "${JAVA_META}" "${JAVA_META}.bak"
  python3 -c "
import json, sys

with open(sys.argv[1]) as f:
    meta = json.load(f)

altered = False
for version in meta.get('versions', []):
    for rep in version.get('representations', []):
        if rep.get('type') == 'sql':
            rep['sql'] = '__SABOTAGE_6D__'
            altered = True
            break
    if altered:
        break

if not altered:
    print('ERROR: could not find a SQL representation to alter', file=sys.stderr)
    sys.exit(1)

with open(sys.argv[1], 'w') as f:
    json.dump(meta, f, indent=2)
print('6d: altered first SQL representation in java_view_metadata.json to __SABOTAGE_6D__')
" "${JAVA_META}"
  SABOTAGE_6D_OUT="$(
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_VIEW_DIR="${TMP}" \
      cargo test -p iceberg --test interop_view \
        test_view_d2_rust_reads_java \
      -- --exact --nocapture 2>&1
  )" || true
  if echo "${SABOTAGE_6D_OUT}" | grep -q "^test test_view_d2_rust_reads_java \.\.\. ok$"; then
    echo "==> SABOTAGE 6d FAILED (chain ${chain_num}): altered SQL in Java metadata still passed Rust D2"
    cp "${JAVA_META}.bak" "${JAVA_META}"
    exit 1
  fi
  echo "        6d PASS: altered SQL in Java metadata caused Rust D2 to fail as expected"
  cp "${JAVA_META}.bak" "${JAVA_META}"
  echo "        6d: restored"

  # ---- 6e: tolerance control — offline; field-order + empty-properties ----
  # This is an OFFLINE test (no env vars needed) that runs on every chain invocation.
  # It pins the COSMETIC divergence between Java and Rust:
  #   Java writes view-uuid first + omits "properties" when empty.
  #   Rust writes format-version first + always emits "properties":{}.
  #   Both parsers are order-insensitive and handle the absent/empty case.
  echo "        6e: tolerance control (field-order + empty-properties) — offline"
  (
    cd "${REPO_ROOT}"
    cargo test -p iceberg --test interop_view test_view_tolerance_controls \
      -- --exact --nocapture
  )
  echo "        6e PASS: tolerance control (field-order + empty-properties) confirmed"

  echo "    [6/${total_steps}] Sabotage battery: all 5 cases closed (6a/6b/6c/6d/6e)"
  echo "==> CHAIN ${chain_num} COMPLETE"
}

# ===========================================================================================
# Run the chain twice (chain ×2).
# ===========================================================================================

run_chain 1
run_chain 2

echo ""
echo "==> DONE — View-metadata interop PASSED (chain ×2) over fixture:"
echo "    View {id long, name string}, 2 versions:"
echo "      Version 1: SQL='SELECT id, name FROM events WHERE id > 0'   dialect=spark"
echo "      Version 2: SQL='SELECT id, name FROM events WHERE id > 100' dialect=spark"
echo ""
echo "    D1 (Rust writes via MemoryCatalog + ReplaceViewVersionAction, Java judges):"
echo "        rust_view_metadata.json: 2 versions, version-log 2 entries"
echo "        Java verify-interop-view: 0 failures (both chains)"
echo ""
echo "    D2 (Java writes via InMemoryCatalog.buildView create + replace, Rust reads):"
echo "        java_view_metadata.json: 2 versions, current-version-id=2"
echo "        Rust test_view_d2_rust_reads_java: PASS (both chains)"
echo ""
echo "    Sabotage battery: 5 cases all closed:"
echo "        6a: altered SQL in Rust metadata → Java D1 FAIL (semantic)"
echo "        6b: dropped default-namespace from version → Rust parse FAIL (required field)"
echo "        6c: dangling current-version-id=99 → Rust parse/validate FAIL"
echo "        6d: altered SQL in Java metadata → Rust D2 assert FAIL"
echo "        6e: tolerance control (field-order + empty-properties) — both directions"
echo ""
echo "    TOLERANCE CONTRACT pinned (cosmetic, not blocking):"
echo "        Java: view-uuid first, omits 'properties' when empty."
echo "        Rust: format-version first, always emits 'properties':{}."
echo "        Both parsers are order-insensitive and handle the absent/empty case."
