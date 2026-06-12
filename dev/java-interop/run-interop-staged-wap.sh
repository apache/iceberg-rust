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
# STAGED-WAP metadata-level interop harness (increment Z1) — proving the Rust
# `FastAppendAction::stage_only()` + `CherryPickAction` and Java 1.10.0 production
# `newFastAppend().set("wap.id","...").stageOnly().commit()` + `ManageSnapshots.cherrypick()`
# produce IDENTICAL canonical snapshot metadata, BOTH sides using REAL staging APIs.
#
# THREE FIXTURES (S-ff / S-replay / S-dedup), BOTH DIRECTIONS.
# TWO VIEWS PER FIXTURE: staged-but-unpublished state (NEW COVERAGE) + post-publish state.
#
# THE CHAIN:
#
#   1. Reset the temp dir.
#   2. Java: build each fixture with REAL stageOnly() — emit:
#      - java_staged_meta.json  (canonical view of the staged-but-unpublished table)
#      - java_final_meta.json   (canonical view of the post-publish table)
#      - wap_dedup_expected_rejection.json (S-dedup only)
#   3. Rust: perform the SAME chain (REAL stage_only() + cherry_pick), land:
#      - rust_staged_table/metadata/final.metadata.json (staged state)
#      - rust_final_table/metadata/final.metadata.json  (post-publish state)
#      - rust_wap_dedup_rejection.json (S-dedup only)
#   4. Java: emit + DIFF canonical views of BOTH Rust-produced tables vs Java's own views:
#      - emit-snapshot-meta over rust_staged_table → diff vs java_staged_meta.json
#      - emit-snapshot-meta over rust_final_table  → diff vs java_final_meta.json
#   5. Java: run verify-interop-staged-wap — asserts per-fixture facts (staged: current
#      unchanged, staged snapshot present; published: FF no new snapshot, replay
#      published-wap-id, dedup rejection confirmed).
#   6. Rust: assert ITS canonical views of BOTH Java-produced tables equal java_{staged,final}_meta.json.
#   7. Sabotage battery: 4 structural corruptions (metadata truncate / bogus-path per state) +
#      1 staged-state-specific sabotage (inject a ref pointing at the staged snapshot → the
#      staged-state view compare MUST FAIL, proving the view is actually comparing what we expect).
#
# A divergence anywhere — current-snapshot-id invariant, summary counts, manifest structure, or
# the staged snapshot's presence / absence from main — fails loudly. TEST-ONLY oracle; the temp
# dir under dev/java-interop/target/ is gitignored.
#
# Requirements: Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64, the
# repo's pinned Rust toolchain. Run from anywhere; paths resolve relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-staged-wap"

MVN="/opt/maven/bin/mvn"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:${PATH}"

FIXTURES=(S-ff S-replay S-dedup)

run_oracle() {
  (cd "${SCRIPT_DIR}" && "${MVN}" -o -q compile exec:java "$@" 2>&1)
}

echo "==> [1/7] Reset the temp dir: ${TMP}"
rm -rf "${TMP}"
mkdir -p "${TMP}"

echo "==> [2/7] Java: build each fixture with REAL stageOnly() — emit staged + final views"
run_oracle -Dexec.args=generate-interop-staged-wap -Dinterop.staged_wap.dir="${TMP}"

echo "==> [3/7] Rust: perform the SAME chain (REAL stage_only() + cherry_pick), land both states"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_STAGED_WAP_GEN_DIR="${TMP}" \
    cargo test -p iceberg --test interop_staged_wap test_staged_wap_gen_rust_produces_each_fixture \
    -- --exact --nocapture
)

echo "==> [4/7] Java: emit + DIFF canonical views of BOTH Rust-produced tables vs Java's own views"
for fixture in "${FIXTURES[@]}"; do
  echo "    ${fixture}: comparing STAGED state..."
  run_oracle -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP}/${fixture}/rust_staged_table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP}/${fixture}/java_view_rust_staged_meta.json"
  if ! diff -u "${TMP}/${fixture}/java_staged_meta.json" "${TMP}/${fixture}/java_view_rust_staged_meta.json"; then
    echo "==> FAILED — ${fixture}/staged: JAVA's view of the RUST STAGED table diverges."
    exit 1
  fi
  echo "    ${fixture}/staged: Java view of Rust staged table == Java view of Java staged table OK"

  echo "    ${fixture}: comparing FINAL state..."
  run_oracle -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP}/${fixture}/rust_final_table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP}/${fixture}/java_view_rust_final_meta.json"
  if ! diff -u "${TMP}/${fixture}/java_final_meta.json" "${TMP}/${fixture}/java_view_rust_final_meta.json"; then
    echo "==> FAILED — ${fixture}/final: JAVA's view of the RUST FINAL table diverges."
    exit 1
  fi
  echo "    ${fixture}/final: Java view of Rust final table == Java view of Java final table OK"
done

echo "==> [5/7] Java: verify-interop-staged-wap — per-fixture facts + dedup rejection confirmation"
STAGED_WAP_VERIFY_OUT="$(
  cd "${SCRIPT_DIR}"
  "${MVN}" -o -q compile exec:java \
    -Dexec.args=verify-interop-staged-wap \
    -Dinterop.staged_wap.dir="${TMP}" 2>&1
)" || true
echo "${STAGED_WAP_VERIFY_OUT}"
if echo "${STAGED_WAP_VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${STAGED_WAP_VERIFY_OUT}" | grep -q 'verify-interop-staged-wap: 0 failures'; then
  echo "==> FAILED — Java rejected the RUST-produced staged-WAP tables."
  exit 1
fi

echo "==> [6/7] Rust: assert ITS canonical views of BOTH Java-produced tables equal java_meta.json"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_STAGED_WAP_DIR="${TMP}" \
    cargo test -p iceberg --test interop_staged_wap test_rust_view_of_java_staged_wap_matches_java \
    -- --exact --nocapture
)

echo "==> [7/7] Sabotage battery"

# ---- Structural sabotage (metadata corruption) ----
# Sabotage 7a: truncate the S-ff STAGED metadata → Java emit must fail / diff must fail.
echo "    7a: truncate S-ff staged metadata → staged-state view compare must FAIL"
SFFSTAGEDDUMP="${TMP}/S-ff/rust_staged_table/metadata/final.metadata.json"
cp "${SFFSTAGEDDUMP}" "${SFFSTAGEDDUMP}.bak"
# Truncate to 10 bytes — guaranteed parse failure.
head -c 10 "${SFFSTAGEDDUMP}.bak" > "${SFFSTAGEDDUMP}"
SABOTAGE_7A_OUT="$(
  cd "${SCRIPT_DIR}"
  "${MVN}" -o -q compile exec:java \
    -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP}/S-ff/rust_staged_table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP}/S-ff/java_view_rust_staged_meta_sabotage.json" 2>&1
)" || true
if echo "${SABOTAGE_7A_OUT}" | grep -qiE "error|exception|failed"; then
  echo "    7a PASS: truncated metadata caused emit to fail as expected"
else
  # If emit succeeded, the diff must still fail.
  if diff -q "${TMP}/S-ff/java_staged_meta.json" "${TMP}/S-ff/java_view_rust_staged_meta_sabotage.json" >/dev/null 2>&1; then
    echo "==> SABOTAGE 7a FAILED: truncated staged metadata still produced a matching view"
    cp "${SFFSTAGEDDUMP}.bak" "${SFFSTAGEDDUMP}"
    exit 1
  fi
  echo "    7a PASS: truncated metadata caused a view divergence as expected"
fi
# Restore.
cp "${SFFSTAGEDDUMP}.bak" "${SFFSTAGEDDUMP}"
echo "    7a: restored"

# Sabotage 7b: truncate the S-replay FINAL metadata → final-state view compare must FAIL.
echo "    7b: truncate S-replay final metadata → final-state view compare must FAIL"
SREPLAYFINAL="${TMP}/S-replay/rust_final_table/metadata/final.metadata.json"
cp "${SREPLAYFINAL}" "${SREPLAYFINAL}.bak"
head -c 10 "${SREPLAYFINAL}.bak" > "${SREPLAYFINAL}"
SABOTAGE_7B_OUT="$(
  cd "${SCRIPT_DIR}"
  "${MVN}" -o -q compile exec:java \
    -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP}/S-replay/rust_final_table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP}/S-replay/java_view_rust_final_meta_sabotage.json" 2>&1
)" || true
if echo "${SABOTAGE_7B_OUT}" | grep -qiE "error|exception|failed"; then
  echo "    7b PASS: truncated metadata caused emit to fail as expected"
else
  if diff -q "${TMP}/S-replay/java_final_meta.json" "${TMP}/S-replay/java_view_rust_final_meta_sabotage.json" >/dev/null 2>&1; then
    echo "==> SABOTAGE 7b FAILED: truncated final metadata still produced a matching view"
    cp "${SREPLAYFINAL}.bak" "${SREPLAYFINAL}"
    exit 1
  fi
  echo "    7b PASS: truncated metadata caused a view divergence as expected"
fi
cp "${SREPLAYFINAL}.bak" "${SREPLAYFINAL}"
echo "    7b: restored"

# Sabotage 7c: inject a bogus snapshot path in the S-dedup staged manifest-list reference →
# the staged-state view compare must fail (or the emit must error).
echo "    7c: bogus data path in S-dedup staged metadata → view compare must FAIL"
SDEDUPSTAGED="${TMP}/S-dedup/rust_staged_table/metadata/final.metadata.json"
cp "${SDEDUPSTAGED}" "${SDEDUPSTAGED}.bak"
# Replace the first manifest-list path with a nonexistent path.
python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    meta = json.load(f)
for snap in meta.get('snapshots', []):
    if snap.get('manifest-list'):
        snap['manifest-list'] = snap['manifest-list'] + '.SABOTAGED'
        break
with open(sys.argv[1], 'w') as f:
    json.dump(meta, f)
" "${SDEDUPSTAGED}"
SABOTAGE_7C_OUT="$(
  cd "${SCRIPT_DIR}"
  "${MVN}" -o -q compile exec:java \
    -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP}/S-dedup/rust_staged_table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP}/S-dedup/java_view_rust_staged_meta_sabotage.json" 2>&1
)" || true
if echo "${SABOTAGE_7C_OUT}" | grep -qiE "error|exception|failed|no such file|FileNotFoundException"; then
  echo "    7c PASS: bogus path caused emit to fail (no such file) as expected"
else
  if diff -q "${TMP}/S-dedup/java_staged_meta.json" "${TMP}/S-dedup/java_view_rust_staged_meta_sabotage.json" >/dev/null 2>&1; then
    echo "==> SABOTAGE 7c FAILED: bogus path still produced a matching view"
    cp "${SDEDUPSTAGED}.bak" "${SDEDUPSTAGED}"
    exit 1
  fi
  echo "    7c PASS: bogus path caused a view divergence as expected"
fi
cp "${SDEDUPSTAGED}.bak" "${SDEDUPSTAGED}"
echo "    7c: restored"

# Sabotage 7d (STAGED-STATE-SPECIFIC): REMOVE the staged snapshot from the S-ff staged metadata.
# The canonical view will now have only 1 snapshot ordinal instead of 2, so the byte-diff against
# java_staged_meta.json MUST FAIL. This proves the view is actually testing that the staged
# ref-less snapshot IS present in metadata.snapshots() — the key new coverage of the
# staged-state view.
echo "    7d (staged-state-specific): remove staged snapshot from S-ff staged metadata → view MUST FAIL"
SFFSTAGEDFILE="${TMP}/S-ff/rust_staged_table/metadata/final.metadata.json"
cp "${SFFSTAGEDFILE}" "${SFFSTAGEDFILE}.bak"
python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    meta = json.load(f)
# Find the staged snapshot id (not the current).
current_id = meta.get('current-snapshot-id', -1)
staged_id = None
for snap in meta.get('snapshots', []):
    sid = snap.get('snapshot-id')
    if sid != current_id:
        staged_id = sid
        break
if staged_id is None:
    print('ERROR: no staged snapshot found in metadata', file=sys.stderr)
    sys.exit(1)
# Remove the staged snapshot from the snapshots list entirely.
meta['snapshots'] = [s for s in meta['snapshots'] if s.get('snapshot-id') != staged_id]
with open(sys.argv[1], 'w') as f:
    json.dump(meta, f)
print(f'removed staged snapshot {staged_id} from metadata')
" "${SFFSTAGEDFILE}"
SABOTAGE_7D_EMIT="$(
  cd "${SCRIPT_DIR}"
  "${MVN}" -o -q compile exec:java \
    -Dexec.args=emit-snapshot-meta \
    -Dinterop.meta.metadata="${TMP}/S-ff/rust_staged_table/metadata/final.metadata.json" \
    -Dinterop.meta.out="${TMP}/S-ff/java_view_rust_staged_meta_sabotage7d.json" 2>&1
)" || true
# The emit produces a 1-snapshot view; the canonical view MUST differ from java_staged_meta.json
# which has 2 ordinals (S0 + staged-w1). An error also counts as a proper closed failure.
if echo "${SABOTAGE_7D_EMIT}" | grep -qiE "error|exception|failed"; then
  echo "    7d PASS: removing staged snapshot caused emit to fail (proper closed failure)"
elif diff -q "${TMP}/S-ff/java_staged_meta.json" "${TMP}/S-ff/java_view_rust_staged_meta_sabotage7d.json" >/dev/null 2>&1; then
  echo "==> SABOTAGE 7d FAILED: removing the staged snapshot did NOT change the canonical view"
  echo "    — the view is not testing that the staged ref-less snapshot IS in metadata.snapshots()."
  cp "${SFFSTAGEDFILE}.bak" "${SFFSTAGEDFILE}"
  exit 1
else
  echo "    7d PASS: removing the staged snapshot caused view divergence as expected"
  echo "         (canonical view correctly encodes the staged ref-less snapshot's presence)"
fi
cp "${SFFSTAGEDFILE}.bak" "${SFFSTAGEDFILE}"
echo "    7d: restored"

echo ""
echo "==> DONE — Staged-WAP interop passed BOTH directions over 3 fixtures (S-ff / S-replay / S-dedup):"
echo "    D1 (Rust acts, Java judges):"
echo "        Staged-state view: Java's canonical view of each Rust staged table == Java's own view"
echo "        Final-state view:  Java's canonical view of each Rust final table  == Java's own view"
echo "        Java verify-interop-staged-wap: all per-fixture facts confirmed"
echo "    D2 (Java acts, Rust verifies):"
echo "        Rust's canonical view of each Java staged + final table == Java's view"
echo "    Sabotage battery: 4 corruptions all failed closed (7a/7b/7c/7d)"
echo ""
echo "    NEW COVERAGE: staged-state canonical view (a metadata view containing a staged"
echo "    ref-less snapshot compared cross-language) — both sides use REAL stageOnly() APIs."
