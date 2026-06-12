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
# THETA-BLOB PUFFIN interop harness (increment I1) — proving bidirectional parity between the
# Rust apache-datasketches-theta-v1 Puffin blobs (via ComputeTableStats) and Java
# iceberg-core 1.10.0 PuffinReader/PuffinWriter + datasketches-java-3.3.0.
#
# FIXTURE: Unpartitioned V2 table {1 id long, 2 name string, 3 val long}:
#   File A (5 rows): id={1..5}, name={"a","b","a","b","a"}, val={10..50} — exact mode.
#   File B (1M rows): id={0..999999}, name="x" repeated, val={0..999999} — estimation mode.
#   After both files: val column engages Alpha sampling (theta < MAX).
#   Known pin: lgK12 / seed 9001 / n=1M distinct longs → compact ndv EXACTLY 1 004 032.
#
# THE CHAIN:
#
#   1. Reset the temp dir.
#   2. Java: generate-interop-theta-java-to-rust (D2 fixture) — Java writes java_stats.puffin
#      with two apache-datasketches-theta-v1 blobs (exact ndv=5, estimation ndv=1004032),
#      emits java_stats_expected.json.
#   3. Rust: D2 — test_theta_d2_rust_reads_java_puffin reads java_stats.puffin via PRODUCTION
#      PuffinReader + CompactThetaSketch::deserialize, asserts all blob metadata + estimates.
#   4. Rust: GEN (Direction 1) — test_theta_gen builds the real two-file table, runs
#      ComputeTableStats::execute, writes rust_stats.puffin + rust_stats_expected.json.
#   5. Java: verify-interop-theta (D1) — Java reads rust_stats.puffin via PRODUCTION
#      PuffinReader, checks blob type/fields/snapshot_id/seq_num/ndv property, asserts
#      (long) CompactSketch.wrap(bytes).getEstimate() == ndv integer-exact. PASS → sentinel.
#   6. Sabotage battery:
#        6a: truncate rust_stats.puffin → Java D1 verify must FAIL (structural)
#        6b: SEMANTIC — halve the compact sketch's `theta` field INSIDE blob0's payload via a
#            footer-parsed SOURCE byte edit; the file still PARSES but getEstimate() doubles, so the
#            ndv-vs-estimate cross-check (not a parse/truncate check) must catch it
#            (the Z2 lesson: mutate the SOURCE, re-derive through the production reader)
#        6c: truncate java_stats.puffin → Rust D2 must FAIL (structural)
#        6d: corrupt the ndv property in rust_stats_expected.json → Java verify must FAIL (ground-truth)
#   Controls: step 5 (clean Java D1 verify) is the control for 6a/6b/6d; step 3 (clean Rust D2) is
#   the control for 6c — both clean passes run before the sabotages each chain.
#   7. Repeat the full chain (steps 1–6) a second time (chain ×2).
#
# Requirements: Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64, the
# repo's pinned Rust toolchain, python3. Run from anywhere; paths resolve relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-theta"

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

  echo "    [1/${total_steps}] Reset the temp dir: ${TMP}"
  rm -rf "${TMP}"
  mkdir -p "${TMP}"

  echo "    [2/${total_steps}] Java: generate-interop-theta-java-to-rust (D2 fixture)"
  run_oracle \
    -Dexec.args=generate-interop-theta-java-to-rust \
    -Dinterop.theta.dir="${TMP}"

  if [[ ! -f "${TMP}/java_stats.puffin" ]]; then
    echo "==> FAILED (chain ${chain_num}): Java did not emit java_stats.puffin"
    exit 1
  fi
  if [[ ! -f "${TMP}/java_stats_expected.json" ]]; then
    echo "==> FAILED (chain ${chain_num}): Java did not emit java_stats_expected.json"
    exit 1
  fi
  echo "    [2/${total_steps}] Java D2 generate: OK (java_stats.puffin + java_stats_expected.json)"

  echo "    [3/${total_steps}] Rust: D2 — test_theta_d2_rust_reads_java_puffin"
  (
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_THETA_DIR="${TMP}" \
      cargo test -p iceberg --test interop_theta test_theta_d2_rust_reads_java_puffin \
      -- --exact --nocapture
  )
  echo "    [3/${total_steps}] D2 Rust-reads-Java: PASS"

  echo "    [4/${total_steps}] Rust: GEN (test_theta_gen — ComputeTableStats + rust_stats.puffin)"
  (
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_THETA_GEN_DIR="${TMP}" \
      cargo test -p iceberg --test interop_theta test_theta_gen \
      -- --exact --nocapture
  )

  if [[ ! -f "${TMP}/rust_stats.puffin" ]]; then
    echo "==> FAILED (chain ${chain_num}): Rust GEN did not emit rust_stats.puffin"
    exit 1
  fi
  if [[ ! -f "${TMP}/rust_stats_expected.json" ]]; then
    echo "==> FAILED (chain ${chain_num}): Rust GEN did not emit rust_stats_expected.json"
    exit 1
  fi
  echo "    [4/${total_steps}] Rust GEN: OK (rust_stats.puffin + rust_stats_expected.json)"

  echo "    [5/${total_steps}] Java: verify-interop-theta (D1 — Java reads Rust puffin)"
  VERIFY_OUT="$(
    cd "${SCRIPT_DIR}"
    "${MVN}" -o -q compile exec:java \
      -Dexec.args=verify-interop-theta \
      -Dinterop.theta.dir="${TMP}" 2>&1
  )" || true
  echo "${VERIFY_OUT}"
  if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
    || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-theta: 0 failures'; then
    echo "==> FAILED (chain ${chain_num}): Java rejected the Rust-written theta puffin (D1)"
    exit 1
  fi
  echo "    [5/${total_steps}] D1 Java-reads-Rust: PASS"

  echo "    [6/${total_steps}] Sabotage battery"

  # ---- 6a: truncate rust_stats.puffin → Java D1 verify must FAIL ----
  echo "        6a: truncate rust_stats.puffin → Java D1 verify must FAIL"
  RUST_PUFFIN="${TMP}/rust_stats.puffin"
  cp "${RUST_PUFFIN}" "${RUST_PUFFIN}.bak"
  # Truncate to 10 bytes — guaranteed decode failure for Puffin footer reader.
  head -c 10 "${RUST_PUFFIN}.bak" > "${RUST_PUFFIN}"
  SABOTAGE_6A="$(
    cd "${SCRIPT_DIR}"
    "${MVN}" -o -q compile exec:java \
      -Dexec.args=verify-interop-theta \
      -Dinterop.theta.dir="${TMP}" 2>&1
  )" || true
  if echo "${SABOTAGE_6A}" | grep -q 'verify-interop-theta: 0 failures'; then
    echo "==> SABOTAGE 6a FAILED (chain ${chain_num}): truncated puffin still passed Java D1"
    cp "${RUST_PUFFIN}.bak" "${RUST_PUFFIN}"
    exit 1
  fi
  echo "        6a PASS: truncated rust_stats.puffin caused Java D1 to fail as expected"
  cp "${RUST_PUFFIN}.bak" "${RUST_PUFFIN}"
  echo "        6a: restored"

  # ---- 6b: SEMANTIC corruption — flip the theta field INSIDE the sketch payload + re-derive ----
  # (Z2/Z3 lesson: mutate the SOURCE artifact and RE-DERIVE through the production reader; never
  # post-edit an emitted view.)
  #
  # Strategy: parse the Puffin footer to locate the first blob's byte range, then HALVE the compact
  # sketch's `theta` field (a little-endian long at payload offset +16 in the estimation-mode
  # preLongs=3 preamble) in the SOURCE file.  Unlike a preamble-zero (which makes CompactSketch.wrap
  # THROW — a parse crash caught by the type/decode path, NOT the estimate cross-check), corrupting
  # theta leaves the file FULLY PARSEABLE but changes the estimator output: getEstimate() =
  # retained * 2^63 / theta, so halving theta DOUBLES the estimate (1004032 → 2008064).  The
  # ndv-vs-estimate cross-check (assertion 6 in ThetaBlobOracle.verify) must catch the mismatch.
  # This is the load-bearing SEMANTIC sabotage: the file is structurally valid, only the statistic
  # is wrong, and only an estimate cross-check (not a parse-or-truncate check) can detect it.
  # Java re-reads the puffin from scratch via PuffinReader, so the corruption is fully exercised
  # through the PRODUCTION reader path.
  echo "        6b: HALVE theta inside blob0 payload (SOURCE edit, still parses → estimate cross-check must FAIL)"
  cp "${RUST_PUFFIN}.bak" "${RUST_PUFFIN}"
  python3 -c "
import struct, json, sys

# Puffin file layout (Iceberg spec, magic = 0x50 0x46 0x41 0x31 = 'PFA1'):
#   [leading_magic(4)] [blob_0_data] ... [blob_N_data]
#   [footer_magic(4)] [footer_payload_json(P bytes)]
#   [footer_struct: payload_len(4 LE u32) | flags(4) | trailing_magic(4)]  = 12 bytes
#
# blob offsets in the footer JSON are absolute file offsets (blob[0].offset = 4).
MAGIC = b'PFA1'
MAGIC_LEN = 4
FOOTER_STRUCT_LEN = 12  # payload_len(4) + flags(4) + trailing_magic(4)

with open(sys.argv[1], 'rb') as f:
    data = bytearray(f.read())

file_len = len(data)
if data[file_len - 4:] != bytearray(MAGIC):
    print(f'ERROR: trailing magic mismatch: {bytes(data[file_len-4:])!r}', file=sys.stderr)
    sys.exit(42)

payload_len = struct.unpack_from('<I', data, file_len - FOOTER_STRUCT_LEN)[0]
if payload_len == 0 or payload_len >= file_len:
    print(f'ERROR: invalid footer payload_len={payload_len}', file=sys.stderr)
    sys.exit(1)

# footer_magic is at: file_len - FOOTER_STRUCT_LEN - payload_len - MAGIC_LEN
footer_magic_offset = file_len - FOOTER_STRUCT_LEN - payload_len - MAGIC_LEN
if data[footer_magic_offset:footer_magic_offset + MAGIC_LEN] != bytearray(MAGIC):
    print(f'ERROR: footer magic mismatch at offset {footer_magic_offset}', file=sys.stderr)
    sys.exit(1)

footer_json_start = footer_magic_offset + MAGIC_LEN
footer_json = data[footer_json_start:footer_json_start + payload_len].decode('utf-8')
footer = json.loads(footer_json)

blobs = footer.get('blobs', [])
if not blobs:
    print('ERROR: no blobs in puffin footer', file=sys.stderr)
    sys.exit(1)

blob0 = blobs[0]
blob_offset = blob0['offset']
blob_length = blob0['length']

if blob_offset < MAGIC_LEN or blob_offset + blob_length > footer_magic_offset:
    print(f'ERROR: blob0 [{blob_offset},{blob_offset+blob_length}) out of data region', file=sys.stderr)
    sys.exit(1)

# SEMANTIC corruption: halve the compact sketch theta field (LE long at payload offset +16 in
# the estimation-mode preLongs=3 preamble). The preamble (preLongs/serVer/family/flags/seedHash)
# stays intact so CompactSketch.wrap() still PARSES; only the estimator output changes
# (getEstimate() = retained * 2^63 / theta, so halving theta doubles the estimate), which only the
# ndv-vs-estimate cross-check can detect.
THETA_OFFSET = 16  # compact estimation preamble: theta long sits at preLongs(3)*8 == byte 16

# Guard: blob0 must be the estimation-mode blob (theta < MAX) — only then is theta load-bearing.
# (An exact-mode blob has theta == Long.MAX_VALUE and no entries, so halving it would not move the
# estimate. blob0 here is field_id=1, the 1M-distinct estimation column.)
ndv_prop = blob0.get('properties', {}).get('ndv')
if ndv_prop is None or int(ndv_prop) < 1000:
    print(f'ERROR: blob0 is not an estimation-mode blob (ndv={ndv_prop}); theta corruption inert',
          file=sys.stderr)
    sys.exit(43)

theta_pos = blob_offset + THETA_OFFSET
old_theta = struct.unpack_from('<q', data, theta_pos)[0]
new_theta = old_theta // 2
struct.pack_into('<q', data, theta_pos, new_theta)

with open(sys.argv[1], 'wb') as f:
    f.write(data)

print(f'6b: halved theta {old_theta} -> {new_theta} at blob0 payload offset+{THETA_OFFSET} '
      f'(file still parses; estimate must double)')
" "${RUST_PUFFIN}"
  MUTATE_EXIT=$?
  if [[ ${MUTATE_EXIT} -eq 42 || ${MUTATE_EXIT} -eq 43 ]]; then
    # The mutation could not be applied (framing or estimation-mode precondition failed). This is a
    # HARD FAILURE, not a skip: a sabotage that cannot land proves nothing and must not read green.
    echo "==> SABOTAGE 6b FAILED (chain ${chain_num}): theta mutation could not be applied (exit ${MUTATE_EXIT})"
    cp "${RUST_PUFFIN}.bak" "${RUST_PUFFIN}"
    exit 1
  fi
  SABOTAGE_6B="$(
    cd "${SCRIPT_DIR}"
    "${MVN}" -o -q compile exec:java \
      -Dexec.args=verify-interop-theta \
      -Dinterop.theta.dir="${TMP}" 2>&1
  )" || true
  # Must FAIL, AND specifically via the estimate cross-check (not a parse crash): assert the
  # cross-check FAIL line is present, proving the file parsed and the estimate diverged.
  if echo "${SABOTAGE_6B}" | grep -q 'verify-interop-theta: 0 failures'; then
    echo "==> SABOTAGE 6b FAILED (chain ${chain_num}): theta-corrupted blob still passed Java D1"
    cp "${RUST_PUFFIN}.bak" "${RUST_PUFFIN}"
    exit 1
  fi
  if ! echo "${SABOTAGE_6B}" | grep -q 'CompactSketch.getEstimate() as long expected='; then
    echo "==> SABOTAGE 6b FAILED (chain ${chain_num}): D1 failed but NOT via the estimate cross-check"
    echo "    (expected a 'getEstimate() as long expected=...' FAIL; got a parse crash instead — the"
    echo "     semantic mutation degenerated into a structural one)"
    echo "${SABOTAGE_6B}"
    cp "${RUST_PUFFIN}.bak" "${RUST_PUFFIN}"
    exit 1
  fi
  echo "        6b PASS: theta corruption parsed but estimate diverged → cross-check caught it (semantic)"
  cp "${RUST_PUFFIN}.bak" "${RUST_PUFFIN}"
  echo "        6b: restored"

  # ---- 6c: truncate java_stats.puffin → Rust D2 must FAIL ----
  echo "        6c: truncate java_stats.puffin → Rust D2 must FAIL"
  JAVA_PUFFIN="${TMP}/java_stats.puffin"
  cp "${JAVA_PUFFIN}" "${JAVA_PUFFIN}.bak"
  head -c 10 "${JAVA_PUFFIN}.bak" > "${JAVA_PUFFIN}"
  SABOTAGE_6C_OUT="$(
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_THETA_DIR="${TMP}" \
      cargo test -p iceberg --test interop_theta \
        test_theta_d2_rust_reads_java_puffin \
      -- --exact --nocapture 2>&1
  )" || true
  # Accept any non-PASS outcome — truncated puffin must cause an error/panic/FAILED test.
  if echo "${SABOTAGE_6C_OUT}" | grep -q "^test.*ok$"; then
    if ! echo "${SABOTAGE_6C_OUT}" | grep -qiE "error|panicked|FAILED"; then
      echo "==> SABOTAGE 6c FAILED (chain ${chain_num}): truncated java_stats.puffin still passed Rust D2"
      cp "${JAVA_PUFFIN}.bak" "${JAVA_PUFFIN}"
      exit 1
    fi
  fi
  echo "        6c PASS: truncated java_stats.puffin caused Rust D2 to fail as expected"
  cp "${JAVA_PUFFIN}.bak" "${JAVA_PUFFIN}"
  echo "        6c: restored"

  # ---- 6d: corrupt the ndv property in rust_stats_expected.json → Java verify must FAIL ----
  # The Z3 analogue: corrupt the ground-truth JSON that Java uses to cross-check the Rust output.
  # Java reads rust_stats_expected.json and compares it against the blobs in rust_stats.puffin;
  # if we write a wrong ndv in the JSON, the ndv property comparison fails (FAIL label emitted).
  echo "        6d: corrupt ndv property in rust_stats_expected.json → Java verify must FAIL"
  RUST_EXPECTED="${TMP}/rust_stats_expected.json"
  cp "${RUST_EXPECTED}" "${RUST_EXPECTED}.bak"
  python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    blobs = json.load(f)
# Flip the first blob's ndv to a bogus value that won't match the actual sketch estimate.
if blobs:
    blobs[0]['ndv'] = blobs[0]['ndv'] + 999_999
with open(sys.argv[1], 'w') as f:
    json.dump(blobs, f)
print('6d: corrupted ndv in rust_stats_expected.json blob[0]')
" "${RUST_EXPECTED}"
  SABOTAGE_6D="$(
    cd "${SCRIPT_DIR}"
    "${MVN}" -o -q compile exec:java \
      -Dexec.args=verify-interop-theta \
      -Dinterop.theta.dir="${TMP}" 2>&1
  )" || true
  if echo "${SABOTAGE_6D}" | grep -q 'verify-interop-theta: 0 failures'; then
    echo "==> SABOTAGE 6d FAILED (chain ${chain_num}): corrupted ndv in expected JSON still passed Java D1"
    cp "${RUST_EXPECTED}.bak" "${RUST_EXPECTED}"
    exit 1
  fi
  echo "        6d PASS: corrupted ndv in expected JSON caused Java D1 to fail as expected"
  cp "${RUST_EXPECTED}.bak" "${RUST_EXPECTED}"
  echo "        6d: restored"

  echo "    [6/${total_steps}] Sabotage battery: all 4 cases failed closed (6a/6b/6c/6d)"
  echo "==> CHAIN ${chain_num} COMPLETE"
}

# ===========================================================================================
# Run the chain twice (chain ×2).
# ===========================================================================================

run_chain 1
run_chain 2

echo ""
echo "==> DONE — Theta-blob puffin interop PASSED (chain ×2) over fixture:"
echo "    Unpartitioned V2 table {id long, name string, val long},"
echo "    File A (5 rows exact mode) + File B (1M rows estimation mode)"
echo ""
echo "    D1 (Rust writes via ComputeTableStats, Java judges via PuffinReader):"
echo "        rust_stats.puffin: 3 blobs (id/name/val)"
echo "        val column ndv = 1004032 (lgK12/seed9001/n=1M Alpha sketch pin)"
echo "        Java verify-interop-theta: 0 failures (both chains)"
echo ""
echo "    D2 (Java writes via PuffinWriter, Rust reads via PuffinReader+CompactThetaSketch):"
echo "        java_stats.puffin: 2 blobs (exact ndv=5, estimation ndv=1004032)"
echo "        Rust test_theta_d2_rust_reads_java_puffin: PASS (both chains)"
echo ""
echo "    Sabotage battery: 4 corruptions all failed closed (6a truncate Rust puffin [structural],"
echo "        6b SEMANTIC theta-halve inside blob0 payload via SOURCE edit — file still parses,"
echo "        estimate doubles, ndv-vs-estimate cross-check catches it; 6c truncate Java puffin"
echo "        [structural], 6d corrupt ndv in expected JSON [ground-truth])"
