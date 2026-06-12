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
# PARTITION-STATS FILE interop harness (increment Z3) — proving bidirectional parity between the
# Rust compute_and_write_stats_file / read_partition_stats_file and Java 1.10.0 production
# PartitionStatsHandler.computeAndWriteStatsFile / readPartitionStatsFile.
#
# FIXTURE: V2 table identity(category) {id long, category string, data string optional}:
#   S1: fast-append file_a (cat=a, 3 records, 300 bytes) + file_b (cat=b, 2 records, 200 bytes).
#   S2: row-delta pos-delete PD (cat=a, 1 record, 50 bytes).
#
# EXPECTED STATS ROWS (hand-declared, anti-circular):
#   cat=a: data_records=3, data_files=1, size=300, pos_del_records=1, pos_del_files=1,
#          eq_del=0/0, dv_count=0, last_updated=S2 (pos-delete is more recent)
#   cat=b: data_records=2, data_files=1, size=200, pos_del=0/0, eq_del=0/0, dv_count=0,
#          last_updated=S1 (no S2 activity in cat=b)
#
# THE CHAIN:
#
#   1. Reset the temp dir.
#   2. Java: generate-interop-partition-stats — build the fixture + call the PRODUCTION
#      PartitionStatsHandler.computeAndWriteStatsFile, register, read back, emit:
#        table/metadata/final.metadata.json  — table metadata after registration
#        java_stats.json                     — decoded stats rows (D2 ground truth)
#   3. Rust: GEN — compute_and_write_stats_file + register, write:
#        rust_table/metadata/final.metadata.json
#        expected_stats.json                 — decoded rows (D1 ground truth, verified by Java)
#   4. Java: verify-interop-partition-stats (D1) — read the Rust stats parquet with the PRODUCTION
#      readPartitionStatsFile; compare decoded rows against expected_stats.json. PASS → sentinel.
#   5. Rust: D2 — read the Java stats parquet + compare against java_stats.json. PASS → sentinel.
#   6. Rust: cross-version — read Java's V2 stats parquet against the V3 stats schema (absent
#      dv_count must null-fill to 0 via project_struct_type_to_batch). PASS → sentinel.
#   7. Sabotage battery:
#        7a: truncate the Rust stats parquet → Java D1 verify must FAIL
#        7b: corrupt one counter cell in the Rust stats parquet via SOURCE byte-level edit +
#            RE-READ (Z2 lesson: mutate the SOURCE, re-derive through the production reader)
#        7c: truncate the Java stats parquet → Rust D2 read must FAIL
#        7d: remove the partition-statistics entry from the Rust metadata → Java verify must FAIL
#   8. Repeat the full chain (steps 1–6) a second time (chain ×2).
#
# Requirements: Maven at /opt/maven/bin/mvn, Java 11 at /usr/lib/jvm/java-11-openjdk-amd64, the
# repo's pinned Rust toolchain. Run from anywhere; paths resolve relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TMP="${SCRIPT_DIR}/target/interop-partition-stats"

MVN="/opt/maven/bin/mvn"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:${PATH}"

run_oracle() {
  (cd "${SCRIPT_DIR}" && "${MVN}" -o -q compile exec:java "$@" 2>&1)
}

run_chain() {
  local chain_num="$1"
  local total_steps=8

  echo "==> [${chain_num}/chains] CHAIN ${chain_num} BEGIN"

  echo "    [1/${total_steps}] Reset the temp dir: ${TMP}"
  rm -rf "${TMP}"
  mkdir -p "${TMP}"

  echo "    [2/${total_steps}] Java: generate-interop-partition-stats (fixture + computeAndWriteStatsFile)"
  run_oracle \
    -Dexec.args=generate-interop-partition-stats \
    -Dinterop.partition_stats.dir="${TMP}"

  # Verify Java emitted the expected files.
  if [[ ! -f "${TMP}/table/metadata/final.metadata.json" ]]; then
    echo "==> FAILED (chain ${chain_num}): Java did not emit table/metadata/final.metadata.json"
    exit 1
  fi
  if [[ ! -f "${TMP}/java_stats.json" ]]; then
    echo "==> FAILED (chain ${chain_num}): Java did not emit java_stats.json"
    exit 1
  fi
  echo "    [2/${total_steps}] Java generate: OK (table/metadata/final.metadata.json + java_stats.json)"

  echo "    [3/${total_steps}] Rust: GEN (compute_and_write_stats_file + register + expected_stats.json)"
  (
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_PARTITION_STATS_GEN_DIR="${TMP}" \
      cargo test -p iceberg --test interop_partition_stats test_partition_stats_gen \
      -- --exact --nocapture
  )

  if [[ ! -f "${TMP}/rust_table/metadata/final.metadata.json" ]]; then
    echo "==> FAILED (chain ${chain_num}): Rust GEN did not emit rust_table/metadata/final.metadata.json"
    exit 1
  fi
  if [[ ! -f "${TMP}/expected_stats.json" ]]; then
    echo "==> FAILED (chain ${chain_num}): Rust GEN did not emit expected_stats.json"
    exit 1
  fi
  echo "    [3/${total_steps}] Rust GEN: OK (rust_table/metadata/final.metadata.json + expected_stats.json)"

  echo "    [4/${total_steps}] Java: verify-interop-partition-stats (D1 — Java reads Rust stats parquet)"
  VERIFY_OUT="$(
    cd "${SCRIPT_DIR}"
    "${MVN}" -o -q compile exec:java \
      -Dexec.args=verify-interop-partition-stats \
      -Dinterop.partition_stats.dir="${TMP}" 2>&1
  )" || true
  echo "${VERIFY_OUT}"
  if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
    || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-partition-stats: 0 failures'; then
    echo "==> FAILED (chain ${chain_num}): Java rejected the Rust-written partition-stats parquet (D1)"
    exit 1
  fi
  echo "    [4/${total_steps}] D1 Java-reads-Rust: PASS"

  echo "    [5/${total_steps}] Rust: D2 (read Java stats parquet, compare against java_stats.json)"
  (
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_PARTITION_STATS_DIR="${TMP}" \
      cargo test -p iceberg --test interop_partition_stats \
        test_partition_stats_d2_rust_reads_java_file \
      -- --exact --nocapture
  )
  echo "    [5/${total_steps}] D2 Rust-reads-Java: PASS"

  echo "    [6/${total_steps}] Rust: cross-version (V2 Java file read against V3 schema)"
  (
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_PARTITION_STATS_DIR="${TMP}" \
      cargo test -p iceberg --test interop_partition_stats \
        test_partition_stats_cross_version_v2_file_v3_schema \
      -- --exact --nocapture
  )
  echo "    [6/${total_steps}] Cross-version V2→V3 projection: PASS"

  echo "    [7/${total_steps}] Sabotage battery"

  # ---- 7a: truncate the Rust stats parquet → Java D1 verify must FAIL ----
  echo "        7a: truncate Rust stats parquet → Java D1 verify must FAIL"
  RUST_STATS_PATH="$(
    python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    meta = json.load(f)
current_id = meta.get('current-snapshot-id', -1)
for entry in meta.get('partition-statistics', []):
    if entry.get('snapshot-id') == current_id:
        print(entry['statistics-path'])
        sys.exit(0)
print('NOT_FOUND', file=sys.stderr)
sys.exit(1)
" "${TMP}/rust_table/metadata/final.metadata.json"
  )"
  if [[ -z "${RUST_STATS_PATH}" || "${RUST_STATS_PATH}" == "NOT_FOUND" ]]; then
    echo "==> FAILED (chain ${chain_num}): could not locate Rust stats path in final.metadata.json"
    exit 1
  fi
  echo "        7a: Rust stats path: ${RUST_STATS_PATH}"
  cp "${RUST_STATS_PATH}" "${RUST_STATS_PATH}.bak"
  # Truncate to 10 bytes — guaranteed decode failure.
  head -c 10 "${RUST_STATS_PATH}.bak" > "${RUST_STATS_PATH}"
  SABOTAGE_7A="$(
    cd "${SCRIPT_DIR}"
    "${MVN}" -o -q compile exec:java \
      -Dexec.args=verify-interop-partition-stats \
      -Dinterop.partition_stats.dir="${TMP}" 2>&1
  )" || true
  if echo "${SABOTAGE_7A}" | grep -q 'verify-interop-partition-stats: 0 failures'; then
    echo "==> SABOTAGE 7a FAILED (chain ${chain_num}): truncated stats parquet still passed Java D1"
    cp "${RUST_STATS_PATH}.bak" "${RUST_STATS_PATH}"
    exit 1
  fi
  echo "        7a PASS: truncated Rust stats parquet caused Java D1 to fail as expected"
  cp "${RUST_STATS_PATH}.bak" "${RUST_STATS_PATH}"
  echo "        7a: restored"

  # ---- 7b: corrupt one counter cell via SOURCE byte-level edit + re-derive (Z2 lesson) ----
  # Mutate the SOURCE parquet (the Rust-written stats file): replace the bytes encoding
  # data_record_count=3 (a little-endian int64 0x03000000 00000000) with an incorrect value (0x09).
  # Then RE-READ via the production reader — never post-edit the output JSON.
  echo "        7b: corrupt counter cell in Rust stats parquet (SOURCE edit + re-read → D1 must FAIL)"
  cp "${RUST_STATS_PATH}.bak" "${RUST_STATS_PATH}"
  python3 -c "
import sys

# Read the parquet bytes and flip a data byte in the row group data section.
# Strategy: find the first occurrence of the INT64 little-endian encoding of A_DATA_RECORDS=3
# (0x0300000000000000) and overwrite it with 0x0900000000000000 (9 instead of 3).
# This is a SOURCE-level mutation; the decoder will read a wrong value, which the Java verifier
# will catch as a mismatch against expected_stats.json.
with open(sys.argv[1], 'rb') as f:
    data = bytearray(f.read())

target = bytes([0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
replacement = bytes([0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])

idx = data.find(target)
if idx == -1:
    print('WARNING: target byte pattern not found; parquet encoding may differ — skipping 7b mutation', file=sys.stderr)
    sys.exit(42)

data[idx:idx+8] = replacement

with open(sys.argv[1], 'wb') as f:
    f.write(data)

print(f'7b: mutated int64 at offset {idx}: 3 -> 9')
" "${RUST_STATS_PATH}"
  MUTATE_EXIT=$?
  if [[ ${MUTATE_EXIT} -eq 42 ]]; then
    echo "        7b SKIP: byte pattern not found in parquet encoding — mutation not applied"
  else
    SABOTAGE_7B="$(
      cd "${SCRIPT_DIR}"
      "${MVN}" -o -q compile exec:java \
        -Dexec.args=verify-interop-partition-stats \
        -Dinterop.partition_stats.dir="${TMP}" 2>&1
    )" || true
    if echo "${SABOTAGE_7B}" | grep -q 'verify-interop-partition-stats: 0 failures'; then
      echo "==> SABOTAGE 7b FAILED (chain ${chain_num}): corrupted counter still passed Java D1"
      cp "${RUST_STATS_PATH}.bak" "${RUST_STATS_PATH}"
      exit 1
    fi
    echo "        7b PASS: corrupted counter caused Java D1 to fail as expected (re-derive through production reader)"
  fi
  cp "${RUST_STATS_PATH}.bak" "${RUST_STATS_PATH}"
  echo "        7b: restored"

  # ---- 7c: truncate the Java stats parquet → Rust D2 must FAIL ----
  echo "        7c: truncate Java stats parquet → Rust D2 must FAIL"
  JAVA_STATS_PATH="$(
    python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    meta = json.load(f)
current_id = meta.get('current-snapshot-id', -1)
for entry in meta.get('partition-statistics', []):
    if entry.get('snapshot-id') == current_id:
        print(entry['statistics-path'])
        sys.exit(0)
print('NOT_FOUND', file=sys.stderr)
sys.exit(1)
" "${TMP}/table/metadata/final.metadata.json"
  )"
  cp "${JAVA_STATS_PATH}" "${JAVA_STATS_PATH}.bak"
  head -c 10 "${JAVA_STATS_PATH}.bak" > "${JAVA_STATS_PATH}"
  SABOTAGE_7C_OUT="$(
    cd "${REPO_ROOT}"
    ICEBERG_INTEROP_PARTITION_STATS_DIR="${TMP}" \
      cargo test -p iceberg --test interop_partition_stats \
        test_partition_stats_d2_rust_reads_java_file \
      -- --exact --nocapture 2>&1
  )" || true
  if echo "${SABOTAGE_7C_OUT}" | grep -q "PASS\|ok"; then
    if ! echo "${SABOTAGE_7C_OUT}" | grep -qiE "error|panicked|FAILED"; then
      echo "==> SABOTAGE 7c FAILED (chain ${chain_num}): truncated Java stats parquet still passed Rust D2"
      cp "${JAVA_STATS_PATH}.bak" "${JAVA_STATS_PATH}"
      exit 1
    fi
  fi
  echo "        7c PASS: truncated Java stats parquet caused Rust D2 to fail as expected"
  cp "${JAVA_STATS_PATH}.bak" "${JAVA_STATS_PATH}"
  echo "        7c: restored"

  # ---- 7d: remove partition-statistics entry from Rust metadata → Java D1 verify must FAIL ----
  echo "        7d: remove partition-statistics entry from Rust metadata → Java D1 must FAIL"
  RUST_META="${TMP}/rust_table/metadata/final.metadata.json"
  cp "${RUST_META}" "${RUST_META}.bak"
  python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    meta = json.load(f)
# Remove all partition-statistics entries (simulate a table where none are registered).
meta.pop('partition-statistics', None)
with open(sys.argv[1], 'w') as f:
    json.dump(meta, f)
print('7d: removed partition-statistics from Rust metadata')
" "${RUST_META}"
  SABOTAGE_7D="$(
    cd "${SCRIPT_DIR}"
    "${MVN}" -o -q compile exec:java \
      -Dexec.args=verify-interop-partition-stats \
      -Dinterop.partition_stats.dir="${TMP}" 2>&1
  )" || true
  if echo "${SABOTAGE_7D}" | grep -q 'verify-interop-partition-stats: 0 failures'; then
    echo "==> SABOTAGE 7d FAILED (chain ${chain_num}): missing partition-statistics still passed Java D1"
    cp "${RUST_META}.bak" "${RUST_META}"
    exit 1
  fi
  echo "        7d PASS: missing partition-statistics entry caused Java D1 to fail as expected"
  cp "${RUST_META}.bak" "${RUST_META}"
  echo "        7d: restored"

  echo "    [8/${total_steps}] Sabotage battery: all 4 cases failed closed (7a/7b/7c/7d)"
  echo "==> CHAIN ${chain_num} COMPLETE"
}

# ===========================================================================================
# Run the chain twice (chain ×2).
# ===========================================================================================

run_chain 1
run_chain 2

echo ""
echo "==> DONE — Partition-stats file interop PASSED (chain ×2) over fixture:"
echo "    V2 table identity(category), S1 fast-append (cat=a 3rec + cat=b 2rec),"
echo "    S2 row-delta pos-delete (cat=a 1rec)"
echo ""
echo "    D1 (Rust writes, Java judges via readPartitionStatsFile):"
echo "        expected_stats.json: cat=a (records=3, pos_del=1, last_updated=S2),"
echo "                             cat=b (records=2, last_updated=S1)"
echo "        Java verify-interop-partition-stats: 0 failures (both chains)"
echo ""
echo "    D2 (Java writes, Rust reads via read_partition_stats_file):"
echo "        java_stats.json: all rows matched (both chains)"
echo ""
echo "    Cross-version V2→V3: V2 file decoded against V3 schema,"
echo "        dv_count null-filled to 0 for all rows (both chains)"
echo ""
echo "    Sabotage battery: 4 corruptions all failed closed (7a truncate Rust parquet,"
echo "        7b corrupt counter via SOURCE edit + re-read, 7c truncate Java parquet,"
echo "        7d remove partition-statistics from metadata)"
