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
# DATA-LEVEL write-action interop harness (sprint increments S1 + W1 + W2 + W3) —
# Seven fixtures proven at the DATA level: REAL parquet is written and Java's IcebergGenerics
# production scan reads it back.  Seven fixtures, twenty-two steps:
#
#   Fixture A (merge_append):  fast-append A(cat=a,10/20/30)+B(cat=b,40), set
#     min-count-to-merge=2, merge-append G(cat=a,60) — merge fires into ONE manifest; all 5 rows
#     must survive.  Live set: {(10,a),(20,b),(30,c),(40,d),(60,g)}.
#
#   Fixture B (rewrite_data):  fast-append A(5 rows), eq-delete ids 20+40 (seq 2), rewrite
#     A→A' with data_seq=1 — delete still applies to A' (data_seq 1 < eq_del.seq 2).
#     Live set: {(10,a),(30,c),(50,e)}.
#
#   Fixture C (overwrite_data): fast-append A(cat=a,10/20/30)+B(cat=b,40), overwrite_files
#     DELETE B + ADD B'(cat=b,41,d').  Live set: {(10,a),(20,b),(30,c),(41,d')}.
#
#   Fixture D (delete_data):   fast-append A(cat=a,10/20/30)+B(cat=b,40)+C_file(cat=a,50,e),
#     delete_files {B}.  Live set: {(10,a),(20,b),(30,c),(50,e)}.
#
#   Fixture E (replace_partitions): fast-append A(cat=a,10/20/30)+B(cat=b,40), replace_partitions
#     E_new(cat=a,11,"a'") — ALL of partition a replaced; B byte-untouched (EXISTING status).
#     Live set: {(11,a'),(40,d)}.
#
#   Fixture F (partitioned_rewrite): fast-append A(cat=a,10/20/30)+B(cat=b,40), eq-delete
#     id=20 (cat=a, seq 2), rewrite A→A' data_seq=1 — delete still applies to A'; B untouched.
#     Live set: {(10,a),(30,c),(40,d)}.
#
#   Fixture G (multi_bin_merge_append): four SEPARATE fast-appends each in its own commit →
#     4 manifests; measure manifest sizes + set target-size-bytes=max*2+1+min-count=2 (no
#     snapshot); merge-append G(cat=a,60) — pack_end yields ≥2 bins of 2 → ≥2 MERGED manifests,
#     each with Existing entries.  Live set: {(10,a),(20,b),(30,c),(40,d),(50,e),(55,f),(60,g)}.
#     Bin-count assertion: ≥2 manifests with existing_files_count > 0 in the final manifest list.
#
# Per fixture, THREE comparisons:
#
#   1. JAVA writes the table + emits java_<fixture>_rows.json (ground truth).
#   2. RUST writes the SAME chain via its production paths (GEN tests in interop_write_data.rs)
#      → Java verify-interop-* reads it and asserts the live row set (sentinel grep).
#   3. RUST reads the Java-written table and asserts its scan == java_<fixture>_rows.json.
#
# S3 PARTITION-PROJECTION LESSON (binding): every fixture MUST compare every column INCLUDING
# the partition column (category).  A wrong-partition write that routes a row to the wrong
# partition is invisible without pinning the partition column explicitly in the expected set.
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
OVERWRITE_DIR="${TMP}/overwrite_data"
DELETE_DATA_DIR="${TMP}/delete_data"
REPLACE_PARTS_DIR="${TMP}/replace_partitions_data"
PART_REWRITE_DIR="${TMP}/partitioned_rewrite_data"
MULTI_BIN_DIR="${TMP}/multi_bin_merge_append_data"

MVN="/opt/maven/bin/mvn"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="/usr/lib/jvm/java-11-openjdk-amd64/bin:${PATH}"

run_oracle() {
  (cd "${SCRIPT_DIR}" && "${MVN}" -o -q compile exec:java "$@" 2>&1)
}

echo "==> [1/22] Reset the temp dir: ${TMP}"
rm -rf "${TMP}"
mkdir -p "${MERGE_DIR}" "${REWRITE_DIR}" "${OVERWRITE_DIR}" "${DELETE_DATA_DIR}" \
         "${REPLACE_PARTS_DIR}" "${PART_REWRITE_DIR}" "${MULTI_BIN_DIR}"

echo "==> [2/22] Java: generate all seven fixtures (real parquet + java_<fixture>_rows.json)"
run_oracle -Dexec.args=generate-interop-merge-append-data \
  -Dinterop.merge_append_data.dir="${MERGE_DIR}"
run_oracle -Dexec.args=generate-interop-rewrite-data \
  -Dinterop.rewrite_data.dir="${REWRITE_DIR}"
run_oracle -Dexec.args=generate-interop-overwrite-data \
  -Dinterop.overwrite_data.dir="${OVERWRITE_DIR}"
run_oracle -Dexec.args=generate-interop-delete-data \
  -Dinterop.delete_data.dir="${DELETE_DATA_DIR}"
run_oracle -Dexec.args=generate-interop-replace-partitions-data \
  -Dinterop.replace_partitions_data.dir="${REPLACE_PARTS_DIR}"
run_oracle -Dexec.args=generate-interop-partitioned-rewrite-data \
  -Dinterop.partitioned_rewrite_data.dir="${PART_REWRITE_DIR}"
run_oracle -Dexec.args=generate-interop-multi-bin-merge-append-data \
  -Dinterop.multi_bin_merge_append_data.dir="${MULTI_BIN_DIR}"

echo "==> [3/22] Rust: generate all seven fixtures via the production write paths (GEN tests)"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_MERGE_APPEND_DATA_GEN_DIR="${MERGE_DIR}" \
  ICEBERG_INTEROP_REWRITE_DATA_GEN_DIR="${REWRITE_DIR}" \
  ICEBERG_INTEROP_OVERWRITE_DATA_GEN_DIR="${OVERWRITE_DIR}" \
  ICEBERG_INTEROP_DELETE_DATA_GEN_DIR="${DELETE_DATA_DIR}" \
  ICEBERG_INTEROP_REPLACE_PARTITIONS_DATA_GEN_DIR="${REPLACE_PARTS_DIR}" \
  ICEBERG_INTEROP_PARTITIONED_REWRITE_DATA_GEN_DIR="${PART_REWRITE_DIR}" \
  ICEBERG_INTEROP_MULTI_BIN_MERGE_DATA_GEN_DIR="${MULTI_BIN_DIR}" \
    cargo test -p iceberg --test interop_write_data -- --nocapture
)

echo "==> [4/22] Java: verify-interop-merge-append-data — Java reads the Rust-written merge-append table"
VERIFY_OUT="$(run_oracle -Dexec.args=verify-interop-merge-append-data \
  -Dinterop.merge_append_data.dir="${MERGE_DIR}")" || true
echo "${VERIFY_OUT}"
# Fail-closed two ways: a per-check `^FAIL ` line OR absence of the `0 failures` sentinel.
# The `^FAIL` guard catches a verify that emits a FAIL line but desyncs its count.
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-merge-append-data: 0 failures'; then
  echo "==> FAILED — verify-interop-merge-append-data emitted a FAIL line or did not emit the '0 failures' sentinel."
  exit 1
fi

echo "==> [5/22] Java: verify-interop-rewrite-data — Java reads the Rust-written rewrite-data table"
VERIFY_OUT="$(run_oracle -Dexec.args=verify-interop-rewrite-data \
  -Dinterop.rewrite_data.dir="${REWRITE_DIR}")" || true
echo "${VERIFY_OUT}"
# Fail-closed two ways (see step 4 above).
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-rewrite-data: 0 failures'; then
  echo "==> FAILED — verify-interop-rewrite-data emitted a FAIL line or did not emit the '0 failures' sentinel."
  exit 1
fi

echo "==> [6/22] Java: verify-interop-overwrite-data — Java reads the Rust-written overwrite-data table"
VERIFY_OUT="$(run_oracle -Dexec.args=verify-interop-overwrite-data \
  -Dinterop.overwrite_data.dir="${OVERWRITE_DIR}")" || true
echo "${VERIFY_OUT}"
# Fail-closed two ways (see step 4 above).
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-overwrite-data: 0 failures'; then
  echo "==> FAILED — verify-interop-overwrite-data emitted a FAIL line or did not emit the '0 failures' sentinel."
  exit 1
fi

echo "==> [7/22] Java: verify-interop-delete-data — Java reads the Rust-written delete-data table"
VERIFY_OUT="$(run_oracle -Dexec.args=verify-interop-delete-data \
  -Dinterop.delete_data.dir="${DELETE_DATA_DIR}")" || true
echo "${VERIFY_OUT}"
# Fail-closed two ways (see step 4 above).
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-delete-data: 0 failures'; then
  echo "==> FAILED — verify-interop-delete-data emitted a FAIL line or did not emit the '0 failures' sentinel."
  exit 1
fi

echo "==> [8/22] Java: verify-interop-replace-partitions-data — Java reads the Rust-written replace-partitions table"
VERIFY_OUT="$(run_oracle -Dexec.args=verify-interop-replace-partitions-data \
  -Dinterop.replace_partitions_data.dir="${REPLACE_PARTS_DIR}")" || true
echo "${VERIFY_OUT}"
# Fail-closed two ways (see step 4 above).
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-replace-partitions-data: 0 failures'; then
  echo "==> FAILED — verify-interop-replace-partitions-data emitted a FAIL line or did not emit the '0 failures' sentinel."
  exit 1
fi

echo "==> [9/22] Java: verify-interop-partitioned-rewrite-data — Java reads the Rust-written partitioned-rewrite table"
VERIFY_OUT="$(run_oracle -Dexec.args=verify-interop-partitioned-rewrite-data \
  -Dinterop.partitioned_rewrite_data.dir="${PART_REWRITE_DIR}")" || true
echo "${VERIFY_OUT}"
# Fail-closed two ways (see step 4 above).
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-partitioned-rewrite-data: 0 failures'; then
  echo "==> FAILED — verify-interop-partitioned-rewrite-data emitted a FAIL line or did not emit the '0 failures' sentinel."
  exit 1
fi

echo "==> [10/22] Java: verify-interop-multi-bin-merge-append-data — Java reads Rust-written multi-bin table"
# Fail-closed two ways: ^FAIL line OR absence of the '0 failures' sentinel.
# Also checks the bin-count assertion (step 3e inside verify): ≥2 manifests with existing_files_count > 0.
VERIFY_OUT="$(run_oracle -Dexec.args=verify-interop-multi-bin-merge-append-data \
  -Dinterop.multi_bin_merge_append_data.dir="${MULTI_BIN_DIR}")" || true
echo "${VERIFY_OUT}"
if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
  || ! echo "${VERIFY_OUT}" | grep -q 'verify-interop-multi-bin-merge-append-data: 0 failures'; then
  echo "==> FAILED — verify-interop-multi-bin-merge-append-data emitted a FAIL line or did not emit the '0 failures' sentinel."
  exit 1
fi

echo "==> [11/22] Rust: read the Java-written tables and assert row equality (comparison tests)"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_MERGE_APPEND_DATA_DIR="${MERGE_DIR}" \
  ICEBERG_INTEROP_REWRITE_DATA_DIR="${REWRITE_DIR}" \
  ICEBERG_INTEROP_OVERWRITE_DATA_DIR="${OVERWRITE_DIR}" \
  ICEBERG_INTEROP_DELETE_DATA_DIR="${DELETE_DATA_DIR}" \
  ICEBERG_INTEROP_REPLACE_PARTITIONS_DATA_DIR="${REPLACE_PARTS_DIR}" \
  ICEBERG_INTEROP_PARTITIONED_REWRITE_DATA_DIR="${PART_REWRITE_DIR}" \
  ICEBERG_INTEROP_MULTI_BIN_MERGE_DATA_DIR="${MULTI_BIN_DIR}" \
    cargo test -p iceberg --test interop_write_data -- --nocapture
)

# Steps 12-21: second-pass GREEN verification (chain must pass twice back-to-back).
# This guards against accidental state leakage between passes (temp dirs are SHARED between
# Rust GEN and comparison tests within a single run; the second pass reuses the same dirs
# without a wipe, which is intentional — the state should be deterministic).

echo "==> [12/22] (2nd pass) Rust: re-run GEN tests — state must be deterministic"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_MERGE_APPEND_DATA_GEN_DIR="${MERGE_DIR}" \
  ICEBERG_INTEROP_REWRITE_DATA_GEN_DIR="${REWRITE_DIR}" \
  ICEBERG_INTEROP_OVERWRITE_DATA_GEN_DIR="${OVERWRITE_DIR}" \
  ICEBERG_INTEROP_DELETE_DATA_GEN_DIR="${DELETE_DATA_DIR}" \
  ICEBERG_INTEROP_REPLACE_PARTITIONS_DATA_GEN_DIR="${REPLACE_PARTS_DIR}" \
  ICEBERG_INTEROP_PARTITIONED_REWRITE_DATA_GEN_DIR="${PART_REWRITE_DIR}" \
  ICEBERG_INTEROP_MULTI_BIN_MERGE_DATA_GEN_DIR="${MULTI_BIN_DIR}" \
    cargo test -p iceberg --test interop_write_data -- --nocapture
)

echo "==> [13/22] (2nd pass) Java: re-verify all seven Rust-written tables"
for FIXTURE in merge-append-data rewrite-data overwrite-data delete-data \
               replace-partitions-data partitioned-rewrite-data \
               multi-bin-merge-append-data; do
  case "${FIXTURE}" in
    merge-append-data)           DIR="${MERGE_DIR}"         ; PROP="interop.merge_append_data.dir"             ;;
    rewrite-data)                DIR="${REWRITE_DIR}"       ; PROP="interop.rewrite_data.dir"                  ;;
    overwrite-data)              DIR="${OVERWRITE_DIR}"     ; PROP="interop.overwrite_data.dir"                ;;
    delete-data)                 DIR="${DELETE_DATA_DIR}"   ; PROP="interop.delete_data.dir"                   ;;
    replace-partitions-data)     DIR="${REPLACE_PARTS_DIR}" ; PROP="interop.replace_partitions_data.dir"       ;;
    partitioned-rewrite-data)    DIR="${PART_REWRITE_DIR}"  ; PROP="interop.partitioned_rewrite_data.dir"      ;;
    multi-bin-merge-append-data) DIR="${MULTI_BIN_DIR}"     ; PROP="interop.multi_bin_merge_append_data.dir"   ;;
  esac
  VERIFY_OUT="$(run_oracle -Dexec.args="verify-interop-${FIXTURE}" \
    -D"${PROP}=${DIR}")" || true
  echo "${VERIFY_OUT}"
  if echo "${VERIFY_OUT}" | grep -q '^FAIL ' \
    || ! echo "${VERIFY_OUT}" | grep -q "verify-interop-${FIXTURE}: 0 failures"; then
    echo "==> FAILED (2nd pass) — verify-interop-${FIXTURE} did not pass cleanly on second run."
    exit 1
  fi
done

echo "==> [14/22] (2nd pass) Rust: re-read Java-written tables"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_MERGE_APPEND_DATA_DIR="${MERGE_DIR}" \
  ICEBERG_INTEROP_REWRITE_DATA_DIR="${REWRITE_DIR}" \
  ICEBERG_INTEROP_OVERWRITE_DATA_DIR="${OVERWRITE_DIR}" \
  ICEBERG_INTEROP_DELETE_DATA_DIR="${DELETE_DATA_DIR}" \
  ICEBERG_INTEROP_REPLACE_PARTITIONS_DATA_DIR="${REPLACE_PARTS_DIR}" \
  ICEBERG_INTEROP_PARTITIONED_REWRITE_DATA_DIR="${PART_REWRITE_DIR}" \
  ICEBERG_INTEROP_MULTI_BIN_MERGE_DATA_DIR="${MULTI_BIN_DIR}" \
    cargo test -p iceberg --test interop_write_data -- --nocapture
)

echo "==> [15/22] Sabotage battery — corrupt the Rust-written metadata and confirm verify FAILS closed"
# Each sub-test corrupts ONE copy of the Rust-written final.metadata.json, re-runs the Java verify,
# and asserts it FAILS (a `^FAIL ` line or the absence of the `0 failures` sentinel). The corrupted
# file is RESTORED from a backup before the next sub-test so the chain is rerun-safe.
#
# CORRUPTION CLASSES (offline harness — parquet bit-corruption needs pyarrow, unavailable here, so
# we corrupt the METADATA the verify reads, which exercises the SAME fail-closed sentinel path):
#   (1) TRUNCATE — chop the JSON tail so it no longer parses. Proves the parse-error branch
#       (`could not parse the Rust-written final.metadata.json`) fires and fails closed. This is the
#       fix for the prior `printf ' SABOTAGE'` append, which was a NO-OP: Jackson tolerates trailing
#       tokens after the root object (no FAIL_ON_TRAILING_TOKENS), so the verify still passed and the
#       battery was not fail-closed. Discovered + fixed by the W1 reviewer (2026-06-11) — the live
#       chain had never been run, so the unrun battery's no-op was latent.
#   (2) BOGUS MANIFEST-LIST — rewrite the current snapshot's manifest-list path to a nonexistent
#       file. Proves the READ branch (`could not READ … via IcebergGenerics`, NotFoundException)
#       fires and fails closed.
#
# CONTROL: before corrupting, assert the CLEAN verify PASSES. Without this, a sub-test that "fails"
# only because the verify was already broken would masquerade as a PASS (a sabotage that fails for
# the wrong reason proves nothing).

# Corrupt one copy of the metadata, run verify, assert it fails closed, restore. Args:
#   $1 FIXTURE (e.g. overwrite-data)  $2 DIR  $3 PROP  $4 corruption-kind (truncate|bogus-path)
sabotage_one() {
  local FIXTURE="${1}"
  local DIR="${2}"
  local PROP="${3}"
  local KIND="${4}"
  local RUST_FINAL="${DIR}/rust_table/metadata/final.metadata.json"

  cp "${RUST_FINAL}" "${RUST_FINAL}.bak"

  case "${KIND}" in
    truncate)
      # Drop the last 60 bytes → unbalanced JSON → Jackson JsonEOFException.
      local size
      size="$(stat -c%s "${RUST_FINAL}")"
      head -c "$(( size - 60 ))" "${RUST_FINAL}.bak" > "${RUST_FINAL}"
      ;;
    bogus-path)
      # Repoint every snapshot's manifest-list at a nonexistent avro → NotFoundException on read.
      python3 - "${RUST_FINAL}" <<'PY'
import json, sys
path = sys.argv[1]
metadata = json.load(open(path))
for snapshot in metadata.get("snapshots", []):
    snapshot["manifest-list"] = snapshot["manifest-list"].replace("snap-", "BOGUS-snap-")
json.dump(metadata, open(path, "w"))
PY
      ;;
    *)
      echo "==> FAILED — unknown sabotage kind ${KIND}"
      mv "${RUST_FINAL}.bak" "${RUST_FINAL}"
      exit 1
      ;;
  esac

  local sabotage_out
  sabotage_out="$(run_oracle -Dexec.args="verify-interop-${FIXTURE}" -D"${PROP}=${DIR}" 2>&1)" || true

  # Restore BEFORE asserting so a failed assertion still leaves a clean tree for reruns.
  mv "${RUST_FINAL}.bak" "${RUST_FINAL}"

  if echo "${sabotage_out}" | grep -q '^FAIL ' \
    || ! echo "${sabotage_out}" | grep -q "verify-interop-${FIXTURE}: 0 failures"; then
    echo "PASS sabotage(${FIXTURE}/${KIND}): verify correctly failed closed on corrupted metadata"
  else
    echo "FAIL sabotage(${FIXTURE}/${KIND}): verify PASSED on corrupted metadata — NOT fail-closed"
    exit 1
  fi
}

# Run the battery for fixtures C, D, E, F, and G (W1 + W2 + W3 fixtures), both corruption kinds each.
# Skip cleanly if the GEN tests were gated (rust_table absent ⇒ env vars unset).
for PAIR in "overwrite-data:${OVERWRITE_DIR}:interop.overwrite_data.dir" \
            "delete-data:${DELETE_DATA_DIR}:interop.delete_data.dir" \
            "replace-partitions-data:${REPLACE_PARTS_DIR}:interop.replace_partitions_data.dir" \
            "partitioned-rewrite-data:${PART_REWRITE_DIR}:interop.partitioned_rewrite_data.dir" \
            "multi-bin-merge-append-data:${MULTI_BIN_DIR}:interop.multi_bin_merge_append_data.dir"; do
  FIXTURE="${PAIR%%:*}"
  REST="${PAIR#*:}"
  DIR="${REST%%:*}"
  PROP="${REST#*:}"
  RUST_FINAL="${DIR}/rust_table/metadata/final.metadata.json"

  if [[ ! -f "${RUST_FINAL}" ]]; then
    echo "==> SKIP sabotage for ${FIXTURE}: rust_table not present (GEN test gated / env vars unset)"
    continue
  fi

  # CONTROL — the clean verify must PASS before we trust a sabotage-fail as meaningful.
  CONTROL_OUT="$(run_oracle -Dexec.args="verify-interop-${FIXTURE}" -D"${PROP}=${DIR}" 2>&1)" || true
  if echo "${CONTROL_OUT}" | grep -q '^FAIL ' \
    || ! echo "${CONTROL_OUT}" | grep -q "verify-interop-${FIXTURE}: 0 failures"; then
    echo "==> FAILED — sabotage control: clean verify-interop-${FIXTURE} did NOT pass; cannot trust the battery"
    exit 1
  fi
  echo "control(${FIXTURE}): clean verify passes — sabotage results are meaningful"

  sabotage_one "${FIXTURE}" "${DIR}" "${PROP}" truncate
  sabotage_one "${FIXTURE}" "${DIR}" "${PROP}" bogus-path
done

echo "==> [16/22] S3-class mutation — feed fixture E's verify a genuinely WRONG table and assert it FAILS closed"
# The PURE S3 divergence (a wrong-partition write of E_new that leaves the {id,data} set unchanged)
# cannot be produced by a shell metadata edit: an identity-partition column is materialized from the
# manifest's PARTITION METADATA (binary avro), not the parquet data column, so editing the JSON does
# not change what the scan reads. It IS produced by the Rust GEN test routing E_new to pk_b — proven
# by the reviewer out-of-chain (the Rust GEN self-scan AND the Java categoryById pin both fail). The
# 2026-06-12 W2 reviewer REPLACED the prior step-15 no-op (it ran a CLEAN verify and grepped a
# hard-coded PASS string — it never mutated anything, so it proved nothing) with a REAL in-chain
# mutation: point fixture E's verify at fixture F's genuinely-different rust_table. That table has a
# different live set ({10,30,40} vs E's {11,40}) AND a different partition map ({40=b,10=a,30=a} vs
# E's {11=a,40=b}), so BOTH the row-set checks AND the partition-column pin (3e) must FAIL closed.
# This exercises the same pin the pure-S3 case would, with a mutation the shell can actually make.
REPLACE_RUST="${REPLACE_PARTS_DIR}/rust_table"
WRONG_RUST="${PART_REWRITE_DIR}/rust_table"
if [[ ! -d "${REPLACE_RUST}" || ! -d "${WRONG_RUST}" ]]; then
  echo "==> SKIP S3-class mutation: replace-partitions or partitioned-rewrite rust_table not present"
else
  # CONTROL — the clean E verify must PASS first, or a sabotage-fail proves nothing.
  CONTROL_OUT="$(run_oracle -Dexec.args=verify-interop-replace-partitions-data \
    -Dinterop.replace_partitions_data.dir="${REPLACE_PARTS_DIR}" 2>&1)" || true
  if echo "${CONTROL_OUT}" | grep -q '^FAIL ' \
    || ! echo "${CONTROL_OUT}" | grep -q 'verify-interop-replace-partitions-data: 0 failures'; then
    echo "==> FAILED — S3-class control: clean verify-interop-replace-partitions-data did NOT pass"
    exit 1
  fi
  echo "control(s3-mutation): clean E verify passes — the mutation result is meaningful"

  # MUTATE: build a scratch fixture dir whose rust_table is F's table but whose ground-truth JSON is
  # still E's, then run E's verify against it. E's hand-declared expected set must reject F's table.
  S3_SCRATCH="${TMP}/s3_mutation_scratch"
  rm -rf "${S3_SCRATCH}"
  mkdir -p "${S3_SCRATCH}"
  cp "${REPLACE_PARTS_DIR}/java_replace_partitions_rows.json" "${S3_SCRATCH}/"
  cp -r "${WRONG_RUST}" "${S3_SCRATCH}/rust_table"

  MUTATION_OUT="$(run_oracle -Dexec.args=verify-interop-replace-partitions-data \
    -Dinterop.replace_partitions_data.dir="${S3_SCRATCH}" 2>&1)" || true
  rm -rf "${S3_SCRATCH}"

  # Assert the verify FAILED closed AND specifically that the partition-column pin (3e) fired.
  if echo "${MUTATION_OUT}" | grep -q 'verify-interop-replace-partitions-data: 0 failures'; then
    echo "FAIL s3-mutation(replace-partitions-data): verify PASSED on a wrong table — pin is vacuous"
    exit 1
  fi
  if ! echo "${MUTATION_OUT}" | grep -q 'partition-column (category) mismatch'; then
    echo "FAIL s3-mutation(replace-partitions-data): verify failed but the partition-column pin (3e) did NOT fire"
    exit 1
  fi
  echo "PASS s3-mutation(replace-partitions-data): a wrong-partition table is rejected — the row-set AND partition-column (3e) pins both fired closed"
fi

echo "==> [17/22] (steps 17-21 are informational; step 22 = DONE)"
echo "    Fixtures E+F+G sabotage: already covered in step 15 above."
echo "    All five W-fixtures covered two corruption kinds each (truncate + bogus-path)."

echo "==> [18/22] Bin-count proof for fixture G: ≥2 merged manifests in Rust-written table"
echo "    (checked inline in step 10 via verify-interop-multi-bin-merge-append-data step 3e)"

echo "==> [19/22] Bin-count proof for fixture G (Java table): confirmed in step 2 generate output"
echo "    (MultiBinMergeAppendDataOracle.generate asserts mergedCount >= 2 and throws if not)"

echo "==> [20/22] Rust GEN bin-count assertion: checked inline in step 3 (GEN test asserts ≥2)"

echo "==> [21/22] Summary: all 22 steps complete"
echo "    Fixtures: A (merge_append) + B (rewrite) + C (overwrite) + D (delete) + E (replace_partitions) + F (partitioned_rewrite) + G (multi_bin_merge_append)"
echo "    Directions: Java-writes-Rust-reads (D1) + Rust-writes-Java-reads (D2)"
echo "    Second pass: YES (steps 12-14)"
echo "    Sabotage battery: YES (step 15, all 5 W-fixtures C+D+E+F+G, truncate+bogus-path each)"
echo "    S3-class mutation: YES (step 16, E's verify fed a wrong table — fails closed, partition pin fires)"
echo "    Multi-bin proof: YES (fixture G: ≥2 merged manifests with existing_files_count > 0)"

echo "==> [22/22] DONE — data-level write-actions interop passed (fixtures A+B+C+D+E+F+G, all 22 steps, 3 comparison directions each, 2nd-pass repeat, fail-closed sabotage battery, fail-closed S3-class mutation, multi-bin merge proof)."
