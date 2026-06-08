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
# UpdateSchema + UpdatePartitionSpec + ManageSnapshots bidirectional interop harness runner.
#
# This is a TEST-ONLY ORACLE (a dev tool, like dev/spark/) — it is NOT part of the shipped Rust library.
# It proves byte-/field-id-level UpdateSchema, UpdatePartitionSpec, AND ManageSnapshots (ref operations)
# compatibility with the Java `iceberg-core` reference in BOTH directions and regenerates the committed
# JSON fixtures. One Java `generate`/`verify` pass covers all three capabilities (the Java oracle drives
# all scenario registries):
#
#   1. mvn ... -Dexec.args=generate
#        -> The Java oracle (re)writes base.metadata.json + java_evolved.metadata.json for EACH scenario
#           of ALL THREE capabilities (update_schema/ + update_partition_spec/ + manage_snapshots/).
#   2. ICEBERG_INTEROP_GEN=1 cargo test ... interop_update_schema / interop_update_partition_spec /
#      interop_manage_snapshots
#        -> Rust applies each scenario's op-sequence to the Java-written base.metadata.json and writes
#           rust_evolved.metadata.json (Direction 2 producer). The same run also asserts Direction 1
#           (Rust reproduces Java's evolution) against the committed java_evolved.metadata.json.
#   3. mvn ... -Dexec.args=verify
#        -> The Java oracle reads each rust_evolved.metadata.json (all three capabilities) and asserts Java
#           parses it and its current schema / default partition spec / refs map + current-snapshot-id
#           matches Java's own evolution (Direction 2 verifier). Exits non-zero on any FAIL.
#
# Step order note: the Rust producer (step 2) reads the base + java_evolved fixtures, and the Java
# generator (step 1) (re)writes them. This script runs generate (1) FIRST so a single invocation always
# regenerates everything consistently before the Rust producers re-run.
#
# Requirements:
#   - Maven at /opt/maven/bin/mvn (override with MVN=...).
#   - A Rust toolchain (the repo's pinned nightly via rust-toolchain.toml).
#   - First Maven run downloads iceberg-core/iceberg-api 1.10.0 from Maven Central.
#
# Run from anywhere; paths are resolved relative to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MVN="${MVN:-/opt/maven/bin/mvn}"

echo "==> [1/4] Java oracle: generate base + java_evolved fixtures (schema + partition + manage_snapshots)"
"${MVN}" -f "${SCRIPT_DIR}" -q compile exec:java -Dexec.args=generate

echo "==> [2/4] Rust: regenerate rust_evolved fixtures + assert Direction 1 (Rust reproduces Java)"
(
  cd "${REPO_ROOT}"
  ICEBERG_INTEROP_GEN=1 cargo test -p iceberg --test interop_update_schema
  ICEBERG_INTEROP_GEN=1 cargo test -p iceberg --test interop_update_partition_spec
  ICEBERG_INTEROP_GEN=1 cargo test -p iceberg --test interop_manage_snapshots
)

echo "==> [3/4] Java oracle: verify (Direction 2 — Java reads Rust output; all three capabilities)"
"${MVN}" -f "${SCRIPT_DIR}" -q exec:java -Dexec.args=verify

echo "==> [4/4] Done — both directions passed (UpdateSchema + UpdatePartitionSpec + ManageSnapshots)."
