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

set -Eeuo pipefail

# Keep this in sync with CARGO_DENY_VERSION in .github/workflows/dependencies.yml.
# The generated DEPENDENCIES.rust.tsv files are version-sensitive, so the local
# cargo-deny must match the version CI uses to avoid spurious diffs.
EXPECTED_CARGO_DENY_VERSION="0.19.9"

CURRENT_STEP=""

on_error() {
  local status=$?
  if [ -n "${CURRENT_STEP}" ]; then
    echo "FAILED: ${CURRENT_STEP}" >&2
  else
    echo "FAILED" >&2
  fi
  exit "${status}"
}

trap on_error ERR

usage() {
  cat <<USAGE
Usage:
  $0 <check|generate>

Commands:
  check
      Run cargo-deny license validation once at the root workspace.
      Default command: none; this argument is required.

  generate
      Regenerate each workspace package's DEPENDENCIES.rust.tsv file.
      Package directories are discovered from cargo metadata.
      Default command: none; this argument is required.

Options:
  -h, --help
      Show this help message.

Examples:
  $0 check
  $0 generate
USAGE
}

show_help_if_requested() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      -h | --help)
        usage
        exit 0
        ;;
    esac
    shift
  done
}

start_step() {
  CURRENT_STEP="$1"
  echo "==> ${CURRENT_STEP}"
}

finish_step() {
  echo "OK: ${CURRENT_STEP}"
  CURRENT_STEP=""
}

run_step() {
  local step="$1"
  shift
  start_step "${step}"
  "$@"
  finish_step
}

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "This script requires '${command_name}', but it is not installed." >&2
    return 1
  fi
}

require_cargo_deny() {
  require_command cargo
  if ! cargo deny --version >/dev/null 2>&1; then
    echo "This script requires 'cargo-deny' for dependency license checks." >&2
    echo "Install it with: cargo install --locked cargo-deny@${EXPECTED_CARGO_DENY_VERSION}" >&2
    return 1
  fi

  # The TSV output is sensitive to the cargo-deny version: different versions
  # resolve the dependency graph differently, producing diffs that CI rejects.
  # Assert the active binary matches the version CI pins so a shadowed or
  # mismatched install fails loudly instead of generating wrong files.
  local actual_version
  actual_version="$(cargo deny --version | awk '{print $2}')"
  if [ "${actual_version}" != "${EXPECTED_CARGO_DENY_VERSION}" ]; then
    echo "ERROR: cargo-deny version mismatch." >&2
    echo "  expected: ${EXPECTED_CARGO_DENY_VERSION} (pinned by CI)" >&2
    echo "  found:    ${actual_version} ($(command -v cargo-deny))" >&2
    echo "" >&2
    echo "Install the pinned version with:" >&2
    echo "  cargo install --locked cargo-deny@${EXPECTED_CARGO_DENY_VERSION}" >&2
    echo "" >&2
    echo "If multiple cargo-deny binaries are installed, 'which -a cargo-deny'" >&2
    echo "shows which one is shadowing the others on your PATH." >&2
    return 1
  fi
}

validate_args() {
  if [ "$#" -ne 1 ]; then
    usage
    return 1
  fi

  case "$1" in
    check | generate) ;;
    *)
      usage
      return 1
      ;;
  esac
}

discover_cargo_dirs() {
  require_command cargo
  require_command jq
  CARGO_DIRS="$(cargo metadata \
    --format-version=1 \
    --no-deps \
    --manifest-path "${REPO_ROOT}/Cargo.toml" |
    jq -r '
      .workspace_members as $workspace_members
      | .packages[]
      | select(.id as $id | $workspace_members | index($id))
      | .manifest_path
      | sub("/Cargo.toml$"; "")
    ' |
    sort)"
}

check_deps_for_dir() {
  local cargo_dir="$1"
  require_cargo_deny
  (
    trap - ERR
    cd "${cargo_dir}"
    cargo deny check license
  )
}

generate_deps_for_dir() {
  local cargo_dir="$1"
  require_cargo_deny
  (
    trap - ERR
    cd "${cargo_dir}"
    cargo deny list -f tsv -t 0.6 > DEPENDENCIES.rust.tsv
  )
}

check_deps() {
  run_step "Check dependency licenses in ${REPO_ROOT}" check_deps_for_dir "${REPO_ROOT}"
}

generate_deps() {
  run_step "Discover Cargo workspace package directories" discover_cargo_dirs
  while IFS= read -r cargo_dir; do
    [ -n "${cargo_dir}" ] || continue
    run_step "Generate dependency list in ${cargo_dir}" generate_deps_for_dir "${cargo_dir}"
  done <<< "${CARGO_DIRS}"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMMAND="${1:-}"
CARGO_DIRS=""

show_help_if_requested "$@"
run_step "Validate dependency command arguments" validate_args "$@"

case "${COMMAND}" in
  check)
    check_deps
    ;;
  generate)
    generate_deps
    ;;
esac
