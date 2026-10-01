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

# bash 3.2 (macOS default) does not run the ERR trap for a failing subshell:
# the script still exits on failure but does not report which step failed.
if [ "${BASH_VERSINFO[0]}" -lt 4 ]; then
  echo "Warning: bash ${BASH_VERSION} will not print which step failed. Use bash 4 or newer to see it." >&2
fi

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
  $0 <check>

Commands:
  check
      Run cargo-deny license validation once at the root workspace.
      Default command: none; this argument is required.

Options:
  -h, --help
      Show this help message.

Examples:
  $0 check
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
    echo "Install it with: cargo install --locked cargo-deny" >&2
    return 1
  fi
}

validate_args() {
  if [ "$#" -ne 1 ]; then
    usage
    return 1
  fi

  case "$1" in
    check) ;;
    *)
      usage
      return 1
      ;;
  esac
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

check_deps() {
  run_step "Check dependency licenses in ${REPO_ROOT}" check_deps_for_dir "${REPO_ROOT}"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMMAND="${1:-}"

show_help_if_requested "$@"
run_step "Validate dependency command arguments" validate_args "$@"

case "${COMMAND}" in
  check)
    check_deps
    ;;
esac
