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

CURRENT_STEP=""
VERIFY_SUCCESS="no"
VERIFY_OWNS_TMPDIR="0"
VERIFY_TMPDIR=""

on_error() {
  local status=$?
  if [ -n "${CURRENT_STEP}" ]; then
    echo "FAILED: ${CURRENT_STEP}" >&2
  else
    echo "FAILED" >&2
  fi
  exit "${status}"
}

cleanup() {
  if [ -z "${VERIFY_TMPDIR}" ]; then
    return
  fi

  if [ "${VERIFY_SUCCESS}" = "yes" ]; then
    if [ "${VERIFY_OWNS_TMPDIR}" = "1" ]; then
      rm -rf "${VERIFY_TMPDIR}"
    fi
  else
    echo "Failed to verify release candidate. See ${VERIFY_TMPDIR} for details." >&2
  fi
}

trap on_error ERR
trap cleanup EXIT

usage() {
  cat <<USAGE
Usage:
  $0 <version> <rc> [options]

Arguments:
  <version>
      Release version in <major>.<minor>.<patch> format.
      Example: 0.9.1

  <rc>
      Numeric release candidate round.
      Example: 2 verifies apache-iceberg-rust-0.9.1-rc2.

Options:
  --dist_dir <dir>
      Local artifact root used when --download is 0.
      Relative paths are resolved from the repository root.
      Default: dist

  --download <0|1>
      Whether to download artifacts from ASF dev dist. Use 0 to verify local artifacts.
      Default: 1

  --verify_signature <0|1>
      Whether to verify the .asc signature with the local GPG keyring.
      Default: 1

  --import_gpg_keys <0|1>
      Whether to download and import Apache Iceberg release keys before signature verification.
      Default: 0

  --check_headers <0|1>
      Whether to check Apache license headers against the extracted source archive.
      Default: 1

  --build <0|1>
      Whether to build and test the Rust source distribution.
      Default: 1

  --python <0|1>
      Whether to build and test pyiceberg-core.
      Default: 1

  --python_bin <path>
      Python interpreter for Python-linked Rust and pyiceberg-core checks.
      Sets PYO3_PYTHON and UV_PYTHON for build and test steps.
      Default: unset

  --tmp_dir <dir>
      Verification sandbox. If omitted, a temporary directory is created and deleted on success.
      Default: auto-created temporary directory

  -h, --help
      Show this help message.

Examples:
  $0 0.9.1 2
  $0 0.9.1 2 --download 0
  $0 0.9.1 2 --import_gpg_keys 1
  $0 0.9.1 2 --python_bin /opt/homebrew/bin/python3.11
  $0 0.9.1 2 --download 0 --build 0 --python 0 --check_headers 0 --verify_signature 0
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

skip_step() {
  local step="$1"
  start_step "${step}"
  echo "OK: ${CURRENT_STEP} (skipped)"
  CURRENT_STEP=""
}

enabled() {
  [ "$1" != "0" ]
}

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "This step requires '${command_name}', but it is not installed." >&2
    return 1
  fi
}

require_checksum_command() {
  if ! command -v shasum >/dev/null 2>&1 && ! command -v sha512sum >/dev/null 2>&1; then
    echo "This step requires either 'shasum' or 'sha512sum'." >&2
    return 1
  fi
}

require_option_value() {
  local option_name="$1"
  local option_value="${2:-}"
  if [ -z "${option_value}" ]; then
    echo "Missing value for ${option_name}." >&2
    usage >&2
    return 1
  fi
}

validate_bool_option() {
  local option_name="$1"
  local option_value="$2"
  case "${option_value}" in
    0 | 1) ;;
    *)
      echo "${option_name} must be 0 or 1, got '${option_value}'." >&2
      return 1
      ;;
  esac
}

parse_args() {
  local positional=()

  while [ "$#" -gt 0 ]; do
    case "$1" in
      -h | --help)
        usage
        exit 0
        ;;
      --dist_dir | --dist-dir)
        require_option_value "$1" "${2:-}"
        RELEASE_DIST_DIR="$2"
        shift 2
        ;;
      --download)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        DOWNLOAD="$2"
        shift 2
        ;;
      --verify_signature | --verify-signature)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        VERIFY_SIGNATURE="$2"
        shift 2
        ;;
      --import_gpg_keys | --import-gpg-keys)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        IMPORT_GPG_KEYS="$2"
        shift 2
        ;;
      --check_headers | --check-headers)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        CHECK_HEADERS="$2"
        shift 2
        ;;
      --build)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        BUILD="$2"
        shift 2
        ;;
      --python)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        PYTHON="$2"
        shift 2
        ;;
      --python_bin | --python-bin)
        require_option_value "$1" "${2:-}"
        PYTHON_BIN="$2"
        shift 2
        ;;
      --tmp_dir | --tmp-dir)
        require_option_value "$1" "${2:-}"
        VERIFY_TMPDIR="$2"
        shift 2
        ;;
      --*)
        echo "Unknown option: $1" >&2
        usage >&2
        return 1
        ;;
      *)
        positional+=("$1")
        shift
        ;;
    esac
  done

  if [ "${#positional[@]}" -ne 2 ]; then
    usage >&2
    return 1
  fi

  VERSION="${positional[0]}"
  RC="${positional[1]}"
}

validate_args() {
  if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Version '${VERSION}' must use format <major>.<minor>.<patch>." >&2
    return 1
  fi

  if [[ ! "${RC}" =~ ^[0-9]+$ ]]; then
    echo "RC '${RC}' must be a number, for example: 0, 1, or 2." >&2
    return 1
  fi

  if [ -n "${PYTHON_BIN}" ] && ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
    echo "Python interpreter '${PYTHON_BIN}' was not found." >&2
    return 1
  fi
}

derive_release_names() {
  RC_ID="apache-iceberg-rust-${VERSION}-rc${RC}"
  ARCHIVE_BASE_NAME="apache-iceberg-rust-${VERSION}"
  ARCHIVE_FILE_NAME="${ARCHIVE_BASE_NAME}.tar.gz"
  ICEBERG_DIST_BASE_URL="https://downloads.apache.org/iceberg"
  DOWNLOAD_RC_BASE_URL="https://dist.apache.org/repos/dist/dev/iceberg/${RC_ID}"

  if [[ "${RELEASE_DIST_DIR}" = /* ]]; then
    LOCAL_DIST_ROOT="${RELEASE_DIST_DIR}"
  else
    LOCAL_DIST_ROOT="${REPO_ROOT}/${RELEASE_DIST_DIR}"
  fi
  LOCAL_RC_DIR="${LOCAL_DIST_ROOT}/${RC_ID}"
}

setup_tmpdir() {
  if [ -z "${VERIFY_TMPDIR}" ]; then
    require_command mktemp
    VERIFY_TMPDIR="$(mktemp -d -t "iceberg-rust-${VERSION}-rc${RC}.XXXXXX")"
    VERIFY_OWNS_TMPDIR="1"
  else
    mkdir -p "${VERIFY_TMPDIR}"
  fi
}

enter_tmpdir() {
  cd "${VERIFY_TMPDIR}"
}

download_file() {
  require_command curl
  local file_name="$1"
  curl \
    --fail \
    --location \
    --remote-name \
    --show-error \
    --silent \
    "${DOWNLOAD_RC_BASE_URL}/${file_name}"
}

copy_local_file() {
  require_command cp
  local file_name="$1"
  cp "${LOCAL_RC_DIR}/${file_name}" "${file_name}"
}

fetch_file() {
  local file_name="$1"
  if enabled "${DOWNLOAD}"; then
    download_file "${file_name}"
  else
    copy_local_file "${file_name}"
  fi
}

import_gpg_keys() {
  require_command curl
  require_command gpg
  curl \
    --fail \
    --location \
    --output KEYS \
    --show-error \
    --silent \
    "${ICEBERG_DIST_BASE_URL}/KEYS"
  gpg --import KEYS
}

verify_source_signature() {
  require_command gpg
  gpg --verify "${ARCHIVE_FILE_NAME}.asc" "${ARCHIVE_FILE_NAME}"
}

verify_checksum() {
  require_checksum_command
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 512 -c "${ARCHIVE_FILE_NAME}.sha512"
  else
    sha512sum -c "${ARCHIVE_FILE_NAME}.sha512"
  fi
}

extract_source_archive() {
  require_command tar
  tar -xzf "${ARCHIVE_FILE_NAME}"
}

check_license_headers() {
  require_command docker
  docker run --rm -v "${VERIFY_TMPDIR}/${ARCHIVE_BASE_NAME}:/github/workspace" apache/skywalking-eyes header check
}

run_with_python_bin() {
  if [ -n "${PYTHON_BIN}" ]; then
    PYO3_PYTHON="${PYTHON_BIN}" UV_PYTHON="${PYTHON_BIN}" "$@"
  else
    "$@"
  fi
}

test_source_distribution() {
  require_command make
  (
    trap - ERR
    cd "${ARCHIVE_BASE_NAME}"
    run_with_python_bin make build
    run_with_python_bin make test
  )
}

test_python_distribution() {
  require_command make
  require_command uv
  (
    trap - ERR
    cd "${ARCHIVE_BASE_NAME}/bindings/python"
    run_with_python_bin make install
    run_with_python_bin make test
  )
}

mark_success() {
  VERIFY_SUCCESS="yes"
  echo "RC looks good!"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

VERSION=""
RC=""
RELEASE_DIST_DIR="dist"
DOWNLOAD="1"
VERIFY_SIGNATURE="1"
IMPORT_GPG_KEYS="0"
CHECK_HEADERS="1"
BUILD="1"
PYTHON="1"
PYTHON_BIN="${PYTHON_BIN:-}"

show_help_if_requested "$@"
run_step "Parse command arguments" parse_args "$@"
run_step "Validate release arguments" validate_args
derive_release_names

run_step "Create verification sandbox" setup_tmpdir
run_step "Enter verification sandbox ${VERIFY_TMPDIR}" enter_tmpdir
run_step "Fetch source archive ${ARCHIVE_FILE_NAME}" fetch_file "${ARCHIVE_FILE_NAME}"

if enabled "${VERIFY_SIGNATURE}"; then
  run_step "Fetch source archive signature" fetch_file "${ARCHIVE_FILE_NAME}.asc"
  if enabled "${IMPORT_GPG_KEYS}"; then
    run_step "Import Apache Iceberg GPG keys" import_gpg_keys
  else
    skip_step "Import Apache Iceberg GPG keys"
  fi
  run_step "Verify source archive signature" verify_source_signature
else
  skip_step "Fetch source archive signature"
  skip_step "Import Apache Iceberg GPG keys"
  skip_step "Verify source archive signature"
fi

run_step "Fetch source archive checksum" fetch_file "${ARCHIVE_FILE_NAME}.sha512"
run_step "Verify source archive checksum" verify_checksum
run_step "Extract source archive" extract_source_archive

if enabled "${CHECK_HEADERS}"; then
  run_step "Check license headers" check_license_headers
else
  skip_step "Check license headers"
fi

if enabled "${BUILD}"; then
  run_step "Build and test Rust source distribution" test_source_distribution
else
  skip_step "Build and test Rust source distribution"
fi

if enabled "${PYTHON}"; then
  run_step "Build and test pyiceberg-core" test_python_distribution
else
  skip_step "Build and test pyiceberg-core"
fi

run_step "Print verification summary" mark_success
