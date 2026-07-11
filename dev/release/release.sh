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
  $0 <version> <rc> [options]

Arguments:
  <version>
      Release version in <major>.<minor>.<patch> format.
      Example: 0.9.1

  <rc>
      Numeric release candidate round that passed the vote.
      Example: 2 promotes v0.9.1-rc.2 and apache-iceberg-rust-0.9.1-rc2.

Options:
  --create_release_tag <0|1>
      Whether to create the signed annotated final release git tag.
      Default: 1

  --move_svn <0|1>
      Whether to move the RC artifacts from ASF dev dist to ASF release dist.
      Default: 1

  --tag_ref <ref>
      Git commit-ish to tag as the final release.
      Default: the commit pointed to by the RC tag

  --dev_dist_url <url>
      SVN directory URL containing RC artifact directories.
      Default: https://dist.apache.org/repos/dist/dev/iceberg

  --release_dist_url <url>
      SVN directory URL where final release artifact directories are published.
      Default: https://dist.apache.org/repos/dist/release/iceberg

  -h, --help
      Show this help message.

Examples:
  $0 0.9.1 2
  $0 0.9.1 2 --create_release_tag 0
  $0 0.9.1 2 --move_svn 0
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

require_gpg_secret_key() {
  require_command gpg
  if ! gpg --list-secret-keys --with-colons 2>/dev/null | grep -q '^sec'; then
    echo "This step requires a GPG secret key for signing the release tag." >&2
    echo "Set up a release signing key first: https://rust.iceberg.apache.org/reference/setup_gpg.html" >&2
    echo "To skip tag creation locally, pass: --create_release_tag 0" >&2
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
      --create_release_tag | --create-release-tag)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        CREATE_RELEASE_TAG="$2"
        shift 2
        ;;
      --move_svn | --move-svn)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        MOVE_SVN="$2"
        shift 2
        ;;
      --tag_ref | --tag-ref)
        require_option_value "$1" "${2:-}"
        TAG_REF="$2"
        shift 2
        ;;
      --dev_dist_url | --dev-dist-url)
        require_option_value "$1" "${2:-}"
        DEV_DIST_URL="$2"
        shift 2
        ;;
      --release_dist_url | --release-dist-url)
        require_option_value "$1" "${2:-}"
        RELEASE_DIST_URL="$2"
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
}

derive_release_names() {
  RC_TAG="v${VERSION}-rc.${RC}"
  RELEASE_TAG="v${VERSION}"
  RC_ID="apache-iceberg-rust-${VERSION}-rc${RC}"
  RELEASE_ID="apache-iceberg-rust-${VERSION}"

  if [ -z "${TAG_REF}" ]; then
    TAG_REF="${RC_TAG}^{commit}"
  fi

  DEV_DIST_URL="${DEV_DIST_URL%/}"
  RELEASE_DIST_URL="${RELEASE_DIST_URL%/}"
  SVN_RC_URL="${DEV_DIST_URL}/${RC_ID}"
  SVN_RELEASE_URL="${RELEASE_DIST_URL}/${RELEASE_ID}"
}

check_tag_ref() {
  require_command git
  git -C "${REPO_ROOT}" rev-parse --verify "${TAG_REF}" >/dev/null
}

check_release_tag_available() {
  require_command git
  if git -C "${REPO_ROOT}" rev-parse --verify --quiet "refs/tags/${RELEASE_TAG}" >/dev/null; then
    echo "Tag '${RELEASE_TAG}' already exists." >&2
    return 1
  fi
}

create_release_tag() {
  require_command git
  require_gpg_secret_key
  git -C "${REPO_ROOT}" tag -s "${RELEASE_TAG}" "${TAG_REF}" -m "Apache Iceberg Rust ${VERSION}"
}

check_svn_rc_available() {
  require_command svn
  if ! svn info "${SVN_RC_URL}" >/dev/null; then
    echo "SVN RC source does not exist: ${SVN_RC_URL}" >&2
    return 1
  fi
}

check_svn_release_available() {
  require_command svn
  if svn info "${SVN_RELEASE_URL}" >/dev/null 2>&1; then
    echo "SVN release target already exists: ${SVN_RELEASE_URL}" >&2
    return 1
  fi
}

move_svn_release() {
  require_command svn
  svn mv "${SVN_RC_URL}" "${SVN_RELEASE_URL}" -m "Release Apache Iceberg Rust ${VERSION}"
}

print_summary() {
  echo "Promoted Apache Iceberg Rust RC to release:"
  echo "  RC tag: ${RC_TAG}"
  echo "  Release tag: ${RELEASE_TAG}"
  echo "  Release tag ref: ${TAG_REF}"
  echo "  SVN RC URL: ${SVN_RC_URL}"
  echo "  SVN release URL: ${SVN_RELEASE_URL}"
  echo
  echo "Next step:"
  echo "  git push origin \"${RELEASE_TAG}\""
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

VERSION=""
RC=""
CREATE_RELEASE_TAG="1"
MOVE_SVN="1"
TAG_REF=""
DEV_DIST_URL="https://dist.apache.org/repos/dist/dev/iceberg"
RELEASE_DIST_URL="https://dist.apache.org/repos/dist/release/iceberg"

show_help_if_requested "$@"
run_step "Parse command arguments" parse_args "$@"
run_step "Validate release arguments" validate_args
derive_release_names

if enabled "${CREATE_RELEASE_TAG}"; then
  run_step "Check release tag ref ${TAG_REF}" check_tag_ref
  run_step "Check release tag ${RELEASE_TAG} is available" check_release_tag_available
  run_step "Create signed release tag ${RELEASE_TAG}" create_release_tag
else
  skip_step "Check release tag ref ${TAG_REF}"
  skip_step "Check release tag ${RELEASE_TAG} is available"
  skip_step "Create signed release tag ${RELEASE_TAG}"
fi

if enabled "${MOVE_SVN}"; then
  run_step "Check SVN RC source ${SVN_RC_URL}" check_svn_rc_available
  run_step "Check SVN release target ${SVN_RELEASE_URL} is available" check_svn_release_available
  run_step "Move SVN RC artifacts to release dist" move_svn_release
else
  skip_step "Check SVN RC source ${SVN_RC_URL}"
  skip_step "Check SVN release target ${SVN_RELEASE_URL} is available"
  skip_step "Move SVN RC artifacts to release dist"
fi

run_step "Print release summary" print_summary
