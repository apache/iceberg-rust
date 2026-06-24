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
      Numeric release candidate round.
      Example: 2 creates tag v0.9.1-rc.2 and dist dir apache-iceberg-rust-0.9.1-rc2.

Options:
  --release_ref <ref>
      Git commit-ish to archive and tag.
      Default: HEAD

  --dist_dir <dir>
      Directory where RC artifacts are written.
      Relative paths are resolved from the repository root.
      Default: dist

  --create_rc_tag <0|1>
      Whether to create the signed annotated RC git tag as the final release step.
      Default: 1

  --check_headers <0|1>
      Whether to check Apache license headers against the generated source archive.
      Default: 1

  --check_deps <0|1>
      Whether to run the dependency license check before creating artifacts.
      Default: 1

  --sign <0|1>
      Whether to create and verify the detached GPG signature for the source archive.
      Default: 1

  --upload_svn <0|1>
      Whether to upload RC artifacts to the ASF dev dist SVN repository.
      Default: 0

  --svn_dist_url <url>
      SVN directory URL where the RC artifact directory will be uploaded.
      Default: https://dist.apache.org/repos/dist/dev/iceberg

  -h, --help
      Show this help message.

Examples:
  $0 0.9.1 2
  $0 0.9.1 2 --create_rc_tag 0 --sign 0
  $0 0.9.1 2 --upload_svn 1
  $0 0.9.1 2 --release_ref abc123 --dist_dir /tmp/iceberg-rust-dist
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

require_cargo_deny() {
  require_command cargo
  if ! cargo deny --version >/dev/null 2>&1; then
    echo "This step requires 'cargo-deny' for dependency license checks." >&2
    echo "Install it with: cargo install cargo-deny" >&2
    echo "To skip this step locally, pass: --check_deps 0" >&2
    return 1
  fi
}

require_gpg_secret_key() {
  require_command gpg
  if ! gpg --list-secret-keys --with-colons 2>/dev/null | grep -q '^sec'; then
    echo "This step requires a GPG secret key for signing release artifacts or tags." >&2
    echo "Set up a release signing key first: https://rust.iceberg.apache.org/reference/setup_gpg.html" >&2
    echo "To skip signing locally, pass: --sign 0 --create_rc_tag 0" >&2
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
      --release_ref | --release-ref)
        require_option_value "$1" "${2:-}"
        RELEASE_REF="$2"
        shift 2
        ;;
      --dist_dir | --dist-dir)
        require_option_value "$1" "${2:-}"
        RELEASE_DIST_DIR="$2"
        shift 2
        ;;
      --create_rc_tag | --create-rc-tag)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        CREATE_RC_TAG="$2"
        shift 2
        ;;
      --check_headers | --check-headers)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        CHECK_HEADERS="$2"
        shift 2
        ;;
      --check_deps | --check-deps)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        CHECK_DEPS="$2"
        shift 2
        ;;
      --sign)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        SIGN_ARCHIVE="$2"
        shift 2
        ;;
      --upload_svn | --upload-svn)
        require_option_value "$1" "${2:-}"
        validate_bool_option "$1" "$2"
        UPLOAD_SVN="$2"
        shift 2
        ;;
      --svn_dist_url | --svn-dist-url)
        require_option_value "$1" "${2:-}"
        SVN_DIST_URL="$2"
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
  RC_ID="apache-iceberg-rust-${VERSION}-rc${RC}"
  ARCHIVE_BASE_NAME="apache-iceberg-rust-${VERSION}"
  ARCHIVE_FILE_NAME="${ARCHIVE_BASE_NAME}.tar.gz"

  if [[ "${RELEASE_DIST_DIR}" = /* ]]; then
    DIST_ROOT="${RELEASE_DIST_DIR}"
  else
    DIST_ROOT="${REPO_ROOT}/${RELEASE_DIST_DIR}"
  fi
  RC_DIR="${DIST_ROOT}/${RC_ID}"
  SVN_DIST_URL="${SVN_DIST_URL%/}"
  SVN_RC_URL="${SVN_DIST_URL}/${RC_ID}"
}

check_release_ref() {
  require_command git
  git -C "${REPO_ROOT}" rev-parse --verify "${RELEASE_REF}^{commit}" >/dev/null
}

check_rc_tag_available() {
  require_command git
  if git -C "${REPO_ROOT}" rev-parse --verify --quiet "refs/tags/${RC_TAG}" >/dev/null; then
    echo "Tag '${RC_TAG}' already exists." >&2
    return 1
  fi
}

check_dependency_licenses() {
  require_cargo_deny
  (
    trap - ERR
    cd "${REPO_ROOT}"
    cargo deny check license
  )
}

prepare_output_directory() {
  rm -rf "${RC_DIR}"
  mkdir -p "${RC_DIR}"
}

create_source_archive() {
  require_command git
  git -C "${REPO_ROOT}" archive \
    --format=tar.gz \
    --output="${RC_DIR}/${ARCHIVE_FILE_NAME}" \
    --prefix="${ARCHIVE_BASE_NAME}/" \
    "${RELEASE_REF}"
}

check_license_headers() {
  require_command tar
  require_command docker
  require_command mktemp

  (
    trap - ERR
    local header_tmp_dir
    header_tmp_dir="$(mktemp -d -t "iceberg-rust-headers.XXXXXX")"
    cleanup() {
      rm -rf "${header_tmp_dir}"
    }
    trap cleanup EXIT

    tar -xzf "${RC_DIR}/${ARCHIVE_FILE_NAME}" -C "${header_tmp_dir}"
    docker run --rm -v "${header_tmp_dir}/${ARCHIVE_BASE_NAME}:/github/workspace" apache/skywalking-eyes header check
  )
}

sign_source_archive() {
  require_gpg_secret_key
  (
    trap - ERR
    cd "${RC_DIR}"
    gpg --armor --output "${ARCHIVE_FILE_NAME}.asc" --detach-sig "${ARCHIVE_FILE_NAME}"
  )
}

verify_source_signature() {
  require_command gpg
  (
    trap - ERR
    cd "${RC_DIR}"
    gpg --verify "${ARCHIVE_FILE_NAME}.asc" "${ARCHIVE_FILE_NAME}"
  )
}

generate_checksum() {
  require_checksum_command
  (
    trap - ERR
    cd "${RC_DIR}"
    if command -v shasum >/dev/null 2>&1; then
      shasum -a 512 "${ARCHIVE_FILE_NAME}" > "${ARCHIVE_FILE_NAME}.sha512"
    else
      sha512sum "${ARCHIVE_FILE_NAME}" > "${ARCHIVE_FILE_NAME}.sha512"
    fi
  )
}

verify_checksum() {
  require_checksum_command
  (
    trap - ERR
    cd "${RC_DIR}"
    if command -v shasum >/dev/null 2>&1; then
      shasum -a 512 -c "${ARCHIVE_FILE_NAME}.sha512"
    else
      sha512sum -c "${ARCHIVE_FILE_NAME}.sha512"
    fi
  )
}

require_release_artifact() {
  local artifact_path="$1"
  if [ ! -f "${artifact_path}" ]; then
    echo "Required release artifact is missing: ${artifact_path}" >&2
    return 1
  fi
}

upload_to_svn() {
  require_command svn
  require_release_artifact "${RC_DIR}/${ARCHIVE_FILE_NAME}"
  require_release_artifact "${RC_DIR}/${ARCHIVE_FILE_NAME}.asc"
  require_release_artifact "${RC_DIR}/${ARCHIVE_FILE_NAME}.sha512"

  if svn info "${SVN_RC_URL}" >/dev/null 2>&1; then
    echo "SVN target already exists: ${SVN_RC_URL}" >&2
    return 1
  fi

  svn import "${RC_DIR}" "${SVN_RC_URL}" -m "Prepare Apache Iceberg Rust ${VERSION} RC${RC}"
}

create_rc_tag() {
  require_command git
  require_gpg_secret_key
  git -C "${REPO_ROOT}" tag -s "${RC_TAG}" "${RELEASE_REF}" -m "Apache Iceberg Rust ${VERSION} RC${RC}"
}

print_summary() {
  echo "Created Apache Iceberg Rust RC artifacts:"
  echo "  Tag: ${RC_TAG}"
  echo "  Directory: ${RC_DIR}"
  echo "  Archive: ${RC_DIR}/${ARCHIVE_FILE_NAME}"
  if enabled "${SIGN_ARCHIVE}"; then
    echo "  Signature: ${RC_DIR}/${ARCHIVE_FILE_NAME}.asc"
  fi
  echo "  Checksum: ${RC_DIR}/${ARCHIVE_FILE_NAME}.sha512"
  if enabled "${UPLOAD_SVN}"; then
    echo "  SVN URL: ${SVN_RC_URL}"
  fi
}

print_vote_email() {
  cat <<MAIL
Draft vote email for dev@iceberg.apache.org:

---------------------------------------------------------
To: dev@iceberg.apache.org
Subject: [VOTE] Release Apache Iceberg Rust ${VERSION} RC${RC}

Hello Apache Iceberg Rust Community,

This is a call for a vote to release Apache Iceberg Rust version ${VERSION}.

The tag to be voted on is: ${RC_TAG}.

The release candidate:

https://dist.apache.org/repos/dist/dev/iceberg/${RC_ID}/

Keys to verify the release candidate:

https://downloads.apache.org/iceberg/KEYS

Git tag for the release:

https://github.com/apache/iceberg-rust/releases/tag/${RC_TAG}

Please download, verify, and test the release candidate.

This vote will be open for at least 72 hours and will remain open until the required number of votes is reached.

Please vote accordingly:
[ ] +1 Approve
[ ] +0 No opinion
[ ] -1 Disapprove (please provide a reason)

To learn more about Apache Iceberg, please visit:
https://rust.iceberg.apache.org/

Checklist for reference:
[ ] Download links are valid
[ ] Checksums and signatures are correct
[ ] LICENSE and NOTICE files are present
[ ] No unexpected binary files are included
[ ] All source files have ASF headers
[ ] The project builds successfully from source
[ ] pyiceberg-core builds and tests successfully

For more details, please refer to:
https://rust.iceberg.apache.org/release.html#how-to-verify-a-release

Thanks,
<your name>
---------------------------------------------------------
MAIL
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

VERSION=""
RC=""
RELEASE_REF="HEAD"
RELEASE_DIST_DIR="dist"
CREATE_RC_TAG="1"
CHECK_HEADERS="1"
CHECK_DEPS="1"
SIGN_ARCHIVE="1"
UPLOAD_SVN="0"
SVN_DIST_URL="https://dist.apache.org/repos/dist/dev/iceberg"

show_help_if_requested "$@"
run_step "Parse command arguments" parse_args "$@"
run_step "Validate release arguments" validate_args
derive_release_names

run_step "Check release reference ${RELEASE_REF}" check_release_ref

if enabled "${CREATE_RC_TAG}"; then
  run_step "Check RC tag ${RC_TAG} is available" check_rc_tag_available
else
  skip_step "Check RC tag ${RC_TAG} is available"
fi

if enabled "${CHECK_DEPS}"; then
  run_step "Check dependency licenses" check_dependency_licenses
else
  skip_step "Check dependency licenses"
fi

run_step "Prepare output directory ${RC_DIR}" prepare_output_directory
run_step "Create source archive ${ARCHIVE_FILE_NAME}" create_source_archive

if enabled "${CHECK_HEADERS}"; then
  run_step "Check source archive license headers" check_license_headers
else
  skip_step "Check source archive license headers"
fi

if enabled "${SIGN_ARCHIVE}"; then
  run_step "Sign source archive ${ARCHIVE_FILE_NAME}" sign_source_archive
  run_step "Verify source archive signature" verify_source_signature
else
  skip_step "Sign source archive ${ARCHIVE_FILE_NAME}"
  skip_step "Verify source archive signature"
fi

run_step "Generate SHA-512 checksum" generate_checksum
run_step "Verify SHA-512 checksum" verify_checksum

if enabled "${UPLOAD_SVN}"; then
  run_step "Upload artifacts to SVN dist ${SVN_RC_URL}" upload_to_svn
else
  skip_step "Upload artifacts to SVN dist ${SVN_RC_URL}"
fi

if enabled "${CREATE_RC_TAG}"; then
  run_step "Create signed RC tag ${RC_TAG}" create_rc_tag
else
  skip_step "Create signed RC tag ${RC_TAG}"
fi

run_step "Print artifact summary" print_summary
run_step "Print draft vote email" print_vote_email
