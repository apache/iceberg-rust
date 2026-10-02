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

# Every distributed artifact must ship its own LICENSE and NOTICE.
# Checking the packaged file list, rather than the crate directory,
# also catches files that exist on disk but are excluded from the published
# crate. Each crate's copy must be the repository root file itself, so the
# license text cannot drift from one crate to the next.

set -Eeuo pipefail

REQUIRED_FILES=(LICENSE NOTICE)

usage() {
  cat <<USAGE
Usage:
  $0 [-h|--help]

Checks that every publishable crate in the workspace packages a top-level
LICENSE and NOTICE file, as required for Apache source and binary releases.
USAGE
}

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "This script requires '${command_name}', but it is not installed." >&2
    return 1
  fi
}

report_error() {
  # GitHub Actions turns `::error::` lines into annotations on the job summary.
  if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    echo "::error::$1"
  else
    echo "ERROR: $1" >&2
  fi
}

# Return name and manifest path for every crate that would be published in a release.
publishable_crates() {
  # `publish` is an empty list for `publish = false` crates and null when the
  # crate may be published to any registry.
  cargo metadata --no-deps --format-version 1 |
    jq -r '.packages[] | select(.publish != []) | "\(.name)\t\(.manifest_path)"'
}

# Check the path resolves to a non-empty regular file, following any symlinks.
check_file_exists() {
  local path="$1"

  # `-f` follows symlinks and rejects directories.
  # `-s` additionally requires content, so an emptied file fails too.
  if [ ! -f "${path}" ] || [ ! -s "${path}" ]; then
    return 1
  fi
}

# Return true if both paths are the same file.
is_same_file() {
  # `-ef` is short for "equal file": it is true when both arguments resolve to the
  # same device and inode.
  [ "$1" -ef "$2" ]
}

# Return true if the given path (second arg) is present in the list of paths (first arg).
packages_file_at_root() {
  local packaged_paths="$1"
  local file_name="$2"

  local packaged_path

  while IFS= read -r packaged_path; do
    if [ "${packaged_path}" = "${file_name}" ]; then
      return 0
    fi
  done <<<"${packaged_paths}"

  return 1
}

# Verify that the necessary files are bundled in the crate.
# Accepts both crate name and its directory, so that artifacts can be fully verified including symlink resolution.
check_crate() {
  local crate="$1"
  local crate_dir="$2"

  local packaged_paths status repo_path
  status=0

  echo "Checking ${crate}..."
  # --no-verify is no-op at the time of writing, but to be clear: packages will not be built, only expected files enumerated
  # --allow-dirty allows for `make check` during development
  if ! packaged_paths="$(cargo package --package "${crate}" --list --locked --no-verify --allow-dirty)"; then
    report_error "${crate} could not be listed by cargo; see the error above"
    return 1
  fi

  for required_file in "${REQUIRED_FILES[@]}"; do
    # Not only verify that the path is included, but verify that it links to the root file.
    repo_path="${crate_dir}/${required_file}"
    if ! packages_file_at_root "${packaged_paths}" "${required_file}"; then
      report_error "${crate} crate is missing ${required_file} in packaged file list"
      status=1
    elif ! check_file_exists "${repo_path}"; then
      report_error "${crate} ${required_file} does not resolve to a non-empty file: ${repo_path}"
      status=1
    elif ! is_same_file "${repo_path}" "${REPO_ROOT}/${required_file}"; then
      report_error "${crate} ${required_file} does not resolve to the repository root ${required_file}: ${repo_path}"
      status=1
    fi
  done

  return "${status}"
}

# Route CLI args
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_command cargo
require_command jq

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

FAILED=0

echo "Checking repository root..."
for required_file in "${REQUIRED_FILES[@]}"; do
  if ! check_file_exists "${REPO_ROOT}/${required_file}"; then
    report_error "Repository root ${required_file} does not resolve to a non-empty file: ${REPO_ROOT}/${required_file}"
    FAILED=1
  fi
done

# Check for non-zero exit code, otherwise store in `PUBLISHABLE_CRATES`
# Must be checked here rather than piped directly, otherwise the error code is ignored
if ! PUBLISHABLE_CRATES="$(publishable_crates)"; then
  report_error "Could not list the publishable crates"
  exit 1
fi

# Verify at least one crate was listed
if [ -z "${PUBLISHABLE_CRATES}" ]; then
  report_error "No publishable crates found in the workspace, expected at least one"
  exit 1
fi

while IFS=$'\t' read -r crate manifest_path; do
  check_crate "${crate}" "$(dirname "${manifest_path}")" || FAILED=1
done <<<"${PUBLISHABLE_CRATES}"

if [ "${FAILED}" -ne 0 ]; then
  echo "Every publishable crate must package a top-level LICENSE and NOTICE resolving to the repository root copies." >&2
  exit 1
fi

echo "All publishable crates package LICENSE and NOTICE."
