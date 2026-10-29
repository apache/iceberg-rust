#!/usr/bin/env python3
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
"""
CLI for assisting maintenance of the project's license checks.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Every distributed artifact must ship its own copy of these files.
REQUIRED_CRATE_FILES = ("LICENSE", "NOTICE")


class Failure(Exception):
    """A user-facing error."""


def step(message: str) -> None:
    print(f"==> {message}", flush=True)


def ok(message: str) -> None:
    print(f"OK: {message}", flush=True)


def report_error(message: str) -> None:
    # GitHub Actions turns `::error::` lines into annotations on the job summary.
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::error::{message}", flush=True)
    else:
        print(f"ERROR: {message}", file=sys.stderr, flush=True)


def require_cargo() -> None:
    """Assert cargo is installed."""
    try:
        subprocess.run(
            ["cargo", "--version"],
            capture_output=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        raise Failure("This script requires 'cargo', but it is not installed.")


def require_cargo_deny() -> None:
    """Assert cargo-deny is installed."""
    try:
        subprocess.run(
            ["cargo", "deny", "--version"],
            capture_output=True,
            check=True,
        )
    except FileNotFoundError:
        raise Failure("This script requires 'cargo', but it is not installed.")
    except subprocess.CalledProcessError:
        raise Failure(
            "This script requires 'cargo-deny' for dependency license checks.\n"
            "Install it with: make install-cargo-deny"
        )


def is_non_empty_file(path: Path) -> bool:
    """Whether the path resolves, following symlinks, to a non-empty regular file."""
    return path.is_file() and path.stat().st_size > 0


def is_same_file(path: Path, other: Path) -> bool:
    """Whether both paths exist and resolve to the same file; False if either is missing."""
    try:
        return path.samefile(other)
    except OSError:
        return False


def publishable_crates() -> list[tuple[str, Path]]:
    """Name and directory of every crate that would be published in a release."""
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise Failure("Could not list the publishable crates.")

    # `publish` is an empty list for `publish = false` crates and null when the
    # crate may be published to any registry.
    return [
        (package["name"], Path(package["manifest_path"]).parent)
        for package in json.loads(result.stdout)["packages"]
        if package["publish"] != []
    ]


def check_crate_files(crate: str, crate_dir: Path) -> bool:
    """Check the packaged crate ships LICENSE and NOTICE resolving to the root files."""
    print(f"Checking {crate}...", flush=True)
    result = subprocess.run(
        [
            "cargo",
            "package",
            "--package",
            crate,
            "--list",
            "--locked",
            # allows `make check` during development.
            "--allow-dirty",
        ],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        report_error(f"{crate} could not be listed by cargo; see the error above")
        return False

    packaged_paths = set(result.stdout.splitlines())
    passed = True
    for file_name in REQUIRED_CRATE_FILES:
        repo_path = crate_dir / file_name
        if file_name not in packaged_paths:
            report_error(f"{crate} crate is missing {file_name} in packaged file list")
            passed = False
        elif not is_non_empty_file(repo_path):
            report_error(f"{crate} {file_name} does not resolve to a non-empty file: {repo_path}")
            passed = False
        elif not is_same_file(repo_path, REPO_ROOT / file_name):
            report_error(
                f"{crate} {file_name} does not resolve to the repository root {file_name}: {repo_path}"
            )
            passed = False
    return passed


def command_check_crates(_args: argparse.Namespace) -> None:
    # Checking the packaged file list, rather than the crate directory, also
    # catches files that exist on disk but are excluded from the published crate.
    # Each crate's copy must be the repository root file itself, so the license
    # text cannot drift from one crate to the next.
    step("Check crate LICENSE and NOTICE files")
    require_cargo()

    passed = True
    for file_name in REQUIRED_CRATE_FILES:
        if not is_non_empty_file(REPO_ROOT / file_name):
            report_error(
                f"Repository root {file_name} does not resolve to a non-empty file: {REPO_ROOT / file_name}"
            )
            passed = False

    crates = publishable_crates()
    if not crates:
        raise Failure("No publishable crates found in the workspace, expected at least one.")

    for crate, crate_dir in crates:
        passed = check_crate_files(crate, crate_dir) and passed

    if not passed:
        raise Failure(
            "Every publishable crate must package a top-level LICENSE and NOTICE "
            "resolving to the repository root copies."
        )

    ok("Check crate LICENSE and NOTICE files")


def command_check_dependencies(_args: argparse.Namespace) -> None:
    step("Check dependency licenses")
    require_cargo_deny()

    result = subprocess.run(
        ["cargo", "deny", "check", "license"],
        cwd=REPO_ROOT,
        check=False,
    )
    if result.returncode != 0:
        raise Failure("cargo-deny rejected the dependency licenses.")

    ok("Check dependency licenses")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    cmd_groups = parser.add_subparsers(dest="group", required=True)

    dependencies = cmd_groups.add_parser(
        "dependencies",
        help="license policy for the Rust dependency graph, enforced through deny.toml",
    )
    dependencies_commands = dependencies.add_subparsers(dest="command", required=True)

    check = dependencies_commands.add_parser(
        "check",
        help="assert every dependency's license is one deny.toml allows",
    )
    check.set_defaults(func=command_check_dependencies)

    crates = cmd_groups.add_parser(
        "crates",
        help="Sub-commands for working with licensing on crates",
    )
    crates_commands = crates.add_subparsers(dest="command", required=True)

    check = crates_commands.add_parser(
        "check",
        help="assert every publishable crate packages the repository root LICENSE and NOTICE",
    )
    check.set_defaults(func=command_check_crates)

    args = parser.parse_args()
    try:
        args.func(args)
    except Failure as failure:
        print(str(failure), file=sys.stderr)
        print(f"FAILED: {args.group} {args.command}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
