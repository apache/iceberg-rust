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
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent


class Failure(Exception):
    """A user-facing error."""


def step(message: str) -> None:
    print(f"==> {message}", flush=True)


def ok(message: str) -> None:
    print(f"OK: {message}", flush=True)


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
            "Install it with: cargo install --locked cargo-deny"
        )


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
