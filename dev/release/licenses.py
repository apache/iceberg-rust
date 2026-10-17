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
#
# /// script
# requires-python = ">=3.11"
# dependencies = ["xxhash"]
# ///
"""
CLI for assisting maintenance of the project's license checks.

Additional validations are applied using this Python script,
to cover cases where we require cargo-deny to fail but it only warns.
"""

import argparse
import json
import subprocess
import sys
import textwrap
from collections import defaultdict
from pathlib import Path

import tomllib

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DENY_TOML = REPO_ROOT / "deny.toml"
SCRIPT_PATH = Path(__file__).resolve()

CrateName = str
CrateVersion = str
Crate = tuple[CrateName, CrateVersion]


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
            "Install it with: make install-cargo-deny"
        )


def parse_declared_clarifications() -> set[CrateName]:
    """Crate names that deny.toml carries a [[licenses.clarify]] block for."""
    with DENY_TOML.open("rb") as handle:
        config = tomllib.load(handle)

    blocks = config.get("licenses", {}).get("clarify", [])
    if not blocks:
        raise Failure(
            f"Expected at least one [[licenses.clarify]] block in {DENY_TOML}, found none.\n"
            "If the last one was deliberately removed, remove this check with it."
        )

    pinned = sorted(block["crate"] for block in blocks if "@" in block["crate"])
    if pinned:
        raise Failure(
            textwrap.dedent("""
                {deny_toml} pins a [[licenses.clarify]] block to a crate version:
                {specs}

                Version pins are rejected by this script.
                cargo-deny supports them, but will not report when they become stale.
                To keep things simple, this script requires that no pinning is used.
                Where newer versions change licensing, the script will reject the clarification
                and the deny.toml file should be updated.
                """).format(
                deny_toml=DENY_TOML,
                specs="\n".join(f"  {spec}" for spec in pinned),
            )
        )

    return {block["crate"] for block in blocks}


def run_cargo_deny() -> tuple[
    dict[CrateName, set[CrateVersion]], dict[CrateName, set[CrateVersion]]
]:
    """
    Run cargo-deny, returning mappings from crate name to crate version
    for every crate version evaluated plus the subset which had a clarification applied.
    """
    result = subprocess.run(
        [
            "cargo",
            "deny",
            "--format=json",
            "check",
            "license",
            # We emit the license accepted messages as warnings so we can count them.
            "--warn=accepted",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    diagnostics = result.stderr

    evaluated: dict[CrateName, set[CrateVersion]] = defaultdict(set)
    clarified: dict[CrateName, set[CrateVersion]] = defaultdict(set)
    errors: list[str] = []
    for line in diagnostics.splitlines():
        if not line.strip():
            continue
        line_truncated = line[:200]
        try:
            diagnostic = json.loads(line)
        except json.JSONDecodeError:
            raise Failure(
                "cargo-deny emitted a line that is not JSON, so its output cannot be "
                f"parsed:\n  {line_truncated}"
            )

        fields = diagnostic.get("fields", {})

        if fields.get("severity") == "error" or fields.get("level") == "ERROR":
            krate = (fields.get("graphs") or [{}])[0].get("Krate", {})
            crate = f"{krate['name']} {krate['version']}: " if krate else ""
            labels = fields.get("labels") or []

            error_msg = f"  {crate}{fields.get('message')}"
            if fields.get("code") == "rejected":
                # For rejections, include the expression. Other codes are too noisy (such as TOML error).
                span = labels[0].get("span")
                error_msg = f"{error_msg} ({span})"
            errors.append(error_msg)

        if fields.get("code") != "accepted":
            continue

        graphs = fields.get("graphs") or []
        if len(graphs) != 1:
            raise Failure(
                f"cargo-deny reported an 'accepted' license verdict covering "
                f"{len(graphs)} crates, but one was expected:\n  {line_truncated}"
            )
        krate = graphs[0].get("Krate", {})

        name, version = krate.get("name"), krate.get("version")
        if name is None or version is None:
            raise Failure(
                "cargo-deny reported an 'accepted' license verdict without naming both "
                f"the crate and its version:\n  {line_truncated}"
            )

        evaluated[name].add(version)
        labels = fields.get("labels") or []

        # There's no machine readable error code.
        # If this error message changes, the script will fail closed anyway as expected clarifications won't be found.
        CLARIFICATION_APPLIED_MSG = "license expression retrieved via user override"
        if any(label.get("message") == CLARIFICATION_APPLIED_MSG for label in labels):
            clarified[name].add(version)

    if result.returncode != 0:
        # Emit list of errors, or simply diagnostics if error code is non-zero but we failed to parse any errors.
        detail = "\n".join(errors) if errors else diagnostics.rstrip("\n")
        raise Failure(f"cargo-deny rejected the dependency licenses:\n{detail}")

    return dict(evaluated), dict(clarified)


def cargo_metadata() -> dict:
    """Resolved cargo metadata for this workspace."""
    result = subprocess.run(
        [
            "cargo",
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--all-features",
            "--manifest-path",
            str(REPO_ROOT / "Cargo.toml"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise Failure(f"cargo metadata failed:\n{result.stderr}")
    return json.loads(result.stdout)


def crate_source_dirs(crates: set[Crate]) -> dict[Crate, str]:
    """Where cargo unpacked each crate's sources.

    The directory is `<name>-<version>`, but names often contain hyphens (eg. my-crate-5.0.3) and
    versions can carry build metadata (eg. my-crate-5.0.3+mybuild).
    """
    packages = cargo_metadata()["packages"]

    dict_crate_to_path = {}

    for package in packages:
        crate = (package["name"], package["version"])
        if crate not in crates:
            continue
        dict_crate_to_path[crate] = str(Path(package["manifest_path"]).parent)

    return dict_crate_to_path


def require_clarifications_applied(
    declared: set[CrateName],
    evaluated: dict[CrateName, set[CrateVersion]],
    actually_clarified: dict[CrateName, set[CrateVersion]],
) -> None:
    """
    Fail if a clarification did not reach a crate version that was evaluated.

    `declared` is all crates declared with a clarification in deny.toml,
    `evaluated` is the list of all crate versions evaluated as part of the dependency graph,
    and `actually_clarified` is the subset of crate versions evaluated which had a clarification applied.
    """
    crates_with_clarifications_missed: list[Crate] = []
    for crate_name in declared:
        if crate_name not in evaluated:
            continue
        versions_evaluated = evaluated[crate_name]
        versions_clarified = actually_clarified.get(crate_name, set())
        for crate_version in versions_evaluated - versions_clarified:
            crates_with_clarifications_missed.append((crate_name, crate_version))
    crates_with_clarifications_missed.sort()
    if not crates_with_clarifications_missed:
        # Nothing to report
        return

    source_dirs = crate_source_dirs(set(crates_with_clarifications_missed))
    error_line_output = []
    for crate_name, crate_version in crates_with_clarifications_missed:
        source_dir = source_dirs.get(
            (crate_name, crate_version), "<source not unpacked>"
        )
        error_line_output.append(
            f"  {crate_name} {crate_version} ({source_dir}): clarification did not apply"
        )

    raise Failure(
        textwrap.dedent("""
            {deny_toml} declares license clarifications that cargo-deny did not apply:
            {crates}

            A clarification is discarded by cargo-deny when any of its attested license
            files are no longer valid (hash mismatch, etc.).

            Review the clarification, checking all files still exist and the hash matches for each.
            You can compute the hash for a license file using the following command:

            uv run {script} dependencies clarification-hash \\
                <crate directory above>/<license file path>

            If a hash has changed, read the file and ensure the license expression still matches.
            If it does, an updated deny.toml should be committed.
            """).format(
            deny_toml=DENY_TOML,
            crates="\n".join(error_line_output),
            script=SCRIPT_PATH,
        )
    )


def require_no_stale_clarifications(
    declared: set[CrateName], evaluated: dict[CrateName, set[CrateVersion]]
) -> None:
    """Fail if a clarification names a crate that is not in the dependency graph."""
    stale: list[CrateName] = sorted(declared - evaluated.keys())
    if not stale:
        return

    raise Failure(
        textwrap.dedent(
            """
            {deny_toml} declares license clarifications for crates that cargo-deny did
            not find in the dependency graph:
            {crates}

            These are likely stale entries that should be removed.
            """
        ).format(
            deny_toml=DENY_TOML,
            crates="\n".join(f"  {crate_name}" for crate_name in stale),
        )
    )


def clarification_file_hash(path: Path) -> str:
    """The hash cargo-deny expects in a [[licenses.clarify]] `license-files` entry.

    XxHash32 with seed 0, over the file with every line terminated by exactly one LF.

    cargo-deny appears to offer no way to compute this value, so this command is added for convenience.
    The logic in this fn is based on review of the cargo-deny source - no src was directly copied.
    """
    try:
        # Import here, so other commands of the script can still work if missing.
        import xxhash
    except ModuleNotFoundError:
        raise Failure(
            "The module 'xxhash' is required to compute the hash for cargo-deny's deny.toml file."
        )

    # Split ONLY on '\n' and drop carriage return after to match cargo-deny.
    lines = path.read_bytes().split(b"\n")
    if lines and lines[-1] == b"":
        # Drop a trailing empty line, otherwise we get double newline at the end.
        lines.pop()
    # Drop any carriage return, ensure every line ends '\n'
    text = b"".join(line.rstrip(b"\r") + b"\n" for line in lines)

    digest = xxhash.xxh32(text, seed=0).intdigest()
    return f"0x{digest:08x}"


def command_clarification_hash(args: argparse.Namespace) -> None:
    """Print the clarification hash of one license file, and nothing else."""
    path: Path = args.file
    if not path.is_file():
        raise Failure(f"No such file: {path}")
    print(clarification_file_hash(path))


def command_check_dependencies(_args: argparse.Namespace) -> None:
    step("Check dependency licenses")
    require_cargo_deny()

    evaluated, clarified = run_cargo_deny()
    declared = parse_declared_clarifications()

    require_clarifications_applied(declared, evaluated, clarified)
    require_no_stale_clarifications(declared, evaluated)

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

    clarification_hash = dependencies_commands.add_parser(
        "clarification-hash",
        help="print the hash deny.toml should record for one crate license file",
    )
    clarification_hash.add_argument(
        "file",
        metavar="FILE",
        type=Path,
        help="the license file to hash",
    )
    clarification_hash.set_defaults(func=command_clarification_hash)

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
