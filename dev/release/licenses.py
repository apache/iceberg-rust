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

# /// script
# requires-python = ">=3.11"
# dependencies = ["xxhash"]
# ///

"""
CLI for assisting maintenance of the project's license checks and any generated artifacts.
"""

import argparse
import json
import subprocess
import sys
import textwrap
from collections import defaultdict
from pathlib import Path

if sys.version_info < (3, 11):
    sys.exit(
        "This script needs Python 3.11 or newer for tomllib; found "
        f"{sys.version.split()[0]} at {sys.executable}."
    )

import tomllib

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

DENY_TOML = REPO_ROOT / "deny.toml"

# How to name this script in a command a reader is meant to run. Fully resolved, not
# `Path(__file__).name`: the bare name is only a usable path from this directory, and the
# check is documented to run from the repository root. Absolute rather than repo-relative
# so the command also works when it was invoked from somewhere else entirely, which is the
# same reason the crate directories in these messages are absolute.
SCRIPT_PATH = Path(__file__).resolve()

CrateName = str
CrateVersion = str
Crate = tuple[CrateName, CrateVersion]


class Failure(Exception):
    """A user-facing error. main() prints it and exits non-zero."""

    def __init__(self, message: str):
        super().__init__(message)


def step(message: str) -> None:
    print(f"==> {message}", flush=True)


def ok(message: str) -> None:
    print(f"OK: {message}", flush=True)


def require_cargo_deny() -> None:
    """Assert cargo-deny is installed.

    Deliberately not version-pinned. The pin in dev/release/generate-dependency-tsv-files.sh exists
    because the DEPENDENCIES.rust.tsv files it generates differ between cargo-deny
    versions; nothing here is committed, so a mismatched binary can only change the
    wording of a diagnostic.
    """
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


def declared_clarifications() -> set[CrateName]:
    """Crate names that deny.toml carries a [[licenses.clarify]] block for."""
    if not DENY_TOML.is_file():
        raise Failure(
            f"Expected the cargo-deny config at {DENY_TOML}, but it is missing."
        )

    with DENY_TOML.open("rb") as handle:
        config = tomllib.load(handle)

    blocks = config.get("licenses", {}).get("clarify", [])
    if not blocks:
        raise Failure(
            f"Expected at least one [[licenses.clarify]] block in {DENY_TOML}, found none.\n"
            "If the last one was deliberately removed, remove this check with it."
        )

    unnamed = [index for index, block in enumerate(blocks) if "crate" not in block]
    if unnamed:
        raise Failure(
            f"{DENY_TOML} has [[licenses.clarify]] block(s) with no `crate` key, at "
            f"position(s) {', '.join(str(index + 1) for index in unnamed)}."
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


def evaluated_crates() -> tuple[
    dict[CrateName, set[CrateVersion]], dict[CrateName, set[CrateVersion]]
]:
    """Every crate cargo-deny judged, along with the subset that had a clarification applied."""
    # cargo-deny is invoked again here, but output is suppressed and JSON is captured to processing.
    result = subprocess.run(
        ["cargo", "deny", "--format", "json", "check", "license", "-W", "accepted"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    # cargo-deny writes its diagnostics to stderr, including in JSON mode.
    diagnostics = result.stderr or result.stdout
    if result.returncode != 0:
        print(diagnostics, file=sys.stderr, end="")
        raise Failure(
            "cargo-deny failed while re-running to verify that no license "
            "clarifications are ignored."
        )

    evaluated: dict[CrateName, set[CrateVersion]] = defaultdict(set)
    clarified: dict[CrateName, set[CrateVersion]] = defaultdict(set)
    for line in diagnostics.splitlines():
        if not line.strip():
            continue
        try:
            diagnostic = json.loads(line)
        except json.JSONDecodeError:
            raise Failure(
                "cargo-deny emitted a line that is not JSON, so its output cannot be "
                f"read reliably:\n  {line[:200]}"
            )

        fields = diagnostic.get("fields", {})

        if fields.get("code") != "accepted":
            # This fn is to fail on any bad accepted outcomes, so ignoring anything else is fine.
            continue

        # `or []`, not a .get default: a JSON null would otherwise reach len() below
        # as None and raise TypeError instead of the Failure written for it.
        graphs = fields.get("graphs") or []
        if len(graphs) != 1:
            # There's a few types of diagnostic,
            # but for 'accepted' we expect a single graph for the one crate it covers.
            raise Failure(
                f"cargo-deny reported an 'accepted' license verdict covering "
                f"{len(graphs)} crates, but one was expected, so the crate it applies "
                f"to is ambiguous:\n  {line[:200]}"
            )
        krate = graphs[0].get("Krate", {})

        name, version = krate.get("name"), krate.get("version")
        if name is None or version is None:
            raise Failure(
                "cargo-deny reported an 'accepted' license verdict without naming both "
                f"the crate and its version:\n  {line[:200]}"
            )

        evaluated[name].add(version)
        labels = fields.get("labels") or []
        CLARIFICATION_APPLIED_MSG = "license expression retrieved via user override"
        if any(label.get("message") == CLARIFICATION_APPLIED_MSG for label in labels):
            clarified[name].add(version)

    return dict(evaluated), dict(clarified)


def cargo_metadata() -> dict:
    """Resolved cargo metadata for this workspace.

    `--locked` so resolving it cannot rewrite Cargo.lock, least of all part-way through
    cutting a release.
    """
    result = subprocess.run(
        [
            "cargo",
            "metadata",
            "--format-version",
            "1",
            "--locked",
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


    The directory is `<name>-<version>`, but names contain hyphens (brotli-decompressor-5.0.3) and
    versions carry build metadata (zstd-sys-2.0.16+zstd.1.5.7).
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
    clarified: dict[CrateName, set[CrateVersion]],
) -> None:
    """
    Fail if a clarification did not reach a crate version that was evaluated.

    `declared` is all crates declared with a clarification in deny.toml,
    `evaluated` is the list of all crate versions evaluated as part of the dependency graph,
    and `clarified` is the subset of crate versions evaluated which had a clarification applied.
    """
    crates_with_clarifications_missed: list[Crate] = sorted(
        (crate_name, crate_version)
        for crate_name in declared & evaluated.keys()
        for crate_version in evaluated[crate_name] - clarified.get(crate_name, set())
    )
    if not crates_with_clarifications_missed:
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
                <crate directory above>/<license file>

            If a hash has changed, read the file and ensure the license expression still matches.
            If it does, the updated deny.toml should be committed.
            """).format(
            deny_toml=DENY_TOML,
            crates="\n".join(error_line_output),
            script=SCRIPT_PATH,
        )
    )


def require_no_stale_clarifications(
    declared: set[CrateName], evaluated: dict[CrateName, set[CrateVersion]]
) -> None:
    """Fail if a clarification names a crate that is not in the dependency graph.

    This catches stale entries to cleanup - not critical.
    """
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

    XxHash32 with seed 0, over the file with CRLF normalised to LF and exactly one
    trailing newline.
    """
    try:
        import xxhash
    except ModuleNotFoundError:
        raise Failure(
            "The module 'xxhash' is required to compute the hash for cargo-deny's deny.toml file."
        )

    text = path.read_bytes().replace(b"\r\n", b"\n").rstrip(b"\n") + b"\n"
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

    result = subprocess.run(
        ["cargo", "deny", "check", "license", "-D", "missing-clarification-file"],
        cwd=REPO_ROOT,
        check=False,
    )
    if result.returncode != 0:
        # error is already on stderr, just exit now
        raise Failure("cargo-deny rejected the dependency licenses.")

    declared = declared_clarifications()
    evaluated, clarified = evaluated_crates()

    require_clarifications_applied(declared, evaluated, clarified)
    require_no_stale_clarifications(declared, evaluated)

    ok("Check dependency licenses")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    # Grouped by subject rather than flat: the dependency graph is one body of work and
    # the wheels are another, and each has both a gate and the tools to satisfy it.
    cmd_groups = parser.add_subparsers(dest="group", required=True)

    dependencies = cmd_groups.add_parser(
        "dependencies",
        help="license policy for the Rust dependency graph, enforced through deny.toml",
    )
    dependencies_commands = dependencies.add_subparsers(dest="command", required=True)

    check = dependencies_commands.add_parser(
        "check",
        help=textwrap.dedent("""
        cargo-deny decides whether a dependency's license is one the project may ship.
        Where a crate's own manifest is wrong or incomplete (typically because it vendors foreign code under different terms),
        deny.toml contains a [[licenses.clarify]] block stating what licenses actually apply.

        cargo-deny ignores those entries when the checksum no longer matches.
        This command adds checks to catch when this happens, as the cargo-deny invocation would otherwise report no error.
        """),
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
