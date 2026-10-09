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

This script relies on `cargo-deny`.
Some dependencies required clarification on which license the source code is distributed under.
`cargo-deny` does not always report failures when these clarifications fail,
so this script supports asserting that none of these failures have occurred.
For example, a clarification may be silently discarded when an attested license file no longer matches.

The `wheel` commands cover attribution rather than policy.
A pyiceberg-core wheel statically links every Rust dependency, so it redistributes that code.
It must carry each crate's license text, and relay the NOTICE files of its Apache-2.0 dependencies.
Source releases and crates.io publications contain none of that code.
`cargo-about` generates these files and takes its own clarifications from bindings/python/about.toml.
It discards a stale clarification just as `cargo-deny` does, so the same assertions are made here.
"""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import zipfile
from collections import defaultdict
from pathlib import Path

if sys.version_info < (3, 11):
    sys.exit(
        "This script needs Python 3.11 or newer for tomllib; found "
        f"{sys.version.split()[0]} at {sys.executable}."
    )

import tomllib  # noqa: E402  (after the version guard, which explains why)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

DENY_TOML = REPO_ROOT / "deny.toml"

# Absolute, so a command printed in a failure message runs from any directory.
SCRIPT_PATH = Path(__file__).resolve()

CrateName = str
CrateVersion = str

# Name and version together, for the places that address one unpacked crate directory.
Crate = tuple[CrateName, CrateVersion]

EXPECTED_CARGO_ABOUT_VERSION = "0.8.4"

# Name of the generated license-text bundle. Gitignored.
THIRD_PARTY_LICENSES_FILE = "THIRD-PARTY-LICENSES"

# Directory, relative to a package, holding one subdirectory of generated legal
# files per published wheel. Gitignored, never staged into a source release.
LICENSES_DIR_NAME = "licenses"

# Headings the generator writes and `verify` greps for.
NOTICE_RELAY_HEADING = "Bundled dependency notices"

LICENSE_APPENDIX_HEADING = "Bundled third-party components"

# The three files every wheel must carry.
REQUIRED_FILES = ("LICENSE", "NOTICE", THIRD_PARTY_LICENSES_FILE)

# Files whose header names the wheel they were generated for, so `verify` can
# reject a set staged into the wrong wheel.
TARGETED_FILES = ("LICENSE", THIRD_PARTY_LICENSES_FILE)

# Floor on file size - below this size should be far too small for a license,
# and indicates there may have been truncation or another failure.
MIN_BUNDLE_BYTES = 10_000

MaturinTarget = str

RustTargetTriple = str

# One entry per published wheel: the maturin target, then the Rust target triples
# that wheel's extension module is linked for. The two differ because a maturin
# target is not always a Rust triple -- universal2-apple-darwin is maturin's own
# fat-binary target, covering two real triples.
WHEEL_LICENSE_REPORTS: dict[MaturinTarget, tuple[RustTargetTriple, ...]] = {
    "x86_64-unknown-linux-gnu": ("x86_64-unknown-linux-gnu",),
    "aarch64-unknown-linux-gnu": ("aarch64-unknown-linux-gnu",),
    "armv7-unknown-linux-gnueabihf": ("armv7-unknown-linux-gnueabihf",),
    "universal2-apple-darwin": ("x86_64-apple-darwin", "aarch64-apple-darwin"),
    "aarch64-apple-darwin": ("aarch64-apple-darwin",),
    "x86_64-pc-windows-msvc": ("x86_64-pc-windows-msvc",),
}

# Copyright placeholders from the SPDX license templates.
#
# When cargo-about cannot match a license file inside a crate, it falls back to the
# registry template which is usually the SPDX. It does not fail when there is missing metadata
# and may leave placeholders in the file. This checks for those placeholders,
# indicating we need to further clarify the license.
#
# When adding a new license to the `accepted` cargo-about config, please verify there's
# no placeholders we should match on here. You may use the following bash snippet:
#
#   curl -sL https://raw.githubusercontent.com/spdx/license-list-data/main/text/<ID>.txt |
#     grep -oE '<[A-Za-z0-9 _-]+>'
#
# Matched individually rather than as a generic `<...>`: the template emits repository
# URLs as <https://...>, and `Copyright [yyyy] [name of copyright owner]` is part of the
# Apache-2.0 appendix, so both forms appear in correct output.
COPYRIGHT_PLACEHOLDERS = (
    b"<year>",
    b"<owner>",
    b"<copyright holders>",
    b"<copyright holder>",
)

# Names of notice files relayed into the wheel's NOTICE: bare NOTICE, and suffixed variants
# such as NOTICE.txt or NOTICE-MIT. Anything else beginning with NOTICE is a hard
# failure rather than a silent skip -- see the Failure raised in _relayable_notices.
RELAYED_NOTICE_FILENAME = re.compile(r"^NOTICE([.\-].*)?$")

# Appended to the project LICENSE in each wheel's generated copy. The line breaks
# are deliberate: the substituted fields sit on their own lines so the rendered
# paragraphs stay within a sensible width whatever the target is named.
LICENSE_APPENDIX_TEMPLATE = """
{rule}
{heading}
{rule}

This binary wheel bundles compiled artifacts built for
{report}, linking the Rust target triple(s)
{triples}. That module statically links
third-party Rust crates, so the wheel redistributes them.

The complete license text of every such crate is in the file
{bundle}, distributed alongside this LICENSE. The
attribution notices those crates require are relayed in NOTICE.

Most of those crates are offered under a choice of licenses, typically
"MIT OR Apache-2.0". Where that is so, this distribution selects one of
them, and reproduces only the selected license.
{bundle} lists each crate under the license selected for it.
"""


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

        # Only `accepted` names a crate. The other codes report on deny.toml itself, and
        # a crate whose license is rejected has already failed the run above.
        if fields.get("code") != "accepted":
            continue

        # A JSON null has to collapse to a list here, so that a malformed diagnostic
        # reaches the Failure below rather than a TypeError from len().
        graphs = fields.get("graphs") or []
        # An `accepted` verdict covers exactly one crate. Other checks attach several.
        if len(graphs) != 1:
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


def cargo_metadata(*extra_args: str) -> dict:
    """Resolved cargo metadata for this workspace.

    `--locked` so resolving it cannot rewrite Cargo.lock, least of all part-way through
    cutting a release. `extra_args` is for callers that want a narrower answer, such as
    `--no-deps` when only the workspace's own crates matter.
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
            *extra_args,
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
        ["cargo", "deny", "check", "license"],
        cwd=REPO_ROOT,
        check=False,
    )
    if result.returncode != 0:
        raise Failure("cargo-deny rejected the dependency licenses.")

    declared = declared_clarifications()
    evaluated, clarified = evaluated_crates()

    # Ordered deliberately: a clarification that stopped applying means a crate is
    # shipping under licenses deny.toml no longer describes, which matters more than a
    # leftover block for a dependency that has gone.
    require_clarifications_applied(declared, evaluated, clarified)
    require_no_stale_clarifications(declared, evaluated)

    ok("Check dependency licenses")


def ensure_one_newline_termination(text: str) -> str:
    """Return text with exactly one trailing newline."""
    return text.rstrip("\n") + "\n"


def require_cargo_about() -> None:
    """Assert cargo-about is installed at the version CI pins."""
    try:
        result = subprocess.run(
            ["cargo", "about", "--version"],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError:
        raise Failure("This script requires 'cargo', but it is not installed.")
    except subprocess.CalledProcessError:
        raise Failure(
            "This script requires 'cargo-about' to bundle dependency license "
            "texts.\nInstall it with: cargo install --locked "
            f"cargo-about@{EXPECTED_CARGO_ABOUT_VERSION}"
        )

    discovered_version = result.stdout.split()[1]
    if discovered_version != EXPECTED_CARGO_ABOUT_VERSION:
        raise Failure(
            textwrap.dedent("""
                cargo-about version mismatch.
                  expected: {expected} (pinned by CI)
                  found:    {found} ({binary})

                Install the pinned version with:
                  cargo install --locked cargo-about@{expected}
                """).format(
                expected=EXPECTED_CARGO_ABOUT_VERSION,
                found=discovered_version,
                binary=shutil.which("cargo-about"),
            )
        )


def require_about_config(package_dir: Path) -> None:
    """Assert the package directory holds a cargo-about configuration."""
    if not (package_dir / "about.toml").is_file():
        raise Failure(
            f"{package_dir / 'about.toml'} does not exist, so there is no cargo-about\n"
            "configuration describing what this package's wheels bundle."
        )


def require_accepted_covers_deny_allow(package_dir: Path) -> None:
    """Assert about.toml accepts every license deny.toml allows.

    deny.toml decides which licenses we may depend on; about.toml's `accepted`
    decides which one we take when a crate offers a choice, and so which text
    ships. The second has to cover everything the first permits, or cargo-about
    fails on a crate cargo-deny was happy with -- a confusing failure that surfaces
    only when a wheel is being built.

    Per-crate `exceptions` in deny.toml are deliberately not compared; cargo-about
    cannot express them.
    """
    deny_toml = REPO_ROOT / "deny.toml"
    about_toml = package_dir / "about.toml"

    with deny_toml.open("rb") as handle:
        allowed = set(tomllib.load(handle).get("licenses", {}).get("allow", []))
    with about_toml.open("rb") as handle:
        accepted = set(tomllib.load(handle)["accepted"])

    missing = sorted(allowed - accepted)
    if missing:
        raise Failure(
            textwrap.dedent("""
                {about_toml} is missing licenses that {deny_toml} allows:
                {licenses}

                cargo-deny permits crates under these, so a dependency could adopt one
                and break the wheel build. Add them to 'accepted', minding that its order
                sets which license is chosen for multi-licensed crates.
                """).format(
                about_toml=about_toml,
                deny_toml=deny_toml,
                licenses="\n".join(f"  {name}" for name in missing),
            )
        )


def require_all_licenses_parsed(stderr: str, package_dir: Path) -> None:
    """Fail if cargo-about skipped a license file it could not parse.

    While cargo-about's `--fail` flag exits non-zero when a crate's license cannot be
    reasonably determined, this doesn't fail when additional licenses are discovered.
    Crates that vendor foreign C sources still declare a license in Cargo.toml
    but cargo-about also discovers the license files inside the vendored tree,
    and one it cannot parse is logged, skipped, but does not affect the exit code.
    Without this check, the crate keeps its declared license and the vendored code
    silently loses its attribution.
    """
    failed_parses = sorted(
        {
            line.strip()
            for line in stderr.splitlines()
            if "failed to parse license" in line
        }
    )
    if not failed_parses:
        return
    raise Failure(
        textwrap.dedent("""
            cargo-about could not parse a license file and skipped it, so the generated
            bundle may be missing at least one required attribution:
            {files}

            Add a [<crate>.clarify] block to {about_toml} naming the license file that
            applies to the code compiled into the wheel.
            """).format(
            files="\n".join(f"  {line}" for line in failed_parses),
            about_toml=package_dir / "about.toml",
        )
    )


def require_clarifications_validated(stderr: str, package_dir: Path) -> None:
    """
    Fail if cargo-about discarded a clarification instead of applying it.

    A clarification applies only if *every* file it lists validates, so one bad file
    discards the whole block. cargo-about then falls back to the crate's own manifest and
    matches license files by name, reporting only a warning. This check fails when that
    happens.

    The two causes need different fixes, so they are reported apart. A checksum mismatch
    means the file changed and the block has to be re-read and re-hashed. A retrieval
    failure means a `git` clarification could not reach the crate's source repository:
    cargo-about fetches those through a third-party CDN, so the file is as it was and the
    run only needs repeating.
    """
    discarded = sorted(
        {
            line.strip()
            for line in stderr.splitlines()
            if "failed to validate all files specified in clarification" in line
        }
    )
    if not discarded:
        return

    unreachable = [line for line in discarded if "unable to retrieve" in line]
    mismatched = [line for line in discarded if line not in unreachable]

    if unreachable and not mismatched:
        raise Failure(
            textwrap.dedent("""
                cargo-about could not retrieve a license file that a clarification in
                {about_toml} attests, so the generated bundle is missing the attribution
                that clarification exists to add:
                {clarifications}

                A clarification with a `git` block is fetched from the crate's source
                repository over the network, because the published crate ships no license
                file to point at. Nothing is wrong with deny.toml or about.toml. Retry,
                and if it keeps failing check whether the host is reachable.
                """).format(
                clarifications="\n".join(f"  {line}" for line in unreachable),
                about_toml=package_dir / "about.toml",
            )
        )

    lines = [f"  {line}" for line in mismatched]
    if unreachable:
        lines += ["", "  Also could not be retrieved over the network:"]
        lines += [f"  {line}" for line in unreachable]

    raise Failure(
        textwrap.dedent("""
            cargo-about discarded a clarification because a file checksum did not match,
            so the generated bundle is missing the attribution that clarification exists
            to add:
            {clarifications}

            The crate's license file may have changed. Check that the file still says
            what the block in {about_toml} claims, then update its `checksum` to the new
            sha256.
            """).format(
            clarifications="\n".join(lines),
            about_toml=package_dir / "about.toml",
        )
    )


def run_cargo_about(
    package_dir: Path, triples: tuple[str, ...], extra_args: list[str]
) -> bytes:
    """
    Run cargo-about for one wheel's triples and return stdout as bytes.

    Some post-invocation validations may also be run.
    """
    target_args = []
    for target_triple in triples:
        target_args.extend(["--target", target_triple])
    command = [
        "cargo",
        "about",
        "generate",
        "--locked",
        "--fail",
        "--config",
        "about.toml",
        *target_args,
        *extra_args,
    ]

    result = subprocess.run(command, cwd=package_dir, capture_output=True, check=False)
    stderr = result.stderr.decode("utf-8", errors="replace")
    if stderr:
        print(stderr, file=sys.stderr, end="")
    if result.returncode != 0:
        raise Failure(f"cargo-about failed for {','.join(triples)}")
    require_all_licenses_parsed(stderr, package_dir)
    require_clarifications_validated(stderr, package_dir)
    return result.stdout


def require_no_placeholder_notices(text: bytes, package_dir: Path) -> None:
    """
    Fail if any reproduced license still has an unfilled copyright placeholder.

    If this fails, it indicates that the script is unable to fulfill the obligation
    to copy the full copyright notice into place.
    """
    incomplete_license_placeholders = sorted(
        {
            line.decode("utf-8").strip()
            for line in text.splitlines()
            if any(
                placeholder in line.lower() for placeholder in COPYRIGHT_PLACEHOLDERS
            )
        }
    )
    if not incomplete_license_placeholders:
        return

    detail = "\n".join(f"  {line}" for line in incomplete_license_placeholders)
    raise Failure(
        textwrap.dedent("""
            The generated bundle reproduces a license whose copyright line is still an
            unfilled SPDX template placeholder:
            {placeholders}

            cargo-about could not find a license file in the crate and fell back to the
            SPDX registry template. Add a [<crate>.clarify] block to {about_toml} naming
            the crate's real license file, using `files` if the crate packages it or `git`
            if it only exists upstream.
            """).format(
            placeholders=detail,
            about_toml=package_dir / "about.toml",
        )
    )


def generate_license_texts(
    package_dir: Path, out_dir: Path, maturin_target: str, triples: tuple[str, ...]
) -> None:
    """Render the license-text bundle and substitute its header tokens.

    cargo-about cannot pass a variable through to the template, so about.hbs carries
    placeholder tokens replaced here.

    Manipulate as bytes to minimize chance of transforming the literal license content.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        rendered_path = Path(temp_dir) / "rendered"
        run_cargo_about(
            package_dir, triples, ["--output-file", str(rendered_path), "about.hbs"]
        )
        rendered = rendered_path.read_bytes()
    text = rendered.replace(b"@@REPORT_NAME@@", maturin_target.encode()).replace(
        b"@@TARGET_TRIPLES@@", ", ".join(triples).encode()
    )
    leftover = {match.decode() for match in re.findall(rb"@@[A-Z_]+@@", text)}
    if leftover:
        raise Failure(
            f"A placeholder token survived substitution: {', '.join(sorted(leftover))}. "
            f"Check the token names in {package_dir / 'about.hbs'}."
        )
    require_no_placeholder_notices(text, package_dir)
    (out_dir / THIRD_PARTY_LICENSES_FILE).write_bytes(text)


def workspace_member_names() -> set[str]:
    """Names of the crates in this repository's Cargo workspace."""
    return {package["name"] for package in cargo_metadata("--no-deps")["packages"]}


def _relayable_notices(
    package_dir: Path, triples: tuple[str, ...]
) -> list[tuple[str, list[str]]]:
    """Collect dependency NOTICE files, grouped by identical content.

    Returns [(notice text, [crate labels])] in first-seen order.

    Workspace crates are skipped: they are ASF-owned and the project's own NOTICE
    already covers them. They are identified by name against `cargo metadata`.
    """
    about = json.loads(run_cargo_about(package_dir, triples, ["--format", "json"]))
    members = workspace_member_names()

    # First, highlight any vendored crates if any.
    # Not addressed here, just confirms none are present.
    local_non_members = [
        f"  {entry['package']['name']}-{entry['package']['version']} ({entry['package']['manifest_path']})"
        for entry in about["crates"]
        if entry["package"].get("source") is None
        and entry["package"]["name"] not in members
    ]
    if local_non_members:
        raise Failure(
            textwrap.dedent("""
                These crates are not from a registry and are not workspace members, so
                they may be vendored third-party code whose NOTICE needs relaying:
                {crates}

                Decide whether each one's NOTICE belongs in the wheel's NOTICE, then
                extend this check accordingly.
                """).format(crates="\n".join(sorted(local_non_members)))
        )

    # All non-workspace crates
    dependency_crates = sorted(
        (
            (
                entry["package"]["name"],
                entry["package"]["version"],
                Path(entry["package"]["manifest_path"]).parent,
            )
            for entry in about["crates"]
            if entry["package"]["name"] not in members
        ),
        key=lambda crate: (crate[0], crate[1]),
    )

    notice_hash_to_crate_label: dict[str, list[str]] = defaultdict(list)
    notice_hash_to_content: dict[str, str] = {}
    possible_unclassified_notice_files: list[str] = []
    for crate_name, crate_version, crate_dir in dependency_crates:
        for notice in sorted(crate_dir.glob("NOTICE*")):
            if not notice.is_file() or not RELAYED_NOTICE_FILENAME.match(notice.name):
                # Collected and raised below, so a human classifies it.
                possible_unclassified_notice_files.append(
                    f"  {crate_name} {crate_version}: {notice.name}"
                )
                continue
            raw_notice_bytes = notice.read_bytes()
            digest = hashlib.sha256(raw_notice_bytes).hexdigest()
            notice_hash_to_content[digest] = raw_notice_bytes.decode("utf-8")
            label = f"  * {crate_name} {crate_version}"
            if label not in notice_hash_to_crate_label.setdefault(digest, []):
                notice_hash_to_crate_label[digest].append(label)

    if possible_unclassified_notice_files:
        raise Failure(
            textwrap.dedent("""
                These crates ship a file or directory whose name starts with NOTICE but is
                not one this script relays:
                {files}

                Apache-2.0 section 4(d) attaches the relay obligation to a NOTICE text
                file, and ASF policy says not to add anything to NOTICE that is not
                legally required, so neither relaying nor skipping is safe to assume.
                Read the file, then widen RELAYED_NOTICE_FILENAME if it should be
                relayed, or skip it explicitly if it should not.
                """).format(files="\n".join(sorted(possible_unclassified_notice_files)))
        )

    return [
        (notice_hash_to_content[digest], sorted(labels))
        for digest, labels in notice_hash_to_crate_label.items()
    ]


def generate_notice(
    package_dir: Path, out_dir: Path, report: str, triples: tuple[str, ...]
) -> None:
    """Write the wheel's NOTICE: the project NOTICE plus relayed dependency notices."""
    base = REPO_ROOT / "NOTICE"
    if not base.is_file():
        raise Failure(f"Expected the project NOTICE at {base}, but it is missing.")

    notices = _relayable_notices(package_dir, triples)

    parts = [
        ensure_one_newline_termination(base.read_text(encoding="utf-8")),
        "\n",
        f"{NOTICE_RELAY_HEADING}\n",
        "-------------------------------------\n",
        "\n",
        "This section applies to the binary wheel built for\n",
        f"{report}, which statically links the dependencies below.\n",
        "Some dependencies may contain a NOTICE file of their own,\n",
        "which under some licenses must be redistributed alongside derivative work.\n",
    ]

    if notices:
        for text, labels in notices:
            parts += [
                "\n",
                "=" * 80 + "\n",
                "Applies to:\n",
                "".join(f"{label}\n" for label in labels),
                "-" * 80 + "\n",
                "\n",
                ensure_one_newline_termination(text),
            ]
    else:
        # Stated explicitly so an empty result reads as a fact about the graph.
        parts += [
            "No dependencies ship a NOTICE file, so there is nothing to relay.\n",
            f"All dependencies license texts are in {THIRD_PARTY_LICENSES_FILE}.\n",
        ]

    # Binary: text mode would translate "\n" to os.linesep, so the same input would
    # produce a different file per platform.
    (out_dir / "NOTICE").write_bytes("".join(parts).encode("utf-8"))


def generate_license(out_dir: Path, report: str, triples: tuple[str, ...]) -> None:
    """Write the wheel's LICENSE: the project LICENSE plus a pointer to the bundle."""
    base = REPO_ROOT / "LICENSE"
    if not base.is_file():
        raise Failure(f"Expected the project LICENSE at {base}, but it is missing.")

    base_text = ensure_one_newline_termination(base.read_text(encoding="utf-8"))
    appendix = LICENSE_APPENDIX_TEMPLATE.format(
        rule="=" * 79,
        heading=LICENSE_APPENDIX_HEADING,
        report=report,
        triples=", ".join(triples),
        bundle=THIRD_PARTY_LICENSES_FILE,
    )

    # Binary: text mode would translate "\n" to os.linesep, so the same input would
    # produce a different file per platform.
    with (out_dir / "LICENSE").open("wb") as handle:
        handle.write(base_text.encode("utf-8"))
        handle.write(appendix.encode("utf-8"))


def command_wheel_generate(args: argparse.Namespace) -> None:
    require_cargo_about()
    package_dir = args.package
    require_about_config(package_dir)
    licenses_dir = package_dir / LICENSES_DIR_NAME
    step(f"Generate per-wheel license files in {licenses_dir}")
    require_accepted_covers_deny_allow(package_dir)
    for maturin_target, rust_triples in WHEEL_LICENSE_REPORTS.items():
        print(f"    {maturin_target} ({' '.join(rust_triples)})", flush=True)
        out_dir = licenses_dir / maturin_target
        out_dir.mkdir(parents=True, exist_ok=True)
        generate_license_texts(package_dir, out_dir, maturin_target, rust_triples)
        generate_notice(package_dir, out_dir, maturin_target, rust_triples)
        generate_license(out_dir, maturin_target, rust_triples)
    ok(f"Generate per-wheel license files in {licenses_dir}")


def command_wheel_stage(args: argparse.Namespace) -> None:
    package_dir = args.package
    require_about_config(package_dir)
    wheel_name = args.maturin_target

    step(f"Stage {wheel_name} license files into {package_dir}")
    source = package_dir / LICENSES_DIR_NAME / wheel_name
    if not source.is_dir():
        raise Failure(f"No generated license report for '{wheel_name}' at {source}.")

    for name in REQUIRED_FILES:
        src = source / name
        if not src.is_file():
            raise Failure(f"Report '{wheel_name}' is missing {name}; regenerate it.")
        dest = package_dir / name
        # LICENSE and NOTICE are checked-in symlinks to the repository root. Unlink
        # first, so copying over them cannot rewrite the files they point at.
        dest.unlink(missing_ok=True)
        shutil.copyfile(src, dest)

    print(
        f"NOTE: staged {wheel_name} license files into {package_dir}.\n"
        "      LICENSE and NOTICE are checked-in symlinks; restore them with:\n"
        f"        git checkout -- {package_dir / 'LICENSE'} {package_dir / 'NOTICE'}",
        file=sys.stderr,
    )
    ok(f"Stage {wheel_name} license files into {package_dir}")


def check_wheel(path: Path, wheel_name: str) -> list[str]:
    """
    Assert one built wheel carries the license files it is required to carry.

    maturin does not error when a `license-files` pattern matches nothing and emits
    no warning either, so a missing generated file silently produces a wheel without
    the license texts of the dependencies it statically links, and a wheel built
    without staging silently carries only the project's own notice.
    (Verified against maturin 1.15.0.)
    """
    errors = []
    with zipfile.ZipFile(path) as wheel:
        contained_files = {name.rsplit("/", 1)[-1]: name for name in wheel.namelist()}

        for required in REQUIRED_FILES:
            if required not in contained_files:
                errors.append(f"{path.name}: missing {required}")
            elif wheel.getinfo(contained_files[required]).file_size == 0:
                errors.append(f"{path.name}: {required} is empty")

        third_party_license_file = contained_files.get(THIRD_PARTY_LICENSES_FILE)
        if third_party_license_file is not None:
            size = wheel.getinfo(third_party_license_file).file_size
            # Check against some minimum expected size, to flag if something doesn't look right.
            if 0 < size < MIN_BUNDLE_BYTES:
                errors.append(
                    f"{path.name}: {THIRD_PARTY_LICENSES_FILE} is only {size} bytes "
                    "which is smaller than the expected minimum size"
                )

        stage_hint = (
            "stage the generated files with "
            "'licenses.py wheel stage <package_dir> --maturin-target <target>'"
        )
        notice = contained_files.get("NOTICE")
        if notice is not None and NOTICE_RELAY_HEADING.encode() not in wheel.read(
            notice
        ):
            errors.append(
                f"{path.name}: NOTICE does not relay the notices of bundled "
                f"dependencies; {stage_hint}"
            )
        license_file = contained_files.get("LICENSE")
        if (
            license_file is not None
            and LICENSE_APPENDIX_HEADING.encode() not in wheel.read(license_file)
        ):
            errors.append(
                f"{path.name}: LICENSE does not point at {THIRD_PARTY_LICENSES_FILE}; "
                f"{stage_hint}"
            )

        # Whole-line matches. universal2-apple-darwin's files name aarch64-apple-darwin
        # as one of the two triples they cover, so a substring match would accept a
        # universal2 set as the aarch64 wheel's.
        for name, marker in (
            (THIRD_PARTY_LICENSES_FILE, f"Wheel:        {wheel_name}"),
            ("LICENSE", f"{wheel_name}, linking the Rust target triple(s)"),
        ):
            member = contained_files.get(name)
            if member is None:
                continue
            lines = wheel.read(member).decode("utf-8", errors="replace").splitlines()
            if marker not in lines:
                errors.append(
                    f"{path.name}: {name} was not generated for {wheel_name}; "
                    "this wheel carries another platform's license files"
                )
    return errors


def command_wheel_verify(args: argparse.Namespace) -> None:
    dist = Path(args.dist)
    wheels = sorted(dist.glob("*.whl"))
    if not wheels:
        raise Failure(f"No wheels found in {dist}")

    errors = []
    for wheel in wheels:
        errors.extend(check_wheel(wheel, args.maturin_target))

    if errors:
        raise Failure("\n".join(f"ERROR: {error}" for error in errors))

    for wheel in wheels:
        ok(f"{wheel.name} bundles {', '.join(REQUIRED_FILES)}")


def add_package_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "package",
        type=Path,
        metavar="PACKAGE_DIR",
        help="package directory holding about.toml, e.g. bindings/python",
    )


def add_maturin_target_argument(parser: argparse.ArgumentParser, help: str) -> None:
    # Required wherever it appears, and constrained to the known wheels. Which wheel a set
    # of license files belongs to is the whole point of generating them per wheel, so it is
    # never inferred; and `choices` turns a mistyped target into an argparse error rather
    # than a confusing "carries another platform's report" failure.
    parser.add_argument(
        "--maturin-target",
        required=True,
        choices=sorted(WHEEL_LICENSE_REPORTS),
        metavar="MATURIN_TARGET",
        help=f"{help}; one of: %(choices)s",
    )


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

    wheel = cmd_groups.add_parser(
        "wheel",
        help="license attribution carried inside the published pyiceberg-core wheels",
    )
    wheel_commands = wheel.add_subparsers(dest="command", required=True)

    generate = wheel_commands.add_parser(
        "generate",
        help=(
            "write one set of license files per published wheel into "
            f"PACKAGE_DIR/{LICENSES_DIR_NAME}/<maturin target>/"
        ),
    )
    add_package_argument(generate)
    generate.set_defaults(func=command_wheel_generate)

    stage = wheel_commands.add_parser(
        "stage",
        help="copy one wheel's generated license files into the package for maturin",
    )
    add_package_argument(stage)
    add_maturin_target_argument(stage, "which wheel's files to stage")
    stage.set_defaults(func=command_wheel_stage)

    verify = wheel_commands.add_parser(
        "verify",
        help="assert built wheels carry the required license files",
    )
    verify.add_argument("dist", help="directory containing built wheels")
    add_maturin_target_argument(
        verify, "assert the wheels carry this wheel's license files"
    )
    verify.set_defaults(func=command_wheel_verify)

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
