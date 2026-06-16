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

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CARGO_TOML = REPO_ROOT / "Cargo.toml"
TOOLCHAIN_TOML = REPO_ROOT / "rust-toolchain.toml"
MIN_AGE_DAYS = 90
GH_API_PAGE_SIZE = 100


def get_rust_releases() -> Iterable[dict]:
    """
    Query GitHub for Rust releases, yielding each and automatically iterating over API pages.
    """
    page = 1
    while True:
        url = f"https://api.github.com/repos/rust-lang/rust/releases?per_page={GH_API_PAGE_SIZE}&page={page}"
        headers = {"Accept": "application/vnd.github+json"}
        if token := os.environ.get("GITHUB_TOKEN"):
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req) as resp:
                batch = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 403:
                sys.exit("ERROR: GitHub API rate limit exceeded. Try again later or set a GITHUB_TOKEN env var.")
            raise
        yield from batch
        if len(batch) < GH_API_PAGE_SIZE:
            # Page size smaller than requested indicates last page.
            break
        page += 1


def find_eligible_version(releases: Iterable[dict], min_age_days: int = MIN_AGE_DAYS) -> tuple[str, str]:
    """
    Find first eligible Rust version meeting min_age_days criteria.

    Assumes the releases are sorted from newest to oldest.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=min_age_days)
    minor_version_pattern = re.compile(r"^\d+\.\d+\.\d+$")

    for release in releases:
        tag = release["tag_name"]
        if not minor_version_pattern.match(tag):
            continue
        published = datetime.fromisoformat(release["published_at"])
        if published <= cutoff:
            version = tag.rsplit(".", 1)[0]  # "1.94.0" -> "1.94"
            return version, release["published_at"][:10]

    print("ERROR: No Rust stable release found that is >=90 days old", file=sys.stderr)
    sys.exit(1)


def update_cargo_toml(version: str) -> bool:
    """
    Update `Cargo.toml`, returning boolean indicating if the file was changed.
    """
    content = CARGO_TOML.read_text()
    new_content = re.sub(
        r'^rust-version = ".*"',
        f'rust-version = "{version}"',
        content,
        count=1,
        flags=re.MULTILINE,
    )
    if content == new_content:
        return False
    CARGO_TOML.write_text(new_content)
    return True


def update_toolchain_toml(release_date: str) -> bool:
    """
    Update `rust-toolchain.toml`, returning boolean indicating if the file was changed.
    """
    content = TOOLCHAIN_TOML.read_text()
    new_content = re.sub(
        r'^channel = ".*"',
        f'channel = "nightly-{release_date}"',
        content,
        count=1,
        flags=re.MULTILINE,
    )
    if content == new_content:
        return False
    TOOLCHAIN_TOML.write_text(new_content)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bump MSRV to the newest Rust stable meeting the minimum age requirement.",
    )
    parser.add_argument(
        "--min-age-days",
        type=int,
        default=MIN_AGE_DAYS,
        help=f"Minimum age in days for a release to be eligible (default: {MIN_AGE_DAYS})",
    )
    args = parser.parse_args()

    print("Fetching Rust releases...")
    releases = get_rust_releases()

    version, release_date = find_eligible_version(releases, args.min_age_days)
    print(f"Newest stable ≥{args.min_age_days} days old: {version} (released {release_date})")

    cargo_changed = update_cargo_toml(version)
    toolchain_changed = update_toolchain_toml(release_date)

    if not cargo_changed and not toolchain_changed:
        print("Already up to date — nothing to change.")
        return

    if cargo_changed:
        print(f"  Updated {CARGO_TOML.relative_to(REPO_ROOT)}: rust-version = \"{version}\"")
    if toolchain_changed:
        print(f"  Updated {TOOLCHAIN_TOML.relative_to(REPO_ROOT)}: channel = \"nightly-{release_date}\"")

    print("\nDone. Run `make check-clippy` to verify the new nightly doesn't introduce lint failures.")


if __name__ == "__main__":
    main()
