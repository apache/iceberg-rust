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

"""Set an explicit version in the Python binding's project metadata."""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
import tomllib
from pathlib import Path


def set_package_version(path: Path, version: str) -> None:
    text = path.read_text(encoding="utf-8")
    project_header = re.search(r"(?m)^\[project\]\s*$", text)
    if project_header is None:
        raise ValueError(f"{path} has no [project] table")

    project_end = re.search(r"(?m)^\[", text[project_header.end() :])
    end = (
        project_header.end() + project_end.start()
        if project_end is not None
        else len(text)
    )
    project = text[project_header.end() : end]

    dynamic_version = re.compile(r'(?m)^dynamic\s*=\s*\[\s*"version"\s*\]\s*$')
    replacement = f"dynamic = []\nversion = {json.dumps(version)}"
    updated_project, replacements = dynamic_version.subn(replacement, project)
    if replacements != 1:
        raise ValueError(
            f'{path} must declare exactly one dynamic = ["version"] in [project]'
        )

    updated = text[: project_header.end()] + updated_project + text[end:]
    parsed_project = tomllib.loads(updated)["project"]
    if parsed_project.get("version") != version or "version" in parsed_project.get(
        "dynamic", []
    ):
        raise ValueError(f"failed to set project.version in {path}")

    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}."
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as output:
            output.write(updated)
        os.chmod(temporary, path.stat().st_mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("version")
    args = parser.parse_args()

    set_package_version(args.path, args.version)
    print(f"Set {args.path} project.version to {args.version}")


if __name__ == "__main__":
    main()
