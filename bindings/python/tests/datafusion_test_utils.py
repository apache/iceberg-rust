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

from pathlib import Path
import re

from packaging.version import Version

_DEPENDENCIES_HEADER = "[dependencies]"
_DATAFUSION_FFI_DEPENDENCY = re.compile(
    r'^\s*datafusion-ffi\s*=\s*(?:"(?P<string_requirement>[^"]+)"|\{(?P<table_body>[^}]*)\})\s*$'
)
_VERSION_FIELD = re.compile(r'\bversion\s*=\s*"(?P<version>[^"]+)"')
_VERSION_TOKEN = re.compile(r"(?P<version>\d+(?:\.\d+){0,2})")
_BINDINGS_MANIFEST_PATH = Path(__file__).resolve().parents[1] / "Cargo.toml"


def _minimum_version_from_requirement(requirement: str) -> Version:
    version_match = _VERSION_TOKEN.search(requirement)
    if version_match is None:
        raise ValueError(
            f"Unable to infer minimum version from datafusion requirement: {requirement}"
        )

    parts = version_match.group("version").split(".")
    padded_parts = (parts + ["0", "0"])[:3]
    return Version(".".join(padded_parts))


def _datafusion_ffi_compatibility_floor(version: Version) -> Version:
    # Python datafusion wheels are published on major/minor lines and may lag rust patch bumps.
    # For FFI compatibility checks we gate on the same major line.
    return Version(f"{version.major}.0.0")


def minimum_datafusion_ffi_version_from_bindings_manifest(
    manifest_path: Path = _BINDINGS_MANIFEST_PATH,
) -> Version:
    in_dependencies = False
    for raw_line in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue

        if line.startswith("[") and line.endswith("]"):
            if line == _DEPENDENCIES_HEADER:
                in_dependencies = True
                continue

            if in_dependencies:
                break
            continue

        if not in_dependencies:
            continue

        dependency_match = _DATAFUSION_FFI_DEPENDENCY.match(line)
        if dependency_match is None:
            continue

        string_requirement = dependency_match.group("string_requirement")
        if string_requirement is not None:
            return _datafusion_ffi_compatibility_floor(
                _minimum_version_from_requirement(string_requirement)
            )

        table_body = dependency_match.group("table_body")
        if table_body is None:
            continue

        version_match = _VERSION_FIELD.search(table_body)
        if version_match is not None:
            return _datafusion_ffi_compatibility_floor(
                _minimum_version_from_requirement(version_match.group("version"))
            )

        raise ValueError("Found datafusion-ffi dependency without a version field")

    raise ValueError("Unable to find datafusion-ffi dependency in [dependencies]")
