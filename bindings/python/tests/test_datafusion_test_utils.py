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

from packaging.version import Version

import pytest

from datafusion_test_utils import (
    _minimum_version_from_requirement,
    minimum_datafusion_ffi_version_from_bindings_manifest,
)


def test_minimum_version_from_requirement_supports_major_only() -> None:
    assert _minimum_version_from_requirement("52") == Version("52.0.0")


def test_minimum_version_from_requirement_supports_semver_operators() -> None:
    assert _minimum_version_from_requirement("^52.1") == Version("52.1.0")
    assert _minimum_version_from_requirement("~52.1.2") == Version("52.1.2")
    assert _minimum_version_from_requirement(">=52.1.2, <53.0.0") == Version("52.1.2")


def test_minimum_datafusion_ffi_version_from_bindings_manifest_reads_string_dependency(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "Cargo.toml"
    manifest_path.write_text(
        """
[dependencies]
datafusion-ffi = "52.0"
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    assert minimum_datafusion_ffi_version_from_bindings_manifest(
        manifest_path
    ) == Version("52.0.0")


def test_minimum_datafusion_ffi_version_from_bindings_manifest_reads_table_dependency(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "Cargo.toml"
    manifest_path.write_text(
        """
[dependencies]
datafusion-ffi = { version = "^52.1", features = ["foo"] }
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    assert minimum_datafusion_ffi_version_from_bindings_manifest(
        manifest_path
    ) == Version("52.0.0")


def test_minimum_version_from_requirement_raises_on_invalid_requirement() -> None:
    with pytest.raises(ValueError, match="Unable to infer minimum version"):
        _minimum_version_from_requirement("workspace = true")


def test_minimum_datafusion_ffi_version_from_bindings_manifest_raises_when_missing_dependency(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "Cargo.toml"
    manifest_path.write_text(
        """
[dependencies]
iceberg = { path = "../../crates/iceberg" }
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Unable to find datafusion-ffi dependency in \\[dependencies\\]",
    ):
        minimum_datafusion_ffi_version_from_bindings_manifest(manifest_path)


def test_minimum_datafusion_ffi_version_from_bindings_manifest_raises_when_missing_version_field(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "Cargo.toml"
    manifest_path.write_text(
        """
[dependencies]
datafusion-ffi = { default-features = false }
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError, match="Found datafusion-ffi dependency without a version field"
    ):
        minimum_datafusion_ffi_version_from_bindings_manifest(manifest_path)
