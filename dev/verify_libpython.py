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

"""Verify that the mise-managed Python installation provides libpython."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import sysconfig


def fail(message: str) -> None:
    print(f"libpython verification failed: {message}", file=sys.stderr)
    raise SystemExit(1)


def normalized(path: Path) -> str:
    return os.path.normcase(str(path.resolve()))


def mise_python_prefix() -> Path:
    try:
        result = subprocess.run(
            ["mise", "where", "python"],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        fail("mise is not available on PATH")
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or error.stdout.strip() or str(error)
        fail(f"could not locate mise Python: {detail}")

    return Path(result.stdout.strip())


def require_file(description: str, candidates: list[Path]) -> Path:
    for candidate in candidates:
        if candidate.is_file():
            return candidate

    searched = "\n  ".join(str(candidate) for candidate in candidates)
    fail(f"{description} was not found; searched:\n  {searched}")


def verify_windows(prefix: Path) -> list[Path]:
    version = f"{sys.version_info.major}{sys.version_info.minor}"
    dll_name = sysconfig.get_config_var("LDLIBRARY") or f"python{version}.dll"
    if not dll_name.lower().endswith(".dll"):
        dll_name = f"python{version}.dll"

    dll = require_file(
        "the Python DLL",
        [
            prefix / dll_name,
            prefix / "DLLs" / dll_name,
            Path(sys.executable).parent / dll_name,
        ],
    )

    import_name = sysconfig.get_config_var("LIBRARY") or f"python{version}.lib"
    if not import_name.lower().endswith(".lib"):
        import_name = f"python{version}.lib"
    libdir = sysconfig.get_config_var("LIBDIR")
    import_candidates = [prefix / "libs" / import_name, prefix / "Libs" / import_name]
    if libdir:
        import_candidates.insert(0, Path(libdir) / import_name)
    import_library = require_file("the Python import library", import_candidates)

    return [dll, import_library]


def verify_unix(prefix: Path) -> list[Path]:
    library_name = sysconfig.get_config_var("LDLIBRARY")
    is_shared = library_name and (
        library_name.endswith((".so", ".dylib")) or ".so." in library_name
    )
    if not is_shared:
        fail(f"LDLIBRARY does not identify a shared library: {library_name!r}")

    libdir = sysconfig.get_config_var("LIBDIR")
    candidates = [prefix / "lib" / library_name]
    if libdir:
        candidates.insert(0, Path(libdir) / library_name)
    return [require_file("the shared Python library", candidates)]


def main() -> None:
    prefix = mise_python_prefix()
    base_prefix = Path(sys.base_prefix)
    if normalized(base_prefix) != normalized(prefix):
        fail(
            "the active interpreter is not the configured mise Python "
            f"(active: {base_prefix}; mise: {prefix})"
        )

    # Windows sysconfig does not expose Py_ENABLE_SHARED even though the
    # standard CPython layout is DLL-based. The DLL and import-library checks
    # below are the authoritative equivalent on that platform.
    if os.name != "nt" and sysconfig.get_config_var("Py_ENABLE_SHARED") != 1:
        fail("Py_ENABLE_SHARED is not enabled")

    libraries = verify_windows(prefix) if os.name == "nt" else verify_unix(prefix)
    print(f"Verified mise Python {sys.version.split()[0]} at {prefix}")
    for library in libraries:
        print(f"Verified {library}")


if __name__ == "__main__":
    main()
