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

import struct
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    _manifests,
    read_manifest_list,
    write_manifest,
    write_manifest_list,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType


@pytest.fixture(autouse=True)
def clear_global_manifests_cache() -> None:
    # Clear the global cache before each test
    cache_clear = getattr(_manifests, "cache_clear", None)
    if callable(cache_clear):
        # pyiceberg<=0.10: _manifests was lru_cache-decorated.
        cache_clear()
        return

    # pyiceberg>=0.11: manifests are cached in module-level LRUCache.
    from pyiceberg import manifest as manifest_module

    manifest_cache = getattr(manifest_module, "_manifest_cache", None)
    manifest_cache_lock = getattr(manifest_module, "_manifest_cache_lock", None)

    if manifest_cache is None:
        return

    if manifest_cache_lock is None:
        manifest_cache.clear()
        return

    with manifest_cache_lock:
        manifest_cache.clear()


def fetch_manifest_entry(
    manifest_entry_file: str, io: FileIO, discard_deleted: bool = True
) -> list[ManifestEntry]:
    from pyiceberg_core import manifest

    bs = io.new_input(manifest_entry_file).open().read()
    manifest = manifest.read_manifest_entries(bs)

    # TODO: Don't convert the types
    # but this is the easiest for now until we
    # have the write part in there as well
    def _convert_entry(entry: Any) -> ManifestEntry:
        data_file_args = {
            "content": DataFileContent(entry.data_file.content),
            "file_path": entry.data_file.file_path,
            "file_format": FileFormat(entry.data_file.file_format),
            "partition": [
                p.value() if p is not None else None for p in entry.data_file.partition
            ],
            "record_count": entry.data_file.record_count,
            "file_size_in_bytes": entry.data_file.file_size_in_bytes,
            "column_sizes": entry.data_file.column_sizes,
            "value_counts": entry.data_file.value_counts,
            "null_value_counts": entry.data_file.null_value_counts,
            "nan_value_counts": entry.data_file.nan_value_counts,
            "lower_bounds": entry.data_file.lower_bounds,
            "upper_bounds": entry.data_file.upper_bounds,
            "key_metadata": entry.data_file.key_metadata,
            "split_offsets": entry.data_file.split_offsets,
            "equality_ids": entry.data_file.equality_ids,
            "sort_order_id": entry.data_file.sort_order_id,
        }
        data_file = (
            DataFile.from_args(**data_file_args)
            if hasattr(DataFile, "from_args")
            else DataFile(**data_file_args)
        )

        manifest_entry_args = {
            "status": ManifestEntryStatus(entry.status),
            "snapshot_id": entry.snapshot_id,
            "sequence_number": entry.sequence_number,
            "file_sequence_number": entry.file_sequence_number,
            "data_file": data_file,
        }
        return (
            ManifestEntry.from_args(**manifest_entry_args)
            if hasattr(ManifestEntry, "from_args")
            else ManifestEntry(**manifest_entry_args)
        )

    return [
        _convert_entry(entry)
        # _inherit_from_manifest(, self)
        for entry in manifest.entries()
        if not discard_deleted or entry.status != ManifestEntryStatus.DELETED
    ]


def test_read_manifest_entry(generated_manifest_entry_file: str) -> None:
    # manifest = ManifestFile.from_args(
    #     manifest_path=generated_manifest_entry_file,
    #     manifest_length=0,
    #     partition_spec_id=0,
    #     added_snapshot_id=0,
    #     sequence_number=0,
    #     partitions=[],
    # )
    # manifest_entries = manifest.fetch_manifest_entry(PyArrowFileIO())
    manifest_entries = fetch_manifest_entry(
        f"file://{generated_manifest_entry_file}", PyArrowFileIO()
    )
    manifest_entry = manifest_entries[0]

    assert manifest_entry.status == ManifestEntryStatus.ADDED
    assert manifest_entry.snapshot_id == 8744736658442914487
    assert manifest_entry.sequence_number == 0
    assert isinstance(manifest_entry.data_file, DataFile)

    data_file = manifest_entry.data_file

    assert data_file.content == DataFileContent.DATA
    assert (
        data_file.file_path
        == "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet"
    )
    assert data_file.file_format == FileFormat.PARQUET
    assert repr(data_file.partition) == "[1, None]"
    assert data_file.record_count == 19513
    assert data_file.file_size_in_bytes == 388872
    assert data_file.column_sizes == {
        1: 53,
        2: 98153,
    }
    assert data_file.value_counts == {
        1: 19513,
        2: 19513,
    }
    assert data_file.null_value_counts == {
        1: 19513,
        2: 0,
    }
    assert data_file.nan_value_counts == {}
    assert data_file.lower_bounds == {
        1: b"\x00\x00\x00\x00",
        2: b"\x00\x00\x00\x00\x00\x00\x00\x00",
    }
    assert data_file.upper_bounds == {
        1: b"\x00\x00\xe0\xbf",
        2: b"\x00\x00\x00\x00\x00\x00\xe0\xbf",
    }
    assert data_file.key_metadata is None
    assert data_file.split_offsets == [4]
    assert data_file.equality_ids is None
    assert data_file.sort_order_id == 0


@pytest.mark.parametrize(
    "payload", [b"", b"not-an-avro-file", b"Obj\x01truncated-after-the-avro-magic"]
)
def test_read_manifest_entries_raises_on_invalid_avro(payload: bytes) -> None:
    from pyiceberg_core import manifest

    with pytest.raises(ValueError):
        manifest.read_manifest_entries(payload)


@pytest.mark.parametrize(
    "payload", [b"", b"not-an-avro-file", b"Obj\x01truncated-after-the-avro-magic"]
)
def test_read_manifest_list_raises_on_invalid_avro(payload: bytes) -> None:
    from pyiceberg_core import manifest

    with pytest.raises(ValueError):
        manifest.read_manifest_list(payload)


def test_manifest_file_without_partition_summaries(tmp_path: Path) -> None:
    """Field summaries are optional, so a manifest list without them reads back as None."""
    from pyiceberg_core import manifest

    manifest_list_file = str(tmp_path / "snap.avro")
    io = PyArrowFileIO()

    with write_manifest_list(
        format_version=2,
        output_file=io.new_output(manifest_list_file),
        snapshot_id=25,
        parent_snapshot_id=None,
        sequence_number=1,
        avro_compression="null",
    ) as writer:
        writer.add_manifests(
            [
                ManifestFile.from_args(
                    manifest_path="s3://bucket/metadata/manifest.avro",
                    manifest_length=1024,
                    partition_spec_id=0,
                    added_snapshot_id=25,
                    sequence_number=1,
                    min_sequence_number=1,
                    added_files_count=1,
                    existing_files_count=0,
                    deleted_files_count=0,
                    added_rows_count=10,
                    existing_rows_count=0,
                    deleted_rows_count=0,
                    partitions=None,
                )
            ]
        )

    bs = io.new_input(manifest_list_file).open().read()
    manifest_file = manifest.read_manifest_list(bs).entries()[0]

    assert manifest_file.partitions is None


SNAPSHOT_ID = 25


def _write_manifest(tmp_path: Path, num_files: int) -> tuple[bytes, ManifestFile]:
    """Writes a v2 manifest whose entries inherit their sequence numbers."""
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "name", StringType(), required=False),
    )
    spec = PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=IdentityTransform(), name="id"
        ),
        spec_id=0,
    )
    path = str(tmp_path / "manifest.avro")
    io = PyArrowFileIO()
    with write_manifest(
        format_version=2,
        spec=spec,
        schema=schema,
        output_file=io.new_output(path),
        snapshot_id=SNAPSHOT_ID,
        avro_compression="deflate",
    ) as writer:
        for i in range(num_files):
            writer.add(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.ADDED,
                    snapshot_id=SNAPSHOT_ID,
                    data_file=DataFile.from_args(
                        content=DataFileContent.DATA,
                        file_path=f"s3://bucket/data/{i}.parquet",
                        file_format=FileFormat.PARQUET,
                        partition=Record(i),
                        record_count=10 + i,
                        file_size_in_bytes=100,
                        column_sizes={1: 8, 2: 16},
                        value_counts={1: 10 + i, 2: 10 + i},
                        null_value_counts={1: 0, 2: i},
                        nan_value_counts={},
                        lower_bounds={1: struct.pack("<q", i)},
                        upper_bounds={1: struct.pack("<q", i)},
                        split_offsets=[4],
                        sort_order_id=0,
                    ),
                )
            )
    manifest_file = ManifestFile.from_args(
        manifest_path=path,
        manifest_length=writer.to_manifest_file().manifest_length,
        partition_spec_id=0,
        content=ManifestContent.DATA,
        sequence_number=5,
        min_sequence_number=5,
        added_snapshot_id=SNAPSHOT_ID,
    )
    return io.new_input(path).open().read(), manifest_file


def _read_entries(
    bs: bytes, manifest_file: ManifestFile, first_row_id: int | None = None
) -> pa.RecordBatchReader:
    from pyiceberg_core import manifest

    return manifest.read_manifest_entries_as_arrow(
        bs,
        manifest_path=manifest_file.manifest_path,
        manifest_length=manifest_file.manifest_length,
        partition_spec_id=manifest_file.partition_spec_id,
        content=manifest_file.content.value,
        sequence_number=manifest_file.sequence_number,
        min_sequence_number=manifest_file.min_sequence_number,
        added_snapshot_id=manifest_file.added_snapshot_id,
        first_row_id=first_row_id,
    )


def test_read_manifest_entries_as_arrow_matches_pyiceberg(tmp_path: Path) -> None:
    bs, manifest_file = _write_manifest(tmp_path, 3)
    reader = _read_entries(bs, manifest_file)
    assert isinstance(reader, pa.RecordBatchReader)

    rows = reader.read_all().to_pylist()
    expected = manifest_file.fetch_manifest_entry(
        PyArrowFileIO(), discard_deleted=False
    )
    assert [
        {
            "status": row["status"],
            "snapshot_id": row["snapshot_id"],
            "sequence_number": row["sequence_number"],
            "file_sequence_number": row["file_sequence_number"],
            "file_path": row["data_file"]["file_path"],
            "partition": row["data_file"]["partition"]["id"],
            "record_count": row["data_file"]["record_count"],
            "null_value_counts": dict(row["data_file"]["null_value_counts"]),
            "lower_bounds": dict(row["data_file"]["lower_bounds"]),
        }
        for row in rows
    ] == [
        {
            "status": entry.status.value,
            "snapshot_id": entry.snapshot_id,
            "sequence_number": entry.sequence_number,
            "file_sequence_number": entry.file_sequence_number,
            "file_path": entry.data_file.file_path,
            "partition": entry.data_file.partition[0],
            "record_count": entry.data_file.record_count,
            "null_value_counts": entry.data_file.null_value_counts,
            "lower_bounds": entry.data_file.lower_bounds,
        }
        for entry in expected
    ]


def test_read_manifest_entries_as_arrow_yields_batches_of_1024_rows(
    tmp_path: Path,
) -> None:
    bs, manifest_file = _write_manifest(tmp_path, 2500)
    assert [batch.num_rows for batch in _read_entries(bs, manifest_file)] == [
        1024,
        1024,
        452,
    ]


def test_read_manifest_entries_as_arrow_releases_input_on_close(
    tmp_path: Path,
) -> None:
    from pyiceberg_core import transform

    bs, manifest_file = _write_manifest(tmp_path, 3)
    refcount = sys.getrefcount(bs)
    reader = _read_entries(bs, manifest_file)
    assert sys.getrefcount(bs) == refcount + 1

    # pyarrow releases the stream without PyO3 attached to the interpreter, so
    # PyO3 defers the decref to its next call.
    reader.close()
    transform.identity(pa.array([1]))
    assert sys.getrefcount(bs) == refcount


def test_read_manifest_entries_as_arrow_batches_outlive_reader(
    tmp_path: Path,
) -> None:
    bs, manifest_file = _write_manifest(tmp_path, 3)
    reader = _read_entries(bs, manifest_file)
    batch = reader.read_next_batch()
    reader.close()
    del reader, bs

    assert batch.column("data_file").field("file_path").to_pylist() == [
        f"s3://bucket/data/{i}.parquet" for i in range(3)
    ]


def test_read_manifest_entries_as_arrow_raises_while_reading(tmp_path: Path) -> None:
    bs, manifest_file = _write_manifest(tmp_path, 3)
    # Assigning first row IDs to the second entry overflows i64.
    reader = _read_entries(bs, manifest_file, first_row_id=2**63 - 15)

    with pytest.raises(pa.ArrowInvalid, match="row ID overflow"):
        reader.read_next_batch()


@pytest.mark.parametrize(
    "payload", [b"", b"not-an-avro-file", b"Obj\x01truncated-after-the-avro-magic"]
)
def test_read_manifest_entries_as_arrow_raises_on_invalid_avro(
    tmp_path: Path, payload: bytes
) -> None:
    _, manifest_file = _write_manifest(tmp_path, 1)
    with pytest.raises(ValueError):
        _read_entries(payload, manifest_file)


@pytest.mark.parametrize("format_version", [1, 2])
def test_read_manifest_list_as_arrow_matches_pyiceberg(
    tmp_path: Path, format_version: int
) -> None:
    from pyiceberg_core import manifest

    path = str(tmp_path / "snap.avro")
    io = PyArrowFileIO()
    with write_manifest_list(
        format_version=format_version,
        output_file=io.new_output(path),
        snapshot_id=SNAPSHOT_ID,
        parent_snapshot_id=None,
        sequence_number=7,
        avro_compression="deflate",
    ) as writer:
        writer.add_manifests(
            [
                ManifestFile.from_args(
                    manifest_path=f"s3://bucket/metadata/{i}.avro",
                    manifest_length=1024,
                    partition_spec_id=0,
                    added_snapshot_id=SNAPSHOT_ID,
                    sequence_number=-1,
                    min_sequence_number=-1,
                    added_files_count=i,
                    existing_files_count=0,
                    deleted_files_count=0,
                    added_rows_count=10 * i,
                    existing_rows_count=0,
                    deleted_rows_count=0,
                )
                for i in range(2)
            ]
        )

    reader = manifest.read_manifest_list_as_arrow(io.new_input(path).open().read())
    assert isinstance(reader, pa.RecordBatchReader)
    rows = reader.read_all().to_pylist()
    expected = list(read_manifest_list(io.new_input(path)))
    fields = [
        "manifest_path",
        "sequence_number",
        "min_sequence_number",
        "added_snapshot_id",
        "added_files_count",
        "added_rows_count",
    ]
    assert [{f: row[f] for f in fields} for row in rows] == [
        {f: getattr(manifest_file, f) for f in fields} for manifest_file in expected
    ]


@pytest.mark.parametrize(
    "payload", [b"", b"not-an-avro-file", b"Obj\x01truncated-after-the-avro-magic"]
)
def test_read_manifest_list_as_arrow_raises_on_invalid_avro(payload: bytes) -> None:
    from pyiceberg_core import manifest

    with pytest.raises(ValueError):
        manifest.read_manifest_list_as_arrow(payload)
