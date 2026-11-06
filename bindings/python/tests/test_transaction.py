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


import json
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg_core import transaction


def _metadata(location: str, partitioned: bool = False) -> str:
    fields = [
        {"id": 1, "name": "id", "required": False, "type": "long"},
        {"id": 2, "name": "name", "required": False, "type": "string"},
    ]
    spec = (
        [{"name": "id", "transform": "identity", "source-id": 1, "field-id": 1000}]
        if partitioned
        else []
    )
    return json.dumps(
        {
            "format-version": 3,
            "table-uuid": str(uuid.uuid4()),
            "location": location,
            "last-sequence-number": 0,
            "last-updated-ms": 1700000000000,
            "last-column-id": 2,
            "current-schema-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": fields}],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": spec}],
            "last-partition-id": 1000 if partitioned else 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "next-row-id": 0,
        }
    )


def _parquet(path: Path, ids: list[int]) -> str:
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), metadata={"PARQUET:field_id": "1"}),
            pa.field("name", pa.string(), metadata={"PARQUET:field_id": "2"}),
        ]
    )
    pq.write_table(
        pa.table({"id": ids, "name": [f"n{i}" for i in ids]}, schema=schema), path
    )
    return f"file://{path}"


def test_add_files_returns_a_v3_append_commit(tmp_path: Path) -> None:
    (tmp_path / "data").mkdir()
    (tmp_path / "metadata").mkdir()
    files = [
        _parquet(tmp_path / "data" / "a.parquet", [1, 2, 3]),
        _parquet(tmp_path / "data" / "b.parquet", [10, 20]),
    ]

    body = json.loads(
        transaction.add_files(
            ["ns", "t"],
            f"file://{tmp_path}/metadata/v1.metadata.json",
            _metadata(f"file://{tmp_path}"),
            files,
            {},
            {"origin": "test"},
        )
    )

    assert [r["type"] for r in body["requirements"]] == [
        "assert-table-uuid",
        "assert-ref-snapshot-id",
    ]
    add, set_ref = body["updates"]
    assert add["action"] == "add-snapshot" and set_ref["action"] == "set-snapshot-ref"
    snapshot = add["snapshot"]
    assert snapshot["first-row-id"] == 0
    assert snapshot["added-rows"] == 5
    assert snapshot["summary"]["added-data-files"] == "2"
    assert snapshot["summary"]["origin"] == "test"
    assert set_ref["snapshot-id"] == snapshot["snapshot-id"]
    # the manifest list is written; nothing else is committed
    assert Path(snapshot["manifest-list"].removeprefix("file://")).is_file()


def test_add_files_refuses_a_partitioned_table(tmp_path: Path) -> None:
    path = _parquet(tmp_path / "a.parquet", [1])
    with pytest.raises(ValueError, match="unpartitioned"):
        transaction.add_files(
            ["ns", "t"],
            f"file://{tmp_path}/m.json",
            _metadata(f"file://{tmp_path}", True),
            [path],
            {},
        )
