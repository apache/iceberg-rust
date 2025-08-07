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


from tempfile import TemporaryDirectory
from typing import (
    Any,
    Dict,
    Generator,
)

import pytest
from pydantic_core import to_json

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    IntegerType,
    NestedField,
)


@pytest.fixture(scope="session")
def avro_schema_manifest_entry() -> Dict[str, Any]:
    return {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {
                "name": "snapshot_id",
                "type": ["null", "long"],
                "default": None,
                "field-id": 1,
            },
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "r2",
                    "fields": [
                        {
                            "name": "file_path",
                            "type": "string",
                            "doc": "Location URI with FS scheme",
                            "field-id": 100,
                        },
                        {
                            "name": "file_format",
                            "type": "string",
                            "doc": "File format name: avro, orc, or parquet",
                            "field-id": 101,
                        },
                        {
                            "name": "partition",
                            "type": {
                                "type": "record",
                                "name": "r102",
                                "fields": [
                                    {
                                        "field-id": 1000,
                                        "default": None,
                                        "name": "VendorID",
                                        "type": ["null", "int"],
                                    },
                                    {
                                        "field-id": 1001,
                                        "default": None,
                                        "name": "tpep_pickup_datetime",
                                        "type": [
                                            "null",
                                            {"type": "int", "logicalType": "date"},
                                        ],
                                    },
                                ],
                            },
                            "field-id": 102,
                        },
                        {
                            "name": "record_count",
                            "type": "long",
                            "doc": "Number of records in the file",
                            "field-id": 103,
                        },
                        {
                            "name": "file_size_in_bytes",
                            "type": "long",
                            "doc": "Total file size in bytes",
                            "field-id": 104,
                        },
                        {
                            "name": "block_size_in_bytes",
                            "type": "long",
                            "field-id": 105,
                        },
                        {
                            "name": "column_sizes",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k117_v118",
                                        "fields": [
                                            {
                                                "name": "key",
                                                "type": "int",
                                                "field-id": 117,
                                            },
                                            {
                                                "name": "value",
                                                "type": "long",
                                                "field-id": 118,
                                            },
                                        ],
                                    },
                                    "logicalType": "map",
                                },
                            ],
                            "doc": "Map of column id to total size on disk",
                            "default": None,
                            "field-id": 108,
                        },
                        {
                            "name": "value_counts",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k119_v120",
                                        "fields": [
                                            {
                                                "name": "key",
                                                "type": "int",
                                                "field-id": 119,
                                            },
                                            {
                                                "name": "value",
                                                "type": "long",
                                                "field-id": 120,
                                            },
                                        ],
                                    },
                                    "logicalType": "map",
                                },
                            ],
                            "doc": "Map of column id to total count, including null and NaN",
                            "default": None,
                            "field-id": 109,
                        },
                        {
                            "name": "null_value_counts",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k121_v122",
                                        "fields": [
                                            {
                                                "name": "key",
                                                "type": "int",
                                                "field-id": 121,
                                            },
                                            {
                                                "name": "value",
                                                "type": "long",
                                                "field-id": 122,
                                            },
                                        ],
                                    },
                                    "logicalType": "map",
                                },
                            ],
                            "doc": "Map of column id to null value count",
                            "default": None,
                            "field-id": 110,
                        },
                        {
                            "name": "nan_value_counts",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k138_v139",
                                        "fields": [
                                            {
                                                "name": "key",
                                                "type": "int",
                                                "field-id": 138,
                                            },
                                            {
                                                "name": "value",
                                                "type": "long",
                                                "field-id": 139,
                                            },
                                        ],
                                    },
                                    "logicalType": "map",
                                },
                            ],
                            "doc": "Map of column id to number of NaN values in the column",
                            "default": None,
                            "field-id": 137,
                        },
                        {
                            "name": "lower_bounds",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k126_v127",
                                        "fields": [
                                            {
                                                "name": "key",
                                                "type": "int",
                                                "field-id": 126,
                                            },
                                            {
                                                "name": "value",
                                                "type": "bytes",
                                                "field-id": 127,
                                            },
                                        ],
                                    },
                                    "logicalType": "map",
                                },
                            ],
                            "doc": "Map of column id to lower bound",
                            "default": None,
                            "field-id": 125,
                        },
                        {
                            "name": "upper_bounds",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k129_v130",
                                        "fields": [
                                            {
                                                "name": "key",
                                                "type": "int",
                                                "field-id": 129,
                                            },
                                            {
                                                "name": "value",
                                                "type": "bytes",
                                                "field-id": 130,
                                            },
                                        ],
                                    },
                                    "logicalType": "map",
                                },
                            ],
                            "doc": "Map of column id to upper bound",
                            "default": None,
                            "field-id": 128,
                        },
                        {
                            "name": "key_metadata",
                            "type": ["null", "bytes"],
                            "doc": "Encryption key metadata blob",
                            "default": None,
                            "field-id": 131,
                        },
                        {
                            "name": "split_offsets",
                            "type": [
                                "null",
                                {"type": "array", "items": "long", "element-id": 133},
                            ],
                            "doc": "Splittable offsets",
                            "default": None,
                            "field-id": 132,
                        },
                        {
                            "name": "sort_order_id",
                            "type": ["null", "int"],
                            "doc": "Sort order ID",
                            "default": None,
                            "field-id": 140,
                        },
                    ],
                },
                "field-id": 2,
            },
        ],
    }


manifest_entry_records = [
    {
        "status": 1,
        "snapshot_id": 8744736658442914487,
        "data_file": {
            "file_path": "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet",
            "file_format": "PARQUET",
            "partition": {"VendorID": 1, "tpep_pickup_datetime": 1925},
            "record_count": 19513,
            "file_size_in_bytes": 388872,
            "block_size_in_bytes": 67108864,
            "column_sizes": [
                {"key": 1, "value": 53},
                {"key": 2, "value": 98153},
                {"key": 3, "value": 98693},
                {"key": 4, "value": 53},
                {"key": 5, "value": 53},
                {"key": 6, "value": 53},
                {"key": 7, "value": 17425},
                {"key": 8, "value": 18528},
                {"key": 9, "value": 53},
                {"key": 10, "value": 44788},
                {"key": 11, "value": 35571},
                {"key": 12, "value": 53},
                {"key": 13, "value": 1243},
                {"key": 14, "value": 2355},
                {"key": 15, "value": 12750},
                {"key": 16, "value": 4029},
                {"key": 17, "value": 110},
                {"key": 18, "value": 47194},
                {"key": 19, "value": 2948},
            ],
            "value_counts": [
                {"key": 1, "value": 19513},
                {"key": 2, "value": 19513},
                {"key": 3, "value": 19513},
                {"key": 4, "value": 19513},
                {"key": 5, "value": 19513},
                {"key": 6, "value": 19513},
                {"key": 7, "value": 19513},
                {"key": 8, "value": 19513},
                {"key": 9, "value": 19513},
                {"key": 10, "value": 19513},
                {"key": 11, "value": 19513},
                {"key": 12, "value": 19513},
                {"key": 13, "value": 19513},
                {"key": 14, "value": 19513},
                {"key": 15, "value": 19513},
                {"key": 16, "value": 19513},
                {"key": 17, "value": 19513},
                {"key": 18, "value": 19513},
                {"key": 19, "value": 19513},
            ],
            "null_value_counts": [
                {"key": 1, "value": 19513},
                {"key": 2, "value": 0},
                {"key": 3, "value": 0},
                {"key": 4, "value": 19513},
                {"key": 5, "value": 19513},
                {"key": 6, "value": 19513},
                {"key": 7, "value": 0},
                {"key": 8, "value": 0},
                {"key": 9, "value": 19513},
                {"key": 10, "value": 0},
                {"key": 11, "value": 0},
                {"key": 12, "value": 19513},
                {"key": 13, "value": 0},
                {"key": 14, "value": 0},
                {"key": 15, "value": 0},
                {"key": 16, "value": 0},
                {"key": 17, "value": 0},
                {"key": 18, "value": 0},
                {"key": 19, "value": 0},
            ],
            "nan_value_counts": [
                {"key": 16, "value": 0},
                {"key": 17, "value": 0},
                {"key": 18, "value": 0},
                {"key": 19, "value": 0},
                {"key": 10, "value": 0},
                {"key": 11, "value": 0},
                {"key": 12, "value": 0},
                {"key": 13, "value": 0},
                {"key": 14, "value": 0},
                {"key": 15, "value": 0},
            ],
            "lower_bounds": [
                {"key": 2, "value": b"2020-04-01 00:00"},
                {"key": 3, "value": b"2020-04-01 00:12"},
                {"key": 7, "value": b"\x03\x00\x00\x00"},
                {"key": 8, "value": b"\x01\x00\x00\x00"},
                {"key": 10, "value": b"\xf6(\\\x8f\xc2\x05S\xc0"},
                {"key": 11, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 13, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 14, "value": b"\x00\x00\x00\x00\x00\x00\xe0\xbf"},
                {"key": 15, "value": b")\\\x8f\xc2\xf5(\x08\xc0"},
                {"key": 16, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 17, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 18, "value": b"\xf6(\\\x8f\xc2\xc5S\xc0"},
                {"key": 19, "value": b"\x00\x00\x00\x00\x00\x00\x04\xc0"},
            ],
            "upper_bounds": [
                {"key": 2, "value": b"2020-04-30 23:5:"},
                {"key": 3, "value": b"2020-05-01 00:41"},
                {"key": 7, "value": b"\t\x01\x00\x00"},
                {"key": 8, "value": b"\t\x01\x00\x00"},
                {"key": 10, "value": b"\xcd\xcc\xcc\xcc\xcc,_@"},
                {"key": 11, "value": b"\x1f\x85\xebQ\\\xe2\xfe@"},
                {"key": 13, "value": b"\x00\x00\x00\x00\x00\x00\x12@"},
                {"key": 14, "value": b"\x00\x00\x00\x00\x00\x00\xe0?"},
                {"key": 15, "value": b"q=\n\xd7\xa3\xf01@"},
                {"key": 16, "value": b"\x00\x00\x00\x00\x00`B@"},
                {"key": 17, "value": b"333333\xd3?"},
                {"key": 18, "value": b"\x00\x00\x00\x00\x00\x18b@"},
                {"key": 19, "value": b"\x00\x00\x00\x00\x00\x00\x04@"},
            ],
            "key_metadata": None,
            "split_offsets": [4],
            "sort_order_id": 0,
        },
    },
    {
        "status": 1,
        "snapshot_id": 8744736658442914487,
        "data_file": {
            "file_path": "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=1/00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00002.parquet",
            "file_format": "PARQUET",
            "partition": {"VendorID": 1, "tpep_pickup_datetime": None},
            "record_count": 95050,
            "file_size_in_bytes": 1265950,
            "block_size_in_bytes": 67108864,
            "column_sizes": [
                {"key": 1, "value": 318},
                {"key": 2, "value": 329806},
                {"key": 3, "value": 331632},
                {"key": 4, "value": 15343},
                {"key": 5, "value": 2351},
                {"key": 6, "value": 3389},
                {"key": 7, "value": 71269},
                {"key": 8, "value": 76429},
                {"key": 9, "value": 16383},
                {"key": 10, "value": 86992},
                {"key": 11, "value": 89608},
                {"key": 12, "value": 265},
                {"key": 13, "value": 19377},
                {"key": 14, "value": 1692},
                {"key": 15, "value": 76162},
                {"key": 16, "value": 4354},
                {"key": 17, "value": 759},
                {"key": 18, "value": 120650},
                {"key": 19, "value": 11804},
            ],
            "value_counts": [
                {"key": 1, "value": 95050},
                {"key": 2, "value": 95050},
                {"key": 3, "value": 95050},
                {"key": 4, "value": 95050},
                {"key": 5, "value": 95050},
                {"key": 6, "value": 95050},
                {"key": 7, "value": 95050},
                {"key": 8, "value": 95050},
                {"key": 9, "value": 95050},
                {"key": 10, "value": 95050},
                {"key": 11, "value": 95050},
                {"key": 12, "value": 95050},
                {"key": 13, "value": 95050},
                {"key": 14, "value": 95050},
                {"key": 15, "value": 95050},
                {"key": 16, "value": 95050},
                {"key": 17, "value": 95050},
                {"key": 18, "value": 95050},
                {"key": 19, "value": 95050},
            ],
            "null_value_counts": [
                {"key": 1, "value": 0},
                {"key": 2, "value": 0},
                {"key": 3, "value": 0},
                {"key": 4, "value": 0},
                {"key": 5, "value": 0},
                {"key": 6, "value": 0},
                {"key": 7, "value": 0},
                {"key": 8, "value": 0},
                {"key": 9, "value": 0},
                {"key": 10, "value": 0},
                {"key": 11, "value": 0},
                {"key": 12, "value": 95050},
                {"key": 13, "value": 0},
                {"key": 14, "value": 0},
                {"key": 15, "value": 0},
                {"key": 16, "value": 0},
                {"key": 17, "value": 0},
                {"key": 18, "value": 0},
                {"key": 19, "value": 0},
            ],
            "nan_value_counts": [
                {"key": 16, "value": 0},
                {"key": 17, "value": 0},
                {"key": 18, "value": 0},
                {"key": 19, "value": 0},
                {"key": 10, "value": 0},
                {"key": 11, "value": 0},
                {"key": 12, "value": 0},
                {"key": 13, "value": 0},
                {"key": 14, "value": 0},
                {"key": 15, "value": 0},
            ],
            "lower_bounds": [
                {"key": 1, "value": b"\x01\x00\x00\x00"},
                {"key": 2, "value": b"2020-04-01 00:00"},
                {"key": 3, "value": b"2020-04-01 00:03"},
                {"key": 4, "value": b"\x00\x00\x00\x00"},
                {"key": 5, "value": b"\x01\x00\x00\x00"},
                {"key": 6, "value": b"N"},
                {"key": 7, "value": b"\x01\x00\x00\x00"},
                {"key": 8, "value": b"\x01\x00\x00\x00"},
                {"key": 9, "value": b"\x01\x00\x00\x00"},
                {"key": 10, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 11, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 13, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 14, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 15, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 16, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 17, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 18, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
                {"key": 19, "value": b"\x00\x00\x00\x00\x00\x00\x00\x00"},
            ],
            "upper_bounds": [
                {"key": 1, "value": b"\x01\x00\x00\x00"},
                {"key": 2, "value": b"2020-04-30 23:5:"},
                {"key": 3, "value": b"2020-05-01 00:1:"},
                {"key": 4, "value": b"\x06\x00\x00\x00"},
                {"key": 5, "value": b"c\x00\x00\x00"},
                {"key": 6, "value": b"Y"},
                {"key": 7, "value": b"\t\x01\x00\x00"},
                {"key": 8, "value": b"\t\x01\x00\x00"},
                {"key": 9, "value": b"\x04\x00\x00\x00"},
                {"key": 10, "value": b"\\\x8f\xc2\xf5(8\x8c@"},
                {"key": 11, "value": b"\xcd\xcc\xcc\xcc\xcc,f@"},
                {"key": 13, "value": b"\x00\x00\x00\x00\x00\x00\x1c@"},
                {"key": 14, "value": b"\x9a\x99\x99\x99\x99\x99\xf1?"},
                {"key": 15, "value": b"\x00\x00\x00\x00\x00\x00Y@"},
                {"key": 16, "value": b"\x00\x00\x00\x00\x00\xb0X@"},
                {"key": 17, "value": b"333333\xd3?"},
                {"key": 18, "value": b"\xc3\xf5(\\\x8f:\x8c@"},
                {"key": 19, "value": b"\x00\x00\x00\x00\x00\x00\x04@"},
            ],
            "key_metadata": None,
            "split_offsets": [4],
            "sort_order_id": 0,
        },
    },
]


@pytest.fixture(scope="session")
def test_schema() -> Schema:
    return Schema(
        NestedField(1, "VendorID", IntegerType(), False),
        NestedField(2, "tpep_pickup_datetime", IntegerType(), False),
    )


@pytest.fixture(scope="session")
def test_partition_spec() -> Schema:
    return PartitionSpec(
        PartitionField(1, 1000, IdentityTransform(), "VendorID"),
        PartitionField(2, 1001, IdentityTransform(), "tpep_pickup_datetime"),
    )


@pytest.fixture(scope="session")
def generated_manifest_entry_file(
    avro_schema_manifest_entry: Dict[str, Any],
    test_schema: Schema,
    test_partition_spec: PartitionSpec,
) -> Generator[str, None, None]:
    from fastavro import parse_schema, writer

    parsed_schema = parse_schema(avro_schema_manifest_entry)

    with TemporaryDirectory() as tmpdir:
        tmp_avro_file = tmpdir + "/manifest.avro"
        with open(tmp_avro_file, "wb") as out:
            writer(
                out,
                parsed_schema,
                manifest_entry_records,
                metadata={
                    "schema": test_schema.model_dump_json(),
                    "partition-spec": to_json(test_partition_spec.fields).decode(
                        "utf-8"
                    ),
                },
            )
        yield tmp_avro_file
