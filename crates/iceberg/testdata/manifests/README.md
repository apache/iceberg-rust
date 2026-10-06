<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->


# Manifest test data

`repeated-decimal-type-definitions.avro` is a V2 data manifest written by
iceberg-rust at commit `ecba0b5`, which used `apache-avro` 0.21. Its table schema
has two optional `decimal(10, 2)` columns, `d1` (id 1) and `d2` (id 2), and its
partition spec has an identity partition on each. It holds one entry for
`s3://b/t/a.parquet` with partition values `123.45` and `-6.78`.

The Avro schema in its header defines the named fixed type `decimal_10_2` twice.
The Avro specification allows only one definition of each name, and
`apache-avro` 0.22 rejects the header, but iceberg-rust wrote manifests like
this one before it moved to 0.22. Tests use it to check that such manifests
still read.

To regenerate it, check out a commit that uses `apache-avro` 0.21, write the
manifest described above with `ManifestWriterBuilder::build_v2_data`, and copy
the file here.

`pyiceberg-v2-data.avro` is a V2 data manifest written by PyIceberg 0.12.0
(apache/iceberg-python commit `0d584073`) with deflate compression. Its table
schema has optional columns `id` (long), `category` (string), and `score`
(double), and its partition spec has an identity partition on `category`. It
holds two entries with column metrics. Its Avro schema is PyIceberg's, so tests
use it to check reading a manifest whose writer schema iceberg-rust didn't
generate.

To regenerate it, run this script with PyIceberg installed, passing the output
path:

```python
import sys

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus, write_manifest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "category", StringType(), required=False),
    NestedField(3, "score", DoubleType(), required=False),
)
SPEC = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category"), spec_id=0)


def entry(i: int) -> ManifestEntry:
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=f"s3://bucket/table/data/category=c{i}/{i:05d}.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(f"c{i}"),
        record_count=100 + i,
        file_size_in_bytes=1000 + i,
        column_sizes={1: 10 + i, 2: 20 + i, 3: 30 + i},
        value_counts={1: 100 + i, 2: 100 + i, 3: 100 + i},
        null_value_counts={1: 0, 2: i, 3: 1},
        nan_value_counts={3: i},
        lower_bounds={1: (i).to_bytes(8, "little"), 2: f"c{i}".encode()},
        upper_bounds={1: (i + 50).to_bytes(8, "little"), 2: f"c{i}".encode()},
        split_offsets=[4],
        sort_order_id=0,
    )
    return ManifestEntry.from_args(
        status=ManifestEntryStatus.ADDED, snapshot_id=7, sequence_number=1, file_sequence_number=1, data_file=data_file
    )


def main(path: str) -> None:
    with write_manifest(2, SPEC, SCHEMA, PyArrowFileIO().new_output(path), snapshot_id=7, avro_compression="deflate") as writer:
        for i in range(2):
            writer.add_entry(entry(i))


if __name__ == "__main__":
    main(sys.argv[1])
```
