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

import os
from pyiceberg.catalog import load_catalog
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta

# Generate a table with various types in memory and dump to a Parquet file
rows = 1001
columns = [
    pa.array([(i % 2 == 1) for i in range(rows)]),
    pa.array([(i % 256 - 128) for i in range(rows)]),
    pa.array([i for i in range(rows)]),
    pa.array([i for i in range(rows)]),
    pa.array([i for i in range(rows)]),
    pa.array([float(i) for i in range(rows)]),
    pa.array([float(i) for i in range(rows)]),
    pa.array([round(i / 100, 2) for i in range(rows)]),
    pa.array([(datetime(1970, 1, 1) + timedelta(days=i)).date() for i in range(rows)]),
    pa.array([(datetime(1970, 1, 1) + timedelta(seconds=i)) for i in range(rows)]),
    pa.array([(datetime(1970, 1, 1) + timedelta(seconds=i)) for i in range(rows)]),
    pa.array([str(i) for i in range(rows)]),
    pa.array([str(i).encode("utf-8") for i in range(rows)]),
]
schema = pa.schema([
    ('cboolean', pa.bool_()),
    ('cint8', pa.int8()),
    ('cint16', pa.int16()),
    ('cint32', pa.int32()),
    ('cint64', pa.int64()),
    ('cfloat32', pa.float32()),
    ('cfloat64', pa.float64()),
    ('cdecimal128', pa.decimal128(8, 2)),
    ('cdate32', pa.date32()),
    ('ctimestamp', pa.timestamp('us')),
    ('ctimestamptz', pa.timestamp('us', tz='UTC')),
    ('cutf8', pa.utf8()),
    ('cbinary', pa.binary()),
])

# Convert to a PyArrow table
table = pa.Table.from_arrays(columns, schema=schema)

# Write to a Parquet file
pq.write_table(table, "types_test.parquet")

# Output the result
print(f"Created a Parquet file with {rows} rows and schema {table.schema}.")


# Load the Parquet file
parquet_file = pq.read_table("./types_test.parquet")

# Connect to the REST catalog
catalog = load_catalog(
    "rest",
    **{
        "type": "rest",
        "uri": "http://rest:8181",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
    },
)

# Create a corresponding Iceberg table and append the file to it
iceberg_table = catalog.create_table_if_not_exists(
    identifier=f"default.types_test",
    schema=parquet_file.schema,
)
iceberg_table.append(df=parquet_file)
