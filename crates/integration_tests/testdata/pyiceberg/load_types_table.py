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

from datafusion import SessionContext
from pyiceberg.catalog import load_catalog
import pyarrow.parquet as pq

# Generate a table with various types in memory and dump to a Parquet file
ctx = SessionContext()
ctx.sql("""
CREATE TABLE types_test (
    cboolean BOOLEAN,
    cint8 TINYINT,
    cint16 SMALLINT,
    cint32 INT,
    cint64 BIGINT,
    cfloat32 REAL,
    cfloat64 DOUBLE PRECISION,
    cdecimal DECIMAL(8, 2),
    cdate32 DATE,
    ctimestamp TIMESTAMP,
    ctimestamptz TIMESTAMPTZ,
    cutf8 TEXT,
    cbinary BYTEA
) AS SELECT
    s % 2 = 1 as cboolean,
    (s % 256 - 128) as cint8,
    s as cint16,
    s as cint32,
    s as cint64,
    s as cfloat32,
    s as cfloat64,
    s::NUMERIC / 100 as cnumeric,
    s as cdate,
    s * 1000 as ctimestamp,
    s * 1000 as ctimestampz,
    s::TEXT as cutf8,
    s::TEXT cbinary
FROM unnest(generate_series(0, 1000)) AS q(s);
""")
a = ctx.sql("COPY types_test TO 'types_test.parquet'")
# File loading fails in the container without this line???
print(f"Created a Parquet file with {a} rows")

# Load the Parquet file
parquet_file = pq.read_table("./types_test.parquet")

# Connect to the REST catalog
catalog = load_catalog(
    "rest",
    **{
        "type": "rest",
        "uri": "http://rest:8181",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    },
)

# Create a corresponding Iceberg table and append the file to it
iceberg_table = catalog.create_table_if_not_exists(
    identifier=f"default.types_test",
    schema=parquet_file.schema,
)
iceberg_table.append(df=parquet_file)
