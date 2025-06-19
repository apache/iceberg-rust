# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#   http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pathlib import Path
from sys import argv
from urllib.request import urlretrieve
import sqlite3

from pyiceberg.catalog import load_catalog
from pyarrow import parquet

working_dir = Path(argv[1])

warehouse_path = working_dir
catalog = load_catalog(
    "default",
    **{
        'type': 'sql',
        "uri": f"sqlite:///{warehouse_path}/benchmarking-catalog.db",
        "warehouse": f"file://{warehouse_path}",
    },
)

data_file_path = working_dir / "yellow_tripdata_2023-01.parquet"
urlretrieve("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet", data_file_path)

df = parquet.read_table(data_file_path)

catalog.create_namespace("default")

table = catalog.create_table(
    "default.taxi_dataset",
    schema=df.schema,
)

table.append(df)

conn = sqlite3.connect(f"{warehouse_path}/benchmarking-catalog.db")
c = conn.cursor()
c.execute("ALTER TABLE iceberg_tables ADD column iceberg_type VARCHAR(5) DEFAULT 'TABLE'")
conn.commit()
c.close()
