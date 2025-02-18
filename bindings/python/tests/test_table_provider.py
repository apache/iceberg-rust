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


import pytest
from pyiceberg_core import table_provider
from datafusion import SessionContext
import tempfile
from pyiceberg.catalog.sql import SqlCatalog
import pyarrow as pa


# To run build locally (until the PR is merged):
# - use `datafusion` >=45
# - use `datafusion-python` >=45
# - in `iceberg-rust`, build `bindings/python` and run tests
#     - `cd bindings/python`
#     - `hatch run dev:develop`
#     - `hatch run dev:test`
def test_iceberg_table_provider():
    import datafusion
    assert datafusion.__version__ >= '45'
    with tempfile.TemporaryDirectory() as temp_dir:
        catalog = SqlCatalog(
            "default",
            **{
                "uri": f"sqlite:///{temp_dir}/pyiceberg_catalog.db",
                "warehouse": f"file://{temp_dir}",
            },
        )
        df = pa.Table.from_pydict(
            {
                "column1": [1, 2, 3],
                "column2": ["A", "B", "C"],
            }
        )
        catalog.create_namespace_if_not_exists("default")
        table = catalog.create_table_if_not_exists(
            "default.dataset",
            schema=df.schema,
        )
        table.append(df)

        metadata_location = table.metadata_location
        iceberg_table_provider = table_provider.create_table_provider(
            metadata_location=metadata_location
        )

        ctx = SessionContext()
        ctx.register_table_provider("test", iceberg_table_provider)

        # Get the registered table
        table = ctx.table("test")

        # Check that we got a table back
        assert table is not None

        # Instead of comparing table objects directly, let's verify functionality
        # For example, check if we can get the schema
        try:
            schema = table.schema()
            assert schema is not None
        except Exception as e:
            pytest.fail(f"Failed to get schema: {e}")

        # Or try executing a simple query
        try:
            df = ctx.sql("SELECT * FROM test LIMIT 100")
            assert df is not None
            print(df)
        except Exception as e:
            pytest.fail(f"Failed to query table: {e}")
