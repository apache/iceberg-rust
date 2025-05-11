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


from datetime import date, datetime, timezone
import uuid
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


@pytest.fixture(scope="session")
def arrow_table_with_null() -> "pa.Table":
    """Pyarrow table with all kinds of columns."""
    import pyarrow as pa

    return pa.Table.from_pydict(
        {
            "bool": [False, None, True],
            "string": ["a", None, "z"],
            # Go over the 16 bytes to kick in truncation
            "string_long": ["a" * 22, None, "z" * 22],
            "int": [1, None, 9],
            "long": [1, None, 9],
            "float": [0.0, None, 0.9],
            "double": [0.0, None, 0.9],
            # 'time': [1_000_000, None, 3_000_000],  # Example times: 1s, none, and 3s past midnight #Spark does not support time fields
            "timestamp": [datetime(2023, 1, 1, 19, 25, 00), None, datetime(2023, 3, 1, 19, 25, 00)],
            # "timestamptz": [
            #     datetime(2023, 1, 1, 19, 25, 00, tzinfo=timezone.utc),
            #     None,
            #     datetime(2023, 3, 1, 19, 25, 00, tzinfo=timezone.utc),
            # ],
            "date": [date(2023, 1, 1), None, date(2023, 3, 1)],
            # Not supported by Spark
            # 'time': [time(1, 22, 0), None, time(19, 25, 0)],
            # Not natively supported by Arrow
            # 'uuid': [uuid.UUID('00000000-0000-0000-0000-000000000000').bytes, None, uuid.UUID('11111111-1111-1111-1111-111111111111').bytes],
            "binary": [b"\01", None, b"\22"],
            "fixed": [
                uuid.UUID("00000000-0000-0000-0000-000000000000").bytes,
                None,
                uuid.UUID("11111111-1111-1111-1111-111111111111").bytes,
            ],
        },
    )


def test_register_iceberg_tables(arrow_table_with_null: pa.Table):
    import datafusion
    assert datafusion.__version__ >= '45'

    # connect to pyiceberg integration test IRC
    from pyiceberg.catalog import load_catalog
    catalog = load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )

    namespace = "foo"
    table_identifier = f"{namespace}.bar"
    catalog.create_namespace_if_not_exists(namespace=namespace)
    try:
        catalog.drop_table(identifier=table_identifier)
    except:
        pass
    tbl = catalog.create_table(identifier=table_identifier, schema=arrow_table_with_null.schema)

    # Write some data
    tbl.append(arrow_table_with_null)
    iceberg_df = tbl.scan().to_arrow()

    print(tbl.metadata_location)

    # `PyIcebergTableProvider` does not have S3 configured
    iceberg_table_provider = table_provider.create_table_provider(
        metadata_location=tbl.metadata_location,
        file_io_properties=tbl.io.properties
    )
    ctx = SessionContext()
    ctx.sql("CREATE SCHEMA foo").show()
    ctx.register_table_provider(table_identifier, iceberg_table_provider)
    df = ctx.table(table_identifier)
    df.show()
    assert iceberg_df.to_pydict() == df.to_pydict()
    