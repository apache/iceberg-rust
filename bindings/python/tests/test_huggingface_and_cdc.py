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

"""Tests for HuggingFace Hub URI support and CDC (content-defined chunking) options.

CDC options are standard Iceberg table properties and work in both Rust and PyIceberg
automatically — no special API is required beyond setting string properties.

HF credentials are passed as file_io_properties to IcebergDataFusionTable.
Tests requiring a real HF token are skipped when HF_OPENDAL_TOKEN is not set.
"""

import os
import pytest
import pyarrow as pa
import datafusion
from datafusion import SessionContext
from packaging.version import Version
from pyiceberg.catalog import load_catalog
from pyiceberg_core.datafusion import IcebergDataFusionTable

requires_datafusion_53 = pytest.mark.skipif(
    Version(datafusion.__version__) < Version("53.0.0"),
    reason="IcebergDataFusionTable requires datafusion>=53 for FFI compatibility",
)

# Property name constants — same strings as the Rust TableProperties constants.
HF_TOKEN = "hf.token"
HF_ENDPOINT = "hf.endpoint"
HF_REVISION = "hf.revision"

PARQUET_CDC_ENABLED = "write.parquet.content-defined-chunking.enabled"
PARQUET_CDC_MIN_CHUNK_SIZE = "write.parquet.content-defined-chunking.min-chunk-size"
PARQUET_CDC_MAX_CHUNK_SIZE = "write.parquet.content-defined-chunking.max-chunk-size"
PARQUET_CDC_NORM_LEVEL = "write.parquet.content-defined-chunking.norm-level"


# ---------------------------------------------------------------------------
# CDC tests — run without any external credentials
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def local_catalog(tmp_path_factory: pytest.TempPathFactory):
    warehouse = tmp_path_factory.mktemp("cdc_warehouse")
    return load_catalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse}",
        },
    )


@pytest.fixture(scope="module")
def sample_table() -> pa.Table:
    return pa.table(
        {
            "id": pa.array(list(range(1000)), type=pa.int32()),
            "payload": pa.array(
                [f"row-{i:06d}" for i in range(1000)], type=pa.large_utf8()
            ),
        }
    )


def test_cdc_table_properties_are_persisted(local_catalog, sample_table):
    """Table properties with CDC options are stored and returned as-is."""
    local_catalog.create_namespace_if_not_exists("cdc_ns")

    # Use values that differ from parquet defaults (256 KiB min, 1 MiB max, 0 norm).
    tbl = local_catalog.create_table_if_not_exists(
        "cdc_ns.cdc_persist",
        schema=sample_table.schema,
        properties={
            PARQUET_CDC_MIN_CHUNK_SIZE: "65536",
            PARQUET_CDC_MAX_CHUNK_SIZE: "524288",
            PARQUET_CDC_NORM_LEVEL: "2",
        },
    )

    props = tbl.properties
    assert props.get(PARQUET_CDC_MIN_CHUNK_SIZE) == "65536"
    assert props.get(PARQUET_CDC_MAX_CHUNK_SIZE) == "524288"
    assert props.get(PARQUET_CDC_NORM_LEVEL) == "2"


def test_cdc_write_via_pyiceberg(local_catalog, sample_table):
    """PyIceberg tbl.append() writes parquet with CDC options when properties are set."""
    local_catalog.create_namespace_if_not_exists("cdc_ns")

    tbl = local_catalog.create_table_if_not_exists(
        "cdc_ns.cdc_pyiceberg_write",
        schema=sample_table.schema,
        properties={PARQUET_CDC_ENABLED: "true"},
    )
    tbl.append(sample_table)

    result = tbl.scan().to_arrow()
    assert len(result) == len(sample_table)


@requires_datafusion_53
def test_cdc_write_and_read_via_datafusion(local_catalog, sample_table):
    """A table with CDC properties can be written and read back via DataFusion."""
    local_catalog.create_namespace_if_not_exists("cdc_ns")

    tbl = local_catalog.create_table_if_not_exists(
        "cdc_ns.cdc_write_read",
        schema=sample_table.schema,
        properties={PARQUET_CDC_ENABLED: "true"},
    )
    tbl.append(sample_table)

    provider = IcebergDataFusionTable(
        identifier=tbl.name(),
        metadata_location=tbl.metadata_location,
        file_io_properties=tbl.io.properties,
    )

    ctx = SessionContext()
    ctx.register_table("cdc_table", provider)
    assert ctx.table("cdc_table").count() == len(sample_table)



# ---------------------------------------------------------------------------
# HF tests — skipped when HF_OPENDAL_TOKEN is not set
# ---------------------------------------------------------------------------

HF_TOKEN_ENV = "HF_OPENDAL_TOKEN"
HF_TABLE_METADATA_ENV = "HF_OPENDAL_TABLE_METADATA"

requires_hf = pytest.mark.skipif(
    not os.environ.get(HF_TOKEN_ENV) or not os.environ.get(HF_TABLE_METADATA_ENV),
    reason=f"{HF_TOKEN_ENV} or {HF_TABLE_METADATA_ENV} not set",
)


@requires_hf
@requires_datafusion_53
def test_hf_file_io_properties_accepted():
    """IcebergDataFusionTable accepts hf.token in file_io_properties without auth/URI errors.

    HF_OPENDAL_TABLE_METADATA must point to a valid metadata JSON file; the test
    verifies that HF credentials are wired correctly and the table can be read.
    """
    token = os.environ[HF_TOKEN_ENV]
    metadata_location = os.environ[HF_TABLE_METADATA_ENV]

    provider = IcebergDataFusionTable(
        identifier=["default", "hf_test"],
        metadata_location=metadata_location,
        file_io_properties={HF_TOKEN: token},
    )

    ctx = SessionContext()
    ctx.register_table("hf_table", provider)
    # A successful count proves the HF backend can read the table's data files.
    assert ctx.table("hf_table").count() >= 0
