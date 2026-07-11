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
Tests requiring live HF credentials are skipped when HF_TOKEN or HF_DATASET is not set.
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


# ---------------------------------------------------------------------------
# CDC tests — run without any external credentials
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def local_catalog(tmp_path_factory: pytest.TempPathFactory):
    warehouse = tmp_path_factory.mktemp("cdc_warehouse")
    catalog = load_catalog(
        "default",
        **{
            "uri": f"sqlite:///{warehouse}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse}",
        },
    )
    yield catalog
    catalog.close()


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
            "write.parquet.content-defined-chunking.min-chunk-size": "65536",
            "write.parquet.content-defined-chunking.max-chunk-size": "524288",
            "write.parquet.content-defined-chunking.norm-level": "2",
        },
    )

    props = tbl.properties
    assert props.get("write.parquet.content-defined-chunking.min-chunk-size") == "65536"
    assert (
        props.get("write.parquet.content-defined-chunking.max-chunk-size") == "524288"
    )
    assert props.get("write.parquet.content-defined-chunking.norm-level") == "2"


def test_cdc_write_via_pyiceberg(local_catalog, sample_table):
    """PyIceberg tbl.append() writes parquet with CDC options when properties are set."""
    local_catalog.create_namespace_if_not_exists("cdc_ns")

    tbl = local_catalog.create_table_if_not_exists(
        "cdc_ns.cdc_pyiceberg_write",
        schema=sample_table.schema,
        properties={"write.parquet.content-defined-chunking.enabled": "true"},
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
        properties={"write.parquet.content-defined-chunking.enabled": "true"},
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
# HF + CDC tests — skipped when HF_TOKEN or HF_DATASET is not set
# ---------------------------------------------------------------------------

requires_hf = pytest.mark.skipif(
    not os.environ.get("HF_TOKEN") or not os.environ.get("HF_DATASET"),
    reason="HF_TOKEN or HF_DATASET not set",
)


@pytest.fixture(scope="module")
def hf_cdc_table(sample_table):
    """Write a CDC-enabled Iceberg table to HF Hub once; share across HF tests.

    Uses FsspecFileIO backed by huggingface_hub's HfFileSystem (hf:// in fsspec).
    HF_TOKEN is read from the environment automatically by HfFileSystem.
    """
    token = os.environ["HF_TOKEN"]
    dataset = os.environ["HF_DATASET"]

    warehouse = f"hf://datasets/{dataset}/iceberg-ci-{os.getpid()}"
    catalog = load_catalog(
        "hf_test",
        **{
            "uri": "sqlite:///:memory:",
            "warehouse": warehouse,
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        },
    )
    catalog.create_namespace("ns")
    tbl = catalog.create_table(
        "ns.cdc_tbl",
        schema=sample_table.schema,
        properties={"write.parquet.content-defined-chunking.enabled": "true"},
    )
    tbl.append(sample_table)
    # HfFileSystem.dircache may reflect the pre-write state; invalidate it so
    # subsequent reads (info/open) see the files just uploaded via xet.
    tbl.io.get_fs("hf").invalidate_cache()
    yield tbl, token
    catalog.close()


@requires_hf
def test_hf_cdc_write_and_read_via_pyarrow(hf_cdc_table, sample_table):
    """PyIceberg writes CDC parquet to HF Hub; PyArrow scan reads it back."""
    tbl, _ = hf_cdc_table
    result = tbl.scan().to_arrow()
    assert len(result) == len(sample_table)


@requires_hf
@requires_datafusion_53
def test_hf_cdc_write_and_read_via_datafusion(hf_cdc_table, sample_table):
    """PyIceberg writes CDC parquet to HF Hub; IcebergDataFusionTable reads it back via opendal-hf."""
    tbl, token = hf_cdc_table
    provider = IcebergDataFusionTable(
        identifier=tbl.name(),
        metadata_location=tbl.metadata_location,
        file_io_properties={"hf.token": token},
    )
    ctx = SessionContext()
    ctx.register_table("hf_table", provider)
    assert ctx.table("hf_table").count() == len(sample_table)
