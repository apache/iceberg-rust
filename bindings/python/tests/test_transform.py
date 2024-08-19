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

from pyiceberg_core.transform import bucket_transform

import pytest
import pyarrow as pa


def test_bucket_pyarrow_array():
    arr = pa.array([1, 2])
    result = bucket_transform(arr, 10)
    expected = pa.array([6, 2], type=pa.int32())
    assert result == expected


def test_bucket_pyarrow_array_list_type_fails():
    arr = pa.array([[1, 2], [3, 4]])
    with pytest.raises(
        ValueError,
        match=r"FeatureUnsupported => Unsupported data type for bucket transform",
    ):
        bucket_transform(arr, 10)


def test_bucket_chunked_array():
    chunked = pa.chunked_array([pa.array([1, 2]), pa.array([3, 4])])
    result_chunks = []
    for arr in chunked.iterchunks():
        result_chunks.append(bucket_transform(arr, 10))

    expected = pa.chunked_array(
        [pa.array([6, 2], type=pa.int32()), pa.array([5, 0], type=pa.int32())]
    )
    assert pa.chunked_array(result_chunks).equals(expected)
