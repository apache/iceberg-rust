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

from datetime import date, datetime

import pyarrow as pa
import pytest
from pyiceberg_core import ArrowArrayTransform


def test_identity_transform():
    arr = pa.array([1, 2])
    result = ArrowArrayTransform.identity(arr)
    assert result == arr


def test_bucket_transform():
    arr = pa.array([1, 2])
    result = ArrowArrayTransform.bucket(arr, 10)
    expected = pa.array([6, 2], type=pa.int32())
    assert result == expected


def test_bucket_transform_fails_for_list_type_input():
    arr = pa.array([[1, 2], [3, 4]])
    with pytest.raises(
        ValueError,
        match=r"FeatureUnsupported => Unsupported data type for bucket transform",
    ):
        ArrowArrayTransform.bucket(arr, 10)


def test_bucket_chunked_array():
    chunked = pa.chunked_array([pa.array([1, 2]), pa.array([3, 4])])
    result_chunks = []
    for arr in chunked.iterchunks():
        result_chunks.append(ArrowArrayTransform.bucket(arr, 10))

    expected = pa.chunked_array(
        [pa.array([6, 2], type=pa.int32()), pa.array([5, 0], type=pa.int32())]
    )
    assert pa.chunked_array(result_chunks).equals(expected)


def test_year_transform():
    arr = pa.array([date(1970, 1, 1), date(2000, 1, 1)])
    result = ArrowArrayTransform.year(arr)
    expected = pa.array([0, 30], type=pa.int32())
    assert result == expected


def test_month_transform():
    arr = pa.array([date(1970, 1, 1), date(2000, 4, 1)])
    result = ArrowArrayTransform.month(arr)
    expected = pa.array([0, 30 * 12 + 3], type=pa.int32())
    assert result == expected


def test_day_transform():
    arr = pa.array([date(1970, 1, 1), date(2000, 4, 1)])
    result = ArrowArrayTransform.day(arr)
    expected = pa.array([0, 11048], type=pa.int32())
    assert result == expected


def test_hour_transform():
    arr = pa.array([datetime(1970, 1, 1, 19, 1, 23), datetime(2000, 3, 1, 12, 1, 23)])
    result = ArrowArrayTransform.hour(arr)
    expected = pa.array([19, 264420], type=pa.int32())
    assert result == expected


def test_truncate_transform():
    arr = pa.array(["this is a long string", "hi my name is sung"])
    result = ArrowArrayTransform.truncate(arr, 5)
    expected = pa.array(["this ", "hi my"])
    assert result == expected
