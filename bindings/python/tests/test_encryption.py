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
from pyiceberg_core import encryption

AES128_KEY = b"0123456789012345"


def test_encode_decode_round_trip():
    encoded = encryption.encode_standard_key_metadata(AES128_KEY, b"ad", 1024)

    assert encryption.decode_standard_key_metadata(encoded) == (AES128_KEY, b"ad", 1024)


def test_encode_decode_without_optional_fields():
    encoded = encryption.encode_standard_key_metadata(AES128_KEY)

    assert encryption.decode_standard_key_metadata(encoded) == (AES128_KEY, None, None)


def test_encoded_wire_format():
    # A version byte, then the Avro datum. Pinned so the encoding stays compatible
    # with the Java and Python implementations.
    assert encryption.encode_standard_key_metadata(AES128_KEY, b"ad", 1024) == (
        b"\x01\x20" + AES128_KEY + b"\x02\x04ad\x02\x80\x10"
    )


@pytest.mark.parametrize("key_length", [16, 24, 32])
def test_encode_accepts_aes_key_lengths(key_length):
    encoded = encryption.encode_standard_key_metadata(bytes(key_length))

    assert encryption.decode_standard_key_metadata(encoded) == (bytes(key_length), None, None)


@pytest.mark.parametrize("key_length", [0, 4, 15, 20, 33])
def test_encode_rejects_invalid_key_length(key_length):
    with pytest.raises(ValueError):
        encryption.encode_standard_key_metadata(bytes(key_length))


def test_decode_rejects_unsupported_version():
    with pytest.raises(ValueError):
        encryption.decode_standard_key_metadata(b"\x02")


def test_decode_rejects_empty_buffer():
    with pytest.raises(ValueError):
        encryption.decode_standard_key_metadata(b"")
