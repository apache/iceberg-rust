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
# A version byte, then the Avro-encoded encryption_key: zigzag length 16, then the key.
ENCODED_PREFIX = b"\x01\x20" + AES128_KEY


def fields(metadata):
    return (metadata.encryption_key, metadata.aad_prefix, metadata.file_length)


# Pinned in both directions so the encoding stays compatible with the Java and Python
# implementations, including the null union tags written for absent optional fields.
@pytest.mark.parametrize(
    "aad_prefix, file_length, encoded",
    [
        (None, None, ENCODED_PREFIX + b"\x00\x00"),
        (b"ad", None, ENCODED_PREFIX + b"\x02\x04ad\x00"),
        (b"ad", 1024, ENCODED_PREFIX + b"\x02\x04ad\x02\x80\x10"),
        (b"", None, ENCODED_PREFIX + b"\x02\x00\x00"),
    ],
)
def test_wire_format(aad_prefix, file_length, encoded):
    assert fields(encryption.decode_standard_key_metadata(encoded)) == (
        AES128_KEY,
        aad_prefix,
        file_length,
    )
    assert (
        encryption.encode_standard_key_metadata(AES128_KEY, aad_prefix, file_length)
        == encoded
    )


def test_repr_redacts_encryption_key():
    encoded = encryption.encode_standard_key_metadata(AES128_KEY, b"ad", 1024)

    rendered = repr(encryption.decode_standard_key_metadata(encoded))

    assert AES128_KEY not in rendered.encode()
    assert rendered == (
        "StandardKeyMetadata(encryption_key=<redacted, 16 bytes>, "
        "aad_prefix=b'ad', file_length=1024)"
    )


@pytest.mark.parametrize("key_length", [16, 24, 32])
def test_encode_accepts_aes_key_lengths(key_length):
    encoded = encryption.encode_standard_key_metadata(bytes(key_length))
    metadata = encryption.decode_standard_key_metadata(encoded)

    assert fields(metadata) == (bytes(key_length), None, None)


@pytest.mark.parametrize("key_length", [0, 4, 15, 20, 33])
def test_encode_rejects_invalid_key_length(key_length):
    with pytest.raises(ValueError, match="key length"):
        encryption.encode_standard_key_metadata(bytes(key_length))


@pytest.mark.parametrize("key_length", [0, 4, 15, 20, 33])
def test_decode_rejects_invalid_key_length(key_length):
    # Key length is validated on decode too, not only on encode. The Avro zigzag length
    # is 2 * key_length, a single byte for every length here.
    data = b"\x01" + bytes([key_length * 2]) + bytes(key_length) + b"\x00\x00"

    with pytest.raises(ValueError, match="Invalid encryption key in key metadata"):
        encryption.decode_standard_key_metadata(data)


@pytest.mark.parametrize("data", [b"\x02", b"\x02\x20" + AES128_KEY + b"\x00\x00"])
def test_decode_rejects_unsupported_version(data):
    with pytest.raises(ValueError, match="Unsupported key metadata version: 2"):
        encryption.decode_standard_key_metadata(data)


def test_decode_rejects_empty_buffer():
    with pytest.raises(ValueError, match="Empty key metadata"):
        encryption.decode_standard_key_metadata(b"")


@pytest.mark.xfail(
    reason="apache/avro-rs#664: a missing union tag decodes as null; fixed upstream in 0.23",
    strict=True,
)
def test_decode_rejects_truncated_union_tags():
    # Version and key, with both union tags omitted, currently decodes as if the
    # optional fields were absent rather than erroring.
    with pytest.raises(ValueError):
        encryption.decode_standard_key_metadata(ENCODED_PREFIX)
