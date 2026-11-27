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
    with pytest.raises(ValueError, match=r"version: 2"):
        encryption.decode_standard_key_metadata(data)


def test_decode_rejects_empty_buffer():
    with pytest.raises(ValueError, match="Empty key metadata"):
        encryption.decode_standard_key_metadata(b"")


def test_decode_tolerates_trailing_bytes():
    # Deliberate, matching Java: a reader must tolerate fields a newer writer appended.
    encoded = encryption.encode_standard_key_metadata(AES128_KEY, b"ad", 1024)

    metadata = encryption.decode_standard_key_metadata(encoded + b"\xde\xad\xbe\xef")

    assert fields(metadata) == (AES128_KEY, b"ad", 1024)


@pytest.mark.xfail(
    reason="apache/avro-rs#664: a missing union tag decodes as null; fixed upstream in 0.23",
    strict=True,
)
def test_decode_rejects_truncated_union_tags():
    # Version and key, with both union tags omitted, currently decodes as if the
    # optional fields were absent rather than erroring.
    with pytest.raises(ValueError):
        encryption.decode_standard_key_metadata(ENCODED_PREFIX)


PLAINTEXT = b"the quick brown fox"
PLAIN_BLOCK_SIZE = 1024 * 1024
BLOCK_OVERHEAD = (
    encryption.AesGcmCipher.NONCE_LENGTH + encryption.AesGcmCipher.TAG_LENGTH
)


@pytest.mark.parametrize("key_length", [16, 24, 32])
@pytest.mark.parametrize("aad", [None, b"", b"aad"])
def test_cipher_round_trip(key_length, aad):
    cipher = encryption.AesGcmCipher(bytes(range(key_length)))

    ciphertext = cipher.encrypt(PLAINTEXT, aad)

    assert len(ciphertext) == BLOCK_OVERHEAD + len(PLAINTEXT)
    assert cipher.decrypt(ciphertext, aad) == PLAINTEXT


def test_cipher_decrypts_known_answer():
    # Test case 4 of McGrew & Viega, "The Galois/Counter Mode of Operation (GCM)", which pins the
    # `nonce || ciphertext || tag` layout shared with Java.
    cipher = encryption.AesGcmCipher(bytes.fromhex("feffe9928665731c6d6a8f9467308308"))
    stored = bytes.fromhex(
        "cafebabefacedbaddecaf888"
        "42831ec2217774244b7221b784d0d49ce3aa212f2c02a4e035c17e2329aca12e"
        "21d514b25466931c7d8f6a5aac84aa051ba30b396a0aac973d58e091"
        "5bc94fbc3221a5db94fae95ae7121a47"
    )

    assert cipher.decrypt(
        stored, bytes.fromhex("feedfacedeadbeeffeedfacedeadbeefabaddad2")
    ) == bytes.fromhex(
        "d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a72"
        "1c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b39"
    )


@pytest.mark.parametrize("key_length", [0, 15, 33])
def test_cipher_rejects_invalid_key_length(key_length):
    with pytest.raises(ValueError, match=f"Invalid data key length: {key_length}"):
        encryption.AesGcmCipher(bytes(key_length))


def test_cipher_rejects_mismatched_aad():
    cipher = encryption.AesGcmCipher(AES128_KEY)

    with pytest.raises(ValueError, match="AES-GCM decryption failed"):
        cipher.decrypt(cipher.encrypt(PLAINTEXT, b"aad"), b"other aad")


def test_cipher_rejects_short_ciphertext():
    with pytest.raises(ValueError, match="expected at least 28 bytes, got 27"):
        encryption.AesGcmCipher(AES128_KEY).decrypt(bytes(27))


# Streams written by Java's `AesGcmOutputFile` (Apache Iceberg 1.11.0) with key `bytes(range(16))`,
# encrypting `patterned_plaintext(length)`.
JAVA_STREAMS = [
    pytest.param(
        "4147533100001000178c6b4fd72e166bc1667356c865baed13878ed51c4340f6426501ec",
        0,
        b"pyiceberg-ags1",
        id="empty",
    ),
    pytest.param(
        "4147533100001000c02998e75683447a486f7dd3dd6c9f8a2a10fbfc956b15fdb35611534fe8d1414c81b17a477b914b"
        "fa0b515a5060cff94301feb0e85535402a2a2966214e20274635d4c19ae525d4da042ac51ed7db0c6698c731df0ba637"
        "4aff8b8d5c05297b8896abbfabe09c3ce1a2389c46d14718c17b0a3e33a4ddebc05a11b18ec9c576",
        100,
        b"pyiceberg-ags1",
        id="partial-block",
    ),
    pytest.param(
        "4147533100001000117d00679bfa455f29880fe9dcd8d2ae9b8a7e81d40dbce6061631524e5ec61cbac9914317e7e60f"
        "dff52b32a779b3ae5339ea8859a40b3454a986a7f36b62f6e46699b036b6abe2ae043a86aee0ac5873066bab2420802c"
        "f970eede664723b455a224b99a5866c862c81640393168355e2acbe90e677a6dbaadab9ffa5cf3dd",
        100,
        None,
        id="partial-block-no-aad",
    ),
]


def patterned_plaintext(length):
    """Return a plaintext whose byte `i` is `i % 251`, so a misordered block changes the bytes."""
    return (bytes(range(251)) * (length // 251 + 1))[:length]


@pytest.mark.parametrize("stream, plaintext_length, aad_prefix", JAVA_STREAMS)
def test_ags1_decrypts_a_java_written_stream(stream, plaintext_length, aad_prefix):
    stream = bytes.fromhex(stream)

    plaintext = encryption.decrypt_ags1(
        stream, bytes(range(16)), file_length=len(stream), aad_prefix=aad_prefix
    )

    assert plaintext == patterned_plaintext(plaintext_length)


@pytest.mark.parametrize("aad_prefix", [None, b"0123456789abcdef"])
@pytest.mark.parametrize(
    "plaintext_length, block_count",
    [
        (0, 1),
        (1, 1),
        (PLAIN_BLOCK_SIZE, 1),
        (PLAIN_BLOCK_SIZE + 7, 2),
        (2 * PLAIN_BLOCK_SIZE, 2),
    ],
)
def test_ags1_round_trip(plaintext_length, block_count, aad_prefix):
    plaintext = patterned_plaintext(plaintext_length)

    stream = encryption.encrypt_ags1(plaintext, AES128_KEY, aad_prefix=aad_prefix)

    assert stream[:8] == b"AGS1\x00\x00\x10\x00"
    assert len(stream) == 8 + block_count * BLOCK_OVERHEAD + plaintext_length
    assert (
        encryption.decrypt_ags1(
            stream, AES128_KEY, file_length=len(stream), aad_prefix=aad_prefix
        )
        == plaintext
    )


@pytest.mark.parametrize("change", ["truncate", "extend"])
def test_ags1_rejects_a_length_other_than_file_length(change):
    stream = encryption.encrypt_ags1(
        patterned_plaintext(2 * PLAIN_BLOCK_SIZE), AES128_KEY
    )
    changed = (
        stream[: 8 + PLAIN_BLOCK_SIZE + BLOCK_OVERHEAD]
        if change == "truncate"
        else stream + b"\x00"
    )

    with pytest.raises(
        ValueError,
        match=f"key metadata expects {len(stream)} bytes, got {len(changed)}",
    ):
        encryption.decrypt_ags1(changed, AES128_KEY, file_length=len(stream))


@pytest.mark.parametrize(
    "key, aad_prefix",
    [(AES128_KEY, b"another file's prefix"), (b"1123456789012345", b"ad")],
    ids=["aad-prefix", "key"],
)
def test_ags1_rejects_the_wrong_key_or_aad_prefix(key, aad_prefix):
    stream = encryption.encrypt_ags1(PLAINTEXT, AES128_KEY, aad_prefix=b"ad")

    with pytest.raises(ValueError, match="AES-GCM decryption failed"):
        encryption.decrypt_ags1(
            stream, key, file_length=len(stream), aad_prefix=aad_prefix
        )
