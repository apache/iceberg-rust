<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Encrypted manifest list test data

`manifest-list-v3-encrypted.avro` is an AES-GCM (AGS1 stream format) encrypted
manifest list containing zero entries. It was generated using an `EncryptionManager`
seeded with the master key bytes below.

The corresponding table metadata fixture at
`../table_metadata/TableMetadataV3ValidEncryption.json` contains:
- A snapshot whose `key-id` references the wrapped DEK entry
- `encryption-keys` with a KEK (wrapped by the master key) and a wrapped DEK entry
  (wrapped by the KEK)

To decrypt this file in tests, seed a `MemoryKeyManagementClient` with:
```
key-id: "master-1"
bytes:  [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
         0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f]
```

If you need to regenerate this file (e.g. after changing the encryption format),
use `add_master_key_bytes` with the bytes above, encrypt an empty manifest list,
and update both this file and the encryption-keys in the JSON fixture.
