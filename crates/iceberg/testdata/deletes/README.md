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

# `deletion-vector-v1` golden fixtures

Inputs to the `dv_blob_golden_*` tests in
[`crates/iceberg/src/delete_vector.rs`](../../src/delete_vector.rs). Each
file is a complete Puffin `deletion-vector-v1` blob payload
(`[length][magic][roaring][crc32]`).

| File | Positions |
|---|---|
| `empty.bin` | _(none)_ |
| `single-position.bin` | `{0}` |
| `small-bitmap.bin` | `{0, 1, 100, 1000}` |
| `spanning-keys.bin` | `{0, 1<<33, (1<<33)+5, 1<<34}` |
| `dense-range.bin` | `0..10_000` (bitmap-container path; Java's RLE produces ~30 B for the same input) |

Regenerate with:

```bash
cargo test -p iceberg dv_blob_regenerate_golden_fixtures -- --ignored --exact
```
