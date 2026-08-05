// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// Strips trailing slashes from a location, preserving a bare URI scheme root
pub(crate) fn strip_trailing_slash(path: &str) -> &str {
    let mut path = path;
    while !path.ends_with("://") {
        let Some(stripped) = path.strip_suffix('/') else {
            break;
        };
        path = stripped;
    }
    path
}

#[test]
fn test_strip_trailing_slash() {
    for (path, expected) in [
        ("s3://bucket/db/tbl", "s3://bucket/db/tbl"),
        ("s3://bucket/db/tbl/", "s3://bucket/db/tbl"),
        ("s3://bucket/db/tbl////", "s3://bucket/db/tbl"),
        ("blobstore://", "blobstore://"),
        ("blobstore:///", "blobstore://"),
        ("file:///", "file://"),
        ("////", ""),
        ("", ""),
    ] {
        assert_eq!(strip_trailing_slash(path), expected);
    }
}
