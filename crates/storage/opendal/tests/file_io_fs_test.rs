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

//! Integration tests for FileIO OpenDAL filesystem storage.

#[cfg(feature = "opendal-fs")]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use iceberg::io::{FileIO, FileIOBuilder};
    use iceberg_storage_opendal::OpenDalStorageFactory;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_file_io_fs_serialization_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("serialization-roundtrip");
        let path = format!("file:/{}", path.display());
        let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Fs)).build();
        let serialized = file_io.serialize_all().unwrap();
        let file_io = FileIO::deserialize_all(&serialized).unwrap();

        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"roundtrip"))
            .await
            .unwrap();
        assert_eq!(
            file_io.new_input(&path).unwrap().read().await.unwrap(),
            Bytes::from_static(b"roundtrip")
        );
        file_io.delete(&path).await.unwrap();
        assert!(!file_io.exists(&path).await.unwrap());
    }
}
