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

//! Integration tests for HDFS FileIO via OpenDAL `services-hdfs` (libhdfs).
//!
//! These tests need the HDFS fixture in `dev/docker-compose.yaml` and
//! Hadoop's Java client: `CLASSPATH` lists `dev/hdfs/client` and then each
//! Hadoop jar, and `$JAVA_HOME/lib/server` is on the library path.

#[cfg(feature = "opendal-hdfs")]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use iceberg::io::{FileIO, FileIOBuilder};
    use iceberg_storage_opendal::OpenDalStorageFactory;
    use iceberg_test_utils::{get_hdfs_endpoint, normalize_test_name_with_parts, set_up};

    fn get_file_io() -> FileIO {
        set_up();
        FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Hdfs)).build()
    }

    fn test_path(suffix: &str) -> String {
        format!(
            "{}/{}",
            get_hdfs_endpoint(),
            normalize_test_name_with_parts!(suffix)
        )
    }

    #[tokio::test]
    async fn test_file_io_libhdfs_write_and_read() {
        let file_io = get_file_io();
        let path = test_path("test_file_io_libhdfs_write_and_read");
        let output = file_io.new_output(&path).unwrap();
        output.write(Bytes::from_static(b"stale")).await.unwrap();

        // The streaming writer overwrites the file.
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from_static(b"01234")).await.unwrap();
        writer.write(Bytes::from_static(b"56789")).await.unwrap();
        writer.close().await.unwrap();

        assert!(file_io.exists(&path).await.unwrap());
        let input = file_io.new_input(&path).unwrap();
        assert_eq!(input.metadata().await.unwrap().size, 10);
        assert_eq!(
            input.read().await.unwrap(),
            Bytes::from_static(b"0123456789")
        );
        let reader = input.reader().await.unwrap();
        assert_eq!(
            reader.read(3..7).await.unwrap(),
            Bytes::from_static(b"3456")
        );
    }

    #[tokio::test]
    async fn test_file_io_libhdfs_delete() {
        let file_io = get_file_io();
        let dir = test_path("test_file_io_libhdfs_delete");
        let paths: Vec<String> = (0..4).map(|i| format!("{dir}/file-{i}")).collect();
        for path in &paths {
            file_io
                .new_output(path)
                .unwrap()
                .write(Bytes::from_static(b"x"))
                .await
                .unwrap();
        }

        file_io.delete(&paths[0]).await.unwrap();
        file_io
            .delete_stream(futures::stream::iter(paths[1..3].to_vec()).boxed())
            .await
            .unwrap();
        for path in &paths[..3] {
            assert!(!file_io.exists(path).await.unwrap(), "{path}");
        }
        assert!(file_io.exists(&paths[3]).await.unwrap());
        file_io.delete_prefix(&dir).await.unwrap();
        assert!(!file_io.exists(&paths[3]).await.unwrap());
    }

    /// `dev/hdfs/client/core-site.xml` points `fs.defaultFS` at the fixture.
    #[tokio::test]
    async fn test_file_io_libhdfs_authority_less_path() {
        let file_io = get_file_io();
        let name = normalize_test_name_with_parts!("test_file_io_libhdfs_authority_less_path");
        file_io
            .new_output(format!("hdfs:///{name}"))
            .unwrap()
            .write(Bytes::from_static(b"x"))
            .await
            .unwrap();

        let path = format!("{}/{name}", get_hdfs_endpoint());
        assert!(file_io.exists(&path).await.unwrap());
        file_io.delete(&path).await.unwrap();
    }
}
