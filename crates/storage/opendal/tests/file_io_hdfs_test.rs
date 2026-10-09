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

//! Integration tests for HDFS FileIO via OpenDAL `services-hdfs-native`.
//!
//! These tests need the HDFS fixture in `dev/docker-compose.yaml`, which
//! `make docker-up` starts with the other fixtures.

#[cfg(feature = "opendal-hdfs-native")]
mod tests {
    use std::net::ToSocketAddrs;
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use iceberg::io::{
        FileIO, FileIOBuilder, HDFS_HOST, HDFS_NAME_NODE, HDFS_PORT, StorageFactory,
    };
    use iceberg_storage_opendal::{OpenDalResolvingStorageFactory, OpenDalStorageFactory};
    use iceberg_test_utils::{get_hdfs_endpoint, normalize_test_name_with_parts, set_up};

    /// The fixture's DataNode advertises `127.0.0.1`; without this the client
    /// dials its container IP, which the host cannot reach on macOS or Windows.
    fn file_io_builder(factory: Arc<dyn StorageFactory>) -> FileIOBuilder {
        set_up();
        FileIOBuilder::new(factory).with_prop("hadoop.dfs.client.use.datanode.hostname", "true")
    }

    fn get_file_io() -> FileIO {
        file_io_builder(Arc::new(OpenDalStorageFactory::HdfsNative)).build()
    }

    fn test_path(suffix: &str) -> String {
        format!(
            "{}/{}",
            get_hdfs_endpoint(),
            normalize_test_name_with_parts!(suffix)
        )
    }

    /// The endpoint with its host replaced by that host's IPv4 address: a
    /// second spelling of the same NameNode, so a second batch key. `None`
    /// when the endpoint already uses an IP literal.
    fn alternate_endpoint(endpoint: &str) -> Option<String> {
        let url = url::Url::parse(endpoint).ok()?;
        let host = url.host_str()?;
        if host.parse::<std::net::IpAddr>().is_ok() {
            return None;
        }
        let addr = (host, url.port()?)
            .to_socket_addrs()
            .ok()?
            .find(std::net::SocketAddr::is_ipv4)?;
        Some(format!("hdfs://{addr}"))
    }

    #[tokio::test]
    async fn test_file_io_hdfs_exists() {
        let file_io = get_file_io();
        let dir = test_path("test_file_io_hdfs_exists");
        let present = format!("{dir}/present");
        let _ = file_io.delete(&present).await;
        file_io
            .new_output(&present)
            .unwrap()
            .write(Bytes::from_static(b"x"))
            .await
            .unwrap();

        assert!(file_io.exists(&present).await.unwrap());
        assert!(!file_io.exists(&format!("{dir}/absent")).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_hdfs_write_and_read() {
        let file_io = get_file_io();
        let path = test_path("test_file_io_hdfs_write_and_read");
        let _ = file_io.delete(&path).await;

        let output = file_io.new_output(&path).unwrap();
        output
            .write(Bytes::from_static(b"hello hdfs"))
            .await
            .unwrap();

        assert!(file_io.exists(&path).await.unwrap());
        let input = file_io.new_input(&path).unwrap();
        assert_eq!(
            input.read().await.unwrap(),
            Bytes::from_static(b"hello hdfs")
        );
    }

    /// The HA flow: table locations carry a logical authority, and
    /// `hdfs.name-node.<nameservice>` declares its comma-separated endpoints
    /// (the single-node fixture is listed twice to drive the list path).
    #[tokio::test]
    async fn test_file_io_hdfs_declared_nameservice() {
        let endpoint = get_hdfs_endpoint();
        let file_io = file_io_builder(Arc::new(OpenDalStorageFactory::HdfsNative))
            .with_prop(
                format!("{HDFS_NAME_NODE}.logical-nameservice"),
                format!("{endpoint},{endpoint}"),
            )
            .build();

        // The path authority is a logical name resolved by its declaration.
        let path = format!(
            "hdfs://logical-nameservice/{}",
            normalize_test_name_with_parts!("test_file_io_hdfs_declared_nameservice")
        );
        let _ = file_io.delete(&path).await;

        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"via configured name node"))
            .await
            .unwrap();

        assert!(file_io.exists(&path).await.unwrap());
        assert_eq!(
            file_io.new_input(&path).unwrap().read().await.unwrap(),
            Bytes::from_static(b"via configured name node")
        );
    }

    /// Authority-less paths resolve through plain `hdfs.name-node`, or through
    /// PyIceberg's `hdfs.host`/`hdfs.port`; both must reach the same cluster.
    #[tokio::test]
    async fn test_file_io_hdfs_authority_less_paths() {
        let endpoint = get_hdfs_endpoint();
        let url = url::Url::parse(&endpoint).unwrap();
        let by_name_node = file_io_builder(Arc::new(OpenDalStorageFactory::HdfsNative))
            .with_prop(HDFS_NAME_NODE, &endpoint)
            .build();
        let by_host_port = file_io_builder(Arc::new(OpenDalStorageFactory::HdfsNative))
            .with_prop(HDFS_HOST, url.host_str().unwrap())
            .with_prop(HDFS_PORT, url.port().unwrap().to_string())
            .build();

        let path = format!(
            "hdfs:///{}",
            normalize_test_name_with_parts!("test_file_io_hdfs_authority_less_paths")
        );
        let _ = by_name_node.delete(&path).await;
        by_name_node
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"authority-less"))
            .await
            .unwrap();

        assert_eq!(
            by_host_port.new_input(&path).unwrap().read().await.unwrap(),
            Bytes::from_static(b"authority-less")
        );
        by_host_port.delete(&path).await.unwrap();
        assert!(!by_name_node.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_hdfs_overwrite() {
        let file_io = get_file_io();
        let path = test_path("test_file_io_hdfs_overwrite");
        let _ = file_io.delete(&path).await;

        for content in [b"first".as_slice(), b"second, longer".as_slice()] {
            file_io
                .new_output(&path)
                .unwrap()
                .write(Bytes::from_static(content))
                .await
                .unwrap();
        }

        assert_eq!(
            file_io.new_input(&path).unwrap().read().await.unwrap(),
            Bytes::from_static(b"second, longer")
        );
    }

    #[tokio::test]
    async fn test_file_io_hdfs_delete_stream() {
        let file_io = get_file_io();

        let paths: Vec<String> = (0..5)
            .map(|i| format!("{}/file-{i}", test_path("test_file_io_hdfs_delete_stream")))
            .collect();
        for path in &paths {
            let _ = file_io.delete(path).await;
            file_io
                .new_output(path)
                .unwrap()
                .write("delete-me".into())
                .await
                .unwrap();
            assert!(file_io.exists(path).await.unwrap());
        }

        let stream = futures::stream::iter(paths.clone()).boxed();
        file_io.delete_stream(stream).await.unwrap();

        for path in &paths {
            assert!(!file_io.exists(path).await.unwrap());
        }
    }

    /// Paths are batched per effective NameNode; two spellings of the fixture's
    /// NameNode drive two operators and two deleters through one stream. The
    /// fixture is one cluster, so the keying itself is pinned by the unit
    /// tests; this proves the multi-operator path completes and cleans up.
    #[tokio::test]
    async fn test_file_io_hdfs_delete_stream_two_name_nodes() {
        let endpoint = get_hdfs_endpoint();
        let Some(alternate) = alternate_endpoint(&endpoint) else {
            eprintln!("Skipping HDFS test: {endpoint} has no second spelling");
            return;
        };
        let file_io = get_file_io();
        let dir = test_path("test_file_io_hdfs_delete_stream_two_name_nodes");
        let alt_dir = dir.replacen(&endpoint, &alternate, 1);
        assert_ne!(dir, alt_dir);

        let paths = vec![format!("{dir}/a"), format!("{alt_dir}/b")];
        for path in &paths {
            let _ = file_io.delete(path).await;
            file_io
                .new_output(path)
                .unwrap()
                .write("delete-me".into())
                .await
                .unwrap();
        }
        // Both spellings reach the same cluster.
        assert!(file_io.exists(&format!("{alt_dir}/a")).await.unwrap());
        assert!(file_io.exists(&format!("{dir}/b")).await.unwrap());

        let stream = futures::stream::iter(paths.clone()).boxed();
        file_io.delete_stream(stream).await.unwrap();

        // Everything is gone, through whichever deleter.
        for path in [
            format!("{dir}/a"),
            format!("{dir}/b"),
            format!("{alt_dir}/a"),
            format!("{alt_dir}/b"),
        ] {
            assert!(!file_io.exists(&path).await.unwrap(), "{path}");
        }
    }

    #[tokio::test]
    async fn test_file_io_hdfs_delete_stream_empty() {
        let file_io = get_file_io();
        let stream = futures::stream::empty().boxed();
        file_io.delete_stream(stream).await.unwrap();
    }

    #[tokio::test]
    async fn test_file_io_hdfs_resolving_storage() {
        let file_io = file_io_builder(Arc::new(OpenDalResolvingStorageFactory::new())).build();
        let path = test_path("test_file_io_hdfs_resolving_storage");
        let _ = file_io.delete(&path).await;

        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"resolving"))
            .await
            .unwrap();

        assert_eq!(
            file_io.new_input(&path).unwrap().read().await.unwrap(),
            Bytes::from_static(b"resolving")
        );

        file_io.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_file_io_hdfs_metadata() {
        let file_io = get_file_io();
        let path = test_path("test_file_io_hdfs_metadata");
        let _ = file_io.delete(&path).await;
        let content = Bytes::from_static(b"0123456789");

        file_io
            .new_output(&path)
            .unwrap()
            .write(content.clone())
            .await
            .unwrap();

        let metadata = file_io.new_input(&path).unwrap().metadata().await.unwrap();
        assert_eq!(metadata.size, content.len() as u64);
    }

    #[tokio::test]
    async fn test_file_io_hdfs_delete() {
        let file_io = get_file_io();
        let path = test_path("test_file_io_hdfs_delete");

        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(b"x"))
            .await
            .unwrap();
        assert!(file_io.exists(&path).await.unwrap());

        file_io.delete(&path).await.unwrap();
        assert!(!file_io.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_hdfs_delete_prefix() {
        let file_io = get_file_io();
        let dir = test_path("test_file_io_hdfs_delete_prefix");
        let _ = file_io.delete_prefix(&dir).await;

        for i in 0..3 {
            let path = format!("{dir}/file_{i}");
            file_io
                .new_output(&path)
                .unwrap()
                .write(Bytes::from(format!("payload {i}")))
                .await
                .unwrap();
        }
        assert!(file_io.exists(&format!("{dir}/file_0")).await.unwrap());

        file_io.delete_prefix(&dir).await.unwrap();

        for i in 0..3 {
            assert!(!file_io.exists(&format!("{dir}/file_{i}")).await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_file_io_hdfs_reader_range() {
        let file_io = get_file_io();
        let path = test_path("test_file_io_hdfs_reader_range");
        let _ = file_io.delete(&path).await;
        let content = Bytes::from_static(b"abcdefghij");

        file_io
            .new_output(&path)
            .unwrap()
            .write(content.clone())
            .await
            .unwrap();

        let reader = file_io.new_input(&path).unwrap().reader().await.unwrap();
        assert_eq!(
            reader.read(0..5).await.unwrap(),
            Bytes::from_static(b"abcde")
        );
        assert_eq!(
            reader.read(5..10).await.unwrap(),
            Bytes::from_static(b"fghij")
        );
    }

    #[tokio::test]
    async fn test_file_io_hdfs_streaming_writer() {
        let file_io = get_file_io();
        let path = test_path("test_file_io_hdfs_streaming_writer");
        let _ = file_io.delete(&path).await;

        let output = file_io.new_output(&path).unwrap();
        let mut writer = output.writer().await.unwrap();
        writer.write(Bytes::from_static(b"part1 ")).await.unwrap();
        writer.write(Bytes::from_static(b"part2")).await.unwrap();
        writer.close().await.unwrap();

        let read = file_io.new_input(&path).unwrap().read().await.unwrap();
        assert_eq!(read, Bytes::from_static(b"part1 part2"));
    }
}
