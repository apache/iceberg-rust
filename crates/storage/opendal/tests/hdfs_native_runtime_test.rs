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

//! A cached HDFS operator is bound to the tokio runtime that built it; once
//! that runtime is gone it must be rebuilt rather than reused. Needs no
//! HDFS: the NameNode is a local listener that only counts dials.

#[cfg(feature = "opendal-hdfs-native")]
mod tests {
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use iceberg::io::{FileIO, FileIOBuilder, HDFS_NAME_NODE};
    use iceberg_storage_opendal::OpenDalStorageFactory;

    /// Accepts and immediately closes connections, counting them.
    fn fake_name_node() -> (u16, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let dials = Arc::new(AtomicUsize::new(0));
        let counter = dials.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(_stream) = stream else { break };
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        (port, dials)
    }

    fn stat(runtime: &tokio::runtime::Runtime, file_io: &FileIO) {
        runtime.block_on(async {
            let input = file_io.new_input("hdfs://x/f").unwrap();
            // The fake NameNode never answers, so this fails; only the dial matters.
            let _ = tokio::time::timeout(Duration::from_secs(10), input.metadata()).await;
        });
    }

    #[test]
    fn test_hdfs_operator_is_rebuilt_after_its_runtime_is_dropped() {
        let (port, dials) = fake_name_node();
        let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::HdfsNative))
            .with_prop(HDFS_NAME_NODE, format!("hdfs://127.0.0.1:{port}"))
            .with_prop("hadoop.dfs.client.failover.max.attempts", "1")
            .build();

        let first = tokio::runtime::Runtime::new().unwrap();
        stat(&first, &file_io);
        let after_first = dials.load(Ordering::SeqCst);
        assert!(after_first >= 1, "the NameNode was never dialed");
        drop(first);

        // Reusing the FileIO from another runtime used to panic inside
        // hdfs-native, whose client was bound to the dropped runtime.
        let second = tokio::runtime::Runtime::new().unwrap();
        stat(&second, &file_io);
        assert!(
            dials.load(Ordering::SeqCst) > after_first,
            "no dial from the second runtime"
        );
    }
}
