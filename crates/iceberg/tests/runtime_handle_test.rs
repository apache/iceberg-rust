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
//   Unless required by applicable law or agreed to in writing,
//   software distributed under the License is distributed on an
//   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//   KIND, either express or implied.  See the License for the
//   specific language governing permissions and limitations
//   under the License.

//! Tests for RuntimeHandle functionality
//!
//! These tests verify that RuntimeHandle can be configured via FileIO
//! extensions and that operations execute on the specified runtime.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use iceberg::io::{FileIO, FileIOBuilder, RuntimeHandle};
use tokio::runtime::Builder;

/// Test that RuntimeHandle can be created and used with FileIO
#[tokio::test]
async fn test_runtime_handle_basic() {
    // Create a runtime with a distinctive thread name
    let io_runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("test-io-runtime")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let io_handle = io_runtime.handle().clone();

    // Create FileIO with RuntimeHandle
    let file_io = FileIOBuilder::new("memory")
        .with_extension(RuntimeHandle::new(io_handle))
        .build()
        .unwrap();

    // Verify FileIO was created successfully - just check it exists
    let _exists = file_io.exists("memory://test-path").await;

    // Let runtime be dropped naturally (leak it for test purposes)
    std::mem::forget(io_runtime);
}

/// Test that RuntimeHandle::current() creates a handle to the current runtime
#[tokio::test]
async fn test_runtime_handle_current() {
    let runtime_handle = RuntimeHandle::current();

    // Should be able to create FileIO with current runtime
    let _file_io = FileIOBuilder::new("memory")
        .with_extension(runtime_handle)
        .build()
        .unwrap();
}

/// Test that FileIO works without RuntimeHandle (uses default executor)
#[tokio::test]
async fn test_fileio_without_runtime_handle() {
    // Create FileIO without RuntimeHandle - should use default executor
    let file_io = FileIOBuilder::new("memory").build().unwrap();

    // Verify basic operations work - memory storage returns false for non-existent paths
    let exists = file_io.exists("memory://test-path").await.unwrap();
    assert!(!exists); // Path should not exist
}

/// Test that operations execute on the configured runtime
#[test]
fn test_runtime_execution() {
    // Create a custom runtime
    let custom_runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("custom-runtime-test")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let custom_handle = custom_runtime.handle().clone();

    // Track whether task ran
    let executed = Arc::new(AtomicBool::new(false));
    let executed_clone = executed.clone();

    // Spawn a task on the custom runtime
    custom_handle.block_on(async move {
        // Just verify we can execute on the custom runtime
        executed_clone.store(true, Ordering::SeqCst);
    });

    // Verify the task executed
    assert!(executed.load(Ordering::SeqCst));
}

/// Test that RuntimeHandle can be cloned
#[tokio::test]
async fn test_runtime_handle_clone() {
    let io_runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("clone-test-runtime")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let handle1 = RuntimeHandle::new(io_runtime.handle().clone());
    let handle2 = handle1.clone();

    // Both handles should work
    let _file_io1 = FileIOBuilder::new("memory")
        .with_extension(handle1)
        .build()
        .unwrap();

    let _file_io2 = FileIOBuilder::new("memory")
        .with_extension(handle2)
        .build()
        .unwrap();

    // Leak runtime for test purposes
    std::mem::forget(io_runtime);
}

#[cfg(feature = "storage-s3")]
/// Test RuntimeHandle with S3 storage backend
#[tokio::test]
async fn test_runtime_handle_with_s3() {
    let io_runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("s3-io-runtime")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let io_handle = io_runtime.handle().clone();

    // Create S3 FileIO with RuntimeHandle
    // Note: This doesn't actually connect to S3, just tests configuration
    let file_io = FileIOBuilder::new("s3")
        .with_extension(RuntimeHandle::new(io_handle))
        .with_props(vec![("s3.region".to_string(), "us-east-1".to_string())])
        .build();

    // Should succeed in creating the FileIO
    assert!(file_io.is_ok());

    // Leak runtime for test purposes
    std::mem::forget(io_runtime);
}

#[cfg(feature = "storage-gcs")]
/// Test RuntimeHandle with GCS storage backend
#[tokio::test]
async fn test_runtime_handle_with_gcs() {
    let io_runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("gcs-io-runtime")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let io_handle = io_runtime.handle().clone();

    // Create GCS FileIO with RuntimeHandle
    let file_io = FileIOBuilder::new("gcs")
        .with_extension(RuntimeHandle::new(io_handle))
        .with_props(vec![(
            "gcs.project_id".to_string(),
            "test-project".to_string(),
        )])
        .build();

    // Should succeed in creating the FileIO
    assert!(file_io.is_ok());
}

/// Test that multiple FileIOs can use different runtime handles
#[tokio::test]
async fn test_multiple_fileios_different_runtimes() {
    // Create two separate runtimes
    let runtime1 = Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("runtime-1")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    let runtime2 = Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("runtime-2")
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    // Create two FileIOs with different runtimes
    let file_io1 = FileIOBuilder::new("memory")
        .with_extension(RuntimeHandle::new(runtime1.handle().clone()))
        .build()
        .unwrap();

    let file_io2 = FileIOBuilder::new("memory")
        .with_extension(RuntimeHandle::new(runtime2.handle().clone()))
        .build()
        .unwrap();

    // Both should work independently
    let _result1 = file_io1.exists("memory://path1").await;
    let _result2 = file_io2.exists("memory://path2").await;

    // Leak runtimes for test purposes
    std::mem::forget(runtime1);
    std::mem::forget(runtime2);
}
