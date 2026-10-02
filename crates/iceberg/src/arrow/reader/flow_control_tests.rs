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

//! A scan must not stall when every read shares one connection.
//!
//! Object-store clients send concurrent requests over a single HTTP/2
//! connection; GCS negotiates HTTP/2, for example. On such a connection the
//! server may only send so far ahead of what the client has read, an allowance
//! HTTP/2 calls the connection window. The server fills a response whether or
//! not anyone is reading it, and the window comes back only once the client
//! consumes the bytes. So a request nobody reads holds part of the window for
//! good, and enough of them stop the server sending for any other request.
//!
//! [`FlowControlledStorage`] models that connection for in-memory files.

use std::collections::BTreeMap;
use std::future::Future;
use std::io::Cursor;
use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::arrow::ArrowReaderBuilder;
use crate::io::{
    FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, MemoryStorage, OutputFile,
    Storage, StorageConfig, StorageFactory,
};
use crate::scan::{FileScanTask, FileScanTaskStream};
use crate::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use crate::{Result, Runtime};

/// Bytes the server sends per turn, like one HTTP/2 DATA frame.
const FRAME_SIZE: usize = 1024;

/// One HTTP/2 connection that every read shares.
#[derive(Debug, Default)]
struct Connection {
    state: Mutex<ConnectionState>,
    /// Wakes the server when a request arrives or the window reopens.
    server_wakeup: Notify,
}

#[derive(Debug, Default)]
struct ConnectionState {
    /// Bytes the server may still send before the client consumes some.
    window: usize,
    /// Open responses by stream id, in request order.
    responses: BTreeMap<u64, Response>,
    next_stream_id: u64,
    /// The stream the server sends to next; it serves streams round-robin.
    next_to_send: u64,
}

#[derive(Debug)]
struct Response {
    len: usize,
    /// Bytes the server has pushed into the receive buffer.
    sent: usize,
    /// Bytes the reader has consumed. `sent - consumed` holds window capacity.
    consumed: usize,
    reader: Option<Waker>,
}

impl Connection {
    fn new(window: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ConnectionState {
                window,
                ..Default::default()
            }),
            server_wakeup: Notify::new(),
        })
    }

    fn window(&self) -> usize {
        self.state.lock().unwrap().window
    }

    /// One line per open response: how much arrived, how much was read.
    fn describe_open_responses(&self) -> String {
        let state = self.state.lock().unwrap();
        state
            .responses
            .iter()
            .map(|(stream_id, response)| {
                format!(
                    "stream {stream_id}: {} of {} bytes received, {} consumed",
                    response.sent, response.len, response.consumed
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Sends a request whose response is `len` bytes; returns its stream id.
    fn request(&self, len: usize) -> u64 {
        let mut state = self.state.lock().unwrap();
        let stream_id = state.next_stream_id;
        state.next_stream_id += 1;
        state.responses.insert(stream_id, Response {
            len,
            sent: 0,
            consumed: 0,
            reader: None,
        });
        drop(state);
        self.server_wakeup.notify_one();
        stream_id
    }

    /// Consumes every byte received on `stream_id`, returning them to the
    /// window. Ready once the whole response has been consumed.
    fn poll_consume(&self, stream_id: u64, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.state.lock().unwrap();
        let response = state
            .responses
            .get_mut(&stream_id)
            .expect("a response is consumed only while its stream is open");
        let received = response.sent - response.consumed;
        response.consumed = response.sent;
        let complete = response.consumed == response.len;
        if complete {
            state.responses.remove(&stream_id);
        } else {
            response.reader = Some(cx.waker().clone());
        }
        state.window += received;
        drop(state);

        if received > 0 {
            self.server_wakeup.notify_one();
        }
        if complete {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    /// Resets `stream_id`, returning the bytes it received but never consumed
    /// to the window.
    fn reset(&self, stream_id: u64) {
        let mut state = self.state.lock().unwrap();
        if let Some(response) = state.responses.remove(&stream_id) {
            state.window += response.sent - response.consumed;
        }
        drop(state);
        self.server_wakeup.notify_one();
    }

    /// Sends one frame to the next stream, round-robin, if the window allows.
    /// Returns `None` when the server has nothing it may send; otherwise the
    /// waker of the reader that received the frame, if that reader is waiting.
    fn send_frame(&self) -> Option<Option<Waker>> {
        let mut state = self.state.lock().unwrap();
        if state.window == 0 {
            return None;
        }
        let next_to_send = state.next_to_send;
        let stream_id = state
            .responses
            .range(next_to_send..)
            .chain(state.responses.range(..next_to_send))
            .find(|(_, response)| response.sent < response.len)
            .map(|(stream_id, _)| *stream_id)?;

        let window = state.window;
        let response = state.responses.get_mut(&stream_id).unwrap();
        let frame = FRAME_SIZE.min(response.len - response.sent).min(window);
        response.sent += frame;
        let reader = response.reader.take();
        state.window -= frame;
        state.next_to_send = stream_id + 1;
        Some(reader)
    }

    /// The server: one frame per turn, so responses arrive over many turns and
    /// the client runs in between, as over a real network.
    async fn serve(self: Arc<Self>) {
        loop {
            match self.send_frame() {
                Some(reader) => {
                    if let Some(reader) = reader {
                        reader.wake();
                    }
                    tokio::task::yield_now().await;
                }
                None => self.server_wakeup.notified().await,
            }
        }
    }
}

/// A response in flight: resolves once its reader has consumed every byte.
struct ResponseFuture {
    connection: Arc<Connection>,
    stream_id: u64,
    complete: bool,
}

impl Future for ResponseFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let poll = self.connection.poll_consume(self.stream_id, cx);
        self.complete = poll.is_ready();
        poll
    }
}

impl Drop for ResponseFuture {
    fn drop(&mut self) {
        if !self.complete {
            self.connection.reset(self.stream_id);
        }
    }
}

/// Reads one file over the shared connection.
struct FlowControlledFileRead {
    connection: Arc<Connection>,
    contents: Bytes,
}

#[async_trait]
impl FileRead for FlowControlledFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let body = self
            .contents
            .slice(range.start as usize..range.end as usize);
        ResponseFuture {
            connection: Arc::clone(&self.connection),
            stream_id: self.connection.request(body.len()),
            complete: false,
        }
        .await;
        Ok(body)
    }
}

/// In-memory files whose readers share one flow-controlled [`Connection`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FlowControlledStorage {
    #[serde(skip)]
    files: MemoryStorage,
    #[serde(skip)]
    connection: Arc<Connection>,
}

#[async_trait]
#[typetag::serde]
impl Storage for FlowControlledStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.files.exists(path).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.files.metadata(path).await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.files.read(path).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        Ok(Box::new(FlowControlledFileRead {
            connection: Arc::clone(&self.connection),
            contents: self.files.read(path).await?,
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.files.write(path, bs).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.files.writer(path).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.files.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.files.delete_prefix(path).await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.files.delete_stream(paths).await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct FlowControlledStorageFactory {
    #[serde(skip)]
    storage: FlowControlledStorage,
}

#[typetag::serde]
impl StorageFactory for FlowControlledStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(self.storage.clone()))
    }
}

/// Writes `num_files` single-row-group Parquet files of `rows_per_file` rows
/// each and returns a scan task per file.
async fn write_files(
    storage: &FlowControlledStorage,
    num_files: usize,
    rows_per_file: i64,
) -> Result<Vec<FileScanTask>> {
    let schema = Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "value", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()?,
    );
    let field = |name: &str, id: &str| {
        Field::new(name, DataType::Int64, false)
            .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())].into())
    };
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        field("id", "1"),
        field("value", "2"),
    ]));
    // Plain, uncompressed pages keep every file the same, predictable size.
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();

    let mut tasks = Vec::with_capacity(num_files);
    for file_num in 0..num_files as i64 {
        let first_id = file_num * rows_per_file;
        let columns = vec![
            Arc::new(Int64Array::from_iter_values(
                first_id..first_id + rows_per_file,
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(0..rows_per_file)) as ArrayRef,
        ];
        let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), columns)?;

        let mut contents = Cursor::new(Vec::new());
        let mut writer = ArrowWriter::try_new(
            &mut contents,
            Arc::clone(&arrow_schema),
            Some(props.clone()),
        )?;
        writer.write(&batch)?;
        writer.close()?;
        let contents = Bytes::from(contents.into_inner());

        let path = format!("memory:/data/file_{file_num}.parquet");
        tasks.push(
            FileScanTask::builder()
                .with_data_file_path(path.clone())
                .with_file_size_in_bytes(contents.len() as u64)
                .with_start(0)
                .with_length(0)
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(Arc::clone(&schema))
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .build()
                .unwrap(),
        );
        storage.write(&path, contents).await?;
    }
    Ok(tasks)
}

/// Reads a dozen files, `concurrency` at a time, over a connection whose window
/// holds two footer prefetches. Every file is larger than the window, so a
/// scan completes only if it keeps consuming what the server sends.
async fn scan_over_shared_connection(concurrency: usize) {
    const NUM_FILES: usize = 12;
    const ROWS_PER_FILE: i64 = 1536;
    const METADATA_SIZE_HINT: usize = 8 * 1024;
    const WINDOW: usize = 2 * METADATA_SIZE_HINT;

    let connection = Connection::new(WINDOW);
    let storage = FlowControlledStorage {
        files: MemoryStorage::new(),
        connection: Arc::clone(&connection),
    };
    let tasks = write_files(&storage, NUM_FILES, ROWS_PER_FILE)
        .await
        .unwrap();
    assert!(
        tasks
            .iter()
            .all(|task| task.file_size_in_bytes() > WINDOW as u64),
        "every file must be larger than the connection window"
    );

    let server = tokio::spawn(Arc::clone(&connection).serve());
    let file_io = FileIOBuilder::new(Arc::new(FlowControlledStorageFactory { storage })).build();
    let reader = ArrowReaderBuilder::new(file_io, Runtime::current())
        .with_data_file_concurrency_limit(concurrency)
        .with_metadata_size_hint(METADATA_SIZE_HINT)
        .build();
    let tasks = Box::pin(futures::stream::iter(tasks.into_iter().map(Ok))) as FileScanTaskStream;

    let scan = reader
        .read(tasks)
        .unwrap()
        .stream()
        .try_collect::<Vec<RecordBatch>>();
    tokio::pin!(scan);
    // Select rather than `timeout`, so the stalled scan still holds its
    // streams when the connection state is reported.
    let batches = tokio::select! {
        batches = &mut scan => batches.unwrap(),
        _ = tokio::time::sleep(Duration::from_secs(10)) => {
            let open_responses = connection.describe_open_responses();
            panic!(
                "scan stalled with {} of {WINDOW} window bytes unconsumed; {open_responses}",
                WINDOW - connection.window()
            );
        }
    };
    server.abort();

    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, NUM_FILES * ROWS_PER_FILE as usize);
    assert_eq!(
        connection.window(),
        WINDOW,
        "a completed scan leaves no unconsumed bytes on the connection"
    );
}

/// A control, not a guard: concurrency 1 takes the reader's other branch and
/// passes either way. It shows the harness itself does not stall.
#[tokio::test]
async fn test_sequential_scan_over_shared_connection() {
    scan_over_shared_connection(1).await;
}

/// Concurrency 4 sits inside the range that stalls, 3 to 10. Below it the
/// window still covers the orphaned footers; above it too few files are left
/// over to orphan.
#[tokio::test]
async fn test_concurrent_scan_does_not_starve_shared_connection() {
    scan_over_shared_connection(4).await;
}
