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

//! Manifest-list compatibility and peak live-allocation regression.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use apache_avro::types::Value;
use apache_avro::{Codec, Reader, Writer};
use iceberg::io::FileIO;
use iceberg::spec::{
    FieldSummary, FormatVersion, ManifestContentType, ManifestFile, ManifestList,
    ManifestListWriter,
};

struct CountingAllocator;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn allocated(bytes: usize) {
    PEAK.fetch_max(LIVE.fetch_add(bytes, Relaxed) + bytes, Relaxed);
}

// The allocator is confined to this test executable. Every allocation is tracked,
// including those made before measurement, so deallocation cannot underflow LIVE.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, size) };
        if !new_ptr.is_null() {
            LIVE.fetch_sub(layout.size(), Relaxed);
            allocated(size);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn entry(version: FormatVersion, index: i64) -> ManifestFile {
    ManifestFile {
        manifest_path: format!("s3://test/metadata/manifest-{index}.avro"),
        manifest_length: 1234,
        partition_spec_id: index as i32,
        content: ManifestContentType::Data,
        sequence_number: if version == FormatVersion::V1 { 0 } else { 7 },
        min_sequence_number: if version == FormatVersion::V1 { 0 } else { 3 },
        added_snapshot_id: 42,
        added_files_count: Some(1),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(9),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: Some(vec![FieldSummary {
            contains_null: true,
            contains_nan: None,
            lower_bound: Some(vec![1, 0, 0, 0].into()),
            upper_bound: None,
        }]),
        key_metadata: Some(vec![1, 2, 3]),
        first_row_id: (version == FormatVersion::V3).then_some(100 + index as u64 * 9),
    }
}

async fn encode(version: FormatVersion, entries: Vec<ManifestFile>) -> Vec<u8> {
    let io = FileIO::new_with_memory();
    let output = io.new_output("memory:///manifest-list.avro").unwrap();
    let writer = output.writer().await.unwrap();
    let mut writer = match version {
        FormatVersion::V1 => ManifestListWriter::v1(writer, 42, None),
        FormatVersion::V2 => ManifestListWriter::v2(writer, 42, None, 7),
        FormatVersion::V3 => ManifestListWriter::v3(writer, 42, None, 7, Some(100)),
    };
    writer.add_manifests(entries.into_iter()).unwrap();
    writer.close().await.unwrap();
    io.new_input("memory:///manifest-list.avro")
        .unwrap()
        .read()
        .await
        .unwrap()
        .to_vec()
}

// A single test keeps the allocation measurement isolated from parallel tests.
#[test]
fn manifest_parser_preserves_formats_and_bounds_intermediate_allocations() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for version in [FormatVersion::V1, FormatVersion::V2, FormatVersion::V3] {
        let expected = vec![entry(version, 0), entry(version, 1)];
        let encoded = runtime.block_on(encode(version, expected.clone()));
        assert_eq!(
            ManifestList::parse_with_version(&encoded, version)
                .unwrap()
                .entries(),
            expected
        );
        let empty = runtime.block_on(encode(version, vec![]));
        assert!(
            ManifestList::parse_with_version(&empty, version)
                .unwrap()
                .entries()
                .is_empty()
        );
        // Truncating the final sync marker must not return a partial list.
        assert!(ManifestList::parse_with_version(&encoded[..encoded.len() - 1], version).is_err());
        if version == FormatVersion::V1 {
            // Reading a V1 file as V2 must retain the version-default behavior.
            assert_eq!(
                ManifestList::parse_with_version(&encoded, FormatVersion::V2)
                    .unwrap()
                    .entries(),
                expected
            );
        }
    }

    let seed = runtime.block_on(encode(FormatVersion::V2, vec![entry(FormatVersion::V2, 0)]));
    let mut reader = Reader::new(seed.as_slice()).unwrap();
    let schema = reader.writer_schema().clone();
    let mut value = reader.next().unwrap().unwrap();
    for codec in [
        Codec::Null,
        Codec::Snappy,
        Codec::Zstandard(Default::default()),
    ] {
        let mut writer = Writer::with_codec(&schema, Vec::new(), codec);
        for index in 0..10_000 {
            let Value::Record(fields) = &mut value else {
                panic!("expected a manifest record");
            };
            let (_, path) = fields
                .iter_mut()
                .find(|(name, _)| name == "manifest_path")
                .unwrap();
            *path = Value::String(format!("s3://test/metadata/manifest-{index:05}.avro"));
            writer.append(value.clone()).unwrap();
        }
        let encoded = writer.into_inner().unwrap();
        let baseline = LIVE.load(Relaxed);
        PEAK.store(baseline, Relaxed);
        let started = Instant::now();
        let parsed = ManifestList::parse_with_version(&encoded, FormatVersion::V2).unwrap();
        let elapsed = started.elapsed();
        let peak = PEAK.load(Relaxed).saturating_sub(baseline);
        assert_eq!(parsed.entries().len(), 10_000);
        assert!(parsed.entries().iter().enumerate().all(|(index, actual)| {
            let mut expected = entry(FormatVersion::V2, 0);
            expected.manifest_path = format!("s3://test/metadata/manifest-{index:05}.avro");
            actual == &expected
        }));
        // The original whole-Value-array parser exceeds this budget. This bounds
        // live allocations in the parser; it is deliberately not an RSS promise.
        eprintln!(
            "10,000 entries, {codec:?}: {} encoded bytes, {peak} bytes peak live allocations, {elapsed:?}",
            encoded.len()
        );
        assert!(peak < 16 * 1024 * 1024, "parser peak: {peak} bytes");
    }
}
