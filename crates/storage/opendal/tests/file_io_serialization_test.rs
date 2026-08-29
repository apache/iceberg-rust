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

use std::sync::Arc;

use iceberg::io::{FileIO, FileIOBuilder, StorageFactory};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
#[cfg(any(
    feature = "opendal-memory",
    feature = "opendal-fs",
    feature = "opendal-s3",
    feature = "opendal-gcs",
    feature = "opendal-oss",
    feature = "opendal-azdls",
    feature = "opendal-hf"
))]
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde_json::{Value, json};

fn assert_file_io_roundtrip(factory: Arc<dyn StorageFactory>, expected_factory: Value) {
    let file_io = FileIOBuilder::new(factory)
        .with_prop("test-property", "test-value")
        .build();

    let serialized = serde_json::to_value(&file_io).unwrap();
    assert_eq!(
        serialized,
        json!({
            "config": {"props": {"test-property": "test-value"}},
            "factory": expected_factory
        })
    );

    let deserialized: FileIO = serde_json::from_value(serialized.clone()).unwrap();
    assert_eq!(
        deserialized.config().get("test-property"),
        Some(&"test-value".to_string())
    );
    assert_eq!(serde_json::to_value(deserialized).unwrap(), serialized);
}

#[test]
fn test_resolving_factory_serialization_roundtrip() {
    assert_file_io_roundtrip(
        Arc::new(OpenDalResolvingStorageFactory::new()),
        json!({"type": "OpenDalResolvingStorageFactory"}),
    );
}

#[cfg(feature = "opendal-memory")]
#[test]
fn test_memory_factory_serialization_roundtrip() {
    assert_file_io_roundtrip(
        Arc::new(OpenDalStorageFactory::Memory),
        json!({"type": "OpenDalStorageFactory", "Memory": null}),
    );
}

#[cfg(feature = "opendal-fs")]
#[test]
fn test_fs_factory_serialization_roundtrip() {
    assert_file_io_roundtrip(
        Arc::new(OpenDalStorageFactory::Fs),
        json!({"type": "OpenDalStorageFactory", "Fs": null}),
    );
}

#[cfg(feature = "opendal-s3")]
#[test]
fn test_s3_factory_serialization_roundtrip() {
    assert_file_io_roundtrip(
        Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }),
        json!({
            "type": "OpenDalStorageFactory",
            "S3": {}
        }),
    );
}

#[cfg(feature = "opendal-gcs")]
#[test]
fn test_gcs_factory_serialization_roundtrip() {
    assert_file_io_roundtrip(
        Arc::new(OpenDalStorageFactory::Gcs),
        json!({"type": "OpenDalStorageFactory", "Gcs": null}),
    );
}

#[cfg(feature = "opendal-oss")]
#[test]
fn test_oss_factory_serialization_roundtrip() {
    assert_file_io_roundtrip(
        Arc::new(OpenDalStorageFactory::Oss),
        json!({"type": "OpenDalStorageFactory", "Oss": null}),
    );
}

#[cfg(feature = "opendal-azdls")]
#[test]
fn test_azdls_factory_serialization_roundtrip() {
    assert_file_io_roundtrip(
        Arc::new(OpenDalStorageFactory::Azdls),
        json!({"type": "OpenDalStorageFactory", "Azdls": null}),
    );
}

#[cfg(feature = "opendal-hf")]
#[test]
fn test_hf_factory_serialization_roundtrip() {
    assert_file_io_roundtrip(
        Arc::new(OpenDalStorageFactory::Hf),
        json!({"type": "OpenDalStorageFactory", "Hf": null}),
    );
}

#[cfg(feature = "opendal-s3")]
mod credential_loader_tests {
    use iceberg_storage_opendal::{AwsCredential, CustomAwsCredentialLoader, ProvideCredential};
    use reqsign_core::Context;

    use super::*;

    #[derive(Debug)]
    struct EmptyCredentialLoader;

    impl ProvideCredential for EmptyCredentialLoader {
        type Credential = AwsCredential;

        async fn provide_credential(
            &self,
            _ctx: &Context,
        ) -> reqsign_core::Result<Option<AwsCredential>> {
            Ok(None)
        }
    }

    fn loader() -> CustomAwsCredentialLoader {
        CustomAwsCredentialLoader::new(EmptyCredentialLoader)
    }

    #[test]
    fn test_s3_factory_does_not_serialize_custom_credential_loader() {
        let with_loader = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: Some(loader()),
        }))
        .build();
        let without_loader = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .build();

        assert_eq!(
            serde_json::to_value(with_loader).unwrap(),
            serde_json::to_value(without_loader).unwrap()
        );
    }

    #[test]
    fn test_resolving_factory_does_not_serialize_custom_credential_loader() {
        let with_loader = FileIOBuilder::new(Arc::new(
            OpenDalResolvingStorageFactory::new().with_s3_credential_loader(loader()),
        ))
        .build();
        let without_loader =
            FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new())).build();

        assert_eq!(
            serde_json::to_value(with_loader).unwrap(),
            serde_json::to_value(without_loader).unwrap()
        );
    }
}
