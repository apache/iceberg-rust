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

use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use futures::executor::block_on;
use iceberg::encryption::{
    AesGcmCipher, AesGcmFileRead, AesGcmFileWrite, SecureKey, StandardKeyMetadata,
};
use iceberg::io::{FileMetadata, FileRead, FileWrite};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::error::to_py_err;

/// The encryption key, AAD prefix and file length needed to decrypt a single file.
#[pyclass(
    frozen,
    name = "StandardKeyMetadata",
    module = "pyiceberg_core.encryption"
)]
pub struct PyStandardKeyMetadata {
    // Python objects, so attribute access increfs rather than copying the key on every read.
    encryption_key: Py<PyBytes>,
    aad_prefix: Option<Py<PyBytes>>,
    file_length: Option<u64>,
}

#[pymethods]
impl PyStandardKeyMetadata {
    #[getter]
    fn encryption_key(&self, py: Python<'_>) -> Py<PyBytes> {
        self.encryption_key.clone_ref(py)
    }

    #[getter]
    fn aad_prefix(&self, py: Python<'_>) -> Option<Py<PyBytes>> {
        self.aad_prefix.as_ref().map(|prefix| prefix.clone_ref(py))
    }

    #[getter]
    fn file_length(&self) -> Option<u64> {
        self.file_length
    }

    /// Redacts the key, so logging an instance cannot leak key material.
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let key_length = self.encryption_key.bind(py).as_bytes().len();
        let aad_prefix = match &self.aad_prefix {
            Some(prefix) => prefix.bind(py).repr()?.to_string(),
            None => "None".to_string(),
        };
        let file_length = match self.file_length {
            Some(file_length) => file_length.to_string(),
            None => "None".to_string(),
        };

        Ok(format!(
            "StandardKeyMetadata(encryption_key=<redacted, {key_length} bytes>, aad_prefix={aad_prefix}, file_length={file_length})"
        ))
    }
}

/// Decode `StandardKeyMetadata` from its wire format.
#[pyfunction]
pub fn decode_standard_key_metadata(
    py: Python<'_>,
    data: &[u8],
) -> PyResult<PyStandardKeyMetadata> {
    let metadata = StandardKeyMetadata::decode(data).map_err(to_py_err)?;

    Ok(PyStandardKeyMetadata {
        encryption_key: PyBytes::new(py, metadata.encryption_key().as_bytes()).unbind(),
        aad_prefix: metadata
            .aad_prefix()
            .map(|aad_prefix| PyBytes::new(py, aad_prefix).unbind()),
        file_length: metadata.file_length(),
    })
}

/// Encode `StandardKeyMetadata` to its wire format.
#[pyfunction]
#[pyo3(signature = (encryption_key, aad_prefix=None, file_length=None))]
pub fn encode_standard_key_metadata<'py>(
    py: Python<'py>,
    encryption_key: &[u8],
    aad_prefix: Option<&[u8]>,
    file_length: Option<u64>,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut metadata = StandardKeyMetadata::try_new(encryption_key).map_err(to_py_err)?;

    if let Some(aad_prefix) = aad_prefix {
        metadata = metadata.with_aad_prefix(aad_prefix);
    }
    if let Some(file_length) = file_length {
        metadata = metadata.with_file_length(file_length);
    }

    Ok(PyBytes::new(py, &metadata.encode().map_err(to_py_err)?))
}

/// AES-GCM with a 128, 192 or 256-bit key, laid out as `nonce || ciphertext || tag`.
#[pyclass(frozen, name = "AesGcmCipher", module = "pyiceberg_core.encryption")]
pub struct PyAesGcmCipher {
    cipher: AesGcmCipher,
}

#[pymethods]
impl PyAesGcmCipher {
    #[classattr]
    const NONCE_LENGTH: usize = AesGcmCipher::NONCE_LEN;
    #[classattr]
    const TAG_LENGTH: usize = AesGcmCipher::TAG_LEN;

    #[new]
    fn new(key: &[u8]) -> PyResult<Self> {
        Ok(Self {
            cipher: new_cipher(key)?,
        })
    }

    /// Encrypt `plaintext` under a random nonce, authenticating `aad` alongside it.
    #[pyo3(signature = (plaintext, aad=None))]
    fn encrypt<'py>(
        &self,
        py: Python<'py>,
        plaintext: &[u8],
        aad: Option<&[u8]>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let ciphertext = self.cipher.encrypt(plaintext, aad).map_err(to_py_err)?;
        Ok(PyBytes::new(py, &ciphertext))
    }

    /// Decrypt `ciphertext`, verifying the `aad` it was encrypted with.
    #[pyo3(signature = (ciphertext, aad=None))]
    fn decrypt<'py>(
        &self,
        py: Python<'py>,
        ciphertext: &[u8],
        aad: Option<&[u8]>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let plaintext = self.cipher.decrypt(ciphertext, aad).map_err(to_py_err)?;
        Ok(PyBytes::new(py, &plaintext))
    }
}

/// Encrypt a whole file into an AGS1 stream.
///
/// The length of the returned stream is the `file_length` to record in the file's key metadata.
#[pyfunction]
#[pyo3(signature = (plaintext, encryption_key, *, aad_prefix=None))]
pub fn encrypt_ags1<'py>(
    py: Python<'py>,
    plaintext: &[u8],
    encryption_key: &[u8],
    aad_prefix: Option<&[u8]>,
) -> PyResult<Bound<'py, PyBytes>> {
    let cipher = Arc::new(new_cipher(encryption_key)?);
    let stream = BufferWrite::default();
    let mut writer = AesGcmFileWrite::new(
        Box::new(stream.clone()),
        cipher,
        aad_prefix.unwrap_or_default(),
    );
    let plaintext = Bytes::copy_from_slice(plaintext);

    py.detach(|| {
        block_on(async {
            writer.write(plaintext).await?;
            writer.close().await
        })
    })
    .map_err(to_py_err)?;

    Ok(PyBytes::new(py, &stream.take()))
}

/// Decrypt a whole AGS1 stream, authenticating every block.
///
/// `file_length` must come from the file's key metadata: a stream truncated at a block boundary
/// still authenticates, so only the trusted length exposes it.
///
/// # Errors
///
/// Raises `ValueError` if `data` is not `file_length` bytes, is not a valid AGS1 stream, or
/// fails authentication under `encryption_key` and `aad_prefix`.
#[pyfunction]
#[pyo3(signature = (data, encryption_key, *, file_length, aad_prefix=None))]
pub fn decrypt_ags1<'py>(
    py: Python<'py>,
    data: &[u8],
    encryption_key: &[u8],
    file_length: u64,
    aad_prefix: Option<&[u8]>,
) -> PyResult<Bound<'py, PyBytes>> {
    if data.len() as u64 != file_length {
        return Err(PyValueError::new_err(format!(
            "Invalid AGS1 stream: key metadata expects {file_length} bytes, got {}",
            data.len()
        )));
    }

    let cipher = Arc::new(new_cipher(encryption_key)?);
    let reader = AesGcmFileRead::new(
        Box::new(BufferRead(Bytes::copy_from_slice(data))),
        cipher,
        aad_prefix.unwrap_or_default().into(),
        file_length,
    )
    .map_err(to_py_err)?;

    let plaintext = py
        .detach(|| block_on(reader.read(0..reader.plaintext_length())))
        .map_err(to_py_err)?;

    Ok(PyBytes::new(py, &plaintext))
}

fn new_cipher(key: &[u8]) -> PyResult<AesGcmCipher> {
    Ok(AesGcmCipher::new(SecureKey::new(key).map_err(to_py_err)?))
}

struct BufferRead(Bytes);

#[async_trait::async_trait]
impl FileRead for BufferRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        if range.end > self.0.len() as u64 {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("Read range {range:?} exceeds {} bytes", self.0.len()),
            ));
        }
        Ok(self.0.slice(range.start as usize..range.end as usize))
    }
}

/// Collects what `AesGcmFileWrite` writes, which owns the writer it is given.
#[derive(Clone, Default)]
struct BufferWrite(Arc<Mutex<Vec<u8>>>);

impl BufferWrite {
    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

#[async_trait::async_trait]
impl FileWrite for BufferWrite {
    async fn write(&mut self, bs: Bytes) -> iceberg::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(&bs);
        Ok(())
    }

    async fn close(&mut self) -> iceberg::Result<FileMetadata> {
        let size = self.0.lock().unwrap_or_else(PoisonError::into_inner).len();
        Ok(FileMetadata { size: size as u64 })
    }
}

pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "encryption")?;

    this.add_class::<PyStandardKeyMetadata>()?;
    this.add_class::<PyAesGcmCipher>()?;
    this.add_function(wrap_pyfunction!(decode_standard_key_metadata, &this)?)?;
    this.add_function(wrap_pyfunction!(encode_standard_key_metadata, &this)?)?;
    this.add_function(wrap_pyfunction!(encrypt_ags1, &this)?)?;
    this.add_function(wrap_pyfunction!(decrypt_ags1, &this)?)?;

    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.encryption", this)
}
