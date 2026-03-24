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

//! Core cryptographic operations for Iceberg encryption.

use std::fmt;
use std::str::FromStr;

use aes_gcm::aead::generic_array::typenum::U12;
use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes128Gcm, Aes256Gcm, AesGcm, Nonce};
use zeroize::Zeroizing;

/// AES-192-GCM with 96-bit nonce. Not provided by `aes-gcm` but constructible
/// from the underlying primitives, same as `Aes128Gcm` and `Aes256Gcm`.
type Aes192Gcm = AesGcm<aes_gcm::aes::Aes192, U12>;

use crate::{Error, ErrorKind, Result};

/// Wrapper for sensitive byte data (encryption keys, DEKs, etc.) that:
/// - Zeroizes memory on drop
/// - Redacts content in [`Debug`] and [`Display`] output
/// - Provides only `&[u8]` access via [`as_bytes()`](Self::as_bytes)
/// - Uses `Box<[u8]>` (immutable boxed slice) since key bytes never grow
///
/// Use this type for any struct field that holds plaintext key material.
/// Because its [`Debug`] impl always prints `[N bytes REDACTED]`, structs
/// containing `SensitiveBytes` can safely derive or implement `Debug`
/// without risk of leaking key material.
#[derive(Clone, PartialEq, Eq)]
struct SensitiveBytes(Zeroizing<Box<[u8]>>);

impl SensitiveBytes {
    /// Wraps the given bytes as sensitive material.
    pub fn new(bytes: impl Into<Box<[u8]>>) -> Self {
        Self(Zeroizing::new(bytes.into()))
    }

    /// Returns the underlying bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the number of bytes.
    #[allow(dead_code)] // Encryption work is ongoing so currently unused
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the byte slice is empty.
    #[allow(dead_code)] // Encryption work is ongoing so currently unused
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SensitiveBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} bytes REDACTED]", self.0.len())
    }
}

impl fmt::Display for SensitiveBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} bytes REDACTED]", self.0.len())
    }
}

/// Supported AES key sizes for AES-GCM encryption.
///
/// The Iceberg spec supports 128, 192, and 256-bit keys for AES-GCM.
/// See: <https://iceberg.apache.org/gcm-stream-spec/#goals>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AesKeySize {
    /// 128-bit AES key (16 bytes)
    Bits128 = 128,
    /// 192-bit AES key (24 bytes)
    Bits192 = 192,
    /// 256-bit AES key (32 bytes)
    Bits256 = 256,
}

impl AesKeySize {
    /// Returns the key length in bytes for this key size.
    pub fn key_length(&self) -> usize {
        match self {
            Self::Bits128 => 16,
            Self::Bits192 => 24,
            Self::Bits256 => 32,
        }
    }

    /// Returns the key size for a given DEK length in bytes.
    ///
    /// Matches Java's `encryption.data-key-length` property semantics:
    /// 16 → 128-bit, 24 → 192-bit, 32 → 256-bit.
    pub fn from_key_length(len: usize) -> Result<Self> {
        match len {
            16 => Ok(Self::Bits128),
            24 => Ok(Self::Bits192),
            32 => Ok(Self::Bits256),
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unsupported data key length: {len} (must be 16, 24, or 32)"),
            )),
        }
    }
}

impl FromStr for AesKeySize {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "128" | "AES_GCM_128" | "AES128_GCM" => Ok(Self::Bits128),
            "192" | "AES_GCM_192" | "AES192_GCM" => Ok(Self::Bits192),
            "256" | "AES_GCM_256" | "AES256_GCM" => Ok(Self::Bits256),
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Unsupported AES key size: {s}"),
            )),
        }
    }
}

/// A secure encryption key that zeroes its memory on drop.
pub struct SecureKey {
    key: SensitiveBytes,
    key_size: AesKeySize,
}

impl SecureKey {
    /// Creates a new secure key with the specified key size.
    ///
    /// # Errors
    /// Returns an error if the key length doesn't match the key size requirements.
    pub fn new(key: &[u8]) -> Result<Self> {
        let key_size = AesKeySize::from_key_length(key.len())?;
        Ok(Self {
            key: SensitiveBytes::new(key),
            key_size,
        })
    }

    /// Generates a new random key for the specified key size.
    pub fn generate(key_size: AesKeySize) -> Self {
        let mut key = vec![0u8; key_size.key_length()];
        OsRng.fill_bytes(&mut key);
        Self {
            key: SensitiveBytes::new(key),
            key_size,
        }
    }

    /// Returns the AES key size.
    pub fn key_size(&self) -> AesKeySize {
        self.key_size
    }

    /// Returns the key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.key.as_bytes()
    }
}

/// AES-GCM cipher for encrypting and decrypting data.
pub struct AesGcmCipher {
    key: SensitiveBytes,
    key_size: AesKeySize,
}

impl AesGcmCipher {
    /// AES-GCM nonce length in bytes (96 bits).
    pub const NONCE_LEN: usize = 12;
    /// AES-GCM authentication tag length in bytes (128 bits).
    pub const TAG_LEN: usize = 16;

    /// Creates a new cipher with the specified key.
    pub fn new(key: SecureKey) -> Self {
        Self {
            key: SensitiveBytes::new(key.as_bytes()),
            key_size: key.key_size(),
        }
    }

    /// Encrypts data using AES-GCM.
    ///
    /// # Arguments
    /// * `plaintext` - The data to encrypt
    /// * `aad` - Additional authenticated data (optional)
    ///
    /// # Returns
    /// The encrypted data in the format: [12-byte nonce][ciphertext][16-byte auth tag]
    /// This matches the Java implementation format for compatibility.
    pub fn encrypt(&self, plaintext: &[u8], aad: Option<&[u8]>) -> Result<Vec<u8>> {
        match self.key_size {
            AesKeySize::Bits128 => {
                encrypt_aes_gcm::<Aes128Gcm>(self.key.as_bytes(), plaintext, aad)
            }
            AesKeySize::Bits192 => {
                encrypt_aes_gcm::<Aes192Gcm>(self.key.as_bytes(), plaintext, aad)
            }
            AesKeySize::Bits256 => {
                encrypt_aes_gcm::<Aes256Gcm>(self.key.as_bytes(), plaintext, aad)
            }
        }
    }

    /// Decrypts data using AES-GCM.
    ///
    /// # Arguments
    /// * `ciphertext` - The encrypted data with format: [12-byte nonce][encrypted data][16-byte auth tag]
    /// * `aad` - Additional authenticated data (must match encryption)
    ///
    /// # Returns
    /// The decrypted plaintext.
    pub fn decrypt(&self, ciphertext: &[u8], aad: Option<&[u8]>) -> Result<Vec<u8>> {
        if ciphertext.len() < Self::NONCE_LEN + Self::TAG_LEN {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "Ciphertext too short: expected at least {} bytes, got {}",
                    Self::NONCE_LEN + Self::TAG_LEN,
                    ciphertext.len()
                ),
            ));
        }

        match self.key_size {
            AesKeySize::Bits128 => {
                decrypt_aes_gcm::<Aes128Gcm>(self.key.as_bytes(), ciphertext, aad)
            }
            AesKeySize::Bits192 => {
                decrypt_aes_gcm::<Aes192Gcm>(self.key.as_bytes(), ciphertext, aad)
            }
            AesKeySize::Bits256 => {
                decrypt_aes_gcm::<Aes256Gcm>(self.key.as_bytes(), ciphertext, aad)
            }
        }
    }
}

fn encrypt_aes_gcm<C>(key_bytes: &[u8], plaintext: &[u8], aad: Option<&[u8]>) -> Result<Vec<u8>>
where C: Aead + AeadCore + KeyInit {
    let cipher = C::new_from_slice(key_bytes).map_err(|e| {
        Error::new(ErrorKind::DataInvalid, "Invalid AES key").with_source(anyhow::anyhow!(e))
    })?;
    let nonce = C::generate_nonce(&mut OsRng);

    let ciphertext = if let Some(aad) = aad {
        cipher.encrypt(&nonce, Payload {
            msg: plaintext,
            aad,
        })
    } else {
        cipher.encrypt(&nonce, plaintext.as_ref())
    }
    .map_err(|e| {
        Error::new(ErrorKind::Unexpected, "AES-GCM encryption failed")
            .with_source(anyhow::anyhow!(e))
    })?;

    // Prepend nonce to ciphertext (Java compatible format)
    let mut result = Vec::with_capacity(nonce.len() + ciphertext.len());
    result.extend_from_slice(&nonce);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

fn decrypt_aes_gcm<C>(key_bytes: &[u8], ciphertext: &[u8], aad: Option<&[u8]>) -> Result<Vec<u8>>
where C: Aead + AeadCore + KeyInit {
    let cipher = C::new_from_slice(key_bytes).map_err(|e| {
        Error::new(ErrorKind::DataInvalid, "Invalid AES key").with_source(anyhow::anyhow!(e))
    })?;

    let nonce = Nonce::from_slice(&ciphertext[..AesGcmCipher::NONCE_LEN]);
    let encrypted_data = &ciphertext[AesGcmCipher::NONCE_LEN..];

    let plaintext = if let Some(aad) = aad {
        cipher.decrypt(nonce, Payload {
            msg: encrypted_data,
            aad,
        })
    } else {
        cipher.decrypt(nonce, encrypted_data)
    }
    .map_err(|e| {
        Error::new(ErrorKind::Unexpected, "AES-GCM decryption failed")
            .with_source(anyhow::anyhow!(e))
    })?;

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aes_key_size() {
        assert_eq!(AesKeySize::Bits128.key_length(), 16);
        assert_eq!(AesKeySize::Bits192.key_length(), 24);
        assert_eq!(AesKeySize::Bits256.key_length(), 32);

        assert_eq!(
            AesKeySize::from_key_length(16).unwrap(),
            AesKeySize::Bits128
        );
        assert_eq!(
            AesKeySize::from_key_length(24).unwrap(),
            AesKeySize::Bits192
        );
        assert_eq!(
            AesKeySize::from_key_length(32).unwrap(),
            AesKeySize::Bits256
        );
        assert!(AesKeySize::from_key_length(8).is_err());

        assert_eq!(AesKeySize::from_str("128").unwrap(), AesKeySize::Bits128);
        assert_eq!(
            AesKeySize::from_str("AES_GCM_128").unwrap(),
            AesKeySize::Bits128
        );
        assert_eq!(
            AesKeySize::from_str("AES_GCM_256").unwrap(),
            AesKeySize::Bits256
        );
        assert!(AesKeySize::from_str("INVALID").is_err());
    }

    #[test]
    fn test_secure_key() {
        // Test key generation
        let key1 = SecureKey::generate(AesKeySize::Bits128);
        assert_eq!(key1.as_bytes().len(), 16);
        assert_eq!(key1.key_size(), AesKeySize::Bits128);

        // Test key creation with validation
        let valid_key = [0u8; 16];
        assert!(SecureKey::new(valid_key.as_slice()).is_ok());

        let invalid_key = [0u8; 33];
        assert!(SecureKey::new(invalid_key.as_slice()).is_err());
    }

    #[test]
    fn test_aes128_gcm_encryption_roundtrip() {
        let key = SecureKey::generate(AesKeySize::Bits128);
        let cipher = AesGcmCipher::new(key);

        let plaintext = b"Hello, Iceberg encryption!";
        let aad = b"additional authenticated data";

        // Test without AAD
        let ciphertext = cipher.encrypt(plaintext, None).unwrap();
        assert!(ciphertext.len() > plaintext.len() + 12); // nonce + tag
        assert_ne!(&ciphertext[12..], plaintext); // encrypted portion differs

        let decrypted = cipher.decrypt(&ciphertext, None).unwrap();
        assert_eq!(decrypted, plaintext);

        // Test with AAD
        let ciphertext = cipher.encrypt(plaintext, Some(aad)).unwrap();
        let decrypted = cipher.decrypt(&ciphertext, Some(aad)).unwrap();
        assert_eq!(decrypted, plaintext);

        // Test with wrong AAD fails
        assert!(cipher.decrypt(&ciphertext, Some(b"wrong aad")).is_err());
    }

    #[test]
    fn test_aes192_gcm_encryption_roundtrip() {
        let key = SecureKey::generate(AesKeySize::Bits192);
        let cipher = AesGcmCipher::new(key);

        let plaintext = b"Hello, Iceberg encryption!";
        let aad = b"additional authenticated data";

        // Test without AAD
        let ciphertext = cipher.encrypt(plaintext, None).unwrap();
        let decrypted = cipher.decrypt(&ciphertext, None).unwrap();
        assert_eq!(decrypted, plaintext);

        // Test with AAD
        let ciphertext = cipher.encrypt(plaintext, Some(aad)).unwrap();
        let decrypted = cipher.decrypt(&ciphertext, Some(aad)).unwrap();
        assert_eq!(decrypted, plaintext);

        // Test with wrong AAD fails
        assert!(cipher.decrypt(&ciphertext, Some(b"wrong aad")).is_err());
    }

    #[test]
    fn test_aes256_gcm_encryption_roundtrip() {
        let key = SecureKey::generate(AesKeySize::Bits256);
        let cipher = AesGcmCipher::new(key);

        let plaintext = b"Hello, Iceberg encryption!";
        let aad = b"additional authenticated data";

        // Test without AAD
        let ciphertext = cipher.encrypt(plaintext, None).unwrap();
        let decrypted = cipher.decrypt(&ciphertext, None).unwrap();
        assert_eq!(decrypted, plaintext);

        // Test with AAD
        let ciphertext = cipher.encrypt(plaintext, Some(aad)).unwrap();
        let decrypted = cipher.decrypt(&ciphertext, Some(aad)).unwrap();
        assert_eq!(decrypted, plaintext);

        // Test with wrong AAD fails
        assert!(cipher.decrypt(&ciphertext, Some(b"wrong aad")).is_err());
    }

    #[test]
    fn test_cross_key_size_incompatibility() {
        let plaintext = b"Cross-key test";

        let key128 = SecureKey::generate(AesKeySize::Bits128);
        let key256 = SecureKey::generate(AesKeySize::Bits256);

        let cipher128 = AesGcmCipher::new(key128);
        let cipher256 = AesGcmCipher::new(key256);

        // Ciphertext from 128-bit key should not decrypt with 256-bit key
        let ciphertext = cipher128.encrypt(plaintext, None).unwrap();
        assert!(cipher256.decrypt(&ciphertext, None).is_err());
    }

    #[test]
    fn test_encryption_with_empty_plaintext() {
        let key = SecureKey::generate(AesKeySize::Bits128);
        let cipher = AesGcmCipher::new(key);

        let plaintext = b"";
        let ciphertext = cipher.encrypt(plaintext, None).unwrap();

        // Even empty plaintext produces nonce + tag
        assert_eq!(ciphertext.len(), 12 + 16); // 12-byte nonce + 16-byte tag

        let decrypted = cipher.decrypt(&ciphertext, None).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_decryption_with_tampered_ciphertext() {
        let key = SecureKey::generate(AesKeySize::Bits128);
        let cipher = AesGcmCipher::new(key);

        let plaintext = b"Sensitive data";
        let mut ciphertext = cipher.encrypt(plaintext, None).unwrap();

        // Tamper with the encrypted portion (after the nonce)
        if ciphertext.len() > 12 {
            ciphertext[12] ^= 0xFF;
        }

        // Decryption should fail due to authentication tag mismatch
        assert!(cipher.decrypt(&ciphertext, None).is_err());
    }

    #[test]
    fn test_different_keys_produce_different_ciphertexts() {
        let key1 = SecureKey::generate(AesKeySize::Bits128);
        let key2 = SecureKey::generate(AesKeySize::Bits128);

        let cipher1 = AesGcmCipher::new(key1);
        let cipher2 = AesGcmCipher::new(key2);

        let plaintext = b"Same plaintext";

        let ciphertext1 = cipher1.encrypt(plaintext, None).unwrap();
        let ciphertext2 = cipher2.encrypt(plaintext, None).unwrap();

        // Different keys should produce different ciphertexts (comparing the encrypted portion)
        // Note: The nonces will also be different, but we're mainly interested in the encrypted data
        assert_ne!(&ciphertext1[12..], &ciphertext2[12..]);
    }

    #[test]
    fn test_ciphertext_format_java_compatible() {
        // Test that our ciphertext format matches Java's: [12-byte nonce][ciphertext][16-byte tag]
        let key = SecureKey::generate(AesKeySize::Bits128);
        let cipher = AesGcmCipher::new(key);

        let plaintext = b"Test data";
        let ciphertext = cipher.encrypt(plaintext, None).unwrap();

        // Format should be: [12-byte nonce][encrypted_data + 16-byte GCM tag]
        assert_eq!(
            ciphertext.len(),
            12 + plaintext.len() + 16,
            "Ciphertext should be nonce + plaintext + tag length"
        );

        // Verify we can decrypt by extracting nonce from the beginning
        let nonce = &ciphertext[..12];
        assert_eq!(nonce.len(), 12, "Nonce should be 12 bytes");

        // The rest is encrypted data + tag
        let encrypted_with_tag = &ciphertext[12..];
        assert_eq!(
            encrypted_with_tag.len(),
            plaintext.len() + 16,
            "Encrypted portion should be plaintext length + 16-byte tag"
        );
    }
}
