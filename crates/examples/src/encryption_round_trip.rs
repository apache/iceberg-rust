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

use iceberg::encryption::kms::MemoryKeyManagementClient;
use iceberg::encryption::{AesGcmCipher, AesKeySize, KeyManagementClient, SecureKey};

/// This example generates a DEK, wrap it with the KEK, encrypt a payload with the DEK,
/// then on the read-side unwrap the DEK and decrypt the payload.
#[tokio::main]
async fn main() {
    let kms = MemoryKeyManagementClient::new();
    let master_key_id = "demo-master-key";
    kms.add_master_key(master_key_id).unwrap();

    let dek = SecureKey::generate(AesKeySize::Bits128);
    let wrapped_dek = kms.wrap_key(dek.as_bytes(), master_key_id).await.unwrap();
    println!("Wrapped DEK: {} bytes", wrapped_dek.len());

    let cipher = AesGcmCipher::new(dek);
    let plaintext = b"hello, encrypted iceberg!";
    let ciphertext = cipher.encrypt(plaintext, None).unwrap();
    println!(
        "Ciphertext: {} bytes (12-byte nonce + payload + 16-byte tag)",
        ciphertext.len()
    );

    let unwrapped = kms.unwrap_key(&wrapped_dek, master_key_id).await.unwrap();
    let recovered_dek = SecureKey::try_from(unwrapped).unwrap();
    let read_cipher = AesGcmCipher::new(recovered_dek);
    let decrypted = read_cipher.decrypt(&ciphertext, None).unwrap();

    assert_eq!(decrypted.as_slice(), plaintext);
    println!(
        "Reading encrypted iceberg file succeeded: {:?}",
        std::str::from_utf8(&decrypted).unwrap()
    );
}
