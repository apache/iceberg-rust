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

use iceberg::Result;
use iceberg::{Error, ErrorKind};
use reqwest::header::HeaderMap;
use reqwest::{Client, IntoUrl, Method, Request, RequestBuilder, Response};
use serde::de::DeserializeOwned;

#[derive(Debug)]
pub(crate) struct HttpClient(Client);

impl HttpClient {
    pub fn try_create(default_headers: HeaderMap) -> Result<Self> {
        Ok(HttpClient(
            Client::builder().default_headers(default_headers).build()?,
        ))
    }

    #[inline]
    pub fn request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        self.0.request(method, url)
    }

    pub async fn query<
        R: DeserializeOwned,
        E: DeserializeOwned + Into<Error>,
        const SUCCESS_CODE: u16,
    >(
        &self,
        request: Request,
    ) -> Result<R> {
        let resp = self.0.execute(request).await?;

        if resp.status().as_u16() == SUCCESS_CODE {
            let text = resp.bytes().await?;
            Ok(serde_json::from_slice::<R>(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("json", String::from_utf8_lossy(&text))
                .with_source(e)
            })?)
        } else {
            let code = resp.status();
            let text = resp.bytes().await?;
            let e = serde_json::from_slice::<E>(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("json", String::from_utf8_lossy(&text))
                .with_context("code", code.to_string())
                .with_source(e)
            })?;
            Err(e.into())
        }
    }

    pub async fn execute<E: DeserializeOwned + Into<Error>, const SUCCESS_CODE: u16>(
        &self,
        request: Request,
    ) -> Result<()> {
        let resp = self.0.execute(request).await?;

        if resp.status().as_u16() == SUCCESS_CODE {
            Ok(())
        } else {
            let code = resp.status();
            let text = resp.bytes().await?;
            let e = serde_json::from_slice::<E>(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("json", String::from_utf8_lossy(&text))
                .with_context("code", code.to_string())
                .with_source(e)
            })?;
            Err(e.into())
        }
    }

    /// More generic logic handling for special cases like head.
    pub async fn do_execute<R, E: DeserializeOwned + Into<Error>>(
        &self,
        request: Request,
        handler: impl FnOnce(&Response) -> Option<R>,
    ) -> Result<R> {
        let resp = self.0.execute(request).await?;

        if let Some(ret) = handler(&resp) {
            Ok(ret)
        } else {
            let code = resp.status();
            let text = resp.bytes().await?;
            let e = serde_json::from_slice::<E>(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("code", code.to_string())
                .with_context("json", String::from_utf8_lossy(&text))
                .with_source(e)
            })?;
            Err(e.into())
        }
    }
}
