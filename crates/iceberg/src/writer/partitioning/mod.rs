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

mod clustered;

use crate::Result;
use crate::spec::PartitionKey;
use crate::writer::{DefaultInput, DefaultOutput};

#[async_trait::async_trait]
pub trait PartitioningWriterBuilder<I = DefaultInput, O = DefaultOutput>:
    Send + Clone + 'static
{
    /// todo doc
    type R: PartitioningWriter<I, O>;
    /// todo doc
    async fn build(self) -> Result<Self::R>;
}

#[async_trait::async_trait]
pub trait PartitioningWriter<I = DefaultInput, O = DefaultOutput>: Send + 'static {
    /// todo doc
    async fn write(&mut self, partition_key: Option<PartitionKey>, input: I) -> Result<()>;
    /// todo doc
    async fn close(&mut self) -> Result<O>;
}
