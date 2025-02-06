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

//! Partition writer

use crate::spec::Struct;
use crate::writer::{IcebergWriterBuilder, SinglePartitionWriterBuilder};
use crate::Result;

#[derive(Clone)]
struct PartitionWriterBuilder<B> {
    inner_writer_builer: B,
    partition_value: Option<Struct>,
}

impl<B: SinglePartitionWriterBuilder> PartitionWriterBuilder<B> {
    pub fn new(inner_writer_builer: B, partition_value: Option<Struct>) -> Self {
        Self {
            inner_writer_builer,
            partition_value,
        }
    }
}

#[async_trait::async_trait]
impl<B: SinglePartitionWriterBuilder> IcebergWriterBuilder for PartitionWriterBuilder<B> {
    type R = B::R;

    async fn build(self) -> Result<Self::R> {
        self.inner_writer_builer.build(self.partition_value).await
    }
}
