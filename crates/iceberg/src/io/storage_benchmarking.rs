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

//! This storage is used to mimick consistent latency for benchmarks of iceberg.
//! It should not be included in standard distributions of iceberg.

use std::thread;
use std::time::Duration;

use opendal::raw::{Access, Layer, LayeredAccess, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead, RpWrite};
use opendal::Operator;
use opendal::services::MemoryConfig;
use opendal::Result;
use rand::{thread_rng, Rng};
use tokio::time::sleep;

pub(crate) fn benchmarking_config_build() -> Result<Operator> {
    Ok(Operator::from_config(MemoryConfig::default())?.layer(DelayLayer).finish())
}

/// Usually takes around 50 ms, to visualize function: $ f\left(x\right)=x^{-0.5}\ \cdot\ 0.10 $
fn gen_amt_latency() -> Duration {
    let x: f64 = thread_rng().gen_range(0.01..10.);
    Duration::from_secs_f64(x.powf(-0.5) * 0.1)
}

/// A layer that artifially introduces predictable relay for better benchmarking
struct DelayLayer;

impl<A: Access> Layer<A> for DelayLayer {
    type LayeredAccess = DelayedAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        DelayedAccessor { inner }
    }
}

#[derive(Debug)]
struct DelayedAccessor<A: Access> {
    inner: A
}

impl<A: Access> LayeredAccess for DelayedAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Lister = A::Lister;
    type BlockingLister = A::BlockingLister;
    type Deleter = A::Deleter;
    type BlockingDeleter = A::BlockingDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        sleep(gen_amt_latency()).await;

        self.inner.read(path, args).await
    }

    fn blocking_read(
        &self,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, Self::BlockingReader)> {
        thread::sleep(gen_amt_latency());

        self.inner.blocking_read(path, args)
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> Result<(RpWrite, Self::BlockingWriter)> {
        self.inner.blocking_write(path, args)
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }

    fn blocking_list(
        &self,
        path: &str,
        args: OpList,
    ) -> Result<(RpList, Self::BlockingLister)> {
        self.inner.blocking_list(path, args)
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
       self.inner.delete().await
       }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
       self.inner.blocking_delete()
   }
}
