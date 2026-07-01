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

use std::any::Any;
use std::future::Future;

use futures::FutureExt;
use futures::future::BoxFuture;

/// A trait for injecting instrumentation into spawned tasks.
///
/// Implementations can wrap futures or blocking closures with tracing spans,
/// metrics, or other observability hooks. The tracer receives type-erased
/// values and must preserve the output without modification.
pub trait RuntimeTracer: Send + Sync + 'static {
    /// Wraps a type-erased future with instrumentation.
    ///
    /// The implementation must not alter the future's output value.
    fn trace_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>>;

    /// Wraps a type-erased blocking closure with instrumentation.
    ///
    /// The implementation must not alter the closure's return value.
    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>;
}

/// Wraps a concrete future with the tracer, handling type erasure and
/// restoration internally.
pub(crate) fn trace_future<T, F>(
    tracer: &dyn RuntimeTracer,
    future: F,
) -> impl Future<Output = T> + Send + 'static
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let erased = async move { Box::new(future.await) as Box<dyn Any + Send> }.boxed();

    tracer.trace_future(erased).map(|any_box| {
        *any_box
            .downcast::<T>()
            .expect("RuntimeTracer must preserve the future's output type")
    })
}

/// Wraps a concrete blocking closure with the tracer, handling type erasure and
/// restoration internally.
pub(crate) fn trace_block<T, F>(
    tracer: &dyn RuntimeTracer,
    f: F,
) -> impl FnOnce() -> T + Send + 'static
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let erased: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> =
        Box::new(|| Box::new(f()) as Box<dyn Any + Send>);

    let traced = tracer.trace_block(erased);

    move || {
        *traced()
            .downcast::<T>()
            .expect("RuntimeTracer must preserve the closure's return type")
    }
}
