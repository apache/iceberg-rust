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

//! Bin-packing algorithm for combining file scan tasks.
//!
//! This module implements a bin-packing algorithm that combines small
//! [`FileScanTask`]s into [`CombinedScanTask`]s to reduce overhead from
//! opening many small files.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

use crate::Result;
use crate::scan::{CombinedScanTask, FileScanTask};

/// Configuration for the bin-packing algorithm.
#[derive(Debug, Clone, Copy)]
pub struct BinPackingConfig {
    /// Target size in bytes for each combined task.
    pub target_size: u64,
    /// The number of bins to keep open for finding the best fit.
    /// Higher values may produce better packing but use more memory.
    pub lookback: usize,
    /// The estimated cost in bytes to open a file.
    /// This is added to the task weight to account for file open overhead.
    pub open_file_cost: u64,
}

impl Default for BinPackingConfig {
    fn default() -> Self {
        Self {
            target_size: 128 * 1024 * 1024, // 128MB
            lookback: 20,
            open_file_cost: 4 * 1024 * 1024, // 4MB
        }
    }
}

/// Calculates the weight of a file scan task for bin-packing purposes.
///
/// The weight considers both the data size and the cost of opening files.
/// For tasks that reference delete files, the delete file sizes are also included.
pub fn calculate_task_weight(task: &FileScanTask, open_file_cost: u64) -> u64 {
    // Base weight is the length of data to read
    let data_weight = task.length;

    // Add delete file content size estimate (we don't have exact sizes,
    // so we count each delete file as contributing open_file_cost)
    let delete_weight = task.deletes.len() as u64 * open_file_cost;

    // The weight is the maximum of:
    // 1. Data size + delete file overhead
    // 2. Number of files to open * open_file_cost
    let file_count = 1 + task.deletes.len() as u64;
    let open_cost_weight = file_count * open_file_cost;

    std::cmp::max(data_weight + delete_weight, open_cost_weight)
}

/// A bin-packing iterator that combines file scan tasks into combined tasks.
///
/// This implements a greedy bin-packing algorithm with lookback:
/// - Tasks are added to open bins that can accommodate them
/// - When no bin fits, the largest bin is emitted and a new bin is created
/// - Lookback controls how many bins are kept open for better fitting
pub struct BinPackingIterator<I> {
    /// The source iterator of file scan tasks
    source: I,
    /// Currently open bins
    bins: Vec<CombinedScanTask>,
    /// Configuration for the packing algorithm
    config: BinPackingConfig,
    /// Whether the source iterator is exhausted
    source_exhausted: bool,
}

impl<I> BinPackingIterator<I>
where I: Iterator<Item = FileScanTask>
{
    /// Creates a new bin-packing iterator.
    pub fn new(source: I, config: BinPackingConfig) -> Self {
        Self {
            source,
            bins: Vec::with_capacity(config.lookback),
            config,
            source_exhausted: false,
        }
    }

    /// Finds the best bin for a task, or returns None if no bin fits.
    fn find_best_bin(&mut self, weight: u64) -> Option<usize> {
        // Find the bin with the most space that can still fit this task
        let mut best_idx = None;
        let mut best_remaining = u64::MAX;

        for (idx, bin) in self.bins.iter().enumerate() {
            if !bin.would_exceed(weight, self.config.target_size) {
                let remaining = self.config.target_size - bin.estimated_size() - weight;
                if remaining < best_remaining {
                    best_remaining = remaining;
                    best_idx = Some(idx);
                }
            }
        }

        best_idx
    }

    /// Removes and returns the largest bin (by estimated size).
    fn remove_largest_bin(&mut self) -> Option<CombinedScanTask> {
        if self.bins.is_empty() {
            return None;
        }

        let largest_idx = self
            .bins
            .iter()
            .enumerate()
            .max_by_key(|(_, bin)| bin.estimated_size())
            .map(|(idx, _)| idx)
            .unwrap();

        Some(self.bins.remove(largest_idx))
    }
}

impl<I> Iterator for BinPackingIterator<I>
where I: Iterator<Item = FileScanTask>
{
    type Item = CombinedScanTask;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If source is exhausted, drain remaining bins
            if self.source_exhausted {
                return self.bins.pop().filter(|bin| !bin.is_empty());
            }

            // Get next task from source
            let Some(task) = self.source.next() else {
                self.source_exhausted = true;
                // Start draining bins
                return self.bins.pop().filter(|bin| !bin.is_empty());
            };

            let weight = calculate_task_weight(&task, self.config.open_file_cost);

            // Try to find a bin that can fit this task
            if let Some(bin_idx) = self.find_best_bin(weight) {
                self.bins[bin_idx].add_task(task, weight);
            } else {
                // No bin fits - need to create a new one
                // If we've hit the lookback limit, emit the largest bin first
                if self.bins.len() >= self.config.lookback {
                    let largest = self.remove_largest_bin();

                    // Create new bin with this task
                    let mut new_bin = CombinedScanTask::new();
                    new_bin.add_task(task, weight);
                    self.bins.push(new_bin);

                    // Return the largest bin
                    if let Some(bin) = largest
                        && !bin.is_empty()
                    {
                        return Some(bin);
                    }
                } else {
                    // Still have room for more bins
                    let mut new_bin = CombinedScanTask::new();
                    new_bin.add_task(task, weight);
                    self.bins.push(new_bin);
                }
            }
        }
    }
}

/// Combines file scan tasks into combined scan tasks using bin-packing.
///
/// This function takes an iterator of file scan tasks and returns an iterator
/// of combined scan tasks, where small tasks have been grouped together.
///
/// # Arguments
/// * `tasks` - Iterator of file scan tasks to combine
/// * `config` - Configuration for the bin-packing algorithm
pub fn plan_tasks<I>(tasks: I, config: BinPackingConfig) -> BinPackingIterator<I>
where I: Iterator<Item = FileScanTask> {
    BinPackingIterator::new(tasks, config)
}

/// A streaming bin-packing adapter that combines file scan tasks into combined tasks.
///
/// This wraps an async stream of `Result<FileScanTask>` and produces a stream of
/// `Result<CombinedScanTask>`, applying bin-packing on-the-fly without collecting
/// all tasks into memory.
pub struct BinPackingStream<S> {
    /// The source stream of file scan tasks
    source: S,
    /// Currently open bins
    bins: Vec<CombinedScanTask>,
    /// Queue of completed bins ready to emit
    ready_bins: VecDeque<CombinedScanTask>,
    /// Configuration for the packing algorithm
    config: BinPackingConfig,
    /// Whether the source stream is exhausted
    source_exhausted: bool,
}

impl<S> BinPackingStream<S>
where S: Stream<Item = Result<FileScanTask>> + Unpin
{
    /// Creates a new streaming bin-packing adapter.
    pub fn new(source: S, config: BinPackingConfig) -> Self {
        Self {
            source,
            bins: Vec::with_capacity(config.lookback),
            ready_bins: VecDeque::new(),
            config,
            source_exhausted: false,
        }
    }

    /// Finds the best bin for a task, or returns None if no bin fits.
    fn find_best_bin(&self, weight: u64) -> Option<usize> {
        let mut best_idx = None;
        let mut best_remaining = u64::MAX;

        for (idx, bin) in self.bins.iter().enumerate() {
            if !bin.would_exceed(weight, self.config.target_size) {
                let remaining = self.config.target_size - bin.estimated_size() - weight;
                if remaining < best_remaining {
                    best_remaining = remaining;
                    best_idx = Some(idx);
                }
            }
        }

        best_idx
    }

    /// Removes and returns the largest bin (by estimated size).
    fn remove_largest_bin(&mut self) -> Option<CombinedScanTask> {
        if self.bins.is_empty() {
            return None;
        }

        let largest_idx = self
            .bins
            .iter()
            .enumerate()
            .max_by_key(|(_, bin)| bin.estimated_size())
            .map(|(idx, _)| idx)
            .unwrap();

        Some(self.bins.remove(largest_idx))
    }

    /// Adds a task to an appropriate bin, possibly emitting a completed bin.
    fn add_task(&mut self, task: FileScanTask) {
        let weight = calculate_task_weight(&task, self.config.open_file_cost);

        if let Some(bin_idx) = self.find_best_bin(weight) {
            self.bins[bin_idx].add_task(task, weight);
        } else {
            // No bin fits - need to create a new one
            if self.bins.len() >= self.config.lookback {
                // Hit lookback limit, emit largest bin
                if let Some(bin) = self.remove_largest_bin()
                    && !bin.is_empty()
                {
                    self.ready_bins.push_back(bin);
                }
            }
            // Create new bin with this task
            let mut new_bin = CombinedScanTask::new();
            new_bin.add_task(task, weight);
            self.bins.push(new_bin);
        }
    }

    /// Drains all remaining bins into the ready queue.
    fn drain_remaining_bins(&mut self) {
        while let Some(bin) = self.bins.pop() {
            if !bin.is_empty() {
                self.ready_bins.push_back(bin);
            }
        }
    }
}

impl<S> Stream for BinPackingStream<S>
where S: Stream<Item = Result<FileScanTask>> + Unpin
{
    type Item = Result<CombinedScanTask>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // First, return any ready bins
            if let Some(bin) = self.ready_bins.pop_front() {
                return Poll::Ready(Some(Ok(bin)));
            }

            // If source is exhausted, drain remaining bins
            if self.source_exhausted {
                if let Some(bin) = self.bins.pop() {
                    if !bin.is_empty() {
                        return Poll::Ready(Some(Ok(bin)));
                    }
                } else {
                    return Poll::Ready(None);
                }
                continue;
            }

            // Poll the source stream
            match Pin::new(&mut self.source).poll_next(cx) {
                Poll::Ready(Some(Ok(task))) => {
                    self.add_task(task);
                    // Continue loop to check if we have ready bins
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    self.source_exhausted = true;
                    self.drain_remaining_bins();
                    // Continue loop to drain ready_bins
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

/// Creates a streaming bin-packing adapter from a stream of file scan tasks.
///
/// This function takes an async stream of file scan tasks and returns a stream
/// of combined scan tasks, applying bin-packing on-the-fly without collecting
/// all tasks into memory.
pub fn plan_tasks_stream<S>(source: S, config: BinPackingConfig) -> BinPackingStream<S>
where S: Stream<Item = Result<FileScanTask>> + Unpin {
    BinPackingStream::new(source, config)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};

    fn create_test_task(path: &str, length: u64) -> FileScanTask {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .unwrap(),
        );

        FileScanTask {
            start: 0,
            length,
            record_count: None,
            data_file_path: path.to_string(),
            data_file_format: DataFileFormat::Parquet,
            schema,
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    #[test]
    fn test_single_task_single_bin() {
        let tasks = vec![create_test_task("file1.parquet", 50 * 1024 * 1024)]; // 50MB

        let config = BinPackingConfig {
            target_size: 128 * 1024 * 1024,
            lookback: 20,
            open_file_cost: 4 * 1024 * 1024,
        };

        let combined: Vec<_> = plan_tasks(tasks.into_iter(), config).collect();

        assert_eq!(combined.len(), 1);
        assert_eq!(combined[0].len(), 1);
    }

    #[test]
    fn test_small_tasks_combined() {
        // Create 10 small tasks of 10MB each
        let tasks: Vec<_> = (0..10)
            .map(|i| create_test_task(&format!("file{i}.parquet"), 10 * 1024 * 1024))
            .collect();

        let config = BinPackingConfig {
            target_size: 128 * 1024 * 1024, // 128MB target
            lookback: 20,
            open_file_cost: 4 * 1024 * 1024, // 4MB per file
        };

        let combined: Vec<_> = plan_tasks(tasks.into_iter(), config).collect();

        // With 10MB data + 4MB open cost = 14MB per task
        // 128MB / 14MB ≈ 9 tasks per bin, so we expect ~2 bins
        assert!(
            combined.len() <= 2,
            "Expected at most 2 bins, got {}",
            combined.len()
        );

        // Total tasks should still be 10
        let total_tasks: usize = combined.iter().map(|c| c.len()).sum();
        assert_eq!(total_tasks, 10);
    }

    #[test]
    fn test_large_task_own_bin() {
        let tasks = vec![
            create_test_task("small.parquet", 10 * 1024 * 1024), // 10MB
            create_test_task("large.parquet", 200 * 1024 * 1024), // 200MB > target
            create_test_task("small2.parquet", 10 * 1024 * 1024), // 10MB
        ];

        let config = BinPackingConfig {
            target_size: 128 * 1024 * 1024, // 128MB target
            lookback: 20,
            open_file_cost: 4 * 1024 * 1024,
        };

        let combined: Vec<_> = plan_tasks(tasks.into_iter(), config).collect();

        // Large task exceeds target, should be in its own bin
        // Small tasks may be combined
        assert!(combined.len() >= 2);

        // Check that large task is in its own bin
        let has_large_alone = combined
            .iter()
            .any(|c| c.len() == 1 && c.tasks()[0].data_file_path == "large.parquet");
        assert!(has_large_alone, "Large task should be in its own bin");
    }

    #[test]
    fn test_calculate_task_weight_basic() {
        let task = create_test_task("file.parquet", 100 * 1024 * 1024); // 100MB
        let open_cost = 4 * 1024 * 1024; // 4MB

        let weight = calculate_task_weight(&task, open_cost);

        // Weight should be max(100MB + 0 deletes, 1 file * 4MB) = 100MB
        assert_eq!(weight, 100 * 1024 * 1024);
    }

    #[test]
    fn test_calculate_task_weight_small_file() {
        let task = create_test_task("small.parquet", 1024 * 1024); // 1MB
        let open_cost = 4 * 1024 * 1024; // 4MB

        let weight = calculate_task_weight(&task, open_cost);

        // Weight should be max(1MB, 4MB open cost) = 4MB
        assert_eq!(weight, 4 * 1024 * 1024);
    }

    #[test]
    fn test_empty_input() {
        let tasks: Vec<FileScanTask> = vec![];
        let config = BinPackingConfig::default();

        let combined: Vec<_> = plan_tasks(tasks.into_iter(), config).collect();

        assert!(combined.is_empty());
    }

    #[test]
    fn test_lookback_limit() {
        // Create many small tasks to test lookback behavior
        let tasks: Vec<_> = (0..50)
            .map(|i| create_test_task(&format!("file{i}.parquet"), 1024 * 1024))
            .collect();

        let config = BinPackingConfig {
            target_size: 128 * 1024 * 1024,
            lookback: 5, // Only keep 5 bins open
            open_file_cost: 4 * 1024 * 1024,
        };

        let combined: Vec<_> = plan_tasks(tasks.into_iter(), config).collect();

        // All tasks should be accounted for
        let total_tasks: usize = combined.iter().map(|c| c.len()).sum();
        assert_eq!(total_tasks, 50);
    }
}
