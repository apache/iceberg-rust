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

//! First-fit-decreasing bin packing with lookback.
//!
//! Used by [`TableScan::plan_tasks()`] to combine split scan tasks into
//! balanced groups whose total weight is roughly `target_weight`.

use std::collections::VecDeque;

struct Bin<T> {
    items: Vec<T>,
    weight: u64,
}

/// Bin-pack `items` into groups whose total weight is roughly `target_weight`.
///
/// Uses a first-fit-decreasing strategy with a sliding `lookback` window of
/// open bins, matching the algorithm in Java Iceberg's `BinPacking.java`.
///
/// Items heavier than `target_weight` are placed in their own bin.
pub(crate) fn bin_pack<T, F>(
    mut items: Vec<T>,
    target_weight: u64,
    lookback: usize,
    weight_fn: F,
) -> Vec<Vec<T>>
where
    F: Fn(&T) -> u64,
{
    if items.is_empty() {
        return vec![];
    }

    let lookback = lookback.max(1);

    // Compute weights and sort descending (heaviest first)
    let mut weighted: Vec<(T, u64)> = items
        .drain(..)
        .map(|item| {
            let w = weight_fn(&item);
            (item, w)
        })
        .collect();
    weighted.sort_by(|a, b| b.1.cmp(&a.1));

    let mut result: Vec<Vec<T>> = Vec::new();
    let mut open_bins: VecDeque<Bin<T>> = VecDeque::new();

    for (item, weight) in weighted {
        // Try to fit into an existing open bin
        let fit_idx = open_bins
            .iter()
            .position(|bin| bin.weight + weight <= target_weight);

        if let Some(idx) = fit_idx {
            open_bins[idx].weight += weight;
            open_bins[idx].items.push(item);
        } else {
            // Evict the largest bin if we've exceeded lookback
            if open_bins.len() >= lookback {
                let max_idx = open_bins
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, b)| b.weight)
                    .map(|(i, _)| i)
                    .unwrap();
                let evicted = open_bins.remove(max_idx).unwrap();
                result.push(evicted.items);
            }

            open_bins.push_back(Bin {
                items: vec![item],
                weight,
            });
        }
    }

    // Flush remaining bins
    for bin in open_bins {
        result.push(bin.items);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_input() {
        let items: Vec<u64> = vec![];
        let result = bin_pack(items, 100, 10, |&x| x);
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_item_fits() {
        let result = bin_pack(vec![50u64], 100, 10, |&x| x);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![50]);
    }

    #[test]
    fn test_single_oversized_item() {
        let result = bin_pack(vec![200u64], 100, 10, |&x| x);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![200]);
    }

    #[test]
    fn test_multiple_small_items_pack_together() {
        let result = bin_pack(vec![30u64, 20, 10, 25, 15], 100, 10, |&x| x);
        // Total weight = 100, fits in one bin
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].iter().sum::<u64>(), 100);
    }

    #[test]
    fn test_items_split_into_multiple_bins() {
        let result = bin_pack(vec![60u64, 60, 60], 100, 10, |&x| x);
        // Each 60 can pair with at most one other 60 (120 > 100), so need at least 2 bins
        // With first-fit-decreasing: first 60 -> bin1, second 60 -> bin2, third 60 -> bin3
        // (none can combine since 60+60=120 > 100)
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_bin_packing_balances_load() {
        // 4 items: 50, 40, 30, 20 with target 70
        let result = bin_pack(vec![50u64, 40, 30, 20], 70, 10, |&x| x);
        // Sorted descending: 50, 40, 30, 20
        // 50 -> bin1(50), 40 -> bin2(40), 30 -> bin2(70), 20 -> bin1(70)
        assert_eq!(result.len(), 2);
        for bin in &result {
            let sum: u64 = bin.iter().sum();
            assert!(sum <= 70, "Bin weight {sum} exceeds target 70");
        }
    }

    #[test]
    fn test_lookback_limits_open_bins() {
        // With lookback=1, only one bin is kept open at a time
        let result = bin_pack(vec![10u64, 10, 10, 10], 100, 1, |&x| x);
        // All items are same weight (10). With lookback=1:
        // item1(10)->bin1(10), item2(10)->bin1(20), item3(10)->bin1(30), item4(10)->bin1(40)
        // They all fit, so 1 bin
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_lookback_causes_suboptimal_packing() {
        // With lookback=1, a tight fit may be missed
        // Items: 80, 70, 30, 20 with target=100, lookback=1
        let result = bin_pack(vec![80u64, 70, 30, 20], 100, 1, |&x| x);
        // Sorted: 80, 70, 30, 20
        // 80->bin1(80). lookback=1, only bin1 open.
        // 70 doesn't fit in bin1(80+70=150>100). Evict bin1([80]). 70->bin2(70).
        // 30 fits in bin2(70+30=100). bin2(100).
        // 20 doesn't fit in bin2(100+20=120>100). Evict bin2([70,30]). 20->bin3(20).
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_custom_weight_function() {
        // Weight function that doubles the value
        let result = bin_pack(vec![30u64, 30, 30], 100, 10, |&x| x * 2);
        // Effective weights: 60, 60, 60
        // 60+60=120 > 100, so each in its own bin
        assert_eq!(result.len(), 3);
    }
}
