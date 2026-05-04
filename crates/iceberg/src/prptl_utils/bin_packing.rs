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

//! First-fit bin packing with bounded look-back.
//!
//! Java analog: `org.apache.iceberg.util.BinPacking::ListPacker`.

use std::collections::VecDeque;

/// Packs items into weight-bounded bins. Each item goes into the first active bin
/// (within a window of `lookback` open bins) that still fits its weight without
/// exceeding `target_weight`; otherwise a new bin opens. Once the active window
/// exceeds `lookback`, the front bin closes.
///
/// Java analog: `org.apache.iceberg.util.BinPacking::ListPacker`.
pub(crate) struct ListPacker {
    target_weight: u64,
    lookback: usize,
}

impl ListPacker {
    /// `lookback` must be `>= 1`; `0` collapses every output to a single-item bin.
    pub(crate) fn new(target_weight: u64, lookback: usize) -> Self {
        assert!(lookback >= 1, "ListPacker lookback must be >= 1");
        Self {
            target_weight,
            lookback,
        }
    }

    /// Pack as if iterating in reverse, restoring the original orientation on both the
    /// outer list and each bin's contents. The item at input index 0 ends up in the
    /// first output bin.
    pub(crate) fn pack_end<T>(&self, items: Vec<T>, weigh: impl Fn(&T) -> u64) -> Vec<Vec<T>> {
        let reversed: Vec<T> = items.into_iter().rev().collect();
        let mut packed = self.pack(reversed, weigh);
        packed.reverse();
        for bin in &mut packed {
            bin.reverse();
        }
        packed
    }

    fn pack<T>(&self, items: Vec<T>, weigh: impl Fn(&T) -> u64) -> Vec<Vec<T>> {
        let mut active: VecDeque<Bin<T>> = VecDeque::new();
        let mut output: Vec<Vec<T>> = Vec::new();

        for item in items {
            let weight = weigh(&item);

            let fit = active
                .iter()
                .position(|b| b.can_add(weight, self.target_weight));

            if let Some(idx) = fit {
                active[idx].add(item, weight);
                continue;
            }

            let mut bin = Bin::default();
            bin.add(item, weight);
            active.push_back(bin);

            if active.len() > self.lookback {
                let evicted = active.pop_front().expect("active is non-empty here");
                output.push(evicted.items);
            }
        }

        while let Some(bin) = active.pop_front() {
            output.push(bin.items);
        }

        output
    }
}

struct Bin<T> {
    items: Vec<T>,
    weight: u64,
}

impl<T> Default for Bin<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            weight: 0,
        }
    }
}

impl<T> Bin<T> {
    fn can_add(&self, weight: u64, target: u64) -> bool {
        self.weight + weight <= target
    }

    fn add(&mut self, item: T, weight: u64) {
        self.items.push(item);
        self.weight += weight;
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn weights(bins: &[Vec<u64>]) -> Vec<u64> {
        bins.iter().map(|b| b.iter().copied().sum()).collect()
    }

    #[test]
    fn pack_single_bin_when_total_below_target() {
        let packer = ListPacker::new(100, 1);
        let bins = packer.pack(vec![10u64, 20, 30], |w| *w);
        assert_eq!(bins, vec![vec![10, 20, 30]]);
    }

    #[test]
    fn pack_splits_when_exceeds_target() {
        let packer = ListPacker::new(50, 1);
        let bins = packer.pack(vec![30u64, 30, 30], |w| *w);
        assert_eq!(bins, vec![vec![30], vec![30], vec![30]]);
    }

    #[test]
    fn pack_first_fit_within_lookback_one() {
        let packer = ListPacker::new(50, 1);
        let bins = packer.pack(vec![20u64, 10, 25, 20], |w| *w);
        assert_eq!(bins, vec![vec![20, 10], vec![25, 20]]);
    }

    #[test]
    fn pack_lookback_two_keeps_earlier_bin_open() {
        let packer = ListPacker::new(50, 2);
        let bins = packer.pack(vec![40u64, 20, 10], |w| *w);
        // 10 fits back into bin0 because lookback=2 keeps it active when bin1 opens.
        assert_eq!(bins, vec![vec![40, 10], vec![20]]);
    }

    #[test]
    fn pack_end_round_trips_when_layout_is_symmetric() {
        // Some inputs pack identically forward and reversed; verify both code paths
        // agree there so a future change to pack_end can't silently regress.
        let packer = ListPacker::new(50, 1);
        let input = vec![20u64, 30, 10, 25];
        assert_eq!(packer.pack(input.clone(), |w| *w), vec![
            vec![20, 30],
            vec![10, 25]
        ]);
        assert_eq!(packer.pack_end(input, |w| *w), vec![vec![20, 30], vec![
            10, 25
        ]]);
    }

    #[test]
    fn pack_end_surfaces_first_item_in_first_bin() {
        let packer = ListPacker::new(50, 1);
        let bins = packer.pack_end(vec![10u64, 20, 25, 25], |w| *w);
        assert_eq!(bins, vec![vec![10, 20], vec![25, 25]]);
        // The input's head item must land in the first output bin; this is the
        // contract the merge manager's first-guard depends on.
        assert_eq!(bins[0][0], 10);
    }

    #[test]
    fn pack_oversized_item_lands_in_its_own_bin() {
        // 100 > target=50, but it isn't rejected — it sits alone in its bin, and the
        // next items pack into a fresh one.
        let packer = ListPacker::new(50, 1);
        let bins = packer.pack(vec![100u64, 10, 20], |w| *w);
        assert_eq!(bins, vec![vec![100], vec![10, 20]]);
    }

    #[test]
    fn pack_no_items_returns_empty() {
        let packer = ListPacker::new(50, 1);
        assert!(packer.pack(Vec::<u64>::new(), |w| *w).is_empty());
        assert!(packer.pack_end(Vec::<u64>::new(), |w| *w).is_empty());
    }

    #[test]
    fn random_inputs_each_bin_within_target_unless_singleton() {
        // Property-style stress: every multi-item bin must satisfy `Σ weights ≤ target`,
        // and no items can be lost or duplicated. Singletons may exceed the target —
        // the algorithm doesn't reject oversized inputs.
        let target: u64 = 1_000;
        let packer = ListPacker::new(target, 1);

        for seed in 0..200u64 {
            let mut rng = StdRng::seed_from_u64(seed);
            let n: usize = rng.random_range(0..200);
            let items: Vec<u64> = (0..n).map(|_| rng.random_range(0..2_000)).collect();
            let bins = packer.pack(items.clone(), |w| *w);
            let total_in: u64 = items.iter().sum();
            let total_out: u64 = bins.iter().flat_map(|b| b.iter().copied()).sum();
            assert_eq!(total_in, total_out, "seed={seed}: items conserved");

            for (bin_idx, bin) in bins.iter().enumerate() {
                let w: u64 = bin.iter().copied().sum();
                if bin.len() > 1 {
                    assert!(
                        w <= target,
                        "seed={seed} bin={bin_idx}: weight {w} > target {target} \
                         despite having multiple items {:?}",
                        bin
                    );
                }
            }

            // pack_end preserves the same item-count and weight invariants.
            let bins_end = packer.pack_end(items.clone(), |w| *w);
            let total_end: u64 = bins_end.iter().flat_map(|b| b.iter().copied()).sum();
            assert_eq!(total_in, total_end, "seed={seed}: pack_end conserves items");
            for bin in &bins_end {
                let w: u64 = bin.iter().copied().sum();
                if bin.len() > 1 {
                    assert!(
                        w <= target,
                        "seed={seed}: pack_end weight {w} > target {target}"
                    );
                }
            }
        }
    }

    #[test]
    fn replace_manifests_max_size_golden() {
        // Java parity: 6 items at 5 MB, target 12 MB → 3 bins of 2.
        // Mirrors `TestRewriteManifests.testReplaceManifestsMaxSize`.
        let target = 12u64 * 1024 * 1024;
        let packer = ListPacker::new(target, 1);
        let bins = packer.pack(vec![5 * 1024 * 1024; 6], |w| *w);
        assert_eq!(bins.len(), 3);
        for bin in &bins {
            assert_eq!(bin.len(), 2);
            assert!(weights(std::slice::from_ref(bin))[0] <= target);
        }
    }
}
