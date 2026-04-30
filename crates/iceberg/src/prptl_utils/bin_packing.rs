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

//! First-fit-decreasing bin packing with bounded look-back.
//!
//! Java analog: `org.apache.iceberg.util.BinPacking::ListPacker`.

// Silenced until `ManifestMergeManager` (next commit) wires this up.
#![allow(dead_code)]

use std::collections::VecDeque;

/// Packs items into weight-bounded bins using a first-fit policy with bounded look-back.
///
/// Each input item is placed into the first bin (within the active window of `lookback`
/// bins) that can still accept its weight without exceeding `target_weight`. When no
/// active bin fits the item, a new bin is opened. If the active window then exceeds
/// `lookback`, one bin is closed and emitted: the front of the deque, or — when
/// `largest_bin_first` is set — the bin with the highest weight. After all items have
/// been processed, any bins still in the active window are drained in deque order.
///
/// Item ordering inside each output bin matches the order in which items were inserted.
///
/// Java analog: `org.apache.iceberg.util.BinPacking::ListPacker`.
pub(crate) struct ListPacker<T> {
    target_weight: u64,
    lookback: usize,
    largest_bin_first: bool,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T> ListPacker<T> {
    /// Construct a new packer.
    ///
    /// `lookback` must be `>= 1`; `0` is rejected because it would mean "no active bin
    /// can ever accept a second item," which collapses every output to a single-item bin.
    pub(crate) fn new(target_weight: u64, lookback: usize, largest_bin_first: bool) -> Self {
        assert!(lookback >= 1, "ListPacker lookback must be >= 1");
        Self {
            target_weight,
            lookback,
            largest_bin_first,
            _marker: std::marker::PhantomData,
        }
    }

    /// Pack items in input order.
    pub(crate) fn pack(&self, items: Vec<T>, weigh: impl Fn(&T) -> u64) -> Vec<Vec<T>> {
        self.pack_iter(items.into_iter(), weigh)
    }

    /// Pack items as if they had been processed in reverse, then restore the original
    /// orientation on both the outer list and within each bin.
    ///
    /// `ManifestMergeManager` uses this so that the new "first" manifest of a snapshot
    /// (which sits at index 0 of the input) lands at the end of the packing run; the
    /// bin containing it then surfaces as the *first* bin of the result, where the
    /// caller's `first`-guard can identify it.
    pub(crate) fn pack_end(&self, items: Vec<T>, weigh: impl Fn(&T) -> u64) -> Vec<Vec<T>> {
        let reversed: Vec<T> = items.into_iter().rev().collect();
        let mut packed = self.pack(reversed, weigh);
        packed.reverse();
        for bin in &mut packed {
            bin.reverse();
        }
        packed
    }

    fn pack_iter<I: Iterator<Item = T>>(&self, items: I, weigh: impl Fn(&T) -> u64) -> Vec<Vec<T>> {
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
                let evicted = if self.largest_bin_first {
                    let max_idx = active
                        .iter()
                        .enumerate()
                        .max_by_key(|(_, b)| b.weight)
                        .map(|(i, _)| i)
                        .expect("active is non-empty here");
                    active.remove(max_idx).expect("index came from this deque")
                } else {
                    active.pop_front().expect("active is non-empty here")
                };
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
        let packer = ListPacker::new(100, 1, false);
        let bins = packer.pack(vec![10u64, 20, 30], |w| *w);
        assert_eq!(bins, vec![vec![10, 20, 30]]);
    }

    #[test]
    fn pack_splits_when_exceeds_target() {
        let packer = ListPacker::new(50, 1, false);
        let bins = packer.pack(vec![30u64, 30, 30], |w| *w);
        // Item 0 starts a bin (30). Item 1 doesn't fit (30+30=60 > 50), so the active
        // bin closes and the new bin (30) opens. Item 2 doesn't fit, so the second bin
        // closes and a third opens. End: drain the third bin.
        assert_eq!(bins, vec![vec![30], vec![30], vec![30]]);
    }

    #[test]
    fn pack_first_fit_within_lookback_one() {
        let packer = ListPacker::new(50, 1, false);
        // 20 fits, then 10 fits (running 30), then 25 doesn't fit; close bin and open
        // a new one with 25. Then 20 fits in the new bin (45). Drain.
        let bins = packer.pack(vec![20u64, 10, 25, 20], |w| *w);
        assert_eq!(bins, vec![vec![20, 10], vec![25, 20]]);
    }

    #[test]
    fn pack_lookback_two_skips_full_bin() {
        let packer = ListPacker::new(50, 2, false);
        // Bin0: [40]. Item 20: doesn't fit in bin0; open bin1 [20]. Window size = 2,
        // still <= lookback. Item 10: bin0 (40+10=50) fits → bin0 = [40, 10].
        // Drain order: bin0 [40, 10], bin1 [20].
        let bins = packer.pack(vec![40u64, 20, 10], |w| *w);
        assert_eq!(bins, vec![vec![40, 10], vec![20]]);
    }

    #[test]
    fn pack_largest_bin_first_eviction() {
        // Use a target where bin0 doesn't end up as the largest, so we can observe the
        // policy difference: with largest_bin_first, the *biggest* active bin is evicted,
        // not the front of the deque.
        let target = 70u64;
        let items = vec![20u64, 30, 50, 25, 5];
        // Trace (lookback=2, both policies):
        //   bin0=[20] → bin0=[20,30](w=50) → bin1=[50](no fit). Item 25 fits nowhere →
        //   bin2=[25], window=3, eviction fires.
        let largest = ListPacker::<u64>::new(target, 2, true).pack(items.clone(), |w| *w);
        let front = ListPacker::<u64>::new(target, 2, false).pack(items, |w| *w);

        // largest_bin_first=true evicts bin1=[50] (heaviest). Then item 5 fits bin0
        // (50+5=55 ≤ 70) → bin0=[20,30,5]. Drain: bin0, bin2.
        assert_eq!(largest, vec![vec![50], vec![20, 30, 5], vec![25]]);

        // largest_bin_first=false evicts bin0=[20,30] (front). Then item 5 fits bin1
        // (50+5=55 ≤ 70) → bin1=[50,5]. Drain: bin1, bin2.
        assert_eq!(front, vec![vec![20, 30], vec![50, 5], vec![25]]);
    }

    #[test]
    fn pack_end_inverts_order() {
        let packer = ListPacker::new(50, 1, false);
        let pack = packer.pack(vec![20u64, 30, 10, 25], |w| *w);
        let pack_end = packer.pack_end(vec![20u64, 30, 10, 25], |w| *w);

        // pack: 20 fits; 30 fits (50); 10 doesn't fit, open new bin [10]; 25 fits (35).
        //   → [[20, 30], [10, 25]]
        // pack_end: reverse input → [25, 10, 30, 20], pack:
        //   25 fits; 10 fits (35); 30 doesn't fit, open new [30]; 20 fits (50).
        //   → [[25, 10], [30, 20]]; reverse outer → [[30, 20], [25, 10]]; reverse each →
        //   → [[20, 30], [10, 25]]
        // Note: same shape here because both inputs happen to round-trip cleanly.
        assert_eq!(pack, vec![vec![20, 30], vec![10, 25]]);
        assert_eq!(pack_end, vec![vec![20, 30], vec![10, 25]]);
    }

    #[test]
    fn pack_end_surfaces_first_item_in_first_bin() {
        // The "first" manifest in MergingSnapshotProducer sits at input index 0. Verify
        // pack_end places it in the first output bin (so the action's first-guard sees it).
        let packer = ListPacker::new(50, 1, false);
        let bins = packer.pack_end(vec![10u64, 20, 25, 25], |w| *w);
        // Reversed: [25, 25, 20, 10]. Pack: 25 fits; 25 fits (50); 20 doesn't fit, open
        // [20]; 10 fits (30) → [[25, 25], [20, 10]]. Reverse outer: [[20, 10], [25, 25]];
        // reverse each: [[10, 20], [25, 25]].
        assert_eq!(bins, vec![vec![10, 20], vec![25, 25]]);
        // first item (10) is in the first output bin.
        assert_eq!(bins[0][0], 10);
    }

    #[test]
    fn pack_oversized_item_lands_in_its_own_bin() {
        let packer = ListPacker::new(50, 1, false);
        let bins = packer.pack(vec![100u64, 10, 20], |w| *w);
        // 100 doesn't fit in target 50, but it's in a bin alone (the algorithm doesn't
        // reject; the bin's can_add returns false for any subsequent item).
        // 10: doesn't fit (100+10=110 > 50), close bin0 [100], open bin1 [10].
        // 20: fits (10+20=30 ≤ 50) → bin1 [10, 20]. Drain → [[100], [10, 20]].
        assert_eq!(bins, vec![vec![100], vec![10, 20]]);
    }

    #[test]
    fn pack_no_items_returns_empty() {
        let packer = ListPacker::new(50, 1, false);
        assert!(packer.pack(Vec::<u64>::new(), |w| *w).is_empty());
        assert!(packer.pack_end(Vec::<u64>::new(), |w| *w).is_empty());
    }

    #[test]
    fn random_inputs_each_bin_within_target_unless_singleton() {
        // Property-style stress test: every output bin's total weight is ≤ target,
        // unless the bin contains exactly one item whose weight is itself ≥ target.
        // Run many seeds; every permutation must satisfy the invariant.
        let target: u64 = 1_000;
        let packer = ListPacker::new(target, 1, false);

        for seed in 0..200u64 {
            let mut rng = StdRng::seed_from_u64(seed);
            let n: usize = rng.gen_range(0..200);
            let items: Vec<u64> = (0..n).map(|_| rng.gen_range(0..2_000)).collect();
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
                    assert!(w <= target, "seed={seed}: pack_end weight {w} > target {target}");
                }
            }
        }
    }

    #[test]
    fn replace_manifests_max_size_golden() {
        // Mirrors Java `TestRewriteManifests.testReplaceManifestsMaxSize`: pack a list
        // of equal-length manifests whose total exceeds the target, verify the output
        // is split into bins each ≤ target.
        // Manifest sizes: 6 items of 5 MB each, target 12 MB → expect 3 bins of 2.
        let target = 12u64 * 1024 * 1024;
        let packer = ListPacker::new(target, 1, false);
        let manifest_sizes: Vec<u64> = vec![5 * 1024 * 1024; 6];
        let bins = packer.pack(manifest_sizes, |w| *w);
        assert_eq!(bins.len(), 3);
        for bin in &bins {
            assert_eq!(bin.len(), 2, "each bin holds 2 manifests of 5MB ≤ 12MB");
            assert!(weights(std::slice::from_ref(bin))[0] <= target);
        }
    }
}
