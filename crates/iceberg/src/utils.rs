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

use std::num::NonZeroUsize;

use crate::spec::{SnapshotRef, TableMetadataRef};

// Use a default value of 1 as the safest option.
// See https://doc.rust-lang.org/std/thread/fn.available_parallelism.html#limitations
// for more details.
const DEFAULT_PARALLELISM: usize = 1;

/// Uses [`std::thread::available_parallelism`] in order to
/// retrieve an estimate of the default amount of parallelism
/// that should be used. Note that [`std::thread::available_parallelism`]
/// returns a `Result` as it can fail, so here we use
/// a default value instead.
/// Note: we don't use a OnceCell or LazyCell here as there
/// are circumstances where the level of available
/// parallelism can change during the lifetime of an executing
/// process, but this should not be called in a hot loop.
pub(crate) fn available_parallelism() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or_else(|_err| {
        // Failed to get the level of parallelism.
        // TODO: log/trace when this fallback occurs.

        // Using a default value.
        NonZeroUsize::new(DEFAULT_PARALLELISM).unwrap()
    })
}

pub mod bin {
    use std::iter::Iterator;
    use std::marker::PhantomData;

    use itertools::Itertools;

    struct Bin<T> {
        bin_weight: u32,
        target_weight: u32,
        items: Vec<T>,
    }

    impl<T> Bin<T> {
        pub fn new(target_weight: u32) -> Self {
            Bin {
                bin_weight: 0,
                target_weight,
                items: Vec::new(),
            }
        }

        pub fn can_add(&self, weight: u32) -> bool {
            self.bin_weight + weight <= self.target_weight
        }

        pub fn add(&mut self, item: T, weight: u32) {
            self.bin_weight += weight;
            self.items.push(item);
        }

        pub fn into_vec(self) -> Vec<T> {
            self.items
        }
    }

    /// ListPacker help to pack item into bin of item. Each bin has close to
    /// target_weight.
    pub(crate) struct ListPacker<T> {
        target_weight: u32,
        _marker: PhantomData<T>,
    }

    impl<T> ListPacker<T> {
        pub fn new(target_weight: u32) -> Self {
            ListPacker {
                target_weight,
                _marker: PhantomData,
            }
        }

        pub fn pack<F>(&self, items: Vec<T>, weight_func: F) -> Vec<Vec<T>>
        where F: Fn(&T) -> u32 {
            let mut bins: Vec<Bin<T>> = vec![];
            for item in items {
                let cur_weight = weight_func(&item);
                let addable_bin =
                    if let Some(bin) = bins.iter_mut().find(|bin| bin.can_add(cur_weight)) {
                        bin
                    } else {
                        bins.push(Bin::new(self.target_weight));
                        bins.last_mut().unwrap()
                    };
                addable_bin.add(item, cur_weight);
            }

            bins.into_iter().map(|bin| bin.into_vec()).collect_vec()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_list_packer_basic_packing() {
            let packer = ListPacker::new(10);
            let items = vec![3, 4, 5, 6, 2, 1];

            let packed = packer.pack(items, |&x| x);

            assert_eq!(packed.len(), 3);
            assert!(packed[0].iter().sum::<u32>() == 10);
            assert!(packed[1].iter().sum::<u32>() == 5);
            assert!(packed[2].iter().sum::<u32>() == 6);
        }

        #[test]
        fn test_list_packer_with_complex_items() {
            #[derive(Debug, PartialEq)]
            struct Item {
                name: String,
                size: u32,
            }

            let packer = ListPacker::new(15);
            let items = vec![
                Item {
                    name: "A".to_string(),
                    size: 7,
                },
                Item {
                    name: "B".to_string(),
                    size: 8,
                },
                Item {
                    name: "C".to_string(),
                    size: 5,
                },
                Item {
                    name: "D".to_string(),
                    size: 6,
                },
            ];

            let packed = packer.pack(items, |item| item.size);

            assert_eq!(packed.len(), 2);
            assert!(packed[0].iter().map(|x| x.size).sum::<u32>() <= 15);
            assert!(packed[1].iter().map(|x| x.size).sum::<u32>() <= 15);
        }

        #[test]
        fn test_list_packer_single_large_item() {
            let packer = ListPacker::new(10);
            let items = vec![15, 5, 3];

            let packed = packer.pack(items, |&x| x);

            assert_eq!(packed.len(), 2);
            assert!(packed[0].contains(&15));
            assert!(packed[1].iter().sum::<u32>() <= 10);
        }

        #[test]
        fn test_list_packer_empty_input() {
            let packer = ListPacker::new(10);
            let items: Vec<u32> = vec![];

            let packed = packer.pack(items, |&x| x);

            assert_eq!(packed.len(), 0);
        }
    }
}

pub struct Ancestors {
    next: Option<SnapshotRef>,
    get_snapshot: Box<dyn Fn(i64) -> Option<SnapshotRef> + Send>,
}

impl Iterator for Ancestors {
    type Item = SnapshotRef;

    fn next(&mut self) -> Option<Self::Item> {
        let snapshot = self.next.take()?;
        let result = snapshot.clone();
        self.next = snapshot
            .parent_snapshot_id()
            .and_then(|id| (self.get_snapshot)(id));
        Some(result)
    }
}

/// Iterate starting from `snapshot` (inclusive) to the root snapshot.
pub fn ancestors_of(
    table_metadata: &TableMetadataRef,
    snapshot: i64,
) -> Box<dyn Iterator<Item = SnapshotRef> + Send> {
    if let Some(snapshot) = table_metadata.snapshot_by_id(snapshot) {
        let table_metadata = table_metadata.clone();
        Box::new(Ancestors {
            next: Some(snapshot.clone()),
            get_snapshot: Box::new(move |id| table_metadata.snapshot_by_id(id).cloned()),
        })
    } else {
        Box::new(std::iter::empty())
    }
}

/// Iterate starting from `snapshot` (inclusive) to `oldest_snapshot_id` (exclusive).
pub fn ancestors_between(
    table_metadata: &TableMetadataRef,
    latest_snapshot_id: i64,
    oldest_snapshot_id: Option<i64>,
) -> Box<dyn Iterator<Item = SnapshotRef> + Send> {
    let Some(oldest_snapshot_id) = oldest_snapshot_id else {
        return Box::new(ancestors_of(table_metadata, latest_snapshot_id));
    };

    if latest_snapshot_id == oldest_snapshot_id {
        return Box::new(std::iter::empty());
    }

    Box::new(
        ancestors_of(table_metadata, latest_snapshot_id)
            .take_while(move |snapshot| snapshot.snapshot_id() != oldest_snapshot_id),
    )
}
