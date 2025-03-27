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

use roaring::RoaringTreemap;

#[allow(unused)]
pub struct DeleteVector {
    inner: RoaringTreemap,
}

impl DeleteVector {
    pub fn iter(&self) -> DeleteVectorIterator {
        let mut iter = self.inner.bitmaps();
        match iter.next() {
            Some((high_bits, bitmap)) => {
                DeleteVectorIterator {
                    inner: Some(DeleteVectorIteratorInner {
                        // iter,
                        high_bits: (high_bits as u64) << 32,
                        bitmap_iter: bitmap.iter(),
                    }),
                }
            }
            _ => DeleteVectorIterator { inner: None },
        }
    }
}

pub struct DeleteVectorIterator<'a> {
    inner: Option<DeleteVectorIteratorInner<'a>>,
}

struct DeleteVectorIteratorInner<'a> {
    // TODO: roaring::treemap::iter::BitmapIter is currently private.
    // See https://github.com/RoaringBitmap/roaring-rs/issues/312
    // iter: roaring::treemap::iter::BitmapIter<'a>,
    high_bits: u64,
    bitmap_iter: roaring::bitmap::Iter<'a>,
}

impl Iterator for DeleteVectorIterator<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let Some(ref mut inner) = &mut self.inner else {
            return None;
        };

        if let Some(lower) = inner.bitmap_iter.next() {
            return Some(inner.high_bits & lower as u64);
        };

        // TODO: roaring::treemap::iter::BitmapIter is currently private.
        // See https://github.com/RoaringBitmap/roaring-rs/issues/312

        // replace with commented-out code below once BitmapIter is pub,
        // or use RoaringTreemap::iter if `advance_to` gets implemented natively
        None

        // let Some((high_bits, bitmap)) = inner.iter.next() else {
        //     self.inner = None;
        //     return None;
        // };
        //
        // inner.high_bits = (high_bits as u64) << 32;
        // inner.bitmap_iter = bitmap.iter();
        //
        // self.next()
    }
}

impl<'a> DeleteVectorIterator<'a> {
    pub fn advance_to(&'a mut self, _pos: u64) {
        // TODO
    }
}
