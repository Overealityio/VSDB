//!
//! A disk-storage replacement for the in-memory Vec.
//!
//! NOTE:
//! - Values will be encoded by some `serde`-like methods
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::vecx_raw::VecxRaw;
//!
//! let dir = format!("/tmp/__vsdb__{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = VecxRaw::new();
//!
//! l.push_ref(&1u8.to_be_bytes());
//! for i in l.iter() {
//!     assert_eq!(&1u8.to_be_bytes(), &i[..]);
//! }
//!
//! l.pop();
//! assert_eq!(l.len(), 0);
//!
//! l.insert_ref(0, &1u8.to_be_bytes());
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
//! ```

#[cfg(test)]
mod test;

use crate::{
    basic::mapx_ord_rawvalue::{MapxOrdRawValue, MapxOrdRawValueIter, ValueMut},
    common::RawValue,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct VecxRaw {
    inner: MapxOrdRawValue<u64>,
}

impl Default for VecxRaw {
    fn default() -> Self {
        Self::new()
    }
}

impl VecxRaw {
    #[inline(always)]
    pub fn new() -> Self {
        VecxRaw {
            inner: MapxOrdRawValue::new(),
        }
    }

    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<RawValue> {
        self.inner.get(&(idx as u64))
    }

    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<ValueMut<'_, u64>> {
        let idx = idx as u64;
        self.inner
            .get(&idx)
            .map(|v| ValueMut::new(&mut self.inner, idx, v))
    }

    #[inline(always)]
    pub fn last(&self) -> Option<RawValue> {
        alt!(self.is_empty(), return None);
        Some(self.inner.get(&(self.len() as u64 - 1)).unwrap())
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub fn push(&mut self, v: RawValue) {
        self.push_ref(&v)
    }

    #[inline(always)]
    pub fn push_ref(&mut self, v: &[u8]) {
        self.inner.insert_ref(&(self.len() as u64), v);
    }

    #[inline(always)]
    pub fn insert(&mut self, idx: usize, v: RawValue) {
        self.insert_ref(idx, &v)
    }

    #[inline(always)]
    pub fn insert_ref(&mut self, idx: usize, v: &[u8]) {
        let idx = idx as u64;
        match (self.len() as u64).cmp(&idx) {
            Ordering::Greater => {
                self.inner
                    .range_ref(&idx..&(self.len() as u64))
                    .for_each(|(i, iv)| {
                        self.inner.insert_ref(&(i + 1), &iv);
                    });
                self.inner.insert_ref(&idx, v);
            }
            Ordering::Equal => {
                self.push_ref(v);
            }
            Ordering::Less => {
                panic!("out of index");
            }
        }
    }

    #[inline(always)]
    pub fn pop(&mut self) -> Option<RawValue> {
        alt!(self.is_empty(), return None);
        self.inner.remove(&(self.len() as u64 - 1))
    }

    #[inline(always)]
    pub fn remove(&mut self, idx: usize) -> RawValue {
        let idx = idx as u64;
        if !self.is_empty() && idx < self.len() as u64 {
            let last_idx = self.len() as u64 - 1;
            let ret = self.inner.remove(&idx).unwrap();
            self.inner.range_ref(&(1 + idx)..).for_each(|(i, v)| {
                self.inner.insert_ref(&(i - 1), &v);
            });
            self.inner.remove(&last_idx);
            return ret;
        }
        panic!("out of index");
    }

    #[inline(always)]
    pub fn swap_remove(&mut self, idx: usize) -> RawValue {
        let idx = idx as u64;
        if !self.is_empty() && idx < self.len() as u64 {
            let last_idx = self.len() as u64 - 1;
            let ret = self.inner.remove(&idx).unwrap();
            if let Some(v) = self.inner.remove(&last_idx) {
                self.inner.insert_ref(&idx, &v);
            }
            return ret;
        }
        panic!("out of index");
    }

    pub fn update(&mut self, idx: usize, v: RawValue) -> Option<RawValue> {
        self.update_ref(idx, &v)
    }

    #[inline(always)]
    pub fn update_ref(&mut self, idx: usize, v: &[u8]) -> Option<RawValue> {
        if idx < self.len() {
            return self.inner.insert_ref(&(idx as u64), v);
        }
        panic!("out of index");
    }

    #[inline(always)]
    pub fn iter(&self) -> VecxRawIter {
        VecxRawIter {
            iter: self.inner.iter(),
        }
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

pub struct VecxRawIter {
    iter: MapxOrdRawValueIter<u64>,
}

impl Iterator for VecxRawIter {
    type Item = RawValue;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.1)
    }
}

impl DoubleEndedIterator for VecxRawIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|v| v.1)
    }
}
