//!
//! A disk-storage replacement for the in-memory Vec.
//!
//! NOTE:
//! - Values will be encoded by some `serde`-like methods
//!
//! # Examples
//!
//! ```
//! use vsdb::Vecx;
//!
//! let dir = format!("/tmp/__vsdb__{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = Vecx::new();
//!
//! l.push(1);
//! for i in l.iter() {
//!     assert_eq!(1, i);
//! }
//!
//! l.pop();
//! assert_eq!(l.len(), 0);
//!
//! l.insert(0, 1);
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
//! ```

#[cfg(test)]
mod test;

use crate::{
    basic::mapx_ord_rawkey::{MapxOrdRawKey, MapxOrdRawKeyIter, ValueMut},
    common::ende::ValueEnDe,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct Vecx<T> {
    inner: MapxOrdRawKey<T>,
}

impl<T> Clone for Vecx<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner }
    }
}

impl<T> Copy for Vecx<T> {}

impl<T: ValueEnDe> Default for Vecx<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ValueEnDe> Vecx<T> {
    #[inline(always)]
    pub fn new() -> Self {
        Vecx {
            inner: MapxOrdRawKey::new(),
        }
    }

    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<T> {
        self.inner.get(&(idx as u64).to_be_bytes())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<ValueMut<'_, T>> {
        let idx_bytes = (idx as u64).to_be_bytes();
        self.inner.get(&idx_bytes).map(|v| {
            ValueMut::new(&mut self.inner, idx_bytes.to_vec().into_boxed_slice(), v)
        })
    }

    #[inline(always)]
    pub fn last(&self) -> Option<T> {
        alt!(self.is_empty(), return None);
        Some(
            self.inner
                .get(&(self.len() as u64 - 1).to_be_bytes())
                .unwrap(),
        )
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
    pub fn push(&mut self, v: T) {
        self.push_ref(&v)
    }

    #[inline(always)]
    pub fn push_ref(&mut self, v: &T) {
        self.inner.insert_ref(&(self.len() as u64).to_be_bytes(), v);
    }

    #[inline(always)]
    pub fn insert(&mut self, idx: usize, v: T) {
        self.insert_ref(idx, &v)
    }

    #[inline(always)]
    pub fn insert_ref(&mut self, idx: usize, v: &T) {
        let idx = idx as u64;
        match (self.len() as u64).cmp(&idx) {
            Ordering::Greater => {
                self.inner
                    .range_ref(
                        &idx.to_be_bytes()[..]..&(self.len() as u64).to_be_bytes()[..],
                    )
                    .for_each(|(i, iv)| {
                        self.inner.insert_ref(
                            &(crate::parse_int!(i, u64) + 1).to_be_bytes(),
                            &iv,
                        );
                    });
                self.inner.insert_ref(&idx.to_be_bytes(), v);
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
    pub fn pop(&mut self) -> Option<T> {
        alt!(self.is_empty(), return None);
        self.inner.remove(&(self.len() as u64 - 1).to_be_bytes())
    }

    #[inline(always)]
    pub fn remove(&mut self, idx: usize) -> T {
        let idx = idx as u64;
        if !self.is_empty() && idx < self.len() as u64 {
            let last_idx = self.len() as u64 - 1;
            let ret = self.inner.remove(&idx.to_be_bytes()).unwrap();
            self.inner
                .range_ref(&(1 + idx).to_be_bytes()[..]..)
                .for_each(|(i, v)| {
                    self.inner
                        .insert_ref(&(crate::parse_int!(i, u64) - 1).to_be_bytes(), &v);
                });
            self.inner.remove(&last_idx.to_be_bytes());
            return ret;
        }
        panic!("out of index");
    }

    #[inline(always)]
    pub fn swap_remove(&mut self, idx: usize) -> T {
        let idx = idx as u64;
        if !self.is_empty() && idx < self.len() as u64 {
            let last_idx = self.len() as u64 - 1;
            let ret = self.inner.remove(&idx.to_be_bytes()).unwrap();
            if let Some(v) = self.inner.remove(&last_idx.to_be_bytes()) {
                self.inner.insert_ref(&idx.to_be_bytes(), &v);
            }
            return ret;
        }
        panic!("out of index");
    }

    pub fn update(&mut self, idx: usize, v: T) -> Option<T> {
        self.update_ref(idx, &v)
    }

    #[inline(always)]
    pub fn update_ref(&mut self, idx: usize, v: &T) -> Option<T> {
        if idx < self.len() {
            return self.inner.insert_ref(&(idx as u64).to_be_bytes(), v);
        }
        panic!("out of index");
    }

    #[inline(always)]
    pub fn iter(&self) -> VecxIter<T> {
        VecxIter {
            iter: self.inner.iter(),
        }
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

pub struct VecxIter<T: ValueEnDe> {
    iter: MapxOrdRawKeyIter<T>,
}

impl<T: ValueEnDe> Iterator for VecxIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.1)
    }
}

impl<T: ValueEnDe> DoubleEndedIterator for VecxIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|v| v.1)
    }
}
