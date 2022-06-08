//!
//! A `Map`-like structure but storing data in disk.
//!
//! NOTE:
//! - Both keys and values will **NOT** be encoded in this structure
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_raw::MapxRaw;
//!
//! let dir = format!("/tmp/__vsdb__{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = MapxRaw::new();
//!
//! l.insert(&[1], &[0]);
//! l.insert(&[1], &[0]);
//! l.insert(&[2], &[0]);
//!
//! l.iter().for_each(|(_, v)| {
//!     assert_eq!(&v[..], &[0]);
//! });
//!
//! l.remove(&[2]);
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
//! ```
//!

#[cfg(test)]
mod test;

use crate::common::{engines, RawKey, RawValue};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut, RangeBounds};

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxRaw {
    inner: engines::Mapx,
}

impl Default for MapxRaw {
    fn default() -> Self {
        Self::new()
    }
}

impl MapxRaw {
    #[inline(always)]
    pub fn new() -> Self {
        MapxRaw {
            inner: engines::Mapx::new(),
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.inner.get(key)
    }

    #[inline(always)]
    pub fn get_mut<'a>(&'a mut self, key: &'a [u8]) -> Option<ValueMut<'a>> {
        self.inner
            .get(key)
            .map(move |v| ValueMut::new(self, key, v))
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    pub fn get_le(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range(..=key).next_back()
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &[u8]) -> Option<(RawKey, RawValue)> {
        self.range(key..).next()
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
    pub fn entry_ref<'a>(&'a mut self, key: &'a [u8]) -> EntryRef<'a> {
        EntryRef { key, hdr: self }
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxRawIter {
        MapxRawIter {
            iter: self.inner.iter(),
        }
    }

    #[inline(always)]
    pub fn range<'a, R: RangeBounds<&'a [u8]>>(&'a self, bounds: R) -> MapxRawIter {
        MapxRawIter {
            iter: self.inner.range(bounds),
        }
    }

    #[inline(always)]
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Option<RawValue> {
        self.inner.insert(key, value)
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &[u8]) -> Option<RawValue> {
        self.inner.remove(key)
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a> {
    hdr: &'a mut MapxRaw,
    key: &'a [u8],
    value: RawValue,
}

impl<'a> ValueMut<'a> {
    fn new(hdr: &'a mut MapxRaw, key: &'a [u8], value: RawValue) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a> Drop for ValueMut<'a> {
    fn drop(&mut self) {
        self.hdr.insert(self.key, &self.value);
    }
}

impl<'a> Deref for ValueMut<'a> {
    type Target = RawValue;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a> DerefMut for ValueMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub struct EntryRef<'a> {
    key: &'a [u8],
    hdr: &'a mut MapxRaw,
}

impl<'a> EntryRef<'a> {
    pub fn or_insert_ref(self, default: &'a [u8]) -> ValueMut<'a> {
        if !self.hdr.contains_key(self.key) {
            self.hdr.insert(self.key, default);
        }
        pnk!(self.hdr.get_mut(self.key))
    }

    pub fn or_insert_ref_with<F>(self, f: F) -> ValueMut<'a>
    where
        F: FnOnce() -> RawValue,
    {
        if !self.hdr.contains_key(self.key) {
            self.hdr.insert(self.key, &f());
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}

pub struct MapxRawIter {
    iter: engines::MapxIter,
}

impl Iterator for MapxRawIter {
    type Item = (RawKey, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl DoubleEndedIterator for MapxRawIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}
