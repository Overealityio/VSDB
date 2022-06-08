//!
//! A `HashMap`-like structure but storing data in disk.
//!
//! NOTE:
//!
//! - Both keys and values will be encoded(serde) in this structure
//!     - Both of them will be encoded by some `serde`-like methods
//!
//! # Examples
//!
//! ```
//! use vsdb::Mapx;
//!
//! let dir = format!("/tmp/__vsdb__{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = Mapx::new();
//!
//! l.insert(1, 0);
//! l.insert_ref(&1, &0);
//! l.insert(2, 0);
//!
//! l.iter().for_each(|(k, v)| {
//!     assert!(k >= 1);
//!     assert_eq!(v, 0);
//! });
//!
//! l.remove(&2);
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
//! ```
//!

#[cfg(test)]
mod test;

use crate::{
    basic::mapx_ord_rawkey::{Entry, MapxOrdRawKey, MapxOrdRawKeyIter, ValueMut},
    common::ende::{KeyEnDe, ValueEnDe},
};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct Mapx<K, V> {
    inner: MapxOrdRawKey<V>,
    p: PhantomData<K>,
}

impl<K, V> Clone for Mapx<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            p: PhantomData,
        }
    }
}

impl<K, V> Copy for Mapx<K, V> {}

impl<K, V> Default for Mapx<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Mapx<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    #[inline(always)]
    pub fn new() -> Self {
        Mapx {
            inner: MapxOrdRawKey::new(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&key.encode())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, V>> {
        let k = key.encode();
        self.inner
            .get(&k)
            .map(|v| ValueMut::new(&mut self.inner, k, v))
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.encode())
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
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.insert_ref(&key, &value)
    }

    #[inline(always)]
    pub fn insert_ref(&mut self, key: &K, value: &V) -> Option<V> {
        self.inner.insert_ref(&key.encode(), value)
    }

    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.set_value_ref(&key, &value)
    }

    #[inline(always)]
    pub fn set_value_ref(&mut self, key: &K, value: &V) {
        self.inner.set_value_ref(&key.encode(), value);
    }

    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, V> {
        self.inner.entry(key.encode())
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxIter<K, V> {
        MapxIter {
            iter: self.inner.iter(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn values(&self) -> MapxValues<K, V> {
        MapxValues { iter: self.iter() }
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(&key.encode())
    }

    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.unset_value(&key.encode());
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

pub struct MapxIter<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    iter: MapxOrdRawKeyIter<V>,
    p: PhantomData<K>,
}

impl<K, V> Iterator for MapxIter<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(k, v)| (<K as KeyEnDe>::decode(&k).unwrap(), v))
    }
}

impl<K, V> DoubleEndedIterator for MapxIter<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (<K as KeyEnDe>::decode(&k).unwrap(), v))
    }
}

pub struct MapxValues<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    iter: MapxIter<K, V>,
}

impl<K, V> Iterator for MapxValues<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(_, v)| v)
    }
}

impl<K, V> DoubleEndedIterator for MapxValues<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|(_, v)| v)
    }
}

impl<K, V> ExactSizeIterator for MapxValues<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
}
