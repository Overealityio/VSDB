//!
//! A `BTreeMap`-like structure but storing data in disk.
//!
//! NOTE:
//!
//! - Both keys and values will be encoded in this structure
//!     - Keys will be encoded by `KeyEnDeOrdered`
//!     - Values will be encoded by some `serde`-like methods
//! - It's your duty to ensure that the encoded key keeps a same order with the original key
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord::MapxOrd;
//!
//! let dir = format!("/tmp/__vsdb__{}", rand::random::<u128>());
//! vsdb::vsdb_set_base_dir(&dir);
//!
//! let mut l = MapxOrd::new();
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
    common::ende::{KeyEnDeOrdered, ValueEnDe},
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxOrd<K, V> {
    inner: MapxOrdRawKey<V>,
    p: PhantomData<K>,
}

impl<K, V> Clone for MapxOrd<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            p: PhantomData,
        }
    }
}

impl<K, V> Copy for MapxOrd<K, V> {}

impl<K, V> Default for MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrd {
            inner: MapxOrdRawKey::new(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&key.to_bytes())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, V>> {
        let k = key.to_bytes();
        self.inner
            .get(&k)
            .map(|v| ValueMut::new(&mut self.inner, k, v))
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.to_bytes())
    }

    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_le(&key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.inner
            .get_ge(&key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
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
        self.inner.insert_ref(&key.to_bytes(), value)
    }

    // used to support efficient versioned-implementations
    #[inline(always)]
    pub(crate) fn insert_ref_encoded_value(
        &mut self,
        key: &K,
        value: &[u8],
    ) -> Option<V> {
        self.inner.insert_ref_encoded_value(&key.to_bytes(), value)
    }

    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.set_value_ref(&key, &value);
    }

    #[inline(always)]
    pub fn set_value_ref(&mut self, key: &K, value: &V) {
        self.inner.insert_ref(&key.to_bytes(), value);
    }

    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, V> {
        Entry {
            key: key.to_bytes(),
            hdr: &mut self.inner,
        }
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxOrdIter<K, V> {
        MapxOrdIter {
            iter: self.inner.iter(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn values(&self) -> MapxOrdValues<K, V> {
        MapxOrdValues { iter: self.iter() }
    }

    #[inline(always)]
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> MapxOrdIter<K, V> {
        self.range_ref((bounds.start_bound(), bounds.end_bound()))
    }

    #[inline(always)]
    pub fn range_ref<'a, R: RangeBounds<&'a K>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdIter<K, V> {
        let ll;
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                ll = lo.to_bytes();
                Bound::Included(&ll[..])
            }
            Bound::Excluded(lo) => {
                ll = lo.to_bytes();
                Bound::Excluded(&ll[..])
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let hh;
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                hh = hi.to_bytes();
                Bound::Included(&hh[..])
            }
            Bound::Excluded(hi) => {
                hh = hi.to_bytes();
                Bound::Excluded(&hh[..])
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxOrdIter {
            iter: self.inner.range_ref((l, h)),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first(&self) -> Option<(K, V)> {
        self.iter().next()
    }

    #[inline(always)]
    pub fn last(&self) -> Option<(K, V)> {
        self.iter().next_back()
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(&key.to_bytes())
    }

    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.remove(&key.to_bytes());
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

pub struct MapxOrdIter<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    iter: MapxOrdRawKeyIter<V>,
    p: PhantomData<K>,
}

impl<K, V> Iterator for MapxOrdIter<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<K, V> DoubleEndedIterator for MapxOrdIter<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<K, V> ExactSizeIterator for MapxOrdIter<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
}

pub struct MapxOrdValues<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    iter: MapxOrdIter<K, V>,
}

impl<K, V> Iterator for MapxOrdValues<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(_, v)| v)
    }
}

impl<K, V> DoubleEndedIterator for MapxOrdValues<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|(_, v)| v)
    }
}

impl<K, V> ExactSizeIterator for MapxOrdValues<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
}
