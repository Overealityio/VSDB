//!
//! A double-key style of `Mapx`.
//!
//! NOTE:
//! - Both keys and values will be encoded in this structure
//!

#[cfg(test)]
mod test;

use crate::{
    basic_multi_key::mapx_raw::MapxRawMk,
    common::ende::{KeyEnDe, ValueEnDe},
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

const KEY_SIZE: usize = 2;

/// A map structure with two-level keys.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxDk<K1, K2, V> {
    inner: MapxRawMk,
    p: PhantomData<(K1, K2, V)>,
}

impl<K1, K2, V> Clone for MapxDk<K1, K2, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            p: PhantomData,
        }
    }
}

impl<K1, K2, V> Copy for MapxDk<K1, K2, V> {}

impl<K1, K2, V> MapxDk<K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            inner: MapxRawMk::new(KEY_SIZE),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &(&K1, &K2)) -> Option<V> {
        let k1 = key.0.encode();
        let k2 = key.1.encode();
        self.inner
            .get(&[&k1, &k2])
            .map(|v| pnk!(ValueEnDe::decode(&v)))
    }

    #[inline(always)]
    pub fn get_mut<'a>(
        &'a mut self,
        key: &'a (&'a K1, &'a K2),
    ) -> Option<ValueMut<'a, K1, K2, V>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &(&K1, &K2)) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub fn entry_ref<'a>(
        &'a mut self,
        key: &'a (&'a K1, &'a K2),
    ) -> Entry<'a, K1, K2, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn insert(&mut self, key: &(&K1, &K2), value: &V) -> Option<V> {
        let k1 = key.0.encode();
        let k2 = key.1.encode();
        let v = value.encode();
        pnk!(self.inner.insert(&[&k1, &k2], &v))
            .map(|old_v| pnk!(ValueEnDe::decode(&old_v)))
    }

    /// Support batch removal.
    #[inline(always)]
    pub fn remove(&mut self, key: &(&K1, Option<&K2>)) -> Option<V> {
        let k1 = key.0.encode();
        let k2 = key.1.map(|k2| k2.encode());
        let k = if let Some(k2) = k2.as_ref() {
            vec![&k1[..], &k2[..]]
        } else {
            vec![&k1[..]]
        };
        pnk!(self.inner.remove(k.as_slice()))
            .map(|old_v| pnk!(ValueEnDe::decode(&old_v)))
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline(always)]
    pub fn key_size(&self) -> usize {
        self.inner.key_size()
    }
}

impl<K1, K2, V> Default for MapxDk<K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a, K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    hdr: &'a mut MapxDk<K1, K2, V>,
    key: &'a (&'a K1, &'a K2),
    value: V,
}

impl<'a, K1, K2, V> ValueMut<'a, K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    fn new(hdr: &'a mut MapxDk<K1, K2, V>, key: &'a (&'a K1, &'a K2), value: V) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a, K1, K2, V> Drop for ValueMut<'a, K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    fn drop(&mut self) {
        pnk!(self.hdr.insert(self.key, &self.value));
    }
}

impl<'a, K1, K2, V> Deref for ValueMut<'a, K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K1, K2, V> DerefMut for ValueMut<'a, K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub struct Entry<'a, K1, K2, V> {
    hdr: &'a mut MapxDk<K1, K2, V>,
    key: &'a (&'a K1, &'a K2),
}

impl<'a, K1, K2, V> Entry<'a, K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    pub fn or_insert_ref(self, default: &'a V) -> ValueMut<'a, K1, K2, V> {
        if !self.hdr.contains_key(self.key) {
            self.hdr.insert(self.key, default);
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}
