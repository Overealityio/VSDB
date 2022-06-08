//!
//! `MapxRawKeyMk`, aka `MapxRawMk` with typed values.
//!
//! NOTE:
//! - Values will be encoded in this structure
//!

#[cfg(test)]
mod test;

use crate::{basic_multi_key::mapx_raw::MapxRawMk, common::ende::ValueEnDe};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxRawKeyMk<V> {
    inner: MapxRawMk,
    p: PhantomData<V>,
}

impl<V> Clone for MapxRawKeyMk<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner,
            p: PhantomData,
        }
    }
}

impl<V> Copy for MapxRawKeyMk<V> {}

impl<V: ValueEnDe> MapxRawKeyMk<V> {
    /// # Panic
    /// Will panic if `0 == key_size`.
    #[inline(always)]
    pub fn new(key_size: usize) -> Self {
        Self {
            inner: MapxRawMk::new(key_size),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &[&[u8]]) -> Option<V> {
        self.inner.get(key).map(|v| pnk!(ValueEnDe::decode(&v)))
    }

    #[inline(always)]
    pub fn get_mut<'a>(&'a mut self, key: &'a [&'a [u8]]) -> Option<ValueMut<'a, V>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &[&[u8]]) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub fn entry_ref<'a>(&'a mut self, key: &'a [&'a [u8]]) -> Entry<'a, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn insert(&mut self, key: &[&[u8]], value: &V) -> Result<Option<V>> {
        let v = value.encode();
        self.inner
            .insert(key, &v)
            .c(d!())
            .map(|v| v.map(|old_v| pnk!(ValueEnDe::decode(&old_v))))
    }

    /// Support batch removal.
    #[inline(always)]
    pub fn remove(&mut self, key: &[&[u8]]) -> Result<Option<V>> {
        self.inner
            .remove(key)
            .c(d!())
            .map(|v| v.map(|old_v| pnk!(ValueEnDe::decode(&old_v))))
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline(always)]
    pub fn iter_op<F>(&self, op: &mut F) -> Result<()>
    where
        F: FnMut(&[&[u8]], &V) -> Result<()>,
    {
        self.inner.iter_op_typed_value(op).c(d!())
    }

    #[inline(always)]
    pub fn iter_op_with_key_prefix<F>(
        &self,
        op: &mut F,
        key_prefix: &[&[u8]],
    ) -> Result<()>
    where
        F: FnMut(&[&[u8]], &V) -> Result<()>,
    {
        self.inner
            .iter_op_typed_value_with_key_prefix(op, key_prefix)
            .c(d!())
    }

    #[inline(always)]
    pub fn key_size(&self) -> usize {
        self.inner.key_size()
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a, V: ValueEnDe> {
    hdr: &'a mut MapxRawKeyMk<V>,
    key: &'a [&'a [u8]],
    value: V,
}

impl<'a, V: ValueEnDe> ValueMut<'a, V> {
    fn new(hdr: &'a mut MapxRawKeyMk<V>, key: &'a [&'a [u8]], value: V) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a, V: ValueEnDe> Drop for ValueMut<'a, V> {
    fn drop(&mut self) {
        pnk!(self.hdr.insert(self.key, &self.value));
    }
}

impl<'a, V: ValueEnDe> Deref for ValueMut<'a, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V: ValueEnDe> DerefMut for ValueMut<'a, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub struct Entry<'a, V> {
    hdr: &'a mut MapxRawKeyMk<V>,
    key: &'a [&'a [u8]],
}

impl<'a, V: ValueEnDe> Entry<'a, V> {
    pub fn or_insert_ref(self, default: &'a V) -> Result<ValueMut<'a, V>> {
        if !self.hdr.contains_key(self.key) {
            self.hdr.insert(self.key, default).c(d!())?;
        }
        self.hdr.get_mut(self.key).c(d!())
    }
}
