#[cfg(test)]
mod test;

use crate::{
    common::{
        ende::{KeyEnDe, ValueEnDe},
        RawValue,
    },
    versioned_multi_key::mapx_raw::MapxRawMkVs,
    BranchName, VersionName, VsMgmt,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

const KEY_SIZE: usize = 2;

/// A versioned map structure with two-level keys.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxDkVs<K1, K2, V> {
    inner: MapxRawMkVs,
    p: PhantomData<(K1, K2, V)>,
}

impl<K1, K2, V> Clone for MapxDkVs<K1, K2, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            p: PhantomData,
        }
    }
}

impl<K1, K2, V> MapxDkVs<K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    #[inline(always)]
    pub fn new() -> Self {
        MapxDkVs {
            inner: MapxRawMkVs::new(KEY_SIZE),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &(&K1, &K2)) -> Option<V> {
        let key = Self::encode_key(key);
        self.inner
            .get(&keyref(&key))
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
    pub fn entry_ref<'a>(
        &'a mut self,
        key: &'a (&'a K1, &'a K2),
    ) -> Entry<'a, K1, K2, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn insert(&mut self, key: (K1, K2), value: V) -> Result<Option<V>> {
        let key = (&key.0, &key.1);
        self.insert_ref(&key, &value).c(d!())
    }

    #[inline(always)]
    pub fn insert_ref(&mut self, key: &(&K1, &K2), value: &V) -> Result<Option<V>> {
        let key = Self::encode_key(key);
        self.inner
            .insert(&keyref(&key), &value.encode())
            .c(d!())
            .map(|v| v.map(|v| pnk!(ValueEnDe::decode(&v))))
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &(&K1, &K2)) -> bool {
        let key = Self::encode_key(key);
        self.inner.contains_key(&keyref(&key))
    }

    /// Support batch removal.
    #[inline(always)]
    pub fn remove(&mut self, key: &(&K1, Option<&K2>)) -> Result<Option<V>> {
        let k1 = key.0.encode();
        let k2 = key.1.map(|k2| k2.encode());
        let key = if let Some(k2) = k2.as_ref() {
            vec![&k1[..], &k2[..]]
        } else {
            vec![&k1[..]]
        };
        self.inner
            .remove(&key)
            .c(d!())
            .map(|v| v.map(|v| pnk!(ValueEnDe::decode(&v))))
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline(always)]
    pub fn get_by_branch(&self, key: &(&K1, &K2), branch_name: BranchName) -> Option<V> {
        let key = Self::encode_key(key);
        self.inner
            .get_by_branch(&keyref(&key), branch_name)
            .map(|v| pnk!(ValueEnDe::decode(&v)))
    }

    #[inline(always)]
    pub fn insert_by_branch(
        &mut self,
        key: (K1, K2),
        value: V,
        branch_name: BranchName,
    ) -> Result<Option<V>> {
        let key = (&key.0, &key.1);
        self.insert_ref_by_branch(&key, &value, branch_name).c(d!())
    }

    #[inline(always)]
    pub fn insert_ref_by_branch(
        &mut self,
        key: &(&K1, &K2),
        value: &V,
        branch_name: BranchName,
    ) -> Result<Option<V>> {
        let key = Self::encode_key(key);
        self.inner
            .insert_by_branch(&keyref(&key), &value.encode(), branch_name)
            .c(d!())
            .map(|v| v.map(|v| pnk!(ValueEnDe::decode(&v))))
    }

    #[inline(always)]
    pub fn contains_key_by_branch(
        &self,
        key: &(&K1, &K2),
        branch_name: BranchName,
    ) -> bool {
        let key = Self::encode_key(key);
        self.inner
            .contains_key_by_branch(&keyref(&key), branch_name)
    }

    /// Support batch removal.
    #[inline(always)]
    pub fn remove_by_branch(
        &mut self,
        key: &(&K1, Option<&K2>),
        branch_name: BranchName,
    ) -> Result<Option<V>> {
        let k1 = key.0.encode();
        let k2 = key.1.map(|k2| k2.encode());
        let key = if let Some(k2) = k2.as_ref() {
            vec![&k1[..], &k2[..]]
        } else {
            vec![&k1[..]]
        };
        self.inner
            .remove_by_branch(&key, branch_name)
            .c(d!())
            .map(|v| v.map(|v| pnk!(ValueEnDe::decode(&v))))
    }

    #[inline(always)]
    pub fn get_by_branch_version(
        &self,
        key: &(&K1, &K2),
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<V> {
        let key = Self::encode_key(key);
        self.inner
            .get_by_branch_version(&keyref(&key), branch_name, version_name)
            .map(|v| pnk!(ValueEnDe::decode(&v)))
    }

    #[inline(always)]
    pub fn contains_key_by_branch_version(
        &self,
        key: &(&K1, &K2),
        branch_name: BranchName,
        version_name: VersionName,
    ) -> bool {
        let key = Self::encode_key(key);
        self.inner.contains_key_by_branch_version(
            &keyref(&key),
            branch_name,
            version_name,
        )
    }

    #[inline(always)]
    fn encode_key(key: &(&K1, &K2)) -> [RawValue; 2] {
        let k1 = key.0.encode();
        let k2 = key.1.encode();
        [k1, k2]
    }

    #[inline(always)]
    pub fn iter_op<F>(&self, op: &mut F) -> Result<()>
    where
        F: FnMut((K1, K2), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2), v).c(d!())
        };

        self.inner.iter_op(&mut cb).c(d!())
    }

    pub fn iter_op_by_branch<F>(&self, branch_name: BranchName, op: &mut F) -> Result<()>
    where
        F: FnMut((K1, K2), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2), v).c(d!())
        };

        self.inner.iter_op_by_branch(branch_name, &mut cb).c(d!())
    }

    pub fn iter_op_by_branch_version<F>(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
        op: &mut F,
    ) -> Result<()>
    where
        F: FnMut((K1, K2), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2), v).c(d!())
        };

        self.inner
            .iter_op_by_branch_version(branch_name, version_name, &mut cb)
            .c(d!())
    }

    pub fn iter_op_with_key_prefix<F>(&self, op: &mut F, key_prefix: &K1) -> Result<()>
    where
        F: FnMut((K1, K2), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2), v).c(d!())
        };

        self.inner
            .iter_op_with_key_prefix(&mut cb, &[&KeyEnDe::encode(key_prefix)[..]])
            .c(d!())
    }

    pub fn iter_op_with_key_prefix_by_branch<F>(
        &self,
        branch_name: BranchName,
        op: &mut F,
        key_prefix: &K1,
    ) -> Result<()>
    where
        F: FnMut((K1, K2), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2), v).c(d!())
        };

        self.inner
            .iter_op_with_key_prefix_by_branch(
                branch_name,
                &mut cb,
                &[&KeyEnDe::encode(key_prefix)[..]],
            )
            .c(d!())
    }

    #[inline(always)]
    pub fn iter_op_with_key_prefix_by_branch_version<F>(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
        op: &mut F,
        key_prefix: &K1,
    ) -> Result<()>
    where
        F: FnMut((K1, K2), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2), v).c(d!())
        };

        self.inner
            .iter_op_with_key_prefix_by_branch_version(
                branch_name,
                version_name,
                &mut cb,
                &[&KeyEnDe::encode(key_prefix)[..]],
            )
            .c(d!())
    }
}

impl<K1, K2, V> Default for MapxDkVs<K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K1, K2, V> VsMgmt for MapxDkVs<K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    crate::impl_vs_methods!();
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a, K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    hdr: &'a mut MapxDkVs<K1, K2, V>,
    key: &'a (&'a K1, &'a K2),
    value: V,
}

impl<'a, K1, K2, V> ValueMut<'a, K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    fn new(
        hdr: &'a mut MapxDkVs<K1, K2, V>,
        key: &'a (&'a K1, &'a K2),
        value: V,
    ) -> Self {
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
        pnk!(self.hdr.insert_ref(self.key, &self.value));
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

pub struct Entry<'a, K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    hdr: &'a mut MapxDkVs<K1, K2, V>,
    key: &'a (&'a K1, &'a K2),
}

impl<'a, K1, K2, V> Entry<'a, K1, K2, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    V: ValueEnDe,
{
    pub fn or_insert_ref(self, default: &V) -> ValueMut<'a, K1, K2, V> {
        if !self.hdr.contains_key(self.key) {
            pnk!(self.hdr.insert_ref(self.key, default));
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}

#[inline(always)]
fn keyref(key_array: &[RawValue; 2]) -> [&[u8]; 2] {
    [&key_array[0][..], &key_array[1][..]]
}
