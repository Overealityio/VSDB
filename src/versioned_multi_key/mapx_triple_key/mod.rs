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

const KEY_SIZE: usize = 3;

/// A versioned map structure with tree-level keys.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxTkVs<K1, K2, K3, V> {
    inner: MapxRawMkVs,
    p: PhantomData<(K1, K2, K3, V)>,
}

impl<K1, K2, K3, V> Clone for MapxTkVs<K1, K2, K3, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            p: PhantomData,
        }
    }
}

impl<K1, K2, K3, V> MapxTkVs<K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    #[inline(always)]
    pub fn new() -> Self {
        MapxTkVs {
            inner: MapxRawMkVs::new(KEY_SIZE),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &(&K1, &K2, &K3)) -> Option<V> {
        let key = Self::encode_key(key);
        self.inner
            .get(&keyref(&key))
            .map(|v| pnk!(ValueEnDe::decode(&v)))
    }

    #[inline(always)]
    pub fn get_mut<'a>(
        &'a mut self,
        key: &'a (&'a K1, &'a K2, &'a K3),
    ) -> Option<ValueMut<'a, K1, K2, K3, V>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    #[inline(always)]
    pub fn entry_ref<'a>(
        &'a mut self,
        key: &'a (&'a K1, &'a K2, &'a K3),
    ) -> Entry<'a, K1, K2, K3, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    pub fn insert(&mut self, key: (K1, K2, K3), value: V) -> Result<Option<V>> {
        let key = (&key.0, &key.1, &key.2);
        self.insert_ref(&key, &value).c(d!())
    }

    #[inline(always)]
    pub fn insert_ref(&mut self, key: &(&K1, &K2, &K3), value: &V) -> Result<Option<V>> {
        let key = Self::encode_key(key);
        self.inner
            .insert(&keyref(&key), &value.encode())
            .c(d!())
            .map(|v| v.map(|v| pnk!(ValueEnDe::decode(&v))))
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &(&K1, &K2, &K3)) -> bool {
        let key = Self::encode_key(key);
        self.inner.contains_key(&keyref(&key))
    }

    /// Support batch removal.
    #[inline(always)]
    pub fn remove(
        &mut self,
        key: &(&K1, Option<(&K2, Option<&K3>)>),
    ) -> Result<Option<V>> {
        let k1 = key.0.encode();
        let k2_k3 = key
            .1
            .map(|(k2, k3)| (k2.encode(), k3.map(|k3| k3.encode())));
        let key = if let Some((k2, k3)) = k2_k3.as_ref() {
            let mut res = vec![&k1[..], &k2[..]];
            if let Some(k3) = k3.as_ref() {
                res.push(&k3[..]);
            }
            res
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
    pub fn get_by_branch(
        &self,
        key: &(&K1, &K2, &K3),
        branch_name: BranchName,
    ) -> Option<V> {
        let key = Self::encode_key(key);
        self.inner
            .get_by_branch(&keyref(&key), branch_name)
            .map(|v| pnk!(ValueEnDe::decode(&v)))
    }

    #[inline(always)]
    pub fn insert_by_branch(
        &mut self,
        key: (K1, K2, K3),
        value: V,
        branch_name: BranchName,
    ) -> Result<Option<V>> {
        let key = (&key.0, &key.1, &key.2);
        self.insert_ref_by_branch(&key, &value, branch_name).c(d!())
    }

    #[inline(always)]
    pub fn insert_ref_by_branch(
        &mut self,
        key: &(&K1, &K2, &K3),
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
        key: &(&K1, &K2, &K3),
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
        key: &(&K1, Option<(&K2, Option<&K3>)>),
        branch_name: BranchName,
    ) -> Result<Option<V>> {
        let k1 = key.0.encode();
        let k2_k3 = key
            .1
            .map(|(k2, k3)| (k2.encode(), k3.map(|k3| k3.encode())));
        let key = if let Some((k2, k3)) = k2_k3.as_ref() {
            let mut res = vec![&k1[..], &k2[..]];
            if let Some(k3) = k3.as_ref() {
                res.push(&k3[..]);
            }
            res
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
        key: &(&K1, &K2, &K3),
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
        key: &(&K1, &K2, &K3),
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
    fn encode_key(key: &(&K1, &K2, &K3)) -> [RawValue; 3] {
        let k1 = key.0.encode();
        let k2 = key.1.encode();
        let k3 = key.2.encode();
        [k1, k2, k3]
    }

    #[inline(always)]
    pub fn iter_op<F>(&self, op: &mut F) -> Result<()>
    where
        F: FnMut((K1, K2, K3), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let k3 = KeyEnDe::decode(k[2]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2, k3), v).c(d!())
        };

        self.inner.iter_op(&mut cb).c(d!())
    }

    pub fn iter_op_by_branch<F>(&self, branch_name: BranchName, op: &mut F) -> Result<()>
    where
        F: FnMut((K1, K2, K3), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let k3 = KeyEnDe::decode(k[2]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2, k3), v).c(d!())
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
        F: FnMut((K1, K2, K3), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let k3 = KeyEnDe::decode(k[2]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2, k3), v).c(d!())
        };

        self.inner
            .iter_op_by_branch_version(branch_name, version_name, &mut cb)
            .c(d!())
    }

    pub fn iter_op_with_key_prefix<F>(
        &self,
        op: &mut F,
        key_prefix: (&K1, Option<&K2>),
    ) -> Result<()>
    where
        F: FnMut((K1, K2, K3), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let k3 = KeyEnDe::decode(k[2]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2, k3), v).c(d!())
        };

        let k1 = KeyEnDe::encode(key_prefix.0);
        let k2;
        let mut prefix = vec![&k1[..]];
        if let Some(key2) = key_prefix.1 {
            k2 = KeyEnDe::encode(key2);
            prefix.push(&k2[..]);
        }
        let key_prefix = &prefix[..];

        self.inner
            .iter_op_with_key_prefix(&mut cb, key_prefix)
            .c(d!())
    }

    pub fn iter_op_with_key_prefix_by_branch<F>(
        &self,
        branch_name: BranchName,
        op: &mut F,
        key_prefix: (&K1, Option<&K2>),
    ) -> Result<()>
    where
        F: FnMut((K1, K2, K3), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let k3 = KeyEnDe::decode(k[2]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2, k3), v).c(d!())
        };

        let k1 = KeyEnDe::encode(key_prefix.0);
        let k2;
        let mut prefix = vec![&k1[..]];
        if let Some(key2) = key_prefix.1 {
            k2 = KeyEnDe::encode(key2);
            prefix.push(&k2[..]);
        }
        let key_prefix = &prefix[..];

        self.inner
            .iter_op_with_key_prefix_by_branch(branch_name, &mut cb, key_prefix)
            .c(d!())
    }

    #[inline(always)]
    pub fn iter_op_with_key_prefix_by_branch_version<F>(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
        op: &mut F,
        key_prefix: (&K1, Option<&K2>),
    ) -> Result<()>
    where
        F: FnMut((K1, K2, K3), V) -> Result<()>,
    {
        let mut cb = |k: &[&[u8]], v: RawValue| -> Result<()> {
            if KEY_SIZE != k.len() {
                return Err(eg!("key size mismatch"));
            }
            let k1 = KeyEnDe::decode(k[0]).c(d!())?;
            let k2 = KeyEnDe::decode(k[1]).c(d!())?;
            let k3 = KeyEnDe::decode(k[2]).c(d!())?;
            let v = ValueEnDe::decode(&v).c(d!())?;
            op((k1, k2, k3), v).c(d!())
        };

        let k1 = KeyEnDe::encode(key_prefix.0);
        let k2;
        let mut prefix = vec![&k1[..]];
        if let Some(key2) = key_prefix.1 {
            k2 = KeyEnDe::encode(key2);
            prefix.push(&k2[..]);
        }
        let key_prefix = &prefix[..];

        self.inner
            .iter_op_with_key_prefix_by_branch_version(
                branch_name,
                version_name,
                &mut cb,
                key_prefix,
            )
            .c(d!())
    }
}

impl<K1, K2, K3, V> Default for MapxTkVs<K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K1, K2, K3, V> VsMgmt for MapxTkVs<K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    crate::impl_vs_methods!();
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    hdr: &'a mut MapxTkVs<K1, K2, K3, V>,
    key: &'a (&'a K1, &'a K2, &'a K3),
    value: V,
}

impl<'a, K1, K2, K3, V> ValueMut<'a, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    fn new(
        hdr: &'a mut MapxTkVs<K1, K2, K3, V>,
        key: &'a (&'a K1, &'a K2, &'a K3),
        value: V,
    ) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a, K1, K2, K3, V> Drop for ValueMut<'a, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    fn drop(&mut self) {
        pnk!(self.hdr.insert_ref(self.key, &self.value));
    }
}

impl<'a, K1, K2, K3, V> Deref for ValueMut<'a, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K1, K2, K3, V> DerefMut for ValueMut<'a, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub struct Entry<'a, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    hdr: &'a mut MapxTkVs<K1, K2, K3, V>,
    key: &'a (&'a K1, &'a K2, &'a K3),
}

impl<'a, K1, K2, K3, V> Entry<'a, K1, K2, K3, V>
where
    K1: KeyEnDe,
    K2: KeyEnDe,
    K3: KeyEnDe,
    V: ValueEnDe,
{
    pub fn or_insert_ref(self, default: &V) -> ValueMut<'a, K1, K2, K3, V> {
        if !self.hdr.contains_key(self.key) {
            pnk!(self.hdr.insert_ref(self.key, default));
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}

#[inline(always)]
fn keyref(key_array: &[RawValue; 3]) -> [&[u8]; 3] {
    [&key_array[0][..], &key_array[1][..], &key_array[2][..]]
}
