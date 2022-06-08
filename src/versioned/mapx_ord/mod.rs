//!
//! Documents => [MapxRawVs](crate::versioned::mapx_raw)
//!
#[cfg(test)]
mod test;

use crate::{
    common::ende::{KeyEnDeOrdered, ValueEnDe},
    versioned::mapx_ord_rawkey::{MapxOrdRawKeyVs, MapxOrdRawKeyVsIter},
    BranchName, VersionName, VsMgmt,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Bound, Deref, DerefMut, RangeBounds},
};

/// Documents => [MapxRawVs](crate::versioned::mapx_raw::MapxRawVs)
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct MapxOrdVs<K, V> {
    inner: MapxOrdRawKeyVs<V>,
    p: PhantomData<K>,
}

impl<K, V> Clone for MapxOrdVs<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            p: PhantomData,
        }
    }
}

impl<K, V> Default for MapxOrdVs<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> MapxOrdVs<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrdVs {
            inner: MapxOrdRawKeyVs::new(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&key.to_bytes())
    }

    #[inline(always)]
    pub fn get_mut<'a>(&'a mut self, key: &'a K) -> Option<ValueMut<'a, K, V>> {
        self.get(key).map(move |v| ValueMut::new(self, key, v))
    }

    #[inline(always)]
    pub fn entry_ref<'a>(&'a mut self, key: &'a K) -> Entry<'a, K, V> {
        Entry { key, hdr: self }
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
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>> {
        self.insert_ref(&key, &value).c(d!())
    }

    #[inline(always)]
    pub fn insert_ref(&mut self, key: &K, value: &V) -> Result<Option<V>> {
        self.inner.insert_ref(&key.to_bytes(), value).c(d!())
    }

    #[inline(always)]
    pub fn iter(&self) -> MapxOrdVsIter<K, V> {
        MapxOrdVsIter {
            iter: self.inner.iter(),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range<'a, R: 'a + RangeBounds<K>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdVsIter<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(i.to_bytes()),
            Bound::Excluded(i) => Bound::Excluded(i.to_bytes()),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(i.to_bytes()),
            Bound::Excluded(i) => Bound::Excluded(i.to_bytes()),
            _ => Bound::Unbounded,
        };

        MapxOrdVsIter {
            iter: self.inner.range((l, h)),
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
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.to_bytes())
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Result<Option<V>> {
        self.inner.remove(&key.to_bytes()).c(d!())
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline(always)]
    pub fn get_by_branch(&self, key: &K, branch_name: BranchName) -> Option<V> {
        self.inner.get_by_branch(&key.to_bytes(), branch_name)
    }

    #[inline(always)]
    pub fn get_le_by_branch(&self, key: &K, branch_name: BranchName) -> Option<(K, V)> {
        self.inner
            .get_le_by_branch(&key.to_bytes(), branch_name)
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    #[inline(always)]
    pub fn get_ge_by_branch(&self, key: &K, branch_name: BranchName) -> Option<(K, V)> {
        self.inner
            .get_ge_by_branch(&key.to_bytes(), branch_name)
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    #[inline(always)]
    pub fn len_by_branch(&self, branch_name: BranchName) -> usize {
        self.inner.len_by_branch(branch_name)
    }

    #[inline(always)]
    pub fn is_empty_by_branch(&self, branch_name: BranchName) -> bool {
        self.inner.is_empty_by_branch(branch_name)
    }

    #[inline(always)]
    pub fn insert_by_branch(
        &mut self,
        key: K,
        value: V,
        branch_name: BranchName,
    ) -> Result<Option<V>> {
        self.insert_ref_by_branch(&key, &value, branch_name).c(d!())
    }

    #[inline(always)]
    pub fn insert_ref_by_branch(
        &mut self,
        key: &K,
        value: &V,
        branch_name: BranchName,
    ) -> Result<Option<V>> {
        self.inner
            .insert_ref_by_branch(&key.to_bytes(), value, branch_name)
            .c(d!())
    }

    #[inline(always)]
    pub fn iter_by_branch(&self, branch_name: BranchName) -> MapxOrdVsIter<K, V> {
        MapxOrdVsIter {
            iter: self.inner.iter_by_branch(branch_name),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_by_branch<'a, R: 'a + RangeBounds<K>>(
        &'a self,
        branch_name: BranchName,
        bounds: R,
    ) -> MapxOrdVsIter<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(i.to_bytes()),
            Bound::Excluded(i) => Bound::Excluded(i.to_bytes()),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(i.to_bytes()),
            Bound::Excluded(i) => Bound::Excluded(i.to_bytes()),
            _ => Bound::Unbounded,
        };

        MapxOrdVsIter {
            iter: self.inner.range_by_branch(branch_name, (l, h)),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first_by_branch(&self, branch_name: BranchName) -> Option<(K, V)> {
        self.iter_by_branch(branch_name).next()
    }

    #[inline(always)]
    pub fn last_by_branch(&self, branch_name: BranchName) -> Option<(K, V)> {
        self.iter_by_branch(branch_name).next_back()
    }

    #[inline(always)]
    pub fn contains_key_by_branch(&self, key: &K, branch_name: BranchName) -> bool {
        self.inner
            .contains_key_by_branch(&key.to_bytes(), branch_name)
    }

    #[inline(always)]
    pub fn remove_by_branch(
        &mut self,
        key: &K,
        branch_name: BranchName,
    ) -> Result<Option<V>> {
        self.inner
            .remove_by_branch(&key.to_bytes(), branch_name)
            .c(d!())
    }

    #[inline(always)]
    pub fn get_by_branch_version(
        &self,
        key: &K,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<V> {
        self.inner
            .get_by_branch_version(&key.to_bytes(), branch_name, version_name)
    }

    #[inline(always)]
    pub fn get_le_by_branch_version(
        &self,
        key: &K,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<(K, V)> {
        self.inner
            .get_le_by_branch_version(&key.to_bytes(), branch_name, version_name)
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    #[inline(always)]
    pub fn get_ge_by_branch_version(
        &self,
        key: &K,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<(K, V)> {
        self.inner
            .get_ge_by_branch_version(&key.to_bytes(), branch_name, version_name)
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    #[inline(always)]
    pub fn len_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> usize {
        self.inner.len_by_branch_version(branch_name, version_name)
    }

    #[inline(always)]
    pub fn is_empty_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> bool {
        self.inner
            .is_empty_by_branch_version(branch_name, version_name)
    }

    #[inline(always)]
    pub fn iter_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> MapxOrdVsIter<K, V> {
        MapxOrdVsIter {
            iter: self.inner.iter_by_branch_version(branch_name, version_name),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn range_by_branch_version<'a, R: 'a + RangeBounds<K>>(
        &'a self,
        branch_name: BranchName,
        version_name: VersionName,
        bounds: R,
    ) -> MapxOrdVsIter<'a, K, V> {
        let l = match bounds.start_bound() {
            Bound::Included(i) => Bound::Included(i.to_bytes()),
            Bound::Excluded(i) => Bound::Excluded(i.to_bytes()),
            _ => Bound::Unbounded,
        };
        let h = match bounds.end_bound() {
            Bound::Included(i) => Bound::Included(i.to_bytes()),
            Bound::Excluded(i) => Bound::Excluded(i.to_bytes()),
            _ => Bound::Unbounded,
        };

        MapxOrdVsIter {
            iter: self
                .inner
                .range_by_branch_version(branch_name, version_name, (l, h)),
            p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn first_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<(K, V)> {
        self.iter_by_branch_version(branch_name, version_name)
            .next()
    }

    #[inline(always)]
    pub fn last_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<(K, V)> {
        self.iter_by_branch_version(branch_name, version_name)
            .next_back()
    }

    #[inline(always)]
    pub fn contains_key_by_branch_version(
        &self,
        key: &K,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> bool {
        self.inner.contains_key_by_branch_version(
            &key.to_bytes(),
            branch_name,
            version_name,
        )
    }
}

impl<K, V> VsMgmt for MapxOrdVs<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    crate::impl_vs_methods!();
}

pub struct MapxOrdVsIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    iter: MapxOrdRawKeyVsIter<'a, V>,
    p: PhantomData<K>,
}

impl<'a, K, V> Iterator for MapxOrdVsIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<'a, K, V> DoubleEndedIterator for MapxOrdVsIter<'a, K, V>
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

impl<'a, K, V> ExactSizeIterator for MapxOrdVsIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    hdr: &'a mut MapxOrdVs<K, V>,
    key: &'a K,
    value: V,
}

impl<'a, K, V> ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn new(hdr: &'a mut MapxOrdVs<K, V>, key: &'a K, value: V) -> Self {
        ValueMut { hdr, key, value }
    }
}

impl<'a, K, V> Drop for ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn drop(&mut self) {
        pnk!(self.hdr.insert_ref(self.key, &self.value));
    }
}

impl<'a, K, V> Deref for ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K, V> DerefMut for ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub struct Entry<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    hdr: &'a mut MapxOrdVs<K, V>,
    key: &'a K,
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    pub fn or_insert_ref(self, default: &V) -> ValueMut<'a, K, V> {
        if !self.hdr.contains_key(self.key) {
            pnk!(self.hdr.insert_ref(self.key, default));
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}
