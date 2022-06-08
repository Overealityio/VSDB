//!
//! Documents => [MapxRawVs](crate::versioned::mapx_raw)
//!

#[cfg(test)]
mod test;

use crate::{
    versioned::mapx_ord_rawkey::{MapxOrdRawKeyVs, MapxOrdRawKeyVsIter},
    BranchName, ValueEnDe, VersionName, VsMgmt,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

/// Documents => [MapxRawVs](crate::versioned::mapx_raw::MapxRawVs)
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct VecxVs<T> {
    inner: MapxOrdRawKeyVs<T>,
}

impl<T> Clone for VecxVs<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ValueEnDe> Default for VecxVs<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ValueEnDe> VecxVs<T> {
    #[inline(always)]
    pub fn new() -> Self {
        VecxVs {
            inner: MapxOrdRawKeyVs::new(),
        }
    }

    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<T> {
        self.inner.get(&(idx as u64).to_be_bytes())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<ValueMut<'_, T>> {
        self.get(idx)
            .map(|v| ValueMut::new(&mut self.inner, idx, v))
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
        self.inner
            .insert_ref(&(self.len() as u64).to_be_bytes(), v)
            .unwrap();
    }

    #[inline(always)]
    pub fn pop(&mut self) -> Result<Option<T>> {
        alt!(self.is_empty(), return Ok(None));
        self.inner.remove(&(self.len() - 1).to_be_bytes()).c(d!())
    }

    pub fn update(&mut self, idx: usize, v: T) -> Result<Option<T>> {
        self.update_ref(idx, &v).c(d!())
    }

    #[inline(always)]
    pub fn update_ref(&mut self, idx: usize, v: &T) -> Result<Option<T>> {
        if idx < self.len() {
            self.inner
                .insert_ref(&(idx as u64).to_be_bytes(), v)
                .c(d!())
        } else {
            Err(eg!("out of index"))
        }
    }

    #[inline(always)]
    pub fn iter(&self) -> VecxVsIter<'_, T> {
        VecxVsIter {
            iter: self.inner.iter(),
        }
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline(always)]
    pub fn get_by_branch(&self, idx: usize, branch_name: BranchName) -> Option<T> {
        self.inner
            .get_by_branch(&(idx as u64).to_be_bytes(), branch_name)
    }

    #[inline(always)]
    pub fn last_by_branch(&self, branch_name: BranchName) -> Option<T> {
        alt!(self.is_empty(), return None);
        Some(
            self.inner
                .get_by_branch(&(self.len() as u64 - 1).to_be_bytes(), branch_name)
                .unwrap(),
        )
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
    pub fn push_by_branch(&mut self, v: T, branch_name: BranchName) {
        self.push_ref_by_branch(&v, branch_name)
    }

    #[inline(always)]
    pub fn push_ref_by_branch(&mut self, v: &T, branch_name: BranchName) {
        self.inner
            .insert_ref_by_branch(&(self.len() as u64).to_be_bytes(), v, branch_name)
            .unwrap();
    }

    #[inline(always)]
    pub fn pop_by_branch(&mut self, branch_name: BranchName) -> Result<Option<T>> {
        alt!(self.is_empty(), return Ok(None));
        self.inner
            .remove_by_branch(&(self.len() - 1).to_be_bytes(), branch_name)
            .c(d!())
    }

    pub fn update_by_branch(
        &mut self,
        idx: usize,
        v: T,
        branch_name: BranchName,
    ) -> Result<Option<T>> {
        self.update_ref_by_branch(idx, &v, branch_name).c(d!())
    }

    #[inline(always)]
    pub fn update_ref_by_branch(
        &mut self,
        idx: usize,
        v: &T,
        branch_name: BranchName,
    ) -> Result<Option<T>> {
        if idx < self.len() {
            self.inner
                .insert_ref_by_branch(&(idx as u64).to_be_bytes(), v, branch_name)
                .c(d!())
        } else {
            Err(eg!("out of index"))
        }
    }

    #[inline(always)]
    pub fn iter_by_branch(&self, branch_name: BranchName) -> VecxVsIter<'_, T> {
        VecxVsIter {
            iter: self.inner.iter_by_branch(branch_name),
        }
    }

    #[inline(always)]
    pub fn get_by_branch_version(
        &self,
        idx: usize,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<T> {
        self.inner.get_by_branch_version(
            &(idx as u64).to_be_bytes(),
            branch_name,
            version_name,
        )
    }

    #[inline(always)]
    pub fn last_by_branch_version(
        &self,
        branch_name: BranchName,
        version_name: VersionName,
    ) -> Option<T> {
        alt!(self.is_empty(), return None);
        Some(
            self.inner
                .get_by_branch_version(
                    &(self.len() as u64 - 1).to_be_bytes(),
                    branch_name,
                    version_name,
                )
                .unwrap(),
        )
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
    ) -> VecxVsIter<'_, T> {
        VecxVsIter {
            iter: self.inner.iter_by_branch_version(branch_name, version_name),
        }
    }
}

impl<T: ValueEnDe> VsMgmt for VecxVs<T> {
    crate::impl_vs_methods!();
}

pub struct VecxVsIter<'a, T: ValueEnDe> {
    iter: MapxOrdRawKeyVsIter<'a, T>,
}

impl<'a, T: ValueEnDe> Iterator for VecxVsIter<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.1)
    }
}

impl<'a, T: ValueEnDe> DoubleEndedIterator for VecxVsIter<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|v| v.1)
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct ValueMut<'a, V: ValueEnDe> {
    hdr: &'a mut MapxOrdRawKeyVs<V>,
    key: u64,
    value: V,
}

impl<'a, V> ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn new(hdr: &'a mut MapxOrdRawKeyVs<V>, key: usize, value: V) -> Self {
        ValueMut {
            hdr,
            key: key as u64,
            value,
        }
    }
}

impl<'a, V> Drop for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        pnk!(self.hdr.insert_ref(&self.key.to_be_bytes(), &self.value));
    }
}

impl<'a, V> Deref for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V> DerefMut for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
