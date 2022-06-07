use super::*;
use crate::{
    common::{BranchName, ParentBranchName, INITIAL_BRANCH_NAME},
    ValueEnDe, VsMgmt,
};

#[test]
fn test_master_branch_exists() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    assert!(hdr.branch_exists(INITIAL_BRANCH_NAME));
}

#[test]
fn test_master_branch_has_versions() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
}

#[test]
fn test_branch_create_no_version() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    let bn = BranchName(b"test");
    let vn = VersionName(b"test1");
    unsafe {
        pnk!(hdr.branch_create_without_new_version(bn, false));
    }
    assert!(hdr.branch_exists(INITIAL_BRANCH_NAME));
    assert!(hdr.branch_exists(bn));
    assert_eq!(false, hdr.branch_has_versions(bn));

    pnk!(hdr.version_create_by_branch(vn, bn));
    assert!(hdr.branch_has_versions(bn));
}

#[test]
fn test_branch_create_by_base_branch() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));
    let bn1 = BranchName(b"test1");
    let vn11 = VersionName(b"testversion11");
    pnk!(hdr.branch_create(bn1, vn11, false));

    let key = <usize as ValueEnDe>::encode(&1);
    let value = 1;
    pnk!(hdr.insert(key, value));

    let bn2 = BranchName(b"test2");
    let vn21 = VersionName(b"testversion21");

    pnk!(hdr.branch_create_by_base_branch(bn2, vn21, ParentBranchName(b"test1"), false));
    let key = <usize as ValueEnDe>::encode(&2);
    let value = 2;
    pnk!(hdr.insert(key, value));
}

#[test]
fn test_branch_remove() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    let bn = BranchName(b"test");
    let vn = VersionName(b"test1");
    pnk!(hdr.branch_create(bn, vn, false));
    assert!(hdr.branch_exists(INITIAL_BRANCH_NAME));
    assert!(hdr.branch_exists(bn));
    pnk!(hdr.branch_remove(bn));
    assert_eq!(false, hdr.branch_exists(bn));
}

#[test]
fn test_branch_merge() {
    let mut hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    let mvn = VersionName(b"manster0");
    pnk!(hdr.version_create(mvn));
    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&1), 1));

    let bn = BranchName(b"test");
    let vn = VersionName(b"test0");

    pnk!(hdr.branch_create(bn, vn, false));
    let key = <usize as ValueEnDe>::encode(&2);
    let value = 2;
    pnk!(hdr.insert(key.clone(), value));
    pnk!(hdr.branch_merge_to(bn, INITIAL_BRANCH_NAME));
    pnk!(hdr.branch_set_default(INITIAL_BRANCH_NAME));
    let val = pnk!(hdr.get_by_branch(&key.clone(), INITIAL_BRANCH_NAME));
    assert_eq!(val, value);
}

#[test]
fn test_branch_pop_version() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
    pnk!(hdr.version_create(VersionName(b"manster0")));
    assert!(hdr.branch_has_versions(INITIAL_BRANCH_NAME));
    pnk!(hdr.branch_pop_version(INITIAL_BRANCH_NAME));
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
}

#[test]
fn test_branch_swap() {
    let mut hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));

    let mkey = <usize as ValueEnDe>::encode(&1);
    let mvalue = 1;
    pnk!(hdr.insert(mkey.clone(), mvalue));

    let bn = BranchName(b"test");
    let vn = VersionName(b"test0");
    pnk!(hdr.branch_create(bn, vn, false));

    let tkey = <usize as ValueEnDe>::encode(&2);
    let tvalue = 2;
    pnk!(hdr.insert(tkey.clone(), tvalue));

    unsafe {
        pnk!(hdr.branch_swap(INITIAL_BRANCH_NAME, bn));
    }
    let val = pnk!(hdr.get_by_branch(&tkey, INITIAL_BRANCH_NAME));
    assert_eq!(val, tvalue);
    let val = pnk!(hdr.get_by_branch(&mkey, bn));
    assert_eq!(val, mvalue);
}
#[test]
fn test_branch_truncate() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));

    let mkey0 = <usize as ValueEnDe>::encode(&1);
    let mvalue0 = 1;
    pnk!(hdr.insert(mkey0.clone(), mvalue0));

    let mkey1 = <usize as ValueEnDe>::encode(&2);
    let mvalue1 = 2;
    pnk!(hdr.insert(mkey1.clone(), mvalue1));

    pnk!(hdr.branch_truncate(INITIAL_BRANCH_NAME));
    assert!(hdr.get(&mkey0).is_none());
    assert!(hdr.get(&mkey1).is_none());
}
#[test]
fn test_branch_truncate_to() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    let vn = VersionName(b"manster0");
    pnk!(hdr.version_create(vn));

    let mkey0 = <usize as ValueEnDe>::encode(&1);
    let mvalue0 = 1;
    pnk!(hdr.insert(mkey0.clone(), mvalue0));

    pnk!(hdr.version_create(VersionName(b"manster1")));

    let mkey1 = <usize as ValueEnDe>::encode(&2);
    let mvalue1 = 2;
    pnk!(hdr.insert(mkey1.clone(), mvalue1));

    pnk!(hdr.branch_truncate_to(INITIAL_BRANCH_NAME, vn));

    assert!(hdr.get(&mkey1).is_none());
}
#[test]
fn test_insert() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));
    let max = 500;
    (0..max)
        .map(|i: usize| (i, (max + i)))
        .for_each(|(key, value)| {
            let key = <usize as ValueEnDe>::encode(&key);
            assert!(hdr.get(&key).is_none());
            assert!(pnk!(hdr.insert(key.clone(), value)).is_none());
            assert!(pnk!(hdr.insert(key.clone(), value)).is_some());
            assert!(hdr.contains_key(&key));
            assert_eq!(pnk!(hdr.get(&key)), value);
            assert_eq!(pnk!(pnk!(hdr.remove(&key))), value);
            assert!(hdr.get(&key).is_none());
        });
}

#[test]
fn test_len() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));
    let max = 500;
    (0..max)
        .map(|i: usize| (i, (max + i)))
        .for_each(|(key, value)| {
            let key = <usize as ValueEnDe>::encode(&key);
            assert!(pnk!(hdr.insert(key, value)).is_none());
        });
    assert_eq!(500, hdr.len());

    for key in 0..max {
        let key = <usize as ValueEnDe>::encode(&key);
        assert!(pnk!(hdr.remove(&key)).is_some());
    }
    assert_eq!(0, hdr.len());
}

#[test]
fn test_valueende() {
    let cnt = 500;
    let dehdr = {
        let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
        pnk!(hdr.version_create(VersionName(b"manster0")));
        (0..cnt).map(|i: usize| (i, i)).for_each(|(key, value)| {
            let key = <usize as ValueEnDe>::encode(&key);
            assert!(pnk!(hdr.insert(key, value)).is_none());
        });
        crate::vsdb_flush();
        <MapxOrdRawKeyVs<usize> as ValueEnDe>::encode(&hdr)
    };
    let reloaded = pnk!(<MapxOrdRawKeyVs<usize> as ValueEnDe>::decode(&dehdr));
    assert_eq!(cnt, reloaded.len());
    (0..cnt).map(|i: usize| i).for_each(|i| {
        let key = <usize as ValueEnDe>::encode(&i);
        assert_eq!(i, reloaded.get(&key).unwrap());
    });
}

#[test]
fn test_emptystr_version() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    pnk!(hdr.version_create(VersionName(b"")));

    let key = <usize as ValueEnDe>::encode(&1);
    let value = 1;

    assert!(hdr.get(&key).is_none());
    assert!(pnk!(hdr.insert(key.clone(), value)).is_none());
    assert!(hdr.contains_key(&key));
    assert_eq!(pnk!(hdr.get(&key)), value);
    assert_eq!(pnk!(pnk!(hdr.remove(&key))), value);
    assert!(hdr.get(&key).is_none());
}

#[test]
fn test_version_create() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    pnk!(hdr.version_create(VersionName(b"v-001")));
    assert!(hdr.version_create(VersionName(b"v-001")).is_err());
    assert!(hdr.is_empty());
    pnk!(hdr.version_create(VersionName(b"v-002")));
    assert!(hdr.is_empty());
}

#[test]
fn test_version_empty() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    pnk!(hdr.version_create(VersionName(b"v-001")));
    assert!(hdr.is_empty());
    pnk!(hdr.version_create(VersionName(b"v-002")));
    assert!(hdr.is_empty());
    assert!(hdr.is_empty_by_branch(INITIAL_BRANCH_NAME));
    assert!(hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-001")));
    assert!(hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-002")));

    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&1), 1));
    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&2), 2));
    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&3), 3));

    assert!(!hdr.is_empty());
    assert!(!hdr.is_empty_by_branch(INITIAL_BRANCH_NAME));
    assert!(hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-001")));
    assert!(!hdr.is_empty_by_branch_version(INITIAL_BRANCH_NAME, VersionName(b"v-002")));
}

#[test]
fn test_version_get() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    let vn = VersionName(b"master0");
    pnk!(hdr.version_create(vn));
    let key = <usize as ValueEnDe>::encode(&1);
    let value = 1;

    pnk!(hdr.insert(key.clone(), value));
    assert_eq!(pnk!(hdr.get(&key)), value);

    let key2 = <usize as ValueEnDe>::encode(&2);
    assert_eq!(hdr.remove(&key2).unwrap(), None);

    assert_eq!(pnk!(pnk!(hdr.remove(&key))), value);
    assert!(hdr.get(&key).is_none());
    assert!(
        hdr.get_by_branch_version(&key, INITIAL_BRANCH_NAME, vn)
            .is_none()
    );
}

#[test]
fn test_version_rebase() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();

    let key = <usize as ValueEnDe>::encode(&0);
    pnk!(hdr.version_create(VersionName(&[0])));
    pnk!(hdr.insert(key.clone(), 0));
    pnk!(hdr.version_create(VersionName(&[1])));
    pnk!(hdr.insert(key.clone(), 1));
    pnk!(hdr.version_create(VersionName(&[2])));
    pnk!(hdr.insert(key.clone(), 2));
    pnk!(hdr.version_create(VersionName(&[3])));
    pnk!(hdr.insert(key.clone(), 3));

    assert!(hdr.version_exists(VersionName(&[0])));
    assert!(hdr.version_exists(VersionName(&[1])));
    assert!(hdr.version_exists(VersionName(&[2])));
    assert!(hdr.version_exists(VersionName(&[3])));

    unsafe {
        pnk!(hdr.version_rebase(VersionName(&[1])));
    }
    assert!(hdr.version_exists(VersionName(&[0])));
    assert!(hdr.version_exists(VersionName(&[1])));
    assert!(!hdr.version_exists(VersionName(&[2])));
    assert!(!hdr.version_exists(VersionName(&[3])));
}

#[test]
fn test_version_rebase_by_branch() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();

    let bn = BranchName(b"test");
    let vn = VersionName(b"test1");
    pnk!(hdr.branch_create(bn, vn, false));
    let key = <usize as ValueEnDe>::encode(&0);
    pnk!(hdr.insert_by_branch(key.clone(), 0, bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[0]), bn));
    pnk!(hdr.insert_by_branch(key.clone(), 1, bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[1]), bn));
    pnk!(hdr.insert_by_branch(key.clone(), 2, bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[2]), bn));
    pnk!(hdr.insert_by_branch(key.clone(), 3, bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[3]), bn));
    pnk!(hdr.insert_by_branch(key.clone(), 4, bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[4]), bn));
    pnk!(hdr.insert_by_branch(key.clone(), 5, bn));

    assert_eq!(0, pnk!(hdr.get_by_branch_version(&key, bn, vn)));
    assert_eq!(
        1,
        pnk!(hdr.get_by_branch_version(&key, bn, VersionName(&[0])))
    );
    assert_eq!(
        2,
        pnk!(hdr.get_by_branch_version(&key, bn, VersionName(&[1])))
    );
    assert_eq!(
        3,
        pnk!(hdr.get_by_branch_version(&key, bn, VersionName(&[2])))
    );
    assert_eq!(
        4,
        pnk!(hdr.get_by_branch_version(&key, bn, VersionName(&[3])))
    );
    assert_eq!(
        5,
        pnk!(hdr.get_by_branch_version(&key, bn, VersionName(&[4])))
    );

    unsafe {
        pnk!(hdr.version_rebase_by_branch(VersionName(&[1]), bn));
    }

    assert!(hdr.version_exists_on_branch(VersionName(&[0]), bn));
    assert!(hdr.version_exists_on_branch(VersionName(&[1]), bn));
    assert!(!hdr.version_exists_on_branch(VersionName(&[2]), bn));
    assert!(!hdr.version_exists_on_branch(VersionName(&[3]), bn));
    assert!(!hdr.version_exists_on_branch(VersionName(&[4]), bn));

    assert_eq!(
        1,
        pnk!(hdr.get_by_branch_version(&key, bn, VersionName(&[0])))
    );
    assert_eq!(
        5,
        pnk!(hdr.get_by_branch_version(&key, bn, VersionName(&[1])))
    );

    assert!(
        hdr.get_by_branch_version(&key, bn, VersionName(&[2]))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(&key, bn, VersionName(&[3]))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(&key, bn, VersionName(&[4]))
            .is_none()
    );
    assert_eq!(5, pnk!(hdr.get_by_branch(&key, bn)));
}

#[test]
fn test_prune() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();

    pnk!(hdr.prune(None));
    pnk!(hdr.prune(Some(1000000000)));

    pnk!(hdr.version_create(VersionName(b"")));
    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&0), 0));
    pnk!(hdr.version_create(VersionName(b"a")));
    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&1), 1));
    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&2), 2));
    pnk!(hdr.version_create(VersionName(b"b")));
    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&3), 3));
    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&4), 4));
    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&5), 5));
    pnk!(hdr.version_create(VersionName(b"c")));
    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&6), 6));
    pnk!(hdr.insert(<usize as ValueEnDe>::encode(&7), 7));

    assert!(hdr.version_exists(VersionName(b"a")));
    assert!(hdr.version_exists(VersionName(b"b")));
    assert!(hdr.version_exists(VersionName(b"c")));

    pnk!(hdr.prune(Some(1)));

    assert!(!hdr.version_exists(VersionName(b"a")));
    assert!(!hdr.version_exists(VersionName(b"b")));
    assert!(hdr.version_exists(VersionName(b"c")));
}

#[test]
fn test_iter() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    let mvn = VersionName(b"manster0");
    pnk!(hdr.version_create(mvn));
    let max = 500;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        let key = <usize as ValueEnDe>::encode(&key);
        assert!(pnk!(hdr.insert(key, value)).is_none());
    });
    for (key, _) in hdr.iter() {
        assert!(pnk!(hdr.remove(&key)).is_some());
    }
    assert_eq!(0, hdr.len());
}

#[test]
fn test_first_last() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    let mvn = VersionName(b"manster0");
    pnk!(hdr.version_create(mvn));
    let max = 500;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        let key = <usize as ValueEnDe>::encode(&key);
        assert!(pnk!(hdr.insert(key, value)).is_none());
    });
    let (_, value) = pnk!(hdr.iter().next());
    assert_eq!(0, value);

    let (_, value) = pnk!(hdr.iter().next_back());
    assert_eq!(max - 1, value);
}

#[test]
fn test_by_branch() {
    let hdr: MapxOrdRawKeyVs<usize> = MapxOrdRawKeyVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));
    let max = 500;
    let bn = BranchName(b"test");
    let vn = VersionName(b"test1");
    pnk!(hdr.branch_create(bn, vn, false));

    assert!(hdr.is_empty_by_branch(INITIAL_BRANCH_NAME));
    assert_eq!(0, hdr.len_by_branch(INITIAL_BRANCH_NAME));

    (0..max).map(|i| i).for_each(|i| {
        let key = <usize as ValueEnDe>::encode(&i);
        pnk!(hdr.insert_by_branch(key, i, INITIAL_BRANCH_NAME));
    });

    assert!(!hdr.is_empty_by_branch(INITIAL_BRANCH_NAME));
    assert_eq!(max, hdr.len_by_branch(INITIAL_BRANCH_NAME));
}
