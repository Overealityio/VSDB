use super::*;
use crate::{
    common::{BranchName, ParentBranchName, INITIAL_BRANCH_NAME},
    ValueEnDe, VsMgmt,
};

#[test]
fn test_master_branch_exists() {
    let mut hdr = MapxRawMkVs::new(2);
    assert!(hdr.branch_exists(INITIAL_BRANCH_NAME));
}

#[test]
fn test_master_branch_has_versions() {
    let mut hdr = MapxRawMkVs::new(2);
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
}

#[test]
fn test_branch_create_no_version() {
    let mut hdr = MapxRawMkVs::new(2);
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
    let mut hdr = MapxRawMkVs::new(2);
    pnk!(hdr.version_create(VersionName(b"manster0")));
    let bn1 = BranchName(b"test1");
    let vn11 = VersionName(b"testversion11");
    pnk!(hdr.branch_create(bn1, vn11, false));

    let value = b"testvalue";
    pnk!(hdr.insert(&[&[1u8], &[1u8]], value));

    let bn2 = BranchName(b"test2");
    let vn21 = VersionName(b"testversion21");

    pnk!(hdr.branch_create_by_base_branch(bn2, vn21, ParentBranchName(b"test1"), false));
    let value = b"testvalue1";
    pnk!(hdr.insert(&[&[2u8], &[2u8]], value));
}

#[test]
fn test_branch_remove() {
    let mut hdr = MapxRawMkVs::new(2);
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
    let mut hdr = MapxRawMkVs::new(2);
    let mvn = VersionName(b"manster0");
    pnk!(hdr.version_create(mvn));
    pnk!(hdr.insert(&[&[2], &[2]], b"manstervalue"));

    let bn = BranchName(b"test");
    let vn = VersionName(b"test0");

    pnk!(hdr.branch_create(bn, vn, false));
    let key: &[&[u8]] = &[&[2], &[2]];
    let value = b"testvalue";
    pnk!(hdr.insert(key, value));
    pnk!(hdr.branch_merge_to(bn, INITIAL_BRANCH_NAME));
    pnk!(hdr.branch_set_default(INITIAL_BRANCH_NAME));
    let val = pnk!(hdr.get_by_branch(key, INITIAL_BRANCH_NAME));
    assert_eq!(val.as_ref(), value);
}

#[test]
fn test_branch_pop_version() {
    let mut hdr = MapxRawMkVs::new(2);
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
    pnk!(hdr.version_create(VersionName(b"manster0")));
    assert!(hdr.branch_has_versions(INITIAL_BRANCH_NAME));
    pnk!(hdr.branch_pop_version(INITIAL_BRANCH_NAME));
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
}

#[test]
fn test_branch_swap() {
    let mut hdr = MapxRawMkVs::new(2);
    pnk!(hdr.version_create(VersionName(b"manster0")));

    let mkey: &[&[u8]] = &[&[1], &[1]];
    let mvalue = b"manstervalue";
    pnk!(hdr.insert(mkey, mvalue));

    let bn = BranchName(b"test");
    let vn = VersionName(b"test0");
    pnk!(hdr.branch_create(bn, vn, false));

    let tkey: &[&[u8]] = &[&[2], &[2]];
    let tvalue = b"testvalue";
    pnk!(hdr.insert(tkey, tvalue));

    unsafe {
        pnk!(hdr.branch_swap(INITIAL_BRANCH_NAME, bn));
    }
    let val = pnk!(hdr.get_by_branch(tkey, INITIAL_BRANCH_NAME));
    assert_eq!(val.as_ref(), tvalue);
    let val = pnk!(hdr.get_by_branch(mkey, bn));
    assert_eq!(val.as_ref(), mvalue);
}

#[test]
fn test_branch_truncate() {
    let mut hdr = MapxRawMkVs::new(2);
    pnk!(hdr.version_create(VersionName(b"manster0")));

    let mkey0: &[&[u8]] = &[&[1], &[1]];
    let mvalue0 = b"manstervalue0";
    pnk!(hdr.insert(mkey0, mvalue0));

    let mkey1: &[&[u8]] = &[&[2], &[2]];
    let mvalue1 = b"manstervalue1";
    pnk!(hdr.insert(mkey1, mvalue1));

    pnk!(hdr.branch_truncate(INITIAL_BRANCH_NAME));
    assert!(hdr.get(mkey0).is_none());
    assert!(hdr.get(mkey1).is_none());
}

#[test]
fn test_branch_truncate_to() {
    let mut hdr = MapxRawMkVs::new(2);
    let vn = VersionName(b"manster0");
    pnk!(hdr.version_create(vn));

    let mkey0: &[&[u8]] = &[&[1], &[1]];
    let mvalue0 = b"manstervalue0";
    pnk!(hdr.insert(mkey0, mvalue0));

    pnk!(hdr.version_create(VersionName(b"manster1")));

    let mkey1: &[&[u8]] = &[&[2], &[2]];
    let mvalue1 = b"manstervalue1";
    pnk!(hdr.insert(mkey1, mvalue1));

    pnk!(hdr.branch_truncate_to(INITIAL_BRANCH_NAME, vn));

    assert!(hdr.get(mkey1).is_none());
}
#[test]
fn test_insert() {
    let mut hdr = MapxRawMkVs::new(2);
    pnk!(hdr.version_create(VersionName(b"manster0")));
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), (max + i).to_be_bytes()))
        .for_each(|(key, value)| {
            let key: &[&[u8]] = &[&key, &key];
            assert!(hdr.get(key).is_none());
            assert!(pnk!(hdr.insert(key, &value)).is_none());
            assert!(pnk!(hdr.insert(key, &value)).is_some());
            assert!(hdr.contains_key(key));
            assert_eq!(&pnk!(hdr.get(key))[..], &value);
            assert_eq!(&pnk!(hdr.remove(key)).unwrap()[..], &value);
            assert!(hdr.get(key).is_none());
        });
}

#[test]
fn test_valueende() {
    let cnt = 500;
    let dehdr = {
        let mut hdr = MapxRawMkVs::new(2);
        pnk!(hdr.version_create(VersionName(b"manster0")));
        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), i.to_be_bytes()))
            .for_each(|(key, value)| {
                let key: &[&[u8]] = &[&key, &key];
                assert!(pnk!(hdr.insert(key, &value)).is_none());
            });
        crate::vsdb_flush();
        <MapxRawMkVs as ValueEnDe>::encode(&hdr)
    };
    let mut reloaded = pnk!(<MapxRawMkVs as ValueEnDe>::decode(&dehdr));
    (0..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        let key: &[&[u8]] = &[&i, &i];
        assert_eq!(i.to_vec().into_boxed_slice(), reloaded.get(key).unwrap());
    });
}

#[test]
fn test_emptystr_version() {
    let mut hdr = MapxRawMkVs::new(2);
    pnk!(hdr.version_create(VersionName(b"")));

    let key: &[&[u8]] = &[&[2], &[2]];
    let value = b"value";

    assert!(hdr.get(key).is_none());
    assert!(pnk!(hdr.insert(key, value)).is_none());
    assert!(hdr.contains_key(key));
    assert_eq!(&pnk!(hdr.get(key))[..], value);
    assert_eq!(&pnk!(hdr.remove(key)).unwrap()[..], value);
    assert!(hdr.get(key).is_none());
}

#[test]
fn test_version_get() {
    let mut hdr = MapxRawMkVs::new(2);
    let vn = VersionName(b"master0");
    pnk!(hdr.version_create(vn));
    let key1: &[&[u8]] = &[&[1], &[1]];
    let value = b"mastervalue";

    pnk!(hdr.insert(key1, value));
    assert_eq!(&hdr.get(key1).unwrap()[..], value);

    let key2: &[&[u8]] = &[&[2], &[2]];
    assert_eq!(hdr.remove(key2).unwrap(), None);

    assert_eq!(&hdr.remove(key1).unwrap().unwrap()[..], value);
    assert!(hdr.get(key1).is_none());
    assert!(
        hdr.get_by_branch_version(key1, INITIAL_BRANCH_NAME, vn)
            .is_none()
    );
}

#[test]
fn test_version_rebase() {
    let mut hdr = MapxRawMkVs::new(2);

    pnk!(hdr.version_create(VersionName(&[0])));
    pnk!(hdr.insert(&[&[0], &[0]], &[0]));
    pnk!(hdr.version_create(VersionName(&[1])));
    pnk!(hdr.insert(&[&[0], &[0]], &[1]));
    pnk!(hdr.version_create(VersionName(&[2])));
    pnk!(hdr.insert(&[&[0], &[0]], &[2]));
    pnk!(hdr.version_create(VersionName(&[3])));
    pnk!(hdr.insert(&[&[0], &[0]], &[3]));

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
    let mut hdr = MapxRawMkVs::new(2);

    let bn = BranchName(b"test");
    let vn = VersionName(b"test1");
    pnk!(hdr.branch_create(bn, vn, false));
    pnk!(hdr.insert_by_branch(&[&[0], &[0]], &[0], bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[0]), bn));
    pnk!(hdr.insert_by_branch(&[&[0], &[0]], &[1], bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[1]), bn));
    pnk!(hdr.insert_by_branch(&[&[0], &[0]], &[2], bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[2]), bn));
    pnk!(hdr.insert_by_branch(&[&[0], &[0]], &[3], bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[3]), bn));
    pnk!(hdr.insert_by_branch(&[&[0], &[0]], &[4], bn));
    pnk!(hdr.version_create_by_branch(VersionName(&[4]), bn));
    pnk!(hdr.insert_by_branch(&[&[0], &[0]], &[5], bn));

    assert_eq!(
        &[0],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], bn, vn))[..]
    );
    assert_eq!(
        &[1],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], bn, VersionName(&[0])))[..]
    );
    assert_eq!(
        &[2],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], bn, VersionName(&[1])))[..]
    );
    assert_eq!(
        &[3],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], bn, VersionName(&[2])))[..]
    );
    assert_eq!(
        &[4],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], bn, VersionName(&[3])))[..]
    );
    assert_eq!(
        &[5],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], bn, VersionName(&[4])))[..]
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
        &[1],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], bn, VersionName(&[0])))[..]
    );
    assert_eq!(
        &[5],
        &pnk!(hdr.get_by_branch_version(&[&[0], &[0]], bn, VersionName(&[1])))[..]
    );

    assert!(
        hdr.get_by_branch_version(&[&[0], &[0]], bn, VersionName(&[2]))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(&[&[0], &[0]], bn, VersionName(&[3]))
            .is_none()
    );
    assert!(
        hdr.get_by_branch_version(&[&[0], &[0]], bn, VersionName(&[4]))
            .is_none()
    );
    assert_eq!(&[5], &pnk!(hdr.get_by_branch(&[&[0], &[0]], bn))[..]);
}

#[test]
fn test_prune() {
    let mut hdr = MapxRawMkVs::new(2);

    pnk!(hdr.prune(None));
    pnk!(hdr.prune(Some(1000000000)));

    pnk!(hdr.version_create(VersionName(b"")));
    pnk!(hdr.insert(&[&[0], &[0]], &[0]));
    pnk!(hdr.version_create(VersionName(b"a")));
    pnk!(hdr.insert(&[&[1], &[1]], &[1]));
    pnk!(hdr.insert(&[&[2], &[2]], &[2]));
    pnk!(hdr.version_create(VersionName(b"b")));
    pnk!(hdr.insert(&[&[3], &[3]], &[3]));
    pnk!(hdr.insert(&[&[4], &[4]], &[4]));
    pnk!(hdr.insert(&[&[5], &[5]], &[5]));
    pnk!(hdr.version_create(VersionName(b"c")));
    pnk!(hdr.insert(&[&[6], &[6]], &[6]));
    pnk!(hdr.insert(&[&[7], &[7]], &[7]));

    assert!(hdr.version_exists(VersionName(b"a")));
    assert!(hdr.version_exists(VersionName(b"b")));
    assert!(hdr.version_exists(VersionName(b"c")));

    pnk!(hdr.prune(Some(1)));

    assert!(!hdr.version_exists(VersionName(b"a")));
    assert!(!hdr.version_exists(VersionName(b"b")));
    assert!(hdr.version_exists(VersionName(b"c")));
}
