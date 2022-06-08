use super::*;
use crate::common::{ParentBranchName, INITIAL_BRANCH_NAME};

#[test]
fn test_master_branch_exists() {
    let mut hdr: VecxVs<String> = VecxVs::new();
    assert!(hdr.branch_exists(INITIAL_BRANCH_NAME));
}

#[test]
fn test_master_branch_has_versions() {
    let mut hdr: VecxVs<String> = VecxVs::new();
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
}

#[test]
fn test_branch_create_no_version() {
    let mut hdr: VecxVs<String> = VecxVs::new();
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
    let mut hdr: VecxVs<String> = VecxVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));
    let bn1 = BranchName(b"test1");
    let vn11 = VersionName(b"testversion11");
    pnk!(hdr.branch_create(bn1, vn11, false));

    let value1 = String::from("testvalue1");
    hdr.push(value1);
    let bn2 = BranchName(b"test2");
    let vn21 = VersionName(b"testversion21");
    pnk!(hdr.branch_create_by_base_branch(bn2, vn21, ParentBranchName(b"test1"), false));
    let value2 = String::from("testvalue2");
    hdr.push(value2);
}

#[test]
fn test_branch_remove() {
    let mut hdr: VecxVs<String> = VecxVs::new();
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
    let mut hdr: VecxVs<String> = VecxVs::new();
    let mvn = VersionName(b"manster0");
    pnk!(hdr.version_create(mvn));
    let value1 = String::from("testvalue1");
    hdr.push(value1);

    let bn = BranchName(b"test");
    let vn = VersionName(b"test0");

    pnk!(hdr.branch_create(bn, vn, false));
    let value2 = String::from("testvalue2");
    hdr.push(value2.clone());
    pnk!(hdr.branch_merge_to(bn, INITIAL_BRANCH_NAME));
    let val = pnk!(hdr.get_by_branch(1, INITIAL_BRANCH_NAME));
    assert_eq!(val, value2);
}

#[test]
fn test_branch_pop_version() {
    let mut hdr: VecxVs<String> = VecxVs::new();
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
    pnk!(hdr.version_create(VersionName(b"manster0")));
    assert!(hdr.branch_has_versions(INITIAL_BRANCH_NAME));
    pnk!(hdr.branch_pop_version(INITIAL_BRANCH_NAME));
    assert_eq!(false, hdr.branch_has_versions(INITIAL_BRANCH_NAME));
}

#[test]
fn test_branch_swap() {
    let mut hdr: VecxVs<String> = VecxVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));

    let mval = String::from("value1");
    hdr.push(mval.clone());

    let bn = BranchName(b"test");
    let vn = VersionName(b"test0");
    pnk!(hdr.branch_create(bn, vn, false));
    let tval = String::from("value2");
    hdr.push(tval.clone());

    unsafe {
        pnk!(hdr.branch_swap(INITIAL_BRANCH_NAME, bn));
    }
    let val = pnk!(hdr.get_by_branch(1, INITIAL_BRANCH_NAME));
    assert_eq!(val, tval);
    let val = pnk!(hdr.get_by_branch(0, bn));
    assert_eq!(val, mval);
}

#[test]
fn test_branch_truncate() {
    let mut hdr: VecxVs<String> = VecxVs::new();
    pnk!(hdr.version_create(VersionName(b"manster0")));

    let tval = String::from("value2");
    hdr.push(tval.clone());

    pnk!(hdr.branch_truncate(INITIAL_BRANCH_NAME));
    assert!(hdr.get(0).is_none());
}

#[test]
fn test_branch_truncate_to() {
    let mut hdr: VecxVs<String> = VecxVs::new();
    let vn = VersionName(b"manster0");
    pnk!(hdr.version_create(vn));

    let mval = String::from("value1");
    hdr.push(mval.clone());

    pnk!(hdr.version_create(VersionName(b"manster1")));

    let tval = String::from("value2");
    hdr.push(tval.clone());

    pnk!(hdr.branch_truncate_to(INITIAL_BRANCH_NAME, vn));

    assert_eq!(pnk!(hdr.get(0)), mval);
}
