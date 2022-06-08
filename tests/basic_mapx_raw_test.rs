use ruc::*;
use vsdb::{basic::mapx_raw::MapxRaw, vsdb_set_base_dir, ValueEnDe};

#[test]
fn basic_cases() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let cnt = 200;
    let hdr = {
        let hdr_i = MapxRaw::new();

        assert_eq!(0, hdr_i.len());
        (0..cnt).for_each(|i: usize| {
            assert!(hdr_i.get(&i.to_be_bytes()).is_none());
        });

        (0..cnt)
            .map(|i: usize| (i.to_be_bytes(), i.to_be_bytes()))
            .for_each(|(i, b)| {
                hdr_i.entry_ref(&i).or_insert_ref(&b);
                assert_eq!(&hdr_i.get(&i).unwrap()[..], &i[..]);
                assert_eq!(&hdr_i.remove(&i).unwrap()[..], &b[..]);
                assert!(hdr_i.get(&i).is_none());
                assert!(hdr_i.insert(&i, &b).is_none());
                assert!(hdr_i.insert(&i, &b).is_some());
            });

        assert_eq!(cnt, hdr_i.len());

        <MapxRaw as ValueEnDe>::encode(&hdr_i)
    };

    let reloaded = pnk!(<MapxRaw as ValueEnDe>::decode(&hdr));

    assert_eq!(cnt, reloaded.len());

    (0..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        assert_eq!(&i[..], &reloaded.get(&i).unwrap()[..]);
    });

    (1..cnt).map(|i: usize| i.to_be_bytes()).for_each(|i| {
        *reloaded.get_mut(&i).unwrap() = i.to_vec().into_boxed_slice();
        assert_eq!(&reloaded.get(&i).unwrap()[..], &i[..]);
        assert!(reloaded.contains_key(&i));
        assert!(reloaded.remove(&i).is_some());
        assert!(!reloaded.contains_key(&i));
    });

    assert_eq!(1, reloaded.len());
    reloaded.clear();
    assert!(reloaded.is_empty());

    reloaded.insert(&[1], &[1]);
    reloaded.insert(&[4], &[4]);
    reloaded.insert(&[6], &[6]);
    reloaded.insert(&[80], &[80]);

    assert!(reloaded.range(&[][..]..&[1][..]).next().is_none());
    assert_eq!(
        vec![4].into_boxed_slice(),
        reloaded.range(&[2][..]..&[10][..]).next().unwrap().1
    );

    assert_eq!(
        vec![80].into_boxed_slice(),
        reloaded.get_ge(&[79]).unwrap().1
    );
    assert_eq!(
        vec![80].into_boxed_slice(),
        reloaded.get_ge(&[80]).unwrap().1
    );
    assert_eq!(
        vec![80].into_boxed_slice(),
        reloaded.get_le(&[80]).unwrap().1
    );
    assert_eq!(
        vec![80].into_boxed_slice(),
        reloaded.get_le(&[100]).unwrap().1
    );
}
