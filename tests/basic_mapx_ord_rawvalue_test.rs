use ruc::*;
use std::ops::Bound;
use vsdb::{basic::mapx_ord_rawvalue::MapxOrdRawValue, vsdb_set_base_dir, ValueEnDe};

#[test]
fn basic_cases() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let cnt = 200;
    let hdr = {
        let hdr_i = MapxOrdRawValue::new();

        assert_eq!(0, hdr_i.len());
        (0usize..cnt).for_each(|i| {
            assert!(hdr_i.get(&i).is_none());
        });

        (0usize..cnt)
            .map(|i| (i, i.to_be_bytes()))
            .for_each(|(i, b)| {
                hdr_i.entry_ref(&i).or_insert_ref(&b);
                assert_eq!(1 + i as usize, hdr_i.len());
                assert_eq!(&hdr_i.get(&i).unwrap()[..], &b[..]);
                assert_eq!(&hdr_i.remove(&i).unwrap()[..], &b);
                assert_eq!(i as usize, hdr_i.len());
                assert!(hdr_i.get(&i).is_none());
                assert!(hdr_i.insert_ref(&i, &b).is_none());
                assert!(hdr_i.insert(i, b.to_vec().into_boxed_slice()).is_some());
            });

        assert_eq!(cnt, hdr_i.len());

        <MapxOrdRawValue<usize> as ValueEnDe>::encode(&hdr_i)
    };

    let reloaded = pnk!(<MapxOrdRawValue<usize> as ValueEnDe>::decode(&hdr));

    assert_eq!(cnt, reloaded.len());

    (0usize..cnt).for_each(|i| {
        assert_eq!(&i.to_be_bytes(), &reloaded.get(&i).unwrap()[..]);
    });

    (1usize..cnt).for_each(|i| {
        *reloaded.get_mut(&i).unwrap() =
            (1 + i).to_be_bytes().to_vec().into_boxed_slice();
        assert_eq!(&reloaded.get(&i).unwrap()[..], &(1 + i).to_be_bytes());
        assert!(reloaded.contains_key(&i));
        assert!(reloaded.remove(&i).is_some());
        assert!(!reloaded.contains_key(&i));
    });

    assert_eq!(1, reloaded.len());
    reloaded.clear();
    assert!(reloaded.is_empty());

    reloaded.insert_ref(&1, &1usize.to_be_bytes());
    reloaded.insert_ref(&10, &10usize.to_be_bytes());
    reloaded.insert_ref(&100, &100usize.to_be_bytes());
    reloaded.insert_ref(&1000, &1000usize.to_be_bytes());

    assert!(reloaded.range(0..1).next().is_none());

    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.range(12..999).next().unwrap().1[..]
    );
    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.range(12..=999).next().unwrap().1[..]
    );

    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.range(100..=999).next().unwrap().1[..]
    );
    assert!(
        reloaded
            .range((Bound::Excluded(100), Bound::Included(999)))
            .next()
            .is_none()
    );

    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.get_ge(&99).unwrap().1[..]
    );
    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.get_ge(&100).unwrap().1[..]
    );
    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.get_le(&100).unwrap().1[..]
    );
    assert_eq!(
        &100usize.to_be_bytes()[..],
        &reloaded.get_le(&101).unwrap().1[..]
    );
}
