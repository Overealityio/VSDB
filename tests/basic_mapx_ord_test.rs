use ruc::*;
use serde::{Deserialize, Serialize};
use std::ops::Bound;
use vsdb::{vsdb_set_base_dir, MapxOrd, ValueEnDe};

#[derive(Serialize, Deserialize, Default, Debug, Eq, PartialEq, Clone)]
struct SampleBlock {
    idx: usize,
    data: Vec<usize>,
}

fn gen_sample(idx: usize) -> SampleBlock {
    SampleBlock {
        idx,
        data: vec![idx],
    }
}

#[test]
fn basic_cases() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let cnt = 200;
    let hdr = {
        let hdr_i = MapxOrd::new();

        assert_eq!(0, hdr_i.len());
        (0..cnt).for_each(|i| {
            assert!(hdr_i.get(&i).is_none());
        });

        (0..cnt).map(|i| (i, gen_sample(i))).for_each(|(i, b)| {
            hdr_i.entry(i).or_insert(b.clone());
            assert_eq!(1 + i as usize, hdr_i.len());
            assert_eq!(pnk!(hdr_i.get(&i)).idx, i);
            assert_eq!(hdr_i.remove(&i), Some(b.clone()));
            assert_eq!(i as usize, hdr_i.len());
            assert!(hdr_i.get(&i).is_none());
            assert!(hdr_i.insert_ref(&i, &b).is_none());
            assert!(hdr_i.insert(i, b).is_some());
        });

        assert_eq!(cnt, hdr_i.len());

        <MapxOrd<usize, SampleBlock> as ValueEnDe>::encode(&hdr_i)
    };

    let reloaded = pnk!(<MapxOrd<usize, SampleBlock> as ValueEnDe>::decode(&hdr));

    assert_eq!(cnt, reloaded.len());

    (0..cnt).for_each(|i| {
        assert_eq!(i, reloaded.get(&i).unwrap().idx);
    });

    (1..cnt).for_each(|i| {
        pnk!(reloaded.get_mut(&i)).idx = 1 + i;
        assert_eq!(pnk!(reloaded.get(&i)).idx, 1 + i);
        assert!(reloaded.contains_key(&i));
        assert!(reloaded.remove(&i).is_some());
        assert!(!reloaded.contains_key(&i));
    });

    assert_eq!(1, reloaded.len());
    reloaded.clear();
    assert!(reloaded.is_empty());

    reloaded.insert_ref(&1, &gen_sample(1));
    reloaded.insert_ref(&10, &gen_sample(10));
    reloaded.insert_ref(&100, &gen_sample(100));
    reloaded.insert_ref(&1000, &gen_sample(1000));

    assert!(reloaded.range(0..1).next().is_none());

    assert_eq!(100, reloaded.range(12..999).next().unwrap().1.idx);
    assert_eq!(100, reloaded.range(12..=999).next().unwrap().1.idx);

    assert_eq!(100, reloaded.range(100..=999).next().unwrap().1.idx);
    assert!(
        reloaded
            .range((Bound::Excluded(100), Bound::Included(999)))
            .next()
            .is_none()
    );

    assert_eq!(100, reloaded.get_ge(&99).unwrap().1.idx);
    assert_eq!(100, reloaded.get_ge(&100).unwrap().1.idx);
    assert_eq!(100, reloaded.get_le(&100).unwrap().1.idx);
    assert_eq!(100, reloaded.get_le(&101).unwrap().1.idx);
}
