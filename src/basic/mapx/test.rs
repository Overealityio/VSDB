use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, (max + i)))
        .for_each(|(key, value)| {
            assert!(hdr.get(&key).is_none());
            hdr.set_value(key, value);
            assert!(hdr.insert(key, value).is_some());
            assert!(hdr.contains_key(&key));
            assert_eq!(pnk!(hdr.get(&key)), value);
            assert_eq!(pnk!(hdr.remove(&key)), value);
            assert!(hdr.get(&key).is_none());
        });
    hdr.clear();
    (0..max).map(|i: usize| i).for_each(|key| {
        assert!(hdr.get(&key).is_none());
    });
    assert!(hdr.is_empty());
}

#[test]
fn test_len() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, (max + i)))
        .for_each(|(key, value)| {
            assert!(hdr.insert(key, value).is_none());
        });
    assert_eq!(500, hdr.len());

    for key in 0..max {
        assert!(hdr.remove(&key).is_some());
    }
    assert_eq!(0, hdr.len());
}

#[test]
fn test_valueende() {
    let cnt = 500;
    let dehdr = {
        let mut hdr: Mapx<usize, usize> = Mapx::new();
        (0..cnt).map(|i: usize| (i, i)).for_each(|(key, value)| {
            assert!(hdr.insert(key, value).is_none());
        });
        crate::vsdb_flush();
        <Mapx<usize, usize> as ValueEnDe>::encode(&hdr)
    };
    let mut reloaded = pnk!(<Mapx<usize, usize> as ValueEnDe>::decode(&dehdr));
    assert_eq!(cnt, reloaded.len());
    (0..cnt).map(|i: usize| i).for_each(|i| {
        assert_eq!(i, reloaded.get(&i).unwrap());
    });
}

#[test]
fn test_iter() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 500;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        assert!(hdr.insert(key, value).is_none());
    });
    for (key, value) in hdr.iter() {
        assert_eq!(key, value);
        hdr.unset_value(&key);
    }
    assert_eq!(0, hdr.len());
}

#[test]
fn test_first_last() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 500;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        assert!(hdr.insert(key, value).is_none());
    });
    let (key, value) = pnk!(hdr.iter().next());
    assert_eq!(key, value);
    assert_eq!(0, key);

    let (key, value) = pnk!(hdr.iter().next_back());
    assert_eq!(key, value);
    assert_eq!(max - 1, key);
}

#[test]
fn test_values() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 500;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        assert!(hdr.insert(key, value).is_none());
    });
    let mut i = 0;
    for it in hdr.values() {
        assert_eq!(i, it);
        i = i + 1;
    }
}

#[test]
fn test_values_first_last() {
    let mut hdr: Mapx<usize, usize> = Mapx::new();
    let max = 500;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        assert!(hdr.insert(key, value).is_none());
    });
    let value = pnk!(hdr.values().next());
    assert_eq!(0, value);

    let value = pnk!(hdr.values().next_back());
    assert_eq!(max - 1, value);
}
