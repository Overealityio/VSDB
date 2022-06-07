use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let hdr = Vecx::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, (max + i)))
        .for_each(|(key, value)| {
            assert!(hdr.get(key).is_none());
            hdr.insert(key, value);
            assert_eq!(pnk!(hdr.get(key)), value);
        });
    hdr.clear();
    (0..max).map(|i: usize| i).for_each(|key| {
        assert!(hdr.get(key).is_none());
    });
    assert!(hdr.is_empty());
}
#[test]
fn test_len() {
    let hdr = Vecx::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, (max + i)))
        .for_each(|(key, value)| {
            hdr.insert(key, value);
        });
    assert_eq!(500, hdr.len());
    hdr.clear();
    assert_eq!(0, hdr.len());
}

#[test]
fn test_valueende() {
    let cnt = 500;
    let dehdr = {
        let hdr = Vecx::new();
        (0..cnt).map(|i: usize| (i, i)).for_each(|(key, value)| {
            hdr.insert(key, value);
        });
        crate::vsdb_flush();
        <Vecx<usize> as ValueEnDe>::encode(&hdr)
    };
    let reloaded = pnk!(<Vecx<usize> as ValueEnDe>::decode(&dehdr));
    assert_eq!(cnt, reloaded.len());
    (0..cnt).map(|i: usize| i).for_each(|i| {
        assert_eq!(i, reloaded.get(i).unwrap());
    });
}

#[test]
fn test_remove() {
    let hdr = Vecx::new();
    let max = 500;
    (0..max)
        .map(|i: usize| (i, (max + i)))
        .for_each(|(key, value)| {
            hdr.insert(key, value);
        });
    assert_eq!(max, hdr.len());

    let idx = 400;
    assert_eq!(max + idx, hdr.remove(idx));
    hdr.clear();
    assert_eq!(0, hdr.len());
}

#[test]
fn test_iter_next() {
    let hdr = Vecx::new();
    let max = 500;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        hdr.insert(key, value);
    });
    let value = pnk!(hdr.iter().next());
    assert_eq!(0, value);

    let value = pnk!(hdr.iter().next_back());
    assert_eq!(max - 1, value);
}

#[test]
fn test_push_pop() {
    let hdr = Vecx::new();
    let max = 500;
    (0..max).map(|i: usize| i).for_each(|value| {
        hdr.push(value);
    });
    for val in (0..max).rev() {
        assert_eq!(val, pnk!(hdr.pop()));
    }
}

#[test]
fn test_swap_remove() {
    let hdr = Vecx::new();
    let max = 500;
    (0..max).map(|i: usize| i).for_each(|value| {
        hdr.push(value);
    });
    for idx in (0..max - 1).rev() {
        assert_eq!(idx, hdr.swap_remove(idx));
    }
    assert_eq!(1, hdr.len());
    let value = pnk!(hdr.last());
    assert_eq!(max - 1, value);
}

#[test]
fn test_last() {
    let hdr = Vecx::new();
    let max = 500;
    (0..max).map(|i: usize| (i, i)).for_each(|(key, value)| {
        hdr.insert(key, value);
    });
    let value = pnk!(hdr.last());
    assert_eq!(max - 1, value);
}
