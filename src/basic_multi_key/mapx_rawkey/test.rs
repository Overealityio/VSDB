use crate::ValueEnDe;

use super::*;
use ruc::*;

#[test]
fn test_insert() {
    let hdr = MapxRawKeyMk::new(2);
    assert_eq!(2, hdr.key_size());
    let max = 500;
    (0..max)
        .map(|i: usize| (i.to_be_bytes(), <usize as ValueEnDe>::encode(&(max + i))))
        .for_each(|(subkey, value)| {
            let key: &[&[u8]] = &[&subkey, &subkey];
            assert!(hdr.get(&key).is_none());
            pnk!(hdr.entry_ref(&key).or_insert_ref(&value));
            assert!(pnk!(hdr.insert(&key, &value)).is_some());
            assert!(hdr.contains_key(&key));
            assert_eq!(pnk!(hdr.get(&key)), value);
            assert_eq!(pnk!(pnk!(hdr.remove(&key))), value);
            assert!(hdr.get(&key).is_none());
            assert!(pnk!(hdr.insert(&key, &value)).is_none());
        });
    hdr.clear();
    (0..max).map(|i: usize| i.to_be_bytes()).for_each(|subkey| {
        assert!(hdr.get(&[&subkey, &subkey]).is_none());
    });
    assert!(hdr.is_empty());
}

#[test]
fn test_valueende() {
    let cnt = 500;
    let dehdr = {
        let hdr: MapxRawKeyMk<Box<[u8]>> = MapxRawKeyMk::new(2);
        let max = 500;
        (0..max)
            .map(|i: usize| (i.to_be_bytes(), <usize as ValueEnDe>::encode(&i)))
            .for_each(|(subkey, value)| {
                let key: &[&[u8]] = &[&subkey, &subkey];
                assert!(pnk!(hdr.insert(&key, &value)).is_none());
            });
        crate::vsdb_flush();
        <MapxRawKeyMk<Box<[u8]>> as ValueEnDe>::encode(&hdr)
    };
    let reloaded = pnk!(<MapxRawKeyMk<Box<[u8]>> as ValueEnDe>::decode(&dehdr));

    (0..cnt)
        .map(|i: usize| (i, i.to_be_bytes()))
        .for_each(|(i, subkey)| {
            let val = pnk!(<usize as ValueEnDe>::decode(&pnk!(
                reloaded.get(&[&subkey, &subkey])
            )));
            assert_eq!(i, val);
        });
}

#[test]
fn test_iter_op() {
    let hdr = MapxRawKeyMk::new(4);
    assert!(hdr.entry_ref(&[]).or_insert_ref(&777).is_err());
    assert!(
        hdr.entry_ref(&[&[11], &[12], &[13], &[14]])
            .or_insert_ref(&777)
            .is_ok()
    );
    assert_eq!(hdr.get(&[&[11], &[12], &[13], &[14]]).unwrap(), 777);

    let mut cnt = 0;
    let mut op = |k: &[&[u8]], v: &u32| {
        cnt += 1;
        assert_eq!(k, &[&[11], &[12], &[13], &[14]]);
        assert_eq!(v, &777);
        Ok(())
    };

    pnk!(hdr.iter_op(&mut op));
    assert_eq!(cnt, 1);
}

#[test]
fn test_iter_op_with_key_prefix() {
    let hdr = MapxRawKeyMk::new(4);
    assert!(hdr.entry_ref(&[]).or_insert_ref(&777).is_err());
    assert!(
        hdr.entry_ref(&[&[11], &[12], &[13], &[14]])
            .or_insert_ref(&777)
            .is_ok()
    );
    assert_eq!(hdr.get(&[&[11], &[12], &[13], &[14]]).unwrap(), 777);

    assert!(
        hdr.entry_ref(&[&[11], &[12], &[13], &[15]])
            .or_insert_ref(&888)
            .is_ok()
    );
    assert_eq!(hdr.get(&[&[11], &[12], &[13], &[15]]).unwrap(), 888);

    let mut cnt = 0;
    let mut op = |k: &[&[u8]], v: &u32| {
        cnt += 1;
        if v == &777 {
            assert_eq!(k, &[&[11], &[12], &[13], &[14]]);
        } else {
            assert_eq!(k, &[&[11], &[12], &[13], &[15]]);
        }
        Ok(())
    };

    // cnt += 2
    pnk!(hdr.iter_op(&mut op));
    // cnt += 2
    pnk!(hdr.iter_op_with_key_prefix(&mut op, &[&[11]]));
    // cnt += 2
    pnk!(hdr.iter_op_with_key_prefix(&mut op, &[&[11], &[12]]));
    // cnt += 2
    pnk!(hdr.iter_op_with_key_prefix(&mut op, &[&[11], &[12], &[13]]));
    // cnt += 1
    pnk!(hdr.iter_op_with_key_prefix(&mut op, &[&[11], &[12], &[13], &[14]]));
    // cnt += 1
    pnk!(hdr.iter_op_with_key_prefix(&mut op, &[&[11], &[12], &[13], &[15]]));

    // cnt += 0
    pnk!(hdr.iter_op_with_key_prefix(&mut op, &[&[111]]));
    // cnt += 0
    pnk!(hdr.iter_op_with_key_prefix(&mut op, &[&[111], &[12]]));
    // cnt += 0
    pnk!(hdr.iter_op_with_key_prefix(&mut op, &[&[111], &[12], &[13]]));
    // cnt += 0
    pnk!(hdr.iter_op_with_key_prefix(&mut op, &[&[111], &[12], &[13], &[14]]));
    // cnt += 0
    pnk!(hdr.iter_op_with_key_prefix(&mut op, &[&[111], &[12], &[13], &[15]]));

    drop(op);
    assert_eq!(cnt, 10);
}
