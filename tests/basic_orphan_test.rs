use ruc::*;
use serde::{Deserialize, Serialize};
use vsdb::{basic::orphan::Orphan, vsdb_set_base_dir};

#[test]
fn basic_cases() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    assert_eq!(Orphan::new(0), 0);
    assert!(Orphan::new(111) > 0);
    assert!(Orphan::new(111) >= 0);
    assert!(Orphan::new(0) < 111);
    assert!(Orphan::new(0) <= 111);

    assert_eq!(Orphan::new(0), Orphan::new(0));
    assert!(Orphan::new(111) > Orphan::new(0));
    assert!(Orphan::new(111) >= Orphan::new(111));
    assert!(Orphan::new(0) < Orphan::new(111));
    assert!(Orphan::new(111) <= Orphan::new(111));

    assert_eq!(Orphan::new(111) + 111, 222);
    assert_eq!(Orphan::new(111) - 111, 0);
    assert_eq!(Orphan::new(111) * 111, 111 * 111);
    assert_eq!(Orphan::new(111) / 2, 55);
    assert_eq!(Orphan::new(111) % 2, 1);

    assert_eq!(-Orphan::new(111), -111);
    assert_eq!(!Orphan::new(111), !111);

    assert_eq!(Orphan::new(111) >> 2, 111 >> 2);
    assert_eq!(Orphan::new(111) << 2, 111 << 2);

    assert_eq!(Orphan::new(111) | 2, 111 | 2);
    assert_eq!(Orphan::new(111) & 2, 111 & 2);
    assert_eq!(Orphan::new(111) ^ 2, 111 ^ 2);

    let mut v = Orphan::new(1);
    v += 1;
    assert_eq!(v, 2);
    v *= 100;
    assert_eq!(v, 200);
    v -= 1;
    assert_eq!(v, 199);
    v /= 10;
    assert_eq!(v, 19);
    v %= 10;
    assert_eq!(v, 9);

    *v.get_mut() = -v.get_value();
    assert_eq!(v, -9);

    *v.get_mut() = !v.get_value();
    assert_eq!(v, !-9);

    *v.get_mut() = 732;
    v >>= 2;
    assert_eq!(v, 732 >> 2);

    *v.get_mut() = 732;
    v <<= 2;
    assert_eq!(v, 732 << 2);

    *v.get_mut() = 732;
    v |= 2;
    assert_eq!(v, 732 | 2);

    *v.get_mut() = 732;
    v &= 2;
    assert_eq!(v, 732 & 2);

    *v.get_mut() = 732;
    v ^= 2;
    assert_eq!(v, 732 ^ 2);
}

#[test]
fn custom_types() {
    info_omit!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    #[derive(
        Default, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize,
    )]
    struct Foo {
        a: i32,
        b: String,
        c: bool,
    }
    assert_eq!(Orphan::new(Foo::default()), Foo::default());
    assert_eq!(Orphan::new(Foo::default()), Orphan::new(Foo::default()));

    assert!(
        Orphan::new(Foo::default())
            < Foo {
                a: 1,
                b: "".to_string(),
                c: true
            }
    );
    assert!(
        Orphan::new(Foo::default())
            <= Foo {
                a: 1,
                b: "".to_string(),
                c: true
            }
    );

    assert!(Orphan::new(Foo::default()) >= Foo::default());
    assert!(Orphan::new(Foo::default()) >= Orphan::new(Foo::default()));
}
