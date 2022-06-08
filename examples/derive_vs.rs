use vsdb::{vsdb_set_base_dir, Vs};

#[derive(Vs, Debug, Default)]
struct VsDerive {
    a: i32,
    b: u64,
}

fn main() {
    pnk!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    dbg!(VsDerive::default());
}
