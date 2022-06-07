use criterion::criterion_main;

mod benchmarks;

criterion_main! {
    benchmarks::versioned_multi_key_mapx_double_key::benches,
    benchmarks::versioned_multi_key_mapx_raw::benches,
    benchmarks::versioned_multi_key_mapx_triple_key::benches,
    benchmarks::versioned_mapx::benches,
    benchmarks::versioned_mapx_ord::benches,
    benchmarks::versioned_mapx_ord_rawkey::benches,
    benchmarks::versioned_mapx_raw::benches,
    benchmarks::versioned_vecx::benches,
    benchmarks::basic_multi_key_mapx_double_key::benches,
    benchmarks::basic_multi_key_mapx_raw::benches,
    benchmarks::basic_multi_key_mapx_rawkey::benches,
    benchmarks::basic_multi_key_mapx_triple_key::benches,
    benchmarks::basic_mapx::benches,
    benchmarks::basic_mapx_ord::benches,
    benchmarks::basic_mapx_ord_rawkey::benches,
    benchmarks::basic_mapx_ord_rawvalue::benches,
    benchmarks::basic_mapx_raw::benches,
    benchmarks::basic_vecx::benches,
    benchmarks::basic_vecx_raw::benches,
}
