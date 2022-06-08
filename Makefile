all: release

export CARGO_NET_GIT_FETCH_WITH_CLI = true

build:
	cargo build

release:
	cargo build --release

lint:
	cargo clippy
	cargo clippy --features "compress"
	cargo check --tests
	cargo check --benches
	cargo check --examples

lintall: lint
	cargo clippy --no-default-features --features "derive,sled_engine,compress,msgpack_codec"
	cargo check --tests --no-default-features --features "derive,sled_engine,msgpack_codec"
	cargo check --benches --no-default-features --features "derive,sled_engine,msgpack_codec"
	cargo check --examples --no-default-features --features "derive,sled_engine,msgpack_codec"

example:
	- rm -rf ~/.vsdb /tmp/.vsdb
	cargo run --example derive_vs
	cargo run --example web_server
	cargo run --example blockchain_state

test: example
	- rm -rf ~/.vsdb /tmp/.vsdb
	cargo test --release --tests --bins --features "derive" # -- --test-threads=1
	- rm -rf ~/.vsdb /tmp/.vsdb
	cargo test --tests --bins --features "derive" # -- --test-threads=1

exampleall:
	- rm -rf ~/.vsdb /tmp/.vsdb
	cargo run --no-default-features --features "derive,sled_engine,msgpack_codec" --example derive_vs
	cargo run --no-default-features --features "derive,sled_engine,msgpack_codec" --example web_server
	cargo run --no-default-features --features "derive,sled_engine,msgpack_codec" --example blockchain_state

testall: test exampleall
	- rm -rf ~/.vsdb /tmp/.vsdb
	cargo test --release --tests --bins --no-default-features --features "derive,sled_engine,msgpack_codec,compress" # -- --test-threads=1
	- rm -rf ~/.vsdb /tmp/.vsdb
	cargo test --tests --bins --no-default-features --features "derive,sled_engine,msgpack_codec" # -- --test-threads=1

bench:
	- rm -rf ~/.vsdb
	cargo bench
	du -sh ~/.vsdb

benchall: bench
	- rm -rf ~/.vsdb
	cargo bench --no-default-features --features "sled_engine,msgpack_codec"
	du -sh ~/.vsdb

fmt:
	cargo +nightly fmt

fmtall:
	bash tools/fmt.sh

update:
	cargo update

clean:
	cargo clean
	git stash
	git clean -fdx

doc:
	cargo doc --open
