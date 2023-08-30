check-fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets --all-features --workspace -- -D warnings

check: check-fmt clippy

test:
	cargo test --no-fail-fast --all-targets --all-features --workspace
	cargo test --no-fail-fast --doc --all-features --workspace