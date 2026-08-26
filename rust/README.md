# realtime-core

The Rust implementation of [`realtime-core`](https://github.com/abrahamahn/realtime-core):
dependency-free primitives for ordered delivery, bounded recovery, subscriptions, reconnect
backoff, and application-defined realtime envelopes.

```rust
use realtime_core::{DeliveryLog, DeliveryRecovery};

let mut log = DeliveryLog::new(128)?;
log.append("document:42", 3, "changed")?;
match log.recover_after(0, None) {
    DeliveryRecovery::Replay { entries, .. } => assert_eq!(entries.len(), 1),
    DeliveryRecovery::SnapshotRequired { .. } => unreachable!(),
}
# Ok::<(), realtime_core::RealtimeError>(())
```

Transport, authorization, persistence, clocks, serialization, and application stream semantics are
owned by the consuming server.

```bash
cargo build --all-targets --locked
cargo check --all-targets --locked
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```
