# realtime-core

The Rust implementation of [`realtime-core`](https://github.com/abrahamahn/realtime-core):
dependency-free primitives for ordered delivery, bounded recovery, subscriptions, reconnect
backoff, and application-defined realtime envelopes.

```rust
use realtime_core::{DeliveryCursor, DeliveryLog, DeliveryRecovery};

let mut log = DeliveryLog::new("server-start-2026-08-26", 128)?;
log.append("document:42", 3, "changed")?;
let cursor = DeliveryCursor::new("server-start-2026-08-26", 0)?;
match log.recover_after(&cursor, None) {
    DeliveryRecovery::Replay { entries, .. } => assert_eq!(entries.len(), 1),
    DeliveryRecovery::SnapshotRequired { .. } => unreachable!(),
}
# Ok::<(), realtime_core::RealtimeError>(())
```

Transport, authorization, persistence, clocks, epoch generation, serialization, and application
stream semantics are owned by the consuming server.

```bash
cargo build --all-targets --locked
cargo check --all-targets --locked
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```
