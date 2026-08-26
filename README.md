# realtime-core

`realtime-core` is a small, application-independent foundation for authoritative realtime systems.
It provides the deterministic state that sits behind transports: subscription indexes, ordered
delivery cursors, bounded replay, snapshot-required detection, reconnect backoff, and typed
snapshot/delta/command/receipt boundaries.

- [`typescript/`](typescript/) — npm package `@abrahamahn/realtime-core`
- [`rust/`](rust/) — Cargo crate `realtime-core` (imported as `realtime_core`)

The implementations share the same concepts and invariants but are independently usable. Neither
folder imports the other or requires a particular application.

## What it is not

`realtime-core` is not a WebSocket framework, broker client, presence product, database, durable
log, authorization system, serializer, or distributed consensus protocol. It contains no Ganbate,
casino, lobby, game, UI, or application channel concepts.

Applications remain responsible for:

- authenticating connections and authorizing every requested stream;
- choosing WebSocket, SSE, QUIC, broker, or in-process transports;
- persisting authoritative state, snapshots, epochs, and sequence cursors where required;
- supplying clocks and payload serialization;
- defining domain-specific stream names, versions, commands, and results.

## Core model

```text
append(stream, stream version, payload)
                    ↓
       monotonic delivery sequence
                    ↓
recoverAfter(cursor, authorized streams)
         ↙                         ↘
 ordered replay             snapshot required
```

`DeliveryLog` deliberately models a bounded in-memory replay window. When a cursor is no longer
provably recoverable, it fails closed with `snapshot-required`; an application then reads its own
authoritative snapshot source.

## Important invariants

- Delivery sequences never repeat or wrap within a log epoch.
- Replay entries are ordered by the global delivery sequence.
- A gap, future cursor, or different epoch cannot be represented as an empty successful replay.
- Capacity eviction never changes the latest sequence.
- Subscription removal updates both indexes atomically.
- Reconnect backoff is deterministic and capped.
- The core performs no I/O and reads no global clock or randomness.

## TypeScript example

```ts
import { DeliveryLog } from '@abrahamahn/realtime-core';

const log = new DeliveryLog<string, { revision: number }>(256);
log.append('document:42', 8, { revision: 8 });

const recovery = log.recoverAfter(0, new Set(['document:42']));
if (recovery.kind === 'snapshot-required') {
  // Load an authoritative snapshot through an application-owned repository.
}
```

## Rust example

```rust
use std::collections::HashSet;
use realtime_core::DeliveryLog;

let mut log = DeliveryLog::new(256)?;
log.append("document:42", 8, 8_u64)?;
let streams = HashSet::from(["document:42"]);
let recovery = log.recover_after(0, Some(&streams));
# Ok::<(), realtime_core::RealtimeError>(())
```

## Integration philosophy

Keep transports and infrastructure outside the core. A server adapter authenticates a connection,
maps application events to `append`, filters replay requests through authorization, and sends the
returned entries. Browser clients may use the backoff helper and typed envelopes without depending
on server infrastructure.

## Development

```bash
cd typescript
pnpm install --frozen-lockfile
pnpm build && pnpm typecheck && pnpm lint && pnpm test

cd ../rust
cargo build --all-targets --locked
cargo check --all-targets --locked
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```
