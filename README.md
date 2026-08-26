# realtime-core

`realtime-core` is a focused, application-independent foundation for authoritative realtime systems.
It provides the deterministic state that sits behind transports: subscription hubs, ordered
delivery cursors, bounded replay, client recovery reduction, reconnect state, heartbeat liveness,
and typed snapshot/delta/command/receipt boundaries.

- [`typescript/`](typescript/) — npm package `@abrahamahn/realtime-core`
- [`rust/`](rust/) — Cargo crate `realtime-core` (imported as `realtime_core`)

The implementations share the same concepts and invariants but are independently usable. Neither
folder imports the other or requires a particular application.

## What it is not

`realtime-core` is not a WebSocket framework, broker client, presence product, database, durable
log, authorization system, serializer, timer scheduler, or distributed consensus protocol. It
contains no product, UI, transport-route, or application channel concepts.

Applications remain responsible for:

- authenticating connections and authorizing every requested stream;
- choosing WebSocket, SSE, QUIC, broker, or in-process transports;
- generating log epochs and persisting authoritative state, snapshots, and cursors where required;
- supplying clocks and payload serialization;
- defining domain-specific stream names, versions, commands, and results.

## Core model

```text
append(stream, stream version, payload)
                    ↓
 fan-out plan + application epoch + monotonic sequence
                    ↓
recoverAfter(cursor, authorized streams)
         ↙                         ↘
 ordered replay             snapshot required
```

`DeliveryLog` deliberately models a bounded in-memory replay window. Its cursor always carries the
application-supplied log epoch together with the sequence. When a cursor is no longer provably
recoverable, it fails closed with `snapshot-required`; an application then reads its authoritative
snapshot source.

## Important invariants

- Delivery sequences never repeat or wrap within a log epoch.
- Numeric sequences from different epochs are never treated as comparable.
- Replay entries are ordered by the global delivery sequence.
- A gap, future cursor, or different epoch cannot be represented as an empty successful replay.
- Capacity eviction never changes the latest sequence.
- Subscription removal updates both indexes atomically.
- A hub plans recipients and recovery but never sends or serializes a payload.
- Client recovery turns any epoch discontinuity into authoritative snapshot work.
- Reconnect state does not reset merely because a transport opened.
- A liveness sweep probes acknowledged connections once before declaring them stale.
- Reconnect backoff is deterministic and capped.
- The core performs no I/O and reads no global clock or randomness.

## TypeScript example

```ts
import { SubscriptionHub } from '@abrahamahn/realtime-core';

const hub = new SubscriptionHub<string, { id: string }, { revision: number }>({
  epoch: crypto.randomUUID(),
  maxEntries: 256,
});
const connection = { id: 'browser-1' };
hub.subscribe('document:42', connection);
const delivery = hub.planDelivery('document:42', 8, { revision: 8 });
// A WebSocket/SSE/in-process adapter sends delivery.entry to delivery.connections.

const recovery = hub.recoverAfter(
  { epoch: delivery.entry.cursor.epoch, sequence: 0 },
  new Set(['document:42']),
);
if (recovery.kind === 'snapshot-required') {
  // Load an authoritative snapshot through an application-owned repository.
}
```

## Rust example

```rust
use std::collections::HashSet;
use realtime_core::{DeliveryCursor, SubscriptionHub};

let mut hub = SubscriptionHub::new("server-start-2026-08-26", 256)?;
hub.subscribe("document:42", "browser:1");
hub.plan_delivery("document:42", 8, 8_u64)?;
let streams = HashSet::from(["document:42"]);
let cursor = DeliveryCursor::new("server-start-2026-08-26", 0)?;
let recovery = hub.recover_after(&cursor, Some(&streams));
# Ok::<(), realtime_core::RealtimeError>(())
```

## Integration philosophy

Keep transports and infrastructure outside the core. A server adapter authenticates a connection,
passes only authorized streams into recovery, serializes delivery plans, and reports heartbeat
acknowledgements. A client adapter owns its socket and timers while delegating recovery and retry
transitions to the core.

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
