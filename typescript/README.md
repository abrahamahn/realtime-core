# @abrahamahn/realtime-core

The TypeScript implementation of [`realtime-core`](https://github.com/abrahamahn/realtime-core):
transport-neutral primitives for ordered delivery, subscription hubs, client recovery, reconnect
state, heartbeat liveness, and command receipts.

It does not open WebSockets, authenticate users, read a database, choose a message broker, own a
clock, or interpret application payloads.

## Example

```ts
import { SubscriptionHub, reduceClientRecovery } from '@abrahamahn/realtime-core';

const hub = new SubscriptionHub<string, { readonly id: string }, { readonly revision: number }>({
  epoch: crypto.randomUUID(),
  maxEntries: 100,
});
const connection = { id: 'connection-1' };
hub.subscribe('document:42', connection);
const plan = hub.planDelivery('document:42', 7, { revision: 7 });
const recovery = hub.recoverAfter(plan.entry.cursor, new Set(['document:42']));
const decision = reduceClientRecovery(
  { cursor: null },
  { kind: 'update', stream: plan.entry.stream, cursor: plan.entry.cursor },
);
void recovery;
```

Applications provide transport adapters, authorization, snapshots, epoch generation, durable
cursor ownership, payload serialization, and domain-specific stream/version rules. A persisted
server may restore its cursor through the `DeliveryLog` constructor's `initialSequence` argument.

## Invariants

- Delivery sequences are positive, monotonic safe integers and never wrap.
- Shared numeric state stays within JavaScript's exact-integer range in both implementations.
- An epoch and sequence travel together as one `DeliveryCursor`.
- A replay is returned only when the requested cursor is fully represented by retained history.
- An evicted, future, or foreign-epoch cursor requires an authoritative snapshot.
- Hub recovery always receives an explicit authorization set; an empty set replays nothing.
- Invalidation collapse rejects entries from different epochs instead of comparing their sequences.
- Subscription indexes remain consistent in both stream-to-connection directions.
- Backoff is deterministic, bounded, and free of clock or random dependencies.
- Reconnect attempts use an unsigned 32-bit counter and zero-delay policies remain zero at every
  attempt.
- Transport-open does not reset retry state until the application marks it stable.
- Heartbeat sweeps deterministically separate probes from stale connections.

## Development

```bash
pnpm install --frozen-lockfile
pnpm build
pnpm typecheck
pnpm lint
pnpm test
```
