# @abrahamahn/realtime-core

The TypeScript implementation of [`realtime-core`](https://github.com/abrahamahn/realtime-core):
transport-neutral primitives for ordered delivery, reconnect recovery, subscriptions, and command
receipts.

It does not open WebSockets, authenticate users, read a database, choose a message broker, own a
clock, or interpret application payloads.

## Example

```ts
import { DeliveryLog, SubscriptionRegistry } from '@abrahamahn/realtime-core';

const subscriptions = new SubscriptionRegistry<string, { readonly id: string }>();
const connection = { id: 'connection-1' };
subscriptions.subscribe('document:42', connection);

const deliveries = new DeliveryLog<string, { readonly revision: number }>(100);
const entry = deliveries.append('document:42', 7, { revision: 7 });
const recovery = deliveries.recoverAfter(entry.sequence - 1, new Set(['document:42']));
```

Applications provide transport adapters, authorization, snapshots, durable sequence ownership,
payload serialization, and domain-specific stream/version rules. A persisted server may restore
its cursor through the `DeliveryLog` constructor's `initialSequence` argument.

## Invariants

- Delivery sequences are positive, monotonic safe integers and never wrap.
- A replay is returned only when the requested cursor is fully represented by retained history.
- An evicted, future, or foreign-epoch cursor requires an authoritative snapshot.
- Subscription indexes remain consistent in both stream-to-connection directions.
- Backoff is deterministic, bounded, and free of clock or random dependencies.

## Development

```bash
pnpm install --frozen-lockfile
pnpm build
pnpm typecheck
pnpm lint
pnpm test
```
