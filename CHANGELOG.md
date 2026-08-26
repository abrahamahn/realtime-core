# Changelog

## 0.3.0

- Add a transport-neutral `SubscriptionHub` that composes subscription indexes with ordered
  delivery and returns fan-out plans without performing I/O.
- Add deterministic client recovery reducers for stream invalidation and snapshot boundaries.
- Add reconnect state transitions that retain backoff across transport-open events and reset only
  after application-defined stability.
- Add transport-neutral heartbeat acknowledgement and liveness sweep tracking.
- Add opt-in latest-delivery-per-stream replay compaction for invalidation protocols.
- Preserve TypeScript and Rust behavior parity for every new deterministic primitive.

## 0.2.0

- Make delivery recovery restart-safe with application-supplied epochs.
- Replace sequence-only recovery with `DeliveryCursor { epoch, sequence }`.
- Report epoch mismatch, history gap, and future cursor as distinct snapshot reasons.
- Add deterministic consumer cursor transitions in TypeScript and Rust.

## 0.1.0

- Add transport-neutral subscription indexes.
- Add bounded ordered delivery and snapshot-required recovery.
- Add deterministic capped reconnect backoff.
- Add generic snapshot, delta, command, and receipt envelopes.
- Provide independent TypeScript and Rust implementations.
