# Changelog

## 0.3.2

- Keep delivery sequences, stream versions, capacities, and reconnect delays within the exact
  cross-language integer range, rejecting larger Rust inputs before mutation.
- Align reconnect attempts to an unsigned 32-bit counter in TypeScript and Rust.
- Preserve zero-delay TypeScript backoff at every attempt instead of producing `NaN` after numeric
  exponent overflow.
- Execute the exact-integer recovery boundary through the shared TypeScript/Rust conformance corpus.

## 0.3.1

- Require explicit authorized streams when recovering through the subscription hub.
- Reject replay compaction across mixed delivery epochs.
- Execute authorization, eviction, epoch, and future-cursor recovery vectors in TypeScript and
  Rust, and verify both published artifacts in CI.

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
