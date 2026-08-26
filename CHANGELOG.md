# Changelog

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
