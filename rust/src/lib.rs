//! Transport-neutral primitives for subscriptions, ordered delivery, reconnect recovery,
//! and application-defined realtime envelopes.
//!
//! This crate performs no network, storage, authentication, clock, broker, or serialization I/O.

mod backoff;
mod envelopes;
mod error;
mod recovery;
mod subscriptions;

pub use backoff::{ReconnectBackoffPolicy, reconnect_delay_ms};
pub use envelopes::{CommandEnvelope, CommandReceipt, DeltaEnvelope, SnapshotEnvelope};
pub use error::RealtimeError;
pub use recovery::{DeliveryEntry, DeliveryLog, DeliveryRecovery, MAX_DELIVERY_SEQUENCE};
pub use subscriptions::{SubscriptionRegistry, SubscriptionStats};
