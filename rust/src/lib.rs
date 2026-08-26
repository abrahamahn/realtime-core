//! Transport-neutral primitives for subscriptions, ordered delivery, reconnect recovery,
//! and application-defined realtime envelopes.
//!
//! This crate performs no network, storage, authentication, clock, broker, or serialization I/O.

mod backoff;
mod client_recovery;
mod envelopes;
mod error;
mod hub;
mod liveness;
mod reconnect;
mod recovery;
mod subscriptions;

pub use backoff::{ReconnectBackoffPolicy, reconnect_delay_ms};
pub use client_recovery::{
    ClientInvalidation, ClientRecoveryDecision, ClientRecoveryEntry, ClientRecoveryEvent,
    ClientRecoveryState, reduce_client_recovery,
};
pub use envelopes::{CommandEnvelope, CommandReceipt, DeltaEnvelope, SnapshotEnvelope};
pub use error::RealtimeError;
pub use hub::{DeliveryPlan, SubscriptionHub, latest_delivery_per_stream};
pub use liveness::{LivenessSweep, LivenessTracker};
pub use reconnect::{
    ReconnectSchedule, ReconnectState, ReconnectStatus, ensure_minimum_reconnect_attempt,
    mark_reconnect_connecting, mark_reconnect_open, mark_reconnect_stable, reset_reconnect_state,
    schedule_reconnect_attempt,
};
pub use recovery::{
    DeliveryCursor, DeliveryCursorTransition, DeliveryEntry, DeliveryLog, DeliveryRecovery,
    MAX_DELIVERY_SEQUENCE, SnapshotRequiredReason, advance_delivery_cursor,
};
pub use subscriptions::{SubscriptionRegistry, SubscriptionStats};
