use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;

use crate::{
    DeliveryCursor, DeliveryEntry, DeliveryLog, DeliveryRecovery, RealtimeError,
    SubscriptionRegistry, SubscriptionStats,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryPlan<Stream, Connection, Payload> {
    pub entry: DeliveryEntry<Stream, Payload>,
    pub connections: Vec<Connection>,
}

/// Composes subscription indexes and ordered recovery without performing I/O.
#[derive(Clone, Debug)]
pub struct SubscriptionHub<Stream, Connection, Payload> {
    subscriptions: SubscriptionRegistry<Stream, Connection>,
    deliveries: DeliveryLog<Stream, Payload>,
}

impl<Stream, Connection, Payload> SubscriptionHub<Stream, Connection, Payload>
where
    Stream: Clone + Eq + Hash + Ord,
    Connection: Clone + Ord,
    Payload: Clone,
{
    /// Creates an empty hub.
    ///
    /// # Errors
    ///
    /// Returns the same epoch and capacity errors as [`DeliveryLog::new`].
    pub fn new(epoch: impl Into<String>, max_entries: usize) -> Result<Self, RealtimeError> {
        Self::with_initial_sequence(epoch, max_entries, 0)
    }

    /// Creates an empty hub whose next delivery follows `initial_sequence`.
    ///
    /// # Errors
    ///
    /// Returns the same epoch and capacity errors as [`DeliveryLog::with_initial_sequence`].
    pub fn with_initial_sequence(
        epoch: impl Into<String>,
        max_entries: usize,
        initial_sequence: u64,
    ) -> Result<Self, RealtimeError> {
        Ok(Self {
            subscriptions: SubscriptionRegistry::new(),
            deliveries: DeliveryLog::with_initial_sequence(epoch, max_entries, initial_sequence)?,
        })
    }

    pub fn subscribe(&mut self, stream: Stream, connection: Connection) -> bool {
        self.subscriptions.subscribe(stream, connection)
    }

    pub fn unsubscribe(&mut self, stream: &Stream, connection: &Connection) -> bool {
        self.subscriptions.unsubscribe(stream, connection)
    }

    pub fn remove_connection(&mut self, connection: &Connection) -> usize {
        self.subscriptions.remove_connection(connection)
    }

    /// Appends a delivery and returns the connections a transport should notify.
    ///
    /// # Errors
    ///
    /// Returns [`RealtimeError::SequenceExhausted`] before mutation when exhausted.
    pub fn plan_delivery(
        &mut self,
        stream: Stream,
        stream_version: u64,
        payload: Payload,
    ) -> Result<DeliveryPlan<Stream, Connection, Payload>, RealtimeError> {
        let connections = self
            .subscriptions
            .subscribers(&stream)
            .into_iter()
            .cloned()
            .collect();
        let entry = self.deliveries.append(stream, stream_version, payload)?;
        Ok(DeliveryPlan { entry, connections })
    }

    #[must_use]
    pub fn recover_after(
        &self,
        cursor: &DeliveryCursor,
        authorized_streams: &HashSet<Stream>,
    ) -> DeliveryRecovery<Stream, Payload> {
        self.deliveries
            .recover_after(cursor, Some(authorized_streams))
    }

    #[must_use]
    pub fn entries(&self) -> impl ExactSizeIterator<Item = &DeliveryEntry<Stream, Payload>> {
        self.deliveries.entries()
    }

    #[must_use]
    pub fn retained_streams(&self) -> HashSet<Stream> {
        self.deliveries
            .entries()
            .map(|entry| entry.stream.clone())
            .collect()
    }

    #[must_use]
    pub fn history_len(&self) -> usize {
        self.deliveries.len()
    }

    #[must_use]
    pub fn latest_cursor(&self) -> DeliveryCursor {
        self.deliveries.latest_cursor()
    }

    #[must_use]
    pub fn subscriber_count(&self, stream: &Stream) -> usize {
        self.subscriptions.subscriber_count(stream)
    }

    #[must_use]
    pub fn stream_count(&self) -> usize {
        self.subscriptions.stream_count()
    }

    #[must_use]
    pub fn stats(&self) -> SubscriptionStats {
        self.subscriptions.stats()
    }
}

/// Keeps only the newest delivery for each stream, ordered by final delivery sequence.
///
/// This is opt-in because non-idempotent delta payloads must replay every entry.
/// # Errors
///
/// Returns [`RealtimeError::MixedDeliveryEpoch`] rather than comparing sequence numbers from
/// unrelated delivery-log lifetimes.
pub fn latest_delivery_per_stream<Stream, Payload>(
    entries: impl IntoIterator<Item = DeliveryEntry<Stream, Payload>>,
) -> Result<Vec<DeliveryEntry<Stream, Payload>>, RealtimeError>
where
    Stream: Clone + Ord,
{
    let mut latest = BTreeMap::<Stream, DeliveryEntry<Stream, Payload>>::new();
    let mut epoch = None::<String>;
    for entry in entries {
        match &epoch {
            None => epoch = Some(entry.cursor.epoch.clone()),
            Some(epoch) if epoch != &entry.cursor.epoch => {
                return Err(RealtimeError::MixedDeliveryEpoch);
            }
            Some(_) => {}
        }
        let should_replace = latest
            .get(&entry.stream)
            .is_none_or(|existing| entry.cursor.sequence > existing.cursor.sequence);
        if should_replace {
            latest.insert(entry.stream.clone(), entry);
        }
    }
    let mut entries = latest.into_values().collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.cursor.sequence);
    Ok(entries)
}
