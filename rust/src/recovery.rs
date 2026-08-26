use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

use crate::RealtimeError;

pub const MAX_DELIVERY_SEQUENCE: u64 = u64::MAX;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryCursor {
    pub epoch: String,
    pub sequence: u64,
}

impl DeliveryCursor {
    /// Creates a cursor with an application-supplied delivery-log epoch.
    ///
    /// # Errors
    ///
    /// Returns [`RealtimeError::InvalidEpoch`] when `epoch` is empty or only whitespace.
    pub fn new(epoch: impl Into<String>, sequence: u64) -> Result<Self, RealtimeError> {
        let epoch = validate_epoch(epoch)?;
        Ok(Self { epoch, sequence })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryCursorTransition {
    Initialized(DeliveryCursor),
    Advanced(DeliveryCursor),
    Stale(DeliveryCursor),
    EpochChanged(DeliveryCursor),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotRequiredReason {
    EpochMismatch,
    HistoryGap,
    FutureCursor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryEntry<Stream, Payload> {
    pub cursor: DeliveryCursor,
    pub stream: Stream,
    pub stream_version: u64,
    pub payload: Payload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryRecovery<Stream, Payload> {
    Replay {
        latest_cursor: DeliveryCursor,
        entries: Vec<DeliveryEntry<Stream, Payload>>,
    },
    SnapshotRequired {
        reason: SnapshotRequiredReason,
        latest_cursor: DeliveryCursor,
        earliest_available_sequence: u64,
    },
}

#[derive(Clone, Debug)]
pub struct DeliveryLog<Stream, Payload> {
    epoch: String,
    max_entries: usize,
    entries: VecDeque<DeliveryEntry<Stream, Payload>>,
    latest_sequence: u64,
}

fn validate_epoch(epoch: impl Into<String>) -> Result<String, RealtimeError> {
    let epoch = epoch.into();
    if epoch.trim().is_empty() {
        return Err(RealtimeError::InvalidEpoch);
    }
    Ok(epoch)
}

/// Advances a consumer cursor and reports epoch changes explicitly.
#[must_use]
pub fn advance_delivery_cursor(
    current: Option<&DeliveryCursor>,
    incoming: &DeliveryCursor,
) -> DeliveryCursorTransition {
    let Some(current) = current else {
        return DeliveryCursorTransition::Initialized(incoming.clone());
    };
    if current.epoch != incoming.epoch {
        return DeliveryCursorTransition::EpochChanged(incoming.clone());
    }
    if incoming.sequence > current.sequence {
        return DeliveryCursorTransition::Advanced(incoming.clone());
    }
    DeliveryCursorTransition::Stale(current.clone())
}

impl<Stream, Payload> DeliveryLog<Stream, Payload> {
    /// Creates an empty delivery log beginning at sequence zero.
    ///
    /// The application owns epoch generation so the core does not depend on randomness or I/O.
    ///
    /// # Errors
    ///
    /// Returns [`RealtimeError::InvalidEpoch`] when `epoch` is empty and
    /// [`RealtimeError::InvalidCapacity`] when `max_entries` is zero.
    pub fn new(epoch: impl Into<String>, max_entries: usize) -> Result<Self, RealtimeError> {
        Self::with_initial_sequence(epoch, max_entries, 0)
    }

    /// Creates an empty log whose next entry follows `initial_sequence`.
    ///
    /// Applications can use this when restoring an authoritative persisted cursor.
    ///
    /// # Errors
    ///
    /// Returns [`RealtimeError::InvalidEpoch`] when `epoch` is empty and
    /// [`RealtimeError::InvalidCapacity`] when `max_entries` is zero.
    pub fn with_initial_sequence(
        epoch: impl Into<String>,
        max_entries: usize,
        initial_sequence: u64,
    ) -> Result<Self, RealtimeError> {
        let epoch = validate_epoch(epoch)?;
        if max_entries == 0 {
            return Err(RealtimeError::InvalidCapacity);
        }
        Ok(Self {
            epoch,
            max_entries,
            entries: VecDeque::with_capacity(max_entries),
            latest_sequence: initial_sequence,
        })
    }

    /// Appends one delivery without allowing its sequence to wrap.
    ///
    /// # Errors
    ///
    /// Returns [`RealtimeError::SequenceExhausted`] before mutation when the sequence is exhausted.
    pub fn append(
        &mut self,
        stream: Stream,
        stream_version: u64,
        payload: Payload,
    ) -> Result<DeliveryEntry<Stream, Payload>, RealtimeError>
    where
        Stream: Clone,
        Payload: Clone,
    {
        let sequence = self
            .latest_sequence
            .checked_add(1)
            .ok_or(RealtimeError::SequenceExhausted)?;
        let entry = DeliveryEntry {
            cursor: DeliveryCursor {
                epoch: self.epoch.clone(),
                sequence,
            },
            stream,
            stream_version,
            payload,
        };
        self.latest_sequence = sequence;
        self.entries.push_back(entry.clone());
        if self.entries.len() > self.max_entries {
            self.entries.pop_front();
        }
        Ok(entry)
    }

    #[must_use]
    pub fn recover_after(
        &self,
        after: &DeliveryCursor,
        streams: Option<&HashSet<Stream>>,
    ) -> DeliveryRecovery<Stream, Payload>
    where
        Stream: Clone + Eq + Hash,
        Payload: Clone,
    {
        let latest_cursor = self.latest_cursor();
        let earliest_available_sequence = self.entries.front().map_or_else(
            || self.latest_sequence.saturating_add(1),
            |entry| entry.cursor.sequence,
        );
        if after.epoch != self.epoch {
            return DeliveryRecovery::SnapshotRequired {
                reason: SnapshotRequiredReason::EpochMismatch,
                latest_cursor,
                earliest_available_sequence,
            };
        }

        let future_cursor = after.sequence > self.latest_sequence;
        let history_gap = !future_cursor
            && self.entries.front().map_or_else(
                || after.sequence != self.latest_sequence,
                |first| after.sequence < first.cursor.sequence.saturating_sub(1),
            );
        if history_gap || future_cursor {
            return DeliveryRecovery::SnapshotRequired {
                reason: if future_cursor {
                    SnapshotRequiredReason::FutureCursor
                } else {
                    SnapshotRequiredReason::HistoryGap
                },
                latest_cursor,
                earliest_available_sequence,
            };
        }

        let entries = self
            .entries
            .iter()
            .filter(|entry| {
                entry.cursor.sequence > after.sequence
                    && streams.is_none_or(|streams| streams.contains(&entry.stream))
            })
            .cloned()
            .collect();
        DeliveryRecovery::Replay {
            latest_cursor,
            entries,
        }
    }

    #[must_use]
    pub fn entries(&self) -> impl ExactSizeIterator<Item = &DeliveryEntry<Stream, Payload>> {
        self.entries.iter()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn latest_cursor(&self) -> DeliveryCursor {
        DeliveryCursor {
            epoch: self.epoch.clone(),
            sequence: self.latest_sequence,
        }
    }
}
