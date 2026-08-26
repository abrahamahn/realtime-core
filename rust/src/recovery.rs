use std::collections::{HashSet, VecDeque};
use std::hash::Hash;

use crate::RealtimeError;

pub const MAX_DELIVERY_SEQUENCE: u64 = u64::MAX;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryEntry<Stream, Payload> {
    pub sequence: u64,
    pub stream: Stream,
    pub stream_version: u64,
    pub payload: Payload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryRecovery<Stream, Payload> {
    Replay {
        latest_sequence: u64,
        entries: Vec<DeliveryEntry<Stream, Payload>>,
    },
    SnapshotRequired {
        latest_sequence: u64,
        earliest_available_sequence: u64,
    },
}

#[derive(Clone, Debug)]
pub struct DeliveryLog<Stream, Payload> {
    max_entries: usize,
    entries: VecDeque<DeliveryEntry<Stream, Payload>>,
    latest_sequence: u64,
}

impl<Stream, Payload> DeliveryLog<Stream, Payload> {
    /// Creates an empty delivery log beginning at sequence zero.
    ///
    /// # Errors
    ///
    /// Returns [`RealtimeError::InvalidCapacity`] when `max_entries` is zero.
    pub fn new(max_entries: usize) -> Result<Self, RealtimeError> {
        Self::with_initial_sequence(max_entries, 0)
    }

    /// Creates an empty log whose next entry follows `initial_sequence`.
    ///
    /// Applications can use this when restoring an authoritative persisted cursor.
    ///
    /// # Errors
    ///
    /// Returns [`RealtimeError::InvalidCapacity`] when `max_entries` is zero.
    pub fn with_initial_sequence(
        max_entries: usize,
        initial_sequence: u64,
    ) -> Result<Self, RealtimeError> {
        if max_entries == 0 {
            return Err(RealtimeError::InvalidCapacity);
        }
        Ok(Self {
            max_entries,
            entries: VecDeque::with_capacity(max_entries),
            latest_sequence: initial_sequence,
        })
    }

    /// Appends one delivery without allowing its global sequence to wrap.
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
            sequence,
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
        after_sequence: u64,
        streams: Option<&HashSet<Stream>>,
    ) -> DeliveryRecovery<Stream, Payload>
    where
        Stream: Clone + Eq + Hash,
        Payload: Clone,
    {
        let Some(first_retained) = self.entries.front() else {
            return if after_sequence == self.latest_sequence {
                DeliveryRecovery::Replay {
                    latest_sequence: self.latest_sequence,
                    entries: Vec::new(),
                }
            } else {
                DeliveryRecovery::SnapshotRequired {
                    latest_sequence: self.latest_sequence,
                    earliest_available_sequence: self.latest_sequence.saturating_add(1),
                }
            };
        };

        if after_sequence > self.latest_sequence
            || after_sequence < first_retained.sequence.saturating_sub(1)
        {
            return DeliveryRecovery::SnapshotRequired {
                latest_sequence: self.latest_sequence,
                earliest_available_sequence: first_retained.sequence,
            };
        }

        let entries = self
            .entries
            .iter()
            .filter(|entry| {
                entry.sequence > after_sequence
                    && streams.is_none_or(|streams| streams.contains(&entry.stream))
            })
            .cloned()
            .collect();
        DeliveryRecovery::Replay {
            latest_sequence: self.latest_sequence,
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
    pub fn latest_sequence(&self) -> u64 {
        self.latest_sequence
    }
}
