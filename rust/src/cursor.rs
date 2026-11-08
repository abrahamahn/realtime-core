use crate::error::RealtimeError;
use crate::limits::MAX_INTEROPERABLE_INTEGER;

pub const MAX_DELIVERY_SEQUENCE: u64 = MAX_INTEROPERABLE_INTEGER;

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
    /// Returns [`RealtimeError::InvalidEpoch`] when `epoch` is empty or only whitespace and
    /// [`RealtimeError::InvalidSequence`] when `sequence` exceeds the exact cross-language range.
    pub fn new(epoch: impl Into<String>, sequence: u64) -> Result<Self, RealtimeError> {
        let epoch = validate_epoch(epoch)?;
        validate_sequence(sequence)?;
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

pub(crate) fn validate_epoch(epoch: impl Into<String>) -> Result<String, RealtimeError> {
    let epoch = epoch.into();
    if epoch.trim().is_empty() {
        return Err(RealtimeError::InvalidEpoch);
    }
    Ok(epoch)
}

pub(crate) const fn validate_sequence(sequence: u64) -> Result<(), RealtimeError> {
    if sequence > MAX_DELIVERY_SEQUENCE {
        return Err(RealtimeError::InvalidSequence);
    }
    Ok(())
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
