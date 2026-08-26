use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RealtimeError {
    InvalidEpoch,
    InvalidCapacity,
    InvalidBackoffPolicy,
    ReconnectAttemptExhausted,
    SequenceExhausted,
}

impl fmt::Display for RealtimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidEpoch => "delivery log epoch must be non-empty",
            Self::InvalidCapacity => "delivery log capacity must be positive",
            Self::InvalidBackoffPolicy => "reconnect maximum delay must be at least its base delay",
            Self::ReconnectAttemptExhausted => "reconnect attempt is exhausted",
            Self::SequenceExhausted => "delivery sequence is exhausted",
        })
    }
}

impl std::error::Error for RealtimeError {}
