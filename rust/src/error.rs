use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RealtimeError {
    InvalidEpoch,
    InvalidCapacity,
    InvalidBackoffPolicy,
    InvalidSequence,
    InvalidStreamVersion,
    MixedDeliveryEpoch,
    ReconnectAttemptExhausted,
    SequenceExhausted,
}

impl fmt::Display for RealtimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidEpoch => "delivery log epoch must be non-empty",
            Self::InvalidCapacity => {
                "delivery log capacity must be positive and interoperably bounded"
            }
            Self::InvalidBackoffPolicy => {
                "reconnect delays must be interoperably bounded and maximum must be at least base"
            }
            Self::InvalidSequence => "delivery sequence exceeds the interoperable integer range",
            Self::InvalidStreamVersion => "stream version exceeds the interoperable integer range",
            Self::MixedDeliveryEpoch => {
                "deliveries from different epochs cannot be collapsed together"
            }
            Self::ReconnectAttemptExhausted => "reconnect attempt is exhausted",
            Self::SequenceExhausted => "delivery sequence is exhausted",
        })
    }
}

impl std::error::Error for RealtimeError {}
