use crate::{MAX_INTEROPERABLE_INTEGER, RealtimeError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconnectBackoffPolicy {
    pub base_ms: u64,
    pub max_ms: u64,
}

/// Returns the deterministic exponential delay for a zero-based reconnect attempt.
///
/// # Errors
///
/// Returns [`RealtimeError::InvalidBackoffPolicy`] when `max_ms` is below `base_ms` or either
/// delay exceeds the exact cross-language integer range.
pub fn reconnect_delay_ms(
    attempt: u32,
    policy: ReconnectBackoffPolicy,
) -> Result<u64, RealtimeError> {
    if policy.max_ms < policy.base_ms
        || policy.base_ms > MAX_INTEROPERABLE_INTEGER
        || policy.max_ms > MAX_INTEROPERABLE_INTEGER
    {
        return Err(RealtimeError::InvalidBackoffPolicy);
    }
    if policy.base_ms == 0 || attempt >= u64::BITS {
        return Ok(if policy.base_ms == 0 {
            0
        } else {
            policy.max_ms
        });
    }
    let multiplier = 1_u64 << attempt;
    Ok(policy
        .base_ms
        .checked_mul(multiplier)
        .unwrap_or(policy.max_ms)
        .min(policy.max_ms))
}
