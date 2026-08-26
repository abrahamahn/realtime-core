use crate::{RealtimeError, ReconnectBackoffPolicy, reconnect_delay_ms};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconnectStatus {
    Idle,
    Waiting,
    Connecting,
    Open,
    Stable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconnectState {
    pub attempt: u32,
    pub status: ReconnectStatus,
}

impl Default for ReconnectState {
    fn default() -> Self {
        Self {
            attempt: 0,
            status: ReconnectStatus::Idle,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconnectSchedule {
    pub state: ReconnectState,
    pub delay_ms: u64,
}

/// Calculates a delay and advances the attempt before a timer or I/O runs.
///
/// # Errors
///
/// Returns policy errors from [`reconnect_delay_ms`] or
/// [`RealtimeError::ReconnectAttemptExhausted`] before the attempt wraps.
pub fn schedule_reconnect_attempt(
    state: ReconnectState,
    policy: ReconnectBackoffPolicy,
) -> Result<ReconnectSchedule, RealtimeError> {
    let attempt = state
        .attempt
        .checked_add(1)
        .ok_or(RealtimeError::ReconnectAttemptExhausted)?;
    Ok(ReconnectSchedule {
        delay_ms: reconnect_delay_ms(state.attempt, policy)?,
        state: ReconnectState {
            attempt,
            status: ReconnectStatus::Waiting,
        },
    })
}

#[must_use]
pub const fn mark_reconnect_connecting(state: ReconnectState) -> ReconnectState {
    ReconnectState {
        status: ReconnectStatus::Connecting,
        ..state
    }
}

#[must_use]
pub const fn mark_reconnect_open(state: ReconnectState) -> ReconnectState {
    ReconnectState {
        status: ReconnectStatus::Open,
        ..state
    }
}

#[must_use]
pub const fn mark_reconnect_stable(_state: ReconnectState) -> ReconnectState {
    ReconnectState {
        attempt: 0,
        status: ReconnectStatus::Stable,
    }
}

#[must_use]
pub const fn ensure_minimum_reconnect_attempt(
    state: ReconnectState,
    minimum_attempt: u32,
) -> ReconnectState {
    ReconnectState {
        attempt: if state.attempt > minimum_attempt {
            state.attempt
        } else {
            minimum_attempt
        },
        ..state
    }
}

#[must_use]
pub fn reset_reconnect_state() -> ReconnectState {
    ReconnectState::default()
}
