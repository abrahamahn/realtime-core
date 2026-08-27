/// Largest integer that remains exact across JavaScript and Rust JSON boundaries.
pub const MAX_INTEROPERABLE_INTEGER: u64 = 9_007_199_254_740_991;

/// Reconnect attempts use an unsigned 32-bit counter in both implementations.
pub const MAX_RECONNECT_ATTEMPT: u32 = u32::MAX;
