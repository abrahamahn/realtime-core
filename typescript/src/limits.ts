/** Largest integer that remains exact across JavaScript and Rust JSON boundaries. */
export const MAX_INTEROPERABLE_INTEGER = Number.MAX_SAFE_INTEGER;

/** Reconnect attempts use an unsigned 32-bit counter in both implementations. */
export const MAX_RECONNECT_ATTEMPT = 0xffff_ffff;
