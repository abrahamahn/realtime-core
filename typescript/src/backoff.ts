import { MAX_RECONNECT_ATTEMPT } from "./limits.js";

export interface ReconnectBackoffPolicy {
  readonly baseMs: number;
  readonly maxMs: number;
}

export function reconnectDelayMs(
  attempt: number,
  policy: ReconnectBackoffPolicy,
): number {
  if (
    !Number.isSafeInteger(attempt) ||
    attempt < 0 ||
    attempt > MAX_RECONNECT_ATTEMPT
  ) {
    throw new RangeError("attempt must be an unsigned 32-bit integer");
  }
  if (!Number.isSafeInteger(policy.baseMs) || policy.baseMs < 0) {
    throw new RangeError("baseMs must be a non-negative safe integer");
  }
  if (!Number.isSafeInteger(policy.maxMs) || policy.maxMs < policy.baseMs) {
    throw new RangeError("maxMs must be a safe integer at least baseMs");
  }
  if (policy.baseMs === 0) return 0;
  if (attempt >= 53) return policy.maxMs;
  return Math.min(policy.maxMs, policy.baseMs * 2 ** attempt);
}
