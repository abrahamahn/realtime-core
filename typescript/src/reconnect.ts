import { reconnectDelayMs, type ReconnectBackoffPolicy } from './backoff.js';

export type ReconnectStatus = 'idle' | 'waiting' | 'connecting' | 'open' | 'stable';

export interface ReconnectState {
  /** Number used by the next exponential-delay calculation. */
  readonly attempt: number;
  readonly status: ReconnectStatus;
}

export interface ReconnectSchedule {
  readonly state: ReconnectState;
  readonly delayMs: number;
}

function validateAttempt(attempt: number): void {
  if (!Number.isSafeInteger(attempt) || attempt < 0) {
    throw new RangeError('attempt must be a non-negative safe integer');
  }
}

function reconnectState(attempt: number, status: ReconnectStatus): ReconnectState {
  validateAttempt(attempt);
  return Object.freeze({ attempt, status });
}

export function createReconnectState(): ReconnectState {
  return reconnectState(0, 'idle');
}

/** Calculates the next delay and advances the attempt before any timer or I/O runs. */
export function scheduleReconnectAttempt(
  state: ReconnectState,
  policy: ReconnectBackoffPolicy,
): ReconnectSchedule {
  validateAttempt(state.attempt);
  if (state.attempt === Number.MAX_SAFE_INTEGER) {
    throw new RangeError('reconnect attempt is exhausted');
  }
  return {
    delayMs: reconnectDelayMs(state.attempt, policy),
    state: reconnectState(state.attempt + 1, 'waiting'),
  };
}

export function markReconnectConnecting(state: ReconnectState): ReconnectState {
  return reconnectState(state.attempt, 'connecting');
}

/** An open transport is not yet considered stable and does not reset backoff. */
export function markReconnectOpen(state: ReconnectState): ReconnectState {
  return reconnectState(state.attempt, 'open');
}

export function markReconnectStable(state: ReconnectState): ReconnectState {
  validateAttempt(state.attempt);
  return reconnectState(0, 'stable');
}

export function ensureMinimumReconnectAttempt(
  state: ReconnectState,
  minimumAttempt: number,
): ReconnectState {
  validateAttempt(minimumAttempt);
  return reconnectState(Math.max(state.attempt, minimumAttempt), state.status);
}

export function resetReconnectState(): ReconnectState {
  return createReconnectState();
}
