import { advanceDeliveryCursor, type DeliveryCursor } from "./recovery.js";

export interface ClientRecoveryState {
  readonly cursor: DeliveryCursor | null;
}

export interface ClientRecoveryEntry<Stream> {
  readonly stream: Stream;
  readonly cursor?: DeliveryCursor;
}

export type ClientRecoveryEvent<Stream> =
  | {
      readonly kind: "update";
      readonly stream: Stream;
      readonly cursor?: DeliveryCursor;
    }
  | {
      readonly kind: "recovery";
      readonly entries: readonly ClientRecoveryEntry<Stream>[];
      readonly latestCursor?: DeliveryCursor;
      readonly snapshotRequired?: boolean;
    };

export type ClientInvalidation<Stream> =
  | { readonly kind: "none" }
  | { readonly kind: "streams"; readonly streams: readonly Stream[] }
  | { readonly kind: "all" };

export interface ClientRecoveryDecision<Stream> {
  readonly state: ClientRecoveryState;
  readonly invalidation: ClientInvalidation<Stream>;
  /** True when continuity was lost and consumers must read authoritative snapshots. */
  readonly requiresSnapshot: boolean;
}

export function createClientRecoveryState(
  cursor: DeliveryCursor | null = null,
): ClientRecoveryState {
  if (cursor === null) return Object.freeze({ cursor: null });
  const transition = advanceDeliveryCursor(null, cursor);
  return Object.freeze({ cursor: transition.cursor });
}

function acceptCursor(
  current: DeliveryCursor | null,
  incoming: DeliveryCursor,
): { readonly cursor: DeliveryCursor; readonly epochChanged: boolean } {
  const transition = advanceDeliveryCursor(current, incoming);
  return {
    cursor: transition.cursor,
    epochChanged: transition.kind === "epoch-changed",
  };
}

/** Reduces transport-decoded delivery events into deterministic invalidation work. */
export function reduceClientRecovery<Stream>(
  state: ClientRecoveryState,
  event: ClientRecoveryEvent<Stream>,
): ClientRecoveryDecision<Stream> {
  if (event.kind === "update") {
    if (event.cursor === undefined) {
      return {
        state,
        invalidation: { kind: "streams", streams: [event.stream] },
        requiresSnapshot: false,
      };
    }
    const accepted = acceptCursor(state.cursor, event.cursor);
    return {
      state: createClientRecoveryState(accepted.cursor),
      invalidation: accepted.epochChanged
        ? { kind: "all" }
        : { kind: "streams", streams: [event.stream] },
      requiresSnapshot: accepted.epochChanged,
    };
  }

  if (event.snapshotRequired === true) {
    return {
      state: createClientRecoveryState(event.latestCursor ?? null),
      invalidation: { kind: "all" },
      requiresSnapshot: true,
    };
  }

  let cursor = state.cursor;
  let epochChanged = false;
  const streams = new Set<Stream>();
  for (const entry of event.entries) {
    streams.add(entry.stream);
    if (entry.cursor !== undefined) {
      const accepted = acceptCursor(cursor, entry.cursor);
      cursor = accepted.cursor;
      epochChanged ||= accepted.epochChanged;
    }
  }
  if (event.latestCursor !== undefined) {
    const accepted = acceptCursor(cursor, event.latestCursor);
    cursor = accepted.cursor;
    epochChanged ||= accepted.epochChanged;
  }

  return {
    state: createClientRecoveryState(cursor),
    invalidation: epochChanged
      ? { kind: "all" }
      : streams.size === 0
        ? { kind: "none" }
        : { kind: "streams", streams: [...streams] },
    requiresSnapshot: epochChanged,
  };
}
