import { MAX_INTEROPERABLE_INTEGER } from "./limits.js";

export interface DeliveryCursor {
  readonly epoch: string;
  readonly sequence: number;
}

export type DeliveryCursorTransition =
  | { readonly kind: "initialized"; readonly cursor: DeliveryCursor }
  | { readonly kind: "advanced"; readonly cursor: DeliveryCursor }
  | { readonly kind: "stale"; readonly cursor: DeliveryCursor }
  | { readonly kind: "epoch-changed"; readonly cursor: DeliveryCursor };

export const MAX_DELIVERY_SEQUENCE = MAX_INTEROPERABLE_INTEGER;

export function validateSequence(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer`);
  }
}

export function freezeCursor(epoch: string, sequence: number): DeliveryCursor {
  return Object.freeze({ epoch, sequence });
}

export function isDeliveryCursor(value: unknown): value is DeliveryCursor {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    return false;
  const cursor = value as Record<string, unknown>;
  return (
    typeof cursor["epoch"] === "string" &&
    cursor["epoch"].trim().length > 0 &&
    typeof cursor["sequence"] === "number" &&
    Number.isSafeInteger(cursor["sequence"]) &&
    cursor["sequence"] >= 0
  );
}

/**
 * Advances a consumer cursor and reports epoch changes explicitly.
 *
 * An epoch change means sequence ordering is no longer comparable and the consumer must refresh
 * its authoritative snapshot before treating subsequent deliveries as continuous.
 */
export function advanceDeliveryCursor(
  current: DeliveryCursor | null | undefined,
  incoming: DeliveryCursor,
): DeliveryCursorTransition {
  if (!isDeliveryCursor(incoming)) {
    throw new RangeError("incoming must be a valid delivery cursor");
  }
  const frozenIncoming = freezeCursor(incoming.epoch, incoming.sequence);
  if (current == null) return { kind: "initialized", cursor: frozenIncoming };
  if (!isDeliveryCursor(current)) {
    throw new RangeError("current must be a valid delivery cursor");
  }
  if (current.epoch !== incoming.epoch) {
    return { kind: "epoch-changed", cursor: frozenIncoming };
  }
  if (incoming.sequence > current.sequence) {
    return { kind: "advanced", cursor: frozenIncoming };
  }
  return {
    kind: "stale",
    cursor: freezeCursor(current.epoch, current.sequence),
  };
}
