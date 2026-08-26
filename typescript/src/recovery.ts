export interface DeliveryCursor {
  readonly epoch: string;
  readonly sequence: number;
}

export type DeliveryCursorTransition =
  | { readonly kind: 'initialized'; readonly cursor: DeliveryCursor }
  | { readonly kind: 'advanced'; readonly cursor: DeliveryCursor }
  | { readonly kind: 'stale'; readonly cursor: DeliveryCursor }
  | { readonly kind: 'epoch-changed'; readonly cursor: DeliveryCursor };

export interface DeliveryLogOptions {
  /** Application-supplied identity for this delivery-log lifetime. */
  readonly epoch: string;
  readonly maxEntries?: number;
  readonly initialSequence?: number;
}

export interface DeliveryEntry<Stream, Payload> {
  readonly cursor: DeliveryCursor;
  readonly stream: Stream;
  readonly streamVersion: number;
  readonly payload: Payload;
}

export type SnapshotRequiredReason = 'epoch-mismatch' | 'history-gap' | 'future-cursor';

export type DeliveryRecovery<Stream, Payload> =
  | {
      readonly kind: 'replay';
      readonly latestCursor: DeliveryCursor;
      readonly entries: readonly DeliveryEntry<Stream, Payload>[];
    }
  | {
      readonly kind: 'snapshot-required';
      readonly reason: SnapshotRequiredReason;
      readonly latestCursor: DeliveryCursor;
      readonly earliestAvailableSequence: number;
    };

export const MAX_DELIVERY_SEQUENCE = Number.MAX_SAFE_INTEGER;

function validateSequence(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer`);
  }
}

function freezeCursor(epoch: string, sequence: number): DeliveryCursor {
  return Object.freeze({ epoch, sequence });
}

export function isDeliveryCursor(value: unknown): value is DeliveryCursor {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const cursor = value as Record<string, unknown>;
  return (
    typeof cursor['epoch'] === 'string' &&
    cursor['epoch'].trim().length > 0 &&
    typeof cursor['sequence'] === 'number' &&
    Number.isSafeInteger(cursor['sequence']) &&
    cursor['sequence'] >= 0
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
    throw new RangeError('incoming must be a valid delivery cursor');
  }
  const frozenIncoming = freezeCursor(incoming.epoch, incoming.sequence);
  if (current == null) return { kind: 'initialized', cursor: frozenIncoming };
  if (!isDeliveryCursor(current)) {
    throw new RangeError('current must be a valid delivery cursor');
  }
  if (current.epoch !== incoming.epoch) {
    return { kind: 'epoch-changed', cursor: frozenIncoming };
  }
  if (incoming.sequence > current.sequence) {
    return { kind: 'advanced', cursor: frozenIncoming };
  }
  return {
    kind: 'stale',
    cursor: freezeCursor(current.epoch, current.sequence),
  };
}

export class DeliveryLog<Stream, Payload> {
  readonly #epoch: string;
  readonly #maxEntries: number;
  readonly #entries: DeliveryEntry<Stream, Payload>[] = [];
  #latestSequence: number;

  constructor(options: DeliveryLogOptions) {
    if (typeof options.epoch !== 'string' || options.epoch.trim().length === 0) {
      throw new RangeError('epoch must be a non-empty string');
    }
    const maxEntries = options.maxEntries ?? 1_000;
    const initialSequence = options.initialSequence ?? 0;
    if (!Number.isSafeInteger(maxEntries) || maxEntries <= 0) {
      throw new RangeError('maxEntries must be a positive safe integer');
    }
    validateSequence(initialSequence, 'initialSequence');
    this.#epoch = options.epoch;
    this.#maxEntries = maxEntries;
    this.#latestSequence = initialSequence;
  }

  append(stream: Stream, streamVersion: number, payload: Payload): DeliveryEntry<Stream, Payload> {
    validateSequence(streamVersion, 'streamVersion');
    if (this.#latestSequence === MAX_DELIVERY_SEQUENCE) {
      throw new RangeError('delivery sequence is exhausted');
    }
    this.#latestSequence += 1;
    const entry: DeliveryEntry<Stream, Payload> = Object.freeze({
      cursor: freezeCursor(this.#epoch, this.#latestSequence),
      stream,
      streamVersion,
      payload,
    });
    this.#entries.push(entry);
    const overflow = this.#entries.length - this.#maxEntries;
    if (overflow > 0) this.#entries.splice(0, overflow);
    return entry;
  }

  recoverAfter(
    after: DeliveryCursor,
    streams?: ReadonlySet<Stream>,
  ): DeliveryRecovery<Stream, Payload> {
    if (!isDeliveryCursor(after)) {
      throw new RangeError('after must be a valid delivery cursor');
    }
    const latestCursor = this.latestCursor();
    const firstRetained = this.#entries[0];
    const earliest =
      firstRetained?.cursor.sequence ?? Math.min(MAX_DELIVERY_SEQUENCE, this.#latestSequence + 1);
    if (after.epoch !== this.#epoch) {
      return {
        kind: 'snapshot-required',
        reason: 'epoch-mismatch',
        latestCursor,
        earliestAvailableSequence: earliest,
      };
    }
    const futureCursor = after.sequence > this.#latestSequence;
    const historyGap =
      !futureCursor &&
      (firstRetained === undefined
        ? after.sequence !== this.#latestSequence
        : after.sequence < firstRetained.cursor.sequence - 1);
    if (historyGap || futureCursor) {
      return {
        kind: 'snapshot-required',
        reason: futureCursor ? 'future-cursor' : 'history-gap',
        latestCursor,
        earliestAvailableSequence: earliest,
      };
    }
    const entries = this.#entries.filter(
      (entry) =>
        entry.cursor.sequence > after.sequence &&
        (streams === undefined || streams.has(entry.stream)),
    );
    return { kind: 'replay', latestCursor, entries };
  }

  entries(): readonly DeliveryEntry<Stream, Payload>[] {
    return [...this.#entries];
  }

  size(): number {
    return this.#entries.length;
  }

  latestCursor(): DeliveryCursor {
    return freezeCursor(this.#epoch, this.#latestSequence);
  }
}
