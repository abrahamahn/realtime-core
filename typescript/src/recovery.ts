import {
  freezeCursor,
  isDeliveryCursor,
  MAX_DELIVERY_SEQUENCE,
  validateSequence,
  type DeliveryCursor,
} from "./cursor.js";

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

export type SnapshotRequiredReason =
  | "epoch-mismatch"
  | "history-gap"
  | "future-cursor";

export type DeliveryRecovery<Stream, Payload> =
  | {
      readonly kind: "replay";
      readonly latestCursor: DeliveryCursor;
      readonly entries: readonly DeliveryEntry<Stream, Payload>[];
    }
  | {
      readonly kind: "snapshot-required";
      readonly reason: SnapshotRequiredReason;
      readonly latestCursor: DeliveryCursor;
      readonly earliestAvailableSequence: number;
    };

export class DeliveryLog<Stream, Payload> {
  readonly #epoch: string;
  readonly #maxEntries: number;
  readonly #entries: DeliveryEntry<Stream, Payload>[] = [];
  #latestSequence: number;

  constructor(options: DeliveryLogOptions) {
    if (
      typeof options.epoch !== "string" ||
      options.epoch.trim().length === 0
    ) {
      throw new RangeError("epoch must be a non-empty string");
    }
    const maxEntries = options.maxEntries ?? 1_000;
    const initialSequence = options.initialSequence ?? 0;
    if (!Number.isSafeInteger(maxEntries) || maxEntries <= 0) {
      throw new RangeError("maxEntries must be a positive safe integer");
    }
    validateSequence(initialSequence, "initialSequence");
    this.#epoch = options.epoch;
    this.#maxEntries = maxEntries;
    this.#latestSequence = initialSequence;
  }

  append(
    stream: Stream,
    streamVersion: number,
    payload: Payload,
  ): DeliveryEntry<Stream, Payload> {
    validateSequence(streamVersion, "streamVersion");
    if (this.#latestSequence === MAX_DELIVERY_SEQUENCE) {
      throw new RangeError("delivery sequence is exhausted");
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
      throw new RangeError("after must be a valid delivery cursor");
    }
    const latestCursor = this.latestCursor();
    const firstRetained = this.#entries[0];
    const earliest =
      firstRetained?.cursor.sequence ??
      Math.min(MAX_DELIVERY_SEQUENCE, this.#latestSequence + 1);
    if (after.epoch !== this.#epoch) {
      return {
        kind: "snapshot-required",
        reason: "epoch-mismatch",
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
        kind: "snapshot-required",
        reason: futureCursor ? "future-cursor" : "history-gap",
        latestCursor,
        earliestAvailableSequence: earliest,
      };
    }
    const entries = this.#entries.filter(
      (entry) =>
        entry.cursor.sequence > after.sequence &&
        (streams === undefined || streams.has(entry.stream)),
    );
    return { kind: "replay", latestCursor, entries };
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
