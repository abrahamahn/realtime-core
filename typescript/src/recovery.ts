export interface DeliveryEntry<Stream, Payload> {
  readonly sequence: number;
  readonly stream: Stream;
  readonly streamVersion: number;
  readonly payload: Payload;
}

export type DeliveryRecovery<Stream, Payload> =
  | {
      readonly kind: 'replay';
      readonly latestSequence: number;
      readonly entries: readonly DeliveryEntry<Stream, Payload>[];
    }
  | {
      readonly kind: 'snapshot-required';
      readonly latestSequence: number;
      readonly earliestAvailableSequence: number;
    };

export const MAX_DELIVERY_SEQUENCE = Number.MAX_SAFE_INTEGER;

export class DeliveryLog<Stream, Payload> {
  readonly #maxEntries: number;
  readonly #entries: DeliveryEntry<Stream, Payload>[] = [];
  #latestSequence: number;

  constructor(maxEntries = 1_000, initialSequence = 0) {
    if (!Number.isSafeInteger(maxEntries) || maxEntries <= 0) {
      throw new RangeError('maxEntries must be a positive safe integer');
    }
    if (!Number.isSafeInteger(initialSequence) || initialSequence < 0) {
      throw new RangeError('initialSequence must be a non-negative safe integer');
    }
    this.#maxEntries = maxEntries;
    this.#latestSequence = initialSequence;
  }

  append(stream: Stream, streamVersion: number, payload: Payload): DeliveryEntry<Stream, Payload> {
    if (!Number.isSafeInteger(streamVersion) || streamVersion < 0) {
      throw new RangeError('streamVersion must be a non-negative safe integer');
    }
    if (this.#latestSequence === MAX_DELIVERY_SEQUENCE) {
      throw new RangeError('delivery sequence is exhausted');
    }
    this.#latestSequence += 1;
    const entry: DeliveryEntry<Stream, Payload> = Object.freeze({
      sequence: this.#latestSequence,
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
    afterSequence: number,
    streams?: ReadonlySet<Stream>,
  ): DeliveryRecovery<Stream, Payload> {
    if (!Number.isSafeInteger(afterSequence) || afterSequence < 0) {
      throw new RangeError('afterSequence must be a non-negative safe integer');
    }
    const firstRetained = this.#entries[0];
    const earliest = firstRetained?.sequence ?? Math.min(MAX_DELIVERY_SEQUENCE, this.#latestSequence + 1);
    const historyGap =
      firstRetained === undefined
        ? afterSequence !== this.#latestSequence
        : afterSequence < firstRetained.sequence - 1;
    if (historyGap || afterSequence > this.#latestSequence) {
      return {
        kind: 'snapshot-required',
        latestSequence: this.#latestSequence,
        earliestAvailableSequence: earliest,
      };
    }
    const entries = this.#entries.filter(
      (entry) =>
        entry.sequence > afterSequence &&
        (streams === undefined || streams.has(entry.stream)),
    );
    return { kind: 'replay', latestSequence: this.#latestSequence, entries };
  }

  entries(): readonly DeliveryEntry<Stream, Payload>[] {
    return [...this.#entries];
  }

  size(): number {
    return this.#entries.length;
  }

  latestSequence(): number {
    return this.#latestSequence;
  }
}
