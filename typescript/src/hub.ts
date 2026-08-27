import {
  DeliveryLog,
  type DeliveryCursor,
  type DeliveryEntry,
  type DeliveryRecovery,
} from './recovery.js';
import { SubscriptionRegistry, type SubscriptionStats } from './subscriptions.js';

export interface SubscriptionHubOptions {
  /** Application-owned identity for this delivery-log lifetime. */
  readonly epoch: string;
  readonly maxEntries?: number;
  readonly initialSequence?: number;
}

/** Pure delivery work for a transport adapter to execute. */
export interface DeliveryPlan<Stream, Connection, Payload> {
  readonly entry: DeliveryEntry<Stream, Payload>;
  readonly connections: readonly Connection[];
}

/**
 * Composes subscription indexes and ordered recovery without performing I/O.
 *
 * A transport adapter remains responsible for authorization, serialization, sending, retries,
 * and pruning connections that fail during an actual send.
 */
export class SubscriptionHub<Stream, Connection extends object, Payload> {
  readonly #subscriptions = new SubscriptionRegistry<Stream, Connection>();
  readonly #deliveries: DeliveryLog<Stream, Payload>;

  constructor(options: SubscriptionHubOptions) {
    this.#deliveries = new DeliveryLog(options);
  }

  subscribe(stream: Stream, connection: Connection): boolean {
    return this.#subscriptions.subscribe(stream, connection);
  }

  unsubscribe(stream: Stream, connection: Connection): boolean {
    return this.#subscriptions.unsubscribe(stream, connection);
  }

  removeConnection(connection: Connection): number {
    return this.#subscriptions.removeConnection(connection);
  }

  planDelivery(
    stream: Stream,
    streamVersion: number,
    payload: Payload,
  ): DeliveryPlan<Stream, Connection, Payload> {
    return {
      entry: this.#deliveries.append(stream, streamVersion, payload),
      connections: this.#subscriptions.subscribers(stream),
    };
  }

  recoverAfter(
    cursor: DeliveryCursor,
    authorizedStreams: ReadonlySet<Stream>,
  ): DeliveryRecovery<Stream, Payload> {
    return this.#deliveries.recoverAfter(cursor, authorizedStreams);
  }

  entries(): readonly DeliveryEntry<Stream, Payload>[] {
    return this.#deliveries.entries();
  }

  retainedStreams(): ReadonlySet<Stream> {
    return new Set(this.#deliveries.entries().map((entry) => entry.stream));
  }

  historySize(): number {
    return this.#deliveries.size();
  }

  latestCursor(): DeliveryCursor {
    return this.#deliveries.latestCursor();
  }

  subscriberCount(stream: Stream): number {
    return this.#subscriptions.subscriberCount(stream);
  }

  streamCount(): number {
    return this.#subscriptions.streamCount();
  }

  stats(): SubscriptionStats {
    return this.#subscriptions.stats();
  }
}

/**
 * Collapses an invalidation-style replay to the newest retained delivery for each stream.
 *
 * This helper is deliberately opt-in: applications whose payloads are non-idempotent deltas must
 * replay every entry instead.
 */
export function latestDeliveryPerStream<Stream, Payload>(
  entries: readonly DeliveryEntry<Stream, Payload>[],
): readonly DeliveryEntry<Stream, Payload>[] {
  const latest = new Map<Stream, DeliveryEntry<Stream, Payload>>();
  let epoch: string | undefined;
  for (const entry of entries) {
    epoch ??= entry.cursor.epoch;
    if (entry.cursor.epoch !== epoch) {
      throw new RangeError('deliveries from different epochs cannot be collapsed together');
    }
    const existing = latest.get(entry.stream);
    if (existing === undefined || entry.cursor.sequence > existing.cursor.sequence) {
      latest.set(entry.stream, entry);
    }
  }
  return [...latest.values()].sort((left, right) => left.cursor.sequence - right.cursor.sequence);
}
