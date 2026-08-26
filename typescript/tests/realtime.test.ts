import { describe, expect, it } from 'vitest';

import {
  DeliveryLog,
  MAX_DELIVERY_SEQUENCE,
  SubscriptionRegistry,
  advanceDeliveryCursor,
  isDeliveryCursor,
  reconnectDelayMs,
  type CommandEnvelope,
  type CommandReceipt,
  type DeltaEnvelope,
  type SnapshotEnvelope,
} from '../src/index.js';

describe('SubscriptionRegistry', () => {
  it('tracks both directions and removes a disconnected consumer atomically', () => {
    const registry = new SubscriptionRegistry<string, object>();
    const first = {};
    const second = {};
    registry.subscribe('table:1', first);
    registry.subscribe('table:1', first);
    registry.subscribe('table:1', second);
    registry.subscribe('table:2', first);
    expect(registry.stats()).toEqual({
      streams: 2,
      subscriptions: 3,
      connections: 2,
    });
    expect(registry.removeConnection(first)).toBe(2);
    expect(registry.subscribers('table:1')).toEqual([second]);
    expect(registry.stats()).toEqual({
      streams: 1,
      subscriptions: 1,
      connections: 1,
    });
    expect(registry.unsubscribe('table:1', second)).toBe(true);
    expect(registry.stats()).toEqual({
      streams: 0,
      subscriptions: 0,
      connections: 0,
    });
  });

  it('reports duplicate subscriptions and unknown removals without corrupting indexes', () => {
    const registry = new SubscriptionRegistry<string, object>();
    const connection = {};
    expect(registry.subscribe('room', connection)).toBe(true);
    expect(registry.subscribe('room', connection)).toBe(false);
    expect(registry.unsubscribe('missing', connection)).toBe(false);
    expect(registry.removeConnection({})).toBe(0);
    expect(registry.stats()).toEqual({
      streams: 1,
      subscriptions: 1,
      connections: 1,
    });
  });
});

describe('DeliveryLog', () => {
  it('replays ordered entries from a stable epoch and delivery sequence', () => {
    const log = new DeliveryLog<string, string>({
      epoch: 'epoch-a',
      maxEntries: 3,
    });
    log.append('a', 1, 'a1');
    log.append('b', 1, 'b1');
    log.append('a', 2, 'a2');
    expect(log.recoverAfter({ epoch: 'epoch-a', sequence: 1 })).toMatchObject({
      kind: 'replay',
      latestCursor: { epoch: 'epoch-a', sequence: 3 },
      entries: [
        {
          cursor: { epoch: 'epoch-a', sequence: 2 },
          stream: 'b',
          payload: 'b1',
        },
        {
          cursor: { epoch: 'epoch-a', sequence: 3 },
          stream: 'a',
          payload: 'a2',
        },
      ],
    });
  });

  it('requires a snapshot when the requested cursor was evicted', () => {
    const log = new DeliveryLog<string, string>({
      epoch: 'epoch-a',
      maxEntries: 2,
    });
    log.append('a', 1, 'a1');
    log.append('a', 2, 'a2');
    log.append('a', 3, 'a3');
    expect(log.recoverAfter({ epoch: 'epoch-a', sequence: 0 })).toEqual({
      kind: 'snapshot-required',
      reason: 'history-gap',
      latestCursor: { epoch: 'epoch-a', sequence: 3 },
      earliestAvailableSequence: 2,
    });
  });

  it('rejects a different epoch even when its numeric sequence is equal', () => {
    const log = new DeliveryLog<string, string>({
      epoch: 'epoch-b',
      maxEntries: 2,
    });
    log.append('room', 1, 'one');

    expect(log.recoverAfter({ epoch: 'epoch-a', sequence: 1 })).toEqual({
      kind: 'snapshot-required',
      reason: 'epoch-mismatch',
      latestCursor: { epoch: 'epoch-b', sequence: 1 },
      earliestAvailableSequence: 1,
    });
  });

  it('distinguishes a future cursor in the current epoch', () => {
    const log = new DeliveryLog<string, string>({
      epoch: 'epoch-a',
      maxEntries: 2,
    });
    expect(log.recoverAfter({ epoch: 'epoch-a', sequence: 9 })).toMatchObject({
      kind: 'snapshot-required',
      reason: 'future-cursor',
      latestCursor: { epoch: 'epoch-a', sequence: 0 },
    });
    log.append('room', 1, 'one');
    expect(log.recoverAfter({ epoch: 'epoch-a', sequence: 9 })).toMatchObject({
      kind: 'snapshot-required',
      reason: 'future-cursor',
      latestCursor: { epoch: 'epoch-a', sequence: 1 },
    });
  });

  it('retains exactly its configured capacity and filters explicit stream sets', () => {
    const log = new DeliveryLog<string, string>({
      epoch: 'epoch-a',
      maxEntries: 2,
      initialSequence: 10,
    });
    log.append('a', 1, 'a1');
    log.append('b', 1, 'b1');
    log.append('a', 2, 'a2');

    expect(log.entries().map((entry) => entry.cursor.sequence)).toEqual([12, 13]);
    expect(log.recoverAfter({ epoch: 'epoch-a', sequence: 11 }, new Set(['a']))).toEqual({
      kind: 'replay',
      latestCursor: { epoch: 'epoch-a', sequence: 13 },
      entries: [
        {
          cursor: { epoch: 'epoch-a', sequence: 13 },
          stream: 'a',
          streamVersion: 2,
          payload: 'a2',
        },
      ],
    });
    expect(log.recoverAfter({ epoch: 'epoch-a', sequence: 11 }, new Set())).toEqual({
      kind: 'replay',
      latestCursor: { epoch: 'epoch-a', sequence: 13 },
      entries: [],
    });
  });

  it('fails before mutating the log when the delivery sequence is exhausted', () => {
    const log = new DeliveryLog<string, string>({
      epoch: 'epoch-a',
      maxEntries: 2,
      initialSequence: MAX_DELIVERY_SEQUENCE,
    });
    expect(() => log.append('a', 1, 'payload')).toThrow(/sequence is exhausted/u);
    expect(log.latestCursor()).toEqual({
      epoch: 'epoch-a',
      sequence: MAX_DELIVERY_SEQUENCE,
    });
    expect(log.size()).toBe(0);
    expect(log.recoverAfter({ epoch: 'epoch-a', sequence: MAX_DELIVERY_SEQUENCE })).toEqual({
      kind: 'replay',
      latestCursor: { epoch: 'epoch-a', sequence: MAX_DELIVERY_SEQUENCE },
      entries: [],
    });
  });

  it('validates epoch, capacity, initial sequence, stream version, and cursors', () => {
    expect(() => new DeliveryLog({ epoch: '' })).toThrow(/epoch/u);
    expect(() => new DeliveryLog({ epoch: 'epoch-a', maxEntries: 0 })).toThrow(/maxEntries/u);
    expect(() => new DeliveryLog({ epoch: 'epoch-a', initialSequence: -1 })).toThrow(
      /initialSequence/u,
    );
    const log = new DeliveryLog<string, string>({ epoch: 'epoch-a' });
    expect(() => log.append('a', -1, 'payload')).toThrow(/streamVersion/u);
    expect(() => log.recoverAfter({ epoch: '', sequence: -1 })).toThrow(/after/u);
  });
});

describe('delivery cursor transitions', () => {
  it('initializes, advances, ignores stale delivery, and reports an epoch change', () => {
    const initialized = advanceDeliveryCursor(null, {
      epoch: 'epoch-a',
      sequence: 2,
    });
    expect(initialized).toEqual({
      kind: 'initialized',
      cursor: { epoch: 'epoch-a', sequence: 2 },
    });
    expect(
      advanceDeliveryCursor(initialized.cursor, {
        epoch: 'epoch-a',
        sequence: 3,
      }),
    ).toEqual({
      kind: 'advanced',
      cursor: { epoch: 'epoch-a', sequence: 3 },
    });
    expect(
      advanceDeliveryCursor(initialized.cursor, {
        epoch: 'epoch-a',
        sequence: 1,
      }),
    ).toEqual({
      kind: 'stale',
      cursor: { epoch: 'epoch-a', sequence: 2 },
    });
    expect(
      advanceDeliveryCursor(initialized.cursor, {
        epoch: 'epoch-b',
        sequence: 2,
      }),
    ).toEqual({
      kind: 'epoch-changed',
      cursor: { epoch: 'epoch-b', sequence: 2 },
    });
  });

  it('validates cursors received across serialization boundaries', () => {
    expect(isDeliveryCursor({ epoch: 'epoch-a', sequence: 0 })).toBe(true);
    expect(isDeliveryCursor({ epoch: '', sequence: 0 })).toBe(false);
    expect(isDeliveryCursor({ epoch: 'epoch-a', sequence: -1 })).toBe(false);
    expect(isDeliveryCursor({ epoch: 'epoch-a', sequence: 0.5 })).toBe(false);
  });
});

describe('reconnectDelayMs', () => {
  it('caps deterministic exponential backoff', () => {
    expect(reconnectDelayMs(0, { baseMs: 1_000, maxMs: 15_000 })).toBe(1_000);
    expect(reconnectDelayMs(10, { baseMs: 1_000, maxMs: 15_000 })).toBe(15_000);
    expect(
      reconnectDelayMs(Number.MAX_SAFE_INTEGER, {
        baseMs: 1_000,
        maxMs: 15_000,
      }),
    ).toBe(15_000);
    expect(reconnectDelayMs(5, { baseMs: 0, maxMs: 0 })).toBe(0);
  });

  it('rejects fractional, negative, and inverted backoff inputs', () => {
    expect(() => reconnectDelayMs(-1, { baseMs: 1, maxMs: 2 })).toThrow(/attempt/u);
    expect(() => reconnectDelayMs(1, { baseMs: 0.5, maxMs: 2 })).toThrow(/baseMs/u);
    expect(() => reconnectDelayMs(1, { baseMs: 3, maxMs: 2 })).toThrow(/maxMs/u);
  });
});

describe('transport-neutral envelopes', () => {
  it('allow applications to supply their own identities and payloads', () => {
    const snapshot: SnapshotEnvelope<string, { readonly score: number }> = {
      kind: 'snapshot',
      stream: 'match:1',
      cursor: { epoch: 'epoch-a', sequence: 4 },
      payload: { score: 7 },
    };
    const delta: DeltaEnvelope<string, string> = {
      kind: 'delta',
      stream: 'match:1',
      cursor: { epoch: 'epoch-a', sequence: 5 },
      payload: 'score-changed',
    };
    const command: CommandEnvelope<string, { readonly move: string }> = {
      commandId: 'command:1',
      expectedVersion: 5,
      payload: { move: 'north' },
    };
    const receipt: CommandReceipt<string, number> = {
      commandId: command.commandId,
      status: 'accepted',
      result: 6,
    };

    expect([snapshot.cursor.sequence, delta.cursor.sequence, receipt.result]).toEqual([4, 5, 6]);
  });
});
