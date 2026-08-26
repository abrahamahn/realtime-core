import { describe, expect, it } from 'vitest';

import {
  DeliveryLog,
  MAX_DELIVERY_SEQUENCE,
  SubscriptionRegistry,
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
    expect(registry.stats()).toEqual({ streams: 1, subscriptions: 1, connections: 1 });
    expect(registry.unsubscribe('table:1', second)).toBe(true);
    expect(registry.stats()).toEqual({ streams: 0, subscriptions: 0, connections: 0 });
  });

  it('reports duplicate subscriptions and unknown removals without corrupting indexes', () => {
    const registry = new SubscriptionRegistry<string, object>();
    const connection = {};
    expect(registry.subscribe('room', connection)).toBe(true);
    expect(registry.subscribe('room', connection)).toBe(false);
    expect(registry.unsubscribe('missing', connection)).toBe(false);
    expect(registry.removeConnection({})).toBe(0);
    expect(registry.stats()).toEqual({ streams: 1, subscriptions: 1, connections: 1 });
  });
});

describe('DeliveryLog', () => {
  it('replays ordered entries from a stable delivery sequence', () => {
    const log = new DeliveryLog<string, string>(3);
    log.append('a', 1, 'a1');
    log.append('b', 1, 'b1');
    log.append('a', 2, 'a2');
    expect(log.recoverAfter(1)).toMatchObject({
      kind: 'replay',
      latestSequence: 3,
      entries: [
        { sequence: 2, stream: 'b', payload: 'b1' },
        { sequence: 3, stream: 'a', payload: 'a2' },
      ],
    });
  });

  it('requires a snapshot when the requested cursor was evicted', () => {
    const log = new DeliveryLog<string, string>(2);
    log.append('a', 1, 'a1');
    log.append('a', 2, 'a2');
    log.append('a', 3, 'a3');
    expect(log.recoverAfter(0)).toEqual({
      kind: 'snapshot-required',
      latestSequence: 3,
      earliestAvailableSequence: 2,
    });
  });

  it('requires a snapshot when the cursor belongs to a newer log epoch', () => {
    const log = new DeliveryLog<string, string>(2);
    log.append('room', 1, 'one');

    expect(log.recoverAfter(9)).toMatchObject({
      kind: 'snapshot-required',
      latestSequence: 1,
    });
  });

  it('retains exactly its configured capacity and filters explicit stream sets', () => {
    const log = new DeliveryLog<string, string>(2, 10);
    log.append('a', 1, 'a1');
    log.append('b', 1, 'b1');
    log.append('a', 2, 'a2');

    expect(log.entries().map((entry) => entry.sequence)).toEqual([12, 13]);
    expect(log.recoverAfter(11, new Set(['a']))).toEqual({
      kind: 'replay',
      latestSequence: 13,
      entries: [{ sequence: 13, stream: 'a', streamVersion: 2, payload: 'a2' }],
    });
    expect(log.recoverAfter(11, new Set())).toEqual({
      kind: 'replay',
      latestSequence: 13,
      entries: [],
    });
  });

  it('fails before mutating the log when the delivery sequence is exhausted', () => {
    const log = new DeliveryLog<string, string>(2, MAX_DELIVERY_SEQUENCE);
    expect(() => log.append('a', 1, 'payload')).toThrow(/sequence is exhausted/u);
    expect(log.latestSequence()).toBe(MAX_DELIVERY_SEQUENCE);
    expect(log.size()).toBe(0);
    expect(log.recoverAfter(MAX_DELIVERY_SEQUENCE)).toEqual({
      kind: 'replay',
      latestSequence: MAX_DELIVERY_SEQUENCE,
      entries: [],
    });
  });

  it('validates capacity, initial sequence, stream version, and cursors', () => {
    expect(() => new DeliveryLog(0)).toThrow(/maxEntries/u);
    expect(() => new DeliveryLog(1, -1)).toThrow(/initialSequence/u);
    const log = new DeliveryLog<string, string>();
    expect(() => log.append('a', -1, 'payload')).toThrow(/streamVersion/u);
    expect(() => log.recoverAfter(-1)).toThrow(/afterSequence/u);
  });
});

describe('reconnectDelayMs', () => {
  it('caps deterministic exponential backoff', () => {
    expect(reconnectDelayMs(0, { baseMs: 1_000, maxMs: 15_000 })).toBe(1_000);
    expect(reconnectDelayMs(10, { baseMs: 1_000, maxMs: 15_000 })).toBe(15_000);
    expect(
      reconnectDelayMs(Number.MAX_SAFE_INTEGER, { baseMs: 1_000, maxMs: 15_000 }),
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
      sequence: 4,
      payload: { score: 7 },
    };
    const delta: DeltaEnvelope<string, string> = {
      kind: 'delta',
      stream: 'match:1',
      sequence: 5,
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

    expect([snapshot.sequence, delta.sequence, receipt.result]).toEqual([4, 5, 6]);
  });
});
