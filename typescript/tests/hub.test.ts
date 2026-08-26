import { describe, expect, it } from 'vitest';

import { SubscriptionHub, latestDeliveryPerStream } from '../src/index.js';

describe('SubscriptionHub', () => {
  it('plans ordered delivery without performing transport I/O', () => {
    const hub = new SubscriptionHub<string, object, string>({
      epoch: 'epoch-a',
      maxEntries: 4,
    });
    const first = {};
    const second = {};
    hub.subscribe('room:1', first);
    hub.subscribe('room:1', second);

    const plan = hub.planDelivery('room:1', 7, 'changed');
    expect(plan.entry).toEqual({
      cursor: { epoch: 'epoch-a', sequence: 1 },
      stream: 'room:1',
      streamVersion: 7,
      payload: 'changed',
    });
    expect(plan.connections).toEqual([first, second]);
    expect(hub.stats()).toEqual({ streams: 1, subscriptions: 2, connections: 2 });
  });

  it('records deliveries without subscribers and recovers only authorized streams', () => {
    const hub = new SubscriptionHub<string, object, string>({ epoch: 'epoch-a' });
    hub.planDelivery('private:1', 1, 'one');
    hub.planDelivery('public', 2, 'two');

    expect(hub.historySize()).toBe(2);
    expect(hub.retainedStreams()).toEqual(new Set(['private:1', 'public']));
    expect(
      hub.recoverAfter({ epoch: 'epoch-a', sequence: 0 }, new Set(['public'])),
    ).toMatchObject({
      kind: 'replay',
      entries: [{ stream: 'public', streamVersion: 2 }],
    });
  });

  it('removes every subscription for a disconnected connection', () => {
    const hub = new SubscriptionHub<string, object, null>({ epoch: 'epoch-a' });
    const connection = {};
    hub.subscribe('one', connection);
    hub.subscribe('two', connection);
    expect(hub.removeConnection(connection)).toBe(2);
    expect(hub.stats()).toEqual({ streams: 0, subscriptions: 0, connections: 0 });
  });
});

describe('latestDeliveryPerStream', () => {
  it('is an explicit invalidation optimization and preserves final delivery order', () => {
    const hub = new SubscriptionHub<string, object, null>({ epoch: 'epoch-a' });
    hub.planDelivery('a', 1, null);
    hub.planDelivery('b', 1, null);
    hub.planDelivery('a', 2, null);

    expect(
      latestDeliveryPerStream(hub.entries()).map((entry) => [
        entry.stream,
        entry.streamVersion,
        entry.cursor.sequence,
      ]),
    ).toEqual([
      ['b', 1, 2],
      ['a', 2, 3],
    ]);
  });
});
