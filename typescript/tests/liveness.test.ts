import { describe, expect, it } from 'vitest';

import { LivenessTracker } from '../src/index.js';

describe('LivenessTracker', () => {
  it('probes an acknowledged connection before declaring it stale', () => {
    const tracker = new LivenessTracker<object>();
    const connection = {};
    expect(tracker.track(connection)).toBe(true);
    expect(tracker.track(connection)).toBe(false);
    expect(tracker.sweep()).toEqual({ probe: [connection], stale: [] });
    expect(tracker.sweep()).toEqual({ probe: [], stale: [connection] });
    expect(tracker.size()).toBe(0);
  });

  it('re-arms acknowledged connections and removes closed ones', () => {
    const tracker = new LivenessTracker<object>();
    const first = {};
    const second = {};
    tracker.track(first);
    tracker.track(second);
    tracker.sweep();
    expect(tracker.acknowledge(first)).toBe(true);
    expect(tracker.remove(second)).toBe(true);
    expect(tracker.sweep()).toEqual({ probe: [first], stale: [] });
    expect(tracker.clear()).toBe(1);
    expect(tracker.acknowledge(first)).toBe(false);
  });
});
