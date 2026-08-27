import { describe, expect, it } from "vitest";

import {
  createReconnectState,
  ensureMinimumReconnectAttempt,
  markReconnectConnecting,
  markReconnectOpen,
  markReconnectStable,
  resetReconnectState,
  scheduleReconnectAttempt,
} from "../src/index.js";

describe("reconnect state", () => {
  it("does not reset backoff merely because a transport opened", () => {
    const first = scheduleReconnectAttempt(createReconnectState(), {
      baseMs: 1_000,
      maxMs: 15_000,
    });
    expect(first.delayMs).toBe(1_000);
    expect(first.state).toEqual({ attempt: 1, status: "waiting" });

    const open = markReconnectOpen(markReconnectConnecting(first.state));
    expect(open).toEqual({ attempt: 1, status: "open" });
    const second = scheduleReconnectAttempt(open, {
      baseMs: 1_000,
      maxMs: 15_000,
    });
    expect(second.delayMs).toBe(2_000);
  });

  it("resets only after stability and can enforce a retry floor", () => {
    const raised = ensureMinimumReconnectAttempt(createReconnectState(), 4);
    expect(
      scheduleReconnectAttempt(raised, { baseMs: 1_000, maxMs: 15_000 })
        .delayMs,
    ).toBe(15_000);
    expect(markReconnectStable(raised)).toEqual({
      attempt: 0,
      status: "stable",
    });
    expect(resetReconnectState()).toEqual({ attempt: 0, status: "idle" });
  });

  it("fails before overflowing its attempt counter", () => {
    const exhausted = ensureMinimumReconnectAttempt(
      createReconnectState(),
      Number.MAX_SAFE_INTEGER,
    );
    expect(() =>
      scheduleReconnectAttempt(exhausted, { baseMs: 1, maxMs: 2 }),
    ).toThrow(/exhausted/u);
  });
});
