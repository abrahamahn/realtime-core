import { describe, expect, it } from "vitest";

import {
  createClientRecoveryState,
  reduceClientRecovery,
} from "../src/index.js";

describe("client recovery reducer", () => {
  it("advances continuous updates and invalidates only their stream", () => {
    const decision = reduceClientRecovery(createClientRecoveryState(), {
      kind: "update",
      stream: "room:1",
      cursor: { epoch: "epoch-a", sequence: 2 },
    });
    expect(decision).toEqual({
      state: { cursor: { epoch: "epoch-a", sequence: 2 } },
      invalidation: { kind: "streams", streams: ["room:1"] },
      requiresSnapshot: false,
    });
  });

  it("fails closed across an epoch change", () => {
    const decision = reduceClientRecovery(
      createClientRecoveryState({ epoch: "epoch-a", sequence: 2 }),
      {
        kind: "update",
        stream: "room:1",
        cursor: { epoch: "epoch-b", sequence: 2 },
      },
    );
    expect(decision.invalidation).toEqual({ kind: "all" });
    expect(decision.requiresSnapshot).toBe(true);
    expect(decision.state.cursor).toEqual({ epoch: "epoch-b", sequence: 2 });
  });

  it("deduplicates recovered stream invalidations while advancing every cursor", () => {
    const decision = reduceClientRecovery(
      createClientRecoveryState({ epoch: "epoch-a", sequence: 1 }),
      {
        kind: "recovery",
        entries: [
          { stream: "room:1", cursor: { epoch: "epoch-a", sequence: 2 } },
          { stream: "room:2", cursor: { epoch: "epoch-a", sequence: 3 } },
          { stream: "room:1", cursor: { epoch: "epoch-a", sequence: 4 } },
        ],
        latestCursor: { epoch: "epoch-a", sequence: 4 },
      },
    );
    expect(decision.invalidation).toEqual({
      kind: "streams",
      streams: ["room:1", "room:2"],
    });
    expect(decision.state.cursor).toEqual({ epoch: "epoch-a", sequence: 4 });
    expect(decision.requiresSnapshot).toBe(false);
  });

  it("turns an explicit reset into authoritative snapshot work", () => {
    const decision = reduceClientRecovery(
      createClientRecoveryState({ epoch: "epoch-a", sequence: 8 }),
      {
        kind: "recovery",
        entries: [],
        latestCursor: { epoch: "epoch-b", sequence: 1 },
        snapshotRequired: true,
      },
    );
    expect(decision).toEqual({
      state: { cursor: { epoch: "epoch-b", sequence: 1 } },
      invalidation: { kind: "all" },
      requiresSnapshot: true,
    });
  });

  it("keeps compatibility updates without cursors transport-neutral", () => {
    const state = createClientRecoveryState({ epoch: "epoch-a", sequence: 3 });
    const decision = reduceClientRecovery(state, {
      kind: "update",
      stream: "legacy",
    });
    expect(decision.state).toBe(state);
    expect(decision.invalidation).toEqual({
      kind: "streams",
      streams: ["legacy"],
    });
  });
});
