import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { DeliveryLog, type DeliveryRecovery } from "../src/index.js";

interface RecoveryCase {
  readonly name: string;
  readonly epoch: string;
  readonly maxEntries: number;
  readonly initialSequence: number;
  readonly entries: readonly {
    readonly stream: string;
    readonly streamVersion: number;
    readonly payload: string;
  }[];
  readonly after: { readonly epoch: string; readonly sequence: number };
  readonly authorizedStreams: readonly string[];
  readonly expected: DeliveryRecovery<string, string>;
}

const fixture = JSON.parse(
  readFileSync(
    new URL("../../rust/fixtures/recovery-v1.json", import.meta.url),
    "utf8",
  ),
) as { readonly profile: string; readonly cases: readonly RecoveryCase[] };

describe("cross-language recovery conformance", () => {
  it("matches authorization, eviction, epoch, and future-cursor decisions", () => {
    expect(fixture.profile).toBe("realtime-core-recovery-v1");
    for (const vector of fixture.cases) {
      const log = new DeliveryLog<string, string>({
        epoch: vector.epoch,
        maxEntries: vector.maxEntries,
        initialSequence: vector.initialSequence,
      });
      for (const entry of vector.entries) {
        log.append(entry.stream, entry.streamVersion, entry.payload);
      }
      expect(
        log.recoverAfter(vector.after, new Set(vector.authorizedStreams)),
        vector.name,
      ).toEqual(vector.expected);
    }
  });
});
