export interface LivenessSweep<Connection> {
  /** Connections that were alive and should receive the next probe. */
  readonly probe: readonly Connection[];
  /** Connections that did not acknowledge the previous probe. */
  readonly stale: readonly Connection[];
}

/** Pure acknowledgement state for heartbeat adapters. */
export class LivenessTracker<Connection extends object> {
  readonly #alive = new Map<Connection, boolean>();

  track(connection: Connection): boolean {
    if (this.#alive.has(connection)) return false;
    this.#alive.set(connection, true);
    return true;
  }

  acknowledge(connection: Connection): boolean {
    if (!this.#alive.has(connection)) return false;
    this.#alive.set(connection, true);
    return true;
  }

  remove(connection: Connection): boolean {
    return this.#alive.delete(connection);
  }

  sweep(): LivenessSweep<Connection> {
    const probe: Connection[] = [];
    const stale: Connection[] = [];
    for (const [connection, alive] of this.#alive) {
      if (alive) {
        probe.push(connection);
        this.#alive.set(connection, false);
      } else {
        stale.push(connection);
        this.#alive.delete(connection);
      }
    }
    return { probe, stale };
  }

  size(): number {
    return this.#alive.size;
  }

  clear(): number {
    const removed = this.#alive.size;
    this.#alive.clear();
    return removed;
  }
}
