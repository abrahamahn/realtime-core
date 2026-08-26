export interface SubscriptionStats {
  readonly streams: number;
  readonly subscriptions: number;
  readonly connections: number;
}

export class SubscriptionRegistry<Stream, Connection extends object> {
  readonly #byStream = new Map<Stream, Set<Connection>>();
  readonly #byConnection = new Map<Connection, Set<Stream>>();

  subscribe(stream: Stream, connection: Connection): boolean {
    let connections = this.#byStream.get(stream);
    if (connections === undefined) {
      connections = new Set();
      this.#byStream.set(stream, connections);
    }
    const added = !connections.has(connection);
    connections.add(connection);
    let streams = this.#byConnection.get(connection);
    if (streams === undefined) {
      streams = new Set();
      this.#byConnection.set(connection, streams);
    }
    streams.add(stream);
    return added;
  }

  unsubscribe(stream: Stream, connection: Connection): boolean {
    const connections = this.#byStream.get(stream);
    const removed = connections?.delete(connection) ?? false;
    if (connections?.size === 0) this.#byStream.delete(stream);
    const streams = this.#byConnection.get(connection);
    streams?.delete(stream);
    if (streams?.size === 0) this.#byConnection.delete(connection);
    return removed;
  }

  removeConnection(connection: Connection): number {
    const streams = this.#byConnection.get(connection);
    if (streams === undefined) return 0;
    const count = streams.size;
    for (const stream of streams) {
      const connections = this.#byStream.get(stream);
      connections?.delete(connection);
      if (connections?.size === 0) this.#byStream.delete(stream);
    }
    this.#byConnection.delete(connection);
    return count;
  }

  subscribers(stream: Stream): readonly Connection[] {
    return [...(this.#byStream.get(stream) ?? [])];
  }

  subscriberCount(stream: Stream): number {
    return this.#byStream.get(stream)?.size ?? 0;
  }

  streamCount(): number {
    return this.#byStream.size;
  }

  stats(): SubscriptionStats {
    let subscriptions = 0;
    for (const connections of this.#byStream.values()) subscriptions += connections.size;
    return {
      streams: this.#byStream.size,
      subscriptions,
      connections: this.#byConnection.size,
    };
  }
}
