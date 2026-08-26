export interface SnapshotEnvelope<Stream, Payload> {
  readonly kind: 'snapshot';
  readonly stream: Stream;
  readonly sequence: number;
  readonly payload: Payload;
}

export interface DeltaEnvelope<Stream, Payload> {
  readonly kind: 'delta';
  readonly stream: Stream;
  readonly sequence: number;
  readonly payload: Payload;
}

export interface CommandEnvelope<Command, Payload> {
  readonly commandId: Command;
  readonly expectedVersion?: number;
  readonly payload: Payload;
}

export type CommandReceipt<Command, Result, Rejection = string> =
  | {
      readonly commandId: Command;
      readonly status: 'accepted';
      readonly result: Result;
    }
  | {
      readonly commandId: Command;
      readonly status: 'rejected';
      readonly rejection: Rejection;
    };
