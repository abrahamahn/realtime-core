#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotEnvelope<Stream, Payload> {
    pub stream: Stream,
    pub sequence: u64,
    pub payload: Payload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaEnvelope<Stream, Payload> {
    pub stream: Stream,
    pub sequence: u64,
    pub payload: Payload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandEnvelope<Command, Payload> {
    pub command_id: Command,
    pub expected_version: Option<u64>,
    pub payload: Payload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandReceipt<Command, Result, Rejection = String> {
    Accepted {
        command_id: Command,
        result: Result,
    },
    Rejected {
        command_id: Command,
        rejection: Rejection,
    },
}
