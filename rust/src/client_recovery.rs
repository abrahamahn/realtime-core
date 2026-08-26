use crate::{DeliveryCursor, DeliveryCursorTransition, advance_delivery_cursor};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientRecoveryState {
    pub cursor: Option<DeliveryCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientRecoveryEntry<Stream> {
    pub stream: Stream,
    pub cursor: Option<DeliveryCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientRecoveryEvent<Stream> {
    Update {
        stream: Stream,
        cursor: Option<DeliveryCursor>,
    },
    Recovery {
        entries: Vec<ClientRecoveryEntry<Stream>>,
        latest_cursor: Option<DeliveryCursor>,
        snapshot_required: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientInvalidation<Stream> {
    None,
    Streams(Vec<Stream>),
    All,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientRecoveryDecision<Stream> {
    pub state: ClientRecoveryState,
    pub invalidation: ClientInvalidation<Stream>,
    pub requires_snapshot: bool,
}

fn accept_cursor(
    current: Option<&DeliveryCursor>,
    incoming: &DeliveryCursor,
) -> (DeliveryCursor, bool) {
    match advance_delivery_cursor(current, incoming) {
        DeliveryCursorTransition::Initialized(cursor)
        | DeliveryCursorTransition::Advanced(cursor)
        | DeliveryCursorTransition::Stale(cursor) => (cursor, false),
        DeliveryCursorTransition::EpochChanged(cursor) => (cursor, true),
    }
}

/// Reduces transport-decoded deliveries into deterministic invalidation work.
#[must_use]
pub fn reduce_client_recovery<Stream>(
    state: &ClientRecoveryState,
    event: &ClientRecoveryEvent<Stream>,
) -> ClientRecoveryDecision<Stream>
where
    Stream: Clone + Eq,
{
    match event {
        ClientRecoveryEvent::Update { stream, cursor } => {
            let Some(cursor) = cursor else {
                return ClientRecoveryDecision {
                    state: state.clone(),
                    invalidation: ClientInvalidation::Streams(vec![stream.clone()]),
                    requires_snapshot: false,
                };
            };
            let (cursor, epoch_changed) = accept_cursor(state.cursor.as_ref(), cursor);
            ClientRecoveryDecision {
                state: ClientRecoveryState {
                    cursor: Some(cursor),
                },
                invalidation: if epoch_changed {
                    ClientInvalidation::All
                } else {
                    ClientInvalidation::Streams(vec![stream.clone()])
                },
                requires_snapshot: epoch_changed,
            }
        }
        ClientRecoveryEvent::Recovery {
            entries,
            latest_cursor,
            snapshot_required,
        } => {
            if *snapshot_required {
                return ClientRecoveryDecision {
                    state: ClientRecoveryState {
                        cursor: latest_cursor.clone(),
                    },
                    invalidation: ClientInvalidation::All,
                    requires_snapshot: true,
                };
            }

            let mut cursor = state.cursor.clone();
            let mut epoch_changed = false;
            let mut streams = Vec::new();
            for entry in entries {
                if !streams.contains(&entry.stream) {
                    streams.push(entry.stream.clone());
                }
                if let Some(incoming) = &entry.cursor {
                    let accepted = accept_cursor(cursor.as_ref(), incoming);
                    cursor = Some(accepted.0);
                    epoch_changed |= accepted.1;
                }
            }
            if let Some(incoming) = latest_cursor {
                let accepted = accept_cursor(cursor.as_ref(), incoming);
                cursor = Some(accepted.0);
                epoch_changed |= accepted.1;
            }

            ClientRecoveryDecision {
                state: ClientRecoveryState { cursor },
                invalidation: if epoch_changed {
                    ClientInvalidation::All
                } else if streams.is_empty() {
                    ClientInvalidation::None
                } else {
                    ClientInvalidation::Streams(streams)
                },
                requires_snapshot: epoch_changed,
            }
        }
    }
}
