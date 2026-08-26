use std::collections::HashSet;

use realtime_core::{
    ClientInvalidation, ClientRecoveryEntry, ClientRecoveryEvent, ClientRecoveryState,
    CommandEnvelope, CommandReceipt, DeliveryCursor, DeliveryCursorTransition, DeliveryLog,
    DeliveryRecovery, LivenessTracker, MAX_DELIVERY_SEQUENCE, RealtimeError,
    ReconnectBackoffPolicy, ReconnectState, ReconnectStatus, SnapshotRequiredReason,
    SubscriptionHub, SubscriptionRegistry, advance_delivery_cursor,
    ensure_minimum_reconnect_attempt, latest_delivery_per_stream, mark_reconnect_connecting,
    mark_reconnect_open, mark_reconnect_stable, reconnect_delay_ms, reduce_client_recovery,
    schedule_reconnect_attempt,
};

fn cursor(epoch: &str, sequence: u64) -> DeliveryCursor {
    DeliveryCursor::new(epoch, sequence).unwrap()
}

#[test]
fn subscription_indexes_stay_consistent_in_both_directions() {
    let mut registry = SubscriptionRegistry::new();
    assert!(registry.subscribe("table:1", "connection:1"));
    assert!(!registry.subscribe("table:1", "connection:1"));
    assert!(registry.subscribe("table:1", "connection:2"));
    assert!(registry.subscribe("table:2", "connection:1"));
    assert_eq!(registry.stats().streams, 2);
    assert_eq!(registry.stats().subscriptions, 3);
    assert_eq!(registry.remove_connection(&"connection:1"), 2);
    assert_eq!(registry.subscribers(&"table:1"), vec![&"connection:2"]);
    assert!(registry.unsubscribe(&"table:1", &"connection:2"));
    assert_eq!(registry.stats().connections, 0);
}

#[test]
fn delivery_replay_is_ordered_filtered_and_capacity_bounded() {
    let mut log = DeliveryLog::with_initial_sequence("epoch-a", 2, 10).unwrap();
    log.append("a", 1, "a1").unwrap();
    log.append("b", 1, "b1").unwrap();
    log.append("a", 2, "a2").unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(
        log.entries()
            .map(|entry| entry.cursor.sequence)
            .collect::<Vec<_>>(),
        vec![12, 13]
    );

    let streams = HashSet::from(["a"]);
    assert_eq!(
        log.recover_after(&cursor("epoch-a", 11), Some(&streams)),
        DeliveryRecovery::Replay {
            latest_cursor: cursor("epoch-a", 13),
            entries: vec![realtime_core::DeliveryEntry {
                cursor: cursor("epoch-a", 13),
                stream: "a",
                stream_version: 2,
                payload: "a2",
            }],
        }
    );
}

#[test]
fn epoch_mismatch_is_detected_even_when_numeric_sequences_are_equal() {
    let mut log = DeliveryLog::new("epoch-b", 2).unwrap();
    log.append("room", 1, "one").unwrap();

    assert_eq!(
        log.recover_after(&cursor("epoch-a", 1), None),
        DeliveryRecovery::SnapshotRequired {
            reason: SnapshotRequiredReason::EpochMismatch,
            latest_cursor: cursor("epoch-b", 1),
            earliest_available_sequence: 1,
        }
    );
}

#[test]
fn evicted_and_future_cursors_require_snapshots_for_distinct_reasons() {
    let empty = DeliveryLog::<&str, &str>::new("epoch-a", 2).unwrap();
    assert!(matches!(
        empty.recover_after(&cursor("epoch-a", 9), None),
        DeliveryRecovery::SnapshotRequired {
            reason: SnapshotRequiredReason::FutureCursor,
            ..
        }
    ));

    let mut log = DeliveryLog::new("epoch-a", 2).unwrap();
    log.append("room", 1, "one").unwrap();
    log.append("room", 2, "two").unwrap();
    log.append("room", 3, "three").unwrap();

    assert_eq!(
        log.recover_after(&cursor("epoch-a", 0), None),
        DeliveryRecovery::SnapshotRequired {
            reason: SnapshotRequiredReason::HistoryGap,
            latest_cursor: cursor("epoch-a", 3),
            earliest_available_sequence: 2,
        }
    );
    assert!(matches!(
        log.recover_after(&cursor("epoch-a", 9), None),
        DeliveryRecovery::SnapshotRequired {
            reason: SnapshotRequiredReason::FutureCursor,
            ..
        }
    ));
}

#[test]
fn cursor_transitions_report_restart_boundaries() {
    assert_eq!(
        advance_delivery_cursor(None, &cursor("epoch-a", 2)),
        DeliveryCursorTransition::Initialized(cursor("epoch-a", 2))
    );
    assert_eq!(
        advance_delivery_cursor(Some(&cursor("epoch-a", 2)), &cursor("epoch-a", 3)),
        DeliveryCursorTransition::Advanced(cursor("epoch-a", 3))
    );
    assert_eq!(
        advance_delivery_cursor(Some(&cursor("epoch-a", 2)), &cursor("epoch-a", 1)),
        DeliveryCursorTransition::Stale(cursor("epoch-a", 2))
    );
    assert_eq!(
        advance_delivery_cursor(Some(&cursor("epoch-a", 2)), &cursor("epoch-b", 2)),
        DeliveryCursorTransition::EpochChanged(cursor("epoch-b", 2))
    );
}

#[test]
fn epoch_and_sequence_exhaustion_fail_before_mutation() {
    assert_eq!(
        DeliveryLog::<&str, &str>::new("", 2).unwrap_err(),
        RealtimeError::InvalidEpoch
    );
    assert_eq!(
        DeliveryCursor::new(" ", 0),
        Err(RealtimeError::InvalidEpoch)
    );
    let mut log = DeliveryLog::with_initial_sequence("epoch-a", 2, MAX_DELIVERY_SEQUENCE).unwrap();
    assert_eq!(
        log.append("stream", 1, "payload"),
        Err(RealtimeError::SequenceExhausted)
    );
    assert!(log.is_empty());
    assert_eq!(
        log.latest_cursor(),
        cursor("epoch-a", MAX_DELIVERY_SEQUENCE)
    );
}

#[test]
fn reconnect_backoff_is_deterministic_capped_and_checked() {
    let policy = ReconnectBackoffPolicy {
        base_ms: 1_000,
        max_ms: 15_000,
    };
    assert_eq!(reconnect_delay_ms(0, policy).unwrap(), 1_000);
    assert_eq!(reconnect_delay_ms(10, policy).unwrap(), 15_000);
    assert_eq!(reconnect_delay_ms(u32::MAX, policy).unwrap(), 15_000);
    assert_eq!(
        reconnect_delay_ms(
            1,
            ReconnectBackoffPolicy {
                base_ms: 2,
                max_ms: 1,
            }
        ),
        Err(RealtimeError::InvalidBackoffPolicy)
    );
}

#[test]
fn command_envelopes_remain_application_defined() {
    let command = CommandEnvelope {
        command_id: "command:1",
        expected_version: Some(4),
        payload: "move:north",
    };
    let receipt: CommandReceipt<_, _, &str> = CommandReceipt::Accepted {
        command_id: command.command_id,
        result: 5,
    };
    assert_eq!(
        receipt,
        CommandReceipt::Accepted {
            command_id: "command:1",
            result: 5,
        }
    );
}

#[test]
fn subscription_hub_plans_delivery_recovery_and_disconnects_without_io() {
    let mut hub = SubscriptionHub::new("epoch-a", 4).unwrap();
    assert!(hub.subscribe("room:1", "connection:1"));
    assert!(hub.subscribe("room:1", "connection:2"));
    let plan = hub.plan_delivery("room:1", 7, "changed").unwrap();
    assert_eq!(plan.entry.cursor, cursor("epoch-a", 1));
    assert_eq!(plan.connections, vec!["connection:1", "connection:2"]);
    assert_eq!(hub.stats().subscriptions, 2);

    hub.plan_delivery("room:2", 1, "second").unwrap();
    let authorized = HashSet::from(["room:2"]);
    assert!(matches!(
        hub.recover_after(&cursor("epoch-a", 0), Some(&authorized)),
        DeliveryRecovery::Replay { entries, .. }
            if entries.len() == 1 && entries[0].stream == "room:2"
    ));
    assert_eq!(hub.remove_connection(&"connection:1"), 1);
}

#[test]
fn invalidation_replay_collapse_is_explicit_and_ordered_by_final_delivery() {
    let mut log = DeliveryLog::new("epoch-a", 4).unwrap();
    let entries = vec![
        log.append("a", 1, ()).unwrap(),
        log.append("b", 1, ()).unwrap(),
        log.append("a", 2, ()).unwrap(),
    ];
    let latest = latest_delivery_per_stream(entries);
    assert_eq!(
        latest
            .iter()
            .map(|entry| (entry.stream, entry.stream_version, entry.cursor.sequence))
            .collect::<Vec<_>>(),
        vec![("b", 1, 2), ("a", 2, 3)]
    );
}

#[test]
fn client_recovery_reduces_continuity_and_snapshot_boundaries() {
    let initial = ClientRecoveryState::default();
    let continuous = reduce_client_recovery(
        &initial,
        &ClientRecoveryEvent::Update {
            stream: "room:1",
            cursor: Some(cursor("epoch-a", 2)),
        },
    );
    assert_eq!(
        continuous.invalidation,
        ClientInvalidation::Streams(vec!["room:1"])
    );
    assert!(!continuous.requires_snapshot);

    let changed = reduce_client_recovery(
        &continuous.state,
        &ClientRecoveryEvent::Update {
            stream: "room:1",
            cursor: Some(cursor("epoch-b", 2)),
        },
    );
    assert_eq!(changed.invalidation, ClientInvalidation::All);
    assert!(changed.requires_snapshot);

    let reset = reduce_client_recovery(
        &changed.state,
        &ClientRecoveryEvent::<&str>::Recovery {
            entries: Vec::new(),
            latest_cursor: Some(cursor("epoch-c", 1)),
            snapshot_required: true,
        },
    );
    assert_eq!(reset.invalidation, ClientInvalidation::All);
    assert_eq!(reset.state.cursor, Some(cursor("epoch-c", 1)));
}

#[test]
fn client_recovery_deduplicates_stream_invalidations_and_advances_all_cursors() {
    let decision = reduce_client_recovery(
        &ClientRecoveryState {
            cursor: Some(cursor("epoch-a", 1)),
        },
        &ClientRecoveryEvent::Recovery {
            entries: vec![
                ClientRecoveryEntry {
                    stream: "one",
                    cursor: Some(cursor("epoch-a", 2)),
                },
                ClientRecoveryEntry {
                    stream: "two",
                    cursor: Some(cursor("epoch-a", 3)),
                },
                ClientRecoveryEntry {
                    stream: "one",
                    cursor: Some(cursor("epoch-a", 4)),
                },
            ],
            latest_cursor: Some(cursor("epoch-a", 4)),
            snapshot_required: false,
        },
    );
    assert_eq!(
        decision.invalidation,
        ClientInvalidation::Streams(vec!["one", "two"])
    );
    assert_eq!(decision.state.cursor, Some(cursor("epoch-a", 4)));
}

#[test]
fn reconnect_state_resets_only_after_stability_and_never_wraps() {
    let policy = ReconnectBackoffPolicy {
        base_ms: 1_000,
        max_ms: 15_000,
    };
    let first = schedule_reconnect_attempt(ReconnectState::default(), policy).unwrap();
    assert_eq!(first.delay_ms, 1_000);
    let open = mark_reconnect_open(mark_reconnect_connecting(first.state));
    assert_eq!(open.attempt, 1);
    assert_eq!(
        schedule_reconnect_attempt(open, policy).unwrap().delay_ms,
        2_000
    );
    assert_eq!(
        mark_reconnect_stable(open),
        ReconnectState {
            attempt: 0,
            status: ReconnectStatus::Stable,
        }
    );
    let quarantined = ensure_minimum_reconnect_attempt(ReconnectState::default(), 4);
    assert_eq!(
        schedule_reconnect_attempt(quarantined, policy)
            .unwrap()
            .delay_ms,
        15_000
    );
    assert_eq!(
        schedule_reconnect_attempt(
            ReconnectState {
                attempt: u32::MAX,
                status: ReconnectStatus::Waiting,
            },
            policy,
        ),
        Err(RealtimeError::ReconnectAttemptExhausted)
    );
}

#[test]
fn liveness_tracker_probes_acknowledged_connections_then_reaps_stale_ones() {
    let mut tracker = LivenessTracker::new();
    assert!(tracker.track("connection:1"));
    assert!(!tracker.track("connection:1"));
    let first = tracker.sweep();
    assert_eq!(first.probe, vec!["connection:1"]);
    assert!(first.stale.is_empty());
    assert!(tracker.acknowledge(&"connection:1"));
    assert_eq!(tracker.sweep().probe, vec!["connection:1"]);
    assert_eq!(tracker.sweep().stale, vec!["connection:1"]);
    assert!(tracker.is_empty());
}
