use std::collections::HashSet;

use realtime_core::{
    CommandEnvelope, CommandReceipt, DeliveryLog, DeliveryRecovery, MAX_DELIVERY_SEQUENCE,
    RealtimeError, ReconnectBackoffPolicy, SubscriptionRegistry, reconnect_delay_ms,
};

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
    let mut log = DeliveryLog::with_initial_sequence(2, 10).unwrap();
    log.append("a", 1, "a1").unwrap();
    log.append("b", 1, "b1").unwrap();
    log.append("a", 2, "a2").unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(
        log.entries()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![12, 13]
    );

    let streams = HashSet::from(["a"]);
    assert_eq!(
        log.recover_after(11, Some(&streams)),
        DeliveryRecovery::Replay {
            latest_sequence: 13,
            entries: vec![realtime_core::DeliveryEntry {
                sequence: 13,
                stream: "a",
                stream_version: 2,
                payload: "a2",
            }],
        }
    );
}

#[test]
fn evicted_future_and_foreign_epoch_cursors_require_snapshots() {
    let mut log = DeliveryLog::new(2).unwrap();
    log.append("room", 1, "one").unwrap();
    log.append("room", 2, "two").unwrap();
    log.append("room", 3, "three").unwrap();

    assert_eq!(
        log.recover_after(0, None),
        DeliveryRecovery::SnapshotRequired {
            latest_sequence: 3,
            earliest_available_sequence: 2,
        }
    );
    assert!(matches!(
        log.recover_after(9, None),
        DeliveryRecovery::SnapshotRequired { .. }
    ));

    let restored = DeliveryLog::<&str, &str>::with_initial_sequence(2, 40).unwrap();
    assert!(matches!(
        restored.recover_after(39, None),
        DeliveryRecovery::SnapshotRequired { .. }
    ));
    assert!(matches!(
        restored.recover_after(40, None),
        DeliveryRecovery::Replay { entries, .. } if entries.is_empty()
    ));
}

#[test]
fn sequence_exhaustion_fails_before_mutation() {
    let mut log = DeliveryLog::with_initial_sequence(2, MAX_DELIVERY_SEQUENCE).unwrap();
    assert_eq!(
        log.append("stream", 1, "payload"),
        Err(RealtimeError::SequenceExhausted)
    );
    assert!(log.is_empty());
    assert_eq!(log.latest_sequence(), MAX_DELIVERY_SEQUENCE);
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
