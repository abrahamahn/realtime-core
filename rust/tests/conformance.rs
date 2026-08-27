use std::collections::HashSet;

use realtime_core::{DeliveryCursor, DeliveryLog, DeliveryRecovery, SnapshotRequiredReason};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecoveryEntry {
    stream: String,
    stream_version: u64,
    payload: String,
}

#[derive(Deserialize)]
struct Cursor {
    epoch: String,
    sequence: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RecoveryCase {
    name: String,
    epoch: String,
    max_entries: usize,
    initial_sequence: u64,
    entries: Vec<RecoveryEntry>,
    after: Cursor,
    authorized_streams: Vec<String>,
    expected: Value,
}

#[derive(Deserialize)]
struct RecoveryFixture {
    profile: String,
    cases: Vec<RecoveryCase>,
}

fn normalized_recovery(recovery: DeliveryRecovery<String, String>) -> Value {
    match recovery {
        DeliveryRecovery::Replay {
            latest_cursor,
            entries,
        } => json!({
            "kind": "replay",
            "latestCursor": {
                "epoch": latest_cursor.epoch,
                "sequence": latest_cursor.sequence,
            },
            "entries": entries.into_iter().map(|entry| json!({
                "cursor": {
                    "epoch": entry.cursor.epoch,
                    "sequence": entry.cursor.sequence,
                },
                "stream": entry.stream,
                "streamVersion": entry.stream_version,
                "payload": entry.payload,
            })).collect::<Vec<_>>(),
        }),
        DeliveryRecovery::SnapshotRequired {
            reason,
            latest_cursor,
            earliest_available_sequence,
        } => json!({
            "kind": "snapshot-required",
            "reason": match reason {
                SnapshotRequiredReason::EpochMismatch => "epoch-mismatch",
                SnapshotRequiredReason::HistoryGap => "history-gap",
                SnapshotRequiredReason::FutureCursor => "future-cursor",
            },
            "latestCursor": {
                "epoch": latest_cursor.epoch,
                "sequence": latest_cursor.sequence,
            },
            "earliestAvailableSequence": earliest_available_sequence,
        }),
    }
}

#[test]
fn recovery_matches_the_cross_language_conformance_corpus() {
    let fixture: RecoveryFixture =
        serde_json::from_str(include_str!("../fixtures/recovery-v1.json")).unwrap();
    assert_eq!(fixture.profile, "realtime-core-recovery-v1");
    for vector in fixture.cases {
        let mut log = DeliveryLog::with_initial_sequence(
            vector.epoch,
            vector.max_entries,
            vector.initial_sequence,
        )
        .unwrap();
        for entry in vector.entries {
            log.append(entry.stream, entry.stream_version, entry.payload)
                .unwrap();
        }
        let authorized = vector
            .authorized_streams
            .into_iter()
            .collect::<HashSet<_>>();
        let after = DeliveryCursor::new(vector.after.epoch, vector.after.sequence).unwrap();
        assert_eq!(
            normalized_recovery(log.recover_after(&after, Some(&authorized))),
            vector.expected,
            "{}",
            vector.name
        );
    }
}
