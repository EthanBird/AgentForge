use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{DeterministicUuidV7, FixedClock};

/// One event captured by an [`EventRecorder`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecordedEvent<T> {
    /// One-based order within this recorder.
    pub sequence: u64,
    /// Deterministic UUIDv7 event identifier.
    pub event_id: Uuid,
    /// Virtual server time, expressed without platform-dependent precision.
    pub occurred_at_unix_ms: i64,
    /// Caller-defined event payload.
    pub payload: T,
}

/// An append-only recorder driven entirely by deterministic test primitives.
#[derive(Clone, Debug)]
pub struct EventRecorder<T> {
    clock: FixedClock,
    ids: DeterministicUuidV7,
    events: Vec<RecordedEvent<T>>,
}

impl<T> EventRecorder<T> {
    /// Creates an empty recorder.
    #[must_use]
    pub fn new(clock: FixedClock, seed: u64) -> Self {
        Self {
            ids: DeterministicUuidV7::new(clock.clone(), seed),
            clock,
            events: Vec::new(),
        }
    }

    /// Appends a payload and returns the complete recorded envelope.
    pub fn record(&mut self, payload: T) -> &RecordedEvent<T> {
        let sequence = u64::try_from(self.events.len())
            .expect("event count must fit in u64")
            .saturating_add(1);
        self.events.push(RecordedEvent {
            sequence,
            event_id: self.ids.next_uuid(),
            occurred_at_unix_ms: self.clock.unix_timestamp_millis(),
            payload,
        });
        self.events.last().expect("event was just appended")
    }

    /// Returns all recorded events in append order.
    #[must_use]
    pub fn events(&self) -> &[RecordedEvent<T>] {
        &self.events
    }

    /// Consumes the recorder and returns all events.
    #[must_use]
    pub fn into_events(self) -> Vec<RecordedEvent<T>> {
        self.events
    }
}

impl<T: Serialize> EventRecorder<T> {
    /// Serializes the event sequence without any runtime duration fields.
    pub fn deterministic_json(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(&self.events)
    }
}
