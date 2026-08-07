use std::{error::Error, fmt};

use uuid::{Builder, Uuid};

use crate::{DeterministicRng, FixedClock};

const MAX_SEQUENCE: u64 = (1_u64 << 56) - 1;

/// Error returned for an invalid deterministic UUIDv7 fixture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UuidGenerationError {
    /// UUIDv7 timestamps cannot predate the Unix epoch.
    BeforeUnixEpoch(i64),
    /// The per-generator 56-bit uniqueness sequence was exhausted.
    SequenceExhausted,
}

impl fmt::Display for UuidGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeUnixEpoch(millis) => {
                write!(formatter, "UUIDv7 timestamp predates Unix epoch: {millis}")
            }
            Self::SequenceExhausted => formatter.write_str("UUIDv7 test sequence exhausted"),
        }
    }
}

impl Error for UuidGenerationError {}

/// Repeatable UUIDv7 generation driven by a [`FixedClock`] and seed.
///
/// Seven counter bytes make IDs unique for the lifetime of a generator; the
/// remaining bytes come from the seeded RNG. UUID version and variant bits are
/// installed by the `uuid` crate's RFC 9562 builder.
#[derive(Clone, Debug)]
pub struct DeterministicUuidV7 {
    clock: FixedClock,
    counter_prefix: [u8; 3],
    sequence: u64,
    last_millis: u64,
}

impl DeterministicUuidV7 {
    /// Creates a generator at sequence zero.
    #[must_use]
    pub fn new(clock: FixedClock, seed: u64) -> Self {
        let mut rng = DeterministicRng::new(seed);
        let mut counter_prefix = [0_u8; 3];
        rng.fill_bytes(&mut counter_prefix);
        Self {
            clock,
            counter_prefix,
            sequence: 0,
            last_millis: 0,
        }
    }

    /// Produces the next deterministic UUIDv7.
    pub fn try_next(&mut self) -> Result<Uuid, UuidGenerationError> {
        let configured_millis = self.clock.unix_timestamp_millis();
        let configured_millis = u64::try_from(configured_millis)
            .map_err(|_| UuidGenerationError::BeforeUnixEpoch(configured_millis))?;
        let unix_millis = configured_millis.max(self.last_millis);

        if self.sequence > MAX_SEQUENCE {
            return Err(UuidGenerationError::SequenceExhausted);
        }

        let mut counter_random = [0_u8; 10];
        counter_random[..3].copy_from_slice(&self.counter_prefix);
        counter_random[3..].copy_from_slice(&self.sequence.to_be_bytes()[1..]);

        let id = Builder::from_unix_timestamp_millis(unix_millis, &counter_random).into_uuid();
        self.sequence = self.sequence.saturating_add(1);
        self.last_millis = unix_millis;
        Ok(id)
    }

    /// Produces the next deterministic UUIDv7.
    ///
    /// # Panics
    ///
    /// Panics when the fixed clock predates the Unix epoch or the generator's
    /// sequence is exhausted. Both are fixture programming errors.
    pub fn next_uuid(&mut self) -> Uuid {
        self.try_next()
            .expect("deterministic UUIDv7 fixture must be valid")
    }

    /// Returns the next sequence number that will be embedded in an ID.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}
