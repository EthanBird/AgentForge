use std::sync::{Arc, Mutex, MutexGuard};

use time::{Duration, OffsetDateTime};

/// A manually advanced server clock shared by all clones.
///
/// The clock stores integer Unix milliseconds so serialized test output does
/// not depend on platform timer precision.
#[derive(Clone, Debug)]
pub struct FixedClock {
    unix_millis: Arc<Mutex<i64>>,
}

impl FixedClock {
    /// Creates a clock fixed at `unix_millis`.
    #[must_use]
    pub fn from_unix_timestamp_millis(unix_millis: i64) -> Self {
        Self {
            unix_millis: Arc::new(Mutex::new(unix_millis)),
        }
    }

    /// Returns the current virtual time as Unix milliseconds.
    #[must_use]
    pub fn unix_timestamp_millis(&self) -> i64 {
        *self.lock()
    }

    /// Returns the current virtual time.
    ///
    /// # Panics
    ///
    /// Panics when the configured timestamp is outside `time`'s supported
    /// range. Such a value is a test-fixture programming error.
    #[must_use]
    pub fn now(&self) -> OffsetDateTime {
        let nanos = i128::from(self.unix_timestamp_millis()) * 1_000_000;
        OffsetDateTime::from_unix_timestamp_nanos(nanos)
            .expect("fixed test clock must be within OffsetDateTime's range")
    }

    /// Moves the virtual clock by an exact number of milliseconds.
    ///
    /// # Panics
    ///
    /// Panics on integer overflow.
    pub fn advance_millis(&self, millis: i64) {
        let mut current = self.lock();
        *current = current
            .checked_add(millis)
            .expect("fixed test clock advance must not overflow");
    }

    /// Moves the virtual clock by a duration rounded toward zero to whole
    /// milliseconds.
    ///
    /// # Panics
    ///
    /// Panics if the duration does not fit in an `i64` millisecond count.
    pub fn advance(&self, duration: Duration) {
        let millis = i64::try_from(duration.whole_milliseconds())
            .expect("fixed test duration must fit in i64 milliseconds");
        self.advance_millis(millis);
    }

    /// Sets the virtual time explicitly.
    pub fn set_unix_timestamp_millis(&self, unix_millis: i64) {
        *self.lock() = unix_millis;
    }

    fn lock(&self) -> MutexGuard<'_, i64> {
        self.unix_millis
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for FixedClock {
    fn default() -> Self {
        // 2026-08-08T00:00:00Z
        Self::from_unix_timestamp_millis(1_754_611_200_000)
    }
}
