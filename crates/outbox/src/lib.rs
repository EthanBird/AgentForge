//! Transactional outbox ports. PostgreSQL delivery arrives in M1.

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-outbox";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
