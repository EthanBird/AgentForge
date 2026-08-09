//! PostgreSQL adapters. Concrete storage arrives in M1.

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-storage-postgres";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
