//! Capability matching and bounty scoring. Policies arrive in M5.

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-matcher";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
