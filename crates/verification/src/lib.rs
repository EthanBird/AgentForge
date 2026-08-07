//! Independent verification ports and policies. Runners arrive in M3.

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-verification";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
