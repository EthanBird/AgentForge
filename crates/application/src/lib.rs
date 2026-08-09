//! Application use cases and infrastructure ports.

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-application";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
