//! Narrow A2A boundary adapters. The MVP adapter is feature-gated later.

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-protocol-a2a";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
