//! Composition root and command-line adapters.

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-control-plane";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
