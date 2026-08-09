//! Persistent supervision obligations. Runtime scheduling arrives in M5.

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-obligation-engine";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
