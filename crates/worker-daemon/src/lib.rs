//! Linux Worker daemon composition root. Runtime execution arrives in M2.

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-worker-daemon";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
