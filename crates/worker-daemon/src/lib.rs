//! Recoverable Linux Worker runtime for the AgentForge MVP.

pub mod journal;
pub mod runtime;
pub mod supervisor;

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-worker-daemon";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
