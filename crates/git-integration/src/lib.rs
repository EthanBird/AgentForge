//! Git relay and merge queue ports. Concrete integration arrives in M4.

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-git-integration";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
