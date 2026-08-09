//! Composition root, HTTP query adapters, and the thin Control Room.

pub mod access;
pub mod control_room;
pub mod ui;

/// Package name used by smoke tests.
pub const CRATE_NAME: &str = "agentforge-control-plane";

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        assert_eq!(super::CRATE_NAME, env!("CARGO_PKG_NAME"));
    }
}
