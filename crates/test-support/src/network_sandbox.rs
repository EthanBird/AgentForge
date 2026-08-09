use std::{convert::TryInto, io};

use seccompiler::{TargetArch, apply_filter, compile_from_json};

const FILTER_NAME: &str = "agentforge_network_deny";

// Deny creation, connection, addressing, data transfer, and configuration for
// every socket family. `errno: 1` is EPERM, giving fixtures a deterministic
// PermissionDenied result instead of a signal.
const NETWORK_DENY_FILTER: &str = r#"
{
  "agentforge_network_deny": {
    "mismatch_action": "allow",
    "match_action": { "errno": 1 },
    "filter": [
      { "syscall": "socket" },
      { "syscall": "socketpair" },
      { "syscall": "connect" },
      { "syscall": "bind" },
      { "syscall": "listen" },
      { "syscall": "accept" },
      { "syscall": "accept4" },
      { "syscall": "getsockname" },
      { "syscall": "getpeername" },
      { "syscall": "sendto" },
      { "syscall": "recvfrom" },
      { "syscall": "sendmsg" },
      { "syscall": "recvmsg" },
      { "syscall": "shutdown" },
      { "syscall": "setsockopt" },
      { "syscall": "getsockopt" },
      { "syscall": "recvmmsg" },
      { "syscall": "sendmmsg" }
    ]
  }
}
"#;

/// Installs an irreversible seccomp-BPF network deny filter on this thread.
///
/// The hermetic runner calls this immediately before `exec`; the filter is
/// inherited by the fixture process and all threads it later creates. Only the
/// safe `seccompiler` API is used by AgentForge code.
pub fn install_network_deny_filter() -> io::Result<()> {
    let architecture: TargetArch = std::env::consts::ARCH.try_into().map_err(|error| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!("unsupported seccomp architecture: {error}"),
        )
    })?;
    let filters = compile_from_json(NETWORK_DENY_FILTER.as_bytes(), architecture)
        .map_err(io::Error::other)?;
    let program = filters.get(FILTER_NAME).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "compiled seccomp policy omitted the network deny filter",
        )
    })?;
    apply_filter(program).map_err(io::Error::other)
}
