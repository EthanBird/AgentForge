#[cfg(target_os = "linux")]
fn main() {
    use std::{env, os::unix::process::CommandExt, process::Command};

    let mut arguments = env::args_os().skip(1);
    let Some(executable) = arguments.next() else {
        eprintln!("af-hermetic-runner: missing fixture executable");
        std::process::exit(126);
    };

    if let Err(error) = agentforge_test_support::install_network_deny_filter() {
        eprintln!("af-hermetic-runner: could not install seccomp policy: {error}");
        std::process::exit(126);
    }

    let error = Command::new(executable).args(arguments).exec();
    eprintln!("af-hermetic-runner: fixture exec failed: {error}");
    std::process::exit(126);
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("af-hermetic-runner: seccomp network isolation requires Linux");
    std::process::exit(126);
}
