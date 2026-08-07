#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

expected_packages="$(printf '%s\n' \
  agentforge-application \
  agentforge-control-plane \
  agentforge-domain \
  agentforge-git-integration \
  agentforge-matcher \
  agentforge-obligation-engine \
  agentforge-outbox \
  agentforge-protocol \
  agentforge-protocol-a2a \
  agentforge-storage-postgres \
  agentforge-test-support \
  agentforge-verification \
  agentforge-worker-daemon | sort)"

actual_packages="$(
  cargo metadata --locked --no-deps --format-version 1 |
    jq -r '.packages[].name' |
    sort
)"

if [[ "$actual_packages" != "$expected_packages" ]]; then
  diff -u <(printf '%s\n' "$expected_packages") <(printf '%s\n' "$actual_packages")
  exit 1
fi

required_paths=(
  Cargo.lock
  Cargo.toml
  rust-toolchain.toml
  crates/domain/src/lib.rs
  crates/protocol/src/lib.rs
  crates/test-support/src/lib.rs
)
for path in "${required_paths[@]}"; do
  [[ -f "$path" ]] || {
    echo "missing required path: $path" >&2
    exit 1
  }
done

forbidden_domain_dependencies='(^| )(axum|git2|jsonschema|reqwest|sqlx|tokio)( |$)'
if cargo tree --locked -p agentforge-domain --edges normal --prefix none |
  awk '{print $1}' |
  grep -Eq "$forbidden_domain_dependencies"; then
  echo "agentforge-domain contains a forbidden infrastructure dependency" >&2
  exit 1
fi

mapfile -t oversized < <(
  find . \
    -path './.git' -prune -o \
    -path './target' -prune -o \
    -type f -size +1M -print
)
if ((${#oversized[@]} > 0)); then
  printf 'undeclared file larger than 1 MiB: %s\n' "${oversized[@]}" >&2
  exit 1
fi

if find . \
  -path './.git' -prune -o \
  -path './target' -prune -o \
  -type f \( -name 'id_rsa' -o -name '*.pem' -o -name '*.p12' -o -name '*.pfx' \) \
  -print -quit | grep -q .; then
  echo "credential-like private key file found in repository" >&2
  exit 1
fi

echo "workspace contract passed (${actual_packages//$'\n'/, })"

