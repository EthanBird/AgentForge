#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="$repo_root/deploy/compose/postgres.yml"

usage() {
  echo "usage: $0 up|wait|down" >&2
  exit 64
}

command_name="${1:-}"
[[ "$command_name" =~ ^(up|wait|down)$ ]] || usage

: "${AGENTFORGE_FIXTURE_ID:?set a unique fixture id}"
: "${AGENTFORGE_POSTGRES_PORT:?set a free local port}"

runtime_root="${TMPDIR:-/tmp}/agentforge-postgres-${AGENTFORGE_FIXTURE_ID}"
password_file="$runtime_root/postgres-password"
project_name="agentforge-${AGENTFORGE_FIXTURE_ID}"

export AGENTFORGE_POSTGRES_DB="af_${AGENTFORGE_FIXTURE_ID//[^a-zA-Z0-9]/_}"
export AGENTFORGE_POSTGRES_USER="af_fixture"
export AGENTFORGE_POSTGRES_PASSWORD_FILE="$password_file"

case "$command_name" in
  up)
    install -d -m 0700 "$runtime_root"
    if [[ ! -s "$password_file" ]]; then
      umask 077
      openssl rand -hex 32 >"$password_file"
    fi
    docker compose --project-name "$project_name" --file "$compose_file" up --detach
    ;;
  wait)
    deadline=$((SECONDS + 60))
    while ((SECONDS < deadline)); do
      status="$(docker inspect --format '{{.State.Health.Status}}' "${project_name}-postgres-1" 2>/dev/null || true)"
      [[ "$status" == healthy ]] && exit 0
      sleep 1
    done
    docker compose --project-name "$project_name" --file "$compose_file" logs postgres >&2
    exit 1
    ;;
  down)
    docker compose --project-name "$project_name" --file "$compose_file" down --volumes --remove-orphans
    if [[ -d "$runtime_root" ]]; then
      find "$runtime_root" -type f -exec shred -u -- {} + 2>/dev/null || true
      rmdir "$runtime_root" 2>/dev/null || true
    fi
    ;;
esac
