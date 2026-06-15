#!/usr/bin/env bash
set -euo pipefail

timeout_seconds="${DOCKER_START_TIMEOUT_SECONDS:-120}"

docker_ready() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

wait_for_docker() {
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if docker_ready; then
      docker info --format 'Docker ready: {{.ServerVersion}}'
      return 0
    fi
    sleep 2
  done
  return 1
}

if docker_ready; then
  docker info --format 'Docker ready: {{.ServerVersion}}'
  exit 0
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  if [[ -d "/Applications/Docker.app" ]]; then
    open -ga Docker || open -a Docker
    if wait_for_docker; then
      exit 0
    fi
    echo "Docker.app was started but Docker did not become ready within ${timeout_seconds}s." >&2
    exit 1
  fi
  if command -v colima >/dev/null 2>&1; then
    colima start
    if wait_for_docker; then
      exit 0
    fi
    echo "colima start completed but Docker did not become ready within ${timeout_seconds}s." >&2
    exit 1
  fi
fi

cat >&2 <<'EOF'
Docker is not available.

Install one local container runtime, then rerun:
  macOS Docker Desktop: brew install --cask docker && open -a Docker
  macOS Colima:         brew install docker colima && colima start

After Docker is ready:
  make docker-doctor
  make local-container-smoke
EOF
exit 1
