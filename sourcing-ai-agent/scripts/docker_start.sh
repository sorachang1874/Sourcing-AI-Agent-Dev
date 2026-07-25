#!/usr/bin/env bash
# Ensure a container engine is running for the disposable testcontainers lanes.
# Docker was retired for Podman (2026-07-24): this prefers `podman machine`,
# falling back to Docker if a host still has it. Honors SOURCING_CONTAINER_ENGINE.
set -euo pipefail

timeout_seconds="${DOCKER_START_TIMEOUT_SECONDS:-120}"

engine="${SOURCING_CONTAINER_ENGINE:-}"
if [[ -z "$engine" ]]; then
  if command -v podman >/dev/null 2>&1; then
    engine="podman"
  elif command -v docker >/dev/null 2>&1; then
    engine="docker"
  else
    echo "No container engine found (looked for podman, docker)." >&2
    exit 1
  fi
fi

engine_ready() {
  command -v "$engine" >/dev/null 2>&1 && "$engine" version --format '{{.Server.Version}}' >/dev/null 2>&1
}

wait_for_engine() {
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if engine_ready; then
      "$engine" version --format "$engine ready: {{.Server.Version}}"
      return 0
    fi
    sleep 2
  done
  return 1
}

if engine_ready; then
  "$engine" version --format "$engine ready: {{.Server.Version}}"
  exit 0
fi

if [[ "$engine" == "podman" ]]; then
  # Boot (or init+boot) the podman machine VM, then wait for the API.
  if ! podman machine list --format '{{.Name}}' 2>/dev/null | grep -q .; then
    podman machine init
  fi
  podman machine start 2>/dev/null || true
  if wait_for_engine; then
    exit 0
  fi
  echo "podman machine did not become ready within ${timeout_seconds}s." >&2
  exit 1
fi

# Docker fallback (only if a host still has Docker installed).
if [[ "$(uname -s)" == "Darwin" ]]; then
  if [[ -d "/Applications/Docker.app" ]]; then
    open -ga Docker || open -a Docker
    if wait_for_engine; then
      exit 0
    fi
    echo "Docker.app was started but did not become ready within ${timeout_seconds}s." >&2
    exit 1
  fi
fi

cat >&2 <<'EOF'
No running container engine.

Podman (default since 2026-07-24):
  brew install podman && podman machine init && podman machine start

After the engine is ready:
  make docker-doctor
  make local-container-smoke
EOF
exit 1
