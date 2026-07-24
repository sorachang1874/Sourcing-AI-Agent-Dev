#!/usr/bin/env bash
# Diagnose the container engine + test deps for the testcontainers lanes.
# Docker was retired for Podman (2026-07-24): prefers podman, falls back to
# docker. Honors SOURCING_CONTAINER_ENGINE. The Docker SDK (docker-py, used by
# testcontainers) talks to whatever DOCKER_HOST points at, so the podman
# machine's Docker-API socket works transparently.
set -euo pipefail

python_bin="${TEST_PYTHON_BIN:-${PYTHON_BIN:-./.venv-tests/bin/python}}"

engine="${SOURCING_CONTAINER_ENGINE:-}"
if [[ -z "$engine" ]]; then
  if command -v podman >/dev/null 2>&1; then
    engine="podman"
  elif command -v docker >/dev/null 2>&1; then
    engine="docker"
  else
    echo "No container engine (podman/docker) installed or on PATH." >&2
    exit 1
  fi
fi

# Cross-engine liveness: `version --format {{.Server.Version}}` works on both
# podman and docker (unlike docker's `info --format {{.ServerVersion}}`).
if ! "$engine" version --format '{{.Server.Version}}' >/dev/null 2>&1; then
  echo "$engine is installed, but its engine/daemon is not reachable." >&2
  echo "Run: make docker-start" >&2
  exit 1
fi

if [[ -z "${DOCKER_HOST:-}" ]]; then
  if [[ "$engine" == "podman" ]]; then
    podman_socket="$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null | head -1 || true)"
    if [[ -n "$podman_socket" && -S "$podman_socket" ]]; then
      export DOCKER_HOST="unix://${podman_socket}"
    fi
  else
    docker_host="$(docker context inspect --format '{{ (index .Endpoints "docker").Host }}' 2>/dev/null || true)"
    if [[ -n "$docker_host" ]]; then
      export DOCKER_HOST="$docker_host"
    fi
  fi
fi

if [[ ! -x "$python_bin" ]]; then
  echo "Python test binary is not executable: $python_bin" >&2
  echo "Run: make bootstrap-test-env" >&2
  exit 1
fi

"$python_bin" - <<'PY'
import importlib.util

missing = [
    module
    for module in ("testcontainers", "psycopg")
    if importlib.util.find_spec(module) is None
]
if missing:
    raise SystemExit(
        "Missing test dependencies: "
        + ", ".join(missing)
        + ". Run: make bootstrap-test-env"
    )
PY

"$engine" version --format "$engine ready: {{.Server.Version}}"
"$python_bin" - <<'PY'
import testcontainers
from docker import from_env

print(f"Testcontainers import ready: {testcontainers.__name__}")
client = from_env()
print(f"Docker SDK ready: {client.version().get('Version')}")
PY
