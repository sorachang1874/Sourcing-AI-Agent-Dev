#!/usr/bin/env bash
set -euo pipefail

python_bin="${TEST_PYTHON_BIN:-${PYTHON_BIN:-./.venv-tests/bin/python}}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker CLI is not installed or not on PATH." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "docker CLI exists, but the Docker daemon is not reachable." >&2
  echo "Run: make docker-start" >&2
  exit 1
fi

if [[ -z "${DOCKER_HOST:-}" ]]; then
  docker_host="$(docker context inspect --format '{{ (index .Endpoints "docker").Host }}' 2>/dev/null || true)"
  if [[ -n "$docker_host" ]]; then
    export DOCKER_HOST="$docker_host"
  fi
fi

if [[ ! -x "$python_bin" ]]; then
  echo "Python test binary is not executable: $python_bin" >&2
  echo "Run: make bootstrap-test-env" >&2
  exit 1
fi

"$python_bin" - <<'PY'
import importlib.util
import sys

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

docker info --format 'Docker ready: {{.ServerVersion}}'
"$python_bin" - <<'PY'
import testcontainers
from docker import from_env

print(f"Testcontainers import ready: {testcontainers.__name__}")
client = from_env()
print(f"Docker SDK ready: {client.version().get('Version')}")
PY
