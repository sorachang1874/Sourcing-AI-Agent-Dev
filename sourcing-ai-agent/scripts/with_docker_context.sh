#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${DOCKER_HOST:-}" ]] && command -v docker >/dev/null 2>&1; then
  docker_host="$(docker context inspect --format '{{ (index .Endpoints "docker").Host }}' 2>/dev/null || true)"
  if [[ -n "$docker_host" ]]; then
    if [[ "$docker_host" == unix://* ]]; then
      docker_socket="${docker_host#unix://}"
      if [[ -S "$docker_socket" ]]; then
        export DOCKER_HOST="$docker_host"
      fi
    else
      export DOCKER_HOST="$docker_host"
    fi
  fi
fi

exec "$@"
