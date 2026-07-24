#!/usr/bin/env bash
# Export a Docker-API-compatible DOCKER_HOST for the testcontainers lanes.
# Docker was retired for Podman (2026-07-24): prefer the podman machine's
# Docker-compatible API socket; fall back to a docker context if a host still
# has Docker. Honors SOURCING_CONTAINER_ENGINE and any pre-set DOCKER_HOST.
set -euo pipefail

engine="${SOURCING_CONTAINER_ENGINE:-}"
if [[ -z "$engine" ]]; then
  if command -v podman >/dev/null 2>&1; then
    engine="podman"
  elif command -v docker >/dev/null 2>&1; then
    engine="docker"
  fi
fi

if [[ -z "${DOCKER_HOST:-}" ]]; then
  if [[ "$engine" == "podman" ]] && command -v podman >/dev/null 2>&1; then
    # podman's machine exposes a Docker-API-compatible socket testcontainers can use.
    podman_socket="$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null | head -1 || true)"
    if [[ -n "$podman_socket" && -S "$podman_socket" ]]; then
      export DOCKER_HOST="unix://${podman_socket}"
    fi
  elif [[ "$engine" == "docker" ]] && command -v docker >/dev/null 2>&1; then
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
fi

exec "$@"
