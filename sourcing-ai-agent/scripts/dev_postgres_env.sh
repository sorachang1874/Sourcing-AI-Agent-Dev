#!/usr/bin/env bash
set -euo pipefail

_return_or_exit() {
  local code="${1:-0}"
  return "${code}" 2>/dev/null || exit "${code}"
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PROJECT_PARENT_ROOT="$(cd -- "${PROJECT_ROOT}/.." && pwd)"
WORKSPACE_ROOT="$(cd -- "${PROJECT_ROOT}/../.." && pwd)"
DEFAULT_LOCAL_PG_ROOT="${PROJECT_PARENT_ROOT}/.local-postgres"
if [[ ! -d "${DEFAULT_LOCAL_PG_ROOT}" ]] && [[ -d "${WORKSPACE_ROOT}/.local-postgres" ]]; then
  DEFAULT_LOCAL_PG_ROOT="${WORKSPACE_ROOT}/.local-postgres"
fi

_candidate_env_files=()
if [[ -n "${SOURCING_LOCAL_POSTGRES_ENV_FILE:-}" ]]; then
  _candidate_env_files+=("${SOURCING_LOCAL_POSTGRES_ENV_FILE}")
fi
_candidate_env_files+=(
  "${PROJECT_ROOT}/.local-postgres.env"
  "${PROJECT_PARENT_ROOT}/.local-postgres.env"
  "${WORKSPACE_ROOT}/.local-postgres.env"
  "${DEFAULT_LOCAL_PG_ROOT}/connection.env"
)

for _candidate_env_file in "${_candidate_env_files[@]}"; do
  if [[ -f "${_candidate_env_file}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${_candidate_env_file}"
    set +a
    break
  fi
done

LOCAL_PG_ROOT="${LOCAL_PG_ROOT:-${DEFAULT_LOCAL_PG_ROOT}}"
LOCAL_PG_EXTRACT="${LOCAL_PG_EXTRACT:-${LOCAL_PG_ROOT}/extract}"
LOCAL_PG_DATA="${LOCAL_PG_DATA:-${LOCAL_PG_ROOT}/data}"
LOCAL_PG_RUN="${LOCAL_PG_RUN:-${LOCAL_PG_ROOT}/run}"
LOCAL_PG_PORT="${LOCAL_PG_PORT:-55432}"
LOCAL_PG_USER="${LOCAL_PG_USER:-sourcing}"
LOCAL_PG_DB="${LOCAL_PG_DB:-sourcing_agent}"

if [[ ! -d "${LOCAL_PG_EXTRACT}" ]] || [[ ! -d "${LOCAL_PG_DATA}" ]]; then
  if [[ -z "${SOURCING_CONTROL_PLANE_POSTGRES_DSN:-}" ]]; then
    _return_or_exit 0
  fi
fi

if [[ -d "${LOCAL_PG_EXTRACT}/usr/lib/x86_64-linux-gnu" ]] || [[ -d "${LOCAL_PG_EXTRACT}/usr/lib/postgresql/16/lib" ]]; then
  export LD_LIBRARY_PATH="${LOCAL_PG_EXTRACT}/usr/lib/x86_64-linux-gnu:${LOCAL_PG_EXTRACT}/usr/lib/postgresql/16/lib:${LD_LIBRARY_PATH:-}"
fi
if [[ -d "${LOCAL_PG_EXTRACT}/usr/lib/postgresql/16/bin" ]]; then
  export PATH="${LOCAL_PG_EXTRACT}/usr/lib/postgresql/16/bin:${PATH}"
fi

export SOURCING_CONTROL_PLANE_POSTGRES_DSN="${SOURCING_CONTROL_PLANE_POSTGRES_DSN:-postgresql://${LOCAL_PG_USER}@127.0.0.1:${LOCAL_PG_PORT}/${LOCAL_PG_DB}}"
export SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE="${SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE:-postgres_only}"
export SOURCING_LOCAL_POSTGRES_ROOT="${LOCAL_PG_ROOT}"
export SOURCING_LOCAL_POSTGRES_RUN_DIR="${LOCAL_PG_RUN}"
