#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/ensure_modern_bash.sh"
ensure_modern_bash "$@"

PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
DEV_RUNTIME_DIR="${DEV_RUNTIME_DIR:-${PROJECT_ROOT}/runtime}"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/python_env_guard.sh"
REPO_PREFERRED_PYTHON_BIN="$(resolve_preferred_repo_python_bin "${PROJECT_ROOT}")"
DEV_PYTHON_BIN="$(resolve_project_python_bin "${PROJECT_ROOT}" "${DEV_PYTHON_BIN-}")"

source "${SCRIPT_DIR}/local_dev_proxy_guard.sh"

DEV_API_HOST="${DEV_API_HOST:-0.0.0.0}"
DEV_API_PORT="${DEV_API_PORT:-8765}"
DEV_DAEMON_POLL_SECONDS="${DEV_DAEMON_POLL_SECONDS:-5}"
DEV_FRONTEND_PORTS_DEFAULT="${DEV_FRONTEND_PORTS:-4173,4174}"
START_DAEMON=1
PRINT_CONFIG=0
DEV_BACKEND_SKIP_POSTGRES_AUTO_SOURCE=0
declare -a FRONTEND_PORTS=()
declare -a EXTRA_ORIGINS=()

_dev_backend_usage() {
  cat <<'EOF'
Usage:
  bash ./scripts/dev_backend.sh
  bash ./scripts/dev_backend.sh --print-config
  bash ./scripts/dev_backend.sh --host 127.0.0.1 --port 8766 --frontend-port 4173 --frontend-port 4174
  bash ./scripts/dev_backend.sh --runtime-dir runtime/test_env --port 8775
  bash ./scripts/dev_backend.sh --no-daemon

What it does:
  - sources local_dev_proxy_guard.sh
  - builds SOURCING_API_ALLOWED_ORIGINS for localhost / 127.0.0.1 frontend ports
  - starts worker daemon in background by default
  - starts hosted API serve in foreground

Options:
  --host <host>                  API bind host. Default: 0.0.0.0
  --port <port>                  API bind port. Default: 8765
  --runtime-dir <path>           Runtime dir / jobs / service namespace. Default: <project>/runtime
  --python-bin <path>            Python executable. Default: prefer <repo>/.venv/bin/python, then <repo>/.venv-tests/bin/python, otherwise python3
  --frontend-port <port>         Allow localhost/127.0.0.1 frontend origin for this port. Repeatable.
  --allow-origin <origin>        Add extra explicit origin. Repeatable.
  --daemon-poll-seconds <sec>    Worker daemon poll interval. Default: 5
  --no-daemon                    Start only serve, do not start worker daemon.
  --print-config                 Print resolved config and exit.
  --help                         Show this help.
EOF
}

_dev_backend_append_csv_value() {
  local value="${1-}"
  local -n target_ref="$2"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  if [[ -n "$value" ]]; then
    target_ref+=("$value")
  fi
}

_dev_backend_build_allowed_origins() {
  local -a values=()
  local raw_existing="${SOURCING_API_ALLOWED_ORIGINS-}"
  local item=""
  local IFS=','
  local -A seen=()
  local key=""

  if [[ -n "$raw_existing" ]]; then
    read -r -a values <<< "$raw_existing"
  fi
  for item in "${FRONTEND_PORTS[@]}"; do
    _dev_backend_append_csv_value "http://localhost:${item}" values
    _dev_backend_append_csv_value "http://127.0.0.1:${item}" values
  done
  for item in "${EXTRA_ORIGINS[@]}"; do
    _dev_backend_append_csv_value "$item" values
  done

  local -a normalized=()
  for item in "${values[@]}"; do
    item="${item#"${item%%[![:space:]]*}"}"
    item="${item%"${item##*[![:space:]]}"}"
    if [[ -z "$item" ]]; then
      continue
    fi
    key="${item,,}"
    if [[ -n "${seen[$key]-}" ]]; then
      continue
    fi
    seen["$key"]=1
    normalized+=("$item")
  done

  local joined=""
  local first=1
  for item in "${normalized[@]}"; do
    if [[ $first -eq 1 ]]; then
      joined="$item"
      first=0
      continue
    fi
    joined="${joined},${item}"
  done
  printf '%s' "$joined"
}

_dev_backend_infer_runtime_environment() {
  local explicit="${SOURCING_RUNTIME_ENVIRONMENT-}"
  if [[ -n "$explicit" ]]; then
    printf '%s' "$explicit"
    return
  fi
  local mode="${SOURCING_EXTERNAL_PROVIDER_MODE:-live}"
  mode="${mode,,}"
  case "$mode" in
    simulate|scripted|replay)
      printf '%s' "$mode"
      return
      ;;
  esac
  local runtime_lower="${DEV_RUNTIME_DIR,,}"
  case "$runtime_lower" in
    *"/test_env"*|*"runtime/test_env"*|*"test_env"*)
      printf 'test'
      ;;
    *"runtime_hosted"*|*"hosted_runtime"*|*"production_runtime"*)
      printf 'production'
      ;;
    *)
      printf 'local_dev'
      ;;
  esac
}

_dev_backend_prepare_isolated_runtime_env() {
  local runtime_env="${SOURCING_RUNTIME_ENVIRONMENT,,}"
  case "$runtime_env" in
    test|simulate|scripted|replay|ci)
      ;;
    *)
      return
      ;;
  esac
  if [[ "${SOURCING_ALLOW_TEST_REPO_POSTGRES_ENV:-}" == "1" ]]; then
    return
  fi
  if [[ -n "${SOURCING_LOCAL_POSTGRES_ENV_FILE:-}" ]]; then
    if [[ -f "${SOURCING_LOCAL_POSTGRES_ENV_FILE}" ]] && grep -Eq '^[[:space:]]*(export[[:space:]]+)?(SOURCING_CONTROL_PLANE_POSTGRES_DSN|LOCAL_PG_ROOT|SOURCING_LOCAL_POSTGRES_ROOT)=' "${SOURCING_LOCAL_POSTGRES_ENV_FILE}"; then
      return
    fi
    DEV_BACKEND_SKIP_POSTGRES_AUTO_SOURCE=1
    unset SOURCING_CONTROL_PLANE_POSTGRES_DSN
    unset SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE
    unset SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES
    unset SOURCING_LOCAL_POSTGRES_ROOT
    unset LOCAL_PG_ROOT LOCAL_PG_EXTRACT LOCAL_PG_DATA LOCAL_PG_RUN LOCAL_PG_PORT LOCAL_PG_USER LOCAL_PG_DB
    return
  fi
  mkdir -p "${DEV_RUNTIME_DIR}"
  local sentinel="${DEV_RUNTIME_DIR}/.isolated-local-postgres.env"
  if [[ ! -f "$sentinel" ]]; then
    cat >"$sentinel" <<'EOF'
# Explicit empty local-postgres env for isolated dev/test runtime.
# This blocks fallback to repo-level .local-postgres.env and prevents test/replay state from sharing live PG.
EOF
  fi
  export SOURCING_LOCAL_POSTGRES_ENV_FILE="$sentinel"
  DEV_BACKEND_SKIP_POSTGRES_AUTO_SOURCE=1
  unset SOURCING_CONTROL_PLANE_POSTGRES_DSN
  unset SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE
  unset SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES
  unset SOURCING_LOCAL_POSTGRES_ROOT
  unset LOCAL_PG_ROOT LOCAL_PG_EXTRACT LOCAL_PG_DATA LOCAL_PG_RUN LOCAL_PG_PORT LOCAL_PG_USER LOCAL_PG_DB
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      DEV_API_HOST="${2-}"
      shift 2
      ;;
    --port)
      DEV_API_PORT="${2-}"
      shift 2
      ;;
    --runtime-dir)
      DEV_RUNTIME_DIR="${2-}"
      shift 2
      ;;
    --python-bin)
      DEV_PYTHON_BIN="$(resolve_project_python_bin "${PROJECT_ROOT}" "${2-}")"
      shift 2
      ;;
    --frontend-port)
      _dev_backend_append_csv_value "${2-}" FRONTEND_PORTS
      shift 2
      ;;
    --allow-origin)
      _dev_backend_append_csv_value "${2-}" EXTRA_ORIGINS
      shift 2
      ;;
    --daemon-poll-seconds)
      DEV_DAEMON_POLL_SECONDS="${2-}"
      shift 2
      ;;
    --no-daemon)
      START_DAEMON=0
      shift
      ;;
    --print-config)
      PRINT_CONFIG=1
      shift
      ;;
    --help|-h)
      _dev_backend_usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      _dev_backend_usage >&2
      exit 2
      ;;
  esac
done

require_python_modules_or_exit "${DEV_PYTHON_BIN}" requests

if [[ "${DEV_RUNTIME_DIR}" != /* ]]; then
  DEV_RUNTIME_DIR="${PROJECT_ROOT}/${DEV_RUNTIME_DIR}"
fi
export SOURCING_RUNTIME_DIR="${DEV_RUNTIME_DIR}"
export SOURCING_RUNTIME_ENVIRONMENT="$(_dev_backend_infer_runtime_environment)"
_dev_backend_prepare_isolated_runtime_env

if [[ -f "${SCRIPT_DIR}/dev_postgres_env.sh" && ${DEV_BACKEND_SKIP_POSTGRES_AUTO_SOURCE} -ne 1 ]]; then
  if "${DEV_PYTHON_BIN}" -c "import psycopg" >/dev/null 2>&1; then
    # Prefer the local Postgres control plane automatically when the active Python can actually speak psycopg.
    source "${SCRIPT_DIR}/dev_postgres_env.sh"
  elif [[ -n "${SOURCING_CONTROL_PLANE_POSTGRES_DSN-}" || -n "${SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE-}" ]]; then
    printf 'Configured Postgres control plane requires psycopg, but %s cannot import it.\n' "${DEV_PYTHON_BIN}" >&2
    exit 2
  else
    printf 'Local dev backend requires Postgres control-plane support; %s cannot import psycopg.\n' "${DEV_PYTHON_BIN}" >&2
    exit 2
  fi
fi

if [[ -n "${SOURCING_CONTROL_PLANE_POSTGRES_DSN-}" || -n "${SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE-}" ]]; then
  require_python_modules_or_exit "${DEV_PYTHON_BIN}" psycopg
  export SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES="${SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES:-1}"
  if [[ -z "${SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA-}" ]]; then
    SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA="$(
      cd "${PROJECT_ROOT}"
      PYTHONPATH=src "${DEV_PYTHON_BIN}" - <<'PY'
from sourcing_agent.local_postgres import resolve_control_plane_postgres_schema
print(resolve_control_plane_postgres_schema())
PY
    )"
    export SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA
  fi
fi

if [[ ${#FRONTEND_PORTS[@]} -eq 0 ]]; then
  IFS=',' read -r -a FRONTEND_PORTS <<< "${DEV_FRONTEND_PORTS_DEFAULT}"
fi

export SOURCING_API_ALLOWED_ORIGINS="$(_dev_backend_build_allowed_origins)"

if [[ $PRINT_CONFIG -eq 1 ]]; then
  printf 'project_root=%s\n' "$PROJECT_ROOT"
  printf 'runtime_dir=%s\n' "$DEV_RUNTIME_DIR"
  printf 'python_bin=%s\n' "$DEV_PYTHON_BIN"
  printf 'api_host=%s\n' "$DEV_API_HOST"
  printf 'api_port=%s\n' "$DEV_API_PORT"
  printf 'start_daemon=%s\n' "$START_DAEMON"
  printf 'daemon_poll_seconds=%s\n' "$DEV_DAEMON_POLL_SECONDS"
  printf 'frontend_ports=%s\n' "$(IFS=,; printf '%s' "${FRONTEND_PORTS[*]}")"
  printf 'SOURCING_API_ALLOWED_ORIGINS=%s\n' "${SOURCING_API_ALLOWED_ORIGINS}"
  printf 'SOURCING_RUNTIME_ENVIRONMENT=%s\n' "${SOURCING_RUNTIME_ENVIRONMENT}"
  printf 'SOURCING_LOCAL_POSTGRES_ENV_FILE=%s\n' "${SOURCING_LOCAL_POSTGRES_ENV_FILE-}"
  printf 'SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=%s\n' "${SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE-}"
  printf 'SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA=%s\n' "${SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA-}"
  printf 'SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=%s\n' "${SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES-}"
  printf 'NO_PROXY=%s\n' "${NO_PROXY-}"
  exit 0
fi

mkdir -p "${DEV_RUNTIME_DIR}/service_logs"
daemon_pid=""
daemon_owned_by_wrapper=0
daemon_log="${DEV_RUNTIME_DIR}/service_logs/dev-worker-daemon.log"
daemon_pid_file="${DEV_RUNTIME_DIR}/service_logs/dev-worker-daemon.pid"

_dev_backend_cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  if [[ $daemon_owned_by_wrapper -eq 1 ]] && [[ -n "${daemon_pid}" ]] && kill -0 "${daemon_pid}" 2>/dev/null; then
    kill "${daemon_pid}" 2>/dev/null || true
    wait "${daemon_pid}" 2>/dev/null || true
  fi
  if [[ $daemon_owned_by_wrapper -eq 1 ]]; then
    rm -f -- "${daemon_pid_file}"
  elif [[ -f "${daemon_pid_file}" ]]; then
    existing_pid="$(tr -cd '0-9' < "${daemon_pid_file}" || true)"
    if [[ -z "${existing_pid}" ]] || ! kill -0 "${existing_pid}" 2>/dev/null; then
      rm -f -- "${daemon_pid_file}"
    fi
  fi
  exit "$exit_code"
}

trap _dev_backend_cleanup EXIT INT TERM

if [[ $START_DAEMON -eq 1 ]]; then
  if [[ -f "${daemon_pid_file}" ]]; then
    existing_daemon_pid="$(tr -cd '0-9' < "${daemon_pid_file}" || true)"
    if [[ -n "${existing_daemon_pid}" ]] && kill -0 "${existing_daemon_pid}" 2>/dev/null; then
      daemon_pid="${existing_daemon_pid}"
      printf 'Reusing existing worker daemon pid %s for runtime %s\n' "${daemon_pid}" "${DEV_RUNTIME_DIR}"
    else
      rm -f -- "${daemon_pid_file}"
    fi
  fi
  if [[ -z "${daemon_pid}" ]]; then
    (
      cd "${PROJECT_ROOT}"
      PYTHONPATH=src "${DEV_PYTHON_BIN}" -m sourcing_agent.cli run-worker-daemon-service --poll-seconds "${DEV_DAEMON_POLL_SECONDS}"
    ) >"${daemon_log}" 2>&1 &
    daemon_pid="$!"
    daemon_owned_by_wrapper=1
    printf '%s\n' "${daemon_pid}" > "${daemon_pid_file}"
    sleep 1
    if ! kill -0 "${daemon_pid}" 2>/dev/null; then
      daemon_pid=""
      daemon_owned_by_wrapper=0
      rm -f -- "${daemon_pid_file}"
      printf 'worker daemon did not stay running; continuing with API serve only. Check %s if needed.\n' "${daemon_log}" >&2
    fi
  fi
fi

printf 'Starting local backend at http://%s:%s\n' "${DEV_API_HOST}" "${DEV_API_PORT}"
printf 'Runtime dir: %s\n' "${DEV_RUNTIME_DIR}"
printf 'Allowed frontend origins: %s\n' "${SOURCING_API_ALLOWED_ORIGINS}"
if [[ -n "${daemon_pid}" ]]; then
  printf 'Worker daemon started with pid %s, log: %s\n' "${daemon_pid}" "${daemon_log}"
fi

cd "${PROJECT_ROOT}"
PYTHONPATH=src "${DEV_PYTHON_BIN}" -m sourcing_agent.cli serve --host "${DEV_API_HOST}" --port "${DEV_API_PORT}"
