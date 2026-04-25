#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/python_env_guard.sh"
REPO_PREFERRED_PYTHON_BIN="$(resolve_preferred_repo_python_bin "${PROJECT_ROOT}")"
HOSTED_PYTHON_BIN="$(resolve_project_python_bin "${PROJECT_ROOT}" "${HOSTED_PYTHON_BIN-}")"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/dev_postgres_env.sh"

source "${SCRIPT_DIR}/local_dev_proxy_guard.sh"

HOSTED_RUNTIME_DIR="${HOSTED_RUNTIME_DIR:-${PROJECT_ROOT}/runtime_hosted}"
HOSTED_API_HOST="${HOSTED_API_HOST:-127.0.0.1}"
HOSTED_API_PORT="${HOSTED_API_PORT:-8765}"
HOSTED_FRONTEND_ORIGIN="${HOSTED_FRONTEND_ORIGIN:-https://demo.111874.xyz}"
HOSTED_DAEMON_POLL_SECONDS="${HOSTED_DAEMON_POLL_SECONDS:-8}"
HOSTED_MAX_PARALLEL_REQUESTS="${HOSTED_MAX_PARALLEL_REQUESTS:-8}"
HOSTED_LIGHT_REQUEST_RESERVED="${HOSTED_LIGHT_REQUEST_RESERVED:-2}"
HOSTED_EXTERNAL_PROVIDER_MODE_DEFAULT="${HOSTED_EXTERNAL_PROVIDER_MODE:-live}"
START_DAEMON=1
PRINT_CONFIG=0

_usage() {
  cat <<'EOF'
Usage:
  bash ./scripts/run_hosted_trial_backend.sh
  bash ./scripts/run_hosted_trial_backend.sh --print-config
  bash ./scripts/run_hosted_trial_backend.sh --host 0.0.0.0 --port 8765 --origin https://demo.111874.xyz

What it does:
  - starts a lean hosted backend profile for colleague trial use
  - defaults to live provider mode; replay/simulate belong to isolated smoke runtimes
  - keeps API concurrency bounded, but no longer at a level where a couple of slow requests can stall the whole API

Options:
  --host <host>              Backend bind host. Default: 127.0.0.1
  --port <port>              Backend port. Default: 8765
  --runtime-dir <path>       Hosted runtime dir. Default: <project>/runtime_hosted
  --python-bin <path>        Python executable. Default: prefer <repo>/.venv/bin/python, then <repo>/.venv-tests/bin/python, otherwise python3
  --origin <url>             Allowed frontend origin. Default: https://demo.111874.xyz
  --daemon-poll-seconds <n>  Worker daemon poll interval. Default: 8
  --max-parallel <n>         SOURCING_API_MAX_PARALLEL_REQUESTS. Default: 8
  --light-reserved <n>       SOURCING_API_LIGHT_REQUEST_RESERVED. Default: 2
  --no-daemon                Start serve only.
  --print-config             Print resolved config and exit.
  --help                     Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOSTED_API_HOST="${2-}"
      shift 2
      ;;
    --port)
      HOSTED_API_PORT="${2-}"
      shift 2
      ;;
    --runtime-dir)
      HOSTED_RUNTIME_DIR="${2-}"
      shift 2
      ;;
    --python-bin)
      HOSTED_PYTHON_BIN="$(resolve_project_python_bin "${PROJECT_ROOT}" "${2-}")"
      shift 2
      ;;
    --origin)
      HOSTED_FRONTEND_ORIGIN="${2-}"
      shift 2
      ;;
    --daemon-poll-seconds)
      HOSTED_DAEMON_POLL_SECONDS="${2-}"
      shift 2
      ;;
    --max-parallel)
      HOSTED_MAX_PARALLEL_REQUESTS="${2-}"
      shift 2
      ;;
    --light-reserved)
      HOSTED_LIGHT_REQUEST_RESERVED="${2-}"
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
      _usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      _usage >&2
      exit 2
      ;;
  esac
done

require_python_modules_or_exit "${HOSTED_PYTHON_BIN}" requests
require_python_modules_or_exit "${HOSTED_PYTHON_BIN}" psycopg

if [[ -z "${SOURCING_CONTROL_PLANE_POSTGRES_DSN-}" ]]; then
  printf 'Hosted trial backend requires SOURCING_CONTROL_PLANE_POSTGRES_DSN.\n' >&2
  exit 2
fi
if [[ "${SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE:-postgres_only}" != "postgres_only" ]]; then
  printf 'Hosted trial backend requires SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only.\n' >&2
  exit 2
fi
export SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE="postgres_only"
export SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES="1"
export SOURCING_PG_ONLY_SQLITE_BACKEND="${SOURCING_PG_ONLY_SQLITE_BACKEND:-shared_memory}"
export SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA="${SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA:-public}"

if [[ "${HOSTED_RUNTIME_DIR}" != /* ]]; then
  HOSTED_RUNTIME_DIR="${PROJECT_ROOT}/${HOSTED_RUNTIME_DIR}"
fi

export SOURCING_RUNTIME_DIR="${HOSTED_RUNTIME_DIR}"
export SOURCING_API_ALLOWED_ORIGINS="${HOSTED_FRONTEND_ORIGIN}"
export SOURCING_API_MAX_PARALLEL_REQUESTS="${HOSTED_MAX_PARALLEL_REQUESTS}"
export SOURCING_API_LIGHT_REQUEST_RESERVED="${HOSTED_LIGHT_REQUEST_RESERVED}"
export SOURCING_EXTERNAL_PROVIDER_MODE="${SOURCING_EXTERNAL_PROVIDER_MODE:-${HOSTED_EXTERNAL_PROVIDER_MODE_DEFAULT}}"
export SOURCING_RUNTIME_ENVIRONMENT="${SOURCING_RUNTIME_ENVIRONMENT:-production}"

if [[ "${SOURCING_RUNTIME_ENVIRONMENT}" == "production" && "${SOURCING_EXTERNAL_PROVIDER_MODE}" != "live" ]]; then
  if [[ "${SOURCING_ALLOW_PRODUCTION_NONLIVE_PROVIDER:-}" != "1" && "${SOURCING_ALLOW_PRODUCTION_REPLAY:-}" != "1" ]]; then
    printf 'Production hosted backend requires SOURCING_EXTERNAL_PROVIDER_MODE=live. Use an isolated test/scripted/replay runtime for non-live smoke, or set SOURCING_ALLOW_PRODUCTION_NONLIVE_PROVIDER=1 for an explicit temporary override.\n' >&2
    exit 2
  fi
fi

if [[ ${PRINT_CONFIG} -eq 1 ]]; then
  printf 'project_root=%s\n' "${PROJECT_ROOT}"
  printf 'runtime_dir=%s\n' "${HOSTED_RUNTIME_DIR}"
  printf 'python_bin=%s\n' "${HOSTED_PYTHON_BIN}"
  printf 'api_host=%s\n' "${HOSTED_API_HOST}"
  printf 'api_port=%s\n' "${HOSTED_API_PORT}"
  printf 'frontend_origin=%s\n' "${HOSTED_FRONTEND_ORIGIN}"
  printf 'start_daemon=%s\n' "${START_DAEMON}"
  printf 'daemon_poll_seconds=%s\n' "${HOSTED_DAEMON_POLL_SECONDS}"
  printf 'SOURCING_API_MAX_PARALLEL_REQUESTS=%s\n' "${SOURCING_API_MAX_PARALLEL_REQUESTS}"
  printf 'SOURCING_API_LIGHT_REQUEST_RESERVED=%s\n' "${SOURCING_API_LIGHT_REQUEST_RESERVED}"
  printf 'SOURCING_EXTERNAL_PROVIDER_MODE=%s\n' "${SOURCING_EXTERNAL_PROVIDER_MODE}"
  printf 'SOURCING_RUNTIME_ENVIRONMENT=%s\n' "${SOURCING_RUNTIME_ENVIRONMENT}"
  printf 'SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=%s\n' "${SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE}"
  printf 'SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA=%s\n' "${SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA}"
  printf 'SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=%s\n' "${SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES}"
  printf 'SOURCING_PG_ONLY_SQLITE_BACKEND=%s\n' "${SOURCING_PG_ONLY_SQLITE_BACKEND}"
  printf 'NO_PROXY=%s\n' "${NO_PROXY-}"
  exit 0
fi

mkdir -p "${HOSTED_RUNTIME_DIR}/service_logs"
daemon_pid=""
daemon_log="${HOSTED_RUNTIME_DIR}/service_logs/hosted-worker-daemon.log"

_cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  if [[ -n "${daemon_pid}" ]] && kill -0 "${daemon_pid}" 2>/dev/null; then
    kill "${daemon_pid}" 2>/dev/null || true
    wait "${daemon_pid}" 2>/dev/null || true
  fi
  exit "${exit_code}"
}

trap _cleanup EXIT INT TERM

if [[ ${START_DAEMON} -eq 1 ]]; then
  (
    cd "${PROJECT_ROOT}"
    PYTHONPATH=src "${HOSTED_PYTHON_BIN}" -m sourcing_agent.cli run-worker-daemon-service --poll-seconds "${HOSTED_DAEMON_POLL_SECONDS}"
  ) >"${daemon_log}" 2>&1 &
  daemon_pid="$!"
  sleep 1
  if ! kill -0 "${daemon_pid}" 2>/dev/null; then
    daemon_pid=""
    printf 'worker daemon did not stay running; continuing with API serve only. Check %s if needed.\n' "${daemon_log}" >&2
  fi
fi

printf 'Starting hosted-trial backend at http://%s:%s\n' "${HOSTED_API_HOST}" "${HOSTED_API_PORT}"
printf 'Runtime dir: %s\n' "${HOSTED_RUNTIME_DIR}"
printf 'Allowed frontend origin: %s\n' "${SOURCING_API_ALLOWED_ORIGINS}"
printf 'External provider mode: %s\n' "${SOURCING_EXTERNAL_PROVIDER_MODE}"
printf 'Runtime environment: %s\n' "${SOURCING_RUNTIME_ENVIRONMENT}"
if [[ -n "${daemon_pid}" ]]; then
  printf 'Worker daemon started with pid %s, log: %s\n' "${daemon_pid}" "${daemon_log}"
fi

cd "${PROJECT_ROOT}"
PYTHONPATH=src "${HOSTED_PYTHON_BIN}" -m sourcing_agent.cli serve --host "${HOSTED_API_HOST}" --port "${HOSTED_API_PORT}"
