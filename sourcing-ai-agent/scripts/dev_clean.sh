#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
DEV_CLEAN_RUNTIME_DIR="${DEV_CLEAN_RUNTIME_DIR:-${PROJECT_ROOT}/runtime}"
SERVICE_LOG_DIR="${DEV_CLEAN_RUNTIME_DIR}/service_logs"
SERVICES_DIR="${DEV_CLEAN_RUNTIME_DIR}/services"

source "${SCRIPT_DIR}/dev_process_helpers.sh"

DEV_CLEAN_API_PORT="${DEV_CLEAN_API_PORT:-8765}"
DEV_CLEAN_FRONTEND_PORT="${DEV_CLEAN_FRONTEND_PORT:-4173}"
DEV_CLEAN_DRY_RUN=0

_dev_clean_usage() {
  cat <<'EOF'
Usage:
  bash ./scripts/dev_clean.sh
  bash ./scripts/dev_clean.sh --api-port 8766 --frontend-port 4174
  bash ./scripts/dev_clean.sh --runtime-dir runtime/test_env --api-port 8775 --frontend-port 4175
  bash ./scripts/dev_clean.sh --dry-run

What it does:
  - runs the local dev stop step first
  - removes helper-owned logs and pid files for the configured ports
  - removes stale local dev service status directories when their recorded pid is no longer running

What it will not touch:
  - workflow jobs, company assets, control-plane data, or workflow-runner/job-recovery logs
  - active service status directories whose pid is still alive

Options:
  --api-port <port>               Backend API port. Default: 8765
  --frontend-port <port>          Frontend port. Default: 4173
  --runtime-dir <path>            Runtime dir / service log namespace. Default: <project>/runtime
  --dry-run                       Print planned cleanup without deleting anything.
  --help                          Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-port)
      DEV_CLEAN_API_PORT="${2-}"
      shift 2
      ;;
    --frontend-port)
      DEV_CLEAN_FRONTEND_PORT="${2-}"
      shift 2
      ;;
    --runtime-dir)
      DEV_CLEAN_RUNTIME_DIR="${2-}"
      shift 2
      ;;
    --dry-run)
      DEV_CLEAN_DRY_RUN=1
      shift
      ;;
    --help|-h)
      _dev_clean_usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      _dev_clean_usage >&2
      exit 2
      ;;
  esac
done
if [[ "${DEV_CLEAN_RUNTIME_DIR}" != /* ]]; then
  DEV_CLEAN_RUNTIME_DIR="${PROJECT_ROOT}/${DEV_CLEAN_RUNTIME_DIR}"
fi
SERVICE_LOG_DIR="${DEV_CLEAN_RUNTIME_DIR}/service_logs"
SERVICES_DIR="${DEV_CLEAN_RUNTIME_DIR}/services"

_dev_clean_remove_file() {
  local target="${1-}"
  if [[ -z "$target" || ! -e "$target" ]]; then
    return 0
  fi
  if [[ $DEV_CLEAN_DRY_RUN -eq 1 ]]; then
    printf 'dry-run: would remove file %s\n' "$target"
    return 0
  fi
  rm -f -- "$target"
  printf 'removed file %s\n' "$target"
}

_dev_clean_remove_dir() {
  local target="${1-}"
  if [[ -z "$target" || ! -d "$target" ]]; then
    return 0
  fi
  if [[ $DEV_CLEAN_DRY_RUN -eq 1 ]]; then
    printf 'dry-run: would remove dir  %s\n' "$target"
    return 0
  fi
  rm -rf -- "$target"
  printf 'removed dir  %s\n' "$target"
}

_dev_clean_maybe_remove_service_dir() {
  local service_dir="${1-}"
  local status_file="${service_dir}/status.json"
  local pid=""
  if [[ ! -d "$service_dir" ]]; then
    return 0
  fi
  pid="$(dev_status_pid_from_file "$status_file" || true)"
  if dev_pid_is_running "$pid"; then
    printf 'kept active service dir %s (pid %s still running)\n' "$service_dir" "$pid"
    return 0
  fi
  _dev_clean_remove_dir "$service_dir"
}

printf 'Cleaning local dev artifacts under %s\n' "$PROJECT_ROOT"
printf '  runtime_dir:         %s\n' "$DEV_CLEAN_RUNTIME_DIR"
printf '  backend_port:        %s\n' "$DEV_CLEAN_API_PORT"
printf '  frontend_port:       %s\n' "$DEV_CLEAN_FRONTEND_PORT"
printf '  dry_run:             %s\n' "$DEV_CLEAN_DRY_RUN"

if [[ $DEV_CLEAN_DRY_RUN -eq 1 ]]; then
  printf 'dry-run: would invoke stop step for backend=%s frontend=%s\n' "$DEV_CLEAN_API_PORT" "$DEV_CLEAN_FRONTEND_PORT"
else
  bash "${SCRIPT_DIR}/dev_stop.sh" \
    --runtime-dir "$DEV_CLEAN_RUNTIME_DIR" \
    --api-port "$DEV_CLEAN_API_PORT" \
    --frontend-port "$DEV_CLEAN_FRONTEND_PORT" \
    || true
fi

declare -a FILES_TO_REMOVE=(
  "${SERVICE_LOG_DIR}/make-dev-backend.log"
  "${SERVICE_LOG_DIR}/make-dev-backend.pid"
  "${SERVICE_LOG_DIR}/dev-worker-daemon.log"
  "${SERVICE_LOG_DIR}/dev-worker-daemon.pid"
  "${SERVICE_LOG_DIR}/frontend-${DEV_CLEAN_FRONTEND_PORT}.log"
  "${SERVICE_LOG_DIR}/serve-${DEV_CLEAN_API_PORT}.log"
  "${SERVICE_LOG_DIR}/serve-${DEV_CLEAN_API_PORT}-current.log"
  "${SERVICE_LOG_DIR}/serve-${DEV_CLEAN_API_PORT}-frontend.log"
)

printf '\nLog And Pid Cleanup\n'
for target in "${FILES_TO_REMOVE[@]}"; do
  _dev_clean_remove_file "$target"
done

printf '\nService State Cleanup\n'
_dev_clean_maybe_remove_service_dir "${SERVICES_DIR}/server-runtime-watchdog"
_dev_clean_maybe_remove_service_dir "${SERVICES_DIR}/server-runtime-watchdog-live${DEV_CLEAN_API_PORT}"
_dev_clean_maybe_remove_service_dir "${SERVICES_DIR}/worker-recovery-daemon"
_dev_clean_maybe_remove_service_dir "${SERVICES_DIR}/worker-recovery-daemon-live${DEV_CLEAN_API_PORT}"

printf '\nLocal dev cleanup finished.\n'
