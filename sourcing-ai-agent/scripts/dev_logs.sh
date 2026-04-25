#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
DEV_LOGS_RUNTIME_DIR="${DEV_LOGS_RUNTIME_DIR:-${PROJECT_ROOT}/runtime}"
SERVICE_LOG_DIR="${DEV_LOGS_RUNTIME_DIR}/service_logs"

DEV_LOGS_API_PORT="${DEV_LOGS_API_PORT:-8765}"
DEV_LOGS_FRONTEND_PORT="${DEV_LOGS_FRONTEND_PORT:-4173}"
DEV_LOGS_LINES="${DEV_LOGS_LINES:-80}"
DEV_LOGS_FOLLOW=1

_dev_logs_usage() {
  cat <<'EOF'
Usage:
  bash ./scripts/dev_logs.sh
  bash ./scripts/dev_logs.sh --no-follow --lines 120
  bash ./scripts/dev_logs.sh --api-port 8766 --frontend-port 4174
  bash ./scripts/dev_logs.sh --runtime-dir runtime/test_env --api-port 8775 --frontend-port 4175

What it does:
  - tails the most relevant local dev logs for make dev / helper-based runs
  - prefers the make-dev backend log plus the current daemon / hosted runtime logs
  - includes only files that currently exist

Options:
  --api-port <port>               Backend API port. Default: 8765
  --frontend-port <port>          Frontend port. Default: 4173
  --runtime-dir <path>            Runtime dir / service log namespace. Default: <project>/runtime
  --lines <count>                 Tail line count per file. Default: 80
  --follow                        Keep following logs. Default behavior.
  --no-follow                     Print the current tail and exit.
  --help                          Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-port)
      DEV_LOGS_API_PORT="${2-}"
      shift 2
      ;;
    --frontend-port)
      DEV_LOGS_FRONTEND_PORT="${2-}"
      shift 2
      ;;
    --runtime-dir)
      DEV_LOGS_RUNTIME_DIR="${2-}"
      shift 2
      ;;
    --lines)
      DEV_LOGS_LINES="${2-}"
      shift 2
      ;;
    --follow)
      DEV_LOGS_FOLLOW=1
      shift
      ;;
    --no-follow)
      DEV_LOGS_FOLLOW=0
      shift
      ;;
    --help|-h)
      _dev_logs_usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      _dev_logs_usage >&2
      exit 2
      ;;
  esac
done

if [[ ! "$DEV_LOGS_LINES" =~ ^[0-9]+$ ]]; then
  printf 'Invalid --lines value: %s\n' "$DEV_LOGS_LINES" >&2
  exit 2
fi
if [[ "${DEV_LOGS_RUNTIME_DIR}" != /* ]]; then
  DEV_LOGS_RUNTIME_DIR="${PROJECT_ROOT}/${DEV_LOGS_RUNTIME_DIR}"
fi
SERVICE_LOG_DIR="${DEV_LOGS_RUNTIME_DIR}/service_logs"

_dev_logs_add_if_exists() {
  local candidate="${1-}"
  local -n target_ref="$2"
  local key=""
  if [[ -z "$candidate" || ! -f "$candidate" ]]; then
    return 0
  fi
  for key in "${target_ref[@]}"; do
    if [[ "$key" == "$candidate" ]]; then
      return 0
    fi
  done
  target_ref+=("$candidate")
}

declare -a LOG_FILES=()

_dev_logs_add_if_exists "${SERVICE_LOG_DIR}/make-dev-backend.log" LOG_FILES
_dev_logs_add_if_exists "${SERVICE_LOG_DIR}/dev-worker-daemon.log" LOG_FILES
_dev_logs_add_if_exists "${SERVICE_LOG_DIR}/serve-${DEV_LOGS_API_PORT}-current.log" LOG_FILES
_dev_logs_add_if_exists "${SERVICE_LOG_DIR}/serve-${DEV_LOGS_API_PORT}.log" LOG_FILES
_dev_logs_add_if_exists "${SERVICE_LOG_DIR}/serve-${DEV_LOGS_API_PORT}-frontend.log" LOG_FILES
_dev_logs_add_if_exists "${SERVICE_LOG_DIR}/worker-daemon-current.log" LOG_FILES
_dev_logs_add_if_exists "${SERVICE_LOG_DIR}/worker-recovery-daemon.log" LOG_FILES
_dev_logs_add_if_exists "${SERVICE_LOG_DIR}/server-runtime-watchdog.log" LOG_FILES
_dev_logs_add_if_exists "${SERVICE_LOG_DIR}/frontend-${DEV_LOGS_FRONTEND_PORT}.log" LOG_FILES

if [[ ${#LOG_FILES[@]} -eq 0 ]]; then
  printf 'No local dev log files found under %s\n' "$SERVICE_LOG_DIR" >&2
  printf 'Expected files include make-dev-backend.log, dev-worker-daemon.log, serve-%s-current.log, worker-daemon-current.log.\n' "$DEV_LOGS_API_PORT" >&2
  exit 1
fi

printf 'Local Dev Logs\n'
printf '  runtime_dir:         %s\n' "$DEV_LOGS_RUNTIME_DIR"
printf '  service_log_dir:     %s\n' "$SERVICE_LOG_DIR"
printf '  backend_port:        %s\n' "$DEV_LOGS_API_PORT"
printf '  frontend_port:       %s\n' "$DEV_LOGS_FRONTEND_PORT"
printf '  lines_per_file:      %s\n' "$DEV_LOGS_LINES"
printf '  follow:              %s\n' "$DEV_LOGS_FOLLOW"
printf '  files:\n'
printf '    %s\n' "${LOG_FILES[@]}"

if [[ $DEV_LOGS_FOLLOW -eq 1 ]]; then
  exec tail -n "$DEV_LOGS_LINES" -F -v "${LOG_FILES[@]}"
fi

exec tail -n "$DEV_LOGS_LINES" -v "${LOG_FILES[@]}"
