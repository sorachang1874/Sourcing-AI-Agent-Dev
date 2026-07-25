#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/ensure_modern_bash.sh"
ensure_modern_bash "$@"

PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
DEV_STATUS_RUNTIME_DIR="${DEV_STATUS_RUNTIME_DIR:-${PROJECT_ROOT}/runtime}"

source "${SCRIPT_DIR}/local_dev_proxy_guard.sh"
source "${SCRIPT_DIR}/dev_process_helpers.sh"

DEV_STATUS_API_PORT="${DEV_STATUS_API_PORT:-8765}"
DEV_STATUS_FRONTEND_PORT="${DEV_STATUS_FRONTEND_PORT:-4173}"
DEV_STATUS_API_BASE_URL="${DEV_STATUS_API_BASE_URL:-http://127.0.0.1:${DEV_STATUS_API_PORT}}"
DEV_STATUS_FRONTEND_BASE_URL="${DEV_STATUS_FRONTEND_BASE_URL:-http://127.0.0.1:${DEV_STATUS_FRONTEND_PORT}}"
DEV_STATUS_API_BASE_URL_EXPLICIT=0
DEV_STATUS_FRONTEND_BASE_URL_EXPLICIT=0

_dev_status_usage() {
  cat <<'EOF'
Usage:
  bash ./scripts/dev_status.sh
  bash ./scripts/dev_status.sh --api-port 8766 --frontend-port 4174 --api-base-url http://localhost:8766
  bash ./scripts/dev_status.sh --runtime-dir runtime/test_env --api-port 8775 --frontend-port 4175

What it does:
  - checks which local processes are listening on the configured backend / frontend ports
  - probes backend health and runtime progress when the backend port is live
  - probes the frontend root URL when the frontend port is live

Options:
  --api-port <port>               Backend API port. Default: 8765
  --frontend-port <port>          Frontend port. Default: 4173
  --runtime-dir <path>            Runtime dir / service namespace. Default: <project>/runtime
  --api-base-url <url>            Backend base URL for health checks. Default: http://127.0.0.1:<api-port>
  --frontend-base-url <url>       Frontend base URL for root probe. Default: http://127.0.0.1:<frontend-port>
  --help                          Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-port)
      DEV_STATUS_API_PORT="${2-}"
      shift 2
      ;;
    --frontend-port)
      DEV_STATUS_FRONTEND_PORT="${2-}"
      shift 2
      ;;
    --runtime-dir)
      DEV_STATUS_RUNTIME_DIR="${2-}"
      shift 2
      ;;
    --api-base-url)
      DEV_STATUS_API_BASE_URL="${2-}"
      DEV_STATUS_API_BASE_URL_EXPLICIT=1
      shift 2
      ;;
    --frontend-base-url)
      DEV_STATUS_FRONTEND_BASE_URL="${2-}"
      DEV_STATUS_FRONTEND_BASE_URL_EXPLICIT=1
      shift 2
      ;;
    --help|-h)
      _dev_status_usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      _dev_status_usage >&2
      exit 2
      ;;
  esac
done

if [[ $DEV_STATUS_API_BASE_URL_EXPLICIT -eq 0 ]]; then
  DEV_STATUS_API_BASE_URL="http://127.0.0.1:${DEV_STATUS_API_PORT}"
fi
if [[ $DEV_STATUS_FRONTEND_BASE_URL_EXPLICIT -eq 0 ]]; then
  DEV_STATUS_FRONTEND_BASE_URL="http://127.0.0.1:${DEV_STATUS_FRONTEND_PORT}"
fi
if [[ "${DEV_STATUS_RUNTIME_DIR}" != /* ]]; then
  DEV_STATUS_RUNTIME_DIR="${PROJECT_ROOT}/${DEV_STATUS_RUNTIME_DIR}"
fi

_dev_status_http_probe() {
  local label="${1-}"
  local url="${2-}"
  local http_code=""
  if http_code="$(curl -sS -o /dev/null -w '%{http_code}' "$url" 2>/dev/null)"; then
    printf '  %-18s ok (%s)\n' "${label}:" "$http_code"
    return 0
  fi
  printf '  %-18s unreachable\n' "${label}:"
  return 1
}

_dev_status_service_summary() {
  local service_dir="${1-}"
  local label="${2-}"
  local status_file="${service_dir}/status.json"
  local pid=""
  local service_name=""
  local cycle_state=""

  if [[ ! -f "$status_file" ]]; then
    return 1
  fi

  pid="$(dev_status_pid_from_file "$status_file" || true)"
  service_name="$(sed -n 's/.*"service_name":[[:space:]]*"\([^"]*\)".*/\1/p' "$status_file" | head -n 1)"
  cycle_state="$(sed -n 's/.*"cycle_state":[[:space:]]*"\([^"]*\)".*/\1/p' "$status_file" | head -n 1)"
  if dev_pid_is_running "$pid"; then
    printf '  %-18s active pid=%s service=%s cycle=%s\n' "${label}:" "$pid" "${service_name:-unknown}" "${cycle_state:-unknown}"
    return 0
  fi
  printf '  %-18s stale status file (pid=%s) at %s\n' "${label}:" "${pid:-unknown}" "$status_file"
  return 0
}

_dev_status_pid_file_summary() {
  local pid_file="${1-}"
  local label="${2-}"
  local pid=""

  if [[ ! -f "$pid_file" ]]; then
    return 1
  fi
  pid="$(tr -cd '0-9' < "$pid_file" || true)"
  if dev_pid_is_running "$pid"; then
    printf '  %-18s active pid=%s file=%s\n' "${label}:" "$pid" "$pid_file"
    ps -p "$pid" -o pid=,ppid=,cmd= 2>/dev/null | sed 's/^/  process:            /'
    return 0
  fi
  printf '  %-18s stale pid=%s at %s\n' "${label}:" "${pid:-unknown}" "$pid_file"
  return 0
}

printf 'Local Dev Status\n'
printf '  project_root:        %s\n' "$PROJECT_ROOT"
printf '  runtime_dir:         %s\n' "$DEV_STATUS_RUNTIME_DIR"
printf '  backend_port:        %s\n' "$DEV_STATUS_API_PORT"
printf '  frontend_port:       %s\n' "$DEV_STATUS_FRONTEND_PORT"
printf '  backend_base_url:    %s\n' "$DEV_STATUS_API_BASE_URL"
printf '  frontend_base_url:   %s\n' "$DEV_STATUS_FRONTEND_BASE_URL"
printf '  NO_PROXY:            %s\n' "${NO_PROXY-}"

mapfile -t backend_pids < <(dev_listen_pids_by_port "$DEV_STATUS_API_PORT" || true)
mapfile -t frontend_pids < <(dev_listen_pids_by_port "$DEV_STATUS_FRONTEND_PORT" || true)
mapfile -t daemon_lines < <(pgrep -af "sourcing_agent\\.cli run-worker-daemon-service" || true)
backend_health_rc=0
backend_runtime_rc=0
frontend_root_rc=0

printf '\nBackend\n'
if [[ ${#backend_pids[@]} -eq 0 ]]; then
  printf '  listeners:          none on tcp:%s\n' "$DEV_STATUS_API_PORT"
else
  printf '  listeners:          %s\n' "${backend_pids[*]}"
  dev_print_process_lines "${backend_pids[@]}" | sed 's/^/  process:            /'
fi
_dev_status_http_probe "health" "${DEV_STATUS_API_BASE_URL%/}/health" || backend_health_rc=$?
_dev_status_http_probe "runtime_progress" "${DEV_STATUS_API_BASE_URL%/}/api/runtime/progress" || backend_runtime_rc=$?
if [[ ${#backend_pids[@]} -eq 0 && ( $backend_health_rc -eq 0 || $backend_runtime_rc -eq 0 ) ]]; then
  printf '  listener_note:      HTTP reachable even though no listener pid is visible in this shell\n'
fi

printf '\nFrontend\n'
if [[ ${#frontend_pids[@]} -eq 0 ]]; then
  printf '  listeners:          none on tcp:%s\n' "$DEV_STATUS_FRONTEND_PORT"
else
  printf '  listeners:          %s\n' "${frontend_pids[*]}"
  dev_print_process_lines "${frontend_pids[@]}" | sed 's/^/  process:            /'
fi
_dev_status_http_probe "root" "${DEV_STATUS_FRONTEND_BASE_URL%/}/" || frontend_root_rc=$?
if [[ ${#frontend_pids[@]} -eq 0 && $frontend_root_rc -eq 0 ]]; then
  printf '  listener_note:      HTTP reachable even though no listener pid is visible in this shell\n'
fi

printf '\nRuntime Services\n'
service_found=0
if _dev_status_service_summary "${DEV_STATUS_RUNTIME_DIR}/services/server-runtime-watchdog" "watchdog_shared"; then
  service_found=1
fi
if _dev_status_service_summary "${DEV_STATUS_RUNTIME_DIR}/services/server-runtime-watchdog-live${DEV_STATUS_API_PORT}" "watchdog_port"; then
  service_found=1
fi
if _dev_status_service_summary "${DEV_STATUS_RUNTIME_DIR}/services/worker-recovery-daemon" "recovery_shared"; then
  service_found=1
fi
if _dev_status_service_summary "${DEV_STATUS_RUNTIME_DIR}/services/worker-recovery-daemon-live${DEV_STATUS_API_PORT}" "recovery_port"; then
  service_found=1
fi
if [[ $service_found -eq 0 ]]; then
  printf '  services:           no matching runtime service status files\n'
fi

printf '\nWorker Daemon Process\n'
runtime_worker_found=0
if _dev_status_pid_file_summary "${DEV_STATUS_RUNTIME_DIR}/service_logs/dev-worker-daemon.pid" "runtime_worker"; then
  runtime_worker_found=1
fi
if [[ ${#daemon_lines[@]} -eq 0 ]]; then
  if [[ $runtime_worker_found -eq 0 ]]; then
    printf '  processes:          none detected via pgrep\n'
  fi
else
  printf '  global_matches:\n'
  printf '%s\n' "${daemon_lines[@]}" | sed 's/^/    /'
fi
