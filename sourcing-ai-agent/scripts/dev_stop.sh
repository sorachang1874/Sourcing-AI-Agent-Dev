#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/ensure_modern_bash.sh"
ensure_modern_bash "$@"

PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

source "${SCRIPT_DIR}/dev_process_helpers.sh"

DEV_STOP_API_PORT="${DEV_STOP_API_PORT:-8765}"
DEV_STOP_FRONTEND_PORT="${DEV_STOP_FRONTEND_PORT:-4173}"
DEV_STOP_RUNTIME_DIR="${DEV_STOP_RUNTIME_DIR:-${PROJECT_ROOT}/runtime}"

_dev_stop_usage() {
  cat <<'EOF'
Usage:
  bash ./scripts/dev_stop.sh
  bash ./scripts/dev_stop.sh --runtime-dir runtime/test_env
  bash ./scripts/dev_stop.sh --api-port 8766 --frontend-port 4174

What it does:
  - sends SIGTERM to any process listening on the configured backend / frontend ports
  - kills matching backend helper wrappers and runtime service daemons under the selected runtime dir
  - relies on the backend helper's shell trap to clean up the paired worker daemon

Options:
  --runtime-dir <path>             Runtime dir / service namespace. Default: <project>/runtime
  --api-port <port>               Backend API port. Default: 8765
  --frontend-port <port>          Frontend port. Default: 4173
  --help                          Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runtime-dir)
      DEV_STOP_RUNTIME_DIR="${2-}"
      shift 2
      ;;
    --api-port)
      DEV_STOP_API_PORT="${2-}"
      shift 2
      ;;
    --frontend-port)
      DEV_STOP_FRONTEND_PORT="${2-}"
      shift 2
      ;;
    --help|-h)
      _dev_stop_usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      _dev_stop_usage >&2
      exit 2
      ;;
  esac
done

if [[ "${DEV_STOP_RUNTIME_DIR}" != /* ]]; then
  DEV_STOP_RUNTIME_DIR="${PROJECT_ROOT}/${DEV_STOP_RUNTIME_DIR}"
fi

_dev_stop_kill_listeners() {
  local label="${1-}"
  local port="${2-}"
  local -a pids=()
  mapfile -t pids < <(dev_listen_pids_by_port "$port" || true)

  if [[ ${#pids[@]} -eq 0 ]]; then
    printf '%s: no listener on tcp:%s\n' "$label" "$port"
    return 0
  fi

  printf '%s: stopping listener(s) on tcp:%s -> %s\n' "$label" "$port" "${pids[*]}"
  kill "${pids[@]}" 2>/dev/null || true
  sleep 1

  mapfile -t pids < <(dev_listen_pids_by_port "$port" || true)
  if [[ ${#pids[@]} -eq 0 ]]; then
    printf '%s: stopped\n' "$label"
    return 0
  fi

  printf '%s: still listening after SIGTERM -> %s\n' "$label" "${pids[*]}" >&2
  return 1
}

_dev_stop_kill_backend_wrappers() {
  local port="${1-}"
  local -a pids=()
  local line=""
  local pid=""
  local cmd=""

  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    pid="${line%% *}"
    cmd="${line#* }"
    if [[ "$cmd" == *"pgrep -af"* ]]; then
      continue
    fi
    if [[ "$cmd" != *"scripts/dev_backend.sh"* ]] && [[ "$cmd" != *"scripts/run_hosted_trial_backend.sh"* ]]; then
      continue
    fi
    if [[ " ${cmd} " != *" --port ${port} "* ]]; then
      continue
    fi
    pids+=("$pid")
  done < <(pgrep -af "scripts/dev_backend\\.sh|scripts/run_hosted_trial_backend\\.sh" || true)

  if [[ ${#pids[@]} -eq 0 ]]; then
    printf 'backend_wrapper: no wrapper process for port %s\n' "$port"
    return 0
  fi

  printf 'backend_wrapper: stopping wrapper process(es) for port %s -> %s\n' "$port" "${pids[*]}"
  kill "${pids[@]}" 2>/dev/null || true
  sleep 1

  local -a remaining=()
  mapfile -t remaining < <(ps -o pid= -p "${pids[@]}" 2>/dev/null | awk '{print $1}')
  if [[ ${#remaining[@]} -eq 0 ]]; then
    printf 'backend_wrapper: stopped\n'
    return 0
  fi

  printf 'backend_wrapper: escalating to SIGKILL -> %s\n' "${remaining[*]}" >&2
  kill -9 "${remaining[@]}" 2>/dev/null || true
  sleep 1
  mapfile -t remaining < <(ps -o pid= -p "${pids[@]}" 2>/dev/null | awk '{print $1}')
  if [[ ${#remaining[@]} -eq 0 ]]; then
    printf 'backend_wrapper: killed\n'
    return 0
  fi

  printf 'backend_wrapper: still running after SIGKILL -> %s\n' "${remaining[*]}" >&2
  return 1
}

_dev_stop_kill_service_pid() {
  local label="${1-}"
  local status_file="${2-}"
  local pid=""

  pid="$(dev_status_pid_from_file "$status_file" || true)"
  if [[ -z "$pid" ]]; then
    printf '%s: no status pid at %s\n' "$label" "$status_file"
    return 0
  fi
  if ! dev_pid_is_running "$pid"; then
    printf '%s: stale status pid=%s at %s\n' "$label" "$pid" "$status_file"
    return 0
  fi

  printf '%s: stopping pid=%s from %s\n' "$label" "$pid" "$status_file"
  kill "$pid" 2>/dev/null || true
  sleep 1
  if ! dev_pid_is_running "$pid"; then
    printf '%s: stopped\n' "$label"
    return 0
  fi

  printf '%s: escalating to SIGKILL -> %s\n' "$label" "$pid" >&2
  kill -9 "$pid" 2>/dev/null || true
  sleep 1
  if ! dev_pid_is_running "$pid"; then
    printf '%s: killed\n' "$label"
    return 0
  fi

  printf '%s: still running after SIGKILL -> %s\n' "$label" "$pid" >&2
  return 1
}

_dev_stop_kill_pid_file() {
  local label="${1-}"
  local pid_file="${2-}"
  local pid=""

  if [[ ! -f "$pid_file" ]]; then
    printf '%s: no pid file at %s\n' "$label" "$pid_file"
    return 0
  fi

  pid="$(tr -cd '0-9' < "$pid_file" || true)"
  if [[ -z "$pid" ]]; then
    printf '%s: invalid pid file at %s\n' "$label" "$pid_file"
    rm -f -- "$pid_file"
    return 0
  fi
  if ! dev_pid_is_running "$pid"; then
    printf '%s: stale pid=%s at %s\n' "$label" "$pid" "$pid_file"
    rm -f -- "$pid_file"
    return 0
  fi

  printf '%s: stopping pid=%s from %s\n' "$label" "$pid" "$pid_file"
  kill "$pid" 2>/dev/null || true
  sleep 1
  if ! dev_pid_is_running "$pid"; then
    rm -f -- "$pid_file"
    printf '%s: stopped\n' "$label"
    return 0
  fi

  printf '%s: escalating to SIGKILL -> %s\n' "$label" "$pid" >&2
  kill -9 "$pid" 2>/dev/null || true
  sleep 1
  if ! dev_pid_is_running "$pid"; then
    rm -f -- "$pid_file"
    printf '%s: killed\n' "$label"
    return 0
  fi

  printf '%s: still running after SIGKILL -> %s\n' "$label" "$pid" >&2
  return 1
}

printf 'Stopping local dev services under %s\n' "$PROJECT_ROOT"
stop_failed=0
_dev_stop_kill_listeners "backend" "$DEV_STOP_API_PORT" || stop_failed=1
_dev_stop_kill_listeners "frontend" "$DEV_STOP_FRONTEND_PORT" || stop_failed=1
_dev_stop_kill_backend_wrappers "$DEV_STOP_API_PORT" || stop_failed=1
_dev_stop_kill_pid_file "worker_daemon" "${DEV_STOP_RUNTIME_DIR}/service_logs/dev-worker-daemon.pid" || stop_failed=1
_dev_stop_kill_service_pid "watchdog_shared" "${DEV_STOP_RUNTIME_DIR}/services/server-runtime-watchdog/status.json" || stop_failed=1
_dev_stop_kill_service_pid "watchdog_port" "${DEV_STOP_RUNTIME_DIR}/services/server-runtime-watchdog-live${DEV_STOP_API_PORT}/status.json" || stop_failed=1
_dev_stop_kill_service_pid "recovery_shared" "${DEV_STOP_RUNTIME_DIR}/services/worker-recovery-daemon/status.json" || stop_failed=1
_dev_stop_kill_service_pid "recovery_port" "${DEV_STOP_RUNTIME_DIR}/services/worker-recovery-daemon-live${DEV_STOP_API_PORT}/status.json" || stop_failed=1

if [[ $stop_failed -ne 0 ]]; then
  printf 'One or more listeners are still active. Re-run make dev-status to inspect residual processes.\n' >&2
  exit 1
fi

printf 'Requested local dev stop completed.\n'
