#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
FRONTEND_DIR="${PROJECT_ROOT}/frontend-demo"

source "${SCRIPT_DIR}/local_dev_proxy_guard.sh"

DEV_FRONTEND_HOST="${DEV_FRONTEND_HOST:-0.0.0.0}"
DEV_FRONTEND_PORT="${DEV_FRONTEND_PORT:-4173}"
DEV_FRONTEND_API_BASE_URL="${VITE_API_BASE_URL:-http://127.0.0.1:8765}"
DEV_FRONTEND_PROXY_MODE="${DEV_FRONTEND_PROXY_MODE:-direct}"
RUN_INSTALL=0
PRINT_CONFIG=0

_dev_frontend_usage() {
  cat <<'EOF'
Usage:
  bash ./scripts/dev_frontend.sh
  bash ./scripts/dev_frontend.sh --print-config
  bash ./scripts/dev_frontend.sh --api-base-url http://localhost:8766 --port 4174
  bash ./scripts/dev_frontend.sh --install

What it does:
  - sources local_dev_proxy_guard.sh
  - starts Vite from frontend-demo with direct backend access by default for local dev

Options:
  --host <host>             Vite bind host. Default: 0.0.0.0
  --port <port>             Vite bind port. Default: 4173
  --api-base-url <url>      Backend API URL. Default: http://127.0.0.1:8765
  --install                 Run npm install before starting dev server.
  --print-config            Print resolved config and exit.
  --help                    Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      DEV_FRONTEND_HOST="${2-}"
      shift 2
      ;;
    --port)
      DEV_FRONTEND_PORT="${2-}"
      shift 2
      ;;
    --api-base-url)
      DEV_FRONTEND_API_BASE_URL="${2-}"
      shift 2
      ;;
    --install)
      RUN_INSTALL=1
      shift
      ;;
    --print-config)
      PRINT_CONFIG=1
      shift
      ;;
    --help|-h)
      _dev_frontend_usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      _dev_frontend_usage >&2
      exit 2
      ;;
  esac
done

if [[ $PRINT_CONFIG -eq 1 ]]; then
  printf 'project_root=%s\n' "$PROJECT_ROOT"
  printf 'frontend_dir=%s\n' "$FRONTEND_DIR"
  printf 'frontend_host=%s\n' "$DEV_FRONTEND_HOST"
  printf 'frontend_port=%s\n' "$DEV_FRONTEND_PORT"
  printf 'frontend_api_mode=%s\n' "$DEV_FRONTEND_PROXY_MODE"
  printf 'frontend_proxy_target=%s\n' "$DEV_FRONTEND_API_BASE_URL"
  printf 'NO_PROXY=%s\n' "${NO_PROXY-}"
  exit 0
fi

cd "${FRONTEND_DIR}"

if [[ $RUN_INSTALL -eq 1 ]]; then
  npm install
fi

printf 'Starting local frontend at http://%s:%s\n' "${DEV_FRONTEND_HOST}" "${DEV_FRONTEND_PORT}"
printf 'Using frontend API mode: %s\n' "${DEV_FRONTEND_PROXY_MODE}"
printf 'Proxying /api requests to: %s\n' "${DEV_FRONTEND_API_BASE_URL}"
printf 'Clearing shell proxy env for local Vite proxy traffic.\n'

resolved_api_base_url="${DEV_FRONTEND_API_BASE_URL}"
resolved_proxy_target="${DEV_FRONTEND_API_BASE_URL}"
if [[ "${DEV_FRONTEND_PROXY_MODE}" == "same-origin" ]]; then
  resolved_api_base_url="same-origin"
fi

env \
  -u http_proxy \
  -u https_proxy \
  -u HTTP_PROXY \
  -u HTTPS_PROXY \
  -u all_proxy \
  -u ALL_PROXY \
  VITE_API_BASE_URL="${resolved_api_base_url}" \
  VITE_DEV_PROXY_TARGET="${resolved_proxy_target}" \
  npm run dev -- --host "${DEV_FRONTEND_HOST}" --port "${DEV_FRONTEND_PORT}"
