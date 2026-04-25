#!/usr/bin/env bash

_LOCAL_DEV_PROXY_GUARD_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${_LOCAL_DEV_PROXY_GUARD_DIR}/ensure_modern_bash.sh"
ensure_modern_bash "$@"

_local_dev_proxy_guard_defaults=("127.0.0.1" "localhost" "::1" "0.0.0.0")

_local_dev_proxy_guard_trim() {
  local value="${1-}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

_local_dev_proxy_guard_build() {
  local combined="${NO_PROXY-}"
  if [[ -z "$combined" && -n "${no_proxy-}" ]]; then
    combined="${no_proxy-}"
  fi

  local -a values=()
  local -A seen=()
  local item=""
  local key=""
  local IFS=','

  if [[ -n "$combined" ]]; then
    read -r -a values <<< "$combined"
  fi

  for item in "${_local_dev_proxy_guard_defaults[@]}"; do
    values+=("$item")
  done

  local -a normalized=()
  for item in "${values[@]}"; do
    item="$(_local_dev_proxy_guard_trim "$item")"
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

_local_dev_proxy_guard_apply() {
  local merged=""
  merged="$(_local_dev_proxy_guard_build)"
  export NO_PROXY="$merged"
  export no_proxy="$merged"
}

_local_dev_proxy_guard_usage() {
  cat <<'EOF'
Usage:
  source ./scripts/local_dev_proxy_guard.sh
  ./scripts/local_dev_proxy_guard.sh --print
  ./scripts/local_dev_proxy_guard.sh <command> [args...]

What it does:
  - Appends 127.0.0.1, localhost, ::1, 0.0.0.0 to NO_PROXY / no_proxy
  - Helps curl / Python / Node local dev calls avoid fake localhost 502s caused by terminal proxy env

Examples:
  source ./scripts/local_dev_proxy_guard.sh
  ./scripts/local_dev_proxy_guard.sh curl http://127.0.0.1:8765/health
  ./scripts/local_dev_proxy_guard.sh env SOURCING_API_ALLOWED_ORIGINS=http://localhost:4173,http://127.0.0.1:4173 PYTHONPATH=src python3 -m sourcing_agent.cli serve --host 0.0.0.0 --port 8765
EOF
}

_local_dev_proxy_guard_is_sourced() {
  [[ "${BASH_SOURCE[0]}" != "$0" ]]
}

_local_dev_proxy_guard_main() {
  _local_dev_proxy_guard_apply

  if _local_dev_proxy_guard_is_sourced; then
    if [[ "${1-}" == "--print" ]]; then
      printf 'NO_PROXY=%s\n' "${NO_PROXY-}"
    fi
    return 0
  fi

  case "${1-}" in
    ""|"--help"|"-h")
      _local_dev_proxy_guard_usage
      ;;
    "--print")
      printf 'NO_PROXY=%s\n' "${NO_PROXY-}"
      ;;
    *)
      exec "$@"
      ;;
  esac
}

_local_dev_proxy_guard_main "$@"
