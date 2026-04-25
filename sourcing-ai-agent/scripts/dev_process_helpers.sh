#!/usr/bin/env bash

dev_pid_is_running() {
  local pid="${1-}"
  if [[ -z "$pid" || ! "$pid" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  kill -0 "$pid" 2>/dev/null
}

dev_status_pid_from_file() {
  local status_file="${1-}"
  if [[ -z "$status_file" || ! -f "$status_file" ]]; then
    return 1
  fi
  sed -n 's/.*"pid":[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$status_file" | head -n 1
}

dev_listen_pids_by_port() {
  local port="${1-}"
  if [[ -z "$port" ]]; then
    return 2
  fi

  if command -v ss >/dev/null 2>&1; then
    ss -ltnp 2>/dev/null \
      | awk -v target=":$port" '$4 ~ target || $5 ~ target' \
      | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' \
      | awk '!seen[$0]++'
    return 0
  fi

  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null \
      | awk 'NR > 1 {print $2}' \
      | awk '!seen[$0]++'
    return 0
  fi

  printf 'Neither lsof nor ss is available to inspect port %s\n' "$port" >&2
  return 1
}

dev_print_process_lines() {
  local pid=""
  for pid in "$@"; do
    ps -p "$pid" -o pid=,ppid=,cmd= 2>/dev/null || true
  done
}
