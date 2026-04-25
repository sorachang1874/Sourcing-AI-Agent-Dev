#!/usr/bin/env bash

ensure_modern_bash() {
  local required_major="4"
  if [[ $# -gt 0 && "$1" =~ ^[0-9]+$ ]]; then
    required_major="$1"
    shift
  fi
  local current_major="0"
  if [[ -n "${BASH_VERSINFO-}" ]]; then
    current_major="${BASH_VERSINFO[0]}"
  fi
  if [[ "${current_major}" -ge "${required_major}" ]]; then
    return 0
  fi

  local candidate=""
  for candidate in /opt/homebrew/bin/bash /usr/local/bin/bash; do
    if [[ -x "${candidate}" ]]; then
      if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
        printf 'This shell session is using bash %s; please re-run under %s.\n' "${BASH_VERSION:-unknown}" "${candidate}" >&2
        return 1
      fi
      exec "${candidate}" "$0" "$@"
    fi
  done

  printf 'This script requires bash >= %s, but current bash is %s and no Homebrew bash was found.\n' \
    "${required_major}" "${BASH_VERSION:-unknown}" >&2
  if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
    return 1
  fi
  exit 1
}
