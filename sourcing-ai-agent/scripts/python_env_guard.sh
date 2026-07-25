#!/usr/bin/env bash

resolve_preferred_repo_python_bin() {
  local project_root="${1:?project_root is required}"

  if [[ -x "${project_root}/.venv/bin/python" ]]; then
    printf '%s' "${project_root}/.venv/bin/python"
    return 0
  fi

  if [[ -x "${project_root}/.venv-tests/bin/python" ]]; then
    printf '%s' "${project_root}/.venv-tests/bin/python"
    return 0
  fi

  printf '%s' "python3"
}

resolve_project_python_bin() {
  local project_root="${1:?project_root is required}"
  local requested_python="${2-}"
  local normalized_requested="${requested_python}"

  if [[ -n "${normalized_requested}" ]]; then
    if [[ "${normalized_requested}" != /* ]] && [[ "${normalized_requested}" == *"/"* ]]; then
      normalized_requested="${project_root}/${normalized_requested#./}"
    fi
    printf '%s' "${normalized_requested}"
    return 0
  fi

  if [[ -x "${project_root}/.venv/bin/python" ]]; then
    printf '%s' "${project_root}/.venv/bin/python"
    return 0
  fi

  if [[ -x "${project_root}/.venv-tests/bin/python" ]]; then
    printf '%s' "${project_root}/.venv-tests/bin/python"
    return 0
  fi

  printf '%s' "python3"
}

python_bin_imports_modules() {
  local python_bin="${1:?python_bin is required}"
  shift || true
  if [[ $# -eq 0 ]]; then
    return 0
  fi
  "${python_bin}" - "$@" <<'PY'
import importlib
import sys

missing = []
for module_name in sys.argv[1:]:
    try:
        importlib.import_module(module_name)
    except Exception:
        missing.append(module_name)

if missing:
    print(",".join(missing))
    raise SystemExit(1)
PY
}

require_python_modules_or_exit() {
  local python_bin="${1:?python_bin is required}"
  shift || true
  if [[ $# -eq 0 ]]; then
    return 0
  fi
  local missing_modules=""
  if missing_modules="$(python_bin_imports_modules "${python_bin}" "$@" 2>/dev/null)"; then
    return 0
  fi
  printf 'Selected Python runtime %s is missing required module(s): %s\n' "${python_bin}" "${missing_modules}" >&2
  if [[ -n "${REPO_PREFERRED_PYTHON_BIN:-}" ]]; then
    printf 'Prefer the repository virtualenv: %s\n' "${REPO_PREFERRED_PYTHON_BIN}" >&2
  fi
  printf 'If you intentionally want another interpreter, set DEV_PYTHON_BIN/HOSTED_PYTHON_BIN explicitly.\n' >&2
  return 2
}
