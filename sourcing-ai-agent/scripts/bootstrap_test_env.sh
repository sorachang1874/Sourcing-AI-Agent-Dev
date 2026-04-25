#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/ensure_modern_bash.sh"
ensure_modern_bash "$@"

PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/python_env_guard.sh"
REPO_PREFERRED_PYTHON_BIN="$(resolve_preferred_repo_python_bin "${PROJECT_ROOT}")"

BOOTSTRAP_PYTHON_BIN="$(resolve_project_python_bin "${PROJECT_ROOT}" "${BOOTSTRAP_TEST_ENV_PYTHON_BIN-}")"
TEST_VENV_DIR="${PROJECT_ROOT}/.venv-tests"
TEST_PYTHON_BIN="${TEST_VENV_DIR}/bin/python"
VALIDATE_ONLY=0
FORCE_RECREATE=0
PRINT_CONFIG=0

TEST_ENV_STATUS=""
TEST_ENV_REASON=""
TEST_ENV_VERSION=""
TEST_ENV_EXECUTABLE=""
TEST_ENV_REAL_EXECUTABLE=""
TEST_ENV_PREFIX=""
TEST_ENV_BASE_PREFIX=""
TEST_ENV_MISSING_MODULES=""

_bootstrap_test_env_usage() {
  cat <<'EOF'
Usage:
  bash ./scripts/bootstrap_test_env.sh
  bash ./scripts/bootstrap_test_env.sh --validate-only
  bash ./scripts/bootstrap_test_env.sh --force-recreate
  BOOTSTRAP_TEST_ENV_PYTHON_BIN=./.venv/bin/python bash ./scripts/bootstrap_test_env.sh

What it does:
  - validates ./.venv-tests against repo expectations
  - recreates it with `python -m venv --copies` when missing/broken
  - installs the repo in editable mode with dev + test extras
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python-bin)
      BOOTSTRAP_PYTHON_BIN="$(resolve_project_python_bin "${PROJECT_ROOT}" "${2-}")"
      shift 2
      ;;
    --validate-only)
      VALIDATE_ONLY=1
      shift
      ;;
    --force-recreate)
      FORCE_RECREATE=1
      shift
      ;;
    --print-config)
      PRINT_CONFIG=1
      shift
      ;;
    --help|-h)
      _bootstrap_test_env_usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      _bootstrap_test_env_usage >&2
      exit 2
      ;;
  esac
done

_reset_test_env_probe_state() {
  TEST_ENV_STATUS=""
  TEST_ENV_REASON=""
  TEST_ENV_VERSION=""
  TEST_ENV_EXECUTABLE=""
  TEST_ENV_REAL_EXECUTABLE=""
  TEST_ENV_PREFIX=""
  TEST_ENV_BASE_PREFIX=""
  TEST_ENV_MISSING_MODULES=""
}

_probe_test_env() {
  _reset_test_env_probe_state
  if [[ ! -x "${TEST_PYTHON_BIN}" ]]; then
    TEST_ENV_STATUS="missing"
    TEST_ENV_REASON="python_missing"
    return 1
  fi

  local probe_output=""
  if ! probe_output="$("${TEST_PYTHON_BIN}" - <<'PY' 2>/dev/null
import importlib
import os
import sys

required_modules = (
    "pytest",
    "requests",
    "psycopg",
    "ruff",
    "mypy",
    "sourcing_agent",
)
missing_modules = []
for module_name in required_modules:
    try:
        importlib.import_module(module_name)
    except Exception:
        missing_modules.append(module_name)

print(f"version={sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
print(f"version_ok={1 if sys.version_info >= (3, 12) else 0}")
print(f"executable={sys.executable}")
print(f"real_executable={os.path.realpath(sys.executable)}")
print(f"prefix={sys.prefix}")
print(f"base_prefix={sys.base_prefix}")
print(f"missing_modules={','.join(missing_modules)}")
PY
)"; then
    TEST_ENV_STATUS="broken"
    TEST_ENV_REASON="python_not_runnable"
    return 1
  fi

  while IFS='=' read -r key value; do
    case "${key}" in
      version)
        TEST_ENV_VERSION="${value}"
        ;;
      version_ok)
        if [[ "${value}" != "1" ]]; then
          TEST_ENV_STATUS="broken"
          TEST_ENV_REASON="python_version_too_old"
        fi
        ;;
      executable)
        TEST_ENV_EXECUTABLE="${value}"
        ;;
      real_executable)
        TEST_ENV_REAL_EXECUTABLE="${value}"
        ;;
      prefix)
        TEST_ENV_PREFIX="${value}"
        ;;
      base_prefix)
        TEST_ENV_BASE_PREFIX="${value}"
        ;;
      missing_modules)
        TEST_ENV_MISSING_MODULES="${value}"
        if [[ -n "${value}" ]]; then
          TEST_ENV_STATUS="broken"
          TEST_ENV_REASON="missing_modules:${value}"
        fi
        ;;
    esac
  done <<< "${probe_output}"

  if [[ -z "${TEST_ENV_STATUS}" ]]; then
    TEST_ENV_STATUS="ready"
    TEST_ENV_REASON="ok"
    return 0
  fi
  return 1
}

_validate_builder_python() {
  local builder_python="${1:?builder_python is required}"
  if [[ "${PRINT_CONFIG}" -eq 1 ]]; then
    return 0
  fi
  require_python_modules_or_exit "${builder_python}" venv pip
  "${builder_python}" - <<'PY' >/dev/null
import sys
if sys.version_info < (3, 12):
    raise SystemExit("bootstrap python must be >= 3.12")
PY
}

_print_summary() {
  printf 'test_env_dir=%s\n' "${TEST_VENV_DIR}"
  printf 'bootstrap_python=%s\n' "${BOOTSTRAP_PYTHON_BIN}"
  printf 'status=%s\n' "${TEST_ENV_STATUS:-unknown}"
  printf 'reason=%s\n' "${TEST_ENV_REASON:-unknown}"
  if [[ -n "${TEST_ENV_VERSION}" ]]; then
    printf 'python_version=%s\n' "${TEST_ENV_VERSION}"
  fi
  if [[ -n "${TEST_ENV_EXECUTABLE}" ]]; then
    printf 'python_executable=%s\n' "${TEST_ENV_EXECUTABLE}"
  fi
  if [[ -n "${TEST_ENV_REAL_EXECUTABLE}" ]]; then
    printf 'python_real_executable=%s\n' "${TEST_ENV_REAL_EXECUTABLE}"
  fi
  if [[ -n "${TEST_ENV_PREFIX}" ]]; then
    printf 'python_prefix=%s\n' "${TEST_ENV_PREFIX}"
  fi
  if [[ -n "${TEST_ENV_BASE_PREFIX}" ]]; then
    printf 'python_base_prefix=%s\n' "${TEST_ENV_BASE_PREFIX}"
  fi
  if [[ -n "${TEST_ENV_MISSING_MODULES}" ]]; then
    printf 'missing_modules=%s\n' "${TEST_ENV_MISSING_MODULES}"
  fi
}

_recreate_test_env() {
  rm -rf -- "${TEST_VENV_DIR}"
  "${BOOTSTRAP_PYTHON_BIN}" -m venv --copies "${TEST_VENV_DIR}"
  PIP_DISABLE_PIP_VERSION_CHECK=1 "${TEST_PYTHON_BIN}" -m pip install --upgrade pip
  PIP_DISABLE_PIP_VERSION_CHECK=1 "${TEST_PYTHON_BIN}" -m pip install -e ".[dev,test]"
}

if [[ "${PRINT_CONFIG}" -eq 1 ]]; then
  _probe_test_env >/dev/null 2>&1 || true
  _print_summary
  exit 0
fi

_validate_builder_python "${BOOTSTRAP_PYTHON_BIN}"

if [[ "${FORCE_RECREATE}" -eq 1 ]]; then
  _recreate_test_env
fi

if ! _probe_test_env; then
  if [[ "${VALIDATE_ONLY}" -eq 1 ]]; then
    _print_summary
    exit 1
  fi
  _recreate_test_env
  _probe_test_env
fi

_print_summary
