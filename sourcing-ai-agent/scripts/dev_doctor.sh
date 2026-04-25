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

DEV_DOCTOR_PYTHON_BIN="$(resolve_project_python_bin "${PROJECT_ROOT}" "${DEV_PYTHON_BIN-}")"
TEST_PYTHON_BIN="${PROJECT_ROOT}/.venv-tests/bin/python"
FRONTEND_DIR="${PROJECT_ROOT}/frontend-demo"
doctor_failed=0

_dev_doctor_usage() {
  cat <<'EOF'
Usage:
  bash ./scripts/dev_doctor.sh

What it checks:
  - active bash path/version
  - selected runtime python and required imports
  - selected test python
  - node/npm presence for frontend
  - backend helper resolved config
  - control-plane runtime resolution
EOF
}

if [[ "${1-}" == "--help" || "${1-}" == "-h" ]]; then
  _dev_doctor_usage
  exit 0
fi

printf 'Local Dev Doctor\n'
printf '  project_root:        %s\n' "${PROJECT_ROOT}"
printf '  bash_path:           %s\n' "${BASH:-unknown}"
printf '  bash_version:        %s\n' "${BASH_VERSION:-unknown}"
printf '  runtime_python:      %s\n' "${DEV_DOCTOR_PYTHON_BIN}"
if [[ -x "${TEST_PYTHON_BIN}" ]]; then
  printf '  test_python:         %s\n' "${TEST_PYTHON_BIN}"
else
  printf '  test_python:         missing (%s)\n' "${TEST_PYTHON_BIN}"
fi

require_python_modules_or_exit "${DEV_DOCTOR_PYTHON_BIN}" requests
printf '  runtime_modules:     requests ok\n'
if python_bin_imports_modules "${DEV_DOCTOR_PYTHON_BIN}" psycopg >/dev/null 2>&1; then
  printf '  runtime_modules:     psycopg ok\n'
else
  printf '  runtime_modules:     psycopg missing\n'
fi

printf '\nResolved test environment\n'
if test_env_output="$(cd "${PROJECT_ROOT}" && BOOTSTRAP_TEST_ENV_PYTHON_BIN="${DEV_DOCTOR_PYTHON_BIN}" bash ./scripts/bootstrap_test_env.sh --validate-only 2>&1)"; then
  printf '%s\n' "${test_env_output}"
else
  printf '%s\n' "${test_env_output}"
  printf 'test_env_hint=run make bootstrap-test-env\n'
  doctor_failed=1
fi

if command -v node >/dev/null 2>&1; then
  printf '  node:                %s\n' "$(node -v)"
else
  printf '  node:                missing\n'
fi
if command -v npm >/dev/null 2>&1; then
  printf '  npm:                 %s\n' "$(npm -v)"
else
  printf '  npm:                 missing\n'
fi

printf '\nResolved backend helper config\n'
(cd "${PROJECT_ROOT}" && env DEV_PYTHON_BIN="${DEV_DOCTOR_PYTHON_BIN}" bash ./scripts/dev_backend.sh --print-config)

printf '\nResolved control-plane runtime\n'
(cd "${PROJECT_ROOT}" && PYTHONPATH=src "${DEV_DOCTOR_PYTHON_BIN}" -m sourcing_agent.cli show-control-plane-runtime)

if [[ -d "${FRONTEND_DIR}" ]]; then
  printf '\nResolved frontend helper config\n'
  (cd "${PROJECT_ROOT}" && bash ./scripts/dev_frontend.sh --print-config)
fi

exit "${doctor_failed}"
