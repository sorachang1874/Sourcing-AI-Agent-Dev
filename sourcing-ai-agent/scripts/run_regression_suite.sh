#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MODE="${1:-fast}"

cd "${REPO_ROOT}"

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if [[ -x "${REPO_ROOT}/.venv-tests/bin/python" ]]; then
    PYTHON_BIN="${REPO_ROOT}/.venv-tests/bin/python"
  elif [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
    PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

run_fast() {
  "${PYTHON_BIN}" "${REPO_ROOT}/scripts/run_pytest_matrix.py" --mode changed
}

run_smoke() {
  "${PYTHON_BIN}" "${REPO_ROOT}/scripts/run_pytest_matrix.py" --mode smoke
}

run_full() {
  "${PYTHON_BIN}" "${REPO_ROOT}/scripts/run_pytest_matrix.py" --mode full
}

run_durations() {
  "${PYTHON_BIN}" "${REPO_ROOT}/scripts/run_pytest_matrix.py" --mode durations
}

run_browser() {
  SOURCING_RUN_FRONTEND_BROWSER_E2E=1 "${PYTHON_BIN}" "${REPO_ROOT}/scripts/run_pytest_matrix.py" \
    --mode full \
    --extra-pytest-arg tests/test_frontend_browser_e2e.py
}

run_all() {
  run_smoke
  run_full
  run_browser
}

case "${MODE}" in
  fast)
    run_fast
    ;;
  smoke)
    run_smoke
    ;;
  full)
    run_full
    ;;
  durations)
    run_durations
    ;;
  browser)
    run_browser
    ;;
  all)
    run_all
    ;;
  *)
    echo "Usage: $0 [fast|smoke|full|durations|browser|all]" >&2
    exit 1
    ;;
esac
