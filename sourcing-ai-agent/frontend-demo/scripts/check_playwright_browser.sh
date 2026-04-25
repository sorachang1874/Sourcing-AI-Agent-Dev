#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd -- "${FRONTEND_DIR}/.." && pwd)"
NPM_CACHE="${NPM_CONFIG_CACHE:-${REPO_ROOT}/.cache/npm}"
PLAYWRIGHT_CACHE="${PLAYWRIGHT_BROWSERS_PATH:-${REPO_ROOT}/.cache/ms-playwright}"
PLAYWRIGHT_LD_LIBRARY_PATH="${PLAYWRIGHT_LD_LIBRARY_PATH:-${REPO_ROOT}/.cache/ubuntu-libs/root/usr/lib/x86_64-linux-gnu}"

cd "${FRONTEND_DIR}"

EXECUTABLE="$(
  PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_CACHE}" \
  NPM_CONFIG_CACHE="${NPM_CACHE}" \
  node --input-type=module -e "import { chromium } from 'playwright'; console.log(chromium.executablePath())"
)"

LD_LIBRARY_PATH="${PLAYWRIGHT_LD_LIBRARY_PATH}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" \
  "${EXECUTABLE}" --version
