#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
FRONTEND_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd -- "${FRONTEND_DIR}/.." && pwd)"
CACHE_ROOT="${REPO_ROOT}/.cache"
NPM_CACHE="${NPM_CONFIG_CACHE:-${CACHE_ROOT}/npm}"
PLAYWRIGHT_CACHE="${PLAYWRIGHT_BROWSERS_PATH:-${CACHE_ROOT}/ms-playwright}"
UBUNTU_LIB_CACHE="${CACHE_ROOT}/ubuntu-libs"
UBUNTU_LIB_ROOT="${PLAYWRIGHT_LD_LIBRARY_PATH:-${UBUNTU_LIB_CACHE}/root/usr/lib/x86_64-linux-gnu}"

mkdir -p "${NPM_CACHE}" "${PLAYWRIGHT_CACHE}" "${UBUNTU_LIB_CACHE}/debs" "${UBUNTU_LIB_CACHE}/root"

cd "${FRONTEND_DIR}"

if [ ! -d "${FRONTEND_DIR}/node_modules/playwright" ]; then
  NPM_CONFIG_CACHE="${NPM_CACHE}" npm install
fi

PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_CACHE}" NPM_CONFIG_CACHE="${NPM_CACHE}" npx playwright install chromium
EXECUTABLE="$(
  PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_CACHE}" \
  NPM_CONFIG_CACHE="${NPM_CACHE}" \
  node --input-type=module -e "import { chromium } from 'playwright'; console.log(chromium.executablePath())"
)"
if [ ! -x "${EXECUTABLE}" ]; then
  PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_CACHE}" NPM_CONFIG_CACHE="${NPM_CACHE}" npx playwright install --force chromium
fi

if [ "$(uname -s)" = "Linux" ]; then
  cd "${UBUNTU_LIB_CACHE}/debs"
  missing_deb=0
  audio_pkg="libasound2t64"
  if ! apt-cache show "${audio_pkg}" >/dev/null 2>&1; then
    audio_pkg="libasound2"
  fi
  for pkg in libnspr4 libnss3 "${audio_pkg}"; do
    if ! compgen -G "${pkg}_*.deb" > /dev/null; then
      missing_deb=1
    fi
  done

  if [ "${missing_deb}" -eq 1 ]; then
    # Refresh the package index before downloading: `apt download` fetches the
    # candidate version resolved from the LOCAL index, and a stale index pins a
    # superseded version whose .deb has been purged from the mirror pool
    # (observed 2026-07-24 on GitHub runners: libasound2t64_1.2.11-1ubuntu0.2
    # 404'd across azure/archive/security mirrors, R-044). The update is
    # best-effort — sudo may be absent on dev machines, where the warm .deb
    # cache above already sets missing_deb=0 and skips this block entirely.
    if command -v sudo >/dev/null 2>&1; then
      sudo apt-get update -y >/dev/null 2>&1 || true
    else
      apt-get update -y >/dev/null 2>&1 || true
    fi
    apt download libnspr4 libnss3 "${audio_pkg}"
  fi

  for deb in ./*.deb; do
    [ -e "${deb}" ] || continue
    dpkg-deb -x "${deb}" "${UBUNTU_LIB_CACHE}/root"
  done
fi

cat <<EOF
Browser environment prepared.

Recommended env for local browser checks:
  export NPM_CONFIG_CACHE='${NPM_CACHE}'
  export PLAYWRIGHT_BROWSERS_PATH='${PLAYWRIGHT_CACHE}'
  export NO_PROXY='*'
EOF

if [ "$(uname -s)" = "Linux" ]; then
  cat <<EOF
  export PLAYWRIGHT_LD_LIBRARY_PATH='${UBUNTU_LIB_ROOT}'
EOF
fi

cat <<EOF

Run validation:
  npm run browser:check
EOF
