#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/ensure_modern_bash.sh"
ensure_modern_bash "$@"

PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
CLASH_SOCKET="${CLASH_CONTROLLER_SOCKET:-/tmp/verge/verge-mihomo.sock}"
CLASH_TOKEN="${CLASH_CONTROLLER_TOKEN:-set-your-secret}"
FAILED=0

_note() {
  printf '%s\n' "$*"
}

_fail() {
  printf 'FAIL: %s\n' "$*" >&2
  FAILED=1
}

_ok() {
  printf 'OK: %s\n' "$*"
}

_http_probe() {
  local label="$1"
  local url="$2"
  local status_line=""
  local attempt=1
  while [[ "$attempt" -le 3 ]]; do
    if status_line="$(curl -IsS --connect-timeout 10 --max-time 20 "$url" 2>&1 | sed -n '1p')"; then
      if [[ "$status_line" =~ ^HTTP/ ]]; then
        if [[ "$attempt" -eq 1 ]]; then
          _ok "${label}: ${status_line}"
        else
          _ok "${label}: ${status_line} after ${attempt} attempts"
        fi
        return 0
      fi
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
  _fail "${label}: cannot reach ${url}; first response line: ${status_line:-<none>}"
}

_check_proxy_env() {
  _note "Proxy environment"
  local names=(
    http_proxy
    https_proxy
    all_proxy
    no_proxy
    HTTP_PROXY
    HTTPS_PROXY
    ALL_PROXY
    NO_PROXY
  )
  local found=0
  local name=""
  for name in "${names[@]}"; do
    if [[ -n "${!name-}" ]]; then
      printf '  %s=<set>\n' "$name"
      found=1
    fi
  done
  if [[ "$found" -eq 0 ]]; then
    _ok "no shell proxy env is set"
  else
    _fail "shell proxy env is set; agents must not rely on or mutate HTTP_PROXY/HTTPS_PROXY/ALL_PROXY"
  fi
}

_check_git_proxy_config() {
  _note "Git proxy config"
  local global_proxy=""
  local local_proxy=""
  global_proxy="$(git config --global --get-regexp '(^http\..*proxy|^https\..*proxy|proxy)' 2>/dev/null || true)"
  local_proxy="$(cd "${PROJECT_ROOT}" && git config --local --get-regexp '(^http\..*proxy|^https\..*proxy|proxy)' 2>/dev/null || true)"
  if [[ -n "$global_proxy" ]]; then
    printf '%s\n' "$global_proxy" | sed 's/=.*/=<redacted>/'
    _fail "global git proxy config is present"
  else
    _ok "no global git proxy config"
  fi
  if [[ -n "$local_proxy" ]]; then
    printf '%s\n' "$local_proxy" | sed 's/=.*/=<redacted>/'
    _fail "repo-local git proxy config is present"
  else
    _ok "no repo-local git proxy config"
  fi
}

_check_dns() {
  _note "GitHub DNS"
  if ! command -v dig >/dev/null 2>&1; then
    _fail "dig is missing; cannot verify GitHub fake-ip status"
    return
  fi
  local output=""
  output="$(dig +short github.com || true)"
  printf '%s\n' "$output" | sed 's/^/  github.com -> /'
  if printf '%s\n' "$output" | grep -Eq '^198\.18\.'; then
    _fail "github.com resolves to Clash fake-ip; fake-ip-filter needs attention, but agents must not reload Clash"
  elif [[ -z "$output" ]]; then
    _fail "github.com did not resolve"
  else
    _ok "github.com resolves to real addresses"
  fi
}

_check_clash_runtime() {
  _note "Clash/Mihomo runtime"
  if [[ ! -S "$CLASH_SOCKET" ]]; then
    _note "SKIP: no Clash controller socket at ${CLASH_SOCKET}"
    return
  fi
  local configs="/tmp/sourcing_agent_clash_configs.$$.json"
  local global="/tmp/sourcing_agent_clash_global.$$.json"
  local proxies="/tmp/sourcing_agent_clash_proxies.$$.json"
  if ! curl --unix-socket "$CLASH_SOCKET" -sS -H "Authorization: Bearer ${CLASH_TOKEN}" http://localhost/configs >"$configs"; then
    _fail "cannot read Clash configs from unix socket"
    rm -f "$configs" "$global" "$proxies"
    return
  fi
  if ! curl --unix-socket "$CLASH_SOCKET" -sS -H "Authorization: Bearer ${CLASH_TOKEN}" http://localhost/proxies/GLOBAL >"$global"; then
    _fail "cannot read Clash GLOBAL proxy selection"
    rm -f "$configs" "$global" "$proxies"
    return
  fi
  curl --unix-socket "$CLASH_SOCKET" -sS -H "Authorization: Bearer ${CLASH_TOKEN}" http://localhost/proxies >"$proxies" || true
  python3 - "$configs" "$global" <<'PY'
import json
import sys
from pathlib import Path

configs = json.loads(Path(sys.argv[1]).read_text())
global_proxy = json.loads(Path(sys.argv[2]).read_text())
print(f"  mode={configs.get('mode')}")
print(f"  GLOBAL={global_proxy.get('now')}")
PY
  if [[ -s "$proxies" ]]; then
    python3 - "$proxies" <<'PY'
import json
import sys
from pathlib import Path

obj = json.loads(Path(sys.argv[1]).read_text())
proxies = obj.get("proxies", {})
for name in ("AI-STRICT", "AI-FIXED-EGRESS-GROUP", "AI-PUBLIC-FALLBACK", "🚀 节点选择", "♻️ 自动测速优选"):
    value = proxies.get(name)
    if value:
        print(f"  {name}={value.get('now')}")
PY
  fi
  local now=""
  now="$(python3 - "$global" <<'PY'
import json
import sys
from pathlib import Path
print(json.loads(Path(sys.argv[1]).read_text()).get("now") or "")
PY
)"
  local direct_ai_group=""
  if [[ -s "$proxies" ]]; then
    direct_ai_group="$(python3 - "$proxies" <<'PY'
import json
import sys
from pathlib import Path

obj = json.loads(Path(sys.argv[1]).read_text())
proxies = obj.get("proxies", {})
for name in ("AI-STRICT", "AI-FIXED-EGRESS-GROUP"):
    value = proxies.get(name)
    if value and value.get("now") == "DIRECT":
        print(name)
        break
PY
)"
  fi
  rm -f "$configs" "$global" "$proxies"
  if [[ "$now" == "DIRECT" ]]; then
    _fail "Clash GLOBAL is DIRECT; Codex/Claude/GitHub traffic can disconnect under current network"
  else
    _ok "Clash GLOBAL is not DIRECT"
  fi
  if [[ -n "$direct_ai_group" ]]; then
    _fail "Clash ${direct_ai_group} is DIRECT; Codex/Claude/model backend traffic can disconnect under current network"
  fi
}

_check_http_and_git() {
  _note "HTTP and GitHub CLI"
  _http_probe "github.com" "https://github.com/"
  _http_probe "api.github.com" "https://api.github.com/"
  _http_probe "raw.githubusercontent.com" "https://raw.githubusercontent.com/github/gitignore/main/Python.gitignore"
  _http_probe "chatgpt.com" "https://chatgpt.com/"

  if command -v gh >/dev/null 2>&1; then
    if gh auth status -h github.com >/tmp/sourcing_agent_gh_auth_status.$$ 2>&1; then
      _ok "gh auth status is valid"
    else
      sed -n '1,12p' /tmp/sourcing_agent_gh_auth_status.$$
      _fail "gh auth status failed; run gh auth login/refresh manually, not by changing proxy env"
    fi
    rm -f /tmp/sourcing_agent_gh_auth_status.$$
  else
    _note "SKIP: gh CLI is missing"
  fi

  if git ls-remote https://github.com/github/gitignore.git HEAD >/tmp/sourcing_agent_git_ls_remote.$$ 2>&1; then
    _ok "git ls-remote over HTTPS works"
  else
    sed -n '1,12p' /tmp/sourcing_agent_git_ls_remote.$$
    _fail "git ls-remote over HTTPS failed"
  fi
  rm -f /tmp/sourcing_agent_git_ls_remote.$$
}

cat <<'EOF'
Agent Network Preflight

This command is read-only. It must not export proxy variables, edit git config,
hot-reload/restart Clash, call Clash controller mutation APIs, or change macOS
network settings.
EOF

_check_proxy_env
_check_git_proxy_config
_check_dns
_check_clash_runtime
_check_http_and_git

if [[ "$FAILED" -ne 0 ]]; then
  cat <<'EOF' >&2

Network preflight failed.

Allowed next steps:
  - Ask the user to pick a non-DIRECT Clash GLOBAL node.
  - Ask the user to manually review Clash DNS/fake-ip settings if GitHub still resolves to 198.18.x.x.
  - Ask the user to refresh gh auth when the token is invalid.

Forbidden next steps for agents:
  - Do not set HTTP_PROXY, HTTPS_PROXY, ALL_PROXY, or NO_PROXY as a workaround.
  - Do not write git proxy config.
  - Do not toggle Clash mode, TUN, DNS, system proxy, or selected nodes.
  - Do not hot-reload or restart Clash/Mihomo, including controller socket PUT /configs.
EOF
fi

exit "$FAILED"
