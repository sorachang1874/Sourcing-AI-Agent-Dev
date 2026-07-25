#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Prepare a portable migration bundle for moving this repo from Linux/WSL to a Mac.

Default behavior is inventory-only to keep load low.

Examples:
  bash scripts/prepare_mac_migration_bundle.sh --inventory-only
  bash scripts/prepare_mac_migration_bundle.sh --stage-repo --stage-postgres --stage-codex
  bash scripts/prepare_mac_migration_bundle.sh --stage-all --include-secrets --include-codex-auth
  bash scripts/prepare_mac_migration_bundle.sh --stage-full-snapshot --codex-session-id 019d6630-2137-7f70-b742-43f979b8207b

Options:
  --bundle-root PATH        Output directory. Defaults to ../migration_exports/sourcing-ai-agent-mac-<timestamp>
  --inventory-only          Only write inventory / manifest files
  --stage-repo              Stage a portable repo + runtime copy
  --stage-repo-full         Stage the entire project tree, including .venv/node_modules/runtime caches
  --stage-postgres          Export a logical Postgres dump with pg_dump
  --stage-codex             Stage ~/.codex state without auth.json by default
  --stage-codex-full        Stage the entire ~/.codex tree, including auth/cache/tmp
  --stage-all               Equivalent to --stage-repo --stage-postgres --stage-codex
  --stage-full-snapshot     Equivalent to --stage-repo-full --stage-postgres --stage-codex-full
  --include-secrets         Include runtime/secrets in the staged repo copy
  --include-codex-auth      Include ~/.codex/auth.json in the staged Codex copy
  --postgres-dsn DSN        Override the resolved Postgres DSN for pg_dump
  --codex-root PATH         Override the source Codex root. Defaults to ~/.codex
  --codex-session-id ID     Record whether a specific Codex resume session exists in the exported state
  --help                    Show this help text
EOF
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PROJECT_PARENT_ROOT="$(cd -- "${PROJECT_ROOT}/.." && pwd)"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"

BUNDLE_ROOT="${PROJECT_PARENT_ROOT}/migration_exports/sourcing-ai-agent-mac-${TIMESTAMP}"
SOURCE_CODEX_ROOT="${HOME}/.codex"
POSTGRES_DSN_OVERRIDE=""
INVENTORY_ONLY=0
STAGE_REPO=0
STAGE_REPO_FULL=0
STAGE_POSTGRES=0
STAGE_CODEX=0
STAGE_CODEX_FULL=0
INCLUDE_SECRETS=0
INCLUDE_CODEX_AUTH=0
CODEX_SESSION_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bundle-root)
      BUNDLE_ROOT="$2"
      shift 2
      ;;
    --inventory-only)
      INVENTORY_ONLY=1
      shift
      ;;
    --stage-repo)
      STAGE_REPO=1
      shift
      ;;
    --stage-repo-full)
      STAGE_REPO_FULL=1
      shift
      ;;
    --stage-postgres)
      STAGE_POSTGRES=1
      shift
      ;;
    --stage-codex)
      STAGE_CODEX=1
      shift
      ;;
    --stage-codex-full)
      STAGE_CODEX_FULL=1
      shift
      ;;
    --stage-all)
      STAGE_REPO=1
      STAGE_POSTGRES=1
      STAGE_CODEX=1
      shift
      ;;
    --stage-full-snapshot)
      STAGE_REPO_FULL=1
      STAGE_POSTGRES=1
      STAGE_CODEX_FULL=1
      INCLUDE_SECRETS=1
      INCLUDE_CODEX_AUTH=1
      shift
      ;;
    --include-secrets)
      INCLUDE_SECRETS=1
      shift
      ;;
    --include-codex-auth)
      INCLUDE_CODEX_AUTH=1
      shift
      ;;
    --postgres-dsn)
      POSTGRES_DSN_OVERRIDE="$2"
      shift 2
      ;;
    --codex-root)
      SOURCE_CODEX_ROOT="$2"
      shift 2
      ;;
    --codex-session-id)
      CODEX_SESSION_ID="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${STAGE_REPO_FULL}" -eq 1 ]]; then
  STAGE_REPO=1
  INCLUDE_SECRETS=1
fi

if [[ "${STAGE_CODEX_FULL}" -eq 1 ]]; then
  STAGE_CODEX=1
  INCLUDE_CODEX_AUTH=1
fi

if [[ "${INVENTORY_ONLY}" -eq 0 && "${STAGE_REPO}" -eq 0 && "${STAGE_POSTGRES}" -eq 0 && "${STAGE_CODEX}" -eq 0 ]]; then
  INVENTORY_ONLY=1
fi

mkdir -p "${BUNDLE_ROOT}/manifest"

log() {
  printf '[prepare-mac-migration] %s\n' "$*"
}

require_binary() {
  local binary="$1"
  if ! command -v "${binary}" >/dev/null 2>&1; then
    echo "Missing required binary: ${binary}" >&2
    exit 1
  fi
}

write_inventory() {
  log "writing inventory files to ${BUNDLE_ROOT}/manifest"
  {
    echo "timestamp_utc=${TIMESTAMP}"
    echo "project_root=${PROJECT_ROOT}"
    echo "bundle_root=${BUNDLE_ROOT}"
    echo "source_codex_root=${SOURCE_CODEX_ROOT}"
  } > "${BUNDLE_ROOT}/manifest/source_context.env"

  {
    du -sh "${PROJECT_ROOT}" 2>/dev/null || true
    du -sh "${SOURCE_CODEX_ROOT}" 2>/dev/null || true
  } > "${BUNDLE_ROOT}/manifest/source_sizes.txt"

  git -C "${PROJECT_ROOT}" rev-parse HEAD > "${BUNDLE_ROOT}/manifest/repo_head.txt" 2>/dev/null || true
  git -C "${PROJECT_ROOT}" status --short > "${BUNDLE_ROOT}/manifest/repo_status.txt" 2>/dev/null || true

  (
    cd "${PROJECT_ROOT}"
    PYTHONPATH=src python3 -m sourcing_agent.cli show-control-plane-runtime
  ) > "${BUNDLE_ROOT}/manifest/control_plane_runtime.json" 2>"${BUNDLE_ROOT}/manifest/control_plane_runtime.stderr" || true

  if [[ -n "${CODEX_SESSION_ID}" ]]; then
    python3 - <<'PY' \
      "${SOURCE_CODEX_ROOT}" \
      "${CODEX_SESSION_ID}" \
      "${BUNDLE_ROOT}/manifest/codex_session_probe.json"
import json
import sqlite3
import sys
from pathlib import Path

codex_root = Path(sys.argv[1]).expanduser()
session_id = sys.argv[2]
output_path = Path(sys.argv[3])
sessions_dir = codex_root / "sessions"
matched_files = sorted(str(path) for path in sessions_dir.rglob(f"*{session_id}*.jsonl")) if sessions_dir.exists() else []
state_db = codex_root / "state_5.sqlite"
thread_present = False
thread_row = {}
if state_db.exists():
    connection = sqlite3.connect(state_db)
    try:
        cursor = connection.cursor()
        row = cursor.execute(
            "select id, cwd, title, created_at, updated_at from threads where id = ? limit 1",
            (session_id,),
        ).fetchone()
        if row:
            thread_present = True
            thread_row = {
                "id": str(row[0] or ""),
                "cwd": str(row[1] or ""),
                "title": str(row[2] or ""),
                "created_at": str(row[3] or ""),
                "updated_at": str(row[4] or ""),
            }
    finally:
        connection.close()

output_path.write_text(
    json.dumps(
        {
            "session_id": session_id,
            "sessions_dir": str(sessions_dir),
            "matched_session_files": matched_files,
            "thread_present_in_state_db": thread_present,
            "thread_row": thread_row,
        },
        ensure_ascii=False,
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
PY
  fi
}

stage_repo() {
  require_binary rsync
  local destination="${BUNDLE_ROOT}/repo/sourcing-ai-agent"
  mkdir -p "${destination}"
  local -a rsync_args=(-a)
  if [[ "${STAGE_REPO_FULL}" -eq 1 ]]; then
    log "staging full project snapshot"
  else
    log "staging portable repo copy"
    rsync_args+=(
      --exclude='.venv/'
      --exclude='.venv-tests/'
      --exclude='.pytest_cache/'
      --exclude='.mypy_cache/'
      --exclude='.ruff_cache/'
      --exclude='.cache/'
      --exclude='frontend-demo/node_modules/'
      --exclude='frontend-demo/dist/'
      --exclude='runtime/cache/'
      --exclude='runtime/hot_cache_company_assets/'
      --exclude='runtime/job_locks/'
      --exclude='runtime/maintenance/'
      --exclude='runtime/provider_cache/'
      --exclude='runtime/runtime_metrics/'
      --exclude='runtime/service_logs/'
      --exclude='runtime/services/'
      --exclude='runtime/simulate_smoke_probe/'
      --exclude='runtime/simulate_smoke_standalone/'
      --exclude='runtime/test_env/'
      --exclude='runtime/test_env_smoke/'
      --exclude='runtime/test_env_smoke_fastpath/'
      --exclude='runtime/test_env_smoke_fresh/'
      --exclude='runtime/test_launcher_probe/'
      --exclude='runtime/live_tests/'
      --exclude='runtime/vendor/'
      --exclude='runtime/_tmp_dry_jobs/'
    )
    if [[ "${INCLUDE_SECRETS}" -eq 0 ]]; then
      rsync_args+=(--exclude='runtime/secrets/')
    fi
  fi
  rsync "${rsync_args[@]}" "${PROJECT_ROOT}/" "${destination}/"
}

stage_codex() {
  require_binary rsync
  if [[ ! -d "${SOURCE_CODEX_ROOT}" ]]; then
    echo "source Codex root does not exist: ${SOURCE_CODEX_ROOT}" > "${BUNDLE_ROOT}/manifest/codex_status.txt"
    return 0
  fi
  local destination="${BUNDLE_ROOT}/codex/.codex"
  mkdir -p "${destination}"
  local -a rsync_args=(-a)
  if [[ "${STAGE_CODEX_FULL}" -eq 1 ]]; then
    log "staging full Codex state"
  else
    log "staging Codex state"
    rsync_args+=(
      --exclude='cache/'
      --exclude='.tmp/'
      --exclude='tmp/'
    )
    if [[ "${INCLUDE_CODEX_AUTH}" -eq 0 ]]; then
      rsync_args+=(--exclude='auth.json')
    fi
  fi
  rsync "${rsync_args[@]}" "${SOURCE_CODEX_ROOT}/" "${destination}/"
}

resolve_postgres_dsn() {
  if [[ -n "${POSTGRES_DSN_OVERRIDE}" ]]; then
    printf '%s' "${POSTGRES_DSN_OVERRIDE}"
    return 0
  fi
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/scripts/dev_postgres_env.sh" || true
  if [[ -n "${SOURCING_CONTROL_PLANE_POSTGRES_DSN:-}" ]]; then
    printf '%s' "${SOURCING_CONTROL_PLANE_POSTGRES_DSN}"
    return 0
  fi
  (
    cd "${PROJECT_ROOT}"
    PYTHONPATH=src python3 - <<'PY'
from pathlib import Path
from sourcing_agent.local_postgres import resolve_control_plane_postgres_dsn

print(resolve_control_plane_postgres_dsn(Path.cwd()), end="")
PY
  )
}

redact_dsn() {
  python3 - <<'PY' "$1"
import re
import sys

dsn = sys.argv[1]
print(re.sub(r"://([^:@/]+)(?::[^@/]*)?@", "://***:***@", dsn))
PY
}

stage_postgres() {
  local destination="${BUNDLE_ROOT}/postgres"
  mkdir -p "${destination}"
  log "exporting logical Postgres dump"
  local dsn
  dsn="$(resolve_postgres_dsn)"
  if [[ -z "${dsn}" ]]; then
    echo "resolved Postgres DSN is empty" > "${BUNDLE_ROOT}/manifest/postgres_status.txt"
    return 0
  fi
  if ! command -v pg_dump >/dev/null 2>&1; then
    echo "pg_dump is not available in PATH" > "${BUNDLE_ROOT}/manifest/postgres_status.txt"
    return 0
  fi
  pg_dump \
    --dbname="${dsn}" \
    --format=custom \
    --no-owner \
    --no-privileges \
    --file="${destination}/control_plane.dump"
  redact_dsn "${dsn}" > "${BUNDLE_ROOT}/manifest/postgres_dsn.redacted.txt"
  if command -v psql >/dev/null 2>&1; then
    psql "${dsn}" -Atqc "select current_database(), current_user, current_setting('server_encoding'), current_setting('server_version');" \
      > "${BUNDLE_ROOT}/manifest/postgres_runtime.txt" || true
  fi
}

write_bundle_manifest() {
  python3 - <<'PY' \
    "${BUNDLE_ROOT}" \
    "${PROJECT_ROOT}" \
    "${SOURCE_CODEX_ROOT}" \
    "${TIMESTAMP}" \
    "${INVENTORY_ONLY}" \
    "${STAGE_REPO}" \
    "${STAGE_REPO_FULL}" \
    "${STAGE_POSTGRES}" \
    "${STAGE_CODEX}" \
    "${STAGE_CODEX_FULL}" \
    "${INCLUDE_SECRETS}" \
    "${INCLUDE_CODEX_AUTH}" \
    "${CODEX_SESSION_ID}"
import json
import sys
from pathlib import Path

bundle_root = Path(sys.argv[1]).resolve()
payload = {
    "generated_at_utc": sys.argv[4],
    "bundle_root": str(bundle_root),
    "project_root": str(Path(sys.argv[2]).resolve()),
    "source_codex_root": str(Path(sys.argv[3]).expanduser()),
    "inventory_only": bool(int(sys.argv[5])),
    "staged_components": {
        "repo": bool(int(sys.argv[6])),
        "repo_full": bool(int(sys.argv[7])),
        "postgres": bool(int(sys.argv[8])),
        "codex": bool(int(sys.argv[9])),
        "codex_full": bool(int(sys.argv[10])),
    },
    "sensitive_components_included": {
        "runtime_secrets": bool(int(sys.argv[11])),
        "codex_auth": bool(int(sys.argv[12])),
    },
    "codex_session_id": str(sys.argv[13] or ""),
    "notes": [
        "Postgres is exported as a logical dump; do not copy Linux/WSL .local-postgres/data directly to macOS.",
        (
            "This bundle includes a full project snapshot, which preserves Linux/WSL .venv/node_modules for forensic continuity but they should still be rebuilt on the Mac."
            if bool(int(sys.argv[7]))
            else "Python virtualenvs and frontend node_modules are intentionally excluded and should be rebuilt on the Mac."
        ),
        (
            "This bundle includes the full ~/.codex tree, intended to maximize Codex resume continuity on the destination Mac."
            if bool(int(sys.argv[10]))
            else "Use --stage-codex-full when you need the strongest chance of preserving Codex resume continuity."
        ),
        "Use configs/local_postgres.env.example to create .local-postgres.env on the destination machine.",
    ],
}
(bundle_root / "manifest" / "bundle_manifest.json").write_text(
    json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
PY
}

write_inventory

if [[ "${INVENTORY_ONLY}" -eq 0 ]]; then
  if [[ "${STAGE_REPO}" -eq 1 ]]; then
    stage_repo
  fi
  if [[ "${STAGE_POSTGRES}" -eq 1 ]]; then
    stage_postgres
  fi
  if [[ "${STAGE_CODEX}" -eq 1 ]]; then
    stage_codex
  fi
fi

write_bundle_manifest
log "bundle prepared at ${BUNDLE_ROOT}"
