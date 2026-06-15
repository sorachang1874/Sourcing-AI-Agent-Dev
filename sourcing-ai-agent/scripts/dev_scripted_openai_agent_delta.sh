#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/ensure_modern_bash.sh"
ensure_modern_bash "$@"

PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/python_env_guard.sh"
PYTHON_BIN="$(resolve_project_python_bin "${PROJECT_ROOT}" "${SCRIPTED_PYTHON_BIN-}")"

SCRIPTED_RUNTIME_DIR="${SCRIPTED_RUNTIME_DIR:-runtime/test_env/openai_agent_delta_streaming}"
SCRIPTED_API_HOST="${SCRIPTED_API_HOST:-0.0.0.0}"
SCRIPTED_API_PORT="${SCRIPTED_API_PORT:-8785}"
SCRIPTED_FRONTEND_PORT="${SCRIPTED_FRONTEND_PORT:-4185}"
SCRIPTED_DAEMON_POLL_SECONDS="${SCRIPTED_DAEMON_POLL_SECONDS:-1}"
SCRIPTED_SCENARIO="${SCRIPTED_SCENARIO:-configs/scripted/openai_agent_and_lovable_streaming.json}"
SCRIPTED_SECRETS_FILE="${SCRIPTED_SECRETS_FILE:-}"
SCRIPTED_OBJECT_STORAGE_PREFIX="${SCRIPTED_OBJECT_STORAGE_PREFIX:-sourcing-ai-agent-scripted-openai-agent}"
SCRIPTED_FAST_RUNTIME="${SCRIPTED_FAST_RUNTIME:-0}"
SCRIPTED_SLOW_STRICT_RUNTIME="${SCRIPTED_SLOW_STRICT_RUNTIME:-0}"
SCRIPTED_HARVEST_SLEEP_SECONDS_CAP="${SCRIPTED_HARVEST_SLEEP_SECONDS_CAP:-}"
SCRIPTED_RESET_RUNTIME="${SCRIPTED_RESET_RUNTIME:-1}"
SCRIPTED_POSTGRES_SCHEMA="${SCRIPTED_POSTGRES_SCHEMA:-sourcing_scripted_openai_agent_delta}"
SCRIPTED_REFERENCE_SEED_MODE="${SCRIPTED_REFERENCE_SEED_MODE:-baseline}"
START_FRONTEND=1
START_BACKEND=1
PRINT_CONFIG=0

_usage() {
  cat <<'EOF'
Usage:
  bash ./scripts/dev_scripted_openai_agent_delta.sh
  bash ./scripts/dev_scripted_openai_agent_delta.sh --backend-only
  bash ./scripts/dev_scripted_openai_agent_delta.sh --print-config

What it does:
  - seeds an isolated OpenAI reference baseline into runtime/test_env/openai_agent_delta_streaming
  - starts backend with SOURCING_EXTERNAL_PROVIDER_MODE=scripted
  - uses configs/scripted/openai_agent_and_lovable_streaming.json by default
  - starts the frontend against that backend
  - never calls live Harvest/DataForSEO providers

Options:
  --runtime-dir <path>          Isolated runtime dir. Default: runtime/test_env/openai_agent_delta_streaming
  --api-host <host>             Backend host. Default: 0.0.0.0
  --api-port <port>             Backend port. Default: 8785
  --frontend-port <port>        Frontend port. Default: 4185
  --scenario <path>             Scripted scenario JSON.
  --secrets-file <path>         Secrets file path for settings load. Default: <runtime-dir>/secrets/providers.local.json
  --daemon-poll-seconds <sec>   Worker daemon poll interval. Default: 1
  --backend-only                Seed and start backend only.
  --seed-only                   Seed the runtime and exit.
  --fast-runtime                Apply fast scripted cooldown/sleep settings for CI/E2E.
  --no-fast-runtime             Use interactive scripted timing. This is the default.
  --slow-strict-runtime         Preserve fixture sleep durations for live-like scheduler/recovery tests.
  --scripted-sleep-cap <sec>    Cap each scripted Harvest sleep. Default: 8; fast runtime forces 0.1 unless overridden; use "none" for no cap.
  --postgres-schema <schema>    Dedicated PG schema for the scripted control plane. Default: sourcing_scripted_openai_agent_delta.
  --no-reference-baseline-seed  Do not seed OpenAI baseline assets. Use for no-baseline scoped-search manual testing.
  --no-reset-runtime            Preserve existing runtime/test_env jobs and artifacts before seeding.
  --print-config                Print resolved config and exit.
  --help                        Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runtime-dir)
      SCRIPTED_RUNTIME_DIR="${2-}"
      shift 2
      ;;
    --api-host)
      SCRIPTED_API_HOST="${2-}"
      shift 2
      ;;
    --api-port)
      SCRIPTED_API_PORT="${2-}"
      shift 2
      ;;
    --frontend-port)
      SCRIPTED_FRONTEND_PORT="${2-}"
      shift 2
      ;;
    --scenario)
      SCRIPTED_SCENARIO="${2-}"
      shift 2
      ;;
    --secrets-file)
      SCRIPTED_SECRETS_FILE="${2-}"
      shift 2
      ;;
    --daemon-poll-seconds)
      SCRIPTED_DAEMON_POLL_SECONDS="${2-}"
      shift 2
      ;;
    --backend-only)
      START_FRONTEND=0
      shift
      ;;
    --seed-only)
      START_BACKEND=0
      START_FRONTEND=0
      shift
      ;;
    --fast-runtime)
      SCRIPTED_FAST_RUNTIME=1
      SCRIPTED_SLOW_STRICT_RUNTIME=0
      shift
      ;;
    --no-fast-runtime)
      SCRIPTED_FAST_RUNTIME=0
      shift
      ;;
    --slow-strict-runtime)
      SCRIPTED_FAST_RUNTIME=0
      SCRIPTED_SLOW_STRICT_RUNTIME=1
      shift
      ;;
    --scripted-sleep-cap)
      SCRIPTED_HARVEST_SLEEP_SECONDS_CAP="${2-}"
      shift 2
      ;;
    --postgres-schema)
      SCRIPTED_POSTGRES_SCHEMA="${2-}"
      shift 2
      ;;
    --no-reference-baseline-seed)
      SCRIPTED_REFERENCE_SEED_MODE=none
      shift
      ;;
    --reset-runtime)
      SCRIPTED_RESET_RUNTIME=1
      shift
      ;;
    --no-reset-runtime)
      SCRIPTED_RESET_RUNTIME=0
      shift
      ;;
    --print-config)
      PRINT_CONFIG=1
      shift
      ;;
    --help|-h)
      _usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      _usage >&2
      exit 2
      ;;
  esac
done

if [[ "${SCRIPTED_RUNTIME_DIR}" != /* ]]; then
  SCRIPTED_RUNTIME_DIR="${PROJECT_ROOT}/${SCRIPTED_RUNTIME_DIR}"
fi
if [[ "${SCRIPTED_SCENARIO}" != /* ]]; then
  SCRIPTED_SCENARIO="${PROJECT_ROOT}/${SCRIPTED_SCENARIO}"
fi
if [[ -z "${SCRIPTED_SECRETS_FILE}" ]]; then
  SCRIPTED_SECRETS_FILE="${SCRIPTED_RUNTIME_DIR}/secrets/providers.local.json"
fi
if [[ "${SCRIPTED_SECRETS_FILE}" != /* ]]; then
  SCRIPTED_SECRETS_FILE="${PROJECT_ROOT}/${SCRIPTED_SECRETS_FILE}"
fi
if [[ -z "${SCRIPTED_HARVEST_SLEEP_SECONDS_CAP}" ]]; then
  if [[ "${SCRIPTED_SLOW_STRICT_RUNTIME}" == "1" ]]; then
    SCRIPTED_HARVEST_SLEEP_SECONDS_CAP=none
  elif [[ "${SCRIPTED_FAST_RUNTIME}" == "1" ]]; then
    SCRIPTED_HARVEST_SLEEP_SECONDS_CAP=0.1
  else
    SCRIPTED_HARVEST_SLEEP_SECONDS_CAP=8
  fi
fi

SCRIPTED_API_BASE_URL="http://127.0.0.1:${SCRIPTED_API_PORT}"
SCRIPTED_FRONTEND_URL="http://127.0.0.1:${SCRIPTED_FRONTEND_PORT}"
SCRIPTED_LOG_DIR="${SCRIPTED_RUNTIME_DIR}/service_logs"
SCRIPTED_BACKEND_LOG="${SCRIPTED_LOG_DIR}/scripted-openai-agent-backend.log"
SCRIPTED_BACKEND_PID_FILE="${SCRIPTED_LOG_DIR}/scripted-openai-agent-backend.pid"
SCRIPTED_SEED_JSON="${SCRIPTED_LOG_DIR}/scripted-openai-agent-seed.json"
SCRIPTED_POSTGRES_ENV_FILE="${SCRIPTED_RUNTIME_DIR}/.scripted-local-postgres.env"

if [[ $PRINT_CONFIG -eq 1 ]]; then
  printf 'project_root=%s\n' "${PROJECT_ROOT}"
  printf 'python_bin=%s\n' "${PYTHON_BIN}"
  printf 'runtime_dir=%s\n' "${SCRIPTED_RUNTIME_DIR}"
  printf 'scenario=%s\n' "${SCRIPTED_SCENARIO}"
  printf 'secrets_file=%s\n' "${SCRIPTED_SECRETS_FILE}"
  printf 'api_url=%s\n' "${SCRIPTED_API_BASE_URL}"
  printf 'frontend_url=%s\n' "${SCRIPTED_FRONTEND_URL}"
  printf 'fast_runtime=%s\n' "${SCRIPTED_FAST_RUNTIME}"
  printf 'slow_strict_runtime=%s\n' "${SCRIPTED_SLOW_STRICT_RUNTIME}"
  printf 'scripted_harvest_sleep_seconds_cap=%s\n' "${SCRIPTED_HARVEST_SLEEP_SECONDS_CAP}"
  printf 'reset_runtime=%s\n' "${SCRIPTED_RESET_RUNTIME}"
  printf 'reference_seed_mode=%s\n' "${SCRIPTED_REFERENCE_SEED_MODE}"
  printf 'control_plane=postgres\n'
  printf 'postgres_schema=%s\n' "${SCRIPTED_POSTGRES_SCHEMA}"
  printf 'live_provider_access_disabled=enabled\n'
  printf 'scripted_local_provider_event_watcher=enabled\n'
  exit 0
fi

if [[ ! -f "${SCRIPTED_SCENARIO}" ]]; then
  printf 'Scripted scenario not found: %s\n' "${SCRIPTED_SCENARIO}" >&2
  exit 2
fi

if [[ "${SCRIPTED_RESET_RUNTIME}" == "1" ]]; then
  SCRIPTED_TEST_ROOT="${PROJECT_ROOT}/runtime/test_env"
  case "${SCRIPTED_RUNTIME_DIR}/" in
    "${SCRIPTED_TEST_ROOT}/"*)
      rm -rf -- "${SCRIPTED_RUNTIME_DIR}"
      ;;
    *)
      printf 'Refusing to reset non-test runtime dir: %s\n' "${SCRIPTED_RUNTIME_DIR}" >&2
      printf 'Use a runtime under %s or pass --no-reset-runtime.\n' "${SCRIPTED_TEST_ROOT}" >&2
      exit 2
      ;;
  esac
fi

mkdir -p "${SCRIPTED_LOG_DIR}"
if [[ ! -f "${SCRIPTED_SECRETS_FILE}" ]]; then
  mkdir -p "$(dirname -- "${SCRIPTED_SECRETS_FILE}")"
  printf '{}\n' >"${SCRIPTED_SECRETS_FILE}"
fi

_shell_quote() {
  printf '%q' "$1"
}

_prepare_scripted_postgres_control_plane() {
  require_python_modules_or_exit "${PYTHON_BIN}" psycopg
  export SOURCING_RUNTIME_ENVIRONMENT=scripted
  export SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA="${SCRIPTED_POSTGRES_SCHEMA}"
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/dev_postgres_env.sh"
  if [[ -z "${SOURCING_CONTROL_PLANE_POSTGRES_DSN:-}" ]]; then
    printf 'PG-only scripted runtime requires a local Postgres DSN. Configure .local-postgres.env; SQLite fallback is not allowed for manual workflow confidence.\n' >&2
    exit 2
  fi
  export SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE="postgres_only"
  export SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1
  export SOURCING_PG_ONLY_SQLITE_BACKEND="shared_memory"

  PYTHONPATH=src "${PYTHON_BIN}" - "${PROJECT_ROOT}" "${SCRIPTED_POSTGRES_SCHEMA}" "${SCRIPTED_RESET_RUNTIME}" <<'PY'
import os
import sys

import psycopg

from sourcing_agent.local_postgres import (
    ensure_local_postgres_started,
    normalize_control_plane_postgres_connect_dsn,
    normalize_control_plane_postgres_schema,
    quote_control_plane_postgres_identifier,
)

project_root = sys.argv[1]
schema = normalize_control_plane_postgres_schema(sys.argv[2])
reset_runtime = str(sys.argv[3]).strip() == "1"
start_result = ensure_local_postgres_started(project_root)
if start_result.get("status") not in {"running", "started", "missing_assets"}:
    raise SystemExit(f"local Postgres is not ready: {start_result}")
dsn = normalize_control_plane_postgres_connect_dsn(os.environ.get("SOURCING_CONTROL_PLANE_POSTGRES_DSN", ""))
if not dsn:
    raise SystemExit("SOURCING_CONTROL_PLANE_POSTGRES_DSN is empty")
allowed_prefixes = ("sourcing_scripted", "sourcing_test", "sourcing_replay", "sourcing_simulate")
if reset_runtime and not schema.startswith(allowed_prefixes):
    raise SystemExit(f"refusing to reset unsafe scripted schema: {schema}")
with psycopg.connect(dsn, autocommit=True) as connection:
    with connection.cursor() as cursor:
        quoted = quote_control_plane_postgres_identifier(schema)
        if reset_runtime:
            cursor.execute(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {quoted}")
PY

  {
    printf '# Generated by dev_scripted_openai_agent_delta.sh. Dedicated PG control plane for manual scripted testing.\n'
    printf 'export SOURCING_CONTROL_PLANE_POSTGRES_DSN=%s\n' "$(_shell_quote "${SOURCING_CONTROL_PLANE_POSTGRES_DSN}")"
    printf 'export SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=%s\n' "$(_shell_quote "${SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE}")"
    printf 'export SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA=%s\n' "$(_shell_quote "${SCRIPTED_POSTGRES_SCHEMA}")"
    printf 'export SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1\n'
    printf 'export SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory\n'
    if [[ -n "${LOCAL_PG_ROOT:-}" ]]; then
      printf 'export LOCAL_PG_ROOT=%s\n' "$(_shell_quote "${LOCAL_PG_ROOT}")"
    fi
    if [[ -n "${LOCAL_PG_EXTRACT:-}" ]]; then
      printf 'export LOCAL_PG_EXTRACT=%s\n' "$(_shell_quote "${LOCAL_PG_EXTRACT}")"
    fi
    if [[ -n "${LOCAL_PG_DATA:-}" ]]; then
      printf 'export LOCAL_PG_DATA=%s\n' "$(_shell_quote "${LOCAL_PG_DATA}")"
    fi
    if [[ -n "${LOCAL_PG_RUN:-}" ]]; then
      printf 'export LOCAL_PG_RUN=%s\n' "$(_shell_quote "${LOCAL_PG_RUN}")"
    fi
    if [[ -n "${LOCAL_PG_PORT:-}" ]]; then
      printf 'export LOCAL_PG_PORT=%s\n' "$(_shell_quote "${LOCAL_PG_PORT}")"
    fi
    if [[ -n "${LOCAL_PG_USER:-}" ]]; then
      printf 'export LOCAL_PG_USER=%s\n' "$(_shell_quote "${LOCAL_PG_USER}")"
    fi
    if [[ -n "${LOCAL_PG_DB:-}" ]]; then
      printf 'export LOCAL_PG_DB=%s\n' "$(_shell_quote "${LOCAL_PG_DB}")"
    fi
  } >"${SCRIPTED_POSTGRES_ENV_FILE}"
  export SOURCING_LOCAL_POSTGRES_ENV_FILE="${SCRIPTED_POSTGRES_ENV_FILE}"
  printf 'Scripted control plane: Postgres schema=%s env=%s\n' "${SCRIPTED_POSTGRES_SCHEMA}" "${SCRIPTED_POSTGRES_ENV_FILE}"
}

_prepare_scripted_postgres_control_plane

export SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1
export SOURCING_SCRIPTED_PROVIDER_WEBHOOK_TOKEN="${SOURCING_SCRIPTED_PROVIDER_WEBHOOK_TOKEN:-local-scripted-provider-webhook-token}"
export SOURCING_PROVIDER_WEBHOOK_TOKEN="${SOURCING_PROVIDER_WEBHOOK_TOKEN:-${SOURCING_SCRIPTED_PROVIDER_WEBHOOK_TOKEN}}"
export SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=0
export APIFY_API_TOKEN=""
export APIFY_TOKEN=""
export APIFY_WEBHOOK_TOKEN=""
export HARVEST_API_TOKEN=""
export HARVEST_PROFILE_API_TOKEN=""
export HARVEST_PROFILE_SEARCH_API_TOKEN=""
export HARVEST_COMPANY_EMPLOYEES_API_TOKEN=""
export DATAFORSEO_LOGIN=""
export DATAFORSEO_PASSWORD=""
export SERPER_API_KEY=""

seed_baseline_summary=""
lovable_identity_summary=""
if [[ "${SCRIPTED_REFERENCE_SEED_MODE}" == "none" ]]; then
  printf 'Skipping reference baseline seed for no-baseline scripted manual runtime.\n'
  printf '{"status":"skipped","reason":"reference_baseline_seed_disabled"}\n' >"${SCRIPTED_SEED_JSON}"
  seed_baseline_summary="OpenAI no-baseline scoped-search ready: no reference baseline seeded."
else
  seed_args=(
    "${PROJECT_ROOT}/scripts/seed_reference_smoke_runtime.py"
    --runtime-dir "${SCRIPTED_RUNTIME_DIR}"
    --provider-mode scripted
    --scripted-scenario "${SCRIPTED_SCENARIO}"
    --runtime-environment scripted
    --output-json "${SCRIPTED_SEED_JSON}"
    --runtime-env-file "${SCRIPTED_POSTGRES_ENV_FILE}"
  )
  if [[ "${SCRIPTED_FAST_RUNTIME}" == "1" ]]; then
    seed_args+=(--fast-runtime)
  fi

  printf 'Seeding scripted OpenAI Agent runtime...\n'
  (
    cd "${PROJECT_ROOT}"
    PYTHONPATH=src "${PYTHON_BIN}" "${seed_args[@]}"
  ) >/dev/null

  printf 'Scripted runtime seeded: %s\n' "${SCRIPTED_RUNTIME_DIR}"
  printf 'Seed summary: %s\n' "${SCRIPTED_SEED_JSON}"
  seed_baseline_summary="$(
    "${PYTHON_BIN}" - "${SCRIPTED_SEED_JSON}" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
seed = dict(payload.get("seed") or {})
assets = list(seed.get("authoritative_assets") or [])
openai = next(
    (
        dict(item)
        for item in assets
        if str(dict(item).get("target_company") or "").strip().lower() == "openai"
    ),
    {},
)
snapshot_id = str(openai.get("snapshot_id") or "").strip()
candidate_count = int(openai.get("candidate_count") or 0)
if not snapshot_id or candidate_count <= 0:
    raise SystemExit("OpenAI baseline seed missing; refusing to start scripted delta environment.")
reasoning = dict(seed.get("openai_reasoning") or {})
print(
    "OpenAI baseline ready: "
    f"snapshot={snapshot_id}, candidates={candidate_count}, "
    f"selected_snapshots={list(reasoning.get('selected_snapshot_ids') or [])}"
)
PY
  )"
  printf '%s\n' "${seed_baseline_summary}"
  lovable_identity_summary="$(
    "${PYTHON_BIN}" - "${SCRIPTED_RUNTIME_DIR}" <<'PY'
import json
import sys
from pathlib import Path

runtime_dir = Path(sys.argv[1])
company_dir = runtime_dir / "company_assets" / "lovable"
snapshot_dir = company_dir / "20260415T020304"
snapshot_dir.mkdir(parents=True, exist_ok=True)
identity = {
    "requested_name": "Lovable",
    "canonical_name": "Lovable",
    "company_key": "lovable",
    "linkedin_slug": "lovable-dev",
    "aliases": ["lovable", "lovable.dev"],
    "confidence": "high",
}
(snapshot_dir / "identity.json").write_text(json.dumps(identity, ensure_ascii=False, indent=2), encoding="utf-8")
(company_dir / "latest_snapshot.json").write_text(
    json.dumps({"snapshot_id": snapshot_dir.name, "company_identity": identity}, ensure_ascii=False, indent=2),
    encoding="utf-8",
)
print(f"Lovable identity ready: snapshot={snapshot_dir.name}, linkedin_slug={identity['linkedin_slug']}")
PY
  )"
  printf '%s\n' "${lovable_identity_summary}"
fi

if [[ $START_BACKEND -eq 0 ]]; then
  printf 'Seed-only mode complete.\n'
  exit 0
fi

export SOURCING_RUNTIME_ENVIRONMENT=scripted
export SOURCING_EXTERNAL_PROVIDER_MODE=scripted
export SOURCING_SCRIPTED_PROVIDER_SCENARIO="${SCRIPTED_SCENARIO}"
export SOURCING_SECRETS_FILE="${SCRIPTED_SECRETS_FILE}"
export OBJECT_STORAGE_PREFIX="${SCRIPTED_OBJECT_STORAGE_PREFIX}"
export SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED=1
export SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED="${SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED:-1}"
export SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS="${SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS:-180}"
export WORKFLOW_PROGRESS_REMOTE_WAIT_TAKEOVER_AFTER_SECONDS="${WORKFLOW_PROGRESS_REMOTE_WAIT_TAKEOVER_AFTER_SECONDS:-90}"

if [[ "${SCRIPTED_FAST_RUNTIME}" == "1" ]]; then
  export WEB_SEARCH_READY_COOLDOWN_SECONDS=0
  export WEB_SEARCH_FETCH_COOLDOWN_SECONDS=0
  export WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS=0
  export WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS=0
  export SEED_DISCOVERY_READY_POLL_MIN_INTERVAL_SECONDS=0
  export SEED_DISCOVERY_FETCH_MIN_INTERVAL_SECONDS=0
  export EXPLORATION_READY_POLL_MIN_INTERVAL_SECONDS=0
  export EXPLORATION_FETCH_MIN_INTERVAL_SECONDS=0
  export DATAFORSEO_TASK_GET_BATCH_WORKERS=4
  export SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP="${SCRIPTED_HARVEST_SLEEP_SECONDS_CAP:-0.1}"
else
  export SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP="${SCRIPTED_HARVEST_SLEEP_SECONDS_CAP}"
fi

printf 'Starting scripted backend in background: %s\n' "${SCRIPTED_API_BASE_URL}"
(
  cd "${PROJECT_ROOT}"
  bash ./scripts/dev_backend.sh \
    --runtime-dir "${SCRIPTED_RUNTIME_DIR}" \
    --host "${SCRIPTED_API_HOST}" \
    --port "${SCRIPTED_API_PORT}" \
    --frontend-port "${SCRIPTED_FRONTEND_PORT}" \
    --daemon-poll-seconds "${SCRIPTED_DAEMON_POLL_SECONDS}"
) >"${SCRIPTED_BACKEND_LOG}" 2>&1 &
backend_pid="$!"
printf '%s\n' "${backend_pid}" >"${SCRIPTED_BACKEND_PID_FILE}"

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  rm -f -- "${SCRIPTED_BACKEND_PID_FILE}"
  if kill -0 "${backend_pid}" 2>/dev/null; then
    printf '\nStopping scripted backend pid %s\n' "${backend_pid}"
    kill "${backend_pid}" 2>/dev/null || true
    wait "${backend_pid}" 2>/dev/null || true
  fi
  exit "${exit_code}"
}
trap cleanup EXIT INT TERM

sleep 2
if ! kill -0 "${backend_pid}" 2>/dev/null; then
  printf 'Scripted backend did not stay running. Check %s\n' "${SCRIPTED_BACKEND_LOG}" >&2
  exit 1
fi

printf 'Backend log: %s\n' "${SCRIPTED_BACKEND_LOG}"
printf 'Open frontend: %s\n' "${SCRIPTED_FRONTEND_URL}"
if [[ "${SCRIPTED_REFERENCE_SEED_MODE}" == "none" ]]; then
  printf 'Try query: 帮我找OpenAI做Agent方向的人\n'
else
  printf 'Try queries: 帮我找OpenAI做Agent方向的人 | 帮我找Lovable的全部成员\n'
fi
printf '%s\n' "${seed_baseline_summary}"
if [[ -n "${lovable_identity_summary}" ]]; then
  printf '%s\n' "${lovable_identity_summary}"
fi
printf 'Provider mode: scripted (no live Harvest/DataForSEO calls)\n'
printf 'Scripted timing: %s, Harvest sleep cap: %ss\n' "$([[ "${SCRIPTED_FAST_RUNTIME}" == "1" ]] && printf fast || printf interactive)" "${SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP}"

if [[ $START_FRONTEND -eq 0 ]]; then
  printf 'Backend-only mode. Press Ctrl+C to stop.\n'
  wait "${backend_pid}"
  exit 0
fi

(
  cd "${PROJECT_ROOT}"
  bash ./scripts/dev_frontend.sh \
    --host "${SCRIPTED_API_HOST}" \
    --port "${SCRIPTED_FRONTEND_PORT}" \
    --api-base-url "${SCRIPTED_API_BASE_URL}"
)
