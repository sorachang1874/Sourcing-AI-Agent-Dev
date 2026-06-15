#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

export SCRIPTED_RUNTIME_DIR="${SCRIPTED_RUNTIME_DIR:-runtime/test_env/openai_no_baseline_scoped_search}"
export SCRIPTED_API_PORT="${SCRIPTED_API_PORT:-8786}"
export SCRIPTED_FRONTEND_PORT="${SCRIPTED_FRONTEND_PORT:-4186}"
export SCRIPTED_SCENARIO="${SCRIPTED_SCENARIO:-configs/scripted/openai_agent_scoped_delta_streaming.json}"
export SCRIPTED_POSTGRES_SCHEMA="${SCRIPTED_POSTGRES_SCHEMA:-sourcing_scripted_openai_no_baseline}"
export SCRIPTED_OBJECT_STORAGE_PREFIX="${SCRIPTED_OBJECT_STORAGE_PREFIX:-sourcing-ai-agent-scripted-openai-no-baseline}"
export SCRIPTED_REFERENCE_SEED_MODE=none

exec "${SCRIPT_DIR}/dev_scripted_openai_agent_delta.sh" --no-reference-baseline-seed "$@"
