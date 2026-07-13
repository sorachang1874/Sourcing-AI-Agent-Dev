#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ACTION="${1:-all}"

cd "${REPO_ROOT}"

PYTHON_QUALITY_TARGETS=(
  "src/sourcing_agent/artifact_cache.py"
  "src/sourcing_agent/asset_paths.py"
  "src/sourcing_agent/asset_sync.py"
  "src/sourcing_agent/cloud_asset_import.py"
  "src/sourcing_agent/company_registry.py"
  "src/sourcing_agent/control_plane_postgres.py"
  "src/sourcing_agent/control_plane_live_postgres.py"
  "src/sourcing_agent/control_plane_repository.py"
  "src/sourcing_agent/acquisition_command_owner.py"
  "src/sourcing_agent/operation_runtime.py"
  "src/sourcing_agent/person_asset_writer.py"
  "src/sourcing_agent/repositories/__init__.py"
  "src/sourcing_agent/repositories/manual_review.py"
  "src/sourcing_agent/repositories/serving_projection.py"
  "src/sourcing_agent/repositories/workflow_runtime.py"
  "src/sourcing_agent/projection_search_index_contract.py"
  "src/sourcing_agent/serving_projection_reader.py"
  "src/sourcing_agent/profile_registry_backfill.py"
  "src/sourcing_agent/profile_registry_utils.py"
  "src/sourcing_agent/linkedin_url_normalization.py"
  "src/sourcing_agent/public_web_quality.py"
  "src/sourcing_agent/public_web_search.py"
  "src/sourcing_agent/public_web_runtime_core.py"
  "src/sourcing_agent/crm_public_web_runtime.py"
  "src/sourcing_agent/legacy_target_candidate_public_web_runtime.py"
  "src/sourcing_agent/regression_matrix.py"
  "src/sourcing_agent/excel_intake.py"
  "src/sourcing_agent/asset_reuse_planning.py"
  "src/sourcing_agent/candidate_artifacts.py"
  "src/sourcing_agent/organization_assets.py"
  "src/sourcing_agent/orchestrator.py"
  "src/sourcing_agent/profile_fetch_owner.py"
  "src/sourcing_agent/smoke_runtime_seed.py"
  "src/sourcing_agent/workflow_smoke.py"
  "tests/test_asset_sync.py"
  "tests/test_asset_paths.py"
  "tests/test_cloud_asset_import.py"
  "tests/test_control_plane_postgres.py"
  "tests/test_company_registry.py"
  "tests/test_profile_registry_backfill.py"
  "tests/test_profile_registry_utils.py"
  "tests/test_linkedin_url_normalization.py"
  "tests/test_public_web_quality.py"
  "tests/test_public_web_search.py"
  "tests/test_target_candidate_public_web.py"
  "tests/test_regression_matrix.py"
  "tests/test_smoke_runtime_seed.py"
  "tests/test_candidate_artifacts.py"
  "tests/test_excel_intake.py"
  "tests/test_operation_runtime.py"
  "tests/test_storage_surface_guardrails.py"
  "tests/test_hosted_workflow_smoke.py"
  "tests/test_settings.py"
)

resolve_tool() {
  local tool_name="$1"
  if [[ -x "${REPO_ROOT}/.venv-tests/bin/${tool_name}" ]]; then
    printf '%s\n' "${REPO_ROOT}/.venv-tests/bin/${tool_name}"
    return 0
  fi
  if command -v "${tool_name}" >/dev/null 2>&1; then
    command -v "${tool_name}"
    return 0
  fi
  printf 'Missing required tool: %s\n' "${tool_name}" >&2
  exit 1
}

run_lint() {
  local ruff_bin
  ruff_bin="$(resolve_tool ruff)"
  "${ruff_bin}" format --check "${PYTHON_QUALITY_TARGETS[@]}"
  "${ruff_bin}" check "${PYTHON_QUALITY_TARGETS[@]}"
}

run_typecheck() {
  local mypy_bin
  mypy_bin="$(resolve_tool mypy)"
  "${mypy_bin}"
}

case "${ACTION}" in
  lint)
    run_lint
    ;;
  typecheck)
    run_typecheck
    ;;
  all)
    run_lint
    run_typecheck
    ;;
  *)
    printf 'Usage: %s [lint|typecheck|all]\n' "$0" >&2
    exit 1
    ;;
esac
