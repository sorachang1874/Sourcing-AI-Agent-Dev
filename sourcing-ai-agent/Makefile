SHELL := $(firstword $(wildcard /opt/homebrew/bin/bash /usr/local/bin/bash /bin/bash))
.DEFAULT_GOAL := help

HOST ?= 0.0.0.0
PYTHON_BIN ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,$(if $(wildcard .venv-tests/bin/python),.venv-tests/bin/python,python3))
TEST_PYTHON_BIN ?= $(if $(wildcard .venv-tests/bin/python),.venv-tests/bin/python,$(PYTHON_BIN))
TESTCONTAINERS_POSTGRES_IMAGE ?= public.ecr.aws/docker/library/postgres:16-alpine
LOCAL_PG_DOCKER_BIN_PATH ?= /Applications/Docker.app/Contents/Resources/bin
LOCAL_PG_DOCKER_CMD = PATH="$(LOCAL_PG_DOCKER_BIN_PATH):$$PATH" PYTHONPATH=src "$(PYTHON_BIN)" -m sourcing_agent.local_postgres_docker
API_PORT ?= 8765
FRONTEND_PORT ?= 4173
RUNTIME_DIR ?= runtime
ALLOWED_FRONTEND_PORTS ?= 4173 4174
API_BASE_URL ?= http://localhost:$(API_PORT)
ALLOW_ORIGIN ?=
EXTRA_BACKEND_ARGS ?=
EXTRA_FRONTEND_ARGS ?=
BACKEND_LOG ?= runtime/service_logs/make-dev-backend.log
BACKEND_PID_FILE ?= runtime/service_logs/make-dev-backend.pid
BACKEND_LAUNCH_LOG ?= runtime/service_logs/dev-backend-launch.log
BACKEND_LAUNCH_STATUS ?= runtime/service_logs/dev-backend-launch.status.json
FRONTEND_LAUNCH_LOG ?= runtime/service_logs/dev-frontend-launch.log
FRONTEND_LAUNCH_STATUS ?= runtime/service_logs/dev-frontend-launch.status.json
TEST_API_PORT ?= 8775
TEST_FRONTEND_PORT ?= 4175
TEST_RUNTIME_DIR ?= runtime/test_env
TEST_ALLOWED_FRONTEND_PORTS ?= 4175 4176
TEST_API_BASE_URL ?= http://localhost:$(TEST_API_PORT)
TEST_SECRETS_FILE ?= runtime/secrets/providers.local.json
TEST_EXTERNAL_PROVIDER_MODE ?= simulate
TEST_RUNTIME_ENVIRONMENT ?= test
TEST_OBJECT_STORAGE_PREFIX ?= sourcing-ai-agent-test
TEST_SCRIPTED_PROVIDER_SCENARIO ?=
TEST_EXTRA_BACKEND_ARGS ?=
TEST_EXTRA_FRONTEND_ARGS ?=
TEST_ENV_SEED_COMPANIES ?=
TEST_ENV_SEED_LINK_MODE ?= symlink
SCRIPTED_OPENAI_AGENT_RUNTIME_DIR ?= runtime/test_env/openai_agent_delta_streaming
SCRIPTED_OPENAI_AGENT_API_PORT ?= 8785
SCRIPTED_OPENAI_AGENT_FRONTEND_PORT ?= 4185
SCRIPTED_OPENAI_AGENT_SCENARIO ?= configs/scripted/openai_agent_and_lovable_streaming.json
SCRIPTED_OPENAI_AGENT_SECRETS_FILE ?= $(SCRIPTED_OPENAI_AGENT_RUNTIME_DIR)/secrets/providers.local.json
TEST_BACKEND_LOG ?= $(TEST_RUNTIME_DIR)/service_logs/test-env-backend.log
TEST_BACKEND_PID_FILE ?= $(TEST_RUNTIME_DIR)/service_logs/test-env-backend.pid
TEST_BACKEND_LAUNCH_LOG ?= $(TEST_RUNTIME_DIR)/service_logs/test-env-backend-launch.log
TEST_BACKEND_LAUNCH_STATUS ?= $(TEST_RUNTIME_DIR)/service_logs/test-env-backend-launch.status.json
LIVE_TEST_API_PORT ?= 8777
LIVE_TEST_RUNTIME_DIR ?= runtime/test_env_live
LIVE_TEST_ALLOWED_FRONTEND_PORTS ?= 4177 4178
LIVE_TEST_API_BASE_URL ?= http://localhost:$(LIVE_TEST_API_PORT)
LIVE_TEST_SECRETS_FILE ?= runtime/secrets/providers.local.json
LIVE_TEST_RUNTIME_ENVIRONMENT ?= test
LIVE_TEST_OBJECT_STORAGE_PREFIX ?= sourcing-ai-agent-live-test
LIVE_TEST_EXTRA_BACKEND_ARGS ?=
LIVE_TEST_BACKEND_LOG ?= $(LIVE_TEST_RUNTIME_DIR)/service_logs/test-env-live-backend.log
LIVE_TEST_BACKEND_PID_FILE ?= $(LIVE_TEST_RUNTIME_DIR)/service_logs/test-env-live-backend.pid
LIVE_LARGE_ORG_CASES ?= xai_full_roster xai_coding_all_members_scoped
CRM_PUBLIC_WEB_LIVE_BASE_URL ?= $(LIVE_TEST_API_BASE_URL)
CRM_PUBLIC_WEB_LIVE_RECORD_IDS ?=
CRM_PUBLIC_WEB_LIVE_REPORT_JSON ?= output/w7g_crm_public_web_live_product_validation.json
CRM_PUBLIC_WEB_LIVE_DRY_RUN ?= 1
CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED ?= 0
CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED ?= 0
CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT ?=
CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_DIGEST_SHA256 ?=
CRM_PUBLIC_WEB_LIVE_FORCE_REFRESH ?= 0
CRM_PUBLIC_WEB_LIVE_POLL_TIMEOUT_SECONDS ?= 600
CRM_PUBLIC_WEB_LIVE_MAX_FETCHES_PER_CANDIDATE ?= 10
CRM_PUBLIC_WEB_LIVE_MAX_AI_EVIDENCE_DOCUMENTS ?= 10
CRM_PUBLIC_WEB_LIVE_MAX_AI_ENTRY_LINKS ?= 10
CRM_PUBLIC_WEB_LIVE_MAX_REMOTE_SEARCH_WAIT_SECONDS ?= 420
CRM_PUBLIC_WEB_LIVE_MAX_PROVIDER_PENDING_WAIT_SECONDS ?= 420
CRM_PUBLIC_WEB_LIVE_MAX_CONCURRENT_CANDIDATE_ANALYSES ?= 1
REVIEW_TITLE ?= independent-review
REVIEW_FILES ?=
REVIEW_BASE ?=
REVIEW_EXTRA_CONTEXT ?=
REVIEW_TIMEOUT_SECONDS ?= 420
REVIEW_OUTPUT ?=
REVIEW_EXECUTE ?= 0
ASSET_MEDIA_BACKFILL_RUNTIME_DIR ?= $(RUNTIME_DIR)
ASSET_MEDIA_BACKFILL_LIMIT ?= 1000
ASSET_MEDIA_BACKFILL_COLLECTION_IDS ?=
ASSET_MEDIA_BACKFILL_PROJECTION_IDS ?=
ASSET_MEDIA_BACKFILL_COMPANIES ?=
ASSET_MEDIA_BACKFILL_PUBLIC_WEB_RUN_IDS ?=
ASSET_MEDIA_BACKFILL_LOGO_SOURCE_URLS ?=
ASSET_MEDIA_BACKFILL_INCLUDE_HOMEPAGE_FAVICON ?= 0
ASSET_MEDIA_BACKFILL_RUN_NOW ?= 0
ASSET_MEDIA_BACKFILL_OUTPUT_JSON ?= output/asset_media_backfill_dry_run.json
ASSET_MEDIA_BACKFILL_APPLY_OUTPUT_JSON ?= output/asset_media_backfill_apply.json
ASSET_MEDIA_BACKFILL_REVIEWED ?= 0
HOSTED_RUNTIME_DIR ?= runtime_hosted
HOSTED_API_HOST ?= 127.0.0.1
HOSTED_API_PORT ?= 8765
HOSTED_FRONTEND_ORIGIN ?= https://demo.111874.xyz
HOSTED_EXTERNAL_PROVIDER_MODE ?= live
HOSTED_RUNTIME_ENVIRONMENT ?= production
HOSTED_MAX_PARALLEL_REQUESTS ?= 8
HOSTED_LIGHT_REQUEST_RESERVED ?= 2
HOSTED_API_BASE_URL ?= https://api.111874.xyz
HOSTED_BACKEND_LAUNCH_LOG ?= $(HOSTED_RUNTIME_DIR)/service_logs/hosted-backend-launch.log
HOSTED_BACKEND_LAUNCH_STATUS ?= $(HOSTED_RUNTIME_DIR)/service_logs/hosted-backend-launch.status.json

BACKEND_CMD = "$(SHELL)" ./scripts/dev_backend.sh --runtime-dir "$(RUNTIME_DIR)" --host "$(HOST)" --port "$(API_PORT)" $(foreach port,$(ALLOWED_FRONTEND_PORTS),--frontend-port "$(port)") $(if $(ALLOW_ORIGIN),--allow-origin "$(ALLOW_ORIGIN)") $(EXTRA_BACKEND_ARGS)
FRONTEND_CMD = "$(SHELL)" ./scripts/dev_frontend.sh --host "$(HOST)" --port "$(FRONTEND_PORT)" --api-base-url "$(API_BASE_URL)" $(EXTRA_FRONTEND_ARGS)
STATUS_CMD = "$(SHELL)" ./scripts/dev_status.sh --runtime-dir "$(RUNTIME_DIR)" --api-port "$(API_PORT)" --frontend-port "$(FRONTEND_PORT)" --api-base-url "$(API_BASE_URL)"
STOP_CMD = "$(SHELL)" ./scripts/dev_stop.sh --runtime-dir "$(RUNTIME_DIR)" --api-port "$(API_PORT)" --frontend-port "$(FRONTEND_PORT)"
LOGS_CMD = "$(SHELL)" ./scripts/dev_logs.sh --runtime-dir "$(RUNTIME_DIR)" --api-port "$(API_PORT)" --frontend-port "$(FRONTEND_PORT)" $(EXTRA_LOG_ARGS)
CLEAN_CMD = "$(SHELL)" ./scripts/dev_clean.sh --runtime-dir "$(RUNTIME_DIR)" --api-port "$(API_PORT)" --frontend-port "$(FRONTEND_PORT)" $(EXTRA_CLEAN_ARGS)
DOCTOR_CMD = env DEV_PYTHON_BIN="$(PYTHON_BIN)" "$(SHELL)" ./scripts/dev_doctor.sh
AGENT_NETWORK_PREFLIGHT_CMD = "$(SHELL)" ./scripts/agent_network_preflight.sh
DOCKER_CONTEXT_CMD = "$(SHELL)" ./scripts/with_docker_context.sh
DOCKER_START_CMD = "$(SHELL)" ./scripts/docker_start.sh
DOCKER_DOCTOR_CMD = env TEST_PYTHON_BIN="$(TEST_PYTHON_BIN)" PYTHON_BIN="$(PYTHON_BIN)" $(DOCKER_CONTEXT_CMD) "$(SHELL)" ./scripts/docker_doctor.sh
BOOTSTRAP_TEST_ENV_CMD = env BOOTSTRAP_TEST_ENV_PYTHON_BIN="$(PYTHON_BIN)" "$(SHELL)" ./scripts/bootstrap_test_env.sh
TEST_BACKEND_CMD = SOURCING_RUNTIME_ENVIRONMENT="$(TEST_RUNTIME_ENVIRONMENT)" SOURCING_EXTERNAL_PROVIDER_MODE="$(TEST_EXTERNAL_PROVIDER_MODE)" SOURCING_SECRETS_FILE="$(abspath $(TEST_SECRETS_FILE))" OBJECT_STORAGE_PREFIX="$(TEST_OBJECT_STORAGE_PREFIX)" $(if $(TEST_SCRIPTED_PROVIDER_SCENARIO),SOURCING_SCRIPTED_PROVIDER_SCENARIO="$(TEST_SCRIPTED_PROVIDER_SCENARIO)") "$(SHELL)" ./scripts/dev_backend.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --host "$(HOST)" --port "$(TEST_API_PORT)" $(foreach port,$(TEST_ALLOWED_FRONTEND_PORTS),--frontend-port "$(port)") $(if $(ALLOW_ORIGIN),--allow-origin "$(ALLOW_ORIGIN)") $(TEST_EXTRA_BACKEND_ARGS)
TEST_FRONTEND_CMD = "$(SHELL)" ./scripts/dev_frontend.sh --host "$(HOST)" --port "$(TEST_FRONTEND_PORT)" --api-base-url "$(TEST_API_BASE_URL)" $(TEST_EXTRA_FRONTEND_ARGS)
TEST_STATUS_CMD = "$(SHELL)" ./scripts/dev_status.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --api-port "$(TEST_API_PORT)" --frontend-port "$(TEST_FRONTEND_PORT)" --api-base-url "$(TEST_API_BASE_URL)"
TEST_STOP_CMD = "$(SHELL)" ./scripts/dev_stop.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --api-port "$(TEST_API_PORT)" --frontend-port "$(TEST_FRONTEND_PORT)"
TEST_LOGS_CMD = "$(SHELL)" ./scripts/dev_logs.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --api-port "$(TEST_API_PORT)" --frontend-port "$(TEST_FRONTEND_PORT)" $(EXTRA_LOG_ARGS)
TEST_CLEAN_CMD = "$(SHELL)" ./scripts/dev_clean.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --api-port "$(TEST_API_PORT)" --frontend-port "$(TEST_FRONTEND_PORT)" $(EXTRA_CLEAN_ARGS)
TEST_SEED_CMD = PYTHONPATH=src "$(PYTHON_BIN)" ./scripts/seed_test_env_assets.py --source-runtime-dir "$(RUNTIME_DIR)" --target-runtime-dir "$(TEST_RUNTIME_DIR)" --link-mode "$(TEST_ENV_SEED_LINK_MODE)" $(foreach company,$(TEST_ENV_SEED_COMPANIES),--company "$(company)")
TEST_BROWSER_E2E_FAST_CMD = PYTHONPATH=src SOURCING_RUN_FRONTEND_BROWSER_E2E=1 "$(PYTHON_BIN)" -m unittest tests.test_frontend_browser_e2e.FrontendBrowserFastE2ETest -v
TEST_BROWSER_E2E_SLOW_CMD = SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_SLOW_BROWSER_E2E=1 PYTHONPATH=src "$(PYTHON_BIN)" -m unittest tests.test_frontend_browser_e2e.FrontendBrowserSlowE2ETest -v
TEST_BROWSER_E2E_FULL_CMD = SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_SLOW_BROWSER_E2E=1 PYTHONPATH=src "$(PYTHON_BIN)" -m unittest tests.test_frontend_browser_e2e -v
TEST_SCRIPTED_LARGE_ORG_CMD = PYTHONPATH=src "$(PYTHON_BIN)" -m unittest \
	tests.test_hosted_workflow_smoke.HostedWorkflowSmokeTest.test_hosted_scripted_google_long_tail_timeout_completes_without_manual_takeover \
	tests.test_hosted_workflow_smoke.HostedWorkflowSmokeTest.test_hosted_scripted_large_org_full_roster_overflow_completes_without_live_provider \
	tests.test_hosted_workflow_smoke.HostedWorkflowSmokeTest.test_hosted_scripted_large_org_profile_tail_reconciles_after_background_prefetch \
	tests.test_hosted_workflow_smoke.HostedWorkflowSmokeTest.test_hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch -v
CI_PG_CONTRACT_CMD = $(DOCKER_CONTEXT_CMD) env PYTHONPATH=src SOURCING_REQUIRE_TESTCONTAINERS=1 SOURCING_RUN_TESTCONTAINERS=1 TESTCONTAINERS_RYUK_DISABLED=1 SOURCING_TESTCONTAINERS_POSTGRES_IMAGE="$(TESTCONTAINERS_POSTGRES_IMAGE)" "$(TEST_PYTHON_BIN)" -m unittest tests.testcontainers_pg_contract -v
CI_WORKFLOW_FAKE_PROVIDER_CMD = $(DOCKER_CONTEXT_CMD) env PYTHONPATH=src SOURCING_REQUIRE_TESTCONTAINERS=1 SOURCING_RUN_TESTCONTAINERS=1 TESTCONTAINERS_RYUK_DISABLED=1 SOURCING_TESTCONTAINERS_POSTGRES_IMAGE="$(TESTCONTAINERS_POSTGRES_IMAGE)" "$(TEST_PYTHON_BIN)" -m unittest tests.testcontainers_workflow_fake_provider -v
CI_FRONTEND_BROWSER_GATE_CMD = $(DOCKER_CONTEXT_CMD) env PYTHONPATH=src SOURCING_REQUIRE_TESTCONTAINERS=1 SOURCING_RUN_TESTCONTAINERS=1 TESTCONTAINERS_RYUK_DISABLED=1 SOURCING_TESTCONTAINERS_POSTGRES_IMAGE="$(TESTCONTAINERS_POSTGRES_IMAGE)" "$(TEST_PYTHON_BIN)" -m unittest tests.testcontainers_frontend_browser_gate -v
CI_WORKFLOW_BROWSER_GATE_CMD = $(DOCKER_CONTEXT_CMD) env PYTHONPATH=src SOURCING_REQUIRE_TESTCONTAINERS=1 SOURCING_RUN_TESTCONTAINERS=1 TESTCONTAINERS_RYUK_DISABLED=1 SOURCING_TESTCONTAINERS_POSTGRES_IMAGE="$(TESTCONTAINERS_POSTGRES_IMAGE)" "$(TEST_PYTHON_BIN)" -m unittest tests.testcontainers_workflow_browser_gate -v
CI_PRE_AGENT_CONTRACT_CMD = export SOURCING_REQUIRE_PG_STORE_TESTS=1 SOURCING_REQUIRE_PG_DURABLE_RUNTIME_TESTS=1 && PYTHONPATH=src "$(TEST_PYTHON_BIN)" -m pytest tests/test_pre_agent_contract_review.py tests/test_independent_review_gate_runner.py tests/test_operation_runtime.py tests/test_crm_public_web_runtime_boundary.py tests/test_legacy_public_web_retirement_audit.py tests/test_storage_surface_guardrails.py tests/test_pg_onconflict_guard.py -q && SOURCING_REQUIRE_PG_DURABLE_RUNTIME_TESTS=1 PYTHONPATH=src "$(TEST_PYTHON_BIN)" -m pytest tests/test_enrichment.py -k 'isolates_retry_wait or retry_wait_as_isolated_batch' -q && PYTHONPATH=src "$(TEST_PYTHON_BIN)" -m pytest tests/test_dataforseo_client.py tests/test_search_provider.py tests/test_public_web_search.py tests/test_seed_discovery.py tests/test_exploratory_enrichment.py -k 'partial_task_errors or dataforseo_provider_submit_batch_retries_only_failed_query_item or dataforseo_provider_submit_batch_fails_closed_without_provider_identity or batch_provider_requires_stable_query_identity_key or provider_chain_rejects_missing_query_identity_before_fallback or dataforseo_provider_fetch_ready_batch_preserves_success_when_one_task_get_fails or batch_queue_submit_failure_is_query_level_not_whole_batch or public_web_batch_query_identity_is_candidate_query_scoped_not_order_scoped or search_seed_worker_key_uses_query_identity_not_order_ordinal or exploration_query_task_key_uses_query_identity_not_order_ordinal or model_native_search_provider_order_fails_closed_without_contract' -q && PYTHONPATH=src "$(TEST_PYTHON_BIN)" -m pytest tests/test_projection_crm_api_contracts.py -k asset_backfill_http_routes_are_explicit_migration_paths -q && PYTHONPATH=src "$(TEST_PYTHON_BIN)" -m pytest tests/test_scripted_smoke_signoff.py -k durable_command_owner_contract -q && PYTHONPATH=src "$(TEST_PYTHON_BIN)" ./scripts/run_crm_public_web_live_product_validation.py --report-json "$(CRM_PUBLIC_WEB_LIVE_REPORT_JSON)"
LOCAL_CONTAINER_SMOKE_CMD = $(DOCKER_CONTEXT_CMD) env PYTHONPATH=src SOURCING_RUN_TESTCONTAINERS=1 TESTCONTAINERS_RYUK_DISABLED=1 SOURCING_TESTCONTAINERS_POSTGRES_IMAGE="$(TESTCONTAINERS_POSTGRES_IMAGE)" "$(TEST_PYTHON_BIN)" -m unittest tests.testcontainers_pg_contract -v
LIVE_TEST_BACKEND_CMD = SOURCING_RUNTIME_ENVIRONMENT="$(LIVE_TEST_RUNTIME_ENVIRONMENT)" SOURCING_EXTERNAL_PROVIDER_MODE="live" SOURCING_LIVE_PROVIDER_CONFIRM="1" SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS="1" SOURCING_ALLOW_TEST_REPO_POSTGRES_ENV="1" SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE="postgres_only" SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES="1" SOURCING_PG_ONLY_SQLITE_BACKEND="shared_memory" SOURCING_SECRETS_FILE="$(abspath $(LIVE_TEST_SECRETS_FILE))" OBJECT_STORAGE_PREFIX="$(LIVE_TEST_OBJECT_STORAGE_PREFIX)" "$(SHELL)" ./scripts/dev_backend.sh --runtime-dir "$(LIVE_TEST_RUNTIME_DIR)" --host "$(HOST)" --port "$(LIVE_TEST_API_PORT)" $(foreach port,$(LIVE_TEST_ALLOWED_FRONTEND_PORTS),--frontend-port "$(port)") $(if $(ALLOW_ORIGIN),--allow-origin "$(ALLOW_ORIGIN)") $(LIVE_TEST_EXTRA_BACKEND_ARGS)
TEST_LIVE_LARGE_ORG_CMD = PYTHONPATH=src SOURCING_LIVE_PROVIDER_CONFIRM="1" SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS="1" "$(PYTHON_BIN)" ./scripts/run_live_large_org_regression.py --base-url "$(LIVE_TEST_API_BASE_URL)" $(foreach case,$(LIVE_LARGE_ORG_CASES),--case "$(case)") --confirm-live-provider-cost --strict
CRM_PUBLIC_WEB_LIVE_VALIDATION_CMD = PYTHONPATH=src "$(PYTHON_BIN)" ./scripts/run_crm_public_web_live_product_validation.py --base-url "$(CRM_PUBLIC_WEB_LIVE_BASE_URL)" $(foreach record,$(CRM_PUBLIC_WEB_LIVE_RECORD_IDS),--crm-record-id "$(record)") --report-json "$(CRM_PUBLIC_WEB_LIVE_REPORT_JSON)" --poll-timeout-seconds "$(CRM_PUBLIC_WEB_LIVE_POLL_TIMEOUT_SECONDS)" --max-fetches-per-candidate "$(CRM_PUBLIC_WEB_LIVE_MAX_FETCHES_PER_CANDIDATE)" --max-ai-evidence-documents "$(CRM_PUBLIC_WEB_LIVE_MAX_AI_EVIDENCE_DOCUMENTS)" --max-ai-entry-links "$(CRM_PUBLIC_WEB_LIVE_MAX_AI_ENTRY_LINKS)" --max-remote-search-wait-seconds "$(CRM_PUBLIC_WEB_LIVE_MAX_REMOTE_SEARCH_WAIT_SECONDS)" --max-provider-pending-wait-seconds "$(CRM_PUBLIC_WEB_LIVE_MAX_PROVIDER_PENDING_WAIT_SECONDS)" --max-concurrent-candidate-analyses "$(CRM_PUBLIC_WEB_LIVE_MAX_CONCURRENT_CANDIDATE_ANALYSES)" $(if $(filter 1,$(CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED)),--reviewed-crm-record-ids,) $(if $(filter 1,$(CRM_PUBLIC_WEB_LIVE_FORCE_REFRESH)),--force-refresh,) $(if $(filter 0,$(CRM_PUBLIC_WEB_LIVE_DRY_RUN)),--execute-live --confirm-live-provider-cost,)
REVIEWER_CODEX_HOME ?= runtime/reviewer_codex_home
REVIEWER_CODEX_BOOTSTRAP_CMD = REVIEWER_CODEX_HOME="$(abspath $(REVIEWER_CODEX_HOME))" "$(SHELL)" ./scripts/bootstrap_reviewer_codex_home.sh
INDEPENDENT_REVIEW_GATE_CMD = CODEX_HOME="$(abspath $(REVIEWER_CODEX_HOME))" PYTHONPATH=src "$(PYTHON_BIN)" ./scripts/run_independent_review_gate.py --title "$(REVIEW_TITLE)" --files "$(REVIEW_FILES)" --base "$(REVIEW_BASE)" --extra-context "$(REVIEW_EXTRA_CONTEXT)" --timeout-seconds "$(REVIEW_TIMEOUT_SECONDS)" $(if $(REVIEW_OUTPUT),--output "$(REVIEW_OUTPUT)",) $(if $(filter 1,$(REVIEW_EXECUTE)),--execute,)
ASSET_MEDIA_BACKFILL_ARGS = --runtime-dir "$(ASSET_MEDIA_BACKFILL_RUNTIME_DIR)" --limit "$(ASSET_MEDIA_BACKFILL_LIMIT)" $(foreach collection,$(ASSET_MEDIA_BACKFILL_COLLECTION_IDS),--collection-id "$(collection)") $(foreach projection,$(ASSET_MEDIA_BACKFILL_PROJECTION_IDS),--projection-id "$(projection)") $(foreach company,$(ASSET_MEDIA_BACKFILL_COMPANIES),--company "$(company)") $(foreach run_id,$(ASSET_MEDIA_BACKFILL_PUBLIC_WEB_RUN_IDS),--public-web-run-id "$(run_id)") $(foreach mapping,$(ASSET_MEDIA_BACKFILL_LOGO_SOURCE_URLS),--company-logo-source-url "$(mapping)") $(if $(filter 1,$(ASSET_MEDIA_BACKFILL_INCLUDE_HOMEPAGE_FAVICON)),--include-homepage-favicon,) $(if $(filter 1,$(ASSET_MEDIA_BACKFILL_RUN_NOW)),--run-now,)
ASSET_MEDIA_BACKFILL_DRY_RUN_CMD = PYTHONPATH=src "$(PYTHON_BIN)" ./scripts/backfill_person_company_asset_media.py $(ASSET_MEDIA_BACKFILL_ARGS) --output-json "$(ASSET_MEDIA_BACKFILL_OUTPUT_JSON)" --strict
ASSET_MEDIA_BACKFILL_APPLY_CMD = PYTHONPATH=src "$(PYTHON_BIN)" ./scripts/backfill_person_company_asset_media.py $(ASSET_MEDIA_BACKFILL_ARGS) --output-json "$(ASSET_MEDIA_BACKFILL_APPLY_OUTPUT_JSON)" --apply --reviewed --strict
HOSTED_TRIAL_BACKEND_CMD = SOURCING_RUNTIME_ENVIRONMENT="$(HOSTED_RUNTIME_ENVIRONMENT)" SOURCING_EXTERNAL_PROVIDER_MODE="$(HOSTED_EXTERNAL_PROVIDER_MODE)" HOSTED_MAX_PARALLEL_REQUESTS="$(HOSTED_MAX_PARALLEL_REQUESTS)" HOSTED_LIGHT_REQUEST_RESERVED="$(HOSTED_LIGHT_REQUEST_RESERVED)" "$(SHELL)" ./scripts/run_hosted_trial_backend.sh --runtime-dir "$(HOSTED_RUNTIME_DIR)" --host "$(HOSTED_API_HOST)" --port "$(HOSTED_API_PORT)" --origin "$(HOSTED_FRONTEND_ORIGIN)"
FRONTEND_HOSTED_BUILD_CMD = cd frontend-demo && VITE_API_BASE_URL="$(HOSTED_API_BASE_URL)" npm run build:hosted
FRONTEND_HOSTED_BUILD_SAME_ORIGIN_CMD = cd frontend-demo && VITE_API_BASE_URL="same-origin" npm run build:hosted
PYTHON_QUALITY_CMD = "$(SHELL)" ./scripts/run_python_quality.sh
DETACHED_LAUNCH_CMD = PYTHONPATH=src "$(PYTHON_BIN)" -m sourcing_agent.cli launch-detached

.PHONY: help bootstrap-test-env dev-doctor agent-network-preflight docker-start docker-doctor local-pg-up local-pg-down local-pg-status dev-backend dev-frontend dev-launch-backend dev-launch-frontend dev dev-status dev-stop dev-logs dev-clean hosted-trial-backend hosted-trial-backend-launch frontend-build-hosted frontend-build-hosted-same-origin lint typecheck verify-python test-env-backend test-env-launch-backend test-env-frontend test-env test-env-status test-env-stop test-env-logs test-env-clean test-env-seed-assets test-env-scripted-openai-agent test-env-scripted-openai-agent-backend test-env-scripted-openai-agent-seed test-env-scripted-openai-agent-status test-env-scripted-openai-agent-stop test-env-scripted-openai-agent-logs test-browser-e2e-fast test-browser-e2e-slow test-browser-e2e-full test-scripted-large-org ci-pg-contract ci-workflow-fake-provider ci-frontend-browser-gate ci-workflow-browser-gate ci-pre-agent-contract independent-review-gate ci-containerized-pre-release local-container-smoke test-env-backend-live test-live-large-org-manual test-crm-public-web-live-product-validation asset-media-backfill-dry-run asset-media-backfill-apply prune-test-env prune-test-env-apply

help:
	@printf '%s\n' \
	  'Common local dev targets:' \
	  '  make bootstrap-test-env  Recreate/validate .venv-tests from the repo runtime Python' \
	  '  make dev-doctor     Check bash/python/env invariants before local startup' \
	  '  make agent-network-preflight  Read-only GitHub/Codex/Clash network check for Agent sessions' \
	  '  make docker-start   Start Docker Desktop/Colima for Testcontainers gates' \
	  '  make docker-doctor  Check Docker daemon and Testcontainers dependencies' \
	  '  make local-pg-up    Start Docker-backed local control-plane Postgres (port 55432) and refresh .local-postgres.env' \
	  '  make local-pg-down  Stop the Docker-backed local control-plane Postgres container' \
	  '  make local-pg-status Show Docker-backed local Postgres container / env-file status' \
	  '  make dev-backend    Start local backend helper' \
	  '  make dev-frontend   Start local frontend helper' \
	  '  make dev-launch-backend  Launch local backend detached (WSL/Cursor-safe)' \
	  '  make dev-launch-frontend Launch local frontend detached' \
	  '  make dev            Start backend in background, then frontend in foreground' \
	  '  make dev-status     Inspect backend / frontend listeners and probe health' \
	  '  make dev-stop       Stop backend / frontend listeners on the configured ports' \
	  '  make dev-logs       Tail backend helper and common runtime logs' \
	  '  make dev-clean      Stop local dev services and remove helper logs/state' \
	  '  make hosted-trial-backend Start lean hosted backend profile for ECS/Cloudflare trial' \
	  '  make hosted-trial-backend-launch Launch hosted trial backend detached' \
	  '  make frontend-build-hosted HOSTED_API_BASE_URL=https://api.111874.xyz' \
	  '  make frontend-build-hosted-same-origin  Build hosted frontend for same-origin /api ingress' \
	  '  make lint           Run staged Python ruff checks for core infra modules' \
	  '  make typecheck      Run staged Python mypy checks for core infra modules' \
	  '  make verify-python  Run staged Python lint + typecheck together' \
	  '  make test-env-backend Start isolated test backend (simulate by default)' \
	  '  make test-env-launch-backend Launch isolated test backend detached' \
	  '  make test-env-frontend Start frontend against the isolated test backend' \
	  '  make test-env       Start isolated test backend in background, then frontend in foreground' \
	  '  make test-env-status Inspect isolated test env listeners and runtime services' \
	  '  make test-env-stop  Stop isolated test env listeners' \
	  '  make test-env-logs  Tail isolated test env logs' \
	  '  make test-env-clean Stop isolated test env and remove helper logs/state' \
	  '  make test-env-seed-assets TEST_ENV_SEED_COMPANIES="anthropic xai"' \
	  '  make test-env-scripted-openai-agent  Start interactive scripted OpenAI Agent delta streaming env' \
	  '  make test-env-scripted-openai-agent-backend  Start only the scripted backend' \
	  '  make test-env-scripted-openai-agent-stop  Stop scripted OpenAI Agent env ports' \
	  '  make test-browser-e2e-fast  Run default fast browser E2E suite' \
	  '  make test-browser-e2e-slow  Run slow large-org browser E2E suite' \
	  '  make test-browser-e2e-full  Run fast + slow browser E2E suite' \
	  '  make test-scripted-large-org Run scripted large-org behavior regressions (overflow + profile tail + no-repeat tail)' \
	  '  make ci-pg-contract Run disposable Testcontainers PG storage/writer contract harness' \
	  '  make ci-workflow-fake-provider Run disposable PG + fake Apify workflow provider contract harness' \
		  '  make ci-frontend-browser-gate Run disposable PG + backend/frontend/browser projection board gate' \
		  '  make ci-workflow-browser-gate Run workflow-driven fake-provider projection browser gate' \
		  '  make ci-pre-agent-contract Run W7/W9/W10/W11 fast contract gates before Agent UI/graph work' \
		  '  make independent-review-gate REVIEW_TITLE="..." REVIEW_FILES="..." REVIEW_EXECUTE=1  Run/read prompt for independent adversarial review' \
		  '  make ci-containerized-pre-release Run Docker doctor + all containerized pre-release gates' \
	  '  make local-container-smoke Run optional local Testcontainers PG smoke' \
	  '  make asset-media-backfill-dry-run  Preview Phase 5c/7c person/company asset-media backfill' \
	  '  make asset-media-backfill-apply ASSET_MEDIA_BACKFILL_REVIEWED=1  Apply reviewed Phase 5c/7c backfill' \
	  '  make test-env-backend-live LIVE_CONFIRM=1  Start isolated live backend for manual large-org validation' \
	  '  make test-live-large-org-manual LIVE_CONFIRM=1  Run manual live large-org regression against the isolated live backend' \
	  '  make test-crm-public-web-live-product-validation  Dry-run W7g CRM Public Web guarded validation' \
	  '' \
	  'Common overrides:' \
	  '  make dev FRONTEND_PORT=4174' \
	  '  make dev API_PORT=8766 FRONTEND_PORT=4174 API_BASE_URL=http://localhost:8766' \
	  '  make dev ALLOWED_FRONTEND_PORTS="4174 4180"' \
	  '  make dev-backend ALLOW_ORIGIN=http://192.168.1.10:4173' \
	  '  make dev-status API_PORT=8766 FRONTEND_PORT=4174 API_BASE_URL=http://localhost:8766' \
	  '  make dev-stop API_PORT=8766 FRONTEND_PORT=4174' \
	  '  make dev-logs EXTRA_LOG_ARGS=--no-follow' \
	  '  make dev-clean EXTRA_CLEAN_ARGS=--dry-run' \
	  '  make bootstrap-test-env PYTHON_BIN=.venv/bin/python' \
	  '  make dev-doctor PYTHON_BIN=.venv/bin/python' \
	  '  make dev-backend EXTRA_BACKEND_ARGS=--print-config' \
	  '  make dev-frontend EXTRA_FRONTEND_ARGS=--print-config' \
	  '  make test-env TEST_API_PORT=8775 TEST_FRONTEND_PORT=4175' \
	  '  make test-env-backend TEST_EXTERNAL_PROVIDER_MODE=scripted TEST_SCRIPTED_PROVIDER_SCENARIO=configs/scripted/google_multimodal_long_tail.json' \
	  '  make test-env-scripted-openai-agent SCRIPTED_OPENAI_AGENT_API_PORT=8785 SCRIPTED_OPENAI_AGENT_FRONTEND_PORT=4185' \
	  '  make test-env-backend-live LIVE_CONFIRM=1 LIVE_TEST_API_PORT=8777' \
	  '  make test-live-large-org-manual LIVE_CONFIRM=1 LIVE_TEST_API_BASE_URL=http://localhost:8777' \
	  '  make test-crm-public-web-live-product-validation CRM_PUBLIC_WEB_LIVE_RECORD_IDS="crm_..."' \
	  '  make asset-media-backfill-dry-run ASSET_MEDIA_BACKFILL_COLLECTION_IDS="company:openai" ASSET_MEDIA_BACKFILL_PROJECTION_IDS="proj_..."' \
	  '  make asset-media-backfill-apply ASSET_MEDIA_BACKFILL_REVIEWED=1 ASSET_MEDIA_BACKFILL_RUN_NOW=1' \
	  '  LIVE_CONFIRM=1 make test-crm-public-web-live-product-validation CRM_PUBLIC_WEB_LIVE_DRY_RUN=0 CRM_PUBLIC_WEB_LIVE_FORCE_REFRESH=1 CRM_PUBLIC_WEB_LIVE_RECORD_IDS="crm_..."'

bootstrap-test-env:
	@$(BOOTSTRAP_TEST_ENV_CMD)

dev-doctor:
	@$(DOCTOR_CMD)

agent-network-preflight:
	@$(AGENT_NETWORK_PREFLIGHT_CMD)

docker-start:
	@$(DOCKER_START_CMD)

docker-doctor:
	@$(DOCKER_DOCTOR_CMD)

local-pg-up:
	@$(LOCAL_PG_DOCKER_CMD) up

local-pg-down:
	@$(LOCAL_PG_DOCKER_CMD) down

local-pg-status:
	@$(LOCAL_PG_DOCKER_CMD) status

dev-backend:
	@$(BACKEND_CMD)

dev-frontend:
	@$(FRONTEND_CMD)

dev-launch-backend:
	@$(DETACHED_LAUNCH_CMD) --description "local dev backend" --log-path "$(BACKEND_LAUNCH_LOG)" --status-path "$(BACKEND_LAUNCH_STATUS)" -- "$(SHELL)" ./scripts/dev_backend.sh --runtime-dir "$(RUNTIME_DIR)" --host "$(HOST)" --port "$(API_PORT)" $(foreach port,$(ALLOWED_FRONTEND_PORTS),--frontend-port "$(port)") $(if $(ALLOW_ORIGIN),--allow-origin "$(ALLOW_ORIGIN)") $(EXTRA_BACKEND_ARGS)

dev-launch-frontend:
	@$(DETACHED_LAUNCH_CMD) --description "local dev frontend" --log-path "$(FRONTEND_LAUNCH_LOG)" --status-path "$(FRONTEND_LAUNCH_STATUS)" -- "$(SHELL)" ./scripts/dev_frontend.sh --host "$(HOST)" --port "$(FRONTEND_PORT)" --api-base-url "$(API_BASE_URL)" $(EXTRA_FRONTEND_ARGS)

dev-status:
	@$(STATUS_CMD)

dev-stop:
	@$(STOP_CMD)

dev-logs:
	@$(LOGS_CMD)

dev-clean:
	@$(CLEAN_CMD)

hosted-trial-backend:
	@$(HOSTED_TRIAL_BACKEND_CMD)

hosted-trial-backend-launch:
	@$(DETACHED_LAUNCH_CMD) --description "hosted trial backend" --log-path "$(HOSTED_BACKEND_LAUNCH_LOG)" --status-path "$(HOSTED_BACKEND_LAUNCH_STATUS)" -- env SOURCING_RUNTIME_ENVIRONMENT="$(HOSTED_RUNTIME_ENVIRONMENT)" SOURCING_EXTERNAL_PROVIDER_MODE="$(HOSTED_EXTERNAL_PROVIDER_MODE)" HOSTED_MAX_PARALLEL_REQUESTS="$(HOSTED_MAX_PARALLEL_REQUESTS)" HOSTED_LIGHT_REQUEST_RESERVED="$(HOSTED_LIGHT_REQUEST_RESERVED)" "$(SHELL)" ./scripts/run_hosted_trial_backend.sh --runtime-dir "$(HOSTED_RUNTIME_DIR)" --host "$(HOSTED_API_HOST)" --port "$(HOSTED_API_PORT)" --origin "$(HOSTED_FRONTEND_ORIGIN)"

frontend-build-hosted:
	@$(FRONTEND_HOSTED_BUILD_CMD)

frontend-build-hosted-same-origin:
	@$(FRONTEND_HOSTED_BUILD_SAME_ORIGIN_CMD)

lint:
	@$(PYTHON_QUALITY_CMD) lint

typecheck:
	@$(PYTHON_QUALITY_CMD) typecheck

verify-python:
	@$(PYTHON_QUALITY_CMD) all

test-env-backend:
	@$(TEST_BACKEND_CMD)

test-env-launch-backend:
	@$(DETACHED_LAUNCH_CMD) --description "isolated test backend" --log-path "$(TEST_BACKEND_LAUNCH_LOG)" --status-path "$(TEST_BACKEND_LAUNCH_STATUS)" -- env SOURCING_RUNTIME_ENVIRONMENT="$(TEST_RUNTIME_ENVIRONMENT)" SOURCING_EXTERNAL_PROVIDER_MODE="$(TEST_EXTERNAL_PROVIDER_MODE)" SOURCING_SECRETS_FILE="$(abspath $(TEST_SECRETS_FILE))" OBJECT_STORAGE_PREFIX="$(TEST_OBJECT_STORAGE_PREFIX)" $(if $(TEST_SCRIPTED_PROVIDER_SCENARIO),SOURCING_SCRIPTED_PROVIDER_SCENARIO="$(TEST_SCRIPTED_PROVIDER_SCENARIO)") "$(SHELL)" ./scripts/dev_backend.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --host "$(HOST)" --port "$(TEST_API_PORT)" $(foreach port,$(TEST_ALLOWED_FRONTEND_PORTS),--frontend-port "$(port)") $(if $(ALLOW_ORIGIN),--allow-origin "$(ALLOW_ORIGIN)") $(TEST_EXTRA_BACKEND_ARGS)

test-env-frontend:
	@$(TEST_FRONTEND_CMD)

test-env-status:
	@$(TEST_STATUS_CMD)

test-env-stop:
	@$(TEST_STOP_CMD)

test-env-logs:
	@$(TEST_LOGS_CMD)

test-env-clean:
	@$(TEST_CLEAN_CMD)

test-env-seed-assets:
	@if [ -z "$(strip $(TEST_ENV_SEED_COMPANIES))" ]; then \
	  printf 'TEST_ENV_SEED_COMPANIES is required. Example: make test-env-seed-assets TEST_ENV_SEED_COMPANIES="anthropic xai"\n' >&2; \
	  exit 1; \
	fi
	@$(TEST_SEED_CMD)

test-env-scripted-openai-agent:
	@"$(SHELL)" ./scripts/dev_scripted_openai_agent_delta.sh --runtime-dir "$(SCRIPTED_OPENAI_AGENT_RUNTIME_DIR)" --api-port "$(SCRIPTED_OPENAI_AGENT_API_PORT)" --frontend-port "$(SCRIPTED_OPENAI_AGENT_FRONTEND_PORT)" --scenario "$(SCRIPTED_OPENAI_AGENT_SCENARIO)" --secrets-file "$(SCRIPTED_OPENAI_AGENT_SECRETS_FILE)"

test-env-scripted-openai-agent-backend:
	@"$(SHELL)" ./scripts/dev_scripted_openai_agent_delta.sh --backend-only --runtime-dir "$(SCRIPTED_OPENAI_AGENT_RUNTIME_DIR)" --api-port "$(SCRIPTED_OPENAI_AGENT_API_PORT)" --frontend-port "$(SCRIPTED_OPENAI_AGENT_FRONTEND_PORT)" --scenario "$(SCRIPTED_OPENAI_AGENT_SCENARIO)" --secrets-file "$(SCRIPTED_OPENAI_AGENT_SECRETS_FILE)"

test-env-scripted-openai-agent-seed:
	@"$(SHELL)" ./scripts/dev_scripted_openai_agent_delta.sh --seed-only --runtime-dir "$(SCRIPTED_OPENAI_AGENT_RUNTIME_DIR)" --api-port "$(SCRIPTED_OPENAI_AGENT_API_PORT)" --frontend-port "$(SCRIPTED_OPENAI_AGENT_FRONTEND_PORT)" --scenario "$(SCRIPTED_OPENAI_AGENT_SCENARIO)" --secrets-file "$(SCRIPTED_OPENAI_AGENT_SECRETS_FILE)"

test-env-scripted-openai-agent-status:
	@"$(SHELL)" ./scripts/dev_status.sh --runtime-dir "$(SCRIPTED_OPENAI_AGENT_RUNTIME_DIR)" --api-port "$(SCRIPTED_OPENAI_AGENT_API_PORT)" --frontend-port "$(SCRIPTED_OPENAI_AGENT_FRONTEND_PORT)" --api-base-url "http://localhost:$(SCRIPTED_OPENAI_AGENT_API_PORT)"

test-env-scripted-openai-agent-stop:
	@"$(SHELL)" ./scripts/dev_stop.sh --runtime-dir "$(SCRIPTED_OPENAI_AGENT_RUNTIME_DIR)" --api-port "$(SCRIPTED_OPENAI_AGENT_API_PORT)" --frontend-port "$(SCRIPTED_OPENAI_AGENT_FRONTEND_PORT)"

test-env-scripted-openai-agent-logs:
	@"$(SHELL)" ./scripts/dev_logs.sh --runtime-dir "$(SCRIPTED_OPENAI_AGENT_RUNTIME_DIR)" --api-port "$(SCRIPTED_OPENAI_AGENT_API_PORT)" --frontend-port "$(SCRIPTED_OPENAI_AGENT_FRONTEND_PORT)" $(EXTRA_LOG_ARGS)

test-browser-e2e-fast:
	@$(TEST_BROWSER_E2E_FAST_CMD)

test-browser-e2e-slow:
	@$(TEST_BROWSER_E2E_SLOW_CMD)

test-browser-e2e-full:
	@$(TEST_BROWSER_E2E_FULL_CMD)

test-scripted-large-org:
	@$(TEST_SCRIPTED_LARGE_ORG_CMD)

ci-pg-contract:
	@$(CI_PG_CONTRACT_CMD)

ci-workflow-fake-provider:
	@$(CI_WORKFLOW_FAKE_PROVIDER_CMD)

ci-frontend-browser-gate:
	@$(CI_FRONTEND_BROWSER_GATE_CMD)

ci-workflow-browser-gate:
	@$(CI_WORKFLOW_BROWSER_GATE_CMD)

ci-pre-agent-contract:
	@$(CI_PRE_AGENT_CONTRACT_CMD)

independent-review-gate:
	@$(REVIEWER_CODEX_BOOTSTRAP_CMD)
	@$(INDEPENDENT_REVIEW_GATE_CMD)

ci-containerized-pre-release:
	@$(DOCKER_DOCTOR_CMD)
	@$(CI_PG_CONTRACT_CMD)
	@$(CI_WORKFLOW_FAKE_PROVIDER_CMD)
	@$(CI_FRONTEND_BROWSER_GATE_CMD)
	@$(CI_WORKFLOW_BROWSER_GATE_CMD)

local-container-smoke:
	@$(LOCAL_CONTAINER_SMOKE_CMD)

test-env-backend-live:
	@if [ "$(LIVE_CONFIRM)" != "1" ]; then \
	  printf 'Refusing to start live test backend without LIVE_CONFIRM=1\n' >&2; \
	  exit 1; \
	fi
	@$(LIVE_TEST_BACKEND_CMD)

test-live-large-org-manual:
	@if [ "$(LIVE_CONFIRM)" != "1" ]; then \
	  printf 'Refusing to run live large-org regression without LIVE_CONFIRM=1\n' >&2; \
	  exit 1; \
	fi
	@$(TEST_LIVE_LARGE_ORG_CMD)

test-crm-public-web-live-product-validation:
	@if [ "$(CRM_PUBLIC_WEB_LIVE_DRY_RUN)" = "0" ] && [ "$(CRM_PUBLIC_WEB_LIVE_FORCE_REFRESH)" != "1" ]; then \
	  printf 'Refusing to execute CRM Public Web live validation without CRM_PUBLIC_WEB_LIVE_FORCE_REFRESH=1\n' >&2; \
	  exit 1; \
	fi
	@if [ "$(CRM_PUBLIC_WEB_LIVE_DRY_RUN)" = "0" ] && [ "$(LIVE_CONFIRM)" != "1" ]; then \
	  printf 'Refusing to execute CRM Public Web live validation without LIVE_CONFIRM=1\n' >&2; \
	  exit 1; \
	fi
	@if [ "$(CRM_PUBLIC_WEB_LIVE_DRY_RUN)" = "0" ] && [ -z "$(strip $(CRM_PUBLIC_WEB_LIVE_RECORD_IDS))" ]; then \
	  printf 'Refusing to execute CRM Public Web live validation without CRM_PUBLIC_WEB_LIVE_RECORD_IDS\n' >&2; \
	  exit 1; \
	fi
	@if [ "$(CRM_PUBLIC_WEB_LIVE_DRY_RUN)" = "0" ] && [ "$(CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED)" != "1" ]; then \
	  printf 'Refusing to execute CRM Public Web live validation without CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED=1\n' >&2; \
	  exit 1; \
	fi
	@if [ "$(CRM_PUBLIC_WEB_LIVE_DRY_RUN)" = "0" ] && [ "$(CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED)" != "1" ]; then \
	  printf 'Refusing to execute CRM Public Web live validation without CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED=1\n' >&2; \
	  exit 1; \
	fi
	@if [ "$(CRM_PUBLIC_WEB_LIVE_DRY_RUN)" = "0" ] && [ -z "$(strip $(CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT))" ]; then \
	  printf 'Refusing to execute CRM Public Web live validation without CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT\n' >&2; \
	  exit 1; \
	fi
	@if [ "$(CRM_PUBLIC_WEB_LIVE_DRY_RUN)" = "0" ] && ! printf '%s' "$(CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_DIGEST_SHA256)" | grep -Eq '^[0-9a-f]{64}$$'; then \
	  printf 'Refusing to execute CRM Public Web live validation without a valid CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_DIGEST_SHA256\n' >&2; \
	  exit 1; \
	fi
	@if [ "$(CRM_PUBLIC_WEB_LIVE_DRY_RUN)" = "0" ] && [ ! -f "$(CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT)" ]; then \
	  printf 'Refusing to execute CRM Public Web live validation because CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT does not exist: %s\n' "$(CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT)" >&2; \
	  exit 1; \
	fi
	@if [ "$(CRM_PUBLIC_WEB_LIVE_DRY_RUN)" = "0" ]; then \
	  printf 'Running ci-pre-agent-contract before CRM Public Web live provider validation\n' >&2; \
	  $(CI_PRE_AGENT_CONTRACT_CMD) || exit $$?; \
	  CRM_PUBLIC_WEB_LIVE_PRE_AGENT_CONTRACT_PASSED=1 CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_PASSED=1 CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT="$(CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_ARTIFACT)" CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_DIGEST_SHA256="$(CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_DIGEST_SHA256)" $(CRM_PUBLIC_WEB_LIVE_VALIDATION_CMD); \
	else \
	  $(CRM_PUBLIC_WEB_LIVE_VALIDATION_CMD); \
	fi

asset-media-backfill-dry-run:
	@$(ASSET_MEDIA_BACKFILL_DRY_RUN_CMD)

asset-media-backfill-apply:
	@if [ "$(ASSET_MEDIA_BACKFILL_REVIEWED)" != "1" ]; then \
	  printf 'Refusing to apply asset/media backfill without ASSET_MEDIA_BACKFILL_REVIEWED=1 after dry-run review\n' >&2; \
	  exit 1; \
	fi
	@$(ASSET_MEDIA_BACKFILL_APPLY_CMD)

PRUNE_TEST_ENV_TTL_DAYS ?= 14

prune-test-env:
	@PYTHONPATH=src ./.venv-tests/bin/python scripts/apply_runtime_asset_retention_prune.py \
	  --policy ttl-local-rebuildable \
	  --min-age-days $(PRUNE_TEST_ENV_TTL_DAYS)

prune-test-env-apply:
	@if [ "$(PRUNE_TEST_ENV_REVIEWED)" != "1" ]; then \
	  printf 'Refusing to apply test_env TTL prune without PRUNE_TEST_ENV_REVIEWED=1 after dry-run review\n' >&2; \
	  exit 1; \
	fi
	@PYTHONPATH=src ./.venv-tests/bin/python scripts/apply_runtime_asset_retention_prune.py \
	  --policy ttl-local-rebuildable \
	  --min-age-days $(PRUNE_TEST_ENV_TTL_DAYS) \
	  --apply --reviewed --strict

dev:
	@mkdir -p "$$(dirname "$(BACKEND_LOG)")"
	@mkdir -p "$$(dirname "$(BACKEND_PID_FILE)")"
	@backend_log="$(BACKEND_LOG)"; \
	backend_pid_file="$(BACKEND_PID_FILE)"; \
	backend_cmd='$(BACKEND_CMD)'; \
	frontend_cmd='$(FRONTEND_CMD)'; \
	printf 'Starting backend in background with log: %s\n' "$$backend_log"; \
	eval "$$backend_cmd" >"$$backend_log" 2>&1 & \
	backend_pid="$$!"; \
	cleanup() { \
	  local exit_code="$$?"; \
	  trap - EXIT INT TERM; \
	  rm -f "$$backend_pid_file"; \
	  if kill -0 "$$backend_pid" 2>/dev/null; then \
	    printf '\nStopping backend pid %s\n' "$$backend_pid"; \
	    kill "$$backend_pid" 2>/dev/null || true; \
	    wait "$$backend_pid" 2>/dev/null || true; \
	  fi; \
	  exit "$$exit_code"; \
	}; \
	trap cleanup EXIT INT TERM; \
	sleep 2; \
	if ! kill -0 "$$backend_pid" 2>/dev/null; then \
	  printf 'Backend did not stay running. Check %s\n' "$$backend_log" >&2; \
	  exit 1; \
	fi; \
	printf '%s\n' "$$backend_pid" >"$$backend_pid_file"; \
	printf 'Starting frontend in foreground at %s\n' "$(API_BASE_URL)"; \
	eval "$$frontend_cmd"

test-env:
	@mkdir -p "$$(dirname "$(TEST_BACKEND_LOG)")"
	@mkdir -p "$$(dirname "$(TEST_BACKEND_PID_FILE)")"
	@test_backend_log="$(TEST_BACKEND_LOG)"; \
	test_backend_pid_file="$(TEST_BACKEND_PID_FILE)"; \
	test_backend_cmd='$(TEST_BACKEND_CMD)'; \
	test_frontend_cmd='$(TEST_FRONTEND_CMD)'; \
	printf 'Starting isolated test backend in background with log: %s\n' "$$test_backend_log"; \
	eval "$$test_backend_cmd" >"$$test_backend_log" 2>&1 & \
	test_backend_pid="$$!"; \
	cleanup() { \
	  local exit_code="$$?"; \
	  trap - EXIT INT TERM; \
	  rm -f "$$test_backend_pid_file"; \
	  if kill -0 "$$test_backend_pid" 2>/dev/null; then \
	    printf '\nStopping isolated test backend pid %s\n' "$$test_backend_pid"; \
	    kill "$$test_backend_pid" 2>/dev/null || true; \
	    wait "$$test_backend_pid" 2>/dev/null || true; \
	  fi; \
	  exit "$$exit_code"; \
	}; \
	trap cleanup EXIT INT TERM; \
	sleep 2; \
	if ! kill -0 "$$test_backend_pid" 2>/dev/null; then \
	  printf 'Isolated test backend did not stay running. Check %s\n' "$$test_backend_log" >&2; \
	  exit 1; \
	fi; \
	printf '%s\n' "$$test_backend_pid" >"$$test_backend_pid_file"; \
	printf 'Starting isolated test frontend in foreground at %s\n' "$(TEST_API_BASE_URL)"; \
	eval "$$test_frontend_cmd"
