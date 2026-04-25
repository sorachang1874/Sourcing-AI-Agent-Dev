SHELL := $(firstword $(wildcard /opt/homebrew/bin/bash /usr/local/bin/bash /bin/bash))
PATH := $(dir $(SHELL)):$(PATH)
.DEFAULT_GOAL := help

HOST ?= 0.0.0.0
PYTHON_BIN ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,$(if $(wildcard .venv-tests/bin/python),.venv-tests/bin/python,python3))
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

BACKEND_CMD = bash ./scripts/dev_backend.sh --runtime-dir "$(RUNTIME_DIR)" --host "$(HOST)" --port "$(API_PORT)" $(foreach port,$(ALLOWED_FRONTEND_PORTS),--frontend-port "$(port)") $(if $(ALLOW_ORIGIN),--allow-origin "$(ALLOW_ORIGIN)") $(EXTRA_BACKEND_ARGS)
FRONTEND_CMD = bash ./scripts/dev_frontend.sh --host "$(HOST)" --port "$(FRONTEND_PORT)" --api-base-url "$(API_BASE_URL)" $(EXTRA_FRONTEND_ARGS)
STATUS_CMD = bash ./scripts/dev_status.sh --runtime-dir "$(RUNTIME_DIR)" --api-port "$(API_PORT)" --frontend-port "$(FRONTEND_PORT)" --api-base-url "$(API_BASE_URL)"
STOP_CMD = bash ./scripts/dev_stop.sh --runtime-dir "$(RUNTIME_DIR)" --api-port "$(API_PORT)" --frontend-port "$(FRONTEND_PORT)"
LOGS_CMD = bash ./scripts/dev_logs.sh --runtime-dir "$(RUNTIME_DIR)" --api-port "$(API_PORT)" --frontend-port "$(FRONTEND_PORT)" $(EXTRA_LOG_ARGS)
CLEAN_CMD = bash ./scripts/dev_clean.sh --runtime-dir "$(RUNTIME_DIR)" --api-port "$(API_PORT)" --frontend-port "$(FRONTEND_PORT)" $(EXTRA_CLEAN_ARGS)
DOCTOR_CMD = env DEV_PYTHON_BIN="$(PYTHON_BIN)" bash ./scripts/dev_doctor.sh
BOOTSTRAP_TEST_ENV_CMD = env BOOTSTRAP_TEST_ENV_PYTHON_BIN="$(PYTHON_BIN)" bash ./scripts/bootstrap_test_env.sh
TEST_BACKEND_CMD = SOURCING_RUNTIME_ENVIRONMENT="$(TEST_RUNTIME_ENVIRONMENT)" SOURCING_EXTERNAL_PROVIDER_MODE="$(TEST_EXTERNAL_PROVIDER_MODE)" SOURCING_SECRETS_FILE="$(abspath $(TEST_SECRETS_FILE))" OBJECT_STORAGE_PREFIX="$(TEST_OBJECT_STORAGE_PREFIX)" $(if $(TEST_SCRIPTED_PROVIDER_SCENARIO),SOURCING_SCRIPTED_PROVIDER_SCENARIO="$(TEST_SCRIPTED_PROVIDER_SCENARIO)") bash ./scripts/dev_backend.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --host "$(HOST)" --port "$(TEST_API_PORT)" $(foreach port,$(TEST_ALLOWED_FRONTEND_PORTS),--frontend-port "$(port)") $(if $(ALLOW_ORIGIN),--allow-origin "$(ALLOW_ORIGIN)") $(TEST_EXTRA_BACKEND_ARGS)
TEST_FRONTEND_CMD = bash ./scripts/dev_frontend.sh --host "$(HOST)" --port "$(TEST_FRONTEND_PORT)" --api-base-url "$(TEST_API_BASE_URL)" $(TEST_EXTRA_FRONTEND_ARGS)
TEST_STATUS_CMD = bash ./scripts/dev_status.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --api-port "$(TEST_API_PORT)" --frontend-port "$(TEST_FRONTEND_PORT)" --api-base-url "$(TEST_API_BASE_URL)"
TEST_STOP_CMD = bash ./scripts/dev_stop.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --api-port "$(TEST_API_PORT)" --frontend-port "$(TEST_FRONTEND_PORT)"
TEST_LOGS_CMD = bash ./scripts/dev_logs.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --api-port "$(TEST_API_PORT)" --frontend-port "$(TEST_FRONTEND_PORT)" $(EXTRA_LOG_ARGS)
TEST_CLEAN_CMD = bash ./scripts/dev_clean.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --api-port "$(TEST_API_PORT)" --frontend-port "$(TEST_FRONTEND_PORT)" $(EXTRA_CLEAN_ARGS)
TEST_SEED_CMD = PYTHONPATH=src "$(PYTHON_BIN)" ./scripts/seed_test_env_assets.py --source-runtime-dir "$(RUNTIME_DIR)" --target-runtime-dir "$(TEST_RUNTIME_DIR)" --link-mode "$(TEST_ENV_SEED_LINK_MODE)" $(foreach company,$(TEST_ENV_SEED_COMPANIES),--company "$(company)")
TEST_BROWSER_E2E_FAST_CMD = PYTHONPATH=src SOURCING_RUN_FRONTEND_BROWSER_E2E=1 "$(PYTHON_BIN)" -m unittest tests.test_frontend_browser_e2e.FrontendBrowserFastE2ETest -v
TEST_BROWSER_E2E_SLOW_CMD = SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_SLOW_BROWSER_E2E=1 PYTHONPATH=src "$(PYTHON_BIN)" -m unittest tests.test_frontend_browser_e2e.FrontendBrowserSlowE2ETest -v
TEST_BROWSER_E2E_FULL_CMD = SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_SLOW_BROWSER_E2E=1 PYTHONPATH=src "$(PYTHON_BIN)" -m unittest tests.test_frontend_browser_e2e -v
TEST_SCRIPTED_LARGE_ORG_CMD = PYTHONPATH=src "$(PYTHON_BIN)" -m unittest \
	tests.test_hosted_workflow_smoke.HostedWorkflowSmokeTest.test_hosted_scripted_google_long_tail_timeout_completes_without_manual_takeover \
	tests.test_hosted_workflow_smoke.HostedWorkflowSmokeTest.test_hosted_scripted_large_org_full_roster_overflow_completes_without_live_provider \
	tests.test_hosted_workflow_smoke.HostedWorkflowSmokeTest.test_hosted_scripted_large_org_profile_tail_reconciles_after_background_prefetch \
	tests.test_hosted_workflow_smoke.HostedWorkflowSmokeTest.test_hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch -v
LIVE_TEST_BACKEND_CMD = SOURCING_RUNTIME_ENVIRONMENT="$(LIVE_TEST_RUNTIME_ENVIRONMENT)" SOURCING_EXTERNAL_PROVIDER_MODE="live" SOURCING_SECRETS_FILE="$(abspath $(LIVE_TEST_SECRETS_FILE))" OBJECT_STORAGE_PREFIX="$(LIVE_TEST_OBJECT_STORAGE_PREFIX)" bash ./scripts/dev_backend.sh --runtime-dir "$(LIVE_TEST_RUNTIME_DIR)" --host "$(HOST)" --port "$(LIVE_TEST_API_PORT)" $(foreach port,$(LIVE_TEST_ALLOWED_FRONTEND_PORTS),--frontend-port "$(port)") $(if $(ALLOW_ORIGIN),--allow-origin "$(ALLOW_ORIGIN)") $(LIVE_TEST_EXTRA_BACKEND_ARGS)
TEST_LIVE_LARGE_ORG_CMD = PYTHONPATH=src "$(PYTHON_BIN)" ./scripts/run_live_large_org_regression.py --base-url "$(LIVE_TEST_API_BASE_URL)" $(foreach case,$(LIVE_LARGE_ORG_CASES),--case "$(case)") --confirm-live-provider-cost --strict
HOSTED_TRIAL_BACKEND_CMD = SOURCING_RUNTIME_ENVIRONMENT="$(HOSTED_RUNTIME_ENVIRONMENT)" SOURCING_EXTERNAL_PROVIDER_MODE="$(HOSTED_EXTERNAL_PROVIDER_MODE)" HOSTED_MAX_PARALLEL_REQUESTS="$(HOSTED_MAX_PARALLEL_REQUESTS)" HOSTED_LIGHT_REQUEST_RESERVED="$(HOSTED_LIGHT_REQUEST_RESERVED)" bash ./scripts/run_hosted_trial_backend.sh --runtime-dir "$(HOSTED_RUNTIME_DIR)" --host "$(HOSTED_API_HOST)" --port "$(HOSTED_API_PORT)" --origin "$(HOSTED_FRONTEND_ORIGIN)"
FRONTEND_HOSTED_BUILD_CMD = cd frontend-demo && VITE_API_BASE_URL="$(HOSTED_API_BASE_URL)" npm run build:hosted
FRONTEND_HOSTED_BUILD_SAME_ORIGIN_CMD = cd frontend-demo && VITE_API_BASE_URL="same-origin" npm run build:hosted
PYTHON_QUALITY_CMD = bash ./scripts/run_python_quality.sh
DETACHED_LAUNCH_CMD = PYTHONPATH=src "$(PYTHON_BIN)" -m sourcing_agent.cli launch-detached

.PHONY: help bootstrap-test-env dev-doctor dev-backend dev-frontend dev-launch-backend dev-launch-frontend dev dev-status dev-stop dev-logs dev-clean hosted-trial-backend hosted-trial-backend-launch frontend-build-hosted frontend-build-hosted-same-origin lint typecheck verify-python test-env-backend test-env-launch-backend test-env-frontend test-env test-env-status test-env-stop test-env-logs test-env-clean test-env-seed-assets test-browser-e2e-fast test-browser-e2e-slow test-browser-e2e-full test-scripted-large-org test-env-backend-live test-live-large-org-manual

help:
	@printf '%s\n' \
	  'Common local dev targets:' \
	  '  make bootstrap-test-env  Recreate/validate .venv-tests from the repo runtime Python' \
	  '  make dev-doctor     Check bash/python/env invariants before local startup' \
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
	  '  make test-browser-e2e-fast  Run default fast browser E2E suite' \
	  '  make test-browser-e2e-slow  Run slow large-org browser E2E suite' \
	  '  make test-browser-e2e-full  Run fast + slow browser E2E suite' \
	  '  make test-scripted-large-org Run scripted large-org behavior regressions (overflow + profile tail + no-repeat tail)' \
	  '  make test-env-backend-live LIVE_CONFIRM=1  Start isolated live backend for manual large-org validation' \
	  '  make test-live-large-org-manual LIVE_CONFIRM=1  Run manual live large-org regression against the isolated live backend' \
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
	  '  make test-env-backend-live LIVE_CONFIRM=1 LIVE_TEST_API_PORT=8777' \
	  '  make test-live-large-org-manual LIVE_CONFIRM=1 LIVE_TEST_API_BASE_URL=http://localhost:8777'

bootstrap-test-env:
	@$(BOOTSTRAP_TEST_ENV_CMD)

dev-doctor:
	@$(DOCTOR_CMD)

dev-backend:
	@$(BACKEND_CMD)

dev-frontend:
	@$(FRONTEND_CMD)

dev-launch-backend:
	@$(DETACHED_LAUNCH_CMD) --description "local dev backend" --log-path "$(BACKEND_LAUNCH_LOG)" --status-path "$(BACKEND_LAUNCH_STATUS)" -- bash ./scripts/dev_backend.sh --runtime-dir "$(RUNTIME_DIR)" --host "$(HOST)" --port "$(API_PORT)" $(foreach port,$(ALLOWED_FRONTEND_PORTS),--frontend-port "$(port)") $(if $(ALLOW_ORIGIN),--allow-origin "$(ALLOW_ORIGIN)") $(EXTRA_BACKEND_ARGS)

dev-launch-frontend:
	@$(DETACHED_LAUNCH_CMD) --description "local dev frontend" --log-path "$(FRONTEND_LAUNCH_LOG)" --status-path "$(FRONTEND_LAUNCH_STATUS)" -- bash ./scripts/dev_frontend.sh --host "$(HOST)" --port "$(FRONTEND_PORT)" --api-base-url "$(API_BASE_URL)" $(EXTRA_FRONTEND_ARGS)

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
	@$(DETACHED_LAUNCH_CMD) --description "hosted trial backend" --log-path "$(HOSTED_BACKEND_LAUNCH_LOG)" --status-path "$(HOSTED_BACKEND_LAUNCH_STATUS)" -- env SOURCING_RUNTIME_ENVIRONMENT="$(HOSTED_RUNTIME_ENVIRONMENT)" SOURCING_EXTERNAL_PROVIDER_MODE="$(HOSTED_EXTERNAL_PROVIDER_MODE)" HOSTED_MAX_PARALLEL_REQUESTS="$(HOSTED_MAX_PARALLEL_REQUESTS)" HOSTED_LIGHT_REQUEST_RESERVED="$(HOSTED_LIGHT_REQUEST_RESERVED)" bash ./scripts/run_hosted_trial_backend.sh --runtime-dir "$(HOSTED_RUNTIME_DIR)" --host "$(HOSTED_API_HOST)" --port "$(HOSTED_API_PORT)" --origin "$(HOSTED_FRONTEND_ORIGIN)"

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
	@$(DETACHED_LAUNCH_CMD) --description "isolated test backend" --log-path "$(TEST_BACKEND_LAUNCH_LOG)" --status-path "$(TEST_BACKEND_LAUNCH_STATUS)" -- env SOURCING_RUNTIME_ENVIRONMENT="$(TEST_RUNTIME_ENVIRONMENT)" SOURCING_EXTERNAL_PROVIDER_MODE="$(TEST_EXTERNAL_PROVIDER_MODE)" SOURCING_SECRETS_FILE="$(abspath $(TEST_SECRETS_FILE))" OBJECT_STORAGE_PREFIX="$(TEST_OBJECT_STORAGE_PREFIX)" $(if $(TEST_SCRIPTED_PROVIDER_SCENARIO),SOURCING_SCRIPTED_PROVIDER_SCENARIO="$(TEST_SCRIPTED_PROVIDER_SCENARIO)") bash ./scripts/dev_backend.sh --runtime-dir "$(TEST_RUNTIME_DIR)" --host "$(HOST)" --port "$(TEST_API_PORT)" $(foreach port,$(TEST_ALLOWED_FRONTEND_PORTS),--frontend-port "$(port)") $(if $(ALLOW_ORIGIN),--allow-origin "$(ALLOW_ORIGIN)") $(TEST_EXTRA_BACKEND_ARGS)

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

test-browser-e2e-fast:
	@$(TEST_BROWSER_E2E_FAST_CMD)

test-browser-e2e-slow:
	@$(TEST_BROWSER_E2E_SLOW_CMD)

test-browser-e2e-full:
	@$(TEST_BROWSER_E2E_FULL_CMD)

test-scripted-large-org:
	@$(TEST_SCRIPTED_LARGE_ORG_CMD)

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
