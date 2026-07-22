"""Single-source contract-lane manifest (harness R0 batch, 2026-07-22).

The curated PG-backed contract lane was defined TWICE — workspace-root
.github/workflows/backend-ci.yml (GH lane) and Makefile CI_PRE_AGENT_CONTRACT_CMD
(local lane) — with overlapping but different lists and no drift guard. Any
per-file split of a lane member risked silent lane-membership drift in one
consumer: exactly the false-green class the 2026-07-09 skip-to-fail hardening
was built against. This module is now the AUTHORITY; tests/test_lane_manifest.py
asserts both consumers match it exactly. Change lanes HERE first, then mirror
into the consumer(s) in the same commit.

Every entry carries its WHY so lane membership stops being undocumented policy
(the provenance-gate REGRESSION_INDEX references these rationales).
"""

# --- GH lane (backend-ci.yml "Contract Test Lane"), full-file members in order.
GH_LANE_FULL: list[tuple[str, str]] = [
    ("tests/test_markdown_status.py", "docs status-banner gate (harness reorg R5) — offline, fails fast"),
    ("tests/test_docs_routing.py", "docs link graph + snapshot budgets + provenance/lane/mypy gates ride this offline block"),
    ("tests/test_lane_manifest.py", "THIS manifest's drift guard: both lane consumers must match the manifest"),
    ("tests/test_provenance.py", "test-provenance gate: new/changed test modules need anchors; grandfather ratchet"),
    ("tests/test_mypy_ratchet.py", "mypy ratchet comparator semantics (offline half; live run is its own CI step)"),
    ("tests/test_artifact_cache.py", "2026-07-21 hot-cache same-file data-destruction incident regression (ran in no lane before)"),
    ("tests/test_latest_snapshot_pointer.py", "2026-07 pointer cross-root drift incident regression (ran in no lane before)"),
    ("tests/test_live_apify_dataset_salvage.py", "GDM fn8/fn24 salvage adapter contract (function attribution + passthrough preservation)"),
    ("tests/test_live_schema_write_fence.py", "2026-07-22 simulate-rows-in-live-schema incident fence"),
    ("tests/test_strategy_contract_preflight.py", "strategy_type cross-surface preflight + Step 2/3 flip-target pins (Contract Field Ownership rule 2)"),
    ("tests/test_pipeline_freeze.py", "R-009 god-file freeze ratchet: test_pipeline only shrinks until salvage-delete completes"),
    ("tests/test_api_auth.py", "C2.1 auth foundation contract"),
    ("tests/test_api_server_identity.py", "server identity contract"),
    ("tests/test_user_private_reads.py", "user-scoped read isolation contract"),
    ("tests/test_provider_task_specs.py", "M2 typed provider-task registry contract"),
    ("tests/test_provider_budget_characterization.py", "provider budget/circuit characterization"),
    ("tests/test_pre_agent_contract_review.py", "pre-agent contract review battery"),
    ("tests/test_operation_runtime.py", "operation runtime lane-full battery (largest lane member)"),
    ("tests/test_crm_public_web_runtime_boundary.py", "CRM public-web boundary + pinned-drain literal guards"),
    ("tests/test_legacy_public_web_retirement_audit.py", "legacy endpoint retirement audit"),
    ("tests/test_storage_surface_guardrails.py", "storage surface guardrails (retired-method scans)"),
    ("tests/test_pg_onconflict_guard.py", "PG ON CONFLICT unique-index parity guard (2026-06-12 defect class)"),
    ("tests/test_migration_runner.py", "schema migration runner contract"),
    ("tests/test_pg_only_dedup_reads.py", "PG-only dedup read contract"),
]

# --- Partial (-k) members shared by BOTH lanes, in invocation order.
LANE_PARTIAL: list[tuple[tuple[str, ...], str, str]] = [
    (
        ("tests/test_enrichment.py",),
        "isolates_retry_wait or retry_wait_as_isolated_batch",
        "retry-wait isolation is the enrichment dispatch invariant that once mass-duplicated paid work",
    ),
    (
        (
            "tests/test_dataforseo_client.py",
            "tests/test_search_provider.py",
            "tests/test_public_web_search.py",
            "tests/test_seed_discovery.py",
            "tests/test_exploratory_enrichment.py",
        ),
        "partial_task_errors or dataforseo_provider_submit_batch_retries_only_failed_query_item or "
        "dataforseo_provider_submit_batch_fails_closed_without_provider_identity or "
        "batch_provider_requires_stable_query_identity_key or "
        "provider_chain_rejects_missing_query_identity_before_fallback or "
        "dataforseo_provider_fetch_ready_batch_preserves_success_when_one_task_get_fails or "
        "batch_queue_submit_failure_is_query_level_not_whole_batch or "
        "public_web_batch_query_identity_is_candidate_query_scoped_not_order_scoped or "
        "search_seed_worker_key_uses_query_identity_not_order_ordinal or "
        "exploration_query_task_key_uses_query_identity_not_order_ordinal or "
        "model_native_search_provider_order_fails_closed_without_contract",
        "query-IDENTITY (not order-ordinal) provider contract: the R-010 retirement's replacement guards",
    ),
    (
        ("tests/test_projection_crm_api_contracts.py",),
        "asset_backfill_http_routes_are_explicit_migration_paths",
        "asset-backfill HTTP routes must stay explicit migration paths, never normal-path",
    ),
    (
        ("tests/test_scripted_smoke_signoff.py",),
        "durable_command_owner_contract",
        "durable command owner contract on the smoke-signoff surface",
    ),
]

# GH lane runs one extra partial member the local lane does not:
GH_ONLY_PARTIAL: list[tuple[tuple[str, ...], str, str]] = [
    (
        ("tests/test_remote_provider_events.py",),
        "terminal_marker_handoff_is_strand_safe",
        "remote-provider terminal marker handoff strand-safety (paid-dispatch dedup class)",
    ),
]

# --- Local lane (Makefile CI_PRE_AGENT_CONTRACT_CMD), full-file members.
# Delta vs GH is deliberate: local lane skips the offline docs/auth/identity/
# migration gates (they run in GH and in `make verify-python` flows) and adds
# the review-gate runner + chains the CRM live product validation script.
MAKE_LANE_FULL: list[tuple[str, str]] = [
    ("tests/test_pre_agent_contract_review.py", "shared with GH lane"),
    ("tests/test_independent_review_gate_runner.py", "review-gate runner is local-ops tooling; GH runners lack codex homes"),
    ("tests/test_operation_runtime.py", "shared with GH lane"),
    ("tests/test_crm_public_web_runtime_boundary.py", "shared with GH lane"),
    ("tests/test_legacy_public_web_retirement_audit.py", "shared with GH lane"),
    ("tests/test_storage_surface_guardrails.py", "shared with GH lane"),
    ("tests/test_pg_onconflict_guard.py", "shared with GH lane"),
]
