import json
from pathlib import Path

from sourcing_agent.harvest_connectors import _build_scripted_sampled_harvest_body
from sourcing_agent.scripted_provider_scenario import (
    find_scripted_rule,
    load_scripted_provider_invocations,
    load_scripted_provider_scenario,
    record_scripted_provider_invocation,
    summarize_scripted_provider_scenario,
    validate_scripted_provider_scenario,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_scripted_provider_scenario_summary_detects_required_behavior_categories() -> None:
    scenario = {
        "search": {
            "rules": [
                {
                    "name": "search_staged_partial",
                    "poll_pending_rounds": 2,
                    "estimated_total_count": 3,
                    "results": [
                        {"title": "A", "url": "https://example.com/a"},
                    ],
                }
            ]
        },
        "harvest": {
            "rules": [
                {
                    "name": "harvest_retry_timeout",
                    "errors": [
                        {"phase": "execute", "round": 1, "kind": "retryable", "status": "429"},
                        {"phase": "execute", "round": 2, "kind": "timeout", "message": "Provider timed out"},
                    ],
                    "body": [],
                }
            ]
        },
    }

    summary = summarize_scripted_provider_scenario(scenario)
    validation = validate_scripted_provider_scenario(scenario)

    assert summary["complete"] is True
    assert summary["coverage"]["retryable_error"] == 1
    assert summary["coverage"]["timeout_error"] == 1
    assert summary["coverage"]["partial_result"] == 1
    assert summary["coverage"]["staged_ready_fetch"] == 1
    assert validation["status"] == "valid"


def test_scripted_provider_scenario_validation_reports_missing_categories() -> None:
    validation = validate_scripted_provider_scenario({"harvest": {"default": {"body": []}}})

    assert validation["status"] == "incomplete"
    assert set(validation["missing_categories"]) == {
        "retryable_error",
        "timeout_error",
        "partial_result",
        "staged_ready_fetch",
    }


def test_provider_behavior_matrix_fixture_covers_required_scenarios() -> None:
    payload = json.loads((_repo_root() / "configs" / "scripted" / "provider_behavior_matrix.json").read_text())
    validation = validate_scripted_provider_scenario(payload)

    assert validation["status"] == "valid"


def test_out_of_order_smoke_fixtures_use_explicit_remote_wait_windows() -> None:
    config_dir = _repo_root() / "configs" / "scripted"
    failures: list[str] = []
    for matrix_path in sorted(config_dir.glob("*smoke_matrix.json")):
        matrix = json.loads(matrix_path.read_text())
        for case in list(matrix.get("cases") or []):
            coverage_tags = {
                str(item or "").strip()
                for item in list(dict(case).get("coverage_tags") or [])
                if str(item or "").strip()
            }
            if "out_of_order_profile_completion" not in coverage_tags:
                continue
            scenario_path = _repo_root() / str(dict(case).get("scripted_scenario") or "")
            scenario = json.loads(scenario_path.read_text())
            remote_wait_seconds: list[float] = []
            for rule in list(dict(scenario.get("harvest") or {}).get("rules") or []):
                if not isinstance(rule, dict):
                    continue
                match = dict(rule.get("match") or {})
                if str(match.get("logical_name") or "").strip() != "harvest_profile_scraper_batch":
                    continue
                if str(rule.get("execute_sleep_position") or "").strip() != "remote_wait":
                    continue
                try:
                    seconds = float(rule.get("scripted_remote_wait_seconds") or 0.0)
                except (TypeError, ValueError):
                    seconds = 0.0
                if seconds > 0.0:
                    remote_wait_seconds.append(seconds)
            if len(remote_wait_seconds) < 2 or max(remote_wait_seconds) <= min(remote_wait_seconds):
                failures.append(f"{matrix_path.name}:{case.get('case')}")

    assert failures == []


def test_openai_agent_scoped_delta_streaming_fixture_documents_long_tail_contract() -> None:
    payload = json.loads(
        (_repo_root() / "configs" / "scripted" / "openai_agent_scoped_delta_streaming.json").read_text()
    )
    validation = validate_scripted_provider_scenario(payload)
    summary = validation["summary"]
    meta = payload["meta"]

    assert validation["status"] == "valid"
    assert meta["driver_query"] == "帮我找OpenAI做Agent方向的人"
    assert "Pulse" in meta["not_represented_by"]
    assert meta["expected_behavior"]["real_api_calls_required"] is False
    assert meta["expected_behavior"]["stage_1_public_web_seed_fallback_allowed"] is False
    assert meta["expected_behavior"]["probe_then_scale"] is True
    assert meta["expected_behavior"]["profile_actor_global_inflight_target"] == 4
    assert meta["expected_behavior"]["slow_strict_runtime_supported"] is True
    assert meta["synthetic_counts"]["current_profile_search_total"] >= 200
    assert meta["synthetic_counts"]["expected_unique_profile_urls_before_profile_scrape"] >= 200
    timing_matrix = meta["worker_timing_matrix"]
    assert {item["match_token"] for item in timing_matrix} >= {
        "openai-agent-current-0001",
        "openai-agent-current-0028",
        "openai-agent-current-0054",
        "openai-agent-current-0055",
        "openai-agent-former-0053",
    }
    assert max(int(item["sleep_seconds"]) for item in timing_matrix) >= 180
    assert min(int(item["sleep_seconds"]) for item in timing_matrix) < max(
        int(item["sleep_seconds"]) for item in timing_matrix
    )
    assert summary["provider_counts"]["harvest"] >= 4
    assert summary["coverage"]["retryable_error"] >= 1
    assert summary["coverage"]["timeout_error"] >= 1
    assert summary["coverage"]["partial_result"] >= 1
    assert summary["coverage"]["staged_ready_fetch"] >= 1


def test_combined_interactive_scripted_fixture_merges_included_rules(monkeypatch) -> None:
    scenario_path = _repo_root() / "configs" / "scripted" / "openai_agent_and_lovable_streaming.json"
    monkeypatch.setenv("SOURCING_SCRIPTED_PROVIDER_SCENARIO", str(scenario_path))

    payload = load_scripted_provider_scenario()
    validation = validate_scripted_provider_scenario(payload)
    rule_names = {
        str(rule.get("name") or "")
        for rule in list(dict(payload.get("harvest") or {}).get("rules") or [])
    }
    search_rule_names = {
        str(rule.get("name") or "")
        for rule in list(dict(payload.get("search") or {}).get("rules") or [])
    }

    assert validation["status"] == "valid"
    assert "openai_agent_current_profile_search_probe_and_scale" in rule_names
    assert "lovable_company_employees_roster" in rule_names
    assert "target_candidate_public_web_candidate_links" in search_rule_names
    assert validation["summary"]["provider_counts"]["harvest"] >= 8
    assert validation["summary"]["provider_counts"]["search"] >= 1


def test_scripted_provider_scenario_include_overlay_merges_same_named_rule(monkeypatch, tmp_path) -> None:
    base_path = tmp_path / "base.json"
    overlay_path = tmp_path / "overlay.json"
    base_path.write_text(
        json.dumps(
            {
                "harvest": {
                    "rules": [
                        {
                            "name": "profile_batch",
                            "match": {"logical_name": "harvest_profile_scraper_batch"},
                            "body": [{"profileUrl": "https://www.linkedin.com/in/example/"}],
                            "scripted_remote_wait_seconds": 1,
                        }
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    overlay_path.write_text(
        json.dumps(
            {
                "includes": ["base.json"],
                "harvest": {
                    "rules": [
                        {
                            "name": "profile_batch",
                            "scripted_remote_wait_seconds": 90,
                            "scripted_actor_run_duration_ms": 300000,
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SOURCING_SCRIPTED_PROVIDER_SCENARIO", str(overlay_path))

    payload = load_scripted_provider_scenario()
    rules = list(dict(payload.get("harvest") or {}).get("rules") or [])
    rule = find_scripted_rule(
        "harvest",
        context={"logical_name": "harvest_profile_scraper_batch", "payload": {}},
    )

    assert len(rules) == 1
    assert rule["_rule_name"] == "profile_batch"
    assert rule["body"] == [{"profileUrl": "https://www.linkedin.com/in/example/"}]
    assert rule["scripted_remote_wait_seconds"] == 90
    assert rule["scripted_actor_run_duration_ms"] == 300000


def test_scripted_provider_invocation_signature_distinguishes_zero_result_retry_attempts(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("SOURCING_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SOURCING_EXTERNAL_PROVIDER_MODE", "scripted")

    first = record_scripted_provider_invocation(
        provider_name="scripted_harvest",
        dispatch_kind="harvest.execute",
        logical_name="harvest_profile_search",
        query_text="Infra",
        task_key="openai-infra",
        payload={"query": "Infra"},
        metadata={"request_context": {"zero_result_retry_attempt": 0}},
    )
    second = record_scripted_provider_invocation(
        provider_name="scripted_harvest",
        dispatch_kind="harvest.execute",
        logical_name="harvest_profile_search",
        query_text="Infra",
        task_key="openai-infra",
        payload={"query": "Infra"},
        metadata={"request_context": {"zero_result_retry_attempt": 1}},
    )
    duplicate = record_scripted_provider_invocation(
        provider_name="scripted_harvest",
        dispatch_kind="harvest.execute",
        logical_name="harvest_profile_search",
        query_text="Infra",
        task_key="openai-infra",
        payload={"query": "Infra"},
        metadata={"request_context": {"zero_result_retry_attempt": 1}},
    )

    invocations = load_scripted_provider_invocations()

    assert first["dispatch_signature"] != second["dispatch_signature"]
    assert second["dispatch_signature"] == duplicate["dispatch_signature"]
    assert first["provider_mode"] == "scripted"
    assert len(invocations) == 3


def test_scripted_provider_rule_match_supports_negative_payload_terms(monkeypatch, tmp_path) -> None:
    scenario_path = tmp_path / "scenario.json"
    scenario_path.write_text(
        json.dumps(
            {
                "harvest": {
                    "rules": [
                        {
                            "name": "slow_mid_tail_without_fast_anchor",
                            "match": {
                                "logical_name": "harvest_profile_scraper_batch",
                                "payload_contains": ["openai-agent-current-0054"],
                                "payload_not_contains": ["openai-agent-current-0028"],
                            },
                            "body": [],
                        },
                        {
                            "name": "fast_anchor",
                            "match": {
                                "logical_name": "harvest_profile_scraper_batch",
                                "payload_contains": ["openai-agent-current-0028"],
                            },
                            "body": [],
                        },
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SOURCING_SCRIPTED_PROVIDER_SCENARIO", str(scenario_path))

    rule = find_scripted_rule(
        "harvest",
        context={
            "logical_name": "harvest_profile_scraper_batch",
            "payload": {
                "profile_urls": [
                    "https://www.linkedin.com/in/openai-agent-current-0028/",
                    "https://www.linkedin.com/in/openai-agent-current-0054/",
                ]
            },
        },
    )

    assert rule["_rule_name"] == "fast_anchor"


def test_scripted_provider_payload_terms_ignore_runtime_context(monkeypatch, tmp_path) -> None:
    scenario_path = tmp_path / "scenario.json"
    scenario_path.write_text(
        json.dumps(
            {
                "harvest": {
                    "rules": [
                        {
                            "name": "openai_agent_rule",
                            "match": {
                                "logical_name": "harvest_profile_search",
                                "payload_contains": ["pastCompanies", "openai", "Agent"],
                            },
                            "body": [{"id": "wrong"}],
                        },
                        {
                            "name": "lovable_former_rule",
                            "match": {
                                "logical_name": "harvest_profile_search",
                                "payload_contains": ["pastCompanies", "lovable"],
                            },
                            "body": [{"id": "right"}],
                        },
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SOURCING_SCRIPTED_PROVIDER_SCENARIO", str(scenario_path))

    rule = find_scripted_rule(
        "harvest",
        context={
            "logical_name": "harvest_profile_search",
            "payload": {
                "profileScraperMode": "Short",
                "pastCompanies": ["https://www.linkedin.com/company/lovable/"],
            },
            "request_context": {
                "runtime_dir": "/tmp/runtime/test_env/openai_agent_delta_streaming",
                "query_text": "",
            },
        },
    )

    assert rule["_rule_name"] == "lovable_former_rule"


def test_scripted_provider_rule_match_supports_exact_payload_fields(monkeypatch, tmp_path) -> None:
    scenario_path = tmp_path / "scenario.json"
    scenario_path.write_text(
        json.dumps(
            {
                "harvest": {
                    "rules": [
                        {
                            "name": "scaled_empty",
                            "match": {
                                "logical_name": "harvest_profile_search",
                                "payload_equals": {"startPage": 1, "takePages": 3},
                            },
                            "body": [],
                        },
                        {
                            "name": "probe",
                            "match": {
                                "logical_name": "harvest_profile_search",
                                "payload_equals": {"startPage": 1, "takePages": 1},
                            },
                            "body": [],
                        },
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SOURCING_SCRIPTED_PROVIDER_SCENARIO", str(scenario_path))

    rule = find_scripted_rule(
        "harvest",
        context={
            "logical_name": "harvest_profile_search",
            "payload": {"startPage": 1, "takePages": 3, "maxItems": 75},
        },
    )

    assert rule["_rule_name"] == "scaled_empty"


def test_google_gemini_provider_quality_fixture_declares_page_coverage_anomalies() -> None:
    payload = json.loads(
        (_repo_root() / "configs" / "scripted" / "google_gemini_provider_quality_streaming.json").read_text()
    )
    meta = payload["meta"]
    current_rules = [
        rule
        for rule in payload["harvest"]["rules"]
        if dict(rule.get("match") or {}).get("logical_name") == "harvest_profile_search"
        and "currentCompanies" in list(dict(rule.get("match") or {}).get("payload_contains") or [])
    ]

    assert meta["expected_provider_anomalies"]["empty_scale_count"] == 1
    assert meta["expected_provider_anomalies"]["probe_total_drift_count"] == 1
    assert meta["expected_provider_anomalies"]["empty_page_range_count"] == 1
    assert meta["expected_provider_anomalies"]["single_page_retry_count"] == 2
    assert any(dict(rule.get("match") or {}).get("payload_equals") == {"startPage": 1, "takePages": 3} for rule in current_rules)
    assert any(dict(rule.get("match") or {}).get("payload_equals") == {"startPage": 2, "takePages": 2} for rule in current_rules)


def test_openai_chatgpt_scoped_delta_streaming_fixture_reproduces_live_smoke_shape() -> None:
    payload = json.loads(
        (_repo_root() / "configs" / "scripted" / "openai_chatgpt_scoped_delta_streaming.json").read_text()
    )
    validation = validate_scripted_provider_scenario(payload)
    summary = validation["summary"]
    meta = payload["meta"]
    live_reference = meta["live_reference"]

    assert validation["status"] == "valid"
    assert meta["driver_query"] == "我想要OpenAI在ChatGPT组的人"
    assert live_reference["baseline_candidate_count"] == 890
    assert live_reference["final_candidate_count"] == 1061
    assert live_reference["deduped_search_count"] == 250
    assert live_reference["profile_batch_required_count"] == 165
    assert meta["expected_behavior"]["real_api_calls_required"] is False
    assert meta["expected_behavior"]["stage_1_public_web_seed_fallback_allowed"] is False
    assert meta["expected_behavior"]["repeated_materialize_signature_count_max"] == 0
    assert meta["expected_behavior"]["same_worker_reconcile_repeat_count_max"] == 0
    timing_matrix = meta["worker_timing_matrix"]
    assert {item["match_token"] for item in timing_matrix} >= {
        "openai-chatgpt-current-0001",
        "openai-chatgpt-current-0048",
        "openai-chatgpt-former-0004",
        "openai-chatgpt-former-0058",
    }
    assert max(int(item["sleep_seconds"]) for item in timing_matrix) >= 150
    assert summary["provider_counts"]["harvest"] >= 6
    assert summary["coverage"]["retryable_error"] >= 1
    assert summary["coverage"]["timeout_error"] >= 1
    assert summary["coverage"]["partial_result"] >= 1
    assert summary["coverage"]["staged_ready_fetch"] >= 1


def test_openai_infra_stage1_lane_skew_fixture_reproduces_zero_current_former_delta_shape() -> None:
    payload = json.loads(
        (_repo_root() / "configs" / "scripted" / "openai_infra_stage1_lane_skew_streaming.json").read_text()
    )
    validation = validate_scripted_provider_scenario(payload)
    summary = validation["summary"]
    meta = payload["meta"]

    assert validation["status"] == "valid"
    assert meta["driver_query"] == "帮我找OpenAI做Infra方向的人"
    assert meta["live_reference"]["job_id"] == "c5248ea4b3b4"
    assert meta["expected_behavior"]["current_lane_zero_result_is_valid"] is True
    assert meta["expected_behavior"]["coherent_stage1_projection_required"] is True
    assert meta["synthetic_counts"]["current_profile_search_returned"] == 0
    assert meta["synthetic_counts"]["former_profile_search_returned"] == 77
    assert meta["synthetic_counts"]["expected_profile_fetch_required_count"] == 77
    rule_counts = {item["name"]: item["result_count"] for item in summary["rules"]}
    assert rule_counts["openai_infra_current_profile_search_true_zero"] == 0
    assert rule_counts["openai_infra_former_profile_search_delta"] == 77
    assert summary["provider_counts"]["harvest"] >= 6
    assert summary["coverage"]["retryable_error"] >= 1
    assert summary["coverage"]["timeout_error"] >= 1
    assert summary["coverage"]["partial_result"] >= 1
    assert summary["coverage"]["staged_ready_fetch"] >= 1


def test_google_gemini_incomplete_shard_fixture_reproduces_no_full_reuse_shape() -> None:
    payload = json.loads(
        (_repo_root() / "configs" / "scripted" / "google_gemini_incomplete_shard_streaming.json").read_text()
    )
    validation = validate_scripted_provider_scenario(payload)
    summary = validation["summary"]
    meta = payload["meta"]

    assert validation["status"] == "valid"
    assert meta["driver_query"] == "帮我找Google做Gemini方向的人"
    assert meta["expected_behavior"]["authoritative_baseline_can_seed_delta"] is True
    assert meta["expected_behavior"]["full_local_reuse_allowed_without_exact_gemini_shard"] is False
    assert meta["expected_behavior"]["coherent_stage1_projection_required"] is True
    assert meta["synthetic_counts"]["current_profile_search_returned"] == 48
    assert meta["synthetic_counts"]["former_profile_search_returned"] == 18
    assert meta["synthetic_counts"]["expected_deduped_profile_url_count"] == 66
    rule_counts = {item["name"]: item["result_count"] for item in summary["rules"]}
    assert rule_counts["google_gemini_current_profile_search_delta"] == 48
    assert rule_counts["google_gemini_former_profile_search_delta"] == 18
    assert summary["provider_counts"]["harvest"] >= 6
    assert summary["coverage"]["retryable_error"] >= 1
    assert summary["coverage"]["timeout_error"] >= 1
    assert summary["coverage"]["partial_result"] >= 1
    assert summary["coverage"]["staged_ready_fetch"] >= 1


def test_google_gemini_large_baseline_small_former_fixture_uses_real_asset_samples() -> None:
    payload = json.loads(
        (
            _repo_root()
            / "configs"
            / "scripted"
            / "google_gemini_large_baseline_small_former_real_asset_streaming.json"
        ).read_text()
    )
    validation = validate_scripted_provider_scenario(payload)
    summary = validation["summary"]
    meta = payload["meta"]

    assert validation["status"] == "valid"
    assert meta["driver_query"] == "帮我找Google在Gemini组的人"
    expected = meta["expected_behavior"]
    assert expected["generated_profile_fallback_allowed"] is False
    assert expected["baseline_candidate_count_min"] == 8000
    assert expected["delta_profile_search_current_count"] == 0
    assert expected["delta_profile_search_former_count"] == 120
    rules = {
        str(rule.get("name") or ""): dict(rule)
        for rule in payload["harvest"]["rules"]
        if isinstance(rule, dict)
    }
    current_rule = rules["google_gemini_current_profile_search_true_zero_real_asset"]
    former_rule = rules["google_gemini_former_profile_search_small_real_asset"]
    scraper_rule = rules["google_gemini_profile_scraper_small_real_asset_batches"]
    assert current_rule["body"] == []
    assert former_rule["sample_candidate_documents_path"].endswith(
        "runtime/company_assets/google/20260428T011339/candidate_documents.json"
    )
    assert former_rule["sample_candidate_employment_scope"] == "former"
    assert former_rule["sample_candidate_limit"] == 120
    assert former_rule["sample_fallback_generated"] is False
    assert scraper_rule["sample_fallback_generated"] is False
    assert scraper_rule["sample_candidate_employment_scope"] == "former"
    former_search_body = _build_scripted_sampled_harvest_body(
        rule=former_rule,
        logical_name="harvest_profile_search",
        payload={
            "pastCompanies": ["https://www.linkedin.com/company/google/"],
            "searchQuery": "Gemini",
            "maxItems": 120,
            "startPage": 1,
            "takePages": 5,
        },
    )
    assert former_search_body is not None
    requested_profile_urls = [
        str(row.get("linkedinUrl") or row.get("profileUrl") or "").strip()
        for row in former_search_body
        if str(row.get("linkedinUrl") or row.get("profileUrl") or "").strip()
    ]
    assert len(requested_profile_urls) == 120
    scraper_body = _build_scripted_sampled_harvest_body(
        rule=scraper_rule,
        logical_name="harvest_profile_scraper_batch",
        payload={"urls": requested_profile_urls},
    )
    assert scraper_body is not None
    assert len(scraper_body) == 120
    rule_counts = {item["name"]: item["result_count"] for item in summary["rules"]}
    assert rule_counts["google_gemini_current_profile_search_true_zero_real_asset"] == 0
    assert summary["provider_counts"]["harvest"] >= 3
    assert summary["coverage"]["partial_result"] >= 1
    assert summary["coverage"]["staged_ready_fetch"] >= 1


def test_openai_whisper_zero_current_overlay_fixture_reproduces_baseline_delta_shape() -> None:
    payload = json.loads(
        (_repo_root() / "configs" / "scripted" / "openai_whisper_zero_current_overlay_streaming.json").read_text()
    )
    validation = validate_scripted_provider_scenario(payload)
    summary = validation["summary"]
    meta = payload["meta"]

    assert validation["status"] == "valid"
    assert meta["driver_query"] == "帮我找OpenAI在Whisper组的人"
    assert meta["expected_behavior"]["current_lane_zero_result_is_valid"] is True
    assert meta["expected_behavior"]["terminal_public_apis_must_serve_baseline_delta_overlay"] is True
    assert meta["expected_behavior"]["raw_delta_only_result_view_allowed"] is False
    assert meta["synthetic_counts"]["current_profile_search_returned"] == 0
    assert meta["synthetic_counts"]["former_profile_search_returned"] == 12
    assert meta["synthetic_counts"]["min_terminal_served_candidate_count"] > meta["synthetic_counts"][
        "baseline_candidate_count"
    ]
    rule_counts = {item["name"]: item["result_count"] for item in summary["rules"]}
    assert rule_counts["openai_whisper_current_profile_search_true_zero"] == 0
    assert rule_counts["openai_whisper_former_profile_search_delta"] == 12
    assert summary["provider_counts"]["harvest"] >= 4
    assert summary["coverage"]["retryable_error"] >= 1
    assert summary["coverage"]["timeout_error"] >= 1
    assert summary["coverage"]["partial_result"] >= 1
    assert summary["coverage"]["staged_ready_fetch"] >= 1


def test_openai_large_late_shard_fixture_reproduces_large_late_profile_scheduler_shape() -> None:
    payload = json.loads(
        (_repo_root() / "configs" / "scripted" / "openai_agent_large_late_shard_streaming.json").read_text()
    )
    validation = validate_scripted_provider_scenario(payload)
    summary = validation["summary"]
    meta = payload["meta"]

    assert validation["status"] == "valid"
    assert meta["driver_query"] == "帮我找OpenAI做Agent方向的人"
    assert meta["synthetic_counts"]["current_profile_search_total"] == 90
    assert meta["synthetic_counts"]["former_profile_search_total"] == 600
    assert meta["synthetic_counts"]["expected_unique_profile_urls_before_profile_scrape"] == 690
    rule_counts = {item["name"]: item["result_count"] for item in summary["rules"]}
    assert rule_counts["openai_agent_large_late_current_profile_search"] == 90
    assert rule_counts["openai_agent_large_late_former_profile_search"] == 600
    assert summary["provider_counts"]["harvest"] >= 4
    assert summary["coverage"]["partial_result"] >= 1
    assert summary["coverage"]["staged_ready_fetch"] >= 1
