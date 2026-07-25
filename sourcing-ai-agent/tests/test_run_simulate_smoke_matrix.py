import importlib.util
import json
import signal
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest


def _load_simulate_smoke_matrix_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "run_simulate_smoke_matrix.py"
    spec = importlib.util.spec_from_file_location("run_simulate_smoke_matrix", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_compact_stdout_record_falls_back_to_start_job_id_after_projection_cutover() -> None:
    smoke = _load_simulate_smoke_matrix_module()
    record = {
        "case": "projection_cutover",
        "start": {"job_id": "job-proj", "status": "queued"},
        "final": {"job_status": "completed"},
        "expectation_failures": [],
    }

    compact = smoke._compact_case_stdout_record(record)

    assert compact["job_id"] == "job-proj"
    assert compact["status"] == "completed"


def test_report_json_compaction_omits_large_background_reconcile_without_touching_original() -> None:
    smoke = _load_simulate_smoke_matrix_module()
    large_candidates = [{"candidate_id": f"cand_{index}", "raw": "x" * 1000} for index in range(150)]
    record = {
        "case": "large-report",
        "job_summary": {
            "background_reconcile": {
                "harvest_prefetch": {
                    "status": "completed",
                    "applied_worker_count": 3,
                    "candidates": large_candidates,
                }
            }
        },
        "final": {
            "job_status": "completed",
            "background_reconcile": {
                "harvest_prefetch": {
                    "status": "completed",
                    "applied_worker_count": 3,
                    "candidates": large_candidates,
                }
            },
        },
    }

    compacted = smoke._compact_smoke_report_records([record])[0]

    compacted_reconcile = compacted["job_summary"]["background_reconcile"]
    assert compacted_reconcile["status"] == "report_compacted"
    assert compacted_reconcile["original_bytes"] > smoke._MAX_BACKGROUND_RECONCILE_REPORT_BYTES
    assert compacted_reconcile["summary"]["harvest_prefetch"]["status"] == "completed"
    assert compacted_reconcile["summary"]["harvest_prefetch"]["applied_worker_count"] == 3
    assert compacted_reconcile["summary"]["harvest_prefetch"]["candidates"]["count"] == 150
    assert record["job_summary"]["background_reconcile"]["harvest_prefetch"]["candidates"] == large_candidates
    assert compacted["final"]["background_reconcile"]["status"] == "report_compacted"


def test_isolated_runtime_groups_follow_case_scripted_scenario_and_runtime_env() -> None:
    smoke = _load_simulate_smoke_matrix_module()
    cases = [
        {
            "case": "agent_a",
            "scripted_scenario": "configs/scripted/openai_agent_scoped_delta_streaming.json",
            "runtime_env": {"HARVEST_PROFILE_PREFETCH_BATCH_SIZE": "3"},
        },
        {
            "case": "agent_b",
            "scripted_scenario": "configs/scripted/openai_agent_scoped_delta_streaming.json",
            "runtime_env": {"HARVEST_PROFILE_PREFETCH_BATCH_SIZE": "3"},
        },
        {
            "case": "health_a",
            "scripted_scenario": "configs/scripted/openai_whisper_zero_current_overlay_streaming.json",
            "runtime_env": {"HARVEST_PROFILE_PREFETCH_BATCH_SIZE": "3"},
        },
        {
            "case": "agent_c",
            "scripted_scenario": "configs/scripted/openai_agent_scoped_delta_streaming.json",
            "runtime_env": {"HARVEST_PROFILE_PREFETCH_BATCH_SIZE": "5"},
        },
    ]

    groups = smoke._isolated_runtime_groups(cases)

    assert len(groups) == 3
    assert [item["case"] for item in groups[0]["cases"]] == ["agent_a", "agent_b"]
    assert [item["case"] for item in groups[1]["cases"]] == ["health_a"]
    assert [item["case"] for item in groups[2]["cases"]] == ["agent_c"]
    assert groups[0]["scripted_scenario"].endswith("openai_agent_scoped_delta_streaming.json")
    assert groups[1]["scripted_scenario"].endswith("openai_whisper_zero_current_overlay_streaming.json")
    assert groups[2]["runtime_env"] == {"HARVEST_PROFILE_PREFETCH_BATCH_SIZE": "5"}


def test_isolated_runtime_groups_force_fresh_cases_do_not_share_runtime_cache() -> None:
    smoke = _load_simulate_smoke_matrix_module()
    cases = [
        {
            "case": "baseline_delta",
            "scripted_scenario": "configs/scripted/openai_agent_scoped_delta_streaming.json",
            "runtime_env": {"RECOVERY_TICK_TOTAL_BUDGET_MS": "25000"},
            "payload": {"raw_user_request": "OpenAI Agent"},
        },
        {
            "case": "no_baseline_fresh",
            "scripted_scenario": "configs/scripted/openai_agent_scoped_delta_streaming.json",
            "runtime_env": {"RECOVERY_TICK_TOTAL_BUDGET_MS": "25000"},
            "review_decision": {"force_fresh_run": True},
            "payload": {
                "raw_user_request": "OpenAI Agent",
                "execution_preferences": {"force_fresh_run": True},
            },
        },
        {
            "case": "baseline_delta_peer",
            "scripted_scenario": "configs/scripted/openai_agent_scoped_delta_streaming.json",
            "runtime_env": {"RECOVERY_TICK_TOTAL_BUDGET_MS": "25000"},
            "payload": {"raw_user_request": "OpenAI Agent"},
        },
    ]

    groups = smoke._isolated_runtime_groups(cases)

    assert len(groups) == 2
    assert [item["case"] for item in groups[0]["cases"]] == [
        "baseline_delta",
        "baseline_delta_peer",
    ]
    assert [item["case"] for item in groups[1]["cases"]] == ["no_baseline_fresh"]


def test_run_smoke_cases_starts_isolated_runtime_with_group_scenario() -> None:
    smoke = _load_simulate_smoke_matrix_module()
    cases = [
        {
            "case": "agent_a",
            "scripted_scenario": "configs/scripted/openai_agent_scoped_delta_streaming.json",
            "runtime_env": {"HARVEST_PROFILE_PREFETCH_BATCH_SIZE": "3"},
        },
        {
            "case": "health_a",
            "scripted_scenario": "configs/scripted/openai_whisper_zero_current_overlay_streaming.json",
            "runtime_env": {"HARVEST_PROFILE_PREFETCH_BATCH_SIZE": "3"},
        },
    ]
    args = SimpleNamespace(
        runtime_dir="/tmp/simulate-smoke-runtime",
        runtime_env_file="",
        provider_mode="scripted",
        scripted_scenario="",
        seed_reference_runtime=False,
        fast_runtime=False,
        live_model_planning=False,
        reviewer="smoke-test",
        poll_seconds=0.05,
        max_poll_seconds=10.0,
        no_auto_continue_stage2=False,
        runtime_tuning_profile="",
        base_url="http://127.0.0.1:8765",
    )
    runtime_calls: list[dict[str, str]] = []
    matrix_calls: list[dict[str, object]] = []

    @contextmanager
    def _fake_runtime(**kwargs):
        runtime_calls.append(
            {
                "runtime_dir": str(kwargs["runtime_dir"]),
                "runtime_env_file": str(kwargs.get("runtime_env_file") or ""),
                "scripted_scenario": str(kwargs["scripted_scenario"]),
                "provider_mode": str(kwargs["provider_mode"]),
            }
        )
        yield SimpleNamespace(base_url=f"http://runtime/{len(runtime_calls)}")

    def _fake_run_hosted_smoke_matrix(*, client, cases, **kwargs):
        del kwargs
        matrix_calls.append(
            {
                "base_url": client.base_url,
                "case_names": [str(dict(item).get("case") or "") for item in list(cases or [])],
            }
        )
        return ([{"case": matrix_calls[-1]["case_names"][0], "final": {"smoke_ready": True}}], [])

    with (
        mock.patch.object(smoke, "isolated_hosted_test_runtime", _fake_runtime),
        mock.patch.object(smoke, "HostedWorkflowSmokeClient", side_effect=lambda base_url: SimpleNamespace(base_url=base_url)),
        mock.patch.object(smoke, "run_hosted_smoke_matrix", side_effect=_fake_run_hosted_smoke_matrix),
    ):
        summaries, failures = smoke._run_smoke_cases(args, cases=cases)

    assert failures == []
    assert len(summaries) == 2
    assert len(runtime_calls) == 2
    assert runtime_calls[0]["scripted_scenario"].endswith("openai_agent_scoped_delta_streaming.json")
    assert runtime_calls[1]["scripted_scenario"].endswith("openai_whisper_zero_current_overlay_streaming.json")
    assert runtime_calls[0]["runtime_dir"] != runtime_calls[1]["runtime_dir"]
    assert runtime_calls[0].get("runtime_env_file", "") == ""
    assert matrix_calls[0]["case_names"] == ["agent_a"]
    assert matrix_calls[1]["case_names"] == ["health_a"]


def test_run_smoke_cases_auto_seeds_reference_runtime_for_seeded_matrix_case() -> None:
    smoke = _load_simulate_smoke_matrix_module()
    cases = [
        {
            "case": "google_large_baseline",
            "scripted_scenario": "configs/scripted/google_vision_language_large_baseline_shard_streaming.json",
            "runtime_env": {"SOURCING_SEED_GOOGLE_LARGE_BASELINE_REAL_ASSET": "1"},
        }
    ]
    args = SimpleNamespace(
        runtime_dir="/tmp/simulate-smoke-runtime",
        runtime_env_file="",
        provider_mode="scripted",
        scripted_scenario="",
        seed_reference_runtime=False,
        fast_runtime=False,
        live_model_planning=False,
        reviewer="smoke-test",
        poll_seconds=0.05,
        max_poll_seconds=10.0,
        no_auto_continue_stage2=False,
        runtime_tuning_profile="",
        base_url="http://127.0.0.1:8765",
    )
    runtime_calls: list[dict[str, object]] = []

    @contextmanager
    def _fake_runtime(**kwargs):
        runtime_calls.append(
            {
                "seed_reference_runtime": bool(kwargs.get("seed_reference_runtime")),
                "extra_env": dict(kwargs.get("extra_env") or {}),
            }
        )
        yield SimpleNamespace(base_url="http://runtime/seeded")

    with (
        mock.patch.object(smoke, "isolated_hosted_test_runtime", _fake_runtime),
        mock.patch.object(smoke, "HostedWorkflowSmokeClient", side_effect=lambda base_url: SimpleNamespace(base_url=base_url)),
        mock.patch.object(smoke, "run_hosted_smoke_matrix", return_value=([{"case": "google_large_baseline"}], [])),
    ):
        smoke._run_smoke_cases(args, cases=cases)

    assert len(runtime_calls) == 1
    assert runtime_calls[0]["seed_reference_runtime"] is True
    assert dict(runtime_calls[0]["extra_env"])["SOURCING_SEED_GOOGLE_LARGE_BASELINE_REAL_ASSET"] == "1"


def test_isolated_runtime_groups_do_not_merge_seeded_and_unseeded_cases() -> None:
    smoke = _load_simulate_smoke_matrix_module()
    cases = [
        {
            "case": "baseline_delta",
            "scripted_scenario": "configs/scripted/openai_agent_scoped_delta_long_latency.json",
            "seed_reference_runtime": True,
            "runtime_env": {"RECOVERY_TICK_TOTAL_BUDGET_MS": "25000"},
        },
        {
            "case": "no_baseline",
            "scripted_scenario": "configs/scripted/openai_agent_scoped_delta_long_latency.json",
            "runtime_env": {"RECOVERY_TICK_TOTAL_BUDGET_MS": "25000"},
        },
    ]

    groups = smoke._isolated_runtime_groups(cases)

    assert len(groups) == 2
    assert [item["case"] for item in groups[0]["cases"]] == ["baseline_delta"]
    assert groups[0]["seed_reference_runtime"] is True
    assert [item["case"] for item in groups[1]["cases"]] == ["no_baseline"]
    assert not groups[1].get("seed_reference_runtime")


def test_shutdown_signal_handler_raises_cleanup_exception() -> None:
    smoke = _load_simulate_smoke_matrix_module()
    original_sigterm = signal.getsignal(signal.SIGTERM)
    original_sigint = signal.getsignal(signal.SIGINT)
    try:
        smoke._install_shutdown_signal_handlers()
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        with pytest.raises(smoke.SmokeMatrixShutdownRequested):
            handler(signal.SIGTERM, None)
    finally:
        signal.signal(signal.SIGTERM, original_sigterm)
        signal.signal(signal.SIGINT, original_sigint)


def test_run_smoke_cases_cli_scenario_override_wins_over_case_scenarios() -> None:
    smoke = _load_simulate_smoke_matrix_module()
    cases = [
        {
            "case": "agent_a",
            "scripted_scenario": "configs/scripted/openai_agent_scoped_delta_streaming.json",
            "runtime_env": {"HARVEST_PROFILE_PREFETCH_BATCH_SIZE": "3"},
        },
        {
            "case": "health_a",
            "scripted_scenario": "configs/scripted/openai_whisper_zero_current_overlay_streaming.json",
            "runtime_env": {"HARVEST_PROFILE_PREFETCH_BATCH_SIZE": "3"},
        },
    ]
    override_path = "configs/scripted/service_gate_coverage_manifest.json"
    args = SimpleNamespace(
        runtime_dir="/tmp/simulate-smoke-runtime",
        runtime_env_file="",
        provider_mode="scripted",
        scripted_scenario=override_path,
        seed_reference_runtime=False,
        fast_runtime=False,
        live_model_planning=False,
        reviewer="smoke-test",
        poll_seconds=0.05,
        max_poll_seconds=10.0,
        no_auto_continue_stage2=False,
        runtime_tuning_profile="",
        base_url="http://127.0.0.1:8765",
    )
    runtime_calls: list[dict[str, str]] = []

    @contextmanager
    def _fake_runtime(**kwargs):
        runtime_calls.append(
            {
                "runtime_dir": str(kwargs["runtime_dir"]),
                "scripted_scenario": str(kwargs["scripted_scenario"]),
            }
        )
        yield SimpleNamespace(base_url="http://runtime/override")

    with (
        mock.patch.object(smoke, "isolated_hosted_test_runtime", _fake_runtime),
        mock.patch.object(smoke, "HostedWorkflowSmokeClient", side_effect=lambda base_url: SimpleNamespace(base_url=base_url)),
        mock.patch.object(smoke, "run_hosted_smoke_matrix", return_value=([{"case": "agent_a"}], [])),
    ):
        smoke._run_smoke_cases(args, cases=cases)

    assert len(runtime_calls) == 1
    assert runtime_calls[0]["scripted_scenario"].endswith("service_gate_coverage_manifest.json")


def test_runtime_contamination_preflight_rejects_active_root_daemon(tmp_path: Path) -> None:
    smoke = _load_simulate_smoke_matrix_module()
    args = SimpleNamespace(runtime_dir=str(tmp_path / "runtime" / "test_env" / "case"))

    with (
        mock.patch.object(
            smoke,
            "audit_active_runtime_daemons",
            return_value={"status": "blocked", "findings": [{"pid": 123, "name": "dev-worker-daemon"}]},
        ),
        pytest.raises(SystemExit, match="active runtime daemons"),
    ):
        smoke._runtime_contamination_preflight(args)


def test_runtime_contamination_preflight_rejects_existing_runtime_contamination(tmp_path: Path) -> None:
    smoke = _load_simulate_smoke_matrix_module()
    args = SimpleNamespace(runtime_dir=str(tmp_path / "runtime" / "test_env" / "case"))

    with (
        mock.patch.object(smoke, "audit_active_runtime_daemons", return_value={"status": "ok", "findings": []}),
        mock.patch.object(
            smoke,
            "_contamination_runtime_scope",
            return_value=(tmp_path / "runtime" / "test_env" / "case", "postgresql://example", "sourcing_scripted_case"),
        ),
        mock.patch.object(
            smoke,
            "build_runtime_contamination_report",
            return_value={"status": "contaminated", "finding_count": 2, "runtime_root": str(tmp_path / "runtime")},
        ),
        pytest.raises(SystemExit, match="runtime contamination preflight failed"),
    ):
        smoke._runtime_contamination_preflight(args)


def test_runtime_contamination_postflight_rejects_orphan_root_daemon(tmp_path: Path) -> None:
    smoke = _load_simulate_smoke_matrix_module()
    args = SimpleNamespace(runtime_dir=str(tmp_path / "runtime" / "test_env" / "case"))

    with (
        mock.patch.object(
            smoke,
            "audit_active_runtime_daemons",
            return_value={"status": "blocked", "findings": [{"pid": 456, "name": "worker-recovery-daemon"}]},
        ),
        pytest.raises(SystemExit, match="active runtime daemons"),
    ):
        smoke._runtime_contamination_postflight(args)


def test_validate_smoke_provider_invocation_modes_rejects_live_invocation_for_scripted() -> None:
    smoke = _load_simulate_smoke_matrix_module()

    failures = smoke._validate_smoke_provider_invocation_modes(
        [
            {
                "case": "openai_agent",
                "provider_invocations": [
                    {"provider_name": "harvest", "provider_mode": "scripted"},
                    {"provider_name": "harvest", "provider_mode": "live"},
                    {"provider_name": "harvest"},
                ],
            }
        ],
        provider_mode="scripted",
    )

    assert failures == [
        "openai_agent: provider_invocations[1].provider_mode=live",
        "openai_agent: provider_invocations[2].provider_mode missing",
    ]


def test_main_writes_report_before_postflight_and_compacts_stdout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    smoke = _load_simulate_smoke_matrix_module()
    report_path = tmp_path / "report.json"
    summary_path = tmp_path / "summary.json"
    huge_payload = "x" * 100_000

    monkeypatch.setattr(
        "sys.argv",
        [
            "run_simulate_smoke_matrix.py",
            "--runtime-dir",
            str(tmp_path / "runtime" / "test_env" / "case"),
            "--provider-mode",
            "scripted",
            "--report-json",
            str(report_path),
            "--summary-json",
            str(summary_path),
        ],
    )
    monkeypatch.setattr(smoke, "load_smoke_cases", lambda matrix_file, selected_cases: [{"case": "large"}])
    monkeypatch.setattr(smoke, "_runtime_contamination_preflight", lambda args, cases=None: None)
    monkeypatch.setattr(
        smoke,
        "_run_smoke_cases",
        lambda args, cases: (
            [
                {
                    "case": "large",
                    "job_id": "job_1",
                    "status": "completed",
                    "huge_diagnostics": huge_payload,
                    "timings_ms": {"total": 123.0, "wait_for_completion": 100.0},
                }
            ],
            [],
        ),
    )

    def _postflight_requires_report(args, cases=None):
        del args, cases
        assert report_path.exists()
        assert summary_path.exists()

    monkeypatch.setattr(smoke, "_runtime_contamination_postflight", _postflight_requires_report)
    monkeypatch.setattr(
        smoke,
        "summarize_smoke_timings",
        lambda summaries: {"timings_ms": {"total": {"p95": 123.0}, "wait_for_completion": {"p95": 100.0}}},
    )

    assert smoke.main() == 0

    captured = capsys.readouterr()
    stdout_payload = json.loads(captured.out)
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert stdout_payload["case_count"] == 1
    assert stdout_payload["cases"] == [
        {"case": "large", "job_id": "job_1", "status": "completed", "expectation_failures": []}
    ]
    assert huge_payload not in captured.out
    assert report_payload[0]["huge_diagnostics"] == huge_payload
