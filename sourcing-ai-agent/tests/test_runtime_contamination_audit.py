from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

from sourcing_agent.runtime_contamination_audit import (
    _runtime_search_terms,
    apply_runtime_contamination_quarantine_plan,
    audit_active_runtime_daemons,
    build_runtime_contamination_quarantine_plan,
    build_runtime_contamination_report,
    outer_runtime_root_for_path,
    scan_live_provider_cache_for_synthetic_fixtures,
)


def test_outer_runtime_root_for_nested_test_runtime(tmp_path: Path) -> None:
    target = tmp_path / "workspace" / "runtime" / "test_env" / "openai_fix2"

    assert outer_runtime_root_for_path(target) == (tmp_path / "workspace" / "runtime").resolve()


def test_live_provider_cache_audit_detects_scripted_fixture_url(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    cache_dir = runtime_root / "provider_cache" / "local_dev" / "live" / "harvest_profile_scraper_batch"
    cache_dir.mkdir(parents=True)
    (cache_dir / "a30f73b69b3049f2.request.json").write_text(
        json.dumps(
            {
                "urls": [
                    "https://www.linkedin.com/in/openai-agent-current-0189/",
                ],
            }
        ),
        encoding="utf-8",
    )

    report = scan_live_provider_cache_for_synthetic_fixtures(runtime_root=runtime_root)

    assert report["status"] == "ok"
    assert report["finding_count"] == 1
    assert report["findings"][0]["reason"] == "synthetic_fixture_in_live_provider_cache"
    assert report["findings"][0]["markers"] == ["linkedin.com/in/openai-agent-current-0189"]


def test_runtime_contamination_report_can_run_without_postgres(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    runtime_root = workspace / "runtime"
    cache_dir = runtime_root / "provider_cache" / "local_dev" / "live" / "harvest_profile_scraper_batch"
    cache_dir.mkdir(parents=True)
    (cache_dir / "clean.request.json").write_text(
        json.dumps({"urls": ["https://www.linkedin.com/in/real-person/"]}),
        encoding="utf-8",
    )

    report = build_runtime_contamination_report(
        workspace_root=workspace,
        target_runtime_dir=runtime_root / "test_env" / "case",
        include_postgres=False,
    )

    assert report["status"] == "clean"
    assert report["finding_count"] == 0
    assert report["provider_cache"]["scanned_file_count"] == 1
    assert report["postgres"]["reason"] == "postgres_disabled"


def test_runtime_contamination_report_can_scope_provider_cache_to_target_runtime(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    root_cache_dir = workspace / "runtime" / "provider_cache" / "local_dev" / "live" / "harvest_profile_scraper_batch"
    root_cache_dir.mkdir(parents=True)
    (root_cache_dir / "bad.request.json").write_text(
        json.dumps({"url": "https://www.linkedin.com/in/openai-agent-current-0189/"}),
        encoding="utf-8",
    )
    target_runtime = workspace / "runtime" / "test_env" / "case"
    target_cache_dir = target_runtime / "provider_cache" / "local_dev" / "live" / "harvest_profile_scraper_batch"
    target_cache_dir.mkdir(parents=True)
    (target_cache_dir / "clean.request.json").write_text(
        json.dumps({"url": "https://www.linkedin.com/in/real-person/"}),
        encoding="utf-8",
    )

    report = build_runtime_contamination_report(
        workspace_root=workspace,
        target_runtime_dir=target_runtime,
        provider_cache_runtime_root=target_runtime,
        include_postgres=False,
    )

    assert report["status"] == "clean"
    assert report["runtime_root"] == str((workspace / "runtime").resolve())
    assert report["provider_cache_runtime_root"] == str(target_runtime)
    assert report["provider_cache"]["scanned_file_count"] == 1
    assert report["provider_cache"]["finding_count"] == 0


def test_runtime_search_terms_do_not_include_bare_runtime_for_output_runtime(tmp_path: Path) -> None:
    terms = _runtime_search_terms(tmp_path / "output" / "pressure_case" / "runtime")

    assert "runtime" not in terms


def test_runtime_search_terms_include_nested_test_runtime_namespace(tmp_path: Path) -> None:
    target = tmp_path / "workspace" / "runtime" / "test_env" / "openai_case"
    terms = _runtime_search_terms(target)

    assert "runtime/test_env/openai_case" in terms
    assert "test_env/openai_case" in terms


def test_active_runtime_daemon_audit_detects_live_root_pid_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    pid_dir = runtime_root / "service_logs"
    pid_dir.mkdir(parents=True)
    (pid_dir / "dev-worker-daemon.pid").write_text(str(os.getpid()), encoding="utf-8")
    monkeypatch.setattr(
        "sourcing_agent.runtime_contamination_audit._process_command_for_pid",
        lambda pid: "/opt/homebrew/bin/python -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 1",
    )

    report = audit_active_runtime_daemons(runtime_root=runtime_root)

    assert report["status"] == "blocked"
    assert report["finding_count"] == 1
    assert report["findings"][0]["name"] == "dev-worker-daemon"


def test_active_runtime_daemon_audit_ignores_stale_pid_reused_by_non_daemon(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    status_dir = runtime_root / "services" / "worker-recovery-daemon"
    status_dir.mkdir(parents=True)
    (status_dir / "status.json").write_text(
        json.dumps(
            {
                "service_name": "worker-recovery-daemon",
                "status": "running",
                "pid": os.getpid(),
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "sourcing_agent.runtime_contamination_audit._process_command_for_pid",
        lambda pid: "/System/Library/PrivateFrameworks/SocialLayer.framework/sociallayerd",
    )
    monkeypatch.setattr(
        "sourcing_agent.runtime_contamination_audit._active_runtime_daemon_process_findings",
        lambda: [],
    )

    report = audit_active_runtime_daemons(runtime_root=runtime_root)

    assert report["status"] == "ok"
    assert report["finding_count"] == 0


def test_active_runtime_daemon_audit_detects_live_root_process_scan(tmp_path: Path, monkeypatch) -> None:
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir(parents=True)

    def _fake_run(*args, **kwargs):
        del args, kwargs
        return SimpleNamespace(
            stdout="\n".join(
                [
                    "12345 /opt/homebrew/bin/python -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 1",
                    "12346 /opt/homebrew/bin/python -m sourcing_agent.cli serve --host 0.0.0.0 --port 8785",
                ]
            )
        )

    monkeypatch.setattr("sourcing_agent.runtime_contamination_audit.subprocess.run", _fake_run)

    report = audit_active_runtime_daemons(runtime_root=runtime_root)

    assert report["status"] == "blocked"
    assert report["finding_count"] == 2
    assert {item["kind"] for item in report["findings"]} == {"process_scan"}


def test_quarantine_plan_is_dry_run_and_lists_exact_cache_move(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    runtime_root = workspace / "runtime"
    cache_dir = runtime_root / "provider_cache" / "local_dev" / "live" / "harvest_profile_scraper_batch"
    cache_dir.mkdir(parents=True)
    request_path = cache_dir / "bad.request.json"
    request_path.write_text(
        json.dumps({"url": "https://www.linkedin.com/in/openai-agent-current-0189/"}),
        encoding="utf-8",
    )

    plan = build_runtime_contamination_quarantine_plan(
        workspace_root=workspace,
        target_runtime_dir=runtime_root / "test_env" / "case",
        sample_limit=10,
    )

    assert plan["status"] == "dry_run_only"
    assert plan["mutates_state"] is False
    assert plan["provider_cache"]["contaminated_request_manifest_count"] == 1
    move = plan["provider_cache"]["file_moves"][0]
    assert move["source"] == str(request_path)
    assert "mv --" in move["shell"]


def test_quarantine_plan_includes_same_stem_cache_payload(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    runtime_root = workspace / "runtime"
    cache_dir = runtime_root / "provider_cache" / "local_dev" / "live" / "harvest_profile_scraper_batch"
    cache_dir.mkdir(parents=True)
    request_path = cache_dir / "bad.request.json"
    payload_path = cache_dir / "bad.json"
    request_path.write_text(
        json.dumps({"url": "https://www.linkedin.com/in/openai-agent-current-0189/"}),
        encoding="utf-8",
    )
    payload_path.write_text(json.dumps({"status": 404}), encoding="utf-8")

    plan = build_runtime_contamination_quarantine_plan(
        workspace_root=workspace,
        target_runtime_dir=runtime_root / "test_env" / "case",
        sample_limit=10,
    )

    move_sources = {item["source"] for item in plan["provider_cache"]["file_moves"]}
    assert plan["provider_cache"]["contaminated_request_manifest_count"] == 1
    assert plan["provider_cache"]["file_move_count"] == 2
    assert str(request_path) in move_sources
    assert str(payload_path) in move_sources


def test_apply_quarantine_plan_moves_cache_payloads_without_postgres(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    runtime_root = workspace / "runtime"
    cache_dir = runtime_root / "provider_cache" / "local_dev" / "live" / "harvest_profile_scraper_batch"
    cache_dir.mkdir(parents=True)
    request_path = cache_dir / "bad.request.json"
    payload_path = cache_dir / "bad.json"
    request_path.write_text(
        json.dumps({"url": "https://www.linkedin.com/in/openai-agent-current-0189/"}),
        encoding="utf-8",
    )
    payload_path.write_text(json.dumps({"status": 404}), encoding="utf-8")

    result = apply_runtime_contamination_quarantine_plan(
        workspace_root=workspace,
        target_runtime_dir=runtime_root / "test_env" / "case",
        include_postgres=False,
        sample_limit=10,
    )

    assert result["status"] == "applied"
    assert result["mutates_state"] is True
    assert result["provider_cache"]["moved_count"] == 2
    assert not request_path.exists()
    assert not payload_path.exists()
    assert result["post_apply_report"]["status"] == "clean"
