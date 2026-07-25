from __future__ import annotations

from pathlib import Path

from sourcing_agent.runtime_asset_retention_audit import (
    build_runtime_asset_retention_report,
    render_runtime_asset_retention_markdown,
)


def _write_bytes(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)


def test_runtime_asset_retention_report_is_read_only_and_filters_markers(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    _write_bytes(workspace / "runtime" / "test_env" / "w6_google_case" / "artifact.json", 128)
    _write_bytes(workspace / "runtime" / "test_env" / "small_openai_case" / "artifact.json", 64)
    _write_bytes(workspace / "output" / "pre_manual_reflection_case" / "report.md", 256)

    report = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env", "output"],
        include_name_markers=["google", "reflection"],
        min_size_bytes=100,
    )

    assert report["contract_version"] == "runtime_asset_retention_audit_v1"
    assert report["read_only"] is True
    assert report["deletion_allowed"] is False
    assert report["summary"]["matched_directory_count"] == 2
    paths = {item["path"] for item in report["directories"]}
    assert "runtime/test_env/w6_google_case" in paths
    assert "output/pre_manual_reflection_case" in paths
    assert "runtime/test_env/small_openai_case" not in paths


def test_runtime_asset_retention_report_classifies_current_and_signoff_dirs(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    _write_bytes(workspace / "output" / "nightly_long_latency_current" / "report.json", 10)
    _write_bytes(workspace / "runtime" / "test_env" / "w6_nightly_20260523_1" / "state.json", 20)

    report = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env", "output"],
        include_name_markers=["nightly", "w6"],
    )

    by_path = {item["path"]: item for item in report["directories"]}
    assert by_path["output/nightly_long_latency_current"]["retention_class"] == "review_current_alias_or_latest_artifact"
    assert (
        by_path["runtime/test_env/w6_nightly_20260523_1"]["retention_class"]
        == "review_signoff_or_pressure_run_artifact"
    )


def test_runtime_asset_retention_markdown_records_post_review_gates(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    _write_bytes(workspace / "runtime" / "test_env" / "google_case" / "artifact.json", 10)

    report = build_runtime_asset_retention_report(workspace_root=workspace, include_name_markers=["google"])
    markdown = render_runtime_asset_retention_markdown(report)

    assert "# Runtime Asset Retention Audit" in markdown
    assert "runtime/test_env/google_case" in markdown
    assert "independent_review_gate_before_any_apply" in markdown
