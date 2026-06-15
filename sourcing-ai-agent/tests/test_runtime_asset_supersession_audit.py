from __future__ import annotations

from sourcing_agent.runtime_asset_supersession_audit import (
    build_runtime_asset_supersession_report,
    render_runtime_asset_supersession_markdown,
)


def test_runtime_asset_supersession_audit_groups_reviewable_run_families() -> None:
    retention = {
        "contract_version": "runtime_asset_retention_audit_v1",
        "read_only": True,
        "deletion_allowed": False,
        "workspace_root": "/repo",
        "directories": [
            {
                "path": "runtime/test_env/nightly_long_latency_rerun17",
                "scan_root": "runtime/test_env",
                "retention_class": "review_signoff_or_pressure_run_artifact",
                "size_bytes": 100,
                "file_count": 10,
                "directory_count": 1,
                "latest_mtime": "2026-05-15T00:00:00+00:00",
                "matched_markers": ["nightly"],
            },
            {
                "path": "runtime/test_env/nightly_long_latency_rerun19",
                "scan_root": "runtime/test_env",
                "retention_class": "review_signoff_or_pressure_run_artifact",
                "size_bytes": 200,
                "file_count": 20,
                "directory_count": 1,
                "latest_mtime": "2026-05-16T00:00:00+00:00",
                "matched_markers": ["nightly"],
            },
            {
                "path": "runtime/test_env/phase12_cutover_20260520_213603",
                "scan_root": "runtime/test_env",
                "retention_class": "review_phase_milestone_artifact",
                "size_bytes": 300,
                "file_count": 30,
                "directory_count": 1,
                "latest_mtime": "2026-05-20T00:00:00+00:00",
                "matched_markers": ["phase12"],
            },
            {
                "path": "runtime/test_env/phase12_cutover_20260521_064141",
                "scan_root": "runtime/test_env",
                "retention_class": "review_phase_milestone_artifact",
                "size_bytes": 400,
                "file_count": 40,
                "directory_count": 1,
                "latest_mtime": "2026-05-21T00:00:00+00:00",
                "matched_markers": ["phase12"],
            },
            {
                "path": "runtime/test_env/google_ordinary_cold_case",
                "scan_root": "runtime/test_env",
                "retention_class": "review_cold_archive_candidate",
                "size_bytes": 500,
                "file_count": 50,
                "directory_count": 1,
                "latest_mtime": "2026-05-10T00:00:00+00:00",
                "matched_markers": ["google"],
            },
            {
                "path": "output/nightly_long_latency_rerun19",
                "scan_root": "output",
                "retention_class": "review_signoff_or_pressure_run_artifact",
                "size_bytes": 50,
                "file_count": 5,
                "directory_count": 0,
                "latest_mtime": "2026-05-16T01:00:00+00:00",
                "matched_markers": ["nightly"],
            },
        ],
    }

    report = build_runtime_asset_supersession_report(retention_report=retention)

    assert report["contract_version"] == "runtime_asset_supersession_audit_v1"
    assert report["read_only"] is True
    assert report["deletion_allowed"] is False
    assert report["source_contract_valid"] is True
    assert report["summary"]["reviewable_artifact_count"] == 5
    assert report["summary"]["skipped_artifact_count"] == 0
    assert report["summary"]["supersession_review_candidate_count"] == 2
    assert report["summary"]["supersession_review_candidate_bytes"] == 400
    by_path = {item["path"]: item for item in report["artifacts"]}
    assert by_path["runtime/test_env/nightly_long_latency_rerun17"]["supersession_status"] == "supersession_review_candidate"
    assert by_path["runtime/test_env/nightly_long_latency_rerun17"]["family_reference_path"] == "runtime/test_env/nightly_long_latency_rerun19"
    assert by_path["output/nightly_long_latency_rerun19"]["supersession_status"] == "singleton_keep"
    assert by_path["runtime/test_env/phase12_cutover_20260520_213603"]["supersession_status"] == "supersession_review_candidate"
    assert by_path["runtime/test_env/phase12_cutover_20260520_213603"]["family_reference_path"] == "runtime/test_env/phase12_cutover_20260521_064141"
    assert "google_ordinary_cold_case" not in by_path


def test_runtime_asset_supersession_markdown_records_post_review_gates() -> None:
    report = build_runtime_asset_supersession_report(
        retention_report={
            "contract_version": "runtime_asset_retention_audit_v1",
            "read_only": True,
            "deletion_allowed": False,
            "directories": [
                {
                    "path": "runtime/test_env/w6_signoff_20260522",
                    "scan_root": "runtime/test_env",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 100,
                    "file_count": 1,
                    "latest_mtime": "2026-05-22T00:00:00+00:00",
                }
            ],
        }
    )

    markdown = render_runtime_asset_supersession_markdown(report)

    assert "Runtime Asset Supersession Audit" in markdown
    assert "Deletion allowed: `False`" in markdown
    assert "independent_review_gate_before_any_apply" in markdown


def test_runtime_asset_supersession_audit_derives_missing_scan_root_without_cross_root_grouping() -> None:
    report = build_runtime_asset_supersession_report(
        retention_report={
            "contract_version": "runtime_asset_retention_audit_v1",
            "read_only": True,
            "deletion_allowed": False,
            "workspace_root": "/repo",
            "directories": [
                {
                    "path": "runtime/test_env/nightly_long_latency_rerun17",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 100,
                    "file_count": 1,
                    "latest_mtime": "2026-05-17T00:00:00+00:00",
                },
                {
                    "path": "output/nightly_long_latency_rerun20",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 200,
                    "file_count": 2,
                    "latest_mtime": "2026-05-20T00:00:00+00:00",
                },
            ],
        }
    )

    assert report["source_contract_valid"] is True
    assert report["summary"]["reviewable_artifact_count"] == 2
    assert report["summary"]["skipped_artifact_count"] == 0
    assert report["summary"]["supersession_review_candidate_count"] == 0
    by_path = {item["path"]: item for item in report["artifacts"]}
    assert by_path["runtime/test_env/nightly_long_latency_rerun17"]["scan_root"] == "runtime/test_env"
    assert by_path["runtime/test_env/nightly_long_latency_rerun17"]["supersession_status"] == "singleton_keep"
    assert by_path["output/nightly_long_latency_rerun20"]["scan_root"] == "output"
    assert by_path["output/nightly_long_latency_rerun20"]["supersession_status"] == "singleton_keep"


def test_runtime_asset_supersession_audit_skips_scan_root_path_mismatch() -> None:
    report = build_runtime_asset_supersession_report(
        retention_report={
            "contract_version": "runtime_asset_retention_audit_v1",
            "read_only": True,
            "deletion_allowed": False,
            "workspace_root": "/repo",
            "directories": [
                {
                    "path": "runtime/test_env/nightly_long_latency_rerun17",
                    "scan_root": "output",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 100,
                    "file_count": 1,
                    "latest_mtime": "2026-05-17T00:00:00+00:00",
                },
                {
                    "path": "/repo/runtime/test_env/nightly_long_latency_rerun18",
                    "scan_root": "runtime/test_env",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 200,
                    "file_count": 2,
                    "latest_mtime": "2026-05-18T00:00:00+00:00",
                },
                {
                    "path": "tmp/nightly_long_latency_rerun19",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 300,
                    "file_count": 3,
                    "latest_mtime": "2026-05-19T00:00:00+00:00",
                },
            ],
        }
    )

    assert report["summary"]["reviewable_artifact_count"] == 1
    assert report["summary"]["skipped_artifact_count"] == 2
    assert report["summary"]["by_skip_reason"] == {
        "path_outside_allowed_scan_roots": 1,
        "scan_root_path_mismatch": 1,
    }
    assert report["artifacts"][0]["path"] == "/repo/runtime/test_env/nightly_long_latency_rerun18"
    assert report["artifacts"][0]["scan_root"] == "runtime/test_env"
    skipped_by_path = {item["path"]: item for item in report["skipped_artifacts"]}
    assert skipped_by_path["runtime/test_env/nightly_long_latency_rerun17"]["skip_reason"] == "scan_root_path_mismatch"
    assert skipped_by_path["tmp/nightly_long_latency_rerun19"]["skip_reason"] == "path_outside_allowed_scan_roots"


def test_runtime_asset_supersession_audit_skips_path_traversal_under_allowed_roots() -> None:
    report = build_runtime_asset_supersession_report(
        retention_report={
            "contract_version": "runtime_asset_retention_audit_v1",
            "read_only": True,
            "deletion_allowed": False,
            "workspace_root": "/repo",
            "directories": [
                {
                    "path": "runtime/test_env/../../secrets/signoff_rerun1",
                    "scan_root": "runtime/test_env",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 100,
                    "file_count": 1,
                    "latest_mtime": "2026-05-17T00:00:00+00:00",
                },
                {
                    "path": "output/../secrets/nightly_rerun1",
                    "scan_root": "output",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 100,
                    "file_count": 1,
                    "latest_mtime": "2026-05-17T00:00:00+00:00",
                },
                {
                    "path": "/outside/repo/runtime/test_env/nightly_rerun2",
                    "scan_root": "runtime/test_env",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 100,
                    "file_count": 1,
                    "latest_mtime": "2026-05-17T00:00:00+00:00",
                },
                {
                    "path": "runtime/test_env",
                    "scan_root": "runtime/test_env",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 100,
                    "file_count": 1,
                    "latest_mtime": "2026-05-17T00:00:00+00:00",
                },
                {
                    "path": "runtime/test_env/./nightly_rerun1",
                    "scan_root": "runtime/test_env",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 100,
                    "file_count": 1,
                    "latest_mtime": "2026-05-17T00:00:00+00:00",
                },
                {
                    "path": "/repo/runtime/test_env/./nightly_rerun2",
                    "scan_root": "runtime/test_env",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 100,
                    "file_count": 1,
                    "latest_mtime": "2026-05-17T00:00:00+00:00",
                },
            ],
        }
    )

    assert report["artifacts"] == []
    assert report["summary"]["skipped_artifact_count"] == 6
    assert report["summary"]["by_skip_reason"] == {
        "absolute_path_outside_workspace_root": 1,
        "path_contains_traversal": 4,
        "path_outside_allowed_scan_roots": 1,
    }


def test_runtime_asset_supersession_audit_blocks_invalid_source_contract() -> None:
    report = build_runtime_asset_supersession_report(
        retention_report={
            "contract_version": "runtime_asset_retention_audit_v0",
            "read_only": True,
            "deletion_allowed": False,
            "directories": [
                {
                    "path": "runtime/test_env/nightly_long_latency_rerun17",
                    "scan_root": "runtime/test_env",
                    "retention_class": "review_signoff_or_pressure_run_artifact",
                    "size_bytes": 100,
                    "file_count": 1,
                    "latest_mtime": "2026-05-17T00:00:00+00:00",
                }
            ],
        }
    )

    assert report["status"] == "blocked_invalid_source_retention_contract"
    assert report["source_contract_valid"] is False
    assert report["artifacts"] == []
    assert report["summary"]["skipped_artifact_count"] == 1
    assert report["summary"]["by_skip_reason"] == {"invalid_source_retention_contract": 1}


def test_runtime_asset_supersession_audit_requires_strict_source_contract_booleans() -> None:
    for raw_read_only, raw_deletion_allowed in (
        ("true", False),
        (1, False),
        (True, "false"),
        (True, 0),
        (None, False),
    ):
        report = build_runtime_asset_supersession_report(
            retention_report={
                "contract_version": "runtime_asset_retention_audit_v1",
                "read_only": raw_read_only,
                "deletion_allowed": raw_deletion_allowed,
                "directories": [
                    {
                        "path": "runtime/test_env/nightly_long_latency_rerun17",
                        "scan_root": "runtime/test_env",
                        "retention_class": "review_signoff_or_pressure_run_artifact",
                        "size_bytes": 100,
                        "file_count": 1,
                        "latest_mtime": "2026-05-17T00:00:00+00:00",
                    }
                ],
            }
        )

        assert report["status"] == "blocked_invalid_source_retention_contract"
        assert report["source_contract_valid"] is False
        assert report["artifacts"] == []
        assert report["summary"]["by_skip_reason"] == {"invalid_source_retention_contract": 1}
