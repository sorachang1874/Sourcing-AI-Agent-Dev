import hashlib
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from sourcing_agent.runtime_asset_supersession_cold_manifest import (
    build_runtime_asset_supersession_cold_manifest,
    render_runtime_asset_supersession_cold_manifest_markdown,
)


def test_supersession_cold_manifest_lists_proof_ready_files_without_allowing_deletion() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_dir = _write_artifact(root, "runtime/test_env/nightly_rerun1", {"result.json": '{"old": true}'})
        ref_dir = _write_artifact(root, "runtime/test_env/nightly_rerun2", {"result.json": '{"new": true}'})
        report = _report(
            [
                _artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir),
                _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
            ]
        )

        manifest = build_runtime_asset_supersession_cold_manifest(supersession_report=report, workspace_root=root)

        assert manifest["contract_version"] == "runtime_asset_supersession_cold_manifest_v1"
        assert manifest["status"] == "ready_for_cold_storage_review"
        assert manifest["read_only"] is True
        assert manifest["deletion_allowed"] is False
        assert manifest["summary"]["proof_ready_artifact_count"] == 1
        artifact = manifest["artifacts"][0]
        assert artifact["status"] == "ready_for_cold_storage_review"
        assert artifact["local_removal_after_cold_copy_recommended"] is True
        assert artifact["deletion_allowed"] is False
        assert artifact["backup_key"] == "runtime-supersession/runtime/test_env/nightly_rerun1"
        assert artifact["file_count"] == 1
        assert artifact["total_size_bytes"] == len('{"old": true}')
        assert artifact["file_sha256_complete"] is True
        assert artifact["files"][0]["sha256"] == hashlib.sha256(b'{"old": true}').hexdigest()
        assert len(artifact["manifest_digest_sha256"]) == 64
        markdown = render_runtime_asset_supersession_cold_manifest_markdown(manifest)
        assert "# Runtime Asset Supersession Cold Manifest" in markdown
        assert "nightly_rerun1" in markdown


def test_supersession_cold_manifest_without_hash_is_planning_only() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_dir = _write_artifact(root, "runtime/test_env/phase12_cutover_20260601", {"a.txt": "a"})
        ref_dir = _write_artifact(root, "runtime/test_env/phase12_cutover_20260602", {"a.txt": "b"})
        report = _report(
            [
                _artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir),
                _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
            ]
        )

        manifest = build_runtime_asset_supersession_cold_manifest(
            supersession_report=report,
            workspace_root=root,
            include_file_sha256=False,
        )

        assert manifest["status"] == "planning_ready_needs_hash_manifest"
        artifact = manifest["artifacts"][0]
        assert artifact["status"] == "planning_ready_needs_hash_manifest"
        assert artifact["file_sha256_complete"] is False
        assert artifact["local_removal_after_cold_copy_recommended"] is False
        assert "sha256" not in artifact["files"][0]


def test_supersession_cold_manifest_blocks_invalid_source_contract() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_dir = _write_artifact(root, "runtime/test_env/nightly_rerun1", {"result.json": "{}"})
        ref_dir = _write_artifact(root, "runtime/test_env/nightly_rerun2", {"result.json": "{}"})
        report = _report(
            [
                _artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir),
                _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
            ],
            source_contract_valid=False,
        )

        manifest = build_runtime_asset_supersession_cold_manifest(supersession_report=report, workspace_root=root)

        assert manifest["status"] == "blocked_invalid_source_supersession_contract"
        artifact = manifest["artifacts"][0]
        assert artifact["status"] == "blocked_cold_storage_manifest"
        assert "invalid_source_supersession_contract" in artifact["blocking_reasons"]
        assert artifact["files"] == []


def test_supersession_cold_manifest_blocks_path_traversal_and_cross_root_reference() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        ref_dir = _write_artifact(root, "output/nightly_rerun2", {"result.json": "{}"})
        candidate = {
            "path": "runtime/test_env/../nightly_rerun1",
            "scan_root": "runtime/test_env",
            "retention_class": "review_signoff_or_pressure_run_artifact",
            "size_bytes": 2,
            "file_count": 1,
            "directory_count": 0,
            "latest_mtime": datetime.now(timezone.utc).isoformat(),
            "family_key": "runtime/test_env:nightly_rerun",
            "family_size": 2,
            "family_reference_path": "output/nightly_rerun2",
            "family_reference_latest_mtime": datetime.now(timezone.utc).isoformat(),
            "supersession_status": "supersession_review_candidate",
        }
        report = _report([candidate, _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir)])

        manifest = build_runtime_asset_supersession_cold_manifest(supersession_report=report, workspace_root=root)

        artifact = manifest["artifacts"][0]
        assert artifact["status"] == "blocked_cold_storage_manifest"
        assert "path_contains_traversal" in artifact["blocking_reasons"]
        assert "family_reference_cross_root" in artifact["blocking_reasons"]


def test_supersession_cold_manifest_respects_selection_limits() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_a = _write_artifact(root, "runtime/test_env/nightly_a_rerun1", {"a": "a" * 5})
        ref_a = _write_artifact(root, "runtime/test_env/nightly_a_rerun2", {"a": "new"})
        old_b = _write_artifact(root, "runtime/test_env/nightly_b_rerun1", {"b": "b" * 4})
        ref_b = _write_artifact(root, "runtime/test_env/nightly_b_rerun2", {"b": "new"})
        report = _report(
            [
                _artifact(root=root, path=old_a, status="supersession_review_candidate", reference=ref_a),
                _artifact(root=root, path=ref_a, status="family_reference_keep", reference=ref_a),
                _artifact(root=root, path=old_b, status="supersession_review_candidate", reference=ref_b),
                _artifact(root=root, path=ref_b, status="family_reference_keep", reference=ref_b),
            ]
        )

        manifest = build_runtime_asset_supersession_cold_manifest(
            supersession_report=report,
            workspace_root=root,
            max_entries=1,
        )

        assert manifest["summary"]["selected_artifact_count"] == 1
        assert manifest["summary"]["deferred_artifact_count"] == 1
        statuses = {item["path"]: item["status"] for item in manifest["artifacts"]}
        assert sorted(statuses.values()) == ["deferred_by_selection_limit", "ready_for_cold_storage_review"]


def test_supersession_cold_manifest_blocks_stale_source_metadata() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_dir = _write_artifact(root, "runtime/test_env/nightly_rerun1", {"result.json": "{}"})
        ref_dir = _write_artifact(root, "runtime/test_env/nightly_rerun2", {"result.json": "{}"})
        candidate = _artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir)
        candidate["size_bytes"] = int(candidate["size_bytes"]) + 1
        report = _report(
            [
                candidate,
                _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
            ]
        )

        manifest = build_runtime_asset_supersession_cold_manifest(supersession_report=report, workspace_root=root)

        artifact = manifest["artifacts"][0]
        assert artifact["status"] == "blocked_cold_storage_manifest"
        assert "source_size_mismatch" in artifact["blocking_reasons"]
        assert artifact["local_removal_after_cold_copy_recommended"] is False


def test_supersession_cold_manifest_blocks_missing_or_invalid_source_mtime() -> None:
    cases = [
        ("", "source_latest_mtime_missing"),
        ("not-a-timestamp", "source_latest_mtime_invalid"),
    ]
    for raw_mtime, expected_blocker in cases:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            old_dir = _write_artifact(root, "runtime/test_env/nightly_rerun1", {"result.json": "{}"})
            ref_dir = _write_artifact(root, "runtime/test_env/nightly_rerun2", {"result.json": "{}"})
            candidate = _artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir)
            candidate["latest_mtime"] = raw_mtime
            report = _report(
                [
                    candidate,
                    _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
                ]
            )

            manifest = build_runtime_asset_supersession_cold_manifest(supersession_report=report, workspace_root=root)

            artifact = manifest["artifacts"][0]
            assert artifact["status"] == "blocked_cold_storage_manifest"
            assert expected_blocker in artifact["blocking_reasons"]
            assert artifact["local_removal_after_cold_copy_recommended"] is False


def test_supersession_cold_manifest_blocks_symlinked_ancestor_before_inventory() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_dir = _write_artifact(root, "output/actual/old", {"result.json": '{"old": true}'})
        ref_dir = _write_artifact(root, "output/actual/ref", {"result.json": '{"new": true}'})
        link_parent = root / "runtime" / "test_env" / "linked"
        link_parent.parent.mkdir(parents=True, exist_ok=True)
        link_parent.symlink_to(root / "output" / "actual", target_is_directory=True)
        report = _report(
            [
                {
                    **_artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir),
                    "path": "runtime/test_env/linked/old",
                    "scan_root": "runtime/test_env",
                    "source_scan_root": "runtime/test_env",
                    "family_reference_path": "runtime/test_env/linked/ref",
                },
                {
                    **_artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
                    "path": "runtime/test_env/linked/ref",
                    "scan_root": "runtime/test_env",
                    "source_scan_root": "runtime/test_env",
                    "family_reference_path": "runtime/test_env/linked/ref",
                },
            ]
        )

        manifest = build_runtime_asset_supersession_cold_manifest(supersession_report=report, workspace_root=root)

        artifact = manifest["artifacts"][0]
        assert artifact["status"] == "blocked_cold_storage_manifest"
        assert "raw_path_contains_symlink" in artifact["blocking_reasons"]
        assert "family_reference_raw_path_contains_symlink" in artifact["blocking_reasons"]
        assert artifact["file_count"] == 0
        assert artifact["local_removal_after_cold_copy_recommended"] is False


def _write_artifact(root: Path, relative_path: str, files: dict[str, str]) -> Path:
    artifact_dir = root / relative_path
    artifact_dir.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        file_path = artifact_dir / name
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding="utf-8")
    return artifact_dir


def _artifact(*, root: Path, path: Path, status: str, reference: Path) -> dict[str, object]:
    stats = _inventory(path)
    relative_path = path.relative_to(root).as_posix()
    reference_relative_path = reference.relative_to(root).as_posix()
    scan_root = "runtime/test_env" if relative_path.startswith("runtime/test_env/") else "output"
    return {
        "path": relative_path,
        "name": path.name,
        "scan_root": scan_root,
        "source_scan_root": scan_root,
        "retention_class": "review_signoff_or_pressure_run_artifact",
        "size_bytes": stats["size_bytes"],
        "file_count": stats["file_count"],
        "directory_count": stats["directory_count"],
        "latest_mtime": stats["latest_mtime"],
        "family_key": f"{scan_root}:nightly_rerun",
        "family_size": 2,
        "family_reference_path": reference_relative_path,
        "family_reference_latest_mtime": _inventory(reference)["latest_mtime"],
        "supersession_status": status,
        "next_gate": "prove_family_reference_supersedes_then_cold_copy_review",
    }


def _inventory(path: Path) -> dict[str, object]:
    file_count = 0
    directory_count = 0
    size_bytes = 0
    latest = datetime.fromtimestamp(path.lstat().st_mtime, timezone.utc)
    for child in sorted(path.rglob("*")):
        stat = child.lstat()
        latest = max(latest, datetime.fromtimestamp(stat.st_mtime, timezone.utc))
        if child.is_dir():
            directory_count += 1
        elif child.is_file():
            file_count += 1
            size_bytes += int(stat.st_size)
    return {
        "file_count": file_count,
        "directory_count": directory_count,
        "size_bytes": size_bytes,
        "latest_mtime": latest.isoformat(),
    }


def _report(artifacts: list[dict[str, object]], *, source_contract_valid: bool = True) -> dict[str, object]:
    return {
        "contract_version": "runtime_asset_supersession_audit_v1",
        "status": "ready_for_supersession_review",
        "read_only": True,
        "deletion_allowed": False,
        "source_contract_valid": source_contract_valid,
        "source_contract_version": "runtime_asset_retention_audit_v1",
        "summary": {},
        "artifacts": artifacts,
        "skipped_artifacts": [],
    }
