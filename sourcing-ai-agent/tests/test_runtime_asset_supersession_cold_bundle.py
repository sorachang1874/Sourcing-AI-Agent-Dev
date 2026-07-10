import hashlib
import json
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from sourcing_agent.runtime_asset_retention_prune import (
    apply_runtime_asset_prune_plan,
    build_independent_review_scope_evidence,
    build_runtime_asset_prune_plan_from_cold_bundle_manifest,
)
from sourcing_agent.runtime_asset_supersession_cold_bundle import (
    build_runtime_asset_supersession_cold_bundle_manifest,
    render_runtime_asset_supersession_cold_bundle_markdown,
)
from sourcing_agent.runtime_asset_supersession_cold_manifest import build_runtime_asset_supersession_cold_manifest


def test_supersession_cold_bundle_manifest_can_feed_reviewed_prune_apply() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_dir = _write_artifact(root, "runtime/test_env/nightly_rerun1", {"result.json": '{"old": true}'})
        ref_dir = _write_artifact(root, "runtime/test_env/nightly_rerun2", {"result.json": '{"new": true}'})
        proof_manifest = build_runtime_asset_supersession_cold_manifest(
            supersession_report=_report(
                [
                    _artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir),
                    _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
                ]
            ),
            workspace_root=root,
        )

        bundle_manifest = build_runtime_asset_supersession_cold_bundle_manifest(
            proof_manifest=proof_manifest,
            workspace_root=root,
            archive_root="runtime/cold_bundles/test",
            create_archives=True,
            compression="gzip",
        )
        bundle_path = root / "runtime" / "asset_governance" / "bundle.json"
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        bundle_path.write_text(json.dumps(bundle_manifest), encoding="utf-8")
        _write_review_artifact(root)

        assert bundle_manifest["status"] == "ready_for_prune_cold_copy_manifest"
        archive = bundle_manifest["archives"][0]
        assert archive["status"] == "archive_ready_for_prune_cold_copy"
        assert archive["archive_verified"] is True
        assert len(archive["archive_sha256"]) == 64
        assert "nightly_rerun1" in render_runtime_asset_supersession_cold_bundle_markdown(bundle_manifest)

        apply_report = apply_runtime_asset_prune_plan(
            plan=_prune_plan(
                root=root, bundle_manifest=bundle_manifest, cold_copy_manifest="runtime/asset_governance/bundle.json"
            ),
            workspace_root=root,
            apply=True,
            reviewed=True,
        )

        assert apply_report["status"] == "applied"
        assert not old_dir.exists()
        assert (root / archive["archive_path"]).exists()


def test_supersession_cold_bundle_apply_blocks_tampered_archive() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_dir = _write_artifact(root, "runtime/test_env/nightly_rerun1", {"result.json": '{"old": true}'})
        ref_dir = _write_artifact(root, "runtime/test_env/nightly_rerun2", {"result.json": '{"new": true}'})
        proof_manifest = build_runtime_asset_supersession_cold_manifest(
            supersession_report=_report(
                [
                    _artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir),
                    _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
                ]
            ),
            workspace_root=root,
        )
        bundle_manifest = build_runtime_asset_supersession_cold_bundle_manifest(
            proof_manifest=proof_manifest,
            workspace_root=root,
            archive_root="runtime/cold_bundles/test",
            create_archives=True,
            compression="gzip",
        )
        archive = bundle_manifest["archives"][0]
        (root / archive["archive_path"]).write_bytes(b"tampered")
        bundle_path = root / "runtime" / "asset_governance" / "bundle.json"
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        bundle_path.write_text(json.dumps(bundle_manifest), encoding="utf-8")
        _write_review_artifact(root)

        apply_report = apply_runtime_asset_prune_plan(
            plan=_prune_plan(
                root=root, bundle_manifest=bundle_manifest, cold_copy_manifest="runtime/asset_governance/bundle.json"
            ),
            workspace_root=root,
            apply=True,
            reviewed=True,
        )

        assert apply_report["status"] == "blocked"
        assert any("cold_bundle_archive_size_mismatch" in blocker for blocker in apply_report["blockers"])
        assert old_dir.exists()


def test_supersession_cold_bundle_apply_blocks_archive_under_prunable_root() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_dir = _write_artifact(root, "runtime/test_env/nightly_rerun1", {"result.json": '{"old": true}'})
        ref_dir = _write_artifact(root, "runtime/test_env/nightly_rerun2", {"result.json": '{"new": true}'})
        proof_manifest = build_runtime_asset_supersession_cold_manifest(
            supersession_report=_report(
                [
                    _artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir),
                    _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
                ]
            ),
            workspace_root=root,
        )
        bundle_manifest = build_runtime_asset_supersession_cold_bundle_manifest(
            proof_manifest=proof_manifest,
            workspace_root=root,
            archive_root="runtime/cold_bundles/test",
            create_archives=True,
            compression="gzip",
        )
        archive = bundle_manifest["archives"][0]
        unsafe_archive = root / "runtime" / "test_env" / "archive_store" / "copy.tar.gzip"
        unsafe_archive.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / archive["archive_path"], unsafe_archive)
        archive["archive_path"] = unsafe_archive.relative_to(root).as_posix()
        bundle_path = root / "runtime" / "asset_governance" / "bundle.json"
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        bundle_path.write_text(json.dumps(bundle_manifest), encoding="utf-8")
        _write_review_artifact(root)

        apply_report = apply_runtime_asset_prune_plan(
            plan=_prune_plan(
                root=root, bundle_manifest=bundle_manifest, cold_copy_manifest="runtime/asset_governance/bundle.json"
            ),
            workspace_root=root,
            apply=True,
            reviewed=True,
        )

        assert apply_report["status"] == "blocked"
        assert any("cold_bundle_archive_inside_prunable_source_root" in blocker for blocker in apply_report["blockers"])
        assert old_dir.exists()


def test_supersession_cold_bundle_apply_blocks_archive_symlink_path() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_dir = _write_artifact(root, "runtime/test_env/nightly_rerun1", {"result.json": '{"old": true}'})
        ref_dir = _write_artifact(root, "runtime/test_env/nightly_rerun2", {"result.json": '{"new": true}'})
        proof_manifest = build_runtime_asset_supersession_cold_manifest(
            supersession_report=_report(
                [
                    _artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir),
                    _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
                ]
            ),
            workspace_root=root,
        )
        bundle_manifest = build_runtime_asset_supersession_cold_bundle_manifest(
            proof_manifest=proof_manifest,
            workspace_root=root,
            archive_root="runtime/cold_bundles/test",
            create_archives=True,
            compression="gzip",
        )
        archive = bundle_manifest["archives"][0]
        link_path = root / "runtime" / "cold_bundles" / "linked.tar.gzip"
        link_path.parent.mkdir(parents=True, exist_ok=True)
        link_path.symlink_to(root / archive["archive_path"])
        archive["archive_path"] = link_path.relative_to(root).as_posix()
        bundle_path = root / "runtime" / "asset_governance" / "bundle.json"
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        bundle_path.write_text(json.dumps(bundle_manifest), encoding="utf-8")
        _write_review_artifact(root)

        apply_report = apply_runtime_asset_prune_plan(
            plan=_prune_plan(
                root=root, bundle_manifest=bundle_manifest, cold_copy_manifest="runtime/asset_governance/bundle.json"
            ),
            workspace_root=root,
            apply=True,
            reviewed=True,
        )

        assert apply_report["status"] == "blocked"
        assert any("cold_bundle_archive_raw_path_contains_symlink" in blocker for blocker in apply_report["blockers"])
        assert old_dir.exists()


def test_supersession_cold_bundle_planning_mode_is_not_prune_ready() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_dir = _write_artifact(root, "runtime/test_env/nightly_rerun1", {"result.json": '{"old": true}'})
        ref_dir = _write_artifact(root, "runtime/test_env/nightly_rerun2", {"result.json": '{"new": true}'})
        proof_manifest = build_runtime_asset_supersession_cold_manifest(
            supersession_report=_report(
                [
                    _artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir),
                    _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
                ]
            ),
            workspace_root=root,
        )

        bundle_manifest = build_runtime_asset_supersession_cold_bundle_manifest(
            proof_manifest=proof_manifest,
            workspace_root=root,
            archive_root="runtime/cold_bundles/test",
            create_archives=False,
            compression="gzip",
        )

        assert bundle_manifest["status"] == "planning_ready_needs_archive_creation"
        assert bundle_manifest["deletion_allowed"] is False
        assert bundle_manifest["archives"][0]["status"] == "blocked_cold_bundle_manifest"
        assert "archive_not_created" in bundle_manifest["archives"][0]["blocking_reasons"]


def test_supersession_cold_bundle_blocks_archive_root_under_source_root() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        root = Path(tempdir)
        old_dir = _write_artifact(root, "runtime/test_env/nightly_rerun1", {"result.json": '{"old": true}'})
        ref_dir = _write_artifact(root, "runtime/test_env/nightly_rerun2", {"result.json": '{"new": true}'})
        proof_manifest = build_runtime_asset_supersession_cold_manifest(
            supersession_report=_report(
                [
                    _artifact(root=root, path=old_dir, status="supersession_review_candidate", reference=ref_dir),
                    _artifact(root=root, path=ref_dir, status="family_reference_keep", reference=ref_dir),
                ]
            ),
            workspace_root=root,
        )

        bundle_manifest = build_runtime_asset_supersession_cold_bundle_manifest(
            proof_manifest=proof_manifest,
            workspace_root=root,
            archive_root="runtime/test_env/archive_store",
            create_archives=False,
            compression="gzip",
        )

        assert bundle_manifest["archives"][0]["status"] == "blocked_cold_bundle_manifest"
        assert "archive_root_inside_prunable_source_root" in bundle_manifest["archives"][0]["blocking_reasons"]


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


def _report(artifacts: list[dict[str, object]]) -> dict[str, object]:
    return {
        "contract_version": "runtime_asset_supersession_audit_v1",
        "status": "ready_for_supersession_review",
        "read_only": True,
        "deletion_allowed": False,
        "source_contract_valid": True,
        "source_contract_version": "runtime_asset_retention_audit_v1",
        "summary": {},
        "artifacts": artifacts,
        "skipped_artifacts": [],
    }


def _write_review_artifact(root: Path, plan: dict[str, object] | None = None) -> None:
    review_path = root / "runtime" / "reviews" / "go.md"
    review_path.parent.mkdir(parents=True, exist_ok=True)
    evidence = dict((plan or {}).get("destructive_review_evidence") or {})
    scope_lines: list[str] = []
    for artifact in list(evidence.get("review_required_artifacts") or []):
        payload = dict(artifact or {})
        scope_lines.append(f"`{payload.get('path')}`")
        scope_lines.append(f"`{payload.get('sha256')}`")
    for token in list(evidence.get("review_required_tokens") or []):
        scope_lines.append(f"`{token}`")
    files = [
        "src/sourcing_agent/runtime_asset_supersession_cold_bundle.py",
        "src/sourcing_agent/runtime_asset_retention_prune.py",
    ]
    raw_output = (
        "\n".join(
            [
                "`src/sourcing_agent/runtime_asset_supersession_cold_bundle.py`",
                "`src/sourcing_agent/runtime_asset_retention_prune.py`",
                "docs/INDEPENDENT_REVIEW_GATE.md",
                *scope_lines,
                "GO",
            ]
        )
        + "\n"
    ).encode("utf-8")
    metadata = _verified_review_metadata(
        root,
        stem="go",
        title="test-cold-bundle-apply",
        files=files,
        raw_output=raw_output,
    )
    review_path.write_text(
        "\n".join(
            [
                "## Review Metadata",
                "- title: test-cold-bundle-apply",
                *metadata,
                "- command: `codex exec --strict-config --sandbox read-only`",
                "- contract_docs_considered: `docs/INDEPENDENT_REVIEW_GATE.md`",
                "## Reviewer Output",
                "",
                raw_output.decode("utf-8").rstrip("\n"),
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _verified_review_metadata(
    root: Path,
    *,
    stem: str,
    title: str,
    files: list[str],
    raw_output: bytes,
) -> list[str]:
    review_dir = root / "runtime" / "reviews"
    review_dir.mkdir(parents=True, exist_ok=True)
    base = _ensure_review_git_scope(root, files=files)
    scope = build_independent_review_scope_evidence(
        workspace_root=root,
        title=title,
        base_ref=base,
        files=files,
        extra_context="",
    )
    thread_id = "019f0000-0000-7000-8000-000000000002"
    config_relative = f"runtime/reviews/{stem}.config.toml"
    config_raw = ('model = "gpt-5.6-sol"\nmodel_reasoning_effort = "ultra"\nservice_tier = "fast"\n').encode("utf-8")
    (root / config_relative).write_bytes(config_raw)
    prompt_relative = f"runtime/reviews/{stem}.prompt.md"
    prompt_raw = _verified_review_prompt(scope).encode("utf-8")
    (root / prompt_relative).write_bytes(prompt_raw)
    normalized_final = raw_output.decode("utf-8").rstrip("\r\n")
    turn_id = "019f0000-0000-7000-8000-000000000012"
    rollout_relative = f"runtime/reviews/{stem}.rollout-{thread_id}.jsonl"
    rollout_events = [
        {
            "type": "session_meta",
            "payload": {"id": thread_id, "cli_version": "0.144.0", "source": "exec"},
        },
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": turn_id}},
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": prompt_raw.decode("utf-8")}],
            },
        },
        {
            "type": "event_msg",
            "payload": {"type": "user_message", "message": prompt_raw.decode("utf-8")},
        },
        {
            "type": "event_msg",
            "payload": {
                "type": "thread_settings_applied",
                "thread_settings": {
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "ultra",
                    "service_tier": "priority",
                },
            },
        },
        {
            "type": "event_msg",
            "payload": {
                "type": "agent_message",
                "phase": "final_answer",
                "message": normalized_final,
            },
        },
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "phase": "final_answer",
                "content": [{"type": "output_text", "text": normalized_final}],
            },
        },
        {
            "type": "event_msg",
            "payload": {
                "type": "task_complete",
                "turn_id": turn_id,
                "last_agent_message": normalized_final,
            },
        },
    ]
    rollout_raw = "".join(json.dumps(event, sort_keys=True) + "\n" for event in rollout_events).encode("utf-8")
    (root / rollout_relative).write_bytes(rollout_raw)
    rollout_sha = hashlib.sha256(rollout_raw).hexdigest()
    causal_binding = {
        "turn_id": turn_id,
        "prompt_sha256": hashlib.sha256(prompt_raw).hexdigest(),
        "normalized_final_output_sha256": hashlib.sha256(normalized_final.encode("utf-8")).hexdigest(),
        "rollout_json_valid": True,
        "text_utf8_valid": True,
        "session_source_exec": True,
        "single_task_turn": True,
        "no_abort": True,
        "prompt_response_item_exact": True,
        "prompt_event_message_exact": True,
        "final_response_item_exact": True,
        "final_event_message_exact": True,
        "task_complete_final_exact": True,
    }
    events_relative = f"runtime/reviews/{stem}.events.jsonl"
    events_raw = (json.dumps({"type": "thread.started", "thread_id": thread_id}) + "\n").encode("utf-8")
    (root / events_relative).write_bytes(events_raw)
    raw_output_relative = f"runtime/reviews/{stem}.raw-output.md"
    (root / raw_output_relative).write_bytes(raw_output)
    evidence_relative = f"runtime/reviews/{stem}.effective-config.json"
    evidence_payload = {
        "contract_version": "independent_review_effective_config_v2",
        "config": {
            "path": config_relative,
            "sha256": hashlib.sha256(config_raw).hexdigest(),
            "model": "gpt-5.6-sol",
            "reasoning_effort": "ultra",
            "service_tier": "fast",
        },
        "session": {
            "session_id": thread_id,
            "thread_id": thread_id,
            "codex_cli_version": "0.144.0",
            "rollout_path": rollout_relative,
            "rollout_sha256": rollout_sha,
        },
        "process": {"reviewer_exit_code": 0},
        "effective": {
            "model": "gpt-5.6-sol",
            "reasoning_effort": "ultra",
            "service_tier": "priority",
            "source": {
                "model": ["thread_settings_applied"],
                "reasoning_effort": ["thread_settings_applied"],
                "service_tier": ["thread_settings_applied"],
            },
        },
        "model_reroutes": [],
        "causal_binding": causal_binding,
        "scope": scope,
        "artifacts": {
            "prompt": {"path": prompt_relative, "sha256": hashlib.sha256(prompt_raw).hexdigest()},
            "events": {"path": events_relative, "sha256": hashlib.sha256(events_raw).hexdigest()},
            "raw_output": {
                "path": raw_output_relative,
                "sha256": hashlib.sha256(raw_output).hexdigest(),
            },
        },
    }
    evidence_raw = (json.dumps(evidence_payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    (root / evidence_relative).write_bytes(evidence_raw)
    return [
        "- reviewer_model: gpt-5.6-sol",
        "- reviewer_reasoning_effort: ultra",
        "- reviewer_service_tier: priority",
        f"- reviewer_config_path: `{config_relative}`",
        f"- reviewer_config_sha256: {hashlib.sha256(config_raw).hexdigest()}",
        "- reviewer_exit_code: 0",
        "- reviewer_codex_cli_version: 0.144.0",
        f"- reviewer_thread_id: {thread_id}",
        f"- reviewer_rollout_path: `{rollout_relative}`",
        f"- reviewer_rollout_sha256: {rollout_sha}",
        f"- reviewer_effective_config_path: `{evidence_relative}`",
        f"- reviewer_effective_config_sha256: {hashlib.sha256(evidence_raw).hexdigest()}",
        f"- review_scope_mode: {scope['scope_mode']}",
        f"- review_base_ref: {scope['base_ref']}",
        f"- review_resolved_base_commit: {scope['resolved_base_commit']}",
        f"- review_resolved_head_commit: {scope['resolved_head_commit']}",
        f"- review_git_diff_sha256: {scope['git_diff_sha256']}",
        f"- review_git_tree_sha256: {scope['git_tree_sha256']}",
        f"- review_extra_context_sha256: {scope['extra_context_sha256']}",
        f"- review_scope_digest_sha256: {scope['scope_digest_sha256']}",
        f"- prompt_path: `{prompt_relative}`",
        f"- prompt_sha256: {hashlib.sha256(prompt_raw).hexdigest()}",
        f"- events_path: `{events_relative}`",
        f"- events_sha256: {hashlib.sha256(events_raw).hexdigest()}",
        f"- raw_output_path: `{raw_output_relative}`",
        f"- raw_output_sha256: {hashlib.sha256(raw_output).hexdigest()}",
    ]


def _ensure_review_git_scope(root: Path, *, files: list[str]) -> str:
    if not (root / ".git").exists():
        root.mkdir(parents=True, exist_ok=True)
        (root / ".gitignore").write_text("runtime/\noutput/\n", encoding="utf-8")
        for relative in files:
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("base\n", encoding="utf-8")
        subprocess.run(["git", "init", "-q"], cwd=root, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=root, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=root, check=True)
        subprocess.run(["git", "add", ".gitignore", *files], cwd=root, check=True)
        subprocess.run(["git", "commit", "-qm", "base"], cwd=root, check=True)
        for relative in files:
            (root / relative).write_text("reviewed\n", encoding="utf-8")
        subprocess.run(["git", "add", *files], cwd=root, check=True)
        subprocess.run(["git", "commit", "-qm", "reviewed"], cwd=root, check=True)
    return subprocess.run(
        ["git", "rev-list", "--max-parents=0", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _verified_review_prompt(scope: dict[str, object]) -> str:
    files = "\n".join(f"- `{path}`" for path in list(scope["files"]))
    return (
        "# Review brief\n\n## Review Scope\n\n"
        f"Title: {scope['title']}\n\n"
        f"Base/ref: {scope['base_ref']}\n\n"
        f"Resolved base commit: {scope['resolved_base_commit']}\n\n"
        f"Resolved head commit: {scope['resolved_head_commit']}\n\n"
        f"Scope digest: {scope['scope_digest_sha256']}\n\n"
        f"Files or scope:\n{files}\n\n"
        "Additional context:\nNone.\n\nRun read-only inspection commands as needed.\n"
    )


def _prune_plan(*, root: Path, bundle_manifest: dict[str, object], cold_copy_manifest: str) -> dict[str, object]:
    review_plan_artifact = "runtime/asset_governance/prune_plan.json"
    plan = build_runtime_asset_prune_plan_from_cold_bundle_manifest(
        bundle_manifest=bundle_manifest,
        workspace_root=root,
        cold_copy_manifest=cold_copy_manifest,
        review_artifact="runtime/reviews/go.md",
        review_title="test-cold-bundle-apply",
        review_required_files=[
            "src/sourcing_agent/runtime_asset_supersession_cold_bundle.py",
            "src/sourcing_agent/runtime_asset_retention_prune.py",
        ],
        review_plan_artifact=review_plan_artifact,
    )
    plan_path = root / review_plan_artifact
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_review_artifact(root, plan)
    return plan
