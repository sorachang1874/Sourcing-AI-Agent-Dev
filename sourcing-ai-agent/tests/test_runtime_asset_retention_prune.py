from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sourcing_agent import runtime_asset_retention_prune as prune_module
from sourcing_agent.runtime_asset_retention_audit import build_runtime_asset_retention_report
from sourcing_agent.runtime_asset_retention_prune import (
    apply_runtime_asset_prune_plan,
    build_runtime_asset_prune_plan,
    build_runtime_asset_prune_plan_from_cold_bundle_manifest,
    render_runtime_asset_prune_apply_markdown,
    render_runtime_asset_prune_plan_markdown,
)


def _write_bytes(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)


def _set_old_mtime(path: Path, *, days: int = 20) -> None:
    timestamp = (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()
    for child in sorted(path.rglob("*"), reverse=True):
        os.utime(child, (timestamp, timestamp))
    os.utime(path, (timestamp, timestamp))


def _review_artifact(workspace: Path) -> str:
    path = workspace / "runtime" / "reviews" / "retention_prune_review.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "Reviewed scope: test.\n\n`GO`\n"
    metadata = _verified_review_metadata(
        workspace,
        stem="retention_prune_review",
        title="runtime asset prune test",
        files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        raw_output=body.encode("utf-8"),
    )
    path.write_text(
        "## Review Metadata\n\n"
        "- title: runtime asset prune test\n" + "\n".join(metadata) + "\n"
        "- command: `codex exec --strict-config --sandbox read-only`\n"
        "- contract_docs_considered: `AGENTS.md`, `docs/INDEPENDENT_REVIEW_GATE.md`\n"
        "\nReviewed scope:\n"
        "- `src/sourcing_agent/runtime_asset_retention_prune.py`\n\n"
        "## Reviewer Output\n\n" + body,
        encoding="utf-8",
    )
    return str(path.relative_to(workspace))


def _verified_review_metadata(
    workspace: Path,
    *,
    stem: str,
    title: str,
    files: list[str],
    raw_output: bytes,
) -> list[str]:
    review_dir = workspace / "runtime" / "reviews"
    review_dir.mkdir(parents=True, exist_ok=True)
    base = _ensure_review_git_scope(workspace, files=files)
    scope = prune_module.build_independent_review_scope_evidence(
        workspace_root=workspace,
        title=title,
        base_ref=base,
        files=files,
        extra_context="",
    )
    thread_id = "019f0000-0000-7000-8000-000000000001"
    config_relative = f"runtime/reviews/{stem}.config.toml"
    config_path = workspace / config_relative
    config_raw = ('model = "gpt-5.6-sol"\nmodel_reasoning_effort = "ultra"\nservice_tier = "fast"\n').encode("utf-8")
    config_path.write_bytes(config_raw)
    prompt_relative = f"runtime/reviews/{stem}.prompt.md"
    prompt_raw = _verified_review_prompt(scope).encode("utf-8")
    (workspace / prompt_relative).write_bytes(prompt_raw)
    normalized_final = raw_output.decode("utf-8").rstrip("\r\n")
    turn_id = "019f0000-0000-7000-8000-000000000011"
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
    rollout_path = workspace / rollout_relative
    rollout_path.write_bytes(rollout_raw)
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
    (workspace / events_relative).write_bytes(events_raw)
    raw_output_relative = f"runtime/reviews/{stem}.raw-output.md"
    (workspace / raw_output_relative).write_bytes(raw_output)
    evidence_relative = f"runtime/reviews/{stem}.effective-config.json"
    evidence_path = workspace / evidence_relative
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
    evidence_path.write_bytes(evidence_raw)
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


def _ensure_review_git_scope(workspace: Path, *, files: list[str]) -> str:
    if not (workspace / ".git").exists():
        workspace.mkdir(parents=True, exist_ok=True)
        (workspace / ".gitignore").write_text("runtime/\noutput/\n", encoding="utf-8")
        for relative in files:
            path = workspace / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("base\n", encoding="utf-8")
        subprocess.run(["git", "init", "-q"], cwd=workspace, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=workspace, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=workspace, check=True)
        subprocess.run(["git", "add", ".gitignore", *files], cwd=workspace, check=True)
        subprocess.run(["git", "commit", "-qm", "base"], cwd=workspace, check=True)
        for relative in files:
            (workspace / relative).write_text("reviewed\n", encoding="utf-8")
        subprocess.run(["git", "add", *files], cwd=workspace, check=True)
        subprocess.run(["git", "commit", "-qm", "reviewed"], cwd=workspace, check=True)
    return subprocess.run(
        ["git", "rev-list", "--max-parents=0", "HEAD"],
        cwd=workspace,
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


def _accepted_exception() -> str:
    return "user_accepted_local_runtime_prune_without_cold_copy_under_disk_pressure"


def _review_kwargs(workspace: Path) -> dict[str, object]:
    return {
        "review_artifact": _review_artifact(workspace),
        "review_title": "runtime asset prune test",
        "review_required_files": ["src/sourcing_agent/runtime_asset_retention_prune.py"],
        "accepted_retention_exception": _accepted_exception(),
    }


def _write_scope_review_artifact(workspace: Path, plan: dict[str, object]) -> str:
    path = workspace / "runtime" / "reviews" / "retention_prune_scope_review.md"
    evidence = dict(plan.get("destructive_review_evidence") or {})
    output_lines: list[str] = []
    for artifact in list(evidence.get("review_required_artifacts") or []):
        payload = dict(artifact or {})
        output_lines.append(f"- `{payload.get('path')}` sha256 `{payload.get('sha256')}`")
    for token in list(evidence.get("review_required_tokens") or []):
        output_lines.append(f"- `{token}`")
    output_lines.append("GO")
    raw_output = ("\n".join(output_lines) + "\n").encode("utf-8")
    metadata = _verified_review_metadata(
        workspace,
        stem="retention_prune_scope_review",
        title="runtime asset prune test",
        files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        raw_output=raw_output,
    )
    lines = [
        "## Review Metadata",
        "",
        "- title: runtime asset prune test",
        *metadata,
        "- command: `codex exec --strict-config --sandbox read-only`",
        "- contract_docs_considered: `AGENTS.md`, `docs/INDEPENDENT_REVIEW_GATE.md`",
        "",
        "Reviewed scope:",
        "- `src/sourcing_agent/runtime_asset_retention_prune.py`",
        "- `docs/INDEPENDENT_REVIEW_GATE.md`",
        "",
        "## Reviewer Output",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(("\n".join(lines) + "\n").encode("utf-8") + raw_output)
    return str(path.relative_to(workspace))


def _write_prune_plan_artifact(workspace: Path, plan: dict[str, object], relative_path: str) -> None:
    path = workspace / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(prune_module.dumps_report(plan), encoding="utf-8")


def _write_cold_copy_manifest(workspace: Path, operations: list[dict[str, object]], *, omit_first: bool = False) -> str:
    path = workspace / "runtime" / "asset_governance" / "test_cold_copy_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    entries = []
    for index, operation in enumerate(operations):
        if index == 0 and omit_first:
            continue
        entries.append(
            {
                "path": operation["path"],
                "size_bytes": operation["size_bytes"],
                "file_count": operation["file_count"],
                "directory_count": operation["directory_count"],
                "latest_mtime": operation["latest_mtime"],
                "sha256": f"test-proof-{index}",
            }
        )
    path.write_text(prune_module.dumps_report({"entries": entries}), encoding="utf-8")
    return str(path.relative_to(workspace))


def _cold_bundle_manifest_for_items(items: list[dict[str, object]]) -> dict[str, object]:
    archives = []
    for index, item in enumerate(items):
        archives.append(
            {
                "path": item["path"],
                "status": "archive_ready_for_prune_cold_copy",
                "blocking_reasons": [],
                "size_bytes": item["size_bytes"],
                "file_count": item["file_count"],
                "directory_count": item["directory_count"],
                "latest_mtime": item["latest_mtime"],
                "retention_class": item["retention_class"],
                "scan_root": item["scan_root"],
                "source_manifest_digest_sha256": f"{index + 1:064x}",
                "archive_path": f"runtime/cold_bundles/test/archive-{index}.tar.zstd",
                "archive_format": "tar.zstd",
                "archive_size_bytes": 1,
                "archive_sha256": f"{index + 2:064x}",
                "archive_created": True,
                "archive_verified": True,
                "deletion_allowed": False,
                "proof": f"{index + 2:064x}",
            }
        )
    return {
        "contract_version": "runtime_asset_supersession_cold_bundle_manifest_v1",
        "status": "ready_for_prune_cold_copy_manifest",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "archive_created": True,
        "deletion_allowed": False,
        "source_contract_valid": True,
        "summary": {
            "archive_count": len(archives),
            "archive_created_count": len(archives),
            "ready_archive_count": len(archives),
            "archive_verified_count": len(archives),
            "blocked_archive_count": 0,
        },
        "archives": archives,
    }


def test_runtime_prune_plan_selects_only_old_allowed_runtime_dirs(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    current_case = workspace / "runtime" / "test_env" / "nightly_current"
    protected_case = workspace / "runtime" / "test_env" / "company_assets" / "google" / "snapshot"
    output_case = workspace / "output" / "pre_manual_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _write_bytes(current_case / "artifact.json", 128)
    _write_bytes(protected_case / "artifact.json", 128)
    _write_bytes(output_case / "report.json", 128)
    _set_old_mtime(old_case)
    _set_old_mtime(current_case)
    _set_old_mtime(workspace / "runtime" / "test_env" / "company_assets")
    _set_old_mtime(output_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env", "output"],
        include_name_markers=["google", "nightly", "pre_manual", "company"],
        min_size_bytes=1,
        max_depth=2,
    )

    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )

    assert plan["contract_version"] == "runtime_asset_retention_prune_v1"
    paths = {item["path"] for item in plan["operations"]}
    assert "runtime/test_env/w6_google_old_case" in paths
    assert "output/pre_manual_old_case" in paths
    assert "runtime/test_env/nightly_current" not in paths
    assert all("company_assets" not in path for path in paths)
    skipped = {item["path"]: item["reason"] for item in plan["skipped"]}
    assert skipped["runtime/test_env/nightly_current"] == "current_or_latest_alias"


def test_runtime_prune_plan_can_filter_to_cold_archive_candidates(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    cold_case = workspace / "runtime" / "test_env" / "google_manual_old_case"
    signoff_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(cold_case / "artifact.json", 128)
    _write_bytes(signoff_case / "artifact.json", 128)
    _set_old_mtime(cold_case)
    _set_old_mtime(signoff_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google", "w6"],
        min_size_bytes=1,
    )

    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        allowed_retention_classes=["review_cold_archive_candidate"],
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        accepted_retention_exception=_accepted_exception(),
    )

    paths = {item["path"] for item in plan["operations"]}
    assert paths == {"runtime/test_env/google_manual_old_case"}
    skipped = {item["path"]: item["reason"] for item in plan["skipped"]}
    assert skipped["runtime/test_env/w6_google_old_case"] == "retention_class_not_allowed_for_plan"


def test_runtime_prune_plan_reclassifies_signoff_like_markers_before_cold_filter(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    cold_case = workspace / "runtime" / "test_env" / "google_manual_old_case"
    smoke_case = workspace / "runtime" / "test_env" / "smoke_gpt55_openai_agent_20260505"
    closeout_case = workspace / "runtime" / "test_env" / "board_runtime_pg_closeout_20260506"
    review_case = workspace / "runtime" / "test_env" / "openai_agent_scripted_20260508_review10"
    rerun_case = workspace / "runtime" / "test_env" / "profile_contract_pg_lovable_rerun71_20260509"
    for path in (cold_case, smoke_case, closeout_case, review_case, rerun_case):
        _write_bytes(path / "artifact.json", 128)
        _set_old_mtime(path)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google", "smoke", "board_runtime", "scripted", "profile_contract"],
        min_size_bytes=1,
    )

    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        allowed_retention_classes=["review_cold_archive_candidate"],
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        accepted_retention_exception=_accepted_exception(),
    )

    paths = {item["path"] for item in plan["operations"]}
    assert paths == {"runtime/test_env/google_manual_old_case"}
    skipped = {item["path"]: item["reason"] for item in plan["skipped"]}
    assert skipped["runtime/test_env/smoke_gpt55_openai_agent_20260505"] == "retention_class_not_allowed_for_plan"
    assert skipped["runtime/test_env/board_runtime_pg_closeout_20260506"] == "retention_class_not_allowed_for_plan"
    assert skipped["runtime/test_env/openai_agent_scripted_20260508_review10"] == "retention_class_not_allowed_for_plan"
    assert (
        skipped["runtime/test_env/profile_contract_pg_lovable_rerun71_20260509"]
        == "retention_class_not_allowed_for_plan"
    )


def test_runtime_prune_apply_is_dry_run_by_default_and_requires_reviewed(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )

    dry_run = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace)
    assert dry_run["status"] == "dry_run_ready"
    assert old_case.exists()

    blocked = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True)
    assert blocked["status"] == "blocked"
    assert "apply_requires_reviewed_flag" in blocked["blockers"]
    assert old_case.exists()


def test_runtime_prune_apply_removes_reviewed_selected_dirs(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "google_manual_old_case"
    keep_case = workspace / "runtime" / "test_env" / "nightly_current"
    _write_bytes(old_case / "artifact.json", 128)
    _write_bytes(keep_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    _set_old_mtime(keep_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google", "nightly"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert report["status"] == "applied"
    assert report["summary"]["removed_count"] == 1
    assert not old_case.exists()
    assert keep_case.exists()
    assert "Runtime Asset Prune Plan" in render_runtime_asset_prune_plan_markdown(plan)
    assert "Runtime Asset Prune Apply Report" in render_runtime_asset_prune_apply_markdown(report)


def test_runtime_prune_plan_from_cold_bundle_manifest_uses_exact_archive_scope(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    bundled_case = workspace / "runtime" / "test_env" / "nightly_rerun_bundled"
    unrelated_case = workspace / "runtime" / "test_env" / "nightly_rerun_unrelated"
    _write_bytes(bundled_case / "artifact.json", 128)
    _write_bytes(unrelated_case / "artifact.json", 256)
    _set_old_mtime(bundled_case)
    _set_old_mtime(unrelated_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["nightly"],
        min_size_bytes=1,
    )
    bundled_item = next(
        item for item in audit["directories"] if item["path"] == "runtime/test_env/nightly_rerun_bundled"
    )
    bundle_manifest = _cold_bundle_manifest_for_items([bundled_item])
    bundle_path = workspace / "runtime" / "asset_governance" / "bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_text(prune_module.dumps_report(bundle_manifest), encoding="utf-8")

    plan = build_runtime_asset_prune_plan_from_cold_bundle_manifest(
        bundle_manifest=bundle_manifest,
        workspace_root=workspace,
        cold_copy_manifest=str(bundle_path.relative_to(workspace)),
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
    )

    assert plan["status"] == "ready_for_review"
    assert plan["summary"]["selected_count"] == 1
    assert [operation["path"] for operation in plan["operations"]] == ["runtime/test_env/nightly_rerun_bundled"]
    assert unrelated_case.exists()


def test_runtime_prune_apply_requires_review_artifact_to_cover_exact_bundle_and_plan_scope(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    bundled_case = workspace / "runtime" / "test_env" / "nightly_rerun_bundled"
    _write_bytes(bundled_case / "artifact.json", 128)
    _set_old_mtime(bundled_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["nightly"],
        min_size_bytes=1,
    )
    bundled_item = next(
        item for item in audit["directories"] if item["path"] == "runtime/test_env/nightly_rerun_bundled"
    )
    bundle_manifest = _cold_bundle_manifest_for_items([bundled_item])
    bundle_path = workspace / "runtime" / "asset_governance" / "bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_text(prune_module.dumps_report(bundle_manifest), encoding="utf-8")
    review_plan_artifact = "runtime/asset_governance/prune_plan.json"
    plan = build_runtime_asset_prune_plan_from_cold_bundle_manifest(
        bundle_manifest=bundle_manifest,
        workspace_root=workspace,
        cold_copy_manifest=str(bundle_path.relative_to(workspace)),
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        review_plan_artifact=review_plan_artifact,
    )
    _write_prune_plan_artifact(workspace, plan, review_plan_artifact)

    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert any(blocker.startswith("review_artifact_missing_artifact_scope:") for blocker in report["blockers"])
    assert any(blocker.startswith("review_artifact_missing_artifact_sha:") for blocker in report["blockers"])
    assert bundled_case.exists()


def test_runtime_prune_apply_derives_bundle_review_tokens_even_if_plan_omits_them(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    bundled_case = workspace / "runtime" / "test_env" / "nightly_rerun_bundled"
    _write_bytes(bundled_case / "artifact.json", 128)
    _set_old_mtime(bundled_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["nightly"],
        min_size_bytes=1,
    )
    bundled_item = next(
        item for item in audit["directories"] if item["path"] == "runtime/test_env/nightly_rerun_bundled"
    )
    bundle_manifest = _cold_bundle_manifest_for_items([bundled_item])
    bundle_path = workspace / "runtime" / "asset_governance" / "bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_text(prune_module.dumps_report(bundle_manifest), encoding="utf-8")
    review_plan_artifact = "runtime/asset_governance/prune_plan.json"
    plan = build_runtime_asset_prune_plan_from_cold_bundle_manifest(
        bundle_manifest=bundle_manifest,
        workspace_root=workspace,
        cold_copy_manifest=str(bundle_path.relative_to(workspace)),
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        review_plan_artifact=review_plan_artifact,
    )
    evidence = dict(plan["destructive_review_evidence"])
    evidence["review_required_tokens"] = []
    plan["destructive_review_evidence"] = evidence
    evidence["review_artifact"] = _write_scope_review_artifact(workspace, plan)
    plan["destructive_review_evidence"] = evidence
    _write_prune_plan_artifact(workspace, plan, review_plan_artifact)

    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert any(
        blocker.startswith("review_artifact_missing_token:runtime_asset_supersession_cold_bundle_manifest_v1")
        for blocker in report["blockers"]
    )
    assert any(
        blocker.startswith("review_artifact_missing_token:source_bundle_manifest_sha256=")
        for blocker in report["blockers"]
    )
    assert any(
        blocker.startswith("review_artifact_missing_token:review_plan_scope_digest_sha256=")
        for blocker in report["blockers"]
    )
    assert bundled_case.exists()


def test_runtime_prune_plan_from_cold_bundle_rejects_archive_count_mismatch(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    bundled_case = workspace / "runtime" / "test_env" / "nightly_rerun_bundled"
    extra_case = workspace / "runtime" / "test_env" / "nightly_rerun_extra"
    _write_bytes(bundled_case / "artifact.json", 128)
    _write_bytes(extra_case / "artifact.json", 128)
    _set_old_mtime(bundled_case)
    _set_old_mtime(extra_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["nightly"],
        min_size_bytes=1,
    )
    items = {item["path"]: item for item in audit["directories"]}
    bundle_manifest = _cold_bundle_manifest_for_items(
        [items["runtime/test_env/nightly_rerun_bundled"], items["runtime/test_env/nightly_rerun_extra"]]
    )
    bundle_manifest["summary"]["archive_count"] = 1
    bundle_path = workspace / "runtime" / "asset_governance" / "bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_text(prune_module.dumps_report(bundle_manifest), encoding="utf-8")

    plan = build_runtime_asset_prune_plan_from_cold_bundle_manifest(
        bundle_manifest=bundle_manifest,
        workspace_root=workspace,
        cold_copy_manifest=str(bundle_path.relative_to(workspace)),
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
    )

    assert plan["status"] == "blocked_invalid_cold_bundle_manifest"
    assert plan["summary"]["selected_count"] == 0
    assert {item["reason"] for item in plan["skipped"]} == {"invalid_cold_bundle_manifest_contract"}


def test_runtime_prune_apply_blocks_mutated_cold_bundle_manifest_after_plan_generation(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "nightly_rerun_bundled"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["nightly"],
        min_size_bytes=1,
    )
    item = next(item for item in audit["directories"] if item["path"] == "runtime/test_env/nightly_rerun_bundled")
    bundle_manifest = _cold_bundle_manifest_for_items([item])
    bundle_path = workspace / "runtime" / "asset_governance" / "bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_text(prune_module.dumps_report(bundle_manifest), encoding="utf-8")
    plan = build_runtime_asset_prune_plan_from_cold_bundle_manifest(
        bundle_manifest=bundle_manifest,
        workspace_root=workspace,
        cold_copy_manifest=str(bundle_path.relative_to(workspace)),
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
    )
    bundle_manifest["archives"][0]["archive_sha256"] = "f" * 64
    bundle_path.write_text(prune_module.dumps_report(bundle_manifest), encoding="utf-8")

    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert "source_bundle_manifest_sha256_mismatch" in report["blockers"]
    assert old_case.exists()


def test_runtime_prune_apply_requires_bundle_archive_path_set_to_match_plan(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    bundled_case = workspace / "runtime" / "test_env" / "nightly_rerun_bundled"
    extra_case = workspace / "runtime" / "test_env" / "nightly_rerun_extra"
    _write_bytes(bundled_case / "artifact.json", 128)
    _write_bytes(extra_case / "artifact.json", 128)
    _set_old_mtime(bundled_case)
    _set_old_mtime(extra_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["nightly"],
        min_size_bytes=1,
    )
    items = {item["path"]: item for item in audit["directories"]}
    original_manifest = _cold_bundle_manifest_for_items([items["runtime/test_env/nightly_rerun_bundled"]])
    bundle_path = workspace / "runtime" / "asset_governance" / "bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_text(prune_module.dumps_report(original_manifest), encoding="utf-8")
    plan = build_runtime_asset_prune_plan_from_cold_bundle_manifest(
        bundle_manifest=original_manifest,
        workspace_root=workspace,
        cold_copy_manifest=str(bundle_path.relative_to(workspace)),
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
    )
    expanded_manifest = _cold_bundle_manifest_for_items(
        [items["runtime/test_env/nightly_rerun_bundled"], items["runtime/test_env/nightly_rerun_extra"]]
    )
    bundle_path.write_text(prune_module.dumps_report(expanded_manifest), encoding="utf-8")
    plan["source_bundle_manifest_sha256"] = prune_module._file_sha256(bundle_path)

    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert "cold_bundle_plan_archive_path_set_mismatch" in report["blockers"]
    assert bundled_case.exists()
    assert extra_case.exists()


def test_runtime_prune_apply_rejects_generic_plan_with_supersession_bundle_manifest(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "nightly_rerun_bundled"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["nightly"],
        min_size_bytes=1,
    )
    item = next(item for item in audit["directories"] if item["path"] == "runtime/test_env/nightly_rerun_bundled")
    bundle_manifest = _cold_bundle_manifest_for_items([item])
    bundle_path = workspace / "runtime" / "asset_governance" / "bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_text(prune_module.dumps_report(bundle_manifest), encoding="utf-8")
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        cold_copy_manifest=str(bundle_path.relative_to(workspace)),
    )

    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert "missing_source_bundle_plan_binding" in report["blockers"]
    assert old_case.exists()


def test_runtime_prune_apply_rejects_bundle_payload_without_archives_field(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "nightly_rerun_bundled"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["nightly"],
        min_size_bytes=1,
    )
    item = next(item for item in audit["directories"] if item["path"] == "runtime/test_env/nightly_rerun_bundled")
    malformed_bundle = _cold_bundle_manifest_for_items([item])
    malformed_bundle["operations"] = malformed_bundle.pop("archives")
    bundle_path = workspace / "runtime" / "asset_governance" / "bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_text(prune_module.dumps_report(malformed_bundle), encoding="utf-8")
    plan = {
        "contract_version": "runtime_asset_retention_prune_v1",
        "status": "ready_for_review",
        "workspace_root": str(workspace),
        "source_bundle_contract_version": "runtime_asset_supersession_cold_bundle_manifest_v1",
        "source_bundle_manifest_path": str(bundle_path.relative_to(workspace)),
        "source_bundle_manifest_sha256": prune_module._file_sha256(bundle_path),
        "destructive_review_evidence": {
            "review_artifact": _review_artifact(workspace),
            "review_title": "runtime asset prune test",
            "review_required_files": ["src/sourcing_agent/runtime_asset_retention_prune.py"],
            "accepted_retention_exception": "",
            "cold_copy_manifest": str(bundle_path.relative_to(workspace)),
            "reuse_index_effect": "none_runtime_output_only",
        },
        "operations": [
            {
                "action": "remove_local_runtime_copy",
                "path": item["path"],
                "size_bytes": item["size_bytes"],
                "file_count": item["file_count"],
                "directory_count": item["directory_count"],
                "latest_mtime": item["latest_mtime"],
                "retention_class": item["retention_class"],
                "matched_markers": item["matched_markers"],
                "source_scan_root": item["scan_root"],
            }
        ],
    }

    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert "cold_bundle_missing_archives" in report["blockers"]
    assert old_case.exists()


def test_runtime_prune_apply_requires_ready_plan_status(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "google_manual_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )
    plan["status"] = "blocked_invalid_cold_bundle_manifest"

    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert "plan_not_ready_for_review" in report["blockers"]
    assert old_case.exists()


def test_runtime_prune_blocks_parent_traversal_and_allowed_roots(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    (workspace / "output" / "case").mkdir(parents=True)
    plan = {
        "contract_version": "runtime_asset_retention_prune_v1",
        "status": "ready_for_review",
        "workspace_root": str(workspace),
        "destructive_review_evidence": {
            "review_artifact": _review_artifact(workspace),
            "review_title": "runtime asset prune test",
            "review_required_files": ["src/sourcing_agent/runtime_asset_retention_prune.py"],
            "accepted_retention_exception": _accepted_exception(),
            "reuse_index_effect": "none_runtime_output_only",
        },
        "operations": [
            {"action": "remove_local_runtime_copy", "path": "output/..", "size_bytes": 0},
            {"action": "remove_local_runtime_copy", "path": "runtime/test_env/..", "size_bytes": 0},
            {"action": "remove_local_runtime_copy", "path": "output", "size_bytes": 0},
            {"action": "remove_local_runtime_copy", "path": "runtime/test_env", "size_bytes": 0},
        ],
    }

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert report["status"] == "blocked"
    assert "operation_preflight_failed" in report["blockers"]
    assert report["summary"]["removed_count"] == 0
    reasons = {item["path"]: item["reason"] for item in report["results"]}
    assert reasons["output/.."] == "unsafe_path_component"
    assert reasons["runtime/test_env/.."] == "unsafe_path_component"
    assert reasons["output"] == "allowed_root_delete_not_allowed"
    assert reasons["runtime/test_env"] == "allowed_root_delete_not_allowed"
    assert workspace.exists()


def test_runtime_prune_blocks_wrong_action_and_stale_manifest(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "google_manual_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )
    wrong_action = copy.deepcopy(plan)
    wrong_action["operations"][0]["action"] = "delete_anything"

    blocked_action = apply_runtime_asset_prune_plan(
        plan=wrong_action,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert blocked_action["status"] == "blocked"
    assert "operation_preflight_failed" in blocked_action["blockers"]
    assert blocked_action["summary"]["removed_count"] == 0
    assert blocked_action["results"][0]["reason"] == "invalid_action"
    assert old_case.exists()

    _write_bytes(old_case / "new_artifact.json", 64)
    stale = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert stale["status"] == "blocked"
    assert "operation_preflight_failed" in stale["blockers"]
    assert stale["summary"]["removed_count"] == 0
    assert stale["results"][0]["reason"] in {
        "size_drift_detected",
        "file_count_drift_detected",
        "latest_mtime_drift_detected",
    }
    assert old_case.exists()


def test_runtime_prune_apply_preflight_blocks_all_deletes_when_any_operation_fails(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    valid_case = workspace / "runtime" / "test_env" / "google_manual_valid_case"
    stale_case = workspace / "runtime" / "test_env" / "google_manual_stale_case"
    _write_bytes(valid_case / "artifact.json", 128)
    _write_bytes(stale_case / "artifact.json", 128)
    _set_old_mtime(valid_case)
    _set_old_mtime(stale_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        allowed_retention_classes=["review_cold_archive_candidate"],
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        accepted_retention_exception=_accepted_exception(),
    )
    assert {operation["path"] for operation in plan["operations"]} == {
        "runtime/test_env/google_manual_stale_case",
        "runtime/test_env/google_manual_valid_case",
    }
    _write_bytes(stale_case / "new_artifact.json", 64)

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert report["status"] == "blocked"
    assert "operation_preflight_failed" in report["blockers"]
    assert report["summary"]["removed_count"] == 0
    assert valid_case.exists()
    assert stale_case.exists()


def test_runtime_prune_apply_blocks_missing_target_without_deleting_siblings(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    valid_case = workspace / "runtime" / "test_env" / "google_manual_valid_case"
    missing_case = workspace / "runtime" / "test_env" / "google_manual_missing_case"
    _write_bytes(valid_case / "artifact.json", 128)
    _write_bytes(missing_case / "artifact.json", 128)
    _set_old_mtime(valid_case)
    _set_old_mtime(missing_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        allowed_retention_classes=["review_cold_archive_candidate"],
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        accepted_retention_exception=_accepted_exception(),
    )
    for operation in plan["operations"]:
        if operation["path"] == "runtime/test_env/google_manual_missing_case":
            missing_operation = operation
            break
    else:
        raise AssertionError("missing case was not selected")
    shutil_target = missing_case
    for child in sorted(shutil_target.rglob("*"), reverse=True):
        child.unlink()
    shutil_target.rmdir()

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert missing_operation["path"] in {item["path"] for item in report["results"]}
    assert report["status"] == "blocked"
    assert "operation_preflight_failed" in report["blockers"]
    assert report["summary"]["removed_count"] == 0
    reasons = {item["path"]: item["reason"] for item in report["results"]}
    assert reasons["runtime/test_env/google_manual_missing_case"] == "target_missing"
    assert valid_case.exists()


def test_runtime_prune_apply_blocks_same_size_older_mtime_drift(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "google_manual_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case, days=20)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        allowed_retention_classes=["review_cold_archive_candidate"],
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        accepted_retention_exception=_accepted_exception(),
    )
    _set_old_mtime(old_case, days=40)

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert report["status"] == "blocked"
    assert "operation_preflight_failed" in report["blockers"]
    assert report["results"][0]["reason"] == "latest_mtime_drift_detected"
    assert old_case.exists()


def test_runtime_prune_apply_requires_go_review_and_exception_or_cold_copy(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(audit_report=audit, workspace_root=workspace, min_age_days=10)

    blocked = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert blocked["status"] == "blocked"
    assert "missing_review_artifact" in blocked["blockers"]
    assert "missing_cold_copy_or_accepted_retention_exception" in blocked["blockers"]
    assert old_case.exists()


def test_runtime_prune_apply_rejects_no_cold_copy_for_signoff_or_phase_artifacts(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google", "w6"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        accepted_retention_exception=_accepted_exception(),
    )

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert report["status"] == "blocked"
    assert "missing_cold_copy_for_retention_classes:review_signoff_or_pressure_run_artifact" in report["blockers"]
    assert old_case.exists()


def test_runtime_prune_apply_rejects_missing_and_incomplete_cold_copy_manifest(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google", "w6"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        cold_copy_manifest="runtime/asset_governance/missing_manifest.json",
    )

    missing = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert missing["status"] == "blocked"
    assert "cold_copy_manifest_missing" in missing["blockers"]
    assert old_case.exists()

    incomplete_plan = copy.deepcopy(plan)
    incomplete_plan["destructive_review_evidence"]["cold_copy_manifest"] = _write_cold_copy_manifest(
        workspace,
        incomplete_plan["operations"],
        omit_first=True,
    )
    incomplete = apply_runtime_asset_prune_plan(
        plan=incomplete_plan, workspace_root=workspace, apply=True, reviewed=True
    )

    assert incomplete["status"] == "blocked"
    assert "cold_copy_manifest_missing_operations:1" in incomplete["blockers"]
    assert old_case.exists()


def test_runtime_prune_apply_rejects_privileged_artifact_with_generic_cold_copy_manifest(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google", "w6"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        review_artifact=_review_artifact(workspace),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
    )
    plan["destructive_review_evidence"]["cold_copy_manifest"] = _write_cold_copy_manifest(workspace, plan["operations"])

    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert (
        "missing_supersession_cold_bundle_for_retention_classes:review_signoff_or_pressure_run_artifact"
        in report["blockers"]
    )
    assert old_case.exists()


def test_runtime_prune_apply_rejects_prompt_no_go_and_unrelated_review_artifacts(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    prompt_path = workspace / "runtime" / "reviews" / "retention.prompt.md"
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text("## Review Metadata\n\nVerdict: GO\n", encoding="utf-8")
    prompt_plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        review_artifact=str(prompt_path.relative_to(workspace)),
        accepted_retention_exception=_accepted_exception(),
    )

    prompt_report = apply_runtime_asset_prune_plan(
        plan=prompt_plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert prompt_report["status"] == "blocked"
    assert "review_artifact_is_prompt" in prompt_report["blockers"]
    assert old_case.exists()

    no_go_path = workspace / "runtime" / "reviews" / "retention_no_go.md"
    no_go_path.write_text(
        "## Review Metadata\n\n"
        "- title: runtime asset prune test\n"
        "- reviewer_model: gpt-5.5\n"
        "- reviewer_reasoning_effort: xhigh\n"
        "- reviewer_service_tier: fast\n"
        "- prompt_path: `runtime/reviews/retention_no_go.prompt.md`\n"
        "- command: `codex exec --model gpt-5.5 -c 'service_tier=\"fast\"' -c 'model_reasoning_effort=\"xhigh\"'`\n"
        "- contract_docs_considered: `docs/INDEPENDENT_REVIEW_GATE.md`\n\n"
        "Reviewed scope:\n"
        "- `src/sourcing_agent/runtime_asset_retention_prune.py`\n\n"
        "## Reviewer Output\n\n"
        "Verdict: NO-GO\n",
        encoding="utf-8",
    )
    no_go_plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        review_artifact=str(no_go_path.relative_to(workspace)),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        accepted_retention_exception=_accepted_exception(),
    )

    no_go_report = apply_runtime_asset_prune_plan(
        plan=no_go_plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert no_go_report["status"] == "blocked"
    assert "review_artifact_not_go" in no_go_report["blockers"]
    assert old_case.exists()


def test_runtime_prune_apply_rejects_review_artifact_missing_runner_model_tier_metadata(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    stale_review = workspace / "runtime" / "reviews" / "retention_v8_style_review.md"
    stale_review.parent.mkdir(parents=True, exist_ok=True)
    stale_review.write_text(
        "## Review Metadata\n\n"
        "- title: runtime asset prune test\n"
        "- reviewer_model: gpt-5.5\n"
        "- timeout_seconds: 600\n"
        "- command: `codex exec --model gpt-5.5 -c 'model_reasoning_effort=\"xhigh\"'`\n"
        "- contract_docs_considered: `docs/INDEPENDENT_REVIEW_GATE.md`\n\n"
        "Reviewed scope:\n"
        "- `src/sourcing_agent/runtime_asset_retention_prune.py`\n\n"
        "## Reviewer Output\n\n"
        "docs/INDEPENDENT_REVIEW_GATE.md\n\n"
        "GO\n",
        encoding="utf-8",
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        review_artifact=str(stale_review.relative_to(workspace)),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        accepted_retention_exception=_accepted_exception(),
    )

    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert "review_artifact_missing_metadata:reviewer_reasoning_effort" in report["blockers"]
    assert "review_artifact_missing_metadata:reviewer_service_tier" in report["blockers"]
    assert "review_artifact_missing_metadata:prompt_path" in report["blockers"]
    assert old_case.exists()


def test_runtime_prune_verifier_rejects_old_self_reported_go_artifact(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    path = workspace / "runtime" / "reviews" / "old-self-reported-go.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "## Review Metadata\n\n"
        "- title: runtime asset prune test\n"
        "- reviewer_model: gpt-5.6-sol\n"
        "- reviewer_reasoning_effort: ultra\n"
        "- reviewer_service_tier: priority\n"
        "- prompt_path: `runtime/reviews/old.prompt.md`\n"
        "- command: `codex exec --strict-config --sandbox read-only`\n"
        "- contract_docs_considered: `docs/INDEPENDENT_REVIEW_GATE.md`\n\n"
        "Reviewed scope: `src/sourcing_agent/runtime_asset_retention_prune.py`\n\n"
        "## Reviewer Output\n\nGO\n",
        encoding="utf-8",
    )

    blockers = prune_module.validate_independent_review_artifact(
        artifact_path=path,
        workspace_root=workspace,
        expected_title="runtime asset prune test",
        required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
    )

    assert "review_artifact_missing_metadata:reviewer_exit_code" in blockers
    assert "review_artifact_missing_metadata:reviewer_effective_config_path" in blockers
    assert "review_artifact_effective_config_evidence_missing" in blockers


def test_runtime_prune_verifier_recomputes_effective_config_and_rollout_hashes(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    artifact_path = workspace / _review_artifact(workspace)
    metadata = prune_module.parse_independent_review_artifact_metadata(artifact_path.read_text(encoding="utf-8"))
    evidence_path = workspace / metadata["reviewer_effective_config_path"]
    rollout_path = workspace / metadata["reviewer_rollout_path"]

    rollout_path.write_bytes(rollout_path.read_bytes() + b"{}\n")
    rollout_blockers = prune_module.validate_independent_review_artifact(
        artifact_path=artifact_path,
        workspace_root=workspace,
        expected_title="runtime asset prune test",
        required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
    )
    assert "review_artifact_rollout_sha256_mismatch" in rollout_blockers

    evidence_path.write_bytes(evidence_path.read_bytes() + b"\n")
    evidence_blockers = prune_module.validate_independent_review_artifact(
        artifact_path=artifact_path,
        workspace_root=workspace,
        expected_title="runtime asset prune test",
        required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
    )
    assert "review_artifact_effective_config_sha256_mismatch" in evidence_blockers


def test_runtime_prune_apply_rejects_default_review_prompt_and_command_metadata(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    stale_review = workspace / "runtime" / "reviews" / "retention_default_prompt_command_review.md"
    stale_review.parent.mkdir(parents=True, exist_ok=True)
    stale_review.write_text(
        "## Review Metadata\n\n"
        "- title: runtime asset prune test\n"
        "- reviewer_model: gpt-5.5\n"
        "- reviewer_reasoning_effort: xhigh\n"
        "- reviewer_service_tier: fast\n"
        "- prompt_path: default\n"
        "- command: default\n"
        "- contract_docs_considered: `docs/INDEPENDENT_REVIEW_GATE.md`\n\n"
        "Reviewed scope:\n"
        "- `src/sourcing_agent/runtime_asset_retention_prune.py`\n\n"
        "## Reviewer Output\n\n"
        "docs/INDEPENDENT_REVIEW_GATE.md\n\n"
        "GO\n",
        encoding="utf-8",
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        review_artifact=str(stale_review.relative_to(workspace)),
        review_title="runtime asset prune test",
        review_required_files=["src/sourcing_agent/runtime_asset_retention_prune.py"],
        accepted_retention_exception=_accepted_exception(),
    )

    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert "review_artifact_missing_metadata:prompt_path" in report["blockers"]
    assert "review_artifact_missing_metadata:command" in report["blockers"]
    assert old_case.exists()


def test_runtime_prune_apply_requires_matching_review_title_and_scope(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        review_artifact=_review_artifact(workspace),
        accepted_retention_exception=_accepted_exception(),
    )

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert report["status"] == "blocked"
    assert "missing_review_title" in report["blockers"]
    assert "missing_review_required_files" in report["blockers"]
    assert old_case.exists()


def test_runtime_prune_apply_reclassifies_current_alias_from_actual_path(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    current_case = workspace / "output" / "nightly_current"
    _write_bytes(current_case / "report.json", 128)
    _set_old_mtime(current_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["output"],
        include_name_markers=["nightly"],
        min_size_bytes=1,
    )
    current_item = dict(audit["directories"][0])
    forged_operation = {
        "action": "remove_local_runtime_copy",
        "path": current_item["path"],
        "size_bytes": current_item["size_bytes"],
        "file_count": current_item["file_count"],
        "directory_count": current_item["directory_count"],
        "latest_mtime": current_item["latest_mtime"],
        "retention_class": "review_cold_archive_candidate",
        "matched_markers": current_item["matched_markers"],
        "source_scan_root": current_item["scan_root"],
    }
    plan = {
        "contract_version": "runtime_asset_retention_prune_v1",
        "status": "ready_for_review",
        "workspace_root": str(workspace),
        "destructive_review_evidence": {
            **_review_kwargs(workspace),
            "reuse_index_effect": "none_runtime_output_only",
        },
        "operations": [forged_operation],
    }
    plan["destructive_review_evidence"]["cold_copy_manifest"] = _write_cold_copy_manifest(workspace, plan["operations"])

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert report["status"] == "blocked"
    assert "operation_preflight_failed" in report["blockers"]
    assert report["summary"]["removed_count"] == 0
    assert report["results"][0]["reason"] == "current_or_latest_alias"
    assert current_case.exists()


def test_runtime_prune_apply_blocks_current_alias_in_parent_path_component(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    current_case = workspace / "output" / "latest" / "google_manual_case"
    _write_bytes(current_case / "report.json", 128)
    _set_old_mtime(current_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["output"],
        include_name_markers=["google"],
        min_size_bytes=1,
        max_depth=3,
    )
    current_item = next(item for item in audit["directories"] if item["path"] == "output/latest/google_manual_case")
    forged_operation = {
        "action": "remove_local_runtime_copy",
        "path": current_item["path"],
        "size_bytes": current_item["size_bytes"],
        "file_count": current_item["file_count"],
        "directory_count": current_item["directory_count"],
        "latest_mtime": current_item["latest_mtime"],
        "retention_class": "review_cold_archive_candidate",
        "matched_markers": current_item["matched_markers"],
        "source_scan_root": current_item["scan_root"],
    }
    plan = {
        "contract_version": "runtime_asset_retention_prune_v1",
        "status": "ready_for_review",
        "workspace_root": str(workspace),
        "destructive_review_evidence": {
            **_review_kwargs(workspace),
            "reuse_index_effect": "none_runtime_output_only",
        },
        "operations": [forged_operation],
    }
    plan["destructive_review_evidence"]["cold_copy_manifest"] = _write_cold_copy_manifest(workspace, plan["operations"])

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert report["status"] == "blocked"
    assert "operation_preflight_failed" in report["blockers"]
    assert report["results"][0]["reason"] == "current_or_latest_alias"
    assert current_case.exists()


def test_runtime_prune_apply_blocks_target_and_parent_symlinks(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    protected_case = workspace / "runtime" / "test_env" / "company_assets" / "google" / "snapshot"
    target_symlink = workspace / "runtime" / "test_env" / "google_manual_link"
    parent_symlink = workspace / "runtime" / "test_env" / "google_parent_link"
    _write_bytes(protected_case / "artifact.json", 128)
    target_symlink.parent.mkdir(parents=True, exist_ok=True)
    target_symlink.symlink_to(protected_case)
    parent_symlink.symlink_to(protected_case.parent)
    _set_old_mtime(protected_case)
    plan = {
        "contract_version": "runtime_asset_retention_prune_v1",
        "status": "ready_for_review",
        "workspace_root": str(workspace),
        "destructive_review_evidence": {
            **_review_kwargs(workspace),
            "reuse_index_effect": "none_runtime_output_only",
        },
        "operations": [
            {
                "action": "remove_local_runtime_copy",
                "path": "runtime/test_env/google_manual_link",
                "source_scan_root": "runtime/test_env",
                "retention_class": "review_cold_archive_candidate",
                "size_bytes": 128,
                "file_count": 1,
                "directory_count": 0,
                "latest_mtime": datetime.now(timezone.utc).isoformat(),
            },
            {
                "action": "remove_local_runtime_copy",
                "path": "runtime/test_env/google_parent_link/snapshot",
                "source_scan_root": "runtime/test_env",
                "retention_class": "review_cold_archive_candidate",
                "size_bytes": 128,
                "file_count": 1,
                "directory_count": 0,
                "latest_mtime": datetime.now(timezone.utc).isoformat(),
            },
        ],
    }

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert report["status"] == "blocked"
    assert "operation_preflight_failed" in report["blockers"]
    reasons = {item["path"]: item["reason"] for item in report["results"]}
    assert reasons["runtime/test_env/google_manual_link"] == "target_symlink_not_allowed"
    assert reasons["runtime/test_env/google_parent_link/snapshot"] == "symlink_path_component_not_allowed"
    assert protected_case.exists()


def test_runtime_prune_apply_blocks_forged_cold_label_for_signoff_like_path(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    smoke_case = workspace / "runtime" / "test_env" / "smoke_gpt55_openai_agent_20260505"
    _write_bytes(smoke_case / "artifact.json", 128)
    _set_old_mtime(smoke_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["smoke"],
        min_size_bytes=1,
    )
    item = dict(audit["directories"][0])
    forged_operation = {
        "action": "remove_local_runtime_copy",
        "path": item["path"],
        "size_bytes": item["size_bytes"],
        "file_count": item["file_count"],
        "directory_count": item["directory_count"],
        "latest_mtime": item["latest_mtime"],
        "retention_class": "review_cold_archive_candidate",
        "matched_markers": item["matched_markers"],
        "source_scan_root": item["scan_root"],
    }
    plan = {
        "contract_version": "runtime_asset_retention_prune_v1",
        "status": "ready_for_review",
        "workspace_root": str(workspace),
        "destructive_review_evidence": {
            **_review_kwargs(workspace),
            "reuse_index_effect": "none_runtime_output_only",
        },
        "operations": [forged_operation],
    }
    plan["destructive_review_evidence"]["cold_copy_manifest"] = _write_cold_copy_manifest(workspace, plan["operations"])

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
    )

    assert report["status"] == "blocked"
    assert "operation_preflight_failed" in report["blockers"]
    assert report["results"][0]["reason"] == "retention_class_path_mismatch"
    assert smoke_case.exists()


def test_runtime_prune_apply_blocks_workspace_root_mismatch(tmp_path: Path) -> None:
    source_workspace = tmp_path / "source"
    target_workspace = tmp_path / "target"
    old_case = source_workspace / "runtime" / "test_env" / "google_manual_old_case"
    target_case = target_workspace / "runtime" / "test_env" / "google_manual_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _write_bytes(target_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    _set_old_mtime(target_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=source_workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=source_workspace,
        min_age_days=10,
        **_review_kwargs(source_workspace),
    )

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=target_workspace,
        apply=True,
        reviewed=True,
    )

    assert report["status"] == "blocked"
    assert "workspace_root_mismatch" in report["blockers"]
    assert target_case.exists()


def test_runtime_prune_cli_apply_returns_nonzero_without_strict_on_blocked(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "google_manual_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    plan = {
        "contract_version": "runtime_asset_retention_prune_v1",
        "status": "ready_for_review",
        "workspace_root": str(workspace),
        "destructive_review_evidence": {
            "review_artifact": _review_artifact(workspace),
            "review_title": "runtime asset prune test",
            "review_required_files": ["src/sourcing_agent/runtime_asset_retention_prune.py"],
            "accepted_retention_exception": _accepted_exception(),
            "reuse_index_effect": "none_runtime_output_only",
        },
        "operations": [
            {
                "action": "delete_anything",
                "path": "runtime/test_env/google_manual_old_case",
                "source_scan_root": "runtime/test_env",
                "retention_class": "review_cold_archive_candidate",
                "size_bytes": 128,
                "file_count": 1,
                "directory_count": 0,
                "latest_mtime": datetime.now(timezone.utc).isoformat(),
            }
        ],
    }
    plan_path = workspace / "plan.json"
    plan_path.write_text(prune_module.dumps_report(plan), encoding="utf-8")
    output_json = workspace / "apply.json"
    output_md = workspace / "apply.md"
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "apply_runtime_asset_retention_prune.py"

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--workspace-root",
            str(workspace),
            "--plan-json",
            str(plan_path),
            "--apply",
            "--reviewed",
            "--output-json",
            str(output_json),
            "--output-md",
            str(output_md),
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    assert completed.returncode == 1
    assert json.loads(output_json.read_text(encoding="utf-8"))["status"] == "blocked"
    assert "Status: `blocked`" in output_md.read_text(encoding="utf-8")
    assert old_case.exists()


def test_runtime_prune_apply_blocks_when_process_check_command_fails(tmp_path: Path, monkeypatch) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )

    def fake_run(*args, **kwargs):  # noqa: ANN002, ANN003
        return subprocess.CompletedProcess(args=args, returncode=2, stdout="", stderr="ps failed")

    monkeypatch.setattr(prune_module.subprocess, "run", fake_run)
    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert "active_runtime_processes_detected" in report["blockers"]
    assert report["active_processes"][0]["command"] == "process_check_failed: returncode=2"
    assert old_case.exists()


def test_runtime_prune_apply_detects_runtime_daemon_token_without_path_or_pid_reference(
    tmp_path: Path, monkeypatch
) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )

    def fake_run(*args, **kwargs):  # noqa: ANN002, ANN003
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout="4242 /opt/homebrew/bin/python -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 2\n",
            stderr="",
        )

    monkeypatch.setattr(prune_module.subprocess, "run", fake_run)
    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert "active_runtime_processes_detected" in report["blockers"]
    assert "run-worker-daemon-service" in report["active_processes"][0]["command"]
    assert old_case.exists()


def test_runtime_prune_apply_detects_runtime_daemon_from_pid_file_without_path_in_command(
    tmp_path: Path, monkeypatch
) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    pid_file = workspace / "runtime" / "test_env" / "service_logs" / "dev-worker-daemon.pid"
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text("4242\n", encoding="utf-8")
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )

    def fake_run(*args, **kwargs):  # noqa: ANN002, ANN003
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout="4242 /Users/changyuyi/.venv/bin/python -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 2\n",
            stderr="",
        )

    monkeypatch.setattr(prune_module.subprocess, "run", fake_run)
    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert "active_runtime_processes_detected" in report["blockers"]
    assert (
        "runtime process reference: runtime/test_env/service_logs/dev-worker-daemon.pid"
        in report["active_processes"][0]["command"]
    )
    assert old_case.exists()


def test_runtime_prune_apply_detects_nested_runtime_status_reference_without_path_in_command(
    tmp_path: Path, monkeypatch
) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    status_path = old_case / "scenario" / "services" / "job-recovery-abc" / "status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        prune_module.dumps_report(
            {
                "service_name": "job-recovery-abc",
                "status": "running",
                "pid": 4242,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )

    def fake_run(*args, **kwargs):  # noqa: ANN002, ANN003
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout="4242 /usr/bin/python worker-loop-without-runtime-path\n",
            stderr="",
        )

    monkeypatch.setattr(prune_module.subprocess, "run", fake_run)
    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "blocked"
    assert "active_runtime_processes_detected" in report["blockers"]
    expected_reference = (
        "runtime process reference: runtime/test_env/w6_google_old_case/scenario/services/job-recovery-abc/status.json"
    )
    assert expected_reference in report["active_processes"][0]["command"]
    assert old_case.exists()


def test_runtime_prune_apply_skips_stale_reference_when_live_pid_is_unrecognized(tmp_path: Path, monkeypatch) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    status_path = old_case / "scenario" / "services" / "job-recovery-abc" / "status.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        prune_module.dumps_report(
            {
                "service_name": "job-recovery-abc",
                "status": "running",
                "pid": 4242,
                "updated_at": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
                "stale_after_seconds": 60,
            }
        ),
        encoding="utf-8",
    )
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )

    def fake_run(*args, **kwargs):  # noqa: ANN002, ANN003
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout="4242 /usr/bin/python worker-loop-without-runtime-path\n",
            stderr="",
        )

    monkeypatch.setattr(prune_module.subprocess, "run", fake_run)
    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    # The status reference is 2 days stale against a 60s heartbeat contract and
    # the live pid belongs to an unrecognizable command: pid reuse, not an
    # active service. A blanket pid-alive block here would let a dead service
    # block pruning of its own runtime directory forever. (This synthetic
    # fixture's plan is independently not ready for review, so the apply still
    # blocks on that — the point here is that the process gate no longer fires.)
    assert "active_runtime_processes_detected" not in report.get("blockers", [])
    assert report.get("active_processes") in ([], None)
    assert old_case.exists()


def test_runtime_prune_apply_ignores_stopped_service_status_pid_reuse(tmp_path: Path, monkeypatch) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    status_path = (
        workspace / "runtime" / "test_env" / "stopped_runtime" / "services" / "job-recovery-abc" / "status.json"
    )
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        prune_module.dumps_report(
            {
                "service_name": "job-recovery-abc",
                "status": "stopped",
                "pid": 4242,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "runtime_dir": str(status_path.parents[3]),
            }
        ),
        encoding="utf-8",
    )
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )

    def fake_run(*args, **kwargs):  # noqa: ANN002, ANN003
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout="4242 /Library/Input Methods/DoubaoIme.app/Contents/MacOS/DoubaoIme\n",
            stderr="",
        )

    monkeypatch.setattr(prune_module.subprocess, "run", fake_run)
    report = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)

    assert report["status"] == "applied"
    assert report["summary"]["removed_count"] == 1
    assert not old_case.exists()


def test_runtime_prune_apply_rejects_skip_process_check_bypass(tmp_path: Path) -> None:
    workspace = tmp_path / "repo"
    old_case = workspace / "runtime" / "test_env" / "w6_google_old_case"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    audit = build_runtime_asset_retention_report(
        workspace_root=workspace,
        scan_roots=["runtime/test_env"],
        include_name_markers=["google"],
        min_size_bytes=1,
    )
    plan = build_runtime_asset_prune_plan(
        audit_report=audit,
        workspace_root=workspace,
        min_age_days=10,
        **_review_kwargs(workspace),
    )

    report = apply_runtime_asset_prune_plan(
        plan=plan,
        workspace_root=workspace,
        apply=True,
        reviewed=True,
        skip_process_check=True,
    )

    assert report["status"] == "blocked"
    assert "process_check_bypass_not_allowed" in report["blockers"]
    assert old_case.exists()


def _ttl_workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "repo"
    (workspace / "runtime" / "test_env").mkdir(parents=True, exist_ok=True)
    return workspace


def test_ttl_local_rebuildable_plan_never_selects_protected_or_infra_entries(tmp_path: Path) -> None:
    workspace = _ttl_workspace(tmp_path)
    test_env = workspace / "runtime" / "test_env"
    run_case = test_env / "board_runtime_old_case_20260101"
    _write_bytes(run_case / "artifact.json", 256)
    infra_names = [
        "company_assets",
        "secrets",
        "object_store",
        "jobs",
        "job_locks",
        "services",
        "service_logs",
        "object_sync",
        "runtime_metrics",
        "provider_cache",
        "hot_cache_company_assets",
    ]
    for name in infra_names:
        _write_bytes(test_env / name / "state.json", 64)
    _write_bytes(test_env / "company_identity_registry.json", 64)
    _write_bytes(test_env / "bootstrap_summary.json", 64)
    _set_old_mtime(test_env)

    plan = prune_module.build_ttl_local_rebuildable_prune_plan(workspace_root=workspace, min_age_days=10)

    assert plan["policy"] == "ttl_local_rebuildable"
    assert plan["status"] == "ready_for_review"
    paths = {item["path"] for item in plan["operations"]}
    assert paths == {"runtime/test_env/board_runtime_old_case_20260101"}
    skipped = {item["path"]: item["reason"] for item in plan["skipped"]}
    for name in infra_names:
        assert skipped[f"runtime/test_env/{name}"] == "protected_name"
    assert skipped["runtime/test_env/company_identity_registry.json"] == "non_directory_entry_protected"
    assert skipped["runtime/test_env/bootstrap_summary.json"] == "non_directory_entry_protected"


def test_ttl_local_rebuildable_plan_refuses_root_outside_test_env(tmp_path: Path) -> None:
    workspace = _ttl_workspace(tmp_path)
    old_output = workspace / "output" / "old_export_20260101"
    _write_bytes(old_output / "report.json", 128)
    _set_old_mtime(old_output)

    for bad_root in ("output", "runtime", "runtime/test_env/../..", "/", "runtime/company_assets"):
        plan = prune_module.build_ttl_local_rebuildable_prune_plan(
            workspace_root=workspace,
            prune_root=bad_root,
            min_age_days=10,
        )
        assert plan["status"] == "blocked_prune_root_not_allowed"
        assert plan["operations"] == []


def test_ttl_local_rebuildable_plan_respects_min_age_days(tmp_path: Path) -> None:
    workspace = _ttl_workspace(tmp_path)
    old_case = workspace / "runtime" / "test_env" / "smoke_old_run_20260101"
    fresh_case = workspace / "runtime" / "test_env" / "smoke_fresh_run_20260610"
    _write_bytes(old_case / "artifact.json", 128)
    _write_bytes(fresh_case / "artifact.json", 128)
    _set_old_mtime(old_case, days=20)

    plan = prune_module.build_ttl_local_rebuildable_prune_plan(workspace_root=workspace, min_age_days=14)

    paths = {item["path"] for item in plan["operations"]}
    assert paths == {"runtime/test_env/smoke_old_run_20260101"}
    skipped = {item["path"]: item["reason"] for item in plan["skipped"]}
    assert skipped["runtime/test_env/smoke_fresh_run_20260610"] == "younger_than_min_age_days"


def test_ttl_local_rebuildable_apply_is_dry_run_by_default_without_review_artifact(tmp_path: Path) -> None:
    workspace = _ttl_workspace(tmp_path)
    old_case = workspace / "runtime" / "test_env" / "nightly_old_run_20260101"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    plan = prune_module.build_ttl_local_rebuildable_prune_plan(workspace_root=workspace, min_age_days=10)
    assert plan["destructive_review_evidence"]["review_artifact"] == ""

    dry_run = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace)
    assert dry_run["status"] == "dry_run_ready"
    assert {item["status"] for item in dry_run["results"]} == {"would_remove"}
    assert old_case.exists()

    blocked = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True)
    assert blocked["status"] == "blocked"
    assert "apply_requires_reviewed_flag" in blocked["blockers"]
    assert old_case.exists()

    applied = apply_runtime_asset_prune_plan(plan=plan, workspace_root=workspace, apply=True, reviewed=True)
    assert applied["status"] == "applied"
    assert not old_case.exists()


def test_ttl_local_rebuildable_apply_blocks_tampered_protected_or_offroot_operations(tmp_path: Path) -> None:
    workspace = _ttl_workspace(tmp_path)
    old_case = workspace / "runtime" / "test_env" / "nightly_old_run_20260101"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    plan = prune_module.build_ttl_local_rebuildable_prune_plan(workspace_root=workspace, min_age_days=10)

    tampered_protected = copy.deepcopy(plan)
    tampered_protected["operations"][0]["path"] = "runtime/test_env/provider_cache"
    blocked = apply_runtime_asset_prune_plan(
        plan=tampered_protected, workspace_root=workspace, apply=True, reviewed=True
    )
    assert blocked["status"] == "blocked"
    assert "ttl_policy_protected_name:runtime/test_env/provider_cache" in blocked["blockers"]

    tampered_nested = copy.deepcopy(plan)
    tampered_nested["operations"][0]["path"] = "runtime/test_env/nightly_old_run_20260101/artifact_dir"
    blocked_nested = apply_runtime_asset_prune_plan(
        plan=tampered_nested, workspace_root=workspace, apply=True, reviewed=True
    )
    assert blocked_nested["status"] == "blocked"
    assert any(
        blocker.startswith("ttl_policy_path_not_top_level_test_env_dir:") for blocker in blocked_nested["blockers"]
    )

    tampered_root = copy.deepcopy(plan)
    tampered_root["prune_root"] = "output"
    blocked_root = apply_runtime_asset_prune_plan(
        plan=tampered_root, workspace_root=workspace, apply=True, reviewed=True
    )
    assert blocked_root["status"] == "blocked"
    assert "ttl_policy_prune_root_not_allowed" in blocked_root["blockers"]

    tampered_age = copy.deepcopy(plan)
    tampered_age["min_age_days"] = 0
    blocked_age = apply_runtime_asset_prune_plan(plan=tampered_age, workspace_root=workspace, apply=True, reviewed=True)
    assert blocked_age["status"] == "blocked"
    assert "ttl_policy_min_age_days_too_low" in blocked_age["blockers"]

    assert old_case.exists()


def test_ttl_local_rebuildable_cli_dry_run_default_and_root_refusal(tmp_path: Path) -> None:
    workspace = _ttl_workspace(tmp_path)
    old_case = workspace / "runtime" / "test_env" / "closeout_old_run_20260101"
    _write_bytes(old_case / "artifact.json", 128)
    _set_old_mtime(old_case)
    script = Path(__file__).resolve().parents[1] / "scripts" / "apply_runtime_asset_retention_prune.py"
    output_json = workspace / "runtime" / "asset_governance" / "ttl_plan.json"

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--workspace-root",
            str(workspace),
            "--policy",
            "ttl-local-rebuildable",
            "--min-age-days",
            "10",
            "--output-json",
            str(output_json),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["policy"] == "ttl_local_rebuildable"
    assert payload["status"] == "ready_for_review"
    assert {item["path"] for item in payload["operations"]} == {"runtime/test_env/closeout_old_run_20260101"}
    assert old_case.exists()

    refused = subprocess.run(
        [
            sys.executable,
            str(script),
            "--workspace-root",
            str(workspace),
            "--policy",
            "ttl-local-rebuildable",
            "--prune-root",
            "output",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert refused.returncode != 0
    assert "only allows --prune-root runtime/test_env" in refused.stderr


def test_pid_file_processes_ignore_unrelated_pid_reuse(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    services = workspace / "runtime" / "test_env" / "old_run_20260101" / "services" / "server-runtime-watchdog"
    services.mkdir(parents=True)
    (services / "status.json").write_text(
        json.dumps(
            {
                "service_name": "server-runtime-watchdog",
                "status": "running",
                "pid": 4242,
                "updated_at": (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),
                "stale_after_seconds": 180,
            }
        ),
        encoding="utf-8",
    )
    # The recorded pid is alive but now belongs to an unrelated process (pid reuse).
    unrelated = {4242: "/opt/homebrew/opt/postgresql@16/bin/postgres -D /opt/homebrew/var/postgresql@16"}
    flagged = prune_module._active_runtime_pid_file_processes(
        root=workspace, process_commands=unrelated, operation_paths=["runtime/test_env/old_run_20260101"]
    )
    assert flagged == []


def test_pid_file_processes_still_flag_live_runtime_process(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    services = workspace / "runtime" / "test_env" / "old_run_20260101" / "services" / "worker-recovery-daemon"
    services.mkdir(parents=True)
    (services / "status.json").write_text(
        json.dumps({"service_name": "worker-recovery-daemon", "status": "running", "pid": 4243}),
        encoding="utf-8",
    )
    by_token = {4243: ".venv/bin/python -m sourcing_agent.worker_daemon --runtime-dir runtime"}
    flagged = prune_module._active_runtime_pid_file_processes(
        root=workspace, process_commands=by_token, operation_paths=["runtime/test_env/old_run_20260101"]
    )
    assert [item["pid"] for item in flagged] == [4243]

    by_workspace_path = {4243: f".venv/bin/python {workspace}/scripts/anything.py"}
    flagged_path = prune_module._active_runtime_pid_file_processes(
        root=workspace, process_commands=by_workspace_path, operation_paths=["runtime/test_env/old_run_20260101"]
    )
    assert [item["pid"] for item in flagged_path] == [4243]
