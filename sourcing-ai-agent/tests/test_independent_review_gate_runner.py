from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

from sourcing_agent.runtime_asset_retention_prune import (
    parse_independent_review_artifact_metadata,
    validate_independent_review_artifact,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = REPO_ROOT / "scripts" / "run_independent_review_gate.py"
THREAD_ID = "019f0000-0000-7000-8000-000000000099"
TURN_ID = "019f0000-0000-7000-8000-000000000100"
REAL_SUBPROCESS_RUN = subprocess.run


def _load_runner():
    spec = importlib.util.spec_from_file_location("independent_review_gate_runner_tests", RUNNER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _configured(runner, codex_home: Path):
    config_path = codex_home / "config.toml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        'model = "gpt-5.6-sol"\nmodel_reasoning_effort = "ultra"\nservice_tier = "fast"\n',
        encoding="utf-8",
    )
    return runner._load_reviewer_configuration(config_path)


def _write_rollout(codex_home: Path, events: list[dict[str, object]]) -> Path:
    path = codex_home / "sessions" / "2026" / "07" / "10" / f"rollout-test-{THREAD_ID}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
    return path


def _session_meta(*, source: str = "exec") -> dict[str, object]:
    return {
        "type": "session_meta",
        "payload": {"id": THREAD_ID, "cli_version": "0.144.0", "source": source},
    }


def _thread_started() -> str:
    return json.dumps({"type": "thread.started", "thread_id": THREAD_ID})


def _completed_exec_rollout(
    *,
    prompt: str,
    final_output: str,
    source: str = "exec",
    extra_events: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    normalized_final = final_output.rstrip("\r\n")
    return [
        _session_meta(source=source),
        {"type": "event_msg", "payload": {"type": "task_started", "turn_id": TURN_ID}},
        {
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": prompt}],
            },
        },
        {"type": "event_msg", "payload": {"type": "user_message", "message": prompt}},
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
        *(extra_events or []),
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
                "turn_id": TURN_ID,
                "last_agent_message": normalized_final,
            },
        },
    ]


def _init_review_repo(root: Path) -> str:
    (root / "docs").mkdir(parents=True)
    (root / "src").mkdir(parents=True)
    (root / "docs" / "INDEPENDENT_REVIEW_BRIEF.md").write_text("# Review brief\n", encoding="utf-8")
    (root / "src" / "example.py").write_text("value = 1\n", encoding="utf-8")
    REAL_SUBPROCESS_RUN(["git", "init", "-q"], cwd=root, check=True)
    REAL_SUBPROCESS_RUN(["git", "config", "user.name", "Test"], cwd=root, check=True)
    REAL_SUBPROCESS_RUN(["git", "config", "user.email", "test@example.com"], cwd=root, check=True)
    REAL_SUBPROCESS_RUN(["git", "add", "docs/INDEPENDENT_REVIEW_BRIEF.md", "src/example.py"], cwd=root, check=True)
    REAL_SUBPROCESS_RUN(["git", "commit", "-qm", "base"], cwd=root, check=True)
    base = REAL_SUBPROCESS_RUN(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()
    (root / "src" / "example.py").write_text("value = 2\n", encoding="utf-8")
    REAL_SUBPROCESS_RUN(["git", "add", "src/example.py"], cwd=root, check=True)
    REAL_SUBPROCESS_RUN(["git", "commit", "-qm", "reviewed"], cwd=root, check=True)
    return base


def test_effective_rollout_accepts_fast_priority_alias_across_real_event_shapes(tmp_path: Path) -> None:
    runner = _load_runner()
    codex_home = tmp_path / "codex-home"
    configured = _configured(runner, codex_home)
    rollout_path = _write_rollout(
        codex_home,
        [
            _session_meta(),
            {
                "type": "turn_context",
                "payload": {"model": "gpt-5.6-sol", "effort": "ultra"},
            },
            {
                "type": "event_msg",
                "payload": {
                    "type": "session_configured",
                    "model": "gpt-5.6-sol",
                    "model_reasoning_effort": "ultra",
                    "service_tier": "fast",
                },
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
        ],
    )

    effective = runner._load_effective_reviewer_configuration(
        events_text=_thread_started(),
        configured=configured,
        codex_home=codex_home,
    )

    assert effective.settings == runner.ReviewerSettings("gpt-5.6-sol", "ultra", "priority")
    assert effective.session_id == THREAD_ID
    assert effective.thread_id == THREAD_ID
    assert effective.codex_cli_version == "0.144.0"
    assert effective.rollout_path == rollout_path
    assert effective.source["service_tier"] == ["session_configured", "thread_settings_applied"]


def test_effective_rollout_rejects_missing_or_conflicting_metadata(tmp_path: Path) -> None:
    runner = _load_runner()
    codex_home = tmp_path / "codex-home"
    configured = _configured(runner, codex_home)
    _write_rollout(
        codex_home,
        [
            _session_meta(),
            {
                "type": "turn_context",
                "payload": {"model": "gpt-5.6-sol", "effort": "ultra"},
            },
        ],
    )

    with pytest.raises(RuntimeError, match="complete effective reviewer settings"):
        runner._load_effective_reviewer_configuration(
            events_text=_thread_started(),
            configured=configured,
            codex_home=codex_home,
        )

    _write_rollout(
        codex_home,
        [
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
            }
        ],
    )
    effective_without_session_metadata = runner._load_effective_reviewer_configuration(
        events_text=_thread_started(),
        configured=configured,
        codex_home=codex_home,
    )
    with pytest.raises(RuntimeError, match="Codex CLI version"):
        runner._effective_config_payload(
            configured=configured,
            effective=effective_without_session_metadata,
            reviewer_exit_code=0,
            scope={},
            prompt_path=tmp_path / "prompt.md",
            prompt_sha256="0" * 64,
            events_path=tmp_path / "events.jsonl",
            events_sha256="0" * 64,
            raw_output_path=tmp_path / "raw.md",
            raw_output_sha256="0" * 64,
            root=tmp_path,
        )

    _write_rollout(
        codex_home,
        [
            _session_meta(),
            {
                "type": "event_msg",
                "payload": {
                    "type": "session_configured",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "ultra",
                    "service_tier": "priority",
                },
            },
            {
                "type": "event_msg",
                "payload": {
                    "type": "thread_settings_applied",
                    "thread_settings": {
                        "model": "gpt-5.6-sol",
                        "reasoning_effort": "xhigh",
                        "service_tier": "priority",
                    },
                },
            },
        ],
    )

    with pytest.raises(RuntimeError, match="conflicting effective reasoning_effort"):
        runner._load_effective_reviewer_configuration(
            events_text=_thread_started(),
            configured=configured,
            codex_home=codex_home,
        )


@pytest.mark.parametrize(
    "reroute_payload,match",
    [
        ({"from_model": "gpt-5.6-sol"}, "ambiguous model_reroute"),
        (
            {"from_model": "gpt-5.6-sol", "to_model": "gpt-5.5"},
            "model_reroute; rerouted reviews fail closed",
        ),
    ],
)
def test_effective_rollout_rejects_model_reroute(
    tmp_path: Path,
    reroute_payload: dict[str, str],
    match: str,
) -> None:
    runner = _load_runner()
    codex_home = tmp_path / "codex-home"
    configured = _configured(runner, codex_home)
    _write_rollout(
        codex_home,
        [
            _session_meta(),
            {
                "type": "event_msg",
                "payload": {
                    "type": "session_configured",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "ultra",
                    "service_tier": "priority",
                },
            },
            {"type": "event_msg", "payload": {"type": "model_reroute", **reroute_payload}},
        ],
    )

    with pytest.raises(RuntimeError, match=match):
        runner._load_effective_reviewer_configuration(
            events_text=_thread_started(),
            configured=configured,
            codex_home=codex_home,
        )


def test_causal_binding_requires_one_complete_root_exec_turn_and_exact_messages() -> None:
    runner = _load_runner()
    from sourcing_agent import runtime_asset_retention_prune as verifier

    prompt = "exact pinned review prompt"
    final_output = "Reviewed exact prompt.\n\nGO\n"
    base_events = _completed_exec_rollout(prompt=prompt, final_output=final_output)

    def causal(events: list[dict[str, object]]):
        raw = "".join(json.dumps(event) + "\n" for event in events).encode("utf-8")
        runner_binding = runner._review_causal_binding(
            rollout_raw=raw,
            prompt_raw=prompt.encode("utf-8"),
            raw_output=final_output.encode("utf-8"),
            expected_thread_id=THREAD_ID,
        )
        verifier_binding, blockers = verifier._parse_independent_review_causal_binding(
            rollout_raw=raw,
            prompt_raw=prompt.encode("utf-8"),
            raw_output=final_output.encode("utf-8"),
            expected_thread_id=THREAD_ID,
        )
        assert verifier_binding == runner_binding
        return runner_binding, blockers

    valid_binding, valid_blockers = causal(base_events)
    assert runner._review_causal_binding_valid(valid_binding) is True
    assert valid_blockers == []

    scenarios = (
        ("non_exec", "session_source_exec", "review_artifact_rollout_causal_session_source_not_exec"),
        ("missing_user_response", "prompt_response_item_exact", "review_artifact_rollout_causal_prompt_response_item_mismatch"),
        ("missing_user_event", "prompt_event_message_exact", "review_artifact_rollout_causal_prompt_event_message_mismatch"),
        ("wrong_final_response", "final_response_item_exact", "review_artifact_rollout_causal_final_response_item_mismatch"),
        ("wrong_final_event", "final_event_message_exact", "review_artifact_rollout_causal_final_event_message_mismatch"),
        ("wrong_task_complete", "task_complete_final_exact", "review_artifact_rollout_causal_task_complete_mismatch"),
        ("duplicate_turn", "single_task_turn", "review_artifact_rollout_causal_turn_mismatch"),
        ("abort", "no_abort", "review_artifact_rollout_causal_abort_present"),
    )
    for scenario, failed_field, expected_blocker in scenarios:
        events = json.loads(json.dumps(base_events))
        if scenario == "non_exec":
            events[0]["payload"]["source"] = "vscode"
        elif scenario == "missing_user_response":
            events.pop(2)
        elif scenario == "missing_user_event":
            events.pop(3)
        elif scenario == "wrong_final_response":
            events[6]["payload"]["content"][0]["text"] = "different final"
        elif scenario == "wrong_final_event":
            events[5]["payload"]["message"] = "different final"
        elif scenario == "wrong_task_complete":
            events[7]["payload"]["last_agent_message"] = "different final"
        elif scenario == "duplicate_turn":
            events.insert(2, {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "other"}})
        else:
            events.insert(5, {"type": "event_msg", "payload": {"type": "turn_aborted", "turn_id": TURN_ID}})
        binding, blockers = causal(events)
        assert binding[failed_field] is False, scenario
        assert expected_blocker in blockers, scenario
        assert runner._review_causal_binding_valid(binding) is False, scenario


def test_signoff_runner_binds_pinned_scope_and_all_raw_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _load_runner()
    root = tmp_path / "repo"
    base = _init_review_repo(root)
    (root / "known-untracked.tmp").write_text("parallel work\n", encoding="utf-8")
    codex_home = tmp_path / "codex-home"
    _configured(runner, codex_home)
    _write_rollout(
        codex_home,
        [
            _session_meta(),
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
        ],
    )
    output_path = root / "runtime" / "reviews" / "valid.md"

    def fake_run(args, **kwargs):
        if args and args[0] == "git":
            return REAL_SUBPROCESS_RUN(args, **kwargs)
        prompt = kwargs["stdin"].read()
        final_output = "Reviewed pinned scope.\n\nGO\n"
        result_path = Path(args[args.index("--output-last-message") + 1])
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(final_output, encoding="utf-8")
        _write_rollout(codex_home, _completed_exec_rollout(prompt=prompt, final_output=final_output))
        return subprocess.CompletedProcess(args, 0, stdout=_thread_started() + "\n", stderr="")

    monkeypatch.setattr(runner, "_repo_root", lambda: root)
    monkeypatch.setattr(runner, "_codex_home", lambda: codex_home)
    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(RUNNER_PATH),
            "--execute",
            "--title",
            "  pinned   scope  ",
            "--base",
            base,
            "--files",
            "src/example.py src/example.py",
            "--extra-context",
            "contract context",
            "--output",
            str(output_path),
        ],
    )

    assert runner.main() == 0

    artifact_text = output_path.read_text(encoding="utf-8")
    metadata = parse_independent_review_artifact_metadata(artifact_text)
    evidence_path = root / metadata["reviewer_effective_config_path"]
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["contract_version"] == "independent_review_effective_config_v2"
    assert evidence["scope"]["title"] == "pinned scope"
    assert evidence["scope"]["scope_mode"] == "pinned_commit_diff"
    assert evidence["scope"]["files"] == ["src/example.py"]
    assert evidence["causal_binding"]["turn_id"] == TURN_ID
    assert all(
        value is True
        for key, value in evidence["causal_binding"].items()
        if key not in {"turn_id", "prompt_sha256", "normalized_final_output_sha256"}
    )
    assert metadata["review_scope_digest_sha256"] == evidence["scope"]["scope_digest_sha256"]
    assert (
        validate_independent_review_artifact(
            artifact_path=output_path,
            workspace_root=root,
            expected_title="pinned scope",
            required_files=["src/example.py"],
            expected_scope_digest=metadata["review_scope_digest_sha256"],
        )
        == []
    )

    replay_blockers = validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="pinned scope",
        required_files=["src/example.py"],
        expected_scope_digest="f" * 64,
    )
    assert "review_artifact_expected_scope_digest_mismatch" in replay_blockers
    new_scope_blockers = validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="pinned scope",
        required_files=["src/different.py"],
    )
    assert "review_artifact_scope_files_mismatch" in new_scope_blockers

    prompt_path = root / metadata["prompt_path"]
    original_prompt = prompt_path.read_bytes()
    prompt_path.write_bytes(original_prompt + b"tamper\n")
    assert "review_artifact_prompt_sha256_mismatch" in validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="pinned scope",
        required_files=["src/example.py"],
    )
    prompt_path.write_bytes(original_prompt)

    for metadata_field, blocker in (
        ("events_path", "review_artifact_events_sha256_mismatch"),
        ("raw_output_path", "review_artifact_raw_output_sha256_mismatch"),
    ):
        evidence_artifact_path = root / metadata[metadata_field]
        original_raw = evidence_artifact_path.read_bytes()
        evidence_artifact_path.write_bytes(original_raw + b"tamper\n")
        assert blocker in validate_independent_review_artifact(
            artifact_path=output_path,
            workspace_root=root,
            expected_title="pinned scope",
            required_files=["src/example.py"],
        )
        evidence_artifact_path.write_bytes(original_raw)

    original_artifact = output_path.read_bytes()
    original_evidence = evidence_path.read_bytes()
    rollout_path = root / metadata["reviewer_rollout_path"]
    original_rollout = rollout_path.read_bytes()
    forged_rollout = original_rollout.replace(b'"priority"', b'"standard"')
    rollout_path.write_bytes(forged_rollout)
    forged_evidence = json.loads(original_evidence)
    forged_evidence["session"]["rollout_sha256"] = hashlib.sha256(forged_rollout).hexdigest()
    forged_evidence_raw = (json.dumps(forged_evidence, indent=2, sort_keys=True) + "\n").encode("utf-8")
    evidence_path.write_bytes(forged_evidence_raw)
    forged_artifact = original_artifact.decode("utf-8").replace(
        metadata["reviewer_rollout_sha256"], hashlib.sha256(forged_rollout).hexdigest()
    )
    forged_artifact = forged_artifact.replace(
        metadata["reviewer_effective_config_sha256"], hashlib.sha256(forged_evidence_raw).hexdigest()
    )
    output_path.write_text(forged_artifact, encoding="utf-8")
    forged_blockers = validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="pinned scope",
        required_files=["src/example.py"],
    )
    assert "review_artifact_rollout_recomputed_mismatch:service_tier" in forged_blockers
    rollout_path.write_bytes(original_rollout)
    evidence_path.write_bytes(original_evidence)
    output_path.write_bytes(original_artifact)

    repacked_scope = runner.build_independent_review_scope_evidence(
        workspace_root=root,
        title="pinned scope",
        base_ref=base,
        files=["src/example.py"],
        extra_context="different repacked contract context",
    )
    repacked_prompt = runner._build_prompt(
        root=root,
        scope=repacked_scope,
        extra_context="different repacked contract context",
    ).encode("utf-8")
    repacked_prompt_sha = hashlib.sha256(repacked_prompt).hexdigest()
    prompt_path.write_bytes(repacked_prompt)
    repacked_evidence = json.loads(original_evidence)
    repacked_evidence["scope"] = repacked_scope
    repacked_evidence["artifacts"]["prompt"]["sha256"] = repacked_prompt_sha
    repacked_evidence["causal_binding"]["prompt_sha256"] = repacked_prompt_sha
    repacked_evidence_raw = (json.dumps(repacked_evidence, indent=2, sort_keys=True) + "\n").encode("utf-8")
    repacked_evidence_sha = hashlib.sha256(repacked_evidence_raw).hexdigest()
    evidence_path.write_bytes(repacked_evidence_raw)
    repacked_artifact = original_artifact.decode("utf-8")
    for old, new in (
        (metadata["review_extra_context_sha256"], str(repacked_scope["extra_context_sha256"])),
        (metadata["review_scope_digest_sha256"], str(repacked_scope["scope_digest_sha256"])),
        (metadata["prompt_sha256"], repacked_prompt_sha),
        (metadata["reviewer_effective_config_sha256"], repacked_evidence_sha),
    ):
        repacked_artifact = repacked_artifact.replace(old, new)
    output_path.write_text(repacked_artifact, encoding="utf-8")

    repack_blockers = validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="pinned scope",
        required_files=["src/example.py"],
        expected_scope_digest=str(repacked_scope["scope_digest_sha256"]),
    )
    assert "review_artifact_rollout_causal_prompt_response_item_mismatch" in repack_blockers
    assert "review_artifact_rollout_causal_prompt_event_message_mismatch" in repack_blockers
    assert "review_artifact_causal_binding_recomputed_mismatch:prompt_response_item_exact" in repack_blockers
    assert "review_artifact_causal_binding_recomputed_mismatch:prompt_event_message_exact" in repack_blockers
    assert "review_artifact_prompt_sha256_mismatch" not in repack_blockers
    assert "review_artifact_scope_digest_recomputed_mismatch" not in repack_blockers
    prompt_path.write_bytes(original_prompt)
    evidence_path.write_bytes(original_evidence)
    output_path.write_bytes(original_artifact)

    (root / "unrelated.tmp").write_text("untracked\n", encoding="utf-8")
    assert (
        validate_independent_review_artifact(
            artifact_path=output_path,
            workspace_root=root,
            expected_title="pinned scope",
            required_files=["src/example.py"],
            expected_scope_digest=metadata["review_scope_digest_sha256"],
        )
        == []
    )
    (root / "src" / "example.py").write_text("value = 3\n", encoding="utf-8")
    changed_scope_blockers = validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="pinned scope",
        required_files=["src/example.py"],
        expected_scope_digest=metadata["review_scope_digest_sha256"],
    )
    assert "review_artifact_scope_current_tree_mismatch" in changed_scope_blockers

    (root / "src" / "example.py").write_text("value = 2\n", encoding="utf-8")
    (root / "src" / "next.py").write_text("next_value = 1\n", encoding="utf-8")
    REAL_SUBPROCESS_RUN(["git", "add", "src/next.py"], cwd=root, check=True)
    REAL_SUBPROCESS_RUN(["git", "commit", "-qm", "next batch"], cwd=root, check=True)
    assert (
        validate_independent_review_artifact(
            artifact_path=output_path,
            workspace_root=root,
            expected_title="pinned scope",
            required_files=["src/example.py"],
            expected_scope_digest=metadata["review_scope_digest_sha256"],
        )
        == []
    )

    (root / "src" / "example.py").write_text("value = staged replacement\n", encoding="utf-8")
    REAL_SUBPROCESS_RUN(["git", "add", "src/example.py"], cwd=root, check=True)
    (root / "src" / "example.py").write_text("value = 2\n", encoding="utf-8")
    staged_worktree_cancellation_blockers = validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="pinned scope",
        required_files=["src/example.py"],
        expected_scope_digest=metadata["review_scope_digest_sha256"],
    )
    assert "review_artifact_scope_current_tree_mismatch" in staged_worktree_cancellation_blockers
    REAL_SUBPROCESS_RUN(["git", "add", "src/example.py"], cwd=root, check=True)

    (root / "src" / "example.py").write_text("value = 4\n", encoding="utf-8")
    REAL_SUBPROCESS_RUN(["git", "add", "src/example.py"], cwd=root, check=True)
    REAL_SUBPROCESS_RUN(["git", "commit", "-qm", "change reviewed scope"], cwd=root, check=True)
    committed_scope_blockers = validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="pinned scope",
        required_files=["src/example.py"],
        expected_scope_digest=metadata["review_scope_digest_sha256"],
    )
    assert "review_artifact_scope_current_tree_mismatch" in committed_scope_blockers

    (root / "src" / "example.py").write_text("value = 2\n", encoding="utf-8")
    dirty_revert_blockers = validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="pinned scope",
        required_files=["src/example.py"],
        expected_scope_digest=metadata["review_scope_digest_sha256"],
    )
    assert "review_artifact_scope_current_tree_mismatch" in dirty_revert_blockers

    REAL_SUBPROCESS_RUN(["git", "add", "src/example.py"], cwd=root, check=True)
    REAL_SUBPROCESS_RUN(["git", "commit", "-qm", "revert reviewed scope bytes"], cwd=root, check=True)
    committed_revert_blockers = validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="pinned scope",
        required_files=["src/example.py"],
        expected_scope_digest=metadata["review_scope_digest_sha256"],
    )
    assert "review_artifact_scope_current_tree_mismatch" in committed_revert_blockers


def test_missing_scoped_file_is_reference_only_and_cannot_sign_off(tmp_path: Path) -> None:
    from sourcing_agent import runtime_asset_retention_prune as review_evidence

    root = tmp_path / "repo"
    base = _init_review_repo(root)
    scope = review_evidence.build_independent_review_scope_evidence(
        workspace_root=root,
        title="missing scope file",
        base_ref=base,
        files=["src/does-not-exist.py"],
        extra_context="contract context",
    )

    assert scope["scope_mode"] == "reference_only_worktree"
    scope["scope_mode"] = "pinned_commit_diff"
    scope["scope_digest_sha256"] = review_evidence.independent_review_scope_digest(scope)
    blockers = review_evidence._independent_review_scope_blockers(
        scope=scope,
        metadata={"review_scope_digest_sha256": scope["scope_digest_sha256"]},
        root=root,
        expected_title="missing scope file",
        required_files=["src/does-not-exist.py"],
        expected_scope_digest=scope["scope_digest_sha256"],
    )
    assert "review_artifact_scope_missing_file:src/does-not-exist.py" in blockers


def test_deleted_file_is_reviewable_but_cannot_satisfy_a_required_file_gate(tmp_path: Path) -> None:
    from sourcing_agent import runtime_asset_retention_prune as review_evidence

    root = tmp_path / "repo"
    base = _init_review_repo(root)
    (root / "src" / "example.py").unlink()
    REAL_SUBPROCESS_RUN(["git", "add", "src/example.py"], cwd=root, check=True)
    REAL_SUBPROCESS_RUN(["git", "commit", "-qm", "delete reviewed file"], cwd=root, check=True)
    scope = review_evidence.build_independent_review_scope_evidence(
        workspace_root=root,
        title="intentional deletion",
        base_ref=base,
        files=["src/example.py"],
        extra_context="contract context",
    )

    assert scope["scope_mode"] == "pinned_commit_diff"
    blockers = review_evidence._independent_review_scope_blockers(
        scope=scope,
        metadata={"review_scope_digest_sha256": scope["scope_digest_sha256"]},
        root=root,
        expected_title="intentional deletion",
        required_files=["src/example.py"],
        expected_scope_digest=scope["scope_digest_sha256"],
    )
    assert "review_artifact_scope_required_file_missing_at_head:src/example.py" in blockers


def test_nonzero_codex_exit_forces_no_go_and_records_durable_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _load_runner()
    root = tmp_path / "repo"
    (root / "docs").mkdir(parents=True)
    (root / "docs" / "INDEPENDENT_REVIEW_BRIEF.md").write_text("# Review brief\n", encoding="utf-8")
    codex_home = tmp_path / "codex-home"
    _configured(runner, codex_home)
    _write_rollout(
        codex_home,
        [
            _session_meta(),
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
        ],
    )
    output_path = root / "runtime" / "reviews" / "nonzero.md"
    prompt_path = root / "runtime" / "reviews" / "nonzero.prompt.md"

    def fake_run(args, **kwargs):
        if args and args[0] == "git":
            return REAL_SUBPROCESS_RUN(args, **kwargs)
        prompt = kwargs["stdin"].read()
        final_output = "GO\n"
        result_path = Path(args[args.index("--output-last-message") + 1])
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(final_output, encoding="utf-8")
        _write_rollout(codex_home, _completed_exec_rollout(prompt=prompt, final_output=final_output))
        return subprocess.CompletedProcess(args, 17, stdout=_thread_started(), stderr="capacity")

    monkeypatch.setattr(runner, "_repo_root", lambda: root)
    monkeypatch.setattr(runner, "_codex_home", lambda: codex_home)
    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(RUNNER_PATH),
            "--execute",
            "--title",
            "nonzero",
            "--files",
            "src/example.py",
            "--output",
            str(output_path),
            "--prompt-output",
            str(prompt_path),
        ],
    )

    return_code = runner.main()

    artifact = output_path.read_text(encoding="utf-8")
    assert return_code == 17
    assert "- reviewer_exit_code: 17" in artifact
    assert "GO\n\nNO-GO: reviewer process exited with status 17" in artifact
    evidence_path = next((root / "runtime" / "reviews").glob("*_nonzero.effective-config.json"))
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["process"] == {"reviewer_exit_code": 17}
    assert set(evidence) == {
        "artifacts",
        "causal_binding",
        "config",
        "contract_version",
        "effective",
        "model_reroutes",
        "process",
        "scope",
        "session",
    }
    assert evidence["scope"]["scope_mode"] == "reference_only_worktree"
    assert evidence["artifacts"]["raw_output"]["sha256"] == hashlib.sha256(b"GO\n").hexdigest()
    blockers = validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="nonzero",
        required_files=["src/example.py"],
    )
    assert "review_artifact_reviewer_exit_not_zero" in blockers
    assert "review_artifact_effective_config_exit_not_zero" in blockers
    assert "review_artifact_scope_not_signoff_capable" in blockers
    assert "review_artifact_not_go" in blockers


def test_codex_timeout_is_recorded_as_invalid_no_go(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _load_runner()
    root = tmp_path / "repo"
    (root / "docs").mkdir(parents=True)
    (root / "docs" / "INDEPENDENT_REVIEW_BRIEF.md").write_text("# Review brief\n", encoding="utf-8")
    codex_home = tmp_path / "codex-home"
    _configured(runner, codex_home)
    output_path = root / "runtime" / "reviews" / "timeout.md"
    prompt_path = root / "runtime" / "reviews" / "timeout.prompt.md"

    def fake_run(args, **kwargs):
        if args and args[0] == "git":
            return REAL_SUBPROCESS_RUN(args, **kwargs)
        raise subprocess.TimeoutExpired(args, kwargs["timeout"])

    monkeypatch.setattr(runner, "_repo_root", lambda: root)
    monkeypatch.setattr(runner, "_codex_home", lambda: codex_home)
    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(RUNNER_PATH),
            "--execute",
            "--title",
            "timeout",
            "--files",
            "src/example.py",
            "--timeout-seconds",
            "60",
            "--output",
            str(output_path),
            "--prompt-output",
            str(prompt_path),
        ],
    )

    return_code = runner.main()

    assert return_code == 124
    assert prompt_path.read_text(encoding="utf-8")
    artifact = output_path.read_text(encoding="utf-8")
    assert "- reviewer_exit_code: timeout" in artifact
    assert "NO-GO: independent review timed out after 60 seconds." in artifact
    assert not list((root / "runtime" / "reviews").glob("*_timeout.effective-config.json"))
