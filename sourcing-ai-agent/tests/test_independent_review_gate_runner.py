from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import time
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


def _session_meta(*, source: str = "exec", thread_source: str = "") -> dict[str, object]:
    payload = {
        "id": THREAD_ID,
        "session_id": THREAD_ID,
        "cli_version": "0.144.0",
        "source": source,
        "parent_thread_id": None,
        "forked_from_id": None,
    }
    if thread_source:
        payload["thread_source"] = thread_source
    return {
        "type": "session_meta",
        "payload": payload,
    }


def _thread_started() -> str:
    return json.dumps({"type": "thread.started", "thread_id": THREAD_ID})


def _completed_exec_rollout(
    *,
    prompt: str,
    final_output: str,
    source: str = "exec",
    thread_source: str = "",
    extra_events: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    normalized_final = final_output.rstrip("\r\n")
    return [
        _session_meta(source=source, thread_source=thread_source),
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


def _completed_app_server_transcript(
    runner,
    *,
    root: Path,
    configured,
    prompt: str,
    final_output: str,
) -> bytes:
    initialize, thread_start, turn_start = runner._app_server_requests(
        root=root,
        configured=configured,
        prompt=prompt,
        thread_id=THREAD_ID,
    )
    final_text = final_output.rstrip("\r\n")
    final_item = {
        "id": "review-final-message",
        "type": "agentMessage",
        "phase": "final_answer",
        "text": final_text,
    }
    thread = {
        "id": THREAD_ID,
        "sessionId": THREAD_ID,
        "source": "vscode",
        "threadSource": runner._APP_SERVER_THREAD_SOURCE,
    }
    turn = {"id": TURN_ID, "items": [], "status": "inProgress"}
    completed_turn = {"id": TURN_ID, "items": [final_item], "status": "completed"}
    messages = [
        ("client", initialize),
        ("server", {"id": runner._APP_SERVER_INITIALIZE_ID, "result": {"userAgent": "test"}}),
        ("client", {"method": "initialized"}),
        ("client", thread_start),
        (
            "server",
            {
                "id": runner._APP_SERVER_THREAD_START_ID,
                "result": {
                    "approvalPolicy": "never",
                    "approvalsReviewer": "user",
                    "cwd": str(root.resolve()),
                    "model": "gpt-5.6-sol",
                    "modelProvider": "openai",
                    "reasoningEffort": "ultra",
                    "runtimeWorkspaceRoots": [str(root.resolve())],
                    "sandbox": {"type": "readOnly", "networkAccess": False},
                    "serviceTier": "priority",
                    "thread": thread,
                },
            },
        ),
        ("server", {"method": "thread/started", "params": {"thread": thread}}),
        ("client", turn_start),
        ("server", {"id": runner._APP_SERVER_TURN_START_ID, "result": {"turn": turn}}),
        ("server", {"method": "turn/started", "params": {"threadId": THREAD_ID, "turn": turn}}),
        (
            "server",
            {
                "method": "item/completed",
                "params": {
                    "completedAtMs": 1,
                    "item": final_item,
                    "threadId": THREAD_ID,
                    "turnId": TURN_ID,
                },
            },
        ),
        (
            "server",
            {
                "method": "turn/completed",
                "params": {"threadId": THREAD_ID, "turn": completed_turn},
            },
        ),
    ]
    return b"".join(
        runner._canonical_json_line({"direction": direction, "message": message})
        for direction, message in messages
    )


def _app_server_result(
    runner,
    *,
    root: Path,
    configured,
    prompt_raw: bytes,
    final_output: str,
    returncode: int = 0,
    stderr: str = "",
):
    transcript_raw = _completed_app_server_transcript(
        runner,
        root=root,
        configured=configured,
        prompt=prompt_raw.decode("utf-8"),
        final_output=final_output,
    )
    evidence = runner._parse_app_server_transcript(
        transcript_raw=transcript_raw,
        configured=configured,
        root=root,
        prompt_raw=prompt_raw,
        raw_output=final_output.encode("utf-8"),
    )
    return runner.AppServerReviewResult(
        args=tuple(runner._build_app_server_args()),
        returncode=returncode,
        transcript_raw=transcript_raw,
        stderr=stderr,
        evidence=evidence,
    )


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


def _init_nested_review_repo(git_root: Path) -> tuple[Path, str]:
    project_root = git_root / "project"
    (project_root / "docs").mkdir(parents=True)
    (project_root / "src").mkdir(parents=True)
    (project_root / "docs" / "INDEPENDENT_REVIEW_BRIEF.md").write_text("# Review brief\n", encoding="utf-8")
    (project_root / "src" / "example.py").write_text("value = 1\n", encoding="utf-8")
    (project_root / "src" / "deleted.py").write_text("deleted = False\n", encoding="utf-8")
    REAL_SUBPROCESS_RUN(["git", "init", "-q"], cwd=git_root, check=True)
    REAL_SUBPROCESS_RUN(["git", "config", "user.name", "Test"], cwd=git_root, check=True)
    REAL_SUBPROCESS_RUN(["git", "config", "user.email", "test@example.com"], cwd=git_root, check=True)
    REAL_SUBPROCESS_RUN(["git", "add", "project"], cwd=git_root, check=True)
    REAL_SUBPROCESS_RUN(["git", "commit", "-qm", "base"], cwd=git_root, check=True)
    base = REAL_SUBPROCESS_RUN(
        ["git", "rev-parse", "HEAD"], cwd=git_root, check=True, capture_output=True, text=True
    ).stdout.strip()
    (project_root / "src" / "example.py").write_text("value = 2\n", encoding="utf-8")
    (project_root / "src" / "deleted.py").unlink()
    REAL_SUBPROCESS_RUN(["git", "add", "-A", "project/src"], cwd=git_root, check=True)
    REAL_SUBPROCESS_RUN(["git", "commit", "-qm", "reviewed"], cwd=git_root, check=True)
    return project_root, base


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
        (
            "missing_user_response",
            "prompt_response_item_exact",
            "review_artifact_rollout_causal_prompt_response_item_mismatch",
        ),
        (
            "missing_user_event",
            "prompt_event_message_exact",
            "review_artifact_rollout_causal_prompt_event_message_mismatch",
        ),
        (
            "wrong_final_response",
            "final_response_item_exact",
            "review_artifact_rollout_causal_final_response_item_mismatch",
        ),
        (
            "wrong_final_event",
            "final_event_message_exact",
            "review_artifact_rollout_causal_final_event_message_mismatch",
        ),
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


def test_app_server_transcript_binds_active_settings_ids_prompt_and_final_output(tmp_path: Path) -> None:
    runner = _load_runner()
    configured = _configured(runner, tmp_path / "codex-home")
    prompt = "Review the exact pinned scope."
    final_output = "No blocking findings.\n\nGO\n"
    transcript_raw = _completed_app_server_transcript(
        runner,
        root=tmp_path,
        configured=configured,
        prompt=prompt,
        final_output=final_output,
    )

    evidence = runner._parse_app_server_transcript(
        transcript_raw=transcript_raw,
        configured=configured,
        root=tmp_path,
        prompt_raw=prompt.encode("utf-8"),
        raw_output=final_output.encode("utf-8"),
    )

    assert evidence.settings == runner.ReviewerSettings("gpt-5.6-sol", "ultra", "priority")
    assert evidence.thread_id == THREAD_ID
    assert evidence.session_id == THREAD_ID
    assert evidence.session_source == "vscode"
    assert evidence.turn_id == TURN_ID
    assert evidence.final_output == final_output.rstrip("\r\n").encode("utf-8")
    records = [json.loads(line) for line in transcript_raw.decode("utf-8").splitlines()]
    assert records[0]["message"]["params"]["capabilities"] == {"experimentalApi": True}
    assert runner._app_server_transcript_binding_valid(evidence.binding) is True


@pytest.mark.parametrize(
    ("mutation", "failed_field"),
    [
        ("initialized", "single_initialized_notification"),
        ("initialize_capability", "single_initialize_request"),
        ("extra_client", "request_settings_exact"),
        ("active_tier", "active_settings_exact"),
        ("request_tier", "request_settings_exact"),
        ("reroute", "no_model_reroute"),
        ("thread_identity", "thread_identity_exact"),
        ("thread_session_identity", "thread_identity_exact"),
        ("turn_identity", "turn_identity_exact"),
        ("final_message", "final_agent_message_exact"),
    ],
)
def test_app_server_transcript_mutations_fail_closed(
    tmp_path: Path,
    mutation: str,
    failed_field: str,
) -> None:
    runner = _load_runner()
    from sourcing_agent import runtime_asset_retention_prune as verifier

    configured = _configured(runner, tmp_path / "codex-home")
    prompt = "Review the exact pinned scope."
    final_output = "No blocking findings.\n\nGO\n"
    transcript_raw = _completed_app_server_transcript(
        runner,
        root=tmp_path,
        configured=configured,
        prompt=prompt,
        final_output=final_output,
    )
    records = [json.loads(line) for line in transcript_raw.decode("utf-8").splitlines()]
    if mutation == "initialized":
        records[2]["message"] = {"method": "initialized", "params": {}}
    elif mutation == "initialize_capability":
        records[0]["message"]["params"]["capabilities"]["experimentalApi"] = False
    elif mutation == "extra_client":
        records.insert(
            6,
            {
                "direction": "client",
                "message": {
                    "method": "thread/settings/update",
                    "params": {"threadId": THREAD_ID, "serviceTier": "standard"},
                },
            },
        )
    elif mutation == "active_tier":
        records[4]["message"]["result"]["serviceTier"] = "standard"
    elif mutation == "request_tier":
        records[3]["message"]["params"]["serviceTier"] = "standard"
    elif mutation == "reroute":
        records.insert(
            -1,
            {
                "direction": "server",
                "message": {
                    "method": "model/rerouted",
                    "params": {"fromModel": "gpt-5.6-sol", "toModel": "gpt-5.5"},
                },
            },
        )
    elif mutation == "thread_identity":
        records[5]["message"]["params"]["thread"]["sessionId"] = "different-session"
    elif mutation == "thread_session_identity":
        records[4]["message"]["result"]["thread"]["sessionId"] = "different-session"
    elif mutation == "turn_identity":
        records[8]["message"]["params"]["turn"]["id"] = "different-turn"
    else:
        records[9]["message"]["params"]["item"]["text"] = "different final"
    mutated_raw = b"".join(
        runner._canonical_json_line(record)
        for record in records
    )

    evidence = runner._parse_app_server_transcript(
        transcript_raw=mutated_raw,
        configured=configured,
        root=tmp_path,
        prompt_raw=prompt.encode("utf-8"),
        raw_output=final_output.encode("utf-8"),
    )
    verifier_evidence, verifier_blockers = verifier._parse_independent_review_app_server_transcript(
        transcript_raw=mutated_raw,
        configured={
            "model": configured.settings.model,
            "reasoning_effort": configured.settings.reasoning_effort,
            "service_tier": configured.settings.service_tier,
        },
        root=tmp_path,
        prompt_raw=prompt.encode("utf-8"),
        raw_output=final_output.encode("utf-8"),
    )

    assert evidence.binding[failed_field] is False
    assert runner._app_server_transcript_binding_valid(evidence.binding) is False
    assert verifier_evidence["binding"] == evidence.binding
    assert f"review_artifact_transcript_binding_invalid:{failed_field}" in verifier_blockers


def test_app_server_effective_settings_use_active_tier_and_rollout_corroboration(tmp_path: Path) -> None:
    runner = _load_runner()
    codex_home = tmp_path / "codex-home"
    configured = _configured(runner, codex_home)
    prompt = "Review the exact pinned scope."
    final_output = "No blocking findings.\n\nGO\n"
    transcript_raw = _completed_app_server_transcript(
        runner,
        root=tmp_path,
        configured=configured,
        prompt=prompt,
        final_output=final_output,
    )
    transcript = runner._parse_app_server_transcript(
        transcript_raw=transcript_raw,
        configured=configured,
        root=tmp_path,
        prompt_raw=prompt.encode("utf-8"),
        raw_output=final_output.encode("utf-8"),
    )
    rollout_path = _write_rollout(
        codex_home,
        [
            _session_meta(source="vscode", thread_source=runner._APP_SERVER_THREAD_SOURCE),
            {"type": "turn_context", "payload": {"model": "gpt-5.6-sol", "effort": "ultra"}},
        ],
    )

    effective = runner._load_app_server_effective_reviewer_configuration(
        transcript=transcript,
        configured=configured,
        codex_home=codex_home,
    )

    assert effective.settings == runner.ReviewerSettings("gpt-5.6-sol", "ultra", "priority")
    assert effective.thread_id == THREAD_ID
    assert effective.session_id == THREAD_ID
    assert effective.rollout_path == rollout_path
    assert effective.source == {
        "model": ["app_server_thread_start_response", "turn_context"],
        "reasoning_effort": ["app_server_thread_start_response", "turn_context"],
        "service_tier": ["app_server_thread_start_response"],
    }

    rollout_path.write_text(
        "".join(
            json.dumps(event) + "\n"
            for event in [
                _session_meta(source="vscode", thread_source=runner._APP_SERVER_THREAD_SOURCE),
                {
                    "type": "turn_context",
                    "payload": {"model": "gpt-5.6-sol", "effort": "ultra"},
                },
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "thread_settings_applied",
                        "thread_settings": {
                            "model": "gpt-5.6-sol",
                            "reasoning_effort": "ultra",
                            "service_tier": "standard",
                        },
                    },
                },
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="service_tier.*differs"):
        runner._load_app_server_effective_reviewer_configuration(
            transcript=transcript,
            configured=configured,
            codex_home=codex_home,
        )


@pytest.mark.parametrize(
    ("mutation", "runner_error", "verifier_blocker"),
    [
        (
            {"id": "different-thread"},
            "identity.*does not match",
            "review_artifact_rollout_invalid_thread_identity",
        ),
        (
            {"session_id": "different-session"},
            "session identity.*does not match",
            "review_artifact_rollout_invalid_session_identity",
        ),
        (
            {"parent_thread_id": "parent-thread"},
            "not an independent root reviewer session",
            "review_artifact_rollout_not_root_session",
        ),
        (
            {"forked_from_id": "fork-source"},
            "not an independent root reviewer session",
            "review_artifact_rollout_not_root_session",
        ),
    ],
)
def test_app_server_rollout_identity_and_root_lineage_fail_closed(
    tmp_path: Path,
    mutation: dict[str, str],
    runner_error: str,
    verifier_blocker: str,
) -> None:
    runner = _load_runner()
    from sourcing_agent import runtime_asset_retention_prune as verifier

    codex_home = tmp_path / "codex-home"
    configured = _configured(runner, codex_home)
    prompt = "Review the exact pinned scope."
    final_output = "No blocking findings.\n\nGO\n"
    transcript_raw = _completed_app_server_transcript(
        runner,
        root=tmp_path,
        configured=configured,
        prompt=prompt,
        final_output=final_output,
    )
    transcript = runner._parse_app_server_transcript(
        transcript_raw=transcript_raw,
        configured=configured,
        root=tmp_path,
        prompt_raw=prompt.encode("utf-8"),
        raw_output=final_output.encode("utf-8"),
    )
    session_meta = _session_meta(source="vscode", thread_source=runner._APP_SERVER_THREAD_SOURCE)
    session_meta["payload"].update(mutation)
    rollout_path = _write_rollout(
        codex_home,
        [
            session_meta,
            {"type": "turn_context", "payload": {"model": "gpt-5.6-sol", "effort": "ultra"}},
        ],
    )

    with pytest.raises(RuntimeError, match=runner_error):
        runner._load_app_server_effective_reviewer_configuration(
            transcript=transcript,
            configured=configured,
            codex_home=codex_home,
        )

    _, blockers = verifier._parse_independent_review_rollout_v3(
        rollout_raw=rollout_path.read_bytes(),
        configured_model="gpt-5.6-sol",
    )
    if mutation.keys() & {"id", "session_id"}:
        # The parser proves uniqueness; the enclosing verifier binds the value
        # to transcript identity. Exercise that comparison explicitly here.
        parsed, _ = verifier._parse_independent_review_rollout_v3(
            rollout_raw=rollout_path.read_bytes(),
            configured_model="gpt-5.6-sol",
        )
        observed_key = "rollout_id" if "id" in mutation else "session_id"
        expected_value = transcript.thread_id if observed_key == "rollout_id" else transcript.session_id
        assert parsed[observed_key] != expected_value
    else:
        assert verifier_blocker in blockers


def test_app_server_transport_runs_scripted_json_rpc_without_model_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _load_runner()
    configured = _configured(runner, tmp_path / "codex-home")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_codex = fake_bin / "codex"
    fake_codex.write_text(
        """#!/usr/bin/env python3
import json
import sys

thread_id = "019f0000-0000-7000-8000-000000000099"
session_id = thread_id
turn_id = "019f0000-0000-7000-8000-000000000100"

def emit(message):
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\\n")
    sys.stdout.flush()

for raw_line in sys.stdin:
    message = json.loads(raw_line)
    method = message.get("method")
    if method == "initialize":
        if message["params"].get("capabilities") != {"experimentalApi": True}:
            emit({"id": message["id"], "error": {"code": -32600, "message": "experimentalApi required"}})
        else:
            emit({"id": message["id"], "result": {"userAgent": "scripted"}})
    elif method == "thread/start":
        params = message["params"]
        thread = {
            "id": thread_id,
            "sessionId": session_id,
            "source": "vscode",
            "threadSource": params["threadSource"],
        }
        emit({
            "id": message["id"],
            "result": {
                "approvalPolicy": params["approvalPolicy"],
                "approvalsReviewer": params["approvalsReviewer"],
                "cwd": params["cwd"],
                "model": params["model"],
                "modelProvider": "openai",
                "reasoningEffort": params["config"]["model_reasoning_effort"],
                "sandbox": {"type": "readOnly", "networkAccess": False},
                "serviceTier": "priority",
                "thread": thread,
            },
        })
        emit({"method": "thread/started", "params": {"thread": thread}})
    elif method == "turn/start":
        active_turn = {"id": turn_id, "items": [], "status": "inProgress"}
        final_item = {
            "id": "scripted-final",
            "type": "agentMessage",
            "phase": "final_answer",
            "text": "GO",
        }
        emit({"id": message["id"], "result": {"turn": active_turn}})
        emit({
            "method": "turn/started",
            "params": {"threadId": thread_id, "turn": active_turn},
        })
        emit({
            "method": "item/completed",
            "params": {
                "completedAtMs": 1,
                "item": final_item,
                "threadId": thread_id,
                "turnId": turn_id,
            },
        })
        emit({
            "method": "turn/completed",
            "params": {
                "threadId": thread_id,
                "turn": {"id": turn_id, "items": [final_item], "status": "completed"},
            },
        })
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ.get('PATH', '')}")

    result = runner._run_app_server_review(
        root=tmp_path,
        configured=configured,
        prompt_raw=b"scripted prompt",
        timeout_seconds=10,
    )

    assert result.returncode == 0
    assert result.evidence.final_output == b"GO"
    assert result.evidence.session_id == THREAD_ID
    assert runner._app_server_transcript_binding_valid(result.evidence.binding) is True


def test_app_server_transport_timeout_is_hard_when_server_stalls_mid_frame(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _load_runner()
    configured = _configured(runner, tmp_path / "codex-home")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_codex = fake_bin / "codex"
    fake_codex.write_text(
        """#!/usr/bin/env python3
import sys
import time

sys.stdin.readline()
sys.stdout.write("{")
sys.stdout.flush()
time.sleep(30)
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake_bin}:{os.environ.get('PATH', '')}")

    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        runner._run_app_server_review(
            root=tmp_path,
            configured=configured,
            prompt_raw=b"scripted prompt",
            timeout_seconds=1,
        )

    assert time.monotonic() - started < 5


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

    def fake_app_server_review(**kwargs):
        prompt = kwargs["prompt_raw"].decode("utf-8")
        final_output = "Reviewed pinned scope.\n\nGO\n"
        _write_rollout(
            codex_home,
            _completed_exec_rollout(
                prompt=prompt,
                final_output=final_output,
                source="vscode",
                thread_source=runner._APP_SERVER_THREAD_SOURCE,
            ),
        )
        return _app_server_result(
            runner,
            root=root,
            configured=kwargs["configured"],
            prompt_raw=kwargs["prompt_raw"],
            final_output=final_output,
        )

    monkeypatch.setattr(runner, "_repo_root", lambda: root)
    monkeypatch.setattr(runner, "_codex_home", lambda: codex_home)
    monkeypatch.setattr(runner, "_run_app_server_review", fake_app_server_review)
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
    assert evidence["contract_version"] == "independent_review_effective_config_v3"
    assert evidence["session"]["session_id"] == THREAD_ID
    assert evidence["session"]["thread_id"] == THREAD_ID
    assert evidence["session"]["turn_id"] == TURN_ID
    assert evidence["transport"]["kind"] == "app_server_stdio"
    assert evidence["transport"]["active_settings_source"] == "thread/start.response"
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

    original_evidence_raw = evidence_path.read_bytes()
    turn_mismatch_evidence = json.loads(original_evidence_raw)
    turn_mismatch_evidence["causal_binding"]["turn_id"] = "different-rollout-turn"
    turn_mismatch_raw = (json.dumps(turn_mismatch_evidence, indent=2, sort_keys=True) + "\n").encode("utf-8")
    turn_mismatch_sha = hashlib.sha256(turn_mismatch_raw).hexdigest()
    evidence_path.write_bytes(turn_mismatch_raw)
    output_path.write_text(
        artifact_text.replace(metadata["reviewer_effective_config_sha256"], turn_mismatch_sha),
        encoding="utf-8",
    )
    assert "review_artifact_effective_config_v3_turn_identity_mismatch" in validate_independent_review_artifact(
        artifact_path=output_path,
        workspace_root=root,
        expected_title="pinned scope",
        required_files=["src/example.py"],
    )
    evidence_path.write_bytes(original_evidence_raw)
    output_path.write_text(artifact_text, encoding="utf-8")

    rollout_path = root / evidence["session"]["rollout_path"]
    original_rollout_raw = rollout_path.read_bytes()
    original_rollout_sha = metadata["reviewer_rollout_sha256"]
    for mutation, expected_blocker in (
        (
            {"session_id": "different-session"},
            "review_artifact_rollout_recomputed_mismatch:session_id",
        ),
        (
            {"parent_thread_id": "parent-thread"},
            "review_artifact_rollout_not_root_session",
        ),
    ):
        rollout_events = [json.loads(line) for line in original_rollout_raw.decode("utf-8").splitlines()]
        session_meta = next(event for event in rollout_events if event.get("type") == "session_meta")
        session_meta["payload"].update(mutation)
        mutated_rollout_raw = "".join(
            json.dumps(event, separators=(",", ":")) + "\n"
            for event in rollout_events
        ).encode("utf-8")
        mutated_rollout_sha = hashlib.sha256(mutated_rollout_raw).hexdigest()
        rollout_path.write_bytes(mutated_rollout_raw)
        mutated_evidence = json.loads(original_evidence_raw)
        mutated_evidence["session"]["rollout_sha256"] = mutated_rollout_sha
        mutated_evidence_raw = (json.dumps(mutated_evidence, indent=2, sort_keys=True) + "\n").encode("utf-8")
        mutated_evidence_sha = hashlib.sha256(mutated_evidence_raw).hexdigest()
        evidence_path.write_bytes(mutated_evidence_raw)
        output_path.write_text(
            artifact_text.replace(original_rollout_sha, mutated_rollout_sha).replace(
                metadata["reviewer_effective_config_sha256"],
                mutated_evidence_sha,
            ),
            encoding="utf-8",
        )

        assert expected_blocker in validate_independent_review_artifact(
            artifact_path=output_path,
            workspace_root=root,
            expected_title="pinned scope",
            required_files=["src/example.py"],
        )

    rollout_path.write_bytes(original_rollout_raw)
    evidence_path.write_bytes(original_evidence_raw)
    output_path.write_text(artifact_text, encoding="utf-8")

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


def test_nested_project_scope_maps_explicitly_to_git_toplevel_and_fails_closed(
    tmp_path: Path,
) -> None:
    from sourcing_agent import runtime_asset_retention_prune as review_evidence

    runner = _load_runner()
    git_root = tmp_path / "parent-repo"
    git_root.mkdir()
    project_root, base = _init_nested_review_repo(git_root)
    scope = review_evidence.build_independent_review_scope_evidence(
        workspace_root=project_root,
        title="nested project scope",
        base_ref=base,
        files=["src/example.py"],
        extra_context="contract context",
    )
    deleted_scope = review_evidence.build_independent_review_scope_evidence(
        workspace_root=project_root,
        title="nested deleted scope",
        base_ref=base,
        files=["src/deleted.py"],
        extra_context="contract context",
    )

    empty_sha256 = hashlib.sha256(b"").hexdigest()
    assert scope["scope_mode"] == "pinned_commit_diff"
    assert scope["files"] == ["src/example.py"]
    assert scope["git_diff_sha256"] != empty_sha256
    assert scope["git_tree_sha256"] != empty_sha256
    pathspec = ":(top,literal)project/src/example.py"
    diff_raw = REAL_SUBPROCESS_RUN(
        [
            "git",
            "diff",
            "--binary",
            "--no-ext-diff",
            scope["resolved_base_commit"],
            scope["resolved_head_commit"],
            "--",
            pathspec,
        ],
        cwd=project_root,
        check=True,
        capture_output=True,
    ).stdout
    tree_raw = REAL_SUBPROCESS_RUN(
        ["git", "ls-tree", "-r", scope["resolved_head_commit"], "--", pathspec],
        cwd=project_root,
        check=True,
        capture_output=True,
    ).stdout
    assert hashlib.sha256(diff_raw).hexdigest() == scope["git_diff_sha256"]
    assert hashlib.sha256(tree_raw).hexdigest() == scope["git_tree_sha256"]
    assert b"\tsrc/example.py\n" in tree_raw
    assert b"\tproject/src/example.py\n" not in tree_raw
    prompt = runner._build_prompt(root=project_root, scope=scope, extra_context="contract context")
    assert f"git show {scope['resolved_head_commit']}:./<project-relative-path>" in prompt

    def scope_blockers(candidate_scope: dict[str, object], *, required_files: list[str]) -> list[str]:
        return review_evidence._independent_review_scope_blockers(
            scope=candidate_scope,
            metadata={"review_scope_digest_sha256": candidate_scope["scope_digest_sha256"]},
            root=project_root,
            expected_title=str(candidate_scope["title"]),
            required_files=required_files,
            expected_scope_digest=str(candidate_scope["scope_digest_sha256"]),
        )

    assert scope_blockers(scope, required_files=["src/example.py"]) == []
    assert deleted_scope["scope_mode"] == "pinned_commit_diff"
    assert scope_blockers(deleted_scope, required_files=[]) == []

    (git_root / "sibling.txt").write_text("unrelated\n", encoding="utf-8")
    REAL_SUBPROCESS_RUN(["git", "add", "sibling.txt"], cwd=git_root, check=True)
    REAL_SUBPROCESS_RUN(["git", "commit", "-qm", "unrelated sibling"], cwd=git_root, check=True)
    assert scope_blockers(scope, required_files=["src/example.py"]) == []
    assert scope_blockers(deleted_scope, required_files=[]) == []

    (project_root / "src" / "deleted.py").write_text("recreated = True\n", encoding="utf-8")
    assert "review_artifact_scope_current_tree_mismatch" in scope_blockers(deleted_scope, required_files=[])
    (project_root / "src" / "deleted.py").unlink()

    example_path = project_root / "src" / "example.py"
    example_path.write_text("value = 3\n", encoding="utf-8")
    assert "review_artifact_scope_current_tree_mismatch" in scope_blockers(scope, required_files=["src/example.py"])
    example_path.write_text("value = 2\n", encoding="utf-8")

    example_path.write_text("value = staged replacement\n", encoding="utf-8")
    REAL_SUBPROCESS_RUN(["git", "add", "--", "./src/example.py"], cwd=project_root, check=True)
    example_path.write_text("value = 2\n", encoding="utf-8")
    assert "review_artifact_scope_current_tree_mismatch" in scope_blockers(scope, required_files=["src/example.py"])
    REAL_SUBPROCESS_RUN(["git", "add", "--", "./src/example.py"], cwd=project_root, check=True)
    assert scope_blockers(scope, required_files=["src/example.py"]) == []

    example_path.write_text("value = 4\n", encoding="utf-8")
    REAL_SUBPROCESS_RUN(["git", "add", "--", "./src/example.py"], cwd=project_root, check=True)
    REAL_SUBPROCESS_RUN(["git", "commit", "-qm", "change nested reviewed scope"], cwd=git_root, check=True)
    assert "review_artifact_scope_current_tree_mismatch" in scope_blockers(scope, required_files=["src/example.py"])


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

    def fake_app_server_review(**kwargs):
        prompt = kwargs["prompt_raw"].decode("utf-8")
        final_output = "GO\n"
        _write_rollout(
            codex_home,
            _completed_exec_rollout(
                prompt=prompt,
                final_output=final_output,
                source="vscode",
                thread_source=runner._APP_SERVER_THREAD_SOURCE,
            ),
        )
        return _app_server_result(
            runner,
            root=root,
            configured=kwargs["configured"],
            prompt_raw=kwargs["prompt_raw"],
            final_output=final_output,
            returncode=17,
            stderr="capacity",
        )

    monkeypatch.setattr(runner, "_repo_root", lambda: root)
    monkeypatch.setattr(runner, "_codex_home", lambda: codex_home)
    monkeypatch.setattr(runner, "_run_app_server_review", fake_app_server_review)
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
    assert evidence["process"] == {"reviewer_exit_code": 17, "timed_out": False}
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
        "transport",
    }
    assert evidence["scope"]["scope_mode"] == "reference_only_worktree"
    assert evidence["artifacts"]["raw_output"]["sha256"] == hashlib.sha256(b"GO").hexdigest()
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

    def fake_app_server_review(**kwargs):
        raise subprocess.TimeoutExpired(runner._build_app_server_args(), kwargs["timeout_seconds"])

    monkeypatch.setattr(runner, "_repo_root", lambda: root)
    monkeypatch.setattr(runner, "_codex_home", lambda: codex_home)
    monkeypatch.setattr(runner, "_run_app_server_review", fake_app_server_review)
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
