#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import selectors
import shlex
import shutil
import subprocess
import tempfile
import time
import tomllib
from pathlib import Path
from typing import NamedTuple

from sourcing_agent.runtime_asset_retention_prune import (
    build_independent_review_scope_evidence,
    independent_review_response_item_matches_raw_output,
    normalize_independent_review_files,
)

_INHERITED_VALUE_SENTINELS = {"", "auto", "default", "inherit"}
_EFFECTIVE_CONFIG_CONTRACT_VERSION = "independent_review_effective_config_v4"
_APP_SERVER_TRANSPORT = "app_server_stdio"
_APP_SERVER_CLIENT_NAME = "sourcing-ai-agent-independent-review-gate"
_APP_SERVER_CLIENT_VERSION = "3"
_APP_SERVER_SERVICE_NAME = "sourcing-ai-agent-independent-review-gate"
_APP_SERVER_THREAD_SOURCE = "sourcing-ai-agent-independent-review-v3"
_APP_SERVER_INITIALIZE_ID = "review-initialize"
_APP_SERVER_THREAD_START_ID = "review-thread-start"
_APP_SERVER_TURN_START_ID = "review-turn-start"
_ROLLOUT_SETTING_EVENT_TYPES = {
    "session_configured",
    "thread_settings_applied",
    "turn_context",
}


class ReviewerSettings(NamedTuple):
    model: str
    reasoning_effort: str
    service_tier: str


class ReviewerConfiguration(NamedTuple):
    settings: ReviewerSettings
    path: Path
    sha256: str


class EffectiveReviewerConfiguration(NamedTuple):
    settings: ReviewerSettings
    thread_id: str
    rollout_path: Path
    session_id: str
    codex_cli_version: str
    source: dict[str, list[str]]
    rollout_sha256: str
    model_reroutes: tuple[dict[str, str], ...]


class AppServerTranscriptEvidence(NamedTuple):
    settings: ReviewerSettings
    thread_id: str
    session_id: str
    session_source: str
    turn_id: str
    final_output: bytes
    binding: dict[str, object]
    model_reroutes: tuple[dict[str, str], ...]


class AppServerReviewResult(NamedTuple):
    args: tuple[str, ...]
    returncode: int
    transcript_raw: bytes
    stderr: str
    evidence: AppServerTranscriptEvidence


class AppServerReviewError(RuntimeError):
    def __init__(self, message: str, *, transcript_raw: bytes = b"", stderr: str = "") -> None:
        super().__init__(message)
        self.transcript_raw = transcript_raw
        self.stderr = stderr


_CAUSAL_BINDING_BOOLEAN_FIELDS = (
    "rollout_json_valid",
    "text_utf8_valid",
    "session_source_exec",
    "single_task_turn",
    "no_abort",
    "prompt_response_item_exact",
    "prompt_event_message_exact",
    "final_response_item_exact",
    "final_event_message_exact",
    "task_complete_final_exact",
)

_APP_SERVER_CAUSAL_BINDING_BOOLEAN_FIELDS = (
    "rollout_json_valid",
    "text_utf8_valid",
    "session_source_matches_thread_start",
    "session_thread_source_exact",
    "single_task_turn",
    "no_abort",
    "prompt_response_item_exact",
    "prompt_event_message_exact",
    "final_response_item_exact",
    "final_event_message_exact",
    "task_complete_final_exact",
)

_APP_SERVER_TRANSCRIPT_BOOLEAN_FIELDS = (
    "transcript_json_valid",
    "single_initialize_request",
    "single_initialize_response",
    "single_initialized_notification",
    "single_thread_start_request",
    "single_thread_start_response",
    "single_turn_start_request",
    "single_turn_start_response",
    "request_settings_exact",
    "active_settings_exact",
    "active_read_only",
    "thread_identity_exact",
    "thread_source_exact",
    "turn_identity_exact",
    "root_turn_start_response_exact",
    "single_turn_completed",
    "turn_status_completed",
    "prompt_request_exact",
    "final_agent_message_exact",
    "observed_turns_unique_complete",
    "observed_turns_started_in_progress",
    "observed_turns_status_completed",
    "observed_turns_error_free",
    "observed_turns_final_agent_message_exact",
    "root_thread_turn_identity_exclusive",
    "child_thread_ids_distinct",
    "observed_final_item_ids_unique",
    "completed_turn_items_binding_exact",
    "no_orphan_final_agent_message",
    "no_protocol_error",
    "no_model_reroute",
    "thread_settings_consistent",
)


def _app_server_turn_key(message: dict[str, object]) -> tuple[str, str]:
    params_raw = message.get("params")
    params = dict(params_raw) if isinstance(params_raw, dict) else {}
    turn_raw = params.get("turn")
    turn = dict(turn_raw) if isinstance(turn_raw, dict) else {}
    return (
        str(params.get("threadId") or "").strip(),
        str(turn.get("id") or "").strip(),
    )


def _app_server_final_item_key(message: dict[str, object]) -> tuple[str, str]:
    params_raw = message.get("params")
    params = dict(params_raw) if isinstance(params_raw, dict) else {}
    return (
        str(params.get("threadId") or "").strip(),
        str(params.get("turnId") or "").strip(),
    )


def _app_server_observed_turn_binding(
    *,
    server_messages: list[dict[str, object]],
    root_thread_id: str,
    root_turn_id: str,
) -> tuple[
    dict[tuple[str, str], list[tuple[int, dict[str, object]]]],
    dict[tuple[str, str], list[tuple[int, dict[str, object]]]],
    dict[tuple[str, str], list[tuple[int, dict[str, object]]]],
    dict[str, object],
]:
    starts: dict[tuple[str, str], list[tuple[int, dict[str, object]]]] = {}
    completes: dict[tuple[str, str], list[tuple[int, dict[str, object]]]] = {}
    final_items: dict[tuple[str, str], list[tuple[int, dict[str, object]]]] = {}
    for index, message in enumerate(server_messages):
        method = str(message.get("method") or "")
        if method in {"turn/started", "turn/completed"} and isinstance(message.get("params"), dict):
            key = _app_server_turn_key(message)
            target = starts if method == "turn/started" else completes
            target.setdefault(key, []).append((index, message))
            continue
        if method != "item/completed" or not isinstance(message.get("params"), dict):
            continue
        params = dict(message["params"])
        item_raw = params.get("item")
        item = dict(item_raw) if isinstance(item_raw, dict) else {}
        if str(item.get("type") or "") != "agentMessage" or str(item.get("phase") or "") != "final_answer":
            continue
        final_items.setdefault(_app_server_final_item_key(message), []).append((index, item))

    observed_keys = set(starts) | set(completes)
    unique_complete = bool(observed_keys)
    started_in_progress = bool(observed_keys)
    status_completed = bool(observed_keys)
    error_free = bool(observed_keys)
    final_messages_exact = bool(observed_keys)
    completed_items_binding_exact = bool(observed_keys)
    final_item_ids: list[str] = []
    final_item_count = 0
    for key in observed_keys:
        keyed_starts = starts.get(key, [])
        keyed_completes = completes.get(key, [])
        keyed_finals = final_items.get(key, [])
        pair_exact = (
            bool(key[0])
            and bool(key[1])
            and len(keyed_starts) == 1
            and len(keyed_completes) == 1
            and keyed_starts[0][0] < keyed_completes[0][0]
        )
        unique_complete = unique_complete and pair_exact

        started_turn_raw = dict(keyed_starts[0][1]["params"]).get("turn") if len(keyed_starts) == 1 else None
        started_turn = dict(started_turn_raw) if isinstance(started_turn_raw, dict) else {}
        completed_turn_raw = (
            dict(keyed_completes[0][1]["params"]).get("turn") if len(keyed_completes) == 1 else None
        )
        completed_turn = dict(completed_turn_raw) if isinstance(completed_turn_raw, dict) else {}
        started_in_progress = started_in_progress and started_turn.get("status") == "inProgress"
        status_completed = status_completed and completed_turn.get("status") == "completed"
        error_free = (
            error_free
            and started_turn.get("error") is None
            and completed_turn.get("error") is None
        )

        final_exact = len(keyed_finals) == 1
        if final_exact:
            final_index, final_item = keyed_finals[0]
            final_item_id = final_item.get("id")
            final_item_text = final_item.get("text")
            final_exact = (
                pair_exact
                and keyed_starts[0][0] < final_index < keyed_completes[0][0]
                and isinstance(final_item_id, str)
                and bool(final_item_id.strip())
                and isinstance(final_item_text, str)
                and bool(final_item_text.strip())
            )
        final_messages_exact = final_messages_exact and final_exact

        completed_items_raw = completed_turn.get("items")
        completed_items = completed_items_raw if isinstance(completed_items_raw, list) else []
        inline_finals = [
            dict(item)
            for item in completed_items
            if isinstance(item, dict)
            and str(item.get("type") or "") == "agentMessage"
            and str(item.get("phase") or "") == "final_answer"
        ]
        items_view = str(completed_turn.get("itemsView") or "").strip()
        if items_view == "notLoaded":
            protocol_exact = isinstance(completed_items_raw, list) and not completed_items
        else:
            protocol_exact = (
                items_view in {"", "loaded"}
                and isinstance(completed_items_raw, list)
                and len(inline_finals) == 1
                and len(keyed_finals) == 1
                and isinstance(inline_finals[0].get("id"), str)
                and inline_finals[0].get("id") == keyed_finals[0][1].get("id")
                and isinstance(inline_finals[0].get("text"), str)
                and inline_finals[0].get("text") == keyed_finals[0][1].get("text")
            )
        completed_items_binding_exact = completed_items_binding_exact and protocol_exact

    for keyed_finals in final_items.values():
        for _, final_item in keyed_finals:
            final_item_count += 1
            final_item_id = final_item.get("id")
            if isinstance(final_item_id, str) and final_item_id.strip():
                final_item_ids.append(final_item_id)

    root_key = (root_thread_id, root_turn_id)
    root_thread_keys = {key for key in observed_keys if key[0] == root_thread_id}
    child_keys = observed_keys - {root_key}
    child_thread_ids = [key[0] for key in child_keys]
    root_thread_turn_identity_exclusive = (
        bool(root_thread_id)
        and bool(root_turn_id)
        and root_key in observed_keys
        and root_thread_keys == {root_key}
    )
    child_thread_ids_distinct = (
        all(thread_id and thread_id != root_thread_id for thread_id in child_thread_ids)
        and len(set(child_thread_ids)) == len(child_thread_ids)
    )
    observed_final_item_ids_unique = (
        final_item_count > 0
        and len(final_item_ids) == final_item_count
        and len(set(final_item_ids)) == len(final_item_ids)
    )

    binding: dict[str, object] = {
        "observed_turn_count": len(observed_keys),
        "observed_turns_unique_complete": unique_complete,
        "observed_turns_started_in_progress": started_in_progress,
        "observed_turns_status_completed": status_completed,
        "observed_turns_error_free": error_free,
        "observed_turns_final_agent_message_exact": final_messages_exact,
        "root_thread_turn_identity_exclusive": root_thread_turn_identity_exclusive,
        "child_thread_ids_distinct": child_thread_ids_distinct,
        "observed_final_item_ids_unique": observed_final_item_ids_unique,
        "completed_turn_items_binding_exact": completed_items_binding_exact,
        "no_orphan_final_agent_message": set(final_items).issubset(observed_keys),
    }
    return starts, completes, final_items, binding


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _split_files(raw: str) -> list[str]:
    return [part for part in shlex.split(raw or "") if part]


def _codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME") or (Path.home() / ".codex")).expanduser().resolve()


def _load_reviewer_configuration(config_path: Path | None = None) -> ReviewerConfiguration:
    resolved_path = (config_path or (_codex_home() / "config.toml")).expanduser().resolve()
    raw = resolved_path.read_bytes()
    payload = tomllib.loads(raw.decode("utf-8"))
    settings = ReviewerSettings(
        model=str(payload.get("model") or "").strip(),
        reasoning_effort=str(payload.get("model_reasoning_effort") or "").strip(),
        service_tier=str(payload.get("service_tier") or "").strip(),
    )
    missing = [
        name
        for name, value in (
            ("model", settings.model),
            ("model_reasoning_effort", settings.reasoning_effort),
            ("service_tier", settings.service_tier),
        )
        if value.lower() in _INHERITED_VALUE_SENTINELS
    ]
    if missing:
        raise RuntimeError(
            f"Codex global config {resolved_path} must explicitly set latest-model review values: " + ", ".join(missing)
        )
    return ReviewerConfiguration(
        settings=settings,
        path=resolved_path,
        sha256=hashlib.sha256(raw).hexdigest(),
    )


def _thread_id_from_events(events_text: str) -> str:
    thread_ids: set[str] = set()
    for raw_line in events_text.splitlines():
        try:
            event = json.loads(raw_line)
        except (TypeError, ValueError):
            continue
        if str(event.get("type") or "") != "thread.started":
            continue
        raw_payload = event.get("payload")
        payload_thread_id = raw_payload.get("thread_id") if isinstance(raw_payload, dict) else ""
        thread_id = str(event.get("thread_id") or payload_thread_id or "").strip()
        if thread_id:
            thread_ids.add(thread_id)
    if len(thread_ids) > 1:
        raise RuntimeError(f"Codex JSON events reported conflicting thread ids: {sorted(thread_ids)!r}")
    return next(iter(thread_ids), "")


def _rollout_path_for_thread(thread_id: str, *, codex_home: Path, timeout_seconds: float = 2.0) -> Path | None:
    sessions_root = codex_home / "sessions"
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    while True:
        matches = sorted(sessions_root.rglob(f"*{thread_id}.jsonl")) if sessions_root.exists() else []
        if matches:
            return matches[-1]
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.05)


def _service_tier_matches(configured: str, effective: str) -> bool:
    return _canonical_service_tier(configured) == _canonical_service_tier(effective)


def _canonical_service_tier(value: object) -> str:
    normalized = str(value or "").strip()
    return "priority" if normalized in {"fast", "priority"} else normalized


def _canonical_json_line(payload: dict[str, object]) -> bytes:
    return (json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


def _app_server_requests(
    *,
    root: Path,
    configured: ReviewerConfiguration,
    prompt: str,
    thread_id: str = "",
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    tier = _canonical_service_tier(configured.settings.service_tier)
    initialize = {
        "id": _APP_SERVER_INITIALIZE_ID,
        "method": "initialize",
        "params": {
            "capabilities": {"experimentalApi": True},
            "clientInfo": {
                "name": _APP_SERVER_CLIENT_NAME,
                "title": "Sourcing AI Agent independent review gate",
                "version": _APP_SERVER_CLIENT_VERSION,
            }
        },
    }
    thread_start = {
        "id": _APP_SERVER_THREAD_START_ID,
        "method": "thread/start",
        "params": {
            "allowProviderModelFallback": False,
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "config": {"model_reasoning_effort": configured.settings.reasoning_effort},
            "cwd": str(root.resolve()),
            "ephemeral": False,
            "model": configured.settings.model,
            "runtimeWorkspaceRoots": [str(root.resolve())],
            "sandbox": "read-only",
            "serviceName": _APP_SERVER_SERVICE_NAME,
            "serviceTier": tier,
            "threadSource": _APP_SERVER_THREAD_SOURCE,
        },
    }
    turn_start = {
        "id": _APP_SERVER_TURN_START_ID,
        "method": "turn/start",
        "params": {
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "cwd": str(root.resolve()),
            "effort": configured.settings.reasoning_effort,
            "input": [{"type": "text", "text": prompt}],
            "model": configured.settings.model,
            "runtimeWorkspaceRoots": [str(root.resolve())],
            "sandboxPolicy": {"type": "readOnly", "networkAccess": False},
            "serviceTier": tier,
            "threadId": thread_id,
        },
    }
    return initialize, thread_start, turn_start


def _transcript_messages(transcript_raw: bytes) -> tuple[list[dict[str, object]], bool]:
    records: list[dict[str, object]] = []
    valid = True
    for raw_line in transcript_raw.decode("utf-8", errors="replace").splitlines():
        if not raw_line.strip():
            continue
        try:
            record = json.loads(raw_line)
        except (TypeError, ValueError):
            valid = False
            continue
        if not isinstance(record, dict) or record.get("direction") not in {"client", "server"}:
            valid = False
            continue
        message = record.get("message")
        if not isinstance(message, dict):
            valid = False
            continue
        records.append({"direction": str(record["direction"]), "message": dict(message)})
    return records, valid


def _parse_app_server_transcript(
    *,
    transcript_raw: bytes,
    configured: ReviewerConfiguration,
    root: Path,
    prompt_raw: bytes,
    raw_output: bytes | None = None,
) -> AppServerTranscriptEvidence:
    records, transcript_json_valid = _transcript_messages(transcript_raw)
    client_messages = [dict(record["message"]) for record in records if record["direction"] == "client"]
    server_messages = [dict(record["message"]) for record in records if record["direction"] == "server"]

    def client_requests(method: str) -> list[dict[str, object]]:
        return [message for message in client_messages if str(message.get("method") or "") == method]

    def server_responses(request_id: str) -> list[dict[str, object]]:
        return [message for message in server_messages if str(message.get("id") or "") == request_id]

    initialize_requests = client_requests("initialize")
    initialized_notifications = client_requests("initialized")
    thread_requests = client_requests("thread/start")
    turn_requests = client_requests("turn/start")
    initialize_responses = server_responses(_APP_SERVER_INITIALIZE_ID)
    thread_responses = server_responses(_APP_SERVER_THREAD_START_ID)
    turn_responses = server_responses(_APP_SERVER_TURN_START_ID)

    thread_result_raw = thread_responses[0].get("result") if len(thread_responses) == 1 else None
    thread_result = dict(thread_result_raw) if isinstance(thread_result_raw, dict) else {}
    thread_raw = thread_result.get("thread")
    thread = dict(thread_raw) if isinstance(thread_raw, dict) else {}
    thread_id = str(thread.get("id") or "").strip()
    session_id = str(thread.get("sessionId") or "").strip()
    active_settings = ReviewerSettings(
        model=str(thread_result.get("model") or "").strip(),
        reasoning_effort=str(thread_result.get("reasoningEffort") or "").strip(),
        service_tier=_canonical_service_tier(thread_result.get("serviceTier")),
    )

    turn_result_raw = turn_responses[0].get("result") if len(turn_responses) == 1 else None
    turn_result = dict(turn_result_raw) if isinstance(turn_result_raw, dict) else {}
    started_turn_raw = turn_result.get("turn")
    started_turn = dict(started_turn_raw) if isinstance(started_turn_raw, dict) else {}
    turn_id = str(started_turn.get("id") or "").strip()

    turn_starts, turn_completes, turn_final_items, observed_turn_binding = _app_server_observed_turn_binding(
        server_messages=server_messages,
        root_thread_id=thread_id,
        root_turn_id=turn_id,
    )
    root_turn_key = (thread_id, turn_id)
    root_started_observations = turn_starts.get(root_turn_key, [])
    root_completed_observations = turn_completes.get(root_turn_key, [])
    root_final_observations = turn_final_items.get(root_turn_key, [])
    completed_notifications = [message for _, message in root_completed_observations]
    completed_params = (
        dict(completed_notifications[0]["params"])
        if len(completed_notifications) == 1 and isinstance(completed_notifications[0].get("params"), dict)
        else {}
    )
    completed_turn_raw = completed_params.get("turn")
    completed_turn = dict(completed_turn_raw) if isinstance(completed_turn_raw, dict) else {}

    final_item = root_final_observations[0][1] if len(root_final_observations) == 1 else {}
    final_text_raw = final_item.get("text")
    final_text = final_text_raw if isinstance(final_text_raw, str) else ""
    final_output = final_text.encode("utf-8")
    normalized_expected = _normalize_trailing_newlines((raw_output or final_output).decode("utf-8", errors="replace"))

    expected_initialize, expected_thread, expected_turn = _app_server_requests(
        root=root,
        configured=configured,
        prompt=prompt_raw.decode("utf-8", errors="replace"),
        thread_id=thread_id,
    )
    request_settings_exact = (
        len(thread_requests) == 1
        and thread_requests[0] == expected_thread
        and len(turn_requests) == 1
        and turn_requests[0] == expected_turn
        and client_messages == [
            expected_initialize,
            {"method": "initialized"},
            expected_thread,
            expected_turn,
        ]
    )
    sandbox = dict(thread_result.get("sandbox")) if isinstance(thread_result.get("sandbox"), dict) else {}
    active_read_only = (
        str(sandbox.get("type") or "") == "readOnly"
        and sandbox.get("networkAccess") is False
        and thread_result.get("approvalPolicy") == "never"
        and thread_result.get("approvalsReviewer") == "user"
        and Path(str(thread_result.get("cwd") or "")).resolve() == root.resolve()
    )
    thread_started_notifications = [
        message
        for message in server_messages
        if str(message.get("method") or "") == "thread/started" and isinstance(message.get("params"), dict)
    ]
    notified_thread_raw = (
        dict(thread_started_notifications[0]["params"]).get("thread")
        if len(thread_started_notifications) == 1
        else None
    )
    notified_thread = dict(notified_thread_raw) if isinstance(notified_thread_raw, dict) else {}
    thread_identity_exact = (
        bool(thread_id)
        and bool(session_id)
        and thread_id == session_id
        and len(thread_started_notifications) == 1
        and str(notified_thread.get("id") or "").strip() == thread_id
        and str(notified_thread.get("sessionId") or "").strip() == session_id
        and notified_thread.get("source") == thread.get("source")
        and notified_thread.get("threadSource") == thread.get("threadSource")
    )
    session_source = str(thread.get("source") or "").strip()
    thread_source_exact = bool(session_source) and thread.get("threadSource") == _APP_SERVER_THREAD_SOURCE
    turn_started_notifications = [message for _, message in root_started_observations]
    notified_turn_params = (
        dict(turn_started_notifications[0]["params"]) if len(turn_started_notifications) == 1 else {}
    )
    notified_turn_raw = notified_turn_params.get("turn")
    notified_turn = dict(notified_turn_raw) if isinstance(notified_turn_raw, dict) else {}
    turn_identity_exact = (
        bool(turn_id)
        and str(started_turn.get("id") or "").strip() == turn_id
        and len(turn_started_notifications) == 1
        and str(notified_turn_params.get("threadId") or "").strip() == thread_id
        and str(notified_turn.get("id") or "").strip() == turn_id
        and str(completed_params.get("threadId") or "").strip() == thread_id
        and str(completed_turn.get("id") or "").strip() == turn_id
    )
    root_turn_start_response_exact = (
        bool(turn_id)
        and str(started_turn.get("id") or "").strip() == turn_id
        and started_turn.get("status") == "inProgress"
        and started_turn.get("error") is None
        and len(turn_started_notifications) == 1
        and str(notified_turn_params.get("threadId") or "").strip() == thread_id
        and str(notified_turn.get("id") or "").strip() == turn_id
        and notified_turn.get("status") == "inProgress"
        and notified_turn.get("error") is None
    )
    active_settings_exact = (
        active_settings.model == configured.settings.model
        and active_settings.reasoning_effort == configured.settings.reasoning_effort
        and _service_tier_matches(configured.settings.service_tier, active_settings.service_tier)
    )

    final_agent_message_exact = (
        len(root_final_observations) == 1
        and isinstance(final_item.get("id"), str)
        and bool(final_item["id"].strip())
        and isinstance(final_item.get("text"), str)
        and bool(final_item["text"].strip())
        and _normalize_trailing_newlines(final_text) == normalized_expected
        and bool(normalized_expected)
    )
    reroutes: list[dict[str, str]] = []
    for message in server_messages:
        if str(message.get("method") or "") != "model/rerouted" or not isinstance(message.get("params"), dict):
            continue
        params = dict(message["params"])
        reroutes.append(
            {
                "from_model": str(params.get("fromModel") or "").strip(),
                "to_model": str(params.get("toModel") or "").strip(),
            }
        )
    protocol_errors = [
        message
        for message in server_messages
        if "error" in message or str(message.get("method") or "") == "error"
    ]
    settings_consistent = True
    for message in server_messages:
        if str(message.get("method") or "") != "thread/settings/updated" or not isinstance(
            message.get("params"), dict
        ):
            continue
        params = dict(message["params"])
        settings_raw = params.get("threadSettings")
        settings = dict(settings_raw) if isinstance(settings_raw, dict) else {}
        observed = ReviewerSettings(
            model=str(settings.get("model") or "").strip(),
            reasoning_effort=str(settings.get("effort") or "").strip(),
            service_tier=_canonical_service_tier(settings.get("serviceTier")),
        )
        if (
            str(params.get("threadId") or "").strip() != thread_id
            or observed.model != active_settings.model
            or observed.reasoning_effort != active_settings.reasoning_effort
            or not _service_tier_matches(observed.service_tier, active_settings.service_tier)
        ):
            settings_consistent = False

    binding: dict[str, object] = {
        "thread_id": thread_id,
        "session_id": session_id,
        "turn_id": turn_id,
        "prompt_sha256": hashlib.sha256(prompt_raw).hexdigest(),
        "normalized_final_output_sha256": hashlib.sha256(normalized_expected.encode("utf-8")).hexdigest(),
        "transcript_json_valid": transcript_json_valid,
        "single_initialize_request": initialize_requests == [expected_initialize],
        "single_initialize_response": len(initialize_responses) == 1 and "result" in initialize_responses[0],
        "single_initialized_notification": initialized_notifications == [{"method": "initialized"}],
        "single_thread_start_request": len(thread_requests) == 1,
        "single_thread_start_response": len(thread_responses) == 1 and bool(thread_result),
        "single_turn_start_request": len(turn_requests) == 1,
        "single_turn_start_response": len(turn_responses) == 1 and bool(started_turn),
        "request_settings_exact": request_settings_exact,
        "active_settings_exact": active_settings_exact,
        "active_read_only": active_read_only,
        "thread_identity_exact": thread_identity_exact,
        "thread_source_exact": thread_source_exact,
        "turn_identity_exact": turn_identity_exact,
        "root_turn_start_response_exact": root_turn_start_response_exact,
        "single_turn_completed": len(completed_notifications) == 1,
        "turn_status_completed": completed_turn.get("status") == "completed" and completed_turn.get("error") is None,
        "prompt_request_exact": request_settings_exact,
        "final_agent_message_exact": final_agent_message_exact,
        **observed_turn_binding,
        "no_protocol_error": not protocol_errors,
        "no_model_reroute": not reroutes,
        "thread_settings_consistent": settings_consistent,
    }
    return AppServerTranscriptEvidence(
        settings=active_settings,
        thread_id=thread_id,
        session_id=session_id,
        session_source=session_source,
        turn_id=turn_id,
        final_output=final_output,
        binding=binding,
        model_reroutes=tuple(reroutes),
    )


def _app_server_transcript_binding_valid(binding: dict[str, object]) -> bool:
    return (
        all(binding.get(field) is True for field in _APP_SERVER_TRANSCRIPT_BOOLEAN_FIELDS)
        and all(str(binding.get(field) or "").strip() for field in ("thread_id", "session_id", "turn_id"))
        and type(binding.get("observed_turn_count")) is int
        and int(binding["observed_turn_count"]) >= 1
    )


def _resolve_codex_executable(*, caller_path: str | None = None) -> Path:
    path_value = os.environ.get("PATH", "") if caller_path is None else caller_path
    candidate = shutil.which("codex", path=path_value)
    if not candidate:
        raise RuntimeError("Codex executable was not found or is not executable on caller PATH")
    candidate_path = Path(candidate).expanduser()
    if not candidate_path.is_absolute():
        candidate_path = Path.cwd() / candidate_path
    try:
        resolved = candidate_path.resolve(strict=True)
    except OSError as exc:
        raise RuntimeError(f"Codex executable selected from caller PATH is unavailable: {candidate_path}") from exc
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise RuntimeError(f"Codex executable selected from caller PATH is not executable: {resolved}")
    return resolved


def _build_app_server_args(codex_executable: Path) -> list[str]:
    if not codex_executable.is_absolute():
        raise RuntimeError("Codex app-server executable must be an absolute path")
    return [str(codex_executable), "--sandbox", "read-only", "app-server", "--strict-config", "--stdio"]


def _run_app_server_review(
    *,
    root: Path,
    configured: ReviewerConfiguration,
    codex_executable: Path,
    prompt_raw: bytes,
    timeout_seconds: int,
) -> AppServerReviewResult:
    try:
        prompt = prompt_raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError("independent review prompt is not valid UTF-8") from exc
    args = _build_app_server_args(codex_executable)
    transcript = bytearray()
    deadline = time.monotonic() + max(1, timeout_seconds)

    with tempfile.TemporaryFile() as stderr_file:
        process = subprocess.Popen(
            args,
            cwd=root,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr_file,
            text=False,
            bufsize=0,
        )
        if process.stdin is None or process.stdout is None:
            process.kill()
            raise RuntimeError("Codex app-server did not expose stdio pipes")
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        stdout_buffer = bytearray()

        def record(direction: str, message: dict[str, object]) -> None:
            transcript.extend(_canonical_json_line({"direction": direction, "message": message}))

        def send(message: dict[str, object]) -> None:
            record("client", message)
            process.stdin.write(_canonical_json_line(message))
            process.stdin.flush()

        def receive() -> dict[str, object]:
            while b"\n" not in stdout_buffer:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(args, timeout_seconds, output=bytes(transcript))
                ready = selector.select(remaining)
                if not ready:
                    raise subprocess.TimeoutExpired(args, timeout_seconds, output=bytes(transcript))
                chunk = os.read(process.stdout.fileno(), 65536)
                if not chunk:
                    raise RuntimeError(f"Codex app-server exited before review completion (status {process.poll()})")
                stdout_buffer.extend(chunk)
            raw_line, _, remainder = stdout_buffer.partition(b"\n")
            stdout_buffer.clear()
            stdout_buffer.extend(remainder)
            try:
                message = json.loads(raw_line)
            except (TypeError, ValueError) as exc:
                raise RuntimeError("Codex app-server emitted invalid JSON") from exc
            if not isinstance(message, dict):
                raise RuntimeError("Codex app-server emitted a non-object JSON message")
            message = dict(message)
            record("server", message)
            if "id" in message and "method" in message:
                raise RuntimeError(f"Codex app-server requested unsupported client action {message.get('method')!r}")
            return message

        def receive_response(request_id: str) -> dict[str, object]:
            while True:
                message = receive()
                if str(message.get("id") or "") != request_id:
                    continue
                if "error" in message:
                    raise RuntimeError(f"Codex app-server request {request_id!r} failed: {message['error']!r}")
                result = message.get("result")
                if not isinstance(result, dict):
                    raise RuntimeError(f"Codex app-server request {request_id!r} returned no object result")
                return dict(result)

        try:
            initialize, thread_start, _ = _app_server_requests(
                root=root,
                configured=configured,
                prompt=prompt,
            )
            send(initialize)
            receive_response(_APP_SERVER_INITIALIZE_ID)
            send({"method": "initialized"})
            send(thread_start)
            thread_result = receive_response(_APP_SERVER_THREAD_START_ID)
            thread_raw = thread_result.get("thread")
            thread = dict(thread_raw) if isinstance(thread_raw, dict) else {}
            thread_id = str(thread.get("id") or "").strip()
            if not thread_id:
                raise RuntimeError("Codex app-server thread/start returned no thread id")
            _, _, turn_start = _app_server_requests(
                root=root,
                configured=configured,
                prompt=prompt,
                thread_id=thread_id,
            )
            send(turn_start)
            turn_result = receive_response(_APP_SERVER_TURN_START_ID)
            turn_raw = turn_result.get("turn")
            turn = dict(turn_raw) if isinstance(turn_raw, dict) else {}
            turn_id = str(turn.get("id") or "").strip()
            if not turn_id:
                raise RuntimeError("Codex app-server turn/start returned no turn id")
            while True:
                message = receive()
                method = str(message.get("method") or "")
                if method == "model/rerouted":
                    raise RuntimeError("Codex app-server recorded model/rerouted; rerouted reviews fail closed")
                params_raw = message.get("params")
                params = dict(params_raw) if isinstance(params_raw, dict) else {}
                completed_turn_raw = params.get("turn")
                completed_turn = dict(completed_turn_raw) if isinstance(completed_turn_raw, dict) else {}
                if (
                    method == "turn/completed"
                    and str(params.get("threadId") or "").strip() == thread_id
                    and str(completed_turn.get("id") or "").strip() == turn_id
                ):
                    break
            process.stdin.close()
            try:
                returncode = process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    returncode = process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    returncode = process.wait(timeout=2)
        except BaseException as exc:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
            stderr_file.seek(0)
            stderr = stderr_file.read().decode("utf-8", errors="replace")
            if isinstance(exc, subprocess.TimeoutExpired):
                exc.output = bytes(transcript)
                exc.stderr = stderr
                raise
            if not isinstance(exc, (OSError, RuntimeError)):
                raise
            raise AppServerReviewError(
                str(exc),
                transcript_raw=bytes(transcript),
                stderr=stderr,
            ) from exc
        finally:
            selector.close()
        stderr_file.seek(0)
        stderr = stderr_file.read().decode("utf-8", errors="replace")

    evidence = _parse_app_server_transcript(
        transcript_raw=bytes(transcript),
        configured=configured,
        root=root,
        prompt_raw=prompt_raw,
    )
    if not _app_server_transcript_binding_valid(evidence.binding):
        failed = [field for field in _APP_SERVER_TRANSCRIPT_BOOLEAN_FIELDS if evidence.binding.get(field) is not True]
        raise AppServerReviewError(
            "Codex app-server transcript failed closed: " + ", ".join(failed),
            transcript_raw=bytes(transcript),
            stderr=stderr,
        )
    return AppServerReviewResult(
        args=tuple(args),
        returncode=int(returncode),
        transcript_raw=bytes(transcript),
        stderr=stderr,
        evidence=evidence,
    )


def _event_kind_and_payload(event: dict[str, object]) -> tuple[str, dict[str, object]]:
    raw_payload = event.get("payload")
    payload: dict[str, object] = dict(raw_payload) if isinstance(raw_payload, dict) else {}
    if str(event.get("type") or "") == "event_msg":
        return str(payload.get("type") or "").strip(), payload
    return str(event.get("type") or "").strip(), payload


def _response_item_message_text(payload: dict[str, object], *, role: str) -> str | None:
    if str(payload.get("type") or "") != "message" or str(payload.get("role") or "") != role:
        return None
    content = payload.get("content")
    if not isinstance(content, list) or not content:
        return None
    expected_part_type = "input_text" if role == "user" else "output_text"
    parts: list[str] = []
    for raw_part in content:
        if not isinstance(raw_part, dict):
            return None
        part = dict(raw_part)
        if str(part.get("type") or "") != expected_part_type or not isinstance(part.get("text"), str):
            return None
        parts.append(str(part["text"]))
    return "".join(parts)


def _normalize_trailing_newlines(value: str) -> str:
    return value.rstrip("\r\n")


def _review_causal_binding(
    *,
    rollout_raw: bytes,
    prompt_raw: bytes,
    raw_output: bytes,
    expected_thread_id: str,
    expected_session_source: str = "exec",
    session_source_field: str = "session_source_exec",
    expected_thread_source: str = "",
) -> dict[str, object]:
    rollout_json_valid = True
    text_utf8_valid = True
    try:
        prompt = prompt_raw.decode("utf-8")
        final_output = raw_output.decode("utf-8")
    except UnicodeDecodeError:
        prompt = ""
        final_output = ""
        text_utf8_valid = False
    normalized_final = _normalize_trailing_newlines(final_output)
    session_sources: list[str] = []
    thread_sources: list[str] = []
    task_starts: list[tuple[int, str]] = []
    task_completes: list[tuple[int, str, str]] = []
    abort_seen = False
    prompt_response_items: list[tuple[int, str]] = []
    prompt_event_messages: list[tuple[int, str]] = []
    final_response_items: list[tuple[int, str]] = []
    final_event_messages: list[tuple[int, str]] = []

    for line_number, raw_line in enumerate(rollout_raw.decode("utf-8", errors="replace").splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            event = json.loads(raw_line)
        except (TypeError, ValueError):
            rollout_json_valid = False
            continue
        if not isinstance(event, dict):
            rollout_json_valid = False
            continue
        kind, payload = _event_kind_and_payload(event)
        if kind == "session_meta":
            session_id = str(payload.get("id") or payload.get("session_id") or "").strip()
            if session_id == expected_thread_id:
                source = payload.get("source")
                session_sources.append(source if isinstance(source, str) else "")
                thread_sources.append(str(payload.get("thread_source") or "").strip())
        elif kind == "task_started":
            task_starts.append((line_number, str(payload.get("turn_id") or "").strip()))
        elif kind == "task_complete":
            task_completes.append(
                (
                    line_number,
                    str(payload.get("turn_id") or "").strip(),
                    str(payload.get("last_agent_message") or ""),
                )
            )
        elif "abort" in kind.lower():
            abort_seen = True
        elif kind == "response_item":
            user_text = _response_item_message_text(payload, role="user")
            if user_text is not None:
                prompt_response_items.append((line_number, user_text))
            assistant_text = _response_item_message_text(payload, role="assistant")
            if assistant_text is not None and str(payload.get("phase") or "") == "final_answer":
                final_response_items.append((line_number, assistant_text))
        elif kind == "user_message" and isinstance(payload.get("message"), str):
            prompt_event_messages.append((line_number, str(payload["message"])))
        elif (
            kind == "agent_message"
            and str(payload.get("phase") or "") == "final_answer"
            and isinstance(payload.get("message"), str)
        ):
            final_event_messages.append((line_number, str(payload["message"])))

    single_task_turn = (
        len(task_starts) == 1
        and len(task_completes) == 1
        and bool(task_starts[0][1])
        and task_starts[0][1] == task_completes[0][1]
        and task_starts[0][0] < task_completes[0][0]
    )
    turn_id = task_starts[0][1] if single_task_turn else ""
    start_line = task_starts[0][0] if single_task_turn else -1
    complete_line = task_completes[0][0] if single_task_turn else -1

    def exact_between(
        observations: list[tuple[int, str]],
        expected: str,
        *,
        normalize_newlines: bool = False,
        allow_response_item_memory_annotation: bool = False,
    ) -> bool:
        window_observations = [
            observed for line_number, observed in observations if start_line < line_number < complete_line
        ]
        matching = [
            observed
            for observed in window_observations
            if (
                independent_review_response_item_matches_raw_output(observed, expected)
                if allow_response_item_memory_annotation
                else _normalize_trailing_newlines(observed) == _normalize_trailing_newlines(expected)
                if normalize_newlines
                else observed == expected
            )
        ]
        return single_task_turn and len(window_observations) == 1 and len(matching) == 1

    task_complete_final_exact = (
        single_task_turn
        and _normalize_trailing_newlines(task_completes[0][2]) == normalized_final
        and bool(normalized_final)
    )
    binding: dict[str, object] = {
        "turn_id": turn_id,
        "prompt_sha256": hashlib.sha256(prompt_raw).hexdigest(),
        "normalized_final_output_sha256": hashlib.sha256(normalized_final.encode("utf-8")).hexdigest(),
        "rollout_json_valid": rollout_json_valid,
        "text_utf8_valid": text_utf8_valid,
        session_source_field: session_sources == [expected_session_source],
        "single_task_turn": single_task_turn,
        "no_abort": not abort_seen,
        "prompt_response_item_exact": exact_between(prompt_response_items, prompt),
        "prompt_event_message_exact": exact_between(prompt_event_messages, prompt),
        "final_response_item_exact": exact_between(
            final_response_items,
            final_output,
            normalize_newlines=True,
            allow_response_item_memory_annotation=(
                expected_session_source == "vscode" and expected_thread_source == _APP_SERVER_THREAD_SOURCE
            ),
        ),
        "final_event_message_exact": exact_between(
            final_event_messages,
            final_output,
            normalize_newlines=True,
        ),
        "task_complete_final_exact": task_complete_final_exact,
    }
    if expected_thread_source:
        binding["session_thread_source_exact"] = thread_sources == [expected_thread_source]
    return binding


def _review_causal_binding_valid(binding: dict[str, object]) -> bool:
    return bool(binding.get("turn_id")) and all(binding.get(field) is True for field in _CAUSAL_BINDING_BOOLEAN_FIELDS)


def _review_app_server_causal_binding(
    *,
    rollout_raw: bytes,
    prompt_raw: bytes,
    raw_output: bytes,
    expected_thread_id: str,
    expected_session_source: str,
) -> dict[str, object]:
    return _review_causal_binding(
        rollout_raw=rollout_raw,
        prompt_raw=prompt_raw,
        raw_output=raw_output,
        expected_thread_id=expected_thread_id,
        expected_session_source=expected_session_source,
        session_source_field="session_source_matches_thread_start",
        expected_thread_source=_APP_SERVER_THREAD_SOURCE,
    )


def _review_app_server_causal_binding_valid(binding: dict[str, object]) -> bool:
    return bool(binding.get("turn_id")) and all(
        binding.get(field) is True for field in _APP_SERVER_CAUSAL_BINDING_BOOLEAN_FIELDS
    )


def _first_mapping_value(mappings: list[dict[str, object]], keys: tuple[str, ...]) -> str:
    for mapping in mappings:
        for key in keys:
            value = mapping.get(key)
            if isinstance(value, dict):
                value = value.get("model") or value.get("value")
            normalized = str(value or "").strip()
            if normalized:
                return normalized
    return ""


def _settings_observation(kind: str, payload: dict[str, object]) -> dict[str, str]:
    if kind not in _ROLLOUT_SETTING_EVENT_TYPES:
        return {}
    containers: list[dict[str, object]] = []
    for key in ("thread_settings", "session_config", "settings", "config"):
        value = payload.get(key)
        if isinstance(value, dict):
            containers.append(dict(value))
    containers.append(payload)
    collaboration_mode = payload.get("collaboration_mode")
    if isinstance(collaboration_mode, dict) and isinstance(collaboration_mode.get("settings"), dict):
        containers.append(dict(collaboration_mode["settings"]))
    model = _first_mapping_value(containers, ("model",))
    reasoning_effort = _first_mapping_value(
        containers,
        ("reasoning_effort", "model_reasoning_effort", "effort"),
    )
    service_tier = _canonical_service_tier(_first_mapping_value(containers, ("service_tier",)))
    return {
        key: value
        for key, value in (
            ("model", model),
            ("reasoning_effort", reasoning_effort),
            ("service_tier", service_tier),
        )
        if value
    }


def _model_reroute(payload: dict[str, object]) -> tuple[str, str]:
    containers = [payload]
    for key in ("reroute", "model_reroute"):
        value = payload.get(key)
        if isinstance(value, dict):
            containers.insert(0, dict(value))
    from_model = _first_mapping_value(
        containers,
        ("from_model", "source_model", "previous_model", "original_model", "from"),
    )
    to_model = _first_mapping_value(
        containers,
        ("to_model", "target_model", "new_model", "rerouted_model", "to"),
    )
    return from_model, to_model


def _load_effective_reviewer_configuration(
    *,
    events_text: str,
    configured: ReviewerConfiguration,
    codex_home: Path | None = None,
) -> EffectiveReviewerConfiguration:
    thread_id = _thread_id_from_events(events_text)
    if not thread_id:
        raise RuntimeError("Codex JSON events did not report thread.started/thread_id")
    rollout_path = _rollout_path_for_thread(thread_id, codex_home=codex_home or _codex_home())
    if rollout_path is None:
        raise RuntimeError(f"Codex rollout for thread {thread_id} was not persisted")
    rollout_raw = rollout_path.read_bytes()
    state: dict[str, str] = {}
    sources: dict[str, list[str]] = {"model": [], "reasoning_effort": [], "service_tier": []}
    reroutes: list[dict[str, str]] = []
    session_ids: set[str] = set()
    cli_versions: set[str] = set()
    for line_number, raw_line in enumerate(rollout_raw.decode("utf-8", errors="replace").splitlines(), start=1):
        try:
            event = json.loads(raw_line)
        except (TypeError, ValueError):
            continue
        if not isinstance(event, dict):
            continue
        kind, payload = _event_kind_and_payload(event)
        if kind == "session_meta":
            session_id = str(payload.get("id") or payload.get("session_id") or "").strip()
            cli_version = str(payload.get("cli_version") or "").strip()
            if session_id:
                session_ids.add(session_id)
            if cli_version:
                cli_versions.add(cli_version)
            continue
        if kind == "model_reroute":
            from_model, to_model = _model_reroute(payload)
            if not from_model or not to_model:
                raise RuntimeError(
                    f"Codex rollout {rollout_path} has ambiguous model_reroute metadata at line {line_number}"
                )
            current_model = state.get("model") or configured.settings.model
            if from_model != current_model:
                raise RuntimeError(
                    f"Codex rollout {rollout_path} has conflicting model_reroute source {from_model!r}; "
                    f"current model is {current_model!r}"
                )
            state["model"] = to_model
            sources["model"].append("model_reroute")
            reroutes.append({"from_model": from_model, "to_model": to_model})
            continue
        observation = _settings_observation(kind, payload)
        for field, value in observation.items():
            previous = state.get(field, "")
            values_match = (
                _service_tier_matches(previous, value) if field == "service_tier" and previous else previous == value
            )
            if previous and not values_match:
                raise RuntimeError(
                    f"Codex rollout {rollout_path} has conflicting effective {field} metadata: "
                    f"{previous!r} versus {value!r}"
                )
            state[field] = _canonical_service_tier(value) if field == "service_tier" else value
            if kind not in sources[field]:
                sources[field].append(kind)
    if len(session_ids) > 1:
        raise RuntimeError(f"Codex rollout {rollout_path} has conflicting session ids: {sorted(session_ids)!r}")
    if session_ids and session_ids != {thread_id}:
        raise RuntimeError(
            f"Codex rollout {rollout_path} session id {next(iter(session_ids))!r} "
            f"does not match reviewer thread id {thread_id!r}"
        )
    if len(cli_versions) > 1:
        raise RuntimeError(f"Codex rollout {rollout_path} has conflicting Codex CLI versions: {sorted(cli_versions)!r}")
    if reroutes:
        raise RuntimeError(f"Codex rollout {rollout_path} recorded model_reroute; rerouted reviews fail closed")
    effective = ReviewerSettings(
        model=state.get("model", ""),
        reasoning_effort=state.get("reasoning_effort", ""),
        service_tier=state.get("service_tier", ""),
    )
    if any(value.lower() in _INHERITED_VALUE_SENTINELS for value in effective) or any(
        not sources[field] for field in sources
    ):
        raise RuntimeError(f"Codex rollout {rollout_path} did not record complete effective reviewer settings")
    if effective.model != configured.settings.model:
        raise RuntimeError(
            f"effective reviewer model {effective.model!r} differs from global config {configured.settings.model!r}"
        )
    if effective.reasoning_effort != configured.settings.reasoning_effort:
        raise RuntimeError(
            "effective reviewer reasoning effort "
            f"{effective.reasoning_effort!r} differs from global config {configured.settings.reasoning_effort!r}"
        )
    if not _service_tier_matches(configured.settings.service_tier, effective.service_tier):
        raise RuntimeError(
            f"effective reviewer service tier {effective.service_tier!r} differs from global config "
            f"{configured.settings.service_tier!r}"
        )
    return EffectiveReviewerConfiguration(
        settings=effective,
        thread_id=thread_id,
        rollout_path=rollout_path,
        session_id=next(iter(session_ids), thread_id),
        codex_cli_version=next(iter(cli_versions), ""),
        source=sources,
        rollout_sha256=hashlib.sha256(rollout_raw).hexdigest(),
        model_reroutes=tuple(reroutes),
    )


def _load_app_server_effective_reviewer_configuration(
    *,
    transcript: AppServerTranscriptEvidence,
    configured: ReviewerConfiguration,
    codex_home: Path | None = None,
) -> EffectiveReviewerConfiguration:
    if not _app_server_transcript_binding_valid(transcript.binding):
        raise RuntimeError("Codex app-server transcript did not bind a complete review turn")
    active = transcript.settings
    if any(not value or value.lower() in _INHERITED_VALUE_SENTINELS for value in active):
        raise RuntimeError("Codex app-server thread/start did not report complete active reviewer settings")
    if active.model != configured.settings.model:
        raise RuntimeError(
            f"active reviewer model {active.model!r} differs from global config {configured.settings.model!r}"
        )
    if active.reasoning_effort != configured.settings.reasoning_effort:
        raise RuntimeError(
            "active reviewer reasoning effort "
            f"{active.reasoning_effort!r} differs from global config {configured.settings.reasoning_effort!r}"
        )
    if not _service_tier_matches(configured.settings.service_tier, active.service_tier):
        raise RuntimeError(
            f"active reviewer service tier {active.service_tier!r} differs from global config "
            f"{configured.settings.service_tier!r}"
        )

    rollout_path = _rollout_path_for_thread(
        transcript.thread_id,
        codex_home=codex_home or _codex_home(),
    )
    if rollout_path is None:
        raise RuntimeError(f"Codex rollout for thread {transcript.thread_id} was not persisted")
    rollout_raw = rollout_path.read_bytes()
    sources: dict[str, list[str]] = {
        "model": ["app_server_thread_start_response"],
        "reasoning_effort": ["app_server_thread_start_response"],
        "service_tier": ["app_server_thread_start_response"],
    }
    observed_fields: set[str] = set()
    reroutes: list[dict[str, str]] = []
    rollout_ids: set[str] = set()
    rollout_session_ids: set[str] = set()
    cli_versions: set[str] = set()
    session_sources: set[str] = set()
    thread_sources: set[str] = set()
    root_lineage_valid = True
    rollout_json_valid = True
    for line_number, raw_line in enumerate(rollout_raw.decode("utf-8", errors="replace").splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            event = json.loads(raw_line)
        except (TypeError, ValueError):
            rollout_json_valid = False
            continue
        if not isinstance(event, dict):
            rollout_json_valid = False
            continue
        kind, payload = _event_kind_and_payload(event)
        if kind == "session_meta":
            rollout_id = str(payload.get("id") or "").strip()
            rollout_session_id = str(payload.get("session_id") or "").strip()
            cli_version = str(payload.get("cli_version") or "").strip()
            source = str(payload.get("source") or "").strip()
            thread_source = str(payload.get("thread_source") or "").strip()
            if rollout_id:
                rollout_ids.add(rollout_id)
            if rollout_session_id:
                rollout_session_ids.add(rollout_session_id)
            if cli_version:
                cli_versions.add(cli_version)
            if source:
                session_sources.add(source)
            if thread_source:
                thread_sources.add(thread_source)
            if str(payload.get("parent_thread_id") or "").strip() or str(
                payload.get("forked_from_id") or ""
            ).strip():
                root_lineage_valid = False
            continue
        if kind == "model_reroute":
            from_model, to_model = _model_reroute(payload)
            if not from_model or not to_model:
                raise RuntimeError(
                    f"Codex rollout {rollout_path} has ambiguous model_reroute metadata at line {line_number}"
                )
            reroutes.append({"from_model": from_model, "to_model": to_model})
            continue
        observation = _settings_observation(kind, payload)
        for field, value in observation.items():
            expected = getattr(active, field)
            matches = _service_tier_matches(expected, value) if field == "service_tier" else expected == value
            if not matches:
                raise RuntimeError(
                    f"Codex rollout {rollout_path} effective {field} {value!r} differs from "
                    f"thread/start active value {expected!r}"
                )
            observed_fields.add(field)
            if kind not in sources[field]:
                sources[field].append(kind)

    if not rollout_json_valid:
        raise RuntimeError(f"Codex rollout {rollout_path} contains invalid JSON")
    if rollout_ids != {transcript.thread_id}:
        raise RuntimeError(
            f"Codex rollout {rollout_path} identity {sorted(rollout_ids)!r} does not match "
            f"thread/start id {transcript.thread_id!r}"
        )
    if rollout_session_ids != {transcript.session_id}:
        raise RuntimeError(
            f"Codex rollout {rollout_path} session identity {sorted(rollout_session_ids)!r} does not match "
            f"thread/start session {transcript.session_id!r}"
        )
    if not root_lineage_valid:
        raise RuntimeError(f"Codex rollout {rollout_path} is not an independent root reviewer session")
    if session_sources != {transcript.session_source}:
        raise RuntimeError(
            f"Codex rollout {rollout_path} source {sorted(session_sources)!r} does not match "
            f"thread/start source {transcript.session_source!r}"
        )
    if thread_sources != {_APP_SERVER_THREAD_SOURCE}:
        raise RuntimeError(
            f"Codex rollout {rollout_path} did not record dedicated thread source {_APP_SERVER_THREAD_SOURCE!r}"
        )
    if len(cli_versions) != 1:
        raise RuntimeError(f"Codex rollout {rollout_path} did not record exactly one Codex CLI version")
    if reroutes or transcript.model_reroutes:
        raise RuntimeError(f"Codex review for thread {transcript.thread_id} recorded a model reroute")
    missing_rollout_fields = {"model", "reasoning_effort"} - observed_fields
    if missing_rollout_fields:
        raise RuntimeError(
            f"Codex rollout {rollout_path} did not corroborate active reviewer settings: "
            + ", ".join(sorted(missing_rollout_fields))
        )
    return EffectiveReviewerConfiguration(
        settings=active,
        thread_id=transcript.thread_id,
        rollout_path=rollout_path,
        session_id=transcript.session_id,
        codex_cli_version=next(iter(cli_versions)),
        source=sources,
        rollout_sha256=hashlib.sha256(rollout_raw).hexdigest(),
        model_reroutes=(),
    )


def _effective_config_payload(
    *,
    configured: ReviewerConfiguration,
    effective: EffectiveReviewerConfiguration,
    reviewer_exit_code: int,
    scope: dict[str, object],
    prompt_path: Path,
    prompt_sha256: str,
    events_path: Path,
    events_sha256: str,
    raw_output_path: Path,
    raw_output_sha256: str,
    root: Path,
    causal_binding: dict[str, object] | None = None,
    transcript_binding: dict[str, object] | None = None,
    turn_id: str = "",
    session_source: str = "",
) -> dict[str, object]:
    if not effective.session_id or not effective.thread_id:
        raise RuntimeError("Codex rollout did not record complete session/thread identity")
    if not effective.codex_cli_version:
        raise RuntimeError("Codex rollout did not record the effective Codex CLI version")
    return {
        "contract_version": _EFFECTIVE_CONFIG_CONTRACT_VERSION,
        "config": {
            "path": str(configured.path),
            "sha256": configured.sha256,
            "model": configured.settings.model,
            "reasoning_effort": configured.settings.reasoning_effort,
            "service_tier": configured.settings.service_tier,
        },
        "session": {
            "session_id": effective.session_id,
            "thread_id": effective.thread_id,
            "turn_id": turn_id,
            "session_source": session_source,
            "codex_cli_version": effective.codex_cli_version,
            "rollout_path": _workspace_path(root, effective.rollout_path),
            "rollout_sha256": effective.rollout_sha256,
        },
        "process": {"reviewer_exit_code": int(reviewer_exit_code), "timed_out": False},
        "effective": {
            "model": effective.settings.model,
            "reasoning_effort": effective.settings.reasoning_effort,
            "service_tier": effective.settings.service_tier,
            "source": effective.source,
        },
        "model_reroutes": list(effective.model_reroutes),
        "causal_binding": dict(causal_binding or {}),
        "transport": {
            "kind": _APP_SERVER_TRANSPORT,
            "protocol": "codex_app_server_jsonrpc_v2",
            "active_settings_source": "thread/start.response",
            "transcript": {
                "path": _workspace_path(root, events_path),
                "sha256": events_sha256,
            },
            "binding": dict(transcript_binding or {}),
        },
        "scope": scope,
        "artifacts": {
            "prompt": {"path": _workspace_path(root, prompt_path), "sha256": prompt_sha256},
            "events": {"path": _workspace_path(root, events_path), "sha256": events_sha256},
            "transcript": {"path": _workspace_path(root, events_path), "sha256": events_sha256},
            "raw_output": {"path": _workspace_path(root, raw_output_path), "sha256": raw_output_sha256},
        },
    }


def _write_effective_config_evidence(
    *,
    path: Path,
    configured: ReviewerConfiguration,
    effective: EffectiveReviewerConfiguration,
    reviewer_exit_code: int,
    scope: dict[str, object],
    prompt_path: Path,
    prompt_sha256: str,
    events_path: Path,
    events_sha256: str,
    raw_output_path: Path,
    raw_output_sha256: str,
    root: Path,
    causal_binding: dict[str, object],
    transcript_binding: dict[str, object],
    turn_id: str,
    session_source: str,
) -> str:
    payload = _effective_config_payload(
        configured=configured,
        effective=effective,
        reviewer_exit_code=reviewer_exit_code,
        scope=scope,
        prompt_path=prompt_path,
        prompt_sha256=prompt_sha256,
        events_path=events_path,
        events_sha256=events_sha256,
        raw_output_path=raw_output_path,
        raw_output_sha256=raw_output_sha256,
        root=root,
        causal_binding=causal_binding,
        transcript_binding=transcript_binding,
        turn_id=turn_id,
        session_source=session_source,
    )
    raw = (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _workspace_path(root: Path, path: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        return resolved.relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(resolved)


def _build_prompt(*, root: Path, scope: dict[str, object], extra_context: str) -> str:
    brief = (root / "docs" / "INDEPENDENT_REVIEW_BRIEF.md").read_text(encoding="utf-8")
    files = list(scope.get("files") or [])
    file_list = "\n".join(f"- `{path}`" for path in files) if files else "- Review the full pinned commit diff."
    return (
        f"{brief}\n\n"
        "## Review Scope\n\n"
        f"Title: {scope['title']}\n\n"
        f"Base/ref: {scope['base_ref']}\n\n"
        f"Resolved base commit: {scope['resolved_base_commit']}\n\n"
        f"Resolved head commit: {scope['resolved_head_commit']}\n\n"
        f"Scope mode: {scope['scope_mode']}\n\n"
        f"Scope digest: {scope['scope_digest_sha256']}\n\n"
        "Files or scope:\n"
        f"{file_list}\n\n"
        "Relevant repository contracts to check first:\n"
        "- `AGENTS.md`\n"
        "- `docs/PRE_AGENT_CONTRACT_REVIEW.md`\n"
        "- `docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`\n"
        "- `docs/INDEPENDENT_REVIEW_GATE.md`\n\n"
        f"Additional context:\n{extra_context.strip() or 'None.'}\n\n"
        "Run read-only inspection commands as needed. Do not edit files. Inspect the exact pinned Git objects above: "
        f"from this project root, use `git diff {scope['resolved_base_commit']} "
        f"{scope['resolved_head_commit']} -- ./<project-relative-path>` and "
        f"`git show {scope['resolved_head_commit']}:./<project-relative-path>`; do not substitute current "
        "working-tree contents. "
        "This is a scoped independent review, not a full development-session resume: use targeted "
        "`git diff -- <files>`, `rg`, and line-range reads for the listed scope and relevant Contract "
        "sections. Do not read full PROGRESS.md, full docs/NEXT_TODO.md, or full long Contract files "
        "unless the review scope explicitly requires that full context. Prioritize blocking correctness, "
        "contract, provider-cost, fallback, migration, and runtime risks."
    )


def _artifact_header(
    *,
    root: Path,
    scope_evidence: dict[str, object],
    configured: ReviewerConfiguration,
    effective: EffectiveReviewerConfiguration,
    timeout_seconds: int,
    prompt_path: Path,
    events_path: Path,
    effective_config_path: Path,
    effective_config_sha256: str,
    prompt_sha256: str,
    events_sha256: str,
    raw_output_path: Path,
    raw_output_sha256: str,
    reviewer_exit_code: int,
    codex_executable: Path,
    shell_command: str,
    transcript: AppServerTranscriptEvidence,
) -> str:
    files = list(scope_evidence.get("files") or [])
    scope = "\n".join(f"- `{path}`" for path in files) if files else "- Current uncommitted diff."
    return (
        "## Review Metadata\n\n"
        f"- title: {scope_evidence['title']}\n"
        f"- base/ref: {scope_evidence['base_ref']}\n"
        f"- reviewer_model: {effective.settings.model}\n"
        f"- reviewer_reasoning_effort: {effective.settings.reasoning_effort}\n"
        f"- reviewer_service_tier: {effective.settings.service_tier}\n"
        "- reviewer_configuration_mode: operator config values requested explicitly; active values verified "
        "from thread/start response\n"
        f"- reviewer_transport: {_APP_SERVER_TRANSPORT}\n"
        f"- reviewer_config_path: `{configured.path}`\n"
        f"- reviewer_config_sha256: {configured.sha256}\n"
        f"- reviewer_exit_code: {reviewer_exit_code}\n"
        f"- reviewer_codex_executable: `{codex_executable}`\n"
        f"- reviewer_codex_cli_version: {effective.codex_cli_version}\n"
        f"- reviewer_session_id: {effective.session_id}\n"
        f"- reviewer_session_source: {transcript.session_source}\n"
        f"- reviewer_thread_id: {effective.thread_id}\n"
        f"- reviewer_turn_id: {transcript.turn_id}\n"
        f"- reviewer_rollout_path: `{_workspace_path(root, effective.rollout_path)}`\n"
        f"- reviewer_rollout_sha256: {effective.rollout_sha256}\n"
        f"- reviewer_effective_config_path: `{_workspace_path(root, effective_config_path)}`\n"
        f"- reviewer_effective_config_sha256: {effective_config_sha256}\n"
        f"- review_scope_mode: {scope_evidence['scope_mode']}\n"
        f"- review_base_ref: {scope_evidence['base_ref']}\n"
        f"- review_resolved_base_commit: {scope_evidence['resolved_base_commit']}\n"
        f"- review_resolved_head_commit: {scope_evidence['resolved_head_commit']}\n"
        f"- review_git_diff_sha256: {scope_evidence['git_diff_sha256']}\n"
        f"- review_git_tree_sha256: {scope_evidence['git_tree_sha256']}\n"
        f"- review_extra_context_sha256: {scope_evidence['extra_context_sha256']}\n"
        f"- review_scope_digest_sha256: {scope_evidence['scope_digest_sha256']}\n"
        f"- timeout_seconds: {timeout_seconds}\n"
        f"- prompt_path: `{_workspace_path(root, prompt_path)}`\n"
        f"- prompt_sha256: {prompt_sha256}\n"
        f"- events_path: `{_workspace_path(root, events_path)}`\n"
        f"- events_sha256: {events_sha256}\n"
        f"- raw_output_path: `{_workspace_path(root, raw_output_path)}`\n"
        f"- raw_output_sha256: {raw_output_sha256}\n"
        f"- command: `{shell_command}`\n"
        "- contract_docs_considered: `AGENTS.md`, `docs/PRE_AGENT_CONTRACT_REVIEW.md`, "
        "`docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`, `docs/INDEPENDENT_REVIEW_GATE.md`\n"
        "- author_validation: see latest assistant handoff/progress note and targeted test output for this run\n"
        "- accepted_exceptions: none recorded by this runner\n"
        "\nReviewed scope:\n"
        f"{scope}\n\n"
        "## Reviewer Output\n\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate or execute the independent review gate prompt.")
    parser.add_argument("--title", default="", help="Review title.")
    parser.add_argument("--files", default="", help="Shell-style space-separated file list.")
    parser.add_argument("--base", default="", help="Optional base branch/ref for reviewer context.")
    parser.add_argument("--extra-context", default="", help="Additional review context.")
    parser.add_argument("--timeout-seconds", type=int, default=420, help="Hard timeout for --execute.")
    parser.add_argument("--output", default="", help="Review output file.")
    parser.add_argument("--prompt-output", default="", help="Prompt output file for dry-run/use elsewhere.")
    parser.add_argument("--execute", action="store_true", help="Run Codex app-server in read-only mode.")
    args = parser.parse_args()

    root = _repo_root()
    try:
        configured = _load_reviewer_configuration()
    except (OSError, RuntimeError, tomllib.TOMLDecodeError) as exc:
        parser.error(str(exc))
    try:
        files = normalize_independent_review_files(_split_files(args.files))
        scope_evidence = build_independent_review_scope_evidence(
            workspace_root=root,
            title=args.title,
            base_ref=args.base,
            files=files,
            extra_context=args.extra_context,
        )
    except ValueError as exc:
        parser.error(str(exc))
    prompt = _build_prompt(root=root, scope=scope_evidence, extra_context=args.extra_context)
    review_dir = root / "runtime" / "reviews"
    review_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_title = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(scope_evidence["title"]))[:80]
    output_path = (
        (root / args.output).resolve() if args.output and not Path(args.output).is_absolute() else Path(args.output)
    )
    if not args.output:
        output_path = review_dir / f"{stamp}_{safe_title}.md"
    prompt_path = (
        (root / args.prompt_output).resolve()
        if args.prompt_output and not Path(args.prompt_output).is_absolute()
        else Path(args.prompt_output)
    )
    if not args.prompt_output:
        prompt_path = review_dir / f"{stamp}_{safe_title}.prompt.md"
    events_path = review_dir / f"{stamp}_{safe_title}.events.jsonl"
    raw_output_path = review_dir / f"{stamp}_{safe_title}.raw-output.md"
    effective_config_path = review_dir / f"{stamp}_{safe_title}.effective-config.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_raw = prompt.encode("utf-8")
    prompt_path.write_bytes(prompt_raw)
    prompt_sha256 = hashlib.sha256(prompt_raw).hexdigest()

    timeout_seconds = max(60, int(args.timeout_seconds or 420))
    try:
        codex_executable = _resolve_codex_executable()
    except (OSError, RuntimeError) as exc:
        output_path.write_text(
            "## Review Metadata\n\n"
            f"- title: {args.title or 'Independent review'}\n"
            "- reviewer_exit_code: invalid_transport\n"
            f"- reviewer_transport: {_APP_SERVER_TRANSPORT}\n"
            "- reviewer_codex_executable: unresolved\n"
            f"- prompt_path: `{prompt_path}`\n\n"
            "## Reviewer Output\n\n"
            f"NO-GO: Codex executable resolution failed closed: {exc}. "
            "Do not treat this as review evidence.\n",
            encoding="utf-8",
        )
        print(f"prompt_written={prompt_path}")
        print(f"review_output={output_path}")
        print("reviewer_codex_executable=unresolved")
        return 2
    codex_args = _build_app_server_args(codex_executable)
    shell_command = " ".join(shlex.quote(part) for part in codex_args)
    if not args.execute:
        print(f"prompt_written={prompt_path}")
        print(f"review_output={output_path}")
        print(f"python_timeout_seconds={timeout_seconds}")
        print(f"reviewer_model={configured.settings.model}")
        print(f"reviewer_reasoning_effort={configured.settings.reasoning_effort}")
        print(f"reviewer_service_tier={configured.settings.service_tier}")
        print(f"reviewer_config_path={configured.path}")
        print(f"reviewer_config_sha256={configured.sha256}")
        print(f"reviewer_transport={_APP_SERVER_TRANSPORT}")
        print(f"reviewer_codex_executable={codex_executable}")
        print(f"reviewer_thread_source={_APP_SERVER_THREAD_SOURCE}")
        print(f"review_scope_mode={scope_evidence['scope_mode']}")
        print(f"review_resolved_base_commit={scope_evidence['resolved_base_commit']}")
        print(f"review_resolved_head_commit={scope_evidence['resolved_head_commit']}")
        print(f"review_scope_digest_sha256={scope_evidence['scope_digest_sha256']}")
        print("execute_command=" + shell_command + "  # JSON-RPC prompt is sent by this runner")
        return 0

    try:
        completed = _run_app_server_review(
            root=root,
            configured=configured,
            codex_executable=codex_executable,
            prompt_raw=prompt_raw,
            timeout_seconds=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        partial_transcript = exc.output if isinstance(exc.output, bytes) else b""
        events_path.write_bytes(partial_transcript)
        timeout_note = (
            "## Review Metadata\n\n"
            f"- title: {args.title or 'Independent review'}\n"
            "- reviewer_exit_code: timeout\n"
            f"- reviewer_transport: {_APP_SERVER_TRANSPORT}\n"
            f"- reviewer_codex_executable: `{codex_executable}`\n"
            f"- prompt_path: `{prompt_path}`\n\n"
            "## Reviewer Output\n\n"
            f"NO-GO: independent review timed out after {timeout_seconds} seconds. "
            "Increase REVIEW_TIMEOUT_SECONDS or reduce REVIEW_FILES scope; do not treat this as review evidence.\n"
        )
        output_path.write_text(timeout_note, encoding="utf-8")
        print(f"prompt_written={prompt_path}")
        print(f"review_output={output_path}")
        return 124
    except AppServerReviewError as exc:
        events_path.write_bytes(exc.transcript_raw)
        if exc.stderr:
            print(exc.stderr.rstrip())
        output_path.write_text(
            "## Review Metadata\n\n"
            f"- title: {args.title or 'Independent review'}\n"
            "- reviewer_exit_code: invalid_transport\n"
            f"- reviewer_transport: {_APP_SERVER_TRANSPORT}\n"
            f"- reviewer_codex_executable: `{codex_executable}`\n"
            f"- prompt_path: `{prompt_path}`\n"
            f"- events_path: `{events_path}`\n"
            f"- events_sha256: {hashlib.sha256(exc.transcript_raw).hexdigest()}\n\n"
            "## Reviewer Output\n\n"
            f"NO-GO: Codex app-server review transport failed closed: {exc}. "
            "Do not treat this as review evidence.\n",
            encoding="utf-8",
        )
        print(f"prompt_written={prompt_path}")
        print(f"review_output={output_path}")
        return 2
    except (OSError, RuntimeError) as exc:
        output_path.write_text(
            "## Review Metadata\n\n"
            f"- title: {args.title or 'Independent review'}\n"
            "- reviewer_exit_code: invalid_transport\n"
            f"- reviewer_transport: {_APP_SERVER_TRANSPORT}\n"
            f"- reviewer_codex_executable: `{codex_executable}`\n"
            f"- prompt_path: `{prompt_path}`\n\n"
            "## Reviewer Output\n\n"
            f"NO-GO: Codex app-server review transport failed closed: {exc}. "
            "Do not treat this as review evidence.\n",
            encoding="utf-8",
        )
        print(f"prompt_written={prompt_path}")
        print(f"review_output={output_path}")
        return 2
    events_raw = completed.transcript_raw
    events_path.write_bytes(events_raw)
    events_sha256 = hashlib.sha256(events_raw).hexdigest()
    print(f"prompt_written={prompt_path}")
    print(f"review_output={output_path}")
    print(f"review_events={events_path}")
    if completed.stderr:
        print(completed.stderr.rstrip())
    reviewer_output_raw = completed.evidence.final_output
    try:
        review_body = reviewer_output_raw.decode("utf-8")
    except UnicodeDecodeError:
        review_body = ""
    raw_output_path.write_bytes(reviewer_output_raw)
    raw_output_sha256 = hashlib.sha256(reviewer_output_raw).hexdigest()
    try:
        effective = _load_app_server_effective_reviewer_configuration(
            transcript=completed.evidence,
            configured=configured,
        )
        rollout_raw = effective.rollout_path.read_bytes()
        rollout_evidence_path = review_dir / f"{stamp}_{safe_title}.rollout-{effective.thread_id}.jsonl"
        rollout_evidence_path.write_bytes(rollout_raw)
        effective = effective._replace(
            rollout_path=rollout_evidence_path,
            rollout_sha256=hashlib.sha256(rollout_raw).hexdigest(),
        )
        causal_binding = _review_app_server_causal_binding(
            rollout_raw=rollout_raw,
            prompt_raw=prompt_raw,
            raw_output=reviewer_output_raw,
            expected_thread_id=effective.thread_id,
            expected_session_source=completed.evidence.session_source,
        )
        if str(causal_binding.get("turn_id") or "").strip() != completed.evidence.turn_id:
            raise RuntimeError(
                "Codex app-server transcript turn does not match the persisted rollout task turn"
            )
        effective_config_sha256 = _write_effective_config_evidence(
            path=effective_config_path,
            configured=configured,
            effective=effective,
            reviewer_exit_code=int(completed.returncode),
            scope=scope_evidence,
            prompt_path=prompt_path,
            prompt_sha256=prompt_sha256,
            events_path=events_path,
            events_sha256=events_sha256,
            raw_output_path=raw_output_path,
            raw_output_sha256=raw_output_sha256,
            root=root,
            causal_binding=causal_binding,
            transcript_binding=completed.evidence.binding,
            turn_id=completed.evidence.turn_id,
            session_source=completed.evidence.session_source,
        )
    except (OSError, RuntimeError) as exc:
        output_path.write_text(
            "## Review Metadata\n\n"
            f"- title: {args.title or 'Independent review'}\n"
            "- reviewer_configuration_mode: operator config values requested explicitly; active values "
            "unverified\n"
            f"- reviewer_transport: {_APP_SERVER_TRANSPORT}\n"
            f"- reviewer_config_path: `{configured.path}`\n"
            f"- reviewer_config_sha256: {configured.sha256}\n"
            f"- reviewer_exit_code: {completed.returncode}\n"
            f"- reviewer_codex_executable: `{codex_executable}`\n"
            f"- prompt_path: `{prompt_path}`\n"
            f"- events_path: `{events_path}`\n\n"
            "## Reviewer Output\n\n"
            f"NO-GO: effective reviewer configuration could not be verified: {exc}. "
            "Do not treat this as review evidence.\n",
            encoding="utf-8",
        )
        return 2
    header = _artifact_header(
        root=root,
        scope_evidence=scope_evidence,
        configured=configured,
        effective=effective,
        timeout_seconds=timeout_seconds,
        prompt_path=prompt_path,
        events_path=events_path,
        effective_config_path=effective_config_path,
        effective_config_sha256=effective_config_sha256,
        prompt_sha256=prompt_sha256,
        events_sha256=events_sha256,
        raw_output_path=raw_output_path,
        raw_output_sha256=raw_output_sha256,
        reviewer_exit_code=int(completed.returncode),
        codex_executable=codex_executable,
        shell_command=shell_command,
        transcript=completed.evidence,
    )
    if completed.returncode != 0:
        reviewer_output = review_body.rstrip("\n") + "\n\n" if review_body else ""
        output_path.write_text(
            header + reviewer_output + f"NO-GO: reviewer process exited with status {completed.returncode}; "
            "do not treat any preceding output as valid review evidence.\n\n"
            "NO-GO\n",
            encoding="utf-8",
        )
        return int(completed.returncode)
    if not review_body.strip():
        output_path.write_text(
            header + "NO-GO: reviewer produced no output; do not treat this as review evidence.\n",
            encoding="utf-8",
        )
        return 2
    if review_body.strip() == "NO-GO":
        invalid_note = (
            "\n\nINVALID_REVIEW_ARTIFACT: bare NO-GO without at least one prioritized finding. "
            "Rerun with narrower scope or clearer prompt; do not treat this as review evidence.\n"
        )
        output_path.write_text(header + review_body + invalid_note, encoding="utf-8")
        return 2
    if not _app_server_transcript_binding_valid(completed.evidence.binding) or not _review_app_server_causal_binding_valid(
        causal_binding
    ):
        reviewer_output = review_body.rstrip("\r\n") + "\n\n"
        output_path.write_text(
            header
            + reviewer_output
            + "INVALID_REVIEW_ARTIFACT: Codex app-server transcript and persisted rollout did not causally bind "
            "the active settings, exact prompt, final output, and single completed turn. Do not treat this as "
            "review evidence.\n\nNO-GO\n",
            encoding="utf-8",
        )
        return 2
    output_path.write_bytes(header.encode("utf-8") + reviewer_output_raw)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
