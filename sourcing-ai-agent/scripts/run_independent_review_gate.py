#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shlex
import subprocess
import time
import tomllib
from pathlib import Path
from typing import NamedTuple

from sourcing_agent.runtime_asset_retention_prune import (
    build_independent_review_scope_evidence,
    normalize_independent_review_files,
)

_INHERITED_VALUE_SENTINELS = {"", "auto", "default", "inherit"}
_EFFECTIVE_CONFIG_CONTRACT_VERSION = "independent_review_effective_config_v2"
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

    def exact_between(observations: list[tuple[int, str]], expected: str, *, normalize_newlines: bool = False) -> bool:
        matching = [
            line_number
            for line_number, observed in observations
            if (start_line < line_number < complete_line)
            and (
                _normalize_trailing_newlines(observed) == _normalize_trailing_newlines(expected)
                if normalize_newlines
                else observed == expected
            )
        ]
        return single_task_turn and len(matching) == 1

    task_complete_final_exact = (
        single_task_turn
        and _normalize_trailing_newlines(task_completes[0][2]) == normalized_final
        and bool(normalized_final)
    )
    return {
        "turn_id": turn_id,
        "prompt_sha256": hashlib.sha256(prompt_raw).hexdigest(),
        "normalized_final_output_sha256": hashlib.sha256(normalized_final.encode("utf-8")).hexdigest(),
        "rollout_json_valid": rollout_json_valid,
        "text_utf8_valid": text_utf8_valid,
        "session_source_exec": session_sources == ["exec"],
        "single_task_turn": single_task_turn,
        "no_abort": not abort_seen,
        "prompt_response_item_exact": exact_between(prompt_response_items, prompt),
        "prompt_event_message_exact": exact_between(prompt_event_messages, prompt),
        "final_response_item_exact": exact_between(
            final_response_items,
            final_output,
            normalize_newlines=True,
        ),
        "final_event_message_exact": exact_between(
            final_event_messages,
            final_output,
            normalize_newlines=True,
        ),
        "task_complete_final_exact": task_complete_final_exact,
    }


def _review_causal_binding_valid(binding: dict[str, object]) -> bool:
    return bool(binding.get("turn_id")) and all(binding.get(field) is True for field in _CAUSAL_BINDING_BOOLEAN_FIELDS)


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
            "codex_cli_version": effective.codex_cli_version,
            "rollout_path": _workspace_path(root, effective.rollout_path),
            "rollout_sha256": effective.rollout_sha256,
        },
        "process": {"reviewer_exit_code": int(reviewer_exit_code)},
        "effective": {
            "model": effective.settings.model,
            "reasoning_effort": effective.settings.reasoning_effort,
            "service_tier": effective.settings.service_tier,
            "source": effective.source,
        },
        "model_reroutes": list(effective.model_reroutes),
        "causal_binding": dict(causal_binding or {}),
        "scope": scope,
        "artifacts": {
            "prompt": {"path": _workspace_path(root, prompt_path), "sha256": prompt_sha256},
            "events": {"path": _workspace_path(root, events_path), "sha256": events_sha256},
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
        f"use `git diff {scope['resolved_base_commit']} {scope['resolved_head_commit']} -- <files>` and "
        f"`git show {scope['resolved_head_commit']}:<path>`; do not substitute current working-tree contents. "
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
    shell_command: str,
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
        "- reviewer_configuration_mode: inherited Codex global config; no model/effort/tier CLI override\n"
        f"- reviewer_config_path: `{configured.path}`\n"
        f"- reviewer_config_sha256: {configured.sha256}\n"
        f"- reviewer_exit_code: {reviewer_exit_code}\n"
        f"- reviewer_codex_cli_version: {effective.codex_cli_version}\n"
        f"- reviewer_thread_id: {effective.thread_id}\n"
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


def _build_codex_args(
    *,
    root: Path,
    output_path: Path,
) -> list[str]:
    return [
        "codex",
        "exec",
        "--strict-config",
        "--cd",
        str(root),
        "--sandbox",
        "read-only",
        "--json",
        "--output-last-message",
        str(output_path),
        "-",
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate or execute the independent review gate prompt.")
    parser.add_argument("--title", default="", help="Review title.")
    parser.add_argument("--files", default="", help="Shell-style space-separated file list.")
    parser.add_argument("--base", default="", help="Optional base branch/ref for reviewer context.")
    parser.add_argument("--extra-context", default="", help="Additional review context.")
    parser.add_argument("--timeout-seconds", type=int, default=420, help="Hard timeout for --execute.")
    parser.add_argument("--output", default="", help="Review output file.")
    parser.add_argument("--prompt-output", default="", help="Prompt output file for dry-run/use elsewhere.")
    parser.add_argument("--execute", action="store_true", help="Run codex exec in read-only mode.")
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
    codex_args = _build_codex_args(
        root=root,
        output_path=output_path,
    )
    shell_command = " ".join(shlex.quote(part) for part in codex_args) + f" < {shlex.quote(str(prompt_path))}"
    if not args.execute:
        print(f"prompt_written={prompt_path}")
        print(f"review_output={output_path}")
        print(f"python_timeout_seconds={timeout_seconds}")
        print(f"reviewer_model={configured.settings.model}")
        print(f"reviewer_reasoning_effort={configured.settings.reasoning_effort}")
        print(f"reviewer_service_tier={configured.settings.service_tier}")
        print(f"reviewer_config_path={configured.path}")
        print(f"reviewer_config_sha256={configured.sha256}")
        print(f"review_scope_mode={scope_evidence['scope_mode']}")
        print(f"review_resolved_base_commit={scope_evidence['resolved_base_commit']}")
        print(f"review_resolved_head_commit={scope_evidence['resolved_head_commit']}")
        print(f"review_scope_digest_sha256={scope_evidence['scope_digest_sha256']}")
        print("execute_command=" + shell_command)
        return 0

    try:
        with prompt_path.open("r", encoding="utf-8") as prompt_handle:
            completed = subprocess.run(
                codex_args,
                cwd=root,
                stdin=prompt_handle,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout_seconds,
            )
    except subprocess.TimeoutExpired:
        timeout_note = (
            "## Review Metadata\n\n"
            f"- title: {args.title or 'Independent review'}\n"
            "- reviewer_exit_code: timeout\n"
            f"- prompt_path: `{prompt_path}`\n\n"
            "## Reviewer Output\n\n"
            f"NO-GO: independent review timed out after {timeout_seconds} seconds. "
            "Increase REVIEW_TIMEOUT_SECONDS or reduce REVIEW_FILES scope; do not treat this as review evidence.\n"
        )
        output_path.write_text(timeout_note, encoding="utf-8")
        print(f"prompt_written={prompt_path}")
        print(f"review_output={output_path}")
        return 124
    events_raw = (completed.stdout or "").encode("utf-8")
    events_path.write_bytes(events_raw)
    events_sha256 = hashlib.sha256(events_raw).hexdigest()
    print(f"prompt_written={prompt_path}")
    print(f"review_output={output_path}")
    print(f"review_events={events_path}")
    if completed.stderr:
        print(completed.stderr.rstrip())
    reviewer_output_raw = output_path.read_bytes() if output_path.exists() else b""
    try:
        review_body = reviewer_output_raw.decode("utf-8")
    except UnicodeDecodeError:
        review_body = ""
    raw_output_path.write_bytes(reviewer_output_raw)
    raw_output_sha256 = hashlib.sha256(reviewer_output_raw).hexdigest()
    try:
        effective = _load_effective_reviewer_configuration(
            events_text=completed.stdout or "",
            configured=configured,
        )
        rollout_raw = effective.rollout_path.read_bytes()
        rollout_evidence_path = review_dir / f"{stamp}_{safe_title}.rollout-{effective.thread_id}.jsonl"
        rollout_evidence_path.write_bytes(rollout_raw)
        effective = effective._replace(
            rollout_path=rollout_evidence_path,
            rollout_sha256=hashlib.sha256(rollout_raw).hexdigest(),
        )
        causal_binding = _review_causal_binding(
            rollout_raw=rollout_raw,
            prompt_raw=prompt_raw,
            raw_output=reviewer_output_raw,
            expected_thread_id=effective.thread_id,
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
        )
    except (OSError, RuntimeError) as exc:
        output_path.write_text(
            "## Review Metadata\n\n"
            f"- title: {args.title or 'Independent review'}\n"
            "- reviewer_configuration_mode: inherited Codex global config; no model/effort/tier CLI override\n"
            f"- reviewer_config_path: `{configured.path}`\n"
            f"- reviewer_config_sha256: {configured.sha256}\n"
            f"- reviewer_exit_code: {completed.returncode}\n"
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
        shell_command=shell_command,
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
    if not _review_causal_binding_valid(causal_binding):
        reviewer_output = review_body.rstrip("\r\n") + "\n\n"
        output_path.write_text(
            header
            + reviewer_output
            + "INVALID_REVIEW_ARTIFACT: persisted Codex exec rollout did not causally bind the exact prompt, "
            "final output, and single completed turn. Do not treat this as review evidence.\n\nNO-GO\n",
            encoding="utf-8",
        )
        return 2
    output_path.write_bytes(header.encode("utf-8") + reviewer_output_raw)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
