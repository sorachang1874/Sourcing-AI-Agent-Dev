"""Offline/scripted harvest harness — simulate/replay/scripted provider bodies.

The 1.4k-line offline test harness embedded in harvest_connectors (recon:
REFACTOR_MASTER_PLAN WS2), split out 2026-07-22. Produces deterministic
provider-shaped bodies for simulate/replay/scripted modes; never performs
network I/O. harvest_connectors imports the entry points
(_build_offline_harvest_body / _build_scripted_harvest_result /
_scripted_float_first); this module never imports harvest_connectors back.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

from .company_registry import normalize_company_key
from .profile_registry_utils import harvest_profile_payload_has_usable_content
from .runtime_tuning import resolved_harvest_scripted_sleep_seconds_cap
from .scripted_provider_scenario import (
    advance_scripted_phase_round,
    find_scripted_rule,
    scripted_pending_rounds,
    scripted_phase_error,
    scripted_rule_artifacts,
    scripted_sleep,
    scripted_sleep_seconds,
)
from .harvest_support import (
    HarvestExecutionArtifact,
    HarvestExecutionResult,
    _harvest_profile_match_context,
    _offline_profile_identifier,
    _payload_cache_key,
    _profile_cache_key,
)

_SCRIPTED_SAMPLE_CANDIDATE_DOC_CACHE: dict[str, tuple[int, int, list[dict[str, Any]]]] = {}
_SCRIPTED_SAMPLE_FILTERED_CANDIDATE_CACHE: dict[str, tuple[int, int, list[dict[str, Any]]]] = {}


def _build_offline_harvest_body(
    *,
    logical_name: str,
    payload: dict[str, Any],
    provider_mode: str,
) -> list[dict[str, Any]]:
    normalized_mode = str(provider_mode or "simulate").strip().lower() or "simulate"
    if normalized_mode == "replay":
        # Replay is intentionally cache-only. On a cache miss we must not synthesize
        # provider rows that can later be promoted into authoritative company assets.
        return []

    metadata = {
        "_offline": True,
        "_provider_mode": provider_mode,
        "_logical_name": logical_name,
    }
    if logical_name == "harvest_company_employees":
        companies = [str(item).strip() for item in list(payload.get("companies") or []) if str(item).strip()]
        company_url = companies[0] if companies else "https://www.linkedin.com/company/example/"
        company_slug_match = re.search(r"/company/([^/]+)/?", company_url)
        company_slug = str(company_slug_match.group(1) if company_slug_match else "example").strip() or "example"
        company_label = {
            "xai": "xAI",
            "anthropicresearch": "Anthropic",
            "openai": "OpenAI",
            "google": "Google",
            "deepmind": "Google DeepMind",
        }.get(company_slug.lower(), company_slug.replace("-", " ").title())
        requested_count = max(1, int(payload.get("maxItems") or 25))
        result_count = min(requested_count, 40)
        search_query = " ".join(str(payload.get("searchQuery") or payload.get("search_query") or "").split()).strip()
        headline_suffix = search_query or "Research Engineer"
        location_values = [str(item).strip() for item in list(payload.get("locations") or []) if str(item).strip()]
        location_name = location_values[0] if location_values else "United States"
        results: list[dict[str, Any]] = []
        for index in range(1, result_count + 1):
            public_identifier = f"{company_slug}-offline-{index}"
            results.append(
                {
                    **metadata,
                    "fullName": f"{company_label} Offline Member {index}",
                    "headline": f"{headline_suffix} at {company_label}",
                    "linkedinUrl": f"https://www.linkedin.com/in/{public_identifier}/",
                    "publicIdentifier": public_identifier,
                    "photoUrl": f"https://cdn.example.com/{public_identifier}.jpg",
                    "locationName": location_name,
                    "item": {
                        "fullName": f"{company_label} Offline Member {index}",
                        "headline": f"{headline_suffix} at {company_label}",
                        "linkedinUrl": f"https://www.linkedin.com/in/{public_identifier}/",
                        "publicIdentifier": public_identifier,
                        "photoUrl": f"https://cdn.example.com/{public_identifier}.jpg",
                        "locationName": location_name,
                    },
                }
            )
        return results
    if logical_name == "harvest_profile_scraper_batch":
        results: list[dict[str, Any]] = []
        for url in list(payload.get("urls") or []):
            profile_url = str(url or "").strip()
            if not profile_url:
                continue
            results.append(
                {
                    **metadata,
                    "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                    "linkedinUrl": profile_url,
                    "publicIdentifier": _offline_profile_identifier(profile_url),
                    "headline": f"Offline {normalized_mode} profile placeholder",
                    "fullName": "",
                    "item": {
                        "linkedinUrl": profile_url,
                        "publicIdentifier": _offline_profile_identifier(profile_url),
                        "headline": f"Offline {normalized_mode} profile placeholder",
                    },
                }
            )
        return results
    return []


def _scripted_harvest_remote_identifiers(
    *,
    logical_name: str,
    rule: dict[str, Any],
    existing: dict[str, Any],
    payload_hash: str,
) -> tuple[str, str]:
    normalized_hash = str(payload_hash or "").strip()[:16] or "unknown"
    run_id = str(rule.get("run_id") or existing.get("run_id") or f"scripted_run_{logical_name}_{normalized_hash}")
    dataset_id = str(
        rule.get("dataset_id") or existing.get("dataset_id") or f"scripted_dataset_{logical_name}_{normalized_hash}"
    )
    return run_id, dataset_id


def _build_scripted_harvest_result(
    *,
    logical_name: str,
    payload: dict[str, Any],
    checkpoint: dict[str, Any] | None,
    request_context: dict[str, Any] | None,
) -> HarvestExecutionResult:
    existing = dict(checkpoint or {})
    rule = find_scripted_rule(
        "harvest",
        context={
            "logical_name": logical_name,
            "payload": payload,
            "request_context": dict(request_context or {}),
            "provider_name": "scripted_harvest",
            "context_contains": [str(existing.get("scripted_rule_name") or "")]
            if str(existing.get("scripted_rule_name") or "").strip()
            else [],
        },
    )
    terminal_remote_event_seen = bool(
        existing.get("remote_provider_terminal_event")
        or existing.get("scripted_force_terminal_fetch")
        or existing.get("force_scripted_terminal_fetch")
    )
    remote_wait_after_submit = _scripted_remote_wait_after_submit(rule)
    if not terminal_remote_event_seen and not remote_wait_after_submit:
        scripted_sleep(
            rule,
            phase="execute",
            seconds_cap=resolved_harvest_scripted_sleep_seconds_cap(request_context),
        )
    updated_checkpoint, round_number = advance_scripted_phase_round(existing, phase="execute")
    payload_hash = _payload_cache_key(payload)
    run_id, dataset_id = _scripted_harvest_remote_identifiers(
        logical_name=logical_name,
        rule=rule,
        existing=existing,
        payload_hash=payload_hash,
    )
    remote_wait_checkpoint = _scripted_remote_wait_checkpoint(
        rule=rule,
        existing=existing,
        request_context=request_context,
    )
    scripted_provider_timings = _scripted_harvest_provider_timings(
        rule=rule,
        remote_wait_checkpoint=remote_wait_checkpoint,
    )
    remote_wait_ready_epoch_ms = int(remote_wait_checkpoint.get("scripted_remote_ready_epoch_ms") or 0)
    if (
        remote_wait_after_submit
        and not terminal_remote_event_seen
        and remote_wait_ready_epoch_ms > 0
        and int(time.time() * 1000) < remote_wait_ready_epoch_ms
    ):
        return HarvestExecutionResult(
            logical_name=logical_name,
            checkpoint={
                **updated_checkpoint,
                "logical_name": logical_name,
                "payload_hash": payload_hash,
                "status": str(rule.get("pending_status") or "submitted"),
                "provider_mode": "scripted",
                "run_id": run_id,
                "dataset_id": dataset_id,
                "scripted_rule_name": str(rule.get("_rule_name") or existing.get("scripted_rule_name") or ""),
                "request_context": dict(request_context or {}),
                **remote_wait_checkpoint,
                **({"provider_timings": scripted_provider_timings} if scripted_provider_timings else {}),
            },
            pending=True,
            message=str(rule.get("pending_message") or f"Scripted Harvest task for {logical_name} is still pending."),
            artifacts=[
                HarvestExecutionArtifact(
                    label="scripted_harvest_pending",
                    payload={
                        "logical_name": logical_name,
                        "round": round_number,
                        "rule": str(rule.get("_rule_name") or ""),
                        "scripted_remote_wait_after_submit": True,
                        "scripted_remote_ready_epoch_ms": remote_wait_ready_epoch_ms,
                    },
                    metadata={
                        "provider_mode": "scripted",
                        **({"provider_timings": scripted_provider_timings} if scripted_provider_timings else {}),
                    },
                ),
            ],
        )
    error_spec = (
        {} if terminal_remote_event_seen else scripted_phase_error(rule, phase="execute", round_number=round_number)
    )
    if error_spec:
        kind = str(error_spec.get("kind") or "runtime").strip().lower()
        message = str(error_spec.get("message") or f"Scripted harvest {kind} error for {logical_name}.").strip()
        if kind == "retryable" or bool(error_spec.get("retryable")):
            return HarvestExecutionResult(
                logical_name=logical_name,
                checkpoint={
                    **updated_checkpoint,
                    "logical_name": logical_name,
                    "payload_hash": payload_hash,
                    "status": str(error_spec.get("status") or f"{kind}_retryable"),
                    "provider_mode": "scripted",
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "scripted_rule_name": str(rule.get("_rule_name") or existing.get("scripted_rule_name") or ""),
                    "request_context": dict(request_context or {}),
                },
                pending=True,
                message=message,
                artifacts=[
                    HarvestExecutionArtifact(
                        label="scripted_retryable_error",
                        payload={
                            "logical_name": logical_name,
                            "round": round_number,
                            "kind": kind,
                            "message": message,
                            "rule": str(rule.get("_rule_name") or ""),
                        },
                        metadata={"provider_mode": "scripted"},
                    )
                ],
            )
        raise RuntimeError(message)

    pending_rounds = 0 if terminal_remote_event_seen else scripted_pending_rounds(rule, phase="execute")
    artifacts = [
        HarvestExecutionArtifact(
            label=str(item.get("label") or "scripted_harvest_artifact"),
            payload=item.get("payload"),
            raw_format=str(item.get("raw_format") or "json"),
            content_type=str(item.get("content_type") or "application/json"),
            metadata=dict(item.get("metadata") or {}),
        )
        for item in scripted_rule_artifacts(rule, phase="execute")
    ]
    base_checkpoint = {
        **updated_checkpoint,
        "logical_name": logical_name,
        "payload_hash": payload_hash,
        "provider_mode": "scripted",
        "scripted_rule_name": str(rule.get("_rule_name") or existing.get("scripted_rule_name") or ""),
        "request_context": dict(request_context or {}),
        **remote_wait_checkpoint,
        **({"provider_timings": scripted_provider_timings} if scripted_provider_timings else {}),
    }
    if terminal_remote_event_seen:
        base_checkpoint["remote_provider_terminal_event_consumed"] = True
    if round_number <= pending_rounds:
        return HarvestExecutionResult(
            logical_name=logical_name,
            checkpoint={
                **base_checkpoint,
                "status": str(rule.get("pending_status") or "submitted"),
                "run_id": run_id,
                "dataset_id": dataset_id,
            },
            pending=True,
            message=str(rule.get("pending_message") or f"Scripted Harvest task for {logical_name} is still pending."),
            artifacts=[
                *artifacts,
                HarvestExecutionArtifact(
                    label="scripted_harvest_pending",
                    payload={
                        "logical_name": logical_name,
                        "round": round_number,
                        "rule": str(rule.get("_rule_name") or ""),
                    },
                    metadata={"provider_mode": "scripted"},
                ),
            ],
        )
    body = rule.get("body")
    if not isinstance(body, list):
        body = _build_scripted_sampled_harvest_body(
            rule=rule,
            logical_name=logical_name,
            payload=payload,
        )
    if not isinstance(body, list):
        body = _build_scripted_generated_harvest_body(
            rule=rule,
            logical_name=logical_name,
            payload=payload,
        )
    if not isinstance(body, list):
        body = _build_offline_harvest_body(
            logical_name=logical_name,
            payload=payload,
            provider_mode="scripted",
        )
    provider_timings = scripted_provider_timings
    return HarvestExecutionResult(
        logical_name=logical_name,
        checkpoint={
            **base_checkpoint,
            "status": "completed",
            "run_id": run_id,
            "dataset_id": dataset_id,
            **({"provider_timings": provider_timings} if provider_timings else {}),
        },
        body=body,
        message=str(rule.get("message") or f"Scripted Harvest response returned for {logical_name}."),
        artifacts=[
            *artifacts,
            HarvestExecutionArtifact(
                label="dataset_items",
                payload=body,
                metadata={
                    "logical_name": logical_name,
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "provider": "scripted_harvest",
                    **({"provider_timings": provider_timings} if provider_timings else {}),
                },
            ),
            HarvestExecutionArtifact(
                label="scripted_harvest_response",
                payload={
                    "logical_name": logical_name,
                    "round": round_number,
                    "rule": str(rule.get("_rule_name") or ""),
                    "body_count": len(body),
                },
                metadata={"provider_mode": "scripted"},
            ),
        ],
    )


def _scripted_harvest_provider_timings(
    *,
    rule: dict[str, Any],
    remote_wait_checkpoint: dict[str, Any],
) -> dict[str, float]:
    timings: dict[str, float] = {}
    actor_duration_ms = _scripted_float_first(
        rule,
        "scripted_actor_run_duration_ms",
        "actor_run_duration_ms",
        "provider_actor_run_duration_ms",
    )
    if actor_duration_ms is None:
        wait_seconds = _scripted_float_first(
            remote_wait_checkpoint,
            "scripted_remote_wait_seconds",
            "remote_wait_seconds",
        )
        if wait_seconds is not None and wait_seconds > 0:
            actor_duration_ms = wait_seconds * 1000.0
    if actor_duration_ms is not None and actor_duration_ms >= 0:
        timings["actor_run_duration_ms"] = round(actor_duration_ms, 2)
    dataset_download_ms = _scripted_float_first(
        rule,
        "scripted_dataset_download_duration_ms",
        "dataset_download_duration_ms",
        "provider_dataset_download_duration_ms",
    )
    if dataset_download_ms is not None and dataset_download_ms >= 0:
        timings["dataset_download_duration_ms"] = round(dataset_download_ms, 2)
    return timings


def _scripted_float_first(mapping: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        raw = dict(mapping or {}).get(key)
        if raw in (None, ""):
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def _build_scripted_generated_harvest_body(
    *,
    rule: dict[str, Any],
    logical_name: str,
    payload: dict[str, Any],
) -> list[dict[str, Any]] | None:
    spec = rule.get("generated_body")
    if not isinstance(spec, dict):
        return None
    kind = str(spec.get("kind") or logical_name or "").strip().lower()
    if kind in {"profile_search", "harvest_profile_search"} or logical_name == "harvest_profile_search":
        return _build_scripted_generated_profile_search_body(spec=spec, payload=payload)
    if (
        kind in {"profile_scraper", "profile_scraper_batch", "harvest_profile_scraper_batch"}
        or logical_name == "harvest_profile_scraper_batch"
    ):
        return _build_scripted_generated_profile_scraper_body(spec=spec, payload=payload)
    if kind in {"company_employees", "harvest_company_employees"} or logical_name == "harvest_company_employees":
        return _build_scripted_generated_company_employee_body(spec=spec, payload=payload)
    return None


def _scripted_remote_wait_after_submit(rule: dict[str, Any]) -> bool:
    if _scripted_bool(rule.get("scripted_remote_wait_after_submit"), False):
        return True
    for key in ("execute_sleep_position", "sleep_position", "scripted_sleep_position"):
        if str(rule.get(key) or "").strip().lower() in {"remote_wait", "after_submit", "post_submit"}:
            return True
    return False


def _scripted_remote_wait_checkpoint(
    *,
    rule: dict[str, Any],
    existing: dict[str, Any],
    request_context: dict[str, Any] | None,
) -> dict[str, Any]:
    if not _scripted_remote_wait_after_submit(rule):
        return {}
    try:
        ready_epoch_ms = int(float(existing.get("scripted_remote_ready_epoch_ms") or 0))
    except (TypeError, ValueError):
        ready_epoch_ms = 0
    wait_seconds = _scripted_remote_wait_seconds(rule)
    if wait_seconds is None:
        wait_seconds = scripted_sleep_seconds(
            rule,
            phase="execute",
            seconds_cap=resolved_harvest_scripted_sleep_seconds_cap(request_context),
        )
    if ready_epoch_ms <= 0 and wait_seconds > 0:
        ready_epoch_ms = int((time.time() + wait_seconds) * 1000)
    payload: dict[str, Any] = {
        "scripted_remote_wait_after_submit": True,
    }
    if wait_seconds > 0:
        payload["scripted_remote_wait_seconds"] = wait_seconds
    if ready_epoch_ms > 0:
        payload["scripted_remote_ready_epoch_ms"] = ready_epoch_ms
    return payload


def _scripted_remote_wait_seconds(rule: dict[str, Any]) -> float | None:
    for key in ("scripted_remote_wait_seconds", "remote_wait_seconds"):
        raw = rule.get(key)
        if raw in (None, ""):
            continue
        try:
            seconds = float(raw)
        except (TypeError, ValueError):
            continue
        return max(0.0, seconds)
    return None


def _build_scripted_sampled_harvest_body(
    *,
    rule: dict[str, Any],
    logical_name: str,
    payload: dict[str, Any],
) -> list[dict[str, Any]] | None:
    rows = _load_scripted_sample_rows(rule, logical_name=logical_name, payload=payload)
    if rows is None:
        return None
    if logical_name == "harvest_profile_scraper_batch":
        matched, missing_urls = _scripted_profile_rows_for_requested_urls(rows, payload)
        if missing_urls and not _scripted_sample_fallback_generated_allowed(rule):
            raise RuntimeError(
                "Scripted Harvest sample fixture is missing requested profile URLs for "
                f"{logical_name}: {', '.join(missing_urls[:5])}"
            )
        if missing_urls and _scripted_sample_fallback_generated_allowed(rule):
            generated_spec = rule.get("generated_body")
            if isinstance(generated_spec, dict):
                generated = _build_scripted_generated_profile_scraper_body(
                    spec=generated_spec,
                    payload={**payload, "urls": missing_urls},
                )
                matched.extend(generated)
        return matched
    if logical_name == "harvest_profile_search" and (
        rule.get("sample_candidate_documents_path") or rule.get("sample_candidate_document_path")
    ):
        return list(rows)
    sliced = _slice_scripted_sample_rows_for_payload(rows, logical_name=logical_name, payload=payload)
    limit = _scripted_int(rule.get("sample_limit") or rule.get("max_sample_rows"), 0)
    if limit > 0:
        sliced = sliced[:limit]
    return sliced


def _load_scripted_sample_rows(
    rule: dict[str, Any],
    *,
    logical_name: str = "",
    payload: dict[str, Any] | None = None,
) -> list[dict[str, Any]] | None:
    real_asset_rows = _load_scripted_real_asset_sample_rows(
        rule,
        logical_name=logical_name,
        payload=dict(payload or {}),
    )
    if real_asset_rows is not None:
        return real_asset_rows
    raw_paths = (
        rule.get("sample_body_paths")
        or rule.get("sample_pool_paths")
        or rule.get("body_paths")
        or rule.get("sample_body_path")
        or rule.get("sample_pool_path")
        or rule.get("body_path")
    )
    if raw_paths in (None, "", [], ()):
        return None
    path_values = list(raw_paths) if isinstance(raw_paths, (list, tuple)) else [raw_paths]
    rows: list[dict[str, Any]] = []
    for raw_path in path_values:
        path = _resolve_scripted_sample_path(raw_path)
        if path is None:
            if not _scripted_sample_fallback_generated_allowed(rule):
                raise RuntimeError(f"Scripted Harvest sample fixture path not found: {raw_path}")
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            if not _scripted_sample_fallback_generated_allowed(rule):
                raise RuntimeError(f"Scripted Harvest sample fixture could not be loaded: {path}") from exc
            continue
        for item in _coerce_scripted_sample_rows(payload):
            rows.append(item)
    if not rows and not _scripted_sample_fallback_generated_allowed(rule):
        raise RuntimeError("Scripted Harvest sample fixture produced no rows.")
    return rows if rows else None


def _load_scripted_real_asset_sample_rows(
    rule: dict[str, Any],
    *,
    logical_name: str,
    payload: dict[str, Any],
) -> list[dict[str, Any]] | None:
    candidate_documents_path = _resolve_scripted_sample_path(
        rule.get("sample_candidate_documents_path") or rule.get("sample_candidate_document_path")
    )
    if candidate_documents_path is None:
        return None
    candidates = _load_scripted_sample_candidate_documents(candidate_documents_path, rule)
    filtered_candidates = _cached_filter_scripted_sample_candidates(
        candidates,
        candidate_documents_path=candidate_documents_path,
        rule=rule,
        sample_root=_scripted_candidate_documents_asset_root(candidate_documents_path),
    )
    if logical_name == "harvest_profile_search":
        return _scripted_profile_search_rows_from_candidates(
            filtered_candidates,
            payload=payload,
        )
    if logical_name == "harvest_profile_scraper_batch":
        return _scripted_profile_scraper_rows_from_candidates(
            filtered_candidates,
            rule=rule,
            candidate_documents_path=candidate_documents_path,
            payload=payload,
        )
    return None


def _cached_filter_scripted_sample_candidates(
    candidates: list[dict[str, Any]],
    *,
    candidate_documents_path: Path,
    rule: dict[str, Any],
    sample_root: Path,
) -> list[dict[str, Any]]:
    try:
        stat = candidate_documents_path.stat()
    except OSError:
        return _filter_scripted_sample_candidates(candidates, rule=rule, sample_root=sample_root)
    signature = (int(stat.st_mtime_ns), int(stat.st_size))
    cache_key_payload = {
        "path": str(candidate_documents_path.resolve()),
        "employment_scope": str(rule.get("sample_candidate_employment_scope") or ""),
        "contains": list(rule.get("sample_candidate_contains") or rule.get("sample_candidate_filter_contains") or []),
        "excludes": list(rule.get("sample_candidate_excludes") or rule.get("sample_candidate_filter_excludes") or []),
        "source_datasets": list(rule.get("sample_candidate_source_datasets") or []),
        "require_profile_source": bool(_scripted_bool(rule.get("sample_candidate_require_profile_source"), False)),
        "limit": _scripted_int(rule.get("sample_candidate_limit"), 0),
        "fallback_generated": _scripted_sample_fallback_generated_allowed(rule),
    }
    cache_key = json.dumps(cache_key_payload, ensure_ascii=False, sort_keys=True)
    cached = _SCRIPTED_SAMPLE_FILTERED_CANDIDATE_CACHE.get(cache_key)
    if cached is not None and cached[:2] == signature:
        return [dict(item) for item in cached[2]]
    filtered = _filter_scripted_sample_candidates(candidates, rule=rule, sample_root=sample_root)
    _SCRIPTED_SAMPLE_FILTERED_CANDIDATE_CACHE[cache_key] = (*signature, [dict(item) for item in filtered])
    return filtered


def _load_scripted_sample_candidate_documents(path: Path, rule: dict[str, Any]) -> list[dict[str, Any]]:
    cache_key = str(path.resolve())
    try:
        stat = path.stat()
    except OSError as exc:
        if not _scripted_sample_fallback_generated_allowed(rule):
            raise RuntimeError(f"Scripted Harvest candidate sample fixture could not be stat'ed: {path}") from exc
        return []
    cached = _SCRIPTED_SAMPLE_CANDIDATE_DOC_CACHE.get(cache_key)
    stat_signature = (int(stat.st_mtime_ns), int(stat.st_size))
    if cached is not None and cached[:2] == stat_signature:
        return [dict(item) for item in cached[2]]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        if not _scripted_sample_fallback_generated_allowed(rule):
            raise RuntimeError(f"Scripted Harvest candidate sample fixture could not be loaded: {path}") from exc
        return []
    rows = _coerce_scripted_sample_rows(payload)
    if not rows and not _scripted_sample_fallback_generated_allowed(rule):
        raise RuntimeError(f"Scripted Harvest candidate sample fixture produced no candidates: {path}")
    _SCRIPTED_SAMPLE_CANDIDATE_DOC_CACHE[cache_key] = (*stat_signature, [dict(item) for item in rows])
    return rows


def _filter_scripted_sample_candidates(
    candidates: list[dict[str, Any]],
    *,
    rule: dict[str, Any],
    sample_root: Path,
) -> list[dict[str, Any]]:
    employment_scope = str(rule.get("sample_candidate_employment_scope") or "").strip().lower()
    contains_terms = [
        str(item).strip().lower()
        for item in list(rule.get("sample_candidate_contains") or rule.get("sample_candidate_filter_contains") or [])
        if str(item).strip()
    ]
    excludes_terms = [
        str(item).strip().lower()
        for item in list(rule.get("sample_candidate_excludes") or rule.get("sample_candidate_filter_excludes") or [])
        if str(item).strip()
    ]
    source_datasets = {
        str(item).strip().lower()
        for item in list(rule.get("sample_candidate_source_datasets") or [])
        if str(item).strip()
    }
    require_profile_source = _scripted_bool(rule.get("sample_candidate_require_profile_source"), False)
    limit = _scripted_int(rule.get("sample_candidate_limit"), 0)
    rows: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for candidate in candidates:
        row = dict(candidate or {})
        metadata = dict(row.get("metadata") or {})
        if employment_scope:
            row_scope = (
                str(row.get("employment_status") or metadata.get("membership_claim_employment_status") or "")
                .strip()
                .lower()
            )
            if row_scope != employment_scope:
                continue
        if source_datasets and str(row.get("source_dataset") or "").strip().lower() not in source_datasets:
            continue
        match_text = json.dumps(row, ensure_ascii=False, sort_keys=True).lower()
        if contains_terms and not all(term in match_text for term in contains_terms):
            continue
        if excludes_terms and any(term in match_text for term in excludes_terms):
            continue
        if require_profile_source and _scripted_candidate_profile_source_path(row, sample_root=sample_root) is None:
            continue
        profile_url = _scripted_candidate_profile_url(row)
        url_key = next(iter(sorted(_scripted_profile_url_match_keys(profile_url))), "")
        if not url_key or url_key in seen_urls:
            continue
        seen_urls.add(url_key)
        rows.append(row)
        if limit > 0 and len(rows) >= limit:
            break
    if not rows and not _scripted_sample_fallback_generated_allowed(rule):
        raise RuntimeError("Scripted Harvest real-asset candidate filter produced no rows.")
    return rows


def _scripted_candidate_profile_url(candidate: dict[str, Any]) -> str:
    metadata = dict(candidate.get("metadata") or {})
    return str(
        candidate.get("linkedin_url")
        or candidate.get("profile_url")
        or metadata.get("profile_url")
        or metadata.get("linkedin_url")
        or ""
    ).strip()


def _scripted_candidate_public_identifier(candidate: dict[str, Any], profile_url: str) -> str:
    metadata = dict(candidate.get("metadata") or {})
    return str(
        metadata.get("public_identifier")
        or candidate.get("public_identifier")
        or _offline_profile_identifier(profile_url)
    ).strip()


def _scripted_profile_search_rows_from_candidates(
    candidates: list[dict[str, Any]],
    *,
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    max_items = _scripted_int(payload.get("maxItems"), 0)
    take_pages = max(1, _scripted_int(payload.get("takePages"), 1))
    start_page = max(1, _scripted_int(payload.get("startPage"), 1))
    page_size = 25
    offset = max(0, (start_page - 1) * page_size)
    cap = max_items if max_items > 0 else take_pages * page_size
    cap = max(1, min(cap, take_pages * page_size))
    total_elements = len(candidates)
    total_pages = max(1, (total_elements + page_size - 1) // page_size) if total_elements else 0
    rows: list[dict[str, Any]] = []
    for candidate in candidates[offset : offset + cap]:
        metadata = dict(candidate.get("metadata") or {})
        profile_url = _scripted_candidate_profile_url(candidate)
        public_identifier = _scripted_candidate_public_identifier(candidate, profile_url)
        full_name = str(
            candidate.get("display_name")
            or candidate.get("name_en")
            or metadata.get("full_name")
            or metadata.get("name")
            or public_identifier
        ).strip()
        headline = str(
            candidate.get("headline")
            or metadata.get("headline")
            or candidate.get("role")
            or candidate.get("focus_areas")
            or ""
        ).strip()
        location = str(metadata.get("profile_location") or metadata.get("location") or "").strip()
        employment_scope = str(candidate.get("employment_status") or "").strip().lower()
        item = {
            "fullName": full_name,
            "linkedinUrl": profile_url,
            "profileUrl": profile_url,
            "publicIdentifier": public_identifier,
            "headline": headline,
            "location": location,
            "summary": str(
                metadata.get("about") or candidate.get("focus_areas") or candidate.get("notes") or ""
            ).strip(),
        }
        if employment_scope == "former":
            item["pastCompany"] = str(candidate.get("target_company") or candidate.get("organization") or "").strip()
        else:
            item["currentCompany"] = str(candidate.get("target_company") or candidate.get("organization") or "").strip()
        rows.append(
            {
                "fullName": full_name,
                "linkedinUrl": profile_url,
                "profileUrl": profile_url,
                "publicIdentifier": public_identifier,
                "headline": headline,
                "location": location,
                "item": item,
                "_meta": {
                    "pagination": {
                        "totalElements": total_elements,
                        "totalPages": total_pages,
                        "pageNumber": start_page,
                        "pageSize": page_size,
                    },
                    "scripted_sample_source": "candidate_documents",
                },
            }
        )
    return rows


def _scripted_profile_scraper_rows_from_candidates(
    candidates: list[dict[str, Any]],
    *,
    rule: dict[str, Any],
    candidate_documents_path: Path,
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    sample_root = _scripted_candidate_documents_asset_root(candidate_documents_path)
    requested_url_keys = {
        key
        for requested_url in list(payload.get("urls") or [])
        for key in _scripted_profile_url_match_keys(requested_url)
    }
    for candidate in candidates:
        profile_url = _scripted_candidate_profile_url(candidate)
        if requested_url_keys and not (_scripted_profile_url_match_keys(profile_url) & requested_url_keys):
            continue
        raw_path = _scripted_candidate_profile_source_path(candidate, sample_root=sample_root)
        if raw_path is None:
            continue
        try:
            payload = json.loads(raw_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if profile_url and not _harvest_profile_match_context(payload).get("requested_profile_url"):
            payload = {
                "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                "item": dict(payload.get("item") or payload),
            }
        if profile_url:
            payload = _attach_scripted_sample_profile_url_alias(payload, profile_url)
        rows.append(payload)
    if not rows and not _scripted_sample_fallback_generated_allowed(rule):
        raise RuntimeError("Scripted Harvest real-asset profile sample produced no readable raw profiles.")
    return rows


def _attach_scripted_sample_profile_url_alias(payload: dict[str, Any], profile_url: str) -> dict[str, Any]:
    normalized_profile_url = str(profile_url or "").strip()
    if not normalized_profile_url:
        return dict(payload)
    enriched = dict(payload)
    aliases = [
        str(item or "").strip()
        for item in list(enriched.get("_scripted_sample_profile_url_aliases") or [])
        if str(item or "").strip()
    ]
    if normalized_profile_url not in aliases:
        aliases.append(normalized_profile_url)
    enriched["_scripted_sample_profile_url_aliases"] = aliases
    return enriched


def _scripted_candidate_documents_asset_root(candidate_documents_path: Path) -> Path:
    path = candidate_documents_path.expanduser().resolve()
    if path.name == "candidate_documents.json":
        return path.parent
    return path.parent


def _scripted_candidate_profile_source_path(candidate: dict[str, Any], *, sample_root: Path) -> Path | None:
    metadata = dict(candidate.get("metadata") or {})
    raw_values = [
        metadata.get("profile_timeline_source_path"),
        metadata.get("profile_source_path"),
        candidate.get("profile_timeline_source_path"),
        candidate.get("source_path"),
    ]
    runtime_company_root = sample_root.parent
    for raw_value in raw_values:
        text = str(raw_value or "").strip()
        if not text:
            continue
        candidates: list[Path] = []
        marker = "/runtime/company_assets/"
        if marker in text:
            relative = text.split(marker, 1)[1]
            candidates.append(Path.cwd() / "runtime" / "company_assets" / relative)
            candidates.append(runtime_company_root.parent / relative)
        object_store_marker = "/runtime/object_store/"
        if object_store_marker in text:
            relative = text.split(object_store_marker, 1)[1]
            candidates.append(Path.cwd() / "runtime" / "object_store" / relative)
            candidates.append(runtime_company_root.parent.parent / "object_store" / relative)
        if text.startswith("runtime/object_store/"):
            relative = text.split("runtime/object_store/", 1)[1]
            candidates.append(Path.cwd() / "runtime" / "object_store" / relative)
            candidates.append(runtime_company_root.parent.parent / "object_store" / relative)
        candidates.append(Path(text).expanduser())
        if not Path(text).is_absolute():
            candidates.append(sample_root / text)
        seen_paths: set[str] = set()
        for candidate_path in candidates:
            path_key = str(candidate_path)
            if path_key in seen_paths:
                continue
            seen_paths.add(path_key)
            if candidate_path.exists() and candidate_path.is_file():
                try:
                    payload = json.loads(candidate_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if isinstance(payload, dict) and harvest_profile_payload_has_usable_content(payload):
                    return candidate_path
    profile_url = _scripted_candidate_profile_url(candidate)
    if profile_url:
        fallback_path = sample_root / "harvest_profiles" / f"{_profile_cache_key(profile_url)}.json"
        if fallback_path.exists():
            try:
                payload = json.loads(fallback_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                payload = {}
            if isinstance(payload, dict) and harvest_profile_payload_has_usable_content(payload):
                return fallback_path
    return None


def _resolve_scripted_sample_path(raw_path: Any) -> Path | None:
    text = str(raw_path or "").strip()
    if not text:
        return None
    path = Path(text).expanduser()
    candidates = [path] if path.is_absolute() else [Path.cwd() / path, Path(__file__).resolve().parents[2] / path]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _coerce_scripted_sample_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("candidates"), list) and (
        "candidate_count" in payload or "snapshot" in payload or "target_company" in payload
    ):
        return [dict(item) for item in payload.get("candidates") or [] if isinstance(item, dict)]
    for key in ("body", "items", "data", "results", "rows", "candidates"):
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(item) for item in value if isinstance(item, dict)]
    return []


def _slice_scripted_sample_rows_for_payload(
    rows: list[dict[str, Any]],
    *,
    logical_name: str,
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    if logical_name not in {"harvest_profile_search", "harvest_company_employees"}:
        return list(rows)
    max_items = _scripted_int(payload.get("maxItems"), 0)
    take_pages = max(1, _scripted_int(payload.get("takePages"), 1))
    start_page = max(1, _scripted_int(payload.get("startPage"), 1))
    page_size = _scripted_payload_page_size(rows, payload)
    offset = max(0, (start_page - 1) * page_size)
    cap = max_items if max_items > 0 else take_pages * page_size
    cap = max(1, min(cap, take_pages * page_size))
    return list(rows)[offset : offset + cap]


def _scripted_payload_page_size(rows: list[dict[str, Any]], payload: dict[str, Any]) -> int:
    for row in rows:
        meta = row.get("_meta")
        if isinstance(meta, dict):
            pagination = meta.get("pagination")
            if isinstance(pagination, dict):
                page_size = _scripted_int(pagination.get("pageSize") or pagination.get("page_size"), 0)
                if page_size > 0:
                    return page_size
    max_items = _scripted_int(payload.get("maxItems"), 0)
    return max(1, min(max_items if max_items > 0 else 25, 100))


def _scripted_profile_rows_for_requested_urls(
    rows: list[dict[str, Any]],
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    requested_urls = [str(item).strip() for item in list(payload.get("urls") or []) if str(item).strip()]
    if not requested_urls:
        return list(rows), []
    by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        for key in _scripted_profile_row_match_keys(row):
            by_key.setdefault(key, row)
    matched: list[dict[str, Any]] = []
    missing_urls: list[str] = []
    seen_rows: set[int] = set()
    for url in requested_urls:
        row = None
        for key in _scripted_profile_url_match_keys(url):
            row = by_key.get(key)
            if row is not None:
                break
        if row is None:
            missing_urls.append(url)
            continue
        row_identity = id(row)
        if row_identity in seen_rows:
            continue
        seen_rows.add(row_identity)
        matched.append(dict(row))
    return matched, missing_urls


def _scripted_profile_row_match_keys(row: dict[str, Any]) -> set[str]:
    values: list[Any] = []
    values.extend(list(row.get("_scripted_sample_profile_url_aliases") or []))
    for key in ("linkedinUrl", "profileUrl", "url", "publicIdentifier", "id"):
        values.append(row.get(key))
    item = row.get("item")
    if isinstance(item, dict):
        for key in ("linkedinUrl", "profileUrl", "url", "publicIdentifier", "id"):
            values.append(item.get(key))
    original_query = row.get("originalQuery")
    if isinstance(original_query, dict):
        values.append(original_query.get("url"))
    harvest_request = row.get("_harvest_request")
    if isinstance(harvest_request, dict):
        for key in ("value", "profile_url", "url"):
            values.append(harvest_request.get(key))
    keys: set[str] = set()
    for value in values:
        keys.update(_scripted_profile_url_match_keys(value))
    return keys


def _scripted_profile_url_match_keys(value: Any) -> set[str]:
    text = str(value or "").strip()
    if not text:
        return set()
    normalized = text.rstrip("/").lower()
    keys = {normalized}
    if "/in/" in normalized:
        identifier = normalized.rsplit("/in/", 1)[-1].strip("/")
        if identifier:
            keys.add(identifier)
    elif re.fullmatch(r"[a-z0-9_-]+", normalized):
        keys.add(normalized)
    return keys


def _scripted_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _scripted_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text if text else default


def _scripted_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _scripted_sample_fallback_generated_allowed(rule: dict[str, Any]) -> bool:
    return _scripted_bool(dict(rule or {}).get("sample_fallback_generated"), True)


def _scripted_template(template: Any, **values: Any) -> str:
    raw_template = str(template or "").strip()
    if not raw_template:
        return ""
    try:
        return raw_template.format(**values)
    except Exception:
        return raw_template


def _scripted_generated_slug(prefix: str, index: int, *, duplicate_every: int = 0) -> str:
    effective_index = int(index or 0)
    if duplicate_every > 0 and effective_index > 1 and effective_index % duplicate_every == 0:
        effective_index -= 1
    normalized_prefix = re.sub(r"[^a-z0-9]+", "-", str(prefix or "scripted-profile").lower()).strip("-")
    return f"{normalized_prefix}-{effective_index:04d}"


def _scripted_name_from_slug(slug: str) -> str:
    tokens = [token for token in re.split(r"[-_\s]+", str(slug or "")) if token]
    ignored = {"scripted", "linkedin", "profile"}
    selected = [token for token in tokens if token.lower() not in ignored and not token.isdigit()]
    if not selected:
        selected = ["Scripted", "Candidate"]
    return " ".join(token.capitalize() for token in selected[:4])


def _scripted_index_from_slug(slug: str, fallback: int) -> int:
    match = re.search(r"(\d+)(?!.*\d)", str(slug or ""))
    if not match:
        return int(fallback or 0)
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return int(fallback or 0)


def _scripted_generated_full_name(
    spec: dict[str, Any],
    *,
    index: int,
    slug: str,
    company: str,
    query: str,
    employment_scope: str = "",
) -> str:
    default_full_name = _scripted_name_from_slug(slug)
    template = spec.get("full_name_template") or spec.get("name_template")
    if template:
        slug_index = _scripted_index_from_slug(slug, index)
        rendered = _scripted_template(
            template,
            index=index,
            slug_index=slug_index,
            slug=slug,
            full_name=default_full_name,
            default_full_name=default_full_name,
            company=company,
            query=query,
            employment_scope=employment_scope,
        )
        if rendered:
            return rendered
    return default_full_name


def _build_scripted_generated_profile_search_body(
    *,
    spec: dict[str, Any],
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    estimated_total = max(
        0,
        _scripted_int(
            spec.get("estimated_total_count")
            or spec.get("total_count")
            or spec.get("totalElements")
            or payload.get("maxItems")
            or 0
        ),
    )
    max_items = max(1, _scripted_int(payload.get("maxItems"), 25))
    take_pages = max(1, _scripted_int(payload.get("takePages"), 1))
    start_page = max(1, _scripted_int(payload.get("startPage"), 1))
    page_size = max(1, _scripted_int(spec.get("page_size") or spec.get("pageSize"), 25))
    generated_cap = max(1, min(max_items, take_pages * page_size))
    explicit_returned_present = "returned_count" in spec or "returnedCount" in spec
    explicit_returned = _scripted_int(spec.get("returned_count", spec.get("returnedCount")), 0)
    if explicit_returned_present:
        returned_count = min(explicit_returned, generated_cap)
    elif estimated_total > 0:
        returned_count = min(estimated_total, generated_cap)
    else:
        returned_count = generated_cap
        estimated_total = max(estimated_total, returned_count)
    total_pages = max(
        1,
        _scripted_int(spec.get("total_pages") or spec.get("totalPages"), 0)
        or ((max(estimated_total, returned_count) + page_size - 1) // page_size),
    )
    company = _scripted_text(spec.get("company"), "OpenAI")
    employment_scope = _scripted_text(spec.get("employment_scope"), "current")
    search_query = _scripted_text(spec.get("search_query") or payload.get("searchQuery"), "Agent")
    prefix = _scripted_text(
        spec.get("linkedin_slug_prefix"),
        f"{normalize_company_key(company) or 'company'}-{normalize_company_key(search_query) or 'query'}-{employment_scope}",
    )
    headline_template = _scripted_text(
        spec.get("headline_template"),
        "{query} researcher at {company}",
    )
    location = _scripted_text(spec.get("location"), "San Francisco Bay Area")
    duplicate_every = max(0, _scripted_int(spec.get("duplicate_url_every"), 0))
    offset = (start_page - 1) * page_size
    rows: list[dict[str, Any]] = []
    for item_index in range(offset + 1, offset + returned_count + 1):
        slug = _scripted_generated_slug(prefix, item_index, duplicate_every=duplicate_every)
        full_name = _scripted_generated_full_name(
            spec,
            index=item_index,
            slug=slug,
            company=company,
            query=search_query,
            employment_scope=employment_scope,
        )
        headline = _scripted_template(
            headline_template,
            index=item_index,
            slug=slug,
            full_name=full_name,
            company=company,
            query=search_query,
            employment_scope=employment_scope,
        )
        profile_url = f"https://www.linkedin.com/in/{slug}/"
        row = {
            "firstName": full_name.split(" ", 1)[0],
            "lastName": full_name.split(" ", 1)[1] if " " in full_name else f"{item_index:04d}",
            "fullName": full_name,
            "linkedinUrl": profile_url,
            "profileUrl": profile_url,
            "publicIdentifier": slug,
            "headline": headline,
            "currentCompany": company if employment_scope != "former" else "",
            "location": location,
            "item": {
                "fullName": full_name,
                "linkedinUrl": profile_url,
                "profileUrl": profile_url,
                "publicIdentifier": slug,
                "headline": headline,
                "currentCompany": company if employment_scope != "former" else "",
                "location": location,
                "summary": _scripted_template(
                    spec.get("summary_template") or "Works on {query} systems and applied research at {company}.",
                    index=item_index,
                    slug=slug,
                    full_name=full_name,
                    company=company,
                    query=search_query,
                    employment_scope=employment_scope,
                ),
            },
            "_meta": {
                "pagination": {
                    "totalElements": estimated_total,
                    "totalPages": total_pages,
                    "pageNumber": start_page,
                    "pageSize": page_size,
                }
            },
        }
        if employment_scope == "former":
            row["pastCompany"] = company
            row["item"]["pastCompany"] = company
        rows.append(row)
    return rows


def _build_scripted_generated_profile_scraper_body(
    *,
    spec: dict[str, Any],
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    urls = [str(item).strip() for item in list(payload.get("urls") or []) if str(item).strip()]
    max_profiles = _scripted_int(spec.get("max_profiles"), 0)
    if max_profiles > 0:
        urls = urls[:max_profiles]
    company = _scripted_text(spec.get("company"), "OpenAI")
    search_query = _scripted_text(spec.get("search_query"), "Agent")
    location = _scripted_text(spec.get("location"), "San Francisco Bay Area")
    title_template = _scripted_text(spec.get("title_template"), "{query} Research Engineer")
    about_template = _scripted_text(
        spec.get("about_template"),
        "Builds {query} models, agent infrastructure, evaluation systems, and product-facing research workflows.",
    )
    rows: list[dict[str, Any]] = []
    for index, profile_url in enumerate(urls, start=1):
        slug = _offline_profile_identifier(profile_url)
        full_name = _scripted_generated_full_name(
            spec,
            index=index,
            slug=slug,
            company=company,
            query=search_query,
        )
        title = _scripted_template(
            title_template,
            index=index,
            slug=slug,
            full_name=full_name,
            company=company,
            query=search_query,
        )
        headline = _scripted_template(
            spec.get("headline_template") or "{title} at {company}",
            index=index,
            slug=slug,
            full_name=full_name,
            company=company,
            query=search_query,
            title=title,
        )
        item = {
            "profileUrl": profile_url,
            "linkedinUrl": profile_url,
            "publicIdentifier": slug,
            "fullName": full_name,
            "headline": headline,
            "location": location,
            "currentCompany": company,
            "photoUrl": f"https://cdn.example.com/{slug}.jpg",
            "about": _scripted_template(
                about_template,
                index=index,
                slug=slug,
                full_name=full_name,
                company=company,
                query=search_query,
                title=title,
            ),
            "experience": [
                {
                    "companyName": company,
                    "title": title,
                    "dateRange": {
                        "start": {"year": 2024},
                    },
                },
                {
                    "companyName": _scripted_text(spec.get("previous_company"), "Google"),
                    "title": _scripted_text(spec.get("previous_title"), "Research Engineer"),
                    "dateRange": {
                        "start": {"year": 2021},
                        "end": {"year": 2024},
                    },
                },
            ],
            "education": [
                {
                    "schoolName": _scripted_text(spec.get("school"), "Stanford University"),
                    "degreeName": _scripted_text(spec.get("degree"), "MS"),
                    "fieldOfStudy": _scripted_text(spec.get("field_of_study"), "Computer Science"),
                }
            ],
            "skills": list(spec.get("skills") or [search_query, "LLM", "Evaluation", "Distributed systems"]),
        }
        rows.append(
            {
                "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                "linkedinUrl": profile_url,
                "profileUrl": profile_url,
                "publicIdentifier": slug,
                "fullName": full_name,
                "headline": headline,
                "currentCompany": company,
                "location": location,
                "item": item,
            }
        )
    return rows


def _build_scripted_generated_company_employee_body(
    *,
    spec: dict[str, Any],
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    company = _scripted_text(spec.get("company"), "OpenAI")
    search_query = _scripted_text(spec.get("search_query") or payload.get("searchQuery"), "Agent")
    count = max(0, _scripted_int(spec.get("count") or spec.get("returned_count") or payload.get("maxItems"), 25))
    prefix = _scripted_text(
        spec.get("linkedin_slug_prefix"),
        f"{normalize_company_key(company) or 'company'}-{normalize_company_key(search_query) or 'query'}-employee",
    )
    headline_template = _scripted_text(spec.get("headline_template"), "{query} engineer at {company}")
    rows: list[dict[str, Any]] = []
    for index in range(1, count + 1):
        slug = _scripted_generated_slug(prefix, index)
        full_name = _scripted_generated_full_name(
            spec,
            index=index,
            slug=slug,
            company=company,
            query=search_query,
        )
        profile_url = f"https://www.linkedin.com/in/{slug}/"
        headline = _scripted_template(
            headline_template,
            index=index,
            slug=slug,
            full_name=full_name,
            company=company,
            query=search_query,
        )
        rows.append(
            {
                "linkedinUrl": profile_url,
                "profileUrl": profile_url,
                "publicIdentifier": slug,
                "fullName": full_name,
                "headline": headline,
                "currentCompany": company,
                "item": {
                    "linkedinUrl": profile_url,
                    "profileUrl": profile_url,
                    "publicIdentifier": slug,
                    "fullName": full_name,
                    "headline": headline,
                    "currentCompany": company,
                },
            }
        )
    return rows
