from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from hashlib import sha1
import json
import os
from pathlib import Path
import threading
import time
from typing import Any


_SCRIPTED_SCENARIO_ENV = "SOURCING_SCRIPTED_PROVIDER_SCENARIO"
_SCRIPTED_PROVIDER_INVOCATION_LOG = "scripted_provider_invocations.jsonl"
_SCRIPTED_PROVIDER_INVOCATION_LOCK = threading.Lock()
_SCRIPTED_PROVIDER_REQUIRED_SCENARIO_CATEGORIES = (
    "retryable_error",
    "timeout_error",
    "partial_result",
    "staged_ready_fetch",
)


def load_scripted_provider_scenario() -> dict[str, Any]:
    raw_path = str(os.getenv(_SCRIPTED_SCENARIO_ENV) or "").strip()
    if not raw_path:
        return {}
    path = Path(raw_path).expanduser()
    if not path.exists():
        return {}
    try:
        mtime_ns = path.stat().st_mtime_ns
    except OSError:
        return {}
    return _load_scripted_provider_scenario_cached(str(path), mtime_ns)


@lru_cache(maxsize=8)
def _load_scripted_provider_scenario_cached(path: str, mtime_ns: int) -> dict[str, Any]:
    del mtime_ns
    try:
        payload = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def summarize_scripted_provider_scenario(payload: dict[str, Any] | None) -> dict[str, Any]:
    scenario = dict(payload or {})
    rules = _iter_scripted_rules(scenario)
    categories = {key: 0 for key in _SCRIPTED_PROVIDER_REQUIRED_SCENARIO_CATEGORIES}
    provider_counts: dict[str, int] = {}
    rule_summaries: list[dict[str, Any]] = []
    for provider_name, rule in rules:
        provider_counts[provider_name] = int(provider_counts.get(provider_name) or 0) + 1
        rule_categories = _scripted_rule_categories(rule)
        for category in rule_categories:
            if category in categories:
                categories[category] += 1
        rule_summaries.append(
            {
                "provider": provider_name,
                "name": str(rule.get("name") or rule.get("_rule_name") or ""),
                "categories": sorted(rule_categories),
                "has_body": isinstance(rule.get("body"), list),
                "result_count": len(list(rule.get("results") or [])) if isinstance(rule.get("results"), list) else 0,
            }
        )
    missing_categories = [key for key, count in categories.items() if int(count or 0) <= 0]
    return {
        "rule_count": len(rule_summaries),
        "provider_counts": provider_counts,
        "coverage": categories,
        "missing_categories": missing_categories,
        "complete": not missing_categories,
        "rules": rule_summaries,
    }


def validate_scripted_provider_scenario(payload: dict[str, Any] | None) -> dict[str, Any]:
    summary = summarize_scripted_provider_scenario(payload)
    return {
        "status": "valid" if bool(summary.get("complete")) else "incomplete",
        "summary": summary,
        "required_categories": list(_SCRIPTED_PROVIDER_REQUIRED_SCENARIO_CATEGORIES),
        "missing_categories": list(summary.get("missing_categories") or []),
    }


def _iter_scripted_rules(scenario: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    collected: list[tuple[str, dict[str, Any]]] = []
    for provider_name in ("search", "harvest"):
        section = dict(scenario.get(provider_name) or {})
        for rule in list(section.get("rules") or []):
            if isinstance(rule, dict):
                collected.append((provider_name, dict(rule)))
        default_rule = section.get("default")
        if isinstance(default_rule, dict):
            collected.append((provider_name, {"name": f"{provider_name}_default", **dict(default_rule)}))
    return collected


def _scripted_rule_categories(rule: dict[str, Any]) -> set[str]:
    categories: set[str] = set()
    for error in list(rule.get("errors") or []):
        if not isinstance(error, dict):
            continue
        kind = str(error.get("kind") or error.get("type") or "").strip().lower()
        status = str(error.get("status") or "").strip().lower()
        message = str(error.get("message") or "").strip().lower()
        if kind == "retryable" or status in {"429", "too_many_requests", "rate_limited"} or "429" in message:
            categories.add("retryable_error")
        if kind == "timeout" or "timeout" in status or "timeout" in message or "timed out" in message:
            categories.add("timeout_error")
    if any(int(_safe_positive_int(rule.get(key)) or 0) > 0 for key in ("execute_pending_rounds", "poll_pending_rounds")):
        categories.add("staged_ready_fetch")
    if any(
        int(_safe_positive_int(rule.get(key)) or 0) > 0
        for key in (
            "pending_rounds",
            "execute_after_rounds",
            "poll_after_rounds",
            "fetch_after_rounds",
        )
    ):
        categories.add("staged_ready_fetch")
    artifacts = list(rule.get("artifacts") or [])
    if any(
        isinstance(item, dict) and str(item.get("phase") or "").strip().lower() in {"poll", "fetch"}
        for item in artifacts
    ):
        categories.add("staged_ready_fetch")
    if bool(rule.get("partial_result") or rule.get("partial_results")):
        categories.add("partial_result")
    if isinstance(rule.get("body"), list):
        expected = _safe_positive_int(rule.get("expected_total_count") or rule.get("estimated_total_count"))
        if expected is not None and len(list(rule.get("body") or [])) < expected:
            categories.add("partial_result")
    if isinstance(rule.get("results"), list):
        expected = _safe_positive_int(rule.get("expected_total_count") or rule.get("estimated_total_count"))
        if expected is not None and len(list(rule.get("results") or [])) < expected:
            categories.add("partial_result")
    return categories


def _safe_positive_int(value: Any) -> int | None:
    try:
        coerced = int(value)
    except (TypeError, ValueError):
        return None
    return coerced if coerced > 0 else None


def scripted_provider_invocation_log_path() -> Path | None:
    runtime_dir = str(os.getenv("SOURCING_RUNTIME_DIR") or "").strip()
    if not runtime_dir:
        return None
    return Path(runtime_dir).expanduser().resolve() / _SCRIPTED_PROVIDER_INVOCATION_LOG


def record_scripted_provider_invocation(
    *,
    provider_name: str,
    dispatch_kind: str,
    logical_name: str = "",
    query_text: str = "",
    task_key: str = "",
    payload: Any = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    log_path = scripted_provider_invocation_log_path()
    if log_path is None:
        return {}
    normalized_payload = _normalized_invocation_payload(payload)
    signature_payload = {
        "provider_name": str(provider_name or "").strip(),
        "dispatch_kind": str(dispatch_kind or "").strip(),
        "logical_name": str(logical_name or "").strip(),
        "query_text": " ".join(str(query_text or "").split()).strip(),
        "task_key": " ".join(str(task_key or "").split()).strip(),
        "payload": normalized_payload,
    }
    signature_text = json.dumps(signature_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    recorded_at = datetime.now(timezone.utc).isoformat()
    event = {
        "recorded_at": recorded_at,
        "provider_name": str(provider_name or "").strip(),
        "dispatch_kind": str(dispatch_kind or "").strip(),
        "logical_name": str(logical_name or "").strip(),
        "query_text": " ".join(str(query_text or "").split()).strip(),
        "task_key": " ".join(str(task_key or "").split()).strip(),
        "dispatch_signature": sha1(signature_text.encode("utf-8")).hexdigest()[:16],
        "payload_hash": sha1(
            json.dumps(normalized_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:16],
        "payload": normalized_payload,
        "metadata": dict(metadata or {}),
    }
    log_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(event, ensure_ascii=False, sort_keys=True)
    with _SCRIPTED_PROVIDER_INVOCATION_LOCK:
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.write("\n")
    return event


def load_scripted_provider_invocations(
    *,
    started_at: datetime | str | None = None,
    completed_at: datetime | str | None = None,
) -> list[dict[str, Any]]:
    log_path = scripted_provider_invocation_log_path()
    if log_path is None or not log_path.exists():
        return []
    start_dt = _coerce_timestamp(started_at)
    end_dt = _coerce_timestamp(completed_at)
    records: list[dict[str, Any]] = []
    try:
        lines = log_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return records
    for line in lines:
        normalized_line = str(line or "").strip()
        if not normalized_line:
            continue
        try:
            payload = json.loads(normalized_line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        recorded_dt = _coerce_timestamp(payload.get("recorded_at"))
        if start_dt is not None and recorded_dt is not None and recorded_dt < start_dt:
            continue
        if end_dt is not None and recorded_dt is not None and recorded_dt > end_dt:
            continue
        records.append(payload)
    return records


def find_scripted_rule(section: str, *, context: dict[str, Any]) -> dict[str, Any]:
    scenario = load_scripted_provider_scenario()
    section_payload = dict(scenario.get(section) or {})
    rules = list(section_payload.get("rules") or [])
    if isinstance(section_payload.get("default"), dict):
        rules.append({"name": f"{section}_default", **dict(section_payload.get("default") or {})})
    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            continue
        if _scripted_rule_matches(rule, context):
            return {
                **rule,
                "_rule_index": index,
                "_rule_name": str(rule.get("name") or f"{section}_rule_{index:03d}"),
            }
    return {}


def scripted_phase_round(checkpoint: dict[str, Any] | None, *, phase: str) -> int:
    checkpoint = dict(checkpoint or {})
    try:
        return int(checkpoint.get(f"scripted_{phase}_round") or 0)
    except (TypeError, ValueError):
        return 0


def advance_scripted_phase_round(
    checkpoint: dict[str, Any] | None,
    *,
    phase: str,
) -> tuple[dict[str, Any], int]:
    updated = dict(checkpoint or {})
    round_number = scripted_phase_round(updated, phase=phase) + 1
    updated[f"scripted_{phase}_round"] = round_number
    return updated, round_number


def scripted_pending_rounds(rule: dict[str, Any], *, phase: str, default: int = 0) -> int:
    for key in (
        f"{phase}_pending_rounds",
        f"{phase}_after_rounds",
        f"{phase}_after_polls",
        f"{phase}_after_attempts",
    ):
        try:
            value = int(rule.get(key) or 0)
        except (TypeError, ValueError):
            value = 0
        if value > 0:
            return value
    try:
        return int(rule.get("pending_rounds") or default)
    except (TypeError, ValueError):
        return default


def scripted_sleep(
    rule: dict[str, Any],
    *,
    phase: str,
    seconds_cap: float | None = None,
) -> None:
    for key in (f"{phase}_sleep_seconds", "sleep_seconds"):
        raw = rule.get(key)
        if raw in (None, "", 0, 0.0):
            continue
        try:
            seconds = float(raw)
        except (TypeError, ValueError):
            continue
        if seconds_cap is not None:
            seconds = min(seconds, max(0.0, float(seconds_cap)))
        if seconds > 0:
            time.sleep(seconds)
            return


def scripted_phase_error(rule: dict[str, Any], *, phase: str, round_number: int) -> dict[str, Any]:
    errors = list(rule.get("errors") or [])
    for item in errors:
        if not isinstance(item, dict):
            continue
        if str(item.get("phase") or "").strip().lower() != str(phase or "").strip().lower():
            continue
        try:
            error_round = int(item.get("round") or 1)
        except (TypeError, ValueError):
            error_round = 1
        if error_round == int(round_number or 0):
            return dict(item)
    return {}


def scripted_context_text(context: dict[str, Any]) -> str:
    try:
        return json.dumps(context, ensure_ascii=False, sort_keys=True).lower()
    except TypeError:
        return str(context).lower()


def scripted_rule_artifacts(rule: dict[str, Any], *, phase: str) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for item in list(rule.get("artifacts") or []):
        if not isinstance(item, dict):
            continue
        artifact_phase = str(item.get("phase") or "").strip().lower()
        if artifact_phase and artifact_phase != str(phase or "").strip().lower():
            continue
        artifacts.append(dict(item))
    return artifacts


def _scripted_rule_matches(rule: dict[str, Any], context: dict[str, Any]) -> bool:
    match = dict(rule.get("match") or {})
    if not match:
        return True
    query_text = " ".join(str(context.get("query_text") or "").split()).strip().lower()
    task_key = str(context.get("task_key") or "").strip().lower()
    logical_name = str(context.get("logical_name") or "").strip().lower()
    provider_name = str(context.get("provider_name") or "").strip().lower()
    context_text = scripted_context_text(context)

    if str(match.get("logical_name") or "").strip().lower():
        if logical_name != str(match.get("logical_name") or "").strip().lower():
            return False
    if str(match.get("provider_name") or "").strip().lower():
        if provider_name != str(match.get("provider_name") or "").strip().lower():
            return False
    if str(match.get("query_equals") or "").strip().lower():
        if query_text != str(match.get("query_equals") or "").strip().lower():
            return False
    if str(match.get("task_key_equals") or "").strip().lower():
        if task_key != str(match.get("task_key_equals") or "").strip().lower():
            return False
    if not _all_terms_in_text(match.get("query_contains"), query_text):
        return False
    if not _all_terms_in_text(match.get("task_key_contains"), task_key):
        return False
    if not _all_terms_in_text(match.get("payload_contains"), context_text):
        return False
    if not _all_terms_in_text(match.get("context_contains"), context_text):
        return False
    return True


def _all_terms_in_text(values: Any, haystack: str) -> bool:
    if values in (None, "", [], (), set()):
        return True
    if isinstance(values, str):
        candidates = [values]
    else:
        candidates = list(values or [])
    for item in candidates:
        needle = " ".join(str(item or "").split()).strip().lower()
        if needle and needle not in haystack:
            return False
    return True


def _normalized_invocation_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        normalized: dict[str, Any] = {}
        for key, value in sorted(payload.items(), key=lambda item: str(item[0])):
            normalized[str(key)] = _normalized_invocation_payload(value)
        return normalized
    if isinstance(payload, list):
        return [_normalized_invocation_payload(item) for item in payload]
    if isinstance(payload, tuple):
        return [_normalized_invocation_payload(item) for item in payload]
    if isinstance(payload, set):
        normalized_items = [_normalized_invocation_payload(item) for item in payload]
        return sorted(normalized_items, key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
    if isinstance(payload, (str, int, float, bool)) or payload is None:
        return payload
    return str(payload)


def _coerce_timestamp(value: datetime | str | None) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    normalized = str(value or "").strip()
    if not normalized:
        return None
    for candidate in (normalized, normalized.replace("Z", "+00:00")):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
    return None
