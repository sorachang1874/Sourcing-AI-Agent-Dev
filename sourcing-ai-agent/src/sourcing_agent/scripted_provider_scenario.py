from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from functools import lru_cache
from hashlib import sha1
from pathlib import Path
from typing import Any

from .runtime_environment import normalize_provider_mode

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
    scenario_path = Path(path)
    return _load_scripted_provider_scenario_file(scenario_path, seen=set())


def _load_scripted_provider_scenario_file(path: Path, *, seen: set[Path]) -> dict[str, Any]:
    resolved_path = path.expanduser().resolve()
    if resolved_path in seen:
        return {}
    seen.add(resolved_path)
    try:
        payload = json.loads(resolved_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return _resolve_scripted_provider_scenario_includes(payload, base_path=resolved_path.parent, seen=seen)


def _resolve_scripted_provider_scenario_includes(
    payload: dict[str, Any],
    *,
    base_path: Path,
    seen: set[Path],
) -> dict[str, Any]:
    include_paths = [
        str(item or "").strip()
        for item in list(payload.get("includes") or payload.get("extends") or [])
        if str(item or "").strip()
    ]
    if not include_paths:
        return dict(payload)
    merged: dict[str, Any] = {}
    for include_path in include_paths:
        include_file = Path(include_path).expanduser()
        if not include_file.is_absolute():
            include_file = base_path / include_file
        included_payload = _load_scripted_provider_scenario_file(include_file, seen=seen)
        merged = _merge_scripted_provider_scenarios(merged, included_payload)
    current_payload = dict(payload)
    current_payload.pop("includes", None)
    current_payload.pop("extends", None)
    return _merge_scripted_provider_scenarios(merged, current_payload)


def _merge_scripted_provider_scenarios(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    if not base:
        base = {}
    if not overlay:
        return dict(base)
    merged = {key: value for key, value in dict(base).items()}
    for key, value in dict(overlay).items():
        if key in {"search", "harvest"} and isinstance(value, dict):
            base_section = dict(merged.get(key) or {})
            overlay_section = dict(value or {})
            base_rules = list(base_section.get("rules") or [])
            overlay_rules = list(overlay_section.get("rules") or [])
            section = {**base_section, **overlay_section}
            if base_rules or overlay_rules:
                section["rules"] = _merge_scripted_provider_rules_by_name(base_rules, overlay_rules)
            merged[key] = section
        elif key == "meta" and isinstance(value, dict):
            merged[key] = {**dict(merged.get(key) or {}), **dict(value or {})}
        else:
            merged[key] = value
    return merged


def _merge_scripted_provider_rules_by_name(
    base_rules: list[Any],
    overlay_rules: list[Any],
) -> list[Any]:
    merged_rules = [dict(rule) if isinstance(rule, dict) else rule for rule in base_rules]
    rule_index_by_name = {
        str(rule.get("name") or "").strip(): index
        for index, rule in enumerate(merged_rules)
        if isinstance(rule, dict) and str(rule.get("name") or "").strip()
    }
    for overlay_rule in overlay_rules:
        if not isinstance(overlay_rule, dict):
            merged_rules.append(overlay_rule)
            continue
        rule_name = str(overlay_rule.get("name") or "").strip()
        if rule_name and rule_name in rule_index_by_name and isinstance(merged_rules[rule_index_by_name[rule_name]], dict):
            index = rule_index_by_name[rule_name]
            merged_rules[index] = {**dict(merged_rules[index]), **dict(overlay_rule)}
            continue
        if rule_name:
            rule_index_by_name[rule_name] = len(merged_rules)
        merged_rules.append(dict(overlay_rule))
    return merged_rules


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
                "has_body": isinstance(rule.get("body"), list) or isinstance(rule.get("generated_body"), dict),
                "result_count": _scripted_rule_result_count(rule),
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
    if isinstance(rule.get("generated_body"), dict):
        generated = dict(rule.get("generated_body") or {})
        expected = _safe_positive_int(
            generated.get("expected_total_count")
            or generated.get("estimated_total_count")
            or generated.get("total_count")
            or rule.get("expected_total_count")
            or rule.get("estimated_total_count")
        )
        result_count = _scripted_rule_generated_result_count(generated)
        if expected is not None and result_count is not None and result_count < expected:
            categories.add("partial_result")
    if isinstance(rule.get("results"), list):
        expected = _safe_positive_int(rule.get("expected_total_count") or rule.get("estimated_total_count"))
        if expected is not None and len(list(rule.get("results") or [])) < expected:
            categories.add("partial_result")
    return categories


def _scripted_rule_result_count(rule: dict[str, Any]) -> int:
    if isinstance(rule.get("results"), list):
        return len(list(rule.get("results") or []))
    if isinstance(rule.get("body"), list):
        return len(list(rule.get("body") or []))
    if isinstance(rule.get("generated_body"), dict):
        generated_count = _scripted_rule_generated_result_count(dict(rule.get("generated_body") or {}))
        if generated_count is not None:
            return generated_count
    return 0


def _scripted_rule_generated_result_count(generated: dict[str, Any]) -> int | None:
    for key in ("returned_count", "count", "max_profiles"):
        coerced = _safe_positive_int(generated.get(key))
        if coerced is not None:
            return coerced
    return _safe_positive_int(generated.get("estimated_total_count") or generated.get("total_count"))


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


def _semantic_invocation_signature_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    raw_metadata = dict(metadata or {})
    request_context = dict(raw_metadata.get("request_context") or {})
    semantic_context: dict[str, Any] = {}
    for key in ("zero_result_retry_attempt",):
        if key in request_context:
            semantic_context[key] = request_context.get(key)
    if not semantic_context:
        return {}
    return {"request_context": semantic_context}


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
    semantic_metadata = _semantic_invocation_signature_metadata(metadata)
    if semantic_metadata:
        signature_payload["metadata"] = semantic_metadata
    signature_text = json.dumps(signature_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    recorded_at = datetime.now(timezone.utc).isoformat()
    event = {
        "recorded_at": recorded_at,
        "provider_mode": normalize_provider_mode(),
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
    return find_scripted_rule_in_scenario(scenario, section, context=context)


def find_scripted_rule_in_scenario(
    scenario: dict[str, Any] | None,
    section: str,
    *,
    context: dict[str, Any],
) -> dict[str, Any]:
    scenario = dict(scenario or {})
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
    seconds = scripted_sleep_seconds(rule, phase=phase, seconds_cap=seconds_cap)
    if seconds > 0:
        time.sleep(seconds)


def scripted_sleep_seconds(
    rule: dict[str, Any],
    *,
    phase: str,
    seconds_cap: float | None = None,
) -> float:
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
            return seconds
    return 0.0


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


def scripted_payload_text(context: dict[str, Any]) -> str:
    """Serialize only the provider request payload for payload_* rule matching."""

    payload = dict(context or {}).get("payload")
    try:
        return json.dumps(payload, ensure_ascii=False, sort_keys=True).lower()
    except TypeError:
        return str(payload).lower()


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
    payload_text = scripted_payload_text(context)
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
    if not _all_terms_in_text(match.get("payload_contains"), payload_text):
        return False
    if not _all_terms_in_text(match.get("context_contains"), context_text):
        return False
    if not _mapping_contains_expected(context.get("payload"), match.get("payload_equals") or match.get("payload_match")):
        return False
    if not _mapping_contains_expected(context, match.get("context_equals") or match.get("context_match")):
        return False
    if _any_terms_in_text(match.get("query_not_contains") or match.get("query_excludes"), query_text):
        return False
    if _any_terms_in_text(match.get("task_key_not_contains") or match.get("task_key_excludes"), task_key):
        return False
    if _any_terms_in_text(match.get("payload_not_contains") or match.get("payload_excludes"), payload_text):
        return False
    if _any_terms_in_text(match.get("context_not_contains") or match.get("context_excludes"), context_text):
        return False
    return True


def _mapping_contains_expected(actual: Any, expected: Any) -> bool:
    if expected in (None, "", [], (), set()):
        return True
    if not isinstance(expected, dict):
        return _scripted_values_equal(actual, expected)
    if not isinstance(actual, dict):
        return False
    for key, expected_value in expected.items():
        if key not in actual:
            return False
        actual_value = actual.get(key)
        if isinstance(expected_value, dict):
            if not _mapping_contains_expected(actual_value, expected_value):
                return False
            continue
        if isinstance(expected_value, list):
            actual_list = actual_value if isinstance(actual_value, list) else [actual_value]
            for item in expected_value:
                if not any(_scripted_values_equal(candidate, item) for candidate in actual_list):
                    return False
            continue
        if not _scripted_values_equal(actual_value, expected_value):
            return False
    return True


def _scripted_values_equal(actual: Any, expected: Any) -> bool:
    if actual == expected:
        return True
    if isinstance(expected, bool):
        if isinstance(actual, bool):
            return actual is expected
        return str(actual).strip().lower() in ({"true", "1", "yes"} if expected else {"false", "0", "no"})
    if isinstance(expected, int | float) and not isinstance(expected, bool):
        try:
            return float(actual) == float(expected)
        except (TypeError, ValueError):
            return False
    return str(actual or "").strip().lower() == str(expected or "").strip().lower()


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


def _any_terms_in_text(values: Any, haystack: str) -> bool:
    if values in (None, "", [], (), set()):
        return False
    if isinstance(values, str):
        candidates = [values]
    else:
        candidates = list(values or [])
    for item in candidates:
        needle = " ".join(str(item or "").split()).strip().lower()
        if needle and needle in haystack:
            return True
    return False


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
