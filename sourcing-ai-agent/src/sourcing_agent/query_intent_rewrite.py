from __future__ import annotations

"""Composition layer for shorthand rewrites.

Stable knowledge lives in query_signal_knowledge.py, business shorthand lives in
query_intent_policy.py, and this module only composes matched policies into the
legacy rewrite payload shape consumed elsewhere in the workflow.
"""

from typing import Any

from .query_intent_policy import match_business_rewrite_policies


def interpret_query_intent_rewrite(text: str) -> dict[str, Any]:
    matched_rewrites = match_business_rewrite_policies(text)

    if not matched_rewrites:
        return {}

    primary = dict(matched_rewrites[0])
    additional = [item for item in matched_rewrites[1:]]
    aggregated_request_patch = _merge_request_patches(
        item.get("request_patch")
        for item in matched_rewrites
    )
    matched_terms = [
        token
        for item in matched_rewrites
        for token in list(item.get("matched_terms") or [])
    ]
    primary["matched_terms"] = _merge_string_lists([], matched_terms)[:12]
    primary["request_patch"] = aggregated_request_patch
    for patch_key, patch_values in aggregated_request_patch.items():
        primary[patch_key] = list(patch_values)
    primary["targeting_terms"] = _merge_string_lists(
        primary.get("targeting_terms"),
        [token for item in additional for token in list(item.get("targeting_terms") or [])],
    )
    if additional:
        primary["additional_rewrites"] = [
            {
                "rewrite_id": str(item.get("rewrite_id") or "").strip(),
                "summary_label": str(item.get("summary_label") or "").strip(),
                "policy_layer": str(item.get("policy_layer") or "").strip(),
                "matched_terms": list(item.get("matched_terms") or []),
                "request_patch": dict(item.get("request_patch") or {}),
            }
            for item in additional
            if str(item.get("rewrite_id") or "").strip()
        ]
    return {
        **primary,
    }


def apply_query_intent_rewrite(payload: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(payload or {})
    raw_text = " ".join(
        item
        for item in [
            str(merged.get("raw_user_request") or "").strip(),
            str(merged.get("query") or "").strip(),
        ]
        if item
    )
    rewrite = interpret_query_intent_rewrite(raw_text)
    if not rewrite:
        return merged

    for patch_key, patch_values in dict(rewrite.get("request_patch") or {}).items():
        merged[patch_key] = _merge_string_lists(merged.get(patch_key), patch_values)
    merged["must_have_keywords"] = _merge_string_lists(merged.get("must_have_keywords"), rewrite.get("must_have_keywords"))
    return merged


def summarize_query_intent_rewrite(text: str) -> str:
    rewrite = interpret_query_intent_rewrite(text)
    if not rewrite:
        return ""
    targeting_terms = [str(item).strip() for item in list(rewrite.get("targeting_terms") or []) if str(item).strip()]
    if not targeting_terms:
        return ""
    return (
        f"自然语言简称改写：{str(rewrite.get('summary_label') or '').strip()} -> "
        + " / ".join(targeting_terms[:3])
    )


def _merge_string_lists(existing: Any, patch: Any) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for source in [existing, patch]:
        if isinstance(source, str):
            items = [source]
        else:
            items = list(source or [])
        for item in items:
            value = str(item or "").strip()
            if not value:
                continue
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(value)
    return merged


def _merge_request_patches(sources: Any) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for source in list(sources or []):
        for key, values in dict(source or {}).items():
            merged[key] = _merge_string_lists(merged.get(key), values)
    return {key: value for key, value in merged.items() if value}
