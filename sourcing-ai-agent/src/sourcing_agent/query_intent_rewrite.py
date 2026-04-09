from __future__ import annotations

from typing import Any


_GREATER_CHINA_HINTS = (
    "华人",
    "泛华人",
    "华语圈",
    "华人圈",
    "chinese member",
    "chinese members",
    "chinese employee",
    "chinese employees",
    "chinese researcher",
    "chinese researchers",
    "greater china talent",
    "greater china members",
)

_GREATER_CHINA_REWRITE = {
    "rewrite_id": "greater_china_outreach",
    "summary_label": "华人 / 泛华人简称",
    "keywords": [
        "Greater China experience",
        "Chinese bilingual outreach",
    ],
    "targeting_terms": [
        "中国大陆 / 港澳台 / 新加坡公开学习或工作经历",
        "中文 / 双语 outreach 适配",
    ],
    "notes": "默认按公开地区经历与语言 / outreach 适配理解，不输出族裔、国籍或身份标签判断。",
}


def interpret_query_intent_rewrite(text: str) -> dict[str, Any]:
    normalized = " ".join(str(text or "").strip().split())
    if not normalized:
        return {}
    lower = normalized.lower()

    matched_terms: list[str] = []
    for token in _GREATER_CHINA_HINTS:
        if token in normalized or token in lower:
            if token not in matched_terms:
                matched_terms.append(token)
    if not matched_terms:
        return {}

    return {
        **_GREATER_CHINA_REWRITE,
        "matched_terms": matched_terms[:8],
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

    merged["keywords"] = _merge_string_lists(merged.get("keywords"), rewrite.get("keywords"))
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
