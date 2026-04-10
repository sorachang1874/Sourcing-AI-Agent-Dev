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

_MULTIMODAL_PROJECT_HINTS = (
    "多模态",
    "multimodal",
    "multimodality",
    "vision-language",
    "vision language",
    "veo",
    "nano banana",
)

_MULTIMODAL_PROJECT_REWRITE = {
    "rewrite_id": "multimodal_project_focus",
    "summary_label": "多模态 / 项目线索",
    "keywords": [
        "multimodal",
        "vision-language",
        "video generation",
        "Veo",
        "Nano Banana",
    ],
    "must_have_facets": [
        "multimodal",
    ],
    "notes": "将项目名与多模态能力线索合并为可审计职业关键词。",
}

_RESEARCHER_ROLE_HINTS = (
    "研究员",
    "研究科学家",
    "researcher",
    "research scientist",
    "applied scientist",
)

_RESEARCHER_ROLE_REWRITE = {
    "rewrite_id": "researcher_role_focus",
    "summary_label": "研究员角色",
    "must_have_facets": [
        "research",
    ],
    "must_have_primary_role_buckets": [
        "research",
    ],
    "notes": "优先收敛到研究员/研究科学家角色。",  # role bucket filter for retrieval stage
}


def interpret_query_intent_rewrite(text: str) -> dict[str, Any]:
    normalized = " ".join(str(text or "").strip().split())
    if not normalized:
        return {}
    lower = normalized.lower()

    matched_rewrites: list[dict[str, Any]] = []
    matched_terms: list[str] = []

    greater_terms = _matched_terms_for_tokens(normalized, lower, _GREATER_CHINA_HINTS)
    if greater_terms:
        matched_rewrites.append({**_GREATER_CHINA_REWRITE, "matched_terms": greater_terms[:8]})
        matched_terms.extend(greater_terms)

    # Multimodal / researcher rewrites are enabled only inside Greater-China shorthand context,
    # so generic queries (for example "coding researcher") remain unchanged.
    if greater_terms:
        multimodal_terms = _matched_terms_for_tokens(normalized, lower, _MULTIMODAL_PROJECT_HINTS)
        if multimodal_terms:
            matched_rewrites.append({**_MULTIMODAL_PROJECT_REWRITE, "matched_terms": multimodal_terms[:8]})
            matched_terms.extend(multimodal_terms)

        researcher_terms = _matched_terms_for_tokens(normalized, lower, _RESEARCHER_ROLE_HINTS)
        if researcher_terms:
            matched_rewrites.append({**_RESEARCHER_ROLE_REWRITE, "matched_terms": researcher_terms[:8]})
            matched_terms.extend(researcher_terms)

    if not matched_rewrites:
        return {}

    primary = dict(matched_rewrites[0])
    additional = [item for item in matched_rewrites[1:]]
    primary["matched_terms"] = _merge_string_lists([], matched_terms)[:12]
    primary["keywords"] = _merge_string_lists(
        primary.get("keywords"),
        [token for item in additional for token in list(item.get("keywords") or [])],
    )
    primary["must_have_facets"] = _merge_string_lists(
        primary.get("must_have_facets"),
        [token for item in additional for token in list(item.get("must_have_facets") or [])],
    )
    primary["must_have_primary_role_buckets"] = _merge_string_lists(
        primary.get("must_have_primary_role_buckets"),
        [token for item in additional for token in list(item.get("must_have_primary_role_buckets") or [])],
    )
    primary["targeting_terms"] = _merge_string_lists(
        primary.get("targeting_terms"),
        [token for item in additional for token in list(item.get("targeting_terms") or [])],
    )
    if additional:
        primary["additional_rewrites"] = [
            {
                "rewrite_id": str(item.get("rewrite_id") or "").strip(),
                "summary_label": str(item.get("summary_label") or "").strip(),
                "matched_terms": list(item.get("matched_terms") or []),
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

    merged["keywords"] = _merge_string_lists(merged.get("keywords"), rewrite.get("keywords"))
    merged["must_have_facets"] = _merge_string_lists(merged.get("must_have_facets"), rewrite.get("must_have_facets"))
    merged["must_have_primary_role_buckets"] = _merge_string_lists(
        merged.get("must_have_primary_role_buckets"),
        rewrite.get("must_have_primary_role_buckets"),
    )
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


def _matched_terms_for_tokens(text: str, lower_text: str, tokens: tuple[str, ...]) -> list[str]:
    matched_terms: list[str] = []
    for token in tokens:
        if token in text or token in lower_text:
            if token not in matched_terms:
                matched_terms.append(token)
    return matched_terms
