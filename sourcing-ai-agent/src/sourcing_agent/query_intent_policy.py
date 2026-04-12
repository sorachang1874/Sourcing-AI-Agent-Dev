from __future__ import annotations

"""Business shorthand policies for request rewriting.

This layer translates user-facing shorthand into retrieval-facing patches.
It depends on stable signal knowledge, but it is not the source of truth for
durable org/product/role mappings.
"""

from dataclasses import dataclass, field
from typing import Any

from .query_signal_knowledge import (
    match_scope_signals_by_rewrite_tag,
    role_bucket_matched_terms,
    role_buckets_from_text,
    scope_signal_keyword_labels,
)


@dataclass(frozen=True, slots=True)
class BusinessRewritePolicy:
    rewrite_id: str
    summary_label: str
    trigger_terms: tuple[str, ...] = ()
    scope_rewrite_tags: tuple[str, ...] = ()
    role_buckets: tuple[str, ...] = ()
    request_patch: dict[str, tuple[str, ...]] = field(default_factory=dict)
    targeting_terms: tuple[str, ...] = ()
    notes: str = ""
    policy_layer: str = "business_policy"

    def to_catalog_record(self) -> dict[str, Any]:
        return {
            "rewrite_id": self.rewrite_id,
            "summary_label": self.summary_label,
            "policy_layer": self.policy_layer,
            "trigger_sources": {
                "terms": list(self.trigger_terms),
                "scope_rewrite_tags": list(self.scope_rewrite_tags),
                "role_buckets": list(self.role_buckets),
            },
            "request_patch": {
                key: list(value)
                for key, value in self.request_patch.items()
                if list(value)
            },
            "targeting_terms": list(self.targeting_terms),
            "notes": self.notes,
        }


BUSINESS_REWRITE_POLICIES: tuple[BusinessRewritePolicy, ...] = (
    BusinessRewritePolicy(
        rewrite_id="greater_china_outreach",
        summary_label="华人 / 泛华人简称",
        trigger_terms=(
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
        ),
        request_patch={
            "keywords": (
                "Greater China experience",
                "Chinese bilingual outreach",
            ),
        },
        targeting_terms=(
            "中国大陆 / 港澳台 / 新加坡公开学习或工作经历",
            "中文 / 双语 outreach 适配",
        ),
        notes="默认按公开地区经历与语言 / outreach 适配理解，不输出族裔、国籍或身份标签判断。",
    ),
    BusinessRewritePolicy(
        rewrite_id="multimodal_project_focus",
        summary_label="多模态 / 项目线索",
        trigger_terms=(
            "多模态",
            "multimodal",
            "multimodality",
            "vision-language",
            "vision language",
        ),
        scope_rewrite_tags=("multimodal_project_focus",),
        request_patch={
            "keywords": (
                "multimodal",
                "vision-language",
                "video generation",
            ),
            "must_have_facets": ("multimodal",),
        },
        notes="将多模态意图与相关项目线索合并为可审计职业关键词。",
    ),
    BusinessRewritePolicy(
        rewrite_id="researcher_role_focus",
        summary_label="研究员角色",
        role_buckets=("research",),
        request_patch={
            "must_have_facets": ("research",),
            "must_have_primary_role_buckets": ("research",),
        },
        notes="优先收敛到研究员/研究科学家角色。",
    ),
)


def list_business_rewrite_policy_catalog() -> list[dict[str, Any]]:
    return [policy.to_catalog_record() for policy in BUSINESS_REWRITE_POLICIES]


def build_supported_rewrite_policy_prompt_context() -> list[dict[str, Any]]:
    return list_business_rewrite_policy_catalog()


def get_business_rewrite_policy(rewrite_id: str) -> BusinessRewritePolicy | None:
    normalized = str(rewrite_id or "").strip()
    if not normalized:
        return None
    for policy in BUSINESS_REWRITE_POLICIES:
        if policy.rewrite_id == normalized:
            return policy
    return None


def match_business_rewrite_policies(text: str) -> list[dict[str, Any]]:
    normalized = " ".join(str(text or "").strip().split())
    if not normalized:
        return []
    lower = normalized.lower()
    matched_role_buckets = role_buckets_from_text(normalized)

    matches: list[dict[str, Any]] = []
    for policy in BUSINESS_REWRITE_POLICIES:
        matched = _match_business_rewrite_policy(
            policy,
            normalized_text=normalized,
            lower_text=lower,
            matched_role_buckets=matched_role_buckets,
        )
        if matched:
            matches.append(matched)
    return matches


def _match_business_rewrite_policy(
    policy: BusinessRewritePolicy,
    *,
    normalized_text: str,
    lower_text: str,
    matched_role_buckets: list[str],
) -> dict[str, Any]:
    matched_terms = _matched_terms_for_tokens(
        normalized_text,
        lower_text,
        policy.trigger_terms,
    )

    matched_scope_labels: list[str] = []
    extra_keywords: list[str] = []
    for rewrite_tag in policy.scope_rewrite_tags:
        scope_matches = match_scope_signals_by_rewrite_tag(normalized_text, rewrite_tag)
        matched_scope_labels = _merge_string_lists(
            matched_scope_labels,
            [str(item.get("canonical_label") or "").strip() for item in scope_matches],
        )
        extra_keywords = _merge_string_lists(
            extra_keywords,
            scope_signal_keyword_labels(str(item.get("canonical_label") or "").strip() for item in scope_matches),
        )

    matched_role_terms: list[str] = []
    for bucket in policy.role_buckets:
        if bucket not in matched_role_buckets:
            continue
        matched_role_terms = _merge_string_lists(
            matched_role_terms,
            role_bucket_matched_terms(normalized_text, bucket) or [bucket],
        )

    combined_terms = _merge_string_lists(matched_terms, matched_scope_labels, matched_role_terms)
    if not combined_terms:
        return {}

    matched = policy.to_catalog_record()
    matched["matched_terms"] = combined_terms[:8]
    request_patch = {
        key: list(value)
        for key, value in dict(matched.get("request_patch") or {}).items()
    }
    for patch_key, patch_values in matched["request_patch"].items():
        matched[patch_key] = list(patch_values)
    if extra_keywords:
        request_patch["keywords"] = _merge_string_lists(request_patch.get("keywords"), extra_keywords)
        matched["keywords"] = _merge_string_lists(matched.get("keywords"), extra_keywords)
    matched["request_patch"] = request_patch
    for patch_key, patch_values in request_patch.items():
        matched[patch_key] = list(patch_values)
    return matched


def _merge_string_lists(*sources: Any) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for source in sources:
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
