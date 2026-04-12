from __future__ import annotations

import re
from typing import Any

from .domain import (
    FACET_ALIAS_MAP,
    ROLE_BUCKET_PRIORITY,
    JobRequest,
    normalize_requested_facet,
    normalize_requested_facets,
    normalize_requested_role_bucket,
    normalize_requested_role_buckets,
)
from .model_provider import ModelClient
from .query_intent_rewrite import interpret_query_intent_rewrite
from .request_normalization import (
    canonicalize_request_payload,
    extract_query_signal_terms,
    merge_unique_request_string_values,
)


_ALLOWED_PATCH_FIELDS = {
    "asset_view",
    "categories",
    "employment_statuses",
    "keywords",
    "must_have_keywords",
    "exclude_keywords",
    "organization_keywords",
    "must_have_facets",
    "must_have_primary_role_buckets",
    "retrieval_strategy",
    "top_k",
    "semantic_rerank_limit",
}

_PATCH_FIELD_ALIASES = {
    "must_have_facet": "must_have_facets",
    "must_have_primary_role_bucket": "must_have_primary_role_buckets",
    "facet": "must_have_facets",
    "role_bucket": "must_have_primary_role_buckets",
}

_ALLOWED_ASSET_VIEWS = {"canonical_merged", "strict_roster_only"}
_ALLOWED_RETRIEVAL_STRATEGIES = {"structured", "hybrid", "semantic"}
_ALLOWED_EMPLOYMENT_STATUSES = {"current", "former"}
_ROLE_LIKE_CATEGORIES = {"employee", "former_employee", "investor", "researcher", "engineer"}
_KNOWN_FACETS = set(FACET_ALIAS_MAP.keys())
_KNOWN_ROLE_BUCKETS = set(ROLE_BUCKET_PRIORITY)


def parse_refinement_instruction(instruction: str, *, target_company: str = "") -> dict[str, Any]:
    text = " ".join(str(instruction or "").strip().split())
    lower = text.lower()
    patch: dict[str, Any] = {}

    asset_view = _parse_asset_view(lower)
    if asset_view:
        patch["asset_view"] = asset_view

    retrieval_strategy = _parse_retrieval_strategy(lower)
    if retrieval_strategy:
        patch["retrieval_strategy"] = retrieval_strategy

    top_k = _parse_top_k(text, lower)
    if top_k > 0:
        patch["top_k"] = top_k

    semantic_rerank_limit = _parse_semantic_rerank_limit(text, lower)
    if semantic_rerank_limit > 0:
        patch["semantic_rerank_limit"] = semantic_rerank_limit

    employment_statuses = _parse_employment_statuses(lower)
    if employment_statuses:
        patch["employment_statuses"] = employment_statuses

    include_terms = _parse_focus_terms(
        text,
        prefixes=[
            "只看",
            "只保留",
            "只要",
            "聚焦",
            "focus on",
            "narrow to",
            "only keep",
            "only show",
        ],
    )
    include_classified = _classify_focus_terms(include_terms)
    _merge_patch_terms(patch, include_classified, include_mode=True)
    _supplement_patch_query_signals(
        patch,
        terms=include_terms or ([text] if text and not _contains_exclude_prefix(text) else []),
        include_mode=True,
        target_company=target_company,
    )

    exclude_terms = _parse_focus_terms(
        text,
        prefixes=[
            "排除",
            "去掉",
            "剔除",
            "过滤掉",
            "不要",
            "exclude",
            "without",
        ],
    )
    exclude_classified = _classify_focus_terms(exclude_terms)
    _merge_patch_terms(patch, exclude_classified, include_mode=False)
    _supplement_patch_query_signals(
        patch,
        terms=exclude_terms,
        include_mode=False,
        target_company=target_company,
    )

    return patch


def compile_refinement_patch_from_instruction(
    *,
    instruction: str,
    base_request: dict[str, Any],
    model_client: ModelClient | None = None,
) -> dict[str, Any]:
    normalized_instruction = " ".join(str(instruction or "").strip().split())
    target_company = str(base_request.get("target_company") or "").strip()
    deterministic_patch = normalize_refinement_patch(
        parse_refinement_instruction(normalized_instruction, target_company=target_company)
    )
    model_raw: dict[str, Any] = {}
    model_patch: dict[str, Any] = {}
    provider_name = ""
    if model_client is not None:
        provider_name = str(model_client.provider_name() or "").strip()
        try:
            model_raw = dict(
                model_client.normalize_refinement_instruction(
                    {
                        "instruction": str(instruction or "").strip(),
                        "normalized_instruction": normalized_instruction,
                        "base_request": dict(base_request or {}),
                        "allowed_patch_fields": sorted(_ALLOWED_PATCH_FIELDS),
                    }
                )
                or {}
            )
        except Exception:
            model_raw = {}
        model_patch = normalize_refinement_patch(
            _rewrite_model_refinement_patch_candidate(
                model_raw,
                instruction=normalized_instruction,
                target_company=target_company,
            )
        )

    merged_patch = dict(model_patch)
    supplemented_keys: list[str] = []
    for key, value in deterministic_patch.items():
        if key in merged_patch:
            continue
        merged_patch[key] = value
        supplemented_keys.append(key)

    merged_request = apply_refinement_patch(base_request, merged_patch)
    compiler_source = "model" if model_patch else "deterministic"
    return {
        "request_patch": merged_patch,
        "merged_request": JobRequest.from_payload(merged_request).to_record(),
        "instruction_compiler": {
            "source": compiler_source,
            "provider": provider_name or ("deterministic" if compiler_source == "deterministic" else ""),
            "model_patch": model_patch,
            "deterministic_patch": deterministic_patch,
            "supplemented_keys": supplemented_keys,
            "fallback_used": bool(supplemented_keys) or not bool(model_patch),
        },
    }


def normalize_refinement_patch(payload: dict[str, Any] | None) -> dict[str, Any]:
    candidate = dict(payload or {})
    if isinstance(candidate.get("patch"), dict):
        candidate = dict(candidate.get("patch") or {})
    patch: dict[str, Any] = {}
    keyword_values = merge_unique_request_string_values(
        candidate.get("keywords"),
        candidate.get("research_direction_keywords"),
        candidate.get("technology_keywords"),
        candidate.get("model_keywords"),
        candidate.get("product_keywords"),
    )
    if keyword_values:
        patch["keywords"] = keyword_values
    organization_keyword_values = merge_unique_request_string_values(
        candidate.get("organization_keywords"),
        candidate.get("team_keywords"),
        candidate.get("sub_org_keywords"),
        candidate.get("project_keywords"),
    )
    if organization_keyword_values:
        patch["organization_keywords"] = organization_keyword_values
    for raw_key, raw_value in candidate.items():
        key = _PATCH_FIELD_ALIASES.get(str(raw_key or "").strip(), str(raw_key or "").strip())
        if key not in _ALLOWED_PATCH_FIELDS:
            continue
        if key == "asset_view":
            value = str(raw_value or "").strip()
            if value in _ALLOWED_ASSET_VIEWS:
                patch[key] = value
            continue
        if key == "retrieval_strategy":
            value = str(raw_value or "").strip().lower()
            if value in _ALLOWED_RETRIEVAL_STRATEGIES:
                patch[key] = value
            continue
        if key in {"top_k", "semantic_rerank_limit"}:
            value = _normalize_positive_int(raw_value, maximum=100)
            if value > 0:
                patch[key] = value
            continue
        if key == "employment_statuses":
            values = [
                item
                for item in _normalize_string_list(raw_value)
                if item in _ALLOWED_EMPLOYMENT_STATUSES
            ]
            if values:
                patch[key] = values
            continue
        if key == "must_have_facets":
            values = [item for item in normalize_requested_facets(raw_value) if item]
            if values:
                patch[key] = values
            continue
        if key == "must_have_primary_role_buckets":
            values = normalize_requested_role_buckets(raw_value)
            if values:
                patch[key] = values
            continue
        if key == "categories":
            values = [item for item in _normalize_string_list(raw_value) if item]
            if values:
                patch[key] = values
            continue
        values = _normalize_string_list(raw_value)
        if values:
            patch[key] = merge_unique_request_string_values(patch.get(key), values)
    return patch


def apply_refinement_patch(base_request: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base_request or {})
    normalized_patch = normalize_refinement_patch(patch)
    for key, value in normalized_patch.items():
        if isinstance(value, list):
            merged[key] = list(value)
        else:
            merged[key] = value
    return canonicalize_request_payload(merged)


def _parse_asset_view(lower: str) -> str:
    if any(
        token in lower
        for token in [
            "strict roster",
            "strict_roster_only",
            "strict roster only",
            "纯净 roster",
            "只看确认成员",
            "只用纯净 roster",
        ]
    ):
        return "strict_roster_only"
    if any(token in lower for token in ["canonical merged", "canonical_merged", "merged asset", "合并视图"]):
        return "canonical_merged"
    return ""


def _parse_retrieval_strategy(lower: str) -> str:
    if any(token in lower for token in ["semantic", "语义"]):
        return "semantic"
    if any(token in lower for token in ["structured", "结构化"]):
        return "structured"
    if any(token in lower for token in ["hybrid", "混合"]):
        return "hybrid"
    return ""


def _parse_top_k(text: str, lower: str) -> int:
    match = re.search(r"(?:top|前)\s*(\d{1,3})", lower)
    if not match:
        match = re.search(r"前\s*(\d{1,3})\s*个", text)
    if not match:
        return 0
    return _normalize_positive_int(match.group(1), maximum=100)


def _parse_semantic_rerank_limit(text: str, lower: str) -> int:
    match = re.search(r"(?:semantic rerank limit|semantic limit|语义重排上限)\s*[:：]?\s*(\d{1,3})", lower)
    if not match:
        return 0
    return _normalize_positive_int(match.group(1), maximum=200)


def _parse_employment_statuses(lower: str) -> list[str]:
    if any(token in lower for token in ["不要 former", "排除 former", "exclude former", "without former", "不要前员工", "排除前员工"]):
        return ["current"]
    if any(token in lower for token in ["不要 current", "排除 current", "exclude current", "without current", "不要在职", "排除在职"]):
        return ["former"]
    statuses: list[str] = []
    if any(token in lower for token in ["current only", "只看 current", "只看在职", "当前成员", "在职"]):
        statuses.append("current")
    if any(token in lower for token in ["former only", "只看 former", "只看前员工", "已离职", "前员工"]):
        statuses.append("former")
    return statuses


def _parse_focus_terms(text: str, *, prefixes: list[str]) -> list[str]:
    terms: list[str] = []
    for prefix in prefixes:
        pattern = re.compile(rf"{re.escape(prefix)}\s*([^。；;\n]+)", flags=re.IGNORECASE)
        for match in pattern.finditer(text):
            fragment = str(match.group(1) or "").strip()
            for item in re.split(r"[,，、/]|(?:\band\b)|(?:\bor\b)|(?:和)|(?:及)|(?:与)", fragment, flags=re.IGNORECASE):
                normalized = " ".join(str(item or "").strip().split())
                if normalized and normalized not in terms:
                    terms.append(normalized)
    return terms


def _classify_focus_terms(terms: list[str]) -> dict[str, list[str]]:
    classified = {
        "categories": [],
        "must_have_facets": [],
        "must_have_primary_role_buckets": [],
        "keywords": [],
    }
    for raw_term in terms:
        if _is_non_filter_fragment(raw_term):
            continue
        rewrite = interpret_query_intent_rewrite(raw_term)
        rewrite_matched = bool(rewrite)
        if rewrite:
            if str(rewrite.get("rewrite_id") or "").strip() == "greater_china_outreach":
                for keyword in list(rewrite.get("keywords") or []):
                    value = str(keyword or "").strip()
                    if value and value not in classified["keywords"]:
                        classified["keywords"].append(value)
            for role_bucket in list(rewrite.get("must_have_primary_role_buckets") or []):
                value = normalize_requested_role_bucket(str(role_bucket or "").strip())
                if value in _KNOWN_ROLE_BUCKETS and value not in classified["must_have_primary_role_buckets"]:
                    classified["must_have_primary_role_buckets"].append(value)
        categories, remainder = _extract_categories_from_term(raw_term)
        for category in categories:
            if category not in classified["categories"]:
                classified["categories"].append(category)
        normalized_remainder = " ".join(remainder.split()).strip()
        if not normalized_remainder:
            continue
        role_bucket = normalize_requested_role_bucket(normalized_remainder)
        if role_bucket in _KNOWN_ROLE_BUCKETS:
            if role_bucket not in classified["must_have_primary_role_buckets"]:
                classified["must_have_primary_role_buckets"].append(role_bucket)
            continue
        facet = normalize_requested_facet(normalized_remainder)
        if facet in _KNOWN_FACETS:
            if facet not in classified["must_have_facets"]:
                classified["must_have_facets"].append(facet)
            continue
        if rewrite_matched:
            continue
        if normalized_remainder not in classified["keywords"]:
            classified["keywords"].append(normalized_remainder)
    return classified


def _extract_categories_from_term(value: str) -> tuple[list[str], str]:
    lowered = " ".join(str(value or "").lower().split())
    categories: list[str] = []
    replacements = {
        "former employee": "former_employee",
        "former employees": "former_employee",
        "researcher": "researcher",
        "engineer": "engineer",
        "employee": "employee",
        "investor": "investor",
    }
    remaining = lowered
    for phrase, category in replacements.items():
        if phrase in remaining and category not in categories:
            categories.append(category)
            remaining = remaining.replace(phrase, " ")
    if "前员工" in remaining and "former_employee" not in categories:
        categories.append("former_employee")
        remaining = remaining.replace("前员工", " ")
    if "研究员" in remaining and "researcher" not in categories:
        categories.append("researcher")
        remaining = remaining.replace("研究员", " ")
    if "工程师" in remaining and "engineer" not in categories:
        categories.append("engineer")
        remaining = remaining.replace("工程师", " ")
    return [item for item in categories if item in _ROLE_LIKE_CATEGORIES], remaining


def _is_non_filter_fragment(value: str) -> bool:
    lowered = " ".join(str(value or "").lower().split())
    if re.fullmatch(r"(?:top|前)\s*\d{1,3}(?:\s*个)?", lowered):
        return True
    if lowered in {
        "strict roster",
        "strict roster only",
        "strict_roster_only",
        "canonical merged",
        "canonical_merged",
    }:
        return True
    return False


def _merge_patch_terms(patch: dict[str, Any], classified: dict[str, list[str]], *, include_mode: bool) -> None:
    if include_mode:
        for category in classified["categories"]:
            patch.setdefault("categories", [])
            if category not in patch["categories"]:
                patch["categories"].append(category)
        for field in ["must_have_facets", "must_have_primary_role_buckets"]:
            for item in classified[field]:
                patch.setdefault(field, [])
                if item not in patch[field]:
                    patch[field].append(item)
        for item in classified["keywords"]:
            patch.setdefault("must_have_keywords", [])
            if item not in patch["must_have_keywords"]:
                patch["must_have_keywords"].append(item)
        return
    for item in classified["keywords"] + classified["must_have_facets"] + classified["must_have_primary_role_buckets"]:
        patch.setdefault("exclude_keywords", [])
        if item not in patch["exclude_keywords"]:
            patch["exclude_keywords"].append(item)


def _supplement_patch_query_signals(
    patch: dict[str, Any],
    *,
    terms: list[str],
    include_mode: bool,
    target_company: str,
) -> None:
    signal_text = " ".join(" ".join(str(item or "").split()).strip() for item in terms if str(item or "").strip())
    if not signal_text:
        return
    extracted = extract_query_signal_terms(signal_text, target_company=target_company)
    if include_mode:
        if extracted["organization_keywords"]:
            patch["organization_keywords"] = merge_unique_request_string_values(
                patch.get("organization_keywords"),
                extracted["organization_keywords"],
                target_company=target_company,
            )
        if extracted["keywords"]:
            patch["must_have_keywords"] = merge_unique_request_string_values(
                patch.get("must_have_keywords"),
                extracted["keywords"],
                target_company=target_company,
            )
        return
    negative_terms = merge_unique_request_string_values(
        extracted["organization_keywords"],
        extracted["keywords"],
        target_company=target_company,
    )
    if negative_terms:
        patch["exclude_keywords"] = merge_unique_request_string_values(
            patch.get("exclude_keywords"),
            negative_terms,
            target_company=target_company,
        )


def _rewrite_model_refinement_patch_candidate(
    payload: dict[str, Any] | None,
    *,
    instruction: str,
    target_company: str,
) -> dict[str, Any]:
    candidate = dict(payload or {})
    if isinstance(candidate.get("patch"), dict):
        candidate = dict(candidate.get("patch") or {})
    rewritten = dict(candidate)
    rewritten["keywords"] = merge_unique_request_string_values(
        candidate.get("keywords"),
        candidate.get("research_direction_keywords"),
        candidate.get("technology_keywords"),
        candidate.get("model_keywords"),
        candidate.get("product_keywords"),
        target_company=target_company,
    )
    rewritten["organization_keywords"] = merge_unique_request_string_values(
        candidate.get("organization_keywords"),
        candidate.get("team_keywords"),
        candidate.get("sub_org_keywords"),
        candidate.get("project_keywords"),
        target_company=target_company,
    )

    include_terms = _parse_focus_terms(
        instruction,
        prefixes=[
            "只看",
            "只保留",
            "只要",
            "聚焦",
            "focus on",
            "narrow to",
            "only keep",
            "only show",
        ],
    )
    exclude_terms = _parse_focus_terms(
        instruction,
        prefixes=[
            "排除",
            "去掉",
            "剔除",
            "过滤掉",
            "不要",
            "exclude",
            "without",
        ],
    )
    _supplement_patch_query_signals(
        rewritten,
        terms=include_terms or ([instruction] if instruction and not _contains_exclude_prefix(instruction) else []),
        include_mode=True,
        target_company=target_company,
    )
    _supplement_patch_query_signals(
        rewritten,
        terms=exclude_terms,
        include_mode=False,
        target_company=target_company,
    )
    return rewritten


def _contains_exclude_prefix(text: str) -> bool:
    lower = str(text or "").lower()
    return any(
        prefix in lower
        for prefix in [
            "排除",
            "去掉",
            "剔除",
            "过滤掉",
            "不要",
            "exclude",
            "without",
        ]
    )


def _normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    raw_items: list[str] = []
    if isinstance(value, str):
        raw_items.extend(re.split(r"[,/|]", value))
    else:
        for item in list(value):
            if isinstance(item, str):
                raw_items.extend(re.split(r"[,/|]", item))
            else:
                raw_items.append(str(item or ""))
    normalized: list[str] = []
    for item in raw_items:
        candidate = " ".join(str(item or "").strip().split())
        if not candidate or candidate in normalized:
            continue
        normalized.append(candidate[:120])
    return normalized[:12]


def _normalize_positive_int(value: Any, *, maximum: int) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return 0
    if normalized <= 0:
        return 0
    return min(normalized, maximum)
