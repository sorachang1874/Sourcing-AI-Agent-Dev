from __future__ import annotations

"""Stable, reusable query signal knowledge.

This module owns durable mappings such as product/team/model -> parent company,
related scope hints, search aliases, and role bucket -> function ids.
Business shorthand policies should live in query_intent_policy.py instead.
"""

import re
from typing import Any, Iterable

from .company_registry import normalize_company_key


GOOGLE_COMPANY_URL = "https://www.linkedin.com/company/google/"
ALPHABET_COMPANY_URL = "https://www.linkedin.com/company/alphabet-inc/"
DEEPMIND_COMPANY_URL = "https://www.linkedin.com/company/deepmind/"


KNOWN_SCOPE_SIGNAL_SPECS: tuple[dict[str, Any], ...] = (
    {
        "canonical_label": "Google DeepMind",
        "aliases": ("deepmind", "google deepmind"),
        "target_company": "Google",
        "organization_keywords": ("Google DeepMind",),
        "keyword_labels": (),
        "search_query_aliases": ("Google DeepMind", "DeepMind"),
        "company_scope_labels": ("Google DeepMind",),
        "related_company_urls": (DEEPMIND_COMPANY_URL,),
        "review_parent_company_keys": ("google", "alphabet"),
        "scope_disambiguation": {
            "inferred_scope": "sub_org_only",
            "sub_org_candidates": ["Google DeepMind"],
            "confidence": 0.86,
            "rationale": (
                "Google DeepMind is treated as a high-confidence Google sub-organization signal, because relevant "
                "members may list either Google or Google DeepMind on LinkedIn."
            ),
            "source": "rules",
        },
    },
    {
        "canonical_label": "Gemini",
        "aliases": ("gemini", "gemini team"),
        "target_company": "Google",
        "organization_keywords": ("Gemini", "Google DeepMind"),
        "keyword_labels": ("Gemini",),
        "search_query_aliases": ("Gemini",),
        "company_scope_labels": ("Google DeepMind",),
        "related_company_urls": (DEEPMIND_COMPANY_URL,),
        "review_parent_company_keys": ("google", "alphabet"),
        "scope_disambiguation": {
            "inferred_scope": "both",
            "sub_org_candidates": ["Google DeepMind", "Gemini"],
            "confidence": 0.82,
            "rationale": (
                "Gemini is treated as a high-confidence Google product signal, and Google DeepMind remains in scope "
                "because relevant members may list either Google or Google DeepMind on LinkedIn."
            ),
            "source": "rules",
        },
    },
    {
        "canonical_label": "Veo",
        "aliases": ("veo", "veo team"),
        "target_company": "Google",
        "organization_keywords": ("Veo", "Google DeepMind"),
        "keyword_labels": ("Veo",),
        "rewrite_tags": ("multimodal_project_focus",),
        "search_query_aliases": ("Veo",),
        "company_scope_labels": ("Google DeepMind",),
        "related_company_urls": (DEEPMIND_COMPANY_URL,),
        "review_parent_company_keys": ("google", "alphabet"),
        "scope_disambiguation": {
            "inferred_scope": "both",
            "sub_org_candidates": ["Google DeepMind", "Veo"],
            "confidence": 0.82,
            "rationale": "Veo is treated as a high-confidence Google / Google DeepMind product signal for sourcing scope.",
            "source": "rules",
        },
    },
    {
        "canonical_label": "Nano Banana",
        "aliases": ("nano banana", "nano-banana", "nanobanana"),
        "target_company": "Google",
        "organization_keywords": ("Nano Banana", "Google DeepMind"),
        "keyword_labels": ("Nano Banana",),
        "rewrite_tags": ("multimodal_project_focus",),
        "search_query_aliases": ("Nano Banana",),
        "company_scope_labels": ("Google DeepMind",),
        "related_company_urls": (DEEPMIND_COMPANY_URL,),
        "review_parent_company_keys": ("google", "alphabet"),
        "scope_disambiguation": {
            "inferred_scope": "both",
            "sub_org_candidates": ["Google DeepMind", "Nano Banana"],
            "confidence": 0.78,
            "rationale": (
                "Nano Banana is treated as a high-confidence Google / Google DeepMind project signal for sourcing scope."
            ),
            "source": "rules",
        },
    },
    {
        "canonical_label": "Google Research",
        "aliases": ("google research",),
        "target_company": "Google",
        "organization_keywords": ("Google Research",),
        "keyword_labels": ("Google Research",),
        "search_query_aliases": ("Google Research",),
        "company_scope_labels": (),
        "related_company_urls": (),
        "review_parent_company_keys": ("google", "alphabet"),
        "scope_disambiguation": {
            "inferred_scope": "uncertain",
            "sub_org_candidates": ["Google Research"],
            "confidence": 0.74,
            "rationale": "Google Research is treated as a sub-organization signal within the broader Google scope.",
            "source": "rules",
        },
    },
    {
        "canonical_label": "Brain Team",
        "aliases": ("brain team", "google brain"),
        "target_company": "Google",
        "organization_keywords": ("Brain Team", "Google Research"),
        "keyword_labels": ("Brain Team",),
        "search_query_aliases": ("Brain Team", "Google Brain"),
        "company_scope_labels": ("Google Research",),
        "related_company_urls": (),
        "review_parent_company_keys": ("google", "alphabet"),
        "scope_disambiguation": {
            "inferred_scope": "uncertain",
            "sub_org_candidates": ["Brain Team", "Google Research"],
            "confidence": 0.72,
            "rationale": "Brain Team is treated as a Google research sub-organization signal.",
            "source": "rules",
        },
    },
    {
        "canonical_label": "ChatGPT",
        "aliases": ("chatgpt", "chat gpt"),
        "target_company": "OpenAI",
        "organization_keywords": ("ChatGPT",),
        "keyword_labels": ("ChatGPT",),
        "search_query_aliases": ("ChatGPT",),
        "company_scope_labels": (),
        "related_company_urls": (),
        "review_parent_company_keys": (),
        "scope_disambiguation": {
            "inferred_scope": "uncertain",
            "sub_org_candidates": ["ChatGPT"],
            "confidence": 0.74,
            "rationale": "ChatGPT is treated as a high-confidence OpenAI product signal.",
            "source": "rules",
        },
    },
    {
        "canonical_label": "Claude",
        "aliases": ("claude",),
        "target_company": "Anthropic",
        "organization_keywords": ("Claude",),
        "keyword_labels": ("Claude",),
        "search_query_aliases": ("Claude",),
        "company_scope_labels": (),
        "related_company_urls": (),
        "review_parent_company_keys": (),
        "scope_disambiguation": {
            "inferred_scope": "uncertain",
            "sub_org_candidates": ["Claude"],
            "confidence": 0.74,
            "rationale": "Claude is treated as a high-confidence Anthropic product signal.",
            "source": "rules",
        },
    },
    {
        "canonical_label": "o1",
        "aliases": ("o1", "o-1"),
        "target_company": "OpenAI",
        "organization_keywords": ("o1",),
        "keyword_labels": ("o1",),
        "search_query_aliases": ("o1",),
        "company_scope_labels": (),
        "related_company_urls": (),
        "review_parent_company_keys": (),
        "scope_disambiguation": {
            "inferred_scope": "uncertain",
            "sub_org_candidates": ["o1"],
            "confidence": 0.72,
            "rationale": "o1 is treated as a high-confidence OpenAI model-family signal.",
            "source": "rules",
        },
    },
)


KNOWN_THEMATIC_SIGNAL_SPECS: tuple[dict[str, Any], ...] = (
    {
        "canonical_label": "Coding",
        "aliases": ("coding", "coding agent", "coding agents", "programming", "code generation", "编程"),
        "keyword_labels": ("Coding",),
        "research_direction_keywords": ("Coding",),
        "provider_search_aliases": ("Coding",),
    },
    {
        "canonical_label": "Math",
        "aliases": ("math", "mathematics", "mathematical", "math reasoning", "数学"),
        "keyword_labels": ("Math",),
        "research_direction_keywords": ("Math",),
        "provider_search_aliases": ("Math",),
    },
    {
        "canonical_label": "Text",
        "aliases": ("text", "language", "nlp", "natural language", "language model", "language models", "文本"),
        "keyword_labels": ("Text",),
        "research_direction_keywords": ("Text",),
        "provider_search_aliases": ("Language Model", "NLP", "Text"),
    },
    {
        "canonical_label": "Audio",
        "aliases": ("audio", "speech", "voice", "speech audio", "音频", "语音"),
        "keyword_labels": ("Audio",),
        "research_direction_keywords": ("Audio",),
        "provider_search_aliases": ("Audio",),
    },
    {
        "canonical_label": "Infra",
        "aliases": ("infra", "infrastructure", "基础设施"),
        "keyword_labels": ("Infra",),
        "research_direction_keywords": ("Infra",),
        "provider_search_aliases": ("Infrastructure", "Infra"),
    },
    {
        "canonical_label": "Vision",
        "aliases": ("vision", "visual", "computer vision", "视觉"),
        "keyword_labels": ("Vision",),
        "research_direction_keywords": ("Vision",),
        "provider_search_aliases": ("Vision", "Computer Vision"),
    },
    {
        "canonical_label": "Multimodal",
        "aliases": ("multimodal", "multi modal", "multimodality", "多模态"),
        "keyword_labels": ("Multimodal",),
        "research_direction_keywords": ("Multimodal",),
        "facet_labels": ("multimodal",),
        "provider_search_aliases": ("Multimodal",),
    },
    {
        "canonical_label": "Reasoning",
        "aliases": ("reasoning", "reasoning model", "reasoning models", "reasoner"),
        "keyword_labels": ("Reasoning",),
        "research_direction_keywords": ("Reasoning",),
        "provider_search_aliases": ("Reasoning",),
    },
    {
        "canonical_label": "RL",
        "aliases": ("rl", "reinforcement learning", "强化学习"),
        "keyword_labels": ("RL",),
        "research_direction_keywords": ("RL",),
        "provider_search_aliases": ("Reinforcement Learning", "RL"),
    },
    {
        "canonical_label": "Eval",
        "aliases": ("eval", "evals", "evaluation", "model evaluation", "alignment evaluation", "评估", "评测"),
        "keyword_labels": ("Eval",),
        "research_direction_keywords": ("Eval",),
        "provider_search_aliases": ("Evaluation", "Model Evaluation", "Eval"),
    },
    {
        "canonical_label": "Pre-train",
        "aliases": ("pre-train", "pre train", "pretraining", "pre-training", "pre training", "预训练"),
        "keyword_labels": ("Pre-train",),
        "research_direction_keywords": ("Pre-train",),
        "provider_search_aliases": ("Pre-train",),
    },
    {
        "canonical_label": "Post-train",
        "aliases": ("post-train", "post train", "posttraining", "post-training", "post training", "后训练"),
        "keyword_labels": ("Post-train",),
        "research_direction_keywords": ("Post-train",),
        "provider_search_aliases": ("Post-train",),
    },
    {
        "canonical_label": "World model",
        "aliases": ("world model", "world models", "world modeling", "world-modeling", "世界模型"),
        "keyword_labels": ("World model",),
        "research_direction_keywords": ("World model",),
        "provider_search_aliases": ("World model",),
    },
    {
        "canonical_label": "Alignment",
        "aliases": ("alignment", "alignments"),
        "keyword_labels": ("Alignment",),
        "research_direction_keywords": ("Alignment",),
        "provider_search_aliases": ("Alignment",),
    },
    {
        "canonical_label": "Safety",
        "aliases": ("safety",),
        "keyword_labels": ("Safety",),
        "research_direction_keywords": ("Safety",),
        "provider_search_aliases": ("Safety",),
    },
)


ROLE_BUCKET_KNOWLEDGE: dict[str, dict[str, Any]] = {
    "product_management": {
        "aliases": ("product manager", "product management", "产品经理", "pm"),
        "role_hints": ("Product Manager", "Senior Product Manager", "Group Product Manager"),
        "function_ids": ("19",),
    },
    "research": {
        "aliases": (
            "researcher",
            "research scientist",
            "applied scientist",
            "scientist",
            "研究员",
            "研究科学家",
        ),
        "role_hints": ("Researcher", "Research Scientist", "Applied Scientist"),
        "function_ids": ("24",),
    },
    "engineering": {
        "aliases": (
            "engineer",
            "engineering",
            "software engineer",
            "research engineer",
            "technical staff",
            "member of technical staff",
        ),
        "role_hints": ("Engineer", "Software Engineer", "Research Engineer"),
        "function_ids": ("8",),
    },
    "infra_systems": {
        "aliases": (
            "infra",
            "infrastructure",
            "infra systems",
            "platform engineer",
            "systems engineer",
            "distributed systems",
        ),
        "role_hints": ("Infrastructure Engineer", "Systems Engineer", "Platform Engineer"),
        "function_ids": ("8",),
    },
    "founding": {
        "aliases": ("founder", "co-founder", "founding", "entrepreneur", "entrepreneurship"),
        "role_hints": ("Founder", "Co-founder"),
        "function_ids": ("9",),
    },
}


_SCOPE_SIGNAL_BY_CANONICAL = {
    str(spec.get("canonical_label") or "").strip(): dict(spec)
    for spec in KNOWN_SCOPE_SIGNAL_SPECS
    if str(spec.get("canonical_label") or "").strip()
}
_SCOPE_SIGNAL_LOOKUP: dict[str, str] = {}
for _canonical_label, _spec in _SCOPE_SIGNAL_BY_CANONICAL.items():
    for _value in (
        [_canonical_label]
        + list(_spec.get("aliases") or [])
        + list(_spec.get("organization_keywords") or [])
        + list(_spec.get("keyword_labels") or [])
    ):
        _normalized = "".join(ch.lower() for ch in str(_value or "") if ch.isalnum())
        if _normalized:
            _SCOPE_SIGNAL_LOOKUP.setdefault(_normalized, _canonical_label)

_THEMATIC_SIGNAL_BY_CANONICAL = {
    str(spec.get("canonical_label") or "").strip(): dict(spec)
    for spec in KNOWN_THEMATIC_SIGNAL_SPECS
    if str(spec.get("canonical_label") or "").strip()
}
_THEMATIC_SIGNAL_LOOKUP: dict[str, str] = {}
for _canonical_label, _spec in _THEMATIC_SIGNAL_BY_CANONICAL.items():
    for _value in ([_canonical_label] + list(_spec.get("aliases") or [])):
        _normalized = "".join(ch.lower() for ch in str(_value or "") if ch.isalnum())
        if _normalized:
            _THEMATIC_SIGNAL_LOOKUP.setdefault(_normalized, _canonical_label)


def canonicalize_scope_signal_label(value: str) -> str:
    normalized = normalize_scope_signal_key(value)
    if not normalized:
        return ""
    canonical = _SCOPE_SIGNAL_LOOKUP.get(normalized)
    if canonical:
        return canonical
    return " ".join(str(value or "").split()).strip()


def normalize_scope_signal_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def lookup_scope_signal(value: str) -> dict[str, Any]:
    canonical = _SCOPE_SIGNAL_LOOKUP.get(normalize_scope_signal_key(value))
    if not canonical:
        return {}
    return dict(_SCOPE_SIGNAL_BY_CANONICAL.get(canonical) or {})


def match_scope_signals(text: str) -> list[dict[str, Any]]:
    normalized_text = " ".join(str(text or "").lower().split()).strip()
    if not normalized_text:
        return []
    matches: list[dict[str, Any]] = []
    seen: set[str] = set()
    for spec in KNOWN_SCOPE_SIGNAL_SPECS:
        aliases = [str(spec.get("canonical_label") or "").strip(), *list(spec.get("aliases") or [])]
        if not any(_alias_matches_text(normalized_text, alias) for alias in aliases):
            continue
        canonical = str(spec.get("canonical_label") or "").strip()
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        matches.append(dict(spec))
    return matches


def match_scope_signals_by_rewrite_tag(text: str, rewrite_tag: str) -> list[dict[str, Any]]:
    normalized_tag = str(rewrite_tag or "").strip().lower()
    if not normalized_tag:
        return []
    matches: list[dict[str, Any]] = []
    for spec in match_scope_signals(text):
        tags = {str(item).strip().lower() for item in list(spec.get("rewrite_tags") or []) if str(item).strip()}
        if normalized_tag in tags:
            matches.append(spec)
    return matches


def resolve_target_company_alias(value: str) -> dict[str, Any]:
    spec = lookup_scope_signal(value)
    target_company = str(spec.get("target_company") or "").strip()
    if not target_company:
        return {}
    organization_keywords = [str(item).strip() for item in list(spec.get("organization_keywords") or []) if str(item).strip()]
    return {
        "target_company": target_company,
        "organization_keywords": organization_keywords,
    }


def scope_signal_search_query_aliases(value: str) -> list[str]:
    spec = lookup_scope_signal(value)
    if not spec:
        return []
    aliases = [str(item).strip() for item in list(spec.get("search_query_aliases") or []) if str(item).strip()]
    return _dedupe_strings(aliases)


def scope_signal_keyword_labels(values: Iterable[str]) -> list[str]:
    labels: list[str] = []
    for value in values:
        spec = lookup_scope_signal(value)
        if not spec:
            canonical = canonicalize_scope_signal_label(str(value or ""))
            if canonical:
                labels.append(canonical)
            continue
        labels.extend(str(item).strip() for item in list(spec.get("keyword_labels") or []) if str(item).strip())
    return _dedupe_strings(labels)


def related_company_scope_labels(target_company: str, values: Iterable[str]) -> list[str]:
    target_key = _scope_parent_company_key(target_company)
    labels: list[str] = []
    for value in values:
        spec = lookup_scope_signal(value)
        if not spec:
            continue
        if _scope_parent_company_key(str(spec.get("target_company") or "")) != target_key:
            continue
        labels.extend(str(item).strip() for item in list(spec.get("company_scope_labels") or []) if str(item).strip())
    return _dedupe_strings(labels)


def related_company_scope_urls(target_company: str, values: Iterable[str]) -> list[str]:
    target_key = _scope_parent_company_key(target_company)
    urls: list[str] = []
    for value in values:
        spec = lookup_scope_signal(value)
        if not spec:
            continue
        if _scope_parent_company_key(str(spec.get("target_company") or "")) != target_key:
            continue
        urls.extend(str(item).strip() for item in list(spec.get("related_company_urls") or []) if str(item).strip())
    return _dedupe_strings(urls)


def lookup_thematic_signal(value: str) -> dict[str, Any]:
    canonical = _THEMATIC_SIGNAL_LOOKUP.get(normalize_scope_signal_key(value))
    if not canonical:
        return {}
    return dict(_THEMATIC_SIGNAL_BY_CANONICAL.get(canonical) or {})


def canonicalize_thematic_signal_label(value: str) -> str:
    spec = lookup_thematic_signal(value)
    if spec:
        keyword_labels = [str(item).strip() for item in list(spec.get("keyword_labels") or []) if str(item).strip()]
        if keyword_labels:
            return keyword_labels[0]
        canonical = str(spec.get("canonical_label") or "").strip()
        if canonical:
            return canonical
    return " ".join(str(value or "").split()).strip()


def thematic_signal_search_query_aliases(value: str) -> list[str]:
    spec = lookup_thematic_signal(value)
    if not spec:
        canonical = canonicalize_thematic_signal_label(value)
        return [canonical] if canonical else []
    aliases = [
        str(item).strip()
        for item in list(spec.get("provider_search_aliases") or spec.get("keyword_labels") or [])
        if str(item).strip()
    ]
    if not aliases:
        canonical = str(spec.get("canonical_label") or "").strip()
        if canonical:
            aliases = [canonical]
    return _dedupe_strings(aliases)


def naturalize_search_query_term(value: str) -> str:
    normalized = " ".join(str(value or "").replace("_", " ").split()).strip()
    if not normalized:
        return ""
    scope_aliases = scope_signal_search_query_aliases(normalized)
    if scope_aliases:
        return scope_aliases[0]
    thematic_aliases = thematic_signal_search_query_aliases(normalized)
    if thematic_aliases:
        return thematic_aliases[0]
    canonical_scope = canonicalize_scope_signal_label(normalized)
    if canonical_scope and canonical_scope != normalized:
        return canonical_scope
    canonical_thematic = canonicalize_thematic_signal_label(normalized)
    if canonical_thematic and canonical_thematic != normalized:
        return canonical_thematic
    return normalized


def naturalize_search_query_terms(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = naturalize_search_query_term(str(value or ""))
        if not normalized:
            continue
        signature = _search_phrase_signature(normalized)
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(normalized)
    return deduped


def match_thematic_signals(text: str) -> list[dict[str, Any]]:
    normalized_text = " ".join(str(text or "").lower().split()).strip()
    if not normalized_text:
        return []
    matches: list[dict[str, Any]] = []
    seen: set[str] = set()
    for spec in KNOWN_THEMATIC_SIGNAL_SPECS:
        aliases = [str(spec.get("canonical_label") or "").strip(), *list(spec.get("aliases") or [])]
        if not any(_alias_matches_text(normalized_text, alias) for alias in aliases):
            continue
        canonical = str(spec.get("canonical_label") or "").strip()
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        matches.append(dict(spec))
    return matches


def scope_review_hints(target_company: str, values: Iterable[str]) -> list[str]:
    target_key = _scope_parent_company_key(target_company)
    hints: list[str] = []
    for value in values:
        spec = lookup_scope_signal(value)
        if not spec:
            continue
        review_parent_keys = {normalize_company_key(item) for item in list(spec.get("review_parent_company_keys") or []) if str(item).strip()}
        if target_key not in review_parent_keys:
            continue
        hints.extend(str(item).strip() for item in list(spec.get("organization_keywords") or []) if str(item).strip())
    return _dedupe_strings(hints)


def role_buckets_from_text(text: str) -> list[str]:
    normalized_text = " ".join(str(text or "").lower().split()).strip()
    if not normalized_text:
        return []
    matched: list[str] = []
    for bucket, payload in ROLE_BUCKET_KNOWLEDGE.items():
        aliases = [str(item).strip() for item in list(payload.get("aliases") or []) if str(item).strip()]
        if not any(_alias_matches_text(normalized_text, alias) for alias in aliases):
            continue
        normalized_bucket = _normalize_role_bucket(bucket)
        if normalized_bucket and normalized_bucket not in matched:
            matched.append(normalized_bucket)
    return matched


def role_bucket_matched_terms(text: str, bucket: str) -> list[str]:
    normalized_text = " ".join(str(text or "").lower().split()).strip()
    normalized_bucket = _normalize_role_bucket(str(bucket or ""))
    if not normalized_text or not normalized_bucket:
        return []
    payload = dict(ROLE_BUCKET_KNOWLEDGE.get(normalized_bucket) or {})
    aliases = sorted(
        [str(item).strip() for item in list(payload.get("aliases") or []) if str(item).strip()],
        key=len,
        reverse=True,
    )
    matched_terms: list[str] = []
    for alias in aliases:
        if any(alias in existing for existing in matched_terms):
            continue
        if _alias_matches_text(normalized_text, alias) and alias not in matched_terms:
            matched_terms.append(alias)
    return matched_terms


def role_bucket_role_hints(buckets: Iterable[str]) -> list[str]:
    role_hints: list[str] = []
    for bucket in buckets:
        normalized_bucket = _normalize_role_bucket(str(bucket or ""))
        if not normalized_bucket:
            continue
        payload = dict(ROLE_BUCKET_KNOWLEDGE.get(normalized_bucket) or {})
        role_hints.extend(str(item).strip() for item in list(payload.get("role_hints") or []) if str(item).strip())
    return _dedupe_strings(role_hints)


def role_bucket_function_ids(buckets: Iterable[str]) -> list[str]:
    function_ids: list[str] = []
    for bucket in buckets:
        normalized_bucket = _normalize_role_bucket(str(bucket or ""))
        if not normalized_bucket:
            continue
        payload = dict(ROLE_BUCKET_KNOWLEDGE.get(normalized_bucket) or {})
        function_ids.extend(str(item).strip() for item in list(payload.get("function_ids") or []) if str(item).strip())
    return _dedupe_strings(function_ids)


def default_large_org_priority_function_ids() -> list[str]:
    return role_bucket_function_ids(("engineering", "founding", "product_management", "research"))


def _alias_matches_text(normalized_text: str, alias: str) -> bool:
    normalized_alias = " ".join(str(alias or "").lower().split()).strip()
    if not normalized_alias:
        return False
    boundary_pattern = rf"(?<![a-z0-9]){re.escape(normalized_alias)}(?![a-z0-9])"
    if re.search(boundary_pattern, normalized_text):
        return True
    compact_alias = normalize_scope_signal_key(normalized_alias)
    compact_text = normalize_scope_signal_key(normalized_text)
    if compact_alias and compact_alias == compact_text:
        return True
    if compact_alias and len(compact_alias) >= 4 and compact_alias in compact_text:
        return True
    return normalized_alias in normalized_text


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = " ".join(str(value or "").split()).strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped


def _search_phrase_signature(value: str) -> str:
    normalized = " ".join(str(value or "").lower().split()).strip()
    if not normalized:
        return ""
    compact = re.sub(r"[\s\-_]+", "", normalized)
    alnum = re.sub(r"[^0-9a-z]+", "", compact)
    return alnum or compact


def _scope_parent_company_key(value: str) -> str:
    normalized = normalize_company_key(value)
    if normalized == "alphabet":
        return "google"
    return normalized


def _normalize_role_bucket(value: str) -> str:
    from .domain import normalize_requested_role_bucket

    return normalize_requested_role_bucket(value)
