from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha1
import re
from typing import Any

from .company_registry import builtin_company_identity, infer_target_company_from_text
from .execution_preferences import (
    apply_execution_preference_policy,
    infer_execution_preferences_from_text,
    merge_execution_preferences,
    normalize_execution_preferences,
)
from .query_signal_knowledge import match_thematic_signals
from .query_intent_rewrite import apply_query_intent_rewrite


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_name_token(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


def make_candidate_id(name_en: str, organization: str, target_company: str) -> str:
    payload = "|".join(
        [
            normalize_name_token(name_en),
            normalize_name_token(organization),
            normalize_name_token(target_company),
        ]
    )
    return sha1(payload.encode("utf-8")).hexdigest()[:16]


def make_evidence_id(candidate_id: str, source_dataset: str, title: str, url: str) -> str:
    payload = "|".join([candidate_id, source_dataset, title, url])
    return sha1(payload.encode("utf-8")).hexdigest()[:16]


@dataclass(slots=True)
class Candidate:
    candidate_id: str
    name_en: str
    name_zh: str = ""
    display_name: str = ""
    category: str = ""
    target_company: str = ""
    organization: str = ""
    employment_status: str = ""
    role: str = ""
    team: str = ""
    joined_at: str = ""
    left_at: str = ""
    current_destination: str = ""
    ethnicity_background: str = ""
    investment_involvement: str = ""
    focus_areas: str = ""
    education: str = ""
    work_history: str = ""
    notes: str = ""
    linkedin_url: str = ""
    media_url: str = ""
    source_dataset: str = ""
    source_path: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EvidenceRecord:
    evidence_id: str
    candidate_id: str
    source_type: str
    title: str
    url: str
    summary: str
    source_dataset: str
    source_path: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class JobRequest:
    raw_user_request: str = ""
    query: str = ""
    target_company: str = ""
    asset_view: str = "canonical_merged"
    target_scope: str = "full_company_asset"
    categories: list[str] = field(default_factory=list)
    employment_statuses: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    must_have_facets: list[str] = field(default_factory=list)
    must_have_primary_role_buckets: list[str] = field(default_factory=list)
    must_have_keywords: list[str] = field(default_factory=list)
    exclude_keywords: list[str] = field(default_factory=list)
    organization_keywords: list[str] = field(default_factory=list)
    retrieval_strategy: str = ""
    planning_mode: str = "heuristic"
    analysis_stage_mode: str = "single_stage"
    execution_preferences: dict[str, Any] = field(default_factory=dict)
    scope_disambiguation: dict[str, Any] = field(default_factory=dict)
    intent_axes: dict[str, Any] = field(default_factory=dict)
    semantic_rerank_limit: int = 0
    top_k: int = 10
    slug_resolution_limit: int = 8
    profile_detail_limit: int = 5
    publication_scan_limit: int = 8
    publication_lead_limit: int = 12
    exploration_limit: int = 6
    scholar_coauthor_follow_up_limit: int = 0

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "JobRequest":
        normalized_payload = apply_query_intent_rewrite(payload)
        if isinstance(normalized_payload, dict) and isinstance(normalized_payload.get("intent_axes"), dict):
            from .request_normalization import materialize_request_payload

            normalized_payload = materialize_request_payload(
                normalized_payload,
                target_company=str(normalized_payload.get("target_company") or "").strip(),
            )
        raw_user_request = _clean(normalized_payload.get("raw_user_request")) or _clean(normalized_payload.get("query"))
        target_company = _clean(normalized_payload.get("target_company"))
        if not target_company:
            inferred_company = infer_target_company_from_text(raw_user_request)
            target_company = str(inferred_company.get("canonical_name") or "").strip()
            if target_company:
                normalized_payload["target_company"] = target_company
        explicit_execution_preferences = normalize_execution_preferences(normalized_payload, target_company=target_company)
        inferred_execution_preferences = infer_execution_preferences_from_text(raw_user_request, target_company=target_company)
        merged_execution_preferences = merge_execution_preferences(
            explicit_execution_preferences,
            inferred_execution_preferences,
        )
        normalized_employment_statuses = _normalize_list(normalized_payload.get("employment_statuses"))
        if not normalized_employment_statuses:
            normalized_employment_statuses = _infer_default_employment_statuses(raw_user_request)
        normalized_keywords = _normalize_list(normalized_payload.get("keywords"))
        normalized_must_have_facets = normalize_requested_facets(
            normalized_payload.get("must_have_facets")
            if normalized_payload.get("must_have_facets") is not None
            else normalized_payload.get("must_have_facet")
        )
        if target_company:
            from .request_normalization import normalize_must_have_facets_for_request_fields

            normalized_must_have_facets, normalized_keywords = normalize_must_have_facets_for_request_fields(
                must_have_facets=normalized_must_have_facets,
                keywords=normalized_keywords,
                target_company=target_company,
            )
        return cls(
            raw_user_request=raw_user_request,
            query=_clean(normalized_payload.get("query")),
            target_company=target_company,
            asset_view=_normalize_asset_view(normalized_payload.get("asset_view")),
            target_scope=_clean(normalized_payload.get("target_scope")) or "full_company_asset",
            categories=_normalize_list(normalized_payload.get("categories")),
            employment_statuses=normalized_employment_statuses,
            keywords=normalized_keywords,
            must_have_facets=normalized_must_have_facets,
            must_have_primary_role_buckets=normalize_requested_role_buckets(
                normalized_payload.get("must_have_primary_role_buckets")
                if normalized_payload.get("must_have_primary_role_buckets") is not None
                else normalized_payload.get("must_have_primary_role_bucket")
            ),
            must_have_keywords=_normalize_list(normalized_payload.get("must_have_keywords")),
            exclude_keywords=_normalize_list(normalized_payload.get("exclude_keywords")),
            organization_keywords=_normalize_list(normalized_payload.get("organization_keywords")),
            retrieval_strategy=_clean(normalized_payload.get("retrieval_strategy")),
            planning_mode=_clean(normalized_payload.get("planning_mode")) or "heuristic",
            analysis_stage_mode=_normalize_analysis_stage_mode(normalized_payload.get("analysis_stage_mode")),
            execution_preferences=apply_execution_preference_policy(
                merged_execution_preferences,
                raw_text=raw_user_request,
                target_company=target_company,
                categories=_normalize_list(normalized_payload.get("categories")),
                employment_statuses=_normalize_list(normalized_payload.get("employment_statuses")),
            ),
            scope_disambiguation=_normalize_scope_disambiguation(
                normalized_payload.get("scope_disambiguation"),
                target_company=target_company,
            ),
            intent_axes=dict(normalized_payload.get("intent_axes") or {})
            if isinstance(normalized_payload.get("intent_axes"), dict)
            else {},
            semantic_rerank_limit=_normalize_semantic_limit(normalized_payload.get("semantic_rerank_limit")),
            top_k=_normalize_top_k(normalized_payload.get("top_k")),
            slug_resolution_limit=_normalize_small_limit(normalized_payload.get("slug_resolution_limit"), default=8, maximum=50),
            profile_detail_limit=_normalize_small_limit(normalized_payload.get("profile_detail_limit"), default=5, maximum=50),
            publication_scan_limit=_normalize_small_limit(normalized_payload.get("publication_scan_limit"), default=8, maximum=50),
            publication_lead_limit=_normalize_small_limit(normalized_payload.get("publication_lead_limit"), default=12, maximum=100),
            exploration_limit=_normalize_small_limit(normalized_payload.get("exploration_limit"), default=6, maximum=50),
            scholar_coauthor_follow_up_limit=_normalize_small_limit(
                normalized_payload.get("scholar_coauthor_follow_up_limit"),
                default=0,
                maximum=100,
            ),
        )

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


def _normalize_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = [value]
    else:
        items = list(value)
    return [_clean(item) for item in items if _clean(item)]


def _infer_default_employment_statuses(raw_text: str) -> list[str]:
    normalized = " ".join(str(raw_text or "").strip().split()).lower()
    if not normalized:
        return ["current", "former"]
    has_current = any(token in normalized for token in ["在职", "当前", "现任", "current"])
    has_former = any(token in normalized for token in ["离职", "former", "前员工", "前成员", "past"])
    if has_current and not has_former:
        return ["current"]
    if has_former and not has_current:
        return ["former"]
    return ["current", "former"]


def _normalize_scope_disambiguation(value: Any, *, target_company: str = "") -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    aliases = {
        "parent_company": "parent",
        "parent_only": "parent",
        "sub_org": "sub_org_only",
        "suborg": "sub_org_only",
        "suborg_only": "sub_org_only",
        "both_parent_and_sub_org": "both",
        "all": "both",
        "ambiguous": "uncertain",
        "unknown": "uncertain",
    }
    allowed_scopes = {"parent", "sub_org_only", "both", "uncertain"}
    normalized: dict[str, Any] = {}

    inferred_scope = str(value.get("inferred_scope") or value.get("scope") or "").strip().lower()
    inferred_scope = aliases.get(inferred_scope, inferred_scope)
    if inferred_scope in allowed_scopes:
        normalized["inferred_scope"] = inferred_scope

    raw_candidates = value.get("sub_org_candidates")
    if isinstance(raw_candidates, str):
        candidate_items = re.split(r"[/,，、;\n]+", raw_candidates)
    elif isinstance(raw_candidates, (list, tuple, set)):
        candidate_items = [str(item or "") for item in raw_candidates]
    else:
        candidate_items = []
    sub_org_candidates: list[str] = []
    for item in candidate_items:
        candidate = " ".join(str(item or "").split()).strip()
        if target_company and candidate.lower().startswith(target_company.lower() + " "):
            if builtin_company_identity(candidate)[1] is None:
                stripped = candidate[len(target_company) :].strip(" \t\r\n-:/")
                if stripped:
                    candidate = stripped
        if not candidate or candidate in sub_org_candidates:
            continue
        sub_org_candidates.append(candidate[:120])
    if sub_org_candidates:
        normalized["sub_org_candidates"] = sub_org_candidates[:10]

    raw_confidence = value.get("confidence")
    if raw_confidence is None:
        raw_confidence = value.get("confidence_score")
    try:
        confidence = float(raw_confidence)
    except (TypeError, ValueError):
        confidence = None
    if confidence is not None:
        normalized["confidence"] = max(0.0, min(round(confidence, 3), 1.0))

    rationale = " ".join(str(value.get("rationale") or "").split()).strip()
    if rationale:
        normalized["rationale"] = rationale[:600]

    source = str(value.get("source") or "").strip().lower()
    if source in {"llm", "rules", "hybrid"}:
        normalized["source"] = source

    if target_company:
        normalized["target_company"] = str(target_company).strip()
    return normalized


def _normalize_top_k(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 10
    return max(1, min(parsed, 50))


def _normalize_semantic_limit(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, min(parsed, 50))


def _normalize_small_limit(value: Any, default: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(0, min(parsed, maximum))


def _normalize_asset_view(value: Any) -> str:
    normalized = _clean(value).lower()
    if not normalized:
        return "canonical_merged"
    if normalized in {"canonical_merged", "strict_roster_only"}:
        return normalized
    return "canonical_merged"


def _normalize_analysis_stage_mode(value: Any) -> str:
    normalized = _clean(value).lower()
    if normalized in {"two_stage", "stage1_then_stage2", "preview_then_ai"}:
        return "two_stage"
    if normalized in {"single_stage", "single", "immediate"}:
        return "single_stage"
    return "single_stage"


CATEGORY_PRIORITY = {
    "employee": 4,
    "former_employee": 4,
    "investor": 3,
    "lead": 1,
}

FACET_PRIORITY = [
    "investor",
    "founding",
    "leadership",
    "recruiting",
    "ops",
    "product_management",
    "infra_systems",
    "research",
    "engineering",
]

ROLE_BUCKET_PRIORITY = FACET_PRIORITY + ["generalist"]

FACET_ALIAS_MAP = {
    "investor": {"investor", "investment"},
    "founding": {"founding", "founder", "cofounder", "co-founder"},
    "leadership": {"leadership", "leader", "head", "director", "vp", "vice president", "chief"},
    "recruiting": {"recruiting", "recruiter", "talent", "talent acquisition"},
    "ops": {"ops", "operations", "business operations", "people operations", "programs", "chief of staff"},
    "product_management": {"product_management", "product management", "product manager", "产品经理", "pm"},
    "infra_systems": {"infra_systems", "infra", "infrastructure", "systems", "platform", "distributed systems"},
    "research": {"research", "researcher", "scientist", "applied scientist"},
    "engineering": {"engineering", "engineer", "technical staff", "member of technical staff"},
    "multimodal": {"multimodal", "multimodality", "vision-language", "vision language"},
    "safety": {"safety", "alignment", "evals", "evaluation"},
    "training": {"training", "pretraining", "pre-training", "post-training", "reinforcement learning"},
    "inference": {"inference", "serving", "runtime", "decoding"},
    "data": {"data", "data systems", "data platform", "datasets"},
    "greater_china_region_experience": {
        "greater_china_region_experience",
        "greater china experience",
        "greater china region experience",
        "greater china",
        "china experience",
    },
    "mainland_china_experience_or_chinese_language": {
        "mainland_china_experience_or_chinese_language",
        "mainland or chinese language",
        "mainland china experience",
        "chinese language signal",
    },
}

ROLE_BUCKET_ALIAS_MAP = {
    "investor": {"investment"},
    "founding": {"founder", "cofounder", "co-founder", "founders"},
    "leadership": {"leader", "leaders", "head", "exec", "executive", "management"},
    "recruiting": {"recruiter", "talent", "talent acquisition", "sourcer"},
    "ops": {"operations", "operation", "bizops", "business operations", "people operations", "chief of staff"},
    "product_management": {"product management", "product manager", "产品经理", "pm"},
    "infra_systems": {"infra", "infrastructure", "systems", "system", "platform", "distributed systems"},
    "research": {"researcher", "scientist", "applied scientist"},
    "engineering": {"engineer", "eng", "technical staff", "member of technical staff"},
    "generalist": {"general", "generalists", "member"},
}


def merge_candidate(existing: Candidate, incoming: Candidate) -> Candidate:
    merged = existing.to_record()
    incoming_record = incoming.to_record()
    if CATEGORY_PRIORITY.get(incoming.category, 0) > CATEGORY_PRIORITY.get(existing.category, 0):
        merged["category"] = incoming.category
    for key, value in incoming_record.items():
        if key == "category":
            continue
        if key == "metadata":
            meta = dict(existing.metadata)
            meta.update(incoming.metadata)
            merged["metadata"] = meta
            continue
        if not _clean(merged.get(key)) and _clean(value):
            merged[key] = value
    if not merged["display_name"]:
        merged["display_name"] = format_display_name(merged["name_en"], merged["name_zh"])
    return normalize_candidate(Candidate(**merged))


def format_display_name(name_en: str, name_zh: str) -> str:
    if _clean(name_zh):
        return f"{_clean(name_en)}（{_clean(name_zh)}）"
    return _clean(name_en)


def normalize_candidate(candidate: Candidate) -> Candidate:
    record = candidate.to_record()
    metadata = dict(record.get("metadata") or {})
    category = _clean(record.get("category")).lower()
    employment_status = _clean(record.get("employment_status")).lower()
    role = _clean(record.get("role"))
    focus_areas = _clean(record.get("focus_areas"))
    membership_decision = _clean(metadata.get("membership_review_decision")).lower()

    if category in {"employee", ""} and _role_indicates_investor(role):
        category = "investor"
        if not _clean(record.get("investment_involvement")):
            record["investment_involvement"] = "是"

    if category in {"employee", ""} and employment_status == "former":
        category = "former_employee"
    elif category == "former_employee" and not employment_status:
        employment_status = "former"

    if category in {"employee", ""} and _focus_indicates_investor(focus_areas):
        category = "investor"
        if not _clean(record.get("investment_involvement")):
            record["investment_involvement"] = "是"

    if category == "investor" and not employment_status:
        employment_status = "current"

    if membership_decision.endswith("non_member") or bool(metadata.get("target_company_mismatch")):
        category = "non_member"
        if employment_status not in {"current", "former"}:
            employment_status = ""

    record["category"] = category
    record["employment_status"] = employment_status
    return Candidate(**record)


def sanitize_candidate_notes(notes: str) -> str:
    text = str(notes or "").strip()
    if not text:
        return ""
    cleaned_parts: list[str] = []
    for raw_part in re.split(r"\s+\|\s+", text):
        part = raw_part
        part = re.sub(r"LinkedIn company roster baseline\.\s*", "", part, flags=re.IGNORECASE)
        part = re.sub(r"Location:\s*[^.|]+\.?\s*", "", part, flags=re.IGNORECASE)
        part = re.sub(r"Source account:\s*[^.|]+\.?\s*", "", part, flags=re.IGNORECASE)
        if re.match(r"^\s*Discovered from low-cost search seed acquisition\b", part, flags=re.IGNORECASE):
            continue
        part = " ".join(part.split()).strip(" .|;")
        if part:
            cleaned_parts.append(part)
    return " | ".join(cleaned_parts)


def candidate_profile_signal_text(candidate: Candidate, *, include_notes: bool = True) -> str:
    metadata = dict(candidate.metadata or {})
    parts = _dedupe_preserve_order(
        [
            str(metadata.get("headline") or "").strip(),
            str(metadata.get("summary") or "").strip(),
            str(metadata.get("about") or "").strip(),
            " / ".join(_normalize_metadata_text_list(metadata.get("languages"), limit=8)),
            " / ".join(_normalize_metadata_text_list(metadata.get("skills"), limit=16)),
            str(metadata.get("profile_location") or "").strip(),
            sanitize_candidate_notes(candidate.notes) if include_notes else "",
        ]
    )
    return " | ".join(parts)


def candidate_searchable_text(candidate: Candidate, *, include_notes: bool = True) -> str:
    timeline_text = candidate_timeline_signal_text(candidate)
    parts = _dedupe_preserve_order(
        [
            candidate.display_name,
            candidate.organization,
            candidate.role,
            candidate.team,
            candidate.focus_areas,
            candidate.investment_involvement,
            candidate.education,
            candidate.work_history,
            candidate_profile_signal_text(candidate, include_notes=include_notes),
            timeline_text,
            candidate.ethnicity_background,
            candidate.current_destination,
        ]
    )
    return " | ".join(parts)


def candidate_timeline_signal_text(candidate: Candidate) -> str:
    metadata = dict(candidate.metadata or {})
    return " | ".join(
        _dedupe_preserve_order(
            _normalize_metadata_text_list(metadata.get("experience_lines"), limit=16)
            + _normalize_metadata_text_list(metadata.get("education_lines"), limit=8)
        )
    )


def normalize_requested_facet(value: str) -> str:
    normalized = _clean(value).lower().replace("-", " ")
    normalized = " ".join(normalized.replace("_", " ").split())
    if not normalized:
        return ""
    for canonical, aliases in FACET_ALIAS_MAP.items():
        if normalized == canonical or normalized in aliases:
            return canonical
    return normalized.replace(" ", "_")


def normalize_requested_facets(values: Any) -> list[str]:
    if values is None:
        return []
    raw_items: list[str] = []
    if isinstance(values, str):
        raw_items.extend(re.split(r"[,/|]", values))
    else:
        for item in list(values):
            if isinstance(item, str):
                raw_items.extend(re.split(r"[,/|]", item))
            else:
                raw_items.append(str(item or ""))
    normalized = [normalize_requested_facet(item) for item in raw_items]
    return _dedupe_preserve_order(normalized)


def normalize_requested_role_bucket(value: str) -> str:
    normalized = _clean(value).lower().replace("-", " ")
    normalized = " ".join(normalized.replace("_", " ").split())
    if not normalized:
        return ""
    for canonical in ROLE_BUCKET_PRIORITY:
        if normalized == canonical.replace("_", " "):
            return canonical
    for canonical, aliases in ROLE_BUCKET_ALIAS_MAP.items():
        if normalized in aliases:
            return canonical
    return normalized.replace(" ", "_")


def normalize_requested_role_buckets(values: Any) -> list[str]:
    if values is None:
        return []
    raw_items: list[str] = []
    if isinstance(values, str):
        raw_items.extend(re.split(r"[,/|]", values))
    else:
        for item in list(values):
            if isinstance(item, str):
                raw_items.extend(re.split(r"[,/|]", item))
            else:
                raw_items.append(str(item or ""))
    normalized = [normalize_requested_role_bucket(item) for item in raw_items]
    allowed = set(ROLE_BUCKET_PRIORITY)
    return [item for item in _dedupe_preserve_order(normalized) if item in allowed]


def derive_candidate_facets(candidate: Candidate) -> list[str]:
    text = _candidate_signal_text(candidate, include_notes=False)
    facets: list[str] = []

    if candidate.category == "investor" or _contains_any(
        text,
        [
            "investor",
            "venture partner",
            "seed investor",
            "angel investor",
            "investment partner",
        ],
    ):
        facets.append("investor")
    if _contains_any(text, ["founder", "co-founder", "cofounder", "founding"]):
        facets.append("founding")
    if _contains_any(text, ["ceo", "cto", "chief", "vp ", "vice president", "head of", "director", "general partner"]):
        facets.append("leadership")
    if _contains_any(text, ["recruit", "talent", "talent acquisition", "sourcer", "people partner"]):
        facets.extend(["recruiting", "ops"])
    if _contains_any(
        text,
        [
            "operations",
            "business operations",
            "people operations",
            "program manager",
            "program management",
            "program staff",
            "programs staff",
            "chief of staff",
            "finance",
            "legal",
            "hr",
        ],
    ):
        facets.append("ops")
    if _contains_any(text, ["product manager", "product management", "产品经理", "group product manager", "senior product manager"]):
        facets.append("product_management")
    if _contains_any(
        text,
        [
            "infrastructure",
            "infra",
            "platform",
            "distributed systems",
            "systems engineer",
            "operating systems",
            "runtime",
            "serving",
            "compiler",
            "kernel",
            "cluster",
            "gpu",
            "gpu compute",
            "compute infrastructure",
            "backend",
            "performance",
        ],
    ):
        facets.append("infra_systems")
    if _contains_any(text, ["research scientist", "research engineer", "researcher", "scientist", "applied scientist", "research"]):
        facets.append("research")
    if _contains_any(text, ["engineer", "engineering", "member of technical staff", "technical staff", "developer", "architect"]):
        facets.append("engineering")
    if _contains_any(text, ["multimodal", "multimodality", "vision-language", "vision language", "vision", "image", "video", "audio", "speech", "diffusion"]):
        facets.append("multimodal")
    if _contains_any(text, ["alignment", "safety", "red team", "red-teaming", "evals", "evaluation"]):
        facets.append("safety")
    if _contains_any(text, ["training", "pretraining", "pre-training", "post-training", "finetuning", "fine-tuning", "reinforcement learning"]):
        facets.append("training")
    if _contains_any(text, ["inference", "decoding", "latency", "serving runtime"]):
        facets.append("inference")
    if _contains_any(text, ["data engineer", "data platform", "data infrastructure", "dataset", "data systems"]):
        facets.append("data")
    facets.extend(_derive_thematic_signal_facets(text))
    facets.extend(_derive_outreach_layer_facets(candidate))
    return _dedupe_preserve_order(facets)


def derive_candidate_role_bucket(candidate: Candidate) -> str:
    facets = derive_candidate_facets(candidate)
    for facet in FACET_PRIORITY:
        if facet in facets:
            return facet
    if facets:
        return facets[0]
    if candidate.category in {"employee", "former_employee"}:
        return "generalist"
    return candidate.category or "unknown"


def derive_candidate_filter_facets(candidate: Candidate) -> list[str]:
    facets = list(derive_candidate_facets(candidate))
    role_bucket = derive_candidate_role_bucket(candidate)
    if role_bucket not in {"", "unknown", "generalist"}:
        facets.insert(0, role_bucket)
    return _dedupe_preserve_order(facets)


def _role_indicates_investor(role: str) -> bool:
    normalized = _clean(role).lower()
    if not normalized:
        return False
    return normalized.startswith("investor at ") or normalized == "investor" or normalized.startswith("investor,")


def _focus_indicates_investor(focus_areas: str) -> bool:
    normalized = _clean(focus_areas).lower()
    if not normalized:
        return False
    return normalized.startswith("investor at ") or normalized.startswith("investor |")


def _candidate_signal_text(candidate: Candidate, *, include_notes: bool) -> str:
    return " ".join(
        part
        for part in [
            candidate.category,
            candidate.organization,
            candidate.role,
            candidate.team,
            candidate.focus_areas,
            candidate.education,
            candidate.work_history,
            candidate_profile_signal_text(candidate, include_notes=include_notes),
            candidate_timeline_signal_text(candidate),
        ]
        if _clean(part)
    ).lower()


def _derive_thematic_signal_facets(text: str) -> list[str]:
    if not text:
        return []
    matched_facets: list[str] = []
    for spec in match_thematic_signals(text):
        facet_labels = [str(item).strip() for item in list(spec.get("facet_labels") or []) if str(item).strip()]
        if not facet_labels:
            canonical_label = str(spec.get("canonical_label") or "").strip()
            if canonical_label:
                facet_labels = [canonical_label.lower().replace("-", " ").replace("_", " ").strip().replace(" ", "_")]
        for facet in facet_labels:
            normalized_facet = normalize_requested_facet(facet)
            if normalized_facet and normalized_facet not in matched_facets:
                matched_facets.append(normalized_facet)
    return matched_facets


def _derive_outreach_layer_facets(candidate: Candidate) -> list[str]:
    metadata = dict(candidate.metadata or {})
    raw_layer = metadata.get("outreach_layer")
    try:
        layer = int(raw_layer)
    except (TypeError, ValueError):
        return []
    if layer >= 3:
        return ["mainland_china_experience_or_chinese_language", "greater_china_region_experience"]
    if layer >= 2:
        return ["greater_china_region_experience"]
    return []


def _contains_any(text: str, patterns: list[str]) -> bool:
    return any(pattern in text for pattern in patterns)


def _normalize_metadata_text_list(value: Any, *, limit: int) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        items = [value]
    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        candidate = _clean(item)
        if not candidate and isinstance(item, dict):
            candidate = _clean(item.get("name") or item.get("title") or item.get("value") or item.get("text"))
        key = candidate.lower()
        if not candidate or key in seen:
            continue
        seen.add(key)
        normalized.append(candidate)
        if len(normalized) >= limit:
            break
    return normalized


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for item in items:
        normalized = _clean(item).lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        results.append(normalized)
    return results


@dataclass(slots=True)
class AcquisitionTask:
    task_id: str
    task_type: str
    title: str
    description: str
    source_hint: str = ""
    status: str = "pending"
    blocking: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RetrievalPlan:
    strategy: str
    reason: str
    structured_filters: list[str] = field(default_factory=list)
    semantic_fields: list[str] = field(default_factory=list)
    filter_layers: list[dict[str, Any]] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AcquisitionStrategyPlan:
    strategy_type: str
    target_population: str
    company_scope: list[str] = field(default_factory=list)
    roster_sources: list[str] = field(default_factory=list)
    search_channel_order: list[str] = field(default_factory=list)
    search_seed_queries: list[str] = field(default_factory=list)
    filter_hints: dict[str, list[str]] = field(default_factory=dict)
    cost_policy: dict[str, Any] = field(default_factory=dict)
    confirmation_points: list[str] = field(default_factory=list)
    reasoning: list[str] = field(default_factory=list)
    organization_execution_profile: dict[str, Any] = field(default_factory=dict)
    strategy_decision_explanation: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PublicationSourcePlan:
    family: str
    priority: str
    rationale: str
    query_hints: list[str] = field(default_factory=list)
    extraction_mode: str = "deterministic"

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PublicationCoveragePlan:
    coverage_goal: str
    source_families: list[PublicationSourcePlan] = field(default_factory=list)
    seed_queries: list[str] = field(default_factory=list)
    extraction_strategy: list[str] = field(default_factory=list)
    validation_steps: list[str] = field(default_factory=list)
    fallback_steps: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["source_families"] = [item.to_record() for item in self.source_families]
        return record


@dataclass(slots=True)
class SearchQueryBundle:
    bundle_id: str
    source_family: str
    priority: str
    objective: str
    execution_mode: str
    queries: list[str] = field(default_factory=list)
    filters: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SearchStrategyPlan:
    planner_mode: str
    objective: str
    query_bundles: list[SearchQueryBundle] = field(default_factory=list)
    follow_up_rules: list[str] = field(default_factory=list)
    review_triggers: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["query_bundles"] = [item.to_record() for item in self.query_bundles]
        return record


@dataclass(slots=True)
class IntentPlanBrief:
    identified_request: list[str] = field(default_factory=list)
    target_output: list[str] = field(default_factory=list)
    default_execution_strategy: list[str] = field(default_factory=list)
    review_focus: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SourcingPlan:
    target_company: str
    target_scope: str
    intent_summary: str
    criteria_summary: str
    retrieval_plan: RetrievalPlan
    acquisition_strategy: AcquisitionStrategyPlan
    publication_coverage: PublicationCoveragePlan
    search_strategy: SearchStrategyPlan
    acquisition_tasks: list[AcquisitionTask]
    asset_reuse_plan: dict[str, Any] = field(default_factory=dict)
    organization_execution_profile: dict[str, Any] = field(default_factory=dict)
    intent_brief: IntentPlanBrief = field(default_factory=IntentPlanBrief)
    assumptions: list[str] = field(default_factory=list)
    open_questions: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["retrieval_plan"] = self.retrieval_plan.to_record()
        record["acquisition_strategy"] = self.acquisition_strategy.to_record()
        record["publication_coverage"] = self.publication_coverage.to_record()
        record["search_strategy"] = self.search_strategy.to_record()
        record["acquisition_tasks"] = [task.to_record() for task in self.acquisition_tasks]
        record["intent_brief"] = self.intent_brief.to_record()
        return record
