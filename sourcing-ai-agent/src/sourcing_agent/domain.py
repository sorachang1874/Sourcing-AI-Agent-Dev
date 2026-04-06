from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha1
from typing import Any


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
    target_scope: str = "full_company_asset"
    categories: list[str] = field(default_factory=list)
    employment_statuses: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    must_have_keywords: list[str] = field(default_factory=list)
    exclude_keywords: list[str] = field(default_factory=list)
    organization_keywords: list[str] = field(default_factory=list)
    retrieval_strategy: str = ""
    planning_mode: str = "heuristic"
    semantic_rerank_limit: int = 15
    top_k: int = 10
    slug_resolution_limit: int = 8
    profile_detail_limit: int = 5
    publication_scan_limit: int = 8
    publication_lead_limit: int = 12
    exploration_limit: int = 6

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "JobRequest":
        raw_user_request = _clean(payload.get("raw_user_request")) or _clean(payload.get("query"))
        return cls(
            raw_user_request=raw_user_request,
            query=_clean(payload.get("query")),
            target_company=_clean(payload.get("target_company")),
            target_scope=_clean(payload.get("target_scope")) or "full_company_asset",
            categories=_normalize_list(payload.get("categories")),
            employment_statuses=_normalize_list(payload.get("employment_statuses")),
            keywords=_normalize_list(payload.get("keywords")),
            must_have_keywords=_normalize_list(payload.get("must_have_keywords")),
            exclude_keywords=_normalize_list(payload.get("exclude_keywords")),
            organization_keywords=_normalize_list(payload.get("organization_keywords")),
            retrieval_strategy=_clean(payload.get("retrieval_strategy")),
            planning_mode=_clean(payload.get("planning_mode")) or "heuristic",
            semantic_rerank_limit=_normalize_semantic_limit(payload.get("semantic_rerank_limit")),
            top_k=_normalize_top_k(payload.get("top_k")),
            slug_resolution_limit=_normalize_small_limit(payload.get("slug_resolution_limit"), default=8, maximum=50),
            profile_detail_limit=_normalize_small_limit(payload.get("profile_detail_limit"), default=5, maximum=50),
            publication_scan_limit=_normalize_small_limit(payload.get("publication_scan_limit"), default=8, maximum=50),
            publication_lead_limit=_normalize_small_limit(payload.get("publication_lead_limit"), default=12, maximum=100),
            exploration_limit=_normalize_small_limit(payload.get("exploration_limit"), default=6, maximum=50),
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
        return 15
    return max(1, min(parsed, 50))


def _normalize_small_limit(value: Any, default: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(0, min(parsed, maximum))


CATEGORY_PRIORITY = {
    "employee": 4,
    "former_employee": 4,
    "investor": 3,
    "lead": 1,
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
    return Candidate(**merged)


def format_display_name(name_en: str, name_zh: str) -> str:
    if _clean(name_zh):
        return f"{_clean(name_en)}（{_clean(name_zh)}）"
    return _clean(name_en)


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
    assumptions: list[str] = field(default_factory=list)
    open_questions: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["retrieval_plan"] = self.retrieval_plan.to_record()
        record["acquisition_strategy"] = self.acquisition_strategy.to_record()
        record["publication_coverage"] = self.publication_coverage.to_record()
        record["search_strategy"] = self.search_strategy.to_record()
        record["acquisition_tasks"] = [task.to_record() for task in self.acquisition_tasks]
        return record
