from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha1
from html import unescape
from itertools import combinations
from pathlib import Path
from typing import Any
from urllib import error, parse, request
from xml.etree import ElementTree as ET

from .agent_runtime import AgentRuntimeCoordinator
from .asset_catalog import AssetCatalog
from .asset_logger import AssetLogger
from .connectors import CompanyIdentity, RapidApiAccount, profile_detail_accounts, search_people_accounts
from .domain import (
    Candidate,
    EvidenceRecord,
    JobRequest,
    make_candidate_id,
    make_evidence_id,
    merge_candidate,
    normalize_name_token,
)
from .exploratory_enrichment import ExploratoryWebEnricher
from .harvest_connectors import (
    HarvestProfileConnector,
    HarvestProfileSearchConnector,
    harvest_connector_available,
    parse_harvest_profile_payload,
    write_harvest_execution_artifact,
)
from .model_provider import ModelClient
from .profile_registry_utils import (
    extract_profile_registry_aliases_from_payload,
    harvest_profile_payload_has_usable_content,
    profile_cache_path_candidates,
)
from .runtime_tuning import (
    resolve_runtime_timing_overrides,
    resolved_harvest_profile_scrape_global_inflight,
    resolved_harvest_prefetch_submit_workers,
    resolved_parallel_exploration_workers,
    runtime_inflight_slot,
)
from .runtime_environment import external_provider_mode
from .search_provider import BaseSearchProvider, DuckDuckGoHtmlSearchProvider, search_response_to_record
from .storage import SQLiteStore

TECHNICAL_SIGNAL_TOKENS = {
    "engineer",
    "engineering",
    "research",
    "scientist",
    "technical staff",
    "infrastructure",
    "machine learning",
    "ml",
    "ai",
    "training",
    "inference",
    "gpu",
    "systems",
    "distributed",
}

SUSPICIOUS_PROFILE_KEYWORDS = {
    "spiritual",
    "healer",
    "healing",
    "psychic",
    "tarot",
    "astrology",
    "spell",
    "sorcery",
    "witchcraft",
    "whatsapp",
    "telegram",
    "روحاني",
    "الروحانية",
    "الروحية",
    "السحر",
    "الحسد",
    "العين",
    "الأرزاق",
    "الارزاق",
    "تنزيل الأموال",
    "تنزيل الاموال",
    "الأوراد",
    "الاوراد",
    "الطاقي",
    "النورانية",
    "الخدام",
}

ACK_STOPWORDS = {
    "anthropic",
    "xai",
    "acknowledgements",
    "acknowledgment",
    "appendix",
    "references",
    "thanks",
    "thank",
    "figure",
    "section",
    "supplementary material",
}

HARVEST_PROFILE_PREFETCH_BATCH_SIZE = 250
HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE = 100
HARVEST_PROFILE_LIVE_FETCH_MAX_CONCURRENCY = 3
HARVEST_PROFILE_NONLIVE_FETCH_CONCURRENCY = 4
PROFILE_REGISTRY_LEASE_SECONDS = 240
PROFILE_REGISTRY_LEASE_WAIT_SECONDS = 18.0
PROFILE_REGISTRY_LEASE_POLL_SECONDS = 0.8


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _external_provider_mode() -> str:
    return external_provider_mode()


def _runtime_timing_overrides_from_request_payload(request_payload: dict[str, Any] | None) -> dict[str, Any]:
    if not request_payload:
        return {}
    execution_preferences = dict(JobRequest.from_payload(dict(request_payload or {})).execution_preferences or {})
    return resolve_runtime_timing_overrides(execution_preferences)


def _runtime_tuning_context_from_request_payload(request_payload: dict[str, Any] | None) -> dict[str, Any]:
    if not request_payload:
        return {}
    execution_preferences = dict(JobRequest.from_payload(dict(request_payload or {})).execution_preferences or {})
    return {
        **resolve_runtime_timing_overrides(execution_preferences),
        **execution_preferences,
    }


HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE = max(
    1,
    _env_int("HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE", 25),
)
HARVEST_PROFILE_PREFETCH_BATCH_SIZE = max(
    HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE,
    _env_int("HARVEST_PROFILE_PREFETCH_BATCH_SIZE", 250),
)
HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE_LIVE = max(
    1,
    _env_int("HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE_LIVE", 75),
)
HARVEST_PROFILE_PREFETCH_BATCH_SIZE_LIVE = max(
    HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE_LIVE,
    _env_int("HARVEST_PROFILE_PREFETCH_BATCH_SIZE_LIVE", 150),
)
HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE_LIVE = max(
    1,
    _env_int("HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE_LIVE", 150),
)


def _recommended_harvest_profile_prefetch_batch_size(total_candidates: int, *, priority: bool) -> int:
    default_size = HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE if priority else HARVEST_PROFILE_PREFETCH_BATCH_SIZE
    count = max(0, int(total_candidates or 0))
    if _external_provider_mode() == "live":
        live_default_size = (
            HARVEST_PROFILE_PRIORITY_PREFETCH_BATCH_SIZE_LIVE
            if priority
            else HARVEST_PROFILE_PREFETCH_BATCH_SIZE_LIVE
        )
        if priority:
            if count >= 1000:
                return min(live_default_size, 100)
            if count >= 200:
                return min(live_default_size, 75)
            return live_default_size
        if count >= 2000:
            return min(live_default_size, 200)
        if count >= 500:
            return min(live_default_size, 150)
        if count >= 150:
            return min(live_default_size, 100)
        return min(live_default_size, 75)
    if priority:
        if count >= 2000:
            return min(default_size, 20)
        if count >= 1000:
            return min(default_size, 25)
        if count >= 300:
            return min(default_size, 40)
        return default_size
    if count >= 5000:
        return min(default_size, 30)
    if count >= 2000:
        return min(default_size, 40)
    if count >= 1000:
        return min(default_size, 50)
    if count >= 300:
        return min(default_size, 75)
    return default_size


def _recommended_harvest_profile_live_fetch_batch_size(total_urls: int) -> int:
    count = max(0, int(total_urls or 0))
    if _external_provider_mode() == "live":
        if count >= 200:
            return HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE_LIVE
        if count >= 50:
            return min(HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE_LIVE, 100)
        return min(HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE_LIVE, 50)
    if count >= 2000:
        return min(HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE, 40)
    if count >= 500:
        return min(HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE, 50)
    if count >= 200:
        return min(HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE, 75)
    return HARVEST_PROFILE_LIVE_FETCH_BATCH_SIZE


def _harvest_profile_request_source_mix(
    source_shards_by_url: dict[str, list[str]] | None,
    *,
    total_urls: int,
) -> dict[str, int]:
    mix = {
        "company_roster": 0,
        "profile_search": 0,
        "targeted": 0,
        "other": 0,
    }
    for labels in dict(source_shards_by_url or {}).values():
        normalized_labels = " ".join(
            str(item or "").strip().lower()
            for item in list(labels or [])
            if str(item or "").strip()
        )
        if not normalized_labels:
            mix["other"] += 1
            continue
        if any(
            token in normalized_labels
            for token in (
                "publication_lead_targeted",
                "dataset:publication_lead",
                "dataset:targeted_name",
                "resolution_source:publication_lead",
            )
        ):
            mix["targeted"] += 1
            continue
        if "harvest_company_employees" in normalized_labels:
            mix["company_roster"] += 1
            continue
        if "harvest_profile_search" in normalized_labels:
            mix["profile_search"] += 1
            continue
        mix["other"] += 1
    labeled_total = sum(mix.values())
    if labeled_total < max(0, int(total_urls or 0)):
        mix["other"] += max(0, int(total_urls or 0) - labeled_total)
    return mix


def _recommended_harvest_profile_live_fetch_window(
    total_urls: int,
    *,
    source_shards_by_url: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    count = max(0, int(total_urls or 0))
    source_mix = _harvest_profile_request_source_mix(
        source_shards_by_url,
        total_urls=count,
    )
    if count <= 0:
        return {
            "batch_size": 1,
            "max_workers": 1,
            "batch_count": 0,
            "source_mix": source_mix,
        }

    labeled_total = max(1, sum(source_mix.values()))
    roster_ratio = float(source_mix.get("company_roster") or 0) / labeled_total
    profile_search_ratio = float(source_mix.get("profile_search") or 0) / labeled_total
    targeted_ratio = float(source_mix.get("targeted") or 0) / labeled_total

    if _external_provider_mode() == "live":
        if count <= 40:
            desired_batch_count = 1
            target_workers = 1
        elif targeted_ratio >= 0.35:
            target_batch_size = 35 if count <= 120 else 45
            desired_batch_count = max(2, (count + target_batch_size - 1) // target_batch_size)
            target_workers = 2
        elif roster_ratio >= 0.6:
            if count >= 360:
                target_batch_size = 75
                minimum_batches = 6
            elif count >= 180:
                target_batch_size = 60
                minimum_batches = 4
            else:
                target_batch_size = 50
                minimum_batches = 3
            desired_batch_count = max(minimum_batches, (count + target_batch_size - 1) // target_batch_size)
            target_workers = HARVEST_PROFILE_LIVE_FETCH_MAX_CONCURRENCY if count >= 180 else 2
        elif profile_search_ratio >= 0.5:
            target_batch_size = 45 if count < 240 else 55
            desired_batch_count = max(3 if count >= 120 else 2, (count + target_batch_size - 1) // target_batch_size)
            target_workers = 2
        else:
            target_batch_size = 40 if count < 120 else 60
            desired_batch_count = max(2 if count < 120 else 4, (count + target_batch_size - 1) // target_batch_size)
            target_workers = 2
    else:
        if count <= 60:
            desired_batch_count = 1
        else:
            target_batch_size = 40 if targeted_ratio >= 0.35 else 50
            desired_batch_count = max(2, (count + target_batch_size - 1) // target_batch_size)
        target_workers = HARVEST_PROFILE_NONLIVE_FETCH_CONCURRENCY

    desired_batch_count = max(1, desired_batch_count)
    batch_size = max(1, (count + desired_batch_count - 1) // desired_batch_count)
    batch_count = max(1, (count + batch_size - 1) // batch_size)
    return {
        "batch_size": batch_size,
        "max_workers": min(max(1, int(target_workers or 1)), batch_count),
        "batch_count": batch_count,
        "source_mix": source_mix,
    }


def _chunk_strings(values: list[str], chunk_size: int) -> list[list[str]]:
    size = max(1, int(chunk_size or 1))
    return [values[index : index + size] for index in range(0, len(values), size)]


def _balanced_chunk_strings(values: list[str], chunk_size: int) -> list[list[str]]:
    normalized = [str(value or "").strip() for value in list(values or []) if str(value or "").strip()]
    if not normalized:
        return []
    size = max(1, int(chunk_size or 1))
    if len(normalized) <= size:
        return [normalized]
    chunk_count = max(1, (len(normalized) + size - 1) // size)
    base_size, remainder = divmod(len(normalized), chunk_count)
    chunks: list[list[str]] = []
    offset = 0
    for index in range(chunk_count):
        current_size = base_size + (1 if index < remainder else 0)
        chunks.append(normalized[offset : offset + current_size])
        offset += current_size
    return chunks


@dataclass(slots=True)
class MultiSourceEnrichmentResult:
    candidates: list[Candidate]
    evidence: list[EvidenceRecord]
    resolved_profiles: list[dict[str, Any]] = field(default_factory=list)
    unresolved_candidates: list[dict[str, Any]] = field(default_factory=list)
    publication_matches: list[dict[str, Any]] = field(default_factory=list)
    lead_candidates: list[Candidate] = field(default_factory=list)
    coauthor_edges: list[dict[str, Any]] = field(default_factory=list)
    artifact_paths: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    queued_harvest_worker_count: int = 0
    queued_exploration_count: int = 0
    stop_reason: str = ""

    def to_record(self) -> dict[str, Any]:
        return {
            "resolved_profile_count": len(self.resolved_profiles),
            "unresolved_candidate_count": len(self.unresolved_candidates),
            "publication_match_count": len(self.publication_matches),
            "lead_candidate_count": len(self.lead_candidates),
            "coauthor_edge_count": len(self.coauthor_edges),
            "queued_harvest_worker_count": self.queued_harvest_worker_count,
            "queued_exploration_count": self.queued_exploration_count,
            "stop_reason": self.stop_reason,
            "artifact_paths": self.artifact_paths,
            "errors": self.errors,
        }


@dataclass(slots=True)
class PublicationRecord:
    publication_id: str
    source: str
    source_dataset: str
    source_path: str
    title: str
    url: str
    year: int | None
    authors: list[str]
    acknowledgement_names: list[str]
    abstract: str = ""
    topics: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return {
            "publication_id": self.publication_id,
            "source": self.source,
            "source_dataset": self.source_dataset,
            "source_path": self.source_path,
            "title": self.title,
            "url": self.url,
            "year": self.year,
            "authors": self.authors,
            "acknowledgement_names": self.acknowledgement_names,
            "abstract": self.abstract,
            "topics": self.topics,
        }


class MultiSourceEnricher:
    def __init__(
        self,
        catalog: AssetCatalog,
        accounts: list[RapidApiAccount],
        harvest_profile_connector: HarvestProfileConnector | None = None,
        harvest_profile_search_connector: HarvestProfileSearchConnector | None = None,
        model_client: ModelClient | None = None,
        search_provider: BaseSearchProvider | None = None,
        worker_runtime: AgentRuntimeCoordinator | None = None,
        store: SQLiteStore | None = None,
    ) -> None:
        self.catalog = catalog
        self.model_client = model_client
        self.store = store
        resolved_search_provider = search_provider or DuckDuckGoHtmlSearchProvider()
        self.slug_resolver = LinkedInSearchSlugResolver(accounts, search_provider=resolved_search_provider)
        self.profile_connector = LinkedInProfileDetailConnector(accounts)
        self.publication_connector = CompanyPublicationConnector(catalog)
        self.harvest_profile_connector = harvest_profile_connector
        self.harvest_profile_search_connector = harvest_profile_search_connector
        self.worker_runtime = worker_runtime
        self.exploratory_enricher = ExploratoryWebEnricher(
            model_client,
            worker_runtime=worker_runtime,
            search_provider=resolved_search_provider,
        )

    def enrich(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        candidates: list[Candidate],
        job_request: JobRequest,
        *,
        asset_logger: AssetLogger | None = None,
        job_id: str = "",
        request_payload: dict[str, Any] | None = None,
        plan_payload: dict[str, Any] | None = None,
        runtime_mode: str = "workflow",
        parallel_exploration_workers: int = 2,
        cost_policy: dict[str, Any] | None = None,
        full_roster_profile_prefetch: bool = False,
        enrichment_scope: str = "full",
    ) -> MultiSourceEnrichmentResult:
        logger = asset_logger or AssetLogger(snapshot_dir)
        effective_cost_policy = dict(cost_policy or {})
        allow_shared_provider_cache = bool(effective_cost_policy.get("allow_shared_provider_cache", True))
        effective_request_payload = dict(request_payload or job_request.to_record() or {})
        runtime_tuning_context = _runtime_tuning_context_from_request_payload(effective_request_payload)
        normalized_scope = str(enrichment_scope or "full").strip().lower()
        linkedin_stage_enabled = normalized_scope not in {"public_web_stage_2", "public_web_only"}
        public_web_stage_enabled = normalized_scope not in {"linkedin_stage_1", "linkedin_only"}
        candidate_map = _candidate_name_map(candidates)
        evidence: list[EvidenceRecord] = []
        resolved_profiles: list[dict[str, Any]] = []
        unresolved_candidates: list[dict[str, Any]] = []
        artifact_paths: dict[str, str] = {}
        errors: list[str] = []
        queued_harvest_worker_count = 0
        queued_exploration_count = 0

        prioritized = _prioritize_candidates(candidates)
        slug_resolution_limit = min(job_request.slug_resolution_limit, len(prioritized))
        profile_detail_limit = job_request.profile_detail_limit
        profile_fetch_count = 0
        prefetched_harvest_profiles: dict[str, dict[str, Any]] = {}
        background_prefetch_urls: set[str] = set()
        if linkedin_stage_enabled and self.harvest_profile_connector is not None and slug_resolution_limit > 0 and profile_detail_limit > 0:
            resolution_prefetch_candidates = prioritized[:slug_resolution_limit]
            background_prefetch_candidates = prioritized if full_roster_profile_prefetch else resolution_prefetch_candidates
            resolution_candidate_ids = {
                str(candidate.candidate_id or "").strip()
                for candidate in resolution_prefetch_candidates
                if str(candidate.candidate_id or "").strip()
            }
            known_profile_urls: list[str] = []
            known_profile_url_set: set[str] = set()
            resolution_prefetch_urls: list[str] = []
            resolution_prefetch_url_set: set[str] = set()
            queue_artifact_paths: list[str] = []
            pending_dispatch_urls: list[str] = []
            priority_dispatch_urls: list[str] = []
            prefetch_source_shards_by_url: dict[str, set[str]] = {}
            resolution_candidate_profile_urls: list[str] = []
            resolution_candidate_profile_url_set: set[str] = set()
            for candidate in resolution_prefetch_candidates:
                for profile_url in _candidate_profile_urls(candidate):
                    normalized_profile_url = str(profile_url or "").strip()
                    if not normalized_profile_url or normalized_profile_url in resolution_candidate_profile_url_set:
                        continue
                    resolution_candidate_profile_url_set.add(normalized_profile_url)
                    resolution_candidate_profile_urls.append(normalized_profile_url)
            cached_resolution_prefetch_profiles = self._hydrate_cached_prefetch_profiles(
                resolution_candidate_profile_urls,
                snapshot_dir,
                source_jobs=[job_id] if str(job_id or "").strip() else [],
            )
            if cached_resolution_prefetch_profiles:
                prefetched_harvest_profiles.update(cached_resolution_prefetch_profiles)
            cached_resolution_prefetch_url_set = {
                str(profile_url or "").strip()
                for profile_url in list(cached_resolution_prefetch_profiles.keys())
                if str(profile_url or "").strip()
            }
            worker_prefetch_enabled = (
                self.worker_runtime is not None
                and bool(job_id)
                and bool(getattr(getattr(self.harvest_profile_connector, "settings", None), "enabled", False))
            )
            priority_prefetch_batch_size = _recommended_harvest_profile_prefetch_batch_size(
                len(resolution_prefetch_candidates),
                priority=True,
            )
            default_prefetch_batch_size = _recommended_harvest_profile_prefetch_batch_size(
                len(background_prefetch_candidates),
                priority=False,
            )

            def _dispatch_prefetch_chunk(
                *,
                chunk_index: int,
                profile_url_chunk: list[str],
            ) -> dict[str, Any]:
                if not profile_url_chunk:
                    return {
                        "chunk_index": chunk_index,
                        "cached_profiles": {},
                        "queued_urls": [],
                        "failed_urls": [],
                        "error_message": "",
                        "summary_path": "",
                    }
                harvest_worker = self._execute_harvest_profile_batch_worker(
                    profile_urls=profile_url_chunk,
                    snapshot_dir=snapshot_dir,
                    job_id=job_id,
                    request_payload=effective_request_payload,
                    plan_payload=plan_payload or {},
                    runtime_mode=runtime_mode,
                    allow_shared_provider_cache=allow_shared_provider_cache,
                )
                cached_profiles = {
                    str(profile_url or "").strip(): dict(payload or {})
                    for profile_url, payload in dict(harvest_worker.get("cached_profiles") or {}).items()
                    if str(profile_url or "").strip()
                }
                worker_summary = dict(harvest_worker.get("summary") or {})
                queued_urls: list[str] = []
                failed_urls: list[str] = []
                error_message = ""
                if str(harvest_worker.get("worker_status") or "") == "queued":
                    queued_urls = [
                        str(profile_url or "").strip()
                        for profile_url in list(
                            worker_summary.get("queued_urls")
                            or worker_summary.get("requested_urls")
                            or []
                        )
                        if str(profile_url or "").strip()
                    ]
                elif str(harvest_worker.get("worker_status") or "") == "failed":
                    failed_urls = [
                        str(profile_url or "").strip()
                        for profile_url in list(
                            harvest_worker.get("failed_urls")
                            or worker_summary.get("failed_urls")
                            or worker_summary.get("queued_urls")
                            or []
                        )
                        if str(profile_url or "").strip()
                    ]
                    error_message = str(worker_summary.get("message") or "").strip()
                return {
                    "chunk_index": chunk_index,
                    "cached_profiles": cached_profiles,
                    "queued_urls": queued_urls,
                    "failed_urls": failed_urls,
                    "error_message": error_message,
                    "summary_path": str(worker_summary.get("summary_path") or "").strip(),
                }

            for candidate in background_prefetch_candidates:
                candidate_id = str(candidate.candidate_id or "").strip()
                candidate_profile_urls = _candidate_profile_urls(candidate)
                candidate_source_shards = _profile_registry_sources_for_candidate(candidate)
                for profile_url in candidate_profile_urls:
                    normalized_profile_url = str(profile_url or "").strip()
                    if not normalized_profile_url or normalized_profile_url in known_profile_url_set:
                        continue
                    known_profile_url_set.add(normalized_profile_url)
                    known_profile_urls.append(normalized_profile_url)
                    prefetch_source_shards_by_url.setdefault(normalized_profile_url, set()).update(candidate_source_shards)
                    if candidate_id and candidate_id in resolution_candidate_ids:
                        if (
                            normalized_profile_url not in resolution_prefetch_url_set
                            and normalized_profile_url not in cached_resolution_prefetch_url_set
                        ):
                            resolution_prefetch_url_set.add(normalized_profile_url)
                            resolution_prefetch_urls.append(normalized_profile_url)
                    if worker_prefetch_enabled:
                        if candidate_id and candidate_id in resolution_candidate_ids and normalized_profile_url in cached_resolution_prefetch_url_set:
                            continue
                        if candidate_id and candidate_id in resolution_candidate_ids:
                            priority_dispatch_urls.append(normalized_profile_url)
                            continue
                        pending_dispatch_urls.append(normalized_profile_url)

            if worker_prefetch_enabled:
                priority_dispatch_chunks = _balanced_chunk_strings(priority_dispatch_urls, priority_prefetch_batch_size)
                pending_dispatch_chunks = _balanced_chunk_strings(pending_dispatch_urls, default_prefetch_batch_size)
                dispatch_specs = [
                    (chunk_index, dispatch_chunk)
                    for chunk_index, dispatch_chunk in enumerate(
                        [*priority_dispatch_chunks, *pending_dispatch_chunks],
                        start=1,
                    )
                    if dispatch_chunk
                ]
                if dispatch_specs:
                    dispatch_source_shards_by_url = {
                        profile_url: sorted(list(prefetch_source_shards_by_url.get(profile_url) or set()))
                        for profile_url in {
                            str(profile_url or "").strip()
                            for _, chunk in dispatch_specs
                            for profile_url in list(chunk or [])
                            if str(profile_url or "").strip()
                        }
                    }
                    prefetch_dispatch_window = _recommended_harvest_profile_live_fetch_window(
                        sum(len(chunk) for _, chunk in dispatch_specs),
                        source_shards_by_url=dispatch_source_shards_by_url,
                    )
                    prefetch_submit_workers = resolved_harvest_prefetch_submit_workers(
                        runtime_tuning_context,
                        chunk_count=len(dispatch_specs),
                        default=int(prefetch_dispatch_window.get("max_workers") or 1),
                    )
                    dispatch_results: list[dict[str, Any]] = []

                    def _merge_dispatch_result(result: dict[str, Any]) -> None:
                        nonlocal prefetched_harvest_profiles
                        nonlocal queued_harvest_worker_count
                        prefetched_harvest_profiles.update(
                            {
                                str(profile_url or "").strip(): dict(payload or {})
                                for profile_url, payload in dict(result.get("cached_profiles") or {}).items()
                                if str(profile_url or "").strip()
                            }
                        )
                        queued_urls = [
                            str(profile_url or "").strip()
                            for profile_url in list(result.get("queued_urls") or [])
                            if str(profile_url or "").strip()
                        ]
                        if queued_urls:
                            queued_harvest_worker_count += 1
                            background_prefetch_urls.update(queued_urls)
                        failed_urls = [
                            str(profile_url or "").strip()
                            for profile_url in list(result.get("failed_urls") or [])
                            if str(profile_url or "").strip()
                        ]
                        if failed_urls:
                            background_prefetch_urls.update(failed_urls)
                        error_message = str(result.get("error_message") or "").strip()
                        if error_message:
                            errors.append(f"harvest_profile_prefetch:{error_message}")
                        summary_path_value = str(result.get("summary_path") or "").strip()
                        if summary_path_value:
                            queue_artifact_paths.append(summary_path_value)

                    if prefetch_submit_workers <= 1 or len(dispatch_specs) <= 1:
                        for chunk_index, dispatch_chunk in dispatch_specs:
                            dispatch_results.append(
                                _dispatch_prefetch_chunk(
                                    chunk_index=chunk_index,
                                    profile_url_chunk=dispatch_chunk,
                                )
                            )
                    else:
                        first_exception: Exception | None = None
                        with ThreadPoolExecutor(
                            max_workers=prefetch_submit_workers,
                            thread_name_prefix="harvest-prefetch-dispatch",
                        ) as executor:
                            dispatch_iter = iter(dispatch_specs)
                            future_to_spec: dict[Any, tuple[int, list[str]]] = {}

                            def _submit_next_dispatch() -> bool:
                                try:
                                    next_chunk_index, next_dispatch_chunk = next(dispatch_iter)
                                except StopIteration:
                                    return False
                                future = executor.submit(
                                    _dispatch_prefetch_chunk,
                                    chunk_index=next_chunk_index,
                                    profile_url_chunk=next_dispatch_chunk,
                                )
                                future_to_spec[future] = (next_chunk_index, next_dispatch_chunk)
                                return True

                            for _ in range(prefetch_submit_workers):
                                if not _submit_next_dispatch():
                                    break

                            while future_to_spec:
                                done, _ = wait(set(future_to_spec.keys()), return_when=FIRST_COMPLETED)
                                for future in done:
                                    chunk_index, dispatch_chunk = future_to_spec.pop(future)
                                    try:
                                        dispatch_results.append(dict(future.result() or {}))
                                    except Exception as exc:
                                        if first_exception is None:
                                            first_exception = exc
                                        errors.append(
                                            "harvest_profile_prefetch:"
                                            f"chunk_{chunk_index}_failed:{','.join(dispatch_chunk)}:{exc}"
                                        )
                                    _submit_next_dispatch()
                        if first_exception is not None:
                            raise first_exception

                    for result in sorted(
                        dispatch_results,
                        key=lambda item: int(item.get("chunk_index") or 0),
                    ):
                        _merge_dispatch_result(result)

            if (
                known_profile_urls
                and worker_prefetch_enabled
            ):
                if queue_artifact_paths:
                    if len(queue_artifact_paths) == 1:
                        artifact_paths["harvest_profile_batch_queue"] = queue_artifact_paths[0]
                    else:
                        for index, path_value in enumerate(queue_artifact_paths, start=1):
                            artifact_paths[f"harvest_profile_batch_queue_{index:02d}"] = path_value
                if full_roster_profile_prefetch and queued_harvest_worker_count > 0:
                    return MultiSourceEnrichmentResult(
                        candidates=sorted(list(candidate_map.values()), key=lambda item: item.display_name),
                        evidence=evidence,
                        artifact_paths=artifact_paths,
                        errors=errors,
                        queued_harvest_worker_count=queued_harvest_worker_count,
                        stop_reason="queued_background_harvest",
                    )
                if queued_harvest_worker_count == 0:
                    prefetched_harvest_profiles = self.harvest_profile_connector.fetch_profiles_by_urls(
                        resolution_prefetch_urls,
                        snapshot_dir,
                        asset_logger=logger,
                        allow_shared_provider_cache=allow_shared_provider_cache,
                    )
                elif not full_roster_profile_prefetch:
                    background_prefetch_urls.update(resolution_prefetch_urls)
        if linkedin_stage_enabled:
            search_inputs: list[Candidate] = []
            for candidate in prioritized[:slug_resolution_limit]:
                verified, profile_fetch_count = self._resolve_candidate_with_known_refs(
                    candidate,
                    identity,
                    snapshot_dir,
                    profile_fetch_count,
                    profile_detail_limit,
                    candidate_map,
                    resolved_profiles,
                    evidence,
                    asset_logger=logger,
                    prefetched_harvest_profiles=prefetched_harvest_profiles,
                    background_prefetch_urls=background_prefetch_urls,
                    allow_shared_provider_cache=allow_shared_provider_cache,
                    source_job_id=job_id,
                )
                if not verified:
                    search_inputs.append(candidate)

            search_results = self.slug_resolver.resolve(search_inputs, identity, snapshot_dir, asset_logger=logger)
            if search_results["summary_path"]:
                artifact_paths["slug_search_summary"] = str(search_results["summary_path"])
            errors.extend(search_results.get("errors", []))

            for item in search_results["results"]:
                candidate = candidate_map.get(item["candidate_key"])
                if candidate is None:
                    continue

                verified = False
                for slug in item["slugs"]:
                    if profile_fetch_count >= profile_detail_limit:
                        break
                    profile_fetch_count += 1
                    profile = self.profile_connector.fetch_profile(slug, snapshot_dir, asset_logger=logger)
                    if profile is None:
                        continue
                    if not _profile_matches_candidate(profile["parsed"], candidate, identity, model_client=self.model_client):
                        continue

                    merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                        candidate,
                        profile["parsed"],
                        profile["raw_path"],
                        profile["account_id"],
                        slug,
                        identity,
                        model_client=self.model_client,
                        resolution_source="slug_search",
                    )
                    candidate_map[item["candidate_key"]] = merged_candidate
                    resolved_profiles.append(resolved_profile)
                    evidence.extend(profile_evidence)
                    verified = True
                    break

                if not verified:
                    unresolved_candidates.append(
                        {
                            "candidate_id": candidate.candidate_id,
                            "display_name": candidate.display_name,
                            "attempted_slugs": item["slugs"],
                            "query_summaries": item["queries"],
                        }
                    )

        publication_result = {
            "artifact_paths": {},
            "errors": [],
            "matched_candidates": [],
            "lead_candidates": [],
            "scholar_coauthor_prospects": [],
            "evidence": [],
            "publication_matches": [],
            "coauthor_edges": [],
        }
        if public_web_stage_enabled:
            publication_result = self.publication_connector.enrich(
                identity=identity,
                snapshot_dir=snapshot_dir,
                candidates=list(candidate_map.values()),
                asset_logger=logger,
                max_publications=job_request.publication_scan_limit,
                max_leads=job_request.publication_lead_limit,
                request_payload=request_payload or job_request.to_record(),
                plan_payload=plan_payload or {},
                existing_evidence=evidence,
            )
            artifact_paths.update(publication_result["artifact_paths"])
            errors.extend(publication_result.get("errors", []))

            for candidate in publication_result["matched_candidates"]:
                candidate_map[_candidate_key(candidate)] = candidate
            for lead_candidate in publication_result["lead_candidates"]:
                key = _candidate_key(lead_candidate)
                existing = candidate_map.get(key)
                candidate_map[key] = merge_candidate(existing, lead_candidate) if existing else lead_candidate
        scholar_coauthor_prospects = list(publication_result.get("scholar_coauthor_prospects") or [])

        lead_candidates_to_resolve = _prioritize_candidates(publication_result["lead_candidates"]) if linkedin_stage_enabled else []
        remaining_profile_budget = max(profile_detail_limit - profile_fetch_count, 0)
        if linkedin_stage_enabled and remaining_profile_budget > 0 and lead_candidates_to_resolve:
            lead_search_limit = min(remaining_profile_budget, len(lead_candidates_to_resolve))
            lead_search_results = self.slug_resolver.resolve(
                lead_candidates_to_resolve[:lead_search_limit],
                identity,
                snapshot_dir,
                asset_logger=logger,
            )
            errors.extend(lead_search_results.get("errors", []))
            for item in lead_search_results["results"]:
                lead_candidate = candidate_map.get(item["candidate_key"])
                if lead_candidate is None:
                    continue
                verified = False
                for slug in item["slugs"]:
                    if profile_fetch_count >= profile_detail_limit:
                        break
                    profile_fetch_count += 1
                    profile = self.profile_connector.fetch_profile(slug, snapshot_dir, asset_logger=logger)
                    if profile is None or not _profile_matches_candidate(
                        profile["parsed"],
                        lead_candidate,
                        identity,
                        model_client=self.model_client,
                    ):
                        continue
                    merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                        lead_candidate,
                        profile["parsed"],
                        profile["raw_path"],
                        profile["account_id"],
                        slug,
                        identity,
                        model_client=self.model_client,
                        resolution_source="publication_lead_second_pass",
                    )
                    candidate_map[item["candidate_key"]] = merged_candidate
                    resolved_profiles.append(resolved_profile)
                    evidence.extend(profile_evidence)
                    verified = True
                    break
                if not verified:
                    unresolved_candidates.append(
                        {
                            "candidate_id": lead_candidate.candidate_id,
                            "display_name": lead_candidate.display_name,
                            "attempted_slugs": item["slugs"],
                            "query_summaries": item["queries"],
                            "resolution_source": "publication_lead_second_pass",
                        }
                    )

        exploration_targets = _exploration_targets(list(candidate_map.values()), unresolved_candidates) if public_web_stage_enabled else []
        if public_web_stage_enabled and job_request.exploration_limit > 0 and exploration_targets:
            runtime_timing_overrides = _runtime_timing_overrides_from_request_payload(request_payload or job_request.to_record())
            exploration = self.exploratory_enricher.enrich(
                snapshot_dir,
                exploration_targets,
                target_company=identity.canonical_name,
                max_candidates=job_request.exploration_limit,
                asset_logger=logger,
                job_id=job_id,
                request_payload=request_payload or job_request.to_record(),
                plan_payload=plan_payload or {},
                runtime_mode=runtime_mode,
                parallel_workers=resolved_parallel_exploration_workers(
                    runtime_timing_overrides,
                    cost_policy=effective_cost_policy,
                    default=parallel_exploration_workers,
                ),
            )
            artifact_paths.update(exploration.artifact_paths)
            errors.extend(exploration.errors)
            evidence.extend(exploration.evidence)
            for explored_candidate in exploration.candidates:
                candidate_map[_candidate_key(explored_candidate)] = merge_candidate(
                    candidate_map.get(_candidate_key(explored_candidate), explored_candidate),
                    explored_candidate,
                )
            queued_exploration_count += int(getattr(exploration, "queued_candidate_count", 0) or 0)
            post_exploration_candidates = _prioritize_candidates(exploration.candidates)
            for explored_candidate in post_exploration_candidates:
                if profile_fetch_count >= profile_detail_limit:
                    break
                if not linkedin_stage_enabled:
                    break
                if not _should_attempt_known_profile_resolution_after_exploration(explored_candidate, identity):
                    continue
                verified, profile_fetch_count = self._resolve_candidate_with_known_refs(
                    explored_candidate,
                    identity,
                    snapshot_dir,
                    profile_fetch_count,
                    profile_detail_limit,
                    candidate_map,
                    resolved_profiles,
                    evidence,
                    asset_logger=logger,
                    allow_shared_provider_cache=allow_shared_provider_cache,
                    source_job_id=job_id,
                )
                if verified:
                    _drop_unresolved_candidate(unresolved_candidates, explored_candidate.candidate_id)
                elif _candidate_key(explored_candidate) not in {_candidate_key(item) for item in lead_candidates_to_resolve}:
                    unresolved_candidates.append(
                        {
                            "candidate_id": explored_candidate.candidate_id,
                            "display_name": explored_candidate.display_name,
                            "attempted_slugs": [],
                            "query_summaries": [],
                            "resolution_source": "exploration_follow_up",
                        }
                    )

        scholar_coauthor_follow_up_limit = max(int(job_request.scholar_coauthor_follow_up_limit or 0), 0)
        if public_web_stage_enabled and linkedin_stage_enabled and scholar_coauthor_prospects and scholar_coauthor_follow_up_limit > 0:
            (
                profile_fetch_count,
                scholar_coauthor_follow_up_summary_path,
                scholar_coauthor_errors,
                scholar_coauthor_queued_count,
            ) = self._follow_up_roster_anchored_scholar_coauthor_prospects(
                prospects=scholar_coauthor_prospects,
                identity=identity,
                snapshot_dir=snapshot_dir,
                candidate_map=candidate_map,
                resolved_profiles=resolved_profiles,
                evidence=evidence,
                profile_fetch_count=profile_fetch_count,
                profile_detail_limit=profile_detail_limit,
                exploration_limit=scholar_coauthor_follow_up_limit,
                asset_logger=logger,
                job_id=job_id,
                request_payload=request_payload or job_request.to_record(),
                plan_payload=plan_payload or {},
                runtime_mode=runtime_mode,
                allow_shared_provider_cache=allow_shared_provider_cache,
            )
            errors.extend(scholar_coauthor_errors)
            if scholar_coauthor_follow_up_summary_path is not None:
                artifact_paths["scholar_coauthor_follow_up"] = str(scholar_coauthor_follow_up_summary_path)
            queued_exploration_count += scholar_coauthor_queued_count

        remaining_profile_budget = max(profile_detail_limit - profile_fetch_count, 0)
        remaining_leads_to_resolve = [
            item
            for item in _prioritize_candidates(
                [candidate_map.get(_candidate_key(candidate), candidate) for candidate in lead_candidates_to_resolve]
            )
            if item.category == "lead"
        ]
        gated_leads_to_resolve = remaining_leads_to_resolve
        if public_web_stage_enabled and remaining_leads_to_resolve:
            gated_leads_to_resolve, publication_lead_gate_summary_path = self._gate_publication_leads_after_exploration(
                lead_candidates=remaining_leads_to_resolve,
                identity=identity,
                snapshot_dir=snapshot_dir,
                candidate_map=candidate_map,
                unresolved_candidates=unresolved_candidates,
                asset_logger=logger,
                allow_targeted_name_search=bool(effective_cost_policy.get("allow_targeted_name_search_api", False)),
            )
            if publication_lead_gate_summary_path is not None:
                artifact_paths["publication_lead_public_web_gate"] = str(publication_lead_gate_summary_path)
        if (
            public_web_stage_enabled
            and linkedin_stage_enabled
            and
            bool(effective_cost_policy.get("allow_targeted_name_search_api", False))
            and remaining_profile_budget > 0
            and gated_leads_to_resolve
        ):
            _, targeted_resolution_summary_path = self._resolve_publication_leads_with_harvest_search(
                lead_candidates=gated_leads_to_resolve,
                identity=identity,
                snapshot_dir=snapshot_dir,
                remaining_profile_budget=remaining_profile_budget,
                candidate_map=candidate_map,
                resolved_profiles=resolved_profiles,
                unresolved_candidates=unresolved_candidates,
                evidence=evidence,
                asset_logger=logger,
                allow_shared_provider_cache=allow_shared_provider_cache,
            )
            if targeted_resolution_summary_path is not None:
                artifact_paths["publication_lead_targeted_resolution"] = str(targeted_resolution_summary_path)

        evidence.extend(publication_result["evidence"])
        final_candidates = list(candidate_map.values())
        stop_reason = ""
        if queued_exploration_count > 0:
            stop_reason = "queued_background_exploration"
        elif queued_harvest_worker_count > 0:
            stop_reason = "queued_background_harvest"
        return MultiSourceEnrichmentResult(
            candidates=sorted(final_candidates, key=lambda item: item.display_name),
            evidence=evidence,
            resolved_profiles=resolved_profiles,
            unresolved_candidates=unresolved_candidates,
            publication_matches=publication_result["publication_matches"],
            lead_candidates=publication_result["lead_candidates"],
            coauthor_edges=publication_result["coauthor_edges"],
            artifact_paths=artifact_paths,
            errors=errors,
            queued_harvest_worker_count=queued_harvest_worker_count,
            queued_exploration_count=queued_exploration_count,
            stop_reason=stop_reason,
        )

    def _hydrate_cached_prefetch_profiles(
        self,
        profile_urls: list[str],
        snapshot_dir: Path,
        *,
        source_shards_by_url: dict[str, list[str]] | None = None,
        source_jobs: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        normalized_urls: list[str] = []
        for profile_url in profile_urls:
            normalized_profile_url = str(profile_url or "").strip()
            if normalized_profile_url and normalized_profile_url not in normalized_urls:
                normalized_urls.append(normalized_profile_url)
        if not normalized_urls:
            return {}

        normalized_source_jobs = [str(item or "").strip() for item in list(source_jobs or []) if str(item or "").strip()]
        source_shards_by_url = {
            str(profile_url or "").strip(): list(values or [])
            for profile_url, values in dict(source_shards_by_url or {}).items()
            if str(profile_url or "").strip()
        }
        registry_entries: dict[str, dict[str, Any]] = {}
        if self.store is not None:
            registry_entries = self.store.get_linkedin_profile_registry_bulk(normalized_urls)

        cached_profiles: dict[str, dict[str, Any]] = {}
        for profile_url in normalized_urls:
            registry_entry = {}
            registry_key = profile_url
            if self.store is not None:
                registry_key = self.store.normalize_linkedin_profile_url(profile_url)
                registry_entry = dict(registry_entries.get(registry_key) or {})
            cached = _load_harvest_profile_payload_from_registry_or_snapshot(
                registry_entry=registry_entry,
                snapshot_dir=snapshot_dir,
                profile_url=profile_url,
                normalized_profile_key=registry_key,
            )
            if cached is None:
                continue
            cached_profiles[profile_url] = cached
            if self.store is None:
                continue
            alias_metadata = _profile_registry_alias_metadata(profile_url, cached)
            self.store.mark_linkedin_profile_registry_fetched(
                profile_url,
                raw_path=str(cached.get("raw_path") or ""),
                source_shards=list(source_shards_by_url.get(profile_url) or []),
                source_jobs=normalized_source_jobs,
                alias_urls=list(alias_metadata.get("alias_urls") or []),
                raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or profile_url),
                sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                snapshot_dir=str(snapshot_dir),
            )
        return cached_profiles

    def queue_background_profile_prefetch(
        self,
        *,
        candidates: list[Candidate],
        snapshot_dir: Path,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool,
        priority: bool = False,
    ) -> dict[str, Any]:
        if self.harvest_profile_connector is None or self.worker_runtime is None or not job_id:
            return {"status": "skipped", "reason": "worker_prefetch_unavailable"}

        normalized_urls: list[str] = []
        seen_urls: set[str] = set()
        source_shards_by_url: dict[str, list[str]] = {}
        for candidate in list(candidates or []):
            candidate_source_shards = _profile_registry_sources_for_candidate(candidate)
            for profile_url in _candidate_profile_urls(candidate):
                normalized_profile_url = str(profile_url or "").strip()
                if not normalized_profile_url:
                    continue
                if normalized_profile_url not in seen_urls:
                    seen_urls.add(normalized_profile_url)
                    normalized_urls.append(normalized_profile_url)
                existing_shards = set(source_shards_by_url.get(normalized_profile_url) or [])
                existing_shards.update(candidate_source_shards)
                source_shards_by_url[normalized_profile_url] = sorted(existing_shards)
        if not normalized_urls:
            return {"status": "skipped", "reason": "no_profile_urls"}

        cached_profiles = self._hydrate_cached_prefetch_profiles(
            normalized_urls,
            snapshot_dir,
            source_shards_by_url=source_shards_by_url,
            source_jobs=[job_id],
        )
        dispatch_urls = [
            profile_url
            for profile_url in normalized_urls
            if str(profile_url or "").strip() and str(profile_url or "").strip() not in cached_profiles
        ]
        if not dispatch_urls:
            return {
                "status": "completed",
                "reason": "reused_local_raw_cache",
                "requested_url_count": len(normalized_urls),
                "dispatched_url_count": 0,
                "cached_profile_count": len(cached_profiles),
                "queued_worker_count": 0,
                "summary_paths": [],
            }

        runtime_tuning_context = _runtime_tuning_context_from_request_payload(dict(request_payload or {}))
        batch_size = _recommended_harvest_profile_prefetch_batch_size(len(normalized_urls), priority=priority)
        dispatch_chunks = _balanced_chunk_strings(dispatch_urls, batch_size)
        if not dispatch_chunks:
            return {
                "status": "skipped",
                "reason": "no_dispatch_chunks",
                "requested_url_count": len(normalized_urls),
                "dispatched_url_count": 0,
                "cached_profile_count": len(cached_profiles),
                "queued_worker_count": 0,
                "summary_paths": [],
            }

        dispatch_source_shards_by_url = {
            profile_url: list(source_shards_by_url.get(profile_url) or [])
            for profile_url in dispatch_urls
        }
        prefetch_dispatch_window = _recommended_harvest_profile_live_fetch_window(
            sum(len(chunk) for chunk in dispatch_chunks),
            source_shards_by_url=dispatch_source_shards_by_url,
        )
        prefetch_submit_workers = resolved_harvest_prefetch_submit_workers(
            runtime_tuning_context,
            chunk_count=len(dispatch_chunks),
            default=int(prefetch_dispatch_window.get("max_workers") or 1),
        )
        dispatch_results: list[dict[str, Any]] = []

        def _dispatch_prefetch_chunk(
            *,
            chunk_index: int,
            profile_url_chunk: list[str],
        ) -> dict[str, Any]:
            if not profile_url_chunk:
                return {
                    "chunk_index": chunk_index,
                    "queued_urls": [],
                    "failed_urls": [],
                    "error_message": "",
                    "summary_path": "",
                }
            harvest_worker = self._execute_harvest_profile_batch_worker(
                profile_urls=profile_url_chunk,
                snapshot_dir=snapshot_dir,
                job_id=job_id,
                request_payload=request_payload,
                plan_payload=plan_payload,
                runtime_mode=runtime_mode,
                allow_shared_provider_cache=allow_shared_provider_cache,
            )
            worker_summary = dict(harvest_worker.get("summary") or {})
            queued_urls: list[str] = []
            failed_urls: list[str] = []
            error_message = ""
            worker_status = str(harvest_worker.get("worker_status") or "")
            if worker_status == "queued":
                queued_urls = [
                    str(profile_url or "").strip()
                    for profile_url in list(
                        worker_summary.get("queued_urls")
                        or worker_summary.get("requested_urls")
                        or []
                    )
                    if str(profile_url or "").strip()
                ]
            elif worker_status == "failed":
                failed_urls = [
                    str(profile_url or "").strip()
                    for profile_url in list(
                        harvest_worker.get("failed_urls")
                        or worker_summary.get("failed_urls")
                        or worker_summary.get("queued_urls")
                        or []
                    )
                    if str(profile_url or "").strip()
                ]
                error_message = str(worker_summary.get("message") or "").strip()
            return {
                "chunk_index": chunk_index,
                "queued_urls": queued_urls,
                "failed_urls": failed_urls,
                "error_message": error_message,
                "summary_path": str(worker_summary.get("summary_path") or "").strip(),
            }

        if prefetch_submit_workers <= 1 or len(dispatch_chunks) <= 1:
            for chunk_index, dispatch_chunk in enumerate(dispatch_chunks, start=1):
                dispatch_results.append(
                    _dispatch_prefetch_chunk(
                        chunk_index=chunk_index,
                        profile_url_chunk=dispatch_chunk,
                    )
                )
        else:
            first_exception: Exception | None = None
            with ThreadPoolExecutor(
                max_workers=prefetch_submit_workers,
                thread_name_prefix="harvest-prefetch-dispatch",
            ) as executor:
                dispatch_iter = iter(list(enumerate(dispatch_chunks, start=1)))
                future_to_spec: dict[Any, tuple[int, list[str]]] = {}

                def _submit_next_dispatch() -> bool:
                    try:
                        next_chunk_index, next_dispatch_chunk = next(dispatch_iter)
                    except StopIteration:
                        return False
                    future = executor.submit(
                        _dispatch_prefetch_chunk,
                        chunk_index=next_chunk_index,
                        profile_url_chunk=next_dispatch_chunk,
                    )
                    future_to_spec[future] = (next_chunk_index, next_dispatch_chunk)
                    return True

                for _ in range(prefetch_submit_workers):
                    if not _submit_next_dispatch():
                        break

                while future_to_spec:
                    done, _ = wait(set(future_to_spec.keys()), return_when=FIRST_COMPLETED)
                    for future in done:
                        chunk_index, dispatch_chunk = future_to_spec.pop(future)
                        try:
                            dispatch_results.append(dict(future.result() or {}))
                        except Exception as exc:
                            if first_exception is None:
                                first_exception = exc
                            dispatch_results.append(
                                {
                                    "chunk_index": chunk_index,
                                    "queued_urls": [],
                                    "failed_urls": list(dispatch_chunk),
                                    "error_message": str(exc),
                                    "summary_path": "",
                                }
                            )
                        _submit_next_dispatch()
            if first_exception is not None:
                raise first_exception

        queued_urls: list[str] = []
        failed_urls: list[str] = []
        summary_paths: list[str] = []
        errors: list[str] = []
        queued_worker_count = 0
        for result in sorted(dispatch_results, key=lambda item: int(item.get("chunk_index") or 0)):
            chunk_queued_urls = [
                str(profile_url or "").strip()
                for profile_url in list(result.get("queued_urls") or [])
                if str(profile_url or "").strip()
            ]
            if chunk_queued_urls:
                queued_worker_count += 1
                for profile_url in chunk_queued_urls:
                    if profile_url not in queued_urls:
                        queued_urls.append(profile_url)
            for profile_url in list(result.get("failed_urls") or []):
                normalized_profile_url = str(profile_url or "").strip()
                if normalized_profile_url and normalized_profile_url not in failed_urls:
                    failed_urls.append(normalized_profile_url)
            error_message = str(result.get("error_message") or "").strip()
            if error_message:
                errors.append(error_message)
            summary_path_value = str(result.get("summary_path") or "").strip()
            if summary_path_value:
                summary_paths.append(summary_path_value)

        if failed_urls and self.store is not None:
            for profile_url in failed_urls:
                self.store.mark_linkedin_profile_registry_failed(
                    profile_url,
                    error="background_prefetch_dispatch_failed",
                    retryable=True,
                    source_shards=list(source_shards_by_url.get(profile_url) or []),
                    source_jobs=[job_id],
                    snapshot_dir=str(snapshot_dir),
                )

        return {
            "status": "queued" if queued_worker_count > 0 else "completed",
            "requested_url_count": len(normalized_urls),
            "dispatched_url_count": len(dispatch_urls),
            "cached_profile_count": len(cached_profiles),
            "queued_worker_count": queued_worker_count,
            "queued_urls": queued_urls,
            "failed_urls": failed_urls,
            "summary_paths": summary_paths,
            "errors": errors,
        }

    def _execute_harvest_profile_batch_worker(
        self,
        *,
        profile_urls: list[str],
        snapshot_dir: Path,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool,
    ) -> dict[str, Any]:
        if self.harvest_profile_connector is None or self.worker_runtime is None or not job_id:
            return {"worker_status": "skipped"}

        normalized_urls: list[str] = []
        for profile_url in profile_urls:
            value = str(profile_url or "").strip()
            if value and value not in normalized_urls:
                normalized_urls.append(value)
        if not normalized_urls:
            return {"worker_status": "completed", "summary": {"requested_url_count": 0, "status": "completed"}}

        cached_profiles = self._hydrate_cached_prefetch_profiles(
            normalized_urls,
            snapshot_dir,
            source_jobs=[job_id] if str(job_id or "").strip() else [],
        )
        dispatch_urls = [
            profile_url
            for profile_url in normalized_urls
            if str(profile_url or "").strip() and str(profile_url or "").strip() not in cached_profiles
        ]
        if not dispatch_urls:
            return {
                "worker_status": "completed",
                "summary": {
                    "logical_name": "harvest_profile_scraper_batch",
                    "requested_url_count": len(normalized_urls),
                    "requested_urls": list(normalized_urls),
                    "queued_urls": [],
                    "dispatched_url_count": 0,
                    "reused_cached_count": len(cached_profiles),
                    "status": "completed",
                    "message": "reused_local_raw_cache",
                },
                "cached_profiles": cached_profiles,
            }

        payload_hash = sha1(
            json.dumps(sorted(dispatch_urls), ensure_ascii=False).encode("utf-8")
        ).hexdigest()[:16]
        worker_handle = self.worker_runtime.begin_worker(
            job_id=job_id,
            request=JobRequest.from_payload(request_payload),
            plan_payload=plan_payload,
            runtime_mode=runtime_mode,
            lane_id="enrichment_specialist",
            worker_key=f"harvest_profile_batch::{payload_hash}",
            stage="enriching",
            span_name=f"harvest_profile_batch:{payload_hash}",
            budget_payload={"requested_url_count": len(dispatch_urls)},
            input_payload={"profile_urls": list(dispatch_urls)},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": list(dispatch_urls),
                "request_payload": request_payload,
                "plan_payload": plan_payload,
                "runtime_mode": runtime_mode,
                "allow_shared_provider_cache": allow_shared_provider_cache,
            },
            handoff_from_lane="acquisition_specialist",
        )
        existing = self.worker_runtime.get_worker(worker_handle.worker_id) or {}
        checkpoint = dict(existing.get("checkpoint") or {})
        output_payload = dict(existing.get("output") or {})
        if str(existing.get("status") or "") == "completed" and output_payload:
            self.worker_runtime.complete_worker(
                worker_handle,
                status="completed",
                checkpoint_payload=checkpoint,
                output_payload=output_payload,
                handoff_to_lane="enrichment_specialist",
            )
            return {
                "worker_status": "completed",
                "summary": dict(output_payload.get("summary") or {}),
                "daemon_action": "reused_output",
            }

        logger = AssetLogger(snapshot_dir)
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        artifact_default_path = harvest_dir / f"harvest_profile_batch_{payload_hash}.queue.json"
        runtime_timing_overrides = _runtime_timing_overrides_from_request_payload(request_payload)
        artifact_paths = {
            str(key): str(value)
            for key, value in dict(checkpoint.get("artifact_paths") or {}).items()
            if str(key).strip()
        }
        summary_path = harvest_dir / f"harvest_profile_batch_{payload_hash}.queue_summary.json"

        try:
            execution = self.harvest_profile_connector.execute_batch_with_checkpoint(
                dispatch_urls,
                snapshot_dir,
                checkpoint=checkpoint,
                allow_shared_provider_cache=allow_shared_provider_cache,
                runtime_timing_overrides=runtime_timing_overrides,
            )
        except Exception as exc:
            error_message = str(exc)
            failure_summary = {
                "logical_name": "harvest_profile_scraper_batch",
                "requested_url_count": len(normalized_urls),
                "requested_urls": list(normalized_urls),
                "failed_urls": list(dispatch_urls),
                "dispatched_url_count": len(dispatch_urls),
                "reused_cached_count": len(cached_profiles),
                "status": "failed",
                "message": error_message,
                "artifact_paths": artifact_paths,
                "retryable": True,
            }
            logger.write_json(
                summary_path,
                failure_summary,
                asset_type="harvest_profile_batch_queue_summary",
                source_kind="harvest_profile_scraper",
                is_raw_asset=False,
                model_safe=True,
            )
            updated_checkpoint = {
                **checkpoint,
                "artifact_paths": artifact_paths,
                "summary_path": str(summary_path),
                "stage": "failed",
                "last_error": error_message,
            }
            updated_output = {"summary": {**failure_summary, "summary_path": str(summary_path)}}
            if self.store is not None:
                for profile_url in dispatch_urls:
                    self.store.mark_linkedin_profile_registry_failed(
                        str(profile_url or ""),
                        error=error_message,
                        retryable=True,
                        source_shards=["enrichment_background_prefetch"],
                        source_jobs=[job_id] if str(job_id or "").strip() else [],
                        snapshot_dir=str(snapshot_dir),
                    )
            self.worker_runtime.complete_worker(
                worker_handle,
                status="failed",
                checkpoint_payload=updated_checkpoint,
                output_payload=updated_output,
            )
            return {
                "worker_status": "failed",
                "summary": updated_output["summary"],
                "cached_profiles": cached_profiles,
                "failed_urls": list(dispatch_urls),
            }

        for artifact in list(execution.artifacts or []):
            artifact_path = write_harvest_execution_artifact(
                logger=logger,
                artifact=artifact,
                default_path=artifact_default_path,
                asset_type="harvest_profile_batch_queue_payload",
                source_kind="harvest_profile_scraper",
                metadata={
                    "logical_name": execution.logical_name,
                    "requested_url_count": len(dispatch_urls),
                    "payload_hash": payload_hash,
                },
            )
            artifact_paths[str(artifact.label)] = str(artifact_path)
        summary = {
            "logical_name": execution.logical_name,
            "requested_url_count": len(normalized_urls),
            "requested_urls": list(normalized_urls),
            "queued_urls": list(dispatch_urls),
            "dispatched_url_count": len(dispatch_urls),
            "reused_cached_count": len(cached_profiles),
            "status": "queued" if execution.pending else "completed",
            "message": str(execution.message or ""),
            "run_id": str(execution.checkpoint.get("run_id") or ""),
            "dataset_id": str(execution.checkpoint.get("dataset_id") or ""),
            "artifact_paths": artifact_paths,
        }
        logger.write_json(
            summary_path,
            summary,
            asset_type="harvest_profile_batch_queue_summary",
            source_kind="harvest_profile_scraper",
            is_raw_asset=False,
            model_safe=True,
        )
        updated_checkpoint = {
            **dict(execution.checkpoint or {}),
            "artifact_paths": artifact_paths,
            "summary_path": str(summary_path),
            "stage": "waiting_remote_harvest" if execution.pending else "completed",
        }
        updated_output = {"summary": {**summary, "summary_path": str(summary_path)}}
        if execution.pending:
            if self.store is not None:
                run_id = str(execution.checkpoint.get("run_id") or "")
                dataset_id = str(execution.checkpoint.get("dataset_id") or "")
                for profile_url in dispatch_urls:
                    self.store.mark_linkedin_profile_registry_queued(
                        profile_url,
                        source_shards=["enrichment_background_prefetch"],
                        source_jobs=[job_id] if str(job_id or "").strip() else [],
                        run_id=run_id,
                        dataset_id=dataset_id,
                        snapshot_dir=str(snapshot_dir),
                    )
            self.worker_runtime.complete_worker(
                worker_handle,
                status="queued",
                checkpoint_payload=updated_checkpoint,
                output_payload=updated_output,
            )
            return {
                "worker_status": "queued",
                "summary": updated_output["summary"],
                "cached_profiles": cached_profiles,
            }

        persisted_profile_count = 0
        unresolved_urls: list[str] = []
        if execution.body is not None:
            persisted = self.harvest_profile_connector.persist_profiles_from_batch_body(
                dispatch_urls,
                execution.body,
                snapshot_dir,
                asset_logger=logger,
            )
            persisted_profile_count = len(dict(persisted.get("profiles") or {}))
            unresolved_urls = [str(item) for item in list(persisted.get("unresolved_urls") or []) if str(item).strip()]
            updated_output["persisted_profile_count"] = persisted_profile_count
            updated_output["unresolved_urls"] = unresolved_urls
            updated_output["summary"]["persisted_profile_count"] = persisted_profile_count
            updated_output["summary"]["unresolved_url_count"] = len(unresolved_urls)
            if self.store is not None:
                run_id = str(execution.checkpoint.get("run_id") or "")
                dataset_id = str(execution.checkpoint.get("dataset_id") or "")
                persisted_profiles = dict(persisted.get("profiles") or {})
                for profile_url, payload in persisted_profiles.items():
                    self.store.mark_linkedin_profile_registry_fetched(
                        str(profile_url or ""),
                        raw_path=str(dict(payload or {}).get("raw_path") or ""),
                        source_shards=["enrichment_background_prefetch"],
                        source_jobs=[job_id] if str(job_id or "").strip() else [],
                        run_id=run_id,
                        dataset_id=dataset_id,
                        snapshot_dir=str(snapshot_dir),
                    )
                for profile_url in unresolved_urls:
                    self.store.mark_linkedin_profile_registry_failed(
                        str(profile_url or ""),
                        error="background_prefetch_unresolved",
                        retryable=True,
                        source_shards=["enrichment_background_prefetch"],
                        source_jobs=[job_id] if str(job_id or "").strip() else [],
                        run_id=run_id,
                        dataset_id=dataset_id,
                        snapshot_dir=str(snapshot_dir),
                    )
        elif self.store is not None:
            run_id = str(execution.checkpoint.get("run_id") or "")
            dataset_id = str(execution.checkpoint.get("dataset_id") or "")
            for profile_url in dispatch_urls:
                self.store.mark_linkedin_profile_registry_failed(
                    str(profile_url or ""),
                    error="background_prefetch_empty_response",
                    retryable=True,
                    source_shards=["enrichment_background_prefetch"],
                    source_jobs=[job_id] if str(job_id or "").strip() else [],
                    run_id=run_id,
                    dataset_id=dataset_id,
                    snapshot_dir=str(snapshot_dir),
                )

        self.worker_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload=updated_checkpoint,
            output_payload=updated_output,
            handoff_to_lane="enrichment_specialist",
        )
        return {
            "worker_status": "completed",
            "summary": updated_output["summary"],
            "cached_profiles": cached_profiles,
        }

    def _fetch_harvest_profiles_for_urls(
        self,
        profile_urls: list[str],
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger,
        prefetched_harvest_profiles: dict[str, dict[str, Any]] | None = None,
        background_prefetch_urls: set[str] | None = None,
        allow_shared_provider_cache: bool = True,
        source_shards_by_url: dict[str, list[str]] | None = None,
        source_jobs: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        normalized_urls: list[str] = []
        for profile_url in profile_urls:
            normalized_profile_url = str(profile_url or "").strip()
            if normalized_profile_url and normalized_profile_url not in normalized_urls:
                normalized_urls.append(normalized_profile_url)
        if self.harvest_profile_connector is None or not normalized_urls:
            return {}

        pending_prefetch_urls = {
            str(item or "").strip()
            for item in list(background_prefetch_urls or set())
            if str(item or "").strip()
        }
        source_shards_by_url = {
            str(profile_url or "").strip(): list(values or [])
            for profile_url, values in dict(source_shards_by_url or {}).items()
            if str(profile_url or "").strip()
        }
        normalized_source_jobs = [str(item or "").strip() for item in list(source_jobs or []) if str(item or "").strip()]
        registry_entries: dict[str, dict[str, Any]] = {}
        if self.store is not None:
            registry_entries = self.store.get_linkedin_profile_registry_bulk(normalized_urls)
        fetched: dict[str, dict[str, Any]] = {}
        pending_urls: list[str] = []
        lease_owner = _profile_registry_lease_owner("enrichment")
        acquired_leases: dict[str, dict[str, Any]] = {}

        def _record_event(
            profile_url: str,
            *,
            event_type: str,
            event_status: str = "",
            detail: str = "",
            metadata: dict[str, Any] | None = None,
            duration_ms: int | None = None,
        ) -> None:
            if self.store is None:
                return
            self.store.record_linkedin_profile_registry_event(
                profile_url,
                event_type=event_type,
                event_status=event_status,
                detail=detail,
                metadata=metadata or {},
                duration_ms=duration_ms,
            )

        def _release_acquired_leases() -> None:
            if self.store is None:
                return
            for profile_url, lease_payload in list(acquired_leases.items()):
                self.store.release_linkedin_profile_registry_lease(
                    profile_url,
                    lease_owner=str(lease_payload.get("lease_owner") or lease_owner),
                    lease_token=str(lease_payload.get("lease_token") or ""),
                )
            acquired_leases.clear()

        def _wait_for_contended_fetches(contended_urls: dict[str, str]) -> None:
            if self.store is None or not contended_urls:
                return
            remaining = dict(contended_urls)
            deadline = time.monotonic() + PROFILE_REGISTRY_LEASE_WAIT_SECONDS
            while remaining and time.monotonic() < deadline:
                resolved_any = False
                for profile_url, normalized_registry_key in list(remaining.items()):
                    registry_entry = self.store.get_linkedin_profile_registry(profile_url) or {}
                    registry_status = str(registry_entry.get("status") or "").strip().lower()
                    cached = _load_harvest_profile_payload_from_registry_or_snapshot(
                        registry_entry=registry_entry,
                        snapshot_dir=snapshot_dir,
                        profile_url=profile_url,
                        normalized_profile_key=normalized_registry_key,
                    )
                    if cached is not None:
                        fetched[profile_url] = cached
                        _record_event(profile_url, event_type="cache_hit_lease_wait")
                        remaining.pop(profile_url, None)
                        resolved_any = True
                        continue
                    if registry_status == "unrecoverable":
                        remaining.pop(profile_url, None)
                        resolved_any = True
                if remaining and not resolved_any:
                    time.sleep(PROFILE_REGISTRY_LEASE_POLL_SECONDS)
            for profile_url in remaining:
                _record_event(profile_url, event_type="lease_contended_skip")

        contended_urls: dict[str, str] = {}
        for normalized_profile_url in normalized_urls:
            profile = None
            if prefetched_harvest_profiles is not None:
                profile = prefetched_harvest_profiles.get(normalized_profile_url)
            if profile is not None:
                fetched[normalized_profile_url] = profile
                if self.store is not None:
                    alias_metadata = _profile_registry_alias_metadata(normalized_profile_url, dict(profile or {}))
                    self.store.mark_linkedin_profile_registry_fetched(
                        normalized_profile_url,
                        raw_path=str(dict(profile or {}).get("raw_path") or ""),
                        source_shards=list(source_shards_by_url.get(normalized_profile_url) or []),
                        source_jobs=normalized_source_jobs,
                        alias_urls=list(alias_metadata.get("alias_urls") or []),
                        raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or normalized_profile_url),
                        sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                        snapshot_dir=str(snapshot_dir),
                    )
                    _record_event(normalized_profile_url, event_type="cache_hit_prefetched")
                continue
            if normalized_profile_url in pending_prefetch_urls:
                if self.store is not None:
                    self.store.mark_linkedin_profile_registry_queued(
                        normalized_profile_url,
                        source_shards=list(source_shards_by_url.get(normalized_profile_url) or []),
                        source_jobs=normalized_source_jobs,
                        snapshot_dir=str(snapshot_dir),
                    )
                    _record_event(normalized_profile_url, event_type="lookup_pending_prefetch")
                continue
            registry_key = normalized_profile_url
            registry_status = ""
            if self.store is not None:
                registry_key = self.store.normalize_linkedin_profile_url(normalized_profile_url)
                registry_entry = dict(registry_entries.get(registry_key) or {})
                registry_status = str(registry_entry.get("status") or "").strip().lower()
                _record_event(
                    normalized_profile_url,
                    event_type="lookup_attempt",
                    event_status=registry_status,
                    metadata={"source_shards": list(source_shards_by_url.get(normalized_profile_url) or [])},
                )
                cached = _load_harvest_profile_payload_from_registry_or_snapshot(
                    registry_entry=registry_entry,
                    snapshot_dir=snapshot_dir,
                    profile_url=normalized_profile_url,
                    normalized_profile_key=registry_key,
                )
                if cached is not None:
                    fetched[normalized_profile_url] = cached
                    alias_metadata = _profile_registry_alias_metadata(normalized_profile_url, cached)
                    self.store.mark_linkedin_profile_registry_fetched(
                        normalized_profile_url,
                        raw_path=str(cached.get("raw_path") or ""),
                        source_shards=list(source_shards_by_url.get(normalized_profile_url) or []),
                        source_jobs=normalized_source_jobs,
                        alias_urls=list(alias_metadata.get("alias_urls") or []),
                        raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or normalized_profile_url),
                        sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                        snapshot_dir=str(snapshot_dir),
                    )
                    _record_event(
                        normalized_profile_url,
                        event_type="cache_hit_registry" if registry_status == "fetched" else "cache_hit_local_raw",
                    )
                    continue
                if registry_status == "fetched":
                    self.store.mark_linkedin_profile_registry_failed(
                        normalized_profile_url,
                        error="registry_cached_raw_missing_or_invalid",
                        retryable=True,
                        source_shards=list(source_shards_by_url.get(normalized_profile_url) or []),
                        source_jobs=normalized_source_jobs,
                        snapshot_dir=str(snapshot_dir),
                    )
                if registry_status == "queued":
                    contended_urls[normalized_profile_url] = registry_key
                    continue
                if registry_status == "unrecoverable":
                    self.store.upsert_linkedin_profile_registry_sources(
                        normalized_profile_url,
                        source_shards=list(source_shards_by_url.get(normalized_profile_url) or []),
                        source_jobs=normalized_source_jobs,
                    )
                    _record_event(normalized_profile_url, event_type="cache_skip_unrecoverable", event_status="unrecoverable")
                    continue
                lease_payload = self.store.acquire_linkedin_profile_registry_lease(
                    normalized_profile_url,
                    lease_owner=lease_owner,
                    lease_seconds=PROFILE_REGISTRY_LEASE_SECONDS,
                )
                if bool(lease_payload.get("acquired")):
                    acquired_leases[normalized_profile_url] = lease_payload
                    pending_urls.append(normalized_profile_url)
                    continue
                contended_urls[normalized_profile_url] = registry_key
                continue
            pending_urls.append(normalized_profile_url)

        if pending_urls:
            live_fetch_window = _recommended_harvest_profile_live_fetch_window(
                len(pending_urls),
                source_shards_by_url=source_shards_by_url,
            )
            live_fetch_batch_size = int(live_fetch_window.get("batch_size") or 1)
            pending_chunks = _balanced_chunk_strings(pending_urls, live_fetch_batch_size)
            live_fetch_workers = min(len(pending_chunks), max(1, int(live_fetch_window.get("max_workers") or 1)))
            global_fetch_budget = resolved_harvest_profile_scrape_global_inflight({})
            fetched_lock = threading.Lock()

            def _record_live_profile_success(
                profile_url: str,
                payload: dict[str, Any],
                *,
                event_type: str,
            ) -> None:
                with fetched_lock:
                    fetched[profile_url] = payload
                if self.store is None:
                    return
                alias_metadata = _profile_registry_alias_metadata(profile_url, payload)
                before_entry = self.store.get_linkedin_profile_registry(profile_url) or {}
                retry_count_before = int(dict(before_entry).get("retry_count") or 0)
                fetched_entry = self.store.mark_linkedin_profile_registry_fetched(
                    profile_url,
                    raw_path=str(payload.get("raw_path") or ""),
                    source_shards=list(source_shards_by_url.get(profile_url) or []),
                    source_jobs=normalized_source_jobs,
                    alias_urls=list(alias_metadata.get("alias_urls") or []),
                    raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or profile_url),
                    sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                    snapshot_dir=str(snapshot_dir),
                )
                _record_event(
                    profile_url,
                    event_type=event_type,
                    metadata={"retry_count_before": retry_count_before},
                    duration_ms=_profile_registry_queue_duration_ms(dict(fetched_entry or {})),
                )

            def _record_live_batch_stream(batch_result: dict[str, Any]) -> None:
                profiles = {
                    str(profile_url or "").strip(): dict(profile or {})
                    for profile_url, profile in dict(batch_result.get("profiles") or {}).items()
                    if str(profile_url or "").strip() and isinstance(profile, dict)
                }
                for profile_url, profile in profiles.items():
                    _record_live_profile_success(
                        profile_url,
                        profile,
                        event_type="live_fetch_streaming_success",
                    )

            def _fetch_live_chunk(pending_chunk: list[str]) -> dict[str, Any]:
                with runtime_inflight_slot(
                    "harvest_profile_scrape",
                    budget=global_fetch_budget,
                    metadata={"chunk_size": len(pending_chunk), "source": "enrichment"},
                ):
                    try:
                        return dict(
                            self.harvest_profile_connector.fetch_profiles_by_urls(
                                pending_chunk,
                                snapshot_dir,
                                asset_logger=asset_logger,
                                allow_shared_provider_cache=allow_shared_provider_cache,
                                on_batch_result=_record_live_batch_stream,
                            )
                            or {}
                        )
                    except TypeError as exc:
                        if "on_batch_result" not in str(exc):
                            raise
                        return dict(
                            self.harvest_profile_connector.fetch_profiles_by_urls(
                                pending_chunk,
                                snapshot_dir,
                                asset_logger=asset_logger,
                                allow_shared_provider_cache=allow_shared_provider_cache,
                            )
                            or {}
                        )

            def _mark_chunk_queued(pending_chunk: list[str]) -> None:
                if self.store is None:
                    return
                for profile_url in pending_chunk:
                    self.store.mark_linkedin_profile_registry_queued(
                        profile_url,
                        source_shards=list(source_shards_by_url.get(profile_url) or []),
                        source_jobs=normalized_source_jobs,
                        snapshot_dir=str(snapshot_dir),
                    )
                    _record_event(profile_url, event_type="live_fetch_requested")

            def _mark_chunk_failed(pending_chunk: list[str], exc: Exception) -> None:
                if self.store is None:
                    return
                for profile_url in pending_chunk:
                    before_entry = self.store.get_linkedin_profile_registry(profile_url) or {}
                    retry_count_before = int(dict(before_entry).get("retry_count") or 0)
                    self.store.mark_linkedin_profile_registry_failed(
                        profile_url,
                        error=f"live_fetch_failed:{exc}",
                        retryable=True,
                        source_shards=list(source_shards_by_url.get(profile_url) or []),
                        source_jobs=normalized_source_jobs,
                        snapshot_dir=str(snapshot_dir),
                    )
                    _record_event(
                        profile_url,
                        event_type="live_fetch_failed",
                        event_status="failed_retryable",
                        detail=f"live_fetch_failed:{exc}",
                        metadata={"retry_count_before": retry_count_before},
                    )

            def _record_chunk_success(pending_chunk: list[str], live_payload: dict[str, Any]) -> None:
                with fetched_lock:
                    fetched.update(live_payload)
                if self.store is None:
                    return
                returned_urls = {
                    str(item or "").strip()
                    for item in dict(live_payload or {}).keys()
                    if str(item or "").strip()
                }
                for profile_url in pending_chunk:
                    if profile_url in returned_urls:
                        payload = dict((live_payload or {}).get(profile_url) or {})
                        _record_live_profile_success(
                            profile_url,
                            payload,
                            event_type="live_fetch_success",
                        )
                        continue
                    before_entry = self.store.get_linkedin_profile_registry(profile_url) or {}
                    retry_count_before = int(dict(before_entry).get("retry_count") or 0)
                    self.store.mark_linkedin_profile_registry_failed(
                        profile_url,
                        error="live_batch_response_missing_profile",
                        retryable=True,
                        source_shards=list(source_shards_by_url.get(profile_url) or []),
                        source_jobs=normalized_source_jobs,
                        snapshot_dir=str(snapshot_dir),
                    )
                    _record_event(
                        profile_url,
                        event_type="live_fetch_failed",
                        event_status="failed_retryable",
                        detail="live_batch_response_missing_profile",
                        metadata={"retry_count_before": retry_count_before},
                    )

            try:
                if live_fetch_workers <= 1 or len(pending_chunks) <= 1:
                    for pending_chunk in pending_chunks:
                        _mark_chunk_queued(pending_chunk)
                        try:
                            live_payload = _fetch_live_chunk(pending_chunk)
                        except Exception as exc:
                            _mark_chunk_failed(pending_chunk, exc)
                            raise
                        _record_chunk_success(pending_chunk, dict(live_payload or {}))
                else:
                    first_exception: Exception | None = None
                    with ThreadPoolExecutor(
                        max_workers=live_fetch_workers,
                        thread_name_prefix="harvest-live-fetch",
                    ) as executor:
                        chunk_iter = iter(pending_chunks)
                        future_to_chunk: dict[Any, list[str]] = {}

                        def _submit_next_chunk() -> bool:
                            try:
                                pending_chunk = next(chunk_iter)
                            except StopIteration:
                                return False
                            _mark_chunk_queued(pending_chunk)
                            future = executor.submit(
                                _fetch_live_chunk,
                                pending_chunk,
                            )
                            future_to_chunk[future] = pending_chunk
                            return True

                        for _ in range(live_fetch_workers):
                            if not _submit_next_chunk():
                                break

                        while future_to_chunk:
                            done, _ = wait(set(future_to_chunk.keys()), return_when=FIRST_COMPLETED)
                            for future in done:
                                pending_chunk = future_to_chunk.pop(future)
                                try:
                                    live_payload = dict(future.result() or {})
                                except Exception as exc:
                                    _mark_chunk_failed(pending_chunk, exc)
                                    if first_exception is None:
                                        first_exception = exc
                                    _submit_next_chunk()
                                    continue
                                _record_chunk_success(pending_chunk, live_payload)
                                _submit_next_chunk()
                    if first_exception is not None:
                        raise first_exception
            finally:
                _release_acquired_leases()
        if contended_urls:
            _wait_for_contended_fetches(contended_urls)
        return fetched

    def _follow_up_roster_anchored_scholar_coauthor_prospects(
        self,
        *,
        prospects: list[Candidate],
        identity: CompanyIdentity,
        snapshot_dir: Path,
        candidate_map: dict[str, Candidate],
        resolved_profiles: list[dict[str, Any]],
        evidence: list[EvidenceRecord],
        profile_fetch_count: int,
        profile_detail_limit: int,
        exploration_limit: int,
        asset_logger: AssetLogger,
        job_id: str,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        runtime_mode: str,
        allow_shared_provider_cache: bool = True,
    ) -> tuple[int, Path | None, list[str], int]:
        if not prospects or exploration_limit <= 0:
            return profile_fetch_count, None, [], 0

        follow_up_dir = snapshot_dir / "publications" / "roster_anchored_scholar_coauthors"
        follow_up_dir.mkdir(parents=True, exist_ok=True)
        summary_path = follow_up_dir / "follow_up_summary.json"
        progress_path = follow_up_dir / "follow_up_progress.json"
        patch_path = follow_up_dir / "follow_up_candidate_patch.json"
        decisions, errors, processed_candidate_ids = _load_scholar_coauthor_follow_up_progress(progress_path, summary_path)
        evidence_keys = {_evidence_resume_key(item) for item in evidence}
        _restore_scholar_coauthor_follow_up_patch(
            patch_path,
            candidate_map=candidate_map,
            resolved_profiles=resolved_profiles,
            evidence=evidence,
            evidence_keys=evidence_keys,
        )
        decisions_by_candidate_id = {
            str(item.get("candidate_id") or "").strip(): dict(item)
            for item in decisions
            if str(item.get("candidate_id") or "").strip()
        }
        queued_prospect_count = 0

        ordered_prospects = [
            prospect
            for prospect in _prioritize_scholar_coauthor_prospects(prospects)
            if prospect.candidate_id not in processed_candidate_ids
        ][:exploration_limit]

        for prospect in ordered_prospects:
            updated_candidates: list[Candidate] = []
            new_evidence_records: list[EvidenceRecord] = []
            new_resolved_profiles: list[dict[str, Any]] = []
            try:
                exploration_result = self.exploratory_enricher._explore_candidate(
                    snapshot_dir=snapshot_dir,
                    candidate=prospect,
                    target_company=identity.canonical_name,
                    logger=asset_logger,
                    job_id=job_id,
                    request_payload=request_payload,
                    plan_payload=plan_payload,
                    runtime_mode=runtime_mode,
                )
                explored_candidate = exploration_result["candidate"]
                errors.extend(list(exploration_result.get("errors") or []))
                if _is_queued_exploration_summary(exploration_result.get("summary") or {}):
                    queued_prospect_count += 1
                    decision_record = {
                        "candidate_id": explored_candidate.candidate_id,
                        "display_name": explored_candidate.display_name,
                        "state": "queued_background_exploration",
                        "confirmed_by_public_web": False,
                        "linkedin_url": explored_candidate.linkedin_url,
                        "next_step": "await_background_search_recovery",
                        "summary": "Background exploration is still queued; resume after worker recovery finishes.",
                        "seed_names": list(explored_candidate.metadata.get("scholar_coauthor_seed_names") or []),
                        "papers": list(explored_candidate.metadata.get("scholar_coauthor_papers") or [])[:8],
                        "profile_verified": False,
                        "exploration_summary": dict(exploration_result.get("summary") or {}),
                    }
                else:
                    decision = _scholar_coauthor_prospect_public_web_resolution(explored_candidate, identity)
                    profile_verified = False
                    prior_evidence_count = len(evidence)
                    prior_resolved_count = len(resolved_profiles)
                    if (
                        decision["confirmed_by_public_web"]
                        and _candidate_known_linkedin_url(explored_candidate)
                        and profile_fetch_count < profile_detail_limit
                    ):
                        _append_unique_evidence_records(
                            evidence,
                            list(exploration_result.get("evidence") or []),
                            evidence_keys=evidence_keys,
                        )
                        profile_verified, profile_fetch_count = self._resolve_candidate_with_known_refs(
                            _annotate_scholar_coauthor_resolution(explored_candidate, decision),
                            identity,
                            snapshot_dir,
                            profile_fetch_count,
                            profile_detail_limit,
                            candidate_map,
                            resolved_profiles,
                            evidence,
                            asset_logger=asset_logger,
                            allow_shared_provider_cache=allow_shared_provider_cache,
                            source_job_id=job_id,
                        )
                    new_evidence_records = list(evidence[prior_evidence_count:])
                    for item in new_evidence_records:
                        evidence_keys.add(_evidence_resume_key(item))
                    new_resolved_profiles = list(resolved_profiles[prior_resolved_count:])
                    if profile_verified:
                        updated_candidate = candidate_map.get(_candidate_key(explored_candidate))
                        if updated_candidate is not None:
                            updated_candidates.append(updated_candidate)
                    decision_record = {
                        "candidate_id": explored_candidate.candidate_id,
                        "display_name": explored_candidate.display_name,
                        "state": decision["state"],
                        "confirmed_by_public_web": decision["confirmed_by_public_web"],
                        "linkedin_url": decision["linkedin_url"],
                        "next_step": decision["next_step"],
                        "summary": decision["summary"],
                        "seed_names": list(explored_candidate.metadata.get("scholar_coauthor_seed_names") or []),
                        "papers": list(explored_candidate.metadata.get("scholar_coauthor_papers") or [])[:8],
                        "profile_verified": profile_verified,
                        "exploration_summary": dict(exploration_result.get("summary") or {}),
                    }
            except Exception as exc:
                error_message = f"scholar_coauthor_follow_up:{prospect.display_name}:{str(exc)[:160]}"
                errors.append(error_message)
                decision_record = {
                    "candidate_id": prospect.candidate_id,
                    "display_name": prospect.display_name,
                    "state": "follow_up_error",
                    "confirmed_by_public_web": False,
                    "linkedin_url": prospect.linkedin_url,
                    "next_step": "retry_follow_up",
                    "summary": f"Scholar coauthor follow-up failed: {str(exc)[:160]}",
                    "seed_names": list(prospect.metadata.get("scholar_coauthor_seed_names") or []),
                    "papers": list(prospect.metadata.get("scholar_coauthor_papers") or [])[:8],
                    "profile_verified": False,
                    "exploration_summary": {},
                    "error": str(exc)[:300],
                }

            decisions_by_candidate_id[decision_record["candidate_id"]] = decision_record
            if not _is_queued_scholar_coauthor_follow_up_decision(decision_record):
                processed_candidate_ids.add(decision_record["candidate_id"])
            _persist_scholar_coauthor_follow_up_patch(
                patch_path,
                updated_candidates=updated_candidates,
                resolved_profiles=new_resolved_profiles,
                new_evidence=new_evidence_records,
                asset_logger=asset_logger,
            )
            _write_scholar_coauthor_follow_up_state(
                progress_path=progress_path,
                summary_path=summary_path,
                identity=identity,
                prospect_total=len(prospects),
                processed_candidate_ids=processed_candidate_ids,
                decisions=list(decisions_by_candidate_id.values()),
                errors=errors,
                asset_logger=asset_logger,
                completed=False,
            )

        _write_scholar_coauthor_follow_up_state(
            progress_path=progress_path,
            summary_path=summary_path,
            identity=identity,
            prospect_total=len(prospects),
            processed_candidate_ids=processed_candidate_ids,
            decisions=list(decisions_by_candidate_id.values()),
            errors=errors,
            asset_logger=asset_logger,
            completed=len(processed_candidate_ids) >= len({prospect.candidate_id for prospect in prospects}),
        )
        return profile_fetch_count, summary_path, errors, queued_prospect_count

    def _gate_publication_leads_after_exploration(
        self,
        *,
        lead_candidates: list[Candidate],
        identity: CompanyIdentity,
        snapshot_dir: Path,
        candidate_map: dict[str, Candidate],
        unresolved_candidates: list[dict[str, Any]],
        asset_logger: AssetLogger,
        allow_targeted_name_search: bool,
    ) -> tuple[list[Candidate], Path | None]:
        if not lead_candidates:
            return [], None

        resolution_dir = snapshot_dir / "publication_lead_resolution"
        resolution_dir.mkdir(parents=True, exist_ok=True)
        decisions: list[dict[str, Any]] = []
        gated_leads: list[Candidate] = []

        for lead_candidate in lead_candidates:
            current_candidate = candidate_map.get(_candidate_key(lead_candidate), lead_candidate)
            decision = _publication_lead_public_web_resolution(
                current_candidate,
                identity,
                allow_targeted_name_search=allow_targeted_name_search,
            )
            current_candidate = _annotate_publication_lead_resolution(current_candidate, decision)
            candidate_map[_candidate_key(current_candidate)] = current_candidate
            _drop_unresolved_candidate(unresolved_candidates, current_candidate.candidate_id)
            unresolved_candidates.append(
                {
                    "candidate_id": current_candidate.candidate_id,
                    "display_name": current_candidate.display_name,
                    "attempted_slugs": [],
                    "query_summaries": [],
                    "resolution_source": "publication_lead_public_web_verification",
                    "publication_lead_resolution_state": decision["state"],
                    "public_web_confirmed": decision["confirmed_by_public_web"],
                    "linkedin_url": decision["linkedin_url"],
                    "next_step": decision["next_step"],
                    "summary": decision["summary"],
                }
            )
            if decision["eligible_for_targeted_name_search"]:
                gated_leads.append(current_candidate)
            decisions.append(
                {
                    "candidate_id": current_candidate.candidate_id,
                    "display_name": current_candidate.display_name,
                    "source_dataset": current_candidate.source_dataset,
                    "source_path": current_candidate.source_path,
                    "publication_title": _candidate_publication_title(current_candidate),
                    "publication_url": str(current_candidate.metadata.get("publication_url") or "").strip(),
                    **decision,
                }
            )

        summary_path = resolution_dir / "public_web_gate.json"
        asset_logger.write_json(
            summary_path,
            {
                "target_company": identity.canonical_name,
                "candidate_count": len(decisions),
                "eligible_for_targeted_name_search_count": len(gated_leads),
                "decisions": decisions,
            },
            asset_type="publication_lead_public_web_gate",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        return gated_leads, summary_path

    def _resolve_publication_leads_with_harvest_search(
        self,
        *,
        lead_candidates: list[Candidate],
        identity: CompanyIdentity,
        snapshot_dir: Path,
        remaining_profile_budget: int,
        candidate_map: dict[str, Candidate],
        resolved_profiles: list[dict[str, Any]],
        unresolved_candidates: list[dict[str, Any]],
        evidence: list[EvidenceRecord],
        asset_logger: AssetLogger,
        allow_shared_provider_cache: bool = True,
    ) -> tuple[int, Path | None]:
        if (
            self.harvest_profile_search_connector is None
            or not harvest_connector_available(self.harvest_profile_search_connector.settings)
            or self.harvest_profile_connector is None
            or remaining_profile_budget <= 0
        ):
            return 0, None

        resolution_dir = snapshot_dir / "publication_lead_resolution"
        resolution_dir.mkdir(parents=True, exist_ok=True)
        search_root = resolution_dir / "search_runs"
        search_root.mkdir(parents=True, exist_ok=True)
        company_url = str(identity.linkedin_company_url or "").strip()
        searches_used = 0
        attempt_runtime: list[dict[str, Any]] = []

        for lead_candidate in lead_candidates:
            current_candidate = candidate_map.get(_candidate_key(lead_candidate), lead_candidate)
            if current_candidate.category != "lead":
                continue
            query_text = (current_candidate.display_name or current_candidate.name_en).strip()
            if not query_text:
                continue
            candidate_dir = search_root / current_candidate.candidate_id
            candidate_dir.mkdir(parents=True, exist_ok=True)
            attempt_runtime.append(
                {
                    "candidate": current_candidate,
                    "candidate_dir": candidate_dir,
                    "attempt": {
                        "candidate_id": current_candidate.candidate_id,
                        "display_name": current_candidate.display_name,
                        "query_text": query_text,
                        "attempts": [],
                        "resolved": False,
                    },
                    "variants": {},
                }
            )

        search_variants = [
            {
                "employment_status": "current",
                "filter_hints": {"current_companies": [company_url] if company_url else []},
                "label": "current_company_exact_name",
            },
            {
                "employment_status": "former",
                "filter_hints": {"past_companies": [company_url] if company_url else []},
                "label": "past_company_exact_name",
            },
        ]

        for variant in search_variants:
            if searches_used >= remaining_profile_budget:
                break
            phase_urls: list[str] = []
            for runtime_item in attempt_runtime:
                attempt = runtime_item["attempt"]
                if attempt["resolved"] or searches_used >= remaining_profile_budget:
                    continue
                current_candidate = candidate_map.get(_candidate_key(runtime_item["candidate"]), runtime_item["candidate"])
                runtime_item["candidate"] = current_candidate
                if current_candidate.category != "lead":
                    continue
                search_result = self.harvest_profile_search_connector.search_profiles(
                    query_text=str(attempt.get("query_text") or ""),
                    filter_hints=dict(variant["filter_hints"]),
                    employment_status=str(variant["employment_status"]),
                    discovery_dir=runtime_item["candidate_dir"],
                    asset_logger=asset_logger,
                    limit=min(self.harvest_profile_search_connector.settings.max_paid_items, 10),
                    allow_shared_provider_cache=allow_shared_provider_cache,
                )
                searches_used += 1
                variant_summary = {
                    "label": variant["label"],
                    "employment_status": variant["employment_status"],
                    "raw_path": str(search_result.get("raw_path") or "") if search_result else "",
                    "row_count": len(list(search_result.get("rows") or [])) if search_result else 0,
                    "matched_names": [],
                    "profile_url": "",
                    "verified": False,
                }
                rows = []
                if search_result is not None:
                    rows = [
                        row
                        for row in list(search_result.get("rows") or [])
                        if _names_match(current_candidate.name_en, str(row.get("full_name") or ""))
                    ]
                    variant_summary["matched_names"] = [str(row.get("full_name") or "") for row in rows]
                profile_urls: list[str] = []
                for row in rows:
                    profile_url = str(row.get("profile_url") or "").strip()
                    if profile_url and profile_url not in profile_urls:
                        profile_urls.append(profile_url)
                    if profile_url and profile_url not in phase_urls:
                        phase_urls.append(profile_url)
                attempt["attempts"].append(variant_summary)
                runtime_item["variants"][str(variant["label"])] = {
                    "rows": rows,
                    "summary": variant_summary,
                }

            fetched_profiles = self._fetch_harvest_profiles_for_urls(
                phase_urls,
                snapshot_dir,
                asset_logger=asset_logger,
                allow_shared_provider_cache=allow_shared_provider_cache,
                source_shards_by_url={
                    profile_url: [f"publication_lead_targeted:{variant['label']}"]
                    for profile_url in phase_urls
                },
            )
            for runtime_item in attempt_runtime:
                attempt = runtime_item["attempt"]
                if attempt["resolved"]:
                    continue
                variant_runtime = dict(runtime_item["variants"].get(str(variant["label"])) or {})
                if not variant_runtime:
                    continue
                current_candidate = candidate_map.get(_candidate_key(runtime_item["candidate"]), runtime_item["candidate"])
                runtime_item["candidate"] = current_candidate
                if current_candidate.category != "lead":
                    continue
                for row in list(variant_runtime.get("rows") or []):
                    profile_url = str(row.get("profile_url") or "").strip()
                    if not profile_url:
                        continue
                    profile = fetched_profiles.get(profile_url)
                    if profile is None or not _profile_matches_candidate(
                        profile["parsed"],
                        current_candidate,
                        identity,
                        model_client=self.model_client,
                    ):
                        continue
                    merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                        current_candidate,
                        profile["parsed"],
                        profile["raw_path"],
                        profile["account_id"],
                        _extract_seed_slug(current_candidate) or extract_linkedin_slug(profile_url),
                        identity,
                        model_client=self.model_client,
                        resolution_source=f"publication_lead_targeted_harvest_{variant['employment_status']}",
                    )
                    candidate_map[_candidate_key(current_candidate)] = merged_candidate
                    resolved_profiles.append(resolved_profile)
                    evidence.extend(profile_evidence)
                    _drop_unresolved_candidate(unresolved_candidates, current_candidate.candidate_id)
                    summary = variant_runtime.get("summary")
                    if isinstance(summary, dict):
                        summary["profile_url"] = profile_url
                        summary["verified"] = True
                    attempt["resolved"] = True
                    break

        attempts_summary = [dict(item["attempt"] or {}) for item in attempt_runtime]
        unresolved_candidate_ids = {
            str(item.get("candidate_id") or "").strip() for item in unresolved_candidates
        }
        for attempt in attempts_summary:
            if attempt.get("resolved"):
                continue
            candidate_id = str(attempt.get("candidate_id") or "").strip()
            if not candidate_id or candidate_id in unresolved_candidate_ids:
                continue
            unresolved_candidates.append(
                {
                    "candidate_id": candidate_id,
                    "display_name": str(attempt.get("display_name") or ""),
                    "attempted_slugs": [],
                    "query_summaries": list(attempt.get("attempts") or []),
                    "resolution_source": "publication_lead_targeted_harvest",
                }
            )
            unresolved_candidate_ids.add(candidate_id)

        summary_path = resolution_dir / "summary.json"
        asset_logger.write_json(
            summary_path,
            {
                "target_company": identity.canonical_name,
                "attempt_count": len(attempts_summary),
                "searches_used": searches_used,
                "attempts": attempts_summary,
            },
            asset_type="publication_lead_targeted_resolution_summary",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        return searches_used, summary_path

    def _resolve_candidate_with_known_refs(
        self,
        candidate: Candidate,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        profile_fetch_count: int,
        profile_detail_limit: int,
        candidate_map: dict[str, Candidate],
        resolved_profiles: list[dict[str, Any]],
        evidence: list[EvidenceRecord],
        *,
        asset_logger: AssetLogger,
        prefetched_harvest_profiles: dict[str, dict[str, Any]] | None = None,
        background_prefetch_urls: set[str] | None = None,
        allow_shared_provider_cache: bool = True,
        source_job_id: str = "",
    ) -> tuple[bool, int]:
        if profile_fetch_count >= profile_detail_limit:
            return False, profile_fetch_count

        profile_urls = _candidate_profile_urls(candidate)
        harvested_profiles = self._fetch_harvest_profiles_for_urls(
            profile_urls,
            snapshot_dir,
            asset_logger=asset_logger,
            prefetched_harvest_profiles=prefetched_harvest_profiles,
            background_prefetch_urls=background_prefetch_urls,
            allow_shared_provider_cache=allow_shared_provider_cache,
            source_shards_by_url={
                profile_url: _profile_registry_sources_for_candidate(candidate)
                for profile_url in profile_urls
            },
            source_jobs=[source_job_id] if str(source_job_id or "").strip() else [],
        )
        for profile_url in profile_urls:
            normalized_profile_url = str(profile_url or "").strip()
            profile = harvested_profiles.get(normalized_profile_url)
            if profile is None or not _profile_matches_candidate(
                profile["parsed"],
                candidate,
                identity,
                model_client=self.model_client,
            ):
                continue
            profile_fetch_count += 1
            merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                candidate,
                profile["parsed"],
                profile["raw_path"],
                profile["account_id"],
                _extract_seed_slug(candidate) or extract_linkedin_slug(normalized_profile_url),
                identity,
                model_client=self.model_client,
                resolution_source="known_profile_url_harvest",
            )
            candidate_map[_candidate_key(candidate)] = merged_candidate
            resolved_profiles.append(resolved_profile)
            evidence.extend(profile_evidence)
            return True, profile_fetch_count

        seed_slug = _extract_seed_slug(candidate)
        if seed_slug and profile_fetch_count < profile_detail_limit:
            profile_fetch_count += 1
            profile = self.profile_connector.fetch_profile(seed_slug, snapshot_dir, asset_logger=asset_logger)
            if profile is not None and _profile_matches_candidate(
                profile["parsed"],
                candidate,
                identity,
                model_client=self.model_client,
            ):
                merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                    candidate,
                    profile["parsed"],
                    profile["raw_path"],
                    profile["account_id"],
                    seed_slug,
                    identity,
                    model_client=self.model_client,
                    resolution_source="seed_slug",
                )
                candidate_map[_candidate_key(candidate)] = merged_candidate
                resolved_profiles.append(resolved_profile)
                evidence.extend(profile_evidence)
                return True, profile_fetch_count
        return False, profile_fetch_count


class LinkedInSearchSlugResolver:
    def __init__(self, accounts: list[RapidApiAccount], *, search_provider: BaseSearchProvider | None = None) -> None:
        self.accounts = search_people_accounts(accounts)
        self._exhausted_account_ids: set[str] = set()
        self.web_fallback = DuckDuckGoLinkedInResolver(search_provider=search_provider)

    def resolve(
        self,
        candidates: list[Candidate],
        identity: CompanyIdentity,
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger | None = None,
    ) -> dict[str, Any]:
        search_dir = snapshot_dir / "slug_search"
        search_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        results: list[dict[str, Any]] = []
        errors: list[str] = []
        web_results = self.web_fallback.resolve(candidates, identity, snapshot_dir, asset_logger=logger)
        errors.extend(web_results.get("errors", []))
        web_result_map = {item["candidate_key"]: item for item in web_results.get("results", [])}

        for candidate in candidates:
            existing_item = web_result_map.get(_candidate_key(candidate), {})
            existing_queries = list(existing_item.get("queries", []))
            existing_slugs = list(existing_item.get("slugs", []))
            if existing_slugs or not self.accounts:
                results.append(
                    {
                        "candidate_id": candidate.candidate_id,
                        "candidate_key": _candidate_key(candidate),
                        "display_name": candidate.display_name,
                        "slugs": existing_slugs,
                        "queries": existing_queries,
                    }
                )
                continue

            queries = _build_people_search_queries(candidate, identity)
            slug_candidates: list[str] = []
            query_summaries: list[dict[str, Any]] = list(existing_queries)

            for index, query in enumerate(queries, start=1):
                raw_path = search_dir / f"{candidate.candidate_id}_q{index:02d}.json"
                payload = None
                cached_from: Path | None = None
                if raw_path.exists():
                    try:
                        payload = json.loads(raw_path.read_text())
                        cached_from = raw_path
                    except json.JSONDecodeError:
                        payload = None
                if payload is None:
                    payload, cached_from = self._load_cached_search_payload(snapshot_dir.parent, snapshot_dir, candidate.candidate_id, index)
                account: RapidApiAccount | None = None
                if payload is None:
                    payload, account, search_errors = self._search_people(query)
                    errors.extend(search_errors)
                if payload is None:
                    query_summaries.append({"query": query, "raw_path": "", "account_id": "", "rows": [], "slugs": [], "mode": "provider_people_search"})
                    continue
                logger.write_json(
                    raw_path,
                    payload,
                    asset_type="provider_people_search_payload",
                    source_kind="slug_resolution",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"query": query, "candidate_id": candidate.candidate_id},
                )

                matched_rows: list[dict[str, Any]] = []
                for row_index, row in enumerate(extract_search_people_rows(payload), start=1):
                    if not _search_result_name_matches_candidate(row, candidate):
                        continue
                    row_summary = {
                        "full_name": row.get("full_name", ""),
                        "headline": row.get("headline", ""),
                        "location": row.get("location", ""),
                        "urn": row.get("urn", ""),
                    }
                    if cached_from:
                        row_summary["cached_search_from"] = str(cached_from)
                    basic_profile = self._fetch_basic_profile(row.get("urn", ""), search_dir, account, asset_logger=logger)
                    if basic_profile is not None:
                        row_summary["basic_profile_path"] = str(basic_profile["raw_path"])
                        row_summary["profile_url"] = basic_profile["parsed"].get("profile_url", "")
                        row_summary["username"] = basic_profile["parsed"].get("username", "")
                        slug = row_summary["username"] or extract_linkedin_slug(row_summary["profile_url"])
                        if slug and slug not in slug_candidates:
                            slug_candidates.append(slug)
                    matched_rows.append(row_summary)
                    if row_index >= 2 and slug_candidates:
                        break

                query_summaries.append(
                    {
                        "query": query,
                        "raw_path": str(raw_path),
                        "account_id": account.account_id if account else "cache",
                        "rows": matched_rows,
                        "slugs": list(slug_candidates),
                        "mode": "provider_people_search",
                    }
                )
                if slug_candidates:
                    break

            results.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "candidate_key": _candidate_key(candidate),
                    "display_name": candidate.display_name,
                    "slugs": list(existing_slugs) + [item for item in slug_candidates if item not in existing_slugs],
                    "queries": query_summaries,
                }
            )

        summary_path = search_dir / "summary.json"
        logger.write_json(
            summary_path,
            {"resolver": "web_first_then_provider", "results": results, "errors": errors},
            asset_type="slug_search_summary",
            source_kind="slug_resolution",
            is_raw_asset=False,
            model_safe=True,
        )
        return {"results": results, "summary_path": summary_path, "errors": errors}

    def _load_cached_search_payload(
        self,
        company_dir: Path,
        snapshot_dir: Path,
        candidate_id: str,
        query_index: int,
    ) -> tuple[dict[str, Any] | None, Path | None]:
        pattern = f"*/slug_search/{candidate_id}_q{query_index:02d}.json"
        candidates = [path for path in company_dir.glob(pattern) if snapshot_dir not in path.parents]
        if not candidates:
            return None, None
        cached_path = max(candidates, key=lambda item: item.stat().st_mtime)
        try:
            return json.loads(cached_path.read_text()), cached_path
        except (OSError, json.JSONDecodeError):
            return None, None

    def _search_people(self, query: str) -> tuple[dict[str, Any] | None, RapidApiAccount | None, list[str]]:
        errors: list[str] = []
        for account in self.accounts:
            if account.account_id in self._exhausted_account_ids:
                continue
            url = build_people_search_url(account, query, limit=5)
            headers = {"x-rapidapi-host": account.host, "x-rapidapi-key": account.api_key, "User-Agent": "Mozilla/5.0"}
            http_request = request.Request(url, headers=headers, method="GET")
            try:
                with request.urlopen(http_request, timeout=30) as response:
                    return json.loads(response.read().decode("utf-8")), account, errors
            except error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="ignore")
                if exc.code == 429:
                    self._exhausted_account_ids.add(account.account_id)
                errors.append(f"search_people:{account.account_id}:{exc.code}:{detail[:120]}")
            except Exception as exc:
                errors.append(f"search_people:{account.account_id}:{str(exc)[:120]}")
        return None, None, errors

    def _fetch_basic_profile(
        self,
        profile_ref: str,
        search_dir: Path,
        account: RapidApiAccount | None,
        *,
        asset_logger: AssetLogger,
    ) -> dict[str, Any] | None:
        profile_ref = str(profile_ref or "").strip()
        if not profile_ref:
            return None
        basic_dir = search_dir / "basic_profiles"
        basic_dir.mkdir(parents=True, exist_ok=True)
        cache_key = sha1(profile_ref.encode("utf-8")).hexdigest()[:16]
        if account is not None:
            raw_path = basic_dir / f"{cache_key}_{account.account_id}.json"
            if raw_path.exists():
                try:
                    payload = json.loads(raw_path.read_text())
                    return {"raw_path": raw_path, "parsed": parse_basic_linkedin_profile_payload(payload)}
                except json.JSONDecodeError:
                    pass
        else:
            raw_path = basic_dir / f"{cache_key}_cache.json"

        company_dir = search_dir.parent.parent
        cached_paths = sorted(company_dir.glob(f"*/slug_search/basic_profiles/{cache_key}_*.json"))
        if cached_paths:
            cached_path = cached_paths[-1]
            try:
                payload = json.loads(cached_path.read_text())
                asset_logger.write_json(
                    raw_path,
                    payload,
                    asset_type="linkedin_basic_profile_payload",
                    source_kind="slug_resolution_basic_profile",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"profile_ref": profile_ref, "copied_from_cache": True},
                )
                return {"raw_path": raw_path, "parsed": parse_basic_linkedin_profile_payload(payload)}
            except (OSError, json.JSONDecodeError):
                pass

        if account is None:
            return None

        url = build_basic_profile_url(account, profile_ref)
        headers = {"x-rapidapi-host": account.host, "x-rapidapi-key": account.api_key, "User-Agent": "Mozilla/5.0"}
        http_request = request.Request(url, headers=headers, method="GET")
        try:
            with request.urlopen(http_request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            if exc.code == 429:
                self._exhausted_account_ids.add(account.account_id)
            return None
        except Exception:
            return None

        asset_logger.write_json(
            raw_path,
            payload,
            asset_type="linkedin_basic_profile_payload",
            source_kind="slug_resolution_basic_profile",
            is_raw_asset=True,
            model_safe=False,
            metadata={"profile_ref": profile_ref, "account_id": account.account_id},
        )
        return {"raw_path": raw_path, "parsed": parse_basic_linkedin_profile_payload(payload)}


class DuckDuckGoLinkedInResolver:
    def __init__(self, *, search_provider: BaseSearchProvider | None = None) -> None:
        self.search_provider = search_provider or DuckDuckGoHtmlSearchProvider()

    def resolve(
        self,
        candidates: list[Candidate],
        identity: CompanyIdentity,
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger | None = None,
    ) -> dict[str, Any]:
        search_dir = snapshot_dir / "slug_search"
        search_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        results: list[dict[str, Any]] = []
        errors: list[str] = []

        for candidate in candidates:
            queries = _build_slug_queries(candidate, identity)
            slug_candidates: list[str] = []
            query_summaries: list[dict[str, Any]] = []
            for index, query in enumerate(queries, start=1):
                try:
                    response = self.search_provider.search(query, max_results=10)
                except Exception as exc:
                    query_summaries.append({"query": query, "raw_path": "", "slugs": [], "error": str(exc)})
                    errors.append(f"slug_search:{candidate.display_name}:{str(exc)[:160]}")
                    continue
                raw_path = search_dir / f"{candidate.candidate_id}_q{index:02d}.{ 'json' if response.raw_format == 'json' else 'html'}"
                if response.raw_format == "json":
                    logger.write_json(
                        raw_path,
                        search_response_to_record(response),
                        asset_type="web_linkedin_search_payload",
                        source_kind="slug_resolution",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={
                            "query": query,
                            "candidate_id": candidate.candidate_id,
                            "provider_name": response.provider_name,
                        },
                    )
                else:
                    logger.write_text(
                        raw_path,
                        str(response.raw_payload or ""),
                        asset_type="web_linkedin_search_html",
                        source_kind="slug_resolution",
                        content_type=response.content_type or "text/html",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={
                            "query": query,
                            "candidate_id": candidate.candidate_id,
                            "provider_name": response.provider_name,
                        },
                    )
                urls = [str(item.url or "").strip() for item in response.results if "linkedin.com/in/" in str(item.url or "")]
                slugs = []
                for url in urls:
                    slug = extract_linkedin_slug(url)
                    if slug and slug not in slug_candidates:
                        slug_candidates.append(slug)
                        slugs.append(slug)
                query_summaries.append(
                    {
                        "query": query,
                        "raw_path": str(raw_path),
                        "slugs": slugs,
                        "provider_name": response.provider_name,
                        "result_count": len(response.results),
                    }
                )
                if slug_candidates:
                    break
            results.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "candidate_key": _candidate_key(candidate),
                    "display_name": candidate.display_name,
                    "slugs": slug_candidates,
                    "queries": query_summaries,
                }
            )

        summary_path = search_dir / "summary.json"
        logger.write_json(
            summary_path,
            {"results": results, "errors": errors},
            asset_type="web_slug_search_summary",
            source_kind="slug_resolution",
            is_raw_asset=False,
            model_safe=True,
        )
        return {"results": results, "summary_path": summary_path, "errors": errors}


class LinkedInProfileDetailConnector:
    def __init__(self, accounts: list[RapidApiAccount]) -> None:
        self.accounts = profile_detail_accounts(accounts)
        self._exhausted_account_ids: set[str] = set()

    def fetch_profile(self, slug: str, snapshot_dir: Path, *, asset_logger: AssetLogger | None = None) -> dict[str, Any] | None:
        profiles_dir = snapshot_dir / "profiles"
        profiles_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        for account in self.accounts:
            if account.account_id in self._exhausted_account_ids:
                continue
            raw_path = profiles_dir / f"{slug}_{account.account_id}.json"
            if raw_path.exists():
                try:
                    payload = json.loads(raw_path.read_text())
                    logger.record_existing(
                        raw_path,
                        asset_type="linkedin_profile_detail_payload",
                        source_kind="profile_detail_connector",
                        content_type="application/json",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={"slug": slug, "account_id": account.account_id, "cached": True},
                    )
                    return {"account_id": account.account_id, "raw_path": raw_path, "parsed": parse_linkedin_profile_payload(payload)}
                except json.JSONDecodeError:
                    pass

            url = build_profile_detail_url(account, slug)
            headers = {"x-rapidapi-host": account.host, "x-rapidapi-key": account.api_key, "User-Agent": "Mozilla/5.0"}
            if "real-time-linkedin-data-scraper-api" in account.host:
                headers["Content-Type"] = "application/json"
            http_request = request.Request(url, headers=headers, method="GET")
            try:
                with request.urlopen(http_request, timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                logger.write_json(
                    raw_path,
                    payload,
                    asset_type="linkedin_profile_detail_payload",
                    source_kind="profile_detail_connector",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"slug": slug, "account_id": account.account_id, "cached": False},
                )
                return {"account_id": account.account_id, "raw_path": raw_path, "parsed": parse_linkedin_profile_payload(payload)}
            except error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="ignore")
                if exc.code == 429:
                    self._exhausted_account_ids.add(account.account_id)
                if exc.code in {404, 429}:
                    continue
                if detail:
                    continue
            except Exception:
                continue
        return None


class CompanyPublicationConnector:
    def __init__(self, catalog: AssetCatalog) -> None:
        self.catalog = catalog

    def enrich(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        candidates: list[Candidate],
        *,
        asset_logger: AssetLogger | None = None,
        max_publications: int,
        max_leads: int,
        request_payload: dict[str, Any] | None = None,
        plan_payload: dict[str, Any] | None = None,
        existing_evidence: list[EvidenceRecord] | None = None,
    ) -> dict[str, Any]:
        publications_dir = snapshot_dir / "publications"
        publications_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)

        if max_publications <= 0:
            return {
                "matched_candidates": candidates,
                "lead_candidates": [],
                "evidence": [],
                "publication_matches": [],
                "coauthor_edges": [],
                "scholar_coauthor_prospects": [],
                "artifact_paths": {},
            }

        errors: list[str] = []
        try:
            publications = self._collect_publications(
                identity,
                publications_dir,
                max_publications,
                asset_logger=logger,
                request_payload=request_payload or {},
                plan_payload=plan_payload or {},
            )
        except Exception as exc:
            errors.append(f"publication_collection:{str(exc)[:160]}")
            publications = []
        candidate_map = _candidate_name_map(candidates)
        lead_map: dict[str, Candidate] = {}
        evidence: list[EvidenceRecord] = []
        publication_matches: list[dict[str, Any]] = []
        coauthor_edges: set[tuple[str, str]] = set()
        scholar_coauthor_result = self._collect_roster_anchored_scholar_coauthors(
            identity,
            publications_dir,
            candidates,
            publications=publications,
            existing_evidence=existing_evidence or [],
            asset_logger=logger,
        )
        evidence.extend(scholar_coauthor_result["evidence"])

        for publication in publications:
            matched_candidates: dict[str, Candidate] = {}
            for name in publication.authors:
                candidate = candidate_map.get(_name_key(name))
                if candidate is not None:
                    matched_candidates[candidate.candidate_id] = candidate
                    evidence.append(
                        EvidenceRecord(
                            evidence_id=make_evidence_id(candidate.candidate_id, publication.source_dataset, publication.title, publication.url),
                            candidate_id=candidate.candidate_id,
                            source_type="publication_author",
                            title=publication.title,
                            url=publication.url,
                            summary=f"{candidate.display_name} appears as an author on this company-related publication.",
                            source_dataset=publication.source_dataset,
                            source_path=publication.source_path,
                            metadata={"publication_id": publication.publication_id, "matched_name": name},
                        )
                    )
                elif len(lead_map) < max_leads and _looks_like_person_name(name):
                    lead = _build_publication_lead(identity, publication, name, "Publication author lead")
                    lead_map.setdefault(_candidate_key(lead), lead)

            for name in publication.acknowledgement_names:
                candidate = candidate_map.get(_name_key(name))
                if candidate is not None:
                    evidence.append(
                        EvidenceRecord(
                            evidence_id=make_evidence_id(
                                candidate.candidate_id,
                                publication.source_dataset,
                                f"{publication.title} acknowledgement",
                                publication.url,
                            ),
                            candidate_id=candidate.candidate_id,
                            source_type="publication_acknowledgement",
                            title=publication.title,
                            url=publication.url,
                            summary=f"{candidate.display_name} is referenced in the acknowledgement evidence for this publication.",
                            source_dataset=publication.source_dataset,
                            source_path=publication.source_path,
                            metadata={"publication_id": publication.publication_id, "matched_name": name},
                        )
                    )
                elif len(lead_map) < max_leads and _looks_like_person_name(name):
                    lead = _build_publication_lead(identity, publication, name, "Acknowledgement lead")
                    lead_map.setdefault(_candidate_key(lead), lead)

            matched_candidate_list = list(matched_candidates.values())
            matched_names = sorted({candidate.display_name or candidate.name_en for candidate in matched_candidate_list})
            for candidate in matched_candidate_list:
                coauthors = [name for name in matched_names if name != (candidate.display_name or candidate.name_en)]
                if not coauthors:
                    continue
                evidence.append(
                    EvidenceRecord(
                        evidence_id=make_evidence_id(
                            candidate.candidate_id,
                            publication.source_dataset,
                            f"{publication.title} coauthor",
                            publication.url,
                        ),
                        candidate_id=candidate.candidate_id,
                        source_type="publication_coauthor",
                        title=publication.title,
                        url=publication.url,
                        summary=f"{candidate.display_name or candidate.name_en} co-authored this company-related publication with {', '.join(coauthors)}.",
                        source_dataset=publication.source_dataset,
                        source_path=publication.source_path,
                        metadata={
                            "publication_id": publication.publication_id,
                            "coauthors": coauthors,
                            "matched_candidates": matched_names,
                        },
                    )
                )

            for left, right in combinations(matched_names, 2):
                coauthor_edges.add((left, right))
            if matched_names:
                publication_matches.append(
                    {
                        "publication_id": publication.publication_id,
                        "title": publication.title,
                        "url": publication.url,
                        "matched_candidates": matched_names,
                    }
                )

        coauthor_graph = [{"source": left, "target": right} for left, right in sorted(coauthor_edges)]
        publication_summary_path = publications_dir / "publication_matches.json"
        coauthor_graph_path = publications_dir / "coauthor_graph.json"
        lead_candidates_path = publications_dir / "publication_leads.json"
        logger.write_json(
            publication_summary_path,
            {"publications": [item.to_record() for item in publications], "matches": publication_matches},
            asset_type="publication_matches",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            coauthor_graph_path,
            coauthor_graph,
            asset_type="publication_coauthor_graph",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            lead_candidates_path,
            [candidate.to_record() for candidate in lead_map.values()],
            asset_type="publication_leads",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        artifact_paths = {
            "publication_matches": str(publication_summary_path),
            "coauthor_graph": str(coauthor_graph_path),
            "publication_leads": str(lead_candidates_path),
        }
        artifact_paths.update(dict(scholar_coauthor_result.get("artifact_paths") or {}))

        return {
            "matched_candidates": candidates,
            "lead_candidates": list(lead_map.values()),
            "evidence": evidence,
            "publication_matches": publication_matches,
            "coauthor_edges": coauthor_graph,
            "scholar_coauthor_prospects": list(scholar_coauthor_result.get("prospect_candidates") or []),
            "errors": errors,
            "artifact_paths": artifact_paths,
        }

    def _collect_publications(
        self,
        identity: CompanyIdentity,
        publications_dir: Path,
        max_publications: int,
        *,
        asset_logger: AssetLogger,
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
    ) -> list[PublicationRecord]:
        if identity.company_key == "anthropic" and self.catalog.anthropic_publications.exists():
            asset_logger.record_existing(
                self.catalog.anthropic_publications,
                asset_type="local_publication_bundle",
                source_kind="publication_enrichment",
                content_type="application/json",
                is_raw_asset=True,
                model_safe=False,
                metadata={"company": identity.canonical_name},
            )
            return _load_local_publications(self.catalog.anthropic_publications, max_publications)
        if self._should_skip_remote_publication_collection(request_payload):
            return []
        publication_plan = dict(plan_payload.get("publication_coverage") or {})
        source_families = {
            str(item.get("family") or "").strip()
            for item in publication_plan.get("source_families") or []
            if isinstance(item, dict)
        }
        seed_queries = [str(item).strip() for item in publication_plan.get("seed_queries") or [] if str(item).strip()]
        results: list[PublicationRecord] = []
        if source_families & {
            "official_research",
            "official_engineering",
            "official_blog_and_docs",
            "product_subbrand_pages",
        }:
            results.extend(
                self._collect_official_surface_publications(
                    identity,
                    publications_dir,
                    max_publications=max_publications,
                    seed_queries=seed_queries,
                    asset_logger=asset_logger,
                )
            )
        remaining_limit = max(max_publications - len(results), 0)
        if remaining_limit > 0 and (not source_families or "publication_platforms" in source_families):
            results.extend(
                self._search_arxiv_publications(
                    identity,
                    publications_dir,
                    remaining_limit,
                    asset_logger=asset_logger,
                )
            )
        return _dedupe_publication_records(results, max_publications)

    def _should_skip_remote_publication_collection(self, request_payload: dict[str, Any]) -> bool:
        external_provider_mode = _external_provider_mode()
        if external_provider_mode in {"simulate", "scripted"}:
            return True
        runtime_timing_overrides = _runtime_timing_overrides_from_request_payload(request_payload)
        return str(runtime_timing_overrides.get("runtime_tuning_profile") or "").strip().lower() == "fast_smoke"

    def _collect_official_surface_publications(
        self,
        identity: CompanyIdentity,
        publications_dir: Path,
        *,
        max_publications: int,
        seed_queries: list[str],
        asset_logger: AssetLogger,
    ) -> list[PublicationRecord]:
        site_root = _identity_site_root(identity)
        if not site_root:
            return []
        official_dir = publications_dir / "official_surfaces"
        official_dir.mkdir(parents=True, exist_ok=True)
        home_url = f"{site_root}/"
        try:
            home_html = _fetch_text(home_url)
        except Exception:
            home_html = ""
        if home_html:
            asset_logger.write_text(
                official_dir / "home.html",
                home_html,
                asset_type="official_surface_home_html",
                source_kind="publication_enrichment",
                content_type="text/html",
                is_raw_asset=True,
                model_safe=False,
                metadata={"url": home_url},
            )
        discovery_manifest: dict[str, Any] = {"site_root": site_root, "seed_queries": seed_queries, "surfaces": []}
        surface_urls = _discover_official_surface_urls(identity, site_root, home_html)
        records: list[PublicationRecord] = []
        for surface_url in surface_urls:
            if len(records) >= max_publications:
                break
            try:
                surface_text = _fetch_text(surface_url)
            except Exception:
                continue
            surface_path = official_dir / _surface_asset_name(surface_url)
            content_type = "application/xml" if surface_url.endswith(".xml") else "text/html"
            asset_logger.write_text(
                surface_path,
                surface_text,
                asset_type="official_surface_payload",
                source_kind="publication_enrichment",
                content_type=content_type,
                is_raw_asset=True,
                model_safe=False,
                metadata={"url": surface_url},
            )
            surface_records: list[PublicationRecord] = []
            if surface_url.endswith(".xml"):
                surface_records = _extract_publications_from_rss(surface_text, surface_url, str(surface_path))
            else:
                surface_records = _extract_publications_from_surface_index(
                    surface_text,
                    surface_url,
                    str(surface_path),
                )
            if not surface_records and surface_url.rstrip("/").endswith(("/tinker", "/research", "/engineering", "/docs")):
                surface_records = _extract_single_surface_page_record(surface_text, surface_url, str(surface_path))
            enriched_records: list[PublicationRecord] = []
            for record in surface_records:
                if len(records) + len(enriched_records) >= max_publications:
                    break
                enriched_records.append(
                    self._hydrate_official_surface_record(record, official_dir, asset_logger=asset_logger)
                )
            discovery_manifest["surfaces"].append(
                {
                    "url": surface_url,
                    "record_count": len(enriched_records),
                }
            )
            records.extend(enriched_records)
        manifest_path = official_dir / "surface_manifest.json"
        asset_logger.write_json(
            manifest_path,
            discovery_manifest,
            asset_type="official_surface_manifest",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        return _dedupe_publication_records(records, max_publications)

    def _hydrate_official_surface_record(
        self,
        record: PublicationRecord,
        official_dir: Path,
        *,
        asset_logger: AssetLogger,
    ) -> PublicationRecord:
        if record.authors and any(_looks_like_person_name(name) for name in record.authors):
            return record
        url = str(record.url or "").strip()
        if not url:
            return record
        try:
            page_html = _fetch_text(url)
        except Exception:
            return record
        raw_path = official_dir / _surface_asset_name(url)
        asset_logger.write_text(
            raw_path,
            page_html,
            asset_type="official_surface_page_html",
            source_kind="publication_enrichment",
            content_type="text/html",
            is_raw_asset=True,
            model_safe=False,
            metadata={"url": url},
        )
        byline_authors = _extract_page_authors(page_html)
        if not byline_authors:
            byline_authors = list(record.authors)
        title = record.title or _extract_page_title(page_html) or record.publication_id
        return PublicationRecord(
            publication_id=record.publication_id,
            source=record.source,
            source_dataset=record.source_dataset,
            source_path=str(raw_path),
            title=title,
            url=url,
            year=record.year or _extract_year_from_html(page_html),
            authors=byline_authors,
            acknowledgement_names=record.acknowledgement_names,
        )

    def _search_arxiv_publications(
        self,
        identity: CompanyIdentity,
        publications_dir: Path,
        max_publications: int,
        *,
        asset_logger: AssetLogger,
    ) -> list[PublicationRecord]:
        queries = [identity.canonical_name]
        if identity.domain:
            domain_root = identity.domain.replace(".com", "").replace(".ai", "").replace(".org", "")
            if domain_root and domain_root not in queries:
                queries.append(domain_root)

        results: list[PublicationRecord] = []
        seen_ids: set[str] = set()
        for query_text in queries:
            search_url = (
                "https://arxiv.org/search/?"
                + parse.urlencode(
                    {
                        "query": query_text,
                        "searchtype": "affiliation",
                        "abstracts": "show",
                        "order": "-announced_date_first",
                        "size": 25,
                    }
                )
            )
            try:
                raw_search = _fetch_text(search_url)
            except Exception:
                continue
            raw_search_path = publications_dir / f"arxiv_search_{normalize_name_token(query_text) or 'query'}.html"
            asset_logger.write_text(
                raw_search_path,
                raw_search,
                asset_type="arxiv_affiliation_search_html",
                source_kind="publication_enrichment",
                content_type="text/html",
                is_raw_asset=True,
                model_safe=False,
                metadata={"query": query_text},
            )
            paper_ids = re.findall(r'href="https://arxiv\.org/abs/([0-9]{4}\.[0-9]{4,5})"', raw_search)
            if not paper_ids:
                paper_ids = re.findall(r'href="/abs/([0-9]{4}\.[0-9]{4,5})"', raw_search)

            for paper_id in paper_ids:
                if paper_id in seen_ids or len(results) >= max_publications:
                    continue
                seen_ids.add(paper_id)
                abs_url = f"https://arxiv.org/abs/{paper_id}"
                try:
                    abs_text = _fetch_text(abs_url)
                except Exception:
                    continue
                abs_path = publications_dir / f"{paper_id}_abs.html"
                asset_logger.write_text(
                    abs_path,
                    abs_text,
                    asset_type="arxiv_abs_html",
                    source_kind="publication_enrichment",
                    content_type="text/html",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"paper_id": paper_id},
                )
                title_match = re.search(r'citation_title" content="([^"]+)"', abs_text)
                author_matches = re.findall(r'citation_author" content="([^"]+)"', abs_text)
                title = unescape(title_match.group(1)).strip() if title_match else paper_id
                year_match = re.search(r'originally announced</span>\s*([A-Za-z]{3},\s+\d{1,2}\s+\d{4})', abs_text)
                year = None
                if year_match:
                    year_digits = re.findall(r"\d{4}", year_match.group(1))
                    if year_digits:
                        year = int(year_digits[0])
                acknowledgement_names: list[str] = []
                html_url = f"https://arxiv.org/html/{paper_id}"
                try:
                    html_text = _fetch_text(html_url)
                    html_path = publications_dir / f"{paper_id}_full.html"
                    asset_logger.write_text(
                        html_path,
                        html_text,
                        asset_type="arxiv_full_html",
                        source_kind="publication_enrichment",
                        content_type="text/html",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={"paper_id": paper_id},
                    )
                    acknowledgement_names = extract_acknowledgement_names_from_html(html_text)
                except Exception:
                    acknowledgement_names = []

                results.append(
                    PublicationRecord(
                        publication_id=paper_id,
                        source="arxiv_affiliation_search",
                        source_dataset=f"{identity.company_key}_arxiv_affiliation",
                        source_path=str(abs_path),
                        title=title,
                        url=abs_url,
                        year=year,
                        authors=[unescape(item).strip() for item in author_matches if unescape(item).strip()],
                        acknowledgement_names=acknowledgement_names,
                    )
                )
                if len(results) >= max_publications:
                    break
            if len(results) >= max_publications:
                break
        return results

    def _collect_roster_anchored_scholar_coauthors(
        self,
        identity: CompanyIdentity,
        publications_dir: Path,
        candidates: list[Candidate],
        *,
        publications: list[PublicationRecord] | None = None,
        existing_evidence: list[EvidenceRecord] | None = None,
        asset_logger: AssetLogger,
    ) -> dict[str, Any]:
        scholar_dir = publications_dir / "roster_anchored_scholar_coauthors"
        scholar_dir.mkdir(parents=True, exist_ok=True)
        selected_seeds = _select_roster_anchored_scholar_seed_candidates(
            candidates,
            publications=publications or [],
            existing_evidence=existing_evidence or [],
            max_seeds=8,
        )
        seed_candidates = [item["candidate"] for item in selected_seeds]
        if not seed_candidates:
            return {"evidence": [], "prospect_candidates": [], "artifact_paths": {}}

        candidate_map = _candidate_name_map(candidates)
        evidence: list[EvidenceRecord] = []
        prospect_map: dict[str, dict[str, Any]] = {}
        scholar_edge_map: dict[tuple[str, str], dict[str, Any]] = {}
        seed_results: list[dict[str, Any]] = []
        seed_roster_records: list[dict[str, Any]] = []
        seed_publications: list[dict[str, Any]] = []
        recent_year_min = max(datetime.now(timezone.utc).year - 2, 2024)

        for seed_spec in selected_seeds:
            seed_candidate = seed_spec["candidate"]
            seed_signals = dict(seed_spec["selection_signals"])
            search_year_min = recent_year_min
            seed_publication_records = self._search_roster_anchored_scholar_publications(
                seed_candidate,
                identity,
                scholar_dir,
                max_publications=8,
                recent_year_min=search_year_min,
                asset_logger=asset_logger,
            )
            expanded_year_min = max(search_year_min - 2, 2022)
            if not seed_publication_records and seed_signals.get("publication_signal_count", 0) > 0 and expanded_year_min < search_year_min:
                search_year_min = expanded_year_min
                seed_publication_records = self._search_roster_anchored_scholar_publications(
                    seed_candidate,
                    identity,
                    scholar_dir,
                    max_publications=8,
                    recent_year_min=search_year_min,
                    asset_logger=asset_logger,
                )
            publication_topics = sorted(
                {
                    topic
                    for publication in seed_publication_records
                    for topic in publication.topics
                    if str(topic or "").strip()
                }
            )
            seed_results.append(
                {
                    "seed_candidate_id": seed_candidate.candidate_id,
                    "seed_name": seed_candidate.display_name or seed_candidate.name_en,
                    "paper_count": len(seed_publication_records),
                    "query_recent_year_min": search_year_min,
                    "titles": [item.title for item in seed_publication_records[:8]],
                    "topics": publication_topics[:10],
                    "selection_score": int(seed_spec["selection_score"]),
                    "selection_signals": seed_signals,
                }
            )
            seed_display_name = seed_candidate.display_name or seed_candidate.name_en
            seed_roster_records.append(
                {
                    **seed_candidate.to_record(),
                    "selection_score": int(seed_spec["selection_score"]),
                    "selection_signals": seed_signals,
                }
            )
            seed_publications.append(
                {
                    "seed_candidate_id": seed_candidate.candidate_id,
                    "seed_name": seed_display_name,
                    "selection_score": int(seed_spec["selection_score"]),
                    "selection_signals": seed_signals,
                    "query_recent_year_min": search_year_min,
                    "publications": [
                        _build_seed_publication_record(seed_candidate, publication) for publication in seed_publication_records
                    ],
                }
            )
            for publication in seed_publication_records:
                paper_ref = _build_scholar_publication_reference(publication)
                for author_name in _dedupe_names(publication.authors):
                    if _name_key(author_name) == _candidate_key(seed_candidate):
                        continue
                    if not _looks_like_person_name(author_name):
                        continue
                    matched_candidate = candidate_map.get(_name_key(author_name))
                    if matched_candidate is not None:
                        matched_name = matched_candidate.display_name or matched_candidate.name_en
                        _record_scholar_edge(
                            scholar_edge_map,
                            source_name=seed_display_name,
                            target_name=matched_name,
                            source_candidate_id=seed_candidate.candidate_id,
                            target_candidate_id=matched_candidate.candidate_id,
                            target_type="roster_member",
                            paper_ref=paper_ref,
                        )
                        evidence.append(
                            EvidenceRecord(
                                evidence_id=make_evidence_id(
                                    matched_candidate.candidate_id,
                                    publication.source_dataset,
                                    f"{publication.title} scholar coauthor with {seed_display_name}",
                                    publication.url,
                                ),
                                candidate_id=matched_candidate.candidate_id,
                                source_type="scholar_coauthor",
                                title=publication.title,
                                url=publication.url,
                                summary=f"{matched_name} co-authored this scholar publication with confirmed roster member {seed_display_name}.",
                                source_dataset=publication.source_dataset,
                                source_path=publication.source_path,
                                metadata={
                                    "publication_id": publication.publication_id,
                                    "seed_candidate_id": seed_candidate.candidate_id,
                                    "seed_name": seed_display_name,
                                    "matched_name": author_name,
                                },
                            )
                        )
                        continue

                    key = _name_key(author_name)
                    prospect = prospect_map.setdefault(
                        key,
                        {
                            "name": author_name,
                            "seed_names": [],
                            "papers": [],
                        },
                    )
                    if seed_display_name not in prospect["seed_names"]:
                        prospect["seed_names"].append(seed_display_name)
                    _record_scholar_edge(
                        scholar_edge_map,
                        source_name=seed_display_name,
                        target_name=author_name,
                        source_candidate_id=seed_candidate.candidate_id,
                        target_candidate_id="",
                        target_type="prospect",
                        paper_ref=paper_ref,
                    )
                    paper_record = dict(paper_ref)
                    paper_record["seed_name"] = seed_display_name
                    if paper_record not in prospect["papers"]:
                        prospect["papers"].append(paper_record)

        graph_path = scholar_dir / "scholar_coauthor_graph.json"
        prospects_path = scholar_dir / "scholar_coauthor_prospects.json"
        seed_roster_path = scholar_dir / "seed_roster.json"
        seed_publications_path = scholar_dir / "seed_publications.json"
        summary_path = scholar_dir / "summary.json"
        prospect_candidates = [
            _build_roster_anchored_scholar_coauthor_prospect(
                identity,
                prospect,
                source_path=prospects_path,
            )
            for prospect in sorted(
                prospect_map.values(),
                key=lambda item: (-len(item["seed_names"]), -len(item["papers"]), item["name"].lower()),
            )
        ]
        graph_edges = [
            {
                **edge,
                "paper_count": len(edge["papers"]),
            }
            for _, edge in sorted(
                scholar_edge_map.items(),
                key=lambda item: (
                    -len(item[1]["papers"]),
                    item[1]["source"].lower(),
                    item[1]["target"].lower(),
                ),
            )
        ]
        asset_logger.write_json(
            graph_path,
            graph_edges,
            asset_type="scholar_coauthor_graph",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        asset_logger.write_json(
            seed_roster_path,
            seed_roster_records,
            asset_type="scholar_coauthor_seed_roster",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        asset_logger.write_json(
            seed_publications_path,
            seed_publications,
            asset_type="scholar_coauthor_seed_publications",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        asset_logger.write_json(
            prospects_path,
            [candidate.to_record() for candidate in prospect_candidates],
            asset_type="scholar_coauthor_prospects",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        asset_logger.write_json(
            summary_path,
            {
                "target_company": identity.canonical_name,
                "seed_count": len(seed_candidates),
                "recent_year_min": recent_year_min,
                "seed_results": seed_results,
                "selection_strategy": "publication_signal_first",
                "graph_edge_count": len(graph_edges),
                "internal_overlap_count": sum(1 for edge in graph_edges if edge["target_type"] == "roster_member"),
                "prospect_count": len(prospect_candidates),
            },
            asset_type="scholar_coauthor_summary",
            source_kind="publication_enrichment",
            is_raw_asset=False,
            model_safe=True,
        )
        return {
            "evidence": evidence,
            "prospect_candidates": prospect_candidates,
            "artifact_paths": {
                "scholar_coauthor_graph": str(graph_path),
                "scholar_coauthor_seed_roster": str(seed_roster_path),
                "scholar_coauthor_seed_publications": str(seed_publications_path),
                "scholar_coauthor_prospects": str(prospects_path),
                "scholar_coauthor_summary": str(summary_path),
            },
        }

    def _search_roster_anchored_scholar_publications(
        self,
        seed_candidate: Candidate,
        identity: CompanyIdentity,
        scholar_dir: Path,
        *,
        max_publications: int,
        recent_year_min: int,
        asset_logger: AssetLogger,
    ) -> list[PublicationRecord]:
        seed_name = (seed_candidate.display_name or seed_candidate.name_en).strip()
        if not seed_name:
            return []
        query = f'au:"{seed_name}"'
        feed_url = "https://export.arxiv.org/api/query?" + parse.urlencode(
            {
                "search_query": query,
                "start": 0,
                "max_results": max_publications,
                "sortBy": "submittedDate",
                "sortOrder": "descending",
            }
        )
        try:
            raw_feed = _fetch_text(feed_url)
        except Exception:
            return []
        raw_path = scholar_dir / f"{seed_candidate.candidate_id}_arxiv_author_feed.xml"
        asset_logger.write_text(
            raw_path,
            raw_feed,
            asset_type="scholar_author_feed_xml",
            source_kind="publication_enrichment",
            content_type="application/xml",
            is_raw_asset=True,
            model_safe=False,
            metadata={"seed_candidate_id": seed_candidate.candidate_id, "seed_name": seed_name, "url": feed_url},
        )
        try:
            root = ET.fromstring(raw_feed)
        except ET.ParseError:
            return []
        namespace = {"atom": "http://www.w3.org/2005/Atom"}
        results: list[PublicationRecord] = []
        seen: set[str] = set()
        for entry in root.findall("atom:entry", namespace):
            title = " ".join((entry.findtext("atom:title", default="", namespaces=namespace) or "").split()).strip()
            url = str(entry.findtext("atom:id", default="", namespaces=namespace) or "").strip()
            published = str(entry.findtext("atom:published", default="", namespaces=namespace) or "").strip()
            abstract = " ".join((entry.findtext("atom:summary", default="", namespaces=namespace) or "").split()).strip()
            year = None
            if published[:4].isdigit():
                year = int(published[:4])
            if year is not None and year < recent_year_min:
                continue
            authors = [
                " ".join((author.findtext("atom:name", default="", namespaces=namespace) or "").split()).strip()
                for author in entry.findall("atom:author", namespace)
            ]
            authors = [item for item in authors if item]
            topics = []
            for category in entry.findall("atom:category", namespace):
                term = str(category.attrib.get("term") or "").strip()
                if term and term not in topics:
                    topics.append(term)
            publication_id = url.rsplit("/", 1)[-1] if url else sha1(title.encode("utf-8")).hexdigest()[:12]
            if publication_id in seen or not title or len(authors) < 2:
                continue
            seen.add(publication_id)
            results.append(
                PublicationRecord(
                    publication_id=publication_id,
                    source="arxiv_author_seed_search",
                    source_dataset=f"{identity.company_key}_roster_anchored_scholar",
                    source_path=str(raw_path),
                    title=title,
                    url=url,
                    year=year,
                    authors=authors,
                    acknowledgement_names=[],
                    abstract=abstract,
                    topics=topics,
                )
            )
            if len(results) >= max_publications:
                break
        return results


def _select_roster_anchored_scholar_seed_candidates(
    candidates: list[Candidate],
    *,
    publications: list[PublicationRecord],
    existing_evidence: list[EvidenceRecord],
    max_seeds: int,
) -> list[dict[str, Any]]:
    eligible = [
        candidate
        for candidate in candidates
        if candidate.category in {"employee", "former_employee"} and _looks_like_person_name(candidate.display_name or candidate.name_en)
    ]
    if not eligible:
        return []

    candidate_map = _candidate_name_map(eligible)
    publication_author_counts: dict[str, int] = {}
    publication_ack_counts: dict[str, int] = {}
    for publication in publications:
        for name in publication.authors:
            matched_candidate = candidate_map.get(_name_key(name))
            if matched_candidate is None:
                continue
            publication_author_counts[matched_candidate.candidate_id] = publication_author_counts.get(matched_candidate.candidate_id, 0) + 1
        for name in publication.acknowledgement_names:
            matched_candidate = candidate_map.get(_name_key(name))
            if matched_candidate is None:
                continue
            publication_ack_counts[matched_candidate.candidate_id] = publication_ack_counts.get(matched_candidate.candidate_id, 0) + 1

    evidence_counts: dict[str, dict[str, int]] = {}
    for item in existing_evidence:
        candidate_counts = evidence_counts.setdefault(item.candidate_id, {})
        candidate_counts[item.source_type] = candidate_counts.get(item.source_type, 0) + 1

    ranked: list[dict[str, Any]] = []
    for candidate in eligible:
        metadata = dict(candidate.metadata or {})
        candidate_evidence_counts = evidence_counts.get(candidate.candidate_id, {})
        publication_metadata_count = int(bool(str(metadata.get("publication_id") or "").strip() or str(metadata.get("publication_url") or "").strip()))
        current_publication_author_matches = int(publication_author_counts.get(candidate.candidate_id, 0))
        current_publication_ack_matches = int(publication_ack_counts.get(candidate.candidate_id, 0))
        existing_publication_author_evidence = int(candidate_evidence_counts.get("publication_author", 0))
        existing_publication_ack_evidence = int(candidate_evidence_counts.get("publication_acknowledgement", 0))
        existing_profile_publication_evidence = int(candidate_evidence_counts.get("linkedin_profile_publication", 0))
        manual_confirmed = str(metadata.get("membership_review_decision") or "").strip() == "manual_confirmed_member"

        score = _candidate_priority(candidate)
        score += publication_metadata_count * 80
        score += current_publication_author_matches * 36
        score += current_publication_ack_matches * 20
        score += existing_publication_author_evidence * 28
        score += existing_publication_ack_evidence * 16
        score += existing_profile_publication_evidence * 18
        if manual_confirmed:
            score += 6
        if candidate.linkedin_url:
            score += 2

        publication_signal_count = (
            publication_metadata_count
            + current_publication_author_matches
            + current_publication_ack_matches
            + existing_publication_author_evidence
            + existing_publication_ack_evidence
            + existing_profile_publication_evidence
        )
        reasons: list[str] = []
        if publication_metadata_count:
            reasons.append("candidate_metadata_publication")
        if current_publication_author_matches:
            reasons.append("current_publication_author_match")
        if current_publication_ack_matches:
            reasons.append("current_publication_acknowledgement_match")
        if existing_publication_author_evidence:
            reasons.append("historical_publication_author_evidence")
        if existing_publication_ack_evidence:
            reasons.append("historical_publication_acknowledgement_evidence")
        if existing_profile_publication_evidence:
            reasons.append("linkedin_profile_publication")
        if manual_confirmed:
            reasons.append("manual_confirmed_member")

        ranked.append(
            {
                "candidate": candidate,
                "selection_score": score,
                "selection_signals": {
                    "publication_signal_count": publication_signal_count,
                    "candidate_priority": _candidate_priority(candidate),
                    "publication_metadata_count": publication_metadata_count,
                    "current_publication_author_matches": current_publication_author_matches,
                    "current_publication_ack_matches": current_publication_ack_matches,
                    "existing_publication_author_evidence": existing_publication_author_evidence,
                    "existing_publication_ack_evidence": existing_publication_ack_evidence,
                    "existing_profile_publication_evidence": existing_profile_publication_evidence,
                    "manual_confirmed_member": manual_confirmed,
                    "selection_reasons": reasons,
                },
            }
        )

    ranked.sort(
        key=lambda item: (
            -int(item["selection_score"]),
            -int(item["selection_signals"]["publication_signal_count"]),
            -int(item["selection_signals"]["current_publication_author_matches"]),
            -int(item["selection_signals"]["existing_profile_publication_evidence"]),
            (item["candidate"].display_name or item["candidate"].name_en).lower(),
        )
    )
    return ranked[:max_seeds]


def _build_seed_publication_record(seed_candidate: Candidate, publication: PublicationRecord) -> dict[str, Any]:
    return {
        **publication.to_record(),
        "seed_candidate_id": seed_candidate.candidate_id,
        "seed_name": seed_candidate.display_name or seed_candidate.name_en,
        "coauthors": [
            name
            for name in _dedupe_names(publication.authors)
            if _name_key(name) != _candidate_key(seed_candidate)
        ],
        "research_direction": list(publication.topics[:8]),
        "abstract_excerpt": publication.abstract[:500],
    }


def _build_scholar_publication_reference(publication: PublicationRecord) -> dict[str, Any]:
    return {
        "publication_id": publication.publication_id,
        "title": publication.title,
        "url": publication.url,
        "year": publication.year,
        "topics": list(publication.topics[:8]),
    }


def _record_scholar_edge(
    edge_map: dict[tuple[str, str], dict[str, Any]],
    *,
    source_name: str,
    target_name: str,
    source_candidate_id: str,
    target_candidate_id: str,
    target_type: str,
    paper_ref: dict[str, Any],
) -> None:
    key = (source_name, target_name)
    edge = edge_map.setdefault(
        key,
        {
            "source": source_name,
            "target": target_name,
            "edge_type": "scholar_coauthor",
            "source_candidate_id": source_candidate_id,
            "target_candidate_id": target_candidate_id,
            "target_type": target_type,
            "papers": [],
        },
    )
    if paper_ref not in edge["papers"]:
        edge["papers"].append(paper_ref)


def extract_linkedin_profile_urls_from_search_html(html_text: str) -> list[str]:
    raw_urls = re.findall(r'result__a" href="([^"]+)"', html_text)
    results: list[str] = []
    for raw_url in raw_urls:
        candidate = unescape(raw_url)
        if "duckduckgo.com/l/" in candidate:
            parsed = parse.urlparse(candidate)
            query = parse.parse_qs(parsed.query)
            candidate = query.get("uddg", [candidate])[0]
        if "linkedin.com/in/" not in candidate:
            continue
        if candidate not in results:
            results.append(candidate)
    return results


def extract_linkedin_slug(url: str) -> str:
    match = re.search(r"linkedin\.com/in/([^/?#]+)", url)
    if not match:
        return ""
    return match.group(1).strip()


def build_people_search_url(account: RapidApiAccount, query: str, *, limit: int = 5) -> str:
    base = account.base_url.rstrip("/")
    if "z-real-time-linkedin-scraper-api1" in account.host:
        endpoint = base[:-len("/api/search/people")] if base.endswith("/api/search/people") else base
        return endpoint + "/api/search/people?" + parse.urlencode({"keywords": query, "limit": limit})
    endpoint_path = str(account.endpoint_search or "/api/search/people").split("?", 1)[0]
    return base + endpoint_path + "?" + parse.urlencode({"keywords": query, "limit": limit})


def build_basic_profile_url(account: RapidApiAccount, username: str) -> str:
    base = account.base_url.rstrip("/")
    if "z-real-time-linkedin-scraper-api1" in account.host:
        endpoint = base[:-len("/api/profile")] if base.endswith("/api/profile") else base
        return endpoint + "/api/profile?" + parse.urlencode({"username": username})
    endpoint_path = "/api/profile"
    return base + endpoint_path + "?" + parse.urlencode({"username": username})


def build_profile_detail_url(account: RapidApiAccount, slug: str) -> str:
    host = account.host
    base = account.base_url.rstrip("/")
    if "real-time-linkedin-data-scraper-api" in host:
        endpoint = base[:-len("/people/profile")] if base.endswith("/people/profile") else base
        return endpoint + "/people/profile?" + parse.urlencode(
            {
                "profile_id": slug,
                "bypass_cache": "false",
                "include_contact_info": "false",
                "include_network_info": "false",
            }
        )
    endpoint = base[:-len("/profile/detail")] if base.endswith("/profile/detail") else base
    return endpoint + "/profile/detail?" + parse.urlencode({"username": slug})


def extract_search_people_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    container = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    rows = list((container or {}).get("data") or [])
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "urn": str(row.get("urn") or "").strip(),
                "full_name": str(row.get("fullName") or row.get("full_name") or "").strip(),
                "headline": str(row.get("headline") or "").strip(),
                "location": str(row.get("location") or "").strip(),
            }
        )
    return normalized


def parse_basic_linkedin_profile_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    first_name = str(data.get("firstName") or data.get("first_name") or "").strip()
    last_name = str(data.get("lastName") or data.get("last_name") or "").strip()
    username = str(data.get("username") or data.get("public_identifier") or "").strip()
    headline = str(data.get("headline") or "").strip()
    location = ""
    raw_location = data.get("location") or {}
    if isinstance(raw_location, dict):
        location = str(raw_location.get("locationName") or raw_location.get("locationShortName") or "").strip()
    else:
        location = str(raw_location or "").strip()
    profile_url = str(data.get("profileUrl") or data.get("profile_url") or "").strip()
    if not profile_url and username:
        profile_url = f"https://www.linkedin.com/in/{username}/"
    full_name = " ".join(part for part in [first_name, last_name] if part).strip()
    return {
        "full_name": full_name,
        "username": username,
        "headline": headline,
        "location": location,
        "profile_url": profile_url,
        "urn": str(data.get("urn") or "").strip(),
    }


def parse_linkedin_profile_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if "raw" in payload:
        payload = payload["raw"]
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    basic_info = data.get("basic_info") if isinstance(data.get("basic_info"), dict) else data
    experience = list(data.get("experience") or [])
    education = list(data.get("education") or [])
    languages = list(data.get("languages") or basic_info.get("languages") or [])
    skills = list(data.get("skills") or basic_info.get("skills") or [])
    publications = list(data.get("publications") or [])

    first_name = str(basic_info.get("first_name") or data.get("first_name") or "").strip()
    last_name = str(basic_info.get("last_name") or data.get("last_name") or "").strip()
    full_name = str(basic_info.get("fullname") or "").strip() or " ".join(part for part in [first_name, last_name] if part).strip()
    headline = str(basic_info.get("headline") or data.get("headline") or "").strip()
    profile_url = str(basic_info.get("profile_url") or data.get("profile_url") or "").strip()
    public_identifier = str(basic_info.get("public_identifier") or data.get("public_identifier") or "").strip()
    summary = str(basic_info.get("about") or data.get("summary") or "").strip()

    location = ""
    raw_location = basic_info.get("location") or data.get("location") or {}
    if isinstance(raw_location, dict):
        location = str(raw_location.get("full") or raw_location.get("name") or "").strip()
    else:
        location = str(raw_location or "").strip()

    current_company = ""
    for item in experience:
        if item.get("is_current"):
            companies = _experience_company_labels(item)
            current_company = companies[0] if companies else ""
            break
    if not current_company and experience:
        companies = _experience_company_labels(experience[0])
        current_company = companies[0] if companies else ""
    if not current_company:
        current_company = str(basic_info.get("current_company") or "").strip()

    return {
        "full_name": full_name,
        "first_name": first_name,
        "last_name": last_name,
        "headline": headline,
        "profile_url": profile_url,
        "public_identifier": public_identifier,
        "summary": summary,
        "location": location,
        "current_company": current_company,
        "experience": experience,
        "education": education,
        "languages": languages,
        "skills": skills,
        "publications": publications,
    }


def extract_acknowledgement_names_from_html(html_text: str) -> list[str]:
    if len(html_text) < 5000:
        return []
    lower_text = html_text.lower()
    start = lower_text.find("acknowledg")
    if start < 0:
        return []
    snippet = html_text[start : start + 12000]
    text = re.sub(r"<[^>]+>", " ", snippet)
    text = unescape(text)
    text = " ".join(text.split())
    names = re.findall(r"\b[A-Z][a-z]+(?:\s+[A-Z][A-Za-z\-\']+)+\b", text)
    filtered: list[str] = []
    for name in names:
        lowered = name.lower()
        if lowered in ACK_STOPWORDS:
            continue
        if any(token in lowered for token in ACK_STOPWORDS):
            continue
        if name not in filtered:
            filtered.append(name)
    return filtered[:20]


def _build_people_search_queries(candidate: Candidate, identity: CompanyIdentity) -> list[str]:
    name = candidate.display_name or candidate.name_en
    compact_role = re.sub(r"[\|\(\)@]+", " ", (candidate.role or "").replace(identity.canonical_name, "")).strip()
    compact_role = " ".join(compact_role.split()[:4])
    queries = [
        f"{name} {identity.canonical_name}".strip(),
        name,
    ]
    if compact_role:
        queries.append(f"{name} {compact_role}".strip())

    deduped: list[str] = []
    for query in queries:
        normalized = " ".join(query.split())
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped


def _build_slug_queries(candidate: Candidate, identity: CompanyIdentity) -> list[str]:
    name = candidate.display_name or candidate.name_en
    compact_role = re.sub(r"[\|\(\)@]+", " ", (candidate.role or "").replace(identity.canonical_name, "")).strip()
    compact_role = " ".join(compact_role.split()[:4])
    queries = [
        f'"{name}" "{identity.canonical_name}" LinkedIn',
        f"{name} {identity.canonical_name} LinkedIn",
        f'"{name}" LinkedIn',
    ]
    if compact_role:
        role_fragment = compact_role
        queries.append(f'"{name}" "{role_fragment}" LinkedIn')
    return queries


def _search_result_name_matches_candidate(row: dict[str, Any], candidate: Candidate) -> bool:
    row_name = str(row.get("full_name") or "").strip()
    if not row_name:
        return False
    return _names_match(candidate.name_en, row_name)


def _profile_matches_candidate(
    profile: dict[str, Any],
    candidate: Candidate,
    identity: CompanyIdentity,
    *,
    model_client: ModelClient | None = None,
) -> bool:
    identifiers_overlap = bool(_candidate_profile_identifiers(candidate) & _profile_identifiers(profile))
    full_name = str(profile.get("full_name") or "").strip()
    if identifiers_overlap and (not full_name or _names_match(candidate.name_en, full_name)):
        return True
    if not _names_match(candidate.name_en, full_name):
        return False
    resolved_category, _ = _classify_profile_membership(profile, identity, model_client=model_client)
    return resolved_category in {"employee", "former_employee"}


def _merge_profile_into_candidate(
    candidate: Candidate,
    profile: dict[str, Any],
    raw_path: Path,
    account_id: str,
    identity: CompanyIdentity,
    *,
    model_client: ModelClient | None = None,
    membership_review: dict[str, Any] | None = None,
) -> Candidate:
    resolved_category, resolved_employment_status = _classify_profile_membership(
        profile,
        identity,
        model_client=model_client,
    )
    can_refresh_membership = candidate.category in {"lead", "employee", "former_employee"}
    membership_review = dict(membership_review or {})
    review_required = bool(membership_review.get("requires_manual_review"))
    review_note = str(membership_review.get("note") or "").strip()
    review_rationale = str(membership_review.get("rationale") or "").strip()
    incoming = Candidate(
        candidate_id=candidate.candidate_id,
        name_en=profile.get("full_name") or candidate.name_en,
        display_name=profile.get("full_name") or candidate.display_name,
        category=resolved_category if can_refresh_membership else candidate.category,
        target_company=candidate.target_company,
        organization=candidate.organization,
        employment_status=resolved_employment_status if can_refresh_membership else candidate.employment_status,
        role=profile.get("headline") or candidate.role,
        team=candidate.team or _infer_team_from_text(profile.get("headline", "")),
        focus_areas=candidate.focus_areas or profile.get("headline", ""),
        education=_format_education(profile.get("education", [])),
        work_history=_format_experience(profile.get("experience", [])),
        notes=_join_nonempty(candidate.notes, review_note, review_rationale, profile.get("summary", ""), profile.get("location", "")),
        linkedin_url=profile.get("profile_url", ""),
        source_dataset=candidate.source_dataset,
        source_path=str(raw_path),
        metadata={
            **candidate.metadata,
            "public_identifier": profile.get("public_identifier", ""),
            "profile_account_id": account_id,
            "profile_location": profile.get("location", ""),
            "more_profiles": list(profile.get("more_profiles", []) or []),
            "headline": profile.get("headline", ""),
            "summary": profile.get("summary", ""),
            "languages": _format_profile_languages(profile.get("languages", [])),
            "skills": _format_profile_skills(profile.get("skills", [])),
            "membership_claim_category": resolved_category,
            "membership_claim_employment_status": resolved_employment_status,
            "membership_review_required": review_required,
            "membership_review_reason": "suspicious_membership" if review_required else "",
            "membership_review_decision": str(membership_review.get("decision") or "").strip(),
            "membership_review_confidence": str(membership_review.get("confidence_label") or "").strip(),
            "membership_review_rationale": review_rationale,
            "membership_review_triggers": list(membership_review.get("trigger_reasons") or []),
            "membership_review_trigger_keywords": list(membership_review.get("trigger_keywords") or []),
        },
    )
    merged = merge_candidate(candidate, incoming)
    merged_record = merged.to_record()
    if can_refresh_membership:
        merged_record["category"] = resolved_category
        merged_record["employment_status"] = resolved_employment_status
    if profile.get("profile_url"):
        merged_record["linkedin_url"] = profile["profile_url"]
    if profile.get("headline"):
        merged_record["focus_areas"] = _join_nonempty(merged.focus_areas, profile["headline"])
    if incoming.education:
        merged_record["education"] = incoming.education
    if incoming.work_history:
        merged_record["work_history"] = incoming.work_history
    merged_record["notes"] = incoming.notes
    merged_record["source_path"] = str(raw_path)
    merged_record["metadata"] = incoming.metadata
    return Candidate(**merged_record)


def _profile_evidence_record(
    candidate: Candidate,
    profile: dict[str, Any],
    raw_path: Path,
    *,
    membership_review: dict[str, Any] | None = None,
) -> EvidenceRecord:
    title = profile.get("headline") or "LinkedIn profile detail"
    url = profile.get("profile_url") or candidate.linkedin_url
    membership_review = dict(membership_review or {})
    if membership_review.get("requires_manual_review"):
        summary = f"LinkedIn profile detail captured for {candidate.display_name}; target-company membership requires manual review."
    else:
        summary = f"LinkedIn profile detail validated {candidate.display_name} at {candidate.target_company}."
    return EvidenceRecord(
        evidence_id=make_evidence_id(candidate.candidate_id, "linkedin_profile_detail", title, url or str(raw_path)),
        candidate_id=candidate.candidate_id,
        source_type="linkedin_profile_detail",
        title=title,
        url=url,
        summary=summary,
        source_dataset="linkedin_profile_detail",
        source_path=str(raw_path),
        metadata={
            "public_identifier": profile.get("public_identifier", ""),
            "membership_review_required": bool(membership_review.get("requires_manual_review")),
            "membership_review_decision": str(membership_review.get("decision") or "").strip(),
        },
    )


def _profile_membership_review_evidence_record(
    candidate: Candidate,
    profile: dict[str, Any],
    raw_path: Path,
    review: dict[str, Any],
) -> EvidenceRecord:
    title = profile.get("headline") or "LinkedIn membership review"
    url = profile.get("profile_url") or candidate.linkedin_url
    rationale = str(review.get("rationale") or "").strip()
    summary = (
        f"LinkedIn membership review flagged {candidate.display_name} for manual review."
        if review.get("requires_manual_review")
        else f"LinkedIn membership review completed for {candidate.display_name}."
    )
    if rationale:
        summary = _join_nonempty(summary, rationale)
    return EvidenceRecord(
        evidence_id=make_evidence_id(candidate.candidate_id, "linkedin_profile_membership_review", title, url or str(raw_path)),
        candidate_id=candidate.candidate_id,
        source_type="linkedin_profile_membership_review",
        title=title,
        url=url,
        summary=summary,
        source_dataset="linkedin_profile_membership_review",
        source_path=str(raw_path),
        metadata={
            "decision": str(review.get("decision") or "").strip(),
            "confidence_label": str(review.get("confidence_label") or "").strip(),
            "trigger_reasons": list(review.get("trigger_reasons") or []),
            "trigger_keywords": list(review.get("trigger_keywords") or []),
        },
    )


def _profile_non_member_evidence_record(candidate: Candidate, profile: dict[str, Any], raw_path: Path) -> EvidenceRecord:
    title = profile.get("headline") or "LinkedIn profile detail"
    url = profile.get("profile_url") or candidate.linkedin_url
    summary = f"LinkedIn profile detail did not confirm {candidate.display_name} as a {candidate.target_company} member."
    return EvidenceRecord(
        evidence_id=make_evidence_id(candidate.candidate_id, "linkedin_profile_non_member", title, url or str(raw_path)),
        candidate_id=candidate.candidate_id,
        source_type="linkedin_profile_non_member",
        title=title,
        url=url,
        summary=summary,
        source_dataset="linkedin_profile_non_member",
        source_path=str(raw_path),
        metadata={"public_identifier": profile.get("public_identifier", "")},
    )


def _extract_seed_slug(candidate: Candidate) -> str:
    seed_slug = str(candidate.metadata.get("seed_slug") or "").strip()
    if seed_slug:
        return seed_slug
    return extract_linkedin_slug(candidate.linkedin_url)


def _apply_verified_profile(
    candidate: Candidate,
    profile: dict[str, Any],
    raw_path: Path,
    account_id: str,
    slug: str,
    identity: CompanyIdentity,
    *,
    model_client: ModelClient | None = None,
    resolution_source: str,
) -> tuple[Candidate, dict[str, Any], list[EvidenceRecord]]:
    resolved_category, resolved_employment_status = _classify_profile_membership(
        profile,
        identity,
        model_client=model_client,
    )
    membership_review = _review_profile_membership(
        profile,
        identity,
        resolved_category=resolved_category,
        resolved_employment_status=resolved_employment_status,
        model_client=model_client,
    )
    if str(membership_review.get("decision") or "").strip() == "non_member":
        return _apply_non_member_profile(candidate, profile, raw_path, account_id)
    merged_candidate = _merge_profile_into_candidate(
        candidate,
        profile,
        raw_path,
        account_id,
        identity,
        model_client=model_client,
        membership_review=membership_review,
    )
    resolved_profile = {
        "candidate_id": candidate.candidate_id,
        "display_name": candidate.display_name,
        "slug": slug,
        "account_id": account_id,
        "profile_url": profile.get("profile_url", ""),
        "raw_path": str(raw_path),
        "resolution_source": resolution_source,
        "resolved_category": resolved_category,
        "resolved_employment_status": resolved_employment_status,
        "membership_review_required": bool(membership_review.get("requires_manual_review")),
        "membership_review_decision": str(membership_review.get("decision") or "").strip(),
        "membership_review_rationale": str(membership_review.get("rationale") or "").strip(),
    }
    evidence = [_profile_evidence_record(merged_candidate, profile, raw_path, membership_review=membership_review)]
    if membership_review.get("triggered"):
        evidence.append(_profile_membership_review_evidence_record(merged_candidate, profile, raw_path, membership_review))
    for publication_item in profile.get("publications", [])[:5]:
        publication_url = str(publication_item.get("url", "")).strip()
        publication_title = str(publication_item.get("title") or publication_item.get("name") or "LinkedIn publication").strip()
        if not publication_url or not publication_title:
            continue
        evidence.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    merged_candidate.candidate_id,
                    "linkedin_profile_publication",
                    publication_title,
                    publication_url,
                ),
                candidate_id=merged_candidate.candidate_id,
                source_type="linkedin_profile_publication",
                title=publication_title,
                url=publication_url,
                summary=f"{merged_candidate.display_name} listed this publication on LinkedIn.",
                source_dataset="linkedin_profile_publication",
                source_path=str(raw_path),
                metadata={"slug": slug, "account_id": account_id, "resolution_source": resolution_source},
            )
        )
    return merged_candidate, resolved_profile, evidence


def _apply_non_member_profile(
    candidate: Candidate,
    profile: dict[str, Any],
    raw_path: Path,
    account_id: str,
) -> tuple[Candidate, dict[str, Any], list[EvidenceRecord]]:
    incoming = Candidate(
        candidate_id=candidate.candidate_id,
        name_en=profile.get("full_name") or candidate.name_en,
        display_name=profile.get("full_name") or candidate.display_name,
        category="non_member",
        target_company=candidate.target_company,
        organization=profile.get("current_company") or candidate.organization,
        employment_status="",
        role=profile.get("headline") or candidate.role,
        team=candidate.team or _infer_team_from_text(profile.get("headline", "")),
        focus_areas=candidate.focus_areas or profile.get("headline", ""),
        education=_format_education(profile.get("education", [])),
        work_history=_format_experience(profile.get("experience", [])),
        notes=_join_nonempty(
            candidate.notes,
            "Profile detail fetched but target-company experience was not confirmed.",
            profile.get("summary", ""),
            profile.get("location", ""),
        ),
        linkedin_url=profile.get("profile_url", "") or candidate.linkedin_url,
        source_dataset=candidate.source_dataset,
        source_path=str(raw_path),
        metadata={
            **candidate.metadata,
            "public_identifier": profile.get("public_identifier", ""),
            "profile_account_id": account_id,
            "profile_location": profile.get("location", ""),
            "more_profiles": list(profile.get("more_profiles", []) or []),
            "headline": profile.get("headline", ""),
            "summary": profile.get("summary", ""),
            "languages": _format_profile_languages(profile.get("languages", [])),
            "skills": _format_profile_skills(profile.get("skills", [])),
            "target_company_mismatch": True,
        },
    )
    merged = merge_candidate(candidate, incoming)
    merged_record = merged.to_record()
    merged_record["category"] = "non_member"
    merged_record["employment_status"] = ""
    merged_record["organization"] = incoming.organization
    if incoming.linkedin_url:
        merged_record["linkedin_url"] = incoming.linkedin_url
    if incoming.education:
        merged_record["education"] = incoming.education
    if incoming.work_history:
        merged_record["work_history"] = incoming.work_history
    merged_record["notes"] = incoming.notes
    merged_record["source_path"] = str(raw_path)
    merged_record["metadata"] = incoming.metadata
    merged_candidate = Candidate(**merged_record)
    resolution = {
        "candidate_id": candidate.candidate_id,
        "display_name": candidate.display_name,
        "account_id": account_id,
        "profile_url": profile.get("profile_url", ""),
        "raw_path": str(raw_path),
        "resolution_source": "company_asset_completion_non_member",
    }
    evidence = [_profile_non_member_evidence_record(merged_candidate, profile, raw_path)]
    return merged_candidate, resolution, evidence


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _profile_registry_sources_for_candidate(candidate: Candidate) -> list[str]:
    metadata = dict(candidate.metadata or {})
    labels: list[str] = []
    candidate_id = str(candidate.candidate_id or "").strip()
    if candidate_id:
        labels.append(f"candidate:{candidate_id}")
    source_dataset = str(candidate.source_dataset or "").strip()
    if source_dataset:
        labels.append(f"dataset:{source_dataset}")
    for key in [
        "source_shard_id",
        "source_shard_title",
        "source_family",
        "source_query",
        "seed_query",
        "query_bundle_id",
        "strategy_id",
    ]:
        value = str(metadata.get(key) or "").strip()
        if value:
            labels.append(f"{key}:{value[:160]}")
    for key in ["source_queries", "scope_keywords", "keywords"]:
        values = metadata.get(key)
        if isinstance(values, (list, tuple, set)):
            for item in list(values)[:3]:
                value = str(item or "").strip()
                if value:
                    labels.append(f"{key}:{value[:160]}")
    return _dedupe_strings(labels)


def _profile_registry_lease_owner(prefix: str) -> str:
    normalized_prefix = str(prefix or "profile_registry").strip() or "profile_registry"
    return f"{normalized_prefix}:{os.getpid()}:{threading.get_ident()}"


def _profile_registry_alias_metadata(profile_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    payload_dict = dict(payload or {})
    alias_metadata = dict(payload_dict.get("profile_registry_aliases") or {})
    if not alias_metadata:
        parsed = dict(payload_dict.get("parsed") or {})
        alias_urls = _dedupe_strings(
            [
                profile_url,
                str(parsed.get("requested_profile_url") or ""),
                str(parsed.get("profile_url") or ""),
                str(parsed.get("url") or ""),
            ]
        )
        alias_metadata = {
            "alias_urls": alias_urls,
            "raw_linkedin_url": str(parsed.get("requested_profile_url") or profile_url),
            "sanity_linkedin_url": str(parsed.get("profile_url") or ""),
        }
    alias_urls = _dedupe_strings(
        [
            *list(alias_metadata.get("alias_urls") or []),
            profile_url,
            str(alias_metadata.get("raw_linkedin_url") or ""),
            str(alias_metadata.get("sanity_linkedin_url") or ""),
        ]
    )
    return {
        "alias_urls": alias_urls,
        "raw_linkedin_url": str(alias_metadata.get("raw_linkedin_url") or profile_url).strip(),
        "sanity_linkedin_url": str(alias_metadata.get("sanity_linkedin_url") or "").strip(),
    }


def _load_harvest_profile_payload_from_snapshot_cache(
    *,
    snapshot_dir: Path,
    profile_url: str,
    normalized_profile_key: str = "",
) -> dict[str, Any] | None:
    harvest_dir = snapshot_dir / "harvest_profiles"
    if not harvest_dir.exists():
        return None
    for candidate_path in profile_cache_path_candidates(
        harvest_dir,
        profile_url,
        normalized_profile_url=str(normalized_profile_key or "").strip(),
    ):
        cached_payload = _load_harvest_profile_payload_from_raw_path(str(candidate_path))
        if cached_payload is not None:
            return cached_payload
    return None


def _load_harvest_profile_payload_from_registry_or_snapshot(
    *,
    registry_entry: dict[str, Any] | None,
    snapshot_dir: Path,
    profile_url: str,
    normalized_profile_key: str = "",
) -> dict[str, Any] | None:
    entry = dict(registry_entry or {})
    cached = _load_harvest_profile_payload_from_raw_path(str(entry.get("last_raw_path") or ""))
    if cached is not None:
        return cached
    return _load_harvest_profile_payload_from_snapshot_cache(
        snapshot_dir=snapshot_dir,
        profile_url=profile_url,
        normalized_profile_key=normalized_profile_key,
    )


def _profile_registry_queue_duration_ms(registry_entry: dict[str, Any]) -> int | None:
    queued_at = str(registry_entry.get("last_queued_at") or registry_entry.get("first_queued_at") or "").strip()
    fetched_at = str(registry_entry.get("last_fetched_at") or "").strip()
    if not queued_at or not fetched_at:
        return None
    try:
        queued_dt = datetime.strptime(queued_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        fetched_dt = datetime.strptime(fetched_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return max(0, int((fetched_dt - queued_dt).total_seconds() * 1000))


def _load_harvest_profile_payload_from_raw_path(raw_path: str) -> dict[str, Any] | None:
    normalized_raw_path = str(raw_path or "").strip()
    if not normalized_raw_path:
        return None
    path = Path(normalized_raw_path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not harvest_profile_payload_has_usable_content(payload):
        return None
    alias_metadata = extract_profile_registry_aliases_from_payload(payload)
    return {
        "raw_path": path,
        "account_id": "harvest_profile_registry",
        "raw_payload": payload,
        "parsed": parse_harvest_profile_payload(payload),
        "profile_registry_aliases": alias_metadata,
    }


def _candidate_profile_urls(candidate: Candidate) -> list[str]:
    urls: list[str] = []
    for value in [candidate.linkedin_url, str(candidate.metadata.get("profile_url") or "").strip()]:
        normalized = str(value or "").strip()
        if normalized and normalized not in urls:
            urls.append(normalized)
    return urls


def _candidate_profile_identifiers(candidate: Candidate) -> set[str]:
    metadata = dict(candidate.metadata or {})
    return _linkedin_identifier_set(
        [
            candidate.linkedin_url,
            metadata.get("profile_url"),
            metadata.get("seed_slug"),
            metadata.get("public_identifier"),
            metadata.get("more_profiles"),
        ]
    )


def _profile_identifiers(profile: dict[str, Any]) -> set[str]:
    return _linkedin_identifier_set(
        [
            profile.get("profile_url"),
            profile.get("requested_profile_url"),
            profile.get("public_identifier"),
            profile.get("username"),
            profile.get("more_profiles"),
        ]
    )


def _linkedin_identifier_set(values: list[Any]) -> set[str]:
    identifiers: set[str] = set()
    for value in values:
        _append_linkedin_identifier(identifiers, value)
    return identifiers


def _append_linkedin_identifier(identifiers: set[str], value: Any) -> None:
    if isinstance(value, dict):
        for key in ["url", "profile_url", "linkedin_url", "public_identifier", "username"]:
            _append_linkedin_identifier(identifiers, value.get(key))
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _append_linkedin_identifier(identifiers, item)
        return
    normalized = _normalize_linkedin_identifier(str(value or ""))
    if normalized:
        identifiers.add(normalized)


def _normalize_linkedin_identifier(value: str) -> str:
    raw = unescape(str(value or "")).strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if "linkedin.com" in lowered:
        slug = extract_linkedin_slug(raw)
        if slug:
            return slug.strip().lower()
        parsed = parse.urlparse(raw if "://" in raw else f"https://{raw.lstrip('/')}")
        host = parsed.netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        if host.startswith("m."):
            host = host[2:]
        path = re.sub(r"/+", "/", parsed.path or "").strip().rstrip("/")
        return f"{host}{path.lower()}"
    return raw.strip().strip("/").lower()


def _company_variants(identity: CompanyIdentity) -> set[str]:
    variants = {_normalize(identity.canonical_name)}
    variants.update(_normalize(alias) for alias in identity.aliases if alias)
    variants.update(_normalize(item) for item in [identity.linkedin_slug, _company_slug_from_url(identity.linkedin_company_url)] if item)
    variants.update(_verified_equivalent_company_variants(identity))
    return {item for item in variants if item}


def _company_reference_labels(identity: CompanyIdentity) -> list[str]:
    labels: list[str] = []
    for value in [identity.canonical_name, *identity.aliases, identity.linkedin_slug, _company_slug_from_url(identity.linkedin_company_url)]:
        text = str(value or "").strip()
        if text and text not in labels:
            labels.append(text)
    metadata = dict(identity.metadata or {})
    for item in list(metadata.get("equivalent_company_names") or []):
        if not isinstance(item, dict):
            continue
        text = str(item.get("name") or item.get("company_name") or item.get("canonical_name") or "").strip()
        if text and text not in labels:
            labels.append(text)
    return labels


def _verified_equivalent_company_variants(identity: CompanyIdentity) -> set[str]:
    metadata = dict(identity.metadata or {})
    equivalent_items = list(metadata.get("equivalent_company_names") or [])
    identity_company_keys = {
        _normalize(str(identity.linkedin_slug or "")),
        _normalize(_company_slug_from_url(identity.linkedin_company_url)),
    }
    identity_company_keys.discard("")
    if not identity_company_keys:
        return set()

    variants: set[str] = set()
    for item in equivalent_items:
        if not isinstance(item, dict):
            continue
        normalized = _normalize(
            str(item.get("name") or item.get("company_name") or item.get("canonical_name") or "")
        )
        if not normalized:
            continue
        item_company_keys = {
            _normalize(str(item.get("linkedin_slug") or "")),
            _normalize(_company_slug_from_url(str(item.get("linkedin_company_url") or item.get("companyLinkedinUrl") or ""))),
        }
        item_company_keys.discard("")
        if item_company_keys & identity_company_keys:
            variants.add(normalized)
    return variants


def _company_label_matches_identity(
    label: str,
    identity: CompanyIdentity,
    *,
    company_variants: set[str] | None = None,
    model_client: ModelClient | None = None,
) -> bool:
    normalized = _normalize(label)
    if not normalized:
        return False
    variants = company_variants or _company_variants(identity)
    if normalized in variants:
        return True
    if not _should_attempt_ai_company_equivalence(label, identity, company_variants=variants):
        return False
    return _ai_company_equivalence_matches(label, identity, model_client=model_client)


def _should_attempt_ai_company_equivalence(
    label: str,
    identity: CompanyIdentity,
    *,
    company_variants: set[str],
) -> bool:
    normalized = _normalize(label)
    if not normalized or normalized in company_variants:
        return False
    label_tokens = _company_name_tokens(label)
    if not label_tokens and len(normalized) < 10:
        return False
    for reference in _company_reference_labels(identity):
        reference_normalized = _normalize(reference)
        if not reference_normalized or reference_normalized == normalized:
            continue
        reference_tokens = _company_name_tokens(reference)
        overlap = label_tokens & reference_tokens
        if len(overlap) >= 2 and len(overlap) >= max(2, min(len(label_tokens), len(reference_tokens)) - 1):
            return True
        prefix_length = _common_prefix_length(normalized, reference_normalized)
        if prefix_length >= 10 and prefix_length / max(1, min(len(normalized), len(reference_normalized))) >= 0.6:
            return True
    return False


def _company_name_tokens(value: str) -> set[str]:
    stopwords = {"and", "the", "inc", "llc", "corp", "co", "company", "limited", "ltd", "plc", "gmbh", "sa"}
    return {
        token
        for token in re.split(r"[^a-z0-9]+", str(value or "").lower())
        if token and token not in stopwords and len(token) >= 2
    }


def _common_prefix_length(left: str, right: str) -> int:
    count = 0
    for left_char, right_char in zip(left, right):
        if left_char != right_char:
            break
        count += 1
    return count


def _ai_company_equivalence_matches(
    label: str,
    identity: CompanyIdentity,
    *,
    model_client: ModelClient | None = None,
) -> bool:
    if model_client is None:
        return False
    if not isinstance(identity.metadata, dict):
        identity.metadata = {}
    cache = identity.metadata.setdefault("_runtime_ai_company_equivalence_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        identity.metadata["_runtime_ai_company_equivalence_cache"] = cache
    normalized = _normalize(label)
    cached = cache.get(normalized)
    if isinstance(cached, dict):
        return bool(cached.get("is_equivalent"))
    decision = model_client.judge_company_equivalence(
        {
            "target_company": {
                "canonical_name": identity.canonical_name,
                "linkedin_slug": identity.linkedin_slug,
                "linkedin_company_url": identity.linkedin_company_url,
                "aliases": _company_reference_labels(identity),
            },
            "observed_companies": [
                {
                    "label": str(label or "").strip(),
                    "normalized_label": normalized,
                }
            ],
        }
    )
    matched_label = str(decision.get("matched_label") or "").strip()
    is_equivalent = str(decision.get("decision") or "uncertain").strip().lower() == "same_company" and (
        not matched_label or _normalize(matched_label) == normalized
    )
    cache[normalized] = {
        "is_equivalent": is_equivalent,
        "matched_label": matched_label,
        "decision": str(decision.get("decision") or "uncertain").strip(),
        "confidence_label": str(decision.get("confidence_label") or "").strip(),
        "rationale": str(decision.get("rationale") or "").strip(),
    }
    return is_equivalent


def _experience_matches_company(
    item: dict[str, Any],
    company_variants: set[str],
    *,
    identity: CompanyIdentity | None = None,
    model_client: ModelClient | None = None,
) -> bool:
    for company in _experience_company_labels(item):
        if _normalize(company) in company_variants:
            return True
        if identity is not None and _company_label_matches_identity(
            company,
            identity,
            company_variants=company_variants,
            model_client=model_client,
        ):
            return True
    return False


def _experience_company_labels(item: dict[str, Any]) -> list[str]:
    values = [
        item.get("company"),
        item.get("company_name"),
        item.get("companyName"),
        item.get("companyUniversalName"),
        _company_slug_from_url(str(item.get("companyLinkedinUrl") or "")),
    ]
    results: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in results:
            results.append(text)
    return results


def _company_slug_from_url(url: str) -> str:
    match = re.search(r"linkedin\.com/company/([^/?#]+)", str(url or ""), re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def _matched_profile_experiences(
    profile: dict[str, Any],
    identity: CompanyIdentity,
    *,
    company_variants: set[str] | None = None,
    model_client: ModelClient | None = None,
) -> list[dict[str, Any]]:
    variants = company_variants or _company_variants(identity)
    matched: list[dict[str, Any]] = []
    for item in profile.get("experience", []) or []:
        if _experience_matches_company(
            item,
            variants,
            identity=identity,
            model_client=model_client,
        ):
            matched.append(item)
    return matched


def _review_profile_membership(
    profile: dict[str, Any],
    identity: CompanyIdentity,
    *,
    resolved_category: str,
    resolved_employment_status: str,
    model_client: ModelClient | None = None,
) -> dict[str, Any]:
    if resolved_category not in {"employee", "former_employee"}:
        return {
            "triggered": False,
            "decision": "",
            "confidence_label": "",
            "rationale": "",
            "requires_manual_review": False,
            "trigger_reasons": [],
            "trigger_keywords": [],
            "note": "",
        }

    company_variants = _company_variants(identity)
    matched_experiences = _matched_profile_experiences(
        profile,
        identity,
        company_variants=company_variants,
        model_client=model_client,
    )
    trigger_keywords = _profile_membership_suspicious_keywords(profile, matched_experiences)
    if len(trigger_keywords) < 2:
        return {
            "triggered": False,
            "decision": "",
            "confidence_label": "",
            "rationale": "",
            "requires_manual_review": False,
            "trigger_reasons": [],
            "trigger_keywords": [],
            "note": "",
        }

    response = model_client.judge_profile_membership(
        {
            "target_company": {
                "canonical_name": identity.canonical_name,
                "linkedin_slug": identity.linkedin_slug,
                "linkedin_company_url": identity.linkedin_company_url,
                "aliases": _company_reference_labels(identity),
            },
            "candidate_membership": {
                "resolved_category": resolved_category,
                "resolved_employment_status": resolved_employment_status,
                "current_company": str(profile.get("current_company") or "").strip(),
                "matched_experiences": [
                    {
                        "title": str(item.get("title") or item.get("position") or "").strip(),
                        "company_name": str(item.get("companyName") or item.get("company_name") or item.get("company") or "").strip(),
                        "company_linkedin_url": str(item.get("companyLinkedinUrl") or "").strip(),
                        "is_current": bool(item.get("is_current") or item.get("isCurrent") or not item.get("endDate")),
                        "description": str(item.get("description") or "").strip(),
                        "skills": [str(value).strip() for value in list(item.get("skills") or []) if str(value).strip()][:5],
                    }
                    for item in matched_experiences[:3]
                ],
            },
            "profile": {
                "full_name": str(profile.get("full_name") or "").strip(),
                "headline": str(profile.get("headline") or "").strip(),
                "summary": str(profile.get("summary") or "").strip()[:1200],
                "location": str(profile.get("location") or "").strip(),
            },
            "heuristic_triggers": {
                "reasons": ["suspicious_profile_content"],
                "keywords": trigger_keywords[:12],
            },
        }
    ) if model_client is not None else {}
    decision = str(response.get("decision") or "uncertain").strip().lower()
    if decision not in {"confirmed_member", "suspicious_member", "non_member", "uncertain"}:
        decision = "uncertain"
    confidence_label = str(response.get("confidence_label") or "low").strip() or "low"
    rationale = str(response.get("rationale") or "").strip()
    requires_manual_review = decision in {"suspicious_member", "uncertain"}
    note = ""
    if requires_manual_review:
        note = "Structured profile signals target-company membership, but the profile content requires manual review."
    return {
        "triggered": True,
        "decision": decision,
        "confidence_label": confidence_label,
        "rationale": rationale,
        "requires_manual_review": requires_manual_review,
        "trigger_reasons": ["suspicious_profile_content"],
        "trigger_keywords": trigger_keywords[:12],
        "note": note,
    }


def _profile_membership_suspicious_keywords(profile: dict[str, Any], matched_experiences: list[dict[str, Any]]) -> list[str]:
    parts = [
        str(profile.get("headline") or "").strip(),
        str(profile.get("summary") or "").strip(),
    ]
    for item in matched_experiences[:3]:
        parts.append(str(item.get("description") or "").strip())
        parts.extend(str(value).strip() for value in list(item.get("skills") or []) if str(value).strip())
    combined = " ".join(part for part in parts if part).lower()
    hits: list[str] = []
    for keyword in sorted(SUSPICIOUS_PROFILE_KEYWORDS):
        if keyword in combined and keyword not in hits:
            hits.append(keyword)
    return hits


def _classify_profile_membership(
    profile: dict[str, Any],
    identity: CompanyIdentity,
    *,
    model_client: ModelClient | None = None,
) -> tuple[str, str]:
    company_variants = _company_variants(identity)
    current_company = str(profile.get("current_company", "")).strip()
    if _company_label_matches_identity(
        current_company,
        identity,
        company_variants=company_variants,
        model_client=model_client,
    ):
        return "employee", "current"
    for item in _matched_profile_experiences(
        profile,
        identity,
        company_variants=company_variants,
        model_client=model_client,
    ):
        if item.get("is_current"):
            return "employee", "current"
        return "former_employee", "former"
    return "lead", ""


def _exploration_targets(candidates: list[Candidate], unresolved_candidates: list[dict[str, Any]]) -> list[Candidate]:
    unresolved_ids = {str(item.get("candidate_id") or "").strip() for item in unresolved_candidates if item.get("candidate_id")}
    targets: list[Candidate] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate.candidate_id in seen:
            continue
        should_explore = candidate.candidate_id in unresolved_ids or candidate.category == "lead" or not candidate.linkedin_url
        if should_explore:
            seen.add(candidate.candidate_id)
            targets.append(candidate)
    return _prioritize_candidates(targets)


def _drop_unresolved_candidate(unresolved_candidates: list[dict[str, Any]], candidate_id: str) -> None:
    unresolved_candidates[:] = [
        item for item in unresolved_candidates if str(item.get("candidate_id") or "").strip() != str(candidate_id or "").strip()
    ]


def _load_local_publications(path: Path, max_publications: int) -> list[PublicationRecord]:
    payload = json.loads(path.read_text())
    publications = list((payload.get("publications") or {}).values())
    publications.sort(key=lambda item: (item.get("year") or 0, item.get("month") or 0), reverse=True)
    results: list[PublicationRecord] = []
    for item in publications:
        if not item.get("is_anthropic_paper"):
            continue
        results.append(
            PublicationRecord(
                publication_id=str(item.get("id") or item.get("url") or item.get("title")),
                source=str(item.get("source") or "local_publications"),
                source_dataset="anthropic_publications_unified",
                source_path=str(path),
                title=str(item.get("title") or "").strip(),
                url=str(item.get("url") or "").strip(),
                year=item.get("year"),
                authors=[str(name).strip() for name in item.get("authors_list") or [] if str(name).strip()],
                acknowledgement_names=[
                    str(name).strip()
                    for name in (item.get("ack") or {}).get("names", []) + (item.get("ack") or {}).get("chinese_candidates", [])
                    if str(name).strip()
                ],
            )
        )
        if len(results) >= max_publications:
            break
    return results


def _dedupe_publication_records(records: list[PublicationRecord], limit: int) -> list[PublicationRecord]:
    deduped: list[PublicationRecord] = []
    seen: set[str] = set()
    for record in records:
        key = str(record.url or record.publication_id or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(record)
        if len(deduped) >= limit:
            break
    return deduped


def _identity_site_root(identity: CompanyIdentity) -> str:
    domain = str(identity.domain or "").strip().lower()
    if not domain:
        return ""
    return f"https://{domain.rstrip('/')}"


def _discover_official_surface_urls(identity: CompanyIdentity, site_root: str, home_html: str) -> list[str]:
    candidates = [
        f"{site_root}/index.xml",
        f"{site_root}/blog/",
        f"{site_root}/blog/index.xml",
        f"{site_root}/news/",
        f"{site_root}/news/index.xml",
    ]
    home_links = set(re.findall(r'href="([^"]+)"', home_html or ""))
    for raw_link in home_links:
        link = str(raw_link or "").strip()
        if not link:
            continue
        if link.startswith("http://") or link.startswith("https://"):
            if not link.startswith(site_root):
                continue
            normalized = link.rstrip("/")
        elif link.startswith("/"):
            normalized = f"{site_root}{link}".rstrip("/")
        else:
            continue
        lowered = normalized.lower()
        if any(token in lowered for token in ["/blog", "/news", "/docs", "/research", "/engineering", "/tinker", "index.xml"]):
            candidates.append(normalized if normalized.endswith(".xml") else f"{normalized}/" if not normalized.endswith("/") else normalized)
            if not normalized.endswith(".xml"):
                candidates.append(f"{normalized}/index.xml")
    deduped: list[str] = []
    seen: set[str] = set()
    for url in candidates:
        normalized = str(url or "").strip()
        if not normalized:
            continue
        if normalized.endswith("/") and normalized != f"{site_root}/":
            normalized = normalized.rstrip("/") + "/"
        lowered = normalized.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(normalized)
    return deduped


def _surface_asset_name(url: str) -> str:
    parsed = parse.urlparse(url)
    path_token = normalize_name_token(parsed.path or "surface")
    if not path_token:
        path_token = "surface"
    suffix = ".xml" if parsed.path.endswith(".xml") else ".html"
    return f"{path_token}{suffix}"


def _extract_publications_from_rss(xml_text: str, feed_url: str, source_path: str) -> list[PublicationRecord]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    items = root.findall(".//item")
    results: list[PublicationRecord] = []
    for item in items:
        title = _xml_text(item.find("title"))
        url = _xml_text(item.find("link"))
        if not title or not url:
            continue
        description = unescape(_xml_text(item.find("description")))
        authors = _parse_author_text(_extract_author_text_from_description(description))
        publication_id = sha1("|".join([feed_url, title, url]).encode("utf-8")).hexdigest()[:16]
        results.append(
            PublicationRecord(
                publication_id=publication_id,
                source="official_rss_feed",
                source_dataset=f"{normalize_name_token(parse.urlparse(feed_url).netloc)}_official_feed",
                source_path=source_path,
                title=title,
                url=url,
                year=_extract_year_from_text(_xml_text(item.find("pubDate"))),
                authors=authors,
                acknowledgement_names=[],
            )
        )
    return results


def _xml_text(element: ET.Element | None) -> str:
    if element is None or element.text is None:
        return ""
    return str(element.text).strip()


def _extract_author_text_from_description(description: str) -> str:
    match = re.search(r'author-date[^>]*>\s*([^<]+?)\s*</div>', description, flags=re.IGNORECASE)
    if match:
        return " ".join(unescape(match.group(1)).split())
    return ""


def _extract_publications_from_surface_index(html_text: str, surface_url: str, source_path: str) -> list[PublicationRecord]:
    results: list[PublicationRecord] = []
    item_pattern = re.compile(
        r'<a class="post-item-link" href="([^"]+)".*?<div class="post-title">(.*?)</div>(?:.*?<div class="author-date">\s*(.*?)\s*</div>)?',
        re.DOTALL,
    )
    for raw_url, raw_title, raw_author in item_pattern.findall(html_text):
        absolute_url = parse.urljoin(surface_url, unescape(raw_url))
        title = " ".join(unescape(re.sub(r"<[^>]+>", " ", raw_title)).split())
        if not title or not absolute_url:
            continue
        author_text = " ".join(unescape(re.sub(r"<[^>]+>", " ", raw_author or "")).split())
        authors = _parse_author_text(author_text)
        results.append(
            PublicationRecord(
                publication_id=sha1("|".join([surface_url, title, absolute_url]).encode("utf-8")).hexdigest()[:16],
                source="official_surface_index",
                source_dataset=f"{normalize_name_token(parse.urlparse(surface_url).path or 'official_surface')}_index",
                source_path=source_path,
                title=title,
                url=absolute_url,
                year=None,
                authors=authors,
                acknowledgement_names=[],
            )
        )
    return results


def _extract_single_surface_page_record(html_text: str, url: str, source_path: str) -> list[PublicationRecord]:
    title = _extract_page_title(html_text)
    if not title:
        return []
    authors = _extract_page_authors(html_text)
    return [
        PublicationRecord(
            publication_id=sha1("|".join([url, title]).encode("utf-8")).hexdigest()[:16],
            source="official_surface_page",
            source_dataset=f"{normalize_name_token(parse.urlparse(url).path or 'official_surface')}_page",
            source_path=source_path,
            title=title,
            url=url,
            year=_extract_year_from_html(html_text),
            authors=authors,
            acknowledgement_names=[],
        )
    ]


def _extract_page_title(html_text: str) -> str:
    for pattern in [
        r'<meta itemprop="name" content="([^"]+)"',
        r'<meta property="og:title" content="([^"]+)"',
        r"<title>(.*?)</title>",
        r'<h1 class="post-title">(.*?)</h1>',
    ]:
        match = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            title = " ".join(unescape(re.sub(r"<[^>]+>", " ", match.group(1))).split())
            if title:
                return title
    return ""


def _extract_page_authors(html_text: str) -> list[str]:
    author_candidates: list[str] = []
    for pattern in [
        r'<span class="author">\s*(.*?)\s*</span>',
        r'<div class="author-date">\s*(.*?)\s*</div>',
        r'<meta name="author" content="([^"]+)"',
        r'author\s*=\s*\{([^}]+)\}',
    ]:
        for match in re.findall(pattern, html_text, flags=re.IGNORECASE | re.DOTALL):
            cleaned = " ".join(unescape(re.sub(r"<[^>]+>", " ", match)).split())
            if cleaned:
                author_candidates.append(cleaned)
    authors: list[str] = []
    for item in author_candidates:
        for author in _parse_author_text(item):
            if author not in authors:
                authors.append(author)
    return authors


def _parse_author_text(text: str) -> list[str]:
    normalized = " ".join(str(text or "").split()).strip()
    if not normalized:
        return []
    lowered = normalized.lower()
    if " in collaboration with " in lowered:
        normalized = normalized[: lowered.index(" in collaboration with ")].strip()
    if normalized.lower() in {"thinking machines lab", "thinking machines"}:
        return []
    normalized = normalized.replace("&", " and ")
    parts = re.split(r"\s+(?:and)\s+|,\s*", normalized)
    authors: list[str] = []
    for part in parts:
        cleaned = " ".join(part.split()).strip(" -")
        if not cleaned or cleaned.lower() in {"thinking machines lab", "thinking machines"}:
            continue
        if _looks_like_person_name(cleaned) and cleaned not in authors:
            authors.append(cleaned)
    return authors


def _extract_year_from_text(text: str) -> int | None:
    match = re.search(r"\b(20\d{2}|19\d{2})\b", str(text or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _extract_year_from_html(html_text: str) -> int | None:
    for pattern in [
        r'<meta itemprop="datePublished" content="([^"]+)"',
        r'<time[^>]*>([^<]+)</time>',
    ]:
        match = re.search(pattern, html_text, flags=re.IGNORECASE)
        if match:
            year = _extract_year_from_text(match.group(1))
            if year is not None:
                return year
    return None


def _build_publication_lead(identity: CompanyIdentity, publication: PublicationRecord, name: str, role: str) -> Candidate:
    candidate_id = make_candidate_id(name, identity.canonical_name, identity.canonical_name)
    return Candidate(
        candidate_id=candidate_id,
        name_en=name,
        display_name=name,
        category="lead",
        target_company=identity.canonical_name,
        organization=identity.canonical_name,
        role=role,
        notes=f"Discovered from {publication.source} publication evidence: {publication.title}",
        source_dataset=publication.source_dataset,
        source_path=publication.source_path,
        metadata={
            "publication_id": publication.publication_id,
            "publication_url": publication.url,
            "publication_title": publication.title,
            "publication_source": publication.source,
            "lead_discovery_method": "publication_author_acknowledgement_scan",
        },
    )


def _build_roster_anchored_scholar_coauthor_prospect(
    identity: CompanyIdentity,
    prospect: dict[str, Any],
    *,
    source_path: Path,
) -> Candidate:
    name = str(prospect.get("name") or "").strip()
    papers = list(prospect.get("papers") or [])
    top_paper = papers[0] if papers else {}
    notes = _join_nonempty(
        f"Discovered from roster-anchored scholar coauthor expansion for {identity.canonical_name}.",
        f"Seed members: {', '.join(list(prospect.get('seed_names') or [])[:4])}" if list(prospect.get("seed_names") or []) else "",
        f"Top paper: {str(top_paper.get('title') or '').strip()}" if top_paper else "",
    )
    return Candidate(
        candidate_id=make_candidate_id(name, identity.canonical_name, identity.canonical_name),
        name_en=name,
        display_name=name,
        category="lead",
        target_company=identity.canonical_name,
        organization=identity.canonical_name,
        role="Roster-Anchored Scholar Coauthor Prospect",
        notes=notes,
        source_dataset="roster_anchored_scholar_coauthor_prospect",
        source_path=str(source_path),
        metadata={
            "publication_title": str(top_paper.get("title") or "").strip(),
            "publication_url": str(top_paper.get("url") or "").strip(),
            "lead_discovery_method": "roster_anchored_scholar_coauthor_expansion",
            "scholar_coauthor_seed_names": list(prospect.get("seed_names") or [])[:8],
            "scholar_coauthor_papers": papers[:12],
        },
    )


def _dedupe_names(values: list[str]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _name_key(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        text = str(value or "").strip()
        if text:
            results.append(text)
    return results


def _publication_lead_public_web_resolution(
    candidate: Candidate,
    identity: CompanyIdentity,
    *,
    allow_targeted_name_search: bool,
) -> dict[str, Any]:
    return _public_web_membership_resolution(
        candidate,
        identity,
        allow_targeted_name_search=allow_targeted_name_search,
        explicit_approval_key="publication_lead_targeted_search_approved",
        no_evidence_state="publication_source_only_unconfirmed",
        no_evidence_label="publication provenance is the only current source",
        confirmed_without_linkedin_next_step="await_user_confirmation_before_paid_search",
    )


def _scholar_coauthor_prospect_public_web_resolution(candidate: Candidate, identity: CompanyIdentity) -> dict[str, Any]:
    return _public_web_membership_resolution(
        candidate,
        identity,
        allow_targeted_name_search=False,
        explicit_approval_key="",
        no_evidence_state="scholar_coauthor_only_unconfirmed",
        no_evidence_label="scholar coauthor provenance is the only current source",
        confirmed_without_linkedin_next_step="hold_for_known_linkedin_or_manual_follow_up",
    )


def _public_web_membership_resolution(
    candidate: Candidate,
    identity: CompanyIdentity,
    *,
    allow_targeted_name_search: bool,
    explicit_approval_key: str,
    no_evidence_state: str,
    no_evidence_label: str,
    confirmed_without_linkedin_next_step: str,
) -> dict[str, Any]:
    linkedin_url = _candidate_known_linkedin_url(candidate)
    affiliation_matches = _publication_lead_company_signal_matches(
        candidate,
        identity,
        metadata_keys=["exploration_affiliation_signals", "analysis_affiliation_signals"],
    )
    work_history_matches = _publication_lead_company_signal_matches(
        candidate,
        identity,
        metadata_keys=["exploration_work_history_signals", "analysis_work_history_signals"],
    )
    summary_matches = _publication_lead_summary_matches(candidate, identity)
    confirmed_by_public_web = bool(affiliation_matches or work_history_matches or summary_matches)
    explicit_approval = bool(explicit_approval_key and candidate.metadata.get(explicit_approval_key))

    if confirmed_by_public_web and linkedin_url:
        state = "confirmed_public_web_with_linkedin"
        next_step = "profile_detail_by_known_linkedin"
    elif confirmed_by_public_web:
        state = "confirmed_public_web_missing_linkedin"
        next_step = confirmed_without_linkedin_next_step
    elif linkedin_url:
        state = "linkedin_discovered_membership_unconfirmed"
        next_step = "hold_for_manual_confirmation_before_profile_scrape"
    else:
        state = no_evidence_state
        next_step = "hold_for_secondary_public_evidence_or_manual_review"

    eligible_for_targeted_name_search = bool(
        allow_targeted_name_search and explicit_approval and confirmed_by_public_web and not linkedin_url
    )

    summary_parts = []
    if affiliation_matches:
        summary_parts.append("public web captured explicit affiliation signals")
    if work_history_matches:
        summary_parts.append("public web captured work-history signals")
    if summary_matches:
        summary_parts.append("public web validated target-company summaries")
    if not summary_parts:
        summary_parts.append(no_evidence_label)
    if linkedin_url:
        summary_parts.append("LinkedIn URL recovered from exploration")
    summary_parts.append(f"next step: {next_step}")

    return {
        "state": state,
        "confirmed_by_public_web": confirmed_by_public_web,
        "linkedin_url": linkedin_url,
        "eligible_for_targeted_name_search": eligible_for_targeted_name_search,
        "next_step": next_step,
        "summary": "; ".join(summary_parts),
        "affiliation_matches": affiliation_matches[:4],
        "work_history_matches": work_history_matches[:4],
        "summary_matches": summary_matches[:4],
    }


def _annotate_publication_lead_resolution(candidate: Candidate, decision: dict[str, Any]) -> Candidate:
    record = candidate.to_record()
    metadata = dict(candidate.metadata or {})
    metadata.update(
        {
            "lead_resolution_strategy": "publication_lead_public_web_verification",
            "publication_lead_resolution_state": str(decision.get("state") or "").strip(),
            "publication_lead_public_web_confirmed": bool(decision.get("confirmed_by_public_web")),
            "publication_lead_targeted_name_search_eligible": bool(decision.get("eligible_for_targeted_name_search")),
            "publication_lead_next_step": str(decision.get("next_step") or "").strip(),
            "publication_lead_public_web_summary": str(decision.get("summary") or "").strip(),
            "publication_lead_public_web_affiliation_matches": list(decision.get("affiliation_matches") or [])[:4],
            "publication_lead_public_web_work_history_matches": list(decision.get("work_history_matches") or [])[:4],
            "publication_lead_public_web_summary_matches": list(decision.get("summary_matches") or [])[:4],
        }
    )
    record["metadata"] = metadata
    return Candidate(**record)


def _annotate_scholar_coauthor_resolution(candidate: Candidate, decision: dict[str, Any]) -> Candidate:
    record = candidate.to_record()
    metadata = dict(candidate.metadata or {})
    metadata.update(
        {
            "lead_resolution_strategy": "roster_anchored_scholar_coauthor_public_web_verification",
            "scholar_coauthor_resolution_state": str(decision.get("state") or "").strip(),
            "scholar_coauthor_public_web_confirmed": bool(decision.get("confirmed_by_public_web")),
            "scholar_coauthor_next_step": str(decision.get("next_step") or "").strip(),
            "scholar_coauthor_public_web_summary": str(decision.get("summary") or "").strip(),
            "scholar_coauthor_public_web_affiliation_matches": list(decision.get("affiliation_matches") or [])[:4],
            "scholar_coauthor_public_web_work_history_matches": list(decision.get("work_history_matches") or [])[:4],
            "scholar_coauthor_public_web_summary_matches": list(decision.get("summary_matches") or [])[:4],
        }
    )
    record["metadata"] = metadata
    return Candidate(**record)


def _load_scholar_coauthor_follow_up_progress(progress_path: Path, summary_path: Path) -> tuple[list[dict[str, Any]], list[str], set[str]]:
    payload = _load_json_payload(progress_path)
    if not payload:
        payload = _load_json_payload(summary_path)
    decisions = [dict(item) for item in list(payload.get("decisions") or []) if isinstance(item, dict)]
    errors = [str(item) for item in list(payload.get("errors") or []) if str(item).strip()]
    processed_candidate_ids = {
        str(item.get("candidate_id") or "").strip()
        for item in decisions
        if str(item.get("candidate_id") or "").strip() and not _is_queued_scholar_coauthor_follow_up_decision(item)
    }
    decision_lookup = {
        str(item.get("candidate_id") or "").strip(): dict(item)
        for item in decisions
        if str(item.get("candidate_id") or "").strip()
    }
    processed_candidate_ids.update(
        candidate_id
        for candidate_id in (
            str(item).strip()
            for item in list(payload.get("processed_candidate_ids") or [])
            if str(item).strip()
        )
        if not _is_queued_scholar_coauthor_follow_up_decision(decision_lookup.get(candidate_id, {}))
    )
    return decisions, errors, processed_candidate_ids


def _is_queued_exploration_summary(summary: dict[str, Any]) -> bool:
    return str(summary.get("status") or "").strip() == "queued"


def _is_queued_scholar_coauthor_follow_up_decision(decision: dict[str, Any]) -> bool:
    if str(decision.get("state") or "").strip() == "queued_background_exploration":
        return True
    exploration_summary = dict(decision.get("exploration_summary") or {})
    return _is_queued_exploration_summary(exploration_summary)


def _restore_scholar_coauthor_follow_up_patch(
    patch_path: Path,
    *,
    candidate_map: dict[str, Candidate],
    resolved_profiles: list[dict[str, Any]],
    evidence: list[EvidenceRecord],
    evidence_keys: set[tuple[str, str, str, str]],
) -> None:
    payload = _load_json_payload(patch_path)
    if not payload:
        return
    for item in list(payload.get("updated_candidates") or []):
        if not isinstance(item, dict):
            continue
        candidate = Candidate(**item)
        candidate_map[_candidate_key(candidate)] = candidate
    _append_unique_resolved_profiles(
        resolved_profiles,
        [item for item in list(payload.get("resolved_profiles") or []) if isinstance(item, dict)],
    )
    _append_unique_evidence_records(
        evidence,
        [
            EvidenceRecord(**item)
            for item in list(payload.get("new_evidence") or [])
            if isinstance(item, dict)
        ],
        evidence_keys=evidence_keys,
    )


def _persist_scholar_coauthor_follow_up_patch(
    patch_path: Path,
    *,
    updated_candidates: list[Candidate],
    resolved_profiles: list[dict[str, Any]],
    new_evidence: list[EvidenceRecord],
    asset_logger: AssetLogger,
) -> None:
    payload = _load_json_payload(patch_path)
    candidate_records = [dict(item) for item in list(payload.get("updated_candidates") or []) if isinstance(item, dict)]
    resolved_profile_records = [dict(item) for item in list(payload.get("resolved_profiles") or []) if isinstance(item, dict)]
    evidence_records = [dict(item) for item in list(payload.get("new_evidence") or []) if isinstance(item, dict)]
    _upsert_follow_up_records(
        candidate_records,
        [item.to_record() for item in updated_candidates],
        key_fn=lambda item: str(item.get("candidate_id") or "").strip() or str(item.get("display_name") or "").strip(),
    )
    _upsert_follow_up_records(
        resolved_profile_records,
        list(resolved_profiles),
        key_fn=_resolved_profile_patch_key,
    )
    _upsert_follow_up_records(
        evidence_records,
        [item.to_record() for item in new_evidence],
        key_fn=_evidence_patch_key,
    )
    asset_logger.write_json(
        patch_path,
        {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "updated_candidates": candidate_records,
            "resolved_profiles": resolved_profile_records,
            "new_evidence": evidence_records,
        },
        asset_type="scholar_coauthor_follow_up_patch",
        source_kind="publication_enrichment",
        is_raw_asset=False,
        model_safe=True,
    )


def _write_scholar_coauthor_follow_up_state(
    *,
    progress_path: Path,
    summary_path: Path,
    identity: CompanyIdentity,
    prospect_total: int,
    processed_candidate_ids: set[str],
    decisions: list[dict[str, Any]],
    errors: list[str],
    asset_logger: AssetLogger,
    completed: bool,
) -> None:
    ordered_decisions = sorted(
        [dict(item) for item in decisions if str(item.get("candidate_id") or "").strip()],
        key=lambda item: (str(item.get("display_name") or "").lower(), str(item.get("candidate_id") or "")),
    )
    payload = {
        "target_company": identity.canonical_name,
        "candidate_count": len(ordered_decisions),
        "prospect_total": int(prospect_total),
        "processed_candidate_ids": sorted(str(item) for item in processed_candidate_ids if str(item).strip()),
        "remaining_candidate_count": max(int(prospect_total) - len(processed_candidate_ids), 0),
        "status": "completed" if completed else "partial",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "decisions": ordered_decisions,
        "errors": [str(item) for item in errors if str(item).strip()],
    }
    asset_logger.write_json(
        progress_path,
        payload,
        asset_type="scholar_coauthor_follow_up_progress",
        source_kind="publication_enrichment",
        is_raw_asset=False,
        model_safe=True,
    )
    asset_logger.write_json(
        summary_path,
        payload,
        asset_type="scholar_coauthor_follow_up",
        source_kind="publication_enrichment",
        is_raw_asset=False,
        model_safe=True,
    )


def _append_unique_resolved_profiles(target: list[dict[str, Any]], items: list[dict[str, Any]]) -> None:
    seen = {_resolved_profile_patch_key(item) for item in target}
    for item in items:
        key = _resolved_profile_patch_key(item)
        if key in seen:
            continue
        target.append(dict(item))
        seen.add(key)


def _append_unique_evidence_records(
    target: list[EvidenceRecord],
    items: list[EvidenceRecord],
    *,
    evidence_keys: set[tuple[str, str, str, str]],
) -> None:
    for item in items:
        key = _evidence_resume_key(item)
        if key in evidence_keys:
            continue
        target.append(item)
        evidence_keys.add(key)


def _upsert_follow_up_records(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
    *,
    key_fn: Any,
) -> None:
    index_by_key = {
        key_fn(item): position
        for position, item in enumerate(existing)
        if key_fn(item)
    }
    for item in incoming:
        key = key_fn(item)
        if not key:
            continue
        if key in index_by_key:
            existing[index_by_key[key]] = dict(item)
            continue
        existing.append(dict(item))
        index_by_key[key] = len(existing) - 1


def _resolved_profile_patch_key(item: dict[str, Any]) -> str:
    if not isinstance(item, dict):
        return ""
    candidate_id = str(item.get("candidate_id") or "").strip()
    profile_url = str(item.get("profile_url") or "").strip()
    raw_path = str(item.get("raw_path") or "").strip()
    resolution_source = str(item.get("resolution_source") or "").strip()
    if candidate_id or profile_url or raw_path or resolution_source:
        return "|".join([candidate_id, profile_url, raw_path, resolution_source])
    return ""


def _evidence_patch_key(item: dict[str, Any]) -> str:
    if not isinstance(item, dict):
        return ""
    evidence_id = str(item.get("evidence_id") or "").strip()
    if evidence_id:
        return evidence_id
    return "|".join(
        [
            str(item.get("candidate_id") or "").strip(),
            str(item.get("source_type") or "").strip(),
            str(item.get("title") or "").strip(),
            str(item.get("url") or "").strip(),
        ]
    )


def _evidence_resume_key(item: EvidenceRecord) -> tuple[str, str, str, str]:
    evidence_id = str(item.evidence_id or "").strip()
    if evidence_id:
        return (evidence_id, "", "", "")
    return (
        str(item.candidate_id or "").strip(),
        str(item.source_type or "").strip(),
        str(item.title or "").strip(),
        str(item.url or "").strip(),
    )


def _load_json_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _candidate_known_linkedin_url(candidate: Candidate) -> str:
    if str(candidate.linkedin_url or "").strip():
        return str(candidate.linkedin_url or "").strip()
    links = dict(candidate.metadata.get("exploration_links") or {})
    linkedin_items = list(links.get("linkedin") or [])
    for item in linkedin_items:
        value = str(item or "").strip()
        if value:
            return value
    return ""


def _candidate_publication_title(candidate: Candidate) -> str:
    return str(candidate.metadata.get("publication_title") or "").strip()


def _should_attempt_known_profile_resolution_after_exploration(candidate: Candidate, identity: CompanyIdentity) -> bool:
    if not _is_publication_lead_candidate(candidate):
        return True
    decision = _publication_lead_public_web_resolution(candidate, identity, allow_targeted_name_search=False)
    return bool(decision.get("confirmed_by_public_web"))


def _is_publication_lead_candidate(candidate: Candidate) -> bool:
    if candidate.category != "lead":
        return False
    source_dataset = str(candidate.source_dataset or "").strip().lower()
    discovery_method = str(candidate.metadata.get("lead_discovery_method") or "").strip().lower()
    return "publication" in source_dataset or discovery_method == "publication_author_acknowledgement_scan"


def _publication_lead_company_signal_matches(
    candidate: Candidate,
    identity: CompanyIdentity,
    *,
    metadata_keys: list[str],
) -> list[dict[str, str]]:
    company_variants = _company_variants(identity)
    matches: list[dict[str, str]] = []
    for metadata_key in metadata_keys:
        for item in list(candidate.metadata.get(metadata_key) or []):
            if not isinstance(item, dict):
                continue
            label = str(item.get("organization") or item.get("company") or "").strip()
            if not label:
                continue
            if not _company_label_matches_identity(label, identity, company_variants=company_variants):
                continue
            normalized = {str(key): str(value).strip() for key, value in item.items() if str(value).strip()}
            normalized["source_key"] = metadata_key
            if normalized not in matches:
                matches.append(normalized)
    return matches


def _publication_lead_summary_matches(candidate: Candidate, identity: CompanyIdentity) -> list[str]:
    matches: list[str] = []
    for value in list(candidate.metadata.get("exploration_validated_summaries") or []):
        text = str(value or "").strip()
        if not text:
            continue
        if _company_text_mentions_identity(text, identity) and text not in matches:
            matches.append(text)
    return matches


def _company_text_mentions_identity(text: str, identity: CompanyIdentity) -> bool:
    normalized_text = _normalize(text)
    if not normalized_text:
        return False
    for label in _company_reference_labels(identity):
        normalized_label = _normalize(label)
        if normalized_label and normalized_label in normalized_text:
            return True
    return False


def _candidate_name_map(candidates: list[Candidate]) -> dict[str, Candidate]:
    return {_candidate_key(candidate): candidate for candidate in candidates}


def _candidate_key(candidate: Candidate) -> str:
    return _name_key(candidate.name_en)


def _name_key(name: str) -> str:
    return normalize_name_token(name)


def _prioritize_candidates(candidates: list[Candidate]) -> list[Candidate]:
    return sorted(candidates, key=lambda item: (-_candidate_priority(item), item.display_name))


def _prioritize_scholar_coauthor_prospects(candidates: list[Candidate]) -> list[Candidate]:
    return sorted(
        candidates,
        key=lambda item: (
            -len(list(item.metadata.get("scholar_coauthor_seed_names") or [])),
            -len(list(item.metadata.get("scholar_coauthor_papers") or [])),
            -int(bool(_candidate_known_linkedin_url(item))),
            -int(bool(str(item.metadata.get("publication_title") or "").strip())),
            -_candidate_priority(item),
            item.display_name,
        ),
    )


def _candidate_priority(candidate: Candidate) -> int:
    text = f"{candidate.role} {candidate.team} {candidate.focus_areas}".lower()
    score = 0
    weighted_tokens = {
        "chief technology officer": 10,
        "cto": 8,
        "co-founder": 8,
        "cofounder": 8,
        "founder": 6,
        "chief executive officer": 6,
        "ceo": 5,
        "infrastructure": 8,
        "infra": 8,
        "training": 6,
        "inference": 6,
        "research engineer": 6,
        "research scientist": 6,
        "member of technical staff": 6,
        "technical staff": 5,
        "engineer": 3,
        "engineering": 3,
        "research": 3,
        "scientist": 3,
        "machine learning": 3,
        "systems": 3,
        "distributed": 3,
    }
    for token, weight in weighted_tokens.items():
        if token in text:
            score += weight
    if candidate.category == "employee" and candidate.employment_status == "current":
        score += 2
    if "." in candidate.display_name:
        score -= 2
    return score


def _names_match(left: str, right: str) -> bool:
    left_tokens = set(_name_tokens(left))
    right_tokens = set(_name_tokens(right))
    if not left_tokens or not right_tokens:
        return False
    return left_tokens <= right_tokens or right_tokens <= left_tokens or len(left_tokens & right_tokens) >= max(2, min(len(left_tokens), len(right_tokens)))


def _name_tokens(value: str) -> list[str]:
    tokens: list[str] = []
    current: list[str] = []
    for char in str(value or "").strip().lower():
        if char.isalnum():
            current.append(char)
            continue
        if current:
            tokens.append("".join(current))
            current = []
    if current:
        tokens.append("".join(current))
    return tokens


def _normalize(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def _join_nonempty(*parts: str) -> str:
    values = []
    for part in parts:
        text = str(part or "").strip()
        if text and text not in values:
            values.append(text)
    return " | ".join(values)


def _format_education(items: list[dict[str, Any]]) -> str:
    formatted = []
    for item in items[:4]:
        school_value = item.get("school") or item.get("school_name") or item.get("schoolName") or item.get("school_name")
        school = _profile_label_from_value(school_value)
        degree = str(item.get("degree") or item.get("degreeName") or "").strip()
        field = str(item.get("field_of_study") or item.get("fieldOfStudy") or item.get("field") or "").strip()
        if not school:
            continue
        segment = ", ".join(part for part in [degree, school, field] if part)
        if segment:
            formatted.append(segment)
    return " / ".join(formatted)


def _format_experience(items: list[dict[str, Any]]) -> str:
    formatted = []
    for item in items[:6]:
        title = str(item.get("title") or item.get("position") or "").strip()
        companies = _experience_company_labels(item)
        company = companies[0] if companies else ""
        if not title and not company:
            continue
        formatted.append(", ".join(part for part in [company, title] if part))
    return " / ".join(formatted)


def _format_profile_languages(items: list[Any]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for item in list(items or [])[:16]:
        if isinstance(item, str):
            label = item.strip()
        elif isinstance(item, dict):
            label = _profile_label_from_value(
                item.get("name")
                or item.get("language")
                or item.get("title")
                or item.get("value")
                or item.get("text")
            )
            level = _profile_label_from_value(item.get("proficiency") or item.get("level"))
            if label and level and level.lower() not in label.lower():
                label = f"{label} ({level})"
        else:
            label = ""
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append(label)
        if len(results) >= 8:
            break
    return results


def _format_profile_skills(items: list[Any]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for item in list(items or [])[:32]:
        label = _profile_label_from_value(item)
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append(label)
        if len(results) >= 16:
            break
    return results


def _profile_label_from_value(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ["name", "title", "value", "text", "label", "schoolName", "school", "companyName", "company"]:
            text = str(value.get(key) or "").strip()
            if text:
                return text
    return str(value or "").strip() if isinstance(value, (int, float, bool)) else ""


def _infer_team_from_text(text: str) -> str:
    lowered = text.lower()
    if "infra" in lowered or "infrastructure" in lowered:
        return "Infrastructure"
    if "research" in lowered:
        return "Research"
    if "training" in lowered:
        return "Training"
    if "inference" in lowered or "serving" in lowered:
        return "Inference"
    return ""


def _looks_like_person_name(name: str) -> bool:
    if len(name.split()) < 2:
        return False
    if any(char.isdigit() for char in name):
        return False
    return True


def _fetch_text(url: str) -> str:
    http_request = request.Request(url, headers={"User-Agent": "Mozilla/5.0"}, method="GET")
    with request.urlopen(http_request, timeout=30) as response:
        return response.read().decode("utf-8", errors="ignore")
