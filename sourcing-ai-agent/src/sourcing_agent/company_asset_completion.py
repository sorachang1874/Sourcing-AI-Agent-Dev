from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .asset_logger import AssetLogger
from .candidate_artifacts import (
    _has_reusable_profile_fields,
    _resolve_company_snapshot,
    build_company_candidate_artifacts,
    build_evidence_records_from_payloads,
    materialize_company_candidate_view,
)
from .connectors import CompanyIdentity
from .domain import Candidate
from .enrichment import (
    DuckDuckGoLinkedInResolver,
    _apply_non_member_profile,
    _apply_verified_profile,
    _candidate_profile_urls,
    _names_match,
    _profile_matches_candidate,
    _recommended_harvest_profile_live_fetch_window,
    extract_linkedin_slug,
)
from .exploratory_enrichment import ExploratoryWebEnricher
from .harvest_connectors import HarvestProfileConnector, parse_harvest_profile_payload
from .model_provider import DeterministicModelClient, ModelClient
from .profile_registry_utils import (
    extract_profile_registry_aliases_from_payload,
    harvest_profile_payload_has_usable_content,
    profile_cache_path_candidates,
)
from .search_provider import build_search_provider
from .runtime_environment import external_provider_mode
from .runtime_tuning import resolved_harvest_profile_scrape_global_inflight, runtime_inflight_slot
from .settings import AppSettings
from .storage import SQLiteStore

_PROFILE_COMPLETION_URL_BATCH_SIZE = 100
_PROFILE_COMPLETION_PARALLEL_BATCH_WORKERS = 4
_PROFILE_REGISTRY_LEASE_SECONDS = 240
_PROFILE_REGISTRY_LEASE_WAIT_SECONDS = 18.0
_PROFILE_REGISTRY_LEASE_POLL_SECONDS = 0.8
_OPAQUE_PROFILE_CANONICAL_RESOLUTION_LIMIT = 100
_OPAQUE_PROFILE_CANONICAL_URLS_PER_CANDIDATE = 3


class CompanyAssetCompletionError(RuntimeError):
    pass


def _external_provider_mode() -> str:
    return external_provider_mode()


def _balanced_chunk_values(values: list[str], chunk_size: int) -> list[list[str]]:
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


def _recommended_profile_completion_fetch_window(
    total_urls: int,
    *,
    source_shards_by_url: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    count = max(0, int(total_urls or 0))
    if _external_provider_mode() == "live":
        return _recommended_harvest_profile_live_fetch_window(
            count,
            source_shards_by_url=source_shards_by_url,
        )
    if count <= 0:
        return {"batch_size": 1, "max_workers": 1, "batch_count": 0, "source_mix": {}}
    batch_size = _PROFILE_COMPLETION_URL_BATCH_SIZE
    batch_count = max(1, (count + batch_size - 1) // batch_size)
    return {
        "batch_size": batch_size,
        "max_workers": min(_PROFILE_COMPLETION_PARALLEL_BATCH_WORKERS, batch_count),
        "batch_count": batch_count,
        "source_mix": {},
    }


def _normalize_candidate_id_values(values: list[str] | set[str] | tuple[str, ...] | None) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in list(values or []):
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        normalized.append(item)
    return normalized


class CompanyAssetCompletionManager:
    def __init__(
        self,
        *,
        runtime_dir: str | Path,
        store: SQLiteStore,
        settings: AppSettings,
        model_client: ModelClient | None = None,
        harvest_profile_connector: HarvestProfileConnector | None = None,
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.store = store
        self.settings = settings
        self.model_client = model_client or DeterministicModelClient()
        self.search_provider = build_search_provider(settings.search)
        self.harvest_profile_connector = harvest_profile_connector or HarvestProfileConnector(
            settings.harvest.profile_scraper
        )
        self.exploratory_enricher = ExploratoryWebEnricher(
            self.model_client,
            worker_runtime=None,
            search_provider=self.search_provider,
        )

    def complete_company_assets(
        self,
        *,
        target_company: str,
        snapshot_id: str = "",
        profile_detail_limit: int = 12,
        exploration_limit: int = 3,
        build_artifacts: bool = True,
    ) -> dict[str, Any]:
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(
            self.runtime_dir, target_company, snapshot_id=snapshot_id
        )
        logger = AssetLogger(snapshot_dir)
        identity = CompanyIdentity(
            requested_name=str(identity_payload.get("requested_name") or target_company).strip() or target_company,
            canonical_name=str(identity_payload.get("canonical_name") or target_company).strip() or target_company,
            company_key=str(identity_payload.get("company_key") or company_key).strip() or company_key,
            linkedin_slug=str(identity_payload.get("linkedin_slug") or "").strip(),
            linkedin_company_url=str(identity_payload.get("linkedin_company_url") or "").strip(),
            domain=str(identity_payload.get("domain") or "").strip(),
            aliases=[str(item).strip() for item in list(identity_payload.get("aliases") or []) if str(item).strip()],
            resolver=str(identity_payload.get("resolver") or "materialized").strip() or "materialized",
            confidence=str(identity_payload.get("confidence") or "high").strip() or "high",
            notes=str(identity_payload.get("notes") or "").strip(),
            local_asset_available=bool(identity_payload.get("local_asset_available", True)),
            metadata=dict(identity_payload.get("metadata") or {}),
        )
        company_name = identity.canonical_name or target_company
        materialized_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company=company_name,
            snapshot_id=snapshot_dir.name,
        )
        materialized_candidates = list(materialized_view["candidates"])
        materialized_evidence = build_evidence_records_from_payloads(list(materialized_view["evidence"]))
        evidence_by_candidate = _evidence_by_candidate(materialized_evidence)
        for candidate in materialized_candidates:
            self.store.upsert_candidate(candidate)
        if materialized_evidence:
            self.store.upsert_evidence_records(materialized_evidence)

        if not materialized_candidates:
            raise CompanyAssetCompletionError(f"No materialized snapshot candidates found for {target_company}")

        completion_dir = snapshot_dir / "asset_completion"
        completion_dir.mkdir(parents=True, exist_ok=True)

        profile_targets = self._select_profile_targets(
            materialized_candidates, evidence_by_candidate=evidence_by_candidate, limit=profile_detail_limit
        )
        profile_results = self._complete_known_profile_targets(
            identity=identity,
            snapshot_dir=snapshot_dir,
            logger=logger,
            candidates=profile_targets,
        )

        refreshed_candidates = self.store.list_candidates_for_company(company_name)
        if not refreshed_candidates and company_name != target_company:
            refreshed_candidates = self.store.list_candidates_for_company(target_company)
        exploration_targets = self._select_exploration_targets(refreshed_candidates, limit=exploration_limit)
        exploration_result = None
        if exploration_targets:
            exploration_result = self.exploratory_enricher.enrich(
                snapshot_dir,
                exploration_targets,
                target_company=company_name,
                max_candidates=len(exploration_targets),
                asset_logger=logger,
                job_id="",
                request_payload={"target_company": company_name, "query": f"{company_name} asset completion"},
                plan_payload={"mode": "company_asset_completion"},
                runtime_mode="maintenance",
                parallel_workers=min(2, len(exploration_targets)),
            )
            for candidate in exploration_result.candidates:
                self.store.upsert_candidate(candidate)
            if exploration_result.evidence:
                self.store.upsert_evidence_records(exploration_result.evidence)

        post_exploration_candidates = self.store.list_candidates_for_company(company_name)
        if not post_exploration_candidates and company_name != target_company:
            post_exploration_candidates = self.store.list_candidates_for_company(target_company)
        refreshed_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company=company_name,
            snapshot_id=snapshot_dir.name,
        )
        post_exploration_evidence = build_evidence_records_from_payloads(list(refreshed_view["evidence"]))
        post_exploration_evidence_by_candidate = _evidence_by_candidate(post_exploration_evidence)
        followup_targets = self._select_profile_targets(
            post_exploration_candidates,
            evidence_by_candidate=post_exploration_evidence_by_candidate,
            limit=max(0, profile_detail_limit - int(profile_results.get("resolved_candidate_count") or 0)),
            preferred_ids={
                item["candidate_id"] for item in (exploration_result.explored_candidates if exploration_result else [])
            },
        )
        followup_results = (
            self._complete_known_profile_targets(
                identity=identity,
                snapshot_dir=snapshot_dir,
                logger=logger,
                candidates=followup_targets,
            )
            if followup_targets
            else {
                "provider_enabled": bool(
                    getattr(getattr(self.harvest_profile_connector, "settings", None), "enabled", True)
                ),
                "requested_candidate_count": 0,
                "requested_url_count": 0,
                "fetched_profile_count": 0,
                "resolved_candidate_count": 0,
                "completed_candidates": [],
                "non_member_candidates": [],
                "manual_review_candidates": [],
                "skipped_candidates": [],
                "errors": [],
            }
        )

        artifact_result = {}
        if build_artifacts:
            artifact_result = build_company_candidate_artifacts(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company=company_name,
                snapshot_id=snapshot_dir.name,
            )

        summary = {
            "status": "completed",
            "target_company": company_name,
            "company_key": company_key,
            "snapshot_id": snapshot_dir.name,
            "materialized_view": {
                "candidate_count": len(materialized_candidates),
                "evidence_count": len(materialized_evidence),
                "source_snapshot_count": len(list(materialized_view.get("source_snapshots") or [])),
                "sqlite_candidate_count": int(materialized_view.get("sqlite_candidate_count") or 0),
                "sqlite_evidence_count": int(materialized_view.get("sqlite_evidence_count") or 0),
            },
            "profile_completion": profile_results,
            "exploration": {
                "target_count": len(exploration_targets),
                "explored_count": len(exploration_result.explored_candidates) if exploration_result else 0,
                "error_count": len(exploration_result.errors) if exploration_result else 0,
                "artifact_paths": dict(exploration_result.artifact_paths) if exploration_result else {},
            },
            "followup_profile_completion": followup_results,
            "artifact_result": artifact_result,
        }
        summary_path = logger.write_json(
            completion_dir / "summary.json",
            summary,
            asset_type="company_asset_completion_summary",
            source_kind="company_asset_completion",
            is_raw_asset=False,
            model_safe=True,
        )
        summary["summary_path"] = str(summary_path)
        return summary

    def complete_snapshot_profiles(
        self,
        *,
        target_company: str,
        snapshot_id: str = "",
        employment_scope: str = "all",
        profile_limit: int = 12,
        only_missing_profile_detail: bool = True,
        force_refresh: bool = False,
        allow_live_refetch_for_unmatched: bool = True,
        build_artifacts: bool = True,
        candidate_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(
            self.runtime_dir, target_company, snapshot_id=snapshot_id
        )
        logger = AssetLogger(snapshot_dir)
        identity = CompanyIdentity(
            requested_name=str(identity_payload.get("requested_name") or target_company).strip() or target_company,
            canonical_name=str(identity_payload.get("canonical_name") or target_company).strip() or target_company,
            company_key=str(identity_payload.get("company_key") or company_key).strip() or company_key,
            linkedin_slug=str(identity_payload.get("linkedin_slug") or "").strip(),
            linkedin_company_url=str(identity_payload.get("linkedin_company_url") or "").strip(),
            domain=str(identity_payload.get("domain") or "").strip(),
            aliases=[str(item).strip() for item in list(identity_payload.get("aliases") or []) if str(item).strip()],
            resolver=str(identity_payload.get("resolver") or "materialized").strip() or "materialized",
            confidence=str(identity_payload.get("confidence") or "high").strip() or "high",
            notes=str(identity_payload.get("notes") or "").strip(),
            local_asset_available=bool(identity_payload.get("local_asset_available", True)),
            metadata=dict(identity_payload.get("metadata") or {}),
        )
        company_name = identity.canonical_name or target_company
        materialized_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company=company_name,
            snapshot_id=snapshot_dir.name,
        )
        materialized_candidates = list(materialized_view["candidates"])
        materialized_evidence = build_evidence_records_from_payloads(list(materialized_view["evidence"]))
        evidence_by_candidate = _evidence_by_candidate(materialized_evidence)
        normalized_candidate_ids = _normalize_candidate_id_values(candidate_ids)
        targets = self._select_targeted_profile_targets(
            materialized_candidates,
            evidence_by_candidate=evidence_by_candidate,
            employment_scope=employment_scope,
            limit=profile_limit,
            only_missing_profile_detail=only_missing_profile_detail,
            candidate_ids=set(normalized_candidate_ids),
        )
        result = self._complete_known_profile_targets(
            identity=identity,
            snapshot_dir=snapshot_dir,
            logger=logger,
            candidates=targets,
            force_refresh=force_refresh,
            allow_live_refetch_for_unmatched=allow_live_refetch_for_unmatched,
        )

        artifact_result = {}
        if build_artifacts:
            artifact_result = build_company_candidate_artifacts(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company=company_name,
                snapshot_id=snapshot_dir.name,
            )

        completion_dir = snapshot_dir / "asset_completion"
        completion_dir.mkdir(parents=True, exist_ok=True)
        scope_key = _normalize_profile_employment_scope(employment_scope)
        targeted_run = bool(normalized_candidate_ids)
        mode_key = "missing_detail" if only_missing_profile_detail else "all_known_urls"
        if targeted_run:
            mode_key = f"selected_{mode_key}"
        summary = {
            "status": "completed",
            "target_company": company_name,
            "company_key": company_key,
            "snapshot_id": snapshot_dir.name,
            "employment_scope": scope_key,
            "profile_mode": mode_key,
            "force_refresh": bool(force_refresh),
            "allow_live_refetch_for_unmatched": bool(allow_live_refetch_for_unmatched),
            "target_candidate_count": len(targets),
            "requested_candidate_ids": normalized_candidate_ids,
            "result": result,
            "artifact_result": artifact_result,
        }
        if targeted_run:
            summary_filename = (
                f"profile_enrichment_selected_candidates_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
            )
        else:
            summary_filename = f"profile_enrichment_{scope_key}_{mode_key}.json"
        summary_path = logger.write_json(
            completion_dir / summary_filename,
            summary,
            asset_type="company_asset_profile_enrichment_summary",
            source_kind="company_asset_completion",
            is_raw_asset=False,
            model_safe=True,
        )
        summary["summary_path"] = str(summary_path)
        return summary

    def _complete_known_profile_targets(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        logger: AssetLogger,
        candidates: list[Candidate],
        force_refresh: bool = False,
        allow_live_refetch_for_unmatched: bool = True,
    ) -> dict[str, Any]:
        provider_enabled = bool(getattr(getattr(self.harvest_profile_connector, "settings", None), "enabled", True))
        if not candidates:
            return {
                "provider_enabled": provider_enabled,
                "requested_candidate_count": 0,
                "requested_url_count": 0,
                "fetched_profile_count": 0,
                "resolved_candidate_count": 0,
                "completed_candidates": [],
                "non_member_candidates": [],
                "manual_review_candidates": [],
                "skipped_candidates": [],
                "errors": [],
            }
        requested_urls: list[str] = []
        candidate_by_url: dict[str, list[Candidate]] = defaultdict(list)
        source_shards_by_url: dict[str, set[str]] = defaultdict(set)
        for candidate in candidates:
            candidate_source_shards = _profile_registry_sources_for_candidate(candidate)
            for profile_url in _candidate_profile_urls(candidate):
                if profile_url not in requested_urls:
                    requested_urls.append(profile_url)
                candidate_by_url[profile_url].append(candidate)
                source_shards_by_url[profile_url].update(candidate_source_shards)
        errors: list[str] = []
        fetched, fetch_errors = self._fetch_profile_batches(
            requested_urls,
            snapshot_dir=snapshot_dir,
            logger=logger,
            use_cache=not force_refresh,
            source_shards_by_url={
                profile_url: sorted(list(source_shards_by_url.get(profile_url) or set()))
                for profile_url in requested_urls
            },
        )
        errors.extend(fetch_errors)
        completed_candidates: list[dict[str, Any]] = []
        non_member_candidates: list[dict[str, Any]] = []
        manual_review_candidates: list[dict[str, Any]] = []
        skipped_candidates: list[dict[str, Any]] = []
        resolved_ids: set[str] = set()
        for profile_url, payload in fetched.items():
            parsed = dict(payload.get("parsed") or {})
            raw_path = Path(str(payload.get("raw_path") or ""))
            matched = False
            prioritized_candidates: list[Candidate] = []
            prioritized_ids: set[str] = set()
            for candidate in candidate_by_url.get(profile_url, []):
                if candidate.candidate_id in prioritized_ids or candidate.candidate_id in resolved_ids:
                    continue
                prioritized_candidates.append(candidate)
                prioritized_ids.add(candidate.candidate_id)
            # Harvest batch responses are not guaranteed to preserve request order, so
            # fall back to the remaining unresolved candidates when the URL bucket misses.
            for candidate in candidates:
                if candidate.candidate_id in prioritized_ids or candidate.candidate_id in resolved_ids:
                    continue
                prioritized_candidates.append(candidate)
                prioritized_ids.add(candidate.candidate_id)
            for candidate in prioritized_candidates:
                if candidate.candidate_id in resolved_ids:
                    continue
                if not _profile_matches_candidate(parsed, candidate, identity, model_client=self.model_client):
                    continue
                merged_candidate, resolved_profile, evidence = _apply_verified_profile(
                    candidate,
                    parsed,
                    raw_path,
                    str(payload.get("account_id") or "harvest_profile_scraper"),
                    extract_linkedin_slug(profile_url),
                    identity,
                    model_client=self.model_client,
                    resolution_source="company_asset_completion",
                )
                self.store.upsert_candidate(merged_candidate)
                if evidence:
                    self.store.upsert_evidence_records(evidence)
                if resolved_profile.get("membership_review_required"):
                    manual_review_candidates.append(resolved_profile)
                else:
                    completed_candidates.append(resolved_profile)
                resolved_ids.add(candidate.candidate_id)
                matched = True
                break
            if not matched:
                errors.append(f"profile_completion_unmatched:{profile_url}")
        for candidate in candidates:
            if candidate.candidate_id in resolved_ids:
                continue
            skipped_candidates.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "display_name": candidate.display_name,
                    "linkedin_urls": _candidate_profile_urls(candidate),
                }
            )

        refreshed_profiles: dict[str, dict[str, Any]] = {}
        candidate_refresh_urls: dict[str, list[str]] = {}
        if allow_live_refetch_for_unmatched and skipped_candidates:
            unresolved_candidates = [
                candidate for candidate in candidates if candidate.candidate_id not in resolved_ids
            ]
            canonical_refresh_urls = self._resolve_canonical_profile_urls_for_unmatched(
                identity=identity,
                snapshot_dir=snapshot_dir,
                logger=logger,
                candidates=unresolved_candidates,
                source_shards_by_candidate={
                    candidate.candidate_id: sorted(list(_profile_registry_sources_for_candidate(candidate)))
                    for candidate in unresolved_candidates
                },
            )
            canonical_url_set: set[str] = set()
            for candidate_id, urls in canonical_refresh_urls.items():
                normalized_urls = _dedupe_strings(list(urls or []))
                if not normalized_urls:
                    continue
                candidate_refresh_urls[candidate_id] = normalized_urls
                for canonical_url in normalized_urls:
                    canonical_url_set.add(canonical_url)
                    candidate = next(
                        (item for item in unresolved_candidates if item.candidate_id == candidate_id), None
                    )
                    if candidate is None:
                        continue
                    candidate_by_url[canonical_url].append(candidate)
                    source_shards_by_url[canonical_url].update(_profile_registry_sources_for_candidate(candidate))

            if canonical_url_set:
                canonical_profiles, canonical_errors = self._fetch_profile_batches(
                    sorted(list(canonical_url_set)),
                    snapshot_dir=snapshot_dir,
                    logger=logger,
                    use_cache=True,
                    source_shards_by_url={
                        profile_url: sorted(list(source_shards_by_url.get(profile_url) or set()))
                        for profile_url in canonical_url_set
                    },
                )
                refreshed_profiles.update(canonical_profiles)
                errors.extend(canonical_errors)

            refresh_requested_urls: list[str] = []
            for item in skipped_candidates:
                candidate_id = str(item.get("candidate_id") or "").strip()
                if candidate_id and candidate_refresh_urls.get(candidate_id):
                    continue
                for profile_url in list(item.get("linkedin_urls") or []):
                    normalized_url = str(profile_url or "").strip()
                    if normalized_url and normalized_url not in refresh_requested_urls:
                        refresh_requested_urls.append(normalized_url)
            if refresh_requested_urls:
                retry_profiles, refresh_errors = self._fetch_profile_batches(
                    refresh_requested_urls,
                    snapshot_dir=snapshot_dir,
                    logger=logger,
                    use_cache=False,
                    source_shards_by_url={
                        profile_url: sorted(list(source_shards_by_url.get(profile_url) or set()))
                        for profile_url in refresh_requested_urls
                    },
                )
                refreshed_profiles.update(retry_profiles)
                errors.extend(refresh_errors)

        final_skipped_candidates: list[dict[str, Any]] = []
        for candidate in candidates:
            if candidate.candidate_id in resolved_ids:
                continue
            refreshed = False
            profile_urls = _dedupe_strings(
                [*list(candidate_refresh_urls.get(candidate.candidate_id) or []), *_candidate_profile_urls(candidate)]
            )
            for profile_url in profile_urls:
                profile = refreshed_profiles.get(profile_url)
                if profile is None:
                    continue
                refreshed = True
                parsed = dict(profile.get("parsed") or {})
                raw_path = Path(str(profile.get("raw_path") or ""))
                if _profile_matches_candidate(parsed, candidate, identity, model_client=self.model_client):
                    merged_candidate, resolved_profile, evidence = _apply_verified_profile(
                        candidate,
                        parsed,
                        raw_path,
                        str(profile.get("account_id") or "harvest_profile_scraper"),
                        extract_linkedin_slug(profile_url),
                        identity,
                        model_client=self.model_client,
                        resolution_source="company_asset_completion",
                    )
                    self.store.upsert_candidate(merged_candidate)
                    if evidence:
                        self.store.upsert_evidence_records(evidence)
                    if resolved_profile.get("membership_review_required"):
                        manual_review_candidates.append(resolved_profile)
                    else:
                        completed_candidates.append(resolved_profile)
                    resolved_ids.add(candidate.candidate_id)
                    break
                if _names_match(candidate.name_en, str(parsed.get("full_name") or "")):
                    merged_candidate, resolved_profile, evidence = _apply_non_member_profile(
                        candidate,
                        parsed,
                        raw_path,
                        str(profile.get("account_id") or "harvest_profile_scraper"),
                    )
                    self.store.upsert_candidate(merged_candidate)
                    if evidence:
                        self.store.upsert_evidence_records(evidence)
                    non_member_candidates.append(resolved_profile)
                    resolved_ids.add(candidate.candidate_id)
                    break
            if candidate.candidate_id in resolved_ids:
                continue
            if refreshed:
                errors.append(f"profile_completion_still_unmatched:{candidate.candidate_id}")
            final_skipped_candidates.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "display_name": candidate.display_name,
                    "linkedin_urls": profile_urls,
                }
            )
        fetch_window = _recommended_profile_completion_fetch_window(
            len(_dedupe_strings(requested_urls)),
            source_shards_by_url=source_shards_by_url,
        )
        url_batches = _balanced_chunk_values(requested_urls, int(fetch_window.get("batch_size") or 1))
        parallel_workers = min(
            len(url_batches),
            max(1, int(fetch_window.get("max_workers") or 1)),
        ) if url_batches else 1
        return {
            "provider_enabled": provider_enabled,
            "requested_candidate_count": len(candidates),
            "requested_url_count": len(requested_urls),
            "fetched_profile_count": len(fetched),
            "resolved_candidate_count": len(resolved_ids),
            "batch_count": len(url_batches),
            "parallel_batch_workers": parallel_workers,
            "profile_fetch_window": fetch_window,
            "completed_candidates": completed_candidates,
            "non_member_candidates": non_member_candidates,
            "manual_review_candidates": manual_review_candidates,
            "skipped_candidates": final_skipped_candidates,
            "errors": errors,
        }

    def _resolve_canonical_profile_urls_for_unmatched(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        logger: AssetLogger,
        candidates: list[Candidate],
        source_shards_by_candidate: dict[str, list[str]] | None = None,
    ) -> dict[str, list[str]]:
        source_shards_by_candidate = {
            str(candidate_id or "").strip(): list(values or [])
            for candidate_id, values in dict(source_shards_by_candidate or {}).items()
            if str(candidate_id or "").strip()
        }
        opaque_candidates: list[Candidate] = []
        for candidate in candidates:
            candidate_urls = _candidate_profile_urls(candidate)
            if not candidate_urls:
                continue
            if any(_looks_like_opaque_linkedin_url(profile_url) for profile_url in candidate_urls):
                opaque_candidates.append(candidate)
        if not opaque_candidates:
            return {}

        resolver = DuckDuckGoLinkedInResolver(search_provider=self.search_provider)
        resolver_result = resolver.resolve(
            opaque_candidates[:_OPAQUE_PROFILE_CANONICAL_RESOLUTION_LIMIT],
            identity,
            snapshot_dir,
            asset_logger=logger,
        )
        candidate_lookup: dict[str, Candidate] = {
            str(candidate.candidate_id or "").strip(): candidate
            for candidate in opaque_candidates
            if str(candidate.candidate_id or "").strip()
        }
        canonical_urls_by_candidate: dict[str, list[str]] = {}
        for item in list(resolver_result.get("results") or []):
            if not isinstance(item, dict):
                continue
            candidate_id = str(item.get("candidate_id") or "").strip()
            candidate = candidate_lookup.get(candidate_id)
            if candidate is None:
                continue
            existing_slugs = {
                str(extract_linkedin_slug(profile_url) or "").strip().lower()
                for profile_url in _candidate_profile_urls(candidate)
                if str(extract_linkedin_slug(profile_url) or "").strip()
            }
            resolved_urls: list[str] = []
            for slug in list(item.get("slugs") or []):
                normalized_slug = str(slug or "").strip()
                if not normalized_slug:
                    continue
                if normalized_slug.lower() in existing_slugs:
                    continue
                if _looks_like_opaque_linkedin_slug(normalized_slug):
                    continue
                canonical_url = f"https://www.linkedin.com/in/{normalized_slug}"
                if canonical_url not in resolved_urls:
                    resolved_urls.append(canonical_url)
                if len(resolved_urls) >= _OPAQUE_PROFILE_CANONICAL_URLS_PER_CANDIDATE:
                    break
            if not resolved_urls:
                continue
            canonical_urls_by_candidate[candidate_id] = resolved_urls
            primary_urls = _candidate_profile_urls(candidate)
            self.store.upsert_linkedin_profile_registry_sources(
                resolved_urls[0],
                source_shards=list(source_shards_by_candidate.get(candidate_id) or []),
                alias_urls=[*primary_urls, *resolved_urls],
                raw_linkedin_url=str(primary_urls[0] if primary_urls else "").strip(),
                sanity_linkedin_url=resolved_urls[0],
            )
        return canonical_urls_by_candidate

    def _fetch_profile_batches(
        self,
        requested_urls: list[str],
        *,
        snapshot_dir: Path,
        logger: AssetLogger,
        use_cache: bool,
        source_shards_by_url: dict[str, list[str]] | None = None,
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        normalized_urls = _dedupe_strings(requested_urls)
        if not normalized_urls:
            return {}, []
        fetched: dict[str, dict[str, Any]] = {}
        errors: list[str] = []
        lease_owner = _profile_registry_lease_owner("profile_completion")
        acquired_leases: dict[str, dict[str, Any]] = {}
        source_shards_by_url = {
            str(profile_url or "").strip(): list(values or [])
            for profile_url, values in dict(source_shards_by_url or {}).items()
            if str(profile_url or "").strip()
        }
        registry_entries = self.store.get_linkedin_profile_registry_bulk(normalized_urls)
        pending_urls: list[str] = []

        def _record_event(
            profile_url: str,
            *,
            event_type: str,
            event_status: str = "",
            detail: str = "",
            metadata: dict[str, Any] | None = None,
            duration_ms: int | None = None,
        ) -> None:
            self.store.record_linkedin_profile_registry_event(
                profile_url,
                event_type=event_type,
                event_status=event_status,
                detail=detail,
                metadata=metadata or {},
                duration_ms=duration_ms,
            )

        def _release_acquired_leases() -> None:
            for profile_url, lease_payload in list(acquired_leases.items()):
                self.store.release_linkedin_profile_registry_lease(
                    profile_url,
                    lease_owner=str(lease_payload.get("lease_owner") or lease_owner),
                    lease_token=str(lease_payload.get("lease_token") or ""),
                )
            acquired_leases.clear()

        def _wait_for_peer_fetch(
            profile_url: str,
            *,
            normalized_registry_key: str,
        ) -> dict[str, Any] | None:
            deadline = time.monotonic() + _PROFILE_REGISTRY_LEASE_WAIT_SECONDS
            while time.monotonic() < deadline:
                registry_entry = self.store.get_linkedin_profile_registry(profile_url) or {}
                registry_status = str(registry_entry.get("status") or "").strip().lower()
                if registry_status == "fetched":
                    cached_payload = _load_harvest_profile_from_raw_path(str(registry_entry.get("last_raw_path") or ""))
                    if cached_payload is not None:
                        return cached_payload
                    local_cached = _load_harvest_profile_from_snapshot_cache(
                        snapshot_dir=snapshot_dir,
                        profile_url=profile_url,
                        normalized_profile_key=normalized_registry_key,
                    )
                    if local_cached is not None:
                        return local_cached
                if registry_status == "unrecoverable":
                    return None
                time.sleep(_PROFILE_REGISTRY_LEASE_POLL_SECONDS)
            return None

        for profile_url in normalized_urls:
            source_shards = list(source_shards_by_url.get(profile_url) or [])
            registry_key = self.store.normalize_linkedin_profile_url(profile_url)
            registry_entry = dict(registry_entries.get(registry_key) or {})
            registry_status = str(registry_entry.get("status") or "").strip().lower()
            _record_event(
                profile_url,
                event_type="lookup_attempt",
                event_status=registry_status,
                metadata={"source_shards": source_shards},
            )
            if use_cache and registry_status == "fetched":
                cached = _load_harvest_profile_from_raw_path(str(registry_entry.get("last_raw_path") or ""))
                if cached is not None:
                    fetched[profile_url] = cached
                    alias_metadata = _profile_registry_alias_metadata(profile_url, cached)
                    self.store.upsert_linkedin_profile_registry_sources(
                        profile_url,
                        source_shards=source_shards,
                        alias_urls=list(alias_metadata.get("alias_urls") or []),
                        raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or ""),
                        sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                    )
                    _record_event(profile_url, event_type="cache_hit_registry")
                    continue
                self.store.mark_linkedin_profile_registry_failed(
                    profile_url,
                    error="registry_cached_raw_missing_or_invalid",
                    retryable=True,
                    source_shards=source_shards,
                    snapshot_dir=str(snapshot_dir),
                )
            if use_cache and registry_status == "queued":
                waited = _wait_for_peer_fetch(profile_url, normalized_registry_key=registry_key)
                if waited is not None:
                    fetched[profile_url] = waited
                    _record_event(profile_url, event_type="cache_hit_lease_wait")
                    continue
            if use_cache and registry_status == "unrecoverable":
                self.store.upsert_linkedin_profile_registry_sources(
                    profile_url,
                    source_shards=source_shards,
                )
                _record_event(profile_url, event_type="cache_skip_unrecoverable", event_status="unrecoverable")
                errors.append(f"profile_completion_registry_unrecoverable:{profile_url}")
                continue
            if use_cache:
                local_cached = _load_harvest_profile_from_snapshot_cache(
                    snapshot_dir=snapshot_dir,
                    profile_url=profile_url,
                    normalized_profile_key=registry_key,
                )
                if local_cached is not None:
                    fetched[profile_url] = local_cached
                    alias_metadata = _profile_registry_alias_metadata(profile_url, local_cached)
                    self.store.mark_linkedin_profile_registry_fetched(
                        profile_url,
                        raw_path=str(local_cached.get("raw_path") or ""),
                        source_shards=source_shards,
                        alias_urls=list(alias_metadata.get("alias_urls") or []),
                        raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or profile_url),
                        sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                        snapshot_dir=str(snapshot_dir),
                    )
                    _record_event(profile_url, event_type="cache_hit_local_raw")
                    continue
            lease_payload = self.store.acquire_linkedin_profile_registry_lease(
                profile_url,
                lease_owner=lease_owner,
                lease_seconds=_PROFILE_REGISTRY_LEASE_SECONDS,
            )
            if bool(lease_payload.get("acquired")):
                acquired_leases[profile_url] = lease_payload
                pending_urls.append(profile_url)
                continue
            waited = _wait_for_peer_fetch(profile_url, normalized_registry_key=registry_key)
            if waited is not None:
                fetched[profile_url] = waited
                _record_event(profile_url, event_type="cache_hit_lease_wait")
                continue
            _record_event(
                profile_url,
                event_type="lease_contended_skip",
                event_status=str(lease_payload.get("lease_owner") or ""),
                detail="peer lease active",
            )
            errors.append(f"profile_completion_registry_lease_contended:{profile_url}")

        if not pending_urls:
            return fetched, errors

        def _mark_queued(url_batch: list[str], *, run_id: str = "", dataset_id: str = "") -> None:
            for requested_url in url_batch:
                self.store.mark_linkedin_profile_registry_queued(
                    requested_url,
                    source_shards=list(source_shards_by_url.get(requested_url) or []),
                    run_id=run_id,
                    dataset_id=dataset_id,
                    snapshot_dir=str(snapshot_dir),
                )
                _record_event(
                    requested_url,
                    event_type="live_fetch_requested",
                    metadata={"batch_size": len(url_batch)},
                )

        def _mark_fetched(profile_url: str, payload: dict[str, Any]) -> None:
            raw_path = str(payload.get("raw_path") or "").strip()
            alias_metadata = _profile_registry_alias_metadata(profile_url, payload)
            before_entry = self.store.get_linkedin_profile_registry(profile_url) or {}
            retry_count_before = int(dict(before_entry).get("retry_count") or 0)
            fetched_entry = self.store.mark_linkedin_profile_registry_fetched(
                profile_url,
                raw_path=raw_path,
                source_shards=list(source_shards_by_url.get(profile_url) or []),
                alias_urls=list(alias_metadata.get("alias_urls") or []),
                raw_linkedin_url=str(alias_metadata.get("raw_linkedin_url") or profile_url),
                sanity_linkedin_url=str(alias_metadata.get("sanity_linkedin_url") or ""),
                snapshot_dir=str(snapshot_dir),
            )
            queue_duration_ms = _profile_registry_queue_duration_ms(dict(fetched_entry or {}))
            _record_event(
                profile_url,
                event_type="live_fetch_success",
                metadata={"retry_count_before": retry_count_before},
                duration_ms=queue_duration_ms,
            )

        def _mark_failed(url_batch: list[str], error_message: str) -> None:
            for requested_url in url_batch:
                before_entry = self.store.get_linkedin_profile_registry(requested_url) or {}
                retry_count_before = int(dict(before_entry).get("retry_count") or 0)
                self.store.mark_linkedin_profile_registry_failed(
                    requested_url,
                    error=error_message,
                    retryable=True,
                    source_shards=list(source_shards_by_url.get(requested_url) or []),
                    snapshot_dir=str(snapshot_dir),
                )
                _record_event(
                    requested_url,
                    event_type="live_fetch_failed",
                    event_status="failed_retryable",
                    detail=error_message,
                    metadata={"retry_count_before": retry_count_before},
                )

        def _fetch_batch(url_batch: list[str]) -> dict[str, dict[str, Any]]:
            with runtime_inflight_slot(
                "harvest_profile_scrape",
                budget=resolved_harvest_profile_scrape_global_inflight({}),
                metadata={"chunk_size": len(url_batch), "source": "company_asset_completion"},
            ):
                return self.harvest_profile_connector.fetch_profiles_by_urls(
                    url_batch,
                    snapshot_dir,
                    asset_logger=logger,
                    use_cache=use_cache,
                )

        fetch_window = _recommended_profile_completion_fetch_window(
            len(pending_urls),
            source_shards_by_url=source_shards_by_url,
        )
        url_batches = _balanced_chunk_values(pending_urls, int(fetch_window.get("batch_size") or 1))
        if not url_batches:
            return fetched, errors
        if len(url_batches) == 1:
            try:
                url_batch = url_batches[0]
                _mark_queued(url_batch)
                try:
                    batch_payload = _fetch_batch(url_batch) or {}
                except Exception as exc:
                    _mark_failed(url_batch, f"batch_failed:{exc}")
                    return fetched, [*errors, f"profile_completion_batch_failed:{len(url_batch)}:{exc}"]
                returned_urls = {
                    str(profile_url or "").strip()
                    for profile_url in batch_payload.keys()
                    if str(profile_url or "").strip()
                }
                for requested_url in url_batch:
                    if requested_url not in returned_urls:
                        before_entry = self.store.get_linkedin_profile_registry(requested_url) or {}
                        retry_count_before = int(dict(before_entry).get("retry_count") or 0)
                        self.store.mark_linkedin_profile_registry_failed(
                            requested_url,
                            error="batch_response_missing_profile",
                            retryable=True,
                            source_shards=list(source_shards_by_url.get(requested_url) or []),
                            snapshot_dir=str(snapshot_dir),
                        )
                        _record_event(
                            requested_url,
                            event_type="live_fetch_failed",
                            event_status="failed_retryable",
                            detail="batch_response_missing_profile",
                            metadata={"retry_count_before": retry_count_before},
                        )
                for profile_url, payload in batch_payload.items():
                    normalized_url = str(profile_url or "").strip()
                    if not normalized_url:
                        continue
                    fetched[normalized_url] = payload
                    _mark_fetched(normalized_url, payload)
                return fetched, errors
            finally:
                _release_acquired_leases()

        max_workers = min(len(url_batches), max(1, int(fetch_window.get("max_workers") or 1)))
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                batch_iter = iter(url_batches)
                futures: dict[Any, list[str]] = {}

                def _submit_next_batch() -> bool:
                    try:
                        url_batch = next(batch_iter)
                    except StopIteration:
                        return False
                    _mark_queued(url_batch)
                    future = executor.submit(_fetch_batch, list(url_batch))
                    futures[future] = list(url_batch)
                    return True

                for _ in range(max_workers):
                    if not _submit_next_batch():
                        break

                while futures:
                    done, _ = wait(set(futures.keys()), return_when=FIRST_COMPLETED)
                    for future in done:
                        url_batch = futures.pop(future)
                        try:
                            batch_payload = future.result() or {}
                        except Exception as exc:
                            _mark_failed(url_batch, f"batch_failed:{exc}")
                            errors.append(f"profile_completion_batch_failed:{len(url_batch)}:{exc}")
                            _submit_next_batch()
                            continue
                        returned_urls = {
                            str(profile_url or "").strip()
                            for profile_url in batch_payload.keys()
                            if str(profile_url or "").strip()
                        }
                        for requested_url in url_batch:
                            if requested_url not in returned_urls:
                                before_entry = self.store.get_linkedin_profile_registry(requested_url) or {}
                                retry_count_before = int(dict(before_entry).get("retry_count") or 0)
                                self.store.mark_linkedin_profile_registry_failed(
                                    requested_url,
                                    error="batch_response_missing_profile",
                                    retryable=True,
                                    source_shards=list(source_shards_by_url.get(requested_url) or []),
                                    snapshot_dir=str(snapshot_dir),
                                )
                                _record_event(
                                    requested_url,
                                    event_type="live_fetch_failed",
                                    event_status="failed_retryable",
                                    detail="batch_response_missing_profile",
                                    metadata={"retry_count_before": retry_count_before},
                                )
                        for profile_url, payload in batch_payload.items():
                            normalized_url = str(profile_url or "").strip()
                            if not normalized_url:
                                continue
                            fetched[normalized_url] = payload
                            _mark_fetched(normalized_url, payload)
                        _submit_next_batch()
            return fetched, errors
        finally:
            _release_acquired_leases()

    def _select_profile_targets(
        self,
        candidates: list[Candidate],
        *,
        evidence_by_candidate: dict[str, list[dict[str, Any]]],
        limit: int,
        preferred_ids: set[str] | None = None,
    ) -> list[Candidate]:
        preferred_ids = preferred_ids or set()
        targets = [
            candidate
            for candidate in candidates
            if _needs_profile_completion(candidate, evidence_by_candidate.get(candidate.candidate_id, []))
        ]
        targets.sort(
            key=lambda item: _profile_priority(item, preferred=item.candidate_id in preferred_ids), reverse=True
        )
        return targets[: max(0, limit)]

    def _select_targeted_profile_targets(
        self,
        candidates: list[Candidate],
        *,
        evidence_by_candidate: dict[str, list[dict[str, Any]]],
        employment_scope: str,
        limit: int,
        only_missing_profile_detail: bool,
        candidate_ids: set[str] | None = None,
    ) -> list[Candidate]:
        normalized_scope = _normalize_profile_employment_scope(employment_scope)
        selected_candidate_ids = {
            str(candidate_id or "").strip()
            for candidate_id in set(candidate_ids or set())
            if str(candidate_id or "").strip()
        }
        targets: list[Candidate] = []
        for candidate in candidates:
            candidate_id = str(candidate.candidate_id or "").strip()
            if selected_candidate_ids and candidate_id not in selected_candidate_ids:
                continue
            if not _candidate_profile_urls(candidate):
                continue
            status = str(candidate.employment_status or "").strip().lower()
            if normalized_scope == "current" and status != "current":
                continue
            if normalized_scope == "former" and status != "former":
                continue
            if (
                not selected_candidate_ids
                and only_missing_profile_detail
                and not _needs_profile_completion(candidate, evidence_by_candidate.get(candidate.candidate_id, []))
            ):
                continue
            targets.append(candidate)
        targets.sort(
            key=lambda item: _profile_priority(
                item,
                preferred=(
                    str(item.candidate_id or "").strip() in selected_candidate_ids
                    or (
                        only_missing_profile_detail
                        and _needs_profile_completion(item, evidence_by_candidate.get(item.candidate_id, []))
                    )
                ),
            ),
            reverse=True,
        )
        if selected_candidate_ids:
            return targets
        if limit <= 0:
            return targets
        return targets[:limit]

    def _select_exploration_targets(self, candidates: list[Candidate], *, limit: int) -> list[Candidate]:
        if limit <= 0:
            return []
        targets = [
            candidate
            for candidate in candidates
            if candidate.category == "lead"
            or (not str(candidate.linkedin_url or "").strip() and not _has_manual_review_confirmation(candidate))
        ]
        targets.sort(key=lambda item: _exploration_priority(item), reverse=True)
        return targets[:limit]


def _needs_profile_completion(candidate: Candidate, evidence: list[dict[str, Any]]) -> bool:
    linkedin_url = str(candidate.linkedin_url or "").strip()
    if not linkedin_url:
        return False
    if _has_reusable_profile_fields(candidate):
        return False
    return True


def _evidence_by_candidate(evidence: list[Any]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in evidence:
        if hasattr(item, "to_record"):
            record = dict(item.to_record())
        elif isinstance(item, dict):
            record = dict(item)
        else:
            continue
        candidate_id = str(record.get("candidate_id") or "").strip()
        if candidate_id:
            grouped[candidate_id].append(record)
    return grouped


def _profile_priority(candidate: Candidate, *, preferred: bool) -> tuple[int, int, int]:
    text = " ".join(
        [
            candidate.category,
            candidate.employment_status,
            candidate.role,
            candidate.team,
            candidate.focus_areas,
            candidate.notes,
        ]
    ).lower()
    relevance = sum(
        1
        for keyword in [
            "research",
            "engineer",
            "technical",
            "member of technical staff",
            "scientist",
            "ai",
            "ml",
            "openai",
            "anthropic",
            "xai",
        ]
        if keyword in text
    )
    category_score = 3 if candidate.category == "former_employee" else 2 if candidate.category == "lead" else 1
    return (1 if preferred else 0, category_score, relevance)


def _exploration_priority(candidate: Candidate) -> tuple[int, int]:
    has_media = 1 if str(candidate.media_url or "").strip() else 0
    return (2 if candidate.category == "lead" else 1, has_media)


def _normalize_profile_employment_scope(value: str) -> str:
    normalized = str(value or "all").strip().lower()
    if normalized in {"current", "former"}:
        return normalized
    return "all"


def _chunk_values(values: list[str], chunk_size: int) -> list[list[str]]:
    size = max(1, int(chunk_size or 1))
    return [list(values[index : index + size]) for index in range(0, len(values), size)]


def _looks_like_opaque_linkedin_slug(slug: str) -> bool:
    normalized = str(slug or "").strip()
    if not normalized:
        return False
    if normalized.lower().startswith("acw"):
        return True
    if re.search(r"[A-Z_]", normalized):
        return True
    return False


def _looks_like_opaque_linkedin_url(profile_url: str) -> bool:
    return _looks_like_opaque_linkedin_slug(extract_linkedin_slug(profile_url))


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


def _load_harvest_profile_from_snapshot_cache(
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
        cached_payload = _load_harvest_profile_from_raw_path(str(candidate_path))
        if cached_payload is not None:
            return cached_payload
    return None


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


def _load_harvest_profile_from_raw_path(raw_path: str) -> dict[str, Any] | None:
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


def _has_manual_review_confirmation(candidate: Candidate) -> bool:
    notes = str(candidate.notes or "").lower()
    return "manual review confirmed" in notes
