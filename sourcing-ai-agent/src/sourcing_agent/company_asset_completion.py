from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from .asset_logger import AssetLogger
from .candidate_artifacts import (
    _has_explicit_profile_capture,
    _has_reusable_profile_fields,
    _resolve_company_snapshot,
    build_company_candidate_artifacts,
    build_evidence_records_from_payloads,
    materialize_company_candidate_view,
)
from .connectors import CompanyIdentity
from .domain import Candidate
from .enrichment import (
    _apply_non_member_profile,
    _apply_verified_profile,
    _candidate_profile_urls,
    _names_match,
    _profile_matches_candidate,
    extract_linkedin_slug,
)
from .exploratory_enrichment import ExploratoryWebEnricher
from .harvest_connectors import HarvestProfileConnector
from .model_provider import DeterministicModelClient, ModelClient
from .search_provider import build_search_provider
from .settings import AppSettings
from .storage import SQLiteStore


class CompanyAssetCompletionError(RuntimeError):
    pass


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
        self.harvest_profile_connector = harvest_profile_connector or HarvestProfileConnector(settings.harvest.profile_scraper)
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
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(self.runtime_dir, target_company, snapshot_id=snapshot_id)
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
            raise CompanyAssetCompletionError(f"No candidates found in SQLite for {target_company}")

        completion_dir = snapshot_dir / "asset_completion"
        completion_dir.mkdir(parents=True, exist_ok=True)

        profile_targets = self._select_profile_targets(materialized_candidates, evidence_by_candidate=evidence_by_candidate, limit=profile_detail_limit)
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
            preferred_ids={item["candidate_id"] for item in (exploration_result.explored_candidates if exploration_result else [])},
        )
        followup_results = self._complete_known_profile_targets(
            identity=identity,
            snapshot_dir=snapshot_dir,
            logger=logger,
            candidates=followup_targets,
        ) if followup_targets else {
            "provider_enabled": bool(getattr(getattr(self.harvest_profile_connector, "settings", None), "enabled", True)),
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

    def _complete_known_profile_targets(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        logger: AssetLogger,
        candidates: list[Candidate],
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
        for candidate in candidates:
            for profile_url in _candidate_profile_urls(candidate):
                if profile_url not in requested_urls:
                    requested_urls.append(profile_url)
                candidate_by_url[profile_url].append(candidate)
        fetched = self.harvest_profile_connector.fetch_profiles_by_urls(requested_urls, snapshot_dir, asset_logger=logger)
        completed_candidates: list[dict[str, Any]] = []
        non_member_candidates: list[dict[str, Any]] = []
        manual_review_candidates: list[dict[str, Any]] = []
        skipped_candidates: list[dict[str, Any]] = []
        errors: list[str] = []
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

        refresh_requested_urls: list[str] = []
        for item in skipped_candidates:
            for profile_url in list(item.get("linkedin_urls") or []):
                normalized_url = str(profile_url or "").strip()
                if normalized_url and normalized_url not in refresh_requested_urls:
                    refresh_requested_urls.append(normalized_url)
        refreshed_profiles = self.harvest_profile_connector.fetch_profiles_by_urls(
            refresh_requested_urls,
            snapshot_dir,
            asset_logger=logger,
            use_cache=False,
        ) if refresh_requested_urls else {}

        final_skipped_candidates: list[dict[str, Any]] = []
        for candidate in candidates:
            if candidate.candidate_id in resolved_ids:
                continue
            refreshed = False
            for profile_url in _candidate_profile_urls(candidate):
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
                    "linkedin_urls": _candidate_profile_urls(candidate),
                }
            )
        return {
            "provider_enabled": provider_enabled,
            "requested_candidate_count": len(candidates),
            "requested_url_count": len(requested_urls),
            "fetched_profile_count": len(fetched),
            "resolved_candidate_count": len(resolved_ids),
            "completed_candidates": completed_candidates,
            "non_member_candidates": non_member_candidates,
            "manual_review_candidates": manual_review_candidates,
            "skipped_candidates": final_skipped_candidates,
            "errors": errors,
        }

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
        targets.sort(key=lambda item: _profile_priority(item, preferred=item.candidate_id in preferred_ids), reverse=True)
        return targets[: max(0, limit)]

    def _select_exploration_targets(self, candidates: list[Candidate], *, limit: int) -> list[Candidate]:
        if limit <= 0:
            return []
        targets = [
            candidate
            for candidate in candidates
            if candidate.category == "lead" or (not str(candidate.linkedin_url or "").strip() and not _has_manual_review_confirmation(candidate))
        ]
        targets.sort(key=lambda item: _exploration_priority(item), reverse=True)
        return targets[:limit]


def _needs_profile_completion(candidate: Candidate, evidence: list[dict[str, Any]]) -> bool:
    linkedin_url = str(candidate.linkedin_url or "").strip()
    if not linkedin_url:
        return False
    if _has_explicit_profile_capture(evidence):
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


def _has_manual_review_confirmation(candidate: Candidate) -> bool:
    notes = str(candidate.notes or "").lower()
    return "manual review confirmed" in notes
