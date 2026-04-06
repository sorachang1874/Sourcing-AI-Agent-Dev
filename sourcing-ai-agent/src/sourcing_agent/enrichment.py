from __future__ import annotations

from dataclasses import dataclass, field
from html import unescape
from hashlib import sha1
from itertools import combinations
import json
from pathlib import Path
import re
from typing import Any
from urllib import error, parse, request
from xml.etree import ElementTree as ET

from .agent_runtime import AgentRuntimeCoordinator
from .asset_catalog import AssetCatalog
from .asset_logger import AssetLogger
from .connectors import CompanyIdentity, RapidApiAccount, profile_detail_accounts, search_people_accounts
from .domain import Candidate, EvidenceRecord, JobRequest, make_candidate_id, make_evidence_id, merge_candidate, normalize_name_token
from .exploratory_enrichment import ExploratoryWebEnricher
from .harvest_connectors import HarvestProfileConnector, HarvestProfileSearchConnector
from .model_provider import ModelClient
from .search_provider import BaseSearchProvider, DuckDuckGoHtmlSearchProvider, search_response_to_record


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

    def to_record(self) -> dict[str, Any]:
        return {
            "resolved_profile_count": len(self.resolved_profiles),
            "unresolved_candidate_count": len(self.unresolved_candidates),
            "publication_match_count": len(self.publication_matches),
            "lead_candidate_count": len(self.lead_candidates),
            "coauthor_edge_count": len(self.coauthor_edges),
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
    ) -> None:
        self.catalog = catalog
        resolved_search_provider = search_provider or DuckDuckGoHtmlSearchProvider()
        self.slug_resolver = LinkedInSearchSlugResolver(accounts, search_provider=resolved_search_provider)
        self.profile_connector = LinkedInProfileDetailConnector(accounts)
        self.publication_connector = CompanyPublicationConnector(catalog)
        self.harvest_profile_connector = harvest_profile_connector
        self.harvest_profile_search_connector = harvest_profile_search_connector
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
    ) -> MultiSourceEnrichmentResult:
        logger = asset_logger or AssetLogger(snapshot_dir)
        effective_cost_policy = dict(cost_policy or {})
        candidate_map = _candidate_name_map(candidates)
        evidence: list[EvidenceRecord] = []
        resolved_profiles: list[dict[str, Any]] = []
        unresolved_candidates: list[dict[str, Any]] = []
        artifact_paths: dict[str, str] = {}
        errors: list[str] = []

        prioritized = _prioritize_candidates(candidates)
        slug_resolution_limit = min(job_request.slug_resolution_limit, len(prioritized))
        profile_detail_limit = job_request.profile_detail_limit
        profile_fetch_count = 0
        prefetched_harvest_profiles: dict[str, dict[str, Any]] = {}
        if self.harvest_profile_connector is not None and slug_resolution_limit > 0 and profile_detail_limit > 0:
            known_profile_urls: list[str] = []
            for candidate in prioritized[:slug_resolution_limit]:
                for profile_url in _candidate_profile_urls(candidate):
                    if profile_url not in known_profile_urls:
                        known_profile_urls.append(profile_url)
            prefetched_harvest_profiles = self.harvest_profile_connector.fetch_profiles_by_urls(
                known_profile_urls,
                snapshot_dir,
                asset_logger=logger,
            )
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
                if not _profile_matches_candidate(profile["parsed"], candidate, identity):
                    continue

                merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                    candidate,
                    profile["parsed"],
                    profile["raw_path"],
                    profile["account_id"],
                    slug,
                    identity,
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

        publication_result = self.publication_connector.enrich(
            identity=identity,
            snapshot_dir=snapshot_dir,
            candidates=list(candidate_map.values()),
            asset_logger=logger,
            max_publications=job_request.publication_scan_limit,
            max_leads=job_request.publication_lead_limit,
            request_payload=request_payload or job_request.to_record(),
            plan_payload=plan_payload or {},
        )
        artifact_paths.update(publication_result["artifact_paths"])
        errors.extend(publication_result.get("errors", []))

        for candidate in publication_result["matched_candidates"]:
            candidate_map[_candidate_key(candidate)] = candidate
        for lead_candidate in publication_result["lead_candidates"]:
            key = _candidate_key(lead_candidate)
            existing = candidate_map.get(key)
            candidate_map[key] = merge_candidate(existing, lead_candidate) if existing else lead_candidate

        lead_candidates_to_resolve = _prioritize_candidates(publication_result["lead_candidates"])
        remaining_profile_budget = max(profile_detail_limit - profile_fetch_count, 0)
        if remaining_profile_budget > 0 and lead_candidates_to_resolve:
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
                    if profile is None or not _profile_matches_candidate(profile["parsed"], lead_candidate, identity):
                        continue
                    merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                        lead_candidate,
                        profile["parsed"],
                        profile["raw_path"],
                        profile["account_id"],
                        slug,
                        identity,
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

        exploration_targets = _exploration_targets(list(candidate_map.values()), unresolved_candidates)
        if job_request.exploration_limit > 0 and exploration_targets:
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
                parallel_workers=parallel_exploration_workers,
            )
            artifact_paths.update(exploration.artifact_paths)
            errors.extend(exploration.errors)
            evidence.extend(exploration.evidence)
            for explored_candidate in exploration.candidates:
                candidate_map[_candidate_key(explored_candidate)] = merge_candidate(
                    candidate_map.get(_candidate_key(explored_candidate), explored_candidate),
                    explored_candidate,
                )

            post_exploration_candidates = _prioritize_candidates(exploration.candidates)
            for explored_candidate in post_exploration_candidates:
                if profile_fetch_count >= profile_detail_limit:
                    break
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

        remaining_profile_budget = max(profile_detail_limit - profile_fetch_count, 0)
        remaining_leads_to_resolve = [
            item
            for item in _prioritize_candidates(
                [candidate_map.get(_candidate_key(candidate), candidate) for candidate in lead_candidates_to_resolve]
            )
            if item.category == "lead"
        ]
        if (
            bool(effective_cost_policy.get("allow_targeted_name_search_api", False))
            and remaining_profile_budget > 0
            and remaining_leads_to_resolve
        ):
            _, targeted_resolution_summary_path = self._resolve_publication_leads_with_harvest_search(
                lead_candidates=remaining_leads_to_resolve,
                identity=identity,
                snapshot_dir=snapshot_dir,
                remaining_profile_budget=remaining_profile_budget,
                candidate_map=candidate_map,
                resolved_profiles=resolved_profiles,
                unresolved_candidates=unresolved_candidates,
                evidence=evidence,
                asset_logger=logger,
            )
            if targeted_resolution_summary_path is not None:
                artifact_paths["publication_lead_targeted_resolution"] = str(targeted_resolution_summary_path)

        evidence.extend(publication_result["evidence"])
        final_candidates = list(candidate_map.values())
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
        )

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
    ) -> tuple[int, Path | None]:
        if (
            self.harvest_profile_search_connector is None
            or not self.harvest_profile_search_connector.settings.enabled
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
        attempts_summary: list[dict[str, Any]] = []

        for lead_candidate in lead_candidates:
            if searches_used >= remaining_profile_budget:
                break
            current_candidate = candidate_map.get(_candidate_key(lead_candidate), lead_candidate)
            if current_candidate.category != "lead":
                continue

            query_text = (current_candidate.display_name or current_candidate.name_en).strip()
            if not query_text:
                continue
            candidate_dir = search_root / current_candidate.candidate_id
            candidate_dir.mkdir(parents=True, exist_ok=True)
            attempt = {
                "candidate_id": current_candidate.candidate_id,
                "display_name": current_candidate.display_name,
                "query_text": query_text,
                "attempts": [],
                "resolved": False,
            }
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

            resolved = False
            for variant_index, variant in enumerate(search_variants, start=1):
                if searches_used >= remaining_profile_budget:
                    break
                search_result = self.harvest_profile_search_connector.search_profiles(
                    query_text=query_text,
                    filter_hints=dict(variant["filter_hints"]),
                    employment_status=str(variant["employment_status"]),
                    discovery_dir=candidate_dir,
                    asset_logger=asset_logger,
                    limit=min(self.harvest_profile_search_connector.settings.max_paid_items, 10),
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
                if search_result is None:
                    attempt["attempts"].append(variant_summary)
                    continue

                rows = [
                    row
                    for row in list(search_result.get("rows") or [])
                    if _names_match(current_candidate.name_en, str(row.get("full_name") or ""))
                ]
                variant_summary["matched_names"] = [str(row.get("full_name") or "") for row in rows]
                for row in rows:
                    profile_url = str(row.get("profile_url") or "").strip()
                    if not profile_url:
                        continue
                    profile = self.harvest_profile_connector.fetch_profile_by_url(
                        profile_url,
                        snapshot_dir,
                        asset_logger=asset_logger,
                    )
                    if profile is None or not _profile_matches_candidate(profile["parsed"], current_candidate, identity):
                        continue
                    merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                        current_candidate,
                        profile["parsed"],
                        profile["raw_path"],
                        profile["account_id"],
                        _extract_seed_slug(current_candidate) or extract_linkedin_slug(profile_url),
                        identity,
                        resolution_source=f"publication_lead_targeted_harvest_{variant['employment_status']}",
                    )
                    candidate_map[_candidate_key(current_candidate)] = merged_candidate
                    resolved_profiles.append(resolved_profile)
                    evidence.extend(profile_evidence)
                    _drop_unresolved_candidate(unresolved_candidates, current_candidate.candidate_id)
                    variant_summary["profile_url"] = profile_url
                    variant_summary["verified"] = True
                    attempt["resolved"] = True
                    resolved = True
                    break
                attempt["attempts"].append(variant_summary)
                if resolved:
                    break

            if not resolved:
                if current_candidate.candidate_id not in {
                    str(item.get("candidate_id") or "").strip() for item in unresolved_candidates
                }:
                    unresolved_candidates.append(
                        {
                            "candidate_id": current_candidate.candidate_id,
                            "display_name": current_candidate.display_name,
                            "attempted_slugs": [],
                            "query_summaries": attempt["attempts"],
                            "resolution_source": "publication_lead_targeted_harvest",
                        }
                    )
            attempts_summary.append(attempt)

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
    ) -> tuple[bool, int]:
        if profile_fetch_count >= profile_detail_limit:
            return False, profile_fetch_count

        profile_urls = _candidate_profile_urls(candidate)
        for profile_url in profile_urls:
            if self.harvest_profile_connector is None:
                continue
            profile = None
            if prefetched_harvest_profiles is not None:
                profile = prefetched_harvest_profiles.get(profile_url)
            if profile is None:
                profile = self.harvest_profile_connector.fetch_profile_by_url(profile_url, snapshot_dir, asset_logger=asset_logger)
            if profile is None or not _profile_matches_candidate(profile["parsed"], candidate, identity):
                continue
            profile_fetch_count += 1
            merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                candidate,
                profile["parsed"],
                profile["raw_path"],
                profile["account_id"],
                _extract_seed_slug(candidate) or extract_linkedin_slug(profile_url),
                identity,
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
            if profile is not None and _profile_matches_candidate(profile["parsed"], candidate, identity):
                merged_candidate, resolved_profile, profile_evidence = _apply_verified_profile(
                    candidate,
                    profile["parsed"],
                    profile["raw_path"],
                    profile["account_id"],
                    seed_slug,
                    identity,
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

        for publication in publications:
            matched_names: list[str] = []
            for name in publication.authors:
                candidate = candidate_map.get(_name_key(name))
                if candidate is not None:
                    matched_names.append(candidate.display_name or candidate.name_en)
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

            for left, right in combinations(sorted(set(matched_names)), 2):
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

        return {
            "matched_candidates": candidates,
            "lead_candidates": list(lead_map.values()),
            "evidence": evidence,
            "publication_matches": publication_matches,
            "coauthor_edges": coauthor_graph,
            "errors": errors,
            "artifact_paths": {
                "publication_matches": str(publication_summary_path),
                "coauthor_graph": str(coauthor_graph_path),
                "publication_leads": str(lead_candidates_path),
            },
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
            current_company = str(item.get("company") or item.get("company_name") or "").strip()
            break
    if not current_company and experience:
        current_company = str(experience[0].get("company") or experience[0].get("company_name") or "").strip()
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


def _profile_matches_candidate(profile: dict[str, Any], candidate: Candidate, identity: CompanyIdentity) -> bool:
    full_name = profile.get("full_name", "")
    if not _names_match(candidate.name_en, full_name):
        return False
    candidate_profile_urls = {str(item).strip().rstrip("/") for item in _candidate_profile_urls(candidate) if str(item).strip()}
    profile_url = str(profile.get("profile_url") or "").strip().rstrip("/")
    if profile_url and profile_url in candidate_profile_urls:
        return True
    company_variants = {_normalize(identity.canonical_name)}
    company_variants.update(_normalize(alias) for alias in identity.aliases if alias)
    current_company = str(profile.get("current_company", "")).strip()
    if _normalize(current_company) in company_variants:
        return True
    for item in profile.get("experience", []) or []:
        company = str(item.get("company") or item.get("company_name") or "").strip()
        if _normalize(company) in company_variants:
            return True
    return False


def _merge_profile_into_candidate(
    candidate: Candidate,
    profile: dict[str, Any],
    raw_path: Path,
    account_id: str,
    identity: CompanyIdentity,
) -> Candidate:
    resolved_category, resolved_employment_status = _classify_profile_membership(profile, identity)
    incoming = Candidate(
        candidate_id=candidate.candidate_id,
        name_en=profile.get("full_name") or candidate.name_en,
        display_name=profile.get("full_name") or candidate.display_name,
        category=resolved_category if candidate.category == "lead" else candidate.category,
        target_company=candidate.target_company,
        organization=candidate.organization,
        employment_status=resolved_employment_status if candidate.category == "lead" else candidate.employment_status,
        role=profile.get("headline") or candidate.role,
        team=candidate.team or _infer_team_from_text(profile.get("headline", "")),
        focus_areas=candidate.focus_areas or profile.get("headline", ""),
        education=_format_education(profile.get("education", [])),
        work_history=_format_experience(profile.get("experience", [])),
        notes=_join_nonempty(candidate.notes, profile.get("summary", ""), profile.get("location", "")),
        linkedin_url=profile.get("profile_url", ""),
        source_dataset=candidate.source_dataset,
        source_path=str(raw_path),
        metadata={
            **candidate.metadata,
            "public_identifier": profile.get("public_identifier", ""),
            "profile_account_id": account_id,
            "profile_location": profile.get("location", ""),
            "more_profiles": list(profile.get("more_profiles", []) or []),
        },
    )
    merged = merge_candidate(candidate, incoming)
    merged_record = merged.to_record()
    if profile.get("profile_url"):
        merged_record["linkedin_url"] = profile["profile_url"]
    if profile.get("headline"):
        merged_record["focus_areas"] = _join_nonempty(merged.focus_areas, profile["headline"])
    if incoming.education:
        merged_record["education"] = incoming.education
    if incoming.work_history:
        merged_record["work_history"] = incoming.work_history
    merged_record["notes"] = _join_nonempty(candidate.notes, profile.get("summary", ""), profile.get("location", ""))
    merged_record["source_path"] = str(raw_path)
    merged_record["metadata"] = incoming.metadata
    return Candidate(**merged_record)


def _profile_evidence_record(candidate: Candidate, profile: dict[str, Any], raw_path: Path) -> EvidenceRecord:
    title = profile.get("headline") or "LinkedIn profile detail"
    url = profile.get("profile_url") or candidate.linkedin_url
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
    resolution_source: str,
) -> tuple[Candidate, dict[str, Any], list[EvidenceRecord]]:
    merged_candidate = _merge_profile_into_candidate(candidate, profile, raw_path, account_id, identity)
    resolved_profile = {
        "candidate_id": candidate.candidate_id,
        "display_name": candidate.display_name,
        "slug": slug,
        "account_id": account_id,
        "profile_url": profile.get("profile_url", ""),
        "raw_path": str(raw_path),
        "resolution_source": resolution_source,
    }
    evidence = [_profile_evidence_record(merged_candidate, profile, raw_path)]
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


def _candidate_profile_urls(candidate: Candidate) -> list[str]:
    urls: list[str] = []
    for value in [candidate.linkedin_url, str(candidate.metadata.get("profile_url") or "").strip()]:
        normalized = str(value or "").strip()
        if normalized and normalized not in urls:
            urls.append(normalized)
    return urls


def _classify_profile_membership(profile: dict[str, Any], identity: CompanyIdentity) -> tuple[str, str]:
    company_variants = {_normalize(identity.canonical_name)}
    company_variants.update(_normalize(alias) for alias in identity.aliases if alias)
    current_company = str(profile.get("current_company", "")).strip()
    if _normalize(current_company) in company_variants:
        return "employee", "current"
    for item in profile.get("experience", []) or []:
        company = str(item.get("company") or item.get("company_name") or "").strip()
        if _normalize(company) in company_variants:
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
        metadata={"publication_id": publication.publication_id, "publication_url": publication.url},
    )


def _candidate_name_map(candidates: list[Candidate]) -> dict[str, Candidate]:
    return {_candidate_key(candidate): candidate for candidate in candidates}


def _candidate_key(candidate: Candidate) -> str:
    return _name_key(candidate.name_en)


def _name_key(name: str) -> str:
    return normalize_name_token(name)


def _prioritize_candidates(candidates: list[Candidate]) -> list[Candidate]:
    return sorted(candidates, key=lambda item: (-_candidate_priority(item), item.display_name))


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
    left_tokens = {token for token in re.split(r"[^A-Za-z0-9]+", left.lower()) if token}
    right_tokens = {token for token in re.split(r"[^A-Za-z0-9]+", right.lower()) if token}
    if not left_tokens or not right_tokens:
        return False
    return left_tokens <= right_tokens or right_tokens <= left_tokens or len(left_tokens & right_tokens) >= max(2, min(len(left_tokens), len(right_tokens)))


def _normalize(value: str) -> str:
    return value.strip().lower()


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
        school = str(item.get("school") or item.get("school_name") or "").strip()
        degree = str(item.get("degree") or "").strip()
        field = str(item.get("field_of_study") or "").strip()
        if not school:
            continue
        segment = ", ".join(part for part in [degree, school, field] if part)
        if segment:
            formatted.append(segment)
    return " / ".join(formatted)


def _format_experience(items: list[dict[str, Any]]) -> str:
    formatted = []
    for item in items[:6]:
        title = str(item.get("title") or "").strip()
        company = str(item.get("company") or item.get("company_name") or "").strip()
        if not title and not company:
            continue
        formatted.append(", ".join(part for part in [company, title] if part))
    return " / ".join(formatted)


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
