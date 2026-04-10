from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from hashlib import sha1
import json
from pathlib import Path
import re
from shutil import copyfile
import time
from typing import Any
from urllib import error, parse, request

from .asset_logger import AssetLogger
from .connectors import CompanyIdentity, CompanyRosterSnapshot
from .settings import HarvestActorSettings


@dataclass(frozen=True, slots=True)
class HarvestExecutionArtifact:
    label: str
    payload: Any
    raw_format: str = "json"
    content_type: str = "application/json"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class HarvestExecutionResult:
    logical_name: str
    checkpoint: dict[str, Any] = field(default_factory=dict)
    body: Any | None = None
    pending: bool = False
    message: str = ""
    artifacts: list[HarvestExecutionArtifact] = field(default_factory=list)


@dataclass(slots=True)
class HarvestProfileConnector:
    settings: HarvestActorSettings

    def execute_batch_with_checkpoint(
        self,
        profile_urls: list[str],
        snapshot_dir: Path,
        *,
        checkpoint: dict[str, Any] | None = None,
        allow_shared_provider_cache: bool = True,
    ) -> HarvestExecutionResult:
        normalized_urls: list[str] = []
        for item in profile_urls:
            profile_url = str(item or "").strip()
            if profile_url and profile_url not in normalized_urls:
                normalized_urls.append(profile_url)
        if not normalized_urls:
            return HarvestExecutionResult(
                logical_name="harvest_profile_scraper_batch",
                checkpoint={"status": "completed"},
                body=[],
            )
        if not self.settings.enabled:
            raise RuntimeError("Harvest profile connector is not enabled.")
        batch_payload = {
            "urls": normalized_urls,
            "profileScraperMode": _profile_scraper_mode(self.settings),
        }
        effective_settings = replace(
            self.settings,
            timeout_seconds=max(
                int(self.settings.timeout_seconds or 0),
                _recommended_harvest_profile_timeout_seconds(
                    len(normalized_urls),
                    collect_email=self.settings.collect_email,
                ),
            ),
            max_paid_items=max(int(self.settings.max_paid_items or 0), len(normalized_urls)),
            max_total_charge_usd=max(
                float(self.settings.max_total_charge_usd or 0.0),
                _recommended_harvest_profile_charge_cap_usd(
                    batch_payload["profileScraperMode"],
                    len(normalized_urls),
                    fallback_per_1k=10.0,
                ),
            ),
        )
        return _execute_harvest_actor_with_checkpoint(
            effective_settings,
            logical_name="harvest_profile_scraper_batch",
            payload=batch_payload,
            base_path=snapshot_dir,
            checkpoint=checkpoint,
            request_context={"requested_urls": list(normalized_urls), "requested_url_count": len(normalized_urls)},
            allow_shared_provider_cache=allow_shared_provider_cache,
        )

    def fetch_profile_by_url(
        self,
        profile_url: str,
        snapshot_dir: Path,
        asset_logger: AssetLogger | None = None,
        *,
        use_cache: bool = True,
        allow_shared_provider_cache: bool = True,
    ) -> dict[str, Any] | None:
        results = self.fetch_profiles_by_urls(
            [profile_url],
            snapshot_dir,
            asset_logger=asset_logger,
            use_cache=use_cache,
            allow_shared_provider_cache=allow_shared_provider_cache,
        )
        return results.get(str(profile_url or "").strip())

    def fetch_profiles_by_urls(
        self,
        profile_urls: list[str],
        snapshot_dir: Path,
        asset_logger: AssetLogger | None = None,
        *,
        use_cache: bool = True,
        allow_shared_provider_cache: bool = True,
    ) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        normalized_urls: list[str] = []
        for item in profile_urls:
            profile_url = str(item or "").strip()
            if profile_url and profile_url not in normalized_urls:
                normalized_urls.append(profile_url)
        if not self.settings.enabled or not normalized_urls:
            return results
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        pending_urls: list[str] = []
        for profile_url in normalized_urls:
            raw_path = harvest_dir / f"{_profile_cache_key(profile_url)}.json"
            if use_cache and raw_path.exists():
                try:
                    payload = json.loads(raw_path.read_text())
                    logger.record_existing(
                        raw_path,
                        asset_type="harvest_profile_payload",
                        source_kind="harvest_profile_scraper",
                        content_type="application/json",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={"profile_url": profile_url, "cached": True},
                    )
                    results[profile_url] = {
                        "raw_path": raw_path,
                        "account_id": "harvest_profile_scraper",
                        "parsed": parse_harvest_profile_payload(payload),
                    }
                    continue
                except json.JSONDecodeError:
                    pass
            if use_cache and allow_shared_provider_cache:
                cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
                    snapshot_dir,
                    logical_name="harvest_profile_scraper",
                    payload={"profile_url": profile_url},
                )
                if cached_body is not None:
                    logger.write_json(
                        raw_path,
                        cached_body,
                        asset_type="harvest_profile_payload",
                        source_kind="harvest_profile_scraper",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={
                            "profile_url": profile_url,
                            "cached": True,
                            "cache_source": cache_source,
                            "cache_origin": str(cache_origin or ""),
                        },
                    )
                    results[profile_url] = {
                        "raw_path": raw_path,
                        "account_id": "harvest_profile_scraper",
                        "parsed": parse_harvest_profile_payload(cached_body),
                    }
                    continue
            pending_urls.append(profile_url)

        if not pending_urls:
            return results

        batch_results, unresolved_urls = self._fetch_profile_batch(
            pending_urls,
            snapshot_dir=snapshot_dir,
            asset_logger=logger,
            use_cache=use_cache,
            allow_shared_provider_cache=allow_shared_provider_cache,
            retry_label="initial_batch",
        )
        results.update(batch_results)

        remaining_unresolved = list(unresolved_urls)
        retry_batch_size = _initial_harvest_profile_retry_batch_size(len(remaining_unresolved))
        while remaining_unresolved and retry_batch_size > 0:
            next_unresolved: list[str] = []
            for url_batch in _chunk_values(remaining_unresolved, retry_batch_size):
                retry_results, retry_unresolved = self._fetch_profile_batch(
                    url_batch,
                    snapshot_dir=snapshot_dir,
                    asset_logger=logger,
                    use_cache=False,
                    allow_shared_provider_cache=allow_shared_provider_cache,
                    retry_label=f"retry_batch_{retry_batch_size}",
                )
                results.update(retry_results)
                next_unresolved.extend(retry_unresolved)
            remaining_unresolved = _dedupe_preserve_order(next_unresolved)
            retry_batch_size = _next_harvest_profile_retry_batch_size(retry_batch_size)

        if len(remaining_unresolved) <= _HARVEST_PROFILE_DIRECT_FALLBACK_THRESHOLD:
            for requested_url in remaining_unresolved:
                single = self._fetch_profile_request(
                    profile_url=requested_url,
                    snapshot_dir=snapshot_dir,
                    asset_logger=logger,
                    use_cache=False,
                    allow_shared_provider_cache=allow_shared_provider_cache,
                    skip_url_request=True,
                )
                if single is not None:
                    results[requested_url] = single
        return results

    def _fetch_profile_batch(
        self,
        profile_urls: list[str],
        *,
        snapshot_dir: Path,
        asset_logger: AssetLogger,
        use_cache: bool,
        allow_shared_provider_cache: bool,
        retry_label: str,
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        normalized_urls = _dedupe_preserve_order(profile_urls)
        if not normalized_urls:
            return {}, []
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        batch_payload = {
            "urls": normalized_urls,
            "profileScraperMode": _profile_scraper_mode(self.settings),
        }
        batch_raw_path = harvest_dir / f"harvest_profile_batch_{_payload_cache_key(batch_payload)}.json"
        body = None
        request_context = {
            "requested_url_count": len(normalized_urls),
            "requested_urls": list(normalized_urls),
            "retry_label": retry_label,
        }
        if use_cache and allow_shared_provider_cache:
            cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
                snapshot_dir,
                logical_name="harvest_profile_scraper_batch",
                payload=batch_payload,
            )
            if cached_body is not None:
                body = cached_body
                request_manifest_path = _write_harvest_request_manifest(
                    asset_logger,
                    batch_raw_path,
                    logical_name="harvest_profile_scraper_batch",
                    payload=batch_payload,
                    request_context={
                        **request_context,
                        "cache_status": str(cache_source or "shared_cache"),
                        "cache_origin": str(cache_origin or ""),
                    },
                )
                asset_logger.write_json(
                    batch_raw_path,
                    body,
                    asset_type="harvest_profile_batch_payload",
                    source_kind="harvest_profile_scraper",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={
                        "requested_url_count": len(normalized_urls),
                        "cached": True,
                        "cache_source": cache_source,
                        "cache_origin": str(cache_origin or ""),
                        "request_manifest_path": str(request_manifest_path),
                        "retry_label": retry_label,
                    },
                )
        effective_settings = replace(
            self.settings,
            timeout_seconds=max(
                int(self.settings.timeout_seconds or 0),
                _recommended_harvest_profile_timeout_seconds(
                    len(normalized_urls),
                    collect_email=self.settings.collect_email,
                ),
            ),
            max_paid_items=max(int(self.settings.max_paid_items or 0), len(normalized_urls)),
            max_total_charge_usd=max(
                float(self.settings.max_total_charge_usd or 0.0),
                _recommended_harvest_profile_charge_cap_usd(
                    batch_payload["profileScraperMode"],
                    len(normalized_urls),
                    fallback_per_1k=10.0,
                ),
            ),
        )
        if body is None:
            body = _run_harvest_actor(effective_settings, batch_payload)
            request_manifest_path = _write_harvest_request_manifest(
                asset_logger,
                batch_raw_path,
                logical_name="harvest_profile_scraper_batch",
                payload=batch_payload,
                request_context={
                    **request_context,
                    "cache_status": "live_api",
                    "effective_timeout_seconds": effective_settings.timeout_seconds,
                    "effective_max_total_charge_usd": effective_settings.max_total_charge_usd,
                    "effective_max_paid_items": effective_settings.max_paid_items,
                },
            )
            asset_logger.write_json(
                batch_raw_path,
                body,
                asset_type="harvest_profile_batch_payload",
                source_kind="harvest_profile_scraper",
                is_raw_asset=True,
                model_safe=False,
                metadata={
                    "requested_url_count": len(normalized_urls),
                    "cached": False,
                    "request_manifest_path": str(request_manifest_path),
                    "retry_label": retry_label,
                },
            )
            if body is not None:
                _persist_shared_harvest_payload(
                    snapshot_dir,
                    logical_name="harvest_profile_scraper_batch",
                    payload=batch_payload,
                    body=body,
                    request_context={
                        **request_context,
                        "effective_timeout_seconds": effective_settings.timeout_seconds,
                        "effective_max_total_charge_usd": effective_settings.max_total_charge_usd,
                        "effective_max_paid_items": effective_settings.max_paid_items,
                    },
                )
        persisted = self.persist_profiles_from_batch_body(
            normalized_urls,
            body,
            snapshot_dir,
            asset_logger=asset_logger,
        )
        return dict(persisted.get("profiles") or {}), list(persisted.get("unresolved_urls") or [])

    def persist_profiles_from_batch_body(
        self,
        profile_urls: list[str],
        body: Any,
        snapshot_dir: Path,
        asset_logger: AssetLogger | None = None,
    ) -> dict[str, Any]:
        normalized_urls: list[str] = []
        for item in profile_urls:
            profile_url = str(item or "").strip()
            if profile_url and profile_url not in normalized_urls:
                normalized_urls.append(profile_url)
        logger = asset_logger or AssetLogger(snapshot_dir)
        items = [item for item in (body if isinstance(body, list) else [body]) if isinstance(item, dict)] if body is not None else []
        matched_items, unresolved_urls = _match_harvest_profile_items_to_requested_urls(normalized_urls, items)
        persisted: dict[str, dict[str, Any]] = {}
        for requested_url, item in matched_items.items():
            persisted[requested_url] = self._persist_profile_payload(
                requested_url=requested_url,
                item=item,
                snapshot_dir=snapshot_dir,
                asset_logger=logger,
                request_kind="url",
                request_value=requested_url,
            )
        return {
            "profiles": persisted,
            "unresolved_urls": unresolved_urls,
        }

    def _fetch_profile_request(
        self,
        *,
        profile_url: str,
        snapshot_dir: Path,
        asset_logger: AssetLogger,
        use_cache: bool,
        allow_shared_provider_cache: bool,
        skip_url_request: bool = False,
    ) -> dict[str, Any] | None:
        normalized_url = str(profile_url or "").strip()
        if not normalized_url or not self.settings.enabled:
            return None
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        raw_path = harvest_dir / f"{_profile_cache_key(normalized_url)}.json"
        if use_cache and raw_path.exists():
            try:
                payload = json.loads(raw_path.read_text())
                asset_logger.record_existing(
                    raw_path,
                    asset_type="harvest_profile_payload",
                    source_kind="harvest_profile_scraper",
                    content_type="application/json",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"profile_url": normalized_url, "cached": True},
                )
                return {
                    "raw_path": raw_path,
                    "account_id": "harvest_profile_scraper",
                    "parsed": parse_harvest_profile_payload(payload),
                }
            except json.JSONDecodeError:
                pass
        if use_cache and allow_shared_provider_cache:
            cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
                snapshot_dir,
                logical_name="harvest_profile_scraper",
                payload={"profile_url": normalized_url},
            )
            if cached_body is not None:
                asset_logger.write_json(
                    raw_path,
                    cached_body,
                    asset_type="harvest_profile_payload",
                    source_kind="harvest_profile_scraper",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={
                        "profile_url": normalized_url,
                        "cached": True,
                        "cache_source": cache_source,
                        "cache_origin": str(cache_origin or ""),
                    },
                )
                return {
                    "raw_path": raw_path,
                    "account_id": "harvest_profile_scraper",
                    "parsed": parse_harvest_profile_payload(cached_body),
                }

        mode = _profile_scraper_mode(self.settings)
        request_variants: list[tuple[str, str, dict[str, Any]]] = []
        if not skip_url_request:
            request_variants.append(("url", normalized_url, {"urls": [normalized_url], "profileScraperMode": mode}))
        slug = _slug_from_linkedin_url(normalized_url)
        if slug:
            request_variants.append(("public_identifier", slug, {"publicIdentifiers": [slug], "profileScraperMode": mode}))
            request_variants.append(("profile_id", slug, {"profileIds": [slug], "profileScraperMode": mode}))

        for request_kind, request_value, payload in request_variants:
            body = _run_harvest_actor(self.settings, payload)
            item = body[0] if isinstance(body, list) and body else body
            if not isinstance(item, dict):
                continue
            return self._persist_profile_payload(
                requested_url=normalized_url,
                item=item,
                snapshot_dir=snapshot_dir,
                asset_logger=asset_logger,
                request_kind=request_kind,
                request_value=request_value,
            )
        return None

    def _persist_profile_payload(
        self,
        *,
        requested_url: str,
        item: dict[str, Any],
        snapshot_dir: Path,
        asset_logger: AssetLogger,
        request_kind: str,
        request_value: str,
    ) -> dict[str, Any]:
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        raw_path = harvest_dir / f"{_profile_cache_key(requested_url)}.json"
        wrapped_payload = {
            "_harvest_request": {
                "kind": request_kind,
                "value": request_value,
                "profile_url": requested_url,
            },
            "item": item,
        }
        asset_logger.write_json(
            raw_path,
            wrapped_payload,
            asset_type="harvest_profile_payload",
            source_kind="harvest_profile_scraper",
            is_raw_asset=True,
            model_safe=False,
            metadata={
                "profile_url": requested_url,
                "cached": False,
                "request_kind": request_kind,
                "request_value": request_value,
            },
        )
        _persist_shared_harvest_payload(
            snapshot_dir,
            logical_name="harvest_profile_scraper",
            payload={"profile_url": requested_url},
            body=wrapped_payload,
            request_context={
                "request_kind": request_kind,
                "request_value": request_value,
                "profile_url": requested_url,
            },
        )
        return {
            "raw_path": raw_path,
            "account_id": "harvest_profile_scraper",
            "parsed": parse_harvest_profile_payload(wrapped_payload),
        }


@dataclass(slots=True)
class HarvestProfileSearchConnector:
    settings: HarvestActorSettings

    def search_profiles(
        self,
        *,
        query_text: str,
        filter_hints: dict[str, list[str]],
        employment_status: str,
        discovery_dir: Path,
        asset_logger: AssetLogger | None = None,
        limit: int = 25,
        pages: int = 1,
        allow_shared_provider_cache: bool = True,
        auto_probe: bool = True,
    ) -> dict[str, Any] | None:
        query_text = str(query_text or "").strip()
        search_dir = discovery_dir / "harvest_profile_search"
        search_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(discovery_dir.parent)
        take_pages = max(1, min(int(pages or 1), 100))
        requested_limit = max(1, int(limit or 25))
        provider_cap_items = max(1, min(_HARVEST_PROVIDER_RESULT_CAP, take_pages * 25))
        requested_limit = min(requested_limit, provider_cap_items)
        past_companies = [str(item).strip() for item in list(filter_hints.get("past_companies") or []) if str(item).strip()]
        former_past_company_scan = (
            str(employment_status or "").strip().lower() == "former"
            and bool(past_companies)
            and not query_text
        )
        should_probe = bool(auto_probe) and (
            requested_limit > 25
            or take_pages > 1
            or former_past_company_scan
        )
        if should_probe:
            probe_result = self.search_profiles(
                query_text=query_text,
                filter_hints=filter_hints,
                employment_status=employment_status,
                discovery_dir=discovery_dir,
                asset_logger=asset_logger,
                limit=25,
                pages=1,
                allow_shared_provider_cache=allow_shared_provider_cache,
                auto_probe=False,
            )
            if probe_result is not None:
                pagination = dict(probe_result.get("pagination") or {})
                provider_total_count = max(0, int(pagination.get("total_elements") or 0))
                provider_total_pages = max(0, int(pagination.get("total_pages") or 0))
                probe_returned_count = max(0, int(pagination.get("returned_count") or len(list(probe_result.get("rows") or []))))
                if provider_total_count <= 0:
                    return probe_result
                if former_past_company_scan:
                    requested_limit = provider_total_count
                    take_pages = max(1, provider_total_pages or ((provider_total_count + 24) // 25))
                else:
                    requested_limit = min(requested_limit, provider_total_count)
                    total_pages_from_count = max(1, provider_total_pages or ((provider_total_count + 24) // 25))
                    take_pages = min(take_pages, total_pages_from_count)
                    requested_limit = min(requested_limit, take_pages * 25)
                take_pages = max(1, min(int(take_pages or 1), 100))
                provider_cap_items = max(1, min(_HARVEST_PROVIDER_RESULT_CAP, take_pages * 25))
                requested_limit = min(max(1, int(requested_limit or 25)), provider_cap_items)
                if take_pages == 1 and probe_returned_count >= requested_limit:
                    return probe_result

        page_coverage_min_items = max(1, ((take_pages - 1) * 25) + 1)
        max_items = requested_limit if requested_limit >= page_coverage_min_items else take_pages * 25
        max_items = min(max_items, max(1, min(_HARVEST_PROVIDER_RESULT_CAP, take_pages * 25)))
        effective_settings = replace(
            self.settings,
            timeout_seconds=max(
                int(self.settings.timeout_seconds or 0),
                _recommended_harvest_profile_search_timeout_seconds(take_pages),
            ),
            max_paid_items=max(int(self.settings.max_paid_items or 0), max_items),
            max_total_charge_usd=max(
                float(self.settings.max_total_charge_usd or 0.0),
                _recommended_harvest_profile_search_charge_cap_usd(take_pages),
            ),
        )
        payload = {
            "profileScraperMode": _profile_search_mode(self.settings),
            "maxItems": max(1, min(max_items, effective_settings.max_paid_items)),
            "startPage": 1,
            "takePages": take_pages,
        }
        if query_text:
            payload["searchQuery"] = query_text
        _apply_harvest_search_filters(payload, filter_hints, employment_status)
        raw_path = search_dir / f"{_payload_cache_key(payload)}.json"
        if raw_path.exists():
            try:
                cached = json.loads(raw_path.read_text())
                request_manifest_path = _write_harvest_request_manifest(
                    logger,
                    raw_path,
                    logical_name="harvest_profile_search",
                    payload=payload,
                    request_context={
                        "query_text": query_text,
                        "employment_status": employment_status,
                        "filter_hints": filter_hints,
                        "cache_status": "snapshot_raw_cache",
                    },
                )
                logger.record_existing(
                    raw_path,
                    asset_type="harvest_profile_search_payload",
                    source_kind="harvest_profile_search",
                    content_type="application/json",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={"query": query_text, "cached": True, "request_manifest_path": str(request_manifest_path)},
                )
                return {
                    "raw_path": raw_path,
                    "account_id": "harvest_profile_search",
                    "rows": parse_harvest_search_rows(cached),
                    "pagination": parse_harvest_search_pagination(cached),
                    "payload": cached,
                }
            except json.JSONDecodeError:
                pass
        cached_body = None
        cache_source = None
        cache_origin = None
        if allow_shared_provider_cache:
            cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
                discovery_dir,
                logical_name="harvest_profile_search",
                payload=payload,
            )
        if cached_body is not None:
            request_manifest_path = _write_harvest_request_manifest(
                logger,
                raw_path,
                logical_name="harvest_profile_search",
                payload=payload,
                request_context={
                    "query_text": query_text,
                    "employment_status": employment_status,
                    "filter_hints": filter_hints,
                    "cache_status": str(cache_source or "shared_cache"),
                    "cache_origin": str(cache_origin or ""),
                },
            )
            logger.write_json(
                raw_path,
                cached_body,
                asset_type="harvest_profile_search_payload",
                source_kind="harvest_profile_search",
                is_raw_asset=True,
                model_safe=False,
                metadata={
                    "query": query_text,
                    "cached": True,
                    "cache_source": cache_source,
                    "cache_origin": str(cache_origin or ""),
                    "request_manifest_path": str(request_manifest_path),
                },
            )
            return {
                "raw_path": raw_path,
                "account_id": "harvest_profile_search",
                "rows": parse_harvest_search_rows(cached_body),
                "pagination": parse_harvest_search_pagination(cached_body),
                "payload": cached_body,
            }
        if not self.settings.enabled:
            return None
        body = _run_harvest_actor(effective_settings, payload)
        if body is None:
            return None
        request_manifest_path = _write_harvest_request_manifest(
            logger,
            raw_path,
            logical_name="harvest_profile_search",
            payload=payload,
            request_context={
                "query_text": query_text,
                "employment_status": employment_status,
                "filter_hints": filter_hints,
                "cache_status": "live_api",
            },
        )
        logger.write_json(
            raw_path,
            body,
            asset_type="harvest_profile_search_payload",
            source_kind="harvest_profile_search",
            is_raw_asset=True,
            model_safe=False,
            metadata={"query": query_text, "cached": False, "request_manifest_path": str(request_manifest_path)},
        )
        _persist_shared_harvest_payload(
            discovery_dir,
            logical_name="harvest_profile_search",
            payload=payload,
            body=body,
            request_context={
                "query_text": query_text,
                "employment_status": employment_status,
                "filter_hints": filter_hints,
            },
        )
        return {
            "raw_path": raw_path,
            "account_id": "harvest_profile_search",
            "rows": parse_harvest_search_rows(body),
            "pagination": parse_harvest_search_pagination(body),
            "payload": body,
        }


@dataclass(slots=True)
class HarvestCompanyEmployeesConnector:
    settings: HarvestActorSettings

    def probe_company_roster_query(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger | None = None,
        company_filters: dict[str, Any] | None = None,
        probe_id: str = "",
        title: str = "",
        max_pages: int = 1,
        page_limit: int = 25,
    ) -> dict[str, Any]:
        if not self.settings.enabled:
            raise RuntimeError("Harvest company-employees connector is not enabled.")
        snapshot_root = Path(snapshot_dir).resolve()
        logger = asset_logger or AssetLogger(snapshot_root)
        probe_root = snapshot_root / "harvest_company_employees" / "probes"
        probe_root.mkdir(parents=True, exist_ok=True)
        payload, normalized_filters, requested_items = _build_harvest_company_employees_payload(
            self.settings,
            identity,
            max_pages=max_pages,
            page_limit=page_limit,
            company_filters=company_filters,
        )
        payload_hash = _payload_cache_key(payload)
        probe_key = str(probe_id or payload_hash).strip() or payload_hash
        base_name = f"harvest_company_employees_probe_{probe_key}"
        request_path = probe_root / f"{base_name}.request.json"
        run_post_path = probe_root / f"{base_name}.run_post.json"
        run_get_path = probe_root / f"{base_name}.run_get.json"
        dataset_path = probe_root / f"{base_name}.dataset_items.json"
        log_path = probe_root / f"{base_name}.log.txt"
        summary_path = probe_root / f"{base_name}.summary.json"

        cached_summary, cache_source, cache_origin = _load_cached_harvest_payload(
            snapshot_dir,
            logical_name="harvest_company_employees_probe_summary",
            payload=payload,
        )
        if isinstance(cached_summary, dict) and cached_summary:
            summary = {
                **dict(cached_summary),
                "summary_path": str(summary_path),
                "cache_source": str(cache_source or ""),
                "cache_origin": str(cache_origin or ""),
            }
            logger.write_json(
                summary_path,
                summary,
                asset_type="harvest_company_employees_probe_summary",
                source_kind="harvest_company_employees_probe",
                is_raw_asset=False,
                model_safe=True,
                metadata={"probe_id": probe_key, "payload_hash": payload_hash, "cache_source": str(cache_source or "")},
            )
            return summary

        logger.write_json(
            request_path,
            {
                "logical_name": "harvest_company_employees_probe",
                "payload_hash": payload_hash,
                "request_payload": payload,
                "request_context": {
                    "company_identity": identity.to_record(),
                    "company_filters": normalized_filters,
                    "probe_id": probe_id,
                    "title": title,
                    "max_pages": max_pages,
                    "page_limit": page_limit,
                },
            },
            asset_type="harvest_company_employees_probe_request",
            source_kind="harvest_company_employees_probe",
            is_raw_asset=False,
            model_safe=True,
            metadata={"probe_id": probe_key, "payload_hash": payload_hash},
        )

        effective_settings = replace(
            self.settings,
            timeout_seconds=max(180, min(900, int(self.settings.timeout_seconds or 0) or 180)),
            max_paid_items=max(int(self.settings.max_paid_items or 0), requested_items),
            max_total_charge_usd=max(
                float(self.settings.max_total_charge_usd or 0.0),
                _recommended_harvest_company_charge_cap_usd(payload["profileScraperMode"], requested_items),
            ),
        )
        submit_payload = _submit_harvest_actor_run(effective_settings, payload)
        logger.write_json(
            run_post_path,
            submit_payload,
            asset_type="harvest_company_employees_probe_run_post",
            source_kind="harvest_company_employees_probe",
            is_raw_asset=True,
            model_safe=False,
            metadata={"probe_id": probe_key, "payload_hash": payload_hash},
        )
        run = _apify_data_record(submit_payload)
        run_id = str(run.get("id") or run.get("runId") or "").strip()
        dataset_id = str(run.get("defaultDatasetId") or run.get("datasetId") or "").strip()
        if not run_id:
            raise RuntimeError("Harvest company-employees probe did not return a run id.")

        deadline = time.time() + max(60, int(effective_settings.timeout_seconds or 180) + 30)
        last_run_payload: dict[str, Any] = submit_payload if isinstance(submit_payload, dict) else {"data": run}
        run_status = _normalize_harvest_run_status(run.get("status") or "submitted")
        while not _harvest_run_is_terminal(run_status):
            if time.time() >= deadline:
                raise RuntimeError(f"Harvest company-employees probe timed out while waiting for run {run_id}.")
            time.sleep(2.0)
            last_run_payload = _get_harvest_actor_run(effective_settings, run_id)
            run = _apify_data_record(last_run_payload)
            dataset_id = str(run.get("defaultDatasetId") or run.get("datasetId") or dataset_id).strip()
            run_status = _normalize_harvest_run_status(run.get("status") or run_status or "running")

        logger.write_json(
            run_get_path,
            last_run_payload,
            asset_type="harvest_company_employees_probe_run_get",
            source_kind="harvest_company_employees_probe",
            is_raw_asset=True,
            model_safe=False,
            metadata={"probe_id": probe_key, "run_id": run_id, "dataset_id": dataset_id},
        )

        log_text = _get_harvest_actor_run_log(effective_settings, run_id)
        logger.write_text(
            log_path,
            log_text,
            asset_type="harvest_company_employees_probe_log",
            source_kind="harvest_company_employees_probe",
            content_type="text/plain",
            is_raw_asset=True,
            model_safe=False,
            metadata={"probe_id": probe_key, "run_id": run_id},
        )
        log_summary = parse_harvest_company_employee_run_log(log_text)

        returned_item_count = 0
        if _harvest_run_succeeded(run_status) and dataset_id:
            dataset_items = _get_harvest_dataset_items(effective_settings, dataset_id)
            returned_item_count = len(dataset_items) if isinstance(dataset_items, list) else int(bool(dataset_items))
            logger.write_json(
                dataset_path,
                dataset_items,
                asset_type="harvest_company_employees_probe_dataset_items",
                source_kind="harvest_company_employees_probe",
                is_raw_asset=True,
                model_safe=False,
                metadata={"probe_id": probe_key, "run_id": run_id, "dataset_id": dataset_id},
            )

        summary = {
            "probe_id": probe_key,
            "title": title,
            "status": "completed" if _harvest_run_succeeded(run_status) else run_status,
            "run_id": run_id,
            "dataset_id": dataset_id,
            "payload_hash": payload_hash,
            "company_identity": identity.to_record(),
            "company_filters": normalized_filters,
            "estimated_total_count": int(log_summary.get("estimated_total_count") or 0),
            "provider_result_limited": bool(log_summary.get("provider_result_limited")),
            "returned_item_count": returned_item_count,
            "log_summary": log_summary,
            "detail": str(log_summary.get("detail") or "").strip(),
            "summary_path": str(summary_path),
            "log_path": str(log_path),
        }
        logger.write_json(
            summary_path,
            summary,
            asset_type="harvest_company_employees_probe_summary",
            source_kind="harvest_company_employees_probe",
            is_raw_asset=False,
            model_safe=True,
            metadata={"probe_id": probe_key, "run_id": run_id, "dataset_id": dataset_id},
        )
        _persist_shared_harvest_payload(
            snapshot_dir,
            logical_name="harvest_company_employees_probe_summary",
            payload=payload,
            body=summary,
            request_context={
                "company_identity": identity.to_record(),
                "company_filters": normalized_filters,
                "probe_id": probe_key,
                "title": title,
            },
        )
        return summary

    def _resolve_probe_sized_payload(
        self,
        *,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        requested_payload: dict[str, Any],
        requested_items: int,
        normalized_filters: dict[str, Any],
        asset_logger: AssetLogger | None = None,
    ) -> tuple[dict[str, Any], int, dict[str, Any]]:
        probe_metadata: dict[str, Any] = {
            "probe_performed": False,
            "estimated_total_count": 0,
            "provider_result_limited": False,
            "status": "",
            "summary_path": "",
            "log_path": "",
            "error": "",
        }
        if requested_items <= 25:
            return dict(requested_payload), max(1, int(requested_items or 25)), probe_metadata
        effective_payload = dict(requested_payload)
        effective_items = max(1, int(requested_items or 25))
        try:
            summary = self.probe_company_roster_query(
                identity,
                snapshot_dir,
                asset_logger=asset_logger,
                company_filters=normalized_filters,
                max_pages=1,
                page_limit=25,
            )
        except Exception as exc:
            probe_metadata["error"] = str(exc)
            return effective_payload, effective_items, probe_metadata
        probe_metadata.update(
            {
                "probe_performed": True,
                "estimated_total_count": max(0, int(summary.get("estimated_total_count") or 0)),
                "provider_result_limited": bool(summary.get("provider_result_limited")),
                "status": str(summary.get("status") or "").strip(),
                "summary_path": str(summary.get("summary_path") or "").strip(),
                "log_path": str(summary.get("log_path") or "").strip(),
            }
        )
        estimated_total_count = int(probe_metadata.get("estimated_total_count") or 0)
        if estimated_total_count > 0:
            effective_items = min(effective_items, estimated_total_count)
            effective_pages = max(1, min((effective_items + 24) // 25, 100))
            effective_items = min(effective_items, effective_pages * 25)
            effective_payload["takePages"] = effective_pages
            effective_payload["maxItems"] = effective_items
        return effective_payload, max(1, int(effective_items or requested_items or 25)), probe_metadata

    def execute_with_checkpoint(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        *,
        max_pages: int = 5,
        page_limit: int = 25,
        company_filters: dict[str, Any] | None = None,
        checkpoint: dict[str, Any] | None = None,
        allow_shared_provider_cache: bool = True,
    ) -> HarvestExecutionResult:
        if not self.settings.enabled:
            raise RuntimeError("Harvest company-employees connector is not enabled.")
        payload, normalized_filters, requested_items = _build_harvest_company_employees_payload(
            self.settings,
            identity,
            max_pages=max_pages,
            page_limit=page_limit,
            company_filters=company_filters,
        )
        effective_requested_items = max(1, int(requested_items or 25))
        probe_metadata: dict[str, Any] = {}
        checkpoint_payload = dict(checkpoint or {})
        has_existing_run = bool(str(checkpoint_payload.get("run_id") or "").strip())
        if not has_existing_run:
            payload, effective_requested_items, probe_metadata = self._resolve_probe_sized_payload(
                identity=identity,
                snapshot_dir=snapshot_dir,
                requested_payload=payload,
                requested_items=effective_requested_items,
                normalized_filters=normalized_filters,
            )
        effective_settings = replace(
            self.settings,
            timeout_seconds=max(
                int(self.settings.timeout_seconds or 0),
                _recommended_harvest_company_timeout_seconds(effective_requested_items),
            ),
            max_paid_items=max(int(self.settings.max_paid_items or 0), effective_requested_items),
            max_total_charge_usd=max(
                float(self.settings.max_total_charge_usd or 0.0),
                _recommended_harvest_company_charge_cap_usd(payload["profileScraperMode"], effective_requested_items),
            ),
        )
        return _execute_harvest_actor_with_checkpoint(
            effective_settings,
            logical_name="harvest_company_employees",
            payload=payload,
            base_path=snapshot_dir,
            checkpoint=checkpoint,
            request_context={
                "company_identity": identity.to_record(),
                "max_pages": max_pages,
                "page_limit": page_limit,
                "effective_take_pages": int(payload.get("takePages") or max_pages or 1),
                "effective_max_items": int(payload.get("maxItems") or effective_requested_items),
                "company_filters": normalized_filters,
                "probe": probe_metadata,
            },
            allow_shared_provider_cache=allow_shared_provider_cache,
        )

    def fetch_company_roster(
        self,
        identity: CompanyIdentity,
        snapshot_dir: Path,
        *,
        asset_logger: AssetLogger | None = None,
        max_pages: int = 5,
        page_limit: int = 25,
        company_filters: dict[str, Any] | None = None,
        allow_shared_provider_cache: bool = True,
    ) -> CompanyRosterSnapshot:
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(snapshot_dir)
        payload, normalized_filters, requested_items = _build_harvest_company_employees_payload(
            self.settings,
            identity,
            max_pages=max_pages,
            page_limit=page_limit,
            company_filters=company_filters,
        )
        effective_requested_items = max(1, int(requested_items or 25))
        probe_metadata: dict[str, Any] = {}
        raw_path = harvest_dir / "harvest_company_employees_raw.json"
        body = None
        if raw_path.exists():
            try:
                body = json.loads(raw_path.read_text())
                request_manifest_path = _write_harvest_request_manifest(
                    logger,
                    raw_path,
                    logical_name="harvest_company_employees",
                    payload=payload,
                    request_context={
                        "company_identity": identity.to_record(),
                        "max_pages": max_pages,
                        "page_limit": page_limit,
                        "company_filters": normalized_filters,
                        "cache_status": "snapshot_raw_cache",
                    },
                )
                logger.record_existing(
                    raw_path,
                    asset_type="harvest_company_employees_payload",
                    source_kind="harvest_company_employees",
                    content_type="application/json",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={
                        "company": identity.canonical_name,
                        "cached": True,
                        "request_manifest_path": str(request_manifest_path),
                    },
                )
            except json.JSONDecodeError:
                body = None
        if body is None:
            queue_summary_path = harvest_dir / "harvest_company_employees_queue_summary.json"
            if queue_summary_path.exists():
                try:
                    queue_summary = json.loads(queue_summary_path.read_text())
                    queue_artifacts = dict(queue_summary.get("artifact_paths") or {})
                    dataset_items_path_value = str(
                        queue_artifacts.get("dataset_items")
                        or (harvest_dir / "harvest_company_employees_queue_dataset_items.json")
                    ).strip()
                    dataset_items_path = Path(dataset_items_path_value).expanduser() if dataset_items_path_value else None
                    if str(queue_summary.get("status") or "").strip().lower() == "completed" and dataset_items_path and dataset_items_path.exists():
                        body = json.loads(dataset_items_path.read_text())
                        request_manifest_path = _write_harvest_request_manifest(
                            logger,
                            raw_path,
                            logical_name="harvest_company_employees",
                            payload=payload,
                            request_context={
                                "company_identity": identity.to_record(),
                                "max_pages": max_pages,
                                "page_limit": page_limit,
                                "company_filters": normalized_filters,
                                "cache_status": "completed_queue_dataset",
                                "queue_summary_path": str(queue_summary_path),
                                "dataset_items_path": str(dataset_items_path),
                            },
                        )
                        logger.write_json(
                            raw_path,
                            body,
                            asset_type="harvest_company_employees_payload",
                            source_kind="harvest_company_employees",
                            is_raw_asset=True,
                            model_safe=False,
                            metadata={
                                "company": identity.canonical_name,
                                "cached": True,
                                "cache_source": "completed_queue_dataset",
                                "queue_summary_path": str(queue_summary_path),
                                "dataset_items_path": str(dataset_items_path),
                                "request_manifest_path": str(request_manifest_path),
                            },
                        )
                except (OSError, json.JSONDecodeError):
                    body = None
        if body is None:
            cached_body = None
            cache_source = None
            cache_origin = None
            if allow_shared_provider_cache:
                cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
                    snapshot_dir,
                    logical_name="harvest_company_employees",
                    payload=payload,
                )
            if cached_body is not None:
                body = cached_body
                request_manifest_path = _write_harvest_request_manifest(
                    logger,
                    raw_path,
                    logical_name="harvest_company_employees",
                    payload=payload,
                    request_context={
                        "company_identity": identity.to_record(),
                        "max_pages": max_pages,
                        "page_limit": page_limit,
                        "company_filters": normalized_filters,
                        "cache_status": str(cache_source or "shared_cache"),
                        "cache_origin": str(cache_origin or ""),
                    },
                )
                logger.write_json(
                    raw_path,
                    body,
                    asset_type="harvest_company_employees_payload",
                    source_kind="harvest_company_employees",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={
                        "company": identity.canonical_name,
                        "cached": True,
                        "cache_source": cache_source,
                        "cache_origin": str(cache_origin or ""),
                        "request_manifest_path": str(request_manifest_path),
                    },
                )
        if body is None:
            if not self.settings.enabled:
                raise RuntimeError("Harvest company-employees connector is not enabled.")
            payload_before_probe = dict(payload)
            payload, effective_requested_items, probe_metadata = self._resolve_probe_sized_payload(
                identity=identity,
                snapshot_dir=snapshot_dir,
                requested_payload=payload,
                requested_items=effective_requested_items,
                normalized_filters=normalized_filters,
                asset_logger=logger,
            )
            payload_changed_after_probe = payload != payload_before_probe
            if payload_changed_after_probe and allow_shared_provider_cache:
                cached_body = None
                cache_source = None
                cache_origin = None
                cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
                    snapshot_dir,
                    logical_name="harvest_company_employees",
                    payload=payload,
                )
                if cached_body is not None:
                    body = cached_body
                    request_manifest_path = _write_harvest_request_manifest(
                        logger,
                        raw_path,
                        logical_name="harvest_company_employees",
                        payload=payload,
                        request_context={
                            "company_identity": identity.to_record(),
                            "max_pages": max_pages,
                            "page_limit": page_limit,
                            "effective_take_pages": int(payload.get("takePages") or max_pages or 1),
                            "effective_max_items": int(payload.get("maxItems") or effective_requested_items),
                            "company_filters": normalized_filters,
                            "cache_status": str(cache_source or "shared_cache"),
                            "cache_origin": str(cache_origin or ""),
                            "probe": probe_metadata,
                        },
                    )
                    logger.write_json(
                        raw_path,
                        body,
                        asset_type="harvest_company_employees_payload",
                        source_kind="harvest_company_employees",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={
                            "company": identity.canonical_name,
                            "cached": True,
                            "cache_source": cache_source,
                            "cache_origin": str(cache_origin or ""),
                            "request_manifest_path": str(request_manifest_path),
                        },
                    )
            if body is None:
                effective_settings = replace(
                    self.settings,
                    timeout_seconds=max(
                        int(self.settings.timeout_seconds or 0),
                        _recommended_harvest_company_timeout_seconds(effective_requested_items),
                    ),
                    max_paid_items=max(int(self.settings.max_paid_items or 0), effective_requested_items),
                    max_total_charge_usd=max(
                        float(self.settings.max_total_charge_usd or 0.0),
                        _recommended_harvest_company_charge_cap_usd(payload["profileScraperMode"], effective_requested_items),
                    ),
                )
                body = _run_harvest_actor(effective_settings, payload)
                if body is None:
                    raise RuntimeError("Harvest company-employees run failed.")
                request_manifest_path = _write_harvest_request_manifest(
                    logger,
                    raw_path,
                    logical_name="harvest_company_employees",
                    payload=payload,
                    request_context={
                        "company_identity": identity.to_record(),
                        "max_pages": max_pages,
                        "page_limit": page_limit,
                        "effective_take_pages": int(payload.get("takePages") or max_pages or 1),
                        "effective_max_items": int(payload.get("maxItems") or effective_requested_items),
                        "company_filters": normalized_filters,
                        "cache_status": "live_api",
                        "probe": probe_metadata,
                    },
                )
                logger.write_json(
                    raw_path,
                    body,
                    asset_type="harvest_company_employees_payload",
                    source_kind="harvest_company_employees",
                    is_raw_asset=True,
                    model_safe=False,
                    metadata={
                        "company": identity.canonical_name,
                        "cached": False,
                        "request_manifest_path": str(request_manifest_path),
                    },
                )
                _persist_shared_harvest_payload(
                    snapshot_dir,
                    logical_name="harvest_company_employees",
                    payload=payload,
                    body=body,
                    request_context={
                        "company_identity": identity.to_record(),
                        "max_pages": max_pages,
                        "page_limit": page_limit,
                        "effective_take_pages": int(payload.get("takePages") or max_pages or 1),
                        "effective_max_items": int(payload.get("maxItems") or effective_requested_items),
                        "company_filters": normalized_filters,
                        "probe": probe_metadata,
                    },
                )
        rows = parse_harvest_company_employee_rows(body)
        deduped_entries = _dedupe_harvest_roster_entries(rows)
        visible_entries = [entry for entry in deduped_entries if not entry["is_headless"]]
        headless_entries = [entry for entry in deduped_entries if entry["is_headless"]]
        merged_path = harvest_dir / "harvest_company_employees_merged.json"
        visible_path = harvest_dir / "harvest_company_employees_visible.json"
        headless_path = harvest_dir / "harvest_company_employees_headless.json"
        summary_path = harvest_dir / "harvest_company_employees_summary.json"
        page_summaries = _build_harvest_roster_page_summaries(rows)
        logger.write_json(
            merged_path,
            deduped_entries,
            asset_type="company_roster_merged",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            visible_path,
            visible_entries,
            asset_type="company_roster_visible",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            headless_path,
            headless_entries,
            asset_type="company_roster_headless",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            summary_path,
            {
                "company_identity": identity.to_record(),
                "company_filters": normalized_filters,
                "raw_entry_count": len(rows),
                "visible_entry_count": len(visible_entries),
                "headless_entry_count": len(headless_entries),
                "page_summaries": page_summaries,
                "accounts_used": ["harvest_company_employees"],
                "errors": [],
                "stop_reason": "completed",
            },
            asset_type="company_roster_summary",
            source_kind="harvest_company_employees",
            is_raw_asset=False,
            model_safe=True,
        )
        return CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=deduped_entries,
            visible_entries=visible_entries,
            headless_entries=headless_entries,
            page_summaries=page_summaries,
            accounts_used=["harvest_company_employees"],
            errors=[],
            stop_reason="completed",
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=summary_path,
        )


def parse_harvest_profile_payload(payload: dict[str, Any]) -> dict[str, Any]:
    request_metadata = payload.get("_harvest_request") if isinstance(payload.get("_harvest_request"), dict) else {}
    data = payload.get("item") if isinstance(payload.get("item"), dict) else payload
    full_name = _full_name_from_payload(data)
    headline = str(data.get("headline") or data.get("occupation") or "").strip()
    profile_url = str(data.get("linkedinUrl") or data.get("profileUrl") or data.get("url") or "").strip()
    requested_profile_url = str(request_metadata.get("profile_url") or "").strip()
    public_identifier = str(data.get("publicIdentifier") or data.get("public_identifier") or "").strip()
    summary = str(data.get("about") or data.get("summary") or data.get("description") or "").strip()
    location_normalized = _normalized_harvest_location(data.get("location") or data.get("locationName"))
    location = str(location_normalized.get("raw_text") or "").strip()
    experience = _coerce_list(data.get("experience") or data.get("experiences") or data.get("positions"))
    education = _coerce_list(data.get("education") or data.get("educations") or data.get("schools"))
    languages = _coerce_list(data.get("languages") or data.get("language") or [])
    skills = _coerce_list(data.get("skills") or data.get("topSkills") or data.get("top_skills") or [])
    publications = _coerce_list(data.get("publications") or data.get("posts") or [])
    current_company = _extract_current_company(data, experience)
    more_profiles = _coerce_list(data.get("moreProfiles") or data.get("more_profiles") or [])
    return {
        "full_name": full_name,
        "first_name": str(data.get("firstName") or data.get("first_name") or "").strip(),
        "last_name": str(data.get("lastName") or data.get("last_name") or "").strip(),
        "headline": headline,
        "profile_url": profile_url,
        "requested_profile_url": requested_profile_url,
        "public_identifier": public_identifier,
        "summary": summary,
        "location": location,
        "location_normalized": location_normalized,
        "current_company": current_company,
        "experience": experience,
        "education": education,
        "languages": languages,
        "skills": skills,
        "publications": publications,
        "more_profiles": more_profiles,
    }


def parse_harvest_search_rows(payload: Any) -> list[dict[str, Any]]:
    items = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        data = item.get("item") if isinstance(item.get("item"), dict) else item
        profile_url = str(data.get("linkedinUrl") or data.get("profileUrl") or data.get("url") or "").strip()
        rows.append(
            {
                "full_name": _full_name_from_payload(data),
                "headline": _headline_from_payload(data),
                "location": _location_text(data.get("location") or data.get("locationName")),
                "location_normalized": _normalized_harvest_location(data.get("location") or data.get("locationName")),
                "profile_url": profile_url,
                "username": str(data.get("publicIdentifier") or data.get("public_identifier") or "").strip() or _slug_from_linkedin_url(profile_url),
                "current_company": str(data.get("currentCompany") or data.get("current_company") or "").strip(),
            }
        )
    return rows


def parse_harvest_search_pagination(payload: Any) -> dict[str, int]:
    items = payload if isinstance(payload, list) else [payload]
    returned_count = 0
    summary = {
        "returned_count": 0,
        "total_elements": 0,
        "total_pages": 0,
        "page_number": 0,
        "page_size": 0,
    }
    for item in items:
        if not isinstance(item, dict):
            continue
        returned_count += 1
        data = item.get("item") if isinstance(item.get("item"), dict) else item
        meta = item.get("_meta")
        if not isinstance(meta, dict) and isinstance(data, dict):
            meta = data.get("_meta")
        pagination = meta.get("pagination") if isinstance(meta, dict) else {}
        if not isinstance(pagination, dict):
            continue
        for source_key, target_key in (
            ("totalElements", "total_elements"),
            ("totalPages", "total_pages"),
            ("pageNumber", "page_number"),
            ("pageSize", "page_size"),
        ):
            if summary[target_key]:
                continue
            try:
                summary[target_key] = max(0, int(pagination.get(source_key) or 0))
            except (TypeError, ValueError):
                continue
    summary["returned_count"] = returned_count
    if summary["page_size"] <= 0 and returned_count:
        summary["page_size"] = min(returned_count, 25)
    if summary["total_pages"] <= 0 and summary["total_elements"] > 0 and summary["page_size"] > 0:
        summary["total_pages"] = max(1, (summary["total_elements"] + summary["page_size"] - 1) // summary["page_size"])
    return summary


def parse_harvest_company_employee_rows(payload: Any) -> list[dict[str, Any]]:
    items = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        data = item.get("item") if isinstance(item.get("item"), dict) else item
        meta = item.get("_meta")
        if not isinstance(meta, dict) and isinstance(data, dict):
            meta = data.get("_meta")
        pagination = meta.get("pagination") if isinstance(meta, dict) else {}
        page_number = pagination.get("pageNumber")
        try:
            page = max(1, int(page_number))
        except (TypeError, ValueError):
            page = 1
        full_name = _full_name_from_payload(data)
        headline = _headline_from_payload(data)
        location = _location_text(data.get("location") or data.get("locationName"))
        profile_url = str(data.get("linkedinUrl") or data.get("profileUrl") or data.get("url") or "").strip()
        public_identifier = str(data.get("publicIdentifier") or data.get("public_identifier") or "").strip()
        member_key = public_identifier or _slug_from_linkedin_url(profile_url) or sha1("|".join([full_name, headline, location]).encode("utf-8")).hexdigest()[:16]
        rows.append(
            {
                "member_key": member_key,
                "member_id": public_identifier,
                "urn": str(data.get("entityUrn") or data.get("urn") or "").strip(),
                "full_name": full_name,
                "headline": headline,
                "location": location,
                "location_normalized": _normalized_harvest_location(data.get("location") or data.get("locationName")),
                "avatar_url": str(data.get("photoUrl") or data.get("pictureUrl") or data.get("profilePicture") or "").strip(),
                "is_headless": full_name == "LinkedIn Member" or not full_name,
                "page": page,
                "source_account_id": "harvest_company_employees",
                "linkedin_url": profile_url,
            }
        )
    return rows


def parse_harvest_company_employee_run_log(log_text: str) -> dict[str, Any]:
    normalized = str(log_text or "")
    scraping_query = ""
    found_query = ""
    estimated_total_count = 0
    provider_result_limited = False
    scraped_page_count = 0
    last_page_size = 0
    for raw_line in normalized.splitlines():
        line = raw_line.strip()
        if "Scraping query:" in line and not scraping_query:
            scraping_query = line.split("Scraping query:", 1)[1].strip()
        if "Found " in line and "profiles total for input" in line:
            match = re.search(r"Found\s+(\d+)\s+profiles total for input\s+(.+)$", line)
            if match:
                estimated_total_count = int(match.group(1))
                found_query = str(match.group(2) or "").strip()
        if "limited to 2500 items" in line:
            provider_result_limited = True
        page_match = re.search(r"Scraped search page\s+(\d+)\.\s+Found\s+(\d+)\s+profiles on the page\.", line)
        if page_match:
            scraped_page_count = max(scraped_page_count, int(page_match.group(1)))
            last_page_size = int(page_match.group(2))
    detail = ""
    if estimated_total_count > 0:
        detail = f"Observed {estimated_total_count} profiles total from Harvest actor log."
        if provider_result_limited:
            detail += " Query exceeded the provider's 2500-result ceiling."
    return {
        "estimated_total_count": estimated_total_count,
        "provider_result_limited": provider_result_limited,
        "scraped_page_count": scraped_page_count,
        "last_page_size": last_page_size,
        "scraping_query": scraping_query,
        "found_query": found_query,
        "detail": detail,
    }


def discover_legacy_harvest_token(legacy_accounts_path: Path | None) -> str:
    if legacy_accounts_path is None or not legacy_accounts_path.exists():
        return ""
    try:
        payload = json.loads(legacy_accounts_path.read_text())
    except json.JSONDecodeError:
        return ""
    for item in payload.get("accounts", []) or []:
        if isinstance(item, dict) and str(item.get("provider") or "").strip().lower() == "apify":
            return str(item.get("api_token") or "").strip()
    return ""


def _runtime_dir_from_path(base_path: Path) -> Path | None:
    current = base_path.resolve()
    for candidate in [current, *current.parents]:
        if candidate.name == "runtime":
            return candidate
    return None


def _shared_harvest_cache_path(base_path: Path, logical_name: str, payload: dict[str, Any]) -> Path | None:
    runtime_dir = _runtime_dir_from_path(base_path)
    if runtime_dir is None:
        return None
    cache_dir = runtime_dir / "provider_cache" / logical_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{_payload_cache_key(payload)}.json"


def _persist_shared_harvest_payload(
    base_path: Path,
    *,
    logical_name: str,
    payload: dict[str, Any],
    body: Any,
    request_context: dict[str, Any] | None = None,
) -> None:
    cache_path = _shared_harvest_cache_path(base_path, logical_name, payload)
    if cache_path is None:
        return
    try:
        cache_path.write_text(json.dumps(body, ensure_ascii=False, indent=2))
        request_path = cache_path.with_name(f"{cache_path.stem}.request.json")
        request_path.write_text(
            json.dumps(
                {
                    "logical_name": logical_name,
                    "payload_hash": _payload_cache_key(payload),
                    "request_payload": payload,
                    "request_context": dict(request_context or {}),
                    "response_path": str(cache_path),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    except Exception:
        return


def _write_harvest_request_manifest(
    logger: AssetLogger,
    raw_path: Path,
    *,
    logical_name: str,
    payload: dict[str, Any],
    request_context: dict[str, Any] | None = None,
) -> Path:
    request_path = raw_path.with_name(f"{raw_path.stem}.request.json")
    logger.write_json(
        request_path,
        {
            "logical_name": logical_name,
            "payload_hash": _payload_cache_key(payload),
            "request_payload": payload,
            "request_context": dict(request_context or {}),
            "response_path": str(raw_path),
        },
        asset_type=f"{logical_name}_request",
        source_kind=logical_name,
        is_raw_asset=False,
        model_safe=True,
        metadata={"payload_hash": _payload_cache_key(payload), "response_path": str(raw_path)},
    )
    return request_path


def write_harvest_execution_artifact(
    *,
    logger: AssetLogger,
    artifact: HarvestExecutionArtifact,
    default_path: Path,
    asset_type: str,
    source_kind: str,
    metadata: dict[str, Any] | None = None,
) -> Path:
    suffix = "json" if str(getattr(artifact, "raw_format", "json") or "json") == "json" else "txt"
    raw_path = default_path.with_name(
        f"{default_path.stem}_{str(getattr(artifact, 'label', 'artifact') or 'artifact')}.{suffix}"
    )
    combined_metadata = {
        **dict(metadata or {}),
        **dict(getattr(artifact, "metadata", {}) or {}),
    }
    if suffix == "json":
        logger.write_json(
            raw_path,
            getattr(artifact, "payload", {}),
            asset_type=asset_type,
            source_kind=source_kind,
            is_raw_asset=True,
            model_safe=False,
            metadata=combined_metadata,
        )
    else:
        logger.write_text(
            raw_path,
            str(getattr(artifact, "payload", "") or ""),
            asset_type=asset_type,
            source_kind=source_kind,
            content_type=str(getattr(artifact, "content_type", "") or "text/plain"),
            is_raw_asset=True,
            model_safe=False,
            metadata=combined_metadata,
        )
    return raw_path


def _load_cached_harvest_payload(
    base_path: Path,
    *,
    logical_name: str,
    payload: dict[str, Any],
) -> tuple[Any | None, str | None, Path | None]:
    cache_path = _shared_harvest_cache_path(base_path, logical_name, payload)
    if cache_path and cache_path.exists():
        try:
            return json.loads(cache_path.read_text()), "shared_cache", cache_path
        except json.JSONDecodeError:
            pass

    runtime_dir = _runtime_dir_from_path(base_path)
    if runtime_dir is None:
        return None, None, None
    live_tests_dir = runtime_dir / "live_tests"
    if not live_tests_dir.exists():
        return None, None, None

    requested_payload = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    for summary_path in sorted(live_tests_dir.rglob("*summary.json")):
        try:
            summary_payload = json.loads(summary_path.read_text())
        except json.JSONDecodeError:
            continue
        candidate_payload = summary_payload.get("input_payload")
        if not isinstance(candidate_payload, dict):
            request_manifest_path = _infer_live_test_request_manifest(summary_path)
            if request_manifest_path is not None and request_manifest_path.exists():
                try:
                    request_manifest_payload = json.loads(request_manifest_path.read_text())
                except json.JSONDecodeError:
                    request_manifest_payload = {}
                candidate_payload = request_manifest_payload.get("request_payload")
        if not isinstance(candidate_payload, dict):
            continue
        if json.dumps(candidate_payload, sort_keys=True, ensure_ascii=False) != requested_payload:
            continue
        raw_path = _infer_live_test_raw_path(summary_path, summary_payload)
        if raw_path is not None and raw_path.exists():
            try:
                body = json.loads(raw_path.read_text())
            except json.JSONDecodeError:
                body = None
            if body is not None:
                if cache_path is not None:
                    try:
                        copyfile(raw_path, cache_path)
                    except Exception:
                        pass
                return body, "live_test_bridge", raw_path
        if cache_path is not None:
            try:
                copyfile(summary_path, cache_path)
            except Exception:
                pass
        return summary_payload, "live_test_bridge_summary", summary_path
    return None, None, None


def _infer_live_test_raw_path(summary_path: Path, summary_payload: dict[str, Any]) -> Path | None:
    raw_path_value = str(summary_payload.get("raw_path") or "").strip()
    if raw_path_value:
        return Path(raw_path_value)
    name = summary_path.name
    if name.endswith("_summary.json"):
        return summary_path.with_name(name.replace("_summary.json", "_raw.json"))
    return None


def _infer_live_test_request_manifest(summary_path: Path) -> Path | None:
    name = summary_path.name
    if name.endswith(".summary.json"):
        candidate = summary_path.with_name(name.replace(".summary.json", ".request.json"))
        if candidate.exists():
            return candidate
    if name.endswith("_summary.json"):
        candidate = summary_path.with_name(name.replace("_summary.json", ".request.json"))
        if candidate.exists():
            return candidate
    return None


def _profile_cache_key(profile_url: str) -> str:
    import hashlib

    return hashlib.sha1(profile_url.strip().encode("utf-8")).hexdigest()[:16]


def _match_harvest_profile_items_to_requested_urls(
    requested_urls: list[str],
    items: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    matched: dict[str, dict[str, Any]] = {}
    remaining_urls = list(requested_urls)
    remaining_items = list(items)

    def _consume(matcher) -> None:
        nonlocal remaining_urls, remaining_items
        next_urls: list[str] = []
        for requested_url in remaining_urls:
            index = matcher(requested_url, remaining_items)
            if index is None:
                next_urls.append(requested_url)
                continue
            matched[requested_url] = remaining_items.pop(index)
        remaining_urls = next_urls

    _consume(_match_item_by_original_query_url)
    _consume(_match_item_by_exact_profile_url)
    _consume(_match_item_by_identifier)
    return matched, remaining_urls


def _match_item_by_original_query_url(requested_url: str, items: list[dict[str, Any]]) -> int | None:
    requested_normalized = _normalize_linkedin_profile_url(requested_url)
    if not requested_normalized:
        return None
    for index, item in enumerate(items):
        original_query = item.get("originalQuery")
        if isinstance(original_query, dict):
            original_url = str(
                original_query.get("url")
                or original_query.get("linkedinUrl")
                or original_query.get("profileUrl")
                or ""
            ).strip()
        else:
            original_url = str(original_query or "").strip()
        if requested_normalized and requested_normalized == _normalize_linkedin_profile_url(original_url):
            return index
    return None


def _match_item_by_exact_profile_url(requested_url: str, items: list[dict[str, Any]]) -> int | None:
    requested_normalized = _normalize_linkedin_profile_url(requested_url)
    if not requested_normalized:
        return None
    for index, item in enumerate(items):
        item_url = str(item.get("linkedinUrl") or item.get("profileUrl") or item.get("url") or "").strip()
        if requested_normalized and requested_normalized == _normalize_linkedin_profile_url(item_url):
            return index
    return None


def _match_item_by_identifier(requested_url: str, items: list[dict[str, Any]]) -> int | None:
    requested_slug = _normalize_profile_identifier(_slug_from_linkedin_url(requested_url))
    if not requested_slug:
        return None
    for index, item in enumerate(items):
        public_identifier = _normalize_profile_identifier(str(item.get("publicIdentifier") or item.get("public_identifier") or "").strip())
        item_url_slug = _normalize_profile_identifier(_slug_from_linkedin_url(str(item.get("linkedinUrl") or item.get("profileUrl") or item.get("url") or "").strip()))
        if requested_slug and requested_slug in {public_identifier, item_url_slug}:
            return index
    return None


def _normalize_linkedin_profile_url(url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    parsed = parse.urlsplit(value)
    host = parsed.netloc.lower().replace("www.", "")
    path = parsed.path.rstrip("/")
    if host and path:
        return f"{host}{path}".lower()
    return value.rstrip("/").lower()


def _normalize_profile_identifier(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "").strip() if ch.isalnum())


_HARVEST_PROFILE_RETRY_BATCH_SIZES = (25, 10, 5)
_HARVEST_PROFILE_DIRECT_FALLBACK_THRESHOLD = 5
_HARVEST_PROVIDER_RESULT_CAP = 2500


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for item in values:
        normalized = str(item or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _chunk_values(values: list[str], chunk_size: int) -> list[list[str]]:
    size = max(1, int(chunk_size or 1))
    return [values[index : index + size] for index in range(0, len(values), size)]


def _initial_harvest_profile_retry_batch_size(unresolved_count: int) -> int:
    count = max(0, int(unresolved_count or 0))
    for size in _HARVEST_PROFILE_RETRY_BATCH_SIZES:
        if count > size:
            return size
    return 0


def _next_harvest_profile_retry_batch_size(current_size: int) -> int:
    normalized = int(current_size or 0)
    for index, size in enumerate(_HARVEST_PROFILE_RETRY_BATCH_SIZES):
        if size == normalized:
            return _HARVEST_PROFILE_RETRY_BATCH_SIZES[index + 1] if index + 1 < len(_HARVEST_PROFILE_RETRY_BATCH_SIZES) else 0
    return 0


def _profile_scraper_mode(settings: HarvestActorSettings) -> str:
    if settings.collect_email:
        return "Profile details + email search ($10 per 1k)"
    return "Profile details no email ($4 per 1k)" if settings.default_mode.lower() == "full" else settings.default_mode


def _profile_search_mode(settings: HarvestActorSettings) -> str:
    if settings.collect_email:
        return "Full + email search"
    return "Full" if settings.default_mode.lower() == "full" else "Short"


def _company_employees_mode(settings: HarvestActorSettings) -> str:
    if settings.collect_email:
        return "Full + email search ($12 per 1k)"
    return "Full ($8 per 1k)" if settings.default_mode.lower() == "full" else "Short ($4 per 1k)"


def _company_employees_max_items(max_pages: int, page_limit: int) -> int:
    take_pages = max(1, min(max_pages, 100))
    per_page_cap = min(max(1, int(page_limit or 25)), 25)
    return min(_HARVEST_PROVIDER_RESULT_CAP, take_pages * per_page_cap)


def _recommended_harvest_company_timeout_seconds(requested_items: int) -> int:
    estimated_pages = max(1, (max(1, int(requested_items or 0)) + 24) // 25)
    return max(300, min(1800, estimated_pages * 15))


def _recommended_harvest_profile_timeout_seconds(requested_items: int, *, collect_email: bool) -> int:
    item_count = max(1, int(requested_items or 0))
    base_seconds = 300 if collect_email else 180
    per_item_seconds = 6 if collect_email else 4
    estimated = base_seconds + (item_count * per_item_seconds)
    return max(300, min(1800, estimated))


def _recommended_harvest_profile_search_timeout_seconds(requested_pages: int) -> int:
    page_count = max(1, int(requested_pages or 0))
    return max(300, min(1800, page_count * 20))


def _estimate_harvest_charge_usd(mode: str, item_count: int, *, fallback_per_1k: float) -> float:
    match = re.search(r"\$(\d+(?:\.\d+)?)\s*per\s*1k", str(mode or ""), flags=re.IGNORECASE)
    price_per_1k = float(match.group(1)) if match else float(fallback_per_1k)
    return round(max(0.01, (price_per_1k * max(1, int(item_count or 0))) / 1000.0), 2)


def _recommended_harvest_profile_charge_cap_usd(mode: str, item_count: int, *, fallback_per_1k: float) -> float:
    estimated = _estimate_harvest_charge_usd(mode, item_count, fallback_per_1k=fallback_per_1k)
    buffered = max(estimated + 0.25, estimated * 1.25)
    return round(buffered, 2)


def _recommended_harvest_profile_search_charge_cap_usd(requested_pages: int) -> float:
    page_count = max(1, int(requested_pages or 0))
    estimated = round(page_count * 0.10, 2)
    return round(max(estimated + 0.25, estimated * 1.15), 2)


def _recommended_harvest_company_charge_cap_usd(mode: str, item_count: int) -> float:
    estimated = _estimate_harvest_charge_usd(mode, item_count, fallback_per_1k=4.0)
    buffered = max(estimated + 2.0, estimated * 1.5)
    return round(buffered, 2)


def _execute_harvest_actor_with_checkpoint(
    settings: HarvestActorSettings,
    *,
    logical_name: str,
    payload: dict[str, Any],
    base_path: Path,
    checkpoint: dict[str, Any] | None = None,
    request_context: dict[str, Any] | None = None,
    allow_shared_provider_cache: bool = True,
) -> HarvestExecutionResult:
    existing = dict(checkpoint or {})
    cached_body = None
    cache_source = None
    cache_origin = None
    if allow_shared_provider_cache:
        cached_body, cache_source, cache_origin = _load_cached_harvest_payload(
            base_path,
            logical_name=logical_name,
            payload=payload,
        )
    if cached_body is not None:
        return HarvestExecutionResult(
            logical_name=logical_name,
            checkpoint={
                "logical_name": logical_name,
                "payload_hash": _payload_cache_key(payload),
                "status": "completed",
                "cache_source": str(cache_source or ""),
                "cache_origin": str(cache_origin or ""),
            },
            body=cached_body,
            message=f"Reused cached Harvest payload from {cache_source or 'shared_cache'}.",
            artifacts=[
                HarvestExecutionArtifact(
                    label="cache_hit",
                    payload={
                        "logical_name": logical_name,
                        "payload_hash": _payload_cache_key(payload),
                        "cache_source": str(cache_source or ""),
                        "cache_origin": str(cache_origin or ""),
                    },
                    metadata={"cache_source": str(cache_source or ""), "cache_origin": str(cache_origin or "")},
                )
            ],
        )

    if not settings.enabled:
        raise RuntimeError(f"Harvest connector is not enabled for {logical_name}.")

    base_checkpoint = {
        "logical_name": logical_name,
        "payload_hash": _payload_cache_key(payload),
        "actor_id": settings.actor_id,
        "request_context": dict(request_context or {}),
    }
    run_id = str(existing.get("run_id") or "").strip()
    dataset_id = str(existing.get("dataset_id") or existing.get("default_dataset_id") or "").strip()

    if not run_id:
        submit_payload = _submit_harvest_actor_run(settings, payload)
        run = _apify_data_record(submit_payload)
        run_id = str(run.get("id") or run.get("runId") or "").strip()
        dataset_id = str(run.get("defaultDatasetId") or run.get("datasetId") or "").strip()
        run_status = _normalize_harvest_run_status(run.get("status") or "submitted")
        artifacts = [
            HarvestExecutionArtifact(
                label="run_post",
                payload=submit_payload,
                metadata={
                    "logical_name": logical_name,
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "provider": "apify",
                },
            )
        ]
        if not run_id:
            raise RuntimeError(f"Harvest async run submission returned no run id for {logical_name}.")
        if _harvest_run_is_terminal(run_status):
            return _complete_harvest_execution(
                settings,
                logical_name=logical_name,
                payload=payload,
                base_path=base_path,
                run=run,
                run_id=run_id,
                dataset_id=dataset_id,
                artifacts=artifacts,
                request_context=request_context,
            )
        return HarvestExecutionResult(
            logical_name=logical_name,
            checkpoint={
                **base_checkpoint,
                "run_id": run_id,
                "dataset_id": dataset_id,
                "status": "submitted",
            },
            pending=True,
            message=f"Submitted Harvest actor run {run_id} for {logical_name}.",
            artifacts=artifacts,
        )

    run_payload = _get_harvest_actor_run(settings, run_id)
    run = _apify_data_record(run_payload)
    run_status = _normalize_harvest_run_status(run.get("status") or existing.get("status") or "running")
    dataset_id = str(run.get("defaultDatasetId") or run.get("datasetId") or dataset_id).strip()
    artifacts = [
        HarvestExecutionArtifact(
            label="run_get",
            payload=run_payload,
            metadata={
                "logical_name": logical_name,
                "run_id": run_id,
                "dataset_id": dataset_id,
                "provider": "apify",
            },
        )
    ]
    if not _harvest_run_is_terminal(run_status):
        return HarvestExecutionResult(
            logical_name=logical_name,
            checkpoint={
                **base_checkpoint,
                "run_id": run_id,
                "dataset_id": dataset_id,
                "status": run_status,
            },
            pending=True,
            message=f"Waiting for Harvest actor run {run_id} to finish ({run_status}).",
            artifacts=artifacts,
        )
    return _complete_harvest_execution(
        settings,
        logical_name=logical_name,
        payload=payload,
        base_path=base_path,
        run=run,
        run_id=run_id,
        dataset_id=dataset_id,
        artifacts=artifacts,
        request_context=request_context,
    )


def _complete_harvest_execution(
    settings: HarvestActorSettings,
    *,
    logical_name: str,
    payload: dict[str, Any],
    base_path: Path,
    run: dict[str, Any],
    run_id: str,
    dataset_id: str,
    artifacts: list[HarvestExecutionArtifact],
    request_context: dict[str, Any] | None = None,
) -> HarvestExecutionResult:
    run_status = _normalize_harvest_run_status(run.get("status") or "unknown")
    if not _harvest_run_succeeded(run_status):
        status_message = str(run.get("statusMessage") or run.get("message") or "").strip()
        detail = f" ({status_message})" if status_message else ""
        raise RuntimeError(f"Harvest actor run {run_id} finished with status {run_status}{detail}.")
    if not dataset_id:
        raise RuntimeError(f"Harvest actor run {run_id} finished without a dataset id.")
    body = _get_harvest_dataset_items(settings, dataset_id)
    artifacts = [
        *artifacts,
        HarvestExecutionArtifact(
            label="dataset_items",
            payload=body,
            metadata={
                "logical_name": logical_name,
                "run_id": run_id,
                "dataset_id": dataset_id,
                "provider": "apify",
            },
        ),
    ]
    _persist_shared_harvest_payload(
        base_path,
        logical_name=logical_name,
        payload=payload,
        body=body,
        request_context={
            **dict(request_context or {}),
            "run_id": run_id,
            "dataset_id": dataset_id,
            "run_status": run_status,
        },
    )
    return HarvestExecutionResult(
        logical_name=logical_name,
        checkpoint={
            "logical_name": logical_name,
            "payload_hash": _payload_cache_key(payload),
            "actor_id": settings.actor_id,
            "run_id": run_id,
            "dataset_id": dataset_id,
            "status": "completed",
            "request_context": dict(request_context or {}),
        },
        body=body,
        message=f"Harvest actor run {run_id} completed and dataset {dataset_id} was cached.",
        artifacts=artifacts,
    )


def _submit_harvest_actor_run(settings: HarvestActorSettings, payload: dict[str, Any]) -> Any:
    endpoint = (
        f"https://api.apify.com/v2/acts/{parse.quote(settings.actor_id, safe='')}/runs?"
        + parse.urlencode(
            {
                "token": settings.api_token,
                "waitForFinish": 0,
                "timeout": settings.timeout_seconds,
                "maxTotalChargeUsd": settings.max_total_charge_usd,
            }
        )
    )
    return _harvest_json_request(endpoint, payload=payload, timeout=settings.timeout_seconds + 15)


def _get_harvest_actor_run(settings: HarvestActorSettings, run_id: str) -> Any:
    endpoint = (
        f"https://api.apify.com/v2/actor-runs/{parse.quote(str(run_id or '').strip(), safe='')}?"
        + parse.urlencode({"token": settings.api_token})
    )
    return _harvest_json_request(endpoint, timeout=settings.timeout_seconds + 15)


def _get_harvest_dataset_items(settings: HarvestActorSettings, dataset_id: str) -> Any:
    endpoint = (
        f"https://api.apify.com/v2/datasets/{parse.quote(str(dataset_id or '').strip(), safe='')}/items?"
        + parse.urlencode(
            {
                "token": settings.api_token,
                "format": "json",
                "clean": "true",
            }
        )
    )
    return _harvest_json_request(endpoint, timeout=settings.timeout_seconds + 15)


def _get_harvest_actor_run_log(settings: HarvestActorSettings, run_id: str) -> str:
    endpoint = (
        f"https://api.apify.com/v2/actor-runs/{parse.quote(str(run_id or '').strip(), safe='')}/log?"
        + parse.urlencode({"token": settings.api_token})
    )
    http_request = request.Request(endpoint, method="GET")
    try:
        with request.urlopen(http_request, timeout=max(15, int(settings.timeout_seconds or 180) + 15)) as response:
            return response.read().decode("utf-8", errors="replace")
    except error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Harvest log API HTTP {exc.code}: {raw_body[:300]}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Harvest log API network error: {exc.reason}") from exc
    except Exception as exc:
        raise RuntimeError(f"Harvest log API request failed: {exc}") from exc


def _harvest_json_request(endpoint: str, *, payload: dict[str, Any] | None = None, timeout: int = 180) -> Any:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    http_request = request.Request(endpoint, data=data, headers=headers, method="POST" if payload is not None else "GET")
    try:
        with request.urlopen(http_request, timeout=max(15, int(timeout or 180))) as response:
            raw_text = response.read().decode("utf-8")
    except error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Harvest API HTTP {exc.code}: {raw_body[:300]}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Harvest API network error: {exc.reason}") from exc
    except Exception as exc:
        raise RuntimeError(f"Harvest API request failed: {exc}") from exc
    try:
        return json.loads(raw_text) if raw_text else None
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Harvest API returned invalid JSON from {endpoint}.") from exc


def _apify_data_record(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload
    return {}


def _normalize_harvest_run_status(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _harvest_run_is_terminal(status: str) -> bool:
    return status in {
        "succeeded",
        "failed",
        "aborted",
        "timed-out",
        "timeout",
    }


def _harvest_run_succeeded(status: str) -> bool:
    return status == "succeeded"


def _run_harvest_actor(settings: HarvestActorSettings, payload: dict[str, Any]) -> Any | None:
    endpoint = (
        f"https://api.apify.com/v2/acts/{parse.quote(settings.actor_id, safe='')}/run-sync-get-dataset-items?"
        + parse.urlencode(
            {
                "token": settings.api_token,
                "timeout": settings.timeout_seconds,
                "format": "json",
                "clean": "true",
                "maxItems": settings.max_paid_items,
                "maxTotalChargeUsd": settings.max_total_charge_usd,
            }
        )
    )
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    http_request = request.Request(endpoint, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(http_request, timeout=settings.timeout_seconds + 15) as response:
            return json.loads(response.read().decode("utf-8"))
    except error.HTTPError:
        return None
    except Exception:
        return None


def _build_harvest_company_employees_payload(
    settings: HarvestActorSettings,
    identity: CompanyIdentity,
    *,
    max_pages: int,
    page_limit: int,
    company_filters: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], int]:
    company_url = str(identity.linkedin_company_url or "").strip()
    if not company_url:
        raise RuntimeError(f"No LinkedIn company URL is available for {identity.requested_name}.")
    take_pages = max(1, min(max_pages, 100))
    requested_items = _company_employees_max_items(take_pages, page_limit)
    payload = {
        "profileScraperMode": _company_employees_mode(settings),
        "companies": [company_url],
        "takePages": take_pages,
        "maxItems": requested_items,
    }
    normalized_filters = _normalize_harvest_company_employee_filters(company_filters)
    _apply_harvest_company_employee_filters(payload, normalized_filters)
    payload_companies = [
        str(item).strip()
        for item in list(payload.get("companies") or [])
        if str(item).strip()
    ]
    if payload_companies:
        payload["companies"] = list(dict.fromkeys(payload_companies))
    else:
        payload["companies"] = [company_url]
    return payload, normalized_filters, requested_items


def _normalize_harvest_company_employee_filters(company_filters: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(company_filters or {})

    def _pick_list(*keys: str) -> list[str]:
        values: list[str] = []
        for key in keys:
            raw = payload.get(key)
            if raw is None:
                continue
            if isinstance(raw, (list, tuple, set)):
                values.extend(str(item).strip() for item in raw if str(item).strip())
            else:
                text = str(raw).strip()
                if text:
                    values.append(text)
        return list(dict.fromkeys(values))

    normalized: dict[str, Any] = {}
    list_key_mapping = {
        "companies": ("companies",),
        "locations": ("locations",),
        "exclude_locations": ("exclude_locations", "excludeLocations"),
        "function_ids": ("function_ids", "functionIds"),
        "exclude_function_ids": ("exclude_function_ids", "excludeFunctionIds"),
        "job_titles": ("job_titles", "jobTitles"),
        "exclude_job_titles": ("exclude_job_titles", "excludeJobTitles"),
        "seniority_level_ids": ("seniority_level_ids", "seniorityLevelIds"),
        "exclude_seniority_level_ids": ("exclude_seniority_level_ids", "excludeSeniorityLevelIds"),
        "schools": ("schools",),
    }
    for normalized_key, source_keys in list_key_mapping.items():
        values = _pick_list(*source_keys)
        if values:
            normalized[normalized_key] = values
    search_query = str(payload.get("search_query") or payload.get("searchQuery") or "").strip()
    if search_query:
        normalized["search_query"] = search_query
    return normalized


def _apply_harvest_company_employee_filters(payload: dict[str, Any], company_filters: dict[str, Any]) -> None:
    mapping = {
        "companies": "companies",
        "locations": "locations",
        "exclude_locations": "excludeLocations",
        "function_ids": "functionIds",
        "exclude_function_ids": "excludeFunctionIds",
        "job_titles": "jobTitles",
        "exclude_job_titles": "excludeJobTitles",
        "seniority_level_ids": "seniorityLevelIds",
        "exclude_seniority_level_ids": "excludeSeniorityLevelIds",
        "schools": "schools",
    }
    for source_key, target_key in mapping.items():
        values = [str(item).strip() for item in company_filters.get(source_key) or [] if str(item).strip()]
        if values:
            payload[target_key] = values
    search_query = str(company_filters.get("search_query") or "").strip()
    if search_query:
        payload["searchQuery"] = search_query


def _apply_harvest_search_filters(payload: dict[str, Any], filter_hints: dict[str, list[str]], employment_status: str) -> None:
    mapping = {
        "current_companies": "currentCompanies",
        "past_companies": "pastCompanies",
        "exclude_current_companies": "excludeCurrentCompanies",
        "exclude_past_companies": "excludePastCompanies",
        "job_titles": "currentJobTitles" if employment_status != "former" else "pastJobTitles",
        "locations": "locations",
        "exclude_locations": "excludeLocations",
        "function_ids": "functionIds",
        "exclude_function_ids": "excludeFunctionIds",
    }
    for source_key, target_key in mapping.items():
        values = [str(item).strip() for item in filter_hints.get(source_key) or [] if str(item).strip()]
        if values:
            payload[target_key] = values
    scope_keywords = [str(item).strip() for item in filter_hints.get("scope_keywords") or [] if str(item).strip()]
    keyword_values = [str(item).strip() for item in filter_hints.get("keywords") or [] if str(item).strip()]
    query_text = str(payload.get("searchQuery") or "").strip()
    if not query_text:
        # Keep former past-company probes broad by default. When caller passes an
        # explicit query_text, payload already carries searchQuery and this branch
        # will not execute.
        if str(employment_status or "").strip().lower() == "former" and list(payload.get("pastCompanies") or []):
            return
        terms = [*scope_keywords[:2], *keyword_values[:2]]
        if terms:
            payload["searchQuery"] = " ".join(dict.fromkeys(terms))


def _payload_cache_key(payload: dict[str, Any]) -> str:
    return sha1(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]


def _slug_from_linkedin_url(url: str) -> str:
    import re

    match = re.search(r"linkedin\.com/in/([^/?#]+)", url)
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def _dedupe_harvest_roster_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in entries:
        member_key = str(entry.get("member_key") or "").strip()
        if not member_key or member_key in seen:
            continue
        seen.add(member_key)
        deduped.append(entry)
    return deduped


def _build_harvest_roster_page_summaries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[int, dict[str, Any]] = {}
    for entry in entries:
        try:
            page = max(1, int(entry.get("page") or 1))
        except (TypeError, ValueError):
            page = 1
        summary = grouped.setdefault(
            page,
            {
                "page": page,
                "entry_count": 0,
                "visible_entry_count": 0,
                "headless_entry_count": 0,
                "account_id": "harvest_company_employees",
            },
        )
        summary["entry_count"] += 1
        if bool(entry.get("is_headless")):
            summary["headless_entry_count"] += 1
        else:
            summary["visible_entry_count"] += 1
    return [grouped[page] for page in sorted(grouped)]


def _coerce_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _full_name_from_payload(data: dict[str, Any]) -> str:
    full_name = str(data.get("fullName") or data.get("full_name") or data.get("name") or "").strip()
    if full_name:
        return full_name
    first_name = str(data.get("firstName") or data.get("first_name") or "").strip()
    last_name = str(data.get("lastName") or data.get("last_name") or "").strip()
    return " ".join(part for part in [first_name, last_name] if part).strip()


def _headline_from_payload(data: dict[str, Any]) -> str:
    direct = str(data.get("headline") or data.get("occupation") or data.get("position") or "").strip()
    if direct:
        return direct
    current_positions = data.get("currentPositions") or data.get("currentPosition") or []
    if isinstance(current_positions, list) and current_positions:
        first = current_positions[0] or {}
        if isinstance(first, dict):
            position = str(first.get("position") or first.get("title") or "").strip()
            company_name = str(first.get("companyName") or first.get("company") or "").strip()
            if position and company_name:
                return f"{position} at {company_name}"
            return position or company_name
    return ""


def _location_text(value: Any) -> str:
    return str(_normalized_harvest_location(value).get("raw_text") or "").strip()


def _normalized_harvest_location(value: Any) -> dict[str, str]:
    raw_text = ""
    country_code = ""
    country = ""
    country_full = ""
    state = ""
    city = ""
    if isinstance(value, dict):
        parsed = value.get("parsed") if isinstance(value.get("parsed"), dict) else {}
        raw_text = str(
            value.get("linkedinText")
            or value.get("text")
            or value.get("name")
            or value.get("full")
            or ""
        ).strip()
        country_code = str(
            value.get("countryCode")
            or parsed.get("countryCode")
            or parsed.get("country_code")
            or ""
        ).strip()
        country = str(parsed.get("country") or value.get("country") or "").strip()
        country_full = str(parsed.get("countryFull") or value.get("countryFull") or country).strip()
        state = str(parsed.get("state") or value.get("state") or "").strip()
        city = str(parsed.get("city") or value.get("city") or "").strip()
        if not raw_text:
            parts = [part for part in [city, state, country_full or country] if part]
            raw_text = ", ".join(parts)
    else:
        raw_text = str(value or "").strip()
    granularity = ""
    if country and not state and not city:
        granularity = "country"
    elif state and not city:
        granularity = "state"
    elif city:
        granularity = "city"
    elif raw_text:
        granularity = "text_only"
    return {
        "raw_text": raw_text,
        "country_code": country_code,
        "country": country,
        "country_full": country_full,
        "state": state,
        "city": city,
        "granularity": granularity,
    }


def _extract_current_company(data: dict[str, Any], experience: list[dict[str, Any]]) -> str:
    for item in experience:
        if bool(item.get("isCurrent") or item.get("is_current")):
            return str(item.get("company") or item.get("companyName") or item.get("company_name") or "").strip()
    current_positions = data.get("currentPositions") or data.get("currentPosition") or []
    if isinstance(current_positions, list):
        for item in current_positions:
            if not isinstance(item, dict):
                continue
            company_name = str(item.get("companyName") or item.get("company") or "").strip()
            if company_name:
                return company_name
    return str(data.get("currentCompany") or data.get("current_company") or "").strip()
