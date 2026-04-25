from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from hashlib import sha1
import json
import os
from pathlib import Path
import re
from shutil import copyfile
import time
from typing import Any, Callable
from urllib import error, parse, request

from .asset_logger import AssetLogger
from .company_registry import normalize_company_key
from .connectors import CompanyIdentity, CompanyRosterSnapshot
from .profile_registry_utils import harvest_profile_payload_has_usable_content
from .profile_timeline import normalized_primary_email_metadata
from .scripted_provider_scenario import (
    advance_scripted_phase_round,
    find_scripted_rule,
    record_scripted_provider_invocation,
    scripted_phase_error,
    scripted_pending_rounds,
    scripted_rule_artifacts,
    scripted_sleep,
)
from .runtime_tuning import (
    apply_runtime_timing_overrides_to_mapping,
    resolved_harvest_poll_interval_seconds,
    resolved_harvest_retry_backoff_seconds,
    resolved_harvest_scripted_sleep_seconds_cap,
)
from .runtime_environment import (
    LIVE_PROVIDER_MODE,
    external_provider_mode,
    shared_provider_cache_context,
    shared_provider_cache_dir,
    validate_runtime_environment,
)
from .settings import HarvestActorSettings


def _external_provider_mode() -> str:
    return external_provider_mode()


def harvest_connector_available(settings: HarvestActorSettings) -> bool:
    if settings.enabled:
        return True
    return _external_provider_mode() in {"simulate", "replay", "scripted"}


def _harvest_connector_available(settings: HarvestActorSettings) -> bool:
    return harvest_connector_available(settings)


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _merge_harvest_request_context(
    *contexts: dict[str, Any] | None,
    runtime_timing_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for item in contexts:
        if isinstance(item, dict):
            merged.update(dict(item))
    return apply_runtime_timing_overrides_to_mapping(
        merged,
        runtime_timing_overrides=runtime_timing_overrides,
    )


class HarvestRetryableRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        endpoint: str = "",
        logical_name: str = "",
        run_id: str = "",
        dataset_id: str = "",
    ) -> None:
        super().__init__(message)
        self.endpoint = str(endpoint or "")
        self.logical_name = str(logical_name or "")
        self.run_id = str(run_id or "")
        self.dataset_id = str(dataset_id or "")


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
        runtime_timing_overrides: dict[str, Any] | None = None,
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
        if not _harvest_connector_available(self.settings):
            raise RuntimeError("Harvest profile connector is not enabled.")
        batch_payload = {
            "urls": normalized_urls,
            "profileScraperMode": _profile_scraper_mode(self.settings),
            "findEmail": bool(self.settings.collect_email),
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
            request_context=_merge_harvest_request_context(
                {"requested_urls": list(normalized_urls), "requested_url_count": len(normalized_urls)},
                runtime_timing_overrides=runtime_timing_overrides,
            ),
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
        runtime_timing_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        results = self.fetch_profiles_by_urls(
            [profile_url],
            snapshot_dir,
            asset_logger=asset_logger,
            use_cache=use_cache,
            allow_shared_provider_cache=allow_shared_provider_cache,
            runtime_timing_overrides=runtime_timing_overrides,
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
        runtime_timing_overrides: dict[str, Any] | None = None,
        on_batch_result: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
        normalized_urls: list[str] = []
        for item in profile_urls:
            profile_url = str(item or "").strip()
            if profile_url and profile_url not in normalized_urls:
                normalized_urls.append(profile_url)
        if not _harvest_connector_available(self.settings) or not normalized_urls:
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
                    cached_result = _build_harvest_profile_result(
                        raw_path=raw_path,
                        payload=payload,
                        account_id="harvest_profile_scraper",
                    )
                    if cached_result is not None:
                        logger.record_existing(
                            raw_path,
                            asset_type="harvest_profile_payload",
                            source_kind="harvest_profile_scraper",
                            content_type="application/json",
                            is_raw_asset=True,
                            model_safe=False,
                            metadata={"profile_url": profile_url, "cached": True},
                        )
                        results[profile_url] = cached_result
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
                    cached_result = _build_harvest_profile_result(
                        raw_path=raw_path,
                        payload=cached_body,
                        account_id="harvest_profile_scraper",
                    )
                    if cached_result is not None:
                        results[profile_url] = cached_result
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
            runtime_timing_overrides=runtime_timing_overrides,
            on_batch_result=on_batch_result,
        )
        results.update(batch_results)

        remaining_unresolved = list(unresolved_urls)
        provider_mode = _external_provider_mode()
        if provider_mode == "live":
            for retry_batch_size in _live_harvest_profile_retry_batch_sizes(
                len(normalized_urls),
                len(remaining_unresolved),
            ):
                next_unresolved: list[str] = []
                for url_batch in _chunk_values(remaining_unresolved, retry_batch_size):
                    retry_results, retry_unresolved = self._fetch_profile_batch(
                        url_batch,
                        snapshot_dir=snapshot_dir,
                        asset_logger=logger,
                        use_cache=False,
                        allow_shared_provider_cache=allow_shared_provider_cache,
                        retry_label=f"live_tail_retry_batch_{retry_batch_size}",
                        runtime_timing_overrides=runtime_timing_overrides,
                        on_batch_result=on_batch_result,
                    )
                    results.update(retry_results)
                    next_unresolved.extend(retry_unresolved)
                remaining_unresolved = _dedupe_preserve_order(next_unresolved)
            should_direct_fallback = _allow_live_harvest_profile_direct_fallback(len(remaining_unresolved))
        else:
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
                        runtime_timing_overrides=runtime_timing_overrides,
                        on_batch_result=on_batch_result,
                    )
                    results.update(retry_results)
                    next_unresolved.extend(retry_unresolved)
                remaining_unresolved = _dedupe_preserve_order(next_unresolved)
                retry_batch_size = _next_harvest_profile_retry_batch_size(retry_batch_size)
            should_direct_fallback = len(remaining_unresolved) <= _HARVEST_PROFILE_DIRECT_FALLBACK_THRESHOLD

        if should_direct_fallback:
            for requested_url in remaining_unresolved:
                single = self._fetch_profile_request(
                    profile_url=requested_url,
                    snapshot_dir=snapshot_dir,
                    asset_logger=logger,
                    use_cache=False,
                    allow_shared_provider_cache=allow_shared_provider_cache,
                    skip_url_request=True,
                    runtime_timing_overrides=runtime_timing_overrides,
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
        runtime_timing_overrides: dict[str, Any] | None = None,
        on_batch_result: Callable[[dict[str, Any]], None] | None = None,
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        normalized_urls = _dedupe_preserve_order(profile_urls)
        if not normalized_urls:
            return {}, []
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        batch_payload = {
            "urls": normalized_urls,
            "profileScraperMode": _profile_scraper_mode(self.settings),
            "findEmail": bool(self.settings.collect_email),
        }
        batch_raw_path = harvest_dir / f"harvest_profile_batch_{_payload_cache_key(batch_payload)}.json"
        body = None
        request_context = _merge_harvest_request_context(
            {
                "requested_url_count": len(normalized_urls),
                "requested_urls": list(normalized_urls),
                "retry_label": retry_label,
            },
            runtime_timing_overrides=runtime_timing_overrides,
        )
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
            body = _run_harvest_actor(
                effective_settings,
                batch_payload,
                request_context=request_context,
            )
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
        if on_batch_result is not None:
            on_batch_result(
                {
                    "provider": "harvest_profile_scraper_batch",
                    "retry_label": retry_label,
                    "requested_urls": list(normalized_urls),
                    "requested_url_count": len(normalized_urls),
                    "profiles": dict(persisted.get("profiles") or {}),
                    "profile_count": len(dict(persisted.get("profiles") or {})),
                    "unresolved_urls": list(persisted.get("unresolved_urls") or []),
                    "raw_path": str(batch_raw_path),
                    "response_level": "batch",
                }
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
        items = (
            [item for item in (body if isinstance(body, list) else [body]) if isinstance(item, dict)]
            if body is not None
            else []
        )
        matched_items, unresolved_urls = _match_harvest_profile_items_to_requested_urls(normalized_urls, items)
        persisted: dict[str, dict[str, Any]] = {}
        invalid_urls: list[str] = []
        for requested_url, item in matched_items.items():
            if not harvest_profile_payload_has_usable_content(item):
                invalid_urls.append(requested_url)
                continue
            persisted_payload = self._persist_profile_payload(
                requested_url=requested_url,
                item=item,
                snapshot_dir=snapshot_dir,
                asset_logger=logger,
                request_kind="url",
                request_value=requested_url,
            )
            if persisted_payload is None:
                invalid_urls.append(requested_url)
                continue
            persisted[requested_url] = persisted_payload
        return {
            "profiles": persisted,
            "unresolved_urls": _dedupe_preserve_order([*unresolved_urls, *invalid_urls]),
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
        runtime_timing_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        normalized_url = str(profile_url or "").strip()
        if not normalized_url or not _harvest_connector_available(self.settings):
            return None
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        raw_path = harvest_dir / f"{_profile_cache_key(normalized_url)}.json"
        if use_cache and raw_path.exists():
            try:
                payload = json.loads(raw_path.read_text())
                cached_result = _build_harvest_profile_result(
                    raw_path=raw_path,
                    payload=payload,
                    account_id="harvest_profile_scraper",
                )
                if cached_result is not None:
                    asset_logger.record_existing(
                        raw_path,
                        asset_type="harvest_profile_payload",
                        source_kind="harvest_profile_scraper",
                        content_type="application/json",
                        is_raw_asset=True,
                        model_safe=False,
                        metadata={"profile_url": normalized_url, "cached": True},
                    )
                    return cached_result
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
                cached_result = _build_harvest_profile_result(
                    raw_path=raw_path,
                    payload=cached_body,
                    account_id="harvest_profile_scraper",
                )
                if cached_result is not None:
                    return cached_result

        mode = _profile_scraper_mode(self.settings)
        request_variants: list[tuple[str, str, dict[str, Any]]] = []
        if not skip_url_request:
            request_variants.append(
                (
                    "url",
                    normalized_url,
                    {
                        "urls": [normalized_url],
                        "profileScraperMode": mode,
                        "findEmail": bool(self.settings.collect_email),
                    },
                )
            )
        slug = _slug_from_linkedin_url(normalized_url)
        if slug:
            request_variants.append(
                (
                    "public_identifier",
                    slug,
                    {
                        "publicIdentifiers": [slug],
                        "profileScraperMode": mode,
                        "findEmail": bool(self.settings.collect_email),
                    },
                )
            )
            request_variants.append(
                (
                    "profile_id",
                    slug,
                    {
                        "profileIds": [slug],
                        "profileScraperMode": mode,
                        "findEmail": bool(self.settings.collect_email),
                    },
                )
            )

        for request_kind, request_value, payload in request_variants:
            body = _run_harvest_actor(
                self.settings,
                payload,
                request_context=_merge_harvest_request_context(
                    {
                        "requested_url_count": 1,
                        "requested_item_count": 1,
                        "requested_profile_url": normalized_url,
                        "request_kind": request_kind,
                        "request_value": request_value,
                    },
                    runtime_timing_overrides=runtime_timing_overrides,
                ),
            )
            if isinstance(body, list):
                matched_items, _ = _match_harvest_profile_items_to_requested_urls(
                    [normalized_url],
                    [item for item in body if isinstance(item, dict)],
                )
                item = matched_items.get(normalized_url)
                if item is None and request_kind in {"public_identifier", "profile_id"}:
                    candidate_items = [entry for entry in body if isinstance(entry, dict)]
                    if len(candidate_items) == 1:
                        item = candidate_items[0]
            else:
                item = body
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
    ) -> dict[str, Any] | None:
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        raw_path = harvest_dir / f"{_profile_cache_key(requested_url)}.json"
        normalized_item = _unwrap_harvest_profile_wrapper(item)
        wrapped_payload = {
            "_harvest_request": {
                "kind": request_kind,
                "value": request_value,
                "profile_url": requested_url,
            },
            "item": normalized_item,
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
        return _build_harvest_profile_result(
            raw_path=raw_path,
            payload=wrapped_payload,
            account_id="harvest_profile_scraper",
        )


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
        runtime_timing_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        query_text = str(query_text or "").strip()
        search_dir = discovery_dir / "harvest_profile_search"
        search_dir.mkdir(parents=True, exist_ok=True)
        logger = asset_logger or AssetLogger(discovery_dir.parent)
        take_pages = max(1, min(int(pages or 1), 100))
        requested_limit = max(1, int(limit or 25))
        provider_cap_items = max(1, min(_HARVEST_PROVIDER_RESULT_CAP, take_pages * 25))
        requested_limit = min(requested_limit, provider_cap_items)
        past_companies = [
            str(item).strip() for item in list(filter_hints.get("past_companies") or []) if str(item).strip()
        ]
        former_past_company_scan = (
            str(employment_status or "").strip().lower() == "former" and bool(past_companies) and not query_text
        )
        should_probe = bool(auto_probe) and (requested_limit > 25 or take_pages > 1 or former_past_company_scan)
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
                runtime_timing_overrides=runtime_timing_overrides,
            )
            if probe_result is not None:
                pagination = dict(probe_result.get("pagination") or {})
                provider_total_count = max(0, int(pagination.get("total_elements") or 0))
                provider_total_pages = max(0, int(pagination.get("total_pages") or 0))
                probe_returned_count = max(
                    0, int(pagination.get("returned_count") or len(list(probe_result.get("rows") or [])))
                )
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
        request_context = _merge_harvest_request_context(
            {
                "query_text": query_text,
                "employment_status": employment_status,
                "filter_hints": filter_hints,
                "requested_item_count": int(payload.get("maxItems") or 0),
                "take_pages": int(payload.get("takePages") or 0),
            },
            runtime_timing_overrides=runtime_timing_overrides,
        )
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
                        **request_context,
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
                    **request_context,
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
        if not _harvest_connector_available(self.settings):
            return None
        body = _run_harvest_actor(
            effective_settings,
            payload,
            request_context=request_context,
        )
        if body is None:
            return None
        request_manifest_path = _write_harvest_request_manifest(
            logger,
            raw_path,
            logical_name="harvest_profile_search",
            payload=payload,
            request_context={
                **request_context,
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
            request_context=request_context,
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
        runtime_timing_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not _harvest_connector_available(self.settings):
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
        request_context = _merge_harvest_request_context(
            {
                "company_identity": identity.to_record(),
                "company_filters": normalized_filters,
                "probe_id": probe_key,
                "title": title,
                "max_pages": max_pages,
                "page_limit": page_limit,
                "requested_item_count": requested_items,
                "take_pages": int(payload.get("takePages") or max_pages or 1),
            },
            runtime_timing_overrides=runtime_timing_overrides,
        )
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
                "request_context": request_context,
            },
            asset_type="harvest_company_employees_probe_request",
            source_kind="harvest_company_employees_probe",
            is_raw_asset=False,
            model_safe=True,
            metadata={"probe_id": probe_key, "payload_hash": payload_hash},
        )

        provider_mode = _external_provider_mode()
        if provider_mode in {"simulate", "replay", "scripted"}:
            summary = _build_offline_company_roster_probe_summary(
                identity=identity,
                company_filters=normalized_filters,
                provider_mode=provider_mode,
                probe_id=probe_key,
                title=title,
                payload_hash=payload_hash,
                summary_path=summary_path,
                log_path=log_path,
            )
            logger.write_text(
                log_path,
                str(dict(summary.get("log_summary") or {}).get("detail") or ""),
                asset_type="harvest_company_employees_probe_log",
                source_kind="harvest_company_employees_probe",
                content_type="text/plain",
                is_raw_asset=False,
                model_safe=True,
                metadata={"probe_id": probe_key, "payload_hash": payload_hash, "provider_mode": provider_mode},
            )
            logger.write_json(
                summary_path,
                summary,
                asset_type="harvest_company_employees_probe_summary",
                source_kind="harvest_company_employees_probe",
                is_raw_asset=False,
                model_safe=True,
                metadata={"probe_id": probe_key, "payload_hash": payload_hash, "provider_mode": provider_mode},
            )
            _persist_shared_harvest_payload(
                snapshot_dir,
                logical_name="harvest_company_employees_probe_summary",
                payload=payload,
                body=summary,
                request_context=request_context,
            )
            return summary

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
            time.sleep(resolved_harvest_poll_interval_seconds(request_context, default=2.0))
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
            dataset_items = _get_harvest_dataset_items(
                effective_settings,
                dataset_id,
                logical_name="harvest_company_employees_probe",
                run_id=run_id,
                request_context=request_context,
            )
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
            "dataset_items_path": str(dataset_path) if returned_item_count else "",
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
            request_context=request_context,
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
        runtime_timing_overrides: dict[str, Any] | None = None,
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
                runtime_timing_overrides=runtime_timing_overrides,
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
                "dataset_items_path": str(summary.get("dataset_items_path") or "").strip(),
                "run_id": str(summary.get("run_id") or "").strip(),
                "dataset_id": str(summary.get("dataset_id") or "").strip(),
                "returned_item_count": max(0, int(summary.get("returned_item_count") or 0)),
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

    def _probe_result_if_complete(
        self,
        *,
        probe_metadata: dict[str, Any],
        payload: dict[str, Any],
        request_context: dict[str, Any] | None = None,
    ) -> HarvestExecutionResult | None:
        if not _harvest_company_probe_result_is_complete(probe_metadata):
            return None
        dataset_items_path = Path(str(probe_metadata.get("dataset_items_path") or "")).expanduser()
        if not dataset_items_path.exists():
            return None
        try:
            body = json.loads(dataset_items_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        run_id = str(probe_metadata.get("run_id") or "").strip()
        dataset_id = str(probe_metadata.get("dataset_id") or "").strip()
        merged_context = _merge_harvest_request_context(
            request_context,
            {
                "cache_status": "probe_complete_dataset",
                "probe": dict(probe_metadata or {}),
                "run_id": run_id,
                "dataset_id": dataset_id,
            },
        )
        return HarvestExecutionResult(
            logical_name="harvest_company_employees",
            checkpoint={
                "logical_name": "harvest_company_employees",
                "payload_hash": _payload_cache_key(payload),
                "actor_id": self.settings.actor_id,
                "run_id": run_id,
                "dataset_id": dataset_id,
                "status": "completed",
                "cache_source": "probe_complete_dataset",
                "request_context": dict(merged_context or {}),
            },
            body=body,
            pending=False,
            message="Reused complete Harvest company-employees probe dataset; no second full run was needed.",
            artifacts=[
                HarvestExecutionArtifact(
                    label="dataset_items",
                    payload=body,
                    metadata={
                        "logical_name": "harvest_company_employees",
                        "run_id": run_id,
                        "dataset_id": dataset_id,
                        "provider": "apify",
                        "cache_source": "probe_complete_dataset",
                    },
                )
            ],
        )

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
        runtime_timing_overrides: dict[str, Any] | None = None,
    ) -> HarvestExecutionResult:
        if not _harvest_connector_available(self.settings):
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
                runtime_timing_overrides=runtime_timing_overrides,
            )
            probe_result = self._probe_result_if_complete(
                probe_metadata=probe_metadata,
                payload=payload,
                request_context=_merge_harvest_request_context(
                    {
                        "company_identity": identity.to_record(),
                        "max_pages": max_pages,
                        "page_limit": page_limit,
                        "effective_take_pages": int(payload.get("takePages") or max_pages or 1),
                        "effective_max_items": int(payload.get("maxItems") or effective_requested_items),
                        "requested_item_count": int(payload.get("maxItems") or effective_requested_items),
                        "take_pages": int(payload.get("takePages") or max_pages or 1),
                        "company_filters": normalized_filters,
                    },
                    runtime_timing_overrides=runtime_timing_overrides,
                ),
            )
            if probe_result is not None:
                return probe_result
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
            request_context=_merge_harvest_request_context(
                {
                    "company_identity": identity.to_record(),
                    "max_pages": max_pages,
                    "page_limit": page_limit,
                    "effective_take_pages": int(payload.get("takePages") or max_pages or 1),
                    "effective_max_items": int(payload.get("maxItems") or effective_requested_items),
                    "requested_item_count": int(payload.get("maxItems") or effective_requested_items),
                    "take_pages": int(payload.get("takePages") or max_pages or 1),
                    "company_filters": normalized_filters,
                    "probe": probe_metadata,
                },
                runtime_timing_overrides=runtime_timing_overrides,
            ),
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
        runtime_timing_overrides: dict[str, Any] | None = None,
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
        request_context = _merge_harvest_request_context(
            {
                "company_identity": identity.to_record(),
                "max_pages": max_pages,
                "page_limit": page_limit,
                "requested_item_count": effective_requested_items,
                "take_pages": int(payload.get("takePages") or max_pages or 1),
                "company_filters": normalized_filters,
            },
            runtime_timing_overrides=runtime_timing_overrides,
        )
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
                        **request_context,
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
                    dataset_items_path = (
                        Path(dataset_items_path_value).expanduser() if dataset_items_path_value else None
                    )
                    if (
                        str(queue_summary.get("status") or "").strip().lower() == "completed"
                        and dataset_items_path
                        and dataset_items_path.exists()
                    ):
                        body = json.loads(dataset_items_path.read_text())
                        request_manifest_path = _write_harvest_request_manifest(
                            logger,
                            raw_path,
                            logical_name="harvest_company_employees",
                            payload=payload,
                            request_context={
                                **request_context,
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
                        **request_context,
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
            if not _harvest_connector_available(self.settings):
                raise RuntimeError("Harvest company-employees connector is not enabled.")
            payload_before_probe = dict(payload)
            payload, effective_requested_items, probe_metadata = self._resolve_probe_sized_payload(
                identity=identity,
                snapshot_dir=snapshot_dir,
                requested_payload=payload,
                requested_items=effective_requested_items,
                normalized_filters=normalized_filters,
                asset_logger=logger,
                runtime_timing_overrides=runtime_timing_overrides,
            )
            payload_changed_after_probe = payload != payload_before_probe
            request_context = _merge_harvest_request_context(
                request_context,
                {
                    "effective_take_pages": int(payload.get("takePages") or max_pages or 1),
                    "effective_max_items": int(payload.get("maxItems") or effective_requested_items),
                    "requested_item_count": int(payload.get("maxItems") or effective_requested_items),
                    "take_pages": int(payload.get("takePages") or max_pages or 1),
                    "probe": probe_metadata,
                },
                runtime_timing_overrides=runtime_timing_overrides,
            )
            probe_result = self._probe_result_if_complete(
                probe_metadata=probe_metadata,
                payload=payload,
                request_context=request_context,
            )
            if probe_result is not None:
                body = probe_result.body
                request_manifest_path = _write_harvest_request_manifest(
                    logger,
                    raw_path,
                    logical_name="harvest_company_employees",
                    payload=payload,
                    request_context={
                        **request_context,
                        "cache_status": "probe_complete_dataset",
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
                        "cache_source": "probe_complete_dataset",
                        "request_manifest_path": str(request_manifest_path),
                    },
                )
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
                            **request_context,
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
                effective_settings = replace(
                    self.settings,
                    timeout_seconds=max(
                        int(self.settings.timeout_seconds or 0),
                        _recommended_harvest_company_timeout_seconds(effective_requested_items),
                    ),
                    max_paid_items=max(int(self.settings.max_paid_items or 0), effective_requested_items),
                    max_total_charge_usd=max(
                        float(self.settings.max_total_charge_usd or 0.0),
                        _recommended_harvest_company_charge_cap_usd(
                            payload["profileScraperMode"], effective_requested_items
                        ),
                    ),
                )
                body = _run_harvest_actor(
                    effective_settings,
                    payload,
                    request_context=request_context,
                )
                if body is None:
                    raise RuntimeError("Harvest company-employees run failed.")
                request_manifest_path = _write_harvest_request_manifest(
                    logger,
                    raw_path,
                    logical_name="harvest_company_employees",
                    payload=payload,
                    request_context={
                        **request_context,
                        "cache_status": "live_api",
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
                    request_context=request_context,
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
    layers = _harvest_profile_payload_layers(payload)
    request_metadata = next(
        (
            dict(layer.get("_harvest_request") or {})
            for layer in layers
            if isinstance(layer.get("_harvest_request"), dict)
        ),
        {},
    )
    data = _unwrap_harvest_profile_wrapper(payload)
    full_name = str(_harvest_profile_layered_value(layers, ["fullName", "full_name", "name"]) or "").strip()
    if not full_name:
        full_name = _full_name_from_payload(data)
    headline = str(_harvest_profile_layered_value(layers, ["headline", "occupation"]) or "").strip()
    profile_url = str(_harvest_profile_layered_value(layers, ["linkedinUrl", "profileUrl", "url"]) or "").strip()
    requested_profile_url = str(request_metadata.get("profile_url") or "").strip()
    public_identifier = str(
        _harvest_profile_layered_value(layers, ["publicIdentifier", "public_identifier"]) or ""
    ).strip()
    summary = str(_harvest_profile_layered_value(layers, ["about", "summary", "description"]) or "").strip()
    location_normalized = _normalized_harvest_location(
        _harvest_profile_layered_value(layers, ["location", "locationName"])
    )
    location = str(location_normalized.get("raw_text") or "").strip()
    experience = _coerce_list(_harvest_profile_layered_value(layers, ["experience", "experiences", "positions"]) or [])
    education = _coerce_list(_harvest_profile_layered_value(layers, ["education", "educations", "schools"]) or [])
    languages = _coerce_list(_harvest_profile_layered_value(layers, ["languages", "language"]) or [])
    skills = _coerce_list(_harvest_profile_layered_value(layers, ["skills", "topSkills", "top_skills"]) or [])
    publications = _coerce_list(_harvest_profile_layered_value(layers, ["publications", "posts"]) or [])
    current_company = _extract_current_company(data, experience)
    more_profiles = _coerce_list(_harvest_profile_layered_value(layers, ["moreProfiles", "more_profiles"]) or [])
    primary_email = ""
    primary_email_metadata: dict[str, Any] = {}
    emails_value = _harvest_profile_layered_value(layers, ["emails"])
    emails = list(emails_value or []) if isinstance(emails_value, list) else []
    for raw_email in emails:
        entry = dict(raw_email or {}) if isinstance(raw_email, dict) else {}
        email_value = str(entry.get("email") or entry.get("address") or entry.get("value") or "").strip()
        if not email_value:
            continue
        status = str(entry.get("status") or "").strip().lower()
        quality_score_raw = entry.get("qualityScore")
        try:
            quality_score = int(float(str(quality_score_raw).strip()))
        except (TypeError, ValueError):
            quality_score = None
        metadata = normalized_primary_email_metadata(
            {
                "source": "harvestapi",
                "status": status,
                "qualityScore": quality_score,
                "foundInLinkedInProfile": entry.get("foundInLinkedInProfile"),
            }
        )
        if status == "risky":
            if metadata and not primary_email_metadata:
                primary_email_metadata = metadata
            continue
        if quality_score is not None and quality_score < 80:
            if metadata and not primary_email_metadata:
                primary_email_metadata = metadata
            continue
        primary_email = email_value
        primary_email_metadata = metadata
        break
    return {
        "full_name": full_name,
        "first_name": str(_harvest_profile_layered_value(layers, ["firstName", "first_name"]) or "").strip(),
        "last_name": str(_harvest_profile_layered_value(layers, ["lastName", "last_name"]) or "").strip(),
        "headline": headline,
        "profile_url": profile_url,
        "requested_profile_url": requested_profile_url,
        "public_identifier": public_identifier,
        "summary": summary,
        "location": location,
        "location_normalized": location_normalized,
        "current_company": current_company,
        "primary_email": primary_email,
        "primary_email_metadata": primary_email_metadata,
        "experience": experience,
        "education": education,
        "languages": languages,
        "skills": skills,
        "publications": publications,
        "more_profiles": more_profiles,
    }


def _build_harvest_profile_result(
    *,
    raw_path: Path,
    payload: Any,
    account_id: str,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if not harvest_profile_payload_has_usable_content(payload):
        return None
    return {
        "raw_path": raw_path,
        "account_id": account_id,
        "parsed": parse_harvest_profile_payload(payload),
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
                "username": str(data.get("publicIdentifier") or data.get("public_identifier") or "").strip()
                or _slug_from_linkedin_url(profile_url),
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
        member_key = (
            public_identifier
            or _slug_from_linkedin_url(profile_url)
            or sha1("|".join([full_name, headline, location]).encode("utf-8")).hexdigest()[:16]
        )
        rows.append(
            {
                "member_key": member_key,
                "member_id": public_identifier,
                "urn": str(data.get("entityUrn") or data.get("urn") or "").strip(),
                "full_name": full_name,
                "headline": headline,
                "location": location,
                "location_normalized": _normalized_harvest_location(data.get("location") or data.get("locationName")),
                "avatar_url": str(
                    data.get("photoUrl") or data.get("pictureUrl") or data.get("profilePicture") or ""
                ).strip(),
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


def _harvest_company_probe_result_is_complete(probe_metadata: dict[str, Any] | None) -> bool:
    metadata = dict(probe_metadata or {})
    if not bool(metadata.get("probe_performed")):
        return False
    if str(metadata.get("status") or "").strip().lower() != "completed":
        return False
    if bool(metadata.get("provider_result_limited")):
        return False
    estimated_total_count = max(0, int(metadata.get("estimated_total_count") or 0))
    returned_item_count = max(0, int(metadata.get("returned_item_count") or 0))
    if estimated_total_count <= 0:
        return False
    if returned_item_count < estimated_total_count:
        return False
    dataset_items_path = str(metadata.get("dataset_items_path") or "").strip()
    return bool(dataset_items_path)


def _offline_company_roster_probe_count(
    identity: CompanyIdentity,
    company_filters: dict[str, Any] | None = None,
) -> int:
    filters = dict(company_filters or {})
    normalized_company = normalize_company_key(identity.canonical_name or identity.requested_name)
    function_ids = sorted(str(item).strip() for item in list(filters.get("function_ids") or []) if str(item).strip())
    exclude_function_ids = sorted(
        str(item).strip() for item in list(filters.get("exclude_function_ids") or []) if str(item).strip()
    )
    search_query = " ".join(str(filters.get("search_query") or "").split()).strip().lower()

    if normalized_company == "xai":
        if function_ids == ["8"]:
            return 3100
        if function_ids == ["24"]:
            return 2600
        if exclude_function_ids == ["8", "24"]:
            return 1200
        if exclude_function_ids == ["8"]:
            return 3900
        return 7100

    if normalized_company == "anthropic":
        if function_ids == ["8"]:
            return 1100
        if exclude_function_ids == ["8"]:
            return 2024
        return 3124

    if normalized_company == "google":
        if "multimodal" in search_query:
            return 1800
        if "veo" in search_query:
            return 900
        if "nano banana" in search_query:
            return 850
        if function_ids == ["8"]:
            return 4200
        if function_ids == ["24"]:
            return 3600
        return 15000

    if normalized_company == "openai":
        if function_ids == ["8"]:
            return 2100
        if function_ids == ["24"]:
            return 1800
        return 5400

    if search_query:
        return 600
    return 180


def _build_offline_company_roster_probe_summary(
    *,
    identity: CompanyIdentity,
    company_filters: dict[str, Any] | None,
    provider_mode: str,
    probe_id: str,
    title: str,
    payload_hash: str,
    summary_path: Path,
    log_path: Path,
) -> dict[str, Any]:
    normalized_filters = dict(company_filters or {})
    normalized_mode = str(provider_mode or "simulate").strip().lower() or "simulate"
    if normalized_mode == "replay":
        estimated_total_count = 0
        provider_result_limited = False
        detail = "Replay mode roster probe has no cached live summary, so provider-side total is unknown."
    else:
        estimated_total_count = max(0, _offline_company_roster_probe_count(identity, normalized_filters))
        provider_result_limited = estimated_total_count > _HARVEST_PROVIDER_RESULT_CAP
        detail = f"Offline {provider_mode} roster probe estimated {estimated_total_count} profiles."
        if provider_result_limited:
            detail += " Query exceeded the provider's 2500-result ceiling."
    return {
        "probe_id": probe_id,
        "title": title,
        "status": "completed",
        "run_id": f"{provider_mode}_probe_{probe_id}",
        "dataset_id": "",
        "payload_hash": payload_hash,
        "company_identity": identity.to_record(),
        "company_filters": normalized_filters,
        "estimated_total_count": estimated_total_count,
        "provider_result_limited": provider_result_limited,
        "returned_item_count": min(25, estimated_total_count),
        "log_summary": {
            "estimated_total_count": estimated_total_count,
            "provider_result_limited": provider_result_limited,
            "scraped_page_count": 1 if estimated_total_count else 0,
            "last_page_size": min(25, estimated_total_count),
            "scraping_query": str(title or probe_id or "").strip(),
            "found_query": str(title or probe_id or "").strip(),
            "detail": detail,
        },
        "detail": detail,
        "summary_path": str(summary_path),
        "log_path": str(log_path),
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
    current = base_path.expanduser()
    env_runtime_dir = str(os.getenv("SOURCING_RUNTIME_DIR") or "").strip()
    if env_runtime_dir:
        configured_runtime = Path(env_runtime_dir).expanduser()
        try:
            resolved_current = current.resolve()
            resolved_configured = configured_runtime.resolve()
            resolved_current.relative_to(resolved_configured)
            return resolved_configured
        except (OSError, ValueError):
            pass
    for candidate in [current, *current.parents]:
        if candidate.name == "runtime":
            return candidate
    resolved_current = current.resolve()
    if resolved_current != current:
        for candidate in [resolved_current, *resolved_current.parents]:
            if candidate.name == "runtime":
                return candidate
    return None


def _shared_harvest_cache_path(base_path: Path, logical_name: str, payload: dict[str, Any]) -> Path | None:
    runtime_dir = _runtime_dir_from_path(base_path)
    if runtime_dir is None:
        return None
    cache_dir = shared_provider_cache_dir(runtime_dir, logical_name)
    if cache_dir is None:
        return None
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{_payload_cache_key(payload)}.json"


def _legacy_shared_harvest_cache_path(base_path: Path, logical_name: str, payload: dict[str, Any]) -> Path | None:
    runtime_dir = _runtime_dir_from_path(base_path)
    if runtime_dir is None:
        return None
    return runtime_dir / "provider_cache" / logical_name / f"{_payload_cache_key(payload)}.json"


def _delete_harvest_cache_payload(cache_path: Path) -> None:
    try:
        cache_path.unlink()
    except Exception:
        pass
    request_manifest_path = cache_path.with_name(f"{cache_path.stem}.request.json")
    if request_manifest_path.exists():
        try:
            request_manifest_path.unlink()
        except Exception:
            pass


def _discard_offline_legacy_shared_harvest_cache(base_path: Path, logical_name: str, payload: dict[str, Any]) -> None:
    cache_path = _legacy_shared_harvest_cache_path(base_path, logical_name, payload)
    if cache_path is None or not cache_path.exists():
        return
    try:
        cached_body = json.loads(cache_path.read_text())
    except json.JSONDecodeError:
        return
    if _harvest_payload_contains_offline_markers(cached_body):
        _delete_harvest_cache_payload(cache_path)


def _harvest_payload_contains_offline_markers(payload: Any) -> bool:
    if isinstance(payload, dict):
        if bool(payload.get("_offline")):
            return True
        provider_mode = str(payload.get("_provider_mode") or "").strip().lower()
        if provider_mode in {"replay", "simulate", "scripted"}:
            return True
        item = payload.get("item")
        if item is not None and _harvest_payload_contains_offline_markers(item):
            return True
        return any(_harvest_payload_contains_offline_markers(value) for value in payload.values())
    if isinstance(payload, list):
        return any(_harvest_payload_contains_offline_markers(item) for item in payload)
    return False


def _shared_harvest_payload_is_cacheable(body: Any, *, request_context: dict[str, Any] | None = None) -> bool:
    if _harvest_payload_contains_offline_markers(body):
        return False
    normalized_mode = str((request_context or {}).get("provider_mode") or _external_provider_mode() or "").strip().lower()
    return normalized_mode not in {"replay", "simulate", "scripted"}


def _persist_shared_harvest_payload(
    base_path: Path,
    *,
    logical_name: str,
    payload: dict[str, Any],
    body: Any,
    request_context: dict[str, Any] | None = None,
) -> None:
    if not _shared_harvest_payload_is_cacheable(body, request_context=request_context):
        return
    cache_path = _shared_harvest_cache_path(base_path, logical_name, payload)
    if cache_path is None:
        return
    runtime_context = shared_provider_cache_context(runtime_dir=_runtime_dir_from_path(base_path))
    try:
        cache_path.write_text(json.dumps(body, ensure_ascii=False, indent=2))
        request_path = cache_path.with_name(f"{cache_path.stem}.request.json")
        request_path.write_text(
            json.dumps(
                {
                    "logical_name": logical_name,
                    "payload_hash": _payload_cache_key(payload),
                    "request_payload": payload,
                    "request_context": {
                        **dict(request_context or {}),
                        "provider_mode": str((request_context or {}).get("provider_mode") or _external_provider_mode() or ""),
                        **runtime_context,
                    },
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
            cached_body = json.loads(cache_path.read_text())
        except json.JSONDecodeError:
            pass
        else:
            if _harvest_payload_contains_offline_markers(cached_body):
                _delete_harvest_cache_payload(cache_path)
            else:
                return cached_body, "shared_cache", cache_path

    _discard_offline_legacy_shared_harvest_cache(base_path, logical_name, payload)
    if _external_provider_mode() != LIVE_PROVIDER_MODE:
        return None, None, None

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


def _harvest_profile_payload_layers(payload: dict[str, Any]) -> list[dict[str, Any]]:
    layers: list[dict[str, Any]] = []
    current: dict[str, Any] | None = dict(payload or {})
    depth = 0
    while isinstance(current, dict) and depth < 6:
        layers.append(current)
        nested = _next_harvest_profile_wrapper(current)
        if nested is None:
            break
        current = nested
        depth += 1
    return layers


def _unwrap_harvest_profile_wrapper(payload: dict[str, Any]) -> dict[str, Any]:
    layers = _harvest_profile_payload_layers(payload)
    for layer in reversed(layers):
        if any(
            layer.get(key)
            for key in (
                "linkedinUrl",
                "profileUrl",
                "url",
                "publicIdentifier",
                "public_identifier",
                "fullName",
                "full_name",
                "headline",
                "occupation",
                "experience",
                "experiences",
                "positions",
                "education",
                "educations",
                "schools",
            )
        ):
            return dict(layer)
    return dict(layers[-1]) if layers else {}


def _next_harvest_profile_wrapper(layer: dict[str, Any]) -> dict[str, Any] | None:
    for key in ("item", "data", "profile", "result"):
        candidate = layer.get(key)
        if isinstance(candidate, dict) and candidate is not layer:
            return dict(candidate)
    return None


def _harvest_profile_layered_value(layers: list[dict[str, Any]], keys: list[str]) -> Any:
    for layer in reversed(layers):
        for key in keys:
            value = layer.get(key)
            if value not in (None, "", [], {}):
                return value
    return None


def _harvest_profile_match_context(payload: dict[str, Any]) -> dict[str, str]:
    layers = _harvest_profile_payload_layers(payload)
    requested_profile_url = ""
    for layer in layers:
        request_metadata = layer.get("_harvest_request")
        if isinstance(request_metadata, dict):
            requested_profile_url = str(request_metadata.get("profile_url") or "").strip() or requested_profile_url
        original_query = layer.get("originalQuery")
        if isinstance(original_query, dict):
            requested_profile_url = (
                str(
                    original_query.get("url")
                    or original_query.get("linkedinUrl")
                    or original_query.get("profileUrl")
                    or ""
                ).strip()
                or requested_profile_url
            )
        elif str(original_query or "").strip():
            requested_profile_url = str(original_query or "").strip() or requested_profile_url
    profile_url = str(_harvest_profile_layered_value(layers, ["linkedinUrl", "profileUrl", "url"]) or "").strip()
    public_identifier = str(
        _harvest_profile_layered_value(layers, ["publicIdentifier", "public_identifier"]) or ""
    ).strip()
    return {
        "requested_profile_url": requested_profile_url,
        "profile_url": profile_url,
        "public_identifier": public_identifier,
    }


def _match_item_by_original_query_url(requested_url: str, items: list[dict[str, Any]]) -> int | None:
    requested_normalized = _normalize_linkedin_profile_url(requested_url)
    if not requested_normalized:
        return None
    for index, item in enumerate(items):
        original_url = str(_harvest_profile_match_context(item).get("requested_profile_url") or "").strip()
        if requested_normalized and requested_normalized == _normalize_linkedin_profile_url(original_url):
            return index
    return None


def _match_item_by_exact_profile_url(requested_url: str, items: list[dict[str, Any]]) -> int | None:
    requested_normalized = _normalize_linkedin_profile_url(requested_url)
    if not requested_normalized:
        return None
    for index, item in enumerate(items):
        item_url = str(_harvest_profile_match_context(item).get("profile_url") or "").strip()
        if requested_normalized and requested_normalized == _normalize_linkedin_profile_url(item_url):
            return index
    return None


def _match_item_by_identifier(requested_url: str, items: list[dict[str, Any]]) -> int | None:
    requested_slug = _normalize_profile_identifier(_slug_from_linkedin_url(requested_url))
    if not requested_slug:
        return None
    for index, item in enumerate(items):
        match_context = _harvest_profile_match_context(item)
        public_identifier = _normalize_profile_identifier(str(match_context.get("public_identifier") or "").strip())
        item_url_slug = _normalize_profile_identifier(
            _slug_from_linkedin_url(str(match_context.get("profile_url") or "").strip())
        )
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
_HARVEST_DATASET_FETCH_MAX_ATTEMPTS = 3
_HARVEST_DATASET_PAGE_SIZE_DEFAULT = 200
_HARVEST_DATASET_PAGE_SIZE_PROFILE_BATCH = 100
_HARVEST_RETRYABLE_HTTP_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}
_HARVEST_RETRYABLE_ERROR_MARKERS = (
    "incompleteread",
    "timed out",
    "timeout",
    "temporarily unavailable",
    "temporary failure",
    "connection aborted",
    "connection reset",
    "remote end closed connection",
    "remotedisconnected",
    "eof occurred",
    "invalid json",
)


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
            return (
                _HARVEST_PROFILE_RETRY_BATCH_SIZES[index + 1]
                if index + 1 < len(_HARVEST_PROFILE_RETRY_BATCH_SIZES)
                else 0
            )
    return 0


def _live_harvest_profile_retry_batch_sizes(
    initial_requested_count: int,
    unresolved_count: int,
) -> list[int]:
    initial_count = max(0, int(initial_requested_count or 0))
    count = max(0, int(unresolved_count or 0))
    if count <= 1:
        return []
    if count <= 3 and initial_count > count:
        return [count]
    return []


def _allow_live_harvest_profile_direct_fallback(unresolved_count: int) -> bool:
    return max(0, int(unresolved_count or 0)) == 1


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


def _recommended_harvest_dataset_page_size(logical_name: str, *, request_context: dict[str, Any] | None = None) -> int:
    normalized_name = str(logical_name or "").strip().lower()
    requested_item_count = 0
    if isinstance(request_context, dict):
        try:
            requested_item_count = max(
                0,
                int(
                    request_context.get("requested_url_count")
                    or request_context.get("requested_item_count")
                    or request_context.get("requested_items")
                    or 0
                ),
            )
        except (TypeError, ValueError):
            requested_item_count = 0
    if normalized_name == "harvest_profile_scraper_batch":
        if requested_item_count >= 500:
            return 50
        if requested_item_count >= 200:
            return 75
        return _HARVEST_DATASET_PAGE_SIZE_PROFILE_BATCH
    if requested_item_count >= 2000:
        return 100
    return _HARVEST_DATASET_PAGE_SIZE_DEFAULT


def _harvest_retry_backoff_seconds(attempt_index: int) -> float:
    return min(8.0, 1.0 * (2 ** max(0, int(attempt_index or 0))))


def _is_retryable_harvest_request_error(exc: Exception) -> bool:
    message = str(exc or "").strip().lower()
    if not message:
        return False
    http_match = re.search(r"harvest api http\s+(\d+)", message)
    if http_match:
        try:
            return int(http_match.group(1)) in _HARVEST_RETRYABLE_HTTP_STATUS_CODES
        except (TypeError, ValueError):
            return False
    return any(marker in message for marker in _HARVEST_RETRYABLE_ERROR_MARKERS)


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
    request_context = _merge_harvest_request_context(existing.get("request_context"), request_context)
    provider_mode = _external_provider_mode()
    validate_runtime_environment(runtime_dir=_runtime_dir_from_path(base_path), provider_mode=provider_mode)
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

    if provider_mode == "scripted":
        return _build_scripted_harvest_result(
            logical_name=logical_name,
            payload=payload,
            checkpoint=existing,
            request_context=request_context,
        )

    if provider_mode in {"simulate", "replay"}:
        simulated_body = _build_offline_harvest_body(
            logical_name=logical_name,
            payload=payload,
            provider_mode=provider_mode,
        )
        message = (
            f"Simulated Harvest response for {logical_name}; no external request was sent."
            if provider_mode == "simulate"
            else f"Replay mode returned cached-or-empty Harvest response for {logical_name} without live provider access."
        )
        return HarvestExecutionResult(
            logical_name=logical_name,
            checkpoint={
                "logical_name": logical_name,
                "payload_hash": _payload_cache_key(payload),
                "status": "completed",
                "provider_mode": provider_mode,
                "request_context": dict(request_context or {}),
            },
            body=simulated_body,
            message=message,
            artifacts=[
                HarvestExecutionArtifact(
                    label=f"{provider_mode}_response",
                    payload={
                        "logical_name": logical_name,
                        "payload_hash": _payload_cache_key(payload),
                        "provider_mode": provider_mode,
                        "body_count": len(simulated_body) if isinstance(simulated_body, list) else 0,
                    },
                    metadata={"provider_mode": provider_mode, "logical_name": logical_name},
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

    def _retryable_pending_result(
        exc: HarvestRetryableRequestError,
        *,
        run_id: str,
        dataset_id: str,
        artifacts: list[HarvestExecutionArtifact],
    ) -> HarvestExecutionResult:
        retry_count = max(0, int(existing.get("dataset_fetch_retry_count") or 0)) + 1
        message = f"Harvest dataset download will be retried for run {run_id} / dataset {dataset_id}: {exc}"
        retry_artifact = HarvestExecutionArtifact(
            label="dataset_items_retryable_error",
            payload={
                "logical_name": logical_name,
                "run_id": run_id,
                "dataset_id": dataset_id,
                "message": str(exc),
                "endpoint": exc.endpoint,
                "retry_count": retry_count,
            },
            metadata={
                "logical_name": logical_name,
                "run_id": run_id,
                "dataset_id": dataset_id,
                "retry_count": retry_count,
                "provider": "apify",
            },
        )
        return HarvestExecutionResult(
            logical_name=logical_name,
            checkpoint={
                **base_checkpoint,
                "run_id": run_id,
                "dataset_id": dataset_id,
                "status": "dataset_download_retryable",
                "dataset_fetch_retry_count": retry_count,
                "last_retryable_error": str(exc),
            },
            pending=True,
            message=message,
            artifacts=[*artifacts, retry_artifact],
        )

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
            try:
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
            except HarvestRetryableRequestError as exc:
                return _retryable_pending_result(exc, run_id=run_id, dataset_id=dataset_id, artifacts=artifacts)
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
    try:
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
    except HarvestRetryableRequestError as exc:
        return _retryable_pending_result(exc, run_id=run_id, dataset_id=dataset_id, artifacts=artifacts)


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
    body = _get_harvest_dataset_items(
        settings,
        dataset_id,
        logical_name=logical_name,
        run_id=run_id,
        request_context=request_context,
    )
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
    endpoint = f"https://api.apify.com/v2/acts/{parse.quote(settings.actor_id, safe='')}/runs?" + parse.urlencode(
        {
            "token": settings.api_token,
            "waitForFinish": 0,
            "timeout": settings.timeout_seconds,
            "maxTotalChargeUsd": settings.max_total_charge_usd,
        }
    )
    return _harvest_json_request(endpoint, payload=payload, timeout=settings.timeout_seconds + 15)


def _get_harvest_actor_run(settings: HarvestActorSettings, run_id: str) -> Any:
    endpoint = (
        f"https://api.apify.com/v2/actor-runs/{parse.quote(str(run_id or '').strip(), safe='')}?"
        + parse.urlencode({"token": settings.api_token})
    )
    return _harvest_json_request(endpoint, timeout=settings.timeout_seconds + 15)


def _get_harvest_dataset_items(
    settings: HarvestActorSettings,
    dataset_id: str,
    *,
    logical_name: str = "",
    run_id: str = "",
    request_context: dict[str, Any] | None = None,
) -> Any:
    page_size = _recommended_harvest_dataset_page_size(logical_name, request_context=request_context)
    offset = 0
    items: list[Any] = []
    while True:
        page = _get_harvest_dataset_items_page(
            settings,
            dataset_id,
            offset=offset,
            limit=page_size,
            logical_name=logical_name,
            run_id=run_id,
            request_context=request_context,
        )
        if page is None:
            break
        if isinstance(page, list):
            page_items = list(page)
        else:
            page_items = [page]
        if not page_items:
            break
        items.extend(page_items)
        if len(page_items) < page_size:
            break
        offset += len(page_items)
    return items


def _get_harvest_dataset_items_page(
    settings: HarvestActorSettings,
    dataset_id: str,
    *,
    offset: int,
    limit: int,
    logical_name: str,
    run_id: str,
    request_context: dict[str, Any] | None = None,
) -> Any:
    endpoint = (
        f"https://api.apify.com/v2/datasets/{parse.quote(str(dataset_id or '').strip(), safe='')}/items?"
        + parse.urlencode(
            {
                "token": settings.api_token,
                "format": "json",
                "clean": "true",
                "offset": max(0, int(offset or 0)),
                "limit": max(1, int(limit or _HARVEST_DATASET_PAGE_SIZE_DEFAULT)),
            }
        )
    )
    last_error: Exception | None = None
    for attempt_index in range(_HARVEST_DATASET_FETCH_MAX_ATTEMPTS):
        try:
            return _harvest_json_request(endpoint, timeout=settings.timeout_seconds + 15)
        except RuntimeError as exc:
            last_error = exc
            if not _is_retryable_harvest_request_error(exc):
                raise
            if attempt_index + 1 >= _HARVEST_DATASET_FETCH_MAX_ATTEMPTS:
                break
            time.sleep(
                resolved_harvest_retry_backoff_seconds(
                    request_context,
                    attempt_index=attempt_index,
                    default=_harvest_retry_backoff_seconds(attempt_index),
                )
            )
    detail = str(last_error or "unknown dataset download failure")
    raise HarvestRetryableRequestError(
        f"Harvest dataset download failed after retries: {detail}",
        endpoint=endpoint,
        logical_name=logical_name,
        run_id=run_id,
        dataset_id=dataset_id,
    )


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
    http_request = request.Request(
        endpoint, data=data, headers=headers, method="POST" if payload is not None else "GET"
    )
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


def _infer_harvest_sync_logical_name(payload: dict[str, Any]) -> str:
    normalized_payload = dict(payload or {})
    if (
        normalized_payload.get("urls")
        or normalized_payload.get("publicIdentifiers")
        or normalized_payload.get("profileIds")
    ):
        return "harvest_profile_scraper_batch"
    if normalized_payload.get("companies"):
        return "harvest_company_employees"
    return "harvest_profile_search"


def _infer_harvest_sync_request_context(payload: dict[str, Any], settings: HarvestActorSettings) -> dict[str, Any]:
    normalized_payload = dict(payload or {})
    if (
        normalized_payload.get("urls")
        or normalized_payload.get("publicIdentifiers")
        or normalized_payload.get("profileIds")
    ):
        requested_url_count = max(
            len(list(normalized_payload.get("urls") or [])),
            len(list(normalized_payload.get("publicIdentifiers") or [])),
            len(list(normalized_payload.get("profileIds") or [])),
        )
        return {
            "requested_url_count": requested_url_count,
            "requested_item_count": requested_url_count,
        }
    requested_item_count = 0
    try:
        requested_item_count = int(normalized_payload.get("maxItems") or 0)
    except (TypeError, ValueError):
        requested_item_count = 0
    if requested_item_count <= 0:
        requested_item_count = max(0, int(settings.max_paid_items or 0))
    return {
        "requested_item_count": requested_item_count,
        "take_pages": int(normalized_payload.get("takePages") or 0)
        if str(normalized_payload.get("takePages") or "").strip()
        else 0,
    }


def _harvest_sync_should_prefer_async(settings: HarvestActorSettings, payload: dict[str, Any]) -> bool:
    logical_name = _infer_harvest_sync_logical_name(payload)
    request_context = _infer_harvest_sync_request_context(payload, settings)
    requested_item_count = max(0, int(request_context.get("requested_item_count") or 0))
    requested_url_count = max(0, int(request_context.get("requested_url_count") or 0))
    if logical_name == "harvest_profile_scraper_batch":
        return requested_url_count >= 50 or requested_item_count >= 50
    if logical_name == "harvest_company_employees":
        return requested_item_count >= 100 or int(request_context.get("take_pages") or 0) >= 5
    return requested_item_count >= 100


def _run_harvest_actor_sync_request(settings: HarvestActorSettings, payload: dict[str, Any]) -> Any | None:
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


def _run_harvest_actor_via_async_dataset(
    settings: HarvestActorSettings,
    payload: dict[str, Any],
    *,
    logical_name: str,
    request_context: dict[str, Any],
) -> Any | None:
    try:
        submit_payload = _submit_harvest_actor_run(settings, payload)
        run = _apify_data_record(submit_payload)
        run_id = str(run.get("id") or run.get("runId") or "").strip()
        dataset_id = str(run.get("defaultDatasetId") or run.get("datasetId") or "").strip()
        if not run_id:
            return None
        run_status = _normalize_harvest_run_status(run.get("status") or "submitted")
        deadline = time.monotonic() + max(60, min(2400, int(settings.timeout_seconds or 180) + 120))
        while not _harvest_run_is_terminal(run_status):
            if time.monotonic() >= deadline:
                return None
            time.sleep(resolved_harvest_poll_interval_seconds(request_context, default=2.0))
            run_payload = _get_harvest_actor_run(settings, run_id)
            run = _apify_data_record(run_payload)
            dataset_id = str(run.get("defaultDatasetId") or run.get("datasetId") or dataset_id).strip()
            run_status = _normalize_harvest_run_status(run.get("status") or run_status or "running")
        if not _harvest_run_succeeded(run_status) or not dataset_id:
            return None
        return _get_harvest_dataset_items(
            settings,
            dataset_id,
            logical_name=logical_name,
            run_id=run_id,
            request_context=request_context,
        )
    except Exception:
        return None


def _run_harvest_actor(
    settings: HarvestActorSettings,
    payload: dict[str, Any],
    *,
    request_context: dict[str, Any] | None = None,
) -> Any | None:
    provider_mode = _external_provider_mode()
    logical_name = _infer_harvest_sync_logical_name(payload)
    request_context = _merge_harvest_request_context(
        _infer_harvest_sync_request_context(payload, settings),
        request_context,
    )
    if provider_mode == "scripted":
        record_scripted_provider_invocation(
            provider_name="scripted_harvest",
            dispatch_kind="harvest.execute",
            logical_name=logical_name,
            payload=payload,
            metadata={"request_context": dict(request_context or {})},
        )
        checkpoint: dict[str, Any] = {}
        max_rounds = max(1, _env_int("SOURCING_SCRIPTED_SYNC_HARVEST_MAX_ROUNDS", 16))
        for _ in range(max_rounds):
            scripted = _build_scripted_harvest_result(
                logical_name=logical_name,
                payload=payload,
                checkpoint=checkpoint,
                request_context=request_context,
            )
            checkpoint = dict(scripted.checkpoint or checkpoint)
            if scripted.pending:
                continue
            return scripted.body
        return None
    if provider_mode in {"simulate", "replay"}:
        return _build_offline_harvest_body(
            logical_name=logical_name,
            payload=payload,
            provider_mode=provider_mode,
        )
    if _harvest_sync_should_prefer_async(settings, payload):
        return _run_harvest_actor_via_async_dataset(
            settings,
            payload,
            logical_name=logical_name,
            request_context=request_context,
        )
    body = _run_harvest_actor_sync_request(settings, payload)
    if body is not None:
        return body
    return _run_harvest_actor_via_async_dataset(
        settings,
        payload,
        logical_name=logical_name,
        request_context=request_context,
    )


def _build_offline_harvest_body(
    *,
    logical_name: str,
    payload: dict[str, Any],
    provider_mode: str,
) -> list[dict[str, Any]]:
    normalized_mode = str(provider_mode or "simulate").strip().lower() or "simulate"
    if normalized_mode == "replay":
        # Replay is intentionally cache-only. On a cache miss we must not synthesize
        # provider rows that can later be promoted into authoritative company assets.
        return []

    metadata = {
        "_offline": True,
        "_provider_mode": provider_mode,
        "_logical_name": logical_name,
    }
    if logical_name == "harvest_company_employees":
        companies = [str(item).strip() for item in list(payload.get("companies") or []) if str(item).strip()]
        company_url = companies[0] if companies else "https://www.linkedin.com/company/example/"
        company_slug_match = re.search(r"/company/([^/]+)/?", company_url)
        company_slug = str(company_slug_match.group(1) if company_slug_match else "example").strip() or "example"
        company_label = {
            "xai": "xAI",
            "anthropicresearch": "Anthropic",
            "openai": "OpenAI",
            "google": "Google",
            "deepmind": "Google DeepMind",
        }.get(company_slug.lower(), company_slug.replace("-", " ").title())
        requested_count = max(1, int(payload.get("maxItems") or 25))
        result_count = min(requested_count, 40)
        search_query = " ".join(str(payload.get("searchQuery") or payload.get("search_query") or "").split()).strip()
        headline_suffix = search_query or "Research Engineer"
        location_values = [str(item).strip() for item in list(payload.get("locations") or []) if str(item).strip()]
        location_name = location_values[0] if location_values else "United States"
        results: list[dict[str, Any]] = []
        for index in range(1, result_count + 1):
            public_identifier = f"{company_slug}-offline-{index}"
            results.append(
                {
                    **metadata,
                    "fullName": f"{company_label} Offline Member {index}",
                    "headline": f"{headline_suffix} at {company_label}",
                    "linkedinUrl": f"https://www.linkedin.com/in/{public_identifier}/",
                    "publicIdentifier": public_identifier,
                    "photoUrl": f"https://cdn.example.com/{public_identifier}.jpg",
                    "locationName": location_name,
                    "item": {
                        "fullName": f"{company_label} Offline Member {index}",
                        "headline": f"{headline_suffix} at {company_label}",
                        "linkedinUrl": f"https://www.linkedin.com/in/{public_identifier}/",
                        "publicIdentifier": public_identifier,
                        "photoUrl": f"https://cdn.example.com/{public_identifier}.jpg",
                        "locationName": location_name,
                    },
                }
            )
        return results
    if logical_name == "harvest_profile_scraper_batch":
        results: list[dict[str, Any]] = []
        for url in list(payload.get("urls") or []):
            profile_url = str(url or "").strip()
            if not profile_url:
                continue
            results.append(
                {
                    **metadata,
                    "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                    "linkedinUrl": profile_url,
                    "publicIdentifier": _offline_profile_identifier(profile_url),
                    "headline": f"Offline {normalized_mode} profile placeholder",
                    "fullName": "",
                    "item": {
                        "linkedinUrl": profile_url,
                        "publicIdentifier": _offline_profile_identifier(profile_url),
                        "headline": f"Offline {normalized_mode} profile placeholder",
                    },
                }
            )
        return results
    return []


def _build_scripted_harvest_result(
    *,
    logical_name: str,
    payload: dict[str, Any],
    checkpoint: dict[str, Any] | None,
    request_context: dict[str, Any] | None,
) -> HarvestExecutionResult:
    existing = dict(checkpoint or {})
    rule = find_scripted_rule(
        "harvest",
        context={
            "logical_name": logical_name,
            "payload": payload,
            "request_context": dict(request_context or {}),
            "provider_name": "scripted_harvest",
            "context_contains": [str(existing.get("scripted_rule_name") or "")]
            if str(existing.get("scripted_rule_name") or "").strip()
            else [],
        },
    )
    scripted_sleep(
        rule,
        phase="execute",
        seconds_cap=resolved_harvest_scripted_sleep_seconds_cap(request_context),
    )
    updated_checkpoint, round_number = advance_scripted_phase_round(existing, phase="execute")
    error_spec = scripted_phase_error(rule, phase="execute", round_number=round_number)
    if error_spec:
        kind = str(error_spec.get("kind") or "runtime").strip().lower()
        message = str(error_spec.get("message") or f"Scripted harvest {kind} error for {logical_name}.").strip()
        if kind == "retryable":
            return HarvestExecutionResult(
                logical_name=logical_name,
                checkpoint={
                    **updated_checkpoint,
                    "logical_name": logical_name,
                    "payload_hash": _payload_cache_key(payload),
                    "status": str(error_spec.get("status") or "submitted"),
                    "provider_mode": "scripted",
                    "run_id": str(rule.get("run_id") or existing.get("run_id") or f"scripted_run_{logical_name}"),
                    "dataset_id": str(
                        rule.get("dataset_id") or existing.get("dataset_id") or f"scripted_dataset_{logical_name}"
                    ),
                    "scripted_rule_name": str(rule.get("_rule_name") or existing.get("scripted_rule_name") or ""),
                    "request_context": dict(request_context or {}),
                },
                pending=True,
                message=message,
                artifacts=[
                    HarvestExecutionArtifact(
                        label="scripted_retryable_error",
                        payload={
                            "logical_name": logical_name,
                            "round": round_number,
                            "message": message,
                            "rule": str(rule.get("_rule_name") or ""),
                        },
                        metadata={"provider_mode": "scripted"},
                    )
                ],
            )
        raise RuntimeError(message)

    pending_rounds = scripted_pending_rounds(rule, phase="execute")
    artifacts = [
        HarvestExecutionArtifact(
            label=str(item.get("label") or "scripted_harvest_artifact"),
            payload=item.get("payload"),
            raw_format=str(item.get("raw_format") or "json"),
            content_type=str(item.get("content_type") or "application/json"),
            metadata=dict(item.get("metadata") or {}),
        )
        for item in scripted_rule_artifacts(rule, phase="execute")
    ]
    base_checkpoint = {
        **updated_checkpoint,
        "logical_name": logical_name,
        "payload_hash": _payload_cache_key(payload),
        "provider_mode": "scripted",
        "scripted_rule_name": str(rule.get("_rule_name") or existing.get("scripted_rule_name") or ""),
        "request_context": dict(request_context or {}),
    }
    if round_number <= pending_rounds:
        return HarvestExecutionResult(
            logical_name=logical_name,
            checkpoint={
                **base_checkpoint,
                "status": str(rule.get("pending_status") or "submitted"),
                "run_id": str(rule.get("run_id") or existing.get("run_id") or f"scripted_run_{logical_name}"),
                "dataset_id": str(
                    rule.get("dataset_id") or existing.get("dataset_id") or f"scripted_dataset_{logical_name}"
                ),
            },
            pending=True,
            message=str(rule.get("pending_message") or f"Scripted Harvest task for {logical_name} is still pending."),
            artifacts=[
                *artifacts,
                HarvestExecutionArtifact(
                    label="scripted_harvest_pending",
                    payload={
                        "logical_name": logical_name,
                        "round": round_number,
                        "rule": str(rule.get("_rule_name") or ""),
                    },
                    metadata={"provider_mode": "scripted"},
                ),
            ],
        )
    body = rule.get("body")
    if not isinstance(body, list):
        body = _build_offline_harvest_body(
            logical_name=logical_name,
            payload=payload,
            provider_mode="scripted",
        )
    return HarvestExecutionResult(
        logical_name=logical_name,
        checkpoint={
            **base_checkpoint,
            "status": "completed",
            "run_id": str(rule.get("run_id") or existing.get("run_id") or f"scripted_run_{logical_name}"),
            "dataset_id": str(
                rule.get("dataset_id") or existing.get("dataset_id") or f"scripted_dataset_{logical_name}"
            ),
        },
        body=body,
        message=str(rule.get("message") or f"Scripted Harvest response returned for {logical_name}."),
        artifacts=[
            *artifacts,
            HarvestExecutionArtifact(
                label="scripted_harvest_response",
                payload={
                    "logical_name": logical_name,
                    "round": round_number,
                    "rule": str(rule.get("_rule_name") or ""),
                    "body_count": len(body),
                },
                metadata={"provider_mode": "scripted"},
            ),
        ],
    )


def _offline_profile_identifier(profile_url: str) -> str:
    normalized = str(profile_url or "").strip().rstrip("/")
    if "/in/" in normalized:
        return normalized.rsplit("/in/", 1)[-1].strip("/") or "offline-profile"
    return "offline-profile"


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
    payload_companies = [str(item).strip() for item in list(payload.get("companies") or []) if str(item).strip()]
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


def _apply_harvest_search_filters(
    payload: dict[str, Any], filter_hints: dict[str, list[str]], employment_status: str
) -> None:
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
            value.get("linkedinText") or value.get("text") or value.get("name") or value.get("full") or ""
        ).strip()
        country_code = str(
            value.get("countryCode") or parsed.get("countryCode") or parsed.get("country_code") or ""
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
