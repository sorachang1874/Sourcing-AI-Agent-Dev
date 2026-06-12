from __future__ import annotations

import json
import re
import time
import uuid
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

from .company_asset_writer import CompanyAssetWriter
from .company_registry import normalize_company_key, resolve_company_alias_key
from .search_provider import BaseSearchProvider, SearchResponse, SearchResultItem
from .storage import _json_safe_payload

DEFAULT_COMPANY_PUBLIC_WEB_SOURCE_FAMILIES: tuple[str, ...] = (
    "company_homepage",
    "company_blog",
    "company_research",
    "company_engineering",
    "company_news",
    "company_docs",
)

COMPANY_PUBLIC_WEB_ALLOWED_SOURCE_FAMILIES: tuple[str, ...] = (
    *DEFAULT_COMPANY_PUBLIC_WEB_SOURCE_FAMILIES,
    "company_rss",
    "company_arxiv",
    "company_openreview",
    "company_crawl",
)

COMPANY_PUBLIC_WEB_COLLECTION_MODES = {
    "seed_url_only",
    "provider_search",
    "collector_bundle",
}

COMPANY_PUBLIC_WEB_TERMINAL_STATUSES = {
    "completed",
    "completed_with_errors",
    "needs_review",
    "failed",
    "cancelled",
}


class CompanyPublicWebCollectorFetcher(Protocol):
    def fetch(self, url: str, *, timeout_seconds: float = 20.0) -> dict[str, Any]:
        ...


class UrllibCompanyPublicWebCollectorFetcher:
    def fetch(self, url: str, *, timeout_seconds: float = 20.0) -> dict[str, Any]:
        request = Request(url, headers={"User-Agent": "SourcingAIAgentCompanyPublicWebCollector/1.0"})
        with urlopen(request, timeout=max(1.0, float(timeout_seconds or 20.0))) as response:  # noqa: S310
            body = response.read()
            content_type = str(response.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        return {
            "url": url,
            "content": body.decode("utf-8", errors="replace"),
            "content_type": content_type,
        }


def refresh_company_public_web_assets(
    *,
    store: Any,
    runtime_dir: str | Path,
    payload: dict[str, Any] | None = None,
    search_provider: BaseSearchProvider | None = None,
    collector_fetcher: CompanyPublicWebCollectorFetcher | None = None,
) -> dict[str, Any]:
    request_payload = dict(payload or {})
    target_company = str(
        request_payload.get("target_company") or request_payload.get("company") or request_payload.get("company_name") or ""
    ).strip()
    if not target_company:
        return {"status": "invalid", "reason": "target_company is required"}
    company_key = str(request_payload.get("company_key") or "").strip() or resolve_company_alias_key(target_company)
    source_families = normalize_company_public_web_source_families(request_payload.get("source_families"))
    seed_urls = normalize_company_public_web_seed_urls(
        request_payload.get("seed_urls") or request_payload.get("urls"),
        target_company=target_company,
        company_key=company_key,
        source_families=source_families,
    )
    options = normalize_company_public_web_options(request_payload)
    collection_mode = str(options["collection_mode"])
    if collection_mode == "collector_bundle" and not (request_payload.get("seed_urls") or request_payload.get("urls")):
        seed_urls = []
    collector_inputs = normalize_company_public_web_collector_inputs(
        request_payload.get("collector_inputs") or request_payload.get("collectors") or options.get("collector_inputs")
    )
    collector_documents = normalize_company_public_web_collector_documents(
        request_payload.get("collector_documents") or request_payload.get("collector_payloads")
    )
    collector_sources = normalize_company_public_web_collector_sources(
        request_payload.get("collector_sources") or request_payload.get("collector_source_urls")
    )
    if bool(options.get("discover_collector_sources")):
        collector_sources = merge_company_public_web_collector_sources(
            collector_sources,
            discover_company_public_web_collector_sources(
                target_company=target_company,
                company_key=company_key,
                source_families=source_families,
                seed_urls=seed_urls,
                max_sources=int(options["max_discovered_collector_sources"]),
            ),
        )
    if collector_sources and collection_mode != "collector_bundle":
        return {
            "status": "invalid",
            "reason": "collector_sources require collection_mode=collector_bundle",
            "collection_mode": collection_mode,
            "target_company": target_company,
            "company_key": company_key,
        }
    if collector_documents:
        collector_inputs = merge_company_public_web_collector_inputs(
            collector_inputs,
            parse_company_public_web_collector_documents(collector_documents),
        )
    if collector_inputs:
        options["collector_input_hash"] = short_hash(
            json.dumps(_json_safe_payload(collector_inputs), sort_keys=True, ensure_ascii=False)
        )
        options["collector_input_counts"] = {
            collector_type: len(records)
            for collector_type, records in collector_inputs.items()
        }
    if collector_sources:
        options["collector_source_count"] = len(collector_sources)
        options["collector_source_hash"] = short_hash(
            json.dumps(_json_safe_payload(collector_sources), sort_keys=True, ensure_ascii=False)
        )
    if collection_mode == "provider_search" and search_provider is None:
        return {
            "status": "invalid",
            "reason": "provider_search collection_mode requires an explicit search_provider",
            "collection_mode": collection_mode,
            "target_company": target_company,
            "company_key": company_key,
        }
    force_refresh = bool(request_payload.get("force_refresh"))
    refresh_nonce = str(request_payload.get("refresh_nonce") or request_payload.get("nonce") or "").strip()
    if force_refresh and not refresh_nonce:
        refresh_nonce = f"force-{utc_compact_timestamp()}-{uuid.uuid4().hex[:12]}"
    idempotency_key = build_company_public_web_run_idempotency_key(
        target_company=target_company,
        company_key=company_key,
        source_families=source_families,
        seed_urls=seed_urls,
        options=options,
        force_refresh=force_refresh,
        nonce=refresh_nonce,
    )
    if not force_refresh:
        existing = store.get_company_public_web_asset_run(idempotency_key=idempotency_key)
        if existing is not None:
            assets = store.list_company_public_web_assets(company_key=company_key, limit=int(options["max_assets"]))
            return {
                "status": "joined",
                "run": existing,
                "assets": assets,
                "summary": dict(existing.get("summary") or {})
                or summarize_company_public_web_assets(assets, run=existing),
                "idempotency_key": idempotency_key,
            }
    defer_company_asset_sync = bool(request_payload.get("defer_company_asset_sync"))

    run_id = str(request_payload.get("run_id") or "").strip()
    if not run_id:
        run_id = f"company-public-web-run-{utc_compact_timestamp()}-{short_hash(idempotency_key)}"
    artifact_root = (
        Path(str(request_payload.get("artifact_root"))).expanduser()
        if str(request_payload.get("artifact_root") or "").strip()
        else Path(runtime_dir).expanduser() / "public_web" / "company_assets" / company_key / run_id
    )
    artifact_root.mkdir(parents=True, exist_ok=True)
    started_at = utc_sql_timestamp()
    initial_run = store.upsert_company_public_web_asset_run(
        {
            "run_id": run_id,
            "target_company": target_company,
            "company_key": company_key,
            "idempotency_key": idempotency_key,
            "status": "running",
            "phase": company_public_web_collection_phase(collection_mode),
            "source_families": source_families,
            "seed_urls": seed_urls,
            "options": options,
            "artifact_root": str(artifact_root),
            "requested_by": str(request_payload.get("requested_by") or request_payload.get("user_id") or "").strip(),
            "force_refresh": force_refresh,
            "started_at": started_at,
            "metadata": {
                "workflow_boundary": "api_cli_only_company_public_web_assets",
                "collection_mode": collection_mode,
                "default_workflow_stage": "not_enabled",
                "target_candidate_lane": "not_enabled",
                "raw_asset_policy": (
                    "Raw HTML/PDF/search payloads are internal collection inputs and are excluded "
                    "from default model/export surfaces."
                ),
            },
        }
    )
    seed_assets = build_company_public_web_seed_assets(
        target_company=target_company,
        company_key=company_key,
        source_families=source_families,
        seed_urls=seed_urls,
        run_id=run_id,
        max_assets=int(options["max_assets"]),
    )
    try:
        effective_collector_inputs = collector_inputs
        collector_source_metrics = build_company_public_web_collector_source_metrics(
            collector_sources=collector_sources,
            fetched_documents=[],
        )
        if collector_sources:
            fetcher = collector_fetcher or UrllibCompanyPublicWebCollectorFetcher()
            fetched_documents = fetch_company_public_web_collector_documents(
                collector_sources=collector_sources,
                fetcher=fetcher,
                timeout_seconds=float(options["collector_fetch_timeout_seconds"]),
                max_documents=int(options["max_collector_documents"]),
            )
            collector_source_metrics = build_company_public_web_collector_source_metrics(
                collector_sources=collector_sources,
                fetched_documents=fetched_documents,
            )
            effective_collector_inputs = merge_company_public_web_collector_inputs(
                effective_collector_inputs,
                parse_company_public_web_collector_documents(fetched_documents),
            )
        search_collection = collect_company_public_web_provider_assets(
            target_company=target_company,
            company_key=company_key,
            source_families=source_families,
            run_id=run_id,
            options=options,
            search_provider=search_provider,
        )
        discovered_assets = merge_company_public_web_discovered_assets(
            [
                *seed_assets,
                *search_collection["assets"],
                *build_company_public_web_collector_assets(
                    target_company=target_company,
                    company_key=company_key,
                    run_id=run_id,
                    collector_inputs=effective_collector_inputs,
                    max_assets=int(options["max_assets"]),
                ),
            ],
            max_assets=int(options["max_assets"]),
        )
        persisted_assets = [
            store.upsert_company_public_web_asset(asset_payload)
            for asset_payload in discovered_assets
        ]
        company_asset_sync = (
            {
                "status": "deferred",
                "reason": "company_asset_sync_deferred_to_typed_command",
                "synced_asset_count": 0,
                "source_asset_count": len(persisted_assets),
            }
            if defer_company_asset_sync
            else sync_company_public_web_assets_to_company_asset_layer(
                store=store,
                run=initial_run,
                assets=persisted_assets,
            )
        )
        artifact_paths = write_company_public_web_asset_artifacts(
            artifact_root=artifact_root,
            run=initial_run,
            assets=persisted_assets,
            request_payload=request_payload,
            query_manifest=search_collection["query_manifest"],
            search_results=search_collection["search_results"],
            collector_manifest=build_company_public_web_collector_manifest(effective_collector_inputs),
            collector_source_metrics=collector_source_metrics,
        )
    except Exception as exc:
        failed_run = store.upsert_company_public_web_asset_run(
            {
                **initial_run,
                "status": "failed",
                "phase": "failed",
                "last_error": str(exc),
                "completed_at": utc_sql_timestamp(),
                "metadata": {
                    **dict(initial_run.get("metadata") or {}),
                    "failure_class": exc.__class__.__name__,
                },
            }
        )
        return {
            "status": "failed",
            "reason": str(exc),
            "run": failed_run,
            "assets": [],
            "summary": summarize_company_public_web_assets([], run=failed_run),
            "idempotency_key": idempotency_key,
            "artifact_paths": {},
            "company_asset_sync": {"status": "not_attempted", "reason": "refresh_failed"},
        }
    summary = summarize_company_public_web_assets(
        persisted_assets,
        run=initial_run,
        query_manifest=search_collection["query_manifest"],
        search_results=search_collection["search_results"],
        collector_manifest=build_company_public_web_collector_manifest(effective_collector_inputs),
        collector_source_metrics=collector_source_metrics,
    )
    completed_run = store.upsert_company_public_web_asset_run(
        {
            **initial_run,
            "status": "completed",
            "phase": "completed",
            "discovered_assets": [_company_public_web_asset_run_projection(asset) for asset in persisted_assets],
            "summary": summary,
            "completed_at": utc_sql_timestamp(),
            "artifact_root": str(artifact_root),
            "metadata": {
                **dict(initial_run.get("metadata") or {}),
                "artifact_paths": artifact_paths,
                "model_safe_asset_count": len(persisted_assets),
                "provider_names": summary["provider_names"],
                "provider_result_count": summary["provider_result_count"],
                "company_asset_sync": company_asset_sync,
            },
        }
    )
    return {
        "status": "completed",
        "run": completed_run,
        "assets": persisted_assets,
        "summary": summary,
        "idempotency_key": idempotency_key,
        "artifact_paths": artifact_paths,
        "company_asset_sync": company_asset_sync,
    }


def list_company_public_web_assets(
    *,
    store: Any,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    query = dict(payload or {})
    target_company = str(query.get("target_company") or query.get("company") or "").strip()
    company_key = str(query.get("company_key") or "").strip() or (
        resolve_company_alias_key(target_company) if target_company else ""
    )
    status = str(query.get("status") or "").strip()
    source_family = str(query.get("source_family") or "").strip()
    limit = max(1, min(int(query.get("limit") or 100), 1000))
    runs = store.list_company_public_web_asset_runs(
        target_company=target_company,
        company_key=company_key,
        status=status,
        limit=limit,
    )
    assets = store.list_company_public_web_assets(
        target_company=target_company,
        company_key=company_key,
        source_family=source_family,
        limit=limit,
    )
    return {
        "status": "ok",
        "target_company": target_company,
        "company_key": company_key,
        "runs": runs,
        "assets": assets,
        "summary": summarize_company_public_web_assets(assets, run=runs[0] if runs else {}),
        "product_boundary": {
            "default_workflow_stage": "not_enabled",
            "target_candidate_lane": "separate",
            "surface": "api_cli_only",
        },
    }


def sync_company_public_web_assets_to_company_asset_layer(
    *,
    store: Any,
    run: dict[str, Any],
    assets: list[dict[str, Any]],
) -> dict[str, Any]:
    """Project model-safe company Public Web rows into canonical company assets."""

    normalized_assets = [dict(asset or {}) for asset in list(assets or []) if dict(asset or {}).get("asset_id")]
    if not normalized_assets:
        return {"status": "skipped", "reason": "no_company_public_web_assets", "synced_asset_count": 0}

    writer = CompanyAssetWriter(store)
    synced_asset_ids: list[str] = []
    synced_evidence_ids: list[str] = []
    try:
        for asset in normalized_assets:
            source_asset_id = str(asset.get("asset_id") or "").strip()
            source_family = str(asset.get("source_family") or "").strip() or "company_public_web_asset"
            canonical_asset_id = f"company-asset-{short_hash(source_asset_id)}"
            evidence_id = f"company-evidence-{short_hash(source_asset_id)}"
            url = str(asset.get("url") or "").strip()
            title = str(asset.get("title") or "").strip()
            summary = str(asset.get("summary") or "").strip()
            metadata = {
                "source": "company_public_web_assets",
                "source_public_web_asset_id": source_asset_id,
                "source_family": source_family,
                "source_asset_kind": str(asset.get("asset_kind") or "").strip(),
                "source_run_ids": list(asset.get("source_run_ids") or []),
                "model_safe_payload": dict(asset.get("model_safe_payload") or {}),
            }
            canonical_asset = writer.record_asset(
                {
                    "asset_id": canonical_asset_id,
                    "workspace_id": "default",
                    "company_key": str(asset.get("company_key") or run.get("company_key") or "").strip(),
                    "target_company": str(asset.get("target_company") or run.get("target_company") or "").strip(),
                    "asset_type": source_family,
                    "source_kind": "company_public_web_model_safe",
                    "source_run_id": str(asset.get("latest_run_id") or run.get("run_id") or "").strip(),
                    "content_ref": url,
                    "source_url": url,
                    "visibility_scope": "public_summary",
                    "status": "available" if str(asset.get("status") or "active").strip() == "active" else "observed",
                    "metadata": metadata,
                }
            )
            evidence = writer.record_evidence(
                {
                    "evidence_id": evidence_id,
                    "workspace_id": "default",
                    "company_key": str(asset.get("company_key") or run.get("company_key") or "").strip(),
                    "target_company": str(asset.get("target_company") or run.get("target_company") or "").strip(),
                    "asset_id": canonical_asset_id,
                    "evidence_type": "company_public_web_asset",
                    "value": summary or title or url,
                    "normalized_value": summary or title or url,
                    "source_url": url,
                    "source_domain": _public_web_domain(url),
                    "evidence_excerpt": summary,
                    "artifact_refs": dict(asset.get("artifact_refs") or {}),
                    "status": "observed",
                    "metadata": metadata,
                }
            )
            synced_asset_ids.append(str(canonical_asset.get("asset_id") or canonical_asset_id))
            synced_evidence_ids.append(str(evidence.get("evidence_id") or evidence_id))
    except RuntimeError as exc:
        if "PG-only durable runtime storage" in str(exc):
            return {
                "status": "skipped",
                "reason": "postgres_required_for_company_asset_layer",
                "synced_asset_count": 0,
                "source_asset_count": len(normalized_assets),
            }
        raise

    return {
        "status": "synced",
        "source": "company_public_web_assets",
        "owner": "CompanyAssetWriter",
        "synced_asset_count": len(synced_asset_ids),
        "synced_evidence_count": len(synced_evidence_ids),
        "company_asset_ids": synced_asset_ids,
        "company_evidence_ids": synced_evidence_ids,
    }


def _public_web_domain(url: str) -> str:
    try:
        return str(urlparse(str(url or "")).netloc or "").lower()
    except ValueError:
        return ""


def normalize_company_public_web_source_families(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = [item.strip() for item in value.split(",")]
    else:
        raw_items = list(value or []) if isinstance(value, (list, tuple, set)) else []
    if not raw_items:
        raw_items = list(DEFAULT_COMPANY_PUBLIC_WEB_SOURCE_FAMILIES)
    allowed = set(COMPANY_PUBLIC_WEB_ALLOWED_SOURCE_FAMILIES)
    seen: set[str] = set()
    result: list[str] = []
    for raw_item in raw_items:
        item = str(raw_item or "").strip().lower()
        if not item or item not in allowed or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result or list(DEFAULT_COMPANY_PUBLIC_WEB_SOURCE_FAMILIES)


def normalize_company_public_web_seed_urls(
    value: Any,
    *,
    target_company: str,
    company_key: str,
    source_families: list[str],
) -> list[str]:
    if isinstance(value, str):
        raw_items = [item.strip() for item in value.split(",")]
    else:
        raw_items = list(value or []) if isinstance(value, (list, tuple, set)) else []
    urls: list[str] = []
    seen: set[str] = set()
    for raw_item in raw_items:
        url = normalize_public_web_url(str(raw_item or ""))
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    if urls:
        return urls
    return default_company_public_web_seed_urls(
        target_company=target_company,
        company_key=company_key,
        source_families=source_families,
    )


def normalize_company_public_web_options(payload: dict[str, Any]) -> dict[str, Any]:
    raw_options = dict(payload.get("options") or {})
    max_assets = _bounded_int(raw_options.get("max_assets") or payload.get("max_assets"), default=50, low=1, high=500)
    collection_mode = str(raw_options.get("collection_mode") or payload.get("collection_mode") or "").strip().lower()
    legacy_search_provider_mode = str(raw_options.get("search_provider") or payload.get("search_provider") or "").strip().lower()
    if not collection_mode and legacy_search_provider_mode in COMPANY_PUBLIC_WEB_COLLECTION_MODES:
        collection_mode = legacy_search_provider_mode
    if not collection_mode:
        collection_mode = "seed_url_only"
    if collection_mode not in COMPANY_PUBLIC_WEB_COLLECTION_MODES:
        collection_mode = "seed_url_only"
    return {
        "max_assets": max_assets,
        "collection_mode": collection_mode,
        "max_queries": _bounded_int(raw_options.get("max_queries") or payload.get("max_queries"), default=6, low=1, high=50),
        "max_results_per_query": _bounded_int(
            raw_options.get("max_results_per_query") or payload.get("max_results_per_query"),
            default=10,
            low=1,
            high=50,
        ),
        "fetch_content": _truthy(raw_options.get("fetch_content", False)),
        "search_provider": str(raw_options.get("search_provider") or payload.get("search_provider") or "").strip(),
        "max_collector_documents": _bounded_int(
            raw_options.get("max_collector_documents") or payload.get("max_collector_documents"),
            default=20,
            low=1,
            high=100,
        ),
        "discover_collector_sources": _truthy(
            raw_options.get("discover_collector_sources") or payload.get("discover_collector_sources")
        ),
        "max_discovered_collector_sources": _bounded_int(
            raw_options.get("max_discovered_collector_sources") or payload.get("max_discovered_collector_sources"),
            default=12,
            low=1,
            high=50,
        ),
        "collector_fetch_timeout_seconds": float(
            _bounded_int(
                raw_options.get("collector_fetch_timeout_seconds") or payload.get("collector_fetch_timeout_seconds"),
                default=20,
                low=1,
                high=120,
            )
        ),
        "raw_assets_included_by_default": False,
    }


def build_company_public_web_seed_assets(
    *,
    target_company: str,
    company_key: str,
    source_families: list[str],
    seed_urls: list[str],
    run_id: str,
    max_assets: int,
) -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    family_by_url = _source_family_by_seed_url(seed_urls=seed_urls, source_families=source_families)
    for url in seed_urls:
        normalized_url_key = normalize_public_web_url_key(url)
        if not normalized_url_key or normalized_url_key in seen_keys:
            continue
        seen_keys.add(normalized_url_key)
        source_family = family_by_url.get(url) or infer_company_public_web_source_family(url)
        title = company_public_web_asset_title(target_company=target_company, source_family=source_family, url=url)
        assets.append(
            {
                "company_key": company_key,
                "target_company": target_company,
                "latest_run_id": run_id,
                "source_family": source_family,
                "asset_kind": "seed_url",
                "title": title,
                "url": url,
                "normalized_url_key": normalized_url_key,
                "summary": f"Model-safe company-level public web seed for {target_company}: {title}.",
                "model_safe_payload": {
                    "target_company": target_company,
                    "company_key": company_key,
                    "source_family": source_family,
                    "title": title,
                    "url": url,
                    "raw_content_included": False,
                },
                "source_run_ids": [run_id],
                "artifact_refs": {},
                "status": "active",
                "metadata": {
                    "collection_mode": "seed_url_only",
                    "default_workflow_stage": "not_enabled",
                    "target_candidate_record_id": "",
                    "raw_content_included": False,
                },
            }
        )
        if len(assets) >= max(1, int(max_assets or 1)):
            break
    return assets


def collect_company_public_web_provider_assets(
    *,
    target_company: str,
    company_key: str,
    source_families: list[str],
    run_id: str,
    options: dict[str, Any],
    search_provider: BaseSearchProvider | None,
) -> dict[str, Any]:
    if str(options.get("collection_mode") or "seed_url_only") != "provider_search":
        return {"assets": [], "query_manifest": [], "search_results": []}
    if search_provider is None:
        raise ValueError("provider_search collection_mode requires an explicit search_provider")
    query_manifest = build_company_public_web_query_manifest(
        target_company=target_company,
        company_key=company_key,
        source_families=source_families,
        max_queries=int(options["max_queries"]),
        max_results_per_query=int(options["max_results_per_query"]),
    )
    search_results: list[dict[str, Any]] = []
    assets: list[dict[str, Any]] = []
    for query_spec in query_manifest:
        query_text = str(query_spec.get("query_text") or "").strip()
        if not query_text:
            continue
        response = search_provider.search(
            query_text,
            max_results=int(options["max_results_per_query"]),
        )
        result_record = company_public_web_search_response_record(
            response=response,
            source_family=str(query_spec.get("source_family") or ""),
        )
        search_results.append(result_record)
        assets.extend(
            build_company_public_web_provider_result_assets(
                target_company=target_company,
                company_key=company_key,
                source_family=str(query_spec.get("source_family") or ""),
                run_id=run_id,
                response=response,
                max_results=int(options["max_results_per_query"]),
            )
        )
    return {"assets": assets, "query_manifest": query_manifest, "search_results": search_results}


def normalize_company_public_web_collector_inputs(value: Any) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(value, dict):
        return {}
    aliases = {
        "rss": "rss_items",
        "rss_feeds": "rss_items",
        "rss_items": "rss_items",
        "arxiv": "arxiv_publications",
        "arxiv_records": "arxiv_publications",
        "arxiv_publications": "arxiv_publications",
        "openreview": "openreview_publications",
        "openreview_records": "openreview_publications",
        "openreview_publications": "openreview_publications",
        "crawl": "crawled_pages",
        "crawled_pages": "crawled_pages",
        "pages": "crawled_pages",
    }
    normalized: dict[str, list[dict[str, Any]]] = {}
    for raw_key, raw_records in value.items():
        collector_type = aliases.get(str(raw_key or "").strip().lower())
        if not collector_type:
            continue
        records = list(raw_records or []) if isinstance(raw_records, (list, tuple)) else []
        for raw_record in records:
            if not isinstance(raw_record, dict):
                continue
            record = sanitize_company_public_web_collector_record(raw_record)
            if record:
                normalized.setdefault(collector_type, []).append(record)
    return {collector_type: records for collector_type, records in normalized.items() if records}


def merge_company_public_web_collector_inputs(
    *inputs: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    merged: dict[str, list[dict[str, Any]]] = {}
    seen: set[tuple[str, str]] = set()
    for input_payload in inputs:
        for collector_type, records in dict(input_payload or {}).items():
            for record in list(records or []):
                dedupe_key = (
                    str(collector_type or ""),
                    normalize_public_web_url(str(record.get("url") or record.get("source_url") or ""))
                    or short_hash(json.dumps(_json_safe_payload(record), sort_keys=True, ensure_ascii=False)),
                )
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                merged.setdefault(collector_type, []).append(record)
    return merged


def normalize_company_public_web_collector_documents(value: Any) -> list[dict[str, Any]]:
    documents = list(value or []) if isinstance(value, (list, tuple)) else []
    normalized: list[dict[str, Any]] = []
    for raw_document in documents:
        if not isinstance(raw_document, dict):
            continue
        url = normalize_public_web_url(str(raw_document.get("url") or raw_document.get("source_url") or ""))
        content = str(raw_document.get("content") or raw_document.get("text") or raw_document.get("body") or "")
        if not url or not content.strip():
            continue
        normalized.append(
            {
                "url": url,
                "content": content,
                "content_type": str(raw_document.get("content_type") or raw_document.get("mime_type") or "").strip().lower(),
                "collector_type": str(raw_document.get("collector_type") or "").strip().lower(),
            }
        )
    return normalized


def normalize_company_public_web_collector_sources(value: Any) -> list[dict[str, Any]]:
    raw_sources = list(value or []) if isinstance(value, (list, tuple)) else []
    sources: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_source in raw_sources:
        if isinstance(raw_source, str):
            source_payload = {"url": raw_source}
        elif isinstance(raw_source, dict):
            source_payload = dict(raw_source)
        else:
            continue
        url = normalize_public_web_url(str(source_payload.get("url") or source_payload.get("source_url") or ""))
        if not url or url in seen:
            continue
        seen.add(url)
        sources.append(
            {
                "url": url,
                "collector_type": str(source_payload.get("collector_type") or "").strip().lower(),
                "content_type": str(source_payload.get("content_type") or "").strip().lower(),
            }
        )
    return sources


def merge_company_public_web_collector_sources(
    *source_lists: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source_list in source_lists:
        for source in normalize_company_public_web_collector_sources(source_list):
            url = str(source.get("url") or "")
            if not url or url in seen:
                continue
            seen.add(url)
            merged.append(source)
    return merged


def discover_company_public_web_collector_sources(
    *,
    target_company: str,
    company_key: str,
    source_families: list[str],
    seed_urls: list[str],
    max_sources: int,
) -> list[dict[str, Any]]:
    domain = company_key_to_domain(company_key or normalize_company_key(target_company))
    families = set(source_families or [])
    sources: list[dict[str, Any]] = []
    if "company_rss" in families:
        for path in ("/feed", "/rss.xml", "/blog/rss.xml", "/research/rss.xml", "/engineering/rss.xml"):
            sources.append({"url": f"https://{domain}{path}", "collector_type": "rss_items"})
    if "company_arxiv" in families:
        sources.append(
            {
                "url": (
                    "https://export.arxiv.org/api/query?"
                    f"search_query=all:{target_company.replace(' ', '+')}&start=0&max_results=25"
                ),
                "collector_type": "arxiv_publications",
            }
        )
    if "company_openreview" in families:
        sources.append(
            {
                "url": f"https://api2.openreview.net/notes?term={target_company.replace(' ', '+')}&limit=25",
                "collector_type": "openreview_publications",
            }
        )
    if "company_crawl" in families:
        for url in seed_urls:
            sources.append({"url": url, "collector_type": "crawled_pages"})
    return merge_company_public_web_collector_sources(sources)[: max(1, int(max_sources or 1))]


def fetch_company_public_web_collector_documents(
    *,
    collector_sources: list[dict[str, Any]],
    fetcher: CompanyPublicWebCollectorFetcher,
    timeout_seconds: float,
    max_documents: int,
) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for source in list(collector_sources or [])[: max(1, int(max_documents or 1))]:
        url = normalize_public_web_url(str(source.get("url") or ""))
        if not url:
            continue
        started_at = time.perf_counter()
        fetched = fetcher.fetch(url, timeout_seconds=timeout_seconds)
        fetch_duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        if not isinstance(fetched, dict):
            raise ValueError(f"collector fetcher returned non-object payload for {url}")
        content = str(fetched.get("content") or fetched.get("text") or fetched.get("body") or "")
        if not content.strip():
            raise ValueError(f"collector source returned empty content: {url}")
        documents.append(
            {
                "url": normalize_public_web_url(str(fetched.get("url") or url)),
                "content": content,
                "content_type": str(
                    fetched.get("content_type") or fetched.get("mime_type") or source.get("content_type") or ""
                ).strip().lower(),
                "collector_type": str(fetched.get("collector_type") or source.get("collector_type") or "").strip().lower(),
                "fetch_duration_ms": fetch_duration_ms,
            }
        )
    return documents


def build_company_public_web_collector_source_metrics(
    *,
    collector_sources: list[dict[str, Any]],
    fetched_documents: list[dict[str, Any]],
) -> dict[str, Any]:
    durations = [_coerce_float(document.get("fetch_duration_ms")) for document in list(fetched_documents or [])]
    durations = [duration for duration in durations if duration >= 0]
    source_type_counts: dict[str, int] = {}
    for source in list(collector_sources or []):
        collector_type = str(dict(source or {}).get("collector_type") or "inferred").strip() or "inferred"
        source_type_counts[collector_type] = source_type_counts.get(collector_type, 0) + 1
    return {
        "collector_source_count": len(list(collector_sources or [])),
        "collector_document_fetch_count": len(list(fetched_documents or [])),
        "collector_document_fetch_failure_count": max(
            0,
            len(list(collector_sources or [])) - len(list(fetched_documents or [])),
        ),
        "collector_source_type_counts": source_type_counts,
        "collector_fetch_duration_ms_total": round(sum(durations), 2),
        "collector_fetch_duration_ms_max": round(max(durations), 2) if durations else 0.0,
    }


def parse_company_public_web_collector_documents(documents: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    collector_inputs: dict[str, list[dict[str, Any]]] = {}
    for document in list(documents or []):
        url = normalize_public_web_url(str(document.get("url") or ""))
        content = str(document.get("content") or "")
        collector_type = infer_company_public_web_collector_type(
            url=url,
            content_type=str(document.get("content_type") or ""),
            declared_type=str(document.get("collector_type") or ""),
            content=content,
        )
        if collector_type == "rss_items":
            records = parse_company_public_web_rss_document(content, source_url=url)
        elif collector_type == "arxiv_publications":
            records = parse_company_public_web_atom_publication_document(content, source_url=url)
        elif collector_type == "openreview_publications":
            records = parse_company_public_web_openreview_document(content, source_url=url)
        else:
            records = [parse_company_public_web_crawled_page_document(content, source_url=url)]
        for record in records:
            sanitized = sanitize_company_public_web_collector_record(record)
            if sanitized:
                collector_inputs.setdefault(collector_type, []).append(sanitized)
    return collector_inputs


def infer_company_public_web_collector_type(
    *,
    url: str,
    content_type: str = "",
    declared_type: str = "",
    content: str = "",
) -> str:
    declared = declared_type.strip().lower()
    if declared in {"rss", "rss_items", "rss_feeds"}:
        return "rss_items"
    if declared in {"arxiv", "arxiv_publications", "arxiv_records"}:
        return "arxiv_publications"
    if declared in {"openreview", "openreview_publications", "openreview_records"}:
        return "openreview_publications"
    if declared in {"crawl", "crawled_pages", "pages"}:
        return "crawled_pages"
    host = urlparse(url).netloc.lower()
    path = urlparse(url).path.lower()
    content_prefix = content.lstrip()[:200].lower()
    if "arxiv.org" in host:
        return "arxiv_publications"
    if "openreview.net" in host:
        return "openreview_publications"
    if "xml" in content_type or path.endswith((".rss", ".xml")) or "/rss" in path or "/feed" in path:
        if "<entry" in content_prefix and "arxiv" in content_prefix:
            return "arxiv_publications"
        return "rss_items"
    if content_prefix.startswith("{") and "openreview" in content.lower():
        return "openreview_publications"
    return "crawled_pages"


def parse_company_public_web_rss_document(content: str, *, source_url: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []
    records: list[dict[str, Any]] = []
    for item in root.findall(".//item"):
        title = _xml_child_text(item, "title")
        url = _xml_child_text(item, "link")
        if not title or not url:
            continue
        records.append(
            {
                "title": title,
                "url": url,
                "summary": strip_markup(_xml_child_text(item, "description")),
                "published_at": _xml_child_text(item, "pubDate"),
                "source_url": source_url,
            }
        )
    if records:
        return records
    for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        title = _xml_child_text(entry, "{http://www.w3.org/2005/Atom}title")
        url = ""
        link = entry.find("{http://www.w3.org/2005/Atom}link")
        if link is not None:
            url = str(link.attrib.get("href") or "").strip()
        if not title or not url:
            continue
        records.append(
            {
                "title": title,
                "url": url,
                "summary": strip_markup(_xml_child_text(entry, "{http://www.w3.org/2005/Atom}summary")),
                "published_at": _xml_child_text(entry, "{http://www.w3.org/2005/Atom}published")
                or _xml_child_text(entry, "{http://www.w3.org/2005/Atom}updated"),
                "source_url": source_url,
            }
        )
    return records


def parse_company_public_web_atom_publication_document(content: str, *, source_url: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return []
    records: list[dict[str, Any]] = []
    for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
        title = _xml_child_text(entry, "{http://www.w3.org/2005/Atom}title")
        url = _xml_child_text(entry, "{http://www.w3.org/2005/Atom}id")
        authors = [
            _xml_child_text(author, "{http://www.w3.org/2005/Atom}name")
            for author in entry.findall("{http://www.w3.org/2005/Atom}author")
        ]
        authors = [author for author in authors if author]
        if not title or not url:
            continue
        records.append(
            {
                "title": " ".join(title.split()),
                "url": url,
                "summary": " ".join(strip_markup(_xml_child_text(entry, "{http://www.w3.org/2005/Atom}summary")).split()),
                "published_at": _xml_child_text(entry, "{http://www.w3.org/2005/Atom}published"),
                "authors": authors,
                "source_url": source_url,
            }
        )
    return records


def parse_company_public_web_openreview_document(content: str, *, source_url: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        payload = {}
    raw_notes = payload.get("notes") if isinstance(payload, dict) else None
    notes = list(raw_notes or []) if isinstance(raw_notes, list) else []
    records: list[dict[str, Any]] = []
    for note in notes:
        if not isinstance(note, dict):
            continue
        note_content = dict(note.get("content") or {})
        title = _openreview_content_value(note_content.get("title"))
        abstract = _openreview_content_value(note_content.get("abstract"))
        authors_value = note_content.get("authors")
        authors = []
        if isinstance(authors_value, dict):
            authors_value = authors_value.get("value")
        if isinstance(authors_value, list):
            authors = [str(item or "").strip() for item in authors_value if str(item or "").strip()]
        note_id = str(note.get("id") or note.get("forum") or "").strip()
        if not title or not note_id:
            continue
        records.append(
            {
                "title": title,
                "url": f"https://openreview.net/forum?id={note_id}",
                "summary": abstract,
                "authors": authors,
                "source_url": source_url,
            }
        )
    return records


def parse_company_public_web_crawled_page_document(content: str, *, source_url: str) -> dict[str, Any]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", content, flags=re.IGNORECASE | re.DOTALL)
    description_match = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        content,
        flags=re.IGNORECASE | re.DOTALL,
    )
    title = strip_markup(title_match.group(1)) if title_match else source_url
    summary = strip_markup(description_match.group(1)) if description_match else strip_markup(content[:500])
    return {
        "title": " ".join(title.split()) or source_url,
        "url": source_url,
        "summary": " ".join(summary.split())[:500],
        "source_url": source_url,
    }


def sanitize_company_public_web_collector_record(record: dict[str, Any]) -> dict[str, Any]:
    url = normalize_public_web_url(
        str(record.get("url") or record.get("link") or record.get("entry_url") or record.get("source_url") or "")
    )
    title = str(record.get("title") or record.get("name") or "").strip()
    summary = str(record.get("summary") or record.get("snippet") or record.get("abstract") or "").strip()
    if not url and not title:
        return {}
    sanitized = {
        "title": title or url,
        "url": url,
        "summary": summary,
        "published_at": str(record.get("published_at") or record.get("published") or record.get("updated_at") or "").strip(),
        "authors": [str(item or "").strip() for item in list(record.get("authors") or []) if str(item or "").strip()]
        if isinstance(record.get("authors"), (list, tuple))
        else [],
        "source_url": normalize_public_web_url(str(record.get("source_url") or record.get("feed_url") or "")),
        "source_family": str(record.get("source_family") or "").strip().lower(),
    }
    metadata = sanitize_company_public_web_provider_metadata(record.get("metadata") or {})
    if metadata:
        sanitized["metadata"] = metadata
    return {key: value for key, value in sanitized.items() if value not in ("", [], {})}


def build_company_public_web_collector_assets(
    *,
    target_company: str,
    company_key: str,
    run_id: str,
    collector_inputs: dict[str, list[dict[str, Any]]],
    max_assets: int,
) -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []
    family_by_collector = {
        "rss_items": "company_rss",
        "arxiv_publications": "company_arxiv",
        "openreview_publications": "company_openreview",
        "crawled_pages": "company_crawl",
    }
    kind_by_collector = {
        "rss_items": "rss_item",
        "arxiv_publications": "arxiv_publication",
        "openreview_publications": "openreview_publication",
        "crawled_pages": "crawled_page",
    }
    for collector_type, records in sorted(collector_inputs.items()):
        for index, record in enumerate(records, start=1):
            url = normalize_public_web_url(str(record.get("url") or record.get("source_url") or ""))
            normalized_url_key = normalize_public_web_url_key(url) if url else short_hash(
                f"{company_key}|{collector_type}|{record.get('title')}|{index}"
            )
            if not normalized_url_key:
                continue
            source_family = str(record.get("source_family") or "").strip().lower()
            if source_family not in COMPANY_PUBLIC_WEB_ALLOWED_SOURCE_FAMILIES:
                source_family = family_by_collector.get(collector_type, "company_crawl")
            title = str(record.get("title") or "").strip() or company_public_web_asset_title(
                target_company=target_company,
                source_family=source_family,
                url=url,
            )
            summary = str(record.get("summary") or "").strip()
            model_safe_payload = {
                "target_company": target_company,
                "company_key": company_key,
                "source_family": source_family,
                "collector_type": collector_type,
                "title": title,
                "url": url,
                "summary": summary,
                "published_at": str(record.get("published_at") or ""),
                "authors": list(record.get("authors") or []),
                "source_url": normalize_public_web_url(str(record.get("source_url") or "")),
                "raw_content_included": False,
            }
            assets.append(
                {
                    "company_key": company_key,
                    "target_company": target_company,
                    "latest_run_id": run_id,
                    "source_family": source_family,
                    "asset_kind": kind_by_collector.get(collector_type, "collector_record"),
                    "title": title,
                    "url": url,
                    "normalized_url_key": normalized_url_key,
                    "summary": summary or f"Model-safe {collector_type.replace('_', ' ')} record for {target_company}: {title}.",
                    "model_safe_payload": {key: value for key, value in model_safe_payload.items() if value not in ("", [], {})},
                    "source_run_ids": [run_id],
                    "artifact_refs": {},
                    "status": "active",
                    "metadata": {
                        "collection_mode": "collector_bundle",
                        "collector_type": collector_type,
                        "source_family": source_family,
                        "raw_content_included": False,
                        "default_workflow_stage": "not_enabled",
                        "target_candidate_record_id": "",
                    },
                }
            )
            if len(assets) >= max(1, int(max_assets or 1)):
                return assets
    return assets


def build_company_public_web_collector_manifest(
    collector_inputs: dict[str, list[dict[str, Any]]] | None,
) -> list[dict[str, Any]]:
    manifest: list[dict[str, Any]] = []
    for collector_type, records in sorted(dict(collector_inputs or {}).items()):
        manifest.append(
            {
                "collector_type": collector_type,
                "record_count": len(list(records or [])),
                "raw_content_included": False,
            }
        )
    return manifest


def build_company_public_web_query_manifest(
    *,
    target_company: str,
    company_key: str,
    source_families: list[str],
    max_queries: int,
    max_results_per_query: int,
) -> list[dict[str, Any]]:
    domain = company_key_to_domain(company_key or normalize_company_key(target_company))
    query_by_family = {
        "company_homepage": f'{target_company} official website',
        "company_blog": f'{target_company} blog site:{domain}',
        "company_research": f'{target_company} research publications',
        "company_engineering": f'{target_company} engineering blog',
        "company_news": f'{target_company} company news',
        "company_docs": f'{target_company} documentation developers',
    }
    manifest: list[dict[str, Any]] = []
    seen: set[str] = set()
    for family in source_families:
        query_text = str(query_by_family.get(family) or f"{target_company} {family.replace('_', ' ')}").strip()
        dedupe_key = query_text.lower()
        if not query_text or dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        manifest.append(
            {
                "query_id": f"company-public-web-query-{short_hash(company_key + '|' + family + '|' + query_text)}",
                "source_family": family,
                "query_text": query_text,
                "max_results": max(1, int(max_results_per_query or 1)),
                "raw_content_included": False,
            }
        )
        if len(manifest) >= max(1, int(max_queries or 1)):
            break
    return manifest


def build_company_public_web_provider_result_assets(
    *,
    target_company: str,
    company_key: str,
    source_family: str,
    run_id: str,
    response: SearchResponse,
    max_results: int,
) -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []
    for rank, item in enumerate(list(response.results or [])[: max(1, int(max_results or 1))], start=1):
        asset = company_public_web_provider_result_asset(
            target_company=target_company,
            company_key=company_key,
            source_family=source_family,
            run_id=run_id,
            response=response,
            item=item,
            result_rank=rank,
        )
        if asset:
            assets.append(asset)
    return assets


def company_public_web_provider_result_asset(
    *,
    target_company: str,
    company_key: str,
    source_family: str,
    run_id: str,
    response: SearchResponse,
    item: SearchResultItem,
    result_rank: int,
) -> dict[str, Any] | None:
    url = normalize_public_web_url(item.url)
    normalized_url_key = normalize_public_web_url_key(url)
    if not url or not normalized_url_key:
        return None
    title = str(item.title or "").strip() or company_public_web_asset_title(
        target_company=target_company,
        source_family=source_family,
        url=url,
    )
    snippet = str(item.snippet or "").strip()
    provider_name = str(response.provider_name or "").strip()
    return {
        "company_key": company_key,
        "target_company": target_company,
        "latest_run_id": run_id,
        "source_family": source_family or infer_company_public_web_source_family(url),
        "asset_kind": "search_result",
        "title": title,
        "url": url,
        "normalized_url_key": normalized_url_key,
        "summary": snippet or f"Model-safe company-level public web search result for {target_company}: {title}.",
        "model_safe_payload": {
            "target_company": target_company,
            "company_key": company_key,
            "source_family": source_family,
            "title": title,
            "url": url,
            "snippet": snippet,
            "provider_name": provider_name,
            "query_text": str(response.query_text or "").strip(),
            "result_rank": max(1, int(result_rank or 1)),
            "raw_content_included": False,
        },
        "source_run_ids": [run_id],
        "artifact_refs": {},
        "status": "active",
        "metadata": {
            "collection_mode": "provider_search",
            "provider_name": provider_name,
            "query_text": str(response.query_text or "").strip(),
            "result_rank": max(1, int(result_rank or 1)),
            "source_family": source_family,
            "raw_content_included": False,
            "default_workflow_stage": "not_enabled",
            "target_candidate_record_id": "",
            "provider_metadata": sanitize_company_public_web_provider_metadata(item.metadata),
        },
    }


def merge_company_public_web_discovered_assets(
    assets: list[dict[str, Any]],
    *,
    max_assets: int,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for asset in assets:
        key = str(dict(asset or {}).get("normalized_url_key") or "").strip()
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        merged.append(asset)
        if len(merged) >= max(1, int(max_assets or 1)):
            break
    return merged


def company_public_web_search_response_record(
    *,
    response: SearchResponse,
    source_family: str,
) -> dict[str, Any]:
    return {
        "provider_name": str(response.provider_name or ""),
        "query_text": str(response.query_text or ""),
        "source_family": source_family,
        "result_count": len(list(response.results or [])),
        "results": [
            {
                "title": str(item.title or ""),
                "url": normalize_public_web_url(item.url),
                "snippet": str(item.snippet or ""),
                "metadata": sanitize_company_public_web_provider_metadata(item.metadata),
                "raw_content_included": False,
            }
            for item in list(response.results or [])
            if normalize_public_web_url(item.url)
        ],
        "raw_payload_included": False,
        "raw_format": str(response.raw_format or ""),
        "metadata": sanitize_company_public_web_provider_metadata(response.metadata),
    }


def write_company_public_web_asset_artifacts(
    *,
    artifact_root: Path,
    run: dict[str, Any],
    assets: list[dict[str, Any]],
    request_payload: dict[str, Any],
    query_manifest: list[dict[str, Any]] | None = None,
    search_results: list[dict[str, Any]] | None = None,
    collector_manifest: list[dict[str, Any]] | None = None,
    collector_source_metrics: dict[str, Any] | None = None,
) -> dict[str, str]:
    artifact_root.mkdir(parents=True, exist_ok=True)
    summary_path = artifact_root / "company_public_web_assets_summary.json"
    assets_path = artifact_root / "company_public_web_assets.json"
    query_manifest_path = artifact_root / "query_manifest.json"
    search_results_path = artifact_root / "model_safe_search_results.json"
    collector_manifest_path = artifact_root / "collector_manifest.json"
    manifest_path = artifact_root / "manifest.json"
    query_manifest = list(query_manifest or [])
    search_results = list(search_results or [])
    collector_manifest = list(collector_manifest or [])
    summary = summarize_company_public_web_assets(
        assets,
        run=run,
        query_manifest=query_manifest,
        search_results=search_results,
        collector_manifest=collector_manifest,
        collector_source_metrics=collector_source_metrics,
    )
    summary_path.write_text(json.dumps(_json_safe_payload(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    assets_path.write_text(json.dumps(_json_safe_payload(assets), ensure_ascii=False, indent=2), encoding="utf-8")
    query_manifest_path.write_text(
        json.dumps(_json_safe_payload(query_manifest), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    search_results_path.write_text(
        json.dumps(_json_safe_payload(search_results), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    collector_manifest_path.write_text(
        json.dumps(_json_safe_payload(collector_manifest), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    manifest = {
        "artifact_kind": "company_public_web_assets",
        "run_id": str(run.get("run_id") or ""),
        "target_company": str(run.get("target_company") or ""),
        "company_key": str(run.get("company_key") or ""),
        "asset_count": len(assets),
        "raw_assets_included": False,
        "query_count": len(query_manifest),
        "provider_result_count": sum(int(item.get("result_count") or 0) for item in search_results),
        "collector_record_count": sum(int(item.get("record_count") or 0) for item in collector_manifest),
        "collector_source_metrics": dict(collector_source_metrics or {}),
        "request": request_payload,
        "written_at": datetime.now(timezone.utc).isoformat(),
        "files": {
            "summary": str(summary_path),
            "assets": str(assets_path),
            "query_manifest": str(query_manifest_path),
            "model_safe_search_results": str(search_results_path),
            "collector_manifest": str(collector_manifest_path),
        },
    }
    manifest_path.write_text(json.dumps(_json_safe_payload(manifest), ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "summary": str(summary_path),
        "assets": str(assets_path),
        "query_manifest": str(query_manifest_path),
        "model_safe_search_results": str(search_results_path),
        "collector_manifest": str(collector_manifest_path),
        "manifest": str(manifest_path),
    }


def summarize_company_public_web_assets(
    assets: list[dict[str, Any]],
    *,
    run: dict[str, Any] | None = None,
    query_manifest: list[dict[str, Any]] | None = None,
    search_results: list[dict[str, Any]] | None = None,
    collector_manifest: list[dict[str, Any]] | None = None,
    collector_source_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source_family_counts: dict[str, int] = {}
    collection_mode_counts: dict[str, int] = {}
    collector_type_counts: dict[str, int] = {}
    provider_names: set[str] = set()
    for asset in list(assets or []):
        asset_payload = dict(asset or {})
        family = str(asset_payload.get("source_family") or "").strip() or "unknown"
        source_family_counts[family] = source_family_counts.get(family, 0) + 1
        metadata = dict(asset_payload.get("metadata") or {})
        collection_mode = str(metadata.get("collection_mode") or "unknown").strip() or "unknown"
        collection_mode_counts[collection_mode] = collection_mode_counts.get(collection_mode, 0) + 1
        collector_type = str(metadata.get("collector_type") or "").strip()
        if collector_type:
            collector_type_counts[collector_type] = collector_type_counts.get(collector_type, 0) + 1
        provider_name = str(metadata.get("provider_name") or "").strip()
        if provider_name:
            provider_names.add(provider_name)
    search_results = list(search_results or [])
    collector_source_metrics = dict(collector_source_metrics or {})
    for result in search_results:
        provider_name = str(dict(result or {}).get("provider_name") or "").strip()
        if provider_name:
            provider_names.add(provider_name)
    return {
        "run_id": str(dict(run or {}).get("run_id") or ""),
        "target_company": str(dict(run or {}).get("target_company") or ""),
        "company_key": str(dict(run or {}).get("company_key") or ""),
        "asset_count": len(list(assets or [])),
        "source_family_counts": source_family_counts,
        "collection_mode_counts": collection_mode_counts,
        "query_count": len(list(query_manifest or [])),
        "provider_result_count": sum(int(item.get("result_count") or 0) for item in search_results),
        "provider_names": sorted(provider_names),
        "collector_record_count": sum(int(item.get("record_count") or 0) for item in list(collector_manifest or [])),
        "collector_type_counts": collector_type_counts,
        "collector_source_count": _safe_int(collector_source_metrics.get("collector_source_count")),
        "collector_document_fetch_count": _safe_int(
            collector_source_metrics.get("collector_document_fetch_count")
        ),
        "collector_document_fetch_failure_count": _safe_int(
            collector_source_metrics.get("collector_document_fetch_failure_count")
        ),
        "collector_source_type_counts": dict(collector_source_metrics.get("collector_source_type_counts") or {}),
        "collector_fetch_duration_ms_total": _coerce_float(
            collector_source_metrics.get("collector_fetch_duration_ms_total")
        ),
        "collector_fetch_duration_ms_max": _coerce_float(
            collector_source_metrics.get("collector_fetch_duration_ms_max")
        ),
        "raw_assets_included": False,
        "default_workflow_stage": "not_enabled",
        "target_candidate_lane": "separate",
    }


def default_company_public_web_seed_urls(
    *,
    target_company: str,
    company_key: str,
    source_families: list[str],
) -> list[str]:
    base_domain = company_key_to_domain(company_key or normalize_company_key(target_company))
    urls_by_family = {
        "company_homepage": f"https://{base_domain}/",
        "company_blog": f"https://{base_domain}/blog",
        "company_research": f"https://{base_domain}/research",
        "company_engineering": f"https://{base_domain}/engineering",
        "company_news": f"https://{base_domain}/news",
        "company_docs": f"https://{base_domain}/docs",
        "company_rss": f"https://{base_domain}/rss.xml",
        "company_arxiv": "https://arxiv.org/search/?query="
        + target_company.replace(" ", "+")
        + "&searchtype=all",
        "company_openreview": "https://openreview.net/search?term=" + target_company.replace(" ", "+"),
        "company_crawl": f"https://{base_domain}/",
    }
    return [urls_by_family[family] for family in source_families if family in urls_by_family]


def company_key_to_domain(company_key: str) -> str:
    normalized = normalize_company_key(company_key) or "company"
    if "." in normalized:
        return normalized
    return f"{normalized}.com"


def normalize_public_web_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    if not text.startswith(("http://", "https://")):
        text = f"https://{text}"
    parsed = urlparse(text)
    if not parsed.netloc:
        return ""
    scheme = parsed.scheme or "https"
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{scheme}://{parsed.netloc.lower()}{path}{query}"


def normalize_public_web_url_key(url: str) -> str:
    normalized = normalize_public_web_url(url)
    if not normalized:
        return ""
    return sha1(normalized.lower().encode("utf-8")).hexdigest()[:24]


def infer_company_public_web_source_family(url: str) -> str:
    host = urlparse(url).netloc.lower()
    path = urlparse(url).path.lower()
    if "arxiv.org" in host:
        return "company_arxiv"
    if "openreview.net" in host:
        return "company_openreview"
    if path.endswith((".rss", ".xml")) or "/rss" in path or "/feed" in path:
        return "company_rss"
    if "/research" in path:
        return "company_research"
    if "/engineering" in path:
        return "company_engineering"
    if "/news" in path:
        return "company_news"
    if "/docs" in path or "/documentation" in path:
        return "company_docs"
    if "/blog" in path:
        return "company_blog"
    return "company_homepage"


def company_public_web_asset_title(*, target_company: str, source_family: str, url: str) -> str:
    labels = {
        "company_homepage": "homepage",
        "company_blog": "blog",
        "company_research": "research",
        "company_engineering": "engineering",
        "company_news": "news",
        "company_docs": "docs",
        "company_rss": "RSS feed",
        "company_arxiv": "arXiv publications",
        "company_openreview": "OpenReview publications",
        "company_crawl": "crawled page",
    }
    return f"{target_company} {labels.get(source_family, source_family)}".strip() or url


def company_public_web_collection_phase(collection_mode: str) -> str:
    if collection_mode == "provider_search":
        return "collecting_provider_assets"
    if collection_mode == "collector_bundle":
        return "collecting_collector_assets"
    return "collecting_seed_assets"


def sanitize_company_public_web_provider_metadata(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    blocked_fragments = ("raw", "html", "content", "body", "payload", "text", "pdf")
    sanitized: dict[str, Any] = {}
    for key, raw_value in value.items():
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        if any(fragment in normalized_key.lower() for fragment in blocked_fragments):
            continue
        if isinstance(raw_value, (dict, list, tuple, set)):
            continue
        sanitized[normalized_key] = raw_value
    return _json_safe_payload(sanitized)


def build_company_public_web_run_idempotency_key(
    *,
    target_company: str,
    company_key: str,
    source_families: list[str],
    seed_urls: list[str],
    options: dict[str, Any],
    force_refresh: bool = False,
    nonce: str = "",
) -> str:
    payload = {
        "target_company": str(target_company or "").strip(),
        "company_key": str(company_key or "").strip(),
        "source_families": sorted(str(item or "").strip() for item in source_families if str(item or "").strip()),
        "seed_urls": sorted(str(item or "").strip() for item in seed_urls if str(item or "").strip()),
        "options": options,
        "force_refresh": bool(force_refresh),
        "nonce": str(nonce or "") if force_refresh else "",
    }
    return "company-public-web-run:" + short_hash(json.dumps(_json_safe_payload(payload), sort_keys=True, ensure_ascii=False))


def _source_family_by_seed_url(*, seed_urls: list[str], source_families: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    if len(seed_urls) == len(source_families):
        for url, family in zip(seed_urls, source_families, strict=False):
            result[url] = family
    return result


def _company_public_web_asset_run_projection(asset: dict[str, Any]) -> dict[str, Any]:
    return {
        "asset_id": str(asset.get("asset_id") or ""),
        "source_family": str(asset.get("source_family") or ""),
        "asset_kind": str(asset.get("asset_kind") or ""),
        "title": str(asset.get("title") or ""),
        "url": str(asset.get("url") or ""),
        "normalized_url_key": str(asset.get("normalized_url_key") or ""),
        "raw_assets_included": False,
    }


def _xml_child_text(element: ET.Element, tag: str) -> str:
    child = element.find(tag)
    if child is None or child.text is None:
        return ""
    return str(child.text or "").strip()


def _openreview_content_value(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("value")
    return str(value or "").strip()


def strip_markup(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", str(value or "")).strip()


def short_hash(value: str) -> str:
    return sha1(str(value or "").encode("utf-8")).hexdigest()[:16]


def utc_sql_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def utc_compact_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _bounded_int(value: Any, *, default: int, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, low), high)


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _coerce_float(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return 0.0
    return parsed if parsed >= 0 else 0.0


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False
