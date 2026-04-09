from __future__ import annotations

import base64
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any
from urllib import parse

import requests

from .dataforseo_client import (
    DataForSeoGoogleOrganicClient,
    MAX_TASK_POST_BATCH_SIZE,
    extract_google_organic_ready_task_ids,
    extract_google_organic_result_block,
    extract_google_organic_submitted_tasks,
    extract_google_organic_task_ids,
    build_google_organic_task,
)
from .settings import SearchProviderSettings
from .web_fetch import DEFAULT_HEADERS, fetch_search_results_html

_SHARED_LIBRARY_PACKAGE_HINTS = {
    "libnspr4.so": "libnspr4",
    "libnss3.so": "libnss3",
}
_DEFAULT_LANE_FETCH_COOLDOWN_SECONDS = 15


@dataclass(frozen=True, slots=True)
class SearchResultItem:
    title: str
    url: str
    snippet: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "url": self.url,
            "snippet": self.snippet,
            "metadata": self.metadata,
        }


@dataclass(frozen=True, slots=True)
class SearchResponse:
    provider_name: str
    query_text: str
    results: list[SearchResultItem]
    raw_payload: Any
    raw_format: str
    final_url: str = ""
    content_type: str = "text/html"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SearchExecutionArtifact:
    label: str
    payload: Any
    raw_format: str = "json"
    content_type: str = "application/json"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SearchExecutionResult:
    provider_name: str
    query_text: str
    response: SearchResponse | None = None
    checkpoint: dict[str, Any] = field(default_factory=dict)
    pending: bool = False
    message: str = ""
    artifacts: list[SearchExecutionArtifact] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SearchBatchSubmissionTask:
    task_key: str
    query_text: str
    checkpoint: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SearchBatchSubmissionResult:
    provider_name: str
    tasks: list[SearchBatchSubmissionTask] = field(default_factory=list)
    artifacts: list[SearchExecutionArtifact] = field(default_factory=list)
    message: str = ""


@dataclass(frozen=True, slots=True)
class SearchBatchReadyTask:
    task_key: str
    task_id: str
    query_text: str
    checkpoint: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SearchBatchReadyResult:
    provider_name: str
    tasks: list[SearchBatchReadyTask] = field(default_factory=list)
    artifacts: list[SearchExecutionArtifact] = field(default_factory=list)
    message: str = ""


@dataclass(frozen=True, slots=True)
class SearchBatchFetchTask:
    task_key: str
    task_id: str
    query_text: str
    response: SearchResponse | None = None
    checkpoint: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SearchBatchFetchResult:
    provider_name: str
    tasks: list[SearchBatchFetchTask] = field(default_factory=list)
    artifacts: list[SearchExecutionArtifact] = field(default_factory=list)
    message: str = ""


class SearchProviderError(RuntimeError):
    def __init__(self, message: str, *, attempts: list[dict[str, str]] | None = None) -> None:
        super().__init__(message)
        self.attempts = attempts or []


class BaseSearchProvider:
    provider_name: str = "base"

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        raise NotImplementedError

    def execute_with_checkpoint(
        self,
        query_text: str,
        *,
        max_results: int = 10,
        timeout: int | None = None,
        checkpoint: dict[str, Any] | None = None,
    ) -> SearchExecutionResult:
        response = self.search(query_text, max_results=max_results, timeout=timeout)
        return SearchExecutionResult(
            provider_name=self.provider_name,
            query_text=query_text,
            response=response,
            checkpoint={
                "provider_name": self.provider_name,
                "status": "completed",
            },
        )

    def submit_batch_queries(self, query_specs: list[dict[str, Any]]) -> SearchBatchSubmissionResult | None:
        return None

    def poll_ready_batch(self, query_specs: list[dict[str, Any]]) -> SearchBatchReadyResult | None:
        return None

    def fetch_ready_batch(self, query_specs: list[dict[str, Any]]) -> SearchBatchFetchResult | None:
        return None


class DuckDuckGoHtmlSearchProvider(BaseSearchProvider):
    provider_name = "duckduckgo_html"

    def __init__(self, *, timeout_seconds: int = 30) -> None:
        self.timeout_seconds = timeout_seconds

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        fetched = fetch_search_results_html(query_text, timeout=timeout or self.timeout_seconds)
        results = parse_duckduckgo_html_results(fetched.text)[:max_results]
        return SearchResponse(
            provider_name=self.provider_name,
            query_text=query_text,
            results=results,
            raw_payload=fetched.text,
            raw_format="html",
            final_url=fetched.final_url,
            content_type=fetched.content_type,
            metadata={"source_label": fetched.source_label},
        )


class BingHtmlSearchProvider(BaseSearchProvider):
    provider_name = "bing_html"

    def __init__(self, *, timeout_seconds: int = 30) -> None:
        self.timeout_seconds = timeout_seconds

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        response = requests.get(
            "https://www.bing.com/search",
            headers={"User-Agent": "Mozilla/5.0"},
            params={
                "q": query_text,
                "count": max(1, min(int(max_results or 10), 20)),
                "setlang": "en-US",
                "cc": "us",
            },
            timeout=timeout or self.timeout_seconds,
        )
        response.raise_for_status()
        results = parse_bing_html_results(response.text)[:max_results]
        return SearchResponse(
            provider_name=self.provider_name,
            query_text=query_text,
            results=results,
            raw_payload=response.text,
            raw_format="html",
            final_url=str(response.url),
            content_type=str(response.headers.get("Content-Type") or "text/html"),
        )


class SerperGoogleSearchProvider(BaseSearchProvider):
    provider_name = "serper_google"

    def __init__(self, *, api_key: str, base_url: str = "https://google.serper.dev/search", timeout_seconds: int = 30) -> None:
        self.api_key = str(api_key or "").strip()
        self.base_url = str(base_url or "https://google.serper.dev/search").strip()
        self.timeout_seconds = timeout_seconds

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        if not self.api_key:
            raise SearchProviderError("Serper API key is not configured.")
        response = requests.post(
            self.base_url,
            headers={
                "X-API-KEY": self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
                **DEFAULT_HEADERS,
            },
            json={"q": query_text, "num": max_results},
            timeout=timeout or self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        results = parse_serper_search_results(payload)[:max_results]
        return SearchResponse(
            provider_name=self.provider_name,
            query_text=query_text,
            results=results,
            raw_payload=payload,
            raw_format="json",
            final_url=self.base_url,
            content_type="application/json",
        )


class DataForSeoGoogleOrganicSearchProvider(BaseSearchProvider):
    provider_name = "dataforseo_google_organic"

    def __init__(
        self,
        *,
        login: str,
        password: str,
        base_url: str = "https://api.dataforseo.com",
        location_name: str = "United States",
        language_name: str = "English",
        device: str = "desktop",
        os: str = "windows",
        depth: int = 10,
        timeout_seconds: int = 30,
    ) -> None:
        self.location_name = str(location_name or "United States").strip() or "United States"
        self.language_name = str(language_name or "English").strip() or "English"
        self.device = str(device or "desktop").strip() or "desktop"
        self.os = str(os or "windows").strip() or "windows"
        self.depth = max(1, int(depth or 10))
        self.client = DataForSeoGoogleOrganicClient(
            login=login,
            password=password,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
        )

    def _build_queue_checkpoint(self, *, query_text: str, depth: int, task_id: str, status: str) -> dict[str, Any]:
        return {
            "provider_name": self.provider_name,
            "query_text": query_text,
            "mode": "dataforseo_standard_queue",
            "location_name": self.location_name,
            "language_name": self.language_name,
            "device": self.device,
            "os": self.os,
            "depth": depth,
            "task_id": task_id,
            "status": status,
        }

    def _overlay_ready_metadata(self, checkpoint: dict[str, Any], reference: dict[str, Any]) -> dict[str, Any]:
        updated = dict(checkpoint or {})
        for key in [
            "ready_poll_token",
            "ready_checked_at",
            "ready_attempted_at",
            "ready_poll_source",
            "ready_poll_label",
            "fetch_attempted_at",
            "fetched_at",
            "fetch_token",
            "lane_fetch_cooldown_seconds",
        ]:
            value = (reference or {}).get(key)
            if str(value or "").strip():
                updated[key] = value
        return updated

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        payload = self.client.live_regular(
            keyword=query_text,
            location_name=self.location_name,
            language_name=self.language_name,
            device=self.device,
            os=self.os,
            depth=max(self.depth, max(1, int(max_results or 10))),
        )
        result_block = extract_google_organic_result_block(payload)
        results = parse_dataforseo_google_organic_results(payload)[:max_results]
        return SearchResponse(
            provider_name=self.provider_name,
            query_text=query_text,
            results=results,
            raw_payload=payload,
            raw_format="json",
            final_url=str(result_block.get("check_url") or ""),
            content_type="application/json",
            metadata={
                "source_label": "dataforseo_google_organic_live",
                "se_results_count": result_block.get("se_results_count"),
                "pages_count": result_block.get("pages_count"),
                "items_count": result_block.get("items_count"),
            },
        )

    def submit_batch_queries(self, query_specs: list[dict[str, Any]]) -> SearchBatchSubmissionResult | None:
        normalized_specs: list[dict[str, Any]] = []
        for spec in list(query_specs or []):
            query_text = " ".join(str((spec or {}).get("query_text") or "").split()).strip()
            if not query_text:
                continue
            max_results = max(1, int((spec or {}).get("max_results") or 10))
            task_key = str((spec or {}).get("task_key") or query_text).strip() or query_text
            depth = max(self.depth, max_results)
            normalized_specs.append(
                {
                    "task_key": task_key,
                    "query_text": query_text,
                    "max_results": max_results,
                    "depth": depth,
                    "task": build_google_organic_task(
                        keyword=query_text,
                        location_name=self.location_name,
                        language_name=self.language_name,
                        device=self.device,
                        os=self.os,
                        depth=depth,
                        tag=task_key,
                    ),
                }
            )
        if not normalized_specs:
            return None

        submitted_tasks: list[SearchBatchSubmissionTask] = []
        artifacts: list[SearchExecutionArtifact] = []
        for batch_index, start in enumerate(range(0, len(normalized_specs), MAX_TASK_POST_BATCH_SIZE), start=1):
            batch_specs = normalized_specs[start : start + MAX_TASK_POST_BATCH_SIZE]
            batch_tasks = [dict(item["task"]) for item in batch_specs]
            payload = self.client.task_post_many(batch_tasks)
            artifact_label = f"task_post_batch_{batch_index:02d}"
            artifacts.append(
                SearchExecutionArtifact(
                    label=artifact_label,
                    payload=payload,
                    metadata={
                        "batch_index": batch_index,
                        "task_count": len(batch_specs),
                        "provider_name": self.provider_name,
                    },
                )
            )
            submitted = extract_google_organic_submitted_tasks(payload, fallback_tasks=batch_tasks)
            for offset, spec in enumerate(batch_specs):
                echoed = submitted[offset] if offset < len(submitted) else {}
                task_id = str(echoed.get("task_id") or "").strip()
                submitted_tasks.append(
                    SearchBatchSubmissionTask(
                        task_key=str(spec["task_key"]),
                        query_text=str(spec["query_text"]),
                        checkpoint=self._build_queue_checkpoint(
                            query_text=str(spec["query_text"]),
                            depth=int(spec["depth"]),
                            task_id=task_id,
                            status="submitted",
                        ),
                        metadata={
                            "artifact_label": artifact_label,
                            "batch_index": batch_index,
                            "task_id": task_id,
                        },
                    )
                )
        return SearchBatchSubmissionResult(
            provider_name=self.provider_name,
            tasks=submitted_tasks,
            artifacts=artifacts,
            message=(
                f"Submitted {len(submitted_tasks)} DataForSEO Standard Queue tasks "
                f"across {len(artifacts)} batch request(s)."
            ),
        )

    def poll_ready_batch(self, query_specs: list[dict[str, Any]]) -> SearchBatchReadyResult | None:
        normalized_specs: list[dict[str, Any]] = []
        for spec in list(query_specs or []):
            checkpoint = dict((spec or {}).get("checkpoint") or {})
            task_id = str((spec or {}).get("task_id") or checkpoint.get("task_id") or "").strip()
            if not task_id:
                continue
            query_text = str((spec or {}).get("query_text") or checkpoint.get("query_text") or "").strip()
            task_key = str((spec or {}).get("task_key") or query_text or task_id).strip() or task_id
            depth = max(self.depth, max(1, int(checkpoint.get("depth") or (spec or {}).get("max_results") or 10)))
            normalized_specs.append(
                {
                    "task_key": task_key,
                    "task_id": task_id,
                    "query_text": query_text,
                    "checkpoint": checkpoint,
                    "depth": depth,
                }
            )
        if not normalized_specs:
            return None

        payload = self.client.tasks_ready()
        ready_ids = set(extract_google_organic_ready_task_ids(payload))
        tasks: list[SearchBatchReadyTask] = []
        for spec in normalized_specs:
            is_ready = str(spec["task_id"]) in ready_ids
            checkpoint = self._build_queue_checkpoint(
                query_text=str(spec["query_text"]),
                depth=int(spec["depth"]),
                task_id=str(spec["task_id"]),
                status="ready_cached" if is_ready else "waiting_for_ready_cached",
            )
            checkpoint = self._overlay_ready_metadata(checkpoint, dict(spec.get("checkpoint") or {}))
            tasks.append(
                SearchBatchReadyTask(
                    task_key=str(spec["task_key"]),
                    task_id=str(spec["task_id"]),
                    query_text=str(spec["query_text"]),
                    checkpoint=checkpoint,
                    metadata={"ready": is_ready},
                )
            )
        return SearchBatchReadyResult(
            provider_name=self.provider_name,
            tasks=tasks,
            artifacts=[
                SearchExecutionArtifact(
                    label="tasks_ready_batch",
                    payload=payload,
                    metadata={
                        "provider_name": self.provider_name,
                        "task_count": len(normalized_specs),
                        "ready_count": len([item for item in tasks if item.metadata.get("ready")]),
                    },
                )
            ],
            message=f"{len([item for item in tasks if item.metadata.get('ready')])}/{len(tasks)} tasks ready.",
        )

    def fetch_ready_batch(self, query_specs: list[dict[str, Any]]) -> SearchBatchFetchResult | None:
        normalized_specs: list[dict[str, Any]] = []
        for spec in list(query_specs or []):
            checkpoint = dict((spec or {}).get("checkpoint") or {})
            task_id = str((spec or {}).get("task_id") or checkpoint.get("task_id") or "").strip()
            if not task_id:
                continue
            query_text = str((spec or {}).get("query_text") or checkpoint.get("query_text") or "").strip()
            task_key = str((spec or {}).get("task_key") or query_text or task_id).strip() or task_id
            depth = max(self.depth, max(1, int(checkpoint.get("depth") or (spec or {}).get("max_results") or 10)))
            normalized_specs.append(
                {
                    "task_key": task_key,
                    "task_id": task_id,
                    "query_text": query_text,
                    "checkpoint": checkpoint,
                    "depth": depth,
                }
            )
        if not normalized_specs:
            return None

        tasks: list[SearchBatchFetchTask] = []
        artifacts: list[SearchExecutionArtifact] = []
        for index, spec in enumerate(normalized_specs, start=1):
            payload = self.client.task_get_regular(str(spec["task_id"]))
            artifacts.append(
                SearchExecutionArtifact(
                    label=f"task_get_batch_{index:02d}",
                    payload=payload,
                    metadata={
                        "provider_name": self.provider_name,
                        "task_key": str(spec["task_key"]),
                        "task_id": str(spec["task_id"]),
                    },
                )
            )
            result_block = extract_google_organic_result_block(payload)
            results = parse_dataforseo_google_organic_results(payload)
            response = SearchResponse(
                provider_name=self.provider_name,
                query_text=str(spec["query_text"]),
                results=results,
                raw_payload=payload,
                raw_format="json",
                final_url=str(result_block.get("check_url") or ""),
                content_type="application/json",
                metadata={
                    "source_label": "dataforseo_google_organic_task_get",
                    "search_mode": "standard_queue",
                    "task_id": str(spec["task_id"]),
                    "se_results_count": result_block.get("se_results_count"),
                    "pages_count": result_block.get("pages_count"),
                    "items_count": result_block.get("items_count"),
                },
            )
            checkpoint = self._build_queue_checkpoint(
                query_text=str(spec["query_text"]),
                depth=int(spec["depth"]),
                task_id=str(spec["task_id"]),
                status="fetched_cached",
            )
            checkpoint = self._overlay_ready_metadata(checkpoint, dict(spec.get("checkpoint") or {}))
            tasks.append(
                SearchBatchFetchTask(
                    task_key=str(spec["task_key"]),
                    task_id=str(spec["task_id"]),
                    query_text=str(spec["query_text"]),
                    response=response,
                    checkpoint=checkpoint,
                    metadata={"fetched": True},
                )
            )
        return SearchBatchFetchResult(
            provider_name=self.provider_name,
            tasks=tasks,
            artifacts=artifacts,
            message=f"Fetched {len(tasks)} ready task result(s).",
        )

    def execute_with_checkpoint(
        self,
        query_text: str,
        *,
        max_results: int = 10,
        timeout: int | None = None,
        checkpoint: dict[str, Any] | None = None,
    ) -> SearchExecutionResult:
        existing = dict(checkpoint or {})
        task_id = str(existing.get("task_id") or "").strip()
        status = str(existing.get("status") or "").strip()
        depth = max(self.depth, max(1, int(max_results or 10)))
        base_checkpoint = self._build_queue_checkpoint(
            query_text=query_text,
            depth=depth,
            task_id=task_id,
            status=status,
        )
        if not task_id:
            payload = self.client.task_post(
                keyword=query_text,
                location_name=self.location_name,
                language_name=self.language_name,
                device=self.device,
                os=self.os,
                depth=depth,
            )
            task_ids = extract_google_organic_task_ids(payload)
            task_id = task_ids[0] if task_ids else ""
            updated_checkpoint = self._build_queue_checkpoint(
                query_text=query_text,
                depth=depth,
                task_id=task_id,
                status="submitted",
            )
            return SearchExecutionResult(
                provider_name=self.provider_name,
                query_text=query_text,
                checkpoint=updated_checkpoint,
                pending=True,
                message=f"Submitted DataForSEO Standard Queue task {task_id or 'unknown'}.",
                artifacts=[
                    SearchExecutionArtifact(
                        label="task_post",
                        payload=payload,
                        metadata={"task_id": task_id, "provider_name": self.provider_name},
                    )
                ],
            )

        if status == "waiting_for_ready_cached":
            return SearchExecutionResult(
                provider_name=self.provider_name,
                query_text=query_text,
                checkpoint=self._overlay_ready_metadata(base_checkpoint, existing),
                pending=True,
                message=f"Waiting for DataForSEO task {task_id} to become ready (lane cache).",
            )

        if status == "ready_cached":
            fetch_cooldown_seconds = max(
                1,
                int(existing.get("lane_fetch_cooldown_seconds") or _DEFAULT_LANE_FETCH_COOLDOWN_SECONDS),
            )
            if (
                not str(existing.get("fetched_at") or "").strip()
                and _timestamp_within_seconds(str(existing.get("fetch_attempted_at") or ""), fetch_cooldown_seconds)
            ):
                return SearchExecutionResult(
                    provider_name=self.provider_name,
                    query_text=query_text,
                    checkpoint=self._overlay_ready_metadata(base_checkpoint, existing),
                    pending=True,
                    message=f"Waiting for lane-level DataForSEO fetch cache for task {task_id}.",
                )
            payload = self.client.task_get_regular(task_id)
            result_block = extract_google_organic_result_block(payload)
            results = parse_dataforseo_google_organic_results(payload)[:max_results]
            response = SearchResponse(
                provider_name=self.provider_name,
                query_text=query_text,
                results=results,
                raw_payload=payload,
                raw_format="json",
                final_url=str(result_block.get("check_url") or ""),
                content_type="application/json",
                metadata={
                    "source_label": "dataforseo_google_organic_task_get",
                    "search_mode": "standard_queue",
                    "task_id": task_id,
                    "se_results_count": result_block.get("se_results_count"),
                    "pages_count": result_block.get("pages_count"),
                    "items_count": result_block.get("items_count"),
                },
            )
            return SearchExecutionResult(
                provider_name=self.provider_name,
                query_text=query_text,
                response=response,
                checkpoint=self._build_queue_checkpoint(
                    query_text=query_text,
                    depth=depth,
                    task_id=task_id,
                    status="completed",
                ),
            )

        ready_payload = self.client.tasks_ready()
        ready_ids = set(extract_google_organic_ready_task_ids(ready_payload))
        artifacts = [
            SearchExecutionArtifact(
                label="tasks_ready",
                payload=ready_payload,
                metadata={"task_id": task_id, "provider_name": self.provider_name},
            )
        ]
        if task_id not in ready_ids:
            return SearchExecutionResult(
                provider_name=self.provider_name,
                query_text=query_text,
                checkpoint={
                    **base_checkpoint,
                    "task_id": task_id,
                    "status": "waiting_for_ready",
                },
                pending=True,
                message=f"Waiting for DataForSEO task {task_id} to become ready.",
                artifacts=artifacts,
            )

        payload = self.client.task_get_regular(task_id)
        result_block = extract_google_organic_result_block(payload)
        results = parse_dataforseo_google_organic_results(payload)[:max_results]
        response = SearchResponse(
            provider_name=self.provider_name,
            query_text=query_text,
            results=results,
            raw_payload=payload,
            raw_format="json",
            final_url=str(result_block.get("check_url") or ""),
            content_type="application/json",
            metadata={
                "source_label": "dataforseo_google_organic_task_get",
                "search_mode": "standard_queue",
                "task_id": task_id,
                "se_results_count": result_block.get("se_results_count"),
                "pages_count": result_block.get("pages_count"),
                "items_count": result_block.get("items_count"),
            },
        )
        return SearchExecutionResult(
            provider_name=self.provider_name,
            query_text=query_text,
            response=response,
            checkpoint=self._build_queue_checkpoint(
                query_text=query_text,
                depth=depth,
                task_id=task_id,
                status="completed",
            ),
            artifacts=artifacts,
        )


class BrowserGoogleSearchProvider(BaseSearchProvider):
    provider_name = "google_browser"

    def __init__(
        self,
        *,
        script_path: str,
        npx_package: str = "playwright@1.59.1",
        node_modules_dir: str = "/tmp/sourcing-playwright-node/node_modules",
        npm_cache_dir: str = "/tmp/.npm-cache",
        browsers_path: str = "/tmp/playwright-browsers",
        headless: bool = True,
        locale: str = "en-US",
        timeout_seconds: int = 30,
    ) -> None:
        self.script_path = str(script_path or "").strip()
        self.npx_package = str(npx_package or "playwright@1.59.1").strip()
        self.node_modules_dir = str(node_modules_dir or "/tmp/sourcing-playwright-node/node_modules").strip()
        self.npm_cache_dir = str(npm_cache_dir or "/tmp/.npm-cache").strip()
        self.browsers_path = str(browsers_path or "/tmp/playwright-browsers").strip()
        self.headless = bool(headless)
        self.locale = str(locale or "en-US").strip() or "en-US"
        self.timeout_seconds = timeout_seconds

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        if not self.script_path:
            raise SearchProviderError("Browser Google search script is not configured.")
        if shutil.which("node") is None:
            raise SearchProviderError("node is not available; browser Google search cannot run.")
        script_path = Path(self.script_path)
        if not script_path.exists():
            raise SearchProviderError(f"Browser Google search script is missing: {script_path}")
        if self.node_modules_dir and not Path(self.node_modules_dir).exists():
            raise SearchProviderError(
                "Browser Google search dependencies are missing. "
                f"Expected node_modules at {self.node_modules_dir}. "
                f"Install {self.npx_package} there before using google_browser."
            )
        env = os.environ.copy()
        if self.npm_cache_dir:
            env.setdefault("NPM_CONFIG_CACHE", self.npm_cache_dir)
        if self.browsers_path:
            env.setdefault("PLAYWRIGHT_BROWSERS_PATH", self.browsers_path)
        if self.node_modules_dir:
            existing_node_path = str(env.get("NODE_PATH") or "").strip()
            env["NODE_PATH"] = (
                f"{self.node_modules_dir}:{existing_node_path}" if existing_node_path else self.node_modules_dir
            )
        command = [
            "node",
            str(script_path),
            "--query",
            query_text,
            "--max-results",
            str(max(1, min(int(max_results or 10), 20))),
            "--locale",
            self.locale,
            "--headless",
            "true" if self.headless else "false",
            "--timeout-ms",
            str(max(5, int(timeout or self.timeout_seconds)) * 1000),
        ]
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=max(10, int(timeout or self.timeout_seconds)) + 30,
                env=env,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise SearchProviderError(f"Browser Google search timed out: {exc}") from exc
        if completed.returncode != 0:
            stderr = (completed.stderr or "").strip()
            raise SearchProviderError(_format_browser_search_failure(stderr))
        payload = _parse_browser_provider_payload(completed.stdout)
        metadata = dict(payload.get("metadata") or {})
        if metadata.get("blocked"):
            raise SearchProviderError(
                "Browser Google search was blocked by Google CAPTCHA. "
                f"Final URL: {str(payload.get('final_url') or 'unknown')}"
            )
        results = [
            SearchResultItem(
                title=str(item.get("title") or "").strip(),
                url=str(item.get("url") or "").strip(),
                snippet=str(item.get("snippet") or "").strip(),
                metadata=dict(item.get("metadata") or {}),
            )
            for item in list(payload.get("results") or [])
            if str(item.get("title") or "").strip() and str(item.get("url") or "").strip()
        ]
        return SearchResponse(
            provider_name=self.provider_name,
            query_text=query_text,
            results=results[:max_results],
            raw_payload=payload,
            raw_format="json",
            final_url=str(payload.get("final_url") or ""),
            content_type="application/json",
            metadata=metadata,
        )


class SearchProviderChain(BaseSearchProvider):
    provider_name = "chain"

    def __init__(self, providers: list[BaseSearchProvider]) -> None:
        self.providers = list(providers)

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        attempts: list[dict[str, str]] = []
        last_error: Exception | None = None
        for provider in self.providers:
            try:
                return provider.search(query_text, max_results=max_results, timeout=timeout)
            except Exception as exc:
                last_error = exc
                attempts.append({"provider_name": getattr(provider, "provider_name", "unknown"), "error": str(exc)})
        if last_error is None:
            raise SearchProviderError("No search providers are configured.", attempts=attempts)
        raise SearchProviderError(str(last_error), attempts=attempts)

    def execute_with_checkpoint(
        self,
        query_text: str,
        *,
        max_results: int = 10,
        timeout: int | None = None,
        checkpoint: dict[str, Any] | None = None,
    ) -> SearchExecutionResult:
        attempts: list[dict[str, str]] = []
        last_error: Exception | None = None
        provider_name = str((checkpoint or {}).get("provider_name") or "").strip()
        if provider_name:
            pinned = next((provider for provider in self.providers if provider.provider_name == provider_name), None)
            if pinned is not None:
                try:
                    return pinned.execute_with_checkpoint(
                        query_text,
                        max_results=max_results,
                        timeout=timeout,
                        checkpoint=checkpoint,
                    )
                except Exception as exc:
                    last_error = exc
                    attempts.append({"provider_name": provider_name, "error": str(exc)})
                    raise SearchProviderError(str(exc), attempts=attempts)
        for provider in self.providers:
            try:
                return provider.execute_with_checkpoint(
                    query_text,
                    max_results=max_results,
                    timeout=timeout,
                    checkpoint={},
                )
            except Exception as exc:
                last_error = exc
                attempts.append({"provider_name": getattr(provider, "provider_name", "unknown"), "error": str(exc)})
        if last_error is None:
            raise SearchProviderError("No search providers are configured.", attempts=attempts)
        raise SearchProviderError(str(last_error), attempts=attempts)

    def submit_batch_queries(self, query_specs: list[dict[str, Any]]) -> SearchBatchSubmissionResult | None:
        for provider in self.providers:
            try:
                result = provider.submit_batch_queries(query_specs)
            except Exception:
                continue
            if result is not None:
                return result
        return None

    def poll_ready_batch(self, query_specs: list[dict[str, Any]]) -> SearchBatchReadyResult | None:
        provider_name = str((query_specs[0] or {}).get("provider_name") or dict((query_specs[0] or {}).get("checkpoint") or {}).get("provider_name") or "").strip() if query_specs else ""
        if provider_name:
            pinned = next((provider for provider in self.providers if provider.provider_name == provider_name), None)
            if pinned is not None:
                return pinned.poll_ready_batch(query_specs)
        for provider in self.providers:
            result = provider.poll_ready_batch(query_specs)
            if result is not None:
                return result
        return None

    def fetch_ready_batch(self, query_specs: list[dict[str, Any]]) -> SearchBatchFetchResult | None:
        provider_name = str((query_specs[0] or {}).get("provider_name") or dict((query_specs[0] or {}).get("checkpoint") or {}).get("provider_name") or "").strip() if query_specs else ""
        if provider_name:
            pinned = next((provider for provider in self.providers if provider.provider_name == provider_name), None)
            if pinned is not None:
                return pinned.fetch_ready_batch(query_specs)
        for provider in self.providers:
            result = provider.fetch_ready_batch(query_specs)
            if result is not None:
                return result
        return None


def build_search_provider(settings: SearchProviderSettings) -> BaseSearchProvider:
    providers: list[BaseSearchProvider] = []
    provider_order = [str(item or "").strip().lower() for item in settings.provider_order if str(item or "").strip()]
    if settings.enable_bing_html and "bing_html" not in provider_order:
        if "duckduckgo_html" in provider_order:
            provider_order.insert(provider_order.index("duckduckgo_html"), "bing_html")
        else:
            provider_order.append("bing_html")
    for normalized in provider_order:
        if not normalized:
            continue
        if normalized == "serper_google" and settings.serper_api_key:
            providers.append(
                SerperGoogleSearchProvider(
                    api_key=settings.serper_api_key,
                    base_url=settings.serper_base_url,
                    timeout_seconds=settings.timeout_seconds,
                )
            )
        elif (
            normalized == "dataforseo_google_organic"
            and settings.enable_dataforseo_google_organic
            and settings.dataforseo_login
            and settings.dataforseo_password
        ):
            providers.append(
                DataForSeoGoogleOrganicSearchProvider(
                    login=settings.dataforseo_login,
                    password=settings.dataforseo_password,
                    base_url=settings.dataforseo_base_url,
                    location_name=settings.dataforseo_default_location_name,
                    language_name=settings.dataforseo_default_language_name,
                    device=settings.dataforseo_default_device,
                    os=settings.dataforseo_default_os,
                    depth=settings.dataforseo_default_depth,
                    timeout_seconds=settings.timeout_seconds,
                )
            )
        elif normalized == "google_browser" and settings.enable_google_browser:
            default_script_path = Path(__file__).resolve().parents[2] / "scripts" / "google_search_browser.cjs"
            providers.append(
                BrowserGoogleSearchProvider(
                    script_path=settings.google_browser_script_path or str(default_script_path),
                    npx_package=settings.google_browser_npx_package,
                    node_modules_dir=settings.google_browser_node_modules_dir,
                    npm_cache_dir=settings.google_browser_npm_cache_dir,
                    browsers_path=settings.google_browser_browsers_path,
                    headless=settings.google_browser_headless,
                    locale=settings.google_browser_locale,
                    timeout_seconds=settings.timeout_seconds,
                )
            )
        elif normalized == "bing_html" and settings.enable_bing_html:
            providers.append(BingHtmlSearchProvider(timeout_seconds=settings.timeout_seconds))
        elif normalized == "duckduckgo_html" and settings.enable_duckduckgo_html:
            providers.append(DuckDuckGoHtmlSearchProvider(timeout_seconds=settings.timeout_seconds))
    if not providers:
        providers.append(DuckDuckGoHtmlSearchProvider(timeout_seconds=settings.timeout_seconds))
    return SearchProviderChain(providers)


def search_response_to_record(response: SearchResponse) -> dict[str, Any]:
    return {
        "provider_name": response.provider_name,
        "query_text": response.query_text,
        "results": [item.to_record() for item in response.results],
        "raw_payload": response.raw_payload,
        "raw_format": response.raw_format,
        "final_url": response.final_url,
        "content_type": response.content_type,
        "metadata": response.metadata,
    }


def search_response_from_record(payload: dict[str, Any], *, fallback_query_text: str = "") -> SearchResponse:
    return SearchResponse(
        provider_name=str(payload.get("provider_name") or "unknown"),
        query_text=str(payload.get("query_text") or fallback_query_text),
        results=[
            SearchResultItem(
                title=str(item.get("title") or "").strip(),
                url=str(item.get("url") or "").strip(),
                snippet=str(item.get("snippet") or "").strip(),
                metadata=dict(item.get("metadata") or {}),
            )
            for item in list(payload.get("results") or [])
            if str(item.get("title") or "").strip() and str(item.get("url") or "").strip()
        ],
        raw_payload=payload.get("raw_payload"),
        raw_format=str(payload.get("raw_format") or "html"),
        final_url=str(payload.get("final_url") or ""),
        content_type=str(payload.get("content_type") or "text/html"),
        metadata=dict(payload.get("metadata") or {}),
    )


def parse_duckduckgo_html_results(html_text: str) -> list[SearchResultItem]:
    pattern = re.compile(
        r'<a class="result__a" href="([^"]+)".*?>(.*?)</a>(?:.*?<a class="result__snippet".*?>(.*?)</a>|.*?<div class="result__snippet".*?>(.*?)</div>)?',
        re.DOTALL,
    )
    results: list[SearchResultItem] = []
    for raw_url, raw_title, raw_snippet_a, raw_snippet_div in pattern.findall(html_text or ""):
        url = unescape(raw_url)
        if "duckduckgo.com/l/" in url:
            parsed = parse.urlparse(url)
            query = parse.parse_qs(parsed.query)
            url = query.get("uddg", [url])[0]
        title = _strip_html(raw_title)
        snippet = _strip_html(raw_snippet_a or raw_snippet_div or "")
        if not url or not title:
            continue
        results.append(SearchResultItem(title=title, url=url, snippet=snippet))
    return results


def parse_bing_html_results(html_text: str) -> list[SearchResultItem]:
    parser = _BingHtmlResultsParser()
    parser.feed(str(html_text or ""))
    if parser.results:
        return parser.results
    results: list[SearchResultItem] = []
    seen_urls: set[str] = set()
    pattern = re.compile(r'<h2[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>\s*</h2>', re.DOTALL)
    for raw_url, raw_title in pattern.findall(str(html_text or "")):
        url = _decode_bing_result_url(raw_url)
        title = _strip_html(raw_title)
        if not title or not url or url in seen_urls:
            continue
        seen_urls.add(url)
        metadata: dict[str, Any] = {}
        try:
            metadata["source_domain"] = parse.urlparse(url).hostname or ""
        except Exception:
            metadata["source_domain"] = ""
        results.append(SearchResultItem(title=title, url=url, snippet="", metadata=metadata))
    return results


def parse_serper_search_results(payload: dict[str, Any]) -> list[SearchResultItem]:
    results: list[SearchResultItem] = []
    for item in list(payload.get("organic") or []):
        title = str(item.get("title") or "").strip()
        url = str(item.get("link") or "").strip()
        snippet = str(item.get("snippet") or "").strip()
        if not title or not url:
            continue
        results.append(
            SearchResultItem(
                title=title,
                url=url,
                snippet=snippet,
                metadata={
                    "position": item.get("position"),
                    "display_link": str(item.get("displayLink") or "").strip(),
                },
            )
        )
    return results


def parse_dataforseo_google_organic_results(payload: dict[str, Any]) -> list[SearchResultItem]:
    result_block = extract_google_organic_result_block(payload)
    results: list[SearchResultItem] = []
    for item in list(result_block.get("items") or []):
        if str(item.get("type") or "").strip().lower() != "organic":
            continue
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        snippet = str(item.get("description") or "").strip()
        if not title or not url:
            continue
        results.append(
            SearchResultItem(
                title=title,
                url=url,
                snippet=snippet,
                metadata={
                    "rank_group": item.get("rank_group"),
                    "rank_absolute": item.get("rank_absolute"),
                    "page": item.get("page"),
                    "domain": str(item.get("domain") or "").strip(),
                    "breadcrumb": str(item.get("breadcrumb") or "").strip(),
                },
            )
        )
    return results


def _decode_bing_result_url(raw_url: str) -> str:
    url = unescape(str(raw_url or "").strip())
    if not url:
        return ""
    try:
        parsed = parse.urlparse(url)
    except Exception:
        return url
    if parsed.netloc.endswith("bing.com") and parsed.path.startswith("/ck/a"):
        encoded_target = (parse.parse_qs(parsed.query).get("u") or [""])[0]
        if encoded_target.startswith("a1"):
            encoded_target = encoded_target[2:]
        if encoded_target:
            padded = encoded_target + ("=" * (-len(encoded_target) % 4))
            try:
                decoded = base64.b64decode(padded).decode("utf-8", errors="ignore").strip()
            except Exception:
                decoded = ""
            if decoded.startswith("http"):
                return decoded
    return url


def _strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    return " ".join(unescape(text).split())


class _BingHtmlResultsParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.results: list[SearchResultItem] = []
        self._result_depth = 0
        self._in_h2 = False
        self._in_title_link = False
        self._in_snippet = False
        self._current_href = ""
        self._current_title: list[str] = []
        self._current_snippet: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key: value or "" for key, value in attrs}
        class_names = set((attr_map.get("class") or "").split())
        if tag == "li" and "b_algo" in class_names:
            if self._result_depth == 0:
                self._current_href = ""
                self._current_title = []
                self._current_snippet = []
                self._in_h2 = False
                self._in_title_link = False
                self._in_snippet = False
            self._result_depth += 1
            return
        if self._result_depth == 0:
            return
        if tag == "li":
            self._result_depth += 1
            return
        if tag == "h2":
            self._in_h2 = True
            return
        if tag == "a" and self._in_h2 and not self._current_href:
            self._current_href = attr_map.get("href", "")
            self._in_title_link = True
            return
        if tag == "p" and not self._current_snippet:
            self._in_snippet = True

    def handle_endtag(self, tag: str) -> None:
        if self._result_depth == 0:
            return
        if tag == "a" and self._in_title_link:
            self._in_title_link = False
            return
        if tag == "h2":
            self._in_h2 = False
            return
        if tag == "p" and self._in_snippet:
            self._in_snippet = False
            return
        if tag == "li":
            self._result_depth -= 1
            if self._result_depth == 0:
                self._flush_result()

    def handle_data(self, data: str) -> None:
        if self._result_depth == 0:
            return
        if self._in_title_link:
            self._current_title.append(data)
        elif self._in_snippet:
            self._current_snippet.append(data)

    def _flush_result(self) -> None:
        url = _decode_bing_result_url(self._current_href)
        title = " ".join(part.strip() for part in self._current_title if part.strip())
        snippet = " ".join(part.strip() for part in self._current_snippet if part.strip())
        if not title or not url:
            return
        metadata: dict[str, Any] = {}
        try:
            metadata["source_domain"] = parse.urlparse(url).hostname or ""
        except Exception:
            metadata["source_domain"] = ""
        self.results.append(SearchResultItem(title=title, url=url, snippet=snippet, metadata=metadata))


def _parse_browser_provider_payload(stdout_text: str) -> dict[str, Any]:
    text = str(stdout_text or "").strip()
    if not text:
        raise SearchProviderError("Browser Google search returned empty stdout.")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SearchProviderError(f"Browser Google search returned invalid JSON: {text[:200]}") from exc


def _format_browser_search_failure(stderr_text: str) -> str:
    text = str(stderr_text or "").strip()
    if not text:
        return "Browser Google search failed: unknown error"
    missing_library = _extract_missing_shared_library(text)
    if missing_library:
        message = f"Browser Google search failed because Chromium is missing shared library `{missing_library}`."
        hinted_package = _SHARED_LIBRARY_PACKAGE_HINTS.get(missing_library)
        if hinted_package:
            message += f" Install it first, for example on Debian/Ubuntu: `sudo apt-get install -y {hinted_package}`."
        else:
            message += " Install the corresponding system package before retrying."
        message += " If this machine stays minimal, move the browser-search lane to a fuller Linux/server environment."
        return message
    if "Host system is missing dependencies" in text or "Missing libraries:" in text:
        return (
            "Browser Google search failed because Playwright/Chromium host dependencies are missing. "
            "Install the required system libraries, for example on Debian/Ubuntu: "
            "`sudo apt-get install -y libnspr4 libnss3`, or move this lane to a fuller Linux/server environment."
        )
    return f"Browser Google search failed: {text}"


def _extract_missing_shared_library(stderr_text: str) -> str:
    patterns = (
        r"error while loading shared libraries:\s*(lib[^\s:]+\.so(?:\.\d+)*)",
        r"cannot open shared object file:\s*(lib[^\s:]+\.so(?:\.\d+)*)",
        r"Missing libraries:\s*(lib[^\s,]+\.so(?:\.\d+)*)",
    )
    for pattern in patterns:
        match = re.search(pattern, stderr_text, re.IGNORECASE)
        if match:
            return str(match.group(1)).strip()
    generic_match = re.search(r"(lib[^\s:]+\.so(?:\.\d+)*)", stderr_text)
    if generic_match:
        return str(generic_match.group(1)).strip()
    return ""


def _timestamp_within_seconds(value: str, seconds: int) -> bool:
    normalized = str(value or "").strip()
    if not normalized:
        return False
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds() < max(0, int(seconds or 0))
