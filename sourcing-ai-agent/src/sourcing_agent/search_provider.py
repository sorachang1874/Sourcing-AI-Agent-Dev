from __future__ import annotations

import base64
import json
import os
import re
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib import parse

import requests

from .dataforseo_client import (
    DATAFORSEO_OK_TASK_STATUS_CODES,
    DATAFORSEO_PENDING_TASK_STATUS_CODES,
    MAX_TASK_POST_BATCH_SIZE,
    DataForSeoGoogleOrganicClient,
    build_google_organic_task,
    dataforseo_task_error,
    extract_google_organic_ready_task_ids,
    extract_google_organic_result_block,
    extract_google_organic_task_ids,
)
from .runtime_environment import assert_live_provider_access_allowed, external_provider_mode
from .runtime_tuning import (
    apply_runtime_timing_overrides_to_search_state,
    resolved_lane_fetch_cooldown_seconds,
    resolved_lane_ready_cooldown_seconds,
    resolved_task_get_batch_workers,
)
from .scripted_provider_scenario import (
    advance_scripted_phase_round,
    find_scripted_rule_in_scenario,
    load_scripted_provider_scenario,
    record_scripted_provider_invocation,
    scripted_pending_rounds,
    scripted_phase_error,
    scripted_rule_artifacts,
    scripted_sleep,
)
from .settings import SearchProviderSettings
from .web_fetch import DEFAULT_HEADERS, fetch_search_results_html

_SHARED_LIBRARY_PACKAGE_HINTS = {
    "libnspr4.so": "libnspr4",
    "libnss3.so": "libnss3",
}
MODEL_NATIVE_SEARCH_PROVIDER_NAME = "model_native_search"
def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _nonnegative_env_int(name: str, default: int) -> int:
    return max(0, _env_int(name, default))

def _default_lane_ready_cooldown_seconds() -> int:
    return _nonnegative_env_int("WEB_SEARCH_READY_COOLDOWN_SECONDS", 15)


def _default_lane_fetch_cooldown_seconds() -> int:
    return _nonnegative_env_int("WEB_SEARCH_FETCH_COOLDOWN_SECONDS", 15)


def _default_dataforseo_task_get_batch_workers() -> int:
    return max(1, _env_int("DATAFORSEO_TASK_GET_BATCH_WORKERS", 8))


def _dataforseo_batch_item_retry_count() -> int:
    return max(0, _env_int("DATAFORSEO_BATCH_ITEM_RETRY_COUNT", 1))


def _dataforseo_error_message_retryable(message: str) -> bool:
    normalized = str(message or "").lower()
    if any(token in normalized for token in ("status_code=40800", "status_code=42900")):
        return True
    match = re.search(r"status_code=(\d+)", normalized)
    if match:
        try:
            return int(match.group(1)) >= 50000
        except ValueError:
            return False
    return any(token in normalized for token in ("timeout", "temporar", "rate limit", "connection"))


def _dataforseo_status_code_from_error(message: str) -> int:
    match = re.search(r"status_code=(\d+)", str(message or ""))
    if not match:
        return 0
    try:
        return int(match.group(1))
    except ValueError:
        return 0


def _dataforseo_status_message_from_error(message: str) -> str:
    text = str(message or "").strip()
    marker = ":"
    if marker not in text:
        return text
    return text.rsplit(marker, 1)[-1].strip()


def _external_provider_mode() -> str:
    return external_provider_mode()


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


def _require_batch_task_key(spec: dict[str, Any], *, provider_name: str, operation: str) -> str:
    task_key = str((spec or {}).get("task_key") or "").strip()
    if task_key:
        return task_key
    raise SearchProviderError(
        f"{provider_name}.{operation} requires stable task_key/query_identity_key; "
        "batch query identity must be assigned by the caller and must not fall back to request order, query text, or task_id."
    )


def _require_batch_task_keys(query_specs: list[dict[str, Any]], *, provider_name: str, operation: str) -> None:
    for spec in list(query_specs or []):
        _require_batch_task_key(dict(spec or {}), provider_name=provider_name, operation=operation)


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


class OfflineSearchProvider(BaseSearchProvider):
    provider_name = "offline_search"

    def __init__(self, *, mode: str) -> None:
        normalized_mode = str(mode or "simulate").strip().lower() or "simulate"
        self.mode = normalized_mode if normalized_mode in {"simulate", "replay"} else "simulate"

    def _build_response(self, query_text: str) -> SearchResponse:
        source_label = "simulated_search_provider" if self.mode == "simulate" else "replay_search_provider"
        note = (
            "Simulated search response; no external provider request was sent."
            if self.mode == "simulate"
            else "Replay search response returned without live provider access."
        )
        return SearchResponse(
            provider_name=self.provider_name,
            query_text=query_text,
            results=[],
            raw_payload={"provider_mode": self.mode, "results": []},
            raw_format="json",
            final_url="",
            content_type="application/json",
            metadata={"provider_mode": self.mode, "source_label": source_label, "note": note},
        )

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:  # noqa: ARG002
        return self._build_response(query_text)

    def execute_with_checkpoint(
        self,
        query_text: str,
        *,
        max_results: int = 10,
        timeout: int | None = None,
        checkpoint: dict[str, Any] | None = None,
    ) -> SearchExecutionResult:
        del max_results, timeout
        response = self._build_response(query_text)
        prior_checkpoint = dict(checkpoint or {})
        return SearchExecutionResult(
            provider_name=self.provider_name,
            query_text=query_text,
            response=response,
            checkpoint={
                **prior_checkpoint,
                "provider_name": self.provider_name,
                "status": "completed",
                "provider_mode": self.mode,
            },
            message=response.metadata.get("note", ""),
            artifacts=[
                SearchExecutionArtifact(
                    label=f"{self.mode}_search_execution",
                    payload={"query_text": query_text, "provider_mode": self.mode, "results": []},
                    metadata={"provider_mode": self.mode, "provider_name": self.provider_name},
                )
            ],
        )

    def submit_batch_queries(self, query_specs: list[dict[str, Any]]) -> SearchBatchSubmissionResult | None:
        tasks: list[SearchBatchSubmissionTask] = []
        for index, spec in enumerate(list(query_specs or []), start=1):
            query_text = " ".join(str((spec or {}).get("query_text") or "").split()).strip()
            if not query_text:
                continue
            task_key = _require_batch_task_key(spec, provider_name=self.provider_name, operation="submit_batch_queries")
            checkpoint = apply_runtime_timing_overrides_to_search_state(
                {
                    "provider_name": self.provider_name,
                    "provider_mode": self.mode,
                    "task_id": f"{self.mode}_task_{index:04d}",
                    "status": "submitted",
                },
                runtime_timing_overrides=dict((spec or {}).get("runtime_timing_overrides") or {}),
            )
            tasks.append(
                SearchBatchSubmissionTask(
                    task_key=task_key,
                    query_text=query_text,
                    checkpoint=checkpoint,
                    metadata={"provider_mode": self.mode},
                )
            )
        if not tasks:
            return None
        return SearchBatchSubmissionResult(
            provider_name=self.provider_name,
            tasks=tasks,
            artifacts=[
                SearchExecutionArtifact(
                    label=f"{self.mode}_search_batch_submit",
                    payload={"provider_mode": self.mode, "task_count": len(tasks)},
                    metadata={"provider_mode": self.mode, "provider_name": self.provider_name},
                )
            ],
            message=(
                "Simulated batch search submission completed without external requests."
                if self.mode == "simulate"
                else "Replay batch search submission completed without live provider access."
            ),
        )

    def poll_ready_batch(self, query_specs: list[dict[str, Any]]) -> SearchBatchReadyResult | None:
        tasks: list[SearchBatchReadyTask] = []
        for spec in list(query_specs or []):
            checkpoint = dict((spec or {}).get("checkpoint") or {})
            query_text = str((spec or {}).get("query_text") or checkpoint.get("query_text") or "").strip()
            task_key = _require_batch_task_key(spec, provider_name=self.provider_name, operation="poll_ready_batch")
            task_id = str((spec or {}).get("task_id") or checkpoint.get("task_id") or "").strip()
            tasks.append(
                SearchBatchReadyTask(
                    task_key=task_key,
                    task_id=task_id or f"{self.mode}_task",
                    query_text=query_text,
                    checkpoint={
                        **checkpoint,
                        "provider_name": self.provider_name,
                        "provider_mode": self.mode,
                        "status": "ready_cached",
                    },
                    metadata={"ready": True, "provider_mode": self.mode},
                )
            )
        if not tasks:
            return None
        return SearchBatchReadyResult(
            provider_name=self.provider_name,
            tasks=tasks,
            artifacts=[
                SearchExecutionArtifact(
                    label=f"{self.mode}_search_batch_ready",
                    payload={"provider_mode": self.mode, "task_count": len(tasks), "ready_count": len(tasks)},
                    metadata={"provider_mode": self.mode, "provider_name": self.provider_name},
                )
            ],
            message="All offline search tasks are ready.",
        )

    def fetch_ready_batch(self, query_specs: list[dict[str, Any]]) -> SearchBatchFetchResult | None:
        tasks: list[SearchBatchFetchTask] = []
        for spec in list(query_specs or []):
            checkpoint = dict((spec or {}).get("checkpoint") or {})
            query_text = str((spec or {}).get("query_text") or checkpoint.get("query_text") or "").strip()
            task_key = _require_batch_task_key(spec, provider_name=self.provider_name, operation="fetch_ready_batch")
            task_id = str((spec or {}).get("task_id") or checkpoint.get("task_id") or "").strip()
            tasks.append(
                SearchBatchFetchTask(
                    task_key=task_key,
                    task_id=task_id or f"{self.mode}_task",
                    query_text=query_text,
                    response=self._build_response(query_text),
                    checkpoint={
                        **checkpoint,
                        "provider_name": self.provider_name,
                        "provider_mode": self.mode,
                        "status": "fetched_cached",
                    },
                    metadata={"fetched": True, "provider_mode": self.mode},
                )
            )
        if not tasks:
            return None
        return SearchBatchFetchResult(
            provider_name=self.provider_name,
            tasks=tasks,
            artifacts=[
                SearchExecutionArtifact(
                    label=f"{self.mode}_search_batch_fetch",
                    payload={"provider_mode": self.mode, "task_count": len(tasks)},
                    metadata={"provider_mode": self.mode, "provider_name": self.provider_name},
                )
            ],
            message="Offline search batch fetch completed.",
        )


class ScriptedSearchProvider(BaseSearchProvider):
    provider_name = "scripted_search"

    def __init__(self, scenario: dict[str, Any] | None = None) -> None:
        self.mode = "scripted"
        self.scenario = dict(load_scripted_provider_scenario() if scenario is None else scenario)

    def _rule_for(self, *, query_text: str = "", task_key: str = "", phase: str = "", checkpoint: dict[str, Any] | None = None) -> dict[str, Any]:
        checkpoint = dict(checkpoint or {})
        rule_name = str(checkpoint.get("scripted_rule_name") or "").strip()
        if rule_name:
            rule = find_scripted_rule_in_scenario(
                self.scenario,
                "search",
                context={
                    "query_text": query_text,
                    "task_key": task_key,
                    "phase": phase,
                    "checkpoint": checkpoint,
                    "provider_name": self.provider_name,
                    "scripted_rule_name": rule_name,
                    "context_contains": [rule_name],
                },
            )
            if rule:
                return rule
        return find_scripted_rule_in_scenario(
            self.scenario,
            "search",
            context={
                "query_text": query_text,
                "task_key": task_key,
                "phase": phase,
                "checkpoint": checkpoint,
                "provider_name": self.provider_name,
            },
        )

    def _build_response(self, query_text: str, *, rule: dict[str, Any] | None = None) -> SearchResponse:
        rule = dict(rule or {})
        results_payload = [
            *_scripted_search_result_templates(query_text, list(rule.get("result_templates") or [])),
            *list(rule.get("results") or []),
        ]
        results: list[SearchResultItem] = []
        for item in results_payload:
            if not isinstance(item, dict):
                continue
            results.append(
                SearchResultItem(
                    title=str(item.get("title") or "").strip(),
                    url=str(item.get("url") or "").strip(),
                    snippet=str(item.get("snippet") or "").strip(),
                    metadata=dict(item.get("metadata") or {}),
                )
            )
        note = str(rule.get("message") or "Scripted search response returned without live provider access.").strip()
        raw_payload = rule.get("raw_payload")
        if not isinstance(raw_payload, dict):
            raw_payload = {
                "provider_mode": self.mode,
                "query_text": query_text,
                "results": [item.to_record() for item in results],
            }
        return SearchResponse(
            provider_name=self.provider_name,
            query_text=query_text,
            results=results,
            raw_payload=raw_payload,
            raw_format="json",
            final_url="",
            content_type="application/json",
            metadata={"provider_mode": self.mode, "note": note, "scripted_rule_name": str(rule.get("_rule_name") or "")},
        )

    def healthcheck(self) -> dict[str, Any]:
        return {
            "provider": self.provider_name,
            "status": "ready",
            "provider_mode": self.mode,
            "note": "Scripted external-provider mode is active.",
        }

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:  # noqa: ARG002
        rule = self._rule_for(query_text=query_text, phase="execute")
        record_scripted_provider_invocation(
            provider_name=self.provider_name,
            dispatch_kind="search.execute",
            query_text=query_text,
            payload={"query_text": query_text, "max_results": max_results},
            metadata={"phase": "execute", "scripted_rule_name": str(rule.get("_rule_name") or "")},
        )
        scripted_sleep(rule, phase="execute")
        return self._build_response(query_text, rule=rule)

    def execute_with_checkpoint(
        self,
        query_text: str,
        *,
        max_results: int = 10,
        timeout: int | None = None,
        checkpoint: dict[str, Any] | None = None,
    ) -> SearchExecutionResult:
        del max_results, timeout
        rule = self._rule_for(query_text=query_text, phase="execute", checkpoint=checkpoint)
        record_scripted_provider_invocation(
            provider_name=self.provider_name,
            dispatch_kind="search.execute_with_checkpoint",
            query_text=query_text,
            payload={"query_text": query_text},
            metadata={
                "phase": "execute",
                "scripted_rule_name": str(rule.get("_rule_name") or ""),
                "checkpoint_status": str(dict(checkpoint or {}).get("status") or ""),
            },
        )
        scripted_sleep(rule, phase="execute")
        updated_checkpoint, round_number = advance_scripted_phase_round(checkpoint, phase="execute")
        error_spec = scripted_phase_error(rule, phase="execute", round_number=round_number)
        if error_spec:
            raise RuntimeError(str(error_spec.get("message") or f"Scripted search {error_spec.get('kind') or 'error'}"))
        pending_rounds = scripted_pending_rounds(rule, phase="execute")
        if round_number <= pending_rounds:
            updated_checkpoint.update(
                {
                    "provider_name": self.provider_name,
                    "provider_mode": self.mode,
                    "status": "submitted",
                    "task_id": str(rule.get("task_id") or f"scripted_search_execute_{round_number:04d}"),
                    "scripted_rule_name": str(rule.get("_rule_name") or ""),
                }
            )
            return SearchExecutionResult(
                provider_name=self.provider_name,
                query_text=query_text,
                checkpoint=updated_checkpoint,
                pending=True,
                message=str(rule.get("pending_message") or "Scripted search task is still pending."),
                artifacts=[
                    SearchExecutionArtifact(
                        label="scripted_search_pending",
                        payload={"query_text": query_text, "round": round_number, "rule": str(rule.get("_rule_name") or "")},
                        metadata={"provider_mode": self.mode},
                    )
                ],
            )
        response = self._build_response(query_text, rule=rule)
        updated_checkpoint.update(
            {
                "provider_name": self.provider_name,
                "provider_mode": self.mode,
                "status": "completed",
                "task_id": str(rule.get("task_id") or f"scripted_search_execute_{round_number:04d}"),
                "scripted_rule_name": str(rule.get("_rule_name") or ""),
            }
        )
        return SearchExecutionResult(
            provider_name=self.provider_name,
            query_text=query_text,
            response=response,
            checkpoint=updated_checkpoint,
            message=response.metadata.get("note", ""),
            artifacts=[
                SearchExecutionArtifact(
                    label="scripted_search_execution",
                    payload={"query_text": query_text, "round": round_number, "rule": str(rule.get("_rule_name") or "")},
                    metadata={"provider_mode": self.mode},
                )
            ],
        )

    def submit_batch_queries(self, query_specs: list[dict[str, Any]]) -> SearchBatchSubmissionResult | None:
        tasks: list[SearchBatchSubmissionTask] = []
        artifacts: list[SearchExecutionArtifact] = []
        for index, spec in enumerate(list(query_specs or []), start=1):
            query_text = " ".join(str((spec or {}).get("query_text") or "").split()).strip()
            if not query_text:
                continue
            task_key = _require_batch_task_key(spec, provider_name=self.provider_name, operation="submit_batch_queries")
            rule = self._rule_for(query_text=query_text, task_key=task_key, phase="submit")
            record_scripted_provider_invocation(
                provider_name=self.provider_name,
                dispatch_kind="search.batch_submit",
                query_text=query_text,
                task_key=task_key,
                payload={"query_text": query_text, "task_key": task_key},
                metadata={"phase": "submit", "scripted_rule_name": str(rule.get("_rule_name") or ""), "batch_index": index},
            )
            scripted_sleep(rule, phase="submit")
            error_spec = scripted_phase_error(rule, phase="submit", round_number=1)
            if error_spec:
                raise RuntimeError(str(error_spec.get("message") or f"Scripted search {error_spec.get('kind') or 'error'}"))
            checkpoint = apply_runtime_timing_overrides_to_search_state(
                {
                    "provider_name": self.provider_name,
                    "provider_mode": self.mode,
                    "task_id": str(rule.get("task_id") or f"scripted_task_{index:04d}"),
                    "status": "submitted",
                    "scripted_rule_name": str(rule.get("_rule_name") or ""),
                },
                runtime_timing_overrides=dict((spec or {}).get("runtime_timing_overrides") or {}),
            )
            tasks.append(
                SearchBatchSubmissionTask(
                    task_key=task_key,
                    query_text=query_text,
                    checkpoint=checkpoint,
                    metadata={"provider_mode": self.mode, "scripted_rule_name": str(rule.get("_rule_name") or "")},
                )
            )
            for artifact in scripted_rule_artifacts(rule, phase="submit"):
                artifacts.append(
                    SearchExecutionArtifact(
                        label=str(artifact.get("label") or "scripted_search_submit_artifact"),
                        payload=artifact.get("payload"),
                        raw_format=str(artifact.get("raw_format") or "json"),
                        content_type=str(artifact.get("content_type") or "application/json"),
                        metadata=dict(artifact.get("metadata") or {}),
                    )
                )
        if not tasks:
            return None
        artifacts.append(
            SearchExecutionArtifact(
                label="scripted_search_batch_submit",
                payload={"provider_mode": self.mode, "task_count": len(tasks)},
                metadata={"provider_mode": self.mode},
            )
        )
        return SearchBatchSubmissionResult(
            provider_name=self.provider_name,
            tasks=tasks,
            artifacts=artifacts,
            message="Scripted batch search submission completed without live provider access.",
        )

    def poll_ready_batch(self, query_specs: list[dict[str, Any]]) -> SearchBatchReadyResult | None:
        tasks: list[SearchBatchReadyTask] = []
        artifacts: list[SearchExecutionArtifact] = []
        for spec in list(query_specs or []):
            checkpoint = dict((spec or {}).get("checkpoint") or {})
            query_text = str((spec or {}).get("query_text") or checkpoint.get("query_text") or "").strip()
            task_key = _require_batch_task_key(spec, provider_name=self.provider_name, operation="poll_ready_batch")
            task_id = str((spec or {}).get("task_id") or checkpoint.get("task_id") or "").strip()
            rule = self._rule_for(query_text=query_text, task_key=task_key, phase="poll", checkpoint=checkpoint)
            scripted_sleep(rule, phase="poll")
            updated_checkpoint, round_number = advance_scripted_phase_round(checkpoint, phase="poll")
            error_spec = scripted_phase_error(rule, phase="poll", round_number=round_number)
            if error_spec:
                raise RuntimeError(str(error_spec.get("message") or f"Scripted search {error_spec.get('kind') or 'error'}"))
            pending_rounds = scripted_pending_rounds(rule, phase="poll")
            status = "ready_cached" if round_number > pending_rounds else "waiting_for_ready_cached"
            updated_checkpoint.update(
                {
                    "provider_name": self.provider_name,
                    "provider_mode": self.mode,
                    "status": status,
                    "task_id": task_id or str(rule.get("task_id") or "scripted_task"),
                    "scripted_rule_name": str(rule.get("_rule_name") or checkpoint.get("scripted_rule_name") or ""),
                }
            )
            tasks.append(
                SearchBatchReadyTask(
                    task_key=task_key,
                    task_id=task_id or str(rule.get("task_id") or "scripted_task"),
                    query_text=query_text,
                    checkpoint=updated_checkpoint,
                    metadata={"ready": status == "ready_cached", "provider_mode": self.mode},
                )
            )
            for artifact in scripted_rule_artifacts(rule, phase="poll"):
                artifacts.append(
                    SearchExecutionArtifact(
                        label=str(artifact.get("label") or "scripted_search_poll_artifact"),
                        payload=artifact.get("payload"),
                        raw_format=str(artifact.get("raw_format") or "json"),
                        content_type=str(artifact.get("content_type") or "application/json"),
                        metadata=dict(artifact.get("metadata") or {}),
                    )
                )
        if not tasks:
            return None
        artifacts.append(
            SearchExecutionArtifact(
                label="scripted_search_batch_ready",
                payload={"provider_mode": self.mode, "task_count": len(tasks)},
                metadata={"provider_mode": self.mode},
            )
        )
        return SearchBatchReadyResult(
            provider_name=self.provider_name,
            tasks=tasks,
            artifacts=artifacts,
            message="Scripted search batch ready poll completed.",
        )

    def fetch_ready_batch(self, query_specs: list[dict[str, Any]]) -> SearchBatchFetchResult | None:
        tasks: list[SearchBatchFetchTask] = []
        artifacts: list[SearchExecutionArtifact] = []
        for spec in list(query_specs or []):
            checkpoint = dict((spec or {}).get("checkpoint") or {})
            query_text = str((spec or {}).get("query_text") or checkpoint.get("query_text") or "").strip()
            task_key = _require_batch_task_key(spec, provider_name=self.provider_name, operation="fetch_ready_batch")
            task_id = str((spec or {}).get("task_id") or checkpoint.get("task_id") or "").strip()
            rule = self._rule_for(query_text=query_text, task_key=task_key, phase="fetch", checkpoint=checkpoint)
            scripted_sleep(rule, phase="fetch")
            error_spec = scripted_phase_error(rule, phase="fetch", round_number=1)
            if error_spec:
                raise RuntimeError(str(error_spec.get("message") or f"Scripted search {error_spec.get('kind') or 'error'}"))
            response = self._build_response(query_text, rule=rule)
            tasks.append(
                SearchBatchFetchTask(
                    task_key=task_key,
                    task_id=task_id or str(rule.get("task_id") or "scripted_task"),
                    query_text=query_text,
                    response=response,
                    checkpoint={
                        **checkpoint,
                        "provider_name": self.provider_name,
                        "provider_mode": self.mode,
                        "status": "fetched_cached",
                        "scripted_rule_name": str(rule.get("_rule_name") or checkpoint.get("scripted_rule_name") or ""),
                    },
                    metadata={"fetched": True, "provider_mode": self.mode},
                )
            )
            for artifact in scripted_rule_artifacts(rule, phase="fetch"):
                artifacts.append(
                    SearchExecutionArtifact(
                        label=str(artifact.get("label") or "scripted_search_fetch_artifact"),
                        payload=artifact.get("payload"),
                        raw_format=str(artifact.get("raw_format") or "json"),
                        content_type=str(artifact.get("content_type") or "application/json"),
                        metadata=dict(artifact.get("metadata") or {}),
                    )
                )
        if not tasks:
            return None
        artifacts.append(
            SearchExecutionArtifact(
                label="scripted_search_batch_fetch",
                payload={"provider_mode": self.mode, "task_count": len(tasks)},
                metadata={"provider_mode": self.mode},
            )
        )
        return SearchBatchFetchResult(
            provider_name=self.provider_name,
            tasks=tasks,
            artifacts=artifacts,
            message="Scripted search batch fetch completed.",
        )


def _scripted_search_result_templates(query_text: str, templates: list[Any]) -> list[dict[str, Any]]:
    if not templates:
        return []
    query_values = _scripted_search_query_values(query_text)
    rendered: list[dict[str, Any]] = []
    for template in templates:
        if not isinstance(template, dict):
            continue
        rendered_item: dict[str, Any] = {}
        for key in ("title", "url", "snippet"):
            rendered_item[key] = _render_scripted_search_template_value(template.get(key), query_values)
        metadata = template.get("metadata")
        if isinstance(metadata, dict):
            rendered_item["metadata"] = {
                str(key): _render_scripted_search_template_value(value, query_values)
                for key, value in metadata.items()
            }
        rendered.append(rendered_item)
    return rendered


def _scripted_search_query_values(query_text: str) -> dict[str, str]:
    normalized_query = " ".join(str(query_text or "").split()).strip()
    quoted_terms = [item.strip() for item in re.findall(r'"([^"]+)"', normalized_query) if item.strip()]
    candidate_name = quoted_terms[0] if quoted_terms else _scripted_search_query_name_guess(normalized_query)
    company = quoted_terms[1] if len(quoted_terms) > 1 else _scripted_search_query_company_guess(normalized_query)
    name_slug = _scripted_search_slug(candidate_name or "candidate")
    company_slug = _scripted_search_slug(company or "company")
    return {
        "query_text": normalized_query,
        "candidate_name": candidate_name or "Candidate",
        "candidate_slug": name_slug,
        "company": company or "Company",
        "company_slug": company_slug,
    }


def _scripted_search_query_name_guess(query_text: str) -> str:
    normalized = re.sub(r"\s+", " ", str(query_text or "")).strip()
    if not normalized:
        return "Candidate"
    cleaned = re.sub(
        r"\b(site|homepage|personal website|personal site|github|scholar|citations|arxiv|email|contact)\b.*$",
        "",
        normalized,
        flags=re.IGNORECASE,
    ).strip()
    tokens = [token for token in re.findall(r"[A-Za-z][A-Za-z'.-]*", cleaned) if token.lower() not in {"or", "and"}]
    if len(tokens) >= 2:
        return " ".join(tokens[:2])
    return " ".join(tokens) or "Candidate"


def _scripted_search_query_company_guess(query_text: str) -> str:
    quoted_terms = [item.strip() for item in re.findall(r'"([^"]+)"', str(query_text or "")) if item.strip()]
    if len(quoted_terms) >= 2:
        return quoted_terms[1]
    return "Company"


def _scripted_search_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return slug or "scripted"


def _render_scripted_search_template_value(value: Any, query_values: dict[str, str]) -> str:
    raw = str(value or "")
    if not raw:
        return ""
    try:
        return raw.format(**query_values)
    except (KeyError, ValueError):
        return raw


class DuckDuckGoHtmlSearchProvider(BaseSearchProvider):
    provider_name = "duckduckgo_html"

    def __init__(self, *, timeout_seconds: int = 30) -> None:
        self.timeout_seconds = timeout_seconds

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        assert_live_provider_access_allowed(
            provider_name=self.provider_name,
            operation="search",
            payload={"query": query_text},
        )
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
        assert_live_provider_access_allowed(
            provider_name=self.provider_name,
            operation="search",
            payload={"query": query_text},
        )
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
        assert_live_provider_access_allowed(
            provider_name=self.provider_name,
            operation="search",
            payload={"query": query_text},
        )
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

    def _build_queue_checkpoint(
        self,
        *,
        query_text: str,
        depth: int,
        task_id: str,
        status: str,
        reference: dict[str, Any] | None = None,
        runtime_timing_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        checkpoint = {
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
        checkpoint = apply_runtime_timing_overrides_to_search_state(
            checkpoint,
            runtime_timing_overrides=runtime_timing_overrides,
        )
        if isinstance(reference, dict) and reference:
            checkpoint = self._overlay_ready_metadata(checkpoint, reference)
        return checkpoint

    def _overlay_ready_metadata(self, checkpoint: dict[str, Any], reference: dict[str, Any]) -> dict[str, Any]:
        updated = dict(checkpoint or {})
        for key in [
            "runtime_tuning_profile",
            "lane_ready_cooldown_seconds",
            "ready_poll_token",
            "ready_checked_at",
            "ready_attempted_at",
            "ready_poll_source",
            "ready_poll_label",
            "fetch_attempted_at",
            "fetched_at",
            "fetch_token",
            "lane_fetch_cooldown_seconds",
            "task_get_batch_workers",
        ]:
            value = (reference or {}).get(key)
            if value not in (None, "", [], {}, ()):
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
            task_key = _require_batch_task_key(spec, provider_name=self.provider_name, operation="submit_batch_queries")
            depth = max(self.depth, max_results)
            normalized_specs.append(
                {
                    "task_key": task_key,
                    "query_text": query_text,
                    "max_results": max_results,
                    "depth": depth,
                    "runtime_timing_overrides": dict((spec or {}).get("runtime_timing_overrides") or {}),
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

        task_order = {
            str(spec["task_key"]): index
            for index, spec in enumerate(normalized_specs)
        }
        submitted_tasks: list[SearchBatchSubmissionTask] = []
        artifacts: list[SearchExecutionArtifact] = []
        max_item_retries = _dataforseo_batch_item_retry_count()

        def _submission_task(
            *,
            spec: dict[str, Any],
            task_payload: dict[str, Any],
            echoed: dict[str, str],
            artifact_label: str,
            batch_index: int,
            retry_attempt: int = 0,
            error_override: str = "",
            response_mapping_source: str = "",
        ) -> SearchBatchSubmissionTask:
            task_id = str(echoed.get("task_id") or task_payload.get("id") or "").strip()
            error = dataforseo_task_error(task_payload) if task_payload else {
                "status_code": 0,
                "status_message": "DataForSEO task response missing from batch payload.",
                "retryable": True,
            }
            if error_override:
                error = {
                    "status_code": int(error.get("status_code") or 0),
                    "status_message": error_override,
                    "retryable": bool(error.get("retryable")) or _dataforseo_error_message_retryable(error_override),
                }
            submitted = bool(task_id) and not error
            status = "submitted" if submitted else (
                "submit_failed_retryable" if bool(error.get("retryable")) else "submit_failed_terminal"
            )
            checkpoint = self._build_queue_checkpoint(
                query_text=str(spec["query_text"]),
                depth=int(spec["depth"]),
                task_id=task_id,
                status=status,
                runtime_timing_overrides=dict(spec.get("runtime_timing_overrides") or {}),
            )
            if error:
                checkpoint.update(
                    {
                        "error": str(error.get("status_message") or "DataForSEO task submit failed.").strip(),
                        "status_code": int(error.get("status_code") or 0),
                        "retryable": bool(error.get("retryable")),
                        "retry_unit": "search_query",
                        "retry_strategy": "dataforseo_batch_failed_query_retry_only",
                        "batch_item_retry_attempt": int(retry_attempt),
                    }
                )
            return SearchBatchSubmissionTask(
                task_key=str(spec["task_key"]),
                query_text=str(spec["query_text"]),
                checkpoint=checkpoint,
                metadata={
                    "artifact_label": artifact_label,
                    "batch_index": batch_index,
                    "query_identity_key": str(spec["task_key"]),
                    "response_mapping_source": str(response_mapping_source or ""),
                    "task_id": task_id,
                    "submitted": submitted,
                    "failed": not submitted,
                    "retryable": bool(error.get("retryable")) if error else False,
                    "retry_unit": "search_query",
                    "retry_strategy": "dataforseo_batch_failed_query_retry_only",
                    "batch_item_retry_attempt": int(retry_attempt),
                    "error": str(error.get("status_message") or "").strip() if error else "",
                    "status_code": int(error.get("status_code") or 0) if error else 0,
                },
            )

        def _append_submission_tasks_from_payload(
            *,
            batch_specs: list[dict[str, Any]],
            payload: dict[str, Any],
            artifact_label: str,
            batch_index: int,
            retry_attempt: int = 0,
            collect_retryable_failures: bool = False,
        ) -> list[dict[str, Any]]:
            retryable_failures: list[dict[str, Any]] = []
            payload_tasks = list(payload.get("tasks") or [])
            payload_by_task_key: dict[str, tuple[dict[str, Any], dict[str, str], str]] = {}
            payload_by_keyword: dict[str, tuple[dict[str, Any], dict[str, str], str]] = {}
            for raw_task_payload in payload_tasks:
                task_payload = dict(raw_task_payload or {})
                data = dict(task_payload.get("data") or {})
                # Do not use request-order fallback for identity. DataForSEO
                # batch ordering is a transport detail; semantic joins must use
                # the provider-echoed tag/keyword or fail closed to item retry.
                echoed = {
                    "task_id": str(task_payload.get("id") or "").strip(),
                    "keyword": " ".join(str(data.get("keyword") or "").split()).strip(),
                    "tag": str(data.get("tag") or "").strip(),
                }
                tag = str(echoed.get("tag") or "").strip()
                keyword = str(echoed.get("keyword") or "").strip()
                if tag:
                    payload_by_task_key[tag] = (task_payload, echoed, "provider_data_tag")
                if keyword and keyword not in payload_by_keyword:
                    payload_by_keyword[keyword] = (task_payload, echoed, "provider_data_keyword")
            for spec in batch_specs:
                task_key = str(spec.get("task_key") or "").strip()
                query_text = " ".join(str(spec.get("query_text") or "").split()).strip()
                mapped = payload_by_task_key.get(task_key) or payload_by_keyword.get(query_text)
                if mapped is not None:
                    task_payload, echoed, mapping_source = mapped
                else:
                    task_payload = {}
                    echoed = {}
                    mapping_source = "missing_provider_identity"
                task_error = dataforseo_task_error(task_payload) if task_payload else {
                    "status_message": "DataForSEO task response missing provider-echoed query identity.",
                    "retryable": True,
                }
                if (
                    collect_retryable_failures
                    and bool(task_error)
                    and bool(task_error.get("retryable"))
                    and int(retry_attempt) < max_item_retries
                ):
                    retryable_failures.append(spec)
                    continue
                submitted_tasks.append(
                    _submission_task(
                        spec=spec,
                        task_payload=task_payload,
                        echoed=echoed,
                        artifact_label=artifact_label,
                        batch_index=batch_index,
                        retry_attempt=retry_attempt,
                        error_override=(
                            "DataForSEO task response missing provider-echoed query identity."
                            if mapping_source == "missing_provider_identity"
                            else ""
                        ),
                        response_mapping_source=mapping_source,
                    )
                )
            return retryable_failures

        for batch_index, start in enumerate(range(0, len(normalized_specs), MAX_TASK_POST_BATCH_SIZE), start=1):
            batch_specs = normalized_specs[start : start + MAX_TASK_POST_BATCH_SIZE]
            batch_tasks = [dict(item["task"]) for item in batch_specs]
            payload = self.client.task_post_many(batch_tasks, allow_partial_task_errors=True)
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
            retryable_specs = _append_submission_tasks_from_payload(
                batch_specs=batch_specs,
                payload=payload,
                artifact_label=artifact_label,
                batch_index=batch_index,
                collect_retryable_failures=True,
            )
            if retryable_specs:
                retry_tasks = [dict(item["task"]) for item in retryable_specs]
                retry_artifact_label = f"{artifact_label}_retry_01"
                try:
                    retry_payload = self.client.task_post_many(
                        retry_tasks,
                        allow_partial_task_errors=True,
                    )
                except Exception as exc:
                    retry_payload = {
                        "status_code": 0,
                        "status_message": str(exc),
                        "tasks": [],
                    }
                    for retry_spec in retryable_specs:
                        submitted_tasks.append(
                            _submission_task(
                                spec=retry_spec,
                                task_payload={},
                                echoed={},
                                artifact_label=retry_artifact_label,
                                batch_index=batch_index,
                                retry_attempt=1,
                                error_override=str(exc),
                                response_mapping_source="retry_exception",
                            )
                        )
                else:
                    _append_submission_tasks_from_payload(
                        batch_specs=retryable_specs,
                        payload=retry_payload,
                        artifact_label=retry_artifact_label,
                        batch_index=batch_index,
                        retry_attempt=1,
                    )
                artifacts.append(
                    SearchExecutionArtifact(
                        label=retry_artifact_label,
                        payload=retry_payload,
                        metadata={
                            "batch_index": batch_index,
                            "task_count": len(retryable_specs),
                            "provider_name": self.provider_name,
                            "retry_unit": "search_query",
                            "retry_strategy": "dataforseo_batch_failed_query_retry_only",
                            "retry_attempt": 1,
                        },
                    )
                )
        ordered_submitted_tasks = sorted(
            submitted_tasks,
            key=lambda task: task_order.get(str(task.task_key), len(task_order)),
        )
        return SearchBatchSubmissionResult(
            provider_name=self.provider_name,
            tasks=ordered_submitted_tasks,
            artifacts=artifacts,
            message=(
                f"Submitted {len(ordered_submitted_tasks)} DataForSEO Standard Queue tasks "
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
            task_key = _require_batch_task_key(spec, provider_name=self.provider_name, operation="poll_ready_batch")
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
        direct_probe_artifacts: list[SearchExecutionArtifact] = []
        direct_probe_metadata_by_task_id: dict[str, dict[str, Any]] = {}
        direct_probe_specs = [
            (index, spec)
            for index, spec in enumerate(normalized_specs, start=1)
            if str(spec["task_id"]) not in ready_ids
        ]

        def _direct_ready_probe(index: int, spec: dict[str, Any]) -> tuple[int, dict[str, Any], dict[str, Any] | None, str]:
            task_id = str(spec["task_id"])
            try:
                return index, spec, self.client.task_get_regular(task_id), ""
            except Exception as exc:
                return index, spec, None, str(exc)

        direct_probe_results: list[tuple[int, dict[str, Any], dict[str, Any] | None, str]] = []
        if direct_probe_specs:
            max_probe_workers = max(
                1,
                min(
                    len(direct_probe_specs),
                    resolved_task_get_batch_workers(
                        [dict(spec.get("checkpoint") or {}) for _index, spec in direct_probe_specs],
                        default=_default_dataforseo_task_get_batch_workers(),
                    ),
                ),
            )
            with ThreadPoolExecutor(max_workers=max_probe_workers) as pool:
                futures = [pool.submit(_direct_ready_probe, index, spec) for index, spec in direct_probe_specs]
                for future in as_completed(futures):
                    direct_probe_results.append(future.result())

        for index, spec, direct_payload, error_text in sorted(direct_probe_results, key=lambda item: item[0]):
            task_id = str(spec["task_id"])
            if direct_payload is None:
                provider_status_code = _dataforseo_status_code_from_error(error_text)
                provider_status_message = _dataforseo_status_message_from_error(error_text)
                wait_state = (
                    "provider_pending"
                    if provider_status_code in DATAFORSEO_PENDING_TASK_STATUS_CODES
                    else "provider_probe_error"
                )
                direct_probe_metadata_by_task_id[task_id] = {
                    "ready": False,
                    "provider_status_code": provider_status_code,
                    "provider_status_message": provider_status_message,
                    "provider_wait_state": wait_state,
                    "readiness_strategy": "dataforseo_task_get_direct_probe",
                }
                direct_probe_artifacts.append(
                    SearchExecutionArtifact(
                        label=f"task_get_ready_probe_{index:02d}_waiting",
                        payload={
                            "provider_name": self.provider_name,
                            "task_key": str(spec["task_key"]),
                            "task_id": task_id,
                            "status": "waiting",
                            "error": error_text,
                            "provider_status_code": provider_status_code,
                            "provider_status_message": provider_status_message,
                            "provider_wait_state": wait_state,
                            "readiness_strategy": "dataforseo_task_get_direct_probe",
                        },
                        metadata={
                            "provider_name": self.provider_name,
                            "task_key": str(spec["task_key"]),
                            "task_id": task_id,
                            "ready": False,
                            "provider_status_code": provider_status_code,
                            "provider_status_message": provider_status_message,
                            "provider_wait_state": wait_state,
                            "readiness_strategy": "dataforseo_task_get_direct_probe",
                        },
                    )
                )
                continue
            direct_tasks = list(direct_payload.get("tasks") or [])
            direct_task = dict(direct_tasks[0] or {}) if direct_tasks else {}
            direct_ready = int(direct_task.get("status_code") or 0) in DATAFORSEO_OK_TASK_STATUS_CODES
            if direct_ready:
                ready_ids.add(task_id)
            direct_probe_metadata_by_task_id[task_id] = {
                "ready": direct_ready,
                "provider_status_code": int(direct_task.get("status_code") or 0),
                "provider_status_message": str(direct_task.get("status_message") or "").strip(),
                "provider_wait_state": "" if direct_ready else "provider_not_ready",
                "readiness_strategy": "dataforseo_task_get_direct_probe",
            }
            direct_probe_artifacts.append(
                SearchExecutionArtifact(
                    label=f"task_get_ready_probe_{index:02d}",
                    payload=direct_payload,
                    metadata={
                        "provider_name": self.provider_name,
                        "task_key": str(spec["task_key"]),
                        "task_id": task_id,
                        "ready": direct_ready,
                        "readiness_strategy": "dataforseo_task_get_direct_probe",
                    },
                )
            )
        tasks: list[SearchBatchReadyTask] = []
        for spec in normalized_specs:
            is_ready = str(spec["task_id"]) in ready_ids
            direct_metadata = dict(direct_probe_metadata_by_task_id.get(str(spec["task_id"])) or {})
            checkpoint = self._build_queue_checkpoint(
                query_text=str(spec["query_text"]),
                depth=int(spec["depth"]),
                task_id=str(spec["task_id"]),
                status="ready_cached" if is_ready else "waiting_for_ready_cached",
                reference=dict(spec.get("checkpoint") or {}),
            )
            tasks.append(
                SearchBatchReadyTask(
                    task_key=str(spec["task_key"]),
                    task_id=str(spec["task_id"]),
                    query_text=str(spec["query_text"]),
                    checkpoint=checkpoint,
                    metadata={"ready": is_ready, **direct_metadata},
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
            ]
            + direct_probe_artifacts,
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
            task_key = _require_batch_task_key(spec, provider_name=self.provider_name, operation="fetch_ready_batch")
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

        def _fetch_task(index: int, spec: dict[str, Any]) -> tuple[int, dict[str, Any], dict[str, Any] | None, str]:
            attempts = 0
            last_error = ""
            while attempts <= _dataforseo_batch_item_retry_count():
                attempts += 1
                try:
                    payload = self.client.task_get_regular(str(spec["task_id"]))
                    return index, spec, payload, ""
                except Exception as exc:
                    last_error = str(exc)
                    if not _dataforseo_error_message_retryable(last_error):
                        break
            return index, spec, None, last_error

        fetched_payloads: list[tuple[int, dict[str, Any], dict[str, Any] | None, str]] = []
        max_workers = max(
            1,
            min(
                len(normalized_specs),
                resolved_task_get_batch_workers(
                    [dict(spec.get("checkpoint") or {}) for spec in normalized_specs],
                    default=_default_dataforseo_task_get_batch_workers(),
                ),
            ),
        )
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_fetch_task, index, spec): index
                for index, spec in enumerate(normalized_specs, start=1)
            }
            for future in as_completed(futures):
                fetched_payloads.append(future.result())

        for index, spec, payload, error_text in sorted(fetched_payloads, key=lambda item: item[0]):
            if payload is None:
                retryable = _dataforseo_error_message_retryable(error_text)
                checkpoint = self._build_queue_checkpoint(
                    query_text=str(spec["query_text"]),
                    depth=int(spec["depth"]),
                    task_id=str(spec["task_id"]),
                    status="fetch_failed_retryable" if retryable else "fetch_failed_terminal",
                    reference=dict(spec.get("checkpoint") or {}),
                )
                checkpoint.update(
                    {
                        "error": str(error_text or "DataForSEO task_get failed."),
                        "retryable": retryable,
                        "retry_unit": "dataforseo_task_id",
                        "retry_strategy": "dataforseo_task_get_failed_task_retry_only",
                    }
                )
                tasks.append(
                    SearchBatchFetchTask(
                        task_key=str(spec["task_key"]),
                        task_id=str(spec["task_id"]),
                        query_text=str(spec["query_text"]),
                        response=None,
                        checkpoint=checkpoint,
                        metadata={
                            "fetched": False,
                            "failed": True,
                            "retryable": retryable,
                            "error": str(error_text or ""),
                            "retry_unit": "dataforseo_task_id",
                            "retry_strategy": "dataforseo_task_get_failed_task_retry_only",
                        },
                    )
                )
                artifacts.append(
                    SearchExecutionArtifact(
                        label=f"task_get_batch_{index:02d}_error",
                        payload={
                            "provider_name": self.provider_name,
                            "task_key": str(spec["task_key"]),
                            "task_id": str(spec["task_id"]),
                            "status": "failed",
                            "error": str(error_text or ""),
                            "retryable": retryable,
                        },
                        metadata={
                            "provider_name": self.provider_name,
                            "task_key": str(spec["task_key"]),
                            "task_id": str(spec["task_id"]),
                            "failed": True,
                            "retryable": retryable,
                        },
                    )
                )
                continue
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
                reference=dict(spec.get("checkpoint") or {}),
            )
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
            reference=existing,
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
                reference=existing,
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
            fetch_cooldown_seconds = resolved_lane_fetch_cooldown_seconds(
                existing,
                default=_default_lane_fetch_cooldown_seconds(),
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

        ready_cooldown_seconds = resolved_lane_ready_cooldown_seconds(
            existing,
            default=_default_lane_ready_cooldown_seconds(),
        )
        ready_poll_source = str(existing.get("ready_poll_source") or "").strip().lower()
        if ready_poll_source == "lane_batch":
            return SearchExecutionResult(
                provider_name=self.provider_name,
                query_text=query_text,
                checkpoint={
                    **self._overlay_ready_metadata(base_checkpoint, existing),
                    "task_id": task_id,
                    "status": "waiting_for_ready_cached",
                    "lane_ready_cooldown_seconds": ready_cooldown_seconds,
                },
                pending=True,
                message=f"Waiting for lane-level DataForSEO ready cache for task {task_id}.",
            )
        if _timestamp_within_seconds(str(existing.get("ready_attempted_at") or ""), ready_cooldown_seconds):
            return SearchExecutionResult(
                provider_name=self.provider_name,
                query_text=query_text,
                checkpoint={
                    **self._overlay_ready_metadata(base_checkpoint, existing),
                    "task_id": task_id,
                    "status": "waiting_for_ready",
                    "lane_ready_cooldown_seconds": ready_cooldown_seconds,
                },
                pending=True,
                message=f"Waiting before next DataForSEO ready poll for task {task_id}.",
            )

        ready_attempted_at = datetime.now(timezone.utc).isoformat()
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
                    "ready_attempted_at": ready_attempted_at,
                    "lane_ready_cooldown_seconds": ready_cooldown_seconds,
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
        assert_live_provider_access_allowed(
            provider_name=self.provider_name,
            operation="search",
            payload={"query": query_text},
        )
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
        _require_batch_task_keys(
            query_specs,
            provider_name=self.provider_name,
            operation="submit_batch_queries",
        )
        for provider in self.providers:
            try:
                result = provider.submit_batch_queries(query_specs)
            except Exception:
                continue
            if result is not None:
                return result
        return None

    def poll_ready_batch(self, query_specs: list[dict[str, Any]]) -> SearchBatchReadyResult | None:
        _require_batch_task_keys(
            query_specs,
            provider_name=self.provider_name,
            operation="poll_ready_batch",
        )
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
        _require_batch_task_keys(
            query_specs,
            provider_name=self.provider_name,
            operation="fetch_ready_batch",
        )
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
    external_mode = _external_provider_mode()
    if external_mode == "scripted":
        return SearchProviderChain([ScriptedSearchProvider()])
    if external_mode in {"simulate", "replay"}:
        return SearchProviderChain([OfflineSearchProvider(mode=external_mode)])
    providers: list[BaseSearchProvider] = []
    provider_order = [str(item or "").strip().lower() for item in settings.provider_order if str(item or "").strip()]
    if MODEL_NATIVE_SEARCH_PROVIDER_NAME in provider_order:
        if not settings.enable_model_native_search:
            raise SearchProviderError(
                "model_native_search is present in SEARCH_PROVIDER_ORDER but SEARCH_PROVIDER_ENABLE_MODEL_NATIVE_SEARCH "
                "is not enabled. Model-native search must be introduced as an explicit experimental evidence source; "
                "it must not silently fall back to DataForSEO, browser search, or DuckDuckGo."
            )
        if str(settings.model_native_search_mode or "").strip().lower() != "experimental_evidence_only":
            raise SearchProviderError(
                "model_native_search requires SEARCH_PROVIDER_MODEL_NATIVE_SEARCH_MODE=experimental_evidence_only. "
                "The model may only collect supplemental evidence with provenance; it cannot replace DataForSEO or "
                "materialize promotion/export signals directly."
            )
        raise SearchProviderError(
            "model_native_search is configured, but no registered provider implementation and owner contract exist yet. "
            "Add a typed provider/command contract with cost budget, provenance, retry/circuit-breaker, export/audit "
            "treatment, and fast preflight before enabling this source in normal runtime."
        )
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
