from __future__ import annotations

from typing import Any

import requests


class DataForSeoClientError(RuntimeError):
    pass


MAX_TASK_POST_BATCH_SIZE = 100


def build_google_organic_task(
    *,
    keyword: str,
    location_name: str = "United States",
    language_name: str = "English",
    device: str = "desktop",
    os: str = "windows",
    depth: int = 10,
    tag: str = "",
) -> dict[str, Any]:
    if not str(keyword or "").strip():
        raise DataForSeoClientError("DataForSEO keyword is required.")
    task = {
        "keyword": str(keyword).strip(),
        "location_name": str(location_name or "United States").strip() or "United States",
        "language_name": str(language_name or "English").strip() or "English",
        "device": str(device or "desktop").strip() or "desktop",
        "os": str(os or "windows").strip() or "windows",
        "depth": max(1, min(int(depth or 10), 100)),
    }
    if str(tag or "").strip():
        task["tag"] = str(tag).strip()
    return task


def extract_google_organic_result_block(payload: dict[str, Any]) -> dict[str, Any]:
    for task in list(payload.get("tasks") or []):
        for result in list(task.get("result") or []):
            if isinstance(result, dict):
                return dict(result)
    return {}


def extract_google_organic_task_ids(payload: dict[str, Any]) -> list[str]:
    task_ids: list[str] = []
    for task in list(payload.get("tasks") or []):
        task_id = str(task.get("id") or "").strip()
        if task_id:
            task_ids.append(task_id)
    return task_ids


def extract_google_organic_ready_task_ids(payload: dict[str, Any]) -> list[str]:
    task_ids: list[str] = []
    for task in list(payload.get("tasks") or []):
        for result in list(task.get("result") or []):
            task_id = str((result or {}).get("id") or "").strip()
            if task_id:
                task_ids.append(task_id)
    return task_ids


def extract_google_organic_submitted_tasks(
    payload: dict[str, Any],
    *,
    fallback_tasks: list[dict[str, Any]] | None = None,
) -> list[dict[str, str]]:
    submitted: list[dict[str, str]] = []
    fallback_items = list(fallback_tasks or [])
    for index, task in enumerate(list(payload.get("tasks") or [])):
        echoed = dict(task.get("data") or {})
        fallback = dict(fallback_items[index] or {}) if index < len(fallback_items) else {}
        submitted.append(
            {
                "task_id": str(task.get("id") or "").strip(),
                "keyword": str(echoed.get("keyword") or fallback.get("keyword") or "").strip(),
                "tag": str(echoed.get("tag") or fallback.get("tag") or "").strip(),
            }
        )
    return submitted


class DataForSeoGoogleOrganicClient:
    def __init__(
        self,
        *,
        login: str,
        password: str,
        base_url: str = "https://api.dataforseo.com",
        timeout_seconds: int = 30,
    ) -> None:
        self.login = str(login or "").strip()
        self.password = str(password or "").strip()
        self.base_url = str(base_url or "https://api.dataforseo.com").strip().rstrip("/")
        self.timeout_seconds = timeout_seconds

    def live_regular(
        self,
        *,
        keyword: str,
        location_name: str = "United States",
        language_name: str = "English",
        device: str = "desktop",
        os: str = "windows",
        depth: int = 10,
        tag: str = "",
    ) -> dict[str, Any]:
        task = build_google_organic_task(
            keyword=keyword,
            location_name=location_name,
            language_name=language_name,
            device=device,
            os=os,
            depth=depth,
            tag=tag,
        )
        return self._request("POST", "/v3/serp/google/organic/live/regular", payload=[task])

    def task_post(
        self,
        *,
        keyword: str,
        location_name: str = "United States",
        language_name: str = "English",
        device: str = "desktop",
        os: str = "windows",
        depth: int = 10,
        tag: str = "",
    ) -> dict[str, Any]:
        task = build_google_organic_task(
            keyword=keyword,
            location_name=location_name,
            language_name=language_name,
            device=device,
            os=os,
            depth=depth,
            tag=tag,
        )
        return self.task_post_many([task])

    def task_post_many(self, tasks: list[dict[str, Any]]) -> dict[str, Any]:
        normalized_tasks = [dict(task or {}) for task in list(tasks or []) if isinstance(task, dict)]
        if not normalized_tasks:
            raise DataForSeoClientError("At least one DataForSEO task is required.")
        if len(normalized_tasks) > MAX_TASK_POST_BATCH_SIZE:
            raise DataForSeoClientError(
                f"DataForSEO task_post supports at most {MAX_TASK_POST_BATCH_SIZE} tasks per request."
            )
        return self._request("POST", "/v3/serp/google/organic/task_post", payload=normalized_tasks)

    def tasks_ready(self) -> dict[str, Any]:
        return self._request("GET", "/v3/serp/google/organic/tasks_ready")

    def task_get_regular(self, task_id: str) -> dict[str, Any]:
        normalized = str(task_id or "").strip()
        if not normalized:
            raise DataForSeoClientError("DataForSEO task_id is required.")
        return self._request("GET", f"/v3/serp/google/organic/task_get/regular/{normalized}")

    def _request(self, method: str, path: str, *, payload: Any | None = None) -> dict[str, Any]:
        if not self.login or not self.password:
            raise DataForSeoClientError("DataForSEO credentials are not configured.")
        response = requests.request(
            method=method.upper(),
            url=f"{self.base_url}{path}",
            auth=(self.login, self.password),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        parsed = response.json()
        self._validate_response(parsed)
        return parsed

    def _validate_response(self, payload: dict[str, Any]) -> None:
        top_level_status = int(payload.get("status_code") or 0)
        if top_level_status and top_level_status != 20000:
            raise DataForSeoClientError(
                f"DataForSEO request failed with status_code={top_level_status}: {payload.get('status_message') or ''}".strip()
            )
        for task in list(payload.get("tasks") or []):
            status_code = int(task.get("status_code") or 0)
            if status_code in {20000, 20100}:
                continue
            raise DataForSeoClientError(
                f"DataForSEO task failed with status_code={status_code}: {task.get('status_message') or ''}".strip()
            )
