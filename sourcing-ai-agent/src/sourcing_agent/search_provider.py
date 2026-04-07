from __future__ import annotations

from dataclasses import dataclass, field
from html import unescape
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any
from urllib import parse

import requests

from .settings import SearchProviderSettings
from .web_fetch import DEFAULT_HEADERS, fetch_search_results_html


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


class SearchProviderError(RuntimeError):
    def __init__(self, message: str, *, attempts: list[dict[str, str]] | None = None) -> None:
        super().__init__(message)
        self.attempts = attempts or []


class BaseSearchProvider:
    provider_name: str = "base"

    def search(self, query_text: str, *, max_results: int = 10, timeout: int | None = None) -> SearchResponse:
        raise NotImplementedError


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
            raise SearchProviderError(f"Browser Google search failed: {stderr or 'unknown error'}")
        payload = _parse_browser_provider_payload(completed.stdout)
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
            metadata=dict(payload.get("metadata") or {}),
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


def build_search_provider(settings: SearchProviderSettings) -> BaseSearchProvider:
    providers: list[BaseSearchProvider] = []
    for provider_name in settings.provider_order:
        normalized = str(provider_name or "").strip().lower()
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


def _strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    return " ".join(unescape(text).split())


def _parse_browser_provider_payload(stdout_text: str) -> dict[str, Any]:
    text = str(stdout_text or "").strip()
    if not text:
        raise SearchProviderError("Browser Google search returned empty stdout.")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SearchProviderError(f"Browser Google search returned invalid JSON: {text[:200]}") from exc
