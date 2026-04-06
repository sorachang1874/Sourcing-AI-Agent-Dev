from __future__ import annotations

from dataclasses import dataclass, field
from html import unescape
import re
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
