from __future__ import annotations

from dataclasses import dataclass
from urllib import parse

import requests


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


@dataclass(frozen=True, slots=True)
class FetchedTextAsset:
    url: str
    final_url: str
    content_type: str
    text: str
    source_label: str


@dataclass(frozen=True, slots=True)
class FetchedBinaryAsset:
    url: str
    final_url: str
    content_type: str
    content: bytes
    source_label: str


def fetch_text_url(url: str, *, timeout: int = 30, source_label: str = "web") -> FetchedTextAsset:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    content_type = str(response.headers.get("Content-Type") or "text/html").strip()
    response.encoding = response.encoding or response.apparent_encoding or "utf-8"
    return FetchedTextAsset(
        url=url,
        final_url=str(response.url or url),
        content_type=content_type,
        text=response.text,
        source_label=source_label,
    )


def fetch_binary_url(url: str, *, timeout: int = 30, source_label: str = "web") -> FetchedBinaryAsset:
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    content_type = str(response.headers.get("Content-Type") or "application/octet-stream").strip()
    return FetchedBinaryAsset(
        url=url,
        final_url=str(response.url or url),
        content_type=content_type,
        content=response.content,
        source_label=source_label,
    )


def fetch_search_results_html(query_text: str, *, timeout: int = 30) -> FetchedTextAsset:
    encoded_query = parse.urlencode({"q": query_text})
    search_endpoints = [
        ("duckduckgo_html", f"https://html.duckduckgo.com/html/?{encoded_query}"),
        ("duckduckgo_html_alt", f"https://duckduckgo.com/html/?{encoded_query}"),
    ]
    last_error: Exception | None = None
    for source_label, url in search_endpoints:
        try:
            return fetch_text_url(url, timeout=timeout, source_label=source_label)
        except Exception as exc:  # pragma: no cover - exercised in integration/live paths
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    raise RuntimeError("No search endpoint is configured.")
