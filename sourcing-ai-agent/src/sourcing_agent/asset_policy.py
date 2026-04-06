from __future__ import annotations

from urllib import parse


DEFAULT_MODEL_CONTEXT_CHAR_LIMIT = 1600
RAW_ASSET_POLICY_SUMMARY = "raw page stored on disk; model only sees compact excerpt"

_BINARY_SUFFIXES = {
    ".pdf",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".zip",
    ".tar",
    ".gz",
}


def is_binary_like_url(url: str) -> bool:
    normalized = str(url or "").strip().lower()
    if not normalized:
        return False
    path = parse.urlparse(normalized).path or ""
    return any(path.endswith(suffix) for suffix in _BINARY_SUFFIXES)
