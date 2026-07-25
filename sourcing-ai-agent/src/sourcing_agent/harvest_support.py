"""Shared harvest support layer — execution result types + profile payload utils.

Split out of harvest_connectors 2026-07-22 (god-file wave 1, REFACTOR_MASTER_PLAN
WS2): strict one-way import order is harvest_support <- harvest_offline_harness
<- harvest_connectors. harvest_connectors re-imports these names so existing
consumers of its module path keep working.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha1
from typing import Any


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


def _profile_cache_key(profile_url: str) -> str:
    import hashlib

    return hashlib.sha1(profile_url.strip().encode("utf-8")).hexdigest()[:16]


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


def _offline_profile_identifier(profile_url: str) -> str:
    normalized = str(profile_url or "").strip().rstrip("/")
    if "/in/" in normalized:
        return normalized.rsplit("/in/", 1)[-1].strip("/") or "offline-profile"
    return "offline-profile"


def _payload_cache_key(payload: dict[str, Any]) -> str:
    return sha1(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
