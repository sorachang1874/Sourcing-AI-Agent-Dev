from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

PROJECTION_SEARCH_INDEX_BUILD_GENERATION_KEY = "projection_person_search_index_build_generation"
PROJECTION_SEARCH_INDEX_BUILD_INPUT_REVISION_KEY = "projection_person_search_index_build_input_revision"
PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY = "projection_person_search_index_input_revision"
PROJECTION_SEARCH_INDEX_BUILD_STATUS_KEY = "search_index_build_status"
PROJECTION_SEARCH_INDEX_BINDING_KEYS = (
    PROJECTION_SEARCH_INDEX_BUILD_GENERATION_KEY,
    PROJECTION_SEARCH_INDEX_BUILD_INPUT_REVISION_KEY,
    PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY,
)
PROJECTION_SEARCH_INDEX_DERIVED_COUNT_KEYS = (
    "public_facet_counts",
    "facet_count_scope",
    "facet_build_status",
    "index_count_scope",
)
PROJECTION_SEARCH_INDEX_DERIVED_READINESS_KEYS = (
    "index_count_scope",
    "profile_indexed_at",
    "evidence_indexed_at",
    *PROJECTION_SEARCH_INDEX_BINDING_KEYS,
)
PROJECTION_SEARCH_INDEX_DERIVED_METADATA_KEYS = (
    PROJECTION_SEARCH_INDEX_BUILD_STATUS_KEY,
    "search_index_writer_id",
    "search_indexed_member_count",
    "search_index_completed_at",
    "public_facet_counts_writer_id",
    "public_facet_counts_source",
    "public_facet_counts_build_status",
    "public_facet_counts_record_count",
    "public_facet_counts_missing_filter_record_count",
    "public_facet_counts_truncated",
)


def new_projection_search_index_input_revision() -> str:
    return f"projidxinput_{uuid4().hex}"


def projection_search_index_build_binding(metadata: dict[str, Any]) -> dict[str, str]:
    source = dict(metadata or {})
    return {key: str(source.get(key) or "").strip() for key in PROJECTION_SEARCH_INDEX_BINDING_KEYS}


def projection_search_index_publication_state(
    metadata: dict[str, Any],
    *,
    publication: dict[str, Any] | None = None,
    require_completed: bool = True,
) -> dict[str, Any]:
    """Validate the build identity that makes index-derived public products visible."""

    source = dict(metadata or {})
    binding = projection_search_index_build_binding(source)
    build_generation = binding[PROJECTION_SEARCH_INDEX_BUILD_GENERATION_KEY]
    build_input_revision = binding[PROJECTION_SEARCH_INDEX_BUILD_INPUT_REVISION_KEY]
    input_revision = binding[PROJECTION_SEARCH_INDEX_INPUT_REVISION_KEY]
    build_status = str(source.get(PROJECTION_SEARCH_INDEX_BUILD_STATUS_KEY) or "").strip()
    if not build_generation or not build_input_revision or not input_revision:
        return {
            "status": "unavailable",
            "reason": "projection_person_search_index_build_binding_missing",
            "build_status": build_status,
            **binding,
        }
    if build_input_revision != input_revision:
        return {
            "status": "unavailable",
            "reason": "projection_person_search_index_stale_input_revision",
            "build_status": build_status,
            **binding,
        }
    if require_completed and build_status != "completed":
        return {
            "status": "unavailable",
            "reason": "projection_person_search_index_build_incomplete",
            "build_status": build_status,
            **binding,
        }
    if publication is not None:
        product_binding = projection_search_index_build_binding(publication)
        if product_binding != binding:
            return {
                "status": "unavailable",
                "reason": "projection_person_search_index_product_binding_mismatch",
                "build_status": build_status,
                "product_binding": product_binding,
                **binding,
            }
    return {
        "status": "ready",
        "reason": "",
        "build_status": build_status,
        **binding,
    }


def preserve_projection_search_index_products(
    *,
    existing_counts: dict[str, Any],
    existing_readiness: dict[str, Any],
    existing_metadata: dict[str, Any],
    next_counts: dict[str, Any],
    next_readiness: dict[str, Any],
    next_metadata: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Preserve index-derived products for a semantically identical publication."""

    preserved_counts = dict(next_counts or {})
    preserved_readiness = dict(next_readiness or {})
    preserved_metadata = dict(next_metadata or {})
    for key in PROJECTION_SEARCH_INDEX_DERIVED_COUNT_KEYS:
        if key in existing_counts:
            preserved_counts[key] = existing_counts.get(key)
    for key in PROJECTION_SEARCH_INDEX_DERIVED_READINESS_KEYS:
        if key in existing_readiness:
            preserved_readiness[key] = existing_readiness.get(key)
    for key in PROJECTION_SEARCH_INDEX_DERIVED_METADATA_KEYS:
        if key in existing_metadata:
            preserved_metadata[key] = existing_metadata.get(key)
    return preserved_counts, preserved_readiness, preserved_metadata


_SEMANTIC_MEMBER_FIELDS = (
    "candidate_identity_key",
    "person_identity_key",
    "profile_url_key",
    "candidate_id",
    "rank_index",
    "rank_key",
    "lane",
    "employment_scope",
    "source_shard_key",
    "source_run_id",
    "row_readiness",
    "profile_readiness",
    "card_readiness",
    "visibility_state",
    "public_summary",
    "projection_metrics",
    "crm_overlay_summary",
    "provenance",
    "metadata",
)


def projection_search_index_semantic_member(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize only member fields that can affect public index rows."""

    payload = dict(row or {})
    normalized: dict[str, Any] = {}
    for field_name in _SEMANTIC_MEMBER_FIELDS:
        value = payload.get(field_name)
        if value is None and f"{field_name}_json" in payload:
            value = _load_json_value(payload.get(f"{field_name}_json"))
        normalized[field_name] = _json_safe(value)
    return normalized


def projection_search_index_members_changed(
    *,
    existing_rows: dict[str, dict[str, Any]],
    next_rows: dict[str, dict[str, Any]],
    replace_members: bool,
) -> bool:
    existing_keys = set(existing_rows)
    next_keys = set(next_rows)
    if replace_members and existing_keys != next_keys:
        return True
    for candidate_key, next_row in next_rows.items():
        existing_row = existing_rows.get(candidate_key)
        if existing_row is None:
            return True
        if projection_search_index_semantic_member(existing_row) != projection_search_index_semantic_member(next_row):
            return True
    return False


def _load_json_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return value


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return sorted(_json_safe(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
