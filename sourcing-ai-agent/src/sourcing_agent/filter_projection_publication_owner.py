"""Foundation-only carrier controls for future filter-projection ownership.

This module deliberately does not model the product owner.  The current tree
has no durable join from one exact start-v2 receipt to the committed Cohort
terminal result and final serving projection.  S1f0b therefore reserves a
distinct, non-adoptable candidate carrier and hardens every generic membership
writer around its invalidation semantics.  Product readers must never treat
these bytes as lineage or terminal-result authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from .projection_search_index_contract import projection_search_index_members_changed

FILTER_PROJECTION_FOUNDATION_STATUS = "foundation_only_unbound"
FILTER_PROJECTION_FOUNDATION_METADATA_KEY = "filter_projection_publication_candidate_v1"
FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY = "filter_projection_membership_candidate_v1"
FILTER_PROJECTION_FOUNDATION_CANDIDATE = "projection_search_service.filter_projection_publication_candidate"
FILTER_PROJECTION_FOUNDATION_CANDIDATE_REVISION = "filter_projection_publication_candidate_v1"
FILTER_PROJECTION_FOUNDATION_MEMBER_CANDIDATE = "projection_search_service.filter_projection_membership_candidate"
FILTER_PROJECTION_FOUNDATION_MEMBER_CANDIDATE_REVISION = "filter_projection_membership_candidate_v1"
FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE = "filter_projection_foundation_candidate"
FILTER_PROJECTION_FOUNDATION_PROJECTION_STATE = "draft"

_FOUNDATION_SCHEMA_VERSION = "filter_projection_publication_foundation.v1"
_FOUNDATION_WRAPPER_SCHEMA_VERSION = "filter_projection_publication_foundation_wrapper.v1"
_MEMBER_WRAPPER_SCHEMA_VERSION = "filter_projection_membership_foundation_wrapper.v1"
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}")
_IDENTITY_TEXT = re.compile(r"\S{1,4096}")


@dataclass(frozen=True, slots=True)
class FilterProjectionPublicationFoundationError(ValueError):
    code: str
    field: str = ""

    def __str__(self) -> str:
        return self.code if not self.field else f"{self.code}:{self.field}"


@dataclass(frozen=True, slots=True)
class FilterProjectionPublicationFoundation:
    """Canonical non-product carrier for one atomic UoW exercise."""

    _foundation_wrapper_json: str
    _members_json: str

    @property
    def foundation_wrapper(self) -> dict[str, Any]:
        return dict(json.loads(self._foundation_wrapper_json))

    @property
    def foundation_record(self) -> dict[str, Any]:
        return dict(self.foundation_wrapper["foundation_record"])

    @property
    def foundation_record_digest(self) -> str:
        return str(self.foundation_wrapper["foundation_record_digest"])

    @property
    def source_run_id(self) -> str:
        return str(self.foundation_record["source_run_id"])

    @property
    def members(self) -> list[dict[str, Any]]:
        return [dict(item) for item in list(json.loads(self._members_json))]

    @property
    def member_count(self) -> int:
        return len(list(json.loads(self._members_json)))

    def projection_metadata(self, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = _require_plain_object(metadata or {}, field="metadata")
        if FILTER_PROJECTION_FOUNDATION_METADATA_KEY in payload:
            raise FilterProjectionPublicationFoundationError(
                "filter_projection_foundation_reserved_key",
                FILTER_PROJECTION_FOUNDATION_METADATA_KEY,
            )
        return {
            **payload,
            FILTER_PROJECTION_FOUNDATION_METADATA_KEY: self.foundation_wrapper,
        }


def build_filter_projection_publication_foundation(
    *,
    source_run_id: str,
    members: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> FilterProjectionPublicationFoundation:
    """Build non-adoptable carrier bytes without asserting product lineage."""

    normalized_source_run_id = _require_identifier(source_run_id, field="source_run_id")
    if type(members) not in {list, tuple} or not members:
        raise FilterProjectionPublicationFoundationError(
            "filter_projection_foundation_members_invalid",
            "members",
        )
    normalized_members: list[dict[str, Any]] = []
    seen_candidates: set[str] = set()
    for index, member_value in enumerate(list(members)):
        member = _require_plain_object(member_value, field=f"members[{index}]")
        candidate_identity_key = _require_identity_text(
            member.get("candidate_identity_key"),
            field=f"members[{index}].candidate_identity_key",
        )
        if candidate_identity_key in seen_candidates:
            raise FilterProjectionPublicationFoundationError(
                "filter_projection_foundation_candidate_duplicate",
                candidate_identity_key,
            )
        seen_candidates.add(candidate_identity_key)
        provenance = _require_plain_object(
            member.get("provenance") or {},
            field=f"members[{index}].provenance",
        )
        if FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY in provenance:
            raise FilterProjectionPublicationFoundationError(
                "filter_projection_foundation_reserved_key",
                FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY,
            )
        normalized_members.append(member)

    foundation_record = {
        "schema_version": _FOUNDATION_SCHEMA_VERSION,
        "status": FILTER_PROJECTION_FOUNDATION_STATUS,
        "candidate": FILTER_PROJECTION_FOUNDATION_CANDIDATE,
        "candidate_revision": FILTER_PROJECTION_FOUNDATION_CANDIDATE_REVISION,
        "source_run_id": normalized_source_run_id,
    }
    foundation_record_digest = _sha256_json(foundation_record)
    foundation_wrapper = {
        "schema_version": _FOUNDATION_WRAPPER_SCHEMA_VERSION,
        "foundation_record": foundation_record,
        "foundation_record_digest": foundation_record_digest,
    }

    decorated_members: list[dict[str, Any]] = []
    for member in normalized_members:
        candidate_identity_key = str(member["candidate_identity_key"])
        member_wrapper = {
            "schema_version": _MEMBER_WRAPPER_SCHEMA_VERSION,
            "status": FILTER_PROJECTION_FOUNDATION_STATUS,
            "candidate": FILTER_PROJECTION_FOUNDATION_MEMBER_CANDIDATE,
            "candidate_revision": FILTER_PROJECTION_FOUNDATION_MEMBER_CANDIDATE_REVISION,
            "parent_foundation_digest": foundation_record_digest,
            "candidate_identity_key": candidate_identity_key,
        }
        decorated_members.append(
            {
                **member,
                "provenance": {
                    **dict(member.get("provenance") or {}),
                    FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY: member_wrapper,
                },
            }
        )

    return FilterProjectionPublicationFoundation(
        _foundation_wrapper_json=_canonical_json(foundation_wrapper),
        _members_json=_canonical_json(decorated_members),
    )


def validate_filter_projection_publication_foundation(
    foundation: FilterProjectionPublicationFoundation,
) -> None:
    """Rebuild and exact-compare a typed value before any carrier can be written."""

    if type(foundation) is not FilterProjectionPublicationFoundation:
        raise TypeError("foundation must be FilterProjectionPublicationFoundation")
    try:
        wrapper = foundation.foundation_wrapper
        record = foundation.foundation_record
        decorated_members = foundation.members
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise FilterProjectionPublicationFoundationError(
            "filter_projection_foundation_value_invalid",
            "foundation",
        ) from exc
    if set(wrapper) != {
        "schema_version",
        "foundation_record",
        "foundation_record_digest",
    } or set(record) != {
        "schema_version",
        "status",
        "candidate",
        "candidate_revision",
        "source_run_id",
    }:
        raise FilterProjectionPublicationFoundationError(
            "filter_projection_foundation_value_invalid",
            "foundation_record",
        )
    undecorated_members: list[dict[str, Any]] = []
    for index, member in enumerate(decorated_members):
        payload = _require_plain_object(member, field=f"members[{index}]")
        provenance = _require_plain_object(
            payload.get("provenance") or {},
            field=f"members[{index}].provenance",
        )
        if FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY not in provenance:
            raise FilterProjectionPublicationFoundationError(
                "filter_projection_foundation_value_invalid",
                f"members[{index}].provenance",
            )
        provenance.pop(FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY, None)
        undecorated_members.append({**payload, "provenance": provenance})
    try:
        source_run_id = _require_identifier(
            record.get("source_run_id"),
            field="source_run_id",
        )
        rebuilt = build_filter_projection_publication_foundation(
            source_run_id=source_run_id,
            members=undecorated_members,
        )
    except FilterProjectionPublicationFoundationError as exc:
        raise FilterProjectionPublicationFoundationError(
            "filter_projection_foundation_value_invalid",
            "foundation",
        ) from exc
    if (
        foundation._foundation_wrapper_json != rebuilt._foundation_wrapper_json
        or foundation._members_json != rebuilt._members_json
    ):
        raise FilterProjectionPublicationFoundationError(
            "filter_projection_foundation_value_invalid",
            "foundation",
        )


def reject_filter_projection_foundation_metadata(metadata: dict[str, Any] | None) -> None:
    payload = _require_plain_object(metadata or {}, field="metadata")
    if FILTER_PROJECTION_FOUNDATION_METADATA_KEY in payload:
        raise FilterProjectionPublicationFoundationError(
            "filter_projection_foundation_reserved_key",
            FILTER_PROJECTION_FOUNDATION_METADATA_KEY,
        )


def reject_filter_projection_foundation_member_carriers(
    members: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> None:
    for index, member in enumerate(list(members or [])):
        payload = _require_plain_object(member, field=f"members[{index}]")
        provenance = _require_plain_object(
            payload.get("provenance") or {},
            field=f"members[{index}].provenance",
        )
        if FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY in provenance:
            raise FilterProjectionPublicationFoundationError(
                "filter_projection_foundation_reserved_key",
                FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY,
            )


def strip_filter_projection_foundation_carriers(
    *,
    metadata: dict[str, Any] | None,
    members: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    stripped_metadata = _require_plain_object(metadata or {}, field="metadata")
    stripped_metadata.pop(FILTER_PROJECTION_FOUNDATION_METADATA_KEY, None)
    stripped_members: list[dict[str, Any]] = []
    for index, member in enumerate(list(members or [])):
        payload = _require_plain_object(member, field=f"members[{index}]")
        provenance = _require_plain_object(
            payload.get("provenance") or {},
            field=f"members[{index}].provenance",
        )
        provenance.pop(FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY, None)
        stripped_members.append({**payload, "provenance": provenance})
    return stripped_metadata, stripped_members


def projection_has_filter_projection_foundation(metadata: Any) -> bool:
    return type(metadata) is dict and FILTER_PROJECTION_FOUNDATION_METADATA_KEY in metadata


def filter_projection_foundation_members_changed(
    *,
    existing_rows: dict[str, dict[str, Any]],
    next_rows: dict[str, dict[str, Any]],
    replace_members: bool,
) -> bool:
    """Compare public member semantics while ignoring the shadow carrier itself."""

    return projection_search_index_members_changed(
        existing_rows={key: _without_foundation_member_carrier(value) for key, value in existing_rows.items()},
        next_rows={key: _without_foundation_member_carrier(value) for key, value in next_rows.items()},
        replace_members=replace_members,
    )


def _without_foundation_member_carrier(value: dict[str, Any]) -> dict[str, Any]:
    payload = dict(value or {})
    raw_provenance = payload.get("provenance")
    if raw_provenance is None:
        raw_provenance = payload.get("provenance_json")
    if isinstance(raw_provenance, str):
        try:
            decoded = json.loads(raw_provenance)
        except (TypeError, ValueError, json.JSONDecodeError):
            decoded = {}
        provenance = dict(decoded) if type(decoded) is dict else {}
    else:
        provenance = dict(raw_provenance) if type(raw_provenance) is dict else {}
    provenance.pop(FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY, None)
    payload["provenance"] = provenance
    payload.pop("provenance_json", None)
    return payload


def _require_plain_object(value: Any, *, field: str) -> dict[str, Any]:
    if type(value) is not dict or any(type(key) is not str for key in value):
        raise FilterProjectionPublicationFoundationError(
            "filter_projection_foundation_value_invalid",
            field,
        )
    return {key: _copy_plain_json(child, field=f"{field}.{key}") for key, child in value.items()}


def _copy_plain_json(value: Any, *, field: str) -> Any:
    if value is None or type(value) in {str, int, float, bool}:
        if type(value) is float and (value != value or value in {float("inf"), float("-inf")}):
            raise FilterProjectionPublicationFoundationError(
                "filter_projection_foundation_value_invalid",
                field,
            )
        return value
    if type(value) is list:
        return [_copy_plain_json(child, field=f"{field}[]") for child in value]
    if type(value) is dict and all(type(key) is str for key in value):
        return {key: _copy_plain_json(child, field=f"{field}.{key}") for key, child in value.items()}
    raise FilterProjectionPublicationFoundationError(
        "filter_projection_foundation_value_invalid",
        field,
    )


def _require_plain_string(value: Any, *, field: str) -> str:
    if type(value) is not str or not value.strip():
        raise FilterProjectionPublicationFoundationError(
            "filter_projection_foundation_value_invalid",
            field,
        )
    return value


def _require_identifier(value: Any, *, field: str) -> str:
    normalized = _require_plain_string(value, field=field)
    if normalized != normalized.strip() or _IDENTIFIER.fullmatch(normalized) is None:
        raise FilterProjectionPublicationFoundationError(
            "filter_projection_foundation_identity_invalid",
            field,
        )
    return normalized


def _require_identity_text(value: Any, *, field: str) -> str:
    normalized = _require_plain_string(value, field=field)
    if normalized != normalized.strip() or _IDENTITY_TEXT.fullmatch(normalized) is None:
        raise FilterProjectionPublicationFoundationError(
            "filter_projection_foundation_identity_invalid",
            field,
        )
    return normalized


def _canonical_json(value: Any) -> str:
    return json.dumps(
        _copy_plain_json(value, field="canonical_json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


__all__ = [
    "FILTER_PROJECTION_FOUNDATION_CANDIDATE",
    "FILTER_PROJECTION_FOUNDATION_CANDIDATE_REVISION",
    "FILTER_PROJECTION_FOUNDATION_MEMBER_CANDIDATE",
    "FILTER_PROJECTION_FOUNDATION_MEMBER_CANDIDATE_REVISION",
    "FILTER_PROJECTION_FOUNDATION_MEMBER_PROVENANCE_KEY",
    "FILTER_PROJECTION_FOUNDATION_METADATA_KEY",
    "FILTER_PROJECTION_FOUNDATION_PROJECTION_STATE",
    "FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE",
    "FILTER_PROJECTION_FOUNDATION_STATUS",
    "FilterProjectionPublicationFoundation",
    "FilterProjectionPublicationFoundationError",
    "build_filter_projection_publication_foundation",
    "filter_projection_foundation_members_changed",
    "projection_has_filter_projection_foundation",
    "reject_filter_projection_foundation_member_carriers",
    "reject_filter_projection_foundation_metadata",
    "strip_filter_projection_foundation_carriers",
    "validate_filter_projection_publication_foundation",
]
