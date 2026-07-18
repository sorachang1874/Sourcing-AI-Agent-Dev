"""Provider-free artifact boundary for selected-person X verification.

This module is owned by the sourcing product.  It exports an allowlisted,
revision-fenced selection artifact and can turn a validated X-First result into
an import *preview*.  It deliberately does not import the sibling ``x_first``
runtime and it never writes Person, CRM, projection, or outreach state.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

SELECTION_SCHEMA_VERSION = "sourcing.x_first.subject_selection.v1"
IMPORT_PREVIEW_SCHEMA_VERSION = "sourcing.x_first.verification_import_preview.v1"
REQUEST_BINDING_SCHEMA_VERSION = "x.portable.selected_subject.request_binding.v1"
PORTABLE_REQUEST_SCHEMA_VERSION = "x.portable.research_campaign.request.v1"
PORTABLE_RESULT_SCHEMA_VERSION = "x.portable.research_campaign.result.v1"
SELECTION_CONTRACT_SCHEMA_SHA256 = "e487190924efeaf9bf05f5836619867a5cece66d707891f4587665c115b281bc"
REQUEST_BINDING_CONTRACT_SCHEMA_SHA256 = "ef2f6dc742658a5b6507f7320bc2ed5530612e97097eadf1c3e4a46405835c1d"
IMPORT_PREVIEW_CONTRACT_SCHEMA_SHA256 = "0909da87ef12efb919e9c8f838d3c5b986c7f15d8cba5c2c12c7ef690c33a541"

_SELECTION_SCHEMA_RELATIVE_PATH = Path(
    "contracts/external/x_first/sourcing.x_first.subject_selection.v1.schema.json"
)
_REQUEST_BINDING_SCHEMA_RELATIVE_PATH = Path(
    "contracts/external/x_first/x.portable.selected_subject.request_binding.v1.schema.json"
)
_IMPORT_PREVIEW_SCHEMA_RELATIVE_PATH = Path(
    "contracts/external/x_first/sourcing.x_first.verification_import_preview.v1.schema.json"
)
_IDENTIFIER_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,127}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_TIMESTAMP_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")

_SELECTION_AUTHORITY = {
    "provider_calls_allowed": False,
    "product_writes_allowed": False,
    "canonical_person_merge_allowed": False,
    "outreach_allowed": False,
}
_IMPORT_PREVIEW_AUTHORITY = {
    "product_writes_allowed": False,
    "canonical_person_write_allowed": False,
    "automatic_cross_source_merge_allowed": False,
    "outreach_allowed": False,
}
_BOUND_TARGET_FIELDS = {
    "workspace_id",
    "projection_id",
    "membership_revision",
    "source_candidate_count",
    "candidate_identity_keys",
}
_SUBJECT_FIELDS = {
    "source_subject_ref",
    "source_record_ref",
    "source_record_sha256",
    "source_status",
    "source_kind",
    "source_profile_url",
    "name_text",
    "x_handle_proposals",
    "professional_facts",
}
_FACT_TYPES = {"affiliation", "role", "education", "project", "location", "other"}
_TEMPORAL_STATES = {"current", "historical", "ambiguous", "not_applicable"}


class XFirstPortableAdapterError(ValueError):
    """Stable fail-closed error for the product-owned X-First boundary."""


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"), sort_keys=True)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _content_sha256(value: Mapping[str, Any], field: str) -> str:
    return canonical_sha256({key: item for key, item in value.items() if key != field})


def _pinned_contract_schema_sha256(*, path: Path, expected: str, error: str) -> str:
    try:
        payload = (project_root() / path).read_bytes()
    except OSError as exc:
        raise XFirstPortableAdapterError(error) from exc
    actual = hashlib.sha256(payload).hexdigest()
    if actual != expected:
        raise XFirstPortableAdapterError(error)
    return actual


def selection_contract_schema_sha256() -> str:
    return _pinned_contract_schema_sha256(
        path=_SELECTION_SCHEMA_RELATIVE_PATH,
        expected=SELECTION_CONTRACT_SCHEMA_SHA256,
        error="x_first_selection_schema_digest_mismatch",
    )


def request_binding_contract_schema_sha256() -> str:
    return _pinned_contract_schema_sha256(
        path=_REQUEST_BINDING_SCHEMA_RELATIVE_PATH,
        expected=REQUEST_BINDING_CONTRACT_SCHEMA_SHA256,
        error="x_first_request_binding_schema_digest_mismatch",
    )


def import_preview_contract_schema_sha256() -> str:
    return _pinned_contract_schema_sha256(
        path=_IMPORT_PREVIEW_SCHEMA_RELATIVE_PATH,
        expected=IMPORT_PREVIEW_CONTRACT_SCHEMA_SHA256,
        error="x_first_import_preview_schema_digest_mismatch",
    )


def _identifier(value: Any, error: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise XFirstPortableAdapterError(error)
    return value


def _sha256(value: Any, error: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise XFirstPortableAdapterError(error)
    return value


def _timestamp(value: Any, error: str) -> str:
    if not isinstance(value, str) or _TIMESTAMP_RE.fullmatch(value) is None:
        raise XFirstPortableAdapterError(error)
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise XFirstPortableAdapterError(error) from exc
    return value


def _normalized_text(value: Any, *, maximum: int) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized[:maximum]


def _normalized_string_list(value: Any, *, maximum_items: int, maximum_text: int) -> list[str]:
    if isinstance(value, (list, tuple)):
        source = list(value)
    else:
        source = []
    result: list[str] = []
    seen: set[str] = set()
    for raw in source:
        normalized = _normalized_text(raw, maximum=maximum_text)
        if normalized is None or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
        if len(result) >= maximum_items:
            break
    return result


def _profile_kind(profile_url: str | None) -> str:
    if profile_url is None:
        return "name_only"
    try:
        parsed = urlsplit(profile_url)
    except ValueError as exc:
        raise XFirstPortableAdapterError("x_first_selection_profile_url_invalid") from exc
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise XFirstPortableAdapterError("x_first_selection_profile_url_invalid")
    host = (parsed.hostname or "").casefold()
    if host in {"linkedin.com", "www.linkedin.com"}:
        return "linkedin_profile"
    return "professional_profile"


def _subject_ref(*, workspace_id: str, projection_id: str, membership_revision: str, member_key: str) -> str:
    digest = canonical_sha256(
        {
            "workspace_id": workspace_id,
            "projection_id": projection_id,
            "membership_revision": membership_revision,
            "member_key": member_key,
        }
    )
    return f"subject_{digest[:24]}"


def _fact(
    *,
    fact_type: str,
    value: str,
    temporal_state: str,
    evidence_ref: str,
) -> dict[str, str]:
    return {
        "fact_type": fact_type,
        "value": value,
        "temporal_state": temporal_state,
        "evidence_ref": evidence_ref,
    }


def _professional_facts(summary: Mapping[str, Any], *, selection_id: str, subject_ref: str) -> list[dict[str, str]]:
    candidates: list[tuple[str, str | None, str]] = [
        ("affiliation", _normalized_text(summary.get("current_company"), maximum=1000), "current"),
        (
            "role",
            _normalized_text(summary.get("headline") or summary.get("title"), maximum=1000),
            "current",
        ),
        ("location", _normalized_text(summary.get("location"), maximum=1000), "ambiguous"),
        ("other", _normalized_text(summary.get("summary"), maximum=1000), "ambiguous"),
    ]
    candidates.extend(
        ("affiliation", value, "ambiguous")
        for value in _normalized_string_list(summary.get("experience_lines"), maximum_items=12, maximum_text=1000)
    )
    candidates.extend(
        ("education", value, "ambiguous")
        for value in _normalized_string_list(summary.get("education_lines"), maximum_items=8, maximum_text=1000)
    )
    facts: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for fact_type, value, temporal_state in candidates:
        if value is None:
            continue
        key = (fact_type, value, temporal_state)
        if key in seen:
            continue
        seen.add(key)
        facts.append(
            _fact(
                fact_type=fact_type,
                value=value,
                temporal_state=temporal_state,
                evidence_ref=f"sourcing://selection/{selection_id}/{subject_ref}/fact/{len(facts) + 1}",
            )
        )
    return facts


def _handle_proposals(
    raw_handles: Sequence[str] | None,
    *,
    selection_id: str,
    subject_ref: str,
) -> list[dict[str, str]]:
    proposals: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in raw_handles or ():
        if not isinstance(raw, str):
            raise XFirstPortableAdapterError("x_first_selection_handle_invalid")
        handle = raw.strip().removeprefix("@").strip()
        key = handle.casefold()
        if _HANDLE_RE.fullmatch(handle) is None or key in seen:
            raise XFirstPortableAdapterError("x_first_selection_handle_invalid")
        seen.add(key)
        proposals.append(
            {
                "handle": handle,
                "binding_status": "cross_source_link_proposed",
                "evidence_ref": f"sourcing://selection/{selection_id}/{subject_ref}/x-handle/{len(proposals) + 1}",
            }
        )
    return proposals


def build_subject_selection_artifact(
    *,
    bound_target_ref: Mapping[str, Any],
    members: Sequence[Mapping[str, Any]],
    exported_at: str,
    selection_id: str | None = None,
    x_handle_proposals_by_candidate: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Export one exact selected projection snapshot through an allowlist."""

    target = dict(bound_target_ref)
    if set(target) != _BOUND_TARGET_FIELDS:
        raise XFirstPortableAdapterError("x_first_selection_bound_target_invalid")
    workspace_id = str(target.get("workspace_id") or "").strip()
    projection_id = str(target.get("projection_id") or "").strip()
    membership_revision = str(target.get("membership_revision") or "").strip()
    source_candidate_count = target.get("source_candidate_count")
    raw_member_keys = target.get("candidate_identity_keys")
    if (
        not workspace_id
        or len(workspace_id) > 256
        or not projection_id
        or len(projection_id) > 256
        or not membership_revision
        or len(membership_revision) > 256
        or isinstance(source_candidate_count, bool)
        or not isinstance(source_candidate_count, int)
        or not isinstance(raw_member_keys, (list, tuple))
    ):
        raise XFirstPortableAdapterError("x_first_selection_bound_target_invalid")
    member_keys = [str(value or "").strip() for value in raw_member_keys]
    if (
        not member_keys
        or member_keys != sorted(member_keys)
        or len(member_keys) != len(set(member_keys))
        or any(not value or len(value) > 500 for value in member_keys)
        or source_candidate_count < len(member_keys)
    ):
        raise XFirstPortableAdapterError("x_first_selection_bound_target_invalid")
    _timestamp(exported_at, "x_first_selection_exported_at_invalid")
    resolved_selection_id = selection_id or (
        "selection_"
        + canonical_sha256(
            {
                "workspace_id": workspace_id,
                "projection_id": projection_id,
                "membership_revision": membership_revision,
                "candidate_identity_keys": member_keys,
            }
        )[:24]
    )
    _identifier(resolved_selection_id, "x_first_selection_id_invalid")

    member_by_key: dict[str, Mapping[str, Any]] = {}
    for raw_member in members:
        if not isinstance(raw_member, Mapping):
            raise XFirstPortableAdapterError("x_first_selection_member_invalid")
        member_key = str(raw_member.get("candidate_identity_key") or "").strip()
        if member_key in member_by_key or member_key not in set(member_keys):
            raise XFirstPortableAdapterError("x_first_selection_member_invalid")
        if str(raw_member.get("projection_id") or projection_id).strip() != projection_id:
            raise XFirstPortableAdapterError("x_first_selection_member_invalid")
        member_by_key[member_key] = raw_member
    if set(member_by_key) != set(member_keys):
        raise XFirstPortableAdapterError("x_first_selection_member_set_mismatch")

    subjects: list[dict[str, Any]] = []
    proposal_map = dict(x_handle_proposals_by_candidate or {})
    if any(key not in set(member_keys) for key in proposal_map):
        raise XFirstPortableAdapterError("x_first_selection_handle_subject_unknown")
    for member_key in member_keys:
        member = member_by_key[member_key]
        summary = member.get("public_summary")
        if not isinstance(summary, Mapping):
            raise XFirstPortableAdapterError("x_first_selection_public_summary_invalid")
        subject_ref = _subject_ref(
            workspace_id=workspace_id,
            projection_id=projection_id,
            membership_revision=membership_revision,
            member_key=member_key,
        )
        name_text = _normalized_text(summary.get("display_name") or summary.get("name"), maximum=512)
        if name_text is None:
            raise XFirstPortableAdapterError("x_first_selection_name_missing")
        profile_url = _normalized_text(
            summary.get("linkedin_url") or summary.get("profile_url"), maximum=512
        )
        source_kind = _profile_kind(profile_url)
        subject: dict[str, Any] = {
            "source_subject_ref": subject_ref,
            "source_record_ref": member_key,
            "source_record_sha256": "",
            "source_status": "source_bound",
            "source_kind": source_kind,
            "source_profile_url": profile_url,
            "name_text": name_text,
            "x_handle_proposals": _handle_proposals(
                proposal_map.get(member_key),
                selection_id=resolved_selection_id,
                subject_ref=subject_ref,
            ),
            "professional_facts": _professional_facts(
                summary,
                selection_id=resolved_selection_id,
                subject_ref=subject_ref,
            ),
        }
        subject["source_record_sha256"] = _content_sha256(subject, "source_record_sha256")
        subjects.append(subject)

    artifact: dict[str, Any] = {
        "schema_version": SELECTION_SCHEMA_VERSION,
        "selection_id": resolved_selection_id,
        "exported_at": exported_at,
        "snapshot": {
            "workspace_ref": workspace_id,
            "projection_ref": projection_id,
            "membership_revision": membership_revision,
            "source_candidate_count": source_candidate_count,
            "selected_candidate_count": len(subjects),
            "selected_member_keys_sha256": canonical_sha256(member_keys),
        },
        "subjects": subjects,
        "authority": dict(_SELECTION_AUTHORITY),
        "contract_schema_sha256": selection_contract_schema_sha256(),
        "artifact_sha256": "",
    }
    artifact["artifact_sha256"] = _content_sha256(artifact, "artifact_sha256")
    validate_subject_selection_artifact(artifact)
    return artifact


def validate_subject_selection_artifact(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise XFirstPortableAdapterError("x_first_selection_not_object")
    artifact = dict(value)
    if set(artifact) != {
        "schema_version",
        "selection_id",
        "exported_at",
        "snapshot",
        "subjects",
        "authority",
        "contract_schema_sha256",
        "artifact_sha256",
    }:
        raise XFirstPortableAdapterError("x_first_selection_shape_invalid")
    if artifact["schema_version"] != SELECTION_SCHEMA_VERSION:
        raise XFirstPortableAdapterError("x_first_selection_version_invalid")
    _identifier(artifact["selection_id"], "x_first_selection_id_invalid")
    _timestamp(artifact["exported_at"], "x_first_selection_exported_at_invalid")
    if artifact["authority"] != _SELECTION_AUTHORITY:
        raise XFirstPortableAdapterError("x_first_selection_authority_invalid")
    if artifact["contract_schema_sha256"] != selection_contract_schema_sha256():
        raise XFirstPortableAdapterError("x_first_selection_schema_digest_mismatch")
    if artifact["artifact_sha256"] != _content_sha256(artifact, "artifact_sha256"):
        raise XFirstPortableAdapterError("x_first_selection_hash_mismatch")

    snapshot = artifact["snapshot"]
    if not isinstance(snapshot, Mapping) or set(snapshot) != {
        "workspace_ref",
        "projection_ref",
        "membership_revision",
        "source_candidate_count",
        "selected_candidate_count",
        "selected_member_keys_sha256",
    }:
        raise XFirstPortableAdapterError("x_first_selection_snapshot_invalid")
    for field in ("workspace_ref", "projection_ref", "membership_revision"):
        if not isinstance(snapshot[field], str) or not snapshot[field].strip() or len(snapshot[field]) > 256:
            raise XFirstPortableAdapterError("x_first_selection_snapshot_invalid")
    source_count = snapshot["source_candidate_count"]
    selected_count = snapshot["selected_candidate_count"]
    if (
        isinstance(source_count, bool)
        or not isinstance(source_count, int)
        or isinstance(selected_count, bool)
        or not isinstance(selected_count, int)
        or selected_count < 1
        or source_count < selected_count
    ):
        raise XFirstPortableAdapterError("x_first_selection_snapshot_invalid")
    _sha256(snapshot["selected_member_keys_sha256"], "x_first_selection_snapshot_invalid")

    subjects = artifact["subjects"]
    if not isinstance(subjects, list) or len(subjects) != selected_count:
        raise XFirstPortableAdapterError("x_first_selection_subject_count_mismatch")
    subject_refs: set[str] = set()
    source_refs: set[str] = set()
    for raw_subject in subjects:
        if not isinstance(raw_subject, Mapping) or set(raw_subject) != _SUBJECT_FIELDS:
            raise XFirstPortableAdapterError("x_first_selection_subject_invalid")
        subject = dict(raw_subject)
        subject_ref = _identifier(subject["source_subject_ref"], "x_first_selection_subject_ref_invalid")
        source_ref = subject["source_record_ref"]
        if (
            subject_ref in subject_refs
            or not isinstance(source_ref, str)
            or not source_ref.strip()
            or len(source_ref) > 512
            or source_ref in source_refs
        ):
            raise XFirstPortableAdapterError("x_first_selection_subject_duplicate")
        subject_refs.add(subject_ref)
        source_refs.add(source_ref)
        if subject["source_status"] != "source_bound":
            raise XFirstPortableAdapterError("x_first_selection_source_status_invalid")
        if subject["source_kind"] not in {"linkedin_profile", "professional_profile", "name_only"}:
            raise XFirstPortableAdapterError("x_first_selection_source_kind_invalid")
        name_text = subject["name_text"]
        if not isinstance(name_text, str) or not name_text.strip() or len(name_text) > 512:
            raise XFirstPortableAdapterError("x_first_selection_name_invalid")
        profile_url = subject["source_profile_url"]
        if profile_url is not None and (not isinstance(profile_url, str) or len(profile_url) > 512):
            raise XFirstPortableAdapterError("x_first_selection_profile_url_invalid")
        if _profile_kind(profile_url) != subject["source_kind"]:
            raise XFirstPortableAdapterError("x_first_selection_source_kind_invalid")
        if subject["source_record_sha256"] != _content_sha256(subject, "source_record_sha256"):
            raise XFirstPortableAdapterError("x_first_selection_source_record_hash_mismatch")
        handles = subject["x_handle_proposals"]
        if not isinstance(handles, list):
            raise XFirstPortableAdapterError("x_first_selection_handle_invalid")
        handle_keys: set[str] = set()
        for proposal in handles:
            if not isinstance(proposal, Mapping) or set(proposal) != {
                "handle",
                "binding_status",
                "evidence_ref",
            }:
                raise XFirstPortableAdapterError("x_first_selection_handle_invalid")
            handle = proposal["handle"]
            if (
                not isinstance(handle, str)
                or _HANDLE_RE.fullmatch(handle) is None
                or handle.casefold() in handle_keys
                or proposal["binding_status"] != "cross_source_link_proposed"
                or not isinstance(proposal["evidence_ref"], str)
                or not proposal["evidence_ref"]
                or len(proposal["evidence_ref"]) > 512
            ):
                raise XFirstPortableAdapterError("x_first_selection_handle_invalid")
            handle_keys.add(handle.casefold())
        facts = subject["professional_facts"]
        if not isinstance(facts, list):
            raise XFirstPortableAdapterError("x_first_selection_fact_invalid")
        for fact in facts:
            if not isinstance(fact, Mapping) or set(fact) != {
                "fact_type",
                "value",
                "temporal_state",
                "evidence_ref",
            }:
                raise XFirstPortableAdapterError("x_first_selection_fact_invalid")
            if (
                fact["fact_type"] not in _FACT_TYPES
                or fact["temporal_state"] not in _TEMPORAL_STATES
                or not isinstance(fact["value"], str)
                or not fact["value"]
                or len(fact["value"]) > 1000
                or not isinstance(fact["evidence_ref"], str)
                or not fact["evidence_ref"]
                or len(fact["evidence_ref"]) > 512
            ):
                raise XFirstPortableAdapterError("x_first_selection_fact_invalid")
    if snapshot["selected_member_keys_sha256"] != canonical_sha256(sorted(source_refs)):
        raise XFirstPortableAdapterError("x_first_selection_member_set_hash_mismatch")


def _validate_request_binding(binding: Any, *, selection: Mapping[str, Any]) -> dict[str, str]:
    request_binding_contract_schema_sha256()
    if not isinstance(binding, Mapping):
        raise XFirstPortableAdapterError("x_first_request_binding_not_object")
    record = dict(binding)
    expected_fields = {
        "schema_version",
        "selection_id",
        "campaign_id",
        "selection_schema_version",
        "selection_contract_schema_sha256",
        "selection_artifact_sha256",
        "portable_request_schema_version",
        "portable_request_contract_schema_sha256",
        "portable_request_sha256",
        "subject_bindings",
        "authority",
        "binding_sha256",
    }
    if set(record) != expected_fields or record["schema_version"] != REQUEST_BINDING_SCHEMA_VERSION:
        raise XFirstPortableAdapterError("x_first_request_binding_shape_invalid")
    if (
        record["selection_id"] != selection["selection_id"]
        or _IDENTIFIER_RE.fullmatch(str(record["campaign_id"] or "")) is None
        or record["selection_schema_version"] != SELECTION_SCHEMA_VERSION
        or record["selection_contract_schema_sha256"] != selection_contract_schema_sha256()
        or record["selection_artifact_sha256"] != selection["artifact_sha256"]
        or record["portable_request_schema_version"] != PORTABLE_REQUEST_SCHEMA_VERSION
        or record["authority"] != _SELECTION_AUTHORITY
        or record["binding_sha256"] != _content_sha256(record, "binding_sha256")
    ):
        raise XFirstPortableAdapterError("x_first_request_binding_invalid")
    _sha256(record["portable_request_contract_schema_sha256"], "x_first_request_binding_invalid")
    _sha256(record["portable_request_sha256"], "x_first_request_binding_invalid")
    raw_bindings = record["subject_bindings"]
    if not isinstance(raw_bindings, list):
        raise XFirstPortableAdapterError("x_first_request_binding_invalid")
    expected = {subject["source_subject_ref"]: subject for subject in selection["subjects"]}
    mapped: dict[str, str] = {}
    for row in raw_bindings:
        if not isinstance(row, Mapping) or set(row) != {
            "source_subject_ref",
            "source_record_ref",
            "seed_ref",
            "source_record_sha256",
            "source_kind",
            "seed_sha256",
        }:
            raise XFirstPortableAdapterError("x_first_request_binding_invalid")
        source_ref = row["source_subject_ref"]
        seed_ref = row["seed_ref"]
        if (
            source_ref not in expected
            or source_ref in mapped
            or seed_ref != source_ref
            or row["source_record_ref"] != expected[source_ref]["source_record_ref"]
            or row["source_record_sha256"] != expected[source_ref]["source_record_sha256"]
            or row["source_kind"] != expected[source_ref]["source_kind"]
            or _SHA256_RE.fullmatch(str(row["seed_sha256"] or "")) is None
        ):
            raise XFirstPortableAdapterError("x_first_request_binding_invalid")
        mapped[source_ref] = seed_ref
    if set(mapped) != set(expected):
        raise XFirstPortableAdapterError("x_first_request_binding_incomplete")
    return mapped


def build_verification_import_preview(
    *,
    selection: Mapping[str, Any],
    request_binding: Mapping[str, Any],
    portable_result: Mapping[str, Any],
) -> dict[str, Any]:
    """Build a read-only review projection from a portable X-First result."""

    import_preview_contract_schema_sha256()
    validate_subject_selection_artifact(selection)
    subject_to_seed = _validate_request_binding(request_binding, selection=selection)
    result = dict(portable_result)
    if result.get("schema_version") != PORTABLE_RESULT_SCHEMA_VERSION:
        raise XFirstPortableAdapterError("x_first_portable_result_version_invalid")
    if result.get("result_sha256") != _content_sha256(result, "result_sha256"):
        raise XFirstPortableAdapterError("x_first_portable_result_hash_mismatch")
    if result.get("request_sha256") != request_binding["portable_request_sha256"]:
        raise XFirstPortableAdapterError("x_first_portable_result_request_mismatch")
    if result.get("campaign_id") != request_binding["campaign_id"]:
        raise XFirstPortableAdapterError("x_first_portable_result_campaign_mismatch")
    if result.get("authority") != _IMPORT_PREVIEW_AUTHORITY:
        raise XFirstPortableAdapterError("x_first_portable_result_authority_invalid")
    status = result.get("status")
    if status not in {"complete", "partial", "failed"}:
        raise XFirstPortableAdapterError("x_first_portable_result_status_invalid")

    outcomes: dict[str, Mapping[str, Any]] = {}
    for outcome in result.get("subject_outcomes", []):
        if not isinstance(outcome, Mapping):
            raise XFirstPortableAdapterError("x_first_portable_result_subject_outcome_invalid")
        seed_ref = outcome.get("seed_ref")
        if seed_ref in outcomes:
            raise XFirstPortableAdapterError("x_first_portable_result_subject_outcome_invalid")
        outcomes[str(seed_ref)] = outcome
    if set(outcomes) != set(subject_to_seed.values()):
        raise XFirstPortableAdapterError("x_first_portable_result_subject_outcome_incomplete")

    account_refs = {
        str(account.get("x_account_ref"))
        for account in result.get("external_accounts", [])
        if isinstance(account, Mapping)
    }
    proposal_refs_by_seed: dict[str, list[str]] = {}
    for proposal in result.get("cross_source_link_proposals", []):
        if not isinstance(proposal, Mapping):
            raise XFirstPortableAdapterError("x_first_portable_result_link_proposal_invalid")
        seed_ref = str(proposal.get("seed_ref") or "")
        account_ref = str(proposal.get("x_account_ref") or "")
        proposal_ref = str(proposal.get("proposal_id") or "")
        if seed_ref not in outcomes or account_ref not in account_refs or not proposal_ref:
            raise XFirstPortableAdapterError("x_first_portable_result_link_proposal_invalid")
        proposal_refs_by_seed.setdefault(seed_ref, []).append(proposal_ref)

    observations_by_account: dict[str, list[str]] = {}
    for observation in result.get("observations", []):
        if not isinstance(observation, Mapping):
            raise XFirstPortableAdapterError("x_first_portable_result_observation_invalid")
        account_ref = str(observation.get("x_account_ref") or "")
        observation_ref = str(observation.get("observation_id") or "")
        if account_ref not in account_refs or not observation_ref:
            raise XFirstPortableAdapterError("x_first_portable_result_observation_invalid")
        observations_by_account.setdefault(account_ref, []).append(observation_ref)

    dimensions_by_account: dict[str, list[dict[str, Any]]] = {}
    for row in result.get("dimension_results", []):
        if not isinstance(row, Mapping):
            raise XFirstPortableAdapterError("x_first_portable_result_dimension_invalid")
        account_ref = str(row.get("x_account_ref") or "")
        if account_ref not in account_refs:
            raise XFirstPortableAdapterError("x_first_portable_result_dimension_invalid")
        dimensions_by_account.setdefault(account_ref, []).append(
            {
                "question_id": row.get("question_id"),
                "dimension_id": row.get("dimension_id"),
                "matched_label_ids": list(row.get("matched_label_ids") or []),
                "relevance_state": row.get("relevance_state"),
                "target_activity_temporal_state": row.get("target_activity_temporal_state"),
                "evidence_refs": list(row.get("evidence_refs") or []),
            }
        )

    subjects: list[dict[str, Any]] = []
    for source_subject in selection["subjects"]:
        source_subject_ref = source_subject["source_subject_ref"]
        seed_ref = subject_to_seed[source_subject_ref]
        outcome = outcomes[seed_ref]
        terminal_state = outcome.get("terminal_state")
        x_account_refs = list(outcome.get("x_account_refs") or [])
        if any(account_ref not in account_refs for account_ref in x_account_refs):
            raise XFirstPortableAdapterError("x_first_portable_result_subject_account_invalid")
        if terminal_state == "analyzed":
            review_state = "identity_review_required"
            if not proposal_refs_by_seed.get(seed_ref):
                raise XFirstPortableAdapterError("x_first_portable_result_link_proposal_missing")
        elif terminal_state == "handle_resolution_required":
            review_state = "handle_resolution_required"
        elif terminal_state == "no_verified_account":
            review_state = "no_verified_account"
        elif terminal_state == "failed":
            review_state = "execution_failed"
        else:
            raise XFirstPortableAdapterError("x_first_portable_result_subject_outcome_invalid")
        subjects.append(
            {
                "source_subject_ref": source_subject_ref,
                "source_record_ref": source_subject["source_record_ref"],
                "seed_ref": seed_ref,
                "terminal_state": terminal_state,
                "x_account_refs": sorted(x_account_refs),
                "link_proposal_refs": sorted(proposal_refs_by_seed.get(seed_ref, [])),
                "observation_refs": sorted(
                    {
                        observation_ref
                        for account_ref in x_account_refs
                        for observation_ref in observations_by_account.get(account_ref, [])
                    }
                ),
                "verification_summaries": sorted(
                    [
                        summary
                        for account_ref in x_account_refs
                        for summary in dimensions_by_account.get(account_ref, [])
                    ],
                    key=lambda row: (str(row["question_id"]), str(row["dimension_id"])),
                ),
                "review_state": review_state,
            }
        )
    preview: dict[str, Any] = {
        "schema_version": IMPORT_PREVIEW_SCHEMA_VERSION,
        "selection_artifact_sha256": selection["artifact_sha256"],
        "request_binding_sha256": request_binding["binding_sha256"],
        "portable_result_sha256": result["result_sha256"],
        "portable_result_status": status,
        "subjects": sorted(subjects, key=lambda row: row["source_subject_ref"]),
        "authority": dict(_IMPORT_PREVIEW_AUTHORITY),
        "preview_sha256": "",
    }
    preview["preview_sha256"] = _content_sha256(preview, "preview_sha256")
    return preview


__all__ = [
    "IMPORT_PREVIEW_SCHEMA_VERSION",
    "IMPORT_PREVIEW_CONTRACT_SCHEMA_SHA256",
    "PORTABLE_REQUEST_SCHEMA_VERSION",
    "PORTABLE_RESULT_SCHEMA_VERSION",
    "REQUEST_BINDING_CONTRACT_SCHEMA_SHA256",
    "REQUEST_BINDING_SCHEMA_VERSION",
    "SELECTION_CONTRACT_SCHEMA_SHA256",
    "SELECTION_SCHEMA_VERSION",
    "XFirstPortableAdapterError",
    "build_subject_selection_artifact",
    "build_verification_import_preview",
    "canonical_json",
    "canonical_sha256",
    "import_preview_contract_schema_sha256",
    "request_binding_contract_schema_sha256",
    "selection_contract_schema_sha256",
    "validate_subject_selection_artifact",
]
