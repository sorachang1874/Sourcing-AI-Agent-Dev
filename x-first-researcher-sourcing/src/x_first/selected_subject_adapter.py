"""Artifact-only adapter from a selected subject snapshot to an X-First request.

The product-owned selection contract is vendored for byte-exact validation.  This
module never imports the product runtime and never writes canonical person state.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from x_first.recall_pool_schema import (
    MiniDraft202012Error,
    assert_schema_valid,
    contract_schema_sha256,
)
from x_first.research_orchestration import (
    REQUEST_SCHEMA_FILE,
    REQUEST_SCHEMA_VERSION,
    canonical_sha256,
    validate_campaign_request,
)

SELECTION_SCHEMA_VERSION = "sourcing.x_first.subject_selection.v1"
SELECTION_SCHEMA_FILE = "sourcing.x_first.subject_selection.v1.schema.json"
BINDING_SCHEMA_VERSION = "x.portable.selected_subject.request_binding.v1"
BINDING_SCHEMA_FILE = "x.portable.selected_subject.request_binding.v1.schema.json"
SELECTION_CONTRACT_SCHEMA_SHA256 = "e487190924efeaf9bf05f5836619867a5cece66d707891f4587665c115b281bc"
BINDING_CONTRACT_SCHEMA_SHA256 = "ef2f6dc742658a5b6507f7320bc2ed5530612e97097eadf1c3e4a46405835c1d"

_FALSE_AUTHORITY = {
    "provider_calls_allowed": False,
    "product_writes_allowed": False,
    "canonical_person_merge_allowed": False,
    "outreach_allowed": False,
}
_CAMPAIGN_INTENT_FIELDS = {
    "schema_version",
    "campaign_id",
    "as_of",
    "scope_selection",
    "analysis_questions",
    "temporal_scope",
    "channel_overrides",
    "experience_verification",
}


class SelectedSubjectAdapterError(ValueError):
    """Stable fail-closed error for selected-subject artifact adaptation."""


def _content_sha256(value: Mapping[str, Any], field: str) -> str:
    return canonical_sha256({key: item for key, item in value.items() if key != field})


def _assert_schema(value: Any, filename: str, error: str) -> None:
    try:
        assert_schema_valid(value, filename)
    except MiniDraft202012Error as exc:
        raise SelectedSubjectAdapterError(error) from exc


def _validate_timestamp(value: str) -> None:
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except (TypeError, ValueError) as exc:
        raise SelectedSubjectAdapterError("subject_selection_exported_at_invalid") from exc


def selected_member_keys_sha256(subjects: Sequence[Mapping[str, Any]]) -> str:
    """Hash the exact, order-independent selected source-subject key set."""

    return canonical_sha256(sorted(subject["source_record_ref"] for subject in subjects))


def _portable_seed(subject: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "seed_ref": subject["source_subject_ref"],
        "source_kind": subject["source_kind"],
        "external_record_ref": subject["source_record_ref"],
        "source_record_sha256": subject["source_record_sha256"],
        "source_status": subject["source_status"],
        "source_profile_url": subject["source_profile_url"],
        "name_text": subject["name_text"],
        "x_handle_proposals": copy.deepcopy(subject["x_handle_proposals"]),
        "professional_facts": copy.deepcopy(subject["professional_facts"]),
    }


def validate_subject_selection(selection: Any) -> None:
    """Validate one product-owned selection artifact against its vendored schema."""

    if not isinstance(selection, Mapping):
        raise SelectedSubjectAdapterError("subject_selection_not_object")
    if contract_schema_sha256(SELECTION_SCHEMA_FILE) != SELECTION_CONTRACT_SCHEMA_SHA256:
        raise SelectedSubjectAdapterError("subject_selection_local_schema_digest_mismatch")
    _assert_schema(selection, SELECTION_SCHEMA_FILE, "subject_selection_schema_invalid")
    if selection["schema_version"] != SELECTION_SCHEMA_VERSION:
        raise SelectedSubjectAdapterError("subject_selection_version_invalid")
    if selection["contract_schema_sha256"] != contract_schema_sha256(SELECTION_SCHEMA_FILE):
        raise SelectedSubjectAdapterError("subject_selection_contract_schema_sha256_mismatch")
    _validate_timestamp(selection["exported_at"])

    subjects = selection["subjects"]
    source_subject_refs = [subject["source_subject_ref"] for subject in subjects]
    source_record_refs = [subject["source_record_ref"] for subject in subjects]
    if len(source_subject_refs) != len(set(source_subject_refs)):
        raise SelectedSubjectAdapterError("subject_selection_subject_ref_duplicate")
    if len(source_record_refs) != len(set(source_record_refs)):
        raise SelectedSubjectAdapterError("subject_selection_record_ref_duplicate")
    snapshot = selection["snapshot"]
    if (
        snapshot["selected_candidate_count"] != len(subjects)
        or snapshot["source_candidate_count"] < snapshot["selected_candidate_count"]
    ):
        raise SelectedSubjectAdapterError("subject_selection_count_invalid")
    if snapshot["selected_member_keys_sha256"] != selected_member_keys_sha256(subjects):
        raise SelectedSubjectAdapterError("subject_selection_member_set_sha256_mismatch")

    for subject in subjects:
        if subject["source_record_sha256"] != _content_sha256(subject, "source_record_sha256"):
            raise SelectedSubjectAdapterError("subject_selection_source_record_sha256_mismatch")
        handles = [row["handle"].casefold() for row in subject["x_handle_proposals"]]
        if len(handles) != len(set(handles)):
            raise SelectedSubjectAdapterError("subject_selection_handle_proposal_duplicate")
        if subject["source_kind"] == "name_only" and (
            subject["name_text"] is None
            or subject["source_profile_url"] is not None
            or subject["x_handle_proposals"]
        ):
            raise SelectedSubjectAdapterError("subject_selection_name_only_shape_invalid")
        if subject["source_kind"] != "name_only" and subject["name_text"] is None:
            raise SelectedSubjectAdapterError("subject_selection_profile_name_missing")

    if selection["artifact_sha256"] != _content_sha256(selection, "artifact_sha256"):
        raise SelectedSubjectAdapterError("subject_selection_artifact_sha256_mismatch")


def _expected_seeds(selection: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        _portable_seed(subject)
        for subject in sorted(selection["subjects"], key=lambda row: row["source_subject_ref"])
    ]


def _expected_subject_bindings(
    selection: Mapping[str, Any], request: Mapping[str, Any]
) -> list[dict[str, Any]]:
    seeds = {seed["seed_ref"]: seed for seed in request["seed_inputs"]}
    return [
        {
            "source_subject_ref": subject["source_subject_ref"],
            "source_record_ref": subject["source_record_ref"],
            "source_record_sha256": subject["source_record_sha256"],
            "source_kind": subject["source_kind"],
            "seed_ref": subject["source_subject_ref"],
            "seed_sha256": canonical_sha256(seeds[subject["source_subject_ref"]]),
        }
        for subject in sorted(selection["subjects"], key=lambda row: row["source_subject_ref"])
    ]


def validate_request_binding(
    binding: Any,
    *,
    selection: Mapping[str, Any],
    request: Mapping[str, Any],
) -> None:
    """Validate exact selection-to-portable-request subject and schema bindings."""

    validate_subject_selection(selection)
    if contract_schema_sha256(BINDING_SCHEMA_FILE) != BINDING_CONTRACT_SCHEMA_SHA256:
        raise SelectedSubjectAdapterError("selected_subject_binding_local_schema_digest_mismatch")
    if not isinstance(binding, Mapping):
        raise SelectedSubjectAdapterError("selected_subject_binding_not_object")
    _assert_schema(binding, BINDING_SCHEMA_FILE, "selected_subject_binding_schema_invalid")
    _assert_schema(request, REQUEST_SCHEMA_FILE, "selected_subject_portable_request_schema_invalid")
    if request.get("request_sha256") != _content_sha256(request, "request_sha256"):
        raise SelectedSubjectAdapterError("selected_subject_portable_request_sha256_mismatch")
    if request.get("seed_inputs") != _expected_seeds(selection):
        raise SelectedSubjectAdapterError("selected_subject_seed_mapping_mismatch")
    if (
        binding["schema_version"] != BINDING_SCHEMA_VERSION
        or binding["selection_id"] != selection["selection_id"]
        or binding["campaign_id"] != request["campaign_id"]
        or binding["selection_schema_version"] != selection["schema_version"]
        or binding["selection_contract_schema_sha256"]
        != contract_schema_sha256(SELECTION_SCHEMA_FILE)
        or binding["selection_artifact_sha256"] != selection["artifact_sha256"]
        or binding["portable_request_schema_version"] != request["schema_version"]
        or binding["portable_request_contract_schema_sha256"]
        != contract_schema_sha256(REQUEST_SCHEMA_FILE)
        or binding["portable_request_sha256"] != request["request_sha256"]
        or binding["subject_bindings"] != _expected_subject_bindings(selection, request)
    ):
        raise SelectedSubjectAdapterError("selected_subject_binding_mismatch")
    if binding["binding_sha256"] != _content_sha256(binding, "binding_sha256"):
        raise SelectedSubjectAdapterError("selected_subject_binding_sha256_mismatch")


def build_selected_subject_request_binding(
    *,
    selection: Mapping[str, Any],
    campaign_intent: Mapping[str, Any],
    catalog: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build and bind one portable request without importing product runtime state."""

    validate_subject_selection(selection)
    if not isinstance(campaign_intent, Mapping) or set(campaign_intent) != _CAMPAIGN_INTENT_FIELDS:
        raise SelectedSubjectAdapterError("selected_subject_campaign_intent_shape_invalid")
    if campaign_intent.get("schema_version") != REQUEST_SCHEMA_VERSION:
        raise SelectedSubjectAdapterError("selected_subject_campaign_intent_version_invalid")

    request = copy.deepcopy(dict(campaign_intent))
    request["seed_inputs"] = _expected_seeds(selection)
    request["authority"] = dict(_FALSE_AUTHORITY)
    request["request_sha256"] = ""
    request["request_sha256"] = _content_sha256(request, "request_sha256")
    validate_campaign_request(request, catalog=catalog, policy=policy)

    binding: dict[str, Any] = {
        "schema_version": BINDING_SCHEMA_VERSION,
        "selection_id": selection["selection_id"],
        "campaign_id": request["campaign_id"],
        "selection_schema_version": selection["schema_version"],
        "selection_contract_schema_sha256": contract_schema_sha256(SELECTION_SCHEMA_FILE),
        "selection_artifact_sha256": selection["artifact_sha256"],
        "portable_request_schema_version": request["schema_version"],
        "portable_request_contract_schema_sha256": contract_schema_sha256(REQUEST_SCHEMA_FILE),
        "portable_request_sha256": request["request_sha256"],
        "subject_bindings": _expected_subject_bindings(selection, request),
        "authority": dict(_FALSE_AUTHORITY),
        "binding_sha256": "",
    }
    binding["binding_sha256"] = _content_sha256(binding, "binding_sha256")
    validate_request_binding(binding, selection=selection, request=request)
    return request, binding
