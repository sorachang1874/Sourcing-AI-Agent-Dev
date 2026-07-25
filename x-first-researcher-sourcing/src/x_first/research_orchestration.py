"""Provider-free generalized orchestration for portable X research campaigns.

This layer resolves a fresh scope catalog, runtime analysis taxonomies, evidence
channels, and cross-source seed proposals into an offline task plan.  It never
calls Grok, Luna, X, LinkedIn, or product/runtime owners.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from x_first.native_x_evidence_contract import classify_single_handle_query_surface
from x_first.recall_pool_schema import assert_schema_valid

POLICY_SCHEMA_VERSION = "x.research_orchestration.policy.v1"
CATALOG_SCHEMA_VERSION = "x.research_scope.catalog.v1"
REQUEST_SCHEMA_VERSION = "x.portable.research_campaign.request.v1"
PLAN_SCHEMA_VERSION = "x.portable.research_campaign.plan.v1"
RESULT_SCHEMA_VERSION = "x.portable.research_campaign.result.v1"

POLICY_SCHEMA_FILE = "x.research_orchestration.policy.v1.schema.json"
CATALOG_SCHEMA_FILE = "x.research_scope.catalog.v1.schema.json"
REQUEST_SCHEMA_FILE = "x.portable.research_campaign.request.v1.schema.json"
PLAN_SCHEMA_FILE = "x.portable.research_campaign.plan.v1.schema.json"
RESULT_SCHEMA_FILE = "x.portable.research_campaign.result.v1.schema.json"

_IDENTIFIER_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,127}")
_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_TIMESTAMP_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")


class ResearchOrchestrationError(ValueError):
    """Stable fail-closed error for portable research planning."""


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"), sort_keys=True)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _content_sha256(value: Mapping[str, Any], field: str) -> str:
    return canonical_sha256({key: item for key, item in value.items() if key != field})


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ResearchOrchestrationError("duplicate_json_key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ResearchOrchestrationError(f"non_finite_json_number:{value}")


def strict_load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ResearchOrchestrationError("json_file_invalid") from exc
    if not isinstance(value, dict):
        raise ResearchOrchestrationError("json_root_not_object")
    return value


def _identifier(value: Any, error: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise ResearchOrchestrationError(error)
    return value


def _timestamp(value: Any, error: str) -> datetime:
    if not isinstance(value, str) or _TIMESTAMP_RE.fullmatch(value) is None:
        raise ResearchOrchestrationError(error)
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as exc:
        raise ResearchOrchestrationError(error) from exc
    return parsed


def _bounded_text(value: Any, *, maximum: int, error: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ResearchOrchestrationError(error)
    return value


def _recall_query_term(alias: str) -> str:
    if (
        alias != alias.strip()
        or '"' in alias
        or "\\" in alias
        or any(ord(character) < 32 or ord(character) == 127 for character in alias)
    ):
        raise ResearchOrchestrationError("scope_catalog_recall_alias_unsafe")
    return f'"{alias}"'


def validate_policy(policy: Any) -> None:
    if not isinstance(policy, Mapping):
        raise ResearchOrchestrationError("orchestration_policy_not_object")
    assert_schema_valid(policy, POLICY_SCHEMA_FILE)
    if policy.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise ResearchOrchestrationError("orchestration_policy_version_invalid")
    registry = policy["channel_registry"]
    registry_ids = [row["channel_id"] for row in registry]
    if registry_ids != policy["channel_order"] or len(registry_ids) != len(set(registry_ids)):
        raise ResearchOrchestrationError("orchestration_channel_registry_invalid")
    defaults = {row["channel_id"]: row["default_enabled"] for row in registry}
    if defaults != {
        "candidate_authored_surface": True,
        "project_direct_credit": False,
        "official_source": False,
        "conversation_graph": False,
    }:
        raise ResearchOrchestrationError("orchestration_channel_defaults_invalid")
    if set(policy["active_followup_states"]) | {"out_of_scope"} != set(policy["generic_relevance_states"]):
        raise ResearchOrchestrationError("orchestration_relevance_partition_invalid")
    if not set(policy["default_precision_states"]).issubset(policy["active_followup_states"]):
        raise ResearchOrchestrationError("orchestration_precision_states_invalid")
    if policy["experience_verification"]["default_enabled"] is not False:
        raise ResearchOrchestrationError("orchestration_experience_default_invalid")


def load_policy(path: Path | None = None) -> dict[str, Any]:
    resolved = path or project_root() / "configs" / "research_orchestration_policy.v1.json"
    policy = strict_load_json(resolved)
    validate_policy(policy)
    return policy


def _validate_no_parent_cycle(nodes: Mapping[str, Mapping[str, Any]]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(scope_id: str) -> None:
        if scope_id in visiting:
            raise ResearchOrchestrationError("scope_catalog_parent_cycle")
        if scope_id in visited:
            return
        visiting.add(scope_id)
        for parent_id in nodes[scope_id]["containment_parent_scope_ids"]:
            visit(parent_id)
        visiting.remove(scope_id)
        visited.add(scope_id)

    for scope_id in nodes:
        visit(scope_id)


def validate_scope_catalog(catalog: Any, *, policy: Mapping[str, Any]) -> None:
    validate_policy(policy)
    if not isinstance(catalog, Mapping):
        raise ResearchOrchestrationError("scope_catalog_not_object")
    assert_schema_valid(catalog, CATALOG_SCHEMA_FILE)
    if catalog.get("schema_version") != CATALOG_SCHEMA_VERSION:
        raise ResearchOrchestrationError("scope_catalog_version_invalid")
    if catalog["catalog_sha256"] != _content_sha256(catalog, "catalog_sha256"):
        raise ResearchOrchestrationError("scope_catalog_hash_mismatch")
    generated_at = _timestamp(catalog["generated_at"], "scope_catalog_generated_at_invalid")
    nodes: dict[str, Mapping[str, Any]] = {}
    for row in catalog["scope_nodes"]:
        scope_id = _identifier(row["scope_id"], "scope_catalog_node_id_invalid")
        if scope_id in nodes:
            raise ResearchOrchestrationError("scope_catalog_node_duplicate")
        if row["scope_kind"] not in policy["supported_scope_node_kinds"]:
            raise ResearchOrchestrationError("scope_catalog_node_kind_invalid")
        if len({alias.casefold() for alias in row["aliases"]}) != len(row["aliases"]):
            raise ResearchOrchestrationError("scope_catalog_alias_duplicate")
        last_verified = _timestamp(row["last_verified_at"], "scope_catalog_node_verified_at_invalid")
        refresh_after = _timestamp(row["refresh_after"], "scope_catalog_node_refresh_after_invalid")
        if last_verified > generated_at or refresh_after <= last_verified:
            raise ResearchOrchestrationError("scope_catalog_node_freshness_window_invalid")
        nodes[scope_id] = row
    for scope_id, row in nodes.items():
        for parent_id in row["containment_parent_scope_ids"]:
            if parent_id not in nodes or parent_id == scope_id:
                raise ResearchOrchestrationError("scope_catalog_parent_invalid")
    _validate_no_parent_cycle(nodes)

    dimensions: set[str] = set()
    for taxonomy in catalog["taxonomies"]:
        dimension_id = _identifier(taxonomy["dimension_id"], "scope_catalog_dimension_id_invalid")
        if dimension_id in dimensions:
            raise ResearchOrchestrationError("scope_catalog_dimension_duplicate")
        dimensions.add(dimension_id)
        label_ids = [row["label_id"] for row in taxonomy["labels"]]
        if len(label_ids) != len(set(label_ids)):
            raise ResearchOrchestrationError("scope_catalog_label_duplicate")
        alias_owners: dict[str, str] = {}
        for label in taxonomy["labels"]:
            if len({alias.casefold() for alias in label["aliases"]}) != len(label["aliases"]):
                raise ResearchOrchestrationError("scope_catalog_label_alias_duplicate")
            for alias in label["aliases"]:
                _recall_query_term(alias)
                alias_key = alias.casefold()
                prior_owner = alias_owners.setdefault(alias_key, label["label_id"])
                if prior_owner != label["label_id"]:
                    raise ResearchOrchestrationError("scope_catalog_label_alias_ambiguous")

    coverage_ids: set[str] = set()
    for coverage in catalog["coverage_assertions"]:
        coverage_id = _identifier(coverage["coverage_id"], "scope_catalog_coverage_id_invalid")
        if coverage_id in coverage_ids:
            raise ResearchOrchestrationError("scope_catalog_coverage_duplicate")
        coverage_ids.add(coverage_id)
        if coverage["root_scope_id"] not in nodes:
            raise ResearchOrchestrationError("scope_catalog_coverage_root_unknown")
        member_ids = coverage["member_scope_ids"]
        if any(member_id not in nodes for member_id in member_ids):
            raise ResearchOrchestrationError("scope_catalog_coverage_member_unknown")
        if member_ids != sorted(member_ids) or coverage["member_set_sha256"] != canonical_sha256(member_ids):
            raise ResearchOrchestrationError("scope_catalog_coverage_member_digest_invalid")
        expected_members = sorted(
            scope_id
            for scope_id, node in nodes.items()
            if node["scope_kind"] in coverage["scope_kinds"]
            and _descends_from(scope_id, coverage["root_scope_id"], nodes)
        )
        if not set(member_ids).issubset(expected_members):
            raise ResearchOrchestrationError("scope_catalog_coverage_member_outside_claim")
        if coverage["coverage_status"] == "complete" and (member_ids != expected_members or coverage["open_gaps"]):
            raise ResearchOrchestrationError("scope_catalog_complete_membership_invalid")
        if coverage["coverage_status"] == "complete" and coverage["source_status"] == "model_mediated_unverified":
            raise ResearchOrchestrationError("scope_catalog_complete_coverage_source_not_authoritative")
        last_verified = _timestamp(coverage["last_verified_at"], "scope_catalog_coverage_verified_at_invalid")
        refresh_after = _timestamp(coverage["refresh_after"], "scope_catalog_coverage_refresh_after_invalid")
        if last_verified > generated_at or refresh_after <= last_verified:
            raise ResearchOrchestrationError("scope_catalog_coverage_freshness_window_invalid")


def _taxonomy_index(catalog: Mapping[str, Any]) -> dict[str, dict[str, Mapping[str, Any]]]:
    return {
        taxonomy["dimension_id"]: {label["label_id"]: label for label in taxonomy["labels"]}
        for taxonomy in catalog["taxonomies"]
    }


def _profile_url_host(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ResearchOrchestrationError("portable_seed_profile_url_invalid")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ResearchOrchestrationError("portable_seed_profile_url_invalid") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.fragment
    ):
        raise ResearchOrchestrationError("portable_seed_profile_url_invalid")
    return parsed.hostname.casefold()


def _x_profile_url_matches_handle(value: str, handle: str) -> bool:
    parsed = urlsplit(value)
    return (
        parsed.query == ""
        and parsed.path.strip("/").casefold() == handle.casefold()
        and "/" not in parsed.path.strip("/")
    )


def _x_status_url_matches(value: str, handle: str, object_id: str) -> bool:
    parsed = urlsplit(value)
    return parsed.query == "" and parsed.path.strip("/").split("/") == [handle, "status", object_id]


def _source_status_is_compatible(claim: str, evidence_statuses: set[str]) -> bool:
    if not evidence_statuses:
        return claim == "model_mediated_unverified"
    if "fixture_synthetic" in evidence_statuses:
        return evidence_statuses == {"fixture_synthetic"} and claim == "fixture_synthetic"
    if "model_mediated_unverified" in evidence_statuses:
        return claim == "model_mediated_unverified"
    if "human_supplied_unverified" in evidence_statuses:
        return claim == "human_supplied_unverified"
    return evidence_statuses == {"source_bound"} and claim == "source_bound"


def _observation_status_for_attempt(source_status: str) -> str:
    return {
        "fixture_synthetic": "fixture_synthetic",
        "receipt_bound": "source_bound",
        "unverified": "model_mediated_unverified",
    }[source_status]


def _attempt_status_summary(statuses: set[str]) -> str:
    if not statuses:
        return "unverified"
    if "fixture_synthetic" in statuses:
        if statuses != {"fixture_synthetic"}:
            raise ResearchOrchestrationError("portable_result_mixed_fixture_attempt_provenance")
        return "fixture_synthetic"
    if statuses == {"receipt_bound"}:
        return "receipt_bound"
    return "unverified"


def _validate_execution_attempt_shape(row: Mapping[str, Any], *, count_field: str) -> None:
    count = row[count_field]
    execution_state = row["execution_state"]
    continuation_state = row["continuation_state"]
    if execution_state == "completed" and count < 1:
        raise ResearchOrchestrationError("portable_result_completed_attempt_without_bound_result")
    if execution_state in {"no_result", "failed"} and count != 0:
        raise ResearchOrchestrationError("portable_result_empty_attempt_with_bound_result")
    if execution_state == "no_result" and continuation_state != "exhausted":
        raise ResearchOrchestrationError("portable_result_no_result_attempt_not_exhausted")
    if execution_state == "failed" and (
        row["result_truncated"] or continuation_state != "unknown" or row["continuation_ref"] is not None
    ):
        raise ResearchOrchestrationError("portable_result_failed_attempt_frontier_invalid")
    if continuation_state == "continuation_available":
        if not row["result_truncated"] or row["continuation_ref"] is None:
            raise ResearchOrchestrationError("portable_result_attempt_continuation_invalid")
    elif row["result_truncated"] or row["continuation_ref"] is not None:
        raise ResearchOrchestrationError("portable_result_attempt_continuation_invalid")


def _validate_pagination_chain(
    attempts: list[Mapping[str, Any]],
    *,
    error: str,
    ordinal_field: str = "ordinal",
) -> bool:
    """Validate an ordered continuation chain and return whether it is exhausted."""

    if not attempts:
        return False
    ordered = sorted(attempts, key=lambda row: row[ordinal_field])
    if [row[ordinal_field] for row in ordered] != list(range(1, len(ordered) + 1)):
        raise ResearchOrchestrationError(error)
    expected_input: str | None = None
    for index, row in enumerate(ordered):
        if row["input_continuation_ref"] != expected_input:
            raise ResearchOrchestrationError(error)
        if row["execution_state"] == "failed":
            continue
        if row["continuation_state"] == "continuation_available":
            expected_input = row["continuation_ref"]
            continue
        if index < len(ordered) - 1:
            raise ResearchOrchestrationError(error)
    return ordered[-1]["execution_state"] != "failed" and ordered[-1]["continuation_state"] == "exhausted"


def validate_campaign_request(
    request: Any,
    *,
    catalog: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    validate_scope_catalog(catalog, policy=policy)
    if not isinstance(request, Mapping):
        raise ResearchOrchestrationError("portable_campaign_request_not_object")
    assert_schema_valid(request, REQUEST_SCHEMA_FILE)
    if request.get("schema_version") != REQUEST_SCHEMA_VERSION:
        raise ResearchOrchestrationError("portable_campaign_request_version_invalid")
    if request["request_sha256"] != _content_sha256(request, "request_sha256"):
        raise ResearchOrchestrationError("portable_campaign_request_hash_mismatch")
    _timestamp(request["as_of"], "portable_campaign_as_of_invalid")

    nodes = {row["scope_id"]: row for row in catalog["scope_nodes"]}
    selection = request["scope_selection"]
    if any(scope_id not in nodes for scope_id in selection["root_scope_ids"]):
        raise ResearchOrchestrationError("portable_campaign_root_scope_unknown")
    if any(scope_id not in nodes for scope_id in selection["selected_scope_ids"]):
        raise ResearchOrchestrationError("portable_campaign_selected_scope_unknown")
    if selection["selection_mode"] == "complete_under_roots" and selection["selected_scope_ids"]:
        raise ResearchOrchestrationError("portable_campaign_complete_selection_must_be_empty")
    if selection["selection_mode"] == "explicit_nodes" and not selection["selected_scope_ids"]:
        raise ResearchOrchestrationError("portable_campaign_explicit_selection_empty")

    taxonomies = _taxonomy_index(catalog)
    question_ids: set[str] = set()
    for question in request["analysis_questions"]:
        question_id = _identifier(question["question_id"], "portable_campaign_question_id_invalid")
        if question_id in question_ids:
            raise ResearchOrchestrationError("portable_campaign_question_duplicate")
        question_ids.add(question_id)
        if question["analysis_mode"] not in policy["supported_analysis_modes"]:
            raise ResearchOrchestrationError("portable_campaign_analysis_mode_invalid")
        dimension = taxonomies.get(question["dimension_id"])
        if dimension is None:
            raise ResearchOrchestrationError("portable_campaign_dimension_unknown")
        if any(label_id not in dimension for label_id in question["target_label_ids"]):
            raise ResearchOrchestrationError("portable_campaign_target_label_unknown")
        if question["analysis_mode"] in {"verification", "hybrid"} and not question["target_label_ids"]:
            raise ResearchOrchestrationError("portable_campaign_verification_target_empty")
        if question["analysis_mode"] == "exploratory":
            if (
                question["target_match_operator"] != "not_applicable"
                or question["recall_execution_policy"] != "authored_surface_only"
                or question["adaptive_stop_policy"] is not None
            ):
                raise ResearchOrchestrationError("portable_campaign_exploratory_policy_invalid")
        elif question["target_match_operator"] not in {"any", "all"}:
            raise ResearchOrchestrationError("portable_campaign_target_policy_invalid")
        if question["recall_execution_policy"] == "adaptive_marginal_gain":
            adaptive_policy = question["adaptive_stop_policy"]
            if adaptive_policy is None:
                raise ResearchOrchestrationError("portable_campaign_adaptive_stop_policy_missing")
            alias_count = len(
                {
                    alias.casefold()
                    for label_id in question["target_label_ids"]
                    for alias in dimension[label_id]["aliases"]
                }
            )
            available_query_count = alias_count * 2
            if (
                adaptive_policy["window_size"] > available_query_count
                or adaptive_policy["window_size"] < adaptive_policy["minimum_attempts_per_surface"] * 2
            ):
                raise ResearchOrchestrationError("portable_campaign_adaptive_stop_policy_unachievable")
        elif question["adaptive_stop_policy"] is not None:
            raise ResearchOrchestrationError("portable_campaign_nonadaptive_stop_policy_invalid")

    channel_ids = [row["channel_id"] for row in request["channel_overrides"]]
    if len(channel_ids) != len(set(channel_ids)) or any(
        channel_id not in policy["channel_order"] for channel_id in channel_ids
    ):
        raise ResearchOrchestrationError("portable_campaign_channel_override_invalid")

    seed_refs: set[str] = set()
    accepted_sources = set(policy["portability"]["accepted_seed_source_kinds"])
    for seed in request["seed_inputs"]:
        seed_ref = _identifier(seed["seed_ref"], "portable_seed_ref_invalid")
        if seed_ref in seed_refs:
            raise ResearchOrchestrationError("portable_seed_ref_duplicate")
        seed_refs.add(seed_ref)
        if seed["source_kind"] not in accepted_sources:
            raise ResearchOrchestrationError("portable_seed_source_kind_invalid")
        profile_host = _profile_url_host(seed["source_profile_url"])
        handles = [row["handle"].casefold() for row in seed["x_handle_proposals"]]
        if len(handles) != len(set(handles)):
            raise ResearchOrchestrationError("portable_seed_handle_duplicate")
        if seed["source_kind"] == "x_account":
            if len(seed["x_handle_proposals"]) != 1 or any(
                row["binding_status"] != "source_asserted" for row in seed["x_handle_proposals"]
            ):
                raise ResearchOrchestrationError("portable_x_seed_binding_invalid")
            if profile_host is not None and profile_host not in {
                "x.com",
                "www.x.com",
                "twitter.com",
                "www.twitter.com",
            }:
                raise ResearchOrchestrationError("portable_x_seed_profile_host_invalid")
            if seed["source_profile_url"] is not None and not _x_profile_url_matches_handle(
                seed["source_profile_url"], seed["x_handle_proposals"][0]["handle"]
            ):
                raise ResearchOrchestrationError("portable_x_seed_profile_handle_invalid")
        elif seed["source_kind"] == "name_only":
            if seed["x_handle_proposals"] or seed["name_text"] is None or seed["source_profile_url"] is not None:
                raise ResearchOrchestrationError("portable_name_only_seed_invalid")
        else:
            if seed["external_record_ref"] is None or seed["name_text"] is None:
                raise ResearchOrchestrationError("portable_external_seed_identity_invalid")
            if any(row["binding_status"] != "cross_source_link_proposed" for row in seed["x_handle_proposals"]):
                raise ResearchOrchestrationError("portable_external_seed_handle_binding_invalid")
            if seed["source_kind"] == "linkedin_profile" and profile_host not in {
                None,
                "linkedin.com",
                "www.linkedin.com",
            }:
                raise ResearchOrchestrationError("portable_linkedin_seed_profile_host_invalid")

    experience = request["experience_verification"]
    allowed_experience = set(policy["experience_verification"]["dimension_ids"])
    if experience["enabled"]:
        if experience["trigger"] != "explicit_query_request" or not experience["dimension_ids"]:
            raise ResearchOrchestrationError("portable_experience_trigger_invalid")
        if not set(experience["dimension_ids"]).issubset(allowed_experience):
            raise ResearchOrchestrationError("portable_experience_dimension_invalid")
    elif experience["trigger"] != "not_requested" or experience["dimension_ids"]:
        raise ResearchOrchestrationError("portable_experience_disabled_shape_invalid")


def _descends_from(scope_id: str, root_id: str, nodes: Mapping[str, Mapping[str, Any]]) -> bool:
    if scope_id == root_id:
        return True
    frontier = list(nodes[scope_id]["containment_parent_scope_ids"])
    seen: set[str] = set()
    while frontier:
        current = frontier.pop()
        if current == root_id:
            return True
        if current in seen:
            continue
        seen.add(current)
        frontier.extend(nodes[current]["containment_parent_scope_ids"])
    return False


def _resolve_fresh_scopes(
    request: Mapping[str, Any],
    catalog: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    as_of = _timestamp(request["as_of"], "portable_campaign_as_of_invalid")
    generated_at = _timestamp(catalog["generated_at"], "scope_catalog_generated_at_invalid")
    if generated_at > as_of:
        raise ResearchOrchestrationError("scope_catalog_generated_after_request")
    nodes = {row["scope_id"]: row for row in catalog["scope_nodes"]}
    selection = request["scope_selection"]
    roots = selection["root_scope_ids"]
    required_kinds = set(selection["required_fresh_scope_kinds"])

    if selection["selection_mode"] == "complete_under_roots":
        selected_ids = {
            scope_id
            for scope_id, row in nodes.items()
            if row["scope_kind"] in required_kinds
            and any(_descends_from(scope_id, root_id, nodes) for root_id in roots)
        }
        selected_ids.update(roots)
        for root_id in roots:
            for scope_kind in required_kinds:
                coverage = [
                    row
                    for row in catalog["coverage_assertions"]
                    if row["root_scope_id"] == root_id and scope_kind in row["scope_kinds"]
                ]
                if len(coverage) != 1:
                    raise ResearchOrchestrationError("scope_catalog_coverage_assertion_missing_or_ambiguous")
                row = coverage[0]
                if row["coverage_status"] != "complete":
                    raise ResearchOrchestrationError("scope_catalog_coverage_incomplete")
                if not (
                    _timestamp(row["last_verified_at"], "scope_catalog_coverage_verified_at_invalid")
                    <= as_of
                    < _timestamp(row["refresh_after"], "scope_catalog_coverage_refresh_after_invalid")
                ):
                    raise ResearchOrchestrationError("scope_catalog_coverage_stale")
                if not any(
                    nodes[scope_id]["scope_kind"] == scope_kind and _descends_from(scope_id, root_id, nodes)
                    for scope_id in selected_ids
                ):
                    raise ResearchOrchestrationError("scope_catalog_required_kind_empty")
    else:
        selected_ids = set(selection["selected_scope_ids"]) | set(roots)
        if any(not any(_descends_from(scope_id, root_id, nodes) for root_id in roots) for scope_id in selected_ids):
            raise ResearchOrchestrationError("scope_catalog_explicit_node_outside_roots")
        if any(
            not any(nodes[scope_id]["scope_kind"] == scope_kind for scope_id in selected_ids)
            for scope_kind in required_kinds
        ):
            raise ResearchOrchestrationError("scope_catalog_explicit_required_kind_empty")

    resolved: list[Mapping[str, Any]] = []
    for scope_id in sorted(selected_ids):
        row = nodes[scope_id]
        last_verified = _timestamp(row["last_verified_at"], "scope_catalog_node_verified_at_invalid")
        refresh_after = _timestamp(row["refresh_after"], "scope_catalog_node_refresh_after_invalid")
        if not last_verified <= as_of < refresh_after or row["status"] == "unknown":
            raise ResearchOrchestrationError("scope_catalog_selected_node_stale_or_unknown")
        if row["source_status"] == "model_mediated_unverified":
            raise ResearchOrchestrationError("scope_catalog_selected_node_source_not_authoritative")
        resolved.append(row)
    return resolved


def _resolved_questions(
    request: Mapping[str, Any],
    catalog: Mapping[str, Any],
) -> list[dict[str, Any]]:
    taxonomies = _taxonomy_index(catalog)
    resolved: list[dict[str, Any]] = []
    for question in request["analysis_questions"]:
        labels = [taxonomies[question["dimension_id"]][label_id] for label_id in question["target_label_ids"]]
        if question["analysis_mode"] == "exploratory" and not labels:
            instruction = (
                "Infer plausible labels in the configured dimension from self-authored public Post and Reply evidence; "
                "return evidence spans and do not force a label."
            )
        else:
            instruction = (
                "Classify evidence against the configured target labels using target_core, target_adjacent, ambiguous, "
                "or out_of_scope; keep temporal state separate."
            )
        resolved.append(
            {
                "question_id": question["question_id"],
                "analysis_mode": question["analysis_mode"],
                "question_text": question["question_text"],
                "dimension_id": question["dimension_id"],
                "target_labels": [dict(label) for label in labels],
                "target_match_operator": question["target_match_operator"],
                "recall_execution_policy": question["recall_execution_policy"],
                "adaptive_stop_policy": (
                    None if question["adaptive_stop_policy"] is None else dict(question["adaptive_stop_policy"])
                ),
                "classification_instruction": instruction,
            }
        )
    return resolved


def build_campaign_plan(
    *,
    request: Mapping[str, Any],
    catalog: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    """Resolve one portable request into an offline, candidate-safe task plan."""

    validate_campaign_request(request, catalog=catalog, policy=policy)
    resolved_scope_rows = _resolve_fresh_scopes(request, catalog)
    scope_ids = [row["scope_id"] for row in resolved_scope_rows]
    questions = _resolved_questions(request, catalog)

    override_by_id = {row["channel_id"]: row["enabled"] for row in request["channel_overrides"]}
    resolved_channels: list[dict[str, Any]] = []
    for channel in policy["channel_registry"]:
        channel_id = channel["channel_id"]
        overridden = channel_id in override_by_id
        resolved_channels.append(
            {
                "channel_id": channel_id,
                "enabled": override_by_id.get(channel_id, channel["default_enabled"]),
                "resolution_source": "request_override" if overridden else "policy_default",
                "authority": channel["authority"],
                "supported_surfaces": list(channel["supported_surfaces"]),
            }
        )
    enabled_channels = {row["channel_id"] for row in resolved_channels if row["enabled"]}
    if not enabled_channels:
        raise ResearchOrchestrationError("portable_campaign_no_enabled_channel")
    if "conversation_graph" in enabled_channels and not enabled_channels.intersection(
        {"project_direct_credit", "official_source"}
    ):
        raise ResearchOrchestrationError("portable_campaign_conversation_graph_anchor_missing")

    handle_groups: dict[str, dict[str, Any]] = {}
    handle_resolution_queue: list[dict[str, Any]] = []
    for seed in request["seed_inputs"]:
        if not seed["x_handle_proposals"]:
            if seed["name_text"] is None:
                raise ResearchOrchestrationError("portable_seed_has_no_handle_or_name")
            handle_resolution_queue.append(
                {
                    "seed_ref": seed["seed_ref"],
                    "source_kind": seed["source_kind"],
                    "external_record_ref": seed["external_record_ref"],
                    "name_text": seed["name_text"],
                    "source_record_sha256": seed["source_record_sha256"],
                    "source_status": seed["source_status"],
                    "source_profile_url": seed["source_profile_url"],
                    "professional_facts": list(seed["professional_facts"]),
                    "reason": "x_handle_resolution_required",
                    "candidate_scoring_allowed": False,
                }
            )
            continue
        for proposal in seed["x_handle_proposals"]:
            key = proposal["handle"].casefold()
            group = handle_groups.setdefault(
                key,
                {
                    "handle": proposal["handle"],
                    "seed_refs": [],
                    "handle_binding_statuses": [],
                    "handle_proposals": [],
                },
            )
            if seed["seed_ref"] not in group["seed_refs"]:
                group["seed_refs"].append(seed["seed_ref"])
            if proposal["binding_status"] not in group["handle_binding_statuses"]:
                group["handle_binding_statuses"].append(proposal["binding_status"])
            proposal_binding = {
                "seed_ref": seed["seed_ref"],
                "binding_status": proposal["binding_status"],
                "evidence_ref": proposal["evidence_ref"],
                "source_status": seed["source_status"],
            }
            if proposal_binding not in group["handle_proposals"]:
                group["handle_proposals"].append(proposal_binding)

    candidate_authored_tasks: list[dict[str, Any]] = []
    if "candidate_authored_surface" in enabled_channels:
        for group in sorted(handle_groups.values(), key=lambda row: row["handle"].casefold()):
            group["seed_refs"].sort()
            group["handle_binding_statuses"].sort()
            group["handle_proposals"].sort(
                key=lambda row: (
                    row["seed_ref"],
                    row["binding_status"],
                    row["evidence_ref"],
                    row["source_status"],
                )
            )
            question_ids = [question["question_id"] for question in questions]
            recall_targets: list[dict[str, Any]] = []
            for question in questions:
                alias_rows = sorted(
                    ((alias, label["label_id"]) for label in question["target_labels"] for alias in label["aliases"]),
                    key=lambda row: (row[0].casefold(), row[1]),
                )
                recall_queries = (
                    []
                    if question["recall_execution_policy"] == "authored_surface_only"
                    else [
                        {
                            "label_id": label_id,
                            "surface": surface,
                            "query_text": (
                                f"from:{group['handle']} {_recall_query_term(alias)} "
                                f"{'filter:replies' if surface == 'reply' else '-filter:replies'}"
                            ),
                        }
                        for alias, label_id in alias_rows
                        for surface in ("post", "reply")
                    ]
                )
                recall_targets.append(
                    {
                        "question_id": question["question_id"],
                        "execution_policy": question["recall_execution_policy"],
                        "adaptive_stop_policy": (
                            None if question["adaptive_stop_policy"] is None else dict(question["adaptive_stop_policy"])
                        ),
                        "recall_aliases": [alias for alias, _label_id in alias_rows],
                        "recall_queries": recall_queries,
                    }
                )
            task_basis = {
                "handle": group["handle"].casefold(),
                "question_ids": question_ids,
                "scope_ids": scope_ids,
            }
            candidate_authored_tasks.append(
                {
                    "task_id": f"cas_{canonical_sha256(task_basis)[:24]}",
                    "seed_refs": list(group["seed_refs"]),
                    "handle": group["handle"],
                    "handle_binding_statuses": list(group["handle_binding_statuses"]),
                    "handle_proposals": list(group["handle_proposals"]),
                    "question_ids": question_ids,
                    "surfaces": ["post", "reply"],
                    "scope_ids": scope_ids,
                    "coverage_queries": [
                        {
                            "surface": "post",
                            "native_tool": "x_keyword_search",
                            "query_text": f"from:{group['handle']} -filter:replies",
                            "coverage_authority": "mechanical_single_handle",
                        },
                        {
                            "surface": "reply",
                            "native_tool": "x_keyword_search",
                            "query_text": f"from:{group['handle']} filter:replies",
                            "coverage_authority": "mechanical_single_handle",
                        },
                    ],
                    "recall_targets": recall_targets,
                    "candidate_scoring_allowed": False,
                }
            )
    if (
        enabled_channels == {"candidate_authored_surface"}
        and not candidate_authored_tasks
        and not handle_resolution_queue
    ):
        raise ResearchOrchestrationError("portable_campaign_candidate_channel_has_no_seed")

    optional_channel_tasks: list[dict[str, Any]] = []
    question_ids = [question["question_id"] for question in questions]
    for channel_id in policy["channel_order"]:
        if channel_id == "candidate_authored_surface" or channel_id not in enabled_channels:
            continue
        optional_channel_tasks.append(
            {
                "task_id": f"opt_{canonical_sha256({'channel_id': channel_id, 'scope_ids': scope_ids})[:24]}",
                "channel_id": channel_id,
                "question_ids": question_ids,
                "scope_ids": scope_ids,
                "dependency": (
                    "requires_direct_credit_or_official_anchor" if channel_id == "conversation_graph" else "none"
                ),
                "candidate_scoring_allowed": False,
            }
        )

    experience = request["experience_verification"]
    experience_plan = {
        "enabled": experience["enabled"],
        "trigger": experience["trigger"],
        "dimension_ids": list(experience["dimension_ids"]),
        "queue_activation": "after_base_population" if experience["enabled"] else "disabled",
        "base_population_rewrite_allowed": False,
        "identity_inference_allowed": False,
    }
    resolved_scope_nodes = [
        {
            "scope_id": row["scope_id"],
            "display_name": row["display_name"],
            "scope_kind": row["scope_kind"],
            "status": row["status"],
            "aliases": list(row["aliases"]),
            "source_status": row["source_status"],
        }
        for row in resolved_scope_rows
    ]
    payload: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "campaign_id": request["campaign_id"],
        "request_sha256": request["request_sha256"],
        "catalog_sha256": catalog["catalog_sha256"],
        "policy_sha256": canonical_sha256(policy),
        "freshness_status": "ready",
        "resolved_scope_nodes": resolved_scope_nodes,
        "resolved_channels": resolved_channels,
        "analysis_contract": {
            "questions": questions,
            "generic_relevance_states": list(policy["generic_relevance_states"]),
            "active_followup_states": list(policy["active_followup_states"]),
            "default_precision_states": list(policy["default_precision_states"]),
            "quality_metric_ids": list(policy["quality_metric_ids"]),
        },
        "temporal_scope": {
            "affiliation_states": list(request["temporal_scope"]["affiliation_states"]),
            "target_activity_states": list(request["temporal_scope"]["target_activity_states"]),
            "axes_are_independent": True,
        },
        "candidate_authored_tasks": candidate_authored_tasks,
        "handle_resolution_queue": sorted(handle_resolution_queue, key=lambda row: row["seed_ref"]),
        "optional_channel_tasks": optional_channel_tasks,
        "experience_verification": experience_plan,
        "integration_boundary": {
            "artifact_transport": "versioned_json_only",
            "runtime_import_allowed": False,
            "canonical_person_write_allowed": False,
            "x_account_is_canonical_person": False,
            "cross_source_links_are_reversible_proposals": True,
        },
        "counts": {
            "resolved_scope_node_count": len(resolved_scope_nodes),
            "enabled_channel_count": len(enabled_channels),
            "candidate_authored_task_count": len(candidate_authored_tasks),
            "handle_resolution_count": len(handle_resolution_queue),
            "optional_channel_task_count": len(optional_channel_tasks),
        },
        "plan_sha256": "",
    }
    payload["plan_sha256"] = _content_sha256(payload, "plan_sha256")
    assert_schema_valid(payload, PLAN_SCHEMA_FILE)
    return payload


def validate_campaign_result(
    result: Any,
    *,
    request: Mapping[str, Any],
    plan: Mapping[str, Any],
    catalog: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    """Validate one portable result without promoting it to canonical person state."""

    validate_campaign_request(request, catalog=catalog, policy=policy)
    expected_plan = build_campaign_plan(request=request, catalog=catalog, policy=policy)
    if plan != expected_plan:
        raise ResearchOrchestrationError("portable_campaign_result_plan_not_canonical")
    if not isinstance(result, Mapping):
        raise ResearchOrchestrationError("portable_campaign_result_not_object")
    assert_schema_valid(result, RESULT_SCHEMA_FILE)
    if result.get("schema_version") != RESULT_SCHEMA_VERSION:
        raise ResearchOrchestrationError("portable_campaign_result_version_invalid")
    if result["result_sha256"] != _content_sha256(result, "result_sha256"):
        raise ResearchOrchestrationError("portable_campaign_result_hash_mismatch")
    if (
        result["campaign_id"] != request["campaign_id"]
        or result["request_sha256"] != request["request_sha256"]
        or result["plan_sha256"] != plan["plan_sha256"]
        or result["catalog_sha256"] != catalog["catalog_sha256"]
    ):
        raise ResearchOrchestrationError("portable_campaign_result_binding_invalid")
    request_as_of = _timestamp(request["as_of"], "portable_campaign_as_of_invalid")
    execution_started_at = _timestamp(
        result["execution_window"]["started_at"], "portable_result_execution_window_invalid"
    )
    execution_completed_at = _timestamp(
        result["execution_window"]["completed_at"], "portable_result_execution_window_invalid"
    )
    if execution_started_at > execution_completed_at:
        raise ResearchOrchestrationError("portable_result_execution_window_invalid")

    seeds = {row["seed_ref"]: row for row in request["seed_inputs"]}
    outcomes = result["subject_outcomes"]
    outcome_refs = [row["seed_ref"] for row in outcomes]
    if len(outcome_refs) != len(set(outcome_refs)) or set(outcome_refs) != set(seeds):
        raise ResearchOrchestrationError("portable_campaign_subject_denominator_invalid")

    accounts: dict[str, Mapping[str, Any]] = {}
    handle_keys: set[str] = set()
    platform_user_ids: set[str] = set()
    for account in result["external_accounts"]:
        account_ref = _identifier(account["x_account_ref"], "portable_result_account_ref_invalid")
        handle_key = account["current_handle"].casefold()
        if account_ref in accounts or handle_key in handle_keys:
            raise ResearchOrchestrationError("portable_result_account_identity_duplicate")
        accounts[account_ref] = account
        handle_keys.add(handle_key)
        host = _profile_url_host(account["profile_url"])
        if host not in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}:
            raise ResearchOrchestrationError("portable_result_account_profile_host_invalid")
        if not _x_profile_url_matches_handle(account["profile_url"], account["current_handle"]):
            raise ResearchOrchestrationError("portable_result_account_profile_handle_invalid")
        if (account["identity_status"] == "stable_platform_id" and account["platform_user_id"] is None) or (
            account["identity_status"] == "provisional_handle" and account["platform_user_id"] is not None
        ):
            raise ResearchOrchestrationError("portable_result_account_identity_status_invalid")
        platform_user_id = account["platform_user_id"]
        if platform_user_id is not None:
            if platform_user_id in platform_user_ids:
                raise ResearchOrchestrationError("portable_result_platform_user_id_duplicate")
            platform_user_ids.add(platform_user_id)
        history_handles: set[str] = set()
        for history in account["handle_history_proposals"]:
            history_handle = history["handle"].casefold()
            if history_handle in history_handles:
                raise ResearchOrchestrationError("portable_result_handle_history_duplicate")
            history_handles.add(history_handle)
            if _timestamp(history["first_observed_at"], "portable_result_handle_history_time_invalid") > _timestamp(
                history["last_observed_at"], "portable_result_handle_history_time_invalid"
            ):
                raise ResearchOrchestrationError("portable_result_handle_history_time_invalid")

    receipt_statuses: dict[str, str] = {}
    attempt_source_statuses: set[str] = set()

    def register_receipt(receipt_ref: str, source_status: str) -> None:
        prior_status = receipt_statuses.get(receipt_ref)
        if prior_status is not None and prior_status != source_status:
            raise ResearchOrchestrationError("portable_result_receipt_source_status_conflict")
        if prior_status is not None:
            raise ResearchOrchestrationError("portable_result_receipt_reused_across_attempts")
        receipt_statuses[receipt_ref] = source_status

    handle_resolution_evidence_by_id: dict[str, Mapping[str, Any]] = {}
    for evidence in result["handle_resolution_evidence"]:
        evidence_id = evidence["evidence_id"]
        seed = seeds.get(evidence["seed_ref"])
        account = accounts.get(evidence["x_account_ref"])
        if (
            evidence_id in handle_resolution_evidence_by_id
            or seed is None
            or seed["source_kind"] == "x_account"
            or account is None
            or _profile_url_host(evidence["canonical_url"])
            not in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}
            or not _x_profile_url_matches_handle(evidence["canonical_url"], account["current_handle"])
        ):
            raise ResearchOrchestrationError("portable_result_handle_resolution_evidence_binding_invalid")
        receipt = evidence["retrieval_receipt"]
        if evidence["query_sha256"] != text_sha256(evidence["query_text"]) or evidence["content_sha256"] != text_sha256(
            evidence["observed_value"]
        ):
            raise ResearchOrchestrationError("portable_result_handle_resolution_evidence_hash_invalid")
        if (
            receipt["receipt_sha256"] != _content_sha256(receipt, "receipt_sha256")
            or evidence["receipt_ref"] != f"sha256:{receipt['receipt_sha256']}"
            or receipt["query_sha256"] != evidence["query_sha256"]
            or receipt["canonical_url"] != evidence["canonical_url"]
            or receipt["observed_at"] != evidence["observed_at"]
            or receipt["content_sha256"] != evidence["content_sha256"]
            or receipt["source_status"] != evidence["source_status"]
            or (evidence["source_status"] == "source_bound" and receipt["receipt_locator"] is None)
        ):
            raise ResearchOrchestrationError("portable_result_handle_resolution_receipt_invalid")
        observed_at = _timestamp(evidence["observed_at"], "portable_result_handle_resolution_evidence_time_invalid")
        if not execution_started_at <= observed_at <= execution_completed_at:
            raise ResearchOrchestrationError("portable_result_handle_resolution_evidence_time_invalid")
        handle_resolution_evidence_by_id[evidence_id] = evidence

    referenced_accounts: set[str] = set()
    for outcome in outcomes:
        if any(account_ref not in accounts for account_ref in outcome["x_account_refs"]):
            raise ResearchOrchestrationError("portable_result_subject_account_unknown")
        active_subject_states = {"analyzed", "research_in_progress"}
        if outcome["terminal_state"] in active_subject_states and not outcome["x_account_refs"]:
            raise ResearchOrchestrationError("portable_result_active_subject_without_account")
        if outcome["terminal_state"] not in active_subject_states and outcome["x_account_refs"]:
            raise ResearchOrchestrationError("portable_result_nonanalyzed_subject_has_account")
        seed = seeds[outcome["seed_ref"]]
        if seed["source_kind"] == "x_account":
            if outcome["terminal_state"] not in active_subject_states:
                raise ResearchOrchestrationError("portable_result_x_seed_terminal_state_invalid")
            expected_handle = seed["x_handle_proposals"][0]["handle"].casefold()
            if any(
                accounts[account_ref]["current_handle"].casefold() != expected_handle
                for account_ref in outcome["x_account_refs"]
            ):
                raise ResearchOrchestrationError("portable_result_x_seed_account_mismatch")
        referenced_accounts.update(outcome["x_account_refs"])

    resolution_attempts_by_seed: dict[str, list[Mapping[str, Any]]] = {}
    resolution_attempt_ids: set[str] = set()
    for attempt in result["handle_resolution_attempts"]:
        seed = seeds.get(attempt["seed_ref"])
        if (
            seed is None
            or seed["source_kind"] == "x_account"
            or attempt["attempt_id"] in resolution_attempt_ids
        ):
            raise ResearchOrchestrationError("portable_result_handle_resolution_attempt_invalid")
        if attempt["query_sha256"] != text_sha256(attempt["query_text"]):
            raise ResearchOrchestrationError("portable_result_handle_resolution_attempt_hash_invalid")
        observed_at = _timestamp(
            attempt["observed_at"], "portable_result_handle_resolution_attempt_time_invalid"
        )
        if not execution_started_at <= observed_at <= execution_completed_at:
            raise ResearchOrchestrationError("portable_result_handle_resolution_attempt_time_invalid")
        if attempt["execution_state"] == "no_match":
            frontier_ok = attempt["error"] is None and (
                (
                    attempt["continuation_state"] == "exhausted"
                    and not attempt["result_truncated"]
                    and attempt["continuation_ref"] is None
                )
                or (
                    attempt["continuation_state"] == "continuation_available"
                    and attempt["result_truncated"]
                    and attempt["continuation_ref"] is not None
                )
            )
        else:
            frontier_ok = (
                attempt["execution_state"] == "failed"
                and attempt["error"] is not None
                and not attempt["result_truncated"]
                and attempt["continuation_state"] == "unknown"
                and attempt["continuation_ref"] is None
            )
        if not frontier_ok:
            raise ResearchOrchestrationError("portable_result_handle_resolution_attempt_frontier_invalid")
        receipt = attempt["retrieval_receipt"]
        if (
            receipt["receipt_sha256"] != _content_sha256(receipt, "receipt_sha256")
            or attempt["receipt_ref"] != f"sha256:{receipt['receipt_sha256']}"
            or receipt["query_sha256"] != attempt["query_sha256"]
            or receipt["observed_at"] != attempt["observed_at"]
            or receipt["execution_state"] != attempt["execution_state"]
            or receipt["result_truncated"] != attempt["result_truncated"]
            or receipt["continuation_state"] != attempt["continuation_state"]
            or receipt["input_continuation_ref"] != attempt["input_continuation_ref"]
            or receipt["continuation_ref"] != attempt["continuation_ref"]
            or receipt["source_status"] != attempt["source_status"]
            or receipt["error"] != attempt["error"]
            or (attempt["source_status"] == "receipt_bound" and receipt["receipt_locator"] is None)
        ):
            raise ResearchOrchestrationError("portable_result_handle_resolution_attempt_receipt_invalid")
        register_receipt(attempt["receipt_ref"], attempt["source_status"])
        resolution_attempt_ids.add(attempt["attempt_id"])
        resolution_attempts_by_seed.setdefault(attempt["seed_ref"], []).append(attempt)

    resolution_chain_exhausted: dict[str, bool] = {}
    for seed_ref, attempts in resolution_attempts_by_seed.items():
        resolution_chain_exhausted[seed_ref] = _validate_pagination_chain(
            attempts,
            error="portable_result_handle_resolution_attempt_pagination_invalid",
        )

    resolution_outcomes_by_seed: dict[str, Mapping[str, Any]] = {}
    for outcome in result["handle_resolution_outcomes"]:
        seed_ref = outcome["seed_ref"]
        seed = seeds.get(seed_ref)
        attempts = resolution_attempts_by_seed.get(seed_ref, [])
        if (
            seed is None
            or seed["source_kind"] == "x_account"
            or seed_ref in resolution_outcomes_by_seed
            or not attempts
        ):
            raise ResearchOrchestrationError("portable_result_handle_resolution_outcome_invalid")
        ordered = sorted(attempts, key=lambda row: row["ordinal"])
        if outcome["attempt_ids"] != [row["attempt_id"] for row in ordered] or outcome["receipt_refs"] != [
            row["receipt_ref"] for row in ordered
        ]:
            raise ResearchOrchestrationError("portable_result_handle_resolution_outcome_binding_invalid")
        if outcome["source_status"] != _attempt_status_summary({row["source_status"] for row in ordered}):
            raise ResearchOrchestrationError("portable_result_handle_resolution_outcome_source_status_invalid")
        expected_error_codes = sorted(
            {row["error"]["error_code"] for row in ordered if row["error"] is not None}
        )
        if outcome["error_codes"] != expected_error_codes:
            raise ResearchOrchestrationError("portable_result_handle_resolution_outcome_error_binding_invalid")
        if outcome["terminal_state"] == "no_verified_account":
            if outcome["reason_code"] != "search_exhausted_no_match" or not resolution_chain_exhausted[seed_ref]:
                raise ResearchOrchestrationError("portable_result_handle_resolution_outcome_state_invalid")
        elif (
            outcome["reason_code"] != "execution_failed"
            or resolution_chain_exhausted[seed_ref]
            or not any(row["execution_state"] == "failed" for row in ordered)
        ):
            raise ResearchOrchestrationError("portable_result_handle_resolution_outcome_state_invalid")
        resolution_outcomes_by_seed[seed_ref] = outcome

    if set(resolution_attempts_by_seed) != set(resolution_outcomes_by_seed):
        raise ResearchOrchestrationError("portable_result_handle_resolution_outcome_coverage_invalid")
    expected_resolution_outcomes: set[str] = set()
    for outcome in outcomes:
        seed_ref = outcome["seed_ref"]
        resolution = resolution_outcomes_by_seed.get(seed_ref)
        if outcome["terminal_state"] in {"no_verified_account", "failed"}:
            if seeds[seed_ref]["source_kind"] != "x_account":
                if resolution is None:
                    raise ResearchOrchestrationError(
                        "portable_result_handle_resolution_outcome_coverage_invalid"
                    )
                expected_resolution_outcomes.add(seed_ref)
        elif resolution is not None:
            raise ResearchOrchestrationError("portable_result_handle_resolution_outcome_coverage_invalid")
        if resolution is not None and resolution["terminal_state"] != outcome["terminal_state"]:
            raise ResearchOrchestrationError("portable_result_handle_resolution_outcome_state_mismatch")
    if set(resolution_outcomes_by_seed) != expected_resolution_outcomes:
        raise ResearchOrchestrationError("portable_result_handle_resolution_outcome_coverage_invalid")

    optional_tasks = {row["task_id"]: row for row in plan["optional_channel_tasks"]}
    discovery_origin_pairs: set[tuple[str, str]] = set()
    discovery_accounts: set[str] = set()
    for origin in result["discovery_origins"]:
        task = optional_tasks.get(origin["origin_task_id"])
        pair = (origin["x_account_ref"], origin["origin_task_id"])
        if (
            origin["x_account_ref"] not in accounts
            or task is None
            or task["channel_id"] != origin["channel_id"]
            or pair in discovery_origin_pairs
        ):
            raise ResearchOrchestrationError("portable_result_discovery_origin_invalid")
        discovery_origin_pairs.add(pair)
        discovery_accounts.add(origin["x_account_ref"])
    proposal_ids: set[str] = set()
    proposal_pairs: set[tuple[str, str]] = set()
    proposed_pairs: set[tuple[str, str]] = set()
    proposal_accounts: set[str] = set()
    used_resolution_evidence: set[str] = set()
    for proposal in result["cross_source_link_proposals"]:
        if proposal["proposal_id"] in proposal_ids:
            raise ResearchOrchestrationError("portable_result_link_proposal_duplicate")
        proposal_ids.add(proposal["proposal_id"])
        seed = seeds.get(proposal["seed_ref"])
        account = accounts.get(proposal["x_account_ref"])
        if seed is None or account is None or seed["source_kind"] == "x_account":
            raise ResearchOrchestrationError("portable_result_link_proposal_binding_invalid")
        pair = (proposal["seed_ref"], proposal["x_account_ref"])
        if pair in proposal_pairs:
            raise ResearchOrchestrationError("portable_result_link_proposal_pair_duplicate")
        proposal_pairs.add(pair)
        proposal_accounts.add(proposal["x_account_ref"])
        matching_hint = not seed["x_handle_proposals"] or any(
            hint["handle"].casefold() == account["current_handle"].casefold()
            and hint["binding_status"] == "cross_source_link_proposed"
            for hint in seed["x_handle_proposals"]
        )
        if not matching_hint:
            raise ResearchOrchestrationError("portable_result_link_proposal_not_seeded")
        allowed_evidence_refs = {
            value for value in (seed["external_record_ref"], seed["source_profile_url"]) if value is not None
        }
        allowed_evidence_refs.update(row["evidence_ref"] for row in seed["x_handle_proposals"])
        allowed_evidence_refs.update(row["evidence_ref"] for row in seed["professional_facts"])
        matching_resolution_evidence = {
            evidence_id
            for evidence_id, evidence in handle_resolution_evidence_by_id.items()
            if evidence["seed_ref"] == proposal["seed_ref"] and evidence["x_account_ref"] == proposal["x_account_ref"]
        }
        allowed_evidence_refs.update(matching_resolution_evidence)
        if not set(proposal["evidence_refs"]).issubset(allowed_evidence_refs):
            raise ResearchOrchestrationError("portable_result_link_proposal_evidence_invalid")
        if not set(proposal["evidence_refs"]).intersection(matching_resolution_evidence):
            raise ResearchOrchestrationError("portable_result_link_proposal_resolution_evidence_missing")
        evidence_statuses = {
            seed["source_status"]
            for evidence_ref in proposal["evidence_refs"]
            if evidence_ref not in handle_resolution_evidence_by_id
        }
        evidence_statuses.update(
            handle_resolution_evidence_by_id[evidence_ref]["source_status"]
            for evidence_ref in proposal["evidence_refs"]
            if evidence_ref in handle_resolution_evidence_by_id
        )
        if not _source_status_is_compatible(proposal["source_status"], evidence_statuses):
            raise ResearchOrchestrationError("portable_result_link_proposal_source_status_invalid")
        used_resolution_evidence.update(set(proposal["evidence_refs"]).intersection(matching_resolution_evidence))
        if proposal["status"] == "proposed":
            proposed_pairs.add(pair)
    expected_proposed_pairs = {
        (outcome["seed_ref"], account_ref)
        for outcome in outcomes
        if outcome["terminal_state"] in {"analyzed", "research_in_progress"}
        and seeds[outcome["seed_ref"]]["source_kind"] != "x_account"
        for account_ref in outcome["x_account_refs"]
    }
    if proposed_pairs != expected_proposed_pairs:
        raise ResearchOrchestrationError("portable_result_link_proposal_coverage_invalid")
    if used_resolution_evidence != set(handle_resolution_evidence_by_id):
        raise ResearchOrchestrationError("portable_result_handle_resolution_evidence_unreferenced")
    if referenced_accounts | discovery_accounts | proposal_accounts != set(accounts):
        raise ResearchOrchestrationError("portable_result_account_origin_binding_incomplete")

    planned_coverage_queries = {
        (task["handle"].casefold(), query["surface"]): query["query_text"]
        for task in plan["candidate_authored_tasks"]
        for query in task["coverage_queries"]
    }
    surface_attempt_keys: set[tuple[str, str, int]] = set()
    terminal_surface_attempt_keys: set[tuple[str, str]] = set()
    surface_attempt_groups: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    surface_attempt_receipts: dict[tuple[str, str], list[str]] = {}
    surface_attempt_by_receipt: dict[str, Mapping[str, Any]] = {}
    attempt_receipts: dict[tuple[str, str], dict[str, str]] = {}

    for attempt in result["surface_attempts"]:
        account = accounts.get(attempt["x_account_ref"])
        group_key = (attempt["x_account_ref"], attempt["surface"])
        key = (*group_key, attempt["ordinal"])
        if account is None or key in surface_attempt_keys:
            raise ResearchOrchestrationError("portable_result_surface_attempt_duplicate_or_unknown")
        _validate_execution_attempt_shape(attempt, count_field="bound_observation_count")
        classified = classify_single_handle_query_surface(attempt["query_text"])
        expected_surface = "authored_post" if attempt["surface"] == "post" else "authored_reply"
        if classified != (account["current_handle"].casefold(), expected_surface):
            raise ResearchOrchestrationError("portable_result_surface_attempt_query_invalid")
        if (
            planned_coverage_queries.get((account["current_handle"].casefold(), attempt["surface"]))
            != attempt["query_text"]
        ):
            raise ResearchOrchestrationError("portable_result_surface_attempt_not_planned")
        if attempt["query_sha256"] != text_sha256(attempt["query_text"]):
            raise ResearchOrchestrationError("portable_result_surface_attempt_hash_invalid")
        surface_attempt_keys.add(key)
        surface_attempt_groups.setdefault(group_key, []).append(attempt)
        register_receipt(attempt["receipt_ref"], attempt["source_status"])
        surface_attempt_by_receipt[attempt["receipt_ref"]] = attempt
        attempt_source_statuses.add(attempt["source_status"])
        if attempt["execution_state"] != "failed":
            attempt_receipts.setdefault(group_key, {})[attempt["receipt_ref"]] = attempt["source_status"]

    for group_key, attempts in surface_attempt_groups.items():
        exhausted = _validate_pagination_chain(
            attempts,
            error="portable_result_surface_attempt_pagination_invalid",
        )
        ordered = sorted(attempts, key=lambda row: row["ordinal"])
        surface_attempt_receipts[group_key] = [
            row["receipt_ref"] for row in ordered if row["execution_state"] != "failed"
        ]
        if exhausted:
            terminal_surface_attempt_keys.add(group_key)

    planned_recall_queries = {
        (
            task["handle"].casefold(),
            target["question_id"],
            query["label_id"],
            query["surface"],
            query["query_text"],
        )
        for task in plan["candidate_authored_tasks"]
        for target in task["recall_targets"]
        for query in target["recall_queries"]
    }
    recall_attempt_keys: set[tuple[str, str, int]] = set()
    semantic_attempt_groups: dict[tuple[str, str, str, str, str], list[Mapping[str, Any]]] = {}
    exhausted_semantic_query_keys: set[tuple[str, str, str, str, str]] = set()
    semantic_attempt_by_receipt: dict[str, Mapping[str, Any]] = {}
    for attempt in result["semantic_recall_attempts"]:
        account = accounts.get(attempt["x_account_ref"])
        _validate_execution_attempt_shape(attempt, count_field="bound_observation_count")
        query_key = (
            attempt["x_account_ref"],
            attempt["question_id"],
            attempt["label_id"],
            attempt["surface"],
            attempt["query_text"],
        )
        key = (attempt["x_account_ref"], attempt["question_id"], attempt["ordinal"])
        planned_key = (
            account["current_handle"].casefold() if account is not None else "",
            attempt["question_id"],
            attempt["label_id"],
            attempt["surface"],
            attempt["query_text"],
        )
        if account is None or key in recall_attempt_keys or planned_key not in planned_recall_queries:
            raise ResearchOrchestrationError("portable_result_semantic_recall_attempt_invalid")
        if attempt["query_sha256"] != text_sha256(attempt["query_text"]):
            raise ResearchOrchestrationError("portable_result_semantic_recall_attempt_hash_invalid")
        recall_attempt_keys.add(key)
        semantic_attempt_groups.setdefault(query_key, []).append(attempt)
        register_receipt(attempt["receipt_ref"], attempt["source_status"])
        semantic_attempt_by_receipt[attempt["receipt_ref"]] = attempt
        attempt_source_statuses.add(attempt["source_status"])
        if attempt["execution_state"] != "failed":
            attempt_receipts.setdefault((attempt["x_account_ref"], attempt["surface"]), {})[attempt["receipt_ref"]] = (
                attempt["source_status"]
            )

    for query_key, attempts in semantic_attempt_groups.items():
        if _validate_pagination_chain(
            attempts,
            error="portable_result_semantic_recall_pagination_invalid",
            ordinal_field="page_ordinal",
        ):
            exhausted_semantic_query_keys.add(query_key)

    observation_ids: set[str] = set()
    platform_object_ids: set[str] = set()
    canonical_observation_urls: set[str] = set()
    observation_by_id: dict[str, Mapping[str, Any]] = {}
    for observation in result["observations"]:
        observation_id = observation["observation_id"]
        account = accounts.get(observation["x_account_ref"])
        if observation_id in observation_ids or account is None:
            raise ResearchOrchestrationError("portable_result_observation_binding_invalid")
        if (
            observation["platform_object_id"] in platform_object_ids
            or observation["canonical_url"] in canonical_observation_urls
        ):
            raise ResearchOrchestrationError("portable_result_observation_platform_object_duplicate")
        observation_ids.add(observation_id)
        platform_object_ids.add(observation["platform_object_id"])
        canonical_observation_urls.add(observation["canonical_url"])
        observation_by_id[observation_id] = observation
        if observation["author_handle"].casefold() != account["current_handle"].casefold():
            raise ResearchOrchestrationError("portable_result_observation_author_invalid")
        if observation["author_platform_user_id"] != account["platform_user_id"]:
            raise ResearchOrchestrationError("portable_result_observation_platform_id_invalid")
        if _profile_url_host(observation["canonical_url"]) not in {
            "x.com",
            "www.x.com",
            "twitter.com",
            "www.twitter.com",
        }:
            raise ResearchOrchestrationError("portable_result_observation_url_host_invalid")
        if not _x_status_url_matches(
            observation["canonical_url"], observation["author_handle"], observation["platform_object_id"]
        ):
            raise ResearchOrchestrationError("portable_result_observation_url_topology_invalid")
        if observation["content_sha256"] != text_sha256(observation["text_or_excerpt"]):
            raise ResearchOrchestrationError("portable_result_observation_content_hash_invalid")
        matching_attempt_status = attempt_receipts.get((observation["x_account_ref"], observation["surface"]), {}).get(
            observation["receipt_ref"]
        )
        if matching_attempt_status is None:
            raise ResearchOrchestrationError("portable_result_observation_attempt_binding_invalid")
        if observation["source_status"] != _observation_status_for_attempt(matching_attempt_status):
            raise ResearchOrchestrationError("portable_result_observation_source_status_invalid")
        published_at = _timestamp(observation["published_at"], "portable_result_published_at_invalid")
        observed_at = _timestamp(observation["observed_at"], "portable_result_observed_at_invalid")
        if (
            published_at > request_as_of
            or published_at > observed_at
            or not execution_started_at <= observed_at <= execution_completed_at
        ):
            raise ResearchOrchestrationError("portable_result_observation_time_order_invalid")

    observation_ids_by_receipt: dict[str, set[str]] = {}
    for observation in result["observations"]:
        observation_ids_by_receipt.setdefault(observation["receipt_ref"], set()).add(observation["observation_id"])
    for receipt_ref, attempt in surface_attempt_by_receipt.items():
        if attempt["bound_observation_count"] != len(observation_ids_by_receipt.get(receipt_ref, set())):
            raise ResearchOrchestrationError("portable_result_surface_attempt_bound_count_invalid")
    for receipt_ref, attempt in semantic_attempt_by_receipt.items():
        bound_observation_ids = observation_ids_by_receipt.get(receipt_ref, set())
        if (
            attempt["bound_observation_count"] != len(bound_observation_ids)
            or set(attempt["new_unique_observation_refs"]) != bound_observation_ids
        ):
            raise ResearchOrchestrationError("portable_result_semantic_attempt_bound_count_invalid")

    for account_ref, account in accounts.items():
        for history in account["handle_history_proposals"]:
            if any(
                evidence_ref not in observation_by_id or observation_by_id[evidence_ref]["x_account_ref"] != account_ref
                for evidence_ref in history["evidence_refs"]
            ):
                raise ResearchOrchestrationError("portable_result_handle_history_evidence_invalid")

    scope_ids = {row["scope_id"] for row in plan["resolved_scope_nodes"]}
    question_by_id = {row["question_id"]: row for row in plan["analysis_contract"]["questions"]}
    result_keys: set[tuple[str, str]] = set()
    dimension_result_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    for dimension_result in result["dimension_results"]:
        account_ref = dimension_result["x_account_ref"]
        question = question_by_id.get(dimension_result["question_id"])
        key = (account_ref, dimension_result["question_id"])
        if account_ref not in accounts or question is None or key in result_keys:
            raise ResearchOrchestrationError("portable_result_dimension_binding_invalid")
        result_keys.add(key)
        dimension_result_by_key[key] = dimension_result
        if dimension_result["dimension_id"] != question["dimension_id"]:
            raise ResearchOrchestrationError("portable_result_dimension_id_invalid")
        target_temporal_state = dimension_result["target_activity_temporal_state"]
        if dimension_result["matches_temporal_filter"] != (
            target_temporal_state in request["temporal_scope"]["target_activity_states"]
        ):
            raise ResearchOrchestrationError("portable_result_target_activity_filter_flag_invalid")
        target_label_ids = {row["label_id"] for row in question["target_labels"]}
        matched_label_ids = set(dimension_result["matched_label_ids"])
        if not matched_label_ids.issubset(target_label_ids):
            raise ResearchOrchestrationError("portable_result_dimension_matched_label_invalid")
        if question["analysis_mode"] == "exploratory" and (
            matched_label_ids or dimension_result["relevance_state"] != "ambiguous"
        ):
            raise ResearchOrchestrationError("portable_result_exploratory_dimension_state_invalid")
        if dimension_result["relevance_state"] == "out_of_scope" and matched_label_ids:
            raise ResearchOrchestrationError("portable_result_out_of_scope_matched_label_invalid")
        if dimension_result["relevance_state"] == "target_core":
            if question["target_match_operator"] == "all" and matched_label_ids != target_label_ids:
                raise ResearchOrchestrationError("portable_result_dimension_all_target_incomplete")
            if question["target_match_operator"] == "any" and not matched_label_ids:
                raise ResearchOrchestrationError("portable_result_dimension_any_target_empty")
        if (
            dimension_result["relevance_state"] in {"target_core", "target_adjacent"}
            and not dimension_result["evidence_refs"]
        ):
            raise ResearchOrchestrationError("portable_result_positive_dimension_without_evidence")
        if (
            dimension_result["target_activity_temporal_state"] in {"current", "historical"}
            and not dimension_result["evidence_refs"]
        ):
            raise ResearchOrchestrationError("portable_result_temporal_dimension_without_evidence")
        if any(evidence_ref not in observation_ids for evidence_ref in dimension_result["evidence_refs"]):
            raise ResearchOrchestrationError("portable_result_dimension_evidence_invalid")
        if any(
            observation_by_id[evidence_ref]["x_account_ref"] != account_ref
            for evidence_ref in dimension_result["evidence_refs"]
        ):
            raise ResearchOrchestrationError("portable_result_dimension_evidence_owner_invalid")
        evidence_statuses = {
            observation_by_id[evidence_ref]["source_status"] for evidence_ref in dimension_result["evidence_refs"]
        }
        if not _source_status_is_compatible(dimension_result["source_status"], evidence_statuses):
            raise ResearchOrchestrationError("portable_result_dimension_source_upgrade_invalid")

    analyzed_accounts = {
        account_ref
        for outcome in outcomes
        if outcome["terminal_state"] == "analyzed"
        for account_ref in outcome["x_account_refs"]
    }
    in_progress_accounts = {
        account_ref
        for outcome in outcomes
        if outcome["terminal_state"] == "research_in_progress"
        for account_ref in outcome["x_account_refs"]
    }
    expected_result_keys = {
        (account_ref, question_id) for account_ref in analyzed_accounts for question_id in question_by_id
    }
    if result_keys != expected_result_keys:
        raise ResearchOrchestrationError("portable_result_dimension_terminal_total_invalid")

    planned_handle_keys = {task["handle"].casefold() for task in plan["candidate_authored_tasks"]}
    if any(
        accounts[account_ref]["current_handle"].casefold() not in planned_handle_keys
        for account_ref in analyzed_accounts
    ):
        raise ResearchOrchestrationError("portable_result_analyzed_account_not_planned")
    if any(
        (account_ref, surface) not in terminal_surface_attempt_keys
        for account_ref in analyzed_accounts
        for surface in ("post", "reply")
    ):
        raise ResearchOrchestrationError("portable_result_analyzed_account_surface_incomplete")
    if any(accounts[account_ref]["identity_status"] == "quarantined_handle_reuse" for account_ref in analyzed_accounts):
        raise ResearchOrchestrationError("portable_result_quarantined_account_analyzed")

    for account_ref in in_progress_accounts:
        live_surface_frontier = False
        for surface in ("post", "reply"):
            surface_chain = surface_attempt_groups.get((account_ref, surface))
            if surface_chain:
                tip = max(surface_chain, key=lambda row: row["ordinal"])
                live_surface_frontier = live_surface_frontier or (
                    tip["execution_state"] == "failed" or tip["continuation_state"] == "continuation_available"
                )
        live_semantic_frontier = False
        for query_key, semantic_chain in semantic_attempt_groups.items():
            if query_key[0] != account_ref:
                continue
            tip = max(semantic_chain, key=lambda row: row["page_ordinal"])
            live_semantic_frontier = live_semantic_frontier or (
                tip["execution_state"] == "failed" or tip["continuation_state"] == "continuation_available"
            )
        if not (live_surface_frontier or live_semantic_frontier):
            raise ResearchOrchestrationError("portable_result_research_in_progress_frontier_invalid")

    recall_attempts_by_subject: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for attempt in result["semantic_recall_attempts"]:
        recall_attempts_by_subject.setdefault((attempt["x_account_ref"], attempt["question_id"]), []).append(attempt)
    for key, unordered_attempts in recall_attempts_by_subject.items():
        attempts = sorted(unordered_attempts, key=lambda row: row["ordinal"])
        if [row["ordinal"] for row in attempts] != list(range(1, len(attempts) + 1)):
            raise ResearchOrchestrationError("portable_result_semantic_recall_ordinal_invalid")
        seen_new_observations: set[str] = set()
        dimension_result = dimension_result_by_key.get(key)
        if dimension_result is None and key[0] not in in_progress_accounts:
            raise ResearchOrchestrationError("portable_result_semantic_recall_subject_not_analyzed")
        dimension_evidence = (
            set(dimension_result["evidence_refs"]) if dimension_result is not None else set(observation_ids)
        )
        for attempt in attempts:
            new_observations = set(attempt["new_unique_observation_refs"])
            target_evidence = set(attempt["new_target_evidence_refs"])
            if not target_evidence.issubset(new_observations) or not target_evidence.issubset(dimension_evidence):
                raise ResearchOrchestrationError("portable_result_semantic_recall_target_evidence_invalid")
            if seen_new_observations.intersection(new_observations):
                raise ResearchOrchestrationError("portable_result_semantic_recall_new_observation_reused")
            for evidence_ref in new_observations:
                observation = observation_by_id.get(evidence_ref)
                if (
                    observation is None
                    or observation["x_account_ref"] != attempt["x_account_ref"]
                    or observation["surface"] != attempt["surface"]
                    or observation["receipt_ref"] != attempt["receipt_ref"]
                ):
                    raise ResearchOrchestrationError("portable_result_semantic_recall_observation_binding_invalid")
            seen_new_observations.update(new_observations)
    recall_outcome_keys: set[tuple[str, str]] = set()
    recall_complete_keys: set[tuple[str, str]] = set()
    for outcome in result["semantic_recall_outcomes"]:
        key = (outcome["x_account_ref"], outcome["question_id"])
        question = question_by_id.get(outcome["question_id"])
        if outcome["x_account_ref"] not in analyzed_accounts or question is None or key in recall_outcome_keys:
            raise ResearchOrchestrationError("portable_result_semantic_recall_outcome_invalid")
        recall_outcome_keys.add(key)
        if outcome["execution_policy"] != question["recall_execution_policy"]:
            raise ResearchOrchestrationError("portable_result_semantic_recall_policy_invalid")
        attempts = sorted(recall_attempts_by_subject.get(key, []), key=lambda row: row["ordinal"])
        if outcome["attempted_query_sha256s"] != [row["query_sha256"] for row in attempts]:
            raise ResearchOrchestrationError("portable_result_semantic_recall_outcome_binding_invalid")
        execution_policy = outcome["execution_policy"]
        terminal_state = outcome["terminal_state"]
        stop_reason = outcome["stop_reason"]
        outcome_query_keys = {
            (
                row["x_account_ref"],
                row["question_id"],
                row["label_id"],
                row["surface"],
                row["query_text"],
            )
            for row in attempts
        }
        attempted_queries_exhausted = outcome_query_keys.issubset(exhausted_semantic_query_keys)
        if execution_policy == "authored_surface_only":
            surface_receipts = {
                receipt_ref
                for surface in ("post", "reply")
                for receipt_ref in surface_attempt_receipts.get((outcome["x_account_ref"], surface), [])
            }
            if (
                attempts
                or terminal_state != "authored_surface_complete"
                or stop_reason != "broad_authored_surface_completed"
                or set(outcome["receipt_refs"]) != surface_receipts
                or outcome["adaptive_stop_decision"] is not None
            ):
                raise ResearchOrchestrationError("portable_result_authored_surface_outcome_invalid")
            recall_complete_keys.add(key)
            continue
        if outcome["receipt_refs"] != [row["receipt_ref"] for row in attempts]:
            raise ResearchOrchestrationError("portable_result_semantic_recall_outcome_binding_invalid")
        if terminal_state == "adaptive_complete":
            adaptive_policy = question["adaptive_stop_policy"]
            decision = outcome["adaptive_stop_decision"]
            if adaptive_policy is None or decision is None:
                raise ResearchOrchestrationError("portable_result_adaptive_stop_decision_missing")
            window_size = adaptive_policy["window_size"]
            if len(attempts) < window_size:
                raise ResearchOrchestrationError("portable_result_adaptive_recall_window_incomplete")
            window = attempts[-window_size:]
            minimum_per_surface = adaptive_policy["minimum_attempts_per_surface"]
            if any(
                sum(row["surface"] == surface for row in window) < minimum_per_surface for surface in ("post", "reply")
            ):
                raise ResearchOrchestrationError("portable_result_adaptive_recall_surface_window_incomplete")
            planned_query_hashes = {
                text_sha256(query_text)
                for handle_key, question_id, _label_id, _surface, query_text in planned_recall_queries
                if question_id == outcome["question_id"]
                and accounts[outcome["x_account_ref"]]["current_handle"].casefold() == handle_key
            }
            attempted_query_hashes = {row["query_sha256"] for row in attempts}
            target_label_ids = {row["label_id"] for row in question["target_labels"]}
            required_label_surface_pairs = {
                (label_id, surface) for label_id in target_label_ids for surface in ("post", "reply")
            }
            attempted_label_surface_pairs = {(row["label_id"], row["surface"]) for row in attempts}
            if not required_label_surface_pairs.issubset(attempted_label_surface_pairs):
                raise ResearchOrchestrationError("portable_result_adaptive_target_label_coverage_invalid")
            window_new_observation_count = sum(len(row["new_unique_observation_refs"]) for row in window)
            window_new_target_count = sum(len(row["new_target_evidence_refs"]) for row in window)
            if (
                decision["decision_sha256"] != _content_sha256(decision, "decision_sha256")
                or decision["window_size"] != window_size
                or decision["window_query_sha256s"] != [row["query_sha256"] for row in window]
                or decision["observed_new_unique_observation_count"] != window_new_observation_count
                or decision["observed_new_target_evidence_count"] != window_new_target_count
                or decision["configured_maximum_new_target_evidence"]
                != adaptive_policy["maximum_new_target_evidence_in_window"]
                or window_new_target_count > adaptive_policy["maximum_new_target_evidence_in_window"]
                or decision["remaining_query_sha256s"] != sorted(planned_query_hashes - attempted_query_hashes)
            ):
                raise ResearchOrchestrationError("portable_result_adaptive_stop_decision_invalid")
            window_statuses = {row["source_status"] for row in window}
            if decision["source_status"] != _attempt_status_summary(window_statuses):
                raise ResearchOrchestrationError("portable_result_adaptive_stop_source_upgrade_invalid")
            audit = decision["scope_frontier_audit"]
            expected_pairs = [
                {"label_id": label_id, "surface": surface} for label_id, surface in sorted(required_label_surface_pairs)
            ]
            expected_attempted_hashes = sorted(attempted_query_hashes)
            if (
                audit["audit_sha256"] != _content_sha256(audit, "audit_sha256")
                or decision["scope_frontier_audit_ref"] != f"sha256:{audit['audit_sha256']}"
                or audit["x_account_ref"] != outcome["x_account_ref"]
                or audit["question_id"] != outcome["question_id"]
                or audit["covered_label_surface_pairs"] != expected_pairs
                or audit["attempted_query_sha256s"] != expected_attempted_hashes
                or audit["remaining_query_sha256s"] != decision["remaining_query_sha256s"]
                or audit["source_status"] != decision["source_status"]
            ):
                raise ResearchOrchestrationError("portable_result_scope_frontier_audit_invalid")
            if (
                execution_policy != "adaptive_marginal_gain"
                or stop_reason != "marginal_gain_sustained_low"
                or not attempted_queries_exhausted
            ):
                raise ResearchOrchestrationError("portable_result_adaptive_recall_outcome_invalid")
            recall_complete_keys.add(key)
        elif terminal_state == "target_match_proven":
            dimension_result = dimension_result_by_key[key]
            matched_label_ids = set(dimension_result["matched_label_ids"])
            target_label_ids = {row["label_id"] for row in question["target_labels"]}
            proven_label_ids = {row["label_id"] for row in attempts if row["new_target_evidence_refs"]}
            proven = dimension_result["relevance_state"] == "target_core"
            if question["target_match_operator"] == "all":
                proven = (
                    proven
                    and target_label_ids.issubset(matched_label_ids)
                    and target_label_ids.issubset(proven_label_ids)
                )
            else:
                proven = proven and bool(target_label_ids.intersection(matched_label_ids, proven_label_ids))
            if (
                execution_policy != "adaptive_marginal_gain"
                or stop_reason != "configured_target_match_proven"
                or outcome["adaptive_stop_decision"] is not None
                or not attempts
                or not attempted_queries_exhausted
                or not proven
            ):
                raise ResearchOrchestrationError("portable_result_target_match_proven_invalid")
            recall_complete_keys.add(key)
        elif terminal_state == "exhaustive_complete":
            expected_attempts = {
                (outcome["x_account_ref"], question_id, label_id, surface, query_text)
                for handle_key, question_id, label_id, surface, query_text in planned_recall_queries
                if question_id == outcome["question_id"]
                and accounts[outcome["x_account_ref"]]["current_handle"].casefold() == handle_key
            }
            if (
                execution_policy != "exhaustive_alias_matrix"
                or stop_reason != "alias_matrix_exhausted"
                or outcome["adaptive_stop_decision"] is not None
                or outcome_query_keys != expected_attempts
                or not attempted_queries_exhausted
            ):
                raise ResearchOrchestrationError("portable_result_exhaustive_recall_outcome_invalid")
            recall_complete_keys.add(key)
        elif terminal_state == "not_run":
            if (
                attempts
                or stop_reason != "not_run"
                or outcome["receipt_refs"]
                or outcome["adaptive_stop_decision"] is not None
            ):
                raise ResearchOrchestrationError("portable_result_semantic_recall_not_run_invalid")
        elif terminal_state == "failed":
            if (
                stop_reason != "execution_failed"
                or outcome["adaptive_stop_decision"] is not None
                or not any(row["execution_state"] == "failed" for row in attempts)
            ):
                raise ResearchOrchestrationError("portable_result_semantic_recall_failed_invalid")
        else:
            raise ResearchOrchestrationError("portable_result_semantic_recall_terminal_state_invalid")
    if recall_outcome_keys != expected_result_keys:
        raise ResearchOrchestrationError("portable_result_semantic_recall_terminal_total_invalid")

    affiliation_keys: set[tuple[str, str]] = set()
    evidence_universe = observation_ids | set(seeds)
    evidence_source_status = {
        **{seed_ref: seed["source_status"] for seed_ref, seed in seeds.items()},
        **{observation_id: observation["source_status"] for observation_id, observation in observation_by_id.items()},
    }
    subject_seed_refs_by_account: dict[str, set[str]] = {account_ref: set() for account_ref in accounts}
    for outcome in outcomes:
        if outcome["terminal_state"] != "analyzed":
            continue
        for account_ref in outcome["x_account_refs"]:
            subject_seed_refs_by_account[account_ref].add(outcome["seed_ref"])

    def evidence_belongs_to_account(account_ref: str, evidence_ref: str) -> bool:
        observation = observation_by_id.get(evidence_ref)
        if observation is not None:
            return observation["x_account_ref"] == account_ref
        return evidence_ref in subject_seed_refs_by_account.get(account_ref, set())

    for affiliation in result["affiliation_results"]:
        key = (affiliation["x_account_ref"], affiliation["scope_id"])
        if (
            affiliation["x_account_ref"] not in accounts
            or affiliation["scope_id"] not in scope_ids
            or key in affiliation_keys
            or any(evidence_ref not in evidence_universe for evidence_ref in affiliation["evidence_refs"])
            or any(
                not evidence_belongs_to_account(affiliation["x_account_ref"], evidence_ref)
                for evidence_ref in affiliation["evidence_refs"]
            )
        ):
            raise ResearchOrchestrationError("portable_result_affiliation_binding_invalid")
        if affiliation["matches_temporal_filter"] != (
            affiliation["temporal_state"] in request["temporal_scope"]["affiliation_states"]
        ):
            raise ResearchOrchestrationError("portable_result_affiliation_filter_flag_invalid")
        if not _source_status_is_compatible(
            affiliation["source_status"],
            {evidence_source_status[evidence_ref] for evidence_ref in affiliation["evidence_refs"]},
        ):
            raise ResearchOrchestrationError("portable_result_affiliation_source_upgrade_invalid")
        if affiliation["temporal_state"] in {"current", "historical"} and not affiliation["evidence_refs"]:
            raise ResearchOrchestrationError("portable_result_affiliation_without_evidence")
        affiliation_keys.add(key)
    expected_affiliation_keys = {
        (account_ref, root_scope_id)
        for account_ref in analyzed_accounts
        for root_scope_id in request["scope_selection"]["root_scope_ids"]
    }
    if affiliation_keys != expected_affiliation_keys:
        raise ResearchOrchestrationError("portable_result_affiliation_terminal_total_invalid")

    finding_ids: set[str] = set()
    for finding in result["exploratory_findings"]:
        question = question_by_id.get(finding["question_id"])
        if (
            finding["finding_id"] in finding_ids
            or finding["x_account_ref"] not in analyzed_accounts
            or question is None
            or question["analysis_mode"] not in {"exploratory", "hybrid"}
            or any(evidence_ref not in observation_ids for evidence_ref in finding["evidence_refs"])
            or any(
                observation_by_id[evidence_ref]["x_account_ref"] != finding["x_account_ref"]
                for evidence_ref in finding["evidence_refs"]
                if evidence_ref in observation_by_id
            )
        ):
            raise ResearchOrchestrationError("portable_result_exploratory_finding_invalid")
        if not _source_status_is_compatible(
            finding["source_status"],
            {observation_by_id[evidence_ref]["source_status"] for evidence_ref in finding["evidence_refs"]},
        ):
            raise ResearchOrchestrationError("portable_result_exploratory_source_upgrade_invalid")
        finding_ids.add(finding["finding_id"])

    if not plan["experience_verification"]["enabled"] and result["experience_verification_queue"]:
        raise ResearchOrchestrationError("portable_result_experience_queue_not_requested")
    experience_accounts: set[str] = set()
    for row in result["experience_verification_queue"]:
        if (
            row["x_account_ref"] not in accounts
            or row["x_account_ref"] in experience_accounts
            or row["dimension_id"] not in plan["experience_verification"]["dimension_ids"]
            or any(evidence_ref not in evidence_universe for evidence_ref in row["evidence_refs"])
            or any(
                not evidence_belongs_to_account(row["x_account_ref"], evidence_ref)
                for evidence_ref in row["evidence_refs"]
            )
        ):
            raise ResearchOrchestrationError("portable_result_experience_queue_binding_invalid")
        if not _source_status_is_compatible(
            row["source_status"],
            {evidence_source_status[evidence_ref] for evidence_ref in row["evidence_refs"]},
        ):
            raise ResearchOrchestrationError("portable_result_experience_queue_evidence_invalid")
        if row["status"] in {"strong_proxy", "weak_proxy"} and not row["evidence_refs"]:
            raise ResearchOrchestrationError("portable_result_experience_positive_without_evidence")
        experience_accounts.add(row["x_account_ref"])
    if plan["experience_verification"]["enabled"] and experience_accounts != analyzed_accounts:
        raise ResearchOrchestrationError("portable_result_experience_queue_terminal_total_invalid")

    optional_evidence_by_id: dict[str, Mapping[str, Any]] = {}
    for evidence in result["optional_channel_evidence"]:
        evidence_id = evidence["evidence_id"]
        if (
            evidence_id in optional_evidence_by_id
            or evidence["task_id"] not in optional_tasks
            or _profile_url_host(evidence["canonical_url"]) is None
            or evidence["content_sha256"] != text_sha256(evidence["text_or_excerpt"])
        ):
            raise ResearchOrchestrationError("portable_result_optional_evidence_invalid")
        observed_at = _timestamp(evidence["observed_at"], "portable_result_optional_evidence_time_invalid")
        if not execution_started_at <= observed_at <= execution_completed_at:
            raise ResearchOrchestrationError("portable_result_optional_evidence_time_invalid")
        optional_evidence_by_id[evidence_id] = evidence

    optional_attempt_ids: set[str] = set()
    optional_attempts_by_task: dict[str, list[Mapping[str, Any]]] = {}
    for attempt in result["optional_channel_attempts"]:
        if attempt["attempt_id"] in optional_attempt_ids or attempt["task_id"] not in optional_tasks:
            raise ResearchOrchestrationError("portable_result_optional_channel_attempt_invalid")
        _validate_execution_attempt_shape(attempt, count_field="bound_evidence_count")
        bound_evidence = [optional_evidence_by_id.get(ref) for ref in attempt["evidence_refs"]]
        if (
            attempt["bound_evidence_count"] != len(attempt["evidence_refs"])
            or any(evidence is None for evidence in bound_evidence)
            or any(evidence["task_id"] != attempt["task_id"] for evidence in bound_evidence)
            or any(evidence["receipt_ref"] != attempt["receipt_ref"] for evidence in bound_evidence)
            or any(
                evidence["source_status"] != _observation_status_for_attempt(attempt["source_status"])
                for evidence in bound_evidence
            )
        ):
            raise ResearchOrchestrationError("portable_result_optional_channel_attempt_bound_count_invalid")
        optional_attempt_ids.add(attempt["attempt_id"])
        register_receipt(attempt["receipt_ref"], attempt["source_status"])
        optional_attempts_by_task.setdefault(attempt["task_id"], []).append(attempt)
        attempt_source_statuses.add(attempt["source_status"])
    for task_attempts in optional_attempts_by_task.values():
        task_attempts.sort(key=lambda row: row["ordinal"])
        _validate_pagination_chain(
            task_attempts,
            error="portable_result_optional_channel_attempt_pagination_invalid",
        )
        evidence_refs = [evidence_ref for row in task_attempts for evidence_ref in row["evidence_refs"]]
        if len(evidence_refs) != len(set(evidence_refs)):
            raise ResearchOrchestrationError("portable_result_optional_channel_evidence_reused")

    optional_outcome_ids: set[str] = set()
    optional_outcome_by_id: dict[str, Mapping[str, Any]] = {}
    for outcome in result["optional_channel_outcomes"]:
        task = optional_tasks.get(outcome["task_id"])
        task_attempts = optional_attempts_by_task.get(outcome["task_id"], [])
        if task is None or outcome["task_id"] in optional_outcome_ids or outcome["channel_id"] != task["channel_id"]:
            raise ResearchOrchestrationError("portable_result_optional_channel_outcome_invalid")
        if outcome["attempt_ids"] != [row["attempt_id"] for row in task_attempts]:
            raise ResearchOrchestrationError("portable_result_optional_channel_outcome_attempt_binding_invalid")
        expected_evidence_refs = {evidence_ref for row in task_attempts for evidence_ref in row["evidence_refs"]}
        if set(outcome["evidence_refs"]) != expected_evidence_refs:
            raise ResearchOrchestrationError("portable_result_optional_channel_outcome_evidence_binding_invalid")
        attempt_statuses = {row["source_status"] for row in task_attempts}
        if outcome["source_status"] != _attempt_status_summary(attempt_statuses):
            raise ResearchOrchestrationError("portable_result_optional_channel_source_upgrade_invalid")
        chain_exhausted = bool(task_attempts) and _validate_pagination_chain(
            task_attempts,
            error="portable_result_optional_channel_attempt_pagination_invalid",
        )
        terminal_state = outcome["terminal_state"]
        if terminal_state == "completed":
            if not outcome["evidence_refs"] or not chain_exhausted:
                raise ResearchOrchestrationError("portable_result_optional_completed_invalid")
        elif terminal_state == "no_result":
            if (
                outcome["evidence_refs"]
                or not chain_exhausted
                or any(row["execution_state"] != "no_result" for row in task_attempts)
            ):
                raise ResearchOrchestrationError("portable_result_optional_no_result_invalid")
        elif terminal_state == "failed":
            if outcome["evidence_refs"] or not any(row["execution_state"] == "failed" for row in task_attempts):
                raise ResearchOrchestrationError("portable_result_optional_failed_invalid")
        elif terminal_state == "research_in_progress":
            if not task_attempts or chain_exhausted:
                raise ResearchOrchestrationError("portable_result_optional_research_in_progress_invalid")
        elif terminal_state == "not_run":
            if task_attempts or outcome["evidence_refs"]:
                raise ResearchOrchestrationError("portable_result_optional_not_run_invalid")
        optional_outcome_ids.add(outcome["task_id"])
        optional_outcome_by_id[outcome["task_id"]] = outcome
    if optional_outcome_ids != set(optional_tasks):
        raise ResearchOrchestrationError("portable_result_optional_channel_terminal_total_invalid")
    anchor_completed = any(
        outcome["channel_id"] in {"project_direct_credit", "official_source"}
        and outcome["terminal_state"] == "completed"
        for outcome in result["optional_channel_outcomes"]
    )
    if any(
        outcome["channel_id"] == "conversation_graph"
        and outcome["terminal_state"] in {"completed", "no_result"}
        and not anchor_completed
        for outcome in result["optional_channel_outcomes"]
    ):
        raise ResearchOrchestrationError("portable_result_conversation_graph_runtime_anchor_missing")

    relationship_ids: set[str] = set()
    relationship_keys: set[tuple[str, str, str, str]] = set()
    for relationship in result["relationship_results"]:
        task = optional_tasks.get(relationship["origin_task_id"])
        outcome = optional_outcome_by_id.get(relationship["origin_task_id"])
        key = (
            relationship["x_account_ref"],
            relationship["scope_id"],
            relationship["relation_kind"],
            relationship["origin_task_id"],
        )
        if (
            relationship["relationship_id"] in relationship_ids
            or key in relationship_keys
            or relationship["x_account_ref"] not in accounts
            or relationship["scope_id"] not in scope_ids
            or task is None
            or outcome is None
            or relationship["channel_id"] != task["channel_id"]
            or outcome["terminal_state"] != "completed"
            or not set(relationship["evidence_refs"]).issubset(outcome["evidence_refs"])
        ):
            raise ResearchOrchestrationError("portable_result_relationship_binding_invalid")
        if relationship["matches_temporal_filter"] != (
            relationship["temporal_state"] in request["temporal_scope"]["affiliation_states"]
        ):
            raise ResearchOrchestrationError("portable_result_relationship_filter_flag_invalid")
        if any(evidence_ref not in optional_evidence_by_id for evidence_ref in relationship["evidence_refs"]):
            raise ResearchOrchestrationError("portable_result_relationship_source_upgrade_invalid")
        relationship_evidence_statuses = {
            optional_evidence_by_id[evidence_ref]["source_status"] for evidence_ref in relationship["evidence_refs"]
        }
        if not _source_status_is_compatible(relationship["source_status"], relationship_evidence_statuses):
            raise ResearchOrchestrationError("portable_result_relationship_source_upgrade_invalid")
        relationship_ids.add(relationship["relationship_id"])
        relationship_keys.add(key)
    for origin in result["discovery_origins"]:
        outcome = optional_outcome_by_id[origin["origin_task_id"]]
        if outcome["terminal_state"] != "completed" or not set(origin["evidence_refs"]).issubset(
            outcome["evidence_refs"]
        ):
            raise ResearchOrchestrationError("portable_result_discovery_origin_outcome_binding_invalid")
        if any(evidence_ref not in optional_evidence_by_id for evidence_ref in origin["evidence_refs"]):
            raise ResearchOrchestrationError("portable_result_discovery_origin_source_upgrade_invalid")
        if not _source_status_is_compatible(
            origin["source_status"],
            {optional_evidence_by_id[evidence_ref]["source_status"] for evidence_ref in origin["evidence_refs"]},
        ):
            raise ResearchOrchestrationError("portable_result_discovery_origin_source_upgrade_invalid")
    optional_outcome_evidence = {
        evidence_ref for outcome in result["optional_channel_outcomes"] for evidence_ref in outcome["evidence_refs"]
    }
    if optional_outcome_evidence != set(optional_evidence_by_id):
        raise ResearchOrchestrationError("portable_result_optional_evidence_terminal_binding_invalid")

    coverage = result["coverage"]
    attempted_surfaces: dict[str, set[str]] = {account_ref: set() for account_ref in accounts}
    completed_surfaces: dict[str, set[str]] = {account_ref: set() for account_ref in accounts}
    for account_ref, surface in surface_attempt_groups:
        attempted_surfaces[account_ref].add(surface)
    for account_ref, surface in terminal_surface_attempt_keys:
        completed_surfaces[account_ref].add(surface)
    both_surface_attempted = sum(surfaces == {"post", "reply"} for surfaces in attempted_surfaces.values())
    both_surface_completed = sum(surfaces == {"post", "reply"} for surfaces in completed_surfaces.values())
    if (
        coverage["subjects_total"] != len(seeds)
        or coverage["terminal_subject_outcomes"]
        != sum(row["terminal_state"] != "research_in_progress" for row in outcomes)
        or coverage["resolved_account_count"] != len(accounts)
        or coverage["observation_count"] != len(observation_ids)
        or coverage["candidate_authored_post_attempted"]
        != sum("post" in surfaces for surfaces in attempted_surfaces.values())
        or coverage["candidate_authored_reply_attempted"]
        != sum("reply" in surfaces for surfaces in attempted_surfaces.values())
        or coverage["candidate_authored_both_surface_attempted"] != both_surface_attempted
    ):
        raise ResearchOrchestrationError("portable_result_coverage_counts_invalid")
    if coverage["coverage_source_status"] != _attempt_status_summary(attempt_source_statuses):
        raise ResearchOrchestrationError("portable_result_coverage_source_status_invalid")
    metric_ids = [row["metric_id"] for row in coverage["metric_values"]]
    if metric_ids != policy["quality_metric_ids"]:
        raise ResearchOrchestrationError("portable_result_quality_metric_registry_invalid")
    target_question_ids = {
        question_id
        for question_id, question in question_by_id.items()
        if question["analysis_mode"] in {"verification", "hybrid"}
    }
    target_results = [row for row in result["dimension_results"] if row["question_id"] in target_question_ids]
    temporal_results = list(result["affiliation_results"]) + target_results
    non_x_seeds = {seed_ref for seed_ref, seed in seeds.items() if seed["source_kind"] != "x_account"}
    resolved_non_x = {seed_ref for seed_ref, _account_ref in proposed_pairs if seed_ref in non_x_seeds}
    native_attempts = (
        list(result["surface_attempts"])
        + list(result["semantic_recall_attempts"])
        + list(result["optional_channel_attempts"])
    )
    marginal_metric = (
        (
            len({row["x_account_ref"] for row in target_results if row["relevance_state"] == "target_core"}),
            len(native_attempts),
        )
        if native_attempts and all(row["source_status"] == "receipt_bound" for row in native_attempts)
        else (0, 0)
    )
    expected_metrics = {
        "candidate_authored_both_surface_coverage_rate": (
            both_surface_completed,
            len(plan["candidate_authored_tasks"]),
        ),
        "source_bound_evidence_rate": (
            sum(row["source_status"] == "source_bound" for row in result["observations"]),
            len(result["observations"]),
        ),
        "target_direction_core_rate": (
            sum(row["relevance_state"] == "target_core" for row in target_results),
            len(target_results),
        ),
        "target_direction_active_rate": (
            sum(row["relevance_state"] in {"target_core", "target_adjacent"} for row in target_results),
            len(target_results),
        ),
        "evidence_backed_temporal_state_rate": (
            sum(
                row["temporal_state" if "temporal_state" in row else "target_activity_temporal_state"]
                in {"current", "historical"}
                and bool(row["evidence_refs"])
                for row in temporal_results
            ),
            len(temporal_results),
        ),
        "target_core_unique_account_yield_per_native_call": marginal_metric,
        "cross_source_handle_resolution_rate": (len(resolved_non_x), len(non_x_seeds)),
    }
    for metric in coverage["metric_values"]:
        expected = expected_metrics.get(metric["metric_id"])
        if expected is not None and (metric["numerator"], metric["denominator"]) != expected:
            raise ResearchOrchestrationError("portable_result_quality_metric_value_invalid")
    optional_success = all(
        outcome["terminal_state"] in {"completed", "no_result"} for outcome in result["optional_channel_outcomes"]
    )
    experience_complete = not plan["experience_verification"]["enabled"] or all(
        row["status"] != "pending" for row in result["experience_verification_queue"]
    )
    planned_handles_with_accounts = {
        accounts[account_ref]["current_handle"].casefold() for account_ref in analyzed_accounts
    }
    complete_ready = (
        all(outcome["terminal_state"] == "analyzed" for outcome in outcomes)
        and not discovery_accounts
        and planned_handles_with_accounts == planned_handle_keys
        and all(completed_surfaces[account_ref] == {"post", "reply"} for account_ref in analyzed_accounts)
        and optional_success
        and experience_complete
        and recall_complete_keys == expected_result_keys
    )
    failed_ready = (
        not analyzed_accounts
        and not discovery_accounts
        and all(outcome["terminal_state"] in {"no_verified_account", "failed"} for outcome in outcomes)
        and all(outcome["terminal_state"] in {"failed", "not_run"} for outcome in result["optional_channel_outcomes"])
    )
    expected_status = "complete" if complete_ready else "failed" if failed_ready else "partial"
    if result["status"] != expected_status:
        raise ResearchOrchestrationError("portable_result_status_derivation_invalid")


def validate_checked_in_assets(root: Path | None = None) -> list[str]:
    base = root or project_root()
    try:
        policy = load_policy(base / "configs" / "research_orchestration_policy.v1.json")
        catalog = strict_load_json(base / "fixtures" / "research_scope_catalog_fixture_v1.json")
        request = strict_load_json(base / "fixtures" / "portable_research_campaign_request_fixture_v1.json")
        expected_plan = strict_load_json(base / "fixtures" / "portable_research_campaign_plan_fixture_v1.json")
        result = strict_load_json(base / "fixtures" / "portable_research_campaign_result_fixture_v1.json")
        registry = strict_load_json(base / "contracts" / "research_orchestration_contract_registry.v1.json")
        validate_scope_catalog(catalog, policy=policy)
        validate_campaign_request(request, catalog=catalog, policy=policy)
        actual_plan = build_campaign_plan(request=request, catalog=catalog, policy=policy)
        if actual_plan != expected_plan:
            return ["portable_research_campaign_plan_fixture_drift"]
        validate_campaign_result(
            result,
            request=request,
            plan=expected_plan,
            catalog=catalog,
            policy=policy,
        )
        expected_contracts = [
            (POLICY_SCHEMA_VERSION, f"contracts/{POLICY_SCHEMA_FILE}"),
            (CATALOG_SCHEMA_VERSION, f"contracts/{CATALOG_SCHEMA_FILE}"),
            (REQUEST_SCHEMA_VERSION, f"contracts/{REQUEST_SCHEMA_FILE}"),
            (PLAN_SCHEMA_VERSION, f"contracts/{PLAN_SCHEMA_FILE}"),
            (RESULT_SCHEMA_VERSION, f"contracts/{RESULT_SCHEMA_FILE}"),
            (
                "sourcing.x_first.subject_selection.v1",
                "contracts/sourcing.x_first.subject_selection.v1.schema.json",
            ),
            (
                "x.portable.selected_subject.request_binding.v1",
                "contracts/x.portable.selected_subject.request_binding.v1.schema.json",
            ),
            (
                "x.portable.research_campaign.package_manifest.v1",
                "contracts/x.portable.research_campaign.package_manifest.v1.schema.json",
            ),
            (
                "x.portable.research_campaign.semantic_validation_receipt.v1",
                "contracts/x.portable.research_campaign.semantic_validation_receipt.v1.schema.json",
            ),
        ]
        if (
            registry.get("schema_version") != "x.research_orchestration.contract_registry.v1"
            or [(row.get("schema_version"), row.get("path")) for row in registry.get("contracts", [])]
            != expected_contracts
            or registry.get("checked_assets")
            != [
                "configs/research_orchestration_policy.v1.json",
                "fixtures/research_scope_catalog_fixture_v1.json",
                "fixtures/portable_research_campaign_request_fixture_v1.json",
                "fixtures/portable_research_campaign_plan_fixture_v1.json",
                "fixtures/portable_research_campaign_result_fixture_v1.json",
                "fixtures/selected_subject_fixture_simulate_package_v1.json",
            ]
            or any(
                registry.get(field) is not False
                for field in (
                    "provider_calls_allowed",
                    "product_writes_allowed",
                    "runtime_import_allowed",
                    "canonical_person_write_allowed",
                )
            )
            or any(not (base / path).is_file() for _, path in expected_contracts)
        ):
            return ["research_orchestration_contract_registry_invalid"]
        from x_first.portable_campaign_package import build_fixture_simulate_package

        package = strict_load_json(base / "fixtures" / "selected_subject_fixture_simulate_package_v1.json")
        package_artifacts = package.get("artifacts")
        package_receipt = package.get("semantic_validation_receipt")
        if not isinstance(package_artifacts, Mapping) or not isinstance(package_receipt, Mapping):
            return ["selected_subject_fixture_simulate_package_invalid"]
        rebuilt_package = build_fixture_simulate_package(
            selection=package_artifacts["selection"],
            policy=package_artifacts["policy"],
            catalog=package_artifacts["catalog"],
            request=package_artifacts["request"],
            binding=package_artifacts["binding"],
            plan=package_artifacts["plan"],
            result=package_artifacts["result"],
            validated_at=package_receipt["validated_at"],
        )
        if rebuilt_package != package:
            return ["selected_subject_fixture_simulate_package_drift"]
    except Exception as exc:  # noqa: BLE001 - stable local asset validator surface
        return [str(exc)]
    return []


def main() -> int:
    errors = validate_checked_in_assets()
    print(canonical_json({"errors": errors, "status": "valid" if not errors else "invalid"}))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
