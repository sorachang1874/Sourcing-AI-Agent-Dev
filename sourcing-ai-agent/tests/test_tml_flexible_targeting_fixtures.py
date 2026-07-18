from __future__ import annotations

import copy
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from sourcing_agent.cohort_provider_compiler import (
    COHORT_EXECUTION_NOT_READY,
    COHORT_PROVIDER,
    COHORT_PROVIDER_MANIFEST_VERSION,
    CohortProviderCompiler,
)
from sourcing_agent.cohort_selection import (
    COHORT_SELECTION_SCHEMA_VERSION,
    CohortSelectionValidationError,
    canonicalize_cohort_selection_request_payload,
    cohort_execution_identity_for_signature,
    cohort_selection_options_payload,
    validate_external_cohort_selection_payload,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONFIG_DIR = _REPO_ROOT / "configs"
_EXPLICIT_FIXTURES = {
    "Thinking Machines Lab": _CONFIG_DIR / "demo_workflow_thinking_machines_lab_cohort.json",
    "Anthropic": _CONFIG_DIR / "demo_workflow_anthropic_cohort.json",
}
_LEGACY_FIXTURES = {
    "Thinking Machines Lab": _CONFIG_DIR / "demo_workflow_thinking_machines_lab.json",
    "Anthropic": _CONFIG_DIR / "demo_workflow_anthropic.json",
}
_COHORT_FIELDS = {
    "schema_version",
    "role_bucket_ids",
    "employment_statuses",
    "role_match",
    "source",
}


def _load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _canonical_with_axis_mirrors(payload: dict[str, object]) -> dict[str, object]:
    canonical = canonicalize_cohort_selection_request_payload(payload)
    cohort = dict(canonical["cohort_selection"])
    canonical["intent_axes"] = {
        "population_boundary": {
            "employment_statuses": list(cohort["employment_statuses"]),
        },
        "thematic_constraints": {
            "must_have_primary_role_buckets": list(cohort["role_bucket_ids"]),
            "role_match": cohort["role_match"],
        },
    }
    return canonicalize_cohort_selection_request_payload(canonical)


def test_explicit_lab_fixtures_share_the_canonical_path_and_have_new_family_identity() -> None:
    options = cohort_selection_options_payload()
    selectable_roles = [str(item["id"]) for item in options["role_buckets"]]
    selectable_statuses = [str(item["id"]) for item in options["employment_statuses"]]
    compiler = CohortProviderCompiler()
    branch_families: set[tuple[str, str, str]] = set()
    selection_identities: set[str] = set()

    for company, path in _EXPLICIT_FIXTURES.items():
        raw = _load_json(path)
        assert raw["target_company"] == company
        raw_cohort = dict(raw["cohort_selection"])
        assert set(raw_cohort) == _COHORT_FIELDS

        canonical = _canonical_with_axis_mirrors(raw)
        cohort = dict(canonical["cohort_selection"])
        roles = list(cohort["role_bucket_ids"])
        statuses = list(cohort["employment_statuses"])

        assert cohort["schema_version"] == COHORT_SELECTION_SCHEMA_VERSION
        assert cohort["source"] == "user_explicit"
        assert roles == [role for role in selectable_roles if role in roles]
        assert statuses and statuses == [status for status in selectable_statuses if status in statuses]
        assert canonical["must_have_primary_role_buckets"] == roles
        assert canonical["employment_statuses"] == statuses
        assert canonical["intent_axes"] == {
            "population_boundary": {"employment_statuses": statuses},
            "thematic_constraints": {
                "must_have_primary_role_buckets": roles,
                "role_match": cohort["role_match"],
            },
        }

        selection_identity = cohort_execution_identity_for_signature(canonical)
        assert selection_identity
        assert cohort_execution_identity_for_signature(_load_json(_LEGACY_FIXTURES[company])) == ""
        selection_identities.add(selection_identity)

        manifest = compiler.compile(
            canonical,
            execution_capability=None,
            requested_result_limit=int(raw["top_k"]),
        )
        expected_lane_count = len(statuses) * max(1, len(roles))
        assert manifest["execution_ready"] is False
        assert manifest["execution_blocker"] == COHORT_EXECUTION_NOT_READY
        assert manifest["compiler_inputs"]["execution_capability"] == {}
        assert len(manifest["lanes"]) == expected_lane_count
        assert manifest["budget"]["planned_provider_calls"] == expected_lane_count
        assert [(lane["employment_status"], lane["role_bucket_id"]) for lane in manifest["lanes"]] == [
            (status, role) for status in statuses for role in roles
        ]
        branch_families.add(
            (
                str(manifest["schema_version"]),
                str(manifest["source"]),
                str(manifest["provider"]),
            )
        )

    assert branch_families == {
        (
            COHORT_PROVIDER_MANIFEST_VERSION,
            "cohort_provider_compiler",
            COHORT_PROVIDER,
        )
    }
    assert len(selection_identities) == len(_EXPLICIT_FIXTURES)


def test_tml_and_anthropic_selectors_are_materially_different() -> None:
    tml = dict(_load_json(_EXPLICIT_FIXTURES["Thinking Machines Lab"])["cohort_selection"])
    anthropic = dict(_load_json(_EXPLICIT_FIXTURES["Anthropic"])["cohort_selection"])

    assert tml["role_bucket_ids"] == ["research", "product_management"]
    assert tml["employment_statuses"] == ["current", "former"]
    assert tml["role_match"] == "any"
    assert anthropic["role_bucket_ids"] == ["engineering", "infra_systems"]
    assert anthropic["employment_statuses"] == ["current"]
    assert anthropic["role_match"] == "all"


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ({"role_bucket_ids": ["sales"]}, "cohort_selection_unknown_role_bucket"),
        ({"role_bucket_ids": ["research", "research"]}, "cohort_selection_duplicate_value"),
        ({"employment_statuses": ["contractor"]}, "cohort_selection_unknown_employment_status"),
        ({"employment_statuses": ["current", "current"]}, "cohort_selection_duplicate_value"),
        ({"employment_statuses": []}, "cohort_selection_empty_employment_statuses"),
        ({"unknown": True}, "cohort_selection_unknown_field"),
        ({"schema_version": "cohort_selection.v2"}, "cohort_selection_unsupported_schema_version"),
        ({"source": "inferred"}, "cohort_selection_invalid_source"),
    ],
)
def test_invalid_selector_mutations_fail_before_io(
    mutation: dict[str, object],
    expected_code: str,
) -> None:
    payload = _load_json(_EXPLICIT_FIXTURES["Thinking Machines Lab"])
    cohort = dict(payload["cohort_selection"])
    cohort.update(mutation)
    payload["cohort_selection"] = cohort

    with (
        patch("builtins.open", side_effect=AssertionError("unexpected file access")),
        patch("socket.create_connection", side_effect=AssertionError("unexpected network access")),
        pytest.raises(CohortSelectionValidationError) as captured,
    ):
        validate_external_cohort_selection_payload(payload)

    assert captured.value.code == expected_code


@pytest.mark.parametrize(
    "conflicting_mirror",
    [
        {"employment_statuses": ["former"]},
        {
            "intent_axes": {
                "population_boundary": {"employment_statuses": ["current", "former"]},
                "thematic_constraints": {
                    "must_have_primary_role_buckets": ["engineering"],
                    "role_match": "any",
                },
            }
        },
    ],
)
def test_conflicting_flat_or_axis_mirrors_fail_before_io(
    conflicting_mirror: dict[str, object],
) -> None:
    payload = copy.deepcopy(_load_json(_EXPLICIT_FIXTURES["Anthropic"]))
    payload.update(conflicting_mirror)

    with (
        patch("builtins.open", side_effect=AssertionError("unexpected file access")),
        patch("socket.create_connection", side_effect=AssertionError("unexpected network access")),
        pytest.raises(CohortSelectionValidationError) as captured,
    ):
        validate_external_cohort_selection_payload(payload)

    assert captured.value.code == "cohort_selection_mirror_conflict"
