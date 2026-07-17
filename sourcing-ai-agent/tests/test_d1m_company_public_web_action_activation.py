from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from sourcing_agent.action_target_binding import (
    AUTHORIZATION_MODE_AUTHENTICATED,
    AUTHORIZATION_MODE_OPEN_OPERATOR,
    COMPANY_PUBLIC_WEB_TARGET_INVALID,
    COMPANY_PUBLIC_WEB_TARGET_OWNER,
    ActionBindContext,
    ActionTargetBindingError,
    CompanyPublicWebTargetBinder,
    build_company_public_web_target_binder_registry,
)
from sourcing_agent.company_public_web_assets import build_company_public_web_seed_assets
from sourcing_agent.operation_runtime import (
    ACTION_REFRESH_COMPANY_PUBLIC_WEB,
    COMPANY_PUBLIC_WEB_ACTION_REQUEST_CONTRACTS,
    COMPANY_PUBLIC_WEB_ACTION_TYPES,
    DEFAULT_ACTION_REGISTRY,
    OPERATION_OWNER_BOUND_ACTION_TYPES,
    ActionRequestValidationError,
    OperationRuntimeStateConflict,
    OperationRuntimeWriter,
)


def _valid_input(**overrides: Any) -> dict[str, Any]:
    return {
        "target_company": "OpenAI",
        "source_families": ["company_research", "company_homepage"],
        "seed_urls": ["https://openai.com/research", "https://openai.com/"],
        **overrides,
    }


def _valid_target() -> dict[str, str]:
    return {"workspace_id": "user-alice", "company_key": "openai"}


def test_company_public_web_refresh_joins_schema_defined_unserved_inventory() -> None:
    records = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    schema_defined = {
        action_type for action_type in records if DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema
    }
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)

    assert COMPANY_PUBLIC_WEB_ACTION_TYPES == (ACTION_REFRESH_COMPANY_PUBLIC_WEB,)
    assert set(COMPANY_PUBLIC_WEB_ACTION_REQUEST_CONTRACTS) == {ACTION_REFRESH_COMPANY_PUBLIC_WEB}
    assert schema_defined == set(OPERATION_OWNER_BOUND_ACTION_TYPES)
    assert len(schema_defined) == 10
    assert sum(not DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema for action_type in records) == 5
    assert spec.request_schema_version == "company_public_web_refresh_request_v1"
    assert len(spec.request_schema_digest) == 64
    assert spec.request_identity_target_fields == ("workspace_id", "company_key")
    assert spec.owner_reserved_request_fields == {
        "workspace_id",
        "tenant_id",
        "company_key",
        "company",
        "company_name",
    }
    assert "agent_tool_enabled" not in records[ACTION_REFRESH_COMPANY_PUBLIC_WEB]


def test_company_public_web_request_is_canonicalized_before_persistence() -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)

    normalized_input, normalized_target = spec.validate_request(
        input_payload=_valid_input(
            target_company=" Open AI ",
            source_families=["company_research", "company_homepage", "company_research"],
            seed_urls=[
                "https://OPENAI.com/research/",
                "https://openai.com/",
                "https://openai.com/research",
            ],
        ),
        target_ref=_valid_target(),
    )

    assert normalized_input == {
        "target_company": "Open AI",
        "source_families": ["company_homepage", "company_research"],
        "seed_urls": ["https://openai.com/", "https://openai.com/research"],
        "max_assets": 50,
        "force_refresh": False,
        "collection_mode": "seed_url_only",
    }
    assert normalized_target == _valid_target()


def test_company_public_web_seed_families_are_an_independent_allowed_set() -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)

    normalized_input, _ = spec.validate_request(
        input_payload=_valid_input(
            source_families=["company_homepage", "company_engineering"],
            seed_urls=["https://openai.com/", "https://openai.com/engineering"],
        ),
        target_ref=_valid_target(),
    )

    assert normalized_input["source_families"] == ["company_engineering", "company_homepage"]
    assert normalized_input["seed_urls"] == [
        "https://openai.com/",
        "https://openai.com/engineering",
    ]
    assets = build_company_public_web_seed_assets(
        target_company="OpenAI",
        company_key="openai",
        source_families=normalized_input["source_families"],
        seed_urls=normalized_input["seed_urls"],
        run_id="run-independent-seed-families",
        max_assets=50,
    )
    assert {asset["url"]: asset["source_family"] for asset in assets} == {
        "https://openai.com/": "company_homepage",
        "https://openai.com/engineering": "company_engineering",
    }


def test_company_public_web_accepts_multiple_seed_urls_with_one_inferred_family() -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)

    normalized_input, _ = spec.validate_request(
        input_payload=_valid_input(
            source_families=["company_homepage"],
            seed_urls=["https://openai.com/about", "https://openai.com/"],
        ),
        target_ref=_valid_target(),
    )

    assert normalized_input["source_families"] == ["company_homepage"]
    assert normalized_input["seed_urls"] == ["https://openai.com/", "https://openai.com/about"]
    assets = build_company_public_web_seed_assets(
        target_company="OpenAI",
        company_key="openai",
        source_families=normalized_input["source_families"],
        seed_urls=normalized_input["seed_urls"],
        run_id="run-repeated-inferred-family",
        max_assets=50,
    )
    assert [asset["source_family"] for asset in assets] == ["company_homepage", "company_homepage"]


def test_company_public_web_rejects_seed_whose_inferred_family_is_not_requested() -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)

    with pytest.raises(
        ActionRequestValidationError,
        match="company_public_web_seed_source_family_not_requested",
    ):
        spec.validate_request(
            input_payload=_valid_input(
                source_families=["company_homepage"],
                seed_urls=["https://openai.com/", "https://openai.com/engineering"],
            ),
            target_ref=_valid_target(),
        )


@pytest.mark.parametrize("missing_field", ["target_company", "source_families", "seed_urls"])
def test_company_public_web_request_requires_explicit_deterministic_seed_inputs(missing_field: str) -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)
    request = _valid_input()
    request.pop(missing_field)

    with pytest.raises(ActionRequestValidationError, match="action_request_schema_validation_failed"):
        spec.validate_request(input_payload=request, target_ref=_valid_target())


@pytest.mark.parametrize(
    "override",
    [
        {"source_families": ["unknown_source_family"]},
        {"source_families": [""]},
        {"seed_urls": [""]},
        {"seed_urls": ["not-a-url"]},
        {"seed_urls": ["ftp://openai.com/research"]},
        {"seed_urls": ["https://"]},
        {"seed_urls": ["https://openai.com/bad path"]},
    ],
)
def test_company_public_web_request_rejects_unknown_sources_and_non_http_urls(
    override: dict[str, Any],
) -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)
    with pytest.raises(ActionRequestValidationError, match="action_request_schema_validation_failed"):
        spec.validate_request(
            input_payload=_valid_input(**override),
            target_ref=_valid_target(),
        )


def test_company_public_web_request_rejects_mixed_valid_and_unnormalizable_seed_urls() -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)

    with pytest.raises(
        ActionRequestValidationError,
        match="company_public_web_request_canonicalization_failed",
    ):
        spec.validate_request(
            input_payload=_valid_input(
                seed_urls=["https://openai.com/", "https:///missing-host"],
            ),
            target_ref=_valid_target(),
        )


@pytest.mark.parametrize(
    ("force_refresh", "refresh_nonce", "reason"),
    [
        (True, None, "company_public_web_refresh_nonce_required"),
        (False, "unused", "company_public_web_refresh_nonce_requires_force_refresh"),
    ],
)
def test_company_public_web_refresh_nonce_is_an_exact_force_identity(
    force_refresh: bool,
    refresh_nonce: str | None,
    reason: str,
) -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)
    request = _valid_input(force_refresh=force_refresh)
    if refresh_nonce is not None:
        request["refresh_nonce"] = refresh_nonce

    with pytest.raises(ActionRequestValidationError, match=reason):
        spec.validate_request(input_payload=request, target_ref=_valid_target())

    forced, _ = spec.validate_request(
        input_payload=_valid_input(force_refresh=True, refresh_nonce="refresh-2026-07-16"),
        target_ref=_valid_target(),
    )
    assert forced["force_refresh"] is True
    assert forced["refresh_nonce"] == "refresh-2026-07-16"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("company", "OpenAI"),
        ("company_name", "OpenAI"),
        ("company_key", "openai"),
        ("command_type", "company.public_web.refresh"),
        ("command_payload", {}),
        ("workflow_run_id", "wf-caller"),
        ("job_id", "job-caller"),
        ("plan_review_id", "review-caller"),
        ("retry_operation_run_id", "retry-caller"),
        ("options", {"max_assets": 5}),
        ("force_refresh", "false"),
    ],
)
def test_company_public_web_request_rejects_legacy_nested_and_identity_aliases(
    field: str,
    value: Any,
) -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)

    with pytest.raises(ActionRequestValidationError, match="action_request_schema_validation_failed"):
        spec.validate_request(
            input_payload=_valid_input(**{field: value}),
            target_ref=_valid_target(),
        )


@pytest.mark.parametrize("collection_mode", ["provider_search", "collector_bundle"])
def test_company_public_web_request_rejects_non_deterministic_collection_modes(collection_mode: str) -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)
    with pytest.raises(ActionRequestValidationError, match="action_request_schema_validation_failed"):
        spec.validate_request(
            input_payload=_valid_input(collection_mode=collection_mode),
            target_ref=_valid_target(),
        )


def test_company_public_web_target_binder_derives_exact_authenticated_and_open_targets() -> None:
    binder = CompanyPublicWebTargetBinder()
    authenticated = binder(
        ActionBindContext(
            authorization_mode=AUTHORIZATION_MODE_AUTHENTICATED,
            workspace_id="user-alice",
            owner_user_id="alice",
            target_selector={"target_company": "Open AI"},
        )
    )
    operator = binder(
        ActionBindContext(
            authorization_mode=AUTHORIZATION_MODE_OPEN_OPERATOR,
            workspace_id="operator-workspace",
            owner_user_id="",
            target_selector={"target_company": "Thinking Machines Lab"},
        )
    )

    assert authenticated.owner_module == COMPANY_PUBLIC_WEB_TARGET_OWNER
    assert authenticated.target_ref == {"workspace_id": "user-alice", "company_key": "openai"}
    assert operator.target_ref == {
        "workspace_id": "operator-workspace",
        "company_key": "thinkingmachineslab",
    }
    assert binder.revalidate_snapshot(
        target_ref=authenticated.target_ref,
        operation_workspace_id="user-alice",
    ) == {"workspace_id": "user-alice", "company_key": "openai"}


@pytest.mark.parametrize(
    "selector",
    [
        {},
        {"target_company": ""},
        {"target_company": 7},
        {"target_company": "OpenAI", "company_key": "openai"},
        {"company": "OpenAI"},
    ],
)
def test_company_public_web_target_binder_rejects_noncanonical_selectors(selector: dict[str, Any]) -> None:
    binder = CompanyPublicWebTargetBinder()
    with pytest.raises(ActionTargetBindingError, match=COMPANY_PUBLIC_WEB_TARGET_INVALID):
        binder(
            ActionBindContext(
                authorization_mode=AUTHORIZATION_MODE_AUTHENTICATED,
                workspace_id="user-alice",
                owner_user_id="alice",
                target_selector=selector,
            )
        )


def test_company_public_web_target_registry_and_revalidation_fail_closed() -> None:
    registry = build_company_public_web_target_binder_registry()
    context = ActionBindContext(
        authorization_mode=AUTHORIZATION_MODE_AUTHENTICATED,
        workspace_id="user-alice",
        owner_user_id="alice",
        target_selector={"target_company": "OpenAI"},
    )
    bound = registry.bind(action_type=ACTION_REFRESH_COMPANY_PUBLIC_WEB, context=context)
    assert registry.to_record() == {
        ACTION_REFRESH_COMPANY_PUBLIC_WEB: {"owner_module": COMPANY_PUBLIC_WEB_TARGET_OWNER}
    }
    assert bound.target_ref == _valid_target()

    binder = CompanyPublicWebTargetBinder()
    for invalid_target, operation_workspace_id in (
        ({"workspace_id": "foreign", "company_key": "openai"}, "user-alice"),
        ({"workspace_id": "user-alice", "company_key": "Open AI"}, "user-alice"),
        ({"workspace_id": "user-alice", "company_key": "openai", "target_company": "OpenAI"}, "user-alice"),
    ):
        with pytest.raises(ActionTargetBindingError, match=COMPANY_PUBLIC_WEB_TARGET_INVALID):
            binder.revalidate_snapshot(
                target_ref=invalid_target,
                operation_workspace_id=operation_workspace_id,
            )


def test_company_public_web_persisted_request_rejects_python_only_containers() -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)
    writer = OperationRuntimeWriter(SimpleNamespace())
    action = {
        "action_id": "action-company-public-web-strict-json",
        "action_type": ACTION_REFRESH_COMPANY_PUBLIC_WEB,
        "workspace_id": "user-alice",
        "owner_module": spec.owner_module,
        "operation_type": spec.operation_type,
        "input": {
            **_valid_input(),
            "source_families": ("company_homepage",),
        },
        "target_ref": _valid_target(),
        "request_schema_version": spec.request_schema_version,
        "request_schema_digest": spec.request_schema_digest,
    }

    with pytest.raises(OperationRuntimeStateConflict, match="operation_action_request_schema_validation_conflict"):
        writer.validate_persisted_action_request(action=action)

    canonical_input, canonical_target = spec.validate_request(
        input_payload=_valid_input(),
        target_ref=_valid_target(),
    )
    canonical_action = {
        **action,
        "input": canonical_input,
        "target_ref": canonical_target,
    }
    assert writer.validate_persisted_action_request(action=canonical_action) is spec


def test_company_public_web_persisted_request_must_equal_canonical_normalization() -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_REFRESH_COMPANY_PUBLIC_WEB)
    writer = OperationRuntimeWriter(SimpleNamespace())
    action = {
        "action_id": "action-company-public-web-canonical-json",
        "action_type": ACTION_REFRESH_COMPANY_PUBLIC_WEB,
        "workspace_id": "user-alice",
        "owner_module": spec.owner_module,
        "operation_type": spec.operation_type,
        "input": {
            **_valid_input(),
            "source_families": ["company_research", "company_homepage", "company_research"],
        },
        "target_ref": _valid_target(),
        "request_schema_version": spec.request_schema_version,
        "request_schema_digest": spec.request_schema_digest,
    }

    with pytest.raises(OperationRuntimeStateConflict, match="operation_action_request_schema_validation_conflict"):
        writer.validate_persisted_action_request(action=action)
