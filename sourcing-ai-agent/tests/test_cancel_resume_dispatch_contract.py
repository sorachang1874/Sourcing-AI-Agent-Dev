"""Characterization contract for the running-command cancel/resume dispatchers.

Phase 4 Step 3 (docs/PHASE4_ENTANGLED_CORE_DESIGN.md §2(c) C1 + §3 Step 3).

These tests pin the exact (owner, command_type) -> owner-specific handler routing
performed by:

    SourcingOrchestrator._cancel_running_workflow_command_via_owner
    SourcingOrchestrator._resume_running_workflow_command_via_owner

They were authored GREEN against the pre-refactor hand-maintained branch ladders
and must stay byte-identical after the ladders are converted to a
CommandTypeSpec.cancel_handler / .resume_handler table lookup. A failure here
means observable cancel/resume dispatch routing changed.

The dispatchers reference only `self.<handler>` plus (on the default path) two
record helpers; they touch no store. We therefore exercise dispatch on a bare
``object.__new__(SourcingOrchestrator)`` instance with the handler methods
stubbed to a routing sentinel — this isolates DISPATCH (how the handler is
found) from each handler's body (pinned elsewhere by the E2E suites).

Do NOT regenerate the golden tables to make a failing test pass.
"""

from __future__ import annotations

from typing import Any

import sourcing_agent.durable_runtime as dr
from sourcing_agent.orchestrator import SourcingOrchestrator

# ---------------------------------------------------------------------------
# Golden dispatch tables (canonical-owner routing for every known command type)
# ---------------------------------------------------------------------------
# Derived from the pre-refactor branch ladders by first-match-wins evaluation
# with each command type presented under its canonical owner. Every one of the
# 41 known command types routes to an owner-specific handler (no canonical-owner
# fall-through to the default).

GOLDEN_CANCEL_DISPATCH: dict[str, str] = {
    "acquisition.intent.resolve": "_cancel_running_orchestration_before_downstream",
    "acquisition.plan.build": "_cancel_running_orchestration_before_downstream",
    "acquisition.plan.commit": "_cancel_running_acquisition_plan_commit_before_probe",
    "acquisition.plan_review.request": "_cancel_running_acquisition_plan_review_request_command",
    "acquisition.probe.collect": "_cancel_running_provider_attempt_command_before_attempt",
    "acquisition.probe.submit": "_cancel_running_provider_attempt_command_before_attempt",
    "acquisition.run.create": "_cancel_running_orchestration_before_downstream",
    "acquisition.scale.plan": "_cancel_running_acquisition_scale_plan_before_discovery",
    "collection.authoritative.merge": "_cancel_running_domain_mutation_command_before_attempt",
    "company.logo.profile_experience.discover": "_cancel_running_domain_mutation_command_before_attempt",
    "company.public_web.assets.materialize": "_cancel_running_company_public_web_assets_materialize_before_sync",
    "company.public_web.refresh": "_cancel_running_orchestration_before_downstream",
    "company.public_web.source.collect": "_cancel_running_provider_attempt_command_before_attempt",
    "crm.note.add": "_cancel_running_crm_writer_command_before_mutation_attempt",
    "crm.public_web.documents.fetch": "_cancel_running_crm_public_web_phase_command",
    "crm.public_web.evidence.adjudicate": "_cancel_running_crm_public_web_phase_command",
    "crm.public_web.model_safe.finalize": "_cancel_running_crm_public_web_phase_command",
    "crm.public_web.queue_batch": "_cancel_running_crm_public_web_queue_batch_before_phase_commands",
    "crm.public_web.search.poll_fetch": "_cancel_running_crm_public_web_phase_command",
    "crm.public_web.search.submit": "_cancel_running_crm_public_web_phase_command",
    "crm.public_web.signals.materialize": "_cancel_running_crm_public_web_phase_command",
    "crm.record.add_from_projection": "_cancel_running_crm_writer_command_before_mutation_attempt",
    "crm.record.update": "_cancel_running_crm_writer_command_before_mutation_attempt",
    "crm.task.create": "_cancel_running_crm_writer_command_before_mutation_attempt",
    "excel.intake.run": "_cancel_running_excel_intake_command",
    "export.crm_public_web.generate": "_cancel_running_export_command",
    "export.projection.generate": "_cancel_running_export_command",
    "linkedin.discovery_query.run": "_cancel_running_provider_attempt_command_before_attempt",
    "linkedin.local_profile_delta.apply": "_cancel_running_domain_mutation_command_before_attempt",
    "linkedin.profile_fetch.activity.run": "_cancel_running_profile_fetch_activity_before_cache_lookup_attempt",
    "linkedin.profile_fetch.provider.fetch": "_cancel_running_provider_attempt_command_before_attempt",
    "linkedin.profile_refill.submit_batch": "_cancel_running_provider_attempt_command_before_attempt",
    "linkedin.profile_terminal.admit": "_cancel_running_domain_mutation_command_before_attempt",
    "linkedin.profile_url_terminal.record": "_cancel_running_domain_mutation_command_before_attempt",
    "media.asset.cache": "_cancel_running_media_asset_cache_command_before_fetch_upload_attempt",
    "projection.board_visible_patch.publish": "_cancel_running_domain_mutation_command_before_attempt",
    "projection.facet_layering.build": "_cancel_running_domain_mutation_command_before_attempt",
    "projection.person_search_index.build": "_cancel_running_domain_mutation_command_before_attempt",
    "projection.profile_admission.apply": "_cancel_running_domain_mutation_command_before_attempt",
    "projection.run_scope.finalize": "_cancel_running_domain_mutation_command_before_attempt",
    "snapshot.compaction.run": "_cancel_running_domain_mutation_command_before_attempt",
}

GOLDEN_RESUME_DISPATCH: dict[str, str] = {
    "acquisition.intent.resolve": "_resume_running_orchestration_command",
    "acquisition.plan.build": "_resume_running_orchestration_command",
    "acquisition.plan.commit": "_resume_running_orchestration_command",
    "acquisition.plan_review.request": "_resume_running_orchestration_command",
    "acquisition.probe.collect": "_resume_running_provider_attempt_command",
    "acquisition.probe.submit": "_resume_running_provider_attempt_command",
    "acquisition.run.create": "_resume_running_orchestration_command",
    "acquisition.scale.plan": "_resume_running_orchestration_command",
    "collection.authoritative.merge": "_resume_running_domain_mutation_command",
    "company.logo.profile_experience.discover": "_resume_running_domain_mutation_command",
    "company.public_web.assets.materialize": "_resume_running_company_public_web_assets_materialize_command",
    "company.public_web.refresh": "_resume_running_orchestration_command",
    "company.public_web.source.collect": "_resume_running_company_public_web_source_collect_command",
    "crm.note.add": "_resume_running_crm_writer_command",
    "crm.public_web.documents.fetch": "_resume_running_crm_public_web_phase_command",
    "crm.public_web.evidence.adjudicate": "_resume_running_crm_public_web_phase_command",
    "crm.public_web.model_safe.finalize": "_resume_running_crm_public_web_phase_command",
    "crm.public_web.queue_batch": "_resume_running_orchestration_command",
    "crm.public_web.search.poll_fetch": "_resume_running_crm_public_web_phase_command",
    "crm.public_web.search.submit": "_resume_running_crm_public_web_phase_command",
    "crm.public_web.signals.materialize": "_resume_running_crm_public_web_phase_command",
    "crm.record.add_from_projection": "_resume_running_crm_writer_command",
    "crm.record.update": "_resume_running_crm_writer_command",
    "crm.task.create": "_resume_running_crm_writer_command",
    "excel.intake.run": "_resume_running_excel_intake_command",
    "export.crm_public_web.generate": "_resume_running_export_command",
    "export.projection.generate": "_resume_running_export_command",
    "linkedin.discovery_query.run": "_resume_running_provider_attempt_command",
    "linkedin.local_profile_delta.apply": "_resume_running_domain_mutation_command",
    "linkedin.profile_fetch.activity.run": "_resume_running_domain_mutation_command",
    "linkedin.profile_fetch.provider.fetch": "_resume_running_provider_attempt_command",
    "linkedin.profile_refill.submit_batch": "_resume_running_provider_attempt_command",
    "linkedin.profile_terminal.admit": "_resume_running_domain_mutation_command",
    "linkedin.profile_url_terminal.record": "_resume_running_domain_mutation_command",
    "media.asset.cache": "_resume_running_media_asset_cache_command",
    "projection.board_visible_patch.publish": "_resume_running_domain_mutation_command",
    "projection.facet_layering.build": "_resume_running_domain_mutation_command",
    "projection.person_search_index.build": "_resume_running_domain_mutation_command",
    "projection.profile_admission.apply": "_resume_running_domain_mutation_command",
    "projection.run_scope.finalize": "_resume_running_domain_mutation_command",
    "snapshot.compaction.run": "_resume_running_domain_mutation_command",
}

# Owner-agnostic fall-through routing: what a KNOWN command type routes to when
# presented with a NON-canonical owner. Owner-guarded branches are skipped, but
# the command-type-only (unconditional) sets still match. ``None`` means the
# command type has no unconditional branch -> default response. Pinned from the
# pre-refactor ladders.
GOLDEN_CANCEL_OWNER_AGNOSTIC: dict[str, str | None] = {
    "acquisition.intent.resolve": "_cancel_running_orchestration_before_downstream",
    "acquisition.plan.build": "_cancel_running_orchestration_before_downstream",
    "acquisition.plan.commit": None,
    "acquisition.plan_review.request": None,
    "acquisition.probe.collect": "_cancel_running_provider_attempt_command_before_attempt",
    "acquisition.probe.submit": "_cancel_running_provider_attempt_command_before_attempt",
    "acquisition.run.create": "_cancel_running_orchestration_before_downstream",
    "acquisition.scale.plan": None,
    "collection.authoritative.merge": "_cancel_running_domain_mutation_command_before_attempt",
    "company.logo.profile_experience.discover": "_cancel_running_domain_mutation_command_before_attempt",
    "company.public_web.assets.materialize": "_cancel_running_domain_mutation_command_before_attempt",
    "company.public_web.refresh": "_cancel_running_orchestration_before_downstream",
    "company.public_web.source.collect": "_cancel_running_provider_attempt_command_before_attempt",
    "crm.note.add": "_cancel_running_domain_mutation_command_before_attempt",
    "crm.public_web.documents.fetch": None,
    "crm.public_web.evidence.adjudicate": None,
    "crm.public_web.model_safe.finalize": None,
    "crm.public_web.queue_batch": None,
    "crm.public_web.search.poll_fetch": None,
    "crm.public_web.search.submit": None,
    "crm.public_web.signals.materialize": None,
    "crm.record.add_from_projection": "_cancel_running_domain_mutation_command_before_attempt",
    "crm.record.update": "_cancel_running_domain_mutation_command_before_attempt",
    "crm.task.create": "_cancel_running_domain_mutation_command_before_attempt",
    "excel.intake.run": None,
    "export.crm_public_web.generate": "_cancel_running_export_command",
    "export.projection.generate": "_cancel_running_export_command",
    "linkedin.discovery_query.run": "_cancel_running_provider_attempt_command_before_attempt",
    "linkedin.local_profile_delta.apply": "_cancel_running_domain_mutation_command_before_attempt",
    "linkedin.profile_fetch.activity.run": "_cancel_running_domain_mutation_command_before_attempt",
    "linkedin.profile_fetch.provider.fetch": "_cancel_running_provider_attempt_command_before_attempt",
    "linkedin.profile_refill.submit_batch": "_cancel_running_provider_attempt_command_before_attempt",
    "linkedin.profile_terminal.admit": "_cancel_running_domain_mutation_command_before_attempt",
    "linkedin.profile_url_terminal.record": "_cancel_running_domain_mutation_command_before_attempt",
    "media.asset.cache": "_cancel_running_domain_mutation_command_before_attempt",
    "projection.board_visible_patch.publish": "_cancel_running_domain_mutation_command_before_attempt",
    "projection.facet_layering.build": "_cancel_running_domain_mutation_command_before_attempt",
    "projection.person_search_index.build": "_cancel_running_domain_mutation_command_before_attempt",
    "projection.profile_admission.apply": "_cancel_running_domain_mutation_command_before_attempt",
    "projection.run_scope.finalize": "_cancel_running_domain_mutation_command_before_attempt",
    "snapshot.compaction.run": "_cancel_running_domain_mutation_command_before_attempt",
}

GOLDEN_RESUME_OWNER_AGNOSTIC: dict[str, str | None] = {
    "acquisition.intent.resolve": "_resume_running_orchestration_command",
    "acquisition.plan.build": "_resume_running_orchestration_command",
    "acquisition.plan.commit": "_resume_running_orchestration_command",
    "acquisition.plan_review.request": "_resume_running_orchestration_command",
    "acquisition.probe.collect": "_resume_running_provider_attempt_command",
    "acquisition.probe.submit": "_resume_running_provider_attempt_command",
    "acquisition.run.create": "_resume_running_orchestration_command",
    "acquisition.scale.plan": "_resume_running_orchestration_command",
    "collection.authoritative.merge": "_resume_running_domain_mutation_command",
    "company.logo.profile_experience.discover": "_resume_running_domain_mutation_command",
    "company.public_web.assets.materialize": "_resume_running_domain_mutation_command",
    "company.public_web.refresh": "_resume_running_orchestration_command",
    "company.public_web.source.collect": "_resume_running_provider_attempt_command",
    "crm.note.add": "_resume_running_domain_mutation_command",
    "crm.public_web.documents.fetch": None,
    "crm.public_web.evidence.adjudicate": None,
    "crm.public_web.model_safe.finalize": None,
    "crm.public_web.queue_batch": "_resume_running_orchestration_command",
    "crm.public_web.search.poll_fetch": None,
    "crm.public_web.search.submit": None,
    "crm.public_web.signals.materialize": None,
    "crm.record.add_from_projection": "_resume_running_domain_mutation_command",
    "crm.record.update": "_resume_running_domain_mutation_command",
    "crm.task.create": "_resume_running_domain_mutation_command",
    "excel.intake.run": None,
    "export.crm_public_web.generate": "_resume_running_export_command",
    "export.projection.generate": "_resume_running_export_command",
    "linkedin.discovery_query.run": "_resume_running_provider_attempt_command",
    "linkedin.local_profile_delta.apply": "_resume_running_domain_mutation_command",
    "linkedin.profile_fetch.activity.run": "_resume_running_domain_mutation_command",
    "linkedin.profile_fetch.provider.fetch": "_resume_running_provider_attempt_command",
    "linkedin.profile_refill.submit_batch": "_resume_running_provider_attempt_command",
    "linkedin.profile_terminal.admit": "_resume_running_domain_mutation_command",
    "linkedin.profile_url_terminal.record": "_resume_running_domain_mutation_command",
    "media.asset.cache": "_resume_running_domain_mutation_command",
    "projection.board_visible_patch.publish": "_resume_running_domain_mutation_command",
    "projection.facet_layering.build": "_resume_running_domain_mutation_command",
    "projection.person_search_index.build": "_resume_running_domain_mutation_command",
    "projection.profile_admission.apply": "_resume_running_domain_mutation_command",
    "projection.run_scope.finalize": "_resume_running_domain_mutation_command",
    "snapshot.compaction.run": "_resume_running_domain_mutation_command",
}

ALL_HANDLER_NAMES = (
    set(GOLDEN_CANCEL_DISPATCH.values())
    | set(GOLDEN_RESUME_DISPATCH.values())
    | {v for v in GOLDEN_CANCEL_OWNER_AGNOSTIC.values() if v}
    | {v for v in GOLDEN_RESUME_OWNER_AGNOSTIC.values() if v}
)

BOGUS_COMMAND_TYPE = "bogus.command.type"


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------
def _canonical_owner(command_type: str) -> str:
    return dr.DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(command_type)


def _routing_orchestrator() -> tuple[SourcingOrchestrator, dict[str, Any]]:
    """A bare orchestrator with every owner-specific handler stubbed.

    Each handler is replaced with a sentinel-returning closure so the
    dispatcher's choice of handler is observable without running any handler
    body or touching a store. The two default-path record helpers are stubbed
    to identity-ish records so the default response is also exercisable.
    """

    orch = object.__new__(SourcingOrchestrator)
    calls: dict[str, Any] = {}

    def _make_stub(handler_name: str):
        def _stub(self, command, *, payload):  # noqa: ANN001
            return {
                "__routed_to__": handler_name,
                "command_type": str((command or {}).get("command_type") or ""),
                "owner": str((command or {}).get("owner") or ""),
                "payload": payload,
            }

        return _stub

    for handler_name in ALL_HANDLER_NAMES:
        # Bind as an instance attribute already partial-applied to ``orch``.
        stub = _make_stub(handler_name)
        setattr(orch, handler_name, lambda command, *, payload, _s=stub: _s(orch, command, payload=payload))

    # Default-path helpers (only reached by the unknown/unowned fall-through).
    orch._workflow_command_api_record = lambda command: {"api_record": dict(command or {})}  # type: ignore[attr-defined]
    orch._workflow_command_control_response_policy_records = lambda command: {  # type: ignore[attr-defined]
        "control_policy_record": True
    }
    return orch, calls


def _command(command_type: str, owner: str, status: str = "running") -> dict[str, Any]:
    return {
        "command_id": "cmd-test",
        "command_type": command_type,
        "owner": owner,
        "status": status,
    }


# ---------------------------------------------------------------------------
# Tests: golden table integrity
# ---------------------------------------------------------------------------
def test_golden_tables_cover_exactly_the_known_command_type_universe():
    known = set(dr.DEFAULT_COMMAND_OWNER_REGISTRY.to_record().keys())
    assert len(known) == 41
    assert set(GOLDEN_CANCEL_DISPATCH.keys()) == known
    assert set(GOLDEN_RESUME_DISPATCH.keys()) == known
    assert set(GOLDEN_CANCEL_OWNER_AGNOSTIC.keys()) == known
    assert set(GOLDEN_RESUME_OWNER_AGNOSTIC.keys()) == known


def test_every_known_command_type_has_a_canonical_owner_handler():
    # No canonical-owner command type falls through to the default response.
    for command_type in dr.DEFAULT_COMMAND_OWNER_REGISTRY.to_record():
        assert GOLDEN_CANCEL_DISPATCH[command_type], command_type
        assert GOLDEN_RESUME_DISPATCH[command_type], command_type


# ---------------------------------------------------------------------------
# Tests: canonical-owner routing
# ---------------------------------------------------------------------------
def test_cancel_dispatch_routes_each_command_type_to_its_handler():
    orch, _ = _routing_orchestrator()
    for command_type, expected_handler in GOLDEN_CANCEL_DISPATCH.items():
        owner = _canonical_owner(command_type)
        result = orch._cancel_running_workflow_command_via_owner(
            _command(command_type, owner), payload={"actor": "tester"}
        )
        assert result.get("__routed_to__") == expected_handler, command_type
        assert result["command_type"] == command_type
        assert result["owner"] == owner


def test_resume_dispatch_routes_each_command_type_to_its_handler():
    orch, _ = _routing_orchestrator()
    for command_type, expected_handler in GOLDEN_RESUME_DISPATCH.items():
        owner = _canonical_owner(command_type)
        result = orch._resume_running_workflow_command_via_owner(
            _command(command_type, owner), payload={"actor": "tester"}
        )
        assert result.get("__routed_to__") == expected_handler, command_type
        assert result["command_type"] == command_type
        assert result["owner"] == owner


# ---------------------------------------------------------------------------
# Tests: owner-mismatch fall-through (defensive path preserved from the ladder)
# ---------------------------------------------------------------------------
def test_cancel_owner_mismatch_falls_through_to_owner_agnostic_or_default():
    orch, _ = _routing_orchestrator()
    wrong_owner = "definitely_not_the_canonical_owner"
    for command_type, expected in GOLDEN_CANCEL_OWNER_AGNOSTIC.items():
        result = orch._cancel_running_workflow_command_via_owner(
            _command(command_type, wrong_owner), payload={"actor": "tester"}
        )
        if expected is None:
            assert result.get("__routed_to__") is None, command_type
            assert result["status"] == "invalid"
            assert result["reason"] == "running_command_requires_owner_specific_cancel"
            assert result["module_state_mutated"] is False
            assert result["owner_specific_control"] is False
        else:
            assert result.get("__routed_to__") == expected, command_type


def test_resume_owner_mismatch_falls_through_to_owner_agnostic_or_default():
    orch, _ = _routing_orchestrator()
    wrong_owner = "definitely_not_the_canonical_owner"
    for command_type, expected in GOLDEN_RESUME_OWNER_AGNOSTIC.items():
        result = orch._resume_running_workflow_command_via_owner(
            _command(command_type, wrong_owner), payload={"actor": "tester"}
        )
        if expected is None:
            assert result.get("__routed_to__") is None, command_type
            assert result["status"] == "invalid"
            assert result["reason"] == "running_command_requires_owner_specific_resume"
            assert result["module_state_mutated"] is False
            assert result["owner_specific_control"] is False
        else:
            assert result.get("__routed_to__") == expected, command_type


# ---------------------------------------------------------------------------
# Tests: default fall-through for unknown command types
# ---------------------------------------------------------------------------
def test_cancel_unknown_command_type_returns_default_invalid_response():
    orch, _ = _routing_orchestrator()
    result = orch._cancel_running_workflow_command_via_owner(
        _command(BOGUS_COMMAND_TYPE, "whoever"), payload={}
    )
    assert result.get("__routed_to__") is None
    assert result["status"] == "invalid"
    assert result["reason"] == "running_command_requires_owner_specific_cancel"
    assert result["command_type"] == BOGUS_COMMAND_TYPE
    assert result["owner"] == "whoever"
    assert result["module_state_mutated"] is False
    assert result["owner_specific_control"] is False
    assert result["contract"] == "w11_workflow_command_owner_specific_control_v1"


def test_resume_unknown_command_type_returns_default_invalid_response():
    orch, _ = _routing_orchestrator()
    result = orch._resume_running_workflow_command_via_owner(
        _command(BOGUS_COMMAND_TYPE, "whoever"), payload={}
    )
    assert result.get("__routed_to__") is None
    assert result["status"] == "invalid"
    assert result["reason"] == "running_command_requires_owner_specific_resume"
    assert result["command_type"] == BOGUS_COMMAND_TYPE
    assert result["owner"] == "whoever"
    assert result["module_state_mutated"] is False
    assert result["owner_specific_control"] is False
    assert result["contract"] == "w11_workflow_command_owner_specific_control_v1"


def test_default_response_preserves_command_status_echo():
    orch, _ = _routing_orchestrator()
    for dispatcher, reason in (
        (
            "_cancel_running_workflow_command_via_owner",
            "running_command_requires_owner_specific_cancel",
        ),
        (
            "_resume_running_workflow_command_via_owner",
            "running_command_requires_owner_specific_resume",
        ),
    ):
        result = getattr(orch, dispatcher)(
            _command(BOGUS_COMMAND_TYPE, "whoever", status="claimed"), payload={}
        )
        assert result["reason"] == reason
        assert result["command_status"] == "claimed"
