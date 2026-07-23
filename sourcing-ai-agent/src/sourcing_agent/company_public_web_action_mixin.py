"""Company public-web action band — WS2 slice-3 re-homing (2026-07-22).

The 18-method `_company_public_web_*` family that commit b6a9169 (track-d
action schema activation) grew inside the orchestrator moves here verbatim
as a mixin (master plan §3 slice 3: regrowth bands return to owners; the
anti-regrowth ratchet enforces the boundary from now on). SourcingOrchestrator
inherits this mixin, so every existing `self._company_public_web_*` call site
is untouched.

Mixin protocol (resolved via the host class at runtime — the four
out-of-family collaborators this band calls):
  _sync_operation_run_from_workflow_command, _workflow_command_from_apply_
  result_or_store, _workflow_command_lease_active, _workflow_command_
  observation — plus `self.store`. A follow-up may convert this to
  composition with an explicit deps registry (CandidateSourceResolver
  precedent); the mixin is the zero-call-site-churn first step.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from .action_target_binding import (
    AUTHORIZATION_MODE_AUTHENTICATED,
    AUTHORIZATION_MODE_OPEN_OPERATOR,
    COMPANY_PUBLIC_WEB_TARGET_INVALID,
    ActionBindContext,
    ActionTargetBindingError,
)
from .company_public_web_assets import (
    COMPANY_PUBLIC_WEB_MATERIALIZATION_SNAPSHOT_DIGEST_KEY,
    COMPANY_PUBLIC_WEB_MATERIALIZATION_SNAPSHOT_SCHEMA_KEY,
    company_public_web_materialization_snapshot_identity,
)
from .company_registry import resolve_company_alias_key
from .durable_runtime import (
    COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_REFRESH_OWNER,
    COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
    command_causality_for,
    command_id_for,
    default_stage_id_for_command_type,
)
from .json_contract import json_contract_equal
from .operation_runtime import (
    ACTION_REFRESH_COMPANY_PUBLIC_WEB,
    COMPANY_PUBLIC_WEB_ACTION_TYPES,
    DEFAULT_ACTION_REGISTRY,
    OPERATION_ACTION_TERMINAL_STATUSES,
    OPERATION_RUN_TERMINAL_STATUSES,
    OperationRuntimeStateConflict,
)
from .workflow_progressed_child_contract import (
    build_company_public_web_phase_plan,
    canonical_progressed_child_identity,
    expected_progressed_child_row,
    progressed_child_completion_contract,
    progressed_child_completion_contract_for_child,
    progressed_child_contract_pin,
    progressed_child_plan_event_violation,
)


class CompanyPublicWebActionMixin:
    """Verbatim band; see module docstring for the host-class protocol."""

    def _bind_operation_company_public_web_target(
        self,
        *,
        action_type: str,
        workspace_id: str,
        expected_workspace_id: str,
        expected_owner_user_id: str,
        target_ref: dict[str, Any],
        input_payload: dict[str, Any],
    ) -> dict[str, Any]:
        if action_type not in COMPANY_PUBLIC_WEB_ACTION_TYPES:
            return {
                "status": "ready",
                "target_ref": target_ref,
                "input_payload": input_payload,
                "owner_bound_target_ref": None,
            }
        expected_workspace = str(expected_workspace_id or "").strip()
        expected_owner = str(expected_owner_user_id or "").strip()
        if bool(expected_workspace) != bool(expected_owner):
            return {"status": "invalid", "reason": "action_bind_context_owner_incomplete"}
        normalized_workspace = str(workspace_id or "default").strip() or "default"
        if expected_workspace and normalized_workspace != expected_workspace:
            return {"status": "invalid", "reason": COMPANY_PUBLIC_WEB_TARGET_INVALID}
        if target_ref:
            return {"status": "invalid", "reason": COMPANY_PUBLIC_WEB_TARGET_INVALID}
        spec = DEFAULT_ACTION_REGISTRY.spec_for(action_type)
        forbidden_input_fields = sorted(spec.owner_reserved_request_fields & set(input_payload))
        if forbidden_input_fields:
            return {
                "status": "invalid",
                "reason": ("action_request_target_fields_are_owner_reserved:" + ",".join(forbidden_input_fields)),
            }
        raw_target_company = input_payload.get("target_company")
        if not isinstance(raw_target_company, str):
            return {"status": "invalid", "reason": COMPANY_PUBLIC_WEB_TARGET_INVALID}
        try:
            context = ActionBindContext(
                authorization_mode=(
                    AUTHORIZATION_MODE_AUTHENTICATED if expected_workspace else AUTHORIZATION_MODE_OPEN_OPERATOR
                ),
                workspace_id=expected_workspace or normalized_workspace,
                owner_user_id=expected_owner,
                target_selector={"target_company": raw_target_company},
            )
            owner_bound_target_ref = self._company_public_web_target_binder_registry.bind(
                action_type=action_type,
                context=context,
            )
        except ActionTargetBindingError as exc:
            return {"status": "invalid", "reason": exc.reason}
        return {
            "status": "ready",
            "target_ref": {},
            "input_payload": dict(input_payload),
            "owner_bound_target_ref": owner_bound_target_ref,
        }

    def _revalidate_company_public_web_action_target(
        self,
        *,
        operation_run: Mapping[str, Any],
        action: Mapping[str, Any],
    ) -> dict[str, Any]:
        if str(action.get("action_type") or "").strip() not in COMPANY_PUBLIC_WEB_ACTION_TYPES:
            return {"status": "ready"}
        raw_target_ref = action.get("target_ref")
        raw_input_payload = action.get("input")
        if not isinstance(raw_target_ref, Mapping) or not isinstance(raw_input_payload, Mapping):
            return {"status": "invalid", "reason": COMPANY_PUBLIC_WEB_TARGET_INVALID}
        operation_workspace_id = str(operation_run.get("workspace_id") or "default").strip() or "default"
        try:
            target = self._company_public_web_target_binder.revalidate_snapshot(
                target_ref=raw_target_ref,
                operation_workspace_id=operation_workspace_id,
            )
        except ActionTargetBindingError as exc:
            return {"status": "invalid", "reason": exc.reason}
        input_payload = dict(raw_input_payload)
        target_company = str(input_payload.get("target_company") or "").strip()
        expected_company_key = resolve_company_alias_key(target_company)
        if (
            not target_company
            or not expected_company_key
            or expected_company_key != str(target.get("company_key") or "").strip()
            or str(action.get("workspace_id") or "default").strip() != operation_workspace_id
            or str(input_payload.get("collection_mode") or "").strip() != "seed_url_only"
        ):
            return {"status": "invalid", "reason": COMPANY_PUBLIC_WEB_TARGET_INVALID}
        return {
            "status": "ready",
            "company_public_web_target": target,
            "company_public_web_input": input_payload,
        }

    def _preflight_company_public_web_action_control(
        self,
        *,
        action: Mapping[str, Any],
        operation_run: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if str(action.get("action_type") or "").strip() not in COMPANY_PUBLIC_WEB_ACTION_TYPES:
            return {"status": "ready"}
        try:
            self.operation_runtime_writer.validate_persisted_action_request(
                action=action,
                operation_run=operation_run,
            )
        except OperationRuntimeStateConflict as exc:
            return {"status": "conflict", "reason": exc.reason}
        workspace_record: Mapping[str, Any] = operation_run or {
            "workspace_id": str(action.get("workspace_id") or "default").strip() or "default"
        }
        return self._revalidate_company_public_web_action_target(
            operation_run=workspace_record,
            action=action,
        )

    @staticmethod
    def _schema_defined_company_public_web_command_plan(
        *,
        operation_run: Mapping[str, Any],
        action: Mapping[str, Any],
        owner: str,
    ) -> dict[str, Any]:
        raw_input_payload = action.get("input")
        raw_target_ref = action.get("target_ref")
        if not isinstance(raw_input_payload, Mapping) or not isinstance(raw_target_ref, Mapping):
            return {
                "status": "invalid",
                "reason": "company_public_web_action_request_invalid",
                "command_type": COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
            }
        input_payload = dict(raw_input_payload)
        target_ref = dict(raw_target_ref)
        target_company = str(input_payload.get("target_company") or "").strip()
        workspace_id = str(operation_run.get("workspace_id") or "default").strip() or "default"
        company_key = str(target_ref.get("company_key") or "").strip()
        source_families = list(input_payload.get("source_families") or [])
        seed_urls = list(input_payload.get("seed_urls") or [])
        max_assets = input_payload.get("max_assets")
        force_refresh = input_payload.get("force_refresh")
        collection_mode = str(input_payload.get("collection_mode") or "").strip()
        refresh_nonce = str(input_payload.get("refresh_nonce") or "").strip()
        if (
            set(target_ref) != {"workspace_id", "company_key"}
            or str(target_ref.get("workspace_id") or "").strip() != workspace_id
            or not target_company
            or not company_key
            or not source_families
            or not seed_urls
            or isinstance(max_assets, bool)
            or not isinstance(max_assets, int)
            or not isinstance(force_refresh, bool)
            or collection_mode != "seed_url_only"
            or (force_refresh and not refresh_nonce)
            or (not force_refresh and "refresh_nonce" in input_payload)
        ):
            return {
                "status": "invalid",
                "reason": "company_public_web_action_request_invalid",
                "command_type": COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
            }
        operation_run_id = str(operation_run.get("operation_run_id") or "").strip()
        workflow_run_id = f"wf_company_public_web_{hashlib.sha1(operation_run_id.encode('utf-8')).hexdigest()[:24]}"
        command_payload: dict[str, Any] = {
            "company_public_web_target": target_ref,
            "target_company": target_company,
            "company_key": company_key,
            "workspace_id": workspace_id,
            "source_families": source_families,
            "seed_urls": seed_urls,
            "max_assets": max_assets,
            "force_refresh": force_refresh,
            "collection_mode": "seed_url_only",
            "operation_run_id": operation_run_id,
            "action_id": str(action.get("action_id") or "").strip(),
            "requested_by": COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            "source": "operation_run_dispatch",
            "migration_phase": "W11_company_public_web_refresh_command",
            "company_asset_write_owner": COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            "normal_path_executes_api_refresh_inline": False,
            "produced_entity_counts": {"company_public_web_run": 1},
        }
        if force_refresh:
            command_payload["refresh_nonce"] = refresh_nonce
        return {
            "status": "ok",
            "command_type": COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
            "owner": owner,
            "workflow_run_id": workflow_run_id,
            "command_payload": command_payload,
            "max_attempts": 3,
            "retry_policy": {"kind": "company_public_web_refresh", "retry_delay_seconds": 30},
        }

    def _company_public_web_phase_command_contract(
        self,
        *,
        parent_command: dict[str, Any],
        command_type: str,
        command_payload: dict[str, Any],
        idempotency_suffix: str,
        source: str = "company_public_web_refresh_owner",
    ) -> dict[str, Any]:
        # Canonical implementation lives in the shared progressed-child
        # contract so planning, UoW completion, and replay all reconstruct the
        # same expected child/event through the registered pure builder.
        return build_company_public_web_phase_plan(
            parent_command=parent_command,
            command_type=command_type,
            command_payload=command_payload,
            idempotency_suffix=idempotency_suffix,
            source=source,
        )

    def _plan_company_public_web_phase_command(
        self,
        *,
        parent_command: dict[str, Any],
        command_type: str,
        command_payload: dict[str, Any],
        idempotency_suffix: str,
        source: str = "company_public_web_refresh_owner",
    ) -> dict[str, Any]:
        contract = self._company_public_web_phase_command_contract(
            parent_command=parent_command,
            command_type=command_type,
            command_payload=command_payload,
            idempotency_suffix=idempotency_suffix,
            source=source,
        )
        plan_event = dict(contract.get("plan_event") or {})
        child_command = dict(contract.get("child_command") or {})
        if not plan_event or not child_command:
            return {}
        try:
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=str(plan_event.get("workflow_run_id") or "").strip(),
                operation_id=str(plan_event.get("operation_id") or "").strip(),
                command_id=str(plan_event.get("command_id") or "").strip(),
                event_family=str(plan_event.get("event_family") or "").strip(),
                event_type=str(plan_event.get("event_type") or "").strip(),
                idempotency_key=str(plan_event.get("idempotency_key") or "").strip(),
                actor=str(plan_event.get("actor") or "").strip(),
                source=str(plan_event.get("source") or "").strip(),
                payload=dict(plan_event.get("payload") or {}),
                artifact_refs=list(plan_event.get("artifact_refs") or []),
            )
        except Exception:
            return {}
        return self._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=str(child_command.get("workflow_run_id") or "").strip(),
            idempotency_key=str(child_command.get("idempotency_key") or "").strip(),
        )

    def _company_public_web_command_causality_matches(
        self,
        *,
        command_record: Mapping[str, Any],
        command_payload: Mapping[str, Any],
        workflow_type: str,
        actor: str,
        source: str,
        source_command_id: str,
        expected_idempotency_key: str,
        max_attempts: int,
        retry_policy: Mapping[str, Any],
        event_payload_causality: Mapping[str, Any] | None = None,
        parent_command_id: str = "",
        causal_group_id: str = "",
        expected_terminal_downstream_command_ids: list[str] | None = None,
    ) -> bool:
        """Rebuild and compare the immutable plan event plus canonical causal envelope."""

        command_id = str(command_record.get("command_id") or "").strip()
        workflow_run_id = str(command_record.get("workflow_run_id") or "").strip()
        operation_id = str(command_record.get("operation_id") or "").strip()
        command_type = str(command_record.get("command_type") or "").strip()
        owner = str(command_record.get("owner") or "").strip()
        idempotency_key = str(command_record.get("idempotency_key") or "").strip()
        normalized_expected_idempotency_key = str(expected_idempotency_key or "").strip()
        if (
            not command_id
            or not workflow_run_id
            or not operation_id
            or not command_type
            or owner != COMPANY_PUBLIC_WEB_REFRESH_OWNER
            or not idempotency_key
            or idempotency_key != normalized_expected_idempotency_key
            or command_id != command_id_for(workflow_run_id, idempotency_key)
        ):
            return False
        event_command_payload = dict(command_payload)
        if event_payload_causality is not None:
            event_command_payload["causality"] = dict(event_payload_causality)
        expected_event_payload: dict[str, Any] = {
            "workflow_type": str(workflow_type or "").strip(),
            "stage_key": default_stage_id_for_command_type(command_type),
            "command_type": command_type,
            "idempotency_key": idempotency_key,
            "payload": event_command_payload,
            "max_attempts": int(max_attempts),
            "retry_policy": dict(retry_policy),
        }
        if parent_command_id:
            expected_event_payload["parent_command_id"] = str(parent_command_id).strip()
            expected_event_payload["causal_group_id"] = str(causal_group_id).strip()
        # Registered progressed-child pairs carry the immutable contract pin
        # minted by the shared builder; expected values must include it.
        expected_contract_pin: dict[str, str] = {}
        if parent_command_id:
            resolved_child_contract = progressed_child_completion_contract_for_child(command_type)
            if resolved_child_contract is not None:
                _child_contract_name, child_contract = resolved_child_contract
                if str(actor or "").strip() == str(child_contract.get("child_plan_event_actor") or "") and str(
                    source or ""
                ).strip() == str(child_contract.get("child_plan_event_source") or ""):
                    expected_contract_pin = progressed_child_contract_pin(_child_contract_name)
        if expected_contract_pin:
            expected_event_payload["progressed_child_contract"] = expected_contract_pin
        source_events = [
            dict(event or {})
            for event in self.store.repos.workflow_runtime.list_workflow_events(workflow_run_id, limit=0)
            if str(dict(event or {}).get("idempotency_key") or "").strip() == f"{idempotency_key}:plan"
        ]
        if len(source_events) != 1:
            return False
        source_event = self.store.repos.workflow_runtime.get_persisted_workflow_event_contract(
            str(source_events[0].get("event_id") or "").strip()
        )
        if not source_event or not bool(source_event.get("persisted_json_contract_valid")):
            return False
        source_sequence = max(0, int(source_event.get("sequence_number") or 0))
        expected_source_event_id = (
            "evt_"
            + hashlib.sha1(
                f"{workflow_run_id}:{source_sequence}:{normalized_expected_idempotency_key}:plan".encode("utf-8")
            ).hexdigest()[:24]
        )
        if not (
            source_sequence > 0
            and str(source_event.get("event_id") or "").strip() == expected_source_event_id
            and str(source_event.get("workflow_run_id") or "").strip() == workflow_run_id
            and str(source_event.get("operation_id") or "").strip() == operation_id
            and str(source_event.get("command_id") or "").strip() == str(source_command_id or "").strip()
            and str(source_event.get("activity_attempt_id") or "").strip() == ""
            and str(source_event.get("event_family") or "").strip() == "workflow_event"
            and str(source_event.get("event_type") or "").strip() == "CommandPlanRequested"
            and str(source_event.get("actor") or "").strip() == str(actor or "").strip()
            and str(source_event.get("source") or "").strip() == str(source or "").strip()
            and json_contract_equal(dict(source_event.get("payload") or {}), expected_event_payload)
            and json_contract_equal(list(source_event.get("artifact_refs") or []), [])
            and str(source_event.get("schema_version") or "").strip() == "workflow_event_v1"
        ):
            return False
        expected_causality = command_causality_for(
            workflow_run_id=workflow_run_id,
            operation_id=operation_id,
            stage_id=default_stage_id_for_command_type(command_type),
            command_type=command_type,
            owner=owner,
            idempotency_key=idempotency_key,
            source_event=source_event,
            command_payload=event_command_payload,
            artifact_refs=(),
        ).to_payload()
        if expected_contract_pin:
            expected_causality["progressed_child_contract"] = expected_contract_pin
        expected_stored_payload = {**event_command_payload, "causality": expected_causality}
        physical_text_fields = {
            "schema_version": "workflow_command_v1",
            "stage_id": str(expected_causality.get("stage_id") or "").strip(),
            "causal_group_id": str(expected_causality.get("causal_group_id") or "").strip(),
            "parent_command_id": str(expected_causality.get("parent_command_id") or "").strip(),
            "source_event_id": str(source_event.get("event_id") or "").strip(),
            "source_event_type": "CommandPlanRequested",
            "no_op_reason": str(expected_causality.get("no_op_reason") or "").strip(),
            "readiness_effect": str(expected_causality.get("readiness_effect") or "").strip(),
            "causality_schema_version": str(expected_causality.get("schema_version") or "").strip(),
        }
        expected_downstream_command_ids = (
            [str(item or "").strip() for item in expected_terminal_downstream_command_ids]
            if expected_terminal_downstream_command_ids is not None
            else list(expected_causality.get("downstream_command_ids") or [])
        )
        return bool(
            json_contract_equal(dict(command_record.get("payload") or {}), expected_stored_payload)
            and all(
                str(command_record.get(field) or "").strip() == expected
                for field, expected in physical_text_fields.items()
            )
            and json_contract_equal(list(command_record.get("artifact_refs") or []), [])
            and json_contract_equal(
                list(command_record.get("input_artifact_refs") or []),
                list(expected_causality.get("input_artifact_refs") or []),
            )
            and json_contract_equal(
                list(command_record.get("output_artifact_refs") or []),
                list(expected_causality.get("output_artifact_refs") or []),
            )
            and json_contract_equal(
                dict(command_record.get("produced_entity_counts") or {}),
                dict(expected_causality.get("produced_entity_counts") or {}),
            )
            and json_contract_equal(
                list(command_record.get("downstream_command_ids") or []),
                expected_downstream_command_ids,
            )
            and int(command_record.get("max_attempts") or 0) == int(max_attempts)
            and json_contract_equal(dict(command_record.get("retry_policy") or {}), dict(retry_policy))
        )

    @staticmethod
    def _company_public_web_physical_claim_pin(command: Mapping[str, Any]) -> dict[str, Any]:
        raw_attempt = command.get("attempt")
        try:
            attempt = 0 if isinstance(raw_attempt, bool) else int(raw_attempt or 0)
        except (TypeError, ValueError):
            attempt = 0
        command_id = str(command.get("command_id") or "").strip()
        lease_owner = str(command.get("lease_owner") or "").strip()
        lease_expires_at = str(command.get("lease_expires_at") or "").strip()
        if not command_id or not lease_owner or not lease_expires_at or attempt <= 0:
            return {}
        return {
            "command_id": command_id,
            "attempt": attempt,
            "lease_owner": lease_owner,
            "lease_expires_at": lease_expires_at,
        }

    @staticmethod
    def _company_public_web_locked_command_identity(command: Mapping[str, Any]) -> dict[str, Any]:
        command_payload = dict(command or {})
        return {
            "command_id": str(command_payload.get("command_id") or "").strip(),
            "workflow_run_id": str(command_payload.get("workflow_run_id") or "").strip(),
            "operation_id": str(command_payload.get("operation_id") or "").strip(),
            "command_type": str(command_payload.get("command_type") or "").strip(),
            "owner": str(command_payload.get("owner") or "").strip(),
            "stage_id": str(command_payload.get("stage_id") or "").strip(),
            "causal_group_id": str(command_payload.get("causal_group_id") or "").strip(),
            "parent_command_id": str(command_payload.get("parent_command_id") or "").strip(),
            "source_event_id": str(command_payload.get("source_event_id") or "").strip(),
            "source_event_type": str(command_payload.get("source_event_type") or "").strip(),
            "input_artifact_refs": list(command_payload.get("input_artifact_refs") or []),
            "output_artifact_refs": list(command_payload.get("output_artifact_refs") or []),
            "produced_entity_counts": dict(command_payload.get("produced_entity_counts") or {}),
            "no_op_reason": str(command_payload.get("no_op_reason") or "").strip(),
            "readiness_effect": str(command_payload.get("readiness_effect") or "").strip(),
            "downstream_command_ids": list(command_payload.get("downstream_command_ids") or []),
            "causality_schema_version": str(command_payload.get("causality_schema_version") or "").strip(),
            "idempotency_key": str(command_payload.get("idempotency_key") or "").strip(),
            "payload": dict(command_payload.get("payload") or {}),
            "artifact_refs": list(command_payload.get("artifact_refs") or []),
            "not_before_at": str(command_payload.get("not_before_at") or "").strip(),
            "max_attempts": max(0, int(command_payload.get("max_attempts") or 0)),
            "retry_policy": dict(command_payload.get("retry_policy") or {}),
            "result": dict(command_payload.get("result") or {}),
            "schema_version": str(command_payload.get("schema_version") or "").strip(),
        }

    def _validate_company_public_web_source_completion_bundle(
        self,
        *,
        source_command: Mapping[str, Any],
        completion_contract: Mapping[str, Any],
        entity_delta_specs: list[dict[str, Any]],
        terminal_result: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Validate a committed D1m source bundle after an acknowledgement loss."""

        expected_source = self._company_public_web_locked_command_identity(source_command)
        command_id = str(expected_source.get("command_id") or "").strip()
        current = self.store.repos.workflow_runtime.get_persisted_workflow_command_contract(command_id)
        if not current or not bool(current.get("persisted_json_contract_valid")):
            return {"status": "invalid", "reason": "company_public_web_source_terminal_command_invalid"}
        current_identity = self._company_public_web_locked_command_identity(current)
        immutable_fields = set(expected_source) - {"downstream_command_ids", "result"}
        if (
            str(current.get("status") or "").strip() != "succeeded"
            or int(current.get("attempt") or 0) != int(source_command.get("attempt") or 0)
            or any(
                not json_contract_equal(current_identity.get(field), expected_source.get(field))
                for field in immutable_fields
            )
            or not json_contract_equal(dict(current.get("result") or {}), dict(terminal_result or {}))
        ):
            return {"status": "invalid", "reason": "company_public_web_source_terminal_result_mismatch"}
        plan_event_spec = dict(completion_contract.get("plan_event") or {})
        child_spec = dict(completion_contract.get("child_command") or {})
        expected_child_id = str(child_spec.get("command_id") or "").strip()
        if list(current.get("downstream_command_ids") or []) != [expected_child_id]:
            return {"status": "invalid", "reason": "company_public_web_source_terminal_edge_mismatch"}
        # The shared versioned progressed-child contract also owns Company
        # Public Web completion replay: the plan event must follow the locked
        # parent source event in sequence, carry the deterministic id and
        # registered idempotency/actor/source pins, and equal the complete
        # expected payload; the child must equal the reconstructed expected
        # identity exactly.  No second hand-maintained verifier remains.
        parent_source_event_id = str(current.get("source_event_id") or "").strip()
        parent_source_event = (
            self.store.repos.workflow_runtime.get_persisted_workflow_event_contract(parent_source_event_id)
            if parent_source_event_id
            else {}
        )
        parent_source_sequence = max(0, int(dict(parent_source_event or {}).get("sequence_number") or 0))
        if (
            not parent_source_event
            or not bool(dict(parent_source_event or {}).get("persisted_json_contract_valid"))
            or parent_source_sequence <= 0
        ):
            return {"status": "invalid", "reason": "company_public_web_source_parent_source_event_invalid"}
        completion = progressed_child_completion_contract("company_public_web_source")
        if completion is None:
            return {"status": "invalid", "reason": "company_public_web_source_completion_contract_unregistered"}
        candidate_events = [
            dict(item or {})
            for item in self.store.repos.workflow_runtime.list_workflow_events(
                str(current.get("workflow_run_id") or "").strip(), limit=0
            )
            if str(dict(item or {}).get("idempotency_key") or "").strip()
            == str(plan_event_spec.get("idempotency_key") or "").strip()
        ]
        if len(candidate_events) != 1:
            return {"status": "invalid", "reason": "company_public_web_source_plan_event_ambiguous"}
        event = self.store.repos.workflow_runtime.get_persisted_workflow_event_contract(
            str(candidate_events[0].get("event_id") or "").strip()
        )
        expected_child_identity = (
            canonical_progressed_child_identity(
                expected_progressed_child_row(
                    contract=completion,
                    parent_command_id=command_id,
                    workflow_run_id=str(current.get("workflow_run_id") or "").strip(),
                    operation_id=str(current.get("operation_id") or "").strip(),
                    child_command=child_spec,
                    child_causality={
                        **dict(completion_contract.get("child_causality") or {}),
                        "source_event_id": str(event.get("event_id") or "").strip(),
                        "source_event_type": "CommandPlanRequested",
                    },
                )
            )
            if event and bool(event.get("persisted_json_contract_valid"))
            else None
        )
        event_violation = (
            progressed_child_plan_event_violation(
                contract=completion,
                parent_command_id=command_id,
                parent_source_sequence=parent_source_sequence,
                child_identity=expected_child_identity,
                event={
                    **event,
                    "payload": dict(event.get("payload") or {}),
                    "artifact_refs": list(event.get("artifact_refs") or []),
                },
                expected_payload=dict(plan_event_spec.get("payload") or {}),
            )
            if expected_child_identity is not None
            else "child_identity_invalid"
        )
        if event_violation:
            return {"status": "invalid", "reason": "company_public_web_source_plan_event_mismatch"}
        child = self.store.repos.workflow_runtime.get_persisted_workflow_command_contract(expected_child_id)
        persisted_child_identity = (
            canonical_progressed_child_identity(child)
            if child and bool(child.get("persisted_json_contract_valid"))
            else None
        )
        if (
            not child
            or not bool(child.get("persisted_json_contract_valid"))
            or str(child.get("command_id") or "").strip() != expected_child_id
            or persisted_child_identity is None
            or not json_contract_equal(persisted_child_identity, expected_child_identity)
        ):
            return {"status": "invalid", "reason": "company_public_web_source_materialize_child_mismatch"}
        expected_delta_ids = [str(item.get("delta_id") or "").strip() for item in entity_delta_specs]
        expected_delta_workspaces = {
            str(item.get("workspace_id") or "").strip() for item in entity_delta_specs if isinstance(item, Mapping)
        }
        if (
            any(not delta_id for delta_id in expected_delta_ids)
            or len(set(expected_delta_ids)) != len(expected_delta_ids)
            or len(expected_delta_workspaces) != 1
            or not next(iter(expected_delta_workspaces), "")
        ):
            return {"status": "invalid", "reason": "company_public_web_source_entity_delta_ambiguous"}
        expected_delta_id_set = set(expected_delta_ids)
        expected_delta_workspace = next(iter(expected_delta_workspaces))
        persisted_deltas = [
            dict(item or {})
            for item in self.store.repos.workflow_runtime.list_entity_deltas(
                workspace_id=expected_delta_workspace,
                command_id=command_id,
                limit=0,
            )
            if str(dict(item or {}).get("delta_id") or "").strip() in expected_delta_id_set
        ]
        if len(persisted_deltas) != len(entity_delta_specs):
            return {"status": "invalid", "reason": "company_public_web_source_entity_delta_ambiguous"}
        deltas_by_id = {str(item.get("delta_id") or "").strip(): dict(item or {}) for item in persisted_deltas}
        for spec in entity_delta_specs:
            delta = deltas_by_id.get(str(spec.get("delta_id") or "").strip())
            if not delta or any(
                not json_contract_equal(delta.get(field), spec.get(field))
                for field in (
                    "workspace_id",
                    "workflow_run_id",
                    "operation_run_id",
                    "command_id",
                    "activity_run_id",
                    "attempt_id",
                    "acquisition_run_id",
                    "entity_type",
                    "entity_key",
                    "delta_kind",
                    "status",
                    "reason",
                    "source_ref",
                    "entity_payload",
                    "projection_effect",
                    "artifact_refs",
                    "idempotency_key",
                    "metadata",
                )
            ):
                return {"status": "invalid", "reason": "company_public_web_source_entity_delta_mismatch"}
        return {
            "status": "ready",
            "workflow_command": current,
            "child_command": child,
            "event": event,
            "entity_deltas": persisted_deltas,
        }

    def _mark_company_public_web_command_succeeded_for_exact_claim(
        self,
        command: Mapping[str, Any],
        *,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        claim = self._company_public_web_physical_claim_pin(command)
        if not claim:
            return {}
        return self.store.mark_workflow_command_succeeded(
            str(claim["command_id"]),
            result=dict(result or {}),
            expected_attempt=int(claim["attempt"]),
            expected_lease_owner=str(claim["lease_owner"]),
            expected_lease_expires_at=str(claim["lease_expires_at"]),
        )

    def _mark_company_public_web_command_failed_for_exact_claim(
        self,
        command: Mapping[str, Any],
        *,
        error_text: str,
        retryable: bool,
        retry_delay_seconds: int,
    ) -> dict[str, Any]:
        claim = self._company_public_web_physical_claim_pin(command)
        if not claim:
            return {}
        return self.store.mark_workflow_command_failed(
            str(claim["command_id"]),
            error_text=str(error_text or "").strip(),
            retryable=bool(retryable),
            retry_delay_seconds=max(0, int(retry_delay_seconds or 0)),
            expected_attempt=int(claim["attempt"]),
            expected_lease_owner=str(claim["lease_owner"]),
            expected_lease_expires_at=str(claim["lease_expires_at"]),
        )

    def _company_public_web_command_matches_physical_claim(
        self,
        command: Mapping[str, Any],
        expected_claim: Mapping[str, Any],
    ) -> bool:
        expected = self._company_public_web_physical_claim_pin(expected_claim)
        current = self._company_public_web_physical_claim_pin(command)
        return bool(
            expected
            and current == expected
            and str(command.get("status") or "").strip() in {"claimed", "running"}
            and self._workflow_command_lease_active(dict(command))
        )

    def _revalidate_company_public_web_command_target(
        self,
        command: Mapping[str, Any],
        *,
        expected_physical_claim: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        command_id = str(command.get("command_id") or "").strip()
        command_record = self.store.repos.workflow_runtime.get_persisted_workflow_command_contract(command_id)
        if expected_physical_claim is not None and (
            not command_record
            or not self._company_public_web_command_matches_physical_claim(
                command_record,
                expected_physical_claim,
            )
        ):
            return {
                "status": "owner_lost",
                "reason": "company_public_web_command_claim_not_current",
                "workflow_command": dict(command_record or {}),
            }
        if not command_record or not bool(command_record.get("persisted_json_contract_valid")):
            return {"status": "invalid", "reason": "company_public_web_command_persisted_json_invalid"}
        raw_payload = command_record.get("payload")
        if not isinstance(raw_payload, Mapping):
            return {"status": "invalid", "reason": "company_public_web_command_payload_invalid"}
        payload = dict(raw_payload)
        command_type = str(command_record.get("command_type") or "").strip()
        if (
            command_type
            not in {
                COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
                COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
                COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
            }
            or str(command_record.get("owner") or "").strip() != COMPANY_PUBLIC_WEB_REFRESH_OWNER
        ):
            return {"status": "invalid", "reason": "company_public_web_command_owner_mismatch"}
        operation_run_id = str(command_record.get("operation_id") or "").strip()
        if not operation_run_id or str(payload.get("operation_id") or "").strip() != operation_run_id:
            return {"status": "invalid", "reason": "company_public_web_command_operation_mismatch"}
        operation_run = self.store.repos.workflow_runtime.get_operation(operation_run_id)
        if not operation_run:
            return {"status": "invalid", "reason": "company_public_web_command_operation_missing"}
        action_id = str(operation_run.get("action_id") or "").strip()
        action = self.store.repos.workflow_runtime.get_action(action_id) if action_id else {}
        if (
            not action
            or str(action.get("action_type") or "").strip() != ACTION_REFRESH_COMPANY_PUBLIC_WEB
            or str(payload.get("action_id") or "").strip() != action_id
            or str(payload.get("operation_run_id") or "").strip() != operation_run_id
        ):
            return {"status": "invalid", "reason": "company_public_web_command_action_mismatch"}
        try:
            spec = self.operation_runtime_writer.validate_persisted_action_request(
                action=action,
                operation_run=operation_run,
            )
        except OperationRuntimeStateConflict:
            return {"status": "invalid", "reason": "company_public_web_action_request_conflict"}
        if (
            str(spec.default_workflow_command_type or "").strip() != COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE
            or COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE not in spec.allowed_workflow_command_types
            or str(spec.owner_module or "").strip() != COMPANY_PUBLIC_WEB_REFRESH_OWNER
            or str(action.get("operation_type") or "").strip() != str(spec.operation_type or "").strip()
            or str(operation_run.get("operation_type") or "").strip() != str(spec.operation_type or "").strip()
            or str(operation_run.get("status") or "").strip() in OPERATION_RUN_TERMINAL_STATUSES
            or str(action.get("status") or "").strip() in OPERATION_ACTION_TERMINAL_STATUSES
            or str(action.get("approval_status") or "").strip() != "approved"
        ):
            return {"status": "invalid", "reason": "company_public_web_command_contract_mismatch"}
        target_preflight = self._revalidate_company_public_web_action_target(
            operation_run=operation_run,
            action=action,
        )
        if str(target_preflight.get("status") or "") != "ready":
            return target_preflight
        target = dict(target_preflight.get("company_public_web_target") or {})
        action_input = dict(target_preflight.get("company_public_web_input") or {})
        raw_bound_target = payload.get("company_public_web_target")
        if (
            not isinstance(raw_bound_target, Mapping)
            or not json_contract_equal(dict(raw_bound_target), target)
            or str(payload.get("workspace_id") or "").strip() != str(target.get("workspace_id") or "").strip()
            or str(payload.get("company_key") or "").strip() != str(target.get("company_key") or "").strip()
            or str(payload.get("target_company") or "").strip() != str(action_input.get("target_company") or "").strip()
        ):
            return {"status": "invalid", "reason": "company_public_web_bound_target_mismatch"}
        root_plan = self._schema_defined_company_public_web_command_plan(
            operation_run=operation_run,
            action=action,
            owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
        )
        if str(root_plan.get("status") or "") != "ok":
            return {"status": "invalid", "reason": "company_public_web_command_payload_invalid"}
        expected_workflow_run_id = str(root_plan.get("workflow_run_id") or "").strip()
        if (
            str(command_record.get("workflow_run_id") or "").strip() != expected_workflow_run_id
            or str(operation_run.get("workspace_id") or "").strip() != str(target.get("workspace_id") or "").strip()
        ):
            return {"status": "invalid", "reason": "company_public_web_command_workflow_mismatch"}
        if command_type == COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE:
            root_command_payload = dict(root_plan.get("command_payload") or {})
            expected_payload = {
                **root_command_payload,
                "operation_id": operation_run_id,
            }
            root_payload_hash = hashlib.sha1(
                json.dumps(root_command_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest()[:24]
            expected_root_idempotency_key = (
                f"{COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE}:operation:{operation_run_id}:{root_payload_hash}"
            )
            workflow_ref = {
                "workflow_run_id": expected_workflow_run_id,
                "command_id": command_id,
                "command_type": COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
                "owner": COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            }
            causal_envelope_matches = self._company_public_web_command_causality_matches(
                command_record=command_record,
                command_payload=expected_payload,
                workflow_type="agent_callable_workflow_command",
                actor="operation_workflow_command_planner",
                source="operation_run_dispatch",
                source_command_id="",
                expected_idempotency_key=expected_root_idempotency_key,
                max_attempts=int(root_plan.get("max_attempts") or 0),
                retry_policy=dict(root_plan.get("retry_policy") or {}),
                event_payload_causality={
                    "operation_id": operation_run_id,
                    "action_id": action_id,
                    "action_type": ACTION_REFRESH_COMPANY_PUBLIC_WEB,
                    "migration_phase": "W11_agent_callable_workflow_command",
                },
            )
            if not causal_envelope_matches or (
                str(command_record.get("status") or "").strip() != "succeeded"
                and not json_contract_equal(dict(operation_run.get("workflow_ref") or {}), workflow_ref)
            ):
                return {"status": "invalid", "reason": "company_public_web_root_command_payload_mismatch"}
            return {
                "status": "ready",
                "workflow_command": command_record,
                "operation_run": operation_run,
                "action": action,
                "company_public_web_target": target,
                "company_public_web_input": action_input,
                "company_public_web_root_plan": root_plan,
            }

        parent_command_id = str(command_record.get("parent_command_id") or "").strip()
        if not parent_command_id or str(payload.get("parent_command_id") or "").strip() != parent_command_id:
            return {"status": "invalid", "reason": "company_public_web_phase_parent_mismatch"}
        parent_command = self.store.repos.workflow_runtime.get_persisted_workflow_command_contract(parent_command_id)
        if not parent_command or not bool(parent_command.get("persisted_json_contract_valid")):
            return {"status": "invalid", "reason": "company_public_web_phase_parent_missing"}
        expected_parent_type = (
            COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE
            if command_type == COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE
            else COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE
        )
        if (
            str(parent_command.get("command_type") or "").strip() != expected_parent_type
            or str(parent_command.get("status") or "").strip() != "succeeded"
            or str(parent_command.get("operation_id") or "").strip() != operation_run_id
            or str(parent_command.get("workflow_run_id") or "").strip() != expected_workflow_run_id
        ):
            return {"status": "invalid", "reason": "company_public_web_phase_parent_mismatch"}
        parent_result = dict(parent_command.get("result") or {})
        if command_id not in [
            str(item or "").strip() for item in list(parent_result.get("downstream_command_ids") or [])
        ]:
            return {"status": "invalid", "reason": "company_public_web_phase_causality_mismatch"}
        parent_preflight = self._revalidate_company_public_web_command_target(parent_command)
        if str(parent_preflight.get("status") or "") != "ready":
            return parent_preflight
        if command_type == COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE:
            expected_payload = {
                **dict(root_plan.get("command_payload") or {}),
                "operation_id": operation_run_id,
                "phase_command_type": COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
                "parent_command_id": parent_command_id,
                "causal_group_id": parent_command_id,
                "source": "company_public_web_refresh_owner",
                "migration_phase": "W11_company_public_web_phase_command",
            }
            parent_payload_json = json.dumps(
                dict(parent_command.get("payload") or {}),
                ensure_ascii=False,
                sort_keys=True,
            )
            expected_source_idempotency_key = (
                f"{COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE}:{parent_command_id}:"
                f"{hashlib.sha1(parent_payload_json.encode('utf-8')).hexdigest()[:24]}"
            )
            expected_terminal_downstream_ids: list[str] | None = None
            if str(command_record.get("status") or "").strip() == "succeeded":
                terminal_run = dict(dict(command_record.get("result") or {}).get("run") or {})
                terminal_run_id = str(terminal_run.get("run_id") or "").strip()
                terminal_snapshot = company_public_web_materialization_snapshot_identity(terminal_run)
                terminal_snapshot_schema = str(
                    terminal_snapshot.get(COMPANY_PUBLIC_WEB_MATERIALIZATION_SNAPSHOT_SCHEMA_KEY) or ""
                ).strip()
                terminal_snapshot_sha256 = str(
                    terminal_snapshot.get(COMPANY_PUBLIC_WEB_MATERIALIZATION_SNAPSHOT_DIGEST_KEY) or ""
                ).strip()
                if not terminal_run_id or not terminal_snapshot:
                    return {"status": "invalid", "reason": "company_public_web_source_terminal_result_invalid"}
                terminal_materialize_suffix = f"{terminal_run_id}:{terminal_snapshot_schema}:{terminal_snapshot_sha256}"
                terminal_materialize_idempotency_key = (
                    f"{COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE}:{command_id}:"
                    f"{hashlib.sha1(terminal_materialize_suffix.encode('utf-8')).hexdigest()[:24]}"
                )
                expected_terminal_downstream_ids = [
                    command_id_for(expected_workflow_run_id, terminal_materialize_idempotency_key)
                ]
            if not self._company_public_web_command_causality_matches(
                command_record=command_record,
                command_payload=expected_payload,
                workflow_type="company_public_web_refresh",
                actor="company_public_web_phase_planner",
                source="company_public_web_refresh_owner",
                source_command_id=parent_command_id,
                expected_idempotency_key=expected_source_idempotency_key,
                max_attempts=3,
                retry_policy={"kind": "company_public_web_phase", "retry_delay_seconds": 30},
                parent_command_id=parent_command_id,
                causal_group_id=parent_command_id,
                expected_terminal_downstream_command_ids=expected_terminal_downstream_ids,
            ):
                return {"status": "invalid", "reason": "company_public_web_source_command_payload_mismatch"}
        else:
            run_id = str(payload.get("run_id") or "").strip()
            source_run = dict(parent_result.get("run") or {})
            run = self.store.get_company_public_web_asset_run(run_id=run_id) if run_id else None
            source_snapshot_identity = company_public_web_materialization_snapshot_identity(source_run)
            current_snapshot_identity = company_public_web_materialization_snapshot_identity(dict(run or {}))
            snapshot_schema_version = str(
                source_snapshot_identity.get(COMPANY_PUBLIC_WEB_MATERIALIZATION_SNAPSHOT_SCHEMA_KEY) or ""
            ).strip()
            snapshot_sha256 = str(
                source_snapshot_identity.get(COMPANY_PUBLIC_WEB_MATERIALIZATION_SNAPSHOT_DIGEST_KEY) or ""
            ).strip()
            expected_materialize_payload = {
                "run_id": run_id,
                "materialization_snapshot_schema_version": snapshot_schema_version,
                "materialization_snapshot_sha256": snapshot_sha256,
                "company_public_web_target": target,
                "target_company": str(action_input.get("target_company") or "").strip(),
                "company_key": str(target.get("company_key") or "").strip(),
                "workspace_id": str(target.get("workspace_id") or "").strip(),
                "max_assets": int(action_input.get("max_assets") or 50),
                "collection_mode": "seed_url_only",
                "operation_run_id": operation_run_id,
                "action_id": action_id,
                "operation_id": operation_run_id,
                "phase_command_type": COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
                "parent_command_id": parent_command_id,
                "causal_group_id": parent_command_id,
                "source": "company_public_web_source_collect_owner",
                "migration_phase": "W11_company_public_web_phase_command",
            }
            materialize_identity_suffix = f"{run_id}:{snapshot_schema_version}:{snapshot_sha256}"
            expected_materialize_idempotency_key = (
                f"{COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE}:{parent_command_id}:"
                f"{hashlib.sha1(materialize_identity_suffix.encode('utf-8')).hexdigest()[:24]}"
            )
            if (
                not run_id
                or not run
                or not source_snapshot_identity
                or current_snapshot_identity != source_snapshot_identity
                or str(run.get("status") or "").strip().lower() != "completed"
                or str(source_run.get("run_id") or "").strip() != run_id
                or str(source_run.get("status") or "").strip().lower() != "completed"
                or str(source_run.get("target_company") or "").strip()
                != str(action_input.get("target_company") or "").strip()
                or str(source_run.get("company_key") or "").strip() != str(target.get("company_key") or "").strip()
                or str(run.get("target_company") or "").strip() != str(action_input.get("target_company") or "").strip()
                or str(run.get("company_key") or "").strip() != str(target.get("company_key") or "").strip()
                or not self._company_public_web_command_causality_matches(
                    command_record=command_record,
                    command_payload=expected_materialize_payload,
                    workflow_type="company_public_web_refresh",
                    actor="company_public_web_phase_planner",
                    source="company_public_web_source_collect_owner",
                    source_command_id=parent_command_id,
                    expected_idempotency_key=expected_materialize_idempotency_key,
                    max_attempts=3,
                    retry_policy={"kind": "company_public_web_phase", "retry_delay_seconds": 30},
                    parent_command_id=parent_command_id,
                    causal_group_id=parent_command_id,
                )
            ):
                return {"status": "invalid", "reason": "company_public_web_materialize_target_mismatch"}
        return {
            "status": "ready",
            "workflow_command": command_record,
            "operation_run": operation_run,
            "action": action,
            "company_public_web_target": target,
            "company_public_web_input": action_input,
            "company_public_web_root_plan": root_plan,
        }

    def _fail_company_public_web_target_preflight(
        self,
        *,
        command_id: str,
        latest_command: Mapping[str, Any],
        target_preflight: Mapping[str, Any],
    ) -> dict[str, Any]:
        reason = str(
            target_preflight.get("reason")
            or target_preflight.get("status")
            or "company_public_web_command_target_preflight_failed"
        ).strip()
        failed = self._mark_company_public_web_command_failed_for_exact_claim(
            latest_command,
            error_text=reason,
            retryable=False,
            retry_delay_seconds=0,
        )
        if not failed:
            current_command = self.store.get_workflow_command(command_id) or dict(latest_command)
            return {
                "status": "skipped",
                "reason": "company_public_web_command_claim_not_current",
                "workflow_command": self._workflow_command_observation(
                    current_command,
                    migration_phase="W11_company_public_web_refresh",
                ),
            }
        self._sync_operation_run_from_workflow_command(
            failed or dict(latest_command),
            actor=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            source="company_public_web.command_owner",
        )
        return {
            **dict(target_preflight),
            "status": "failed",
            "workflow_command": self._workflow_command_observation(
                failed or dict(latest_command),
                migration_phase="W11_company_public_web_refresh",
            ),
        }

    def _close_company_public_web_owner_lost_activity_attempt(
        self,
        *,
        command: Mapping[str, Any],
        activity: Mapping[str, Any],
        attempt: Mapping[str, Any],
        reason: str,
        terminalize_exact_command: bool = False,
        terminalize_exhausted_command: bool = False,
    ) -> dict[str, Any]:
        """Close a stale D1m attempt or one exact deterministic terminal failure.

        ActivityRun is shared by every physical attempt for a command. The
        native closure uses the same advisory identities as the runtime
        repository and updates that shared row only while its metadata still
        names this attempt's lease owner. This keeps a later attempt's running
        ActivityRun intact while ensuring the losing ActivityAttempt is not
        left hanging. The opt-in terminal mode is used only for a deterministic
        brownfield collision and atomically fails the exact current command,
        ActivityRun, and ActivityAttempt.
        """

        command_payload = dict(command or {})
        activity_payload = dict(activity or {})
        attempt_payload = dict(attempt or {})
        normalized_reason = str(reason or "company_public_web_source_run_owner_lost").strip()
        if (not activity_payload or not attempt_payload) and not terminalize_exhausted_command:
            return {
                "outcome": "not_started",
                "reason": normalized_reason,
                "attempt_closed": False,
                "activity_closed": False,
            }
        closure = self.store._call_control_plane_postgres_native(  # noqa: SLF001
            "close_company_public_web_owner_lost_activity_attempt",
            table_name="workflow_activity_attempts",
            command_id=str(command_payload.get("command_id") or "").strip(),
            expected_command_attempt=max(0, int(command_payload.get("attempt") or 0)),
            expected_lease_owner=str(command_payload.get("lease_owner") or "").strip(),
            expected_lease_expires_at=str(command_payload.get("lease_expires_at") or "").strip(),
            activity_run_id=str(activity_payload.get("activity_run_id") or "").strip(),
            activity_idempotency_key=str(activity_payload.get("idempotency_key") or "").strip(),
            attempt_id=str(attempt_payload.get("attempt_id") or "").strip(),
            attempt_idempotency_key=str(attempt_payload.get("idempotency_key") or "").strip(),
            workspace_id=str(
                activity_payload.get("workspace_id")
                or dict(command_payload.get("payload") or {}).get("workspace_id")
                or "default"
            ).strip()
            or "default",
            reason=normalized_reason,
            terminalize_exact_command=bool(terminalize_exact_command),
            terminalize_exhausted_command=bool(terminalize_exhausted_command),
        )
        result = dict(closure or {})
        return {
            "outcome": str(result.get("outcome") or "unavailable").strip(),
            "reason": str(result.get("reason") or normalized_reason).strip(),
            "attempt_closed": bool(result.get("attempt_closed")),
            "activity_closed": bool(result.get("activity_closed")),
            "command_closed": bool(result.get("command_closed")),
            "attempt_id": str(dict(result.get("attempt") or {}).get("attempt_id") or "").strip(),
            "activity_run_id": str(dict(result.get("activity") or {}).get("activity_run_id") or "").strip(),
            "command_id": str(dict(result.get("command") or {}).get("command_id") or "").strip(),
        }

    def _terminalize_exhausted_company_public_web_source_command(
        self,
        command: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Atomically close one expired final D1m source attempt before reclaim."""

        command_payload = dict(command or {})
        if (
            str(command_payload.get("command_type") or "").strip() != COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE
            or str(command_payload.get("owner") or "").strip() != COMPANY_PUBLIC_WEB_REFRESH_OWNER
            or str(command_payload.get("status") or "").strip() != "running"
            or int(command_payload.get("attempt") or 0) < max(1, int(command_payload.get("max_attempts") or 1))
        ):
            return {"outcome": "not_exhausted", "command_closed": False}
        return self._close_company_public_web_owner_lost_activity_attempt(
            command=command_payload,
            activity={},
            attempt={},
            reason="workflow_command_attempts_exhausted_after_lease_expiry",
            terminalize_exhausted_command=True,
        )

    @staticmethod
    def _company_public_web_source_entity_delta_spec(
        *,
        command: Mapping[str, Any],
        activity: Mapping[str, Any],
        attempt: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        result_payload = dict(result or {})
        run = dict(result_payload.get("run") or {})
        run_id = str(run.get("run_id") or "").strip()
        command_id = str(command_payload.get("command_id") or "").strip()
        workflow_run_id = str(command_payload.get("workflow_run_id") or "").strip()
        operation_run_id = str(command_payload.get("operation_id") or "").strip()
        activity_run_id = str(dict(activity or {}).get("activity_run_id") or "").strip()
        attempt_id = str(dict(attempt or {}).get("attempt_id") or "").strip()
        if not all((run_id, command_id, workflow_run_id, operation_run_id, activity_run_id, attempt_id)):
            return {}
        workspace_id = str(dict(command_payload.get("payload") or {}).get("workspace_id") or "default").strip()
        workspace_id = workspace_id or "default"
        idempotency_key = (
            f"workflow_command_entity_delta:company_public_web_run:{command_id}:company_public_web_run:{run_id}"
        )
        artifact_refs = [
            str(ref or "").strip()
            for ref in list(dict(result_payload.get("artifact_paths") or {}).values())
            if str(ref or "").strip()
        ]
        return {
            "delta_id": "entitydelta_" + hashlib.sha1(idempotency_key.encode("utf-8")).hexdigest()[:24],
            "workspace_id": workspace_id,
            "workflow_run_id": workflow_run_id,
            "operation_run_id": operation_run_id,
            "command_id": command_id,
            "activity_run_id": activity_run_id,
            "attempt_id": attempt_id,
            "acquisition_run_id": "",
            "entity_type": "company_public_web_run",
            "entity_key": run_id,
            "delta_kind": "company_public_web_refreshed",
            "status": "recorded",
            "reason": "company_public_web_refresh_completed",
            "source_ref": {
                "run_id": run_id,
                "target_company": str(run.get("target_company") or "").strip(),
                "company_key": str(run.get("company_key") or "").strip(),
                "command_type": str(command_payload.get("command_type") or "").strip(),
            },
            "entity_payload": {
                "run_id": run_id,
                "summary": dict(result_payload.get("summary") or {}),
            },
            "projection_effect": {
                "entered_projection": False,
                "company_asset_layer_synced": False,
            },
            "artifact_refs": artifact_refs,
            "idempotency_key": idempotency_key,
            "metadata": {
                "company_public_web_owner": COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
            },
        }

