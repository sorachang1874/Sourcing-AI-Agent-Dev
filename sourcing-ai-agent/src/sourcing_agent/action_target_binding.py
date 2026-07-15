"""Server-context target binders for schema-defined Agent actions."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, Protocol

from sourcing_agent.operation_runtime import (
    ACTION_ADD_CRM_NOTE,
    ACTION_CREATE_CRM_TASK,
    ACTION_SET_CRM_STAGE,
    OwnerBoundTargetRef,
)
from sourcing_agent.request_ownership import exact_crm_owner_matches

AuthorizationMode = Literal["authenticated", "open_operator"]
AUTHORIZATION_MODE_AUTHENTICATED: Literal["authenticated"] = "authenticated"
AUTHORIZATION_MODE_OPEN_OPERATOR: Literal["open_operator"] = "open_operator"
CRM_RECORD_TARGET_OWNER = "crm_writer"
CRM_RECORD_TARGET_NOT_FOUND = "crm_record_not_found"
CRM_RECORD_TARGET_STALE = "crm_record_target_stale"


class ActionTargetBindingError(ValueError):
    """A stable fail-closed target-binding outcome raised before persistence."""

    def __init__(self, reason: str) -> None:
        self.reason = str(reason or "action_target_binding_failed").strip() or "action_target_binding_failed"
        super().__init__(self.reason)


def _freeze_json_object(value: Mapping[str, Any]) -> Mapping[str, Any]:
    try:
        copied = json.loads(
            json.dumps(
                dict(value),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ActionTargetBindingError("action_bind_context_selector_not_json") from exc
    if not isinstance(copied, dict):
        raise ActionTargetBindingError("action_bind_context_selector_must_be_object")
    return _freeze_json_value(copied)


def _freeze_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json_value(child) for key, child in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json_value(child) for child in value)
    return value


@dataclass(frozen=True)
class ActionBindContext:
    """Authenticated/server-owned scope and locator passed to an owner binder.

    ``target_selector`` is a lookup hint from trusted route/conversation state;
    it is not persisted as caller input. The binder resolves it against the
    canonical owner row and mints the durable target snapshot.
    """

    authorization_mode: AuthorizationMode
    workspace_id: str
    target_selector: Mapping[str, Any]
    owner_user_id: str = ""

    def __post_init__(self) -> None:
        workspace_id = str(self.workspace_id or "").strip()
        owner_user_id = str(self.owner_user_id or "").strip()
        if not workspace_id or workspace_id != self.workspace_id:
            raise ActionTargetBindingError("action_bind_context_workspace_invalid")
        if self.authorization_mode == AUTHORIZATION_MODE_AUTHENTICATED:
            if not owner_user_id or owner_user_id != self.owner_user_id:
                raise ActionTargetBindingError("action_bind_context_owner_invalid")
        elif self.authorization_mode == AUTHORIZATION_MODE_OPEN_OPERATOR:
            if owner_user_id:
                raise ActionTargetBindingError("action_bind_context_open_owner_forbidden")
        else:
            raise ActionTargetBindingError("action_bind_context_authorization_mode_invalid")
        object.__setattr__(self, "workspace_id", workspace_id)
        object.__setattr__(self, "owner_user_id", owner_user_id)
        object.__setattr__(self, "target_selector", _freeze_json_object(self.target_selector))


TargetBinder = Callable[[ActionBindContext], OwnerBoundTargetRef]


@dataclass(frozen=True)
class ActionTargetBinderSpec:
    action_type: str
    owner_module: str
    binder: TargetBinder

    def __post_init__(self) -> None:
        for field_name, value in (("action_type", self.action_type), ("owner_module", self.owner_module)):
            if not value or value != value.strip():
                raise ValueError(f"action target binder {field_name} must be normalized")
        if not callable(self.binder):
            raise ValueError("action target binder must be callable")


class ActionTargetBinderRegistry:
    """Closed action-to-owner-binder mapping; unknown actions fail closed."""

    def __init__(self, specs: tuple[ActionTargetBinderSpec, ...] = ()) -> None:
        self._specs: dict[str, ActionTargetBinderSpec] = {}
        for spec in specs:
            self.register(spec)

    def register(self, spec: ActionTargetBinderSpec) -> None:
        if spec.action_type in self._specs:
            raise ValueError(f"action target binder already registered: {spec.action_type}")
        self._specs[spec.action_type] = spec

    def bind(self, *, action_type: str, context: ActionBindContext) -> OwnerBoundTargetRef:
        normalized_action = str(action_type or "").strip()
        spec = self._specs.get(normalized_action)
        if spec is None:
            raise ActionTargetBindingError(f"action_target_binder_missing:{normalized_action}")
        target = spec.binder(context)
        if not isinstance(target, OwnerBoundTargetRef) or target.owner_module != spec.owner_module:
            raise ActionTargetBindingError("action_target_binder_owner_mismatch")
        return target

    def to_record(self) -> dict[str, dict[str, str]]:
        return {action_type: {"owner_module": spec.owner_module} for action_type, spec in sorted(self._specs.items())}


class CRMRecordLookup(Protocol):
    def get_crm_record(self, crm_record_id: str) -> dict[str, Any]: ...


class CRMRecordTargetBinder:
    """Resolve one exact CRM owner row and mint its immutable target snapshot."""

    def __init__(self, store: CRMRecordLookup) -> None:
        self.store = store

    def __call__(self, context: ActionBindContext) -> OwnerBoundTargetRef:
        selector = dict(context.target_selector)
        if not selector:
            raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
        if set(selector) != {"crm_record_id"}:
            raise ActionTargetBindingError("crm_record_target_selector_invalid")
        crm_record_id = str(selector.get("crm_record_id") or "").strip()
        if not crm_record_id:
            raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
        record = self.store.get_crm_record(crm_record_id)
        if not self._context_owns_record(context=context, record=record):
            raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
        stored_record_id = str(record.get("crm_record_id") or "").strip()
        stored_workspace_id = str(record.get("workspace_id") or "default").strip() or "default"
        stored_owner_user_id = str(record.get("owner_user_id") or "").strip()
        crm_version = record.get("crm_version")
        if (
            stored_record_id != crm_record_id
            or isinstance(crm_version, bool)
            or not isinstance(crm_version, int)
            or crm_version <= 0
        ):
            raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
        return OwnerBoundTargetRef(
            owner_module=CRM_RECORD_TARGET_OWNER,
            target_ref={
                "crm_record_id": stored_record_id,
                "workspace_id": stored_workspace_id,
                "owner_user_id": stored_owner_user_id,
                "crm_version": crm_version,
            },
        )

    @staticmethod
    def _context_owns_record(*, context: ActionBindContext, record: dict[str, Any] | None) -> bool:
        if record is None:
            return False
        if context.authorization_mode == AUTHORIZATION_MODE_AUTHENTICATED:
            return exact_crm_owner_matches(
                record,
                expected_workspace_id=context.workspace_id,
                expected_owner_user_id=context.owner_user_id,
            )
        return (str(record.get("workspace_id") or "default").strip() or "default") == context.workspace_id

    def revalidate_snapshot(
        self,
        *,
        target_ref: Mapping[str, Any],
        operation_workspace_id: str,
    ) -> dict[str, Any]:
        """Recheck the owner/version snapshot before command planning.

        Command execution must additionally carry these pins into its owner UoW;
        this pre-dispatch check alone is not a TOCTOU claim.
        """

        target = dict(target_ref)
        if set(target) != {"crm_record_id", "workspace_id", "owner_user_id", "crm_version"}:
            raise ActionTargetBindingError("crm_record_bound_target_invalid")
        crm_record_id = str(target.get("crm_record_id") or "").strip()
        workspace_id = str(target.get("workspace_id") or "").strip()
        owner_user_id = str(target.get("owner_user_id") or "").strip()
        crm_version = target.get("crm_version")
        normalized_operation_workspace = str(operation_workspace_id or "").strip()
        if (
            not crm_record_id
            or not workspace_id
            or workspace_id != normalized_operation_workspace
            or isinstance(crm_version, bool)
            or not isinstance(crm_version, int)
            or crm_version <= 0
        ):
            raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
        record = self.store.get_crm_record(crm_record_id)
        if not record:
            raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
        if (str(record.get("workspace_id") or "default").strip() or "default") != workspace_id or str(
            record.get("owner_user_id") or ""
        ).strip() != owner_user_id:
            raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
        if record.get("crm_version") != crm_version:
            raise ActionTargetBindingError(CRM_RECORD_TARGET_STALE)
        return dict(record)


def build_crm_existing_record_target_binder_registry(
    store: CRMRecordLookup,
    *,
    binder: CRMRecordTargetBinder | None = None,
) -> ActionTargetBinderRegistry:
    binder = binder or CRMRecordTargetBinder(store)
    return ActionTargetBinderRegistry(
        tuple(
            ActionTargetBinderSpec(
                action_type=action_type,
                owner_module=CRM_RECORD_TARGET_OWNER,
                binder=binder,
            )
            for action_type in (
                ACTION_SET_CRM_STAGE,
                ACTION_ADD_CRM_NOTE,
                ACTION_CREATE_CRM_TASK,
            )
        )
    )
