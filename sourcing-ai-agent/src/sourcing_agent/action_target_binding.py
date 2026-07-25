"""Server-context target binders for schema-defined Agent actions."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, Protocol

from sourcing_agent.company_registry import resolve_company_alias_key
from sourcing_agent.operation_runtime import (
    ACQUISITION_ROOT_ACTION_TYPES,
    COMPANY_PUBLIC_WEB_ACTION_TYPES,
    CRM_EXISTING_RECORD_ACTION_TYPES,
    CRM_PROJECTION_SELECTION_ACTION_TYPES,
    CRM_RECORD_BATCH_ACTION_TYPES,
    OwnerBoundTargetRef,
)
from sourcing_agent.request_ownership import exact_crm_owner_matches

AuthorizationMode = Literal["authenticated", "open_operator"]
AUTHORIZATION_MODE_AUTHENTICATED: Literal["authenticated"] = "authenticated"
AUTHORIZATION_MODE_OPEN_OPERATOR: Literal["open_operator"] = "open_operator"
CRM_RECORD_TARGET_OWNER = "crm_writer"
CRM_RECORD_BATCH_TARGET_OWNER = "person_evidence_ingestion"
CRM_RECORD_TARGET_NOT_FOUND = "crm_record_not_found"
CRM_RECORD_TARGET_STALE = "crm_record_target_stale"
CRM_RECORD_BATCH_TARGET_SELECTOR_FIELDS = (
    "crm_record_ids",
    "record_ids",
    "crm_record_id",
    "record_id",
    "person_identity_key",
)
CRM_RECORD_BATCH_LIMIT = 1000
CRM_PROJECTION_SELECTION_TARGET_OWNER = "crm_writer"
CRM_PROJECTION_SELECTION_TARGET_INVALID = "crm_projection_selection_target_invalid"
CRM_PROJECTION_SELECTION_TARGET_NOT_FOUND = "crm_projection_selection_not_found"
CRM_PROJECTION_SELECTION_LIMIT = 100_000
CRM_PROJECTION_SELECTION_SELECTOR_ALIASES = (
    ("projection_id", ("projection_id", "serving_projection_id")),
    (
        "expected_membership_revision",
        ("expected_membership_revision", "membership_revision"),
    ),
    (
        "candidate_identity_keys",
        ("candidate_identity_keys", "candidate_ids", "candidate_identity_key", "candidate_id"),
    ),
)
ACQUISITION_ROOT_TARGET_OWNER = "acquisition_run_writer"
ACQUISITION_ROOT_TARGET_INVALID = "acquisition_root_target_invalid"
COMPANY_PUBLIC_WEB_TARGET_OWNER = "company_public_web_owner"
COMPANY_PUBLIC_WEB_TARGET_INVALID = "company_public_web_target_invalid"


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

    def get_crm_record_by_person_identity(
        self,
        person_identity_key: str,
        *,
        workspace_id: str,
    ) -> dict[str, Any]: ...


class ProjectionMemberSnapshotReader(Protocol):
    def get_projection_member_snapshot(
        self,
        projection_id: str,
        *,
        candidate_identity_keys: list[str] | tuple[str, ...] | None = None,
        limit: int = 100_000,
        page_size: int = 500,
        require_all_requested: bool = True,
    ) -> dict[str, Any]: ...


class AcquisitionRootTargetBinder:
    """Mint and revalidate the server-owned workspace for one root run."""

    def __call__(self, context: ActionBindContext) -> OwnerBoundTargetRef:
        if dict(context.target_selector):
            raise ActionTargetBindingError(ACQUISITION_ROOT_TARGET_INVALID)
        return OwnerBoundTargetRef(
            owner_module=ACQUISITION_ROOT_TARGET_OWNER,
            target_ref={"workspace_id": context.workspace_id},
        )

    @staticmethod
    def revalidate_snapshot(
        *,
        target_ref: Mapping[str, Any],
        operation_workspace_id: str,
    ) -> dict[str, str]:
        target = dict(target_ref)
        workspace_id = str(target.get("workspace_id") or "").strip()
        operation_workspace = str(operation_workspace_id or "").strip()
        if (
            set(target) != {"workspace_id"}
            or not workspace_id
            or workspace_id != target.get("workspace_id")
            or workspace_id != operation_workspace
        ):
            raise ActionTargetBindingError(ACQUISITION_ROOT_TARGET_INVALID)
        return {"workspace_id": workspace_id}


class CompanyPublicWebTargetBinder:
    """Mint the canonical workspace/company target for deterministic refresh."""

    _SELECTOR_FIELDS = {"target_company"}
    _TARGET_FIELDS = {"workspace_id", "company_key"}

    def __call__(self, context: ActionBindContext) -> OwnerBoundTargetRef:
        selector = dict(context.target_selector)
        raw_target_company = selector.get("target_company")
        if set(selector) != self._SELECTOR_FIELDS or not isinstance(raw_target_company, str):
            raise ActionTargetBindingError(COMPANY_PUBLIC_WEB_TARGET_INVALID)
        target_company = raw_target_company.strip()
        company_key = resolve_company_alias_key(target_company)
        if (
            not target_company
            or len(target_company) > 500
            or not company_key
            or len(company_key) > 200
            or company_key != company_key.strip()
        ):
            raise ActionTargetBindingError(COMPANY_PUBLIC_WEB_TARGET_INVALID)
        return OwnerBoundTargetRef(
            owner_module=COMPANY_PUBLIC_WEB_TARGET_OWNER,
            target_ref={
                "workspace_id": context.workspace_id,
                "company_key": company_key,
            },
        )

    @classmethod
    def revalidate_snapshot(
        cls,
        *,
        target_ref: Mapping[str, Any],
        operation_workspace_id: str,
    ) -> dict[str, str]:
        target = dict(target_ref)
        raw_workspace_id = target.get("workspace_id")
        raw_company_key = target.get("company_key")
        if (
            set(target) != cls._TARGET_FIELDS
            or not isinstance(raw_workspace_id, str)
            or not isinstance(raw_company_key, str)
        ):
            raise ActionTargetBindingError(COMPANY_PUBLIC_WEB_TARGET_INVALID)
        workspace_id = raw_workspace_id.strip()
        company_key = raw_company_key.strip()
        if (
            not workspace_id
            or workspace_id != raw_workspace_id
            or workspace_id != str(operation_workspace_id or "").strip()
            or not company_key
            or company_key != raw_company_key
            or len(company_key) > 200
            or resolve_company_alias_key(company_key) != company_key
        ):
            raise ActionTargetBindingError(COMPANY_PUBLIC_WEB_TARGET_INVALID)
        return {"workspace_id": workspace_id, "company_key": company_key}


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


class CRMRecordBatchTargetBinder:
    """Resolve one bounded CRM-record set and mint a canonical owner snapshot."""

    def __init__(self, store: CRMRecordLookup) -> None:
        self.store = store

    def __call__(self, context: ActionBindContext) -> OwnerBoundTargetRef:
        selector = dict(context.target_selector)
        present = [field for field in CRM_RECORD_BATCH_TARGET_SELECTOR_FIELDS if field in selector]
        if len(present) != 1 or set(selector) != {present[0]}:
            raise ActionTargetBindingError("crm_record_batch_target_selector_invalid")
        selector_field = present[0]
        if selector_field == "person_identity_key":
            raw_person_identity_key = selector.get(selector_field)
            if not isinstance(raw_person_identity_key, str):
                raise ActionTargetBindingError("crm_record_batch_target_selector_invalid")
            person_identity_key = raw_person_identity_key.strip()
            if not person_identity_key:
                raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
            record = self.store.get_crm_record_by_person_identity(
                person_identity_key,
                workspace_id=context.workspace_id,
            )
            raw_record_ids: Any = [str(record.get("crm_record_id") or "").strip()] if record else []
        elif selector_field in {"crm_record_ids", "record_ids"}:
            raw_record_ids = selector.get(selector_field)
            if not isinstance(raw_record_ids, (list, tuple)):
                raise ActionTargetBindingError("crm_record_batch_target_selector_invalid")
        else:
            raw_record_id = selector.get(selector_field)
            if not isinstance(raw_record_id, str):
                raise ActionTargetBindingError("crm_record_batch_target_selector_invalid")
            raw_record_ids = [raw_record_id]
        record_ids = self._normalize_record_ids(raw_record_ids)
        snapshots: list[dict[str, Any]] = []
        for record_id in record_ids:
            record = self.store.get_crm_record(record_id)
            if not CRMRecordTargetBinder._context_owns_record(context=context, record=record):
                raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
            assert record is not None
            snapshot = self._snapshot_record(record, expected_record_id=record_id)
            snapshots.append(snapshot)
        return OwnerBoundTargetRef(
            owner_module=CRM_RECORD_BATCH_TARGET_OWNER,
            target_ref={
                "crm_record_ids": [snapshot["crm_record_id"] for snapshot in snapshots],
                "workspace_id": context.workspace_id,
                "crm_record_snapshots": snapshots,
            },
        )

    @staticmethod
    def _normalize_record_ids(value: Any) -> list[str]:
        if not isinstance(value, (list, tuple)):
            raise ActionTargetBindingError("crm_record_batch_target_selector_invalid")
        raw_values = list(value)
        if any(not isinstance(item, str) for item in raw_values):
            raise ActionTargetBindingError("crm_record_batch_target_selector_invalid")
        normalized = [item.strip() for item in raw_values]
        if not normalized or any(not item for item in normalized) or len(normalized) > CRM_RECORD_BATCH_LIMIT:
            raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
        return sorted(set(normalized))

    @staticmethod
    def _snapshot_record(record: Mapping[str, Any], *, expected_record_id: str) -> dict[str, Any]:
        record_id = str(record.get("crm_record_id") or "").strip()
        workspace_id = str(record.get("workspace_id") or "default").strip() or "default"
        owner_user_id = str(record.get("owner_user_id") or "").strip()
        crm_version = record.get("crm_version")
        if (
            record_id != expected_record_id
            or isinstance(crm_version, bool)
            or not isinstance(crm_version, int)
            or crm_version <= 0
        ):
            raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
        return {
            "crm_record_id": record_id,
            "workspace_id": workspace_id,
            "owner_user_id": owner_user_id,
            "crm_version": crm_version,
        }

    def revalidate_snapshot(
        self,
        *,
        target_ref: Mapping[str, Any],
        operation_workspace_id: str,
    ) -> list[dict[str, Any]]:
        target = dict(target_ref)
        if set(target) != {"crm_record_ids", "workspace_id", "crm_record_snapshots"}:
            raise ActionTargetBindingError("crm_record_batch_bound_target_invalid")
        workspace_id = str(target.get("workspace_id") or "").strip()
        operation_workspace = str(operation_workspace_id or "").strip()
        if not workspace_id or workspace_id != operation_workspace:
            raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
        raw_record_ids = target.get("crm_record_ids")
        try:
            record_ids = self._normalize_record_ids(raw_record_ids)
        except ActionTargetBindingError as exc:
            raise ActionTargetBindingError("crm_record_batch_bound_target_invalid") from exc
        if list(raw_record_ids) != record_ids:
            raise ActionTargetBindingError("crm_record_batch_bound_target_invalid")
        raw_snapshots = target.get("crm_record_snapshots")
        if not isinstance(raw_snapshots, (list, tuple)) or len(raw_snapshots) != len(record_ids):
            raise ActionTargetBindingError("crm_record_batch_bound_target_invalid")
        snapshots: list[dict[str, Any]] = []
        for raw_snapshot in raw_snapshots:
            if not isinstance(raw_snapshot, Mapping):
                raise ActionTargetBindingError("crm_record_batch_bound_target_invalid")
            snapshot = dict(raw_snapshot)
            if set(snapshot) != {"crm_record_id", "workspace_id", "owner_user_id", "crm_version"}:
                raise ActionTargetBindingError("crm_record_batch_bound_target_invalid")
            if (
                not isinstance(snapshot.get("crm_record_id"), str)
                or not isinstance(snapshot.get("workspace_id"), str)
                or not isinstance(snapshot.get("owner_user_id"), str)
                or isinstance(snapshot.get("crm_version"), bool)
                or not isinstance(snapshot.get("crm_version"), int)
                or snapshot["crm_version"] <= 0
            ):
                raise ActionTargetBindingError("crm_record_batch_bound_target_invalid")
            snapshots.append(snapshot)
        if [str(snapshot.get("crm_record_id") or "").strip() for snapshot in snapshots] != record_ids:
            raise ActionTargetBindingError("crm_record_batch_bound_target_invalid")
        records: list[dict[str, Any]] = []
        for record_id, snapshot in zip(record_ids, snapshots):
            if str(snapshot.get("workspace_id") or "").strip() != workspace_id:
                raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
            record = self.store.get_crm_record(record_id)
            if not record:
                raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
            current_snapshot = self._snapshot_record(record, expected_record_id=record_id)
            if (
                current_snapshot["workspace_id"] != workspace_id
                or current_snapshot["owner_user_id"] != str(snapshot.get("owner_user_id") or "").strip()
            ):
                raise ActionTargetBindingError(CRM_RECORD_TARGET_NOT_FOUND)
            if current_snapshot["crm_version"] != snapshot.get("crm_version"):
                raise ActionTargetBindingError(CRM_RECORD_TARGET_STALE)
            records.append(dict(record))
        return records


class CRMProjectionSelectionTargetBinder:
    """Mint a CRM destination scope plus an exact shared projection-member snapshot."""

    _SELECTOR_FIELDS = {
        "projection_id",
        "expected_membership_revision",
        "candidate_identity_keys",
    }
    _TARGET_FIELDS = {
        "workspace_id",
        "projection_id",
        "membership_revision",
        "source_candidate_count",
        "candidate_identity_keys",
    }

    def __init__(self, reader: ProjectionMemberSnapshotReader) -> None:
        self.reader = reader

    @staticmethod
    def _normalize_candidate_identity_keys(value: Any) -> list[str]:
        raw_values = list(value) if isinstance(value, (list, tuple)) else [value]
        if not raw_values or any(not isinstance(item, str) for item in raw_values):
            raise ActionTargetBindingError(CRM_PROJECTION_SELECTION_TARGET_INVALID)
        normalized = [item.strip() for item in raw_values]
        if (
            any(not item for item in normalized)
            or any(len(item) > 500 for item in normalized)
            or len(normalized) > CRM_PROJECTION_SELECTION_LIMIT
            or len(set(normalized)) != len(normalized)
        ):
            raise ActionTargetBindingError(CRM_PROJECTION_SELECTION_TARGET_INVALID)
        return sorted(normalized)

    def __call__(self, context: ActionBindContext) -> OwnerBoundTargetRef:
        selector = dict(context.target_selector)
        if set(selector) != self._SELECTOR_FIELDS:
            raise ActionTargetBindingError(CRM_PROJECTION_SELECTION_TARGET_INVALID)
        raw_projection_id = selector.get("projection_id")
        raw_expected_revision = selector.get("expected_membership_revision")
        if not isinstance(raw_projection_id, str) or not isinstance(raw_expected_revision, str):
            raise ActionTargetBindingError(CRM_PROJECTION_SELECTION_TARGET_INVALID)
        projection_id = raw_projection_id.strip()
        expected_revision = raw_expected_revision.strip()
        candidate_identity_keys = self._normalize_candidate_identity_keys(selector.get("candidate_identity_keys"))
        if (
            not projection_id
            or projection_id != raw_projection_id
            or len(projection_id) > 200
            or not expected_revision
            or expected_revision != raw_expected_revision
            or len(expected_revision) > 200
        ):
            raise ActionTargetBindingError(CRM_PROJECTION_SELECTION_TARGET_INVALID)
        snapshot = self.reader.get_projection_member_snapshot(
            projection_id,
            candidate_identity_keys=candidate_identity_keys,
            limit=len(candidate_identity_keys),
            require_all_requested=True,
        )
        if str(snapshot.get("status") or "").strip() != "ready":
            raise ActionTargetBindingError(
                str(snapshot.get("reason") or "crm_projection_selection_not_ready").strip()
                or "crm_projection_selection_not_ready"
            )
        membership_revision = str(snapshot.get("membership_revision") or "").strip()
        source_candidate_count = snapshot.get("source_candidate_count")
        member_keys = sorted(
            str(dict(member or {}).get("candidate_identity_key") or "").strip()
            for member in list(snapshot.get("members") or [])
        )
        if (
            membership_revision != expected_revision
            or isinstance(source_candidate_count, bool)
            or not isinstance(source_candidate_count, int)
            or source_candidate_count < len(candidate_identity_keys)
            or member_keys != candidate_identity_keys
        ):
            raise ActionTargetBindingError("projection_membership_revision_stale")
        return OwnerBoundTargetRef(
            owner_module=CRM_PROJECTION_SELECTION_TARGET_OWNER,
            target_ref={
                "workspace_id": context.workspace_id,
                "projection_id": projection_id,
                "membership_revision": membership_revision,
                "source_candidate_count": source_candidate_count,
                "candidate_identity_keys": candidate_identity_keys,
            },
        )

    def revalidate_snapshot(
        self,
        *,
        target_ref: Mapping[str, Any],
        operation_workspace_id: str,
    ) -> dict[str, Any]:
        target = dict(target_ref)
        if set(target) != self._TARGET_FIELDS:
            raise ActionTargetBindingError(CRM_PROJECTION_SELECTION_TARGET_INVALID)
        workspace_id = str(target.get("workspace_id") or "").strip()
        projection_id = str(target.get("projection_id") or "").strip()
        membership_revision = str(target.get("membership_revision") or "").strip()
        source_candidate_count = target.get("source_candidate_count")
        candidate_identity_keys = self._normalize_candidate_identity_keys(target.get("candidate_identity_keys"))
        if (
            not workspace_id
            or workspace_id != str(operation_workspace_id or "").strip()
            or not projection_id
            or projection_id != target.get("projection_id")
            or not membership_revision
            or isinstance(source_candidate_count, bool)
            or not isinstance(source_candidate_count, int)
            or source_candidate_count < len(candidate_identity_keys)
            or list(target.get("candidate_identity_keys") or []) != candidate_identity_keys
        ):
            raise ActionTargetBindingError(CRM_PROJECTION_SELECTION_TARGET_INVALID)
        snapshot = self.reader.get_projection_member_snapshot(
            projection_id,
            candidate_identity_keys=candidate_identity_keys,
            limit=len(candidate_identity_keys),
            require_all_requested=True,
        )
        member_keys = sorted(
            str(dict(member or {}).get("candidate_identity_key") or "").strip()
            for member in list(snapshot.get("members") or [])
        )
        if (
            str(snapshot.get("status") or "").strip() != "ready"
            or str(snapshot.get("membership_revision") or "").strip() != membership_revision
            or snapshot.get("source_candidate_count") != source_candidate_count
            or member_keys != candidate_identity_keys
        ):
            raise ActionTargetBindingError("projection_membership_revision_stale")
        return dict(snapshot)


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
            for action_type in CRM_EXISTING_RECORD_ACTION_TYPES
        )
    )


def build_acquisition_root_target_binder_registry(
    *,
    binder: AcquisitionRootTargetBinder | None = None,
) -> ActionTargetBinderRegistry:
    binder = binder or AcquisitionRootTargetBinder()
    return ActionTargetBinderRegistry(
        tuple(
            ActionTargetBinderSpec(
                action_type=action_type,
                owner_module=ACQUISITION_ROOT_TARGET_OWNER,
                binder=binder,
            )
            for action_type in ACQUISITION_ROOT_ACTION_TYPES
        )
    )


def build_company_public_web_target_binder_registry(
    *,
    binder: CompanyPublicWebTargetBinder | None = None,
) -> ActionTargetBinderRegistry:
    binder = binder or CompanyPublicWebTargetBinder()
    return ActionTargetBinderRegistry(
        tuple(
            ActionTargetBinderSpec(
                action_type=action_type,
                owner_module=COMPANY_PUBLIC_WEB_TARGET_OWNER,
                binder=binder,
            )
            for action_type in COMPANY_PUBLIC_WEB_ACTION_TYPES
        )
    )


def build_crm_record_batch_target_binder_registry(
    store: CRMRecordLookup,
    *,
    binder: CRMRecordBatchTargetBinder | None = None,
) -> ActionTargetBinderRegistry:
    binder = binder or CRMRecordBatchTargetBinder(store)
    return ActionTargetBinderRegistry(
        tuple(
            ActionTargetBinderSpec(
                action_type=action_type,
                owner_module=CRM_RECORD_BATCH_TARGET_OWNER,
                binder=binder,
            )
            for action_type in CRM_RECORD_BATCH_ACTION_TYPES
        )
    )


def build_crm_projection_selection_target_binder_registry(
    reader: ProjectionMemberSnapshotReader,
    *,
    binder: CRMProjectionSelectionTargetBinder | None = None,
) -> ActionTargetBinderRegistry:
    binder = binder or CRMProjectionSelectionTargetBinder(reader)
    return ActionTargetBinderRegistry(
        tuple(
            ActionTargetBinderSpec(
                action_type=action_type,
                owner_module=CRM_PROJECTION_SELECTION_TARGET_OWNER,
                binder=binder,
            )
            for action_type in CRM_PROJECTION_SELECTION_ACTION_TYPES
        )
    )
