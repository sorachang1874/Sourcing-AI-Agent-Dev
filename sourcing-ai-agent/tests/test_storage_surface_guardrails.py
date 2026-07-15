import ast
import inspect
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from sourcing_agent.repositories import serving_projection_repo
from sourcing_agent.repositories.manual_review import ManualReviewRepository
from sourcing_agent.repositories.serving_projection import ServingProjectionRepository
from sourcing_agent.repositories.workflow_runtime import WorkflowRuntimeRepository
from sourcing_agent.storage import ControlPlaneStore

_RETIRED_MANUAL_REVIEW_STORE_METHODS = {
    "replace_manual_review_items",
    "list_manual_review_items",
    "count_manual_review_items",
    "cleanup_manual_review_items",
    "review_manual_review_item",
    "get_manual_review_item",
    "merge_manual_review_item_metadata",
}
_MANUAL_REVIEW_REPOSITORY_METHODS = {
    "replace_items",
    "list_items",
    "count_items",
    "cleanup_items",
    "review_item",
    "get_item",
    "merge_item_metadata",
}
_RETIRED_OPERATION_CONTROL_STORE_METHODS = {
    "upsert_agent_action",
    "get_agent_action",
    "list_agent_actions",
    "update_agent_action_state",
    "upsert_operation_run",
    "get_operation_run",
    "list_operation_runs",
    "update_operation_run_state",
    "append_operation_event",
    "list_operation_events",
    "list_operation_events_for_action",
    "_agent_action_from_row",
    "_operation_run_from_row",
    "_operation_event_from_row",
}
_RETIRED_OPERATION_CONTROL_CALL_ATTRIBUTES = {
    name for name in _RETIRED_OPERATION_CONTROL_STORE_METHODS if not name.startswith("_")
}
_RETIRED_OPERATION_CONTROL_NATIVE_DISPATCH_KEYS = {
    "upsert_agent_action",
    "get_agent_action",
    "update_agent_action_state",
    "upsert_operation_run",
    "get_operation_run",
    "update_operation_run_state",
    "append_operation_event",
}
_OPERATION_CONTROL_REPOSITORY_METHODS = {
    "upsert_action",
    "get_action",
    "list_actions",
    "update_action_state",
    "upsert_operation",
    "get_operation",
    "list_operations",
    "update_operation_state",
    "append_operation_event",
    "reject_action_with_event",
    "cancel_operation_with_event",
    "fail_operation_for_stale_input_with_event",
    "hold_operation_dispatch_lock",
    "list_operation_events",
    "list_operation_events_for_action",
    "_action_from_row",
    "_operation_from_row",
    "_operation_event_from_row",
}
_RETIRED_ACQUISITION_CONTROL_STORE_METHODS = {
    "upsert_acquisition_run",
    "get_acquisition_run",
    "list_acquisition_runs",
    "upsert_acquisition_discovery_lane",
    "get_acquisition_discovery_lane",
    "list_acquisition_discovery_lanes",
    "_acquisition_run_from_row",
    "_acquisition_discovery_lane_from_row",
}
_RETIRED_ACQUISITION_CONTROL_CALL_ATTRIBUTES = {
    name for name in _RETIRED_ACQUISITION_CONTROL_STORE_METHODS if not name.startswith("_")
}
_ACQUISITION_CONTROL_REPOSITORY_METHODS = {
    "upsert_acquisition_run",
    "get_acquisition_run",
    "list_acquisition_runs",
    "upsert_discovery_lane",
    "get_discovery_lane",
    "list_discovery_lanes",
    "_acquisition_run_from_row",
    "_discovery_lane_from_row",
}
_RETIRED_ACTIVITY_SPINE_STORE_METHODS = {
    "upsert_workflow_activity_run",
    "get_workflow_activity_run",
    "list_workflow_activity_runs",
    "upsert_workflow_activity_attempt",
    "get_workflow_activity_attempt",
    "list_workflow_activity_attempts",
    "upsert_workflow_entity_delta",
    "get_workflow_entity_delta",
    "list_workflow_entity_deltas",
    "_workflow_activity_run_from_row",
    "_workflow_activity_attempt_from_row",
    "_workflow_entity_delta_from_row",
}
_RETIRED_ACTIVITY_SPINE_CALL_ATTRIBUTES = {
    name for name in _RETIRED_ACTIVITY_SPINE_STORE_METHODS if not name.startswith("_")
}
_ACTIVITY_SPINE_REPOSITORY_METHODS = {
    "upsert_activity_run",
    "get_activity_run",
    "list_activity_runs_by_ids",
    "list_activity_runs",
    "upsert_activity_attempt",
    "get_activity_attempt",
    "list_activity_attempts",
    "upsert_entity_delta",
    "get_entity_delta",
    "list_entity_deltas",
    "_activity_run_from_row",
    "_activity_attempt_from_row",
    "_entity_delta_from_row",
}
_RETIRED_WORKFLOW_READ_MODEL_STORE_METHODS = {
    "append_workflow_event",
    "list_workflow_events",
    "upsert_workflow_current_state",
    "get_workflow_current_state",
    "enqueue_runtime_outbox",
    "mark_runtime_outbox_dispatched",
    "_workflow_event_from_row",
    "_workflow_current_state_from_row",
    "_runtime_outbox_from_row",
}
_RETIRED_WORKFLOW_READ_MODEL_CALL_ATTRIBUTES = {
    name for name in _RETIRED_WORKFLOW_READ_MODEL_STORE_METHODS if not name.startswith("_")
}
_RETIRED_WORKFLOW_READ_MODEL_NATIVE_DISPATCH_KEYS = {
    "append_workflow_event",
    "upsert_workflow_current_state",
    "enqueue_runtime_outbox",
    "mark_runtime_outbox_dispatched",
}
_WORKFLOW_READ_MODEL_REPOSITORY_METHODS = {
    "append_workflow_event",
    "list_workflow_events",
    "upsert_workflow_current_state",
    "get_workflow_current_state",
    "enqueue_runtime_outbox",
    "mark_runtime_outbox_dispatched",
    "_workflow_event_from_row",
    "_workflow_current_state_from_row",
    "_runtime_outbox_from_row",
}
_RETIRED_RECOVERY_INTENT_STORE_METHODS = {
    "upsert_workflow_recovery_intent",
    "claim_workflow_recovery_intents",
    "mark_workflow_recovery_intent_consumed",
    "get_workflow_recovery_intent",
    "_workflow_recovery_intent_from_row",
}
_RETIRED_RECOVERY_INTENT_CALL_ATTRIBUTES = {
    name for name in _RETIRED_RECOVERY_INTENT_STORE_METHODS if not name.startswith("_")
}
_RETIRED_RECOVERY_INTENT_NATIVE_DISPATCH_KEYS = {
    "upsert_workflow_recovery_intent",
    "claim_workflow_recovery_intents",
    "mark_workflow_recovery_intent_consumed",
}
_RECOVERY_INTENT_REPOSITORY_METHODS = {
    "upsert_recovery_intent",
    "claim_recovery_intents",
    "mark_recovery_intent_consumed",
    "get_recovery_intent",
    "_recovery_intent_from_row",
}
_RETIRED_SERVING_PROJECTION_STORE_METHODS = {
    "upsert_serving_projection",
    "get_serving_projection",
    "list_serving_projections",
    "upsert_run_projection_link",
    "get_run_projection_link",
    "list_run_projection_links",
    "upsert_collection_authoritative_pointer",
    "get_collection_authoritative_pointer",
    "list_collection_authoritative_pointers",
    "_serving_projection_from_row",
    "_run_projection_link_from_row",
    "_collection_authoritative_pointer_from_row",
    "upsert_projection_manifest_shard",
    "get_projection_manifest_shard",
    "list_projection_manifest_shards",
    "_projection_manifest_shard_from_row",
    "upsert_serving_projection_members",
    "replace_serving_projection_members",
    "list_serving_projection_members",
    "list_serving_projection_members_by_identity_keys",
    "list_serving_projection_members_by_person_identity",
    "count_serving_projection_members_by_readiness",
    "get_serving_projection_member",
    "count_serving_projection_members",
    "_serving_projection_member_from_row",
    "_serving_projection_member_row_payload",
    "replace_projection_person_search_index",
    "delete_projection_person_search_index",
    "upsert_projection_person_search_index_rows",
    "update_projection_person_search_index_scope",
    "count_projection_person_search_index",
    "search_projection_person_index",
    "filter_projection_person_search_index",
    "get_projection_person_search_index_summary",
    "list_projection_person_search_index_rows",
    "_search_projection_person_index_rows",
    "_list_projection_person_search_index_rows",
    "_projection_person_search_index_from_row",
    "_projection_person_search_index_row_payload",
}
_RETIRED_SERVING_PROJECTION_CALL_ATTRIBUTES = {
    name for name in _RETIRED_SERVING_PROJECTION_STORE_METHODS if not name.startswith("_")
}
_RETIRED_SERVING_PROJECTION_NATIVE_DISPATCH_KEYS = {
    "upsert_serving_projection",
    "upsert_run_projection_link",
    "upsert_collection_authoritative_pointer",
    "upsert_projection_manifest_shard",
    "upsert_serving_projection_members",
    "replace_serving_projection_members",
    "replace_projection_person_search_index",
}
_SERVING_PROJECTION_REPOSITORY_METHODS = {
    "upsert",
    "get",
    "list",
    "upsert_run_link",
    "get_run_link",
    "list_run_links",
    "upsert_authoritative_pointer",
    "get_authoritative_pointer",
    "list_authoritative_pointers",
    "_projection_from_row",
    "_run_link_from_row",
    "_authoritative_pointer_from_row",
    "upsert_manifest_shard",
    "get_manifest_shard",
    "list_manifest_shards",
    "_manifest_shard_from_row",
    "upsert_members",
    "replace_members",
    "list_members",
    "list_members_by_identity_keys",
    "list_members_by_person_identity",
    "count_members_by_readiness",
    "get_member",
    "count_members",
    "_member_from_row",
    "_member_row_payload",
    "replace_person_search_index",
    "delete_person_search_index",
    "upsert_person_search_index_rows",
    "update_person_search_index_scope",
    "count_person_search_index",
    "search_person_index",
    "filter_person_search_index",
    "get_person_search_index_summary",
    "list_person_search_index_rows",
    "_search_person_index_rows",
    "_list_person_search_index_rows",
    "_person_search_index_from_row",
    "_person_search_index_row_payload",
    "hold_publication_lock",
}


def _class_method_names(path: Path, class_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    class_node = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name)
    return {node.name for node in class_node.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _assigned_literal_dict_keys(path: Path, assignment_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(isinstance(target, ast.Name) and target.id == assignment_name for target in targets):
            continue
        value = node.value
        if not isinstance(value, ast.Dict):
            return set()
        return {key.value for key in value.keys if isinstance(key, ast.Constant) and isinstance(key.value, str)}
    raise AssertionError(f"assignment not found: {assignment_name}")


def _is_store_receiver(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "store" or node.id.endswith("_store")
    return isinstance(node, ast.Attribute) and node.attr in {"store", "_store"}


def _retired_serving_projection_references(tree: ast.AST) -> list[tuple[int, str]]:
    offenders: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr in _RETIRED_SERVING_PROJECTION_CALL_ATTRIBUTES
            and _is_store_receiver(node.value)
        ):
            offenders.append((node.lineno, node.attr))
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and _is_store_receiver(node.args[0])
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _RETIRED_SERVING_PROJECTION_CALL_ATTRIBUTES
        ):
            offenders.append((node.lineno, f"getattr:{node.args[1].value}"))
    return offenders


def _retired_operation_control_references(tree: ast.AST) -> list[tuple[int, str]]:
    offenders: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr in _RETIRED_OPERATION_CONTROL_CALL_ATTRIBUTES
            and _is_store_receiver(node.value)
        ):
            offenders.append((node.lineno, node.attr))
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and _is_store_receiver(node.args[0])
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _RETIRED_OPERATION_CONTROL_CALL_ATTRIBUTES
        ):
            offenders.append((node.lineno, f"getattr:{node.args[1].value}"))
    return offenders


def _retired_acquisition_control_references(tree: ast.AST) -> list[tuple[int, str]]:
    offenders: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr in _RETIRED_ACQUISITION_CONTROL_CALL_ATTRIBUTES
            and _is_store_receiver(node.value)
        ):
            offenders.append((node.lineno, node.attr))
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and _is_store_receiver(node.args[0])
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _RETIRED_ACQUISITION_CONTROL_CALL_ATTRIBUTES
        ):
            offenders.append((node.lineno, f"getattr:{node.args[1].value}"))
    return offenders


def _retired_activity_spine_references(tree: ast.AST) -> list[tuple[int, str]]:
    offenders: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr in _RETIRED_ACTIVITY_SPINE_CALL_ATTRIBUTES
            and _is_store_receiver(node.value)
        ):
            offenders.append((node.lineno, node.attr))
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and _is_store_receiver(node.args[0])
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _RETIRED_ACTIVITY_SPINE_CALL_ATTRIBUTES
        ):
            offenders.append((node.lineno, f"getattr:{node.args[1].value}"))
    return offenders


def _is_workflow_read_model_store_receiver(node: ast.AST) -> bool:
    if _is_store_receiver(node):
        return True
    if isinstance(node, ast.Name):
        return node.id == "ControlPlaneStore"
    return isinstance(node, ast.Attribute) and node.attr.endswith("_store")


def _is_patch_object_call(node: ast.Call) -> bool:
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "object"
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "patch"
    )


def _retired_workflow_read_model_references(tree: ast.AST) -> list[tuple[int, str]]:
    offenders: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr in _RETIRED_WORKFLOW_READ_MODEL_CALL_ATTRIBUTES
            and _is_workflow_read_model_store_receiver(node.value)
        ):
            offenders.append((node.lineno, node.attr))
        if not isinstance(node, ast.Call):
            continue
        if (
            isinstance(node.func, ast.Name)
            and node.func.id in {"getattr", "hasattr"}
            and len(node.args) >= 2
            and _is_workflow_read_model_store_receiver(node.args[0])
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _RETIRED_WORKFLOW_READ_MODEL_CALL_ATTRIBUTES
        ):
            offenders.append((node.lineno, f"{node.func.id}:{node.args[1].value}"))
        if (
            _is_patch_object_call(node)
            and len(node.args) >= 2
            and _is_workflow_read_model_store_receiver(node.args[0])
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _RETIRED_WORKFLOW_READ_MODEL_CALL_ATTRIBUTES
        ):
            offenders.append((node.lineno, f"mock.patch.object:{node.args[1].value}"))
    return offenders


def _retired_recovery_intent_references(tree: ast.AST) -> list[tuple[int, str]]:
    offenders: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr in _RETIRED_RECOVERY_INTENT_CALL_ATTRIBUTES
            and _is_workflow_read_model_store_receiver(node.value)
        ):
            offenders.append((node.lineno, node.attr))
        if not isinstance(node, ast.Call):
            continue
        if (
            isinstance(node.func, ast.Name)
            and node.func.id in {"getattr", "hasattr"}
            and len(node.args) >= 2
            and _is_workflow_read_model_store_receiver(node.args[0])
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _RETIRED_RECOVERY_INTENT_CALL_ATTRIBUTES
        ):
            offenders.append((node.lineno, f"{node.func.id}:{node.args[1].value}"))
        if (
            _is_patch_object_call(node)
            and len(node.args) >= 2
            and _is_workflow_read_model_store_receiver(node.args[0])
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _RETIRED_RECOVERY_INTENT_CALL_ATTRIBUTES
        ):
            offenders.append((node.lineno, f"mock.patch.object:{node.args[1].value}"))
    return offenders


def _workflow_runtime_state_update_count(tree: ast.AST) -> int:
    count = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in {"update_action_state", "update_operation_state"}:
            continue
        repository = node.func.value
        if not isinstance(repository, ast.Attribute) or repository.attr != "workflow_runtime":
            continue
        repos = repository.value
        if isinstance(repos, ast.Attribute) and repos.attr == "repos":
            count += 1
    return count


def _generationless_projection_index_writes(tree: ast.AST) -> list[tuple[int, str]]:
    offenders: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        method_name = node.func.attr
        if method_name == "delete_person_search_index":
            offenders.append((node.lineno, method_name))
            continue
        if method_name not in {"replace_person_search_index", "upsert_person_search_index_rows"}:
            continue
        if not any(keyword.arg == "build_generation" for keyword in node.keywords):
            offenders.append((node.lineno, method_name))
    return offenders


def _invalid_bulk_upsert_wrapper_calls(tree: ast.AST) -> list[tuple[int, str]]:
    offenders: list[tuple[int, str]] = []
    wrapper_names = {"_call_control_plane_postgres_native", "_call_native_write"}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in wrapper_names or not node.args:
            continue
        method_arg = node.args[0]
        if not isinstance(method_arg, ast.Constant) or method_arg.value != "bulk_upsert_rows":
            continue
        keyword_names = {keyword.arg for keyword in node.keywords if keyword.arg is not None}
        if len(node.args) != 1 or not {"table_name", "rows"} <= keyword_names:
            offenders.append((node.lineno, node.func.attr))
    return offenders


class _CatalogFaultAdapter:
    def __init__(self, *, authoritative: bool, failing_method: str, prefer_read: bool = True) -> None:
        self.authoritative = authoritative
        self.failing_method = failing_method
        self.prefer_read = prefer_read
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def should_prefer_read(self, table_name: str) -> bool:
        return self.prefer_read

    def is_authoritative(self, table_name: str) -> bool:
        return self.authoritative

    def _raise_if_failing(self, method_name: str) -> None:
        if self.failing_method == method_name:
            raise RuntimeError(f"{method_name}-boom")

    def select_one(self, table_name: str, **kwargs: object) -> None:
        self._raise_if_failing("select_one")
        return None

    def select_many(self, table_name: str, **kwargs: object) -> list[dict[str, object]]:
        self._raise_if_failing("select_many")
        return []

    def upsert_row(self, table_name: str, row: dict[str, object]) -> None:
        self._raise_if_failing("upsert_row")

    def bulk_upsert_rows(
        self,
        *,
        table_name: str,
        rows: list[dict[str, object]],
        **kwargs: object,
    ) -> int:
        self._raise_if_failing("bulk_upsert_rows")
        self.calls.append(("bulk_upsert_rows", table_name, {"rows": rows, **kwargs}))
        return len(rows)

    def delete_rows(self, *, table_name: str, **kwargs: object) -> int:
        self._raise_if_failing("delete_rows")
        return 1

    def replace_rows(self, *, table_name: str, rows: list[dict[str, object]], **kwargs: object) -> int:
        self._raise_if_failing("replace_rows")
        self.calls.append(("replace_rows", table_name, {"rows": rows, **kwargs}))
        return len(rows)

    def write_serving_projection_members_with_input_revision(
        self,
        *,
        table_name: str,
        projection_id: str,
        rows: list[dict[str, object]],
        replace_members: bool,
        input_revision: str,
        **kwargs: object,
    ) -> dict[str, object]:
        method_name = "write_serving_projection_members_with_input_revision"
        self._raise_if_failing(method_name)
        self.calls.append(
            (
                method_name,
                table_name,
                {
                    "projection_id": projection_id,
                    "rows": rows,
                    "replace_members": replace_members,
                    "input_revision": input_revision,
                    **kwargs,
                },
            )
        )
        return {"applied": True, "member_count": len(rows), "input_revision": input_revision}

    def upsert_row_and_replace_rows(
        self,
        *,
        table_name: str,
        row: dict[str, object],
        replace_rows: list[dict[str, object]],
        **kwargs: object,
    ) -> dict[str, int]:
        self._raise_if_failing("upsert_row_and_replace_rows")
        return {"upserted_count": 1, "replaced_count": len(replace_rows)}

    def upsert_row_and_upsert_rows(
        self,
        *,
        table_name: str,
        row: dict[str, object],
        upsert_rows: list[dict[str, object]],
        **kwargs: object,
    ) -> dict[str, int]:
        self._raise_if_failing("upsert_row_and_upsert_rows")
        return {"upserted_count": 1, "child_upserted_count": len(upsert_rows)}

    def count_rows(self, table_name: str, **kwargs: object) -> int:
        self._raise_if_failing("count_rows")
        return 0


class _OperationControlFaultAdapter:
    mode = "postgres_only"

    def __init__(self, *, authoritative: bool, failing_method: str, prefer_read: bool = True) -> None:
        self.authoritative = authoritative
        self.failing_method = failing_method
        self.prefer_read = prefer_read

    def should_prefer_read(self, _table_name: str) -> bool:
        return self.prefer_read

    def is_authoritative(self, _table_name: str) -> bool:
        return self.authoritative

    def _raise_if_failing(self, method_name: str) -> None:
        if self.failing_method == method_name:
            raise RuntimeError(f"{method_name}-boom")

    def select_one(self, _table_name: str, **_kwargs: object) -> None:
        self._raise_if_failing("select_one")
        return None

    def select_many(self, _table_name: str, **_kwargs: object) -> list[dict[str, object]]:
        self._raise_if_failing("select_many")
        return []

    def upsert_agent_action(self, *, table_name: str, row: dict[str, object]) -> None:
        self._raise_if_failing("upsert_agent_action")
        return None

    def upsert_workflow_runtime_identity_row(
        self,
        *,
        table_name: str,
        row: dict[str, object],
        **_kwargs: object,
    ) -> None:
        self._raise_if_failing("upsert_workflow_runtime_identity_row")
        return None

    def cancel_acquisition_owner_command(
        self,
        command_id: str,
        *,
        table_name: str = "workflow_commands",
        **_kwargs: object,
    ) -> None:
        self._raise_if_failing("cancel_acquisition_owner_command")
        return None


class _WorkflowReadModelFaultAdapter:
    mode = "postgres_only"

    def __init__(self, *, failing_method: str) -> None:
        self.failing_method = failing_method

    def should_prefer_read(self, _table_name: str) -> bool:
        return True

    def is_authoritative(self, _table_name: str) -> bool:
        return True

    def _raise_if_failing(self, method_name: str) -> None:
        if self.failing_method == method_name:
            raise RuntimeError(f"{method_name}-boom")

    def select_one(self, _table_name: str, **_kwargs: object) -> None:
        self._raise_if_failing("select_one")
        return None

    def select_many(self, _table_name: str, **_kwargs: object) -> list[dict[str, object]]:
        self._raise_if_failing("select_many")
        return []

    def append_workflow_event(self, *, table_name: str, row: dict[str, object]) -> None:
        self._raise_if_failing("append_workflow_event")
        return None

    def upsert_workflow_current_state(self, *, table_name: str, row: dict[str, object]) -> None:
        self._raise_if_failing("upsert_workflow_current_state")
        return None

    def enqueue_runtime_outbox(self, *, table_name: str, row: dict[str, object]) -> None:
        self._raise_if_failing("enqueue_runtime_outbox")
        return None

    def mark_runtime_outbox_dispatched(self, *, table_name: str, outbox_id: str) -> None:
        self._raise_if_failing("mark_runtime_outbox_dispatched")
        return None


class _RecoveryIntentFaultAdapter:
    mode = "postgres_only"

    def __init__(self, *, failing_method: str) -> None:
        self.failing_method = failing_method

    def should_prefer_read(self, _table_name: str) -> bool:
        return True

    def is_authoritative(self, _table_name: str) -> bool:
        return True

    def _raise_if_failing(self, method_name: str) -> None:
        if self.failing_method == method_name:
            raise RuntimeError(f"{method_name}-boom")

    def select_one(self, _table_name: str, **_kwargs: object) -> None:
        self._raise_if_failing("select_one")
        return None

    def upsert_workflow_recovery_intent(self, *, table_name: str, **_kwargs: object) -> None:
        self._raise_if_failing("upsert_workflow_recovery_intent")
        return None

    def claim_workflow_recovery_intents(self, *, table_name: str, **_kwargs: object) -> None:
        self._raise_if_failing("claim_workflow_recovery_intents")
        return None

    def mark_workflow_recovery_intent_consumed(self, *, table_name: str, **_kwargs: object) -> None:
        self._raise_if_failing("mark_workflow_recovery_intent_consumed")
        return None


class _BulkUpsertRecordingAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[dict[str, object]]]] = []

    def should_prefer_read(self, _table_name: str) -> bool:
        return True

    def is_authoritative(self, _table_name: str) -> bool:
        return True

    def bulk_upsert_rows(
        self,
        *,
        table_name: str,
        rows: list[dict[str, object]],
        **kwargs: object,
    ) -> int:
        self.calls.append((table_name, rows))
        return len(rows)


class _WorkflowEvidenceBatchRecordingAdapter:
    mode = "postgres_only"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.rows_by_table = {
            "workflow_activity_runs": {
                "activity-1": {
                    "activity_run_id": "activity-1",
                    "workflow_run_id": "workflow-1",
                    "command_id": "command-1",
                    "activity_type": "test.activity",
                    "owner": "test-owner",
                },
                "activity-2": {
                    "activity_run_id": "activity-2",
                    "workflow_run_id": "workflow-1",
                    "command_id": "command-2",
                    "activity_type": "test.activity",
                    "owner": "test-owner",
                },
            },
            "workflow_commands": {
                "command-1": {
                    "command_id": "command-1",
                    "workflow_run_id": "workflow-1",
                    "command_type": "test.command",
                    "owner": "test-owner",
                    "status": "running",
                },
                "command-2": {
                    "command_id": "command-2",
                    "workflow_run_id": "workflow-1",
                    "command_type": "test.command",
                    "owner": "test-owner",
                    "status": "running",
                },
            },
        }

    def should_prefer_read(self, _table_name: str) -> bool:
        return True

    def is_authoritative(self, _table_name: str) -> bool:
        return True

    def select_many(self, table_name: str, **kwargs: object) -> list[dict[str, object]]:
        self.calls.append((table_name, dict(kwargs)))
        rows_by_id = self.rows_by_table[table_name]
        return [rows_by_id[value] for value in reversed(list(kwargs.get("params") or [])) if value in rows_by_id]


def _runtime_error(callable_) -> RuntimeError:
    with pytest.raises(RuntimeError) as raised:
        callable_()
    return raised.value


def test_production_code_does_not_reintroduce_sqlitestore_facade_name() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts"]
    offenders: list[str] = []
    for root in checked_roots:
        for path in root.rglob("*.py"):
            if path.name == "__pycache__":
                continue
            if "SQLiteStore" in path.read_text(encoding="utf-8"):
                offenders.append(str(path.relative_to(repo_root)))

    assert offenders == []


def test_production_code_uses_storage_neutral_linkedin_url_normalization() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts"]
    offenders: list[str] = []
    for root in checked_roots:
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if ".normalize_linkedin_profile_url(" in text:
                offenders.append(str(path.relative_to(repo_root)))

    assert offenders == []


def test_production_code_does_not_reintroduce_retired_sqlite_snapshot_entrypoints() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts"]
    retired_tokens = {
        "export-sqlite-snapshot",
        "restore-sqlite-snapshot",
        "--confirm-legacy-sqlite",
        "--allow-sqlite-snapshot-restore",
        "--with-sqlite-backup",
        "--without-sqlite",
        "SOURCING_ENABLE_SQLITE_PROFILE_REGISTRY_FALLBACK",
        "--source-db-path",
        "export_sqlite_snapshot",
        "restore_sqlite_snapshot",
    }
    offenders: list[str] = []
    for root in checked_roots:
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if any(token in text for token in retired_tokens):
                offenders.append(str(path.relative_to(repo_root)))

    assert offenders == []


def test_manual_review_storage_facade_is_retired_to_repository() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    storage_methods = _class_method_names(repo_root / "src" / "sourcing_agent" / "storage.py", "ControlPlaneStore")
    repository_methods = _class_method_names(
        repo_root / "src" / "sourcing_agent" / "repositories" / "manual_review.py",
        "ManualReviewRepository",
    )
    namespace_source = (repo_root / "src" / "sourcing_agent" / "repositories" / "__init__.py").read_text(
        encoding="utf-8"
    )

    assert storage_methods.isdisjoint(_RETIRED_MANUAL_REVIEW_STORE_METHODS)
    assert _MANUAL_REVIEW_REPOSITORY_METHODS <= repository_methods
    assert "self.manual_review = ManualReviewRepository(adapter)" in namespace_source


def test_manual_review_repository_mapper_preserves_nullable_and_json_error_contract() -> None:
    repository = ManualReviewRepository(object())
    row = {
        "review_item_id": 7,
        "job_id": None,
        "candidate_id": "candidate-1",
        "target_company": None,
        "review_type": "manual_identity_resolution",
        "priority": "high",
        "status": "open",
        "summary": None,
        "candidate_json": '{"name":"Test"}',
        "evidence_json": "[]",
        "metadata_json": '{"source":"manual"}',
        "reviewed_by": None,
        "review_notes": None,
        "reviewed_at": None,
        "created_at": "2026-07-10 00:00:00",
        "updated_at": "2026-07-10 00:00:00",
    }

    mapped = repository._item_from_row(row)
    assert mapped["job_id"] is None
    assert mapped["target_company"] is None
    assert mapped["summary"] is None
    assert mapped["metadata"] == {"source": "manual"}

    with pytest.raises(json.JSONDecodeError):
        repository._item_from_row({**row, "metadata_json": "{"})


def test_operation_control_storage_facade_is_retired_to_workflow_runtime_repository() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    storage_path = repo_root / "src" / "sourcing_agent" / "storage.py"
    repository_path = repo_root / "src" / "sourcing_agent" / "repositories" / "workflow_runtime.py"
    storage_methods = _class_method_names(storage_path, "ControlPlaneStore")
    repository_methods = _class_method_names(repository_path, "WorkflowRuntimeRepository")
    namespace_source = (repo_root / "src" / "sourcing_agent" / "repositories" / "__init__.py").read_text(
        encoding="utf-8"
    )
    descriptor_dispatch_keys = _assigned_literal_dict_keys(repository_path, "FROM_ROW_DESCRIPTORS")

    assert storage_methods.isdisjoint(_RETIRED_OPERATION_CONTROL_STORE_METHODS)
    assert _OPERATION_CONTROL_REPOSITORY_METHODS <= repository_methods
    assert "self.workflow_runtime = WorkflowRuntimeRepository(adapter)" in namespace_source
    assert descriptor_dispatch_keys.isdisjoint(
        {"_agent_action_from_row", "_operation_run_from_row", "_operation_event_from_row"}
    )


def test_operation_control_retired_calls_and_native_dispatch_keys_cannot_return() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    synthetic = ast.parse(
        "\n".join(
            [
                'store.get_agent_action("action")',
                "callback = api_store.list_operation_runs",
                'dynamic = getattr(self.store, "update_operation_run_state")',
                'store.repos.workflow_runtime.get_action("action")',
            ]
        )
    )
    assert {label for _line, label in _retired_operation_control_references(synthetic)} == {
        "get_agent_action",
        "list_operation_runs",
        "getattr:update_operation_run_state",
    }

    offenders: list[str] = []
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts", repo_root / "tests"]
    current_test = Path(__file__).resolve()
    for root in checked_roots:
        for path in root.rglob("*.py"):
            if path.resolve() == current_test:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            offenders.extend(
                f"{path.relative_to(repo_root)}:{line}:{label}"
                for line, label in _retired_operation_control_references(tree)
            )

    native_dispatch_keys = _assigned_literal_dict_keys(
        repo_root / "src" / "sourcing_agent" / "storage.py",
        "_CONTROL_PLANE_POSTGRES_NATIVE_TABLES",
    )
    assert offenders == []
    assert native_dispatch_keys.isdisjoint(_RETIRED_OPERATION_CONTROL_NATIVE_DISPATCH_KEYS)


def test_operation_control_native_writers_require_explicit_authority_table() -> None:
    from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter

    expected_tables = {
        "upsert_agent_action": "agent_actions",
        "update_agent_action_state": "agent_actions",
        "upsert_operation_run": "operation_runs",
        "update_operation_run_state": "operation_runs",
        "append_operation_event": "operation_events",
        "reject_agent_action_with_event": "agent_actions",
        "cancel_operation_run_with_event": "operation_runs",
        "fail_operation_run_for_stale_input_with_event": "operation_runs",
        "hold_operation_dispatch_lock": "operation_runs",
        "hold_serving_projection_publication_lock": "serving_projections",
    }
    for method_name, table_name in expected_tables.items():
        parameter = inspect.signature(getattr(LiveControlPlanePostgresAdapter, method_name)).parameters["table_name"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default == table_name


def test_operation_control_repository_preserves_authority_fault_contracts() -> None:
    read_repository = WorkflowRuntimeRepository(
        _OperationControlFaultAdapter(authoritative=True, failing_method="select_one")
    )
    with pytest.raises(
        RuntimeError,
        match=("Postgres authoritative read failed for agent_actions via select_one: RuntimeError: select_one-boom"),
    ):
        read_repository.get_action("action-1")

    write_repository = WorkflowRuntimeRepository(
        _OperationControlFaultAdapter(authoritative=True, failing_method="upsert_agent_action")
    )
    with pytest.raises(
        RuntimeError,
        match=(
            "Postgres authoritative write failed for agent_actions via upsert_agent_action: "
            "RuntimeError: upsert_agent_action-boom"
        ),
    ):
        write_repository.upsert_action(
            action_id="action-1",
            action_type="candidate.lookup",
            owner_module="operation_runtime",
            operation_type="projection.read",
            idempotency_key="action-idem-1",
        )

    tier_b_repository = WorkflowRuntimeRepository(
        _OperationControlFaultAdapter(
            authoritative=False,
            failing_method="select_one",
            prefer_read=False,
        )
    )
    assert tier_b_repository.get_action("action-1") == {}
    assert tier_b_repository.list_actions() == []


def test_operation_control_repository_preserves_explicit_postgres_only_gate() -> None:
    repository = WorkflowRuntimeRepository(SimpleNamespace(mode="prefer_postgres"))
    with pytest.raises(RuntimeError, match="agent_actions is PG-only durable runtime storage"):
        repository.get_action("action-1")


def test_acquisition_control_storage_facade_is_retired_to_workflow_runtime_repository() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    storage_path = repo_root / "src" / "sourcing_agent" / "storage.py"
    repository_path = repo_root / "src" / "sourcing_agent" / "repositories" / "workflow_runtime.py"
    storage_methods = _class_method_names(storage_path, "ControlPlaneStore")
    repository_methods = _class_method_names(repository_path, "WorkflowRuntimeRepository")
    descriptor_dispatch_keys = _assigned_literal_dict_keys(repository_path, "FROM_ROW_DESCRIPTORS")

    assert storage_methods.isdisjoint(_RETIRED_ACQUISITION_CONTROL_STORE_METHODS)
    assert _ACQUISITION_CONTROL_REPOSITORY_METHODS <= repository_methods
    assert descriptor_dispatch_keys.isdisjoint({"_acquisition_run_from_row", "_acquisition_discovery_lane_from_row"})


def test_workflow_runtime_identity_writer_requires_explicit_contract_handshake() -> None:
    from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter

    parameters = inspect.signature(LiveControlPlanePostgresAdapter.upsert_workflow_runtime_identity_row).parameters
    for name in ("table_name", "id_column", "immutable_columns", "terminal_statuses", "write_once"):
        assert parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        assert parameters[name].default is inspect.Parameter.empty


def test_acquisition_owner_cancel_writer_is_bound_to_workflow_commands() -> None:
    from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter

    parameter = inspect.signature(LiveControlPlanePostgresAdapter.cancel_acquisition_owner_command).parameters[
        "table_name"
    ]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default == "workflow_commands"


def test_acquisition_control_repository_preserves_authority_write_fault_contract() -> None:
    payload = {
        "acquisition_run_id": "acqrun-fault",
        "workflow_run_id": "wf-fault",
        "idempotency_key": "acquisition_run:fault",
    }
    repository = WorkflowRuntimeRepository(
        _OperationControlFaultAdapter(
            authoritative=True,
            failing_method="upsert_workflow_runtime_identity_row",
        )
    )
    with pytest.raises(
        RuntimeError,
        match=(
            "Postgres authoritative write failed for acquisition_runs via "
            "upsert_workflow_runtime_identity_row: RuntimeError: upsert_workflow_runtime_identity_row-boom"
        ),
    ):
        repository.upsert_acquisition_run(payload)

    no_confirmation_repository = WorkflowRuntimeRepository(
        _OperationControlFaultAdapter(
            authoritative=True,
            failing_method="",
        )
    )
    with pytest.raises(
        RuntimeError,
        match=(
            "Postgres authoritative write failed for acquisition_runs via "
            "upsert_workflow_runtime_identity_row: postgres-only: authoritative upsert returned no row"
        ),
    ):
        no_confirmation_repository.upsert_acquisition_run(payload)


def test_acquisition_owner_cancel_preserves_authority_write_fault_contract() -> None:
    repository = WorkflowRuntimeRepository(
        _OperationControlFaultAdapter(
            authoritative=True,
            failing_method="cancel_acquisition_owner_command",
        )
    )
    with pytest.raises(
        RuntimeError,
        match=(
            "Postgres authoritative write failed for workflow_commands via "
            "cancel_acquisition_owner_command: RuntimeError: cancel_acquisition_owner_command-boom"
        ),
    ):
        repository.cancel_acquisition_owner_command(
            "command-1",
            cancel_kind="plan_commit",
            actor="api",
            reason="operator_cancelled",
        )

    no_confirmation_repository = WorkflowRuntimeRepository(
        _OperationControlFaultAdapter(authoritative=True, failing_method="")
    )
    with pytest.raises(
        RuntimeError,
        match=(
            "Postgres authoritative write failed for workflow_commands via "
            "cancel_acquisition_owner_command: postgres-only: authoritative cancellation returned no outcome"
        ),
    ):
        no_confirmation_repository.cancel_acquisition_owner_command(
            "command-1",
            cancel_kind="plan_commit",
            actor="api",
            reason="operator_cancelled",
        )


def test_acquisition_control_retired_store_calls_cannot_return() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    synthetic = ast.parse(
        "\n".join(
            [
                'store.get_acquisition_run("run")',
                "callback = api_store.list_acquisition_discovery_lanes",
                'dynamic = getattr(self.store, "upsert_acquisition_run")',
                'store.repos.workflow_runtime.get_acquisition_run("run")',
            ]
        )
    )
    assert {label for _line, label in _retired_acquisition_control_references(synthetic)} == {
        "get_acquisition_run",
        "list_acquisition_discovery_lanes",
        "getattr:upsert_acquisition_run",
    }

    offenders: list[str] = []
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts", repo_root / "tests"]
    current_test = Path(__file__).resolve()
    for root in checked_roots:
        for path in root.rglob("*.py"):
            if path.resolve() == current_test:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            offenders.extend(
                f"{path.relative_to(repo_root)}:{line}:{label}"
                for line, label in _retired_acquisition_control_references(tree)
            )

    assert offenders == []


def test_activity_spine_storage_facade_is_retired_to_workflow_runtime_repository() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    storage_path = repo_root / "src" / "sourcing_agent" / "storage.py"
    repository_path = repo_root / "src" / "sourcing_agent" / "repositories" / "workflow_runtime.py"
    storage_methods = _class_method_names(storage_path, "ControlPlaneStore")
    repository_methods = _class_method_names(repository_path, "WorkflowRuntimeRepository")
    descriptor_dispatch_keys = _assigned_literal_dict_keys(repository_path, "FROM_ROW_DESCRIPTORS")

    assert storage_methods.isdisjoint(_RETIRED_ACTIVITY_SPINE_STORE_METHODS)
    assert _ACTIVITY_SPINE_REPOSITORY_METHODS <= repository_methods
    assert descriptor_dispatch_keys.isdisjoint(
        {
            "_workflow_activity_run_from_row",
            "_workflow_activity_attempt_from_row",
            "_workflow_entity_delta_from_row",
        }
    )


def test_workflow_evidence_batch_readers_are_bounded_single_query_and_request_ordered() -> None:
    adapter = _WorkflowEvidenceBatchRecordingAdapter()
    repository = WorkflowRuntimeRepository(adapter)
    store = object.__new__(ControlPlaneStore)
    store._control_plane_postgres = adapter

    activities = repository.list_activity_runs_by_ids(
        [" activity-2 ", "activity-1", "activity-2", "missing-activity", ""]
    )
    commands = store.list_workflow_commands_by_ids((" command-2 ", "command-1", "command-2", "missing-command", ""))

    assert [row["activity_run_id"] for row in activities] == ["activity-2", "activity-1"]
    assert [row["command_id"] for row in commands] == ["command-2", "command-1"]
    assert adapter.calls == [
        (
            "workflow_activity_runs",
            {
                "where_sql": "activity_run_id IN (%s, %s, %s)",
                "params": ["activity-2", "activity-1", "missing-activity"],
                "order_by_sql": "",
                "limit": 3,
                "offset": 0,
            },
        ),
        (
            "workflow_commands",
            {
                "where_sql": "command_id IN (%s, %s, %s)",
                "params": ["command-2", "command-1", "missing-command"],
                "order_by_sql": "",
                "limit": 3,
                "offset": 0,
            },
        ),
    ]

    adapter.calls.clear()
    assert repository.list_activity_runs_by_ids([]) == []
    assert store.list_workflow_commands_by_ids(()) == []
    with pytest.raises(ValueError, match="activity_run_ids supports at most 500 identifiers per batch"):
        repository.list_activity_runs_by_ids([f"activity-{index}" for index in range(501)])
    with pytest.raises(ValueError, match="command_ids supports at most 500 identifiers per batch"):
        store.list_workflow_commands_by_ids([f"command-{index}" for index in range(501)])
    assert adapter.calls == []


def test_activity_spine_retired_store_calls_cannot_return() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    synthetic = ast.parse(
        "\n".join(
            [
                'store.get_workflow_activity_run("run")',
                "callback = api_store.list_workflow_activity_attempts",
                'dynamic = getattr(self.store, "upsert_workflow_entity_delta")',
                'store.repos.workflow_runtime.get_activity_run("run")',
                'api.get_workflow_activity_attempt("attempt")',
            ]
        )
    )
    assert {label for _line, label in _retired_activity_spine_references(synthetic)} == {
        "get_workflow_activity_run",
        "list_workflow_activity_attempts",
        "getattr:upsert_workflow_entity_delta",
    }

    offenders: list[str] = []
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts", repo_root / "tests"]
    current_test = Path(__file__).resolve()
    for root in checked_roots:
        for path in root.rglob("*.py"):
            if path.resolve() == current_test:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            offenders.extend(
                f"{path.relative_to(repo_root)}:{line}:{label}"
                for line, label in _retired_activity_spine_references(tree)
            )

    assert offenders == []


def test_workflow_read_model_storage_facade_is_retired_to_workflow_runtime_repository() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    storage_path = repo_root / "src" / "sourcing_agent" / "storage.py"
    repository_path = repo_root / "src" / "sourcing_agent" / "repositories" / "workflow_runtime.py"
    storage_methods = _class_method_names(storage_path, "ControlPlaneStore")
    repository_methods = _class_method_names(repository_path, "WorkflowRuntimeRepository")
    descriptor_dispatch_keys = _assigned_literal_dict_keys(repository_path, "FROM_ROW_DESCRIPTORS")

    assert len(_RETIRED_WORKFLOW_READ_MODEL_STORE_METHODS) == 9
    assert storage_methods.isdisjoint(_RETIRED_WORKFLOW_READ_MODEL_STORE_METHODS)
    assert len(_WORKFLOW_READ_MODEL_REPOSITORY_METHODS) == 9
    assert _WORKFLOW_READ_MODEL_REPOSITORY_METHODS <= repository_methods
    assert descriptor_dispatch_keys.isdisjoint(
        {
            "_workflow_event_from_row",
            "_workflow_current_state_from_row",
            "_runtime_outbox_from_row",
        }
    )


def test_workflow_read_model_retired_calls_and_native_dispatch_keys_cannot_return() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    synthetic = ast.parse(
        "\n".join(
            [
                'store.append_workflow_event(workflow_run_id="run")',
                "callback = api_store.list_workflow_events",
                'dynamic = getattr(self._store, "get_workflow_current_state")',
                'feature = hasattr(self.store, "enqueue_runtime_outbox")',
                'patcher = mock.patch.object(store, "mark_runtime_outbox_dispatched")',
                'ControlPlaneStore.upsert_workflow_current_state(store, workflow_run_id="run")',
                'store.repos.workflow_runtime.append_workflow_event(workflow_run_id="run")',
                'adapter.append_workflow_event({}, table_name="workflow_events")',
                'live_pg_adapter.enqueue_runtime_outbox({}, table_name="runtime_outbox")',
            ]
        )
    )
    assert {label for _line, label in _retired_workflow_read_model_references(synthetic)} == {
        "append_workflow_event",
        "list_workflow_events",
        "getattr:get_workflow_current_state",
        "hasattr:enqueue_runtime_outbox",
        "mock.patch.object:mark_runtime_outbox_dispatched",
        "upsert_workflow_current_state",
    }

    offenders: list[str] = []
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts", repo_root / "tests"]
    current_test = Path(__file__).resolve()
    for root in checked_roots:
        for path in root.rglob("*.py"):
            if path.resolve() == current_test:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            offenders.extend(
                f"{path.relative_to(repo_root)}:{line}:{label}"
                for line, label in _retired_workflow_read_model_references(tree)
            )

    native_dispatch_keys = _assigned_literal_dict_keys(
        repo_root / "src" / "sourcing_agent" / "storage.py",
        "_CONTROL_PLANE_POSTGRES_NATIVE_TABLES",
    )
    assert offenders == []
    assert native_dispatch_keys.isdisjoint(_RETIRED_WORKFLOW_READ_MODEL_NATIVE_DISPATCH_KEYS)


@pytest.mark.parametrize(
    ("method_name", "table_name", "invoke_with_wrong_table"),
    [
        (
            "append_workflow_event",
            "workflow_events",
            lambda adapter: adapter.append_workflow_event({}, table_name="runtime_outbox"),
        ),
        (
            "upsert_workflow_current_state",
            "workflow_current_state",
            lambda adapter: adapter.upsert_workflow_current_state({}, table_name="workflow_events"),
        ),
        (
            "enqueue_runtime_outbox",
            "runtime_outbox",
            lambda adapter: adapter.enqueue_runtime_outbox({}, table_name="workflow_events"),
        ),
        (
            "mark_runtime_outbox_dispatched",
            "runtime_outbox",
            lambda adapter: adapter.mark_runtime_outbox_dispatched("outbox-1", table_name="workflow_events"),
        ),
    ],
)
def test_workflow_read_model_native_writers_require_exact_authority_table(
    method_name: str,
    table_name: str,
    invoke_with_wrong_table,
) -> None:
    from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter

    method = getattr(LiveControlPlanePostgresAdapter, method_name)
    parameter = inspect.signature(method).parameters["table_name"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default == table_name

    adapter = object.__new__(LiveControlPlanePostgresAdapter)
    with pytest.raises(ValueError, match=rf"{method_name} requires table_name={table_name}"):
        invoke_with_wrong_table(adapter)


def test_workflow_read_model_repository_preserves_authority_fault_contracts() -> None:
    read_cases = [
        (
            "workflow_events",
            "select_many",
            lambda repository: repository.list_workflow_events("workflow-run-1"),
        ),
        (
            "workflow_current_state",
            "select_one",
            lambda repository: repository.get_workflow_current_state("workflow-run-1"),
        ),
    ]
    write_cases = [
        (
            "workflow_events",
            "append_workflow_event",
            lambda repository: repository.append_workflow_event(
                workflow_run_id="workflow-run-1",
                event_family="workflow",
                event_type="workflow.started",
                idempotency_key="event-idem-1",
            ),
        ),
        (
            "workflow_current_state",
            "upsert_workflow_current_state",
            lambda repository: repository.upsert_workflow_current_state(
                workflow_run_id="workflow-run-1",
                status="running",
            ),
        ),
        (
            "runtime_outbox",
            "enqueue_runtime_outbox",
            lambda repository: repository.enqueue_runtime_outbox(
                workflow_run_id="workflow-run-1",
                outbox_type="workflow.wakeup",
                idempotency_key="outbox-idem-1",
            ),
        ),
        (
            "runtime_outbox",
            "mark_runtime_outbox_dispatched",
            lambda repository: repository.mark_runtime_outbox_dispatched("outbox-1"),
        ),
    ]

    for table_name, primitive, call in read_cases:
        repository = WorkflowRuntimeRepository(_WorkflowReadModelFaultAdapter(failing_method=primitive))
        error = _runtime_error(lambda: call(repository))
        assert str(error) == (
            f"Postgres authoritative read failed for {table_name} via {primitive}: RuntimeError: {primitive}-boom"
        )
        assert isinstance(error.__cause__, RuntimeError)

    for table_name, primitive, call in write_cases:
        repository = WorkflowRuntimeRepository(_WorkflowReadModelFaultAdapter(failing_method=primitive))
        error = _runtime_error(lambda: call(repository))
        assert str(error) == (
            f"Postgres authoritative write failed for {table_name} via {primitive}: RuntimeError: {primitive}-boom"
        )
        assert isinstance(error.__cause__, RuntimeError)


@pytest.mark.parametrize(
    ("method_name", "invoke"),
    [
        (
            "append_workflow_event",
            lambda repository: repository.append_workflow_event(
                workflow_run_id="workflow-run-1",
                event_family="workflow",
                event_type="workflow.started",
                idempotency_key="event-idem-1",
            ),
        ),
        (
            "enqueue_runtime_outbox",
            lambda repository: repository.enqueue_runtime_outbox(
                workflow_run_id="workflow-run-1",
                outbox_type="workflow.wakeup",
                idempotency_key="outbox-idem-1",
            ),
        ),
    ],
)
def test_workflow_read_model_authoritative_native_writes_require_returned_rows(method_name: str, invoke) -> None:
    repository = WorkflowRuntimeRepository(_WorkflowReadModelFaultAdapter(failing_method=""))

    error = _runtime_error(lambda: invoke(repository))

    assert method_name in str(error)
    assert "returned no row" in str(error)


def test_recovery_intent_storage_facade_is_retired_to_workflow_runtime_repository() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    storage_path = repo_root / "src" / "sourcing_agent" / "storage.py"
    repository_path = repo_root / "src" / "sourcing_agent" / "repositories" / "workflow_runtime.py"
    storage_methods = _class_method_names(storage_path, "ControlPlaneStore")
    repository_methods = _class_method_names(repository_path, "WorkflowRuntimeRepository")
    descriptor_dispatch_keys = _assigned_literal_dict_keys(repository_path, "FROM_ROW_DESCRIPTORS")

    assert len(_RETIRED_RECOVERY_INTENT_STORE_METHODS) == 5
    assert storage_methods.isdisjoint(_RETIRED_RECOVERY_INTENT_STORE_METHODS)
    assert len(_RECOVERY_INTENT_REPOSITORY_METHODS) == 5
    assert _RECOVERY_INTENT_REPOSITORY_METHODS <= repository_methods
    assert descriptor_dispatch_keys.isdisjoint({"_workflow_recovery_intent_from_row"})


def test_recovery_intent_retired_calls_and_native_dispatch_keys_cannot_return() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    synthetic = ast.parse(
        "\n".join(
            [
                'store.upsert_workflow_recovery_intent("job-1")',
                "callback = api_store.claim_workflow_recovery_intents",
                'dynamic = getattr(self._store, "get_workflow_recovery_intent")',
                'feature = hasattr(self.store, "mark_workflow_recovery_intent_consumed")',
                'patcher = mock.patch.object(store, "claim_workflow_recovery_intents")',
                'ControlPlaneStore.get_workflow_recovery_intent(store, "job-1")',
                'store.repos.workflow_runtime.upsert_recovery_intent("job-1")',
                'adapter.upsert_workflow_recovery_intent(job_id="job-1", table_name="workflow_recovery_intents")',
                "live_pg_adapter.claim_workflow_recovery_intents("
                'lease_owner="daemon-1", table_name="workflow_recovery_intents")',
            ]
        )
    )
    assert {label for _line, label in _retired_recovery_intent_references(synthetic)} == {
        "upsert_workflow_recovery_intent",
        "claim_workflow_recovery_intents",
        "getattr:get_workflow_recovery_intent",
        "hasattr:mark_workflow_recovery_intent_consumed",
        "mock.patch.object:claim_workflow_recovery_intents",
        "get_workflow_recovery_intent",
    }

    offenders: list[str] = []
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts", repo_root / "tests"]
    current_test = Path(__file__).resolve()
    for root in checked_roots:
        for path in root.rglob("*.py"):
            if path.resolve() == current_test:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            offenders.extend(
                f"{path.relative_to(repo_root)}:{line}:{label}"
                for line, label in _retired_recovery_intent_references(tree)
            )

    native_dispatch_keys = _assigned_literal_dict_keys(
        repo_root / "src" / "sourcing_agent" / "storage.py",
        "_CONTROL_PLANE_POSTGRES_NATIVE_TABLES",
    )
    assert offenders == []
    assert native_dispatch_keys.isdisjoint(_RETIRED_RECOVERY_INTENT_NATIVE_DISPATCH_KEYS)


@pytest.mark.parametrize(
    ("method_name", "invoke_with_wrong_table"),
    [
        (
            "upsert_workflow_recovery_intent",
            lambda adapter: adapter.upsert_workflow_recovery_intent("job-1", table_name="workflow_events"),
        ),
        (
            "claim_workflow_recovery_intents",
            lambda adapter: adapter.claim_workflow_recovery_intents(
                lease_owner="daemon-1", table_name="workflow_events"
            ),
        ),
        (
            "mark_workflow_recovery_intent_consumed",
            lambda adapter: adapter.mark_workflow_recovery_intent_consumed("job-1", table_name="workflow_events"),
        ),
    ],
)
def test_recovery_intent_native_writers_require_exact_authority_table(
    method_name: str,
    invoke_with_wrong_table,
) -> None:
    from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter

    method = getattr(LiveControlPlanePostgresAdapter, method_name)
    parameter = inspect.signature(method).parameters["table_name"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default == "workflow_recovery_intents"

    adapter = object.__new__(LiveControlPlanePostgresAdapter)
    with pytest.raises(
        ValueError,
        match=rf"{method_name} requires table_name=workflow_recovery_intents",
    ):
        invoke_with_wrong_table(adapter)


def test_recovery_intent_repository_preserves_postgres_only_and_authority_fault_contracts() -> None:
    repository = WorkflowRuntimeRepository(SimpleNamespace(mode="prefer_postgres"))
    with pytest.raises(RuntimeError, match="workflow_recovery_intents is PG-only durable runtime storage"):
        repository.get_recovery_intent("job-1")

    cases = [
        (
            "select_one",
            lambda recovery_repository: recovery_repository.get_recovery_intent("job-1"),
            "read",
        ),
        (
            "upsert_workflow_recovery_intent",
            lambda recovery_repository: recovery_repository.upsert_recovery_intent("job-1"),
            "write",
        ),
        (
            "claim_workflow_recovery_intents",
            lambda recovery_repository: recovery_repository.claim_recovery_intents(lease_owner="daemon-1"),
            "write",
        ),
        (
            "mark_workflow_recovery_intent_consumed",
            lambda recovery_repository: recovery_repository.mark_recovery_intent_consumed(
                "job-1",
                lease_owner="daemon-1",
                claimed_at="2026-07-13T00:00:00Z",
            ),
            "write",
        ),
    ]
    for primitive, call, operation in cases:
        recovery_repository = WorkflowRuntimeRepository(_RecoveryIntentFaultAdapter(failing_method=primitive))
        error = _runtime_error(lambda: call(recovery_repository))
        assert str(error) == (
            f"Postgres authoritative {operation} failed for workflow_recovery_intents "
            f"via {primitive}: RuntimeError: {primitive}-boom"
        )
        assert isinstance(error.__cause__, RuntimeError)


def test_recovery_intent_authoritative_returned_row_contracts_preserve_fenced_noop() -> None:
    repository = WorkflowRuntimeRepository(_RecoveryIntentFaultAdapter(failing_method=""))

    upsert_error = _runtime_error(lambda: repository.upsert_recovery_intent("job-1"))
    assert "upsert_workflow_recovery_intent" in str(upsert_error)
    assert "returned no row" in str(upsert_error)

    claim_error = _runtime_error(lambda: repository.claim_recovery_intents(lease_owner="daemon-1"))
    assert "claim_workflow_recovery_intents" in str(claim_error)
    assert "returned no rows" in str(claim_error)

    assert (
        repository.mark_recovery_intent_consumed(
            "job-1",
            lease_owner="daemon-1",
            claimed_at="2026-07-13T00:00:00Z",
        )
        == {}
    )
    assert repository.get_recovery_intent("missing-job") == {}


def test_operation_state_sync_residual_callers_are_ratcheted() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_root = repo_root / "src" / "sourcing_agent"
    counts = {
        str(path.relative_to(repo_root)): count
        for path in source_root.rglob("*.py")
        if (count := _workflow_runtime_state_update_count(ast.parse(path.read_text(encoding="utf-8"))))
    }
    assert set(counts) <= {
        "src/sourcing_agent/acquisition_command_owner.py",
        "src/sourcing_agent/command_kernel.py",
        "src/sourcing_agent/crm_public_web_owner.py",
        "src/sourcing_agent/operation_runtime.py",
        "src/sourcing_agent/orchestrator.py",
    }
    assert sum(counts.values()) <= 26


def test_serving_projection_catalog_storage_facade_is_retired_to_repository() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    storage_methods = _class_method_names(repo_root / "src" / "sourcing_agent" / "storage.py", "ControlPlaneStore")
    repository_methods = _class_method_names(
        repo_root / "src" / "sourcing_agent" / "repositories" / "serving_projection.py",
        "ServingProjectionRepository",
    )
    namespace_source = (repo_root / "src" / "sourcing_agent" / "repositories" / "__init__.py").read_text(
        encoding="utf-8"
    )

    assert storage_methods.isdisjoint(_RETIRED_SERVING_PROJECTION_STORE_METHODS)
    assert _SERVING_PROJECTION_REPOSITORY_METHODS <= repository_methods
    assert "self.serving_projection = ServingProjectionRepository(adapter)" in namespace_source


def test_serving_projection_duck_accessor_never_falls_back_to_retired_store_facade() -> None:
    repository = object()
    canonical_store = SimpleNamespace(repos=SimpleNamespace(serving_projection=repository))
    legacy_only_store = SimpleNamespace(get_serving_projection_member=lambda *_args: {})

    assert serving_projection_repo(canonical_store) is repository
    assert serving_projection_repo(legacy_only_store) is None


def test_serving_projection_retired_calls_and_native_dispatch_keys_cannot_return() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts"]
    synthetic = ast.parse(
        "\n".join(
            [
                'store.get_serving_projection("proj")',
                "callback = store.list_collection_authoritative_pointers",
                'dynamic = getattr(self.store, "upsert_run_projection_link")',
                'store.search_projection_person_index("proj", search_keyword="ada")',
                "index_callback = store.count_projection_person_search_index",
                'index_dynamic = getattr(self.store, "delete_projection_person_search_index")',
                'orchestrator.get_run_projection_link("run")',
            ]
        )
    )
    assert {label for _line, label in _retired_serving_projection_references(synthetic)} == {
        "get_serving_projection",
        "list_collection_authoritative_pointers",
        "getattr:upsert_run_projection_link",
        "search_projection_person_index",
        "count_projection_person_search_index",
        "getattr:delete_projection_person_search_index",
    }

    offenders: list[str] = []
    for root in checked_roots:
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            offenders.extend(
                f"{path.relative_to(repo_root)}:{line}:{label}"
                for line, label in _retired_serving_projection_references(tree)
            )

    native_dispatch_keys = _assigned_literal_dict_keys(
        repo_root / "src" / "sourcing_agent" / "storage.py",
        "_CONTROL_PLANE_POSTGRES_NATIVE_TABLES",
    )
    assert offenders == []
    assert native_dispatch_keys.isdisjoint(_RETIRED_SERVING_PROJECTION_NATIVE_DISPATCH_KEYS)


def test_projection_search_index_production_writes_require_generation_fence() -> None:
    synthetic = ast.parse(
        "\n".join(
            [
                'repo.replace_person_search_index("proj", rows)',
                'repo.upsert_person_search_index_rows("proj", rows)',
                'repo.delete_person_search_index("proj")',
                'repo.replace_person_search_index("proj", rows, build_generation="generation")',
            ]
        )
    )
    assert _generationless_projection_index_writes(synthetic) == [
        (1, "replace_person_search_index"),
        (2, "upsert_person_search_index_rows"),
        (3, "delete_person_search_index"),
    ]

    repo_root = Path(__file__).resolve().parents[1]
    offenders: list[str] = []
    for path in (repo_root / "src" / "sourcing_agent").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        offenders.extend(
            f"{path.relative_to(repo_root)}:{line}:{method_name}"
            for line, method_name in _generationless_projection_index_writes(tree)
        )
    assert offenders == []


def test_bulk_upsert_wrapper_calls_require_explicit_table_and_rows_keywords() -> None:
    valid_source = (
        'self._call_control_plane_postgres_native("bulk_upsert_rows", table_name="members", rows=payload_rows)'
    )
    assert _invalid_bulk_upsert_wrapper_calls(ast.parse(valid_source)) == []

    mutated_source = valid_source.replace('table_name="members"', '"members"')
    assert _invalid_bulk_upsert_wrapper_calls(ast.parse(mutated_source)) == [(1, "_call_control_plane_postgres_native")]

    synthetic = ast.parse(
        "\n".join(
            [
                'self._call_control_plane_postgres_native("bulk_upsert_rows", "members", payload_rows)',
                'self._call_native_write("bulk_upsert_rows", table_name="members")',
                'self._call_native_write("bulk_upsert_rows", rows=payload_rows)',
                valid_source,
            ]
        )
    )
    assert _invalid_bulk_upsert_wrapper_calls(synthetic) == [
        (1, "_call_control_plane_postgres_native"),
        (2, "_call_native_write"),
        (3, "_call_native_write"),
    ]

    repo_root = Path(__file__).resolve().parents[1]
    checked_roots = [repo_root / "src" / "sourcing_agent", repo_root / "scripts"]
    offenders: list[str] = []
    for root in checked_roots:
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            offenders.extend(
                f"{path.relative_to(repo_root)}:{line}:{wrapper}"
                for line, wrapper in _invalid_bulk_upsert_wrapper_calls(tree)
            )

    assert offenders == []


@pytest.mark.parametrize(
    "invoke",
    [
        lambda store: store._call_control_plane_postgres_native("bulk_upsert_rows", "members", []),
        lambda store: (lambda call: call("bulk_upsert_rows", "members", []))(store._call_control_plane_postgres_native),
        lambda store: getattr(store, "_call_control_plane_postgres_native")("bulk_upsert_rows", "members", []),
        lambda store: ControlPlaneStore._call_control_plane_postgres_native(store, "bulk_upsert_rows", "members", []),
        lambda store: store._call_control_plane_postgres_native("".join(("bulk_upsert", "_rows")), "members", []),
    ],
    ids=["direct-positional", "alias", "getattr", "unbound", "computed-method-name"],
)
def test_bulk_upsert_runtime_contract_rejects_positional_ast_bypasses(invoke) -> None:
    adapter = _BulkUpsertRecordingAdapter()
    store = object.__new__(ControlPlaneStore)
    store._control_plane_postgres = adapter

    with pytest.raises(TypeError, match="positional payload arguments are not allowed"):
        invoke(store)

    assert adapter.calls == []


@pytest.mark.parametrize(
    ("kwargs", "error_type", "message"),
    [
        ({"rows": []}, TypeError, "explicit table_name keyword argument"),
        ({"table_name": "", "rows": []}, ValueError, "non-empty table_name"),
        ({"table_name": "   ", "rows": []}, ValueError, "non-empty table_name"),
        ({"table_name": "members"}, TypeError, "explicit rows keyword argument"),
    ],
    ids=["missing-table", "empty-table", "blank-table", "missing-rows"],
)
def test_bulk_upsert_runtime_contract_requires_table_and_explicit_rows(
    kwargs: dict[str, object],
    error_type: type[Exception],
    message: str,
) -> None:
    adapter = _BulkUpsertRecordingAdapter()
    store = object.__new__(ControlPlaneStore)
    store._control_plane_postgres = adapter

    with pytest.raises(error_type, match=message):
        store._call_control_plane_postgres_native("bulk_upsert_rows", **kwargs)

    assert adapter.calls == []


def test_bulk_upsert_runtime_contract_allows_explicit_empty_rows_across_write_boundaries() -> None:
    adapter = _BulkUpsertRecordingAdapter()
    store = object.__new__(ControlPlaneStore)
    store._control_plane_postgres = adapter
    repository = ServingProjectionRepository(adapter)

    assert (
        store._call_control_plane_postgres_native(
            "bulk_upsert_rows",
            table_name="store_members",
            rows=[],
        )
        == 0
    )
    assert (
        repository._call_native_write(
            "bulk_upsert_rows",
            table_name="repository_members",
            rows=[],
        )
        == 0
    )
    assert adapter.calls == [("store_members", []), ("repository_members", [])]


@pytest.mark.parametrize(
    ("table_name", "include_rows", "error_type", "message"),
    [
        ("", True, ValueError, "non-empty table_name"),
        ("repository_members", False, TypeError, "explicit rows keyword argument"),
    ],
    ids=["repository-empty-table", "repository-missing-rows"],
)
def test_repository_bulk_upsert_runtime_contract_fails_closed(
    table_name: str,
    include_rows: bool,
    error_type: type[Exception],
    message: str,
) -> None:
    adapter = _BulkUpsertRecordingAdapter()
    repository = ServingProjectionRepository(adapter)
    kwargs: dict[str, object] = {"rows": []} if include_rows else {}

    with pytest.raises(error_type, match=message):
        repository._call_native_write("bulk_upsert_rows", table_name=table_name, **kwargs)

    assert adapter.calls == []


def test_serving_projection_catalog_repository_preserves_all_tier_a_b_fault_contracts() -> None:
    read_cases = [
        ("serving_projections", "select_one", lambda repo: repo.get("proj"), {}),
        ("serving_projections", "select_many", lambda repo: repo.list(), []),
        ("run_projection_links", "select_one", lambda repo: repo.get_run_link("run"), {}),
        ("run_projection_links", "select_many", lambda repo: repo.list_run_links("run"), []),
        (
            "collection_authoritative_pointers",
            "select_one",
            lambda repo: repo.get_authoritative_pointer("company:test"),
            {},
        ),
        (
            "collection_authoritative_pointers",
            "select_many",
            lambda repo: repo.list_authoritative_pointers(),
            [],
        ),
    ]
    write_cases = [
        (
            "serving_projections",
            "upsert_serving_projection",
            lambda repo: repo.upsert({"projection_id": "proj"}),
        ),
        (
            "run_projection_links",
            "upsert_run_projection_link",
            lambda repo: repo.upsert_run_link({"run_id": "run", "projection_id": "proj"}),
        ),
        (
            "collection_authoritative_pointers",
            "upsert_collection_authoritative_pointer",
            lambda repo: repo.upsert_authoritative_pointer(
                {"collection_id": "company:test", "active_projection_id": "proj"}
            ),
        ),
    ]
    checks = 0
    for table_name, primitive, call, sentinel in read_cases:
        strict_repo = ServingProjectionRepository(_CatalogFaultAdapter(authoritative=True, failing_method=primitive))
        strict_error = _runtime_error(lambda: call(strict_repo))
        assert str(strict_error) == (
            f"Postgres authoritative read failed for {table_name} via {primitive}: RuntimeError: {primitive}-boom"
        )
        assert isinstance(strict_error.__cause__, RuntimeError)
        checks += 1

        non_authoritative_repo = ServingProjectionRepository(
            _CatalogFaultAdapter(authoritative=False, failing_method=primitive)
        )
        assert call(non_authoritative_repo) == sentinel
        checks += 1

    for table_name, old_method_name, call in write_cases:
        strict_repo = ServingProjectionRepository(_CatalogFaultAdapter(authoritative=True, failing_method="upsert_row"))
        strict_error = _runtime_error(lambda: call(strict_repo))
        assert str(strict_error) == (
            f"Postgres authoritative write failed for {table_name} via upsert_row: RuntimeError: upsert_row-boom"
        )
        assert isinstance(strict_error.__cause__, RuntimeError)
        checks += 1

        non_authoritative_repo = ServingProjectionRepository(
            _CatalogFaultAdapter(authoritative=False, failing_method="upsert_row")
        )
        no_confirmation = _runtime_error(lambda: call(non_authoritative_repo))
        assert str(no_confirmation) == (
            f"Postgres authoritative write failed for {table_name} via {old_method_name}: "
            "postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)"
        )
        assert no_confirmation.__cause__ is None
        checks += 1

    assert checks == 18


def test_serving_projection_manifest_repository_preserves_all_tier_a_b_fault_contracts() -> None:
    read_cases = [
        ("select_one", lambda repo: repo.get_manifest_shard("manifest:1"), {}),
        ("select_many", lambda repo: repo.list_manifest_shards("proj"), []),
    ]
    checks = 0
    for primitive, call, sentinel in read_cases:
        strict_repo = ServingProjectionRepository(_CatalogFaultAdapter(authoritative=True, failing_method=primitive))
        strict_error = _runtime_error(lambda: call(strict_repo))
        assert str(strict_error) == (
            "Postgres authoritative read failed for projection_manifest_shards "
            f"via {primitive}: RuntimeError: {primitive}-boom"
        )
        assert isinstance(strict_error.__cause__, RuntimeError)
        checks += 1

        non_authoritative_repo = ServingProjectionRepository(
            _CatalogFaultAdapter(authoritative=False, failing_method=primitive)
        )
        assert call(non_authoritative_repo) == sentinel
        checks += 1

    payload = {"projection_id": "proj", "shard_id": "manifest:1"}
    strict_write_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="upsert_row")
    )
    strict_write_error = _runtime_error(lambda: strict_write_repo.upsert_manifest_shard(payload))
    assert str(strict_write_error) == (
        "Postgres authoritative write failed for projection_manifest_shards via upsert_row: "
        "RuntimeError: upsert_row-boom"
    )
    assert isinstance(strict_write_error.__cause__, RuntimeError)
    checks += 1

    non_authoritative_write_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=False, failing_method="upsert_row")
    )
    no_confirmation = _runtime_error(lambda: non_authoritative_write_repo.upsert_manifest_shard(payload))
    assert str(no_confirmation) == (
        "Postgres authoritative write failed for projection_manifest_shards via upsert_projection_manifest_shard: "
        "postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)"
    )
    assert no_confirmation.__cause__ is None
    checks += 1

    assert checks == 6


def test_serving_projection_member_repository_preserves_tier_a_b_and_count_sentinels() -> None:
    read_cases = [
        ("select_many", lambda repo: repo.list_members("proj"), []),
        ("select_many", lambda repo: repo.list_members_by_identity_keys("proj", ["candidate:1"]), []),
        ("select_many", lambda repo: repo.list_members_by_person_identity("person:1"), []),
        ("select_one", lambda repo: repo.get_member("proj", "candidate:1"), {}),
    ]
    checks = 0
    for primitive, call, sentinel in read_cases:
        strict_repo = ServingProjectionRepository(_CatalogFaultAdapter(authoritative=True, failing_method=primitive))
        strict_error = _runtime_error(lambda: call(strict_repo))
        assert str(strict_error) == (
            "Postgres authoritative read failed for serving_projection_members "
            f"via {primitive}: RuntimeError: {primitive}-boom"
        )
        assert isinstance(strict_error.__cause__, RuntimeError)
        checks += 1

        non_authoritative_repo = ServingProjectionRepository(
            _CatalogFaultAdapter(authoritative=False, failing_method=primitive)
        )
        assert call(non_authoritative_repo) == sentinel
        checks += 1

    strict_bulk_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(
            authoritative=True,
            failing_method="write_serving_projection_members_with_input_revision",
        )
    )
    bulk_error = _runtime_error(
        lambda: strict_bulk_repo.upsert_members("proj", [{"candidate_identity_key": "candidate:1"}])
    )
    assert str(bulk_error) == (
        "Postgres authoritative write failed for serving_projection_members "
        "via write_serving_projection_members_with_input_revision: "
        "RuntimeError: write_serving_projection_members_with_input_revision-boom"
    )
    assert isinstance(bulk_error.__cause__, RuntimeError)
    checks += 1

    non_authoritative_bulk_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(
            authoritative=False,
            failing_method="write_serving_projection_members_with_input_revision",
        )
    )
    assert non_authoritative_bulk_repo.upsert_members("proj", [{"candidate_identity_key": "candidate:1"}]) == 0
    checks += 1

    strict_readiness_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="select_many")
    )
    readiness_error = _runtime_error(lambda: strict_readiness_repo.count_members_by_readiness("proj"))
    assert str(readiness_error) == (
        "Postgres authoritative read failed for serving_projection_members via select_many: "
        "RuntimeError: select_many-boom"
    )
    assert isinstance(readiness_error.__cause__, RuntimeError)
    checks += 1
    non_authoritative_readiness_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=False, failing_method="select_many")
    )
    assert non_authoritative_readiness_repo.count_members_by_readiness("proj") == {}
    checks += 1

    strict_count_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="count_rows")
    )
    count_error = _runtime_error(lambda: strict_count_repo.count_members("proj"))
    assert str(count_error) == (
        "Postgres authoritative read failed for serving_projection_members via count_rows: "
        "RuntimeError: count_rows-boom"
    )
    assert isinstance(count_error.__cause__, RuntimeError)
    checks += 1
    non_authoritative_count_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=False, failing_method="count_rows")
    )
    assert non_authoritative_count_repo.count_members("proj") == 0
    checks += 1

    strict_replace_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(
            authoritative=True,
            failing_method="write_serving_projection_members_with_input_revision",
        )
    )
    replace_error = _runtime_error(
        lambda: strict_replace_repo.replace_members("proj", [{"candidate_identity_key": "candidate:1"}])
    )
    assert str(replace_error) == (
        "Postgres authoritative write failed for serving_projection_members "
        "via write_serving_projection_members_with_input_revision: "
        "RuntimeError: write_serving_projection_members_with_input_revision-boom"
    )
    assert isinstance(replace_error.__cause__, RuntimeError)
    checks += 1
    non_authoritative_replace_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(
            authoritative=False,
            failing_method="write_serving_projection_members_with_input_revision",
        )
    )
    assert non_authoritative_replace_repo.replace_members("proj", [{"candidate_identity_key": "candidate:1"}]) == 0
    checks += 1

    atomic_adapter = _CatalogFaultAdapter(authoritative=True, failing_method="")
    atomic_repo = ServingProjectionRepository(atomic_adapter)
    assert atomic_repo.upsert_members("proj", [{"candidate_identity_key": "candidate:1"}]) == 1
    upsert_method, upsert_table, upsert_call = atomic_adapter.calls[-1]
    assert upsert_method == "write_serving_projection_members_with_input_revision"
    assert upsert_table == "serving_projection_members"
    assert upsert_call["projection_id"] == "proj"
    assert upsert_call["replace_members"] is False
    assert str(upsert_call["input_revision"]).startswith("projidxinput_")
    checks += 1
    assert atomic_repo.replace_members("proj", [{"candidate_identity_key": "candidate:1"}]) == 1
    replace_method, replace_table, replace_call = atomic_adapter.calls[-1]
    assert replace_method == "write_serving_projection_members_with_input_revision"
    assert replace_table == "serving_projection_members"
    assert replace_call["projection_id"] == "proj"
    assert replace_call["replace_members"] is True
    assert str(replace_call["input_revision"]).startswith("projidxinput_")
    checks += 1

    strict_publication_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="upsert_row_and_replace_rows")
    )
    publication_error = _runtime_error(
        lambda: strict_publication_repo.upsert_with_replaced_members(
            {"projection_id": "proj", "state": "serving"},
            [{"candidate_identity_key": "candidate:1"}],
        )
    )
    assert str(publication_error) == (
        "Postgres authoritative write failed for serving_projections via upsert_row_and_replace_rows: "
        "RuntimeError: upsert_row_and_replace_rows-boom"
    )
    assert isinstance(publication_error.__cause__, RuntimeError)
    checks += 1

    invariant_repo = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="", prefer_read=False)
    )
    invariant_error = _runtime_error(
        lambda: invariant_repo.upsert_members("proj", [{"candidate_identity_key": "candidate:1"}])
    )
    assert str(invariant_error) == (
        "postgres-only invariant violated for serving_projection_members in upsert_serving_projection_members: "
        "should_prefer_read returned False; legacy SQLite tail retired (B4)"
    )
    checks += 1

    assert checks == 20


def test_projection_person_search_index_replace_uses_one_atomic_native_write() -> None:
    adapter = _CatalogFaultAdapter(authoritative=True, failing_method="")
    repository = ServingProjectionRepository(adapter)

    result = repository.replace_person_search_index(
        "proj-index-atomic",
        [
            {
                "candidate_identity_key": "candidate:1",
                "person_identity_key": "person:1",
                "indexed_text": "Ada Engineer",
            }
        ],
    )

    assert result["status"] == "indexed"
    assert result["indexed_count"] == 1
    assert len(adapter.calls) == 1
    method_name, table_name, call = adapter.calls[0]
    assert method_name == "replace_rows"
    assert table_name == "projection_person_search_index"
    assert call["where_sql"] == "projection_id = %s"
    assert call["params"] == ["proj-index-atomic"]
    assert call["transaction_lock_key"] == "projection_person_search_index:proj-index-atomic"
    assert len(call["rows"]) == 1

    incremental = repository.upsert_person_search_index_rows(
        "proj-index-atomic",
        [{"candidate_identity_key": "candidate:2", "person_identity_key": "person:2"}],
    )
    assert incremental["status"] == "indexed"
    assert len(adapter.calls) == 2
    method_name, table_name, call = adapter.calls[1]
    assert method_name == "bulk_upsert_rows"
    assert table_name == "projection_person_search_index"
    assert call["transaction_lock_key"] == "projection_person_search_index:proj-index-atomic"

    strict_failure = ServingProjectionRepository(
        _CatalogFaultAdapter(authoritative=True, failing_method="replace_rows")
    )
    error = _runtime_error(
        lambda: strict_failure.replace_person_search_index(
            "proj-index-atomic",
            [{"candidate_identity_key": "candidate:1"}],
        )
    )
    assert str(error) == (
        "Postgres authoritative write failed for projection_person_search_index via replace_rows: "
        "RuntimeError: replace_rows-boom"
    )


def test_serving_projection_member_mapper_preserves_irregular_read_contract() -> None:
    repository = ServingProjectionRepository(object())
    row = {
        "projection_id": "proj-guard",
        "candidate_identity_key": "candidate:1",
        "rank_index": -7,
        "public_summary_json": '{"name":"Ada"}',
        "projection_metrics_json": "{",
        "crm_overlay_summary_json": "[]",
        "provenance_json": None,
        "metadata_json": '{"source":"guard"}',
    }

    class BrokenRow:
        def __getitem__(self, key: str) -> object:
            raise RuntimeError(key)

    mapped = repository._member_from_row(row)
    assert mapped["rank_index"] == 0
    assert mapped["public_summary"] == {"name": "Ada"}
    assert mapped["projection_metrics"] == {}
    assert mapped["crm_overlay_summary"] == {}
    assert mapped["provenance"] == {}
    assert mapped["metadata"] == {"source": "guard"}
    assert repository._member_from_row(None) == {}
    broken = repository._member_from_row(BrokenRow())
    assert broken["projection_id"] == ""
    assert broken["rank_index"] == 0
    assert broken["public_summary"] == {}


def test_serving_projection_catalog_repository_mappers_preserve_descriptor_contract() -> None:
    repository = ServingProjectionRepository(object())
    projection_row = {
        "projection_id": "proj-guard",
        "projection_type": "run_scope_projection",
        "projection_version": None,
        "counts_json": '{"candidate_count":2}',
        "readiness_json": "not-json",
    }
    run_link_row = {
        "run_id": "run-guard",
        "projection_id": "proj-guard",
        "link_type": "result",
        "metadata_json": '{"source":"guard"}',
    }
    pointer_row = {
        "collection_id": "company:guard",
        "active_projection_id": "proj-guard",
        "metadata_json": "[]",
    }

    projection = repository._projection_from_row(projection_row)
    assert projection["projection_version"] == "serving_projection_v1"
    assert projection["counts"] == {"candidate_count": 2}
    assert projection["readiness"] == {}
    assert repository._run_link_from_row(run_link_row)["metadata"] == {"source": "guard"}
    assert repository._authoritative_pointer_from_row(pointer_row)["metadata"] == {}


def test_serving_projection_manifest_shard_mapper_preserves_irregular_read_contract() -> None:
    repository = ServingProjectionRepository(object())
    negative_row = {
        "shard_id": "negative",
        "projection_id": "proj-guard",
        "shard_kind": "candidate_identity_manifest",
        "shard_index": -7,
        "manifest_ref": "s3://bucket/negative.json",
        "row_count": -3,
        "metadata_json": "{",
    }
    malformed_row = {
        "shard_id": "malformed",
        "projection_id": "proj-guard",
        "shard_index": "bad",
        "row_count": "bad",
        "metadata_json": "[]",
    }

    class BrokenRow:
        def __getitem__(self, key: str) -> object:
            raise RuntimeError(key)

    assert repository._manifest_shard_from_row(None) == {}
    assert repository._manifest_shard_from_row(negative_row) == {
        "shard_id": "negative",
        "projection_id": "proj-guard",
        "shard_kind": "candidate_identity_manifest",
        "shard_index": 0,
        "manifest_ref": "s3://bucket/negative.json",
        "row_count": 0,
        "content_signature": "",
        "metadata": {},
        "created_at": "",
        "updated_at": "",
    }
    malformed = repository._manifest_shard_from_row(malformed_row)
    assert malformed["shard_index"] == 0
    assert malformed["row_count"] == 0
    assert malformed["metadata"] == {}
    assert repository._manifest_shard_from_row(BrokenRow()) == {
        "shard_id": "",
        "projection_id": "",
        "shard_kind": "",
        "shard_index": 0,
        "manifest_ref": "",
        "row_count": 0,
        "content_signature": "",
        "metadata": {},
        "created_at": "",
        "updated_at": "",
    }
