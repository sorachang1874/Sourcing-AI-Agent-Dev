from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.action_target_binding import (
    ACQUISITION_ROOT_TARGET_INVALID,
    ACQUISITION_ROOT_TARGET_OWNER,
    AUTHORIZATION_MODE_AUTHENTICATED,
    AUTHORIZATION_MODE_OPEN_OPERATOR,
    AcquisitionRootTargetBinder,
    ActionBindContext,
    ActionTargetBindingError,
)
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.durable_runtime import (
    ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
)
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.operation_runtime import (
    ACQUISITION_ROOT_ACTION_REQUEST_CONTRACTS,
    ACTION_START_ACQUISITION_RUN,
    DEFAULT_ACTION_REGISTRY,
    OPERATION_OWNER_BOUND_ACTION_TYPES,
)
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin, psycopg

FULL_ZERO_WRITE_TABLES = (
    "agent_actions",
    "operation_runs",
    "operation_events",
    "workflow_events",
    "workflow_current_state",
    "workflow_commands",
    "runtime_outbox",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "plan_review_sessions",
    "acquisition_runs",
    "jobs",
)


def test_acquisition_root_remains_in_the_current_schema_defined_unserved_set() -> None:
    records = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    schema_defined = {
        action_type for action_type in records if DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema
    }
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN)

    assert set(ACQUISITION_ROOT_ACTION_REQUEST_CONTRACTS) == {ACTION_START_ACQUISITION_RUN}
    assert schema_defined == set(OPERATION_OWNER_BOUND_ACTION_TYPES)
    assert len(schema_defined) == 10
    assert sum(not DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema for action_type in records) == 5
    assert spec.request_schema_version == "acquisition_root_request_v1"
    assert len(spec.request_schema_digest) == 64
    assert spec.request_identity_target_fields == ("workspace_id",)
    assert spec.allowed_workflow_command_types == (ACQUISITION_RUN_CREATE_COMMAND_TYPE,)
    assert "agent_tool_enabled" not in records[ACTION_START_ACQUISITION_RUN]


def test_acquisition_root_binder_mints_authenticated_and_open_mode_workspace() -> None:
    binder = AcquisitionRootTargetBinder()

    authenticated = binder(
        ActionBindContext(
            authorization_mode=AUTHORIZATION_MODE_AUTHENTICATED,
            workspace_id="user-alice",
            owner_user_id="alice",
            target_selector={},
        )
    )
    assert authenticated.owner_module == ACQUISITION_ROOT_TARGET_OWNER
    assert authenticated.target_ref == {"workspace_id": "user-alice"}

    operator = binder(
        ActionBindContext(
            authorization_mode=AUTHORIZATION_MODE_OPEN_OPERATOR,
            workspace_id="operator-workspace",
            owner_user_id="",
            target_selector={},
        )
    )
    assert operator.target_ref == {"workspace_id": "operator-workspace"}

    with pytest.raises(ActionTargetBindingError, match=ACQUISITION_ROOT_TARGET_INVALID):
        binder(
            ActionBindContext(
                authorization_mode=AUTHORIZATION_MODE_AUTHENTICATED,
                workspace_id="user-alice",
                owner_user_id="alice",
                target_selector={"workspace_id": "foreign"},
            )
        )


class D1iAcquisitionRootActionActivationPGTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir, schema_label="d1i_acquisition_root_action")
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "d1i-acquisition-root-action.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        self.orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, model_client),
        )

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def _table_state(self, table_names: tuple[str, ...]) -> dict[str, tuple[str, ...]]:
        fixture = self._pg_durable_runtime_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        state: dict[str, tuple[str, ...]] = {}
        with psycopg.connect(
            fixture.dsn,
            autocommit=True,
            connect_timeout=5,
            client_encoding="utf8",
        ) as connection:
            with connection.cursor() as cursor:
                for table_name in table_names:
                    quoted_table = quote_control_plane_postgres_identifier(table_name)
                    cursor.execute(f"SELECT row_to_json(t)::text FROM {quoted_schema}.{quoted_table} AS t")
                    state[table_name] = tuple(sorted(str(row[0]) for row in cursor.fetchall()))
        return state

    def _execute_pg(self, sql: str, params: tuple[Any, ...]) -> None:
        fixture = self._pg_durable_runtime_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        with psycopg.connect(
            fixture.dsn,
            autocommit=True,
            connect_timeout=5,
            client_encoding="utf8",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.format(schema=quoted_schema), params)

    def _submit(
        self,
        *,
        input_payload: dict[str, Any] | None = None,
        workspace_id: str = "user-alice",
        expected_workspace_id: str = "user-alice",
        expected_owner_user_id: str = "alice",
        idempotency_key: str,
        extra_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        owner_scope = (
            {
                "expected_workspace_id": expected_workspace_id,
                "expected_owner_user_id": expected_owner_user_id,
            }
            if expected_workspace_id or expected_owner_user_id
            else {}
        )
        return self.orchestrator.submit_operation_action(
            {
                "action_type": ACTION_START_ACQUISITION_RUN,
                "workspace_id": workspace_id,
                "input": dict(
                    input_payload
                    or {
                        "target_company": "Thinking Machines Lab",
                        "query": "Thinking Machines Lab pre-training researchers",
                    }
                ),
                "budget": {"max_provider_calls": 1, "max_usd": 1.0},
                "idempotency_key": idempotency_key,
                "actor": expected_owner_user_id or "operator",
                **dict(extra_payload or {}),
            },
            **owner_scope,
        )

    def _approve_dispatch(self, submission: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        approved = self.orchestrator.approve_operation_action_api(
            submission["action"]["action_id"],
            {"actor": "alice"},
        )
        self.assertEqual(approved.get("status"), "queued", approved)
        dispatched = self.orchestrator.dispatch_operation_run_api(
            approved["operation_run"]["operation_run_id"],
            {"actor": "alice"},
        )
        self.assertEqual(dispatched.get("status"), "planned", dispatched)
        return approved, dispatched

    def _claim_running_root(
        self,
        dispatched: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_id = str(dispatched["workflow_command"]["command_id"])
        claimed = self.store.claim_workflow_command(
            command_id,
            lease_owner=lease_owner,
            lease_seconds=300,
        )
        self.assertEqual(claimed.get("lease_owner"), lease_owner, claimed)
        running = self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner)
        self.assertEqual(running.get("status"), "running", running)
        latest = self.store.get_workflow_command(command_id)
        self.assertIsNotNone(latest)
        self.assertEqual((latest or {}).get("lease_owner"), lease_owner)
        self.assertTrue(str((latest or {}).get("lease_expires_at") or ""))
        self.assertGreater(int((latest or {}).get("attempt") or 0), 0)
        return dict(latest or {})

    def _root_uow_contract(self, root: dict[str, Any]) -> dict[str, Any]:
        owner = self.orchestrator._acquisition_command_owner  # noqa: SLF001
        contract = owner._acquisition_root_intent_plan_contract(  # noqa: SLF001
            root,
            claim_attempt=int(root.get("attempt") or 0),
        )
        self.assertTrue(contract)
        return contract

    def _complete_root_direct(
        self,
        root: dict[str, Any],
        *,
        expected_lease_owner: str | None = None,
        expected_lease_expires_at: str | None = None,
        expected_attempt: int | None = None,
    ) -> dict[str, Any]:
        owner = self.orchestrator._acquisition_command_owner  # noqa: SLF001
        contract = self._root_uow_contract(root)
        return self.store.repos.workflow_runtime.complete_acquisition_root_command(
            str(root.get("command_id") or ""),
            expected_lease_owner=str(expected_lease_owner or root.get("lease_owner") or ""),
            expected_lease_expires_at=str(expected_lease_expires_at or root.get("lease_expires_at") or ""),
            expected_attempt=int(expected_attempt or root.get("attempt") or 0),
            expected_root_command=owner._acquisition_root_locked_identity(root),  # noqa: SLF001
            plan_event=dict(contract.get("plan_event") or {}),
            child_command=dict(contract.get("child_command") or {}),
            child_causality=dict(contract.get("child_causality") or {}),
            root_result=dict(contract.get("root_result") or {}),
        )

    def _complete_root_through_owner(self, *, idempotency_key: str) -> dict[str, Any]:
        submission = self._submit(idempotency_key=idempotency_key)
        approved, dispatched = self._approve_dispatch(submission)
        drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {
                "workflow_run_id": dispatched["workflow_command"]["workflow_run_id"],
                "command_limit": 1,
            }
        )
        self.assertEqual(drained.get("completed_count"), 1, drained)
        root = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        self.assertEqual((root or {}).get("status"), "succeeded", root)
        children = [
            command
            for command in self.store.list_workflow_commands(
                workflow_run_id=dispatched["workflow_command"]["workflow_run_id"],
                limit=0,
            )
            if command["command_type"] == ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE
        ]
        self.assertEqual(len(children), 1, children)
        return {
            "submission": submission,
            "approved": approved,
            "dispatched": dispatched,
            "root": dict(root or {}),
            "child": dict(children[0]),
        }

    def test_invalid_alias_missing_blank_and_target_requests_are_zero_write(self) -> None:
        baseline = self._table_state(FULL_ZERO_WRITE_TABLES)
        invalid_inputs = (
            {},
            {"target_company": "Thinking Machines Lab"},
            {"query": "researchers"},
            {"target_company": " ", "query": "researchers"},
            {"target_company": "Thinking Machines Lab", "query": " "},
            {
                "target_company": "Thinking Machines Lab",
                "query": "researchers",
                "raw_user_request": "duplicate query alias",
            },
        )
        forbidden_aliases: tuple[tuple[str, Any], ...] = (
            ("command_type", ACQUISITION_RUN_CREATE_COMMAND_TYPE),
            ("command_payload", {}),
            ("workflow_payload", {}),
            ("workflow_run_id", "wf-caller"),
            ("job_id", "job-caller"),
            ("plan_review_id", "7"),
            ("requester_id", "mallory"),
            ("tenant_id", "foreign"),
            ("workspace_id", "foreign"),
            ("max_attempts", 99),
            ("retry_policy", {"kind": "caller"}),
            ("limit", 25),
        )
        requests: list[dict[str, Any]] = [{"input": invalid_input} for invalid_input in invalid_inputs] + [
            {
                "input": {
                    "target_company": "Thinking Machines Lab",
                    "query": "researchers",
                    field: value,
                }
            }
            for field, value in forbidden_aliases
        ]
        requests.extend(
            (
                {
                    "input": {
                        "target_company": "Thinking Machines Lab",
                        "query": "researchers",
                    },
                    "target_ref": {"workspace_id": "user-alice"},
                },
                {
                    "input": {
                        "target_company": "Thinking Machines Lab",
                        "query": "researchers",
                    },
                    "input_payload": {
                        "target_company": "Thinking Machines Lab",
                        "query": "researchers",
                    },
                },
                {"input": "not-an-object"},
            )
        )

        for index, request in enumerate(requests):
            with self.subTest(index=index, request=request):
                result = self.orchestrator.submit_operation_action(
                    {
                        "action_type": ACTION_START_ACQUISITION_RUN,
                        "workspace_id": "user-alice",
                        "budget": {"max_provider_calls": 1, "max_usd": 1.0},
                        "idempotency_key": f"invalid:{index}",
                        **request,
                    },
                    expected_workspace_id="user-alice",
                    expected_owner_user_id="alice",
                )
                self.assertEqual(result.get("status"), "invalid", result)
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

        wrong_workspace = self._submit(
            workspace_id="foreign",
            idempotency_key="invalid:foreign-workspace",
        )
        self.assertEqual(wrong_workspace.get("status"), "invalid", wrong_workspace)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), baseline)

    def test_authenticated_no_ref_alias_replay_and_open_mode_are_positive(self) -> None:
        alias = self._submit(
            input_payload={
                "target_company": "Thinking Machines Lab",
                "raw_user_request": "pre-training researchers",
            },
            idempotency_key="auth:alias",
        )
        self.assertEqual(alias.get("status"), "approval_required", alias)
        self.assertEqual(alias["action"]["target_ref"], {"workspace_id": "user-alice"})
        self.assertEqual(
            alias["action"]["input"],
            {
                "target_company": "Thinking Machines Lab",
                "query": "pre-training researchers",
            },
        )
        self.assertEqual(alias["action"]["request_schema_version"], "acquisition_root_request_v1")

        replay = self._submit(
            input_payload={
                "target_company": "Thinking Machines Lab",
                "query": "pre-training researchers",
            },
            idempotency_key="auth:alias",
        )
        self.assertEqual(replay.get("status"), "approval_required", replay)
        self.assertTrue(replay.get("idempotent_replay"), replay)
        self.assertEqual(replay["action"]["action_id"], alias["action"]["action_id"])

        operator = self._submit(
            workspace_id="operator-workspace",
            expected_workspace_id="",
            expected_owner_user_id="",
            idempotency_key="open:operator",
        )
        self.assertEqual(operator.get("status"), "approval_required", operator)
        self.assertEqual(operator["action"]["target_ref"], {"workspace_id": "operator-workspace"})

    def test_approve_retry_and_resume_revalidate_root_target_before_writes(self) -> None:
        approval_submission = self._submit(idempotency_key="control:approval")
        approval_action_id = approval_submission["action"]["action_id"]
        self._execute_pg(
            "UPDATE {schema}.agent_actions SET target_ref_json = %s WHERE action_id = %s",
            (json.dumps({"workspace_id": "foreign"}), approval_action_id),
        )
        before_approval = self._table_state(FULL_ZERO_WRITE_TABLES)
        approval = self.orchestrator.approve_operation_action_api(approval_action_id, {"actor": "alice"})
        self.assertEqual(approval.get("status"), "invalid", approval)
        self.assertEqual(approval.get("reason"), ACQUISITION_ROOT_TARGET_INVALID)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_approval)

        retry_submission = self._submit(idempotency_key="control:retry")
        retry_approved = self.orchestrator.approve_operation_action_api(
            retry_submission["action"]["action_id"],
            {"actor": "alice"},
        )
        retry_run_id = retry_approved["operation_run"]["operation_run_id"]
        cancelled = self.orchestrator.cancel_operation_run_api(retry_run_id, {"actor": "alice"})
        self.assertEqual(cancelled.get("status"), "cancelled", cancelled)
        self._execute_pg(
            "UPDATE {schema}.agent_actions SET target_ref_json = %s WHERE action_id = %s",
            (json.dumps({"workspace_id": "foreign"}), retry_submission["action"]["action_id"]),
        )
        before_retry = self._table_state(FULL_ZERO_WRITE_TABLES)
        retry = self.orchestrator.retry_operation_run_api(retry_run_id, {"actor": "alice"})
        self.assertEqual(retry.get("status"), "invalid", retry)
        self.assertEqual(retry.get("reason"), ACQUISITION_ROOT_TARGET_INVALID)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_retry)

        resume_submission = self._submit(idempotency_key="control:resume")
        resume_approved = self.orchestrator.approve_operation_action_api(
            resume_submission["action"]["action_id"],
            {"actor": "alice"},
        )
        resume_run_id = resume_approved["operation_run"]["operation_run_id"]
        self._execute_pg(
            "UPDATE {schema}.agent_actions SET target_ref_json = %s WHERE action_id = %s",
            (json.dumps({"workspace_id": "foreign"}), resume_submission["action"]["action_id"]),
        )
        before_resume = self._table_state(FULL_ZERO_WRITE_TABLES)
        resume = self.orchestrator.resume_operation_run_api(resume_run_id, {"actor": "alice"})
        self.assertEqual(resume.get("status"), "invalid", resume)
        self.assertEqual(resume.get("reason"), ACQUISITION_ROOT_TARGET_INVALID)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_resume)

    def test_same_owner_dispatch_and_root_owner_create_only_one_intent_child(self) -> None:
        submission = self._submit(idempotency_key="positive:root")
        approved, dispatched = self._approve_dispatch(submission)
        command = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        self.assertIsNotNone(command)
        payload = dict((command or {}).get("payload") or {})
        self.assertEqual(payload["acquisition_root_target"], {"workspace_id": "user-alice"})
        self.assertEqual(payload["target_company"], "Thinking Machines Lab")
        self.assertEqual(payload["query"], "Thinking Machines Lab pre-training researchers")
        self.assertEqual(payload["action_id"], submission["action"]["action_id"])
        self.assertEqual(payload["operation_id"], approved["operation_run"]["operation_run_id"])

        domain_tables = ("plan_review_sessions", "acquisition_runs", "jobs")
        before_domain = self._table_state(domain_tables)
        drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {"workflow_run_id": dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(drained.get("completed_count"), 1, drained)
        commands = self.store.list_workflow_commands(
            workflow_run_id=dispatched["workflow_command"]["workflow_run_id"],
            limit=100,
        )
        self.assertEqual(len(commands), 2)
        children = [
            command for command in commands if command["command_type"] == ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE
        ]
        self.assertEqual(len(children), 1)
        self.assertEqual(children[0]["parent_command_id"], dispatched["workflow_command"]["command_id"])
        self.assertEqual(self._table_state(domain_tables), before_domain)

        terminal_root = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        self.assertEqual(
            (terminal_root or {}).get("downstream_command_ids"),
            [children[0]["command_id"]],
        )
        replay = self.orchestrator._run_acquisition_run_create_command(dict(terminal_root or {}))  # noqa: SLF001
        self.assertEqual(replay.get("status"), "completed", replay)
        self.assertEqual(
            len(
                self.store.list_workflow_commands(
                    workflow_run_id=dispatched["workflow_command"]["workflow_run_id"],
                    limit=100,
                )
            ),
            2,
        )

    def test_intent_child_parent_uniqueness_rejects_an_unknown_followup_producer(self) -> None:
        submission = self._submit(idempotency_key="child-uniqueness:root")
        _, dispatched = self._approve_dispatch(submission)
        workflow_run_id = str(dispatched["workflow_command"]["workflow_run_id"])
        root_command_id = str(dispatched["workflow_command"]["command_id"])
        drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {"workflow_run_id": workflow_run_id, "command_limit": 1}
        )
        self.assertEqual(drained.get("completed_count"), 1, drained)

        self.assertIsNotNone(psycopg)
        assert psycopg is not None
        with self.assertRaises(psycopg.errors.UniqueViolation) as raised:
            self._execute_pg(
                """
                INSERT INTO {schema}.workflow_commands (
                    command_id,
                    workflow_run_id,
                    operation_id,
                    command_type,
                    owner,
                    parent_command_id,
                    idempotency_key
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "cmd_unknown_competing_intent_child",
                    workflow_run_id,
                    dispatched["workflow_command"]["operation_id"],
                    ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
                    "unknown-producer",
                    root_command_id,
                    "unknown-producer:alternate-idempotency",
                ),
            )

        self.assertEqual(str(raised.exception.sqlstate or ""), "23505")
        self.assertEqual(
            raised.exception.diag.constraint_name,
            "workflow_commands_acquisition_root_single_child_uk",
        )
        commands = self.store.list_workflow_commands(workflow_run_id=workflow_run_id, limit=0)
        self.assertEqual(len(commands), 2, commands)
        self.assertEqual(
            sum(command["command_type"] == ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE for command in commands),
            1,
        )

    def test_acquisition_root_rejects_a_different_type_child_from_an_unknown_producer(self) -> None:
        submission = self._submit(idempotency_key="child-shape:root")
        _, dispatched = self._approve_dispatch(submission)
        workflow_run_id = str(dispatched["workflow_command"]["workflow_run_id"])
        root_command_id = str(dispatched["workflow_command"]["command_id"])
        drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {"workflow_run_id": workflow_run_id, "command_limit": 1}
        )
        self.assertEqual(drained.get("completed_count"), 1, drained)

        self.assertIsNotNone(psycopg)
        assert psycopg is not None
        with self.assertRaises(psycopg.errors.CheckViolation) as raised:
            self._execute_pg(
                """
                INSERT INTO {schema}.workflow_commands (
                    command_id,
                    workflow_run_id,
                    operation_id,
                    command_type,
                    owner,
                    parent_command_id,
                    idempotency_key
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "cmd_unknown_competing_wrong_type_child",
                    workflow_run_id,
                    dispatched["workflow_command"]["operation_id"],
                    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
                    "unknown-producer",
                    root_command_id,
                    "unknown-producer:wrong-type",
                ),
            )

        self.assertEqual(str(raised.exception.sqlstate or ""), "23514")
        self.assertEqual(
            raised.exception.diag.constraint_name,
            "workflow_commands_acquisition_root_child_shape_ck",
        )
        commands = self.store.list_workflow_commands(workflow_run_id=workflow_run_id, limit=0)
        self.assertEqual(len(commands), 2, commands)
        self.assertEqual(
            sum(command["command_type"] == ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE for command in commands),
            1,
        )

    def test_succeeded_root_replay_accepts_legitimate_child_retry_schedule_mutation(self) -> None:
        submission = self._submit(idempotency_key="mutable-retry-schedule:root")
        _, dispatched = self._approve_dispatch(submission)
        workflow_run_id = str(dispatched["workflow_command"]["workflow_run_id"])
        drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {"workflow_run_id": workflow_run_id, "command_limit": 1}
        )
        self.assertEqual(drained.get("completed_count"), 1, drained)
        commands = self.store.list_workflow_commands(workflow_run_id=workflow_run_id, limit=0)
        child = next(
            command for command in commands if command["command_type"] == ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE
        )
        claimed = self.store.claim_workflow_command(
            child["command_id"],
            lease_owner="acquisition-planner-retry",
            lease_seconds=300,
        )
        self.assertTrue(claimed, child)
        retry_waiting = self.store.mark_workflow_command_failed(
            child["command_id"],
            error_text="injected retryable planner failure",
            retryable=True,
            retry_delay_seconds=60,
        )
        self.assertEqual(retry_waiting.get("status"), "retry_wait", retry_waiting)
        self.assertTrue(str(retry_waiting.get("not_before_at") or "").strip(), retry_waiting)

        terminal_root = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        replay = self.orchestrator._run_acquisition_run_create_command(dict(terminal_root or {}))  # noqa: SLF001

        self.assertEqual(replay.get("status"), "completed", replay)
        persisted_child = self.store.get_workflow_command(child["command_id"])
        self.assertEqual((persisted_child or {}).get("status"), "retry_wait", persisted_child)
        self.assertEqual(
            (persisted_child or {}).get("not_before_at"),
            retry_waiting.get("not_before_at"),
        )
        self.assertEqual(len(self.store.list_workflow_commands(workflow_run_id=workflow_run_id, limit=0)), 2)

    def test_root_uow_rejects_expired_and_reclaimed_claims_without_any_write(self) -> None:
        expired_submission = self._submit(idempotency_key="claim-fence:expired")
        _, expired_dispatched = self._approve_dispatch(expired_submission)
        expired_root = self._claim_running_root(
            expired_dispatched,
            lease_owner="acquisition-run-writer-expired",
        )
        self._execute_pg(
            """
            UPDATE {schema}.workflow_commands
            SET lease_expires_at = TO_CHAR(
                clock_timestamp() AT TIME ZONE 'UTC' - INTERVAL '1 second',
                'YYYY-MM-DD HH24:MI:SS'
            )
            WHERE command_id = %s
            """,
            (expired_root["command_id"],),
        )
        expired_root = dict(self.store.get_workflow_command(expired_root["command_id"]) or {})
        before_expired = self._table_state(FULL_ZERO_WRITE_TABLES)

        expired_result = self._complete_root_direct(expired_root)

        self.assertEqual(expired_result.get("outcome"), "stale_claim", expired_result)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_expired)

        reclaimed_submission = self._submit(idempotency_key="claim-fence:reclaimed")
        _, reclaimed_dispatched = self._approve_dispatch(reclaimed_submission)
        old_root = self._claim_running_root(
            reclaimed_dispatched,
            lease_owner="acquisition-run-writer-old",
        )
        self._execute_pg(
            """
            UPDATE {schema}.workflow_commands
            SET lease_expires_at = TO_CHAR(
                clock_timestamp() AT TIME ZONE 'UTC' - INTERVAL '1 second',
                'YYYY-MM-DD HH24:MI:SS'
            )
            WHERE command_id = %s
            """,
            (old_root["command_id"],),
        )
        new_root = self.store.claim_workflow_command(
            old_root["command_id"],
            lease_owner="acquisition-run-writer-new",
            lease_seconds=300,
        )
        self.assertEqual(new_root.get("lease_owner"), "acquisition-run-writer-new", new_root)
        new_root = self.store.mark_workflow_command_running(
            old_root["command_id"],
            lease_owner="acquisition-run-writer-new",
        )
        self.assertEqual(new_root.get("status"), "running", new_root)
        self.assertGreater(int(new_root.get("attempt") or 0), int(old_root.get("attempt") or 0))
        before_reclaimed = self._table_state(FULL_ZERO_WRITE_TABLES)

        reclaimed_result = self._complete_root_direct(
            old_root,
            expected_lease_owner=str(old_root["lease_owner"]),
            expected_lease_expires_at=str(old_root["lease_expires_at"]),
            expected_attempt=int(old_root["attempt"]),
        )

        self.assertEqual(reclaimed_result.get("outcome"), "stale_claim", reclaimed_result)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before_reclaimed)
        latest = self.store.get_workflow_command(old_root["command_id"])
        self.assertEqual((latest or {}).get("lease_owner"), "acquisition-run-writer-new")
        self.assertEqual((latest or {}).get("attempt"), new_root.get("attempt"))

    def test_root_complete_and_fail_claim_fences_treat_naive_lease_text_as_utc_in_non_utc_session(self) -> None:
        adapter = self.store._control_plane_postgres
        original_connect = adapter._connect

        @contextmanager
        def non_utc_connect() -> Iterator[Any]:
            with original_connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SET TIME ZONE 'Asia/Singapore'")
                yield connection

        completed_submission = self._submit(idempotency_key="lease-timezone:complete")
        _, completed_dispatched = self._approve_dispatch(completed_submission)
        completed_root = self._claim_running_root(
            completed_dispatched,
            lease_owner="acquisition-run-writer-non-utc-complete",
        )
        with mock.patch.object(adapter, "_connect", non_utc_connect):
            completed = self._complete_root_direct(completed_root)
        self.assertEqual(completed.get("outcome"), "applied", completed)

        failed_submission = self._submit(idempotency_key="lease-timezone:fail")
        _, failed_dispatched = self._approve_dispatch(failed_submission)
        failed_root = self._claim_running_root(
            failed_dispatched,
            lease_owner="acquisition-run-writer-non-utc-fail",
        )
        with mock.patch.object(adapter, "_connect", non_utc_connect):
            failed = self.store.repos.workflow_runtime.fail_acquisition_root_command_claim(
                failed_root["command_id"],
                expected_lease_owner=str(failed_root["lease_owner"]),
                expected_lease_expires_at=str(failed_root["lease_expires_at"]),
                expected_attempt=int(failed_root["attempt"]),
                reason="injected_non_utc_preflight_failure",
            )
        self.assertEqual(failed.get("outcome"), "applied", failed)
        self.assertEqual(failed["workflow_command"]["status"], "failed_terminal")

    def test_root_uow_final_update_failure_rolls_back_event_child_and_terminal_result(self) -> None:
        submission = self._submit(idempotency_key="root-uow:rollback")
        _, dispatched = self._approve_dispatch(submission)
        root = self._claim_running_root(
            dispatched,
            lease_owner="acquisition-run-writer-rollback",
        )
        before = self._table_state(FULL_ZERO_WRITE_TABLES)
        adapter = self.store._control_plane_postgres
        with adapter._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE FUNCTION d1i_fail_root_completion() RETURNS trigger AS $$
                    BEGIN
                        IF NEW.status = 'succeeded'
                           AND NEW.command_type = 'acquisition.run.create' THEN
                            RAISE EXCEPTION 'd1i injected root completion failure';
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                    """
                )
                cursor.execute(
                    """
                    CREATE TRIGGER d1i_fail_root_completion
                    BEFORE UPDATE ON workflow_commands
                    FOR EACH ROW EXECUTE FUNCTION d1i_fail_root_completion()
                    """
                )
            connection.commit()

        with self.assertRaisesRegex(RuntimeError, "d1i injected root completion failure"):
            self._complete_root_direct(root)

        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before)

    def test_precommit_uow_exception_is_not_reconciled_as_success(self) -> None:
        submission = self._submit(idempotency_key="root-uow:owner-precommit-failure")
        _, dispatched = self._approve_dispatch(submission)
        workflow_run_id = str(dispatched["workflow_command"]["workflow_run_id"])
        root_command_id = str(dispatched["workflow_command"]["command_id"])
        guarded_tables = tuple(table for table in FULL_ZERO_WRITE_TABLES if table != "workflow_commands")
        before = self._table_state(guarded_tables)
        adapter = self.store._control_plane_postgres
        with adapter._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE FUNCTION d1i_fail_root_owner_completion() RETURNS trigger AS $$
                    BEGIN
                        IF NEW.status = 'succeeded'
                           AND NEW.command_type = 'acquisition.run.create' THEN
                            RAISE EXCEPTION 'd1i injected owner precommit failure';
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                    """
                )
                cursor.execute(
                    """
                    CREATE TRIGGER d1i_fail_root_owner_completion
                    BEFORE UPDATE ON workflow_commands
                    FOR EACH ROW EXECUTE FUNCTION d1i_fail_root_owner_completion()
                    """
                )
            connection.commit()

        with self.assertRaisesRegex(RuntimeError, "d1i injected owner precommit failure"):
            self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
                {"workflow_run_id": workflow_run_id, "command_limit": 1}
            )

        self.assertEqual(self._table_state(guarded_tables), before)
        commands = self.store.list_workflow_commands(workflow_run_id=workflow_run_id, limit=0)
        self.assertEqual(len(commands), 1, commands)
        self.assertEqual(commands[0]["command_id"], root_command_id)
        self.assertEqual(commands[0]["status"], "running")
        self.assertEqual(commands[0]["downstream_command_ids"], [])

    def test_root_child_plan_uses_actual_locked_stream_sequence_not_a_fixed_slot(self) -> None:
        submission = self._submit(idempotency_key="stream-sequence:shared")
        _, dispatched = self._approve_dispatch(submission)
        workflow_run_id = str(dispatched["workflow_command"]["workflow_run_id"])
        extra = self.store.repos.workflow_runtime.append_workflow_event(
            workflow_run_id=workflow_run_id,
            operation_id=str(dispatched["workflow_command"]["operation_id"]),
            event_family="workflow_event",
            event_type="WorkflowSignalReceived",
            idempotency_key="stream-sequence:shared:interleaved",
            actor="test-shared-producer",
            source="test_d1i",
            payload={"signal": "legitimate_shared_stream_event"},
        )
        self.assertGreater(int(extra.get("sequence_number") or 0), 0, extra)

        drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {"workflow_run_id": workflow_run_id, "command_limit": 1}
        )

        self.assertEqual(drained.get("completed_count"), 1, drained)
        events = self.store.repos.workflow_runtime.list_workflow_events(workflow_run_id, limit=0)
        child_plan_events = [
            event
            for event in events
            if event.get("command_id") == dispatched["workflow_command"]["command_id"]
            and event.get("event_type") == "CommandPlanRequested"
        ]
        self.assertEqual(len(child_plan_events), 1, child_plan_events)
        self.assertEqual(
            int(child_plan_events[0]["sequence_number"]),
            int(extra["sequence_number"]) + 1,
        )
        self.assertNotEqual(int(child_plan_events[0]["sequence_number"]), 3)

    def test_preexisting_noncanonical_root_plan_event_blocks_uow_before_child_or_event_writes(self) -> None:
        submission = self._submit(idempotency_key="plan-event-ambiguity:root")
        _, dispatched = self._approve_dispatch(submission)
        root = dict(self.store.get_workflow_command(dispatched["workflow_command"]["command_id"]) or {})
        preexisting = self.store.repos.workflow_runtime.append_workflow_event(
            workflow_run_id=str(root["workflow_run_id"]),
            operation_id=str(root["operation_id"]),
            command_id=str(root["command_id"]),
            event_family="workflow_event",
            event_type="CommandPlanRequested",
            idempotency_key="plan-event-ambiguity:noncanonical",
            actor="brownfield-producer",
            source="test_d1i",
            payload={"command_type": ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE},
        )
        self.assertTrue(preexisting.get("event_id"), preexisting)
        guarded_tables = tuple(table for table in FULL_ZERO_WRITE_TABLES if table != "workflow_commands")
        before = self._table_state(guarded_tables)

        drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {"workflow_run_id": root["workflow_run_id"], "command_limit": 1}
        )

        self.assertEqual(drained.get("failed_count"), 1, drained)
        self.assertEqual(self._table_state(guarded_tables), before)
        commands = self.store.list_workflow_commands(workflow_run_id=root["workflow_run_id"], limit=0)
        self.assertEqual(len(commands), 1, commands)
        self.assertEqual(commands[0]["status"], "failed_terminal")

    def test_preexisting_child_with_invalid_result_container_rolls_back_root_uow(self) -> None:
        submission = self._submit(idempotency_key="preexisting-child-json:root")
        _, dispatched = self._approve_dispatch(submission)
        root = self._claim_running_root(
            dispatched,
            lease_owner="acquisition-run-writer-preexisting-child",
        )
        contract = self._root_uow_contract(root)
        event_spec = dict(contract["plan_event"])
        event = self.store.repos.workflow_runtime.append_workflow_event(
            workflow_run_id=str(event_spec["workflow_run_id"]),
            operation_id=str(event_spec["operation_id"]),
            command_id=str(event_spec["command_id"]),
            event_family=str(event_spec["event_family"]),
            event_type=str(event_spec["event_type"]),
            idempotency_key=str(event_spec["idempotency_key"]),
            actor=str(event_spec["actor"]),
            source=str(event_spec["source"]),
            payload=dict(event_spec["payload"]),
            artifact_refs=list(event_spec["artifact_refs"]),
        )
        child_spec = dict(contract["child_command"])
        child_causality = {
            **dict(contract["child_causality"]),
            "source_event_id": str(event["event_id"]),
            "source_event_type": "CommandPlanRequested",
        }
        child_payload = {
            **dict(child_spec["payload"]),
            "causality": child_causality,
        }
        child = self.store.upsert_workflow_command(
            command_id=str(child_spec["command_id"]),
            workflow_run_id=str(child_spec["workflow_run_id"]),
            operation_id=str(child_spec["operation_id"]),
            command_type=str(child_spec["command_type"]),
            owner=str(child_spec["owner"]),
            idempotency_key=str(child_spec["idempotency_key"]),
            payload=child_payload,
            artifact_refs=list(child_spec["artifact_refs"]),
            max_attempts=int(child_spec["max_attempts"]),
            retry_policy=dict(child_spec["retry_policy"]),
        )
        self._execute_pg(
            "UPDATE {schema}.workflow_commands SET result_json = '[]' WHERE command_id = %s",
            (child["command_id"],),
        )
        before = self._table_state(FULL_ZERO_WRITE_TABLES)

        result = self._complete_root_direct(root)

        self.assertEqual(result.get("outcome"), "conflict", result)
        self.assertEqual(result.get("reason"), "acquisition_root_intent_child_json_invalid", result)
        self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before)

    def test_root_uow_exact_reuse_preserves_preexisting_child_retry_schedule(self) -> None:
        submission = self._submit(idempotency_key="preexisting-child-retry:root")
        _, dispatched = self._approve_dispatch(submission)
        root = self._claim_running_root(
            dispatched,
            lease_owner="acquisition-run-writer-preexisting-retry",
        )
        contract = self._root_uow_contract(root)
        event_spec = dict(contract["plan_event"])
        event = self.store.repos.workflow_runtime.append_workflow_event(
            workflow_run_id=str(event_spec["workflow_run_id"]),
            operation_id=str(event_spec["operation_id"]),
            command_id=str(event_spec["command_id"]),
            event_family=str(event_spec["event_family"]),
            event_type=str(event_spec["event_type"]),
            idempotency_key=str(event_spec["idempotency_key"]),
            actor=str(event_spec["actor"]),
            source=str(event_spec["source"]),
            payload=dict(event_spec["payload"]),
            artifact_refs=list(event_spec["artifact_refs"]),
        )
        child_spec = dict(contract["child_command"])
        child_causality = {
            **dict(contract["child_causality"]),
            "source_event_id": str(event["event_id"]),
            "source_event_type": "CommandPlanRequested",
        }
        child = self.store.upsert_workflow_command(
            command_id=str(child_spec["command_id"]),
            workflow_run_id=str(child_spec["workflow_run_id"]),
            operation_id=str(child_spec["operation_id"]),
            command_type=str(child_spec["command_type"]),
            owner=str(child_spec["owner"]),
            idempotency_key=str(child_spec["idempotency_key"]),
            payload={**dict(child_spec["payload"]), "causality": child_causality},
            artifact_refs=list(child_spec["artifact_refs"]),
            max_attempts=int(child_spec["max_attempts"]),
            retry_policy=dict(child_spec["retry_policy"]),
        )
        claimed = self.store.claim_workflow_command(
            child["command_id"],
            lease_owner="acquisition-planner-preexisting-retry",
            lease_seconds=300,
        )
        self.assertTrue(claimed, child)
        retry_waiting = self.store.mark_workflow_command_failed(
            child["command_id"],
            error_text="injected retry before root terminalization",
            retryable=True,
            retry_delay_seconds=60,
        )
        self.assertEqual(retry_waiting.get("status"), "retry_wait", retry_waiting)
        self.assertTrue(str(retry_waiting.get("not_before_at") or "").strip(), retry_waiting)

        result = self._complete_root_direct(root)

        self.assertEqual(result.get("outcome"), "applied", result)
        persisted_child = self.store.get_workflow_command(child["command_id"])
        self.assertEqual((persisted_child or {}).get("status"), "retry_wait", persisted_child)
        self.assertEqual(
            (persisted_child or {}).get("not_before_at"),
            retry_waiting.get("not_before_at"),
        )
        persisted_root = self.store.get_workflow_command(root["command_id"])
        self.assertEqual((persisted_root or {}).get("status"), "succeeded", persisted_root)
        self.assertEqual(
            (persisted_root or {}).get("downstream_command_ids"),
            [child["command_id"]],
        )

    def test_postcommit_reducer_failure_is_repaired_by_exact_succeeded_replay(self) -> None:
        submission = self._submit(idempotency_key="postcommit-repair:root")
        _, dispatched = self._approve_dispatch(submission)
        workflow_run_id = str(dispatched["workflow_command"]["workflow_run_id"])
        owner = self.orchestrator._acquisition_command_owner  # noqa: SLF001
        with mock.patch.object(
            owner.durable_runtime_writer,
            "reduce_and_persist",
            side_effect=RuntimeError("injected postcommit reducer failure"),
        ):
            drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
                {"workflow_run_id": workflow_run_id, "command_limit": 1}
            )

        self.assertEqual(drained.get("completed_count"), 1, drained)
        root = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        self.assertEqual((root or {}).get("status"), "succeeded", root)
        plan_event = [
            event
            for event in self.store.repos.workflow_runtime.list_workflow_events(workflow_run_id, limit=0)
            if event.get("command_id") == dispatched["workflow_command"]["command_id"]
        ][0]
        stale_state = self.store.repos.workflow_runtime.get_workflow_current_state(workflow_run_id)
        self.assertLess(
            int(stale_state.get("last_processed_sequence_number") or 0),
            int(plan_event["sequence_number"]),
        )

        replay = self.orchestrator._run_acquisition_run_create_command(dict(root or {}))  # noqa: SLF001

        self.assertEqual(replay.get("status"), "completed", replay)
        repaired_state = self.store.repos.workflow_runtime.get_workflow_current_state(workflow_run_id)
        self.assertEqual(
            int(repaired_state.get("last_processed_sequence_number") or 0),
            int(plan_event["sequence_number"]),
        )
        self.assertEqual(
            len(self.store.list_workflow_commands(workflow_run_id=workflow_run_id, limit=0)),
            2,
        )

    def test_commit_acknowledgement_ambiguity_routes_succeeded_row_through_exact_replay(self) -> None:
        submission = self._submit(idempotency_key="commit-ambiguity:root")
        approved, dispatched = self._approve_dispatch(submission)
        repository = self.store.repos.workflow_runtime
        real_complete = repository.complete_acquisition_root_command

        def commit_then_report_stale(*args: Any, **kwargs: Any) -> dict[str, Any]:
            committed = real_complete(*args, **kwargs)
            self.assertEqual(committed.get("outcome"), "applied", committed)
            return {
                **committed,
                "outcome": "stale_claim",
                "reason": "injected_ambiguous_commit_acknowledgement",
            }

        with mock.patch.object(
            repository,
            "complete_acquisition_root_command",
            side_effect=commit_then_report_stale,
        ):
            drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
                {
                    "workflow_run_id": dispatched["workflow_command"]["workflow_run_id"],
                    "command_limit": 1,
                }
            )

        self.assertEqual(drained.get("completed_count"), 1, drained)
        self.assertEqual(
            drained["items"][0].get("reason"),
            "acquisition_run_create_command_already_succeeded",
        )
        root = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        self.assertEqual((root or {}).get("status"), "succeeded", root)
        self.assertEqual(
            self.store.repos.workflow_runtime.get_operation(approved["operation_run"]["operation_run_id"])["status"],
            "running",
        )
        self.assertEqual(
            len(
                self.store.list_workflow_commands(
                    workflow_run_id=dispatched["workflow_command"]["workflow_run_id"],
                    limit=0,
                )
            ),
            2,
        )

    def test_successful_commit_exception_routes_authoritative_row_through_exact_replay(self) -> None:
        submission = self._submit(idempotency_key="commit-exception:root")
        approved, dispatched = self._approve_dispatch(submission)
        repository = self.store.repos.workflow_runtime
        real_complete = repository.complete_acquisition_root_command
        adapter = self.store._control_plane_postgres
        original_connect = adapter._connect
        fixture = self._pg_durable_runtime_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        disconnect_injected = False

        class _PostCommitDisconnect:
            def __init__(self, delegate: Any) -> None:
                self.delegate = delegate

            def __enter__(self) -> _PostCommitDisconnect:
                self.delegate.__enter__()
                return self

            def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
                return bool(self.delegate.__exit__(exc_type, exc, traceback))

            def commit(self) -> None:
                nonlocal disconnect_injected
                self.delegate.commit()
                if disconnect_injected:
                    return
                disconnect_injected = True
                backend_pid = int(self.delegate.info.backend_pid)
                with psycopg.connect(
                    fixture.dsn,
                    autocommit=True,
                    connect_timeout=5,
                    client_encoding="utf8",
                ) as killer:
                    with killer.cursor() as cursor:
                        cursor.execute("SELECT pg_terminate_backend(%s)", (backend_pid,))
                        if not bool(cursor.fetchone()[0]):
                            raise AssertionError("failed to terminate committed root UoW backend")
                # The server-side COMMIT above is durable. This next round trip
                # turns the killed session into a real psycopg connection error
                # at the adapter commit boundary, modeling a lost acknowledgement.
                with self.delegate.cursor() as cursor:
                    cursor.execute("SELECT 1")

            def __getattr__(self, name: str) -> Any:
                return getattr(self.delegate, name)

        def commit_then_disconnect(*args: Any, **kwargs: Any) -> dict[str, Any]:
            with mock.patch.object(
                adapter,
                "_connect",
                side_effect=lambda: _PostCommitDisconnect(original_connect()),
            ):
                return real_complete(*args, **kwargs)

        with mock.patch.object(
            repository,
            "complete_acquisition_root_command",
            side_effect=commit_then_disconnect,
        ):
            drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
                {
                    "workflow_run_id": dispatched["workflow_command"]["workflow_run_id"],
                    "command_limit": 1,
                }
            )

        self.assertTrue(disconnect_injected)
        self.assertEqual(drained.get("completed_count"), 1, drained)
        self.assertEqual(
            drained["items"][0].get("reason"),
            "acquisition_run_create_command_already_succeeded",
        )
        root = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        self.assertEqual((root or {}).get("status"), "succeeded", root)
        children = [
            command
            for command in self.store.list_workflow_commands(
                workflow_run_id=dispatched["workflow_command"]["workflow_run_id"],
                limit=0,
            )
            if command["command_type"] == ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE
        ]
        self.assertEqual(len(children), 1, children)
        self.assertEqual(
            (root or {}).get("downstream_command_ids"),
            [children[0]["command_id"]],
        )
        self.assertEqual(
            self.store.repos.workflow_runtime.get_operation(approved["operation_run"]["operation_run_id"])["status"],
            "running",
        )
        self.assertEqual(
            len(
                self.store.list_workflow_commands(
                    workflow_run_id=dispatched["workflow_command"]["workflow_run_id"],
                    limit=0,
                )
            ),
            2,
        )

    def test_fail_cas_late_success_routes_authoritative_terminal_row_through_exact_replay(self) -> None:
        submission = self._submit(idempotency_key="fail-cas-late-success:root")
        _, dispatched = self._approve_dispatch(submission)
        owner = self.orchestrator._acquisition_command_owner  # noqa: SLF001
        repository = self.store.repos.workflow_runtime
        real_preflight = owner._preflight_acquisition_root_operation_action_command  # noqa: SLF001
        preflight_calls = 0

        def fail_first_preflight(command: dict[str, Any]) -> dict[str, Any]:
            nonlocal preflight_calls
            preflight_calls += 1
            if preflight_calls == 1:
                return {"status": "invalid", "reason": "injected_preflight_race"}
            return real_preflight(command)

        def complete_before_old_fail_cas(
            command_id: str,
            *,
            expected_lease_owner: str,
            expected_lease_expires_at: str,
            expected_attempt: int,
            reason: str,
        ) -> dict[str, Any]:
            del reason
            running = dict(self.store.get_workflow_command(command_id) or {})
            contract = self._root_uow_contract(running)
            committed = repository.complete_acquisition_root_command(
                command_id,
                expected_lease_owner=expected_lease_owner,
                expected_lease_expires_at=expected_lease_expires_at,
                expected_attempt=expected_attempt,
                expected_root_command=owner._acquisition_root_locked_identity(running),  # noqa: SLF001
                plan_event=dict(contract.get("plan_event") or {}),
                child_command=dict(contract.get("child_command") or {}),
                child_causality=dict(contract.get("child_causality") or {}),
                root_result=dict(contract.get("root_result") or {}),
            )
            self.assertEqual(committed.get("outcome"), "applied", committed)
            return {
                "outcome": "stale_claim",
                "reason": "injected_late_success_before_fail_cas",
                "workflow_command": dict(committed.get("workflow_command") or {}),
            }

        with (
            mock.patch.object(
                owner,
                "_preflight_acquisition_root_operation_action_command",
                side_effect=fail_first_preflight,
            ),
            mock.patch.object(
                repository,
                "fail_acquisition_root_command_claim",
                side_effect=complete_before_old_fail_cas,
            ),
        ):
            drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
                {
                    "workflow_run_id": dispatched["workflow_command"]["workflow_run_id"],
                    "command_limit": 1,
                }
            )

        self.assertEqual(drained.get("completed_count"), 1, drained)
        self.assertEqual(preflight_calls, 2)
        root = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])
        self.assertEqual((root or {}).get("status"), "succeeded", root)
        self.assertEqual(
            len(
                self.store.list_workflow_commands(
                    workflow_run_id=dispatched["workflow_command"]["workflow_run_id"],
                    limit=0,
                )
            ),
            2,
        )

    def test_succeeded_replay_rejects_forged_result_event_and_child_shapes_with_zero_write(self) -> None:
        variants = (
            "empty_result",
            "forged_result",
            "result_count_bool",
            "result_boolean_int",
            "root_physical_causality",
            "root_physical_foreign_child",
            "root_physical_extra_child",
            "missing_plan_event",
            "forged_plan_event",
            "blank_plan_event_schema",
            "reordered_plan_event",
            "missing_child",
            "foreign_child",
            "child_count_bool",
            "child_artifact_container_object",
        )
        for variant in variants:
            with self.subTest(variant=variant):
                scope = self._complete_root_through_owner(idempotency_key=f"replay-guard:{variant}")
                root = dict(scope["root"])
                child = dict(scope["child"])
                if variant == "empty_result":
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET result_json = '{{}}' WHERE command_id = %s",
                        (root["command_id"],),
                    )
                elif variant == "forged_result":
                    forged_result = {**dict(root.get("result") or {}), "target_company": "Foreign Lab"}
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET result_json = %s WHERE command_id = %s",
                        (json.dumps(forged_result), root["command_id"]),
                    )
                elif variant == "result_count_bool":
                    forged_result = {
                        **dict(root.get("result") or {}),
                        "downstream_command_count": True,
                    }
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET result_json = %s WHERE command_id = %s",
                        (json.dumps(forged_result), root["command_id"]),
                    )
                elif variant == "result_boolean_int":
                    forged_result = {
                        **dict(root.get("result") or {}),
                        "module_state_mutated": 0,
                    }
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET result_json = %s WHERE command_id = %s",
                        (json.dumps(forged_result), root["command_id"]),
                    )
                elif variant == "root_physical_causality":
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET downstream_command_ids_json = '[]' WHERE command_id = %s",
                        (root["command_id"],),
                    )
                elif variant == "root_physical_foreign_child":
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET downstream_command_ids_json = %s WHERE command_id = %s",
                        (json.dumps(["cmd_foreign_child"]), root["command_id"]),
                    )
                elif variant == "root_physical_extra_child":
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET downstream_command_ids_json = %s WHERE command_id = %s",
                        (
                            json.dumps([child["command_id"], "cmd_extra_child"]),
                            root["command_id"],
                        ),
                    )
                elif variant == "missing_plan_event":
                    self._execute_pg(
                        "DELETE FROM {schema}.workflow_events WHERE event_id = %s",
                        (child["source_event_id"],),
                    )
                elif variant == "forged_plan_event":
                    forged_payload = {
                        **next(
                            event["payload"]
                            for event in self.store.repos.workflow_runtime.list_workflow_events(
                                root["workflow_run_id"],
                                limit=0,
                            )
                            if event["event_id"] == child["source_event_id"]
                        ),
                        "stage_id": "forged-stage",
                    }
                    self._execute_pg(
                        "UPDATE {schema}.workflow_events SET payload_json = %s WHERE event_id = %s",
                        (json.dumps(forged_payload), child["source_event_id"]),
                    )
                elif variant == "blank_plan_event_schema":
                    self._execute_pg(
                        "UPDATE {schema}.workflow_events SET schema_version = '' WHERE event_id = %s",
                        (child["source_event_id"],),
                    )
                elif variant == "reordered_plan_event":
                    plan_event = next(
                        event
                        for event in self.store.repos.workflow_runtime.list_workflow_events(
                            root["workflow_run_id"],
                            limit=0,
                        )
                        if event["event_id"] == child["source_event_id"]
                    )
                    workflow_started = next(
                        event
                        for event in self.store.repos.workflow_runtime.list_workflow_events(
                            root["workflow_run_id"],
                            limit=0,
                        )
                        if int(event["sequence_number"]) == 1
                    )
                    reordered_event_id = (
                        "evt_"
                        + hashlib.sha1(
                            f"{root['workflow_run_id']}:1:{plan_event['idempotency_key']}".encode()
                        ).hexdigest()[:24]
                    )
                    self._execute_pg(
                        "UPDATE {schema}.workflow_events SET sequence_number = 100 WHERE event_id = %s",
                        (workflow_started["event_id"],),
                    )
                    self._execute_pg(
                        """
                        UPDATE {schema}.workflow_events
                        SET sequence_number = 1, event_id = %s
                        WHERE event_id = %s
                        """,
                        (reordered_event_id, child["source_event_id"]),
                    )
                    child_payload = dict(child.get("payload") or {})
                    child_payload["causality"] = {
                        **dict(child_payload.get("causality") or {}),
                        "source_event_id": reordered_event_id,
                    }
                    self._execute_pg(
                        """
                        UPDATE {schema}.workflow_commands
                        SET source_event_id = %s, payload_json = %s
                        WHERE command_id = %s
                        """,
                        (reordered_event_id, json.dumps(child_payload), child["command_id"]),
                    )
                elif variant == "missing_child":
                    self._execute_pg(
                        "DELETE FROM {schema}.workflow_commands WHERE command_id = %s",
                        (child["command_id"],),
                    )
                elif variant == "foreign_child":
                    payload = dict(child.get("payload") or {})
                    payload["causality"] = {
                        **dict(payload.get("causality") or {}),
                        "parent_command_id": "cmd_foreign_root",
                    }
                    self._execute_pg(
                        """
                        UPDATE {schema}.workflow_commands
                        SET parent_command_id = %s, payload_json = %s
                        WHERE command_id = %s
                        """,
                        ("cmd_foreign_root", json.dumps(payload), child["command_id"]),
                    )
                elif variant == "child_count_bool":
                    child_payload = {
                        **dict(child.get("payload") or {}),
                        "intent_count": True,
                    }
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET payload_json = %s WHERE command_id = %s",
                        (json.dumps(child_payload), child["command_id"]),
                    )
                elif variant == "child_artifact_container_object":
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET artifact_refs_json = '{{}}' WHERE command_id = %s",
                        (child["command_id"],),
                    )

                latest_root = dict(self.store.get_workflow_command(root["command_id"]) or {})
                before = self._table_state(FULL_ZERO_WRITE_TABLES)
                replay = self.orchestrator._run_acquisition_run_create_command(latest_root)  # noqa: SLF001

                self.assertEqual(replay.get("status"), "failed", replay)
                self.assertIn(
                    replay.get("reason"),
                    {
                        "acquisition_root_terminal_result_mismatch",
                        "acquisition_root_physical_causality_mismatch",
                        "acquisition_root_plan_event_ambiguous",
                        "acquisition_root_plan_event_identity_mismatch",
                        "acquisition_root_plan_event_json_contract_invalid",
                        "acquisition_root_intent_child_ambiguous",
                        "acquisition_root_intent_child_identity_mismatch",
                        "acquisition_root_intent_child_json_contract_invalid",
                        "acquisition_root_persisted_json_contract_invalid",
                    },
                    replay,
                )
                self.assertEqual(self._table_state(FULL_ZERO_WRITE_TABLES), before)

    def test_cancelled_root_never_creates_a_child_or_resurrects_operation(self) -> None:
        cancelled_submission = self._submit(idempotency_key="guard:cancelled")
        cancelled_approved, cancelled_dispatched = self._approve_dispatch(cancelled_submission)
        cancelled_run_id = cancelled_approved["operation_run"]["operation_run_id"]
        cancelled = self.orchestrator.cancel_operation_run_api(cancelled_run_id, {"actor": "alice"})
        self.assertEqual(cancelled.get("status"), "cancelled", cancelled)
        guarded_tables = (
            "workflow_events",
            "workflow_current_state",
            "runtime_outbox",
            "plan_review_sessions",
            "acquisition_runs",
            "jobs",
        )
        before_cancelled_drain = self._table_state(guarded_tables)
        cancelled_drain = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {"workflow_run_id": cancelled_dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
        )
        self.assertEqual(cancelled_drain.get("failed_count"), 1, cancelled_drain)
        self.assertEqual(
            cancelled_drain["items"][0].get("reason"),
            "acquisition_root_command_operation_terminal",
        )
        self.assertEqual(self._table_state(guarded_tables), before_cancelled_drain)
        self.assertEqual(self.store.repos.workflow_runtime.get_operation(cancelled_run_id)["status"], "cancelled")
        self.assertEqual(
            self.store.repos.workflow_runtime.get_action(cancelled_submission["action"]["action_id"])["status"],
            "cancelled",
        )
        self.assertEqual(
            len(
                self.store.list_workflow_commands(
                    workflow_run_id=cancelled_dispatched["workflow_command"]["workflow_run_id"],
                    limit=100,
                )
            ),
            1,
        )

    def test_command_authority_and_envelope_mismatches_only_terminalize_the_root(self) -> None:
        guarded_tables = (
            "workflow_events",
            "workflow_current_state",
            "runtime_outbox",
            "workflow_activity_runs",
            "workflow_activity_attempts",
            "workflow_entity_deltas",
            "plan_review_sessions",
            "acquisition_runs",
            "jobs",
        )
        variants = (
            "missing_action",
            "foreign_operation",
            "payload",
            "bound_target",
            "malformed_bound_target",
            "max_attempts",
            "retry_policy",
        )
        for variant in variants:
            with self.subTest(variant=variant):
                submission = self._submit(idempotency_key=f"command-guard:{variant}")
                approved, dispatched = self._approve_dispatch(submission)
                command_id = dispatched["workflow_command"]["command_id"]
                command = self.store.get_workflow_command(command_id)
                payload = dict((command or {}).get("payload") or {})
                foreign_run_id = ""
                if variant == "missing_action":
                    payload["action_id"] = "action-missing"
                    self.store.update_workflow_command_payload(command_id, payload=payload)
                elif variant == "foreign_operation":
                    foreign_submission = self._submit(idempotency_key=f"command-guard:{variant}:foreign")
                    foreign_approved = self.orchestrator.approve_operation_action_api(
                        foreign_submission["action"]["action_id"],
                        {"actor": "alice"},
                    )
                    foreign_run_id = foreign_approved["operation_run"]["operation_run_id"]
                    payload["operation_id"] = foreign_run_id
                    self.store.update_workflow_command_payload(command_id, payload=payload)
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET operation_id = %s WHERE command_id = %s",
                        (foreign_run_id, command_id),
                    )
                elif variant == "payload":
                    payload["query"] = "forged query"
                    self.store.update_workflow_command_payload(command_id, payload=payload)
                elif variant == "bound_target":
                    payload["acquisition_root_target"] = {"workspace_id": "foreign"}
                    self.store.update_workflow_command_payload(command_id, payload=payload)
                elif variant == "malformed_bound_target":
                    payload["acquisition_root_target"] = ["user-alice"]
                    self.store.update_workflow_command_payload(command_id, payload=payload)
                elif variant == "max_attempts":
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET max_attempts = %s WHERE command_id = %s",
                        (99, command_id),
                    )
                elif variant == "retry_policy":
                    self._execute_pg(
                        "UPDATE {schema}.workflow_commands SET retry_policy_json = %s WHERE command_id = %s",
                        (json.dumps(["malformed"]), command_id),
                    )

                original_run_id = approved["operation_run"]["operation_run_id"]
                original_status = self.store.repos.workflow_runtime.get_operation(original_run_id)["status"]
                foreign_status = (
                    self.store.repos.workflow_runtime.get_operation(foreign_run_id)["status"] if foreign_run_id else ""
                )
                before = self._table_state(guarded_tables)
                drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
                    {"workflow_run_id": dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
                )
                self.assertEqual(drained.get("failed_count"), 1, drained)
                self.assertEqual(self._table_state(guarded_tables), before)
                self.assertEqual(
                    self.store.repos.workflow_runtime.get_operation(original_run_id)["status"],
                    original_status,
                )
                if foreign_run_id:
                    self.assertEqual(
                        self.store.repos.workflow_runtime.get_operation(foreign_run_id)["status"],
                        foreign_status,
                    )
                commands = self.store.list_workflow_commands(
                    workflow_run_id=dispatched["workflow_command"]["workflow_run_id"],
                    limit=100,
                )
                self.assertEqual(len(commands), 1)
                self.assertEqual(commands[0]["command_id"], command_id)

    def test_paired_root_causality_and_canonical_envelope_mutations_fail_before_child_writes(self) -> None:
        causality_variants: tuple[tuple[str, str, str, Any], ...] = (
            ("stage_id", "stage_id", "stage_id", "forged-stage"),
            ("causal_group_id", "causal_group_id", "causal_group_id", "forged-group"),
            ("parent_command_id", "parent_command_id", "parent_command_id", "cmd_forged_parent"),
            ("source_event_id", "source_event_id", "source_event_id", "evt_forged_source"),
            ("source_event_type", "source_event_type", "source_event_type", "ForgedEvent"),
            (
                "input_artifact_refs",
                "input_artifact_refs_json",
                "input_artifact_refs",
                ["artifact://forged-input"],
            ),
            (
                "output_artifact_refs",
                "output_artifact_refs_json",
                "output_artifact_refs",
                ["artifact://forged-output"],
            ),
            (
                "produced_entity_counts",
                "produced_entity_counts_json",
                "produced_entity_counts",
                {"forged": 1},
            ),
            ("no_op_reason", "no_op_reason", "no_op_reason", "forged-no-op"),
            ("readiness_effect", "readiness_effect", "readiness_effect", "forged-ready"),
            (
                "downstream_command_ids",
                "downstream_command_ids_json",
                "downstream_command_ids",
                ["cmd_forged_downstream"],
            ),
            (
                "causality_schema_version",
                "causality_schema_version",
                "schema_version",
                "command_causality_v999",
            ),
            ("idempotency_key", "idempotency_key", "idempotency_key", "forged-idempotency"),
        )
        guarded_tables = tuple(table for table in FULL_ZERO_WRITE_TABLES if table != "workflow_commands")
        for name, physical_column, payload_field, forged_value in causality_variants:
            with self.subTest(variant=name):
                submission = self._submit(idempotency_key=f"causality-guard:{name}")
                _, dispatched = self._approve_dispatch(submission)
                command_id = str(dispatched["workflow_command"]["command_id"])
                root = dict(self.store.get_workflow_command(command_id) or {})
                payload = dict(root.get("payload") or {})
                payload["causality"] = {
                    **dict(payload.get("causality") or {}),
                    payload_field: forged_value,
                }
                physical_value = json.dumps(forged_value) if physical_column.endswith("_json") else str(forged_value)
                self._execute_pg(
                    f"""
                    UPDATE {{schema}}.workflow_commands
                    SET {physical_column} = %s, payload_json = %s
                    WHERE command_id = %s
                    """,
                    (physical_value, json.dumps(payload), command_id),
                )
                before = self._table_state(guarded_tables)

                drained = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
                    {
                        "workflow_run_id": dispatched["workflow_command"]["workflow_run_id"],
                        "command_limit": 1,
                    }
                )

                self.assertEqual(drained.get("failed_count"), 1, drained)
                self.assertEqual(self._table_state(guarded_tables), before)
                commands = self.store.list_workflow_commands(
                    workflow_run_id=dispatched["workflow_command"]["workflow_run_id"],
                    limit=0,
                )
                self.assertEqual(len(commands), 1, commands)
                self.assertEqual(commands[0]["status"], "failed_terminal")
                self.assertEqual(commands[0]["command_id"], command_id)

        command_id_submission = self._submit(idempotency_key="causality-guard:command-id")
        command_id_approved, command_id_dispatched = self._approve_dispatch(command_id_submission)
        original_command_id = str(command_id_dispatched["workflow_command"]["command_id"])
        forged_command_id = "cmd_forged_deterministic_root"
        forged_workflow_ref = {
            **dict(command_id_approved["operation_run"].get("workflow_ref") or {}),
            "command_id": forged_command_id,
        }
        self._execute_pg(
            "UPDATE {schema}.workflow_commands SET command_id = %s WHERE command_id = %s",
            (forged_command_id, original_command_id),
        )
        self._execute_pg(
            "UPDATE {schema}.operation_runs SET workflow_ref_json = %s WHERE operation_run_id = %s",
            (
                json.dumps(forged_workflow_ref),
                command_id_approved["operation_run"]["operation_run_id"],
            ),
        )
        before_command_id = self._table_state(guarded_tables)
        command_id_result = self.orchestrator._run_acquisition_run_create_command(  # noqa: SLF001
            dict(self.store.get_workflow_command(forged_command_id) or {})
        )
        self.assertEqual(command_id_result.get("status"), "failed", command_id_result)
        self.assertEqual(self._table_state(guarded_tables), before_command_id)
        self.assertEqual(
            len(
                self.store.list_workflow_commands(
                    workflow_run_id=command_id_dispatched["workflow_command"]["workflow_run_id"],
                    limit=0,
                )
            ),
            1,
        )

        operation_type_submission = self._submit(idempotency_key="causality-guard:operation-type")
        operation_type_approved, operation_type_dispatched = self._approve_dispatch(operation_type_submission)
        self._execute_pg(
            "UPDATE {schema}.agent_actions SET operation_type = %s WHERE action_id = %s",
            ("forged_operation_type", operation_type_submission["action"]["action_id"]),
        )
        self._execute_pg(
            "UPDATE {schema}.operation_runs SET operation_type = %s WHERE operation_run_id = %s",
            (
                "forged_operation_type",
                operation_type_approved["operation_run"]["operation_run_id"],
            ),
        )
        before_operation_type = self._table_state(guarded_tables)
        operation_type_result = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {
                "workflow_run_id": operation_type_dispatched["workflow_command"]["workflow_run_id"],
                "command_limit": 1,
            }
        )
        self.assertEqual(operation_type_result.get("failed_count"), 1, operation_type_result)
        self.assertEqual(self._table_state(guarded_tables), before_operation_type)
        operation_type_commands = self.store.list_workflow_commands(
            workflow_run_id=operation_type_dispatched["workflow_command"]["workflow_run_id"],
            limit=0,
        )
        self.assertEqual(len(operation_type_commands), 1, operation_type_commands)
        self.assertEqual(operation_type_commands[0]["status"], "failed_terminal")

        event_submission = self._submit(idempotency_key="causality-guard:source-envelope")
        _, event_dispatched = self._approve_dispatch(event_submission)
        event_root = dict(self.store.get_workflow_command(event_dispatched["workflow_command"]["command_id"]) or {})
        self._execute_pg(
            "UPDATE {schema}.workflow_events SET actor = %s WHERE event_id = %s",
            ("forged-source-actor", event_root["source_event_id"]),
        )
        before_event = self._table_state(guarded_tables)
        event_result = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {
                "workflow_run_id": event_dispatched["workflow_command"]["workflow_run_id"],
                "command_limit": 1,
            }
        )
        self.assertEqual(event_result.get("failed_count"), 1, event_result)
        self.assertEqual(self._table_state(guarded_tables), before_event)
        event_commands = self.store.list_workflow_commands(
            workflow_run_id=event_dispatched["workflow_command"]["workflow_run_id"],
            limit=0,
        )
        self.assertEqual(len(event_commands), 1, event_commands)
        self.assertEqual(event_commands[0]["status"], "failed_terminal")

        schema_submission = self._submit(idempotency_key="causality-guard:source-schema")
        _, schema_dispatched = self._approve_dispatch(schema_submission)
        schema_root = dict(self.store.get_workflow_command(schema_dispatched["workflow_command"]["command_id"]) or {})
        self._execute_pg(
            "UPDATE {schema}.workflow_events SET schema_version = '' WHERE event_id = %s",
            (schema_root["source_event_id"],),
        )
        before_schema = self._table_state(guarded_tables)
        schema_result = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {
                "workflow_run_id": schema_dispatched["workflow_command"]["workflow_run_id"],
                "command_limit": 1,
            }
        )
        self.assertEqual(schema_result.get("failed_count"), 1, schema_result)
        self.assertEqual(self._table_state(guarded_tables), before_schema)
        schema_commands = self.store.list_workflow_commands(
            workflow_run_id=schema_root["workflow_run_id"],
            limit=0,
        )
        self.assertEqual(len(schema_commands), 1, schema_commands)
        self.assertEqual(schema_commands[0]["status"], "failed_terminal")

        source_json_submission = self._submit(idempotency_key="causality-guard:source-json-shape")
        _, source_json_dispatched = self._approve_dispatch(source_json_submission)
        source_json_root = dict(
            self.store.get_workflow_command(source_json_dispatched["workflow_command"]["command_id"]) or {}
        )
        self._execute_pg(
            "UPDATE {schema}.workflow_events SET artifact_refs_json = '{{}}' WHERE event_id = %s",
            (source_json_root["source_event_id"],),
        )
        before_source_json = self._table_state(guarded_tables)
        source_json_result = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {
                "workflow_run_id": source_json_root["workflow_run_id"],
                "command_limit": 1,
            }
        )
        self.assertEqual(source_json_result.get("failed_count"), 1, source_json_result)
        self.assertEqual(self._table_state(guarded_tables), before_source_json)
        source_json_commands = self.store.list_workflow_commands(
            workflow_run_id=source_json_root["workflow_run_id"],
            limit=0,
        )
        self.assertEqual(len(source_json_commands), 1, source_json_commands)
        self.assertEqual(source_json_commands[0]["status"], "failed_terminal")

        root_json_submission = self._submit(idempotency_key="causality-guard:root-json-shape")
        _, root_json_dispatched = self._approve_dispatch(root_json_submission)
        root_json_command_id = str(root_json_dispatched["workflow_command"]["command_id"])
        self._execute_pg(
            "UPDATE {schema}.workflow_commands SET input_artifact_refs_json = '{{}}' WHERE command_id = %s",
            (root_json_command_id,),
        )
        before_root_json = self._table_state(guarded_tables)
        root_json_result = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {
                "workflow_run_id": root_json_dispatched["workflow_command"]["workflow_run_id"],
                "command_limit": 1,
            }
        )
        self.assertEqual(root_json_result.get("failed_count"), 1, root_json_result)
        self.assertEqual(self._table_state(guarded_tables), before_root_json)
        root_json_commands = self.store.list_workflow_commands(
            workflow_run_id=root_json_dispatched["workflow_command"]["workflow_run_id"],
            limit=0,
        )
        self.assertEqual(len(root_json_commands), 1, root_json_commands)
        self.assertEqual(root_json_commands[0]["status"], "failed_terminal")

        typed_submission = self._submit(idempotency_key="causality-guard:type-strict-run-count")
        _, typed_dispatched = self._approve_dispatch(typed_submission)
        typed_root = dict(self.store.get_workflow_command(typed_dispatched["workflow_command"]["command_id"]) or {})
        typed_payload = {**dict(typed_root.get("payload") or {}), "run_count": True}
        source_event = next(
            event
            for event in self.store.repos.workflow_runtime.list_workflow_events(
                typed_root["workflow_run_id"],
                limit=0,
            )
            if event["event_id"] == typed_root["source_event_id"]
        )
        source_event_payload = dict(source_event.get("payload") or {})
        source_event_payload["payload"] = {
            **dict(source_event_payload.get("payload") or {}),
            "run_count": True,
        }
        self._execute_pg(
            "UPDATE {schema}.workflow_commands SET payload_json = %s WHERE command_id = %s",
            (json.dumps(typed_payload), typed_root["command_id"]),
        )
        self._execute_pg(
            "UPDATE {schema}.workflow_events SET payload_json = %s WHERE event_id = %s",
            (json.dumps(source_event_payload), typed_root["source_event_id"]),
        )
        before_typed = self._table_state(guarded_tables)

        typed_result = self.orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
            {
                "workflow_run_id": typed_root["workflow_run_id"],
                "command_limit": 1,
            }
        )

        self.assertEqual(typed_result.get("failed_count"), 1, typed_result)
        self.assertEqual(self._table_state(guarded_tables), before_typed)
        typed_commands = self.store.list_workflow_commands(
            workflow_run_id=typed_root["workflow_run_id"],
            limit=0,
        )
        self.assertEqual(len(typed_commands), 1, typed_commands)
        self.assertEqual(typed_commands[0]["status"], "failed_terminal")

    def test_running_transition_loss_never_invokes_root_executor(self) -> None:
        submission = self._submit(idempotency_key="command-guard:running-transition")
        _, dispatched = self._approve_dispatch(submission)
        command = self.store.get_workflow_command(dispatched["workflow_command"]["command_id"])

        with (
            mock.patch.object(self.store, "mark_workflow_command_running", return_value={}),
            mock.patch.object(
                self.orchestrator._acquisition_command_owner,  # noqa: SLF001
                "_execute_acquisition_run_create_command_payload",
            ) as execute,
        ):
            result = self.orchestrator._run_acquisition_run_create_command(dict(command or {}))  # noqa: SLF001

        self.assertEqual(result.get("status"), "queued", result)
        self.assertEqual(result.get("reason"), "acquisition_run_create_command_running_transition_not_applied")
        execute.assert_not_called()
        self.assertEqual(
            len(
                self.store.list_workflow_commands(
                    workflow_run_id=dispatched["workflow_command"]["workflow_run_id"],
                    limit=100,
                )
            ),
            1,
        )


if __name__ == "__main__":
    unittest.main()
