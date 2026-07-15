from __future__ import annotations

import json
import tempfile
import unittest
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


def test_acquisition_root_is_the_fifth_schema_defined_unserved_action() -> None:
    records = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    schema_defined = {
        action_type for action_type in records if DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema
    }
    spec = DEFAULT_ACTION_REGISTRY.spec_for(ACTION_START_ACQUISITION_RUN)

    assert set(ACQUISITION_ROOT_ACTION_REQUEST_CONTRACTS) == {ACTION_START_ACQUISITION_RUN}
    assert schema_defined == set(OPERATION_OWNER_BOUND_ACTION_TYPES)
    assert len(schema_defined) == 5
    assert sum(not DEFAULT_ACTION_REGISTRY.spec_for(action_type).has_request_schema for action_type in records) == 10
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
