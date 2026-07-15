from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Callable

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.operation_runtime import (
    ACTION_ADD_CRM_NOTE,
    ACTION_EXPORT_CANDIDATES,
    ACTION_FILTER_PROJECTION,
    ACTION_SET_CRM_STAGE,
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

ZERO_WRITE_TABLES = (
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
    "crm_records",
    "crm_engagements",
    "crm_tasks",
    "crm_events",
)


class D1gOperationAPIExactOwnerPGTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir, schema_label="d1g_operation_owner")
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "d1g-operation-owner.db",
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
        self.writer = self.orchestrator.operation_runtime_writer

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def _table_state(self, table_names: tuple[str, ...] = ZERO_WRITE_TABLES) -> dict[str, tuple[str, ...]]:
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

    def _execute_pg(self, statement: str, parameters: tuple[Any, ...]) -> None:
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
                cursor.execute(statement.format(schema=quoted_schema), parameters)

    def _bind_planned_command(
        self,
        operation_run: dict[str, Any],
        *,
        suffix: str,
        command_type: str,
        owner: str,
    ) -> dict[str, Any]:
        command = self.store.upsert_workflow_command(
            workflow_run_id=f"workflow-{suffix}",
            operation_id=str(operation_run.get("operation_run_id") or ""),
            command_type=command_type,
            owner=owner,
            idempotency_key=f"planned-guard:{suffix}",
        )
        self.store.repos.workflow_runtime.update_operation_state(
            str(operation_run.get("operation_run_id") or ""),
            status="planned",
            workflow_ref_patch={
                "workflow_run_id": command["workflow_run_id"],
                "command_id": command["command_id"],
                "command_type": command["command_type"],
                "owner": command["owner"],
            },
        )
        return command

    def _submit_filter(self, suffix: str, *, workspace_id: str) -> Any:
        return self.writer.submit_action(
            action_type=ACTION_FILTER_PROJECTION,
            workspace_id=workspace_id,
            target_ref={"projection_id": f"projection-{suffix}"},
            input_payload={"filters": {"role": ["researcher"]}},
            idempotency_key=f"filter:{suffix}",
            actor="fixture",
        )

    def _submit_export(self, suffix: str, *, workspace_id: str) -> Any:
        return self.writer.submit_action(
            action_type=ACTION_EXPORT_CANDIDATES,
            workspace_id=workspace_id,
            target_ref={"projection_id": f"projection-{suffix}"},
            input_payload={"include_crm_notes": True},
            idempotency_key=f"export:{suffix}",
            actor="fixture",
        )

    def _approved_export(self, suffix: str, *, workspace_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        submitted = self._submit_export(suffix, workspace_id=workspace_id)
        approved = self.orchestrator.approve_operation_action_api(
            submitted.action["action_id"],
            {"actor": "fixture"},
            expected_workspace_id=workspace_id,
        )
        self.assertEqual(approved["status"], "queued", approved)
        return approved["action"], approved["operation_run"]

    def _assert_denied_without_write(
        self,
        call: Callable[[], dict[str, Any]],
        baseline: dict[str, tuple[str, ...]],
    ) -> None:
        result = call()
        self.assertEqual(result.get("status"), "not_found", result)
        self.assertEqual(self._table_state(), baseline)

    def test_authenticated_lists_hide_foreign_and_malformed_links_while_open_mode_is_unchanged(self) -> None:
        owned = self._submit_filter("owned-list", workspace_id="user-alice")
        foreign = self._submit_filter("foreign-list", workspace_id="user-bob")
        repository = self.store.repos.workflow_runtime
        malformed = repository.upsert_operation(
            operation_run_id="operation-malformed-link",
            workspace_id="user-alice",
            action_id=foreign.action["action_id"],
            owner_module="projection_search_service",
            operation_type="projection.filter",
            idempotency_key="operation:malformed-link",
            status="queued",
        )

        actions = self.orchestrator.list_operation_actions_api(
            {"workspace_id": "user-bob"},
            expected_workspace_id="user-alice",
        )["actions"]
        runs = self.orchestrator.list_operation_runs_api(
            {"workspace_id": "user-bob", "include_status_summary": True},
            expected_workspace_id="user-alice",
        )["operation_runs"]
        self.assertEqual([item["action_id"] for item in actions], [owned.action["action_id"]])
        self.assertEqual([item["operation_run_id"] for item in runs], [owned.operation_run["operation_run_id"]])

        self.assertEqual(
            self.orchestrator.get_operation_run_api(
                malformed["operation_run_id"],
                expected_workspace_id="user-alice",
            )["status"],
            "not_found",
        )
        open_runs = self.orchestrator.list_operation_runs_api({"workspace_id": "user-alice"})["operation_runs"]
        self.assertEqual(
            {item["operation_run_id"] for item in open_runs},
            {owned.operation_run["operation_run_id"], malformed["operation_run_id"]},
        )
        self.assertEqual(
            self.orchestrator.get_operation_run_api(malformed["operation_run_id"])["status"],
            "ok",
        )

    def test_foreign_missing_and_malformed_reads_and_controls_are_full_zero_write(self) -> None:
        foreign_action = self._submit_export("foreign-control", workspace_id="user-bob").action
        foreign_run = self._submit_filter("foreign-control", workspace_id="user-bob").operation_run
        foreign_link_action = self._submit_filter("foreign-link", workspace_id="user-bob").action
        malformed_run = self.store.repos.workflow_runtime.upsert_operation(
            operation_run_id="operation-owned-root-foreign-action",
            workspace_id="user-alice",
            action_id=foreign_link_action["action_id"],
            owner_module="projection_search_service",
            operation_type="projection.filter",
            idempotency_key="operation:owned-root-foreign-action",
            status="queued",
        )
        baseline = self._table_state()
        expected = "user-alice"

        action_calls: tuple[Callable[[str], dict[str, Any]], ...] = (
            lambda action_id: self.orchestrator.get_operation_action_api(
                action_id,
                expected_workspace_id=expected,
            ),
            lambda action_id: self.orchestrator.approve_operation_action_api(
                action_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
            lambda action_id: self.orchestrator.reject_operation_action_api(
                action_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
        )
        for call in action_calls:
            for action_id in (foreign_action["action_id"], "action-missing"):
                with self.subTest(resource="action", call=call, resource_id=action_id):
                    self._assert_denied_without_write(lambda call=call, action_id=action_id: call(action_id), baseline)

        run_calls: tuple[Callable[[str], dict[str, Any]], ...] = (
            lambda run_id: self.orchestrator.get_operation_run_api(
                run_id,
                expected_workspace_id=expected,
            ),
            lambda run_id: self.orchestrator.get_operation_run_provenance_api(
                run_id,
                expected_workspace_id=expected,
            ),
            lambda run_id: self.orchestrator.cancel_operation_run_api(
                run_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
            lambda run_id: self.orchestrator.retry_operation_run_api(
                run_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
            lambda run_id: self.orchestrator.resume_operation_run_api(
                run_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
            lambda run_id: self.orchestrator.dispatch_operation_run_api(
                run_id,
                {"actor": "alice"},
                expected_workspace_id=expected,
            ),
        )
        for call in run_calls:
            for run_id in (
                foreign_run["operation_run_id"],
                malformed_run["operation_run_id"],
                "operation-missing",
            ):
                with self.subTest(resource="run", call=call, resource_id=run_id):
                    self._assert_denied_without_write(lambda call=call, run_id=run_id: call(run_id), baseline)

    def test_status_and_provenance_filter_shared_workflow_commands_before_limit(self) -> None:
        owned = self._submit_filter("owned-shared-workflow", workspace_id="user-alice")
        foreign = self._submit_filter("foreign-shared-workflow", workspace_id="user-bob")
        workflow_run_id = "workflow-shared-by-caller"
        foreign_command = self.store.upsert_workflow_command(
            workflow_run_id=workflow_run_id,
            operation_id=foreign.operation_run["operation_run_id"],
            command_type="acquisition.run.start",
            owner="acquisition_runner",
            idempotency_key="shared-workflow:foreign",
            payload={"private_marker": "bob-private"},
        )
        owned_command = self.store.upsert_workflow_command(
            workflow_run_id=workflow_run_id,
            operation_id=owned.operation_run["operation_run_id"],
            command_type="acquisition.run.start",
            owner="acquisition_runner",
            idempotency_key="shared-workflow:owned",
            payload={"private_marker": "alice-private"},
        )
        repository = self.store.repos.workflow_runtime
        for operation in (owned.operation_run, foreign.operation_run):
            repository.update_operation_state(
                operation["operation_run_id"],
                workflow_ref_patch={"workflow_run_id": workflow_run_id},
            )

        open_first = self.store.list_workflow_commands(workflow_run_id=workflow_run_id, limit=1)
        owned_first = self.store.list_workflow_commands(
            workflow_run_id=workflow_run_id,
            linked_operation_workspace_id="user-alice",
            limit=1,
        )
        self.assertEqual([item["command_id"] for item in open_first], [foreign_command["command_id"]])
        self.assertEqual([item["command_id"] for item in owned_first], [owned_command["command_id"]])

        detail = self.orchestrator.get_operation_run_api(
            owned.operation_run["operation_run_id"],
            expected_workspace_id="user-alice",
        )
        provenance = self.orchestrator.get_operation_run_provenance_api(
            owned.operation_run["operation_run_id"],
            expected_workspace_id="user-alice",
        )
        summary = detail["operation_run"]["status_summary"]
        self.assertEqual(summary["workflow_command_count"], 1)
        self.assertEqual(summary["latest_workflow_command"]["command_id"], owned_command["command_id"])
        self.assertEqual(
            [item["command_id"] for item in provenance["workflow_commands"]],
            [owned_command["command_id"]],
        )

        open_summary = self.orchestrator.get_operation_run_api(owned.operation_run["operation_run_id"])[
            "operation_run"
        ]["status_summary"]
        self.assertEqual(open_summary["workflow_command_count"], 2)
        self.assertEqual(
            {
                item["command_id"]
                for item in self.orchestrator.get_operation_run_provenance_api(owned.operation_run["operation_run_id"])[
                    "workflow_commands"
                ]
            },
            {owned_command["command_id"], foreign_command["command_id"]},
        )

    def test_event_reads_filter_foreign_workspace_rows_before_limit(self) -> None:
        repository = self.store.repos.workflow_runtime
        action = repository.upsert_action(
            action_id="action-owned-malformed-events",
            workspace_id="user-alice",
            action_type=ACTION_FILTER_PROJECTION,
            owner_module="projection_search_service",
            operation_type="projection.filter",
            idempotency_key="action:owned-malformed-events",
            status="queued",
        )
        operation = repository.upsert_operation(
            operation_run_id="operation-owned-malformed-events",
            workspace_id="user-alice",
            action_id=action["action_id"],
            owner_module="projection_search_service",
            operation_type="projection.filter",
            idempotency_key="operation:owned-malformed-events",
            status="queued",
        )
        foreign_action_event = repository.append_operation_event(
            workspace_id="user-bob",
            event_stream_id=action["action_id"],
            operation_run_id=operation["operation_run_id"],
            action_id=action["action_id"],
            event_family="operation_event",
            event_type="ForeignActionEvidence",
            idempotency_key="event:foreign-action-evidence",
        )
        owned_action_event = repository.append_operation_event(
            workspace_id="user-alice",
            event_stream_id=action["action_id"],
            operation_run_id=operation["operation_run_id"],
            action_id=action["action_id"],
            event_family="operation_event",
            event_type="OwnedActionEvidence",
            idempotency_key="event:owned-action-evidence",
        )
        foreign_run_event = repository.append_operation_event(
            workspace_id="user-bob",
            event_stream_id=operation["operation_run_id"],
            operation_run_id=operation["operation_run_id"],
            action_id=action["action_id"],
            event_family="operation_event",
            event_type="ForeignRunEvidence",
            idempotency_key="event:foreign-run-evidence",
        )
        owned_run_event = repository.append_operation_event(
            workspace_id="user-alice",
            event_stream_id=operation["operation_run_id"],
            operation_run_id=operation["operation_run_id"],
            action_id=action["action_id"],
            event_family="operation_event",
            event_type="OwnedRunEvidence",
            idempotency_key="event:owned-run-evidence",
        )

        self.assertEqual(
            repository.list_operation_events(action["action_id"], limit=1)[0]["event_id"],
            foreign_action_event["event_id"],
        )
        self.assertEqual(
            repository.list_operation_events(
                action["action_id"],
                expected_workspace_id="user-alice",
                limit=1,
            )[0]["event_id"],
            owned_action_event["event_id"],
        )
        self.assertEqual(
            repository.list_operation_events(operation["operation_run_id"], limit=1)[0]["event_id"],
            foreign_run_event["event_id"],
        )
        self.assertEqual(
            repository.list_operation_events(
                operation["operation_run_id"],
                expected_workspace_id="user-alice",
                limit=1,
            )[0]["event_id"],
            owned_run_event["event_id"],
        )

        action_detail = self.orchestrator.get_operation_action_api(
            action["action_id"],
            expected_workspace_id="user-alice",
        )
        run_detail = self.orchestrator.get_operation_run_api(
            operation["operation_run_id"],
            expected_workspace_id="user-alice",
        )
        provenance = self.orchestrator.get_operation_run_provenance_api(
            operation["operation_run_id"],
            expected_workspace_id="user-alice",
        )
        foreign_event_ids = {foreign_action_event["event_id"], foreign_run_event["event_id"]}
        for events in (
            action_detail["events"],
            run_detail["events"],
            provenance["action_events"],
            provenance["operation_events"],
            provenance["event_timeline"],
        ):
            self.assertTrue(foreign_event_ids.isdisjoint({event["event_id"] for event in events}))
        self.assertEqual(
            run_detail["operation_run"]["status_summary"]["latest_event"]["event_id"],
            owned_run_event["event_id"],
        )
        self.assertEqual(
            [
                event["event_id"]
                for event in self.orchestrator._operation_event_api_records(  # noqa: SLF001
                    [foreign_run_event, owned_run_event],
                    expected_workspace_id="user-alice",
                )
            ],
            [owned_run_event["event_id"]],
        )
        control_response = self.orchestrator._operation_run_control_response_record(  # noqa: SLF001
            {
                "status": "ok",
                "operation_run": operation,
                "events": [foreign_run_event, owned_run_event],
            },
            expected_workspace_id="user-alice",
        )
        self.assertEqual(
            [event["event_id"] for event in control_response["events"]],
            [owned_run_event["event_id"]],
        )
        self.assertIn(
            foreign_run_event["event_id"],
            {
                event["event_id"]
                for event in self.orchestrator.get_operation_run_api(operation["operation_run_id"])["events"]
            },
        )

    def test_authenticated_planned_command_references_fail_closed_without_writes(self) -> None:
        expected = "user-alice"
        foreign_operation = self._submit_filter("planned-ref-foreign", workspace_id="user-bob").operation_run
        same_workspace_other = self._submit_filter("planned-ref-other", workspace_id=expected).operation_run
        foreign_command = self.store.upsert_workflow_command(
            workflow_run_id="workflow-planned-ref",
            operation_id=foreign_operation["operation_run_id"],
            command_type="export.projection.generate",
            owner="projection_exporter",
            idempotency_key="planned-ref:foreign",
        )
        other_command = self.store.upsert_workflow_command(
            workflow_run_id="workflow-planned-ref",
            operation_id=same_workspace_other["operation_run_id"],
            command_type="export.projection.generate",
            owner="projection_exporter",
            idempotency_key="planned-ref:other",
        )
        blank_operation_command = self.store.upsert_workflow_command(
            workflow_run_id="workflow-planned-ref",
            operation_id="",
            command_type="export.projection.generate",
            owner="projection_exporter",
            idempotency_key="planned-ref:blank-operation",
        )
        repository = self.store.repos.workflow_runtime

        invalid_refs: tuple[tuple[str, str], ...] = (
            ("foreign", foreign_command["command_id"]),
            ("same-workspace-other", other_command["command_id"]),
            ("blank-operation", blank_operation_command["command_id"]),
            ("missing", "command-does-not-exist"),
            ("blank", ""),
        )
        prepared: list[tuple[str, str]] = []
        for suffix, command_id in invalid_refs:
            _action, operation = self._approved_export(f"planned-ref-{suffix}", workspace_id=expected)
            repository.update_operation_state(
                operation["operation_run_id"],
                status="planned",
                workflow_ref_patch={"command_id": command_id},
            )
            prepared.append((suffix, operation["operation_run_id"]))

        positive_action, positive_operation = self._approved_export("planned-ref-current", workspace_id=expected)
        positive_command = self.store.upsert_workflow_command(
            workflow_run_id="workflow-planned-ref",
            operation_id=positive_operation["operation_run_id"],
            command_type="export.projection.generate",
            owner="projection_exporter",
            idempotency_key="planned-ref:current",
        )
        repository.update_operation_state(
            positive_operation["operation_run_id"],
            status="planned",
            workflow_ref_patch={"command_id": positive_command["command_id"]},
        )
        baseline = self._table_state()

        for suffix, operation_run_id in prepared:
            with self.subTest(reference=suffix):
                self._assert_denied_without_write(
                    lambda operation_run_id=operation_run_id: self.orchestrator.dispatch_operation_run_api(
                        operation_run_id,
                        {"actor": "alice"},
                        expected_workspace_id=expected,
                    ),
                    baseline,
                )

        positive = self.orchestrator.dispatch_operation_run_api(
            positive_operation["operation_run_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(positive["status"], "planned", positive)
        self.assertEqual(positive["workflow_command"]["command_id"], positive_command["command_id"])
        self.assertEqual(positive["action"]["action_id"], positive_action["action_id"])
        self.assertEqual(self._table_state(), baseline)

        open_foreign = self.orchestrator.dispatch_operation_run_api(prepared[0][1], {"actor": "operator"})
        self.assertEqual(open_foreign["status"], "planned", open_foreign)
        self.assertEqual(open_foreign["workflow_command"]["command_id"], foreign_command["command_id"])
        self.assertEqual(self._table_state(), baseline)

    def test_planned_replay_runs_request_and_approval_guards_before_returning_command(self) -> None:
        workspace_id = "user-alice"

        export_action, export_operation = self._approved_export(
            "planned-unapproved-export",
            workspace_id=workspace_id,
        )
        self.store.repos.workflow_runtime.update_action_state(
            export_action["action_id"],
            status="approval_required",
            approval_status="required",
        )
        export_command = self._bind_planned_command(
            export_operation,
            suffix="planned-unapproved-export",
            command_type="export.projection.generate",
            owner="projection_exporter",
        )
        export_baseline = self._table_state()
        for expected_workspace_id in (workspace_id, ""):
            with self.subTest(kind="export-approval", expected_workspace_id=expected_workspace_id):
                result = self.orchestrator.dispatch_operation_run_api(
                    export_operation["operation_run_id"],
                    {"actor": "alice"},
                    expected_workspace_id=expected_workspace_id,
                )
                self.assertEqual(result.get("status"), "approval_required", result)
                self.assertNotIn("workflow_command", result)
                self.assertEqual(self._table_state(), export_baseline)
                self.assertIsNotNone(self.store.get_workflow_command(export_command["command_id"]))

        for mode, expected_workspace_id in (("authenticated", workspace_id), ("open", "")):
            record_id = f"record-sensitive-{mode}"
            self.store.upsert_crm_record(
                {
                    "crm_record_id": record_id,
                    "workspace_id": workspace_id,
                    "owner_user_id": "alice",
                    "person_identity_key": f"person::{record_id}",
                }
            )
            submitted = self.orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_SET_CRM_STAGE,
                    "workspace_id": workspace_id,
                    "target_ref": {"crm_record_id": record_id},
                    "input": {"stage": "do_not_contact"},
                    "idempotency_key": f"stage:{mode}",
                    "actor": "alice",
                },
                expected_workspace_id=workspace_id,
                expected_owner_user_id="alice",
            )
            command = self._bind_planned_command(
                submitted["operation_run"],
                suffix=f"planned-sensitive-{mode}",
                command_type="crm.record.update",
                owner="crm_writer",
            )
            protected_before = self._table_state()
            result = self.orchestrator.dispatch_operation_run_api(
                submitted["operation_run"]["operation_run_id"],
                {"actor": "alice"},
                expected_workspace_id=expected_workspace_id,
            )
            self.assertEqual(result.get("status"), "approval_required", result)
            self.assertNotIn("workflow_command", result)
            self.assertEqual(self._table_state(), protected_before)
            self.assertEqual(
                self.store.get_workflow_command(command["command_id"])["status"],
                command["status"],
            )

        invalid_record_id = "record-schema-invalid"
        self.store.upsert_crm_record(
            {
                "crm_record_id": invalid_record_id,
                "workspace_id": workspace_id,
                "owner_user_id": "alice",
                "person_identity_key": f"person::{invalid_record_id}",
            }
        )
        invalid = self.orchestrator.submit_operation_action(
            {
                "action_type": ACTION_ADD_CRM_NOTE,
                "workspace_id": workspace_id,
                "target_ref": {"crm_record_id": invalid_record_id},
                "input": {"note": "valid before persisted tamper"},
                "idempotency_key": "note:schema-invalid-planned",
                "actor": "alice",
            },
            expected_workspace_id=workspace_id,
            expected_owner_user_id="alice",
        )
        self._bind_planned_command(
            invalid["operation_run"],
            suffix="planned-schema-invalid",
            command_type="crm.note.add",
            owner="crm_writer",
        )
        self._execute_pg(
            "UPDATE {schema}.agent_actions SET input_json = %s::jsonb WHERE action_id = %s",
            (json.dumps({"note": ""}), invalid["action"]["action_id"]),
        )
        invalid_baseline = self._table_state()
        for expected_workspace_id in (workspace_id, ""):
            with self.subTest(kind="schema-invalid", expected_workspace_id=expected_workspace_id):
                result = self.orchestrator.dispatch_operation_run_api(
                    invalid["operation_run"]["operation_run_id"],
                    {"actor": "alice"},
                    expected_workspace_id=expected_workspace_id,
                )
                self.assertEqual(result.get("status"), "conflict", result)
                self.assertEqual(result.get("reason"), "operation_action_request_schema_validation_conflict")
                self.assertTrue(result.get("request_schema_revalidation_required"), result)
                self.assertEqual(self._table_state(), invalid_baseline)

        pin_drift_action, pin_drift_operation = self._approved_export(
            "planned-pin-drift",
            workspace_id=workspace_id,
        )
        self._bind_planned_command(
            pin_drift_operation,
            suffix="planned-pin-drift",
            command_type="export.projection.generate",
            owner="projection_exporter",
        )
        forged_digest = "f" * 64
        self._execute_pg(
            "UPDATE {schema}.agent_actions SET request_schema_version = %s, request_schema_digest = %s "
            "WHERE action_id = %s",
            ("forged_v1", forged_digest, pin_drift_action["action_id"]),
        )
        self._execute_pg(
            "UPDATE {schema}.operation_runs SET request_schema_version = %s, request_schema_digest = %s "
            "WHERE operation_run_id = %s",
            ("forged_v1", forged_digest, pin_drift_operation["operation_run_id"]),
        )
        pin_baseline = self._table_state()
        for expected_workspace_id in (workspace_id, ""):
            with self.subTest(kind="pin-drift", expected_workspace_id=expected_workspace_id):
                result = self.orchestrator.dispatch_operation_run_api(
                    pin_drift_operation["operation_run_id"],
                    {"actor": "alice"},
                    expected_workspace_id=expected_workspace_id,
                )
                self.assertEqual(result.get("status"), "conflict", result)
                self.assertEqual(result.get("reason"), "operation_action_request_schema_pin_conflict")
                self.assertTrue(result.get("request_schema_revalidation_required"), result)
                self.assertEqual(self._table_state(), pin_baseline)

    def test_same_owner_controls_and_explicit_open_mode_reach_existing_semantics(self) -> None:
        expected = "user-alice"
        approval = self._submit_export("owned-approve", workspace_id=expected)
        approved = self.orchestrator.approve_operation_action_api(
            approval.action["action_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(approved["status"], "queued", approved)

        rejection = self._submit_export("owned-reject", workspace_id=expected)
        rejected = self.orchestrator.reject_operation_action_api(
            rejection.action["action_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(rejected["status"], "rejected", rejected)

        resumable = self._submit_filter("owned-resume", workspace_id=expected)
        resumed = self.orchestrator.resume_operation_run_api(
            resumable.operation_run["operation_run_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(resumed["status"], "queued", resumed)

        cancellable = self._submit_filter("owned-cancel", workspace_id=expected)
        cancelled = self.orchestrator.cancel_operation_run_api(
            cancellable.operation_run["operation_run_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(cancelled["status"], "cancelled", cancelled)

        retryable = self._submit_filter("owned-retry", workspace_id=expected)
        repository = self.store.repos.workflow_runtime
        repository.update_operation_state(retryable.operation_run["operation_run_id"], status="failed")
        repository.update_action_state(retryable.action["action_id"], status="failed")
        retried = self.orchestrator.retry_operation_run_api(
            retryable.operation_run["operation_run_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(retried["status"], "queued", retried)

        self.store.upsert_crm_record(
            {
                "crm_record_id": "record-owned-dispatch",
                "workspace_id": expected,
                "owner_user_id": "alice",
                "person_identity_key": "person::owned-dispatch",
            }
        )
        submitted = self.orchestrator.submit_operation_action(
            {
                "action_type": ACTION_ADD_CRM_NOTE,
                "workspace_id": expected,
                "target_ref": {"crm_record_id": "record-owned-dispatch"},
                "input": {"note": "Owner-scoped dispatch"},
                "idempotency_key": "note:owned-dispatch",
                "actor": "alice",
            },
            expected_workspace_id=expected,
            expected_owner_user_id="alice",
        )
        crm_before = self._table_state(("crm_records", "crm_engagements", "crm_tasks", "crm_events"))
        dispatched = self.orchestrator.dispatch_operation_run_api(
            submitted["operation_run"]["operation_run_id"],
            {"actor": "alice"},
            expected_workspace_id=expected,
        )
        self.assertEqual(dispatched["status"], "planned", dispatched)
        self.assertEqual(
            self._table_state(("crm_records", "crm_engagements", "crm_tasks", "crm_events")),
            crm_before,
        )
        self.assertEqual(
            self.orchestrator.get_operation_action_api(
                approval.action["action_id"],
                expected_workspace_id=expected,
            )["status"],
            "ok",
        )
        self.assertEqual(
            self.orchestrator.get_operation_run_provenance_api(
                approved["operation_run"]["operation_run_id"],
                expected_workspace_id=expected,
            )["status"],
            "ok",
        )

        operator_run = self._submit_filter("operator-open", workspace_id="operator-workspace")
        self.assertEqual(
            self.orchestrator.get_operation_run_api(operator_run.operation_run["operation_run_id"])["status"],
            "ok",
        )
        open_resumed = self.orchestrator.resume_operation_run_api(
            operator_run.operation_run["operation_run_id"],
            {"actor": "legacy-operator"},
        )
        self.assertEqual(open_resumed["status"], "queued", open_resumed)


if __name__ == "__main__":
    unittest.main()
