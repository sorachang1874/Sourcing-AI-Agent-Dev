from __future__ import annotations

import hashlib
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from sourcing_agent.acquisition_plan_preview import (
    ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT,
    ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION,
    ACQUISITION_PLAN_PREVIEW_RESULT_SPEC,
    ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE,
)
from sourcing_agent.agent_canary_registry import PLAN_ACQUISITION_TOOL_SPEC
from sourcing_agent.control_plane_live_postgres import ControlPlaneAdvisoryLockBusy
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin, psycopg

_REGISTRY_DIGEST = hashlib.sha256(b"company-registry-v1").hexdigest()
_START_REQUEST_DIGEST = hashlib.sha256(b"acquisition-root-request-v2").hexdigest()
_ZERO_SIDE_EFFECT_TABLES = (
    "workflow_commands",
    "acquisition_runs",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "plan_review_sessions",
    "runtime_outbox",
)


def _input_payload(*, thematic_constraints: list[str] | None = None) -> dict[str, Any]:
    return {
        "cohort_selection": {
            "schema_version": "cohort_selection.v1",
            "role_bucket_ids": ["research", "engineering"],
            "employment_statuses": ["current", "former"],
            "role_match": "any",
            "source": "user_explicit",
        },
        "source_preferences": [ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE],
        "coverage_intent": ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT,
        "thematic_constraints": thematic_constraints or ["Pre-training"],
        "provider_mode_intent": "simulate",
        "budget": {
            "max_provider_calls": 4,
            "max_provider_items": 20,
            "max_output_candidates": 10,
            "max_cost_micro_usd": 2_000_000,
            "max_elapsed_seconds": 900,
        },
    }


def _target_ref() -> dict[str, Any]:
    return {
        "workspace_id": "workspace_1",
        "requester_id": "requester_1",
        "company_target": {
            "canonical_company_id": "thinkingmachineslab",
            "canonical_name": "Thinking Machines Lab",
            "company_registry_revision": "company_registry.v1",
            "company_registry_digest": _REGISTRY_DIGEST,
            "provider_company_labels": ["Thinking Machines Lab", "thinkingmachinesai"],
        },
    }


def _uow_kwargs(*, suffix: str = "1", thematic_constraints: list[str] | None = None) -> dict[str, Any]:
    input_payload = _input_payload(thematic_constraints=thematic_constraints)
    return {
        "action_id": f"act_preview_{suffix}",
        "operation_run_id": f"oprun_preview_{suffix}",
        "preview_id": f"preview_{suffix}",
        "workspace_id": "workspace_1",
        "requester_id": "requester_1",
        "conversation_id": "conversation_1",
        "input_payload": input_payload,
        "target_ref": _target_ref(),
        "budget": dict(input_payload["budget"]),
        "idempotency_key": f"plan_acquisition:{suffix}",
        "request_schema_version": ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_VERSION,
        "request_schema_digest": ACQUISITION_PLAN_PREVIEW_REQUEST_SCHEMA_DIGEST,
        "tool_name": PLAN_ACQUISITION_TOOL_SPEC.tool_name,
        "tool_spec_version": PLAN_ACQUISITION_TOOL_SPEC.tool_spec_version,
        "tool_spec_digest": PLAN_ACQUISITION_TOOL_SPEC.tool_spec_digest,
        "result_schema_version": ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.result_schema_version,
        "result_schema_digest": ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.result_schema_digest,
        "result_serializer_owner": ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.serializer_owner,
        "result_serializer_revision": ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.serializer_revision,
        "result_serializer_contract_digest": ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.serializer_contract_digest,
        "start_request_schema_version": "acquisition_root_request_v2",
        "start_request_schema_digest": _START_REQUEST_DIGEST,
        "actor": "test-user",
        "source": "test.d1n_f4a",
        "ttl_seconds": 3600,
    }


class D1nAcquisitionPlanPreviewUowPGTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    pg_store_schema_label = "d1n_preview_uow"

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.store = self.make_pg_store(Path(self.tempdir.name) / "preview-uow.db")
        self.repository = self.store.repos.workflow_runtime
        self.adapter = self.store._control_plane_postgres  # noqa: SLF001

    def _table_counts(self, table_names: tuple[str, ...]) -> dict[str, int]:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        counts: dict[str, int] = {}
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                for table_name in table_names:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {quoted_schema}.{quote_control_plane_postgres_identifier(table_name)}"
                    )
                    counts[table_name] = int(cursor.fetchone()[0])
        return counts

    def test_success_exact_replay_scoped_read_and_zero_side_effects(self) -> None:
        kwargs = _uow_kwargs()
        first = self.repository.create_acquisition_plan_preview_uow(**kwargs)
        second = self.repository.create_acquisition_plan_preview_uow(**kwargs)

        self.assertEqual(first["outcome"], "created")
        self.assertFalse(first["replayed"])
        self.assertEqual(second["outcome"], "replayed")
        self.assertTrue(second["replayed"])
        self.assertEqual(first["preview"]["preview_revision"], second["preview"]["preview_revision"])
        self.assertEqual(first["preview"]["preview_digest"], second["preview"]["preview_digest"])
        self.assertEqual(first["action"]["status"], "completed")
        self.assertEqual(first["operation_run"]["status"], "completed")
        self.assertEqual(first["event"]["event_type"], "AcquisitionPlanPreviewCreated")
        self.assertEqual(first["action"]["result_schema_digest"], kwargs["result_schema_digest"])
        self.assertEqual(
            first["operation_run"]["result_serializer_contract_digest"],
            kwargs["result_serializer_contract_digest"],
        )

        preview = first["preview"]
        exact = self.repository.get_acquisition_plan_preview(
            preview["preview_id"],
            workspace_id=kwargs["workspace_id"],
            requester_id=kwargs["requester_id"],
            preview_revision=preview["preview_revision"],
            preview_digest=preview["preview_digest"],
        )
        self.assertEqual(exact["preview_id"], preview["preview_id"])
        self.assertIsNotNone(exact["created_at"].utcoffset())
        for owner_patch in (
            {"workspace_id": "foreign"},
            {"requester_id": "foreign"},
            {"preview_revision": preview["preview_revision"] + 1},
            {"preview_digest": "f" * 64},
        ):
            read_args = {
                "workspace_id": kwargs["workspace_id"],
                "requester_id": kwargs["requester_id"],
                "preview_revision": preview["preview_revision"],
                "preview_digest": preview["preview_digest"],
                **owner_patch,
            }
            self.assertEqual(
                self.repository.get_acquisition_plan_preview(preview["preview_id"], **read_args),
                {},
            )

        counts = self._table_counts(
            ("agent_actions", "operation_runs", "acquisition_plan_previews", "operation_events")
            + _ZERO_SIDE_EFFECT_TABLES
        )
        self.assertEqual(counts["agent_actions"], 1)
        self.assertEqual(counts["operation_runs"], 1)
        self.assertEqual(counts["acquisition_plan_previews"], 1)
        self.assertEqual(counts["operation_events"], 1)
        self.assertTrue(all(counts[table_name] == 0 for table_name in _ZERO_SIDE_EFFECT_TABLES), counts)

    def test_precommit_faults_roll_back_the_complete_bundle(self) -> None:
        aggregate_tables = ("operation_runs", "agent_actions", "acquisition_plan_previews", "operation_events")
        for fault_point in (
            "after_operation_write",
            "after_action_write",
            "after_preview_write",
            "after_event_write",
        ):
            with self.subTest(fault_point=fault_point):
                with self.assertRaisesRegex(RuntimeError, "injected acquisition plan preview fault"):
                    self.adapter.create_acquisition_plan_preview_uow(
                        table_name="acquisition_plan_previews",
                        **_uow_kwargs(),
                        fault_injection_point=fault_point,
                    )
                self.assertEqual(self._table_counts(aggregate_tables), dict.fromkeys(aggregate_tables, 0))

    def test_postcommit_lost_ack_exact_reloads_without_duplicate(self) -> None:
        kwargs = _uow_kwargs()
        with self.assertRaisesRegex(RuntimeError, "injected acquisition plan preview fault after commit"):
            self.adapter.create_acquisition_plan_preview_uow(
                table_name="acquisition_plan_previews",
                **kwargs,
                fault_injection_point="after_commit",
            )

        replay = self.repository.create_acquisition_plan_preview_uow(**kwargs)
        self.assertEqual(replay["outcome"], "replayed")
        counts = self._table_counts(
            ("operation_runs", "agent_actions", "acquisition_plan_previews", "operation_events")
        )
        self.assertEqual(set(counts.values()), {1})

    def test_same_idempotency_changed_request_or_identity_conflicts_without_write(self) -> None:
        kwargs = _uow_kwargs()
        first = self.repository.create_acquisition_plan_preview_uow(**kwargs)
        baseline_counts = self._table_counts(
            ("operation_runs", "agent_actions", "acquisition_plan_previews", "operation_events")
        )

        changed = _uow_kwargs(thematic_constraints=["Foundation models"])
        with self.assertRaisesRegex(RuntimeError, "immutable identity collision"):
            self.repository.create_acquisition_plan_preview_uow(**changed)
        split = {**kwargs, "preview_id": "preview_different"}
        with self.assertRaisesRegex(RuntimeError, "partial aggregate collision|immutable identity collision"):
            self.repository.create_acquisition_plan_preview_uow(**split)
        self.assertEqual(
            self._table_counts(("operation_runs", "agent_actions", "acquisition_plan_previews", "operation_events")),
            baseline_counts,
        )
        self.assertGreater(first["preview"]["preview_revision"], 0)

    def test_concurrent_identical_submission_has_one_bundle(self) -> None:
        kwargs = _uow_kwargs()
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(
                    lambda _: self.repository.create_acquisition_plan_preview_uow(**kwargs),
                    range(8),
                )
            )

        self.assertEqual(sum(result["outcome"] == "created" for result in results), 1)
        self.assertEqual(sum(result["outcome"] == "replayed" for result in results), 7)
        self.assertEqual(len({result["preview"]["preview_revision"] for result in results}), 1)
        counts = self._table_counts(
            ("operation_runs", "agent_actions", "acquisition_plan_previews", "operation_events")
        )
        self.assertEqual(set(counts.values()), {1})

    def test_distinct_previews_allocate_strictly_increasing_gap_tolerant_revisions(self) -> None:
        first = self.repository.create_acquisition_plan_preview_uow(**_uow_kwargs(suffix="1"))
        second = self.repository.create_acquisition_plan_preview_uow(**_uow_kwargs(suffix="2"))
        self.assertGreater(second["preview"]["preview_revision"], first["preview"]["preview_revision"])

    def test_event_stream_lock_contention_uses_one_finite_deadline(self) -> None:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        kwargs = _uow_kwargs()
        lock_key = self.adapter._advisory_lock_key(  # noqa: SLF001
            f"operation_events:{kwargs['operation_run_id']}"
        )
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as blocker:
            with blocker.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_lock(hashtext(%s))", (lock_key,))
            blocker.commit()
            started = time.monotonic()
            try:
                with pytest.raises(ControlPlaneAdvisoryLockBusy):
                    self.adapter.create_acquisition_plan_preview_uow(
                        table_name="acquisition_plan_previews",
                        **kwargs,
                        lock_timeout_seconds=0.15,
                    )
            finally:
                with blocker.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_key,))
                blocker.commit()
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual(
            self._table_counts(("operation_runs", "agent_actions", "acquisition_plan_previews", "operation_events")),
            {
                "operation_runs": 0,
                "agent_actions": 0,
                "acquisition_plan_previews": 0,
                "operation_events": 0,
            },
        )

    def test_migration_owns_revision_sequence_and_preview_rows_are_immutable(self) -> None:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        preview = self.repository.create_acquisition_plan_preview_uow(**_uow_kwargs())["preview"]
        quoted_schema = quote_control_plane_postgres_identifier(fixture.schema)
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_get_serial_sequence(%s, 'preview_revision')",
                    (f"{fixture.schema}.acquisition_plan_previews",),
                )
                sequence_name = str(cursor.fetchone()[0] or "")
                self.assertTrue(sequence_name.endswith(".acquisition_plan_preview_revision_seq"), sequence_name)
                with self.assertRaises(Exception):
                    cursor.execute(
                        f"UPDATE {quoted_schema}.acquisition_plan_previews SET requester_id = 'changed' "
                        "WHERE preview_id = %s",
                        (preview["preview_id"],),
                    )
            connection.rollback()
