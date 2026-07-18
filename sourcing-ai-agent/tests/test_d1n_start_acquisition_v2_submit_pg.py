from __future__ import annotations

import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest

from sourcing_agent import acquisition_start_v2_postgres as start_pg
from sourcing_agent.acquisition_start_v2 import (
    ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
    ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    AcquisitionStartV2BindContext,
    AcquisitionStartV2OwnerBinder,
    AcquisitionStartV2ToolPins,
)
from sourcing_agent.agent_canary_registry import START_ACQUISITION_RUN_TOOL_SPEC
from sourcing_agent.agent_tool_result_slot import AgentToolOccurrence
from sourcing_agent.control_plane_live_postgres import ControlPlaneAdvisoryLockBusy
from sourcing_agent.local_postgres import quote_control_plane_postgres_identifier
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin, psycopg
from tests.test_d1n_acquisition_plan_preview_uow import _uow_kwargs

_BASELINE_TABLES = (
    "agent_actions",
    "operation_runs",
    "acquisition_plan_previews",
    "operation_events",
    "agent_tool_result_slots",
)
_ZERO_EFFECT_TABLES = (
    "workflow_events",
    "workflow_current_state",
    "workflow_commands",
    "runtime_outbox",
    "agent_tool_result_attempts",
    "agent_tool_result_journal",
    "acquisition_runs",
    "workflow_activity_runs",
    "workflow_activity_attempts",
    "workflow_entity_deltas",
    "acquisition_discovery_lanes",
    "plan_review_sessions",
    "crm_tasks",
    "company_assets",
    "company_evidence",
    "company_assertions",
)
_ALL_ASSERTED_TABLES = _BASELINE_TABLES + _ZERO_EFFECT_TABLES
_PRESERVED_SUBMIT_TABLES = tuple(
    table_name
    for table_name in _ALL_ASSERTED_TABLES
    if table_name not in {"agent_actions", "operation_events"}
)


class _RepositoryPreviewReader:
    def __init__(self, repository: Any) -> None:
        self._repository = repository

    def get_acquisition_plan_preview(self, preview_id: str, **owner: Any) -> dict[str, Any] | None:
        row = self._repository.get_acquisition_plan_preview(preview_id, **owner)
        preview = row.get("preview") if isinstance(row, dict) else None
        return dict(preview) if isinstance(preview, dict) else None


class D1nStartAcquisitionV2SubmitPGMatrixTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    pg_store_schema_label = "d1n_start_submit_matrix"

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.store = self.make_pg_store(Path(self.tempdir.name) / "start-submit-matrix.db")
        self.repository = self.store.repos.workflow_runtime
        self.adapter = self.store._control_plane_postgres  # noqa: SLF001

    def _schema_connection(self) -> tuple[Any, str]:
        fixture = self._pg_store_fixture
        self.assertIsNotNone(fixture)
        self.assertIsNotNone(psycopg)
        assert fixture is not None
        assert psycopg is not None
        return fixture, quote_control_plane_postgres_identifier(fixture.schema)

    def _table_counts(self, table_names: tuple[str, ...] = _ALL_ASSERTED_TABLES) -> dict[str, int]:
        fixture, quoted_schema = self._schema_connection()
        assert psycopg is not None
        counts: dict[str, int] = {}
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                for table_name in table_names:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {quoted_schema}."
                        f"{quote_control_plane_postgres_identifier(table_name)}"
                    )
                    counts[table_name] = int(cursor.fetchone()[0])
        return counts

    def _table_snapshot(self, table_names: tuple[str, ...] = _ALL_ASSERTED_TABLES) -> dict[str, tuple[str, ...]]:
        fixture, quoted_schema = self._schema_connection()
        assert psycopg is not None
        snapshot: dict[str, tuple[str, ...]] = {}
        with psycopg.connect(fixture.dsn, client_encoding="utf8") as connection:
            with connection.cursor() as cursor:
                for table_name in table_names:
                    quoted_table = quote_control_plane_postgres_identifier(table_name)
                    cursor.execute(
                        f"SELECT row_to_json(row_value)::text "
                        f"FROM {quoted_schema}.{quoted_table} AS row_value ORDER BY 1"
                    )
                    snapshot[table_name] = tuple(str(row[0]) for row in cursor.fetchall())
        return snapshot

    def _expected_counts(self, *, submitted: bool) -> dict[str, int]:
        counts = dict.fromkeys(_ALL_ASSERTED_TABLES, 0)
        counts.update(
            {
                "agent_actions": 2 if submitted else 1,
                "operation_runs": 1,
                "acquisition_plan_previews": 1,
                "operation_events": 2 if submitted else 1,
                "agent_tool_result_slots": 1,
            }
        )
        return counts

    def _arrange_occurrence(self, *, suffix: str) -> AgentToolOccurrence:
        kwargs = _uow_kwargs(suffix=f"submit_matrix_{suffix}")
        kwargs["start_request_schema_version"] = ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        kwargs["start_request_schema_digest"] = ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
        preview_bundle = self.repository.create_acquisition_plan_preview_uow(**kwargs)
        preview = dict(preview_bundle["preview"])
        reference = {
            "preview_id": preview["preview_id"],
            "preview_revision": preview["preview_revision"],
            "preview_digest": preview["preview_digest"],
        }
        tool_pins = AcquisitionStartV2ToolPins(
            tool_spec_version=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_version,
            tool_spec_digest=START_ACQUISITION_RUN_TOOL_SPEC.tool_spec_digest,
        )
        bound = AcquisitionStartV2OwnerBinder(_RepositoryPreviewReader(self.repository)).bind(
            input_payload=reference,
            context=AcquisitionStartV2BindContext(
                workspace_id=kwargs["workspace_id"],
                requester_id=kwargs["requester_id"],
            ),
            tool_pins=tool_pins,
            now=datetime.now(timezone.utc),
        )
        occurrence = AgentToolOccurrence.from_tool_spec(
            result_slot_id=f"slot_start_submit_matrix_{suffix}",
            slot_generation=1,
            workspace_id=kwargs["workspace_id"],
            actor_id=kwargs["requester_id"],
            runtime_namespace="isolated_local_canary",
            provider_mode="simulate",
            turn_id=f"turn_start_submit_matrix_{suffix}",
            step_id=f"step_start_submit_matrix_{suffix}",
            tool_spec=START_ACQUISITION_RUN_TOOL_SPEC,
            canonical_args=bound.to_record(),
            occurrence_ordinal=1,
        )
        self.repository.reserve_agent_tool_result_slot(occurrence=occurrence)
        self.assertEqual(self._table_counts(), self._expected_counts(submitted=False))
        return occurrence

    def test_precommit_faults_roll_back_full_submit_effect_surface(self) -> None:
        occurrence = self._arrange_occurrence(suffix="rollback")
        baseline_counts = self._table_counts()
        baseline_snapshot = self._table_snapshot()

        for fault_point in ("after_action_write", "after_event_write"):
            with self.subTest(fault_point=fault_point):
                with self.assertRaisesRegex(RuntimeError, "injected acquisition start submit fault"):
                    start_pg.submit_acquisition_start_v2_action_uow(
                        self.adapter,
                        occurrence=occurrence,
                        fault_injection_point=fault_point,
                    )
                self.assertEqual(self._table_counts(), baseline_counts)
                self.assertEqual(self._table_snapshot(), baseline_snapshot)

    def test_postcommit_lost_ack_exact_replay_preserves_committed_bundle(self) -> None:
        occurrence = self._arrange_occurrence(suffix="lost_ack")

        with self.assertRaisesRegex(RuntimeError, "injected acquisition start submit fault after commit"):
            start_pg.submit_acquisition_start_v2_action_uow(
                self.adapter,
                occurrence=occurrence,
                fault_injection_point="after_commit",
            )

        self.assertEqual(self._table_counts(), self._expected_counts(submitted=True))
        committed_snapshot = self._table_snapshot()
        replay = start_pg.submit_acquisition_start_v2_action_uow(
            self.adapter,
            occurrence=occurrence,
        )

        self.assertEqual(replay["outcome"], "replayed")
        self.assertTrue(replay["replayed"])
        self.assertEqual(self._table_counts(), self._expected_counts(submitted=True))
        self.assertEqual(self._table_snapshot(), committed_snapshot)
        binding = start_pg.revalidate_acquisition_start_v2_occurrence(occurrence)
        self.assertEqual(replay["action"]["action_id"], binding.action_id)
        self.assertEqual(replay["event"]["event_stream_id"], binding.action_id)
        self.assertEqual(replay["event"]["sequence_number"], 1)
        submitted_at = replay["action"]["created_at"]
        self.assertEqual(replay["action"]["updated_at"], submitted_at)
        self.assertEqual(replay["event"]["occurred_at"], submitted_at)
        self.assertEqual(replay["event"]["recorded_at"], submitted_at)
        self.assertEqual(replay["event"]["created_at"], submitted_at)

    def test_eight_concurrent_identical_calls_commit_one_submit_bundle(self) -> None:
        occurrence = self._arrange_occurrence(suffix="concurrent")
        preserved_baseline = self._table_snapshot(_PRESERVED_SUBMIT_TABLES)
        barrier = Barrier(8)

        def submit(_: int) -> dict[str, Any]:
            barrier.wait(timeout=5.0)
            return start_pg.submit_acquisition_start_v2_action_uow(
                self.adapter,
                occurrence=occurrence,
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(submit, range(8)))

        self.assertEqual(sum(result["outcome"] == "submitted" for result in results), 1)
        self.assertEqual(sum(result["outcome"] == "replayed" for result in results), 7)
        self.assertEqual(len({result["action"]["action_id"] for result in results}), 1)
        self.assertEqual(len({result["action"]["created_at"] for result in results}), 1)
        self.assertEqual(len({result["event"]["event_id"] for result in results}), 1)
        self.assertEqual(self._table_counts(), self._expected_counts(submitted=True))
        self.assertEqual(self._table_snapshot(_PRESERVED_SUBMIT_TABLES), preserved_baseline)

    def test_first_event_stream_lock_contention_uses_one_finite_deadline(self) -> None:
        occurrence = self._arrange_occurrence(suffix="lock_busy")
        baseline_counts = self._table_counts()
        baseline_snapshot = self._table_snapshot()
        binding = start_pg.revalidate_acquisition_start_v2_occurrence(occurrence)
        raw_lock_key = start_pg.acquisition_start_v2_submit_lock_groups(binding)[0][0]
        physical_lock_key = self.adapter._advisory_lock_key(raw_lock_key)  # noqa: SLF001
        fixture, _ = self._schema_connection()
        assert psycopg is not None

        with psycopg.connect(fixture.dsn, client_encoding="utf8") as blocker:
            with blocker.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_lock(hashtext(%s))", (physical_lock_key,))
            blocker.commit()
            started = time.monotonic()
            try:
                with pytest.raises(ControlPlaneAdvisoryLockBusy):
                    start_pg.submit_acquisition_start_v2_action_uow(
                        self.adapter,
                        occurrence=occurrence,
                        lock_timeout_seconds=0.15,
                    )
            finally:
                with blocker.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (physical_lock_key,))
                blocker.commit()

        self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual(self._table_counts(), baseline_counts)
        self.assertEqual(self._table_snapshot(), baseline_snapshot)
