"""Track B B2 — postgres_only dedup-read regression tests.

Pins two latent stale-read bugs the SQLite/PG dual-path created (invariant-7 family):
``find_latest_job_by_idempotency_key`` and ``find_pending_plan_review_session`` read the
in-memory SQLite shadow, which is EMPTY in postgres_only (writes route to PG), so they
returned ``None`` and silently defeated idempotency / pending-plan-review dedup — yielding
DUPLICATE jobs and DUPLICATE pending plan-review sessions on resubmit. The B2 fix adds a
PG-authoritative read (mirroring the sibling find_latest_job_by_request_signature). These
tests FAIL on the pre-fix SQLite-only code and PASS after, because the store fixture writes
to PG while the buggy reads queried the empty shadow.

Skips without a local PG DSN; SOURCING_REQUIRE_PG_STORE_TESTS=1 turns the skip into a failure.
"""

from __future__ import annotations

import tempfile
import unittest

from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class PgOnlyDedupReadTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.store = self.make_pg_store(f"{self.tempdir.name}/control_plane.db")

    def _save_job(self, *, job_id: str, status: str, idempotency_key: str, target_company: str = "Acme") -> None:
        self.store.save_job(
            job_id,
            "workflow",
            status,
            "done",
            {"target_company": target_company},
            idempotency_key=idempotency_key,
        )

    def test_find_latest_job_by_idempotency_key_reads_pg(self) -> None:
        self._save_job(job_id="job-b2-idem", status="completed", idempotency_key="idem-b2-key")
        found = self.store.find_latest_job_by_idempotency_key(idempotency_key="idem-b2-key")
        self.assertIsNotNone(
            found, "idempotency dedup must find the PG-persisted job (pre-fix read the empty SQLite shadow -> None)"
        )
        self.assertEqual(found["job_id"], "job-b2-idem")

    def test_find_latest_job_by_idempotency_key_status_filter_on_pg(self) -> None:
        self._save_job(job_id="job-b2-running", status="running", idempotency_key="idem-b2-status")
        self.assertIsNone(
            self.store.find_latest_job_by_idempotency_key(idempotency_key="idem-b2-status", statuses=["completed"])
        )
        self.assertIsNotNone(
            self.store.find_latest_job_by_idempotency_key(idempotency_key="idem-b2-status", statuses=["running"])
        )

    def test_find_latest_job_by_idempotency_key_absent_returns_none(self) -> None:
        self.assertIsNone(self.store.find_latest_job_by_idempotency_key(idempotency_key="idem-b2-absent"))

    def test_find_pending_plan_review_session_reads_pg(self) -> None:
        request_payload = {"target_company": "OpenAI", "plan_id": "plan-b2", "request_source": "test.b2"}
        self.store.create_plan_review_session(
            target_company="OpenAI",
            request_payload=request_payload,
            plan_payload={"plan_id": "plan-b2", "target_company": "OpenAI"},
            gate_payload={"required_before_execution": True},
        )
        found = self.store.find_pending_plan_review_session(target_company="OpenAI", request_payload=request_payload)
        self.assertIsNotNone(
            found,
            "pending plan-review dedup must find the PG-persisted session (pre-fix read the empty SQLite shadow -> None)",
        )
        self.assertEqual(found["status"], "pending")

    def test_find_pending_plan_review_session_absent_returns_none(self) -> None:
        self.assertIsNone(
            self.store.find_pending_plan_review_session(
                target_company="Nobody", request_payload={"target_company": "Nobody"}
            )
        )


if __name__ == "__main__":
    unittest.main()
